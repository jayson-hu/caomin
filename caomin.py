"""
@softeware pycharm
@time 2020/9/6 12:27
"""
import random
import sys
import time
import os
from hashlib import md5

import pymysql
import requests
import re
import csv
import redis
from threading import Thread
from queue import Queue
from fake_useragent import UserAgent

# from urllib import parse
# from urllib import request
# from lxml import etree


# re.findall()
# re.match()

"""
url=''
req=request.Request(url,headers)
resp=request.urlopen(req)
html=resp.read().decode('utf-8')
urlencode进行编码解析
params={'':''}
param=urllib.parse.urlencode(params)
#quote编码
word=''
param=urllib.parse.quote(word)

"""


class CaoMin:
    def __init__(self):
        self.url = 'http://caomintv.com/video/type/1--%E9%9F%A9%E5%9B%BD----hits-{}.html'
        self.num = 0
        self.r = redis.Redis(host='127.0.0.1', port=6379, db=1)
        self.db = pymysql.connect(host='127.0.0.1', port=3306,user='root', password='123456', db='caomin_db', charset='utf8')
        self.cursor = self.db.cursor()
        self.directory='D:/Caomin/'
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def get_first_html(self, url):
        """
        获取一级页面
        :return:
        """
        headers = {'User-Agent': UserAgent().random}

        # headers = {'User-Agent':random.choice(ua_list)}
        resp = requests.get(url=url, headers=headers)
        first_html = resp.text
        # print(first_html)
        return first_html

    def parse_html(self, regex, html):
        """
        解析页面
        :param regex:
        :param html:
        :return:
        """
        pattern = re.compile(regex, re.S)
        film_list = pattern.findall(html)
        return film_list

    def check_duplicated_href(self, film_list):

        for film_href in film_list:
            finger = md5(("http://caomintv.com"+film_href[0]).encode('utf-8')).hexdigest()
            # sadd()添加成功返回1，否则返回0
            result = self.r.sadd('film:urls', finger)
            # if not result:
            #     sys.exit()
            self.save_html(film_href)  # 保存单个链接
            time.sleep(random.uniform(0, 1))

    def save_html(self, film_href):
        """
        保存页面(现在是单个)
        mysql 存储方法
        :return:
        """
        # db=pymysql.connect('localhost','root','123456','maoyandb',charset='utf8')
        # cursor=db.cursor()
        film = 'http://caomintv.com' + str(film_href[0])
        film_name = film_href[1]
        film_star = film_href[2]
        # print('film_star',film_star)
        # print('film_name',film_name)
        # print('film',film)
        # ins='insert into films values (%s,%s,%s)'
        # self.cursor.execute(ins, (film, film_name, film_star))
        # # self.cursor.executemany(ins,[('','',''),()])
        # self.db.commit()
        self.num += 1
        print('%s条数据抓取完成' % self.num)

        # first:每条数据进行redis存储指纹，保证数据的唯一性，确认之后写入数据库
        # second:进行确认之后，需要对数据进行进行大批量的csv存储，单次写入增加磁盘IO，
        # films = []
        # for film_info in film_list:
        #     # film_dict['film_href'] = 'http://caomintv.com'+film_info[0]
        #     # film_dict['film_name'] = film_info[1].strip()
        #     # film_dict['film_star'] = film_info[2].strip()
        #     one_film = ('http://caomintv.com' + film_info[0], film_info[1].strip(), film_info[2].strip())
        #     self.num += 1
        #     films.append(one_film)
        # print(film_dict)
        film_directory='{}film_test1.csv'.format(self.directory)
        with open(film_directory, 'a') as f:
            writer = csv.writer(f)
            # writer.writerow(film_href)
            writer.writerow([film,film_name,film_star])
            # writer.writerows(films)

    def get_page_last_item(self, regex, page):
        """
        利用某一个页面，获取到总页数
        :param regex:正则表达式
        :param page:传入的某一个页面页码
        :return: 最后一个页码
        """
        url = self.url.format(page)
        html = self.get_first_html(url=url)
        pattern = re.compile(regex, re.S)
        page_num = pattern.findall(html)[-1]
        if page_num.startswith('.'):
            # page_num=page_num[3:]
            page_num = page_num.lstrip('...')
        print('页面的总页数', page_num)
        return page_num

    def run(self):
        page_item = 1
        get_last_item_regex = '<a class="page-link" href=".*?">(.*?)</a></li><li class="page-item">'
        page_item = self.get_page_last_item(regex=get_last_item_regex, page=page_item)
        first_page_regex = '<li class="col-md-2 col-sm-3 col-xs-4">.*?<h2.*?>.*?<a href="(.*?)" title=(.*?)>.*?</a>.*?<a .*?>(.*?)</a>.*?</li>'
        # for offset in range(0, int(page_item) + 1):
        for offset in range(0, 4):
            url = self.url.format(offset)
            html = self.get_first_html(url=url)
            film_list = self.parse_html(first_page_regex, html=html)
            self.check_duplicated_href(film_list)

            self.save_html(film_list)
            # time.sleep(random.uniform(0, 1))
            print('完成了%s页' % (offset+1))
        self.cursor.close()
        self.db.close()
        print('总共数据%s条' % self.num)


if __name__ == '__main__':
    start_time = time.time()
    print('start_time',start_time)
    caomin = CaoMin()
    caomin.run()
    end_time = time.time()
    cost_time = end_time - start_time
    print("花费的时间%s" % cost_time)
