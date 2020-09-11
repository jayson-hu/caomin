import redis

r = redis.Redis(host='127.0.0.1', port=6379, db=1)
result=r.sadd('url1','sssssss1')
print(result)
if result:
    print('success')
else:
    print('fail')