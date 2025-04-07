import redis


redis_client = redis.Redis(host='redis', port=6379, db=0)


def get_cache_by_key(key: str):
    value = redis_client.get(key)

    return value


def set_cache_by_key(key: str, value: str):
    redis_client.set(key, value)

    return "OK"


def check_cache_by_key(key: str):
    exists = redis_client.exists(key)
    return "True" if exists > 0 else "False"


def delete_cache_by_key(key: str):
    redis_client.delete(key)

    return "OK"


def get_cache_keys():
    keys = [key.decode('utf-8') for key in redis_client.keys()]

    return keys
