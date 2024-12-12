from django_redis import get_redis_connection

from cache.cache import Cache


cache = Cache(get_redis_connection()) 
