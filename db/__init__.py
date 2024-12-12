import redis
from redis import client
from typing import Union

from db.native import _RedisNativePipeline, _RedisNativeClient
from db.list import _RedisListPipeline, _RedisListClient
from db.set import _RedisSetPipeline, _RedisSetClient
from db.hmap import _RedisHashMapPipeline, _RedisHashMapClient
from exceptions import InvalidFormatError


class RedisNative:
    def __new__(cls, redis_conn) -> Union[
        _RedisNativeClient, _RedisNativePipeline]:
        if isinstance(redis_conn, client.Pipeline):
            return _RedisNativePipeline(redis_conn)
        if isinstance(redis_conn, redis.Redis):
            return _RedisNativeClient(redis_conn)
        raise InvalidFormatError(
            "Invalid Redis connection type. Expected "
            "redis.Redis or redis.client.Pipeline, "
            "got {}".format(type(redis_conn)))


class RedisList:
    def __new__(cls, redis_conn) -> Union[
        _RedisListClient, _RedisListPipeline]:
        if isinstance(redis_conn, client.Pipeline):
            return _RedisListPipeline(redis_conn)
        if isinstance(redis_conn, redis.Redis):
            return _RedisListClient(redis_conn)
        raise InvalidFormatError(
            "Invalid Redis connection type. Expected "
            "redis.Redis or redis.client.Pipeline, "
            "got {}".format(type(redis_conn)))


class RedisSet:
    def __new__(cls, redis_conn) -> Union[
        _RedisSetClient, _RedisSetPipeline]:
        if isinstance(redis_conn, client.Pipeline):
            return _RedisSetPipeline(redis_conn)
        if isinstance(redis_conn, redis.Redis):
            return _RedisSetClient(redis_conn)
        raise InvalidFormatError(
            "Invalid Redis connection type. Expected "
            "redis.Redis or redis.client.Pipeline, "
            "got {}".format(type(redis_conn)))


class RedisHashMap:
    def __new__(cls, redis_conn) -> Union[
        _RedisHashMapClient, _RedisHashMapPipeline]:
        if isinstance(redis_conn, client.Pipeline):
            return _RedisHashMapPipeline(redis_conn)
        if isinstance(redis_conn, redis.Redis):
            return _RedisHashMapClient(redis_conn)
        raise InvalidFormatError(
            "Invalid Redis connection type. Expected "
            "redis.Redis or redis.client.Pipeline, "
            "got {}".format(type(redis_conn)))
