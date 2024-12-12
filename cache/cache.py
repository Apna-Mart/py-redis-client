import datetime
import redis
from typing import Union

from cache.mapper import Mapper
from constants import CacheDataType
from db.base import _RedisDB
from exceptions import InvalidFormatError


class Cache:
    def __init__(self, redis_conn) -> None:
        if not isinstance(redis_conn, redis.Redis):
            raise InvalidFormatError(
                "Redis connection not configured for cache")
        self.redis_conn = redis_conn
    
    def delete(self, *keys) -> bool:
        _RedisDB(self.redis_conn).delete(*keys)
        return True
    
    def exists(self, *keys) -> bool:
        res = _RedisDB(self.redis_conn).exists(*keys)
        return True if res == len(keys) else False
    
    def expire(self, expiry: int,
               *keys) -> bool:
        if not isinstance(expiry, int):
            raise InvalidFormatError(
                "Cache expire format wrong. Expected "
                "int got {}".format(type(expiry)))
        expiry = datetime.timedelta(seconds=expiry)
        return _RedisDB(self.redis_conn).expire(
            expiry, *keys)
    
    def flush(self) -> bool:
        return _RedisDB(self.redis_conn).flush()
    
    def __set(self, data: dict, timeout: int = None,
            separator: str = "") -> None:
        if timeout is not None and not isinstance(
            timeout, int):
            raise InvalidFormatError(
                "Cache set expiry wrong. Expected "
                "int got {}".format(type(timeout)))
        timeout = datetime.timedelta(
            seconds=timeout) if timeout else None
        separator = str(separator) if separator else None
        Mapper.map_to_db(
            self.redis_conn, data, timeout, separator)
    
    def set(self, key: str, value: CacheDataType,
            timeout: int = None, separator: str = "") -> None:
        """
        Will not set any value if it is None
        """
        self.__set({key: value}, timeout, separator)
    
    def set_many(self, data: dict,
            timeout: int = None,
            separator: str = "") -> None:
        """
        None or empty dicts/lists/sets are skipped.
        """
        if not isinstance(data, dict):
            raise InvalidFormatError(
                "Cache set format wrong. Expected "
                "dict got {}".format(type(data)))
        self.__set(data, timeout, separator)
    
    def get(self, key: str) -> Union[
        CacheDataType, None]:
        return Mapper.unmap_from_db(
            self.redis_conn, key).get(key)
    
    def get_many(self, *keys) -> dict[
        str: CacheDataType]:
        return Mapper.unmap_from_db(
            self.redis_conn, *keys)
