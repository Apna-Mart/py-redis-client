from typing import Iterable, List

from constants import (
    ExpiryType, RedisNativeTypes, CONVERT, UNCONVERT)
from conversions import Conversions
from db.base import _RedisDB
from exceptions import InavlidRedisValueError


class _RedisListPipeline(_RedisDB):
    conv = Conversions(CONVERT)
    unconv = Conversions(UNCONVERT)

    def set(self, key: str, data: Iterable,
            expiry: ExpiryType = None,
            redis_multi: bool = False) -> None:
        if not isinstance(data, Iterable):
            raise InavlidRedisValueError(
                "Invalid value for List set. "
                "Expected iterable, got - {}".format(
                    type(data)))
        res = [self.conv.final_value(
            obj) for obj in data]
        self.db_multi(redis_multi)
        self.delete(*[key])
        self.db_instance.rpush(key, *res)
        if expiry:
            self.expire(expiry, *[key])
        self.db_execute(redis_multi)
    
    def execute_get(self, key: str):
        return self.db_instance.lrange(
            key, 0, -1)
    
    def format_get(self, *values) -> List[
        RedisNativeTypes]:
        return [self.unconv.final_value(
            obj) for obj in values]


class _RedisListClient(_RedisListPipeline):
    def get(self, key: str) -> List[
        RedisNativeTypes]:
        return self.format_get(*self.execute_get(
            key))
