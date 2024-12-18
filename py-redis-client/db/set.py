from typing import Iterable, Set

from constants import (
    ExpiryType, RedisNativeTypes, CONVERT, UNCONVERT)
from conversions import Conversions
from db.base import _RedisDB
from exceptions import InavlidRedisValueError


class _RedisSetPipeline(_RedisDB):
    conv = Conversions(CONVERT)
    unconv = Conversions(UNCONVERT)

    def set(self, key: str, data: Iterable,
            expiry: ExpiryType = None,
            redis_multi: bool = False) -> None:
        if not isinstance(data, Iterable):
            raise InavlidRedisValueError(
                "Invalid value for Set set. "
                "Expected iterable, got - {}".format(
                    type(data)))
        res = [self.conv.final_value(
            obj) for obj in data]
        self.db_multi(redis_multi)
        self.delete(*[key])
        self.db_instance.sadd(key, *res)
        if expiry:
            self.expire(expiry, *[key])
        self.db_execute(redis_multi)
    
    def execute_get(self, key: str):
        return self.db_instance.smembers(
            key)
    
    def format_get(self, *values) -> Set[
        RedisNativeTypes]:
        return {self.unconv.final_value(
            obj) for obj in values}


class _RedisSetClient(_RedisSetPipeline):
    def get(self, key: str) -> Set[
        RedisNativeTypes]:
        return self.format_get(*self.execute_get(
            key))
