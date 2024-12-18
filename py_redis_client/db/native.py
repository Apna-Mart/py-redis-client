from typing import Iterable

from py_redis_client.constants import (
    ExpiryType, RedisNativeTypes, CONVERT, UNCONVERT)
from py_redis_client.conversions import Conversions
from py_redis_client.db.base import _RedisDB
from py_redis_client.exceptions import InvalidFormatError


class _RedisNativePipeline(_RedisDB):
    conv = Conversions(CONVERT)
    unconv = Conversions(UNCONVERT)

    def set_many(self, data: dict,
                 expiry: ExpiryType = None, 
                 redis_multi: bool = False) -> None:
        if not isinstance(data, dict):
            raise InvalidFormatError(
                "Invalid format for Native set_many. "
                "Expected dict, got {}".format(type(data)))
        res = {}
        keys = []
        for key, value in data.items():
            res[key] = self.conv.final_value(
                value)
            keys.append(key)
        if expiry:
            self.db_multi(redis_multi)
        self.db_instance.mset(res)
        if expiry:
            self.expire(expiry, *keys)
            self.db_execute(redis_multi)
    
    def execute_get_many(self, *keys):
        return self.db_instance.mget(keys)

    def format_get_many(
        self, keys: Iterable[str], values: Iterable[
            RedisNativeTypes]) -> dict[
            str: RedisNativeTypes]:
        return {key: self.unconv.final_value(values[idx])
                for idx, key in enumerate(keys)
                if values[idx] is not None}


class _RedisNativeClient(_RedisNativePipeline):
    def set(self, key: str, value: str,
            expiry: ExpiryType = None) -> bool:
        return self.db_instance.set(
            key, self.conv.final_value(value),
            ex=expiry)
    
    def get(self, key: str) -> RedisNativeTypes:
        val = self.db_instance.get(key)
        return self.unconv.final_value(
            val) if val is not None else None
    
    def get_many(self, *keys) -> dict[
        str: RedisNativeTypes]:
        return self.format_get_many(
            keys, self.execute_get_many(
                *keys))
