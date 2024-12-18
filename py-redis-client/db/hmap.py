from constants import (
    ExpiryType, RedisNativeTypes, CONVERT, UNCONVERT)
from conversions import Conversions
from db.base import _RedisDB
from exceptions import (
    InvalidFormatError, InavlidRedisValueError)


class _RedisHashMapPipeline(_RedisDB):
    conv = Conversions(CONVERT)
    unconv = Conversions(UNCONVERT)

    def set(self, key: str, data: dict,
            expiry: ExpiryType = None,
            redis_multi: bool = False) -> None:
        if not isinstance(data, dict):
            raise InavlidRedisValueError(
                "Invalid value for Hash Map set. "
                "Expected dict, got {}".format(type(data)))
        res = {k: self.conv.final_value(v) for k, v in 
            data.items()}
        if expiry:
            self.db_multi(redis_multi)
        self.db_instance.hmset(key, res)
        if expiry:
            self.expire(expiry, *[key])
            self.db_execute(redis_multi)
    
    def execute_get(self, key: str):
        return self.db_instance.hgetall(
            key)
    
    def format_get(self, data: dict) -> dict[
        RedisNativeTypes: RedisNativeTypes]:
        if not isinstance(data, dict):
            raise InvalidFormatError(
                "Invalid value for Hash Map format_get. "
                "Expected dict, got {}".format(type(data)))
        return {k.decode("utf-8"): self.unconv.final_value(
            v) for k, v in data.items()}


class _RedisHashMapClient(_RedisHashMapPipeline):
    def get(self, key) -> dict[
        RedisNativeTypes: RedisNativeTypes]:
        return self.format_get(self.execute_get(
            key))
