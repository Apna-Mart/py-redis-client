import datetime
import redis
from redis import client
from typing import Union

from conversions import Conversions
from exceptions import MethodNotImplementedError


class _RedisDB:
    conv = Conversions()

    def __init__(self, redis_conn: Union[
        redis.Redis, client.Pipeline]) -> None:
        self.db_instance = redis_conn
    
    def exists(self, *keys) -> int:
        for key in keys:
            self.conv.key_validate(key)
        return self.db_instance.exists(*keys)
    
    def delete(self, *keys) -> int:
        for key in keys:
            self.conv.key_validate(key)
        return self.db_instance.delete(*keys)

    def expire(self, expiry: datetime.timedelta,
               *keys) -> bool:
        res = []
        for key in keys:
            self.conv.key_validate(key)
            res.append(self.db_instance.expire(
                key, expiry))
        return all(res)
    
    def flush(self) -> bool:
        return self.db_instance.flushdb()
    
    def db_multi(self, multi: bool = True) -> None:
        if multi:
            if not isinstance(
            self.db_instance, client.Pipeline):
                raise MethodNotImplementedError(
                    "Cannot be implemented without redis"
                    " pipeline instance")
            self.db_instance.multi()
    
    def db_execute(self, multi: bool = True) -> None:
        if multi:
            if not isinstance(
            self.db_instance, client.Pipeline):
                raise MethodNotImplementedError(
                "Cannot be implemented without redis"
                " pipeline instance")
            self.db_instance.execute()
    
    def __getattr__(self, name):
        raise MethodNotImplementedError(
            "The class has no method '{}'".format(name))
