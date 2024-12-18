import datetime
from typing import Union, Any

from py_redis_client.constants import (
    RedisNativeTypes, CONVERT, UNCONVERT)
from py_redis_client.exceptions import (
    MethodNotImplementedError, InavlidRedisKeyError,
    InavlidRedisValueError)


class Conversions:
    types_allowed = [
        "str", "int", "bool", "float",
        "datetime", "date", "time"]

    def __init__(self, conv: str,
                 decode: bool = True) -> None:
        self.kl_typ = conv
        self.decode = decode
    
    def __direct_convert(self, klass, value):
        return (klass.__name__ + str(value) if
                self.kl_typ == CONVERT else klass(value[
                    len(klass.__name__):]))

    def __str(self, value: str) -> str:
        return self.__direct_convert(str, value)
    
    def __int(self, value: Union[
        int, str]) -> Union[int, str]:
        return self.__direct_convert(int, value)
    
    def __float(self, value: Union[
        float, str]) -> Union[float, str]:
        return self.__direct_convert(float, value)
    
    def __bool(self, value: Union[
        bool, str]) -> Union[bool, str]:
        return ("bool" + str(value) if self.kl_typ == CONVERT
                else True if value[4:] == "True" else False)
    
    def __datetime(self, value: Union[
        datetime.datetime, str]) -> Union[datetime.datetime, str]:
        return ("datetime" + value.isoformat() if
                self.kl_typ == CONVERT else
                datetime.datetime.fromisoformat(value[8:]))
    
    def __date(self, value: Union[
        datetime.date, str]) -> Union[datetime.date, str]:
        return ("date" + value.strftime("%Y-%m-%d") if
                self.kl_typ == CONVERT else datetime.datetime.strptime(
                    value[4:], "%Y-%m-%d").date())
    
    def __time(self, value: Union[
        datetime.time, str]) -> Union[datetime.time, str]:
        return ("time" + value.strftime("%H:%M:%S") if
                self.kl_typ == CONVERT else datetime.datetime.strptime(
                    value[4:], "%H:%M:%S").time())
    
    def key_validate(self, key: Any) -> bool:
        if not isinstance(key, str):
            raise InavlidRedisKeyError(
                "Key passed not string - {}. Type - {}".format(
                    key, type(key).__name__))
        return True
    
    def final_value(self, value: Union[
        RedisNativeTypes, bytes]) -> RedisNativeTypes:
        error_msg = ("Value passed not valid - {}. Type - {}".format(
            value, type(value).__name__)
            if self.kl_typ == CONVERT else
            "Found value not of redis native type - {}".format(
                value))
        try:
            method_name = "_{}__".format(
                self.__class__.__name__)
            if self.kl_typ == CONVERT:
                method_name += type(value).__name__
            elif self.kl_typ == UNCONVERT:
                if self.decode:
                    value = value.decode("utf-8")
                for k in self.types_allowed:
                    if value.startswith(k):
                        method_name += k
                        break
            else:
                raise MethodNotImplementedError(
                    "Conversions set for not applicable method")
            return getattr(self, method_name)(value)
        except MethodNotImplementedError:
            raise
        except AttributeError:
            raise InavlidRedisValueError(error_msg)
