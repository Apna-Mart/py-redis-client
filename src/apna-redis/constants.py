import datetime
from typing import Union, List, Set


RedisNativeTypes = Union[
    str, float, int, bool, datetime.datetime,
    datetime.date, datetime.time]
CacheDataValueType = Union[RedisNativeTypes, List[
    RedisNativeTypes], Set[RedisNativeTypes]]
CacheDataType = Union[CacheDataValueType, dict[
    str, CacheDataValueType]]
ExpiryType = Union[datetime.timedelta, None]
CONVERT = "convert"
UNCONVERT = "unconvert"
LIST = "list"
SET = "set"
HASHMAP = "hmap"
ADDRESS = "addr"
LIST_SEP = "lsep"
SET_SEP = "ssep"
