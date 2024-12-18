import datetime
import redis
from typing import Union, Any

from django.core.cache import caches
from django.conf import settings

from py_redis_client.cache.mapper import Mapper
from py_redis_client.constants import CacheDataType
from py_redis_client.db.base import _RedisDB
from py_redis_client.exceptions import InvalidFormatError


class Cache:
    """
    A class to interact with Redis-backed Django caches.

    This class provides methods for performing common cache operations, such as 
    storing, retrieving, deleting, and setting expiration times for cache keys.
    It ensures that the specified cache uses a Redis backend.

    Attributes:
        redis_conn (redis.Redis): Redis client instance for interacting with the cache.

    Usage:
        Initialize the Cache class with a valid Django cache name:

            cache = Cache("default")

        Perform operations such as:

            cache.set("key", "value", timeout=3600)
            value = cache.get("key")
            cache.delete("key")

    Note:
        Ensure the cache is configured in Django settings with a Redis backend.
    """

    def __init__(self, cache_name: str = "default") -> None:
        """
        Initializes the Cache instance with the specified Django cache name.

        Args:
            cache_name (str): The name of the cache defined in Django settings.

        Raises:
            InvalidFormatError: If the cache name is not defined in settings or
            is not configured with a Redis backend.
        """
        if cache_name not in settings.CACHES:
            raise InvalidFormatError(
                f"Cache '{cache_name}' is not defined in the Django settings."
            )

        cache = caches[cache_name]
        if not isinstance(cache.client.get_client(), redis.Redis):
            raise InvalidFormatError(
                f"Cache '{cache_name}' is not configured with a Redis backend."
            )
        self.redis_conn = cache.client.get_client()

    def delete(self, *keys: str) -> bool:
        """
        Deletes the specified keys from the cache.

        Args:
            *keys (str): Keys to be deleted from the cache.

        Returns:
            bool: True if the operation is successful.
        """
        _RedisDB(self.redis_conn).delete(*keys)
        return True

    def exists(self, *keys: str) -> bool:
        """
        Checks if all specified keys exist in the cache.

        Args:
            *keys (str): Keys to be checked in the cache.

        Returns:
            bool: True if all keys exist, False otherwise.
        """
        res = _RedisDB(self.redis_conn).exists(*keys)
        return True if res == len(keys) else False

    def expire(self, expiry: int, *keys: str) -> bool:
        """
        Sets an expiration time for the specified keys.

        Args:
            expiry (int): Expiration time in seconds.
            *keys (str): Keys to set the expiration for.

        Returns:
            bool: True if the operation is successful.

        Raises:
            InvalidFormatError: If the expiry is not an integer.
        """
        if not isinstance(expiry, int):
            raise InvalidFormatError(
                f"Cache expire format wrong. Expected int, got {type(expiry)}."
            )
        expiry = datetime.timedelta(seconds=expiry)
        return _RedisDB(self.redis_conn).expire(expiry, *keys)

    def flush(self) -> bool:
        """
        Clears all data from the cache.

        Returns:
            bool: True if the operation is successful.
        """
        return _RedisDB(self.redis_conn).flush()

    def __set(self, data: dict[str, Any], timeout: int = None, separator: str = "") -> None:
        """
        Internal method to set data in the cache.

        Args:
            data (dict): Data to store in the cache.
            timeout (int, optional): Expiration time in seconds. Defaults to None.
            separator (str, optional): Separator for nested keys. Defaults to "".

        Raises:
            InvalidFormatError: If timeout is not an integer.
        """
        if timeout is not None and not isinstance(timeout, int):
            raise InvalidFormatError(
                f"Cache set expiry wrong. Expected int, got {type(timeout)}."
            )
        timeout = datetime.timedelta(seconds=timeout) if timeout else None
        separator = str(separator) if separator else None
        Mapper.map_to_db(self.redis_conn, data, timeout, separator)

    def set(self, key: str, value: CacheDataType, timeout: int = None, separator: str = "") -> None:
        """
        Stores a single key-value pair in the cache.

        Args:
            key (str): The key to store.
            value (CacheDataType): The value to store.
            timeout (int, optional): Expiration time in seconds. Defaults to None.
            separator (str, optional): Separator for nested keys. Defaults to "".

        Note:
            Will not set any value if it is None.
        """
        self.__set({key: value}, timeout, separator)

    def set_many(self, data: dict[str, CacheDataType], timeout: int = None, separator: str = "") -> None:
        """
        Stores multiple key-value pairs in the cache.

        Args:
            data (dict): Dictionary of key-value pairs to store.
            timeout (int, optional): Expiration time in seconds. Defaults to None.
            separator (str, optional): Separator for nested keys. Defaults to "".

        Raises:
            InvalidFormatError: If data is not a dictionary.

        Note:
            None or empty dicts/lists/sets are skipped.
        """
        if not isinstance(data, dict):
            raise InvalidFormatError(
                f"Cache set format wrong. Expected dict, got {type(data)}."
            )
        self.__set(data, timeout, separator)

    def get(self, key: str) -> Union[CacheDataType, None]:
        """
        Retrieves a value for a given key from the cache.

        Args:
            key (str): The key to retrieve.

        Returns:
            CacheDataType | None: The value associated with the key, or None if not found.
        """
        return Mapper.unmap_from_db(self.redis_conn, key).get(key)

    def get_many(self, *keys: str) -> dict[str, CacheDataType]:
        """
        Retrieves values for multiple keys from the cache.

        Args:
            *keys (str): Keys to retrieve.

        Returns:
            dict: A dictionary with keys and their corresponding values.
        """
        return Mapper.unmap_from_db(self.redis_conn, *keys)
