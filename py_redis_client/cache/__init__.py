from django.conf import settings

from py_redis_client.cache.cache import Cache


# Dictionary-like object for multiple Cache instances
def get_caches():
    """
    Provides a dictionary-like interface to access multiple Cache instances.

    Returns:
        dict[str, Cache]: A dictionary where keys are cache names and values are Cache instances.
    """
    return {name: Cache(name) for name in settings.CACHES.keys()}

# Global instance of Cache for default cache backend
cache = Cache("default")
caches = get_caches()
