class Error(Exception):
    """
    Base class for exceptions in this module.
    """

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class MethodNotImplementedError(Error):
    """
        Exception raised if method not implemented
    """
    pass


class InvalidFormatError(Error):
    """
        Exception raised if an invalid value found
    """
    pass


class InavlidRedisValueError(Error):
    """
        Exception raised if an invalid value passed/found
    """
    pass


class InavlidRedisKeyError(Error):
    """
        Exception raised if an invalid key passed/found
    """
    pass
