import redis
from redis import client
from typing import List

from exceptions import InvalidFormatError


class Operation:
    def __init__(
            self, klass,
            executable: str, variables: List[str] = [],
            pipe: client.Pipeline = None,
            args: list = [], kwargs: dict = {}) -> None:
        self.pipe = pipe
        self.variables = variables
        self.klass = klass
        self.executable = executable
        self.args = args
        self.kwargs = kwargs
    
    @property
    def execute(self):
        try:
            instance = self.klass(self.pipe)
            method = getattr(instance, self.executable)
            method(*self.args, **self.kwargs)
        except Exception as err:
            raise InvalidFormatError(
                "Invalid operation. Error - {}".format(err))


class PipeExecution:
    operations: List[Operation] = []

    def __init__(self, redis: redis.Redis) -> None:
        self.redis = redis

    def add_operation(
            self, operation: Operation) -> None:
        self.operations.append(operation)
    
    @property
    def clear_operations(self):
        self.operations = []
    
    @property
    def execute(self):
        variables = []
        with self.redis.pipeline() as pipe:
            for op in self.operations:
                op.pipe = pipe
                op.execute
                variables.extend(op.variables)
            result = pipe.execute()
        if not variables:
            variables = [""] * len(result)
        try:
            return {variables[idx]: e for idx, e in enumerate(
                result)}
        except:
            raise InvalidFormatError(
                "Mismatch between variables and results "
                "in Pipe executions")
