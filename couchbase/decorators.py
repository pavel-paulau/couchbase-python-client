import json

from decorator import decorator


@decorator
def get_as_json(operation, *args, **kargs):
    value = operation(*args, **kargs)
    try:
        if isinstance(value, tuple):
            return json.loads(value[0]), value[1]
        else:
            return json.loads(value)
    except (TypeError, ValueError):
        return value


@decorator
def set_as_json(operation, self, key, value, *args, **kargs):
    try:
        value = json.dumps(value)
    except TypeError:
        pass
    return operation(self, key, value, *args, **kargs)
