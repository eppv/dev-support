import timeit
from external.utils.var import color


def track_exec_time(func):
    def wrapper(*args, **kwargs):
        execution_time = timeit.timeit(lambda: func(*args, **kwargs), number=1)
        execution_time_str = str(execution_time)
        print(f"{color('INFO', 'green')} - Function '{color(func.__name__, 'yellow')}' executed in {color(execution_time_str, 'yellow')} seconds.")
        return func(*args, **kwargs)
    return wrapper
