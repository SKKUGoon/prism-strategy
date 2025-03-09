import time
import functools

def timeit(func):
    """Decorator that measures execution time of a function or method.
    
    Args:
        func: The function to be timed
        
    Returns:
        Wrapped function that prints execution time
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        duration = end_time - start_time
        print(f"{func.__name__} took {duration:.4f} seconds to execute")
        return result
    return wrapper
