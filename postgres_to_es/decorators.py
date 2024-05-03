import time
from functools import wraps
import random
from logger import logger


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10, exceptions=(Exception,)):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            sleep_time = start_sleep_time

            while True:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    sleep_time = min(
                        (sleep_time * (1 + random.uniform(-0.5, 0.5))) * factor,
                        border_sleep_time
                    )
                    logger.warning(f"Failed to execute function [{func}]. Sleep {sleep_time}s. Error: {str(e)}")
                    time.sleep(sleep_time)
                except Exception as e:
                    logger.error(f'Failed to execute function [{func}]. Error: {str(e)}')
                    break

        return wrapper

    return decorator
