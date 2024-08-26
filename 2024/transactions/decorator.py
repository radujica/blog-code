import logging
from collections import deque
from functools import wraps
from typing import Optional, Callable

# TODO: perhaps this name could be configurable via env-var, to avoid clashes
QUEUE: Optional[deque] = None


logger = logging.getLogger()


class TransactionAlreadyExistsException(Exception):
    pass


def rollback(rollback_func: Callable):
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            global QUEUE

            # this avoids making the function un-callable if not used within transaction;
            # also being explicit, since empty queue also violates expectation
            if QUEUE is not None:
                QUEUE.appendleft((rollback_func, args, kwargs))

            return func(*args, **kwargs)

        return wrapped
    return decorator


def warn_when_missing_rollback_decorators():
    logger.warning("Found no function decorated with a rollback; are you sure you need a @transaction?")


def transaction():
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            global QUEUE

            # to prevent transaction-within-transaction, since once a transaction fails, all callbacks encountered so far are ran (including from previous transaction)
            if QUEUE is not None:
                raise TransactionAlreadyExistsException()

            try:
                QUEUE = deque()

                return func(*args, **kwargs)
            except Exception as e:
                # TODO: what if one of these fails?
                for rollback_fn, rollback_args, rollback_kwargs in QUEUE:
                    rollback_fn(*rollback_args, **rollback_kwargs)

                raise e
            finally:
                if len(QUEUE) == 0:
                    warn_when_missing_rollback_decorators()
                QUEUE = None

        return wrapped

    return decorator
