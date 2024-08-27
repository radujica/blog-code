import logging
from collections import deque
from functools import wraps
from typing import Optional, Callable

_rollback_queue: Optional[deque] = None


logger = logging.getLogger()


class TransactionAlreadyExistsException(Exception):
    pass


# TODO: this should perhaps be used as transaction.rollback(...), so re-org the dir structure
def rollback(rollback_func: Callable):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            global _rollback_queue

            # this avoids making the function un-callable if not used within transaction;
            # also being explicit, since empty queue also violates expectation
            if _rollback_queue is not None:
                _rollback_queue.appendleft((rollback_func, args, kwargs))

            return func(*args, **kwargs)

        return wrapper
    return decorator


def warn_when_missing_rollback_decorators():
    logger.warning("Found no function decorated with a rollback; are you sure you need a @transaction?")


def transaction(func: Callable):
    @wraps(func)
    def wrapper(*args, **kwargs):
        global _rollback_queue

        # to prevent transaction-within-transaction, since once a transaction fails,
        # all callbacks encountered so far are ran, including from outer transaction
        if _rollback_queue is not None:
            raise TransactionAlreadyExistsException()

        try:
            _rollback_queue = deque()

            return func(*args, **kwargs)
        except Exception as e:
            # TODO: what if one of these fails?
            for rollback_fn, rollback_args, rollback_kwargs in _rollback_queue:
                rollback_fn(*rollback_args, **rollback_kwargs)

            raise e
        finally:
            if len(_rollback_queue) == 0:
                warn_when_missing_rollback_decorators()
            _rollback_queue = None

    return wrapper
