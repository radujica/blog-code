import platform
import sys
from functools import wraps
from types import CodeType
from typing import Callable


# .commit, .abort, .rollback


"""Refs
https://www.andy-pearce.com/blog/posts/2024/Feb/whats-new-in-python-312-f-strings-and-interpreter-changes/

https://github.com/thuhak/pytransaction
https://github.com/zopefoundation/transaction
"""


SYS_COVERAGE_TOOL_ID = 3
SYS_COVERAGE_TOOL_NAME = "TRANSACTION"


def rollback(rollback_func: Callable):
    print("in @rollback")
    def decoratorrr(func):
        print("in decoratorrr")
        @wraps(func)
        def wrappeddd():
            print("in wrappedd")
            return func()

        wrappeddd.has_rollback = True
        wrappeddd.rollback_func = rollback_func

        return wrappeddd
    return decoratorrr


# TODO: something like this might already exist
class IncorrectPlatformException(Exception):
    pass


def monitor_rollback_apply(code: CodeType, instruction_offset: int, callable: object, arg0: object):
    print("--")
    print(code.co_name)  # <- this is parent, e.g. run_two_things
    print(callable.__name__)  # <- actual func, e.g. write
    print(getattr(callable, "has_rollback", False))
    # logic would be: if there's rollback defined, track it globally
    print("---")

# TODO: should also be a context manager
def transaction():
    # TODO: also check for Python >= 3.12
    if platform.python_implementation() != "CPython":
        raise IncorrectPlatformException("lineage_with_inference only works on CPython")

    print("in @transaction")
    def decorator(func):
        print("in decorator")
        @wraps(func)
        def wrapped():
            print("in wrapped")
            try:
                print("running")
                sys.monitoring.use_tool_id(SYS_COVERAGE_TOOL_ID, SYS_COVERAGE_TOOL_NAME)
                sys.monitoring.register_callback(SYS_COVERAGE_TOOL_ID, sys.monitoring.events.CALL, monitor_rollback_apply)
                sys.monitoring.set_events(SYS_COVERAGE_TOOL_ID, sys.monitoring.events.CALL)

                return func()
            except Exception as e:
                print("rolling back")
                # TODO: here need to trigger the rollbacks
                raise e
            finally:
                # TODO: probably need to send a DISABLE event for SYS_COVERAGE_TOOL_ID?
                sys.monitoring.free_tool_id(SYS_COVERAGE_TOOL_ID)
                sys.monitoring.register_callback(SYS_COVERAGE_TOOL_ID, sys.monitoring.events.CALL, None)

        return wrapped
    return decorator


def revert_write():
    print("reverting write!")


def revert_log():
    print("reverting log!")


def revert_all():
    print("reverting all!")


# rollback(rollback_func=revert_write)(write)
@rollback(rollback_func=revert_write)
def write():
    print("written!")


@rollback(rollback_func=revert_log)
def log():
    raise Exception()
    print("logged!")


@transaction()
def run_two_things():
    write()
    log()


if __name__ == '__main__':
    print("in main")
    run_two_things()
