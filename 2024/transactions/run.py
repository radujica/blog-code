from decorator import rollback, transaction

# .commit, .abort, .rollback


"""Refs
https://www.andy-pearce.com/blog/posts/2024/Feb/whats-new-in-python-312-f-strings-and-interpreter-changes/

https://github.com/thuhak/pytransaction
https://github.com/zopefoundation/transaction
"""


def revert_write(name: str):
    print("reverting write!")


def revert_log():
    print("reverting log!")


def revert_all():
    print("reverting all!")


# fn = rollback(rollback_func=revert_write)(write)
# fn(df=...)
@rollback(rollback_func=revert_write)
def write(name: str):
    print("written!")


@rollback(rollback_func=revert_log)
def log():
    # raise Exception()
    print("logged!")


@transaction
def run_two_things(name: str):
    write(name)
    log()


"""This approach requires a global variable + naming the transaction in the rollback decorators!

The problem with the runtime is that it has to be dynamic, e.g. a loop of transactions, and also allow multiple non-overlapping transactions

for t in ["table1", "table2"]:
    run_two_things(t)
    
alternatives
    
- one option is to have a single decorator which does the rollback registering <- but this is ugly, and still need a way to track if processing even reached that point
- ??

TODO: check what happens with transaction within transaction <- actually think it's fine? though this assumes the exception in not caught in the inner transaction
TODO: how to tackle concurrency? <- this requires a transaction name, otherwise can't tell from where does the rollback registered func originates
"""
if __name__ == '__main__':
    print("in main")
    for t in ["table1", "table2"]:
        run_two_things(t)
