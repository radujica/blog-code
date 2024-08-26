from decorator import rollback, transaction

# the inner funcs are purely to make mocking easier for the tests

def inner_revert_write(name: str):
    print(f"reverting write of {name}!")


def revert_write(name: str):
    inner_revert_write(name)


def inner_revert_log():
    print("reverting log!")


def revert_log():
    inner_revert_log()


def inner_write(name: str):
    print(name)


@rollback(rollback_func=revert_write)
def write(name: str):
    inner_write(name)


def inner_log():
    print("logged!")


@rollback(rollback_func=revert_log)
def log():
    inner_log()


@transaction
def run_two_things(name: str):
    write(name)
    log()
