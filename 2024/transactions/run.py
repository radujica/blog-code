from decorator import rollback, transaction


def revert_write(name: str):
    print("reverting write!")


def revert_log():
    print("reverting log!")


def revert_all():
    print("reverting all!")


# aka rollback(rollback_func=revert_write)(write)(name=...)
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


if __name__ == '__main__':
    print("in main")
    for t in ["table1", "table2"]:
        run_two_things(t)
