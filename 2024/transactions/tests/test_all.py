from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from unittest import mock

import pytest

from decorator import transaction, TransactionAlreadyExistsException
from tests.pipeline import run_two_things, write


def test_sanity():
    # aka valid case is runnable, args/kwargs are passed, etc.
    run_two_things("name")


@mock.patch("tests.pipeline.revert_log")
@mock.patch("tests.pipeline.log")
@mock.patch("tests.pipeline.inner_revert_write")
@mock.patch("tests.pipeline.inner_write")
def test_first_fails_second_not_executed_is_only_first_reverted(inner_write_mock, revert_write_mock, log_mock, revert_log_mock):
    inner_write_mock.side_effect = Exception()
    with pytest.raises(Exception):
        run_two_things("name")

    revert_write_mock.assert_called_once()
    assert not log_mock.called
    assert not revert_log_mock.called


@mock.patch("tests.pipeline.inner_revert_log")
@mock.patch("tests.pipeline.inner_revert_write")
@mock.patch("tests.pipeline.inner_log")
def test_first_succeeded_second_fails_are_all_reverted(inner_log_mock, revert_write_mock, revert_log_mock):
    inner_log_mock.side_effect = Exception()
    with pytest.raises(Exception):
        run_two_things("name")

    revert_write_mock.assert_called_once()
    revert_log_mock.assert_called_once()


def test_multiple_transaction_are_not_allowed():
    @transaction
    def f1():
        pass

    @transaction
    def f2():
        f1()

    # note this still allows the first transaction to start
    with pytest.raises(TransactionAlreadyExistsException):
        f2()


@mock.patch("decorator.warn_when_missing_rollback_decorators")
def test_is_warning_when_no_rollback_registered(warn_mock):
    @transaction
    def f1():
        pass

    f1()
    warn_mock.assert_called_once()


def test_rollback_decorated_function_is_still_runnable_on_its_own():
    write("name")


@pytest.mark.skip("not meant to work :/")
@mock.patch("tests.pipeline.inner_log")
def test_queue_is_isolated_per_thread(inner_log_mock):
    num_expected_executions = 20

    with ThreadPoolExecutor(max_workers=10) as executor:
        for i in range(num_expected_executions):
            executor.submit(run_two_things, f"name{i}")

    assert inner_log_mock.call_count == num_expected_executions


# needs to be pickle-able for ProcessPoolExecutor, so at the top level in the file/module
@transaction
def f3():
    return 1


# for process cannot use mock as the processes run on different stacks
def test_queue_is_isolated_per_process():
    num_expected_executions = 20

    futures = list()
    with ProcessPoolExecutor(max_workers=10) as executor:
        for i in range(num_expected_executions):
            futures.append(executor.submit(f3))

    assert sum((f.result() for f in futures)) == num_expected_executions
