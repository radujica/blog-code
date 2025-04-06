import dagster as dg
from dagster_duckdb_pandas import DuckDBPandasIOManager


@dg.asset
def table1(context: dg.AssetExecutionContext) -> None:
    context.log.info("Hello, world table1!")


@dg.asset(deps=[table1])
def table2(context: dg.AssetExecutionContext) -> None:
    context.log.info("Hello, world table2!")


@dg.asset_check(asset=table2)
def table2_checks():
    return dg.AssetCheckResult(
        passed=True,
    )


@dg.asset
def table3(context: dg.AssetExecutionContext, table1) -> None:
    context.log.info("Hello, world table3!")


class Table4Builder:
    def compute(self):
        return "table4-data"
    

@dg.asset
def table4() -> None:
    Table4Builder().compute()


daily_job = dg.define_asset_job(
    "daily_reload", selection=["table1"]
)

daily_schedule = dg.ScheduleDefinition(
    job=daily_job,
    cron_schedule="0 2 * * *",
)

def has_new_files() -> bool:
    ...


@dg.sensor(
    job=daily_job,
    minimum_interval_seconds=5,
)
def new_file_sensor():
    new_files = has_new_files()
    if new_files:
        yield dg.RunRequest()
    else:
        yield dg.SkipReason("No new files found")


retry_policy = dg.RetryPolicy(
    max_retries=3,
    delay=5,  # in s
    backoff=dg.Backoff.EXPONENTIAL,
)


table4_job = dg.define_asset_job(
    "table4_job",
    selection=["table4"],
    op_retry_policy=retry_policy,
)

@dg.asset_sensor(asset_key=dg.AssetKey("table4"), job_name="table4_job")
def table4_data_sensor():
    return dg.RunRequest()


partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01")

@dg.asset(partitions_def=partitions)
def table5(context: dg.AssetExecutionContext):
    date = context.partition_key
    ...

defs = dg.Definitions(
    assets=[table1, table2, table3, table4, table5],
    resources={
        "io_manager": DuckDBPandasIOManager(database="sales.duckdb", schema="public")
    },
    jobs=[daily_job, table4_job],
    schedules=[daily_schedule],
    sensors=[new_file_sensor, table4_data_sensor],
    asset_checks=[table2_checks],
)


@dg.asset
def loaded_file() -> str:
    return "file contents"


@dg.asset
def processed_file(loaded_file: str) -> str:
    return loaded_file.strip()


def test_processed_file() -> None:
    assert processed_file(" contents    ") == "contents"
