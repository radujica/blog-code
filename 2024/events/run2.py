from datetime import datetime
from typing import Iterable

import polars as pl


# this extends to not trigger too early
def keep_only_complete_propagation(events: pl.DataFrame) -> pl.DataFrame:
    df = (
        events
        .with_columns(pl.len().over(pl.col("correlation_id")).alias("count"))
        .filter(pl.col("count") == pl.max("count"))
        .drop("count")
    )

    return df


def is_time_to_trigger(events: pl.DataFrame, last_run_ts: datetime, dependencies: Iterable[str]) -> bool:
    df = events.filter(pl.col("table_name").is_in(dependencies))
    df = df.filter(pl.col("timestamp") > last_run_ts)
    df = keep_only_complete_propagation(df)

    return not df.is_empty()


def run():
    events = pl.read_csv("data/events2.csv", has_header=True, try_parse_dates=True)
    last_run_ts = datetime.min
    dependencies = ["purchases", "num_purchases_per_day"]

    print(is_time_to_trigger(events, last_run_ts, dependencies))


if __name__ == "__main__":
    run()
