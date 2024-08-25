from datetime import datetime
from typing import Iterable

import polars as pl

# this is basically the naive version, just filter for table and timestamp
def is_time_to_trigger(events: pl.DataFrame, last_run_ts: datetime, dependencies: Iterable[str]) -> bool:
    df = events.filter(pl.col("table_name").is_in(dependencies))
    df = df.filter(pl.col("timestamp") > last_run_ts)

    return not df.is_empty()


def run():
    events = pl.read_csv("data/events1.csv", has_header=True, try_parse_dates=True)
    last_run_ts = datetime.min
    dependencies = ["purchases"]

    print(is_time_to_trigger(events, last_run_ts, dependencies))


if __name__ == "__main__":
    run()
