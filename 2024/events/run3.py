from datetime import datetime
from typing import Iterable

import polars as pl


pipeline_to_tables = {
    "purchases": ["purchases", "num_purchases_per_day", "purchases_per_buyer"],
    "exchange_rate": ["exchange_rate"]
}
table_to_pipeline = {t: k for k, v in pipeline_to_tables.items() for t in v}
table_to_pipeline_df = pl.DataFrame({"table_name": table_to_pipeline.keys(), "pipeline": table_to_pipeline.values()})


# this fixes the inference if tables can be computed by different pipelines, i.e. with different correlation id
def keep_only_complete_propagation(events: pl.DataFrame) -> pl.DataFrame:
    df = (
        events
        .join(table_to_pipeline_df, on="table_name", how="left")
        .with_columns(pl.len().over([pl.col("correlation_id"), pl.col("pipeline")]).alias("count"))
        .with_columns(pl.max("count").over(pl.col("pipeline")).alias("max"))
        .filter(pl.col("count") == pl.col("max"))
        .drop("pipeline", "count", "max")
    )

    return df


def is_time_to_trigger(events: pl.DataFrame, last_run_ts: datetime, dependencies: Iterable[str]) -> bool:
    df = events.filter(pl.col("table_name").is_in(dependencies))
    df = df.filter(pl.col("timestamp") > last_run_ts)
    df = keep_only_complete_propagation(df)

    return not df.is_empty()


def run():
    events = pl.read_csv("data/events3.csv", has_header=True, try_parse_dates=True)
    last_run_ts = datetime.min
    dependencies = ["purchases", "num_purchases_per_day", "exchange_rate"]

    print(is_time_to_trigger(events, last_run_ts, dependencies))


if __name__ == "__main__":
    run()
