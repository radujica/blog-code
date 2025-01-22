from datetime import datetime, timedelta
from typing import Dict, List, Tuple
import polars as pl


execution_logs = pl.DataFrame([
  ["table1", "succeeded", datetime(2025, 1, 1, 0, 0), datetime(2025, 1, 1, 0, 5)],
  ["table1", "failed", datetime(2025, 1, 1, 0, 15), None],
  ["table1", "succeeded", datetime(2025, 1, 1, 0, 30), datetime(2025, 1, 1, 0, 40)],
  ["table2", "succeeded", datetime(2025, 1, 1, 2, 0), datetime(2025, 1, 1, 2, 30)],
], schema=["table", "status", "start_ts", "end_ts"], orient="row")
print(execution_logs)

# we assume "simple" cron expressions, i.e. without /,-
table_schedules = pl.DataFrame([
  ["table1", "@hourly"],
  ["table2", "5 * * * *"],
], schema=["table", "schedule"], orient="row")
print(table_schedules)

# we only look at memory
# in "Gi"; assume only 3Gi available
available_resources = 3
table_resources = pl.DataFrame([
  ["table1", 1],
  ["table2", 2.5],
], schema=["table", "memory"], orient="row")
print(table_resources)

mean_durations = (
  execution_logs
  .filter(pl.col("status") == "succeeded")
  .with_columns(
    duration=pl.col("end_ts") - pl.col("start_ts")
  )
  .select("table", "duration")
  .group_by("table")
  .agg(pl.mean("duration").alias("mean_duration"))
)
print(mean_durations)


def to_dict(df: pl.DataFrame, key: str) -> Dict[str, str]:
  return {table: duration[0][0] for table, duration in df.rows_by_key(key=key).items()}


table_to_schedule = to_dict(table_schedules, "table")
table_to_mean_duration = to_dict(mean_durations, "table")
table_to_resources = to_dict(table_resources, "table")


def timedelta_to_polars_interval(td: timedelta) -> str:
  mapping = {
    timedelta(days=1): "1d",
    timedelta(hours=1): "1h",
  }

  return mapping[td]


# yes, croniter would've solved everything regarding cron, including complex -/, expressions
# however its status is unstable
def parse_cron_minutes(cron_expression: str) -> int:
  return int(cron_expression.split(" ", 1)[0])


# these are very naive and missing checks;
# we assume cron is the "else", a simple expression of hourly with potentially some minutes schedule
def schedule_to_first_datetime(schedule: str) -> datetime:
  mapping = {
    "@daily": datetime(2025, 1, 1),
    "@hourly": datetime(2025, 1, 1),
  }
  first_datetime = mapping.get(schedule)
  if not first_datetime:
    first_datetime = datetime(2025, 1, 1, 0, parse_cron_minutes(schedule))

  return first_datetime


def schedule_to_delta_next_trigger(schedule: str) -> timedelta:
  mapping = {
    "@daily": timedelta(days=1),
    "@hourly": timedelta(hours=1)
  }
  delta_next_trigger = mapping.get(schedule)
  if not delta_next_trigger:
    delta_next_trigger = timedelta(hours=1)

  return delta_next_trigger


def generate_expected_runtime_intervals(
  first_datetime: datetime,
  delta_to_next_trigger: timedelta,
  mean_duration: timedelta,
  min_frequency: timedelta
):
  assert mean_duration < delta_to_next_trigger, f"Cannot have overlapping intervals!"
  start_times = (
    pl.datetime_range(
      first_datetime,
      first_datetime + min_frequency,
      timedelta_to_polars_interval(delta_to_next_trigger),
      eager=True
    )
  )
  end_times = start_times + mean_duration

  return list(zip(start_times, end_times))


table_to_intervals = {
  table: generate_expected_runtime_intervals(
    schedule_to_first_datetime(schedule),
    schedule_to_delta_next_trigger(schedule),
    # default if no previous execution (recorded)
    table_to_mean_duration.get(table, timedelta(minutes=10)),
    timedelta(days=30)
  )
  for table, schedule in table_to_schedule.items()
}

# https://www.geeksforgeeks.org/find-intersection-of-intervals-given-by-two-lists/
tables = list(table_to_intervals.keys())
intervals = table_to_intervals.values()

def interval_contains_datetime(interval: Tuple[datetime, datetime], poll: datetime) -> bool:
  return poll >= interval[0] and poll <= interval[1]

def generate_polls(
    from_datetime: datetime,
    duration_to_check: timedelta,
    frequency: timedelta
) -> List[datetime]:
  return pl.datetime_range(
    from_datetime,
    # - to avoid the last value being the first
    from_datetime + duration_to_check - frequency,
    frequency,
    eager=True
  )

# TODO: these should be cleaned up further as intervals depends on the same duration_to_check
def compute_estimated_resource_usage_brute_force(
    intervals: List[List[Tuple[datetime, int, str]]],
    duration_to_check: timedelta,
    frequency: timedelta
) -> List[Tuple[datetime, int, str]]:
  polls = generate_polls(datetime(2025, 1, 1), duration_to_check, frequency)
  
  result = list()
  for p in polls:
    resources = 0
    tables_involved = list()
    for interval_index, table_intervals in enumerate(intervals):
      for interval in table_intervals:
        if interval_contains_datetime(interval, p):
          resources += table_to_resources[tables[interval_index]]
          tables_involved.append(tables[interval_index])
    result.append((p, resources, tables_involved))

  return result


# notes:
# - this is buggy if interval in table_intervals is smaller than freq
# - this also doesn't work if the intervals overlap
def compute_estimated_resource_usage_pointers(
    intervals: List[List[Tuple[datetime, int, str]]],
    duration_to_check: timedelta,
    frequency: timedelta
) -> List[Tuple[datetime, int, str]]:
  polls = generate_polls(datetime(2025, 1, 1), duration_to_check, frequency)
  result = list()
  pointers = [0 for _ in range(len(intervals))]
  lengths = [len(table_intervals) for table_intervals in intervals]
  
  def compare(interval: Tuple[datetime, datetime], poll: datetime) -> int:
    if interval_contains_datetime(interval, poll):
      return 0
    elif poll > interval[1]:
      return 1
    else:
      return -1
    
  for poll in polls:
    resources = 0
    tables_involved = list()
    for interval_index, table_intervals in enumerate(intervals):
      if pointers[interval_index] >= lengths[interval_index]:
        continue

      comparison = compare(table_intervals[pointers[interval_index]], poll)
      if comparison == 0:
        resources += table_to_resources[tables[interval_index]]
        tables_involved.append(tables[interval_index])
      elif comparison > 0:
        pointers[interval_index] += 1
      elif comparison < 0:
        pass
    result.append((poll, resources, tables_involved))

  return result


result = compute_estimated_resource_usage_brute_force(intervals, duration_to_check=timedelta(hours=6), frequency=timedelta(seconds=30))
filtered_result = [r for r in result if r[1] > available_resources]
print(any(filtered_result))

# only in notebook
import altair as alt
df = pl.DataFrame(result, schema=["timestamp", "sum_resources", "tables"], orient="row")
base = alt.Chart(df).mark_line().encode(x="timestamp", y="sum_resources")
line = alt.Chart().mark_rule().encode(y=alt.datum(available_resources), color=alt.value("red"))
base + line


result_pointers = compute_estimated_resource_usage_pointers(intervals, duration_to_check=timedelta(hours=6), frequency=timedelta(seconds=30))
filtered_result_pointers = [r for r in result if r[1] > available_resources]

assert filtered_result == filtered_result_pointers
assert result == result_pointers

print("timing...")
import timeit
duration = timedelta(days=30)
frequency = timedelta(seconds=30)

brute_time = timeit.timeit(lambda: compute_estimated_resource_usage_brute_force(intervals, duration, frequency), number=10)
pointers_time = timeit.timeit(lambda: compute_estimated_resource_usage_pointers(intervals, duration, frequency), number=10)

print(f"brute_force={brute_time:.3f}s, pointers={pointers_time:.3f}s")
print("done!")
