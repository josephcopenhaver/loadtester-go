# metrics

The output CSV file has the following metric possibilities:
|Column Header Name|Data Type|Description|
| - | - | - |
| sample_time | RFC3339Nano Time String | The time at which the metrics record or interval sample is packaged for emission to the metrics file. |
| interval_id | RFC3339Nano Time String | A guage like value. It is the latest interval_id time among records in the interval sample. An interval id is the time at which a batch of tasks is expected to start being queued and is unique per interval of a loadtest. |
| num_interval_tasks | Integer | A guage like value. This value maps to the configuration of the loadtest at the time that the latest interval_id record in the sample interval's originating task was created. It's the number of tasks per interval the configuration expects to be emitting. |
| lag | Integer | A guage like value. This value maps to the calculated lag in loadtest emission for the latest interval_id record in the sample interval. Always greater than or equal to zero. |
| sum_lag | Duration String | The total amount of time that the loadtest is behind in expected load generation since it started. This value may not cover the latest two sample intervals depending on backpressure rates. Always greater than or equal to zero. |
| num_tasks | Integer | The sampled number of tasks this interval. |
| num_pass | Integer | The sampled number of tasks that completed without any issue this interval. |
| num_fail | Integer | The sampled number of tasks that failed to complete in some fashion this interval. Could be because of a gracefully returned error or due to a panic in the task. |
| num_retry | Integer | The sampled number of tasks that failed this interval because they returned a non-nil error, could be retried, and as such were queued to be tried again. Only present in output file when the Retry() option is set to true ( default value: true) |
| num_panic | Integer | The sampled number of tasks that failed this interval because they panicked. Tasks that increase this metric cannot be retried. Avoid panicking in your tasks! |
| min_queue_latency | Duration String | The smallest duration a task spent in the queue phase for tasks completed in the sample interval. |
| avg_queue_latency | Duration String | The average duration a task spent in the queue phase for tasks completed in the sample interval. |
| max_queue_latency | Duration String | The largest duration a task spent in the queue phase for tasks completed in the sample interval. |
| min_task_latency | Duration String | The smallest duration a task spent in the execution phase for tasks completed in the sample interval. |
| avg_task_latency | Duration String | The average duration a task spent in the execution phase for tasks completed in the sample interval. |
| max_task_latency | Duration String | The largest duration a task spent in the execution phase for tasks completed in the sample interval. |
| p25_queue_latency | RFC3339Nano Time String \| empty string | 25% of all metric records in the sample interval have a queue phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p50_queue_latency | RFC3339Nano Time String \| empty string | 50% of all metric records in the sample interval have a queue phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p75_queue_latency | RFC3339Nano Time String \| empty string | 75% of all metric records in the sample interval have a queue phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p80_queue_latency | RFC3339Nano Time String \| empty string | 80% of all metric records in the sample interval have a queue phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p85_queue_latency | RFC3339Nano Time String \| empty string | 85% of all metric records in the sample interval have a queue phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p90_queue_latency | RFC3339Nano Time String \| empty string | 90% of all metric records in the sample interval have a queue phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p95_queue_latency | RFC3339Nano Time String \| empty string | 95% of all metric records in the sample interval have a queue phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p99_queue_latency | RFC3339Nano Time String \| empty string | 99% of all metric records in the sample interval have a queue phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p99p9_queue_latency | RFC3339Nano Time String \| empty string | 99.90% of all metric records in the sample interval have a queue phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p99p99_queue_latency | RFC3339Nano Time String \| empty string | 99.99% of all metric records in the sample interval have a queue phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p25_task_latency | RFC3339Nano Time String \| empty string | 25% of all metric records in the sample interval have an execution phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p50_task_latency | RFC3339Nano Time String \| empty string | 50% of all metric records in the sample interval have an execution phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p75_task_latency | RFC3339Nano Time String \| empty string | 75% of all metric records in the sample interval have an execution phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p80_task_latency | RFC3339Nano Time String \| empty string | 80% of all metric records in the sample interval have an execution phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p85_task_latency | RFC3339Nano Time String \| empty string | 85% of all metric records in the sample interval have an execution phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p90_task_latency | RFC3339Nano Time String \| empty string | 90% of all metric records in the sample interval have an execution phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p95_task_latency | RFC3339Nano Time String \| empty string | 95% of all metric records in the sample interval have an execution phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p99_task_latency | RFC3339Nano Time String \| empty string | 99% of all metric records in the sample interval have an execution phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p99p9_task_latency | RFC3339Nano Time String \| empty string | 99.90% of all metric records in the sample interval have an execution phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| p99p99_task_latency | RFC3339Nano Time String \| empty string | 99.99% of all metric records in the sample interval have an execution phase latency at or under this value. It will be an empty string if there are no records in the sample interval. Only present in output file when the MetricsLatencyPercentile() option is set to true ( default value: false) |
| queue_latency_variance | Duration String \| empty string | The welford variance value computed for the queue phase of tasks completed in the sample interval. Always output in nanoseconds squared. No unit indicator will be at the end of the integer value. Will be set to an empty string if the sample size for the interval is less than two. Only present in output file when the MetricsLatencyVariance() option is set to true ( default value: false) |
| task_latency_variance | Duration String \| empty string | The welford variance value computed for the execution phase of tasks completed in the sample interval. Always output in nanoseconds squared. No unit indicator will be at the end of the integer value. Will be set to an empty string if the sample size for the interval is less than two. Only present in output file when the MetricsLatencyVariance() option is set to true ( default value: false) |
| percent_done | Double | A numeric value with precision to the second decimal place ranging from 0 to 100. Only present in output file when the MaxTasks() option is set to a value greater than zero ( default value: 0) |

# changelog

In version 2.x of this "library" sums of queued and execution durations for all tasks in a sample were being reported. This was ultimately not useful information. The various percentiles now reported in 3.x are much more standard ways of interpreting data as well as showing bursty behavior that may exist even in relatively low variance samples of data.

## changes

| version | metric                 | change             |
| ------- | ---------------------- | ------------------ |
| 5.x     | queue_latency_variance | :wavy_dash: format |
| 5.x     | task_latency_variance  | :wavy_dash: format |
| 3.x     | p25_queue_latency      | :heavy_plus_sign:  |
| 3.x     | p50_queue_latency      | :heavy_plus_sign:  |
| 3.x     | p75_queue_latency      | :heavy_plus_sign:  |
| 3.x     | p80_queue_latency      | :heavy_plus_sign:  |
| 3.x     | p85_queue_latency      | :heavy_plus_sign:  |
| 3.x     | p90_queue_latency      | :heavy_plus_sign:  |
| 3.x     | p95_queue_latency      | :heavy_plus_sign:  |
| 3.x     | p99_queue_latency      | :heavy_plus_sign:  |
| 3.x     | p99p9_queue_latency    | :heavy_plus_sign:  |
| 3.x     | p99p99_queue_latency   | :heavy_plus_sign:  |
| 3.x     | p25_task_latency       | :heavy_plus_sign:  |
| 3.x     | p50_task_latency       | :heavy_plus_sign:  |
| 3.x     | p75_task_latency       | :heavy_plus_sign:  |
| 3.x     | p80_task_latency       | :heavy_plus_sign:  |
| 3.x     | p85_task_latency       | :heavy_plus_sign:  |
| 3.x     | p90_task_latency       | :heavy_plus_sign:  |
| 3.x     | p95_task_latency       | :heavy_plus_sign:  |
| 3.x     | p99_task_latency       | :heavy_plus_sign:  |
| 3.x     | p99p9_task_latency     | :heavy_plus_sign:  |
| 3.x     | p99p99_task_latency    | :heavy_plus_sign:  |
| 3.x     | queue_latency_variance | :heavy_plus_sign:  |
| 3.x     | task_latency_variance  | :heavy_plus_sign:  |
| 3.x     | sum_queued_duration    | :heavy_minus_sign: |
| 3.x     | sum_task_duration      | :heavy_minus_sign: |
