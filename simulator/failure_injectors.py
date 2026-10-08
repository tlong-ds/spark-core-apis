"""Controlled failures for simulator runs; never imported by the core pipeline."""

from time import sleep


def fail_partition_zero_on_first_attempt(
    entries, retry_observation_seconds: int = 0
):
    """Fail exactly once, then allow Spark's retry attempt to process the data.

    The condition uses Spark's task-attempt metadata rather than a shared mutable
    flag: partition 0 / attempt 0 fails, while attempt 1 succeeds. Therefore a
    successful job proves Spark rescheduled that failed task.
    """
    from pyspark import TaskContext

    context = TaskContext.get()
    if context.partitionId() == 0 and context.attemptNumber() == 0:
        raise RuntimeError(
            "SIMULATED_TASK_RETRY: partition=0 attempt=0; retry should use attempt=1"
        )
    if context.partitionId() == 0 and context.attemptNumber() == 1 and retry_observation_seconds:
        # This is deliberately in the retry task, not in the driver.  While it
        # sleeps, the live Spark UI shows the Stage as active, its first task
        # attempt as failed, and this retry as running.
        print(
            "SIMULATED_TASK_RETRY: partition=0 attempt=1 is active; "
            f"holding for {retry_observation_seconds}s for Spark UI observation"
        )
        sleep(retry_observation_seconds)
    yield from entries


def fail_partition_zero_every_attempt(entries, retry_observation_seconds: int = 0):
    """Make a real RDD task fail permanently for a failed-Job demonstration."""
    from pyspark import TaskContext

    context = TaskContext.get()
    if context.partitionId() == 0:
        raise RuntimeError(
            "SIMULATED_RDD_TASK_FAILURE: partition=0; mapPartitions raised a Python exception"
        )
    yield from entries


INJECTORS = {
    "fail_partition_zero_on_first_attempt": fail_partition_zero_on_first_attempt,
    "fail_partition_zero_every_attempt": fail_partition_zero_every_attempt,
}
