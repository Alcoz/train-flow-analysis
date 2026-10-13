import dagster as dg
from dagster._core.test_utils import create_run_for_test
from orchestration_dagster.defs.jobs.maintenance_jobs import purge_old_runs_job


def _run_purge(instance: dg.DagsterInstance, **config) -> dg.ExecuteInProcessResult:
    return purge_old_runs_job.execute_in_process(
        instance=instance,
        run_config={"ops": {"purge_old_runs": {"config": config}}},
    )


def test_purge_deletes_finished_runs_of_targeted_jobs():
    """Finished runs of a high-frequency job past the retention window are deleted."""
    with dg.instance_for_test() as instance:
        old_run = create_run_for_test(
            instance,
            job_name="trip_update_getter_job",
            status=dg.DagsterRunStatus.SUCCESS,
        )

        result = _run_purge(instance, retention_days=0)

        assert result.output_for_node("purge_old_runs") == 1
        assert instance.get_run_by_id(old_run.run_id) is None


def test_purge_keeps_other_jobs_and_unfinished_runs():
    """Runs of non-targeted jobs and runs still in progress are never deleted."""
    with dg.instance_for_test() as instance:
        daily_run = create_run_for_test(
            instance,
            job_name="sncf_data_preparation_job",
            status=dg.DagsterRunStatus.SUCCESS,
        )
        running_run = create_run_for_test(
            instance,
            job_name="trip_update_getter_job",
            status=dg.DagsterRunStatus.STARTED,
        )

        result = _run_purge(instance, retention_days=0)

        assert result.output_for_node("purge_old_runs") == 0
        assert instance.get_run_by_id(daily_run.run_id) is not None
        assert instance.get_run_by_id(running_run.run_id) is not None


def test_purge_keeps_runs_inside_retention_window():
    """Recent runs stay available for debugging."""
    with dg.instance_for_test() as instance:
        recent_run = create_run_for_test(
            instance,
            job_name="service_alerts_getter_job",
            status=dg.DagsterRunStatus.FAILURE,
        )

        result = _run_purge(instance)

        assert result.output_for_node("purge_old_runs") == 0
        assert instance.get_run_by_id(recent_run.run_id) is not None
