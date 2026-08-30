# utils.py

import os
import time

from pathlib import Path
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)


def establish_databricks_connection():
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    if not host:
        raise ValueError("DATABRICKS_HOST environment variable is not set")

    if not token:
        raise ValueError("DATABRICKS_TOKEN environment variable is not set")

    # Avoid adding https:// twice
    if not host.startswith("http"):
        host = f"https://{host}"

    return WorkspaceClient(
        host=host,
        token=token,
    )


def trigger_databricks_job(job_id: int):
    w = establish_databricks_connection()

    job_trigger = w.jobs.run_now(job_id=job_id)

    print(f"Databricks job triggered. Run ID: {job_trigger.run_id}")

    while True:
        job_run = w.jobs.get_run(run_id=job_trigger.run_id)

        lifecycle_state = job_run.state.life_cycle_state
        result_state = job_run.state.result_state

        # print(
        #     f"Run ID: {job_trigger.run_id}, "
        #     f"Lifecycle state: {lifecycle_state}, "
        #     f"Result state: {result_state}"
        # )

        if lifecycle_state in [
            RunLifeCycleState.TERMINATED,
            RunLifeCycleState.SKIPPED,
            RunLifeCycleState.INTERNAL_ERROR,
        ]:
            if result_state == RunResultState.SUCCESS:
                print("Databricks job completed successfully")
                return

            raise Exception(
                f"Databricks job failed. "
                f"Lifecycle state: {lifecycle_state}, "
                f"Result state: {result_state}"
            )

        time.sleep(5)
