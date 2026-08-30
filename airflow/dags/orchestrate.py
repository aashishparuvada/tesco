from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
import pendulum
import utils


@dag(
    dag_id="dbt_tesco_pipeline",
    description="databricks job (bronze) -> silver models (including obt) -> tests -> snapshot gold dimensions -> fact table",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Kolkata"),
    catchup=False,
    tags=["dbt"],
)
def orchestrate():
    @task
    def databricks_ingest_cdc():
        utils.establish_databricks_connection()
        utils.trigger_databricks_job(job_id=1088290843715942)

    @task.bash
    def source_freshness():
        return "cd /opt/dbt && dbt source freshness"

    silver = BashOperator(
        task_id="silver",
        cwd="/opt/dbt/",
        bash_command="dbt run --select silver --exclude obt",
    )

    silver_test = BashOperator(
        task_id="silver_test",
        cwd="/opt/dbt/",
        bash_command="dbt test --select silver --exclude obt",
    )

    obt = BashOperator(task_id="obt", cwd="/opt/dbt/", bash_command="dbt run -s obt")

    obt_test = BashOperator(
        task_id="obt_test", cwd="/opt/dbt/", bash_command="dbt test -s obt"
    )

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/dbt/",
        bash_command="dbt run -s gold/ephemeral",
    )

    gold_dimensions = BashOperator(
        task_id="gold_dimensions", cwd="/opt/dbt", bash_command="dbt snapshot"
    )

    gold_fact = BashOperator(
        task_id="gold_fact", cwd="/opt/dbt/", bash_command="dbt run -s gold/fact"
    )

    (
        databricks_ingest_cdc()
        >> source_freshness()
        >> silver
        >> silver_test
        >> obt
        >> obt_test
        >> gold_ephemeral
        >> gold_dimensions
        >> gold_fact
    )


orchestrate_dag = orchestrate()
