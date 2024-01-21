from airflow.decorators import dag, task
from datetime import datetime

from airflow.models.baseoperator import chain
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["retail_cont"],
)
def retail_cont():
    report = DbtTaskGroup(
        group_id="report",
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS, select=["path:models/report"]
        ),
    )

    @task.external_python(python="/usr/local/airflow/soda_venv/bin/python")
    def check_report(scan_name="check_report", checks_subpath="report"):
        from include.soda.check_function import check

        return check(scan_name, checks_subpath)

    chain(
        report,
        check_report(),
    )


retail_cont()
