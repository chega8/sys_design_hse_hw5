"""Отдельная проверка витрины каждые пять минут."""
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from pipeline.data_quality import check_dataset, write_report, require_quality


@dag(schedule="*/5 * * * *", start_date=datetime(2026, 1, 1), catchup=False,
     max_active_runs=1, default_args={"retries": 1, "retry_delay": timedelta(seconds=30)})
def data_quality():
    checked = task(check_dataset)()
    report = task(write_report)(checked)
    task(require_quality)(report)


data_quality()
