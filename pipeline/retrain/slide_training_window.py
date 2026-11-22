"""Подготовка окна без изменения указателя месяца."""
from datetime import date
import pandas as pd
from dateutil.relativedelta import relativedelta
from pipeline.db import get_conn, LABELED_COLUMNS, month_end_exclusive
from pipeline.settings import PENDING_WINDOW


def slide_training_window(batch_info):
    """Читает три месяца витрины, пишет pending parquet. Повтор заменяет тот же файл."""
    month = date.fromisoformat(batch_info["batch_month"])
    start = month - relativedelta(months=2)
    conn = get_conn()
    try:
        frame = pd.read_sql(
            f"SELECT {', '.join(LABELED_COLUMNS)} FROM labeled_transactions "
            "WHERE transaction_time >= %s AND transaction_time < %s ORDER BY transaction_time",
            conn, params=(start, month_end_exclusive(month)))
    finally:
        conn.close()
    periods = pd.to_datetime(frame["transaction_time"]).dt.to_period("M")
    expected = pd.period_range(start, month, freq="M")
    if set(periods.unique()) != set(expected):
        raise ValueError("Для обучения нужны три последовательных месяца")
    PENDING_WINDOW.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(PENDING_WINDOW, index=False)
    return {"dataset_path": str(PENDING_WINDOW), "batch_month": batch_info["batch_month"]}
