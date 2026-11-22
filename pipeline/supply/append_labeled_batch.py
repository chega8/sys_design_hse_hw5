"""Запись месячного батча с защитой от повтора."""
from datetime import date
import pandas as pd
from pipeline.db import copy_labeled_frame, get_conn, month_end_exclusive


def append_labeled_batch(batch_info):
    """Читает parquet, пишет месяц и маркер в одной транзакции. Повтор ничего не добавляет."""
    frame = pd.read_parquet(batch_info["batch_path"])
    month = date.fromisoformat(batch_info["batch_month"])
    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(74301)")
                cur.execute("CREATE TABLE IF NOT EXISTS supplied_months (month DATE PRIMARY KEY, rows INTEGER NOT NULL)")
                cur.execute("SELECT rows FROM supplied_months WHERE month = %s", (month,))
                saved = cur.fetchone()
                if saved:
                    return {**batch_info, "rows": saved[0]}
                cur.execute("SELECT count(*) FROM labeled_transactions WHERE transaction_time >= %s AND transaction_time < %s",
                            (month, month_end_exclusive(month)))
                if cur.fetchone()[0]:
                    raise ValueError(f"Месяц {month} уже занят другим батчем")
                copy_labeled_frame(conn, frame, commit=False)
                cur.execute("INSERT INTO supplied_months VALUES (%s, %s)", (month, len(frame)))
    finally:
        conn.close()
    return {**batch_info, "rows": len(frame)}
