"""Проверки витрины и небольшой HTML-отчёт."""
import html
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from pipeline.db import get_conn

REPORTS = Path(os.getenv("DQ_REPORTS_DIR", "/opt/airflow/reports"))


def check_dataset():
    """Читает витрину в одной транзакции, возвращает счётчики. Данные не меняет."""
    conditions = {
        "Витрина не пустая": "count(*) > 0",
        "Обязательные поля заполнены": "count(*) FILTER (WHERE transaction_time IS NULL OR amount IS NULL OR target IS NULL OR cat_id IS NULL OR trim(cat_id) = '' OR merch IS NULL OR trim(merch) = '') = 0",
        "Сумма конечная и неотрицательная": "count(*) FILTER (WHERE amount < 0 OR amount IN ('NaN'::float8, 'Infinity'::float8, '-Infinity'::float8)) = 0",
        "Метка 0 или 1": "count(*) FILTER (WHERE target NOT IN (0, 1)) = 0",
        "Оба класса присутствуют": "count(DISTINCT target) = 2",
        "Координаты в допустимых пределах": "count(*) FILTER (WHERE lat IS NULL OR lon IS NULL OR merchant_lat IS NULL OR merchant_lon IS NULL OR NOT (lat BETWEEN -90 AND 90 AND merchant_lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180 AND merchant_lon BETWEEN -180 AND 180)) = 0",
        "Население задано и неотрицательное": "count(*) FILTER (WHERE population_city IS NULL OR population_city < 0) = 0",
        "Штат — две заглавные буквы": "count(*) FILTER (WHERE us_state IS NULL OR us_state !~ '^[A-Z]{2}$') = 0",
        "Время от 2000 года, не позже текущего момента": "count(*) FILTER (WHERE transaction_time < '2000-01-01' OR transaction_time > CURRENT_TIMESTAMP) = 0",
    }
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*), min(transaction_time), max(transaction_time), " + ", ".join(conditions.values()) + " FROM labeled_transactions")
            values = cur.fetchone()
    finally:
        conn.close()
    return {"checked_at": datetime.now(timezone.utc).isoformat(), "rows": values[0],
            "min_time": str(values[1]), "max_time": str(values[2]),
            "checks": [{"rule": name, "passed": bool(ok)} for name, ok in zip(conditions, values[3:])]}


def write_report(result):
    """Пишет HTML и JSON даже для плохих данных; повтор атомарно заменяет latest."""
    REPORTS.mkdir(parents=True, exist_ok=True)
    rows = "".join(f"<tr><td>{html.escape(c['rule'])}</td><td>{'OK' if c['passed'] else 'FAIL'}</td></tr>" for c in result["checks"])
    document = f"""<!doctype html><html lang="ru"><meta charset="utf-8">
<title>Проверка данных</title><style>body{{font:16px sans-serif;margin:40px}}td,th{{border:1px solid #aaa;padding:8px}}table{{border-collapse:collapse}}</style>
<h1>Проверка labeled_transactions</h1><p>{html.escape(result['checked_at'])}</p>
<p>Строк: {result['rows']}. Период: {result['min_time']} — {result['max_time']}</p>
<table><tr><th>Правило</th><th>Результат</th></tr>{rows}</table></html>"""
    for name, contents in (("latest.html", document), ("latest.json", json.dumps(result, ensure_ascii=False, indent=2))):
        temporary = REPORTS / (name + ".tmp")
        temporary.write_text(contents)
        temporary.replace(REPORTS / name)
    return result


def require_quality(result):
    """Читает результаты после сохранения отчёта и роняет задачу при нарушениях."""
    failed = [c["rule"] for c in result["checks"] if not c["passed"]]
    if failed:
        raise ValueError("Не пройдены DQ: " + "; ".join(failed))
