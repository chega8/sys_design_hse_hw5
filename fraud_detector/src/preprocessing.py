"""Общие признаки для обучения и потока Kafka."""
import numpy as np
import pandas as pd

CATEGORIES = ["merch", "cat_id", "gender", "one_city", "us_state", "jobs"]
NUMERIC = ["amount", "lat", "lon", "merchant_lat", "merchant_lon", "population_city"]


def run_preproc(frame):
    """Читает сырые строки, возвращает признаки; исходный DataFrame не меняет."""
    out = pd.DataFrame(index=frame.index)
    for col in CATEGORIES:
        out[col] = frame[col].fillna("unknown").astype(str).str.strip().str.lower()
    for col in NUMERIC:
        out[col] = pd.to_numeric(frame[col], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0)
    out["amount"] = out["amount"].clip(lower=0)
    out["population_city"] = out["population_city"].clip(lower=0)
    out["log_amount"] = np.log1p(out["amount"])
    out["log_population"] = np.log1p(out["population_city"])
    times = pd.to_datetime(frame["transaction_time"], errors="raise")
    hour = times.dt.hour + times.dt.minute / 60
    out["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    out["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    out["weekday"] = times.dt.dayofweek
    out["is_night"] = (hour < 6).astype(int)
    # Приближённое расстояние по координатам, в километрах.
    dlat = np.radians(out["merchant_lat"] - out["lat"])
    dlon = np.radians(out["merchant_lon"] - out["lon"])
    middle_lat = np.radians((out["lat"] + out["merchant_lat"]) / 2)
    out["distance_km"] = 6371 * np.sqrt(dlat ** 2 + (np.cos(middle_lat) * dlon) ** 2)
    if "target" in frame:
        out["target"] = frame["target"]
    return out
