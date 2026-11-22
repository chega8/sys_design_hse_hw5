import logging
import os
import threading
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier

from metrics import MODEL_RELOADS, MODEL_RELOADED_AT


logger = logging.getLogger(__name__)

THRESHOLD = 0.5
MODELS_DIR = Path(os.getenv("MODELS_DIR", "/models"))

_lock = threading.Lock()
_model = None
_version = None


def _model_path(version: str) -> Path:
    return MODELS_DIR / f"my_catboost_{version}.cbm"


def ensure_bootstrap():
    """Проверяет начальную модель, которую data_loader обучает с общими признаками."""
    if not (MODELS_DIR / "latest.txt").exists():
        raise FileNotFoundError("Начальная модель ещё не создана data_loader")


def _read_latest_version() -> str:
    ensure_bootstrap()
    version = (MODELS_DIR / "latest.txt").read_text(encoding="utf-8").strip()
    if not version:
        raise RuntimeError(f"Пустой указатель модели: {MODELS_DIR / 'latest.txt'}")
    return version


def _load_model(version: str) -> CatBoostClassifier:
    path = _model_path(version)
    if not path.exists():
        raise FileNotFoundError(f"Файл модели {version} не найден: {path}")
    os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
    model = CatBoostClassifier()
    model.load_model(str(path))
    logger.info("Загружена модель версии %s из %s", version, path)
    return model


def _current_model():
    """Возвращает модель, которая соответствует latest.txt. Вызывать под _lock."""
    global _model, _version
    version = _read_latest_version()
    if _model is None or version != _version:
        _model = _load_model(version)
        if _version is not None:
            MODEL_RELOADS.labels(version=version).inc()
            MODEL_RELOADED_AT.labels(version=version).set_to_current_time()
        _version = version
    return _model, _version


def make_pred(dt, source_info="kafka"):
    with _lock:
        model, model_version = _current_model()
        features = list(model.feature_names_)
        missing = [column for column in features if column not in dt.columns]
        if missing:
            raise ValueError(f"Во входных данных нет признаков модели: {missing}")
        frame = dt[features].copy()
        for idx in model.get_cat_feature_indices():
            column = features[idx]
            if not pd.api.types.is_numeric_dtype(frame[column]):
                frame[column] = frame[column].astype(str)
        y_proba = model.predict_proba(frame, thread_count=1)[:, 1]

    submission = pd.DataFrame({
        "score": y_proba,
        "fraud_flag": (y_proba > THRESHOLD) * 1,
    })
    logger.debug("Prediction complete for data from %s, model_version=%s", source_info, model_version)
    return submission, y_proba, model_version
