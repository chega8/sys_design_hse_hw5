"""Сравнение кандидата с опубликованной моделью на одном свежем месяце."""
import json
from pathlib import Path
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import roc_auc_score, f1_score
from pipeline.retrain.fit_catboost import prepare_features
from pipeline.settings import MODELS_DIR


def quality(y, proba):
    """ROC-AUC и F1 при том же пороге 0.5, что используется в сервисе."""
    if y.nunique() != 2:
        raise ValueError("Для сравнения моделей в test нужны оба класса")
    return {"roc_auc": float(roc_auc_score(y, proba)),
            "f1": float(f1_score(y, proba > 0.5, zero_division=0))}


def is_better(candidate, current):
    """Публикуем при росте AUC и отсутствии падения F1."""
    return candidate["roc_auc"] > current["roc_auc"] and candidate["f1"] >= current["f1"]


def evaluate_model(info):
    """Читает test и две модели, пишет решение в JSON; production и окно не меняет."""
    test = pd.read_parquet(info["test_proc_path"])
    x, y = prepare_features(test)
    latest = MODELS_DIR / "latest.txt"
    version = latest.read_text().strip()
    current = CatBoostClassifier()
    current.load_model(str(MODELS_DIR / f"my_catboost_{version}.cbm"))
    candidate = CatBoostClassifier()
    candidate_path = Path(info["staging_model"])
    if not candidate_path.exists():
        candidate_path = MODELS_DIR / f"my_catboost_{info['version']}.cbm"
    candidate.load_model(str(candidate_path))
    new = quality(y, candidate.predict_proba(x)[:, 1])
    old = quality(y, current.predict_proba(x)[:, 1])
    decision = {**info, "current_version": version, "candidate_metrics": new,
                "current_metrics": old, "approved": is_better(new, old)}
    # Если публикация успела пройти до сбоя, повтор завершает фиксацию того же месяца.
    if version == info["version"]:
        import hashlib
        current_path = MODELS_DIR / f"my_catboost_{version}.cbm"
        same_file = hashlib.sha256(candidate_path.read_bytes()).digest() == hashlib.sha256(current_path.read_bytes()).digest()
        decision["approved"] = decision["approved"] or same_file
    (MODELS_DIR / f"{info['version']}.evaluation.json").write_text(json.dumps(decision, indent=2))
    return decision
