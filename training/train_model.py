import json
import sys
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "fraud_detector"))
from src.features import CATEGORICAL_FEATURES, prepare_features


TRAIN_PATH = PROJECT_ROOT / "fraud_detector" / "train_data" / "train.csv"
MODEL_DIRECTORY = PROJECT_ROOT / "fraud_detector" / "models"
MODEL_PATH = MODEL_DIRECTORY / "my_model.cbm"
METADATA_PATH = MODEL_DIRECTORY / "my_model_metadata.json"


def main():
    print("Чтение train")

    train_data = pd.read_csv(TRAIN_PATH)

    target = train_data["target"]
    features = train_data.drop(columns=["target"])

    print("Подготовка признаков")

    features = prepare_features(features)

    x_train, x_valid, y_train, y_valid = train_test_split(
        features,
        target,
        test_size=0.2,
        random_state=42,
        stratify=target,
    )

    print("Обучение модели")

    model = CatBoostClassifier(
        iterations=150,
        depth=6,
        learning_rate=0.1,
        loss_function="Logloss",
        eval_metric="AUC",
        auto_class_weights="Balanced",
        random_seed=42,
        thread_count=-1,
        verbose=10,
        allow_writing_files=False,
    )

    model.fit(
        x_train,
        y_train,
        cat_features=CATEGORICAL_FEATURES,
        eval_set=(x_valid, y_valid),
        early_stopping_rounds=20,
    )

    valid_scores = model.predict_proba(x_valid)[:, 1]
    auc = roc_auc_score(y_valid, valid_scores)

    print(f"Validation ROC-AUC: {auc:.4f}")

    MODEL_DIRECTORY.mkdir(parents=True, exist_ok=True)
    model.save_model(MODEL_PATH)

    metadata = {
        "feature_names": list(features.columns),
        "categorical_features": CATEGORICAL_FEATURES,
        "threshold": 0.5,
        "validation_roc_auc": float(auc),
    }

    with open(METADATA_PATH, "w", encoding="utf-8") as metadata_file:
        json.dump(
            metadata,
            metadata_file,
            ensure_ascii=False,
            indent=2,
        )

    print(f"Модель сохранена: {MODEL_PATH}")
    print(f"Настройки сохранены: {METADATA_PATH}")


if __name__ == "__main__":
    main()