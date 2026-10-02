import json
import logging
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier


logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
MODEL_PATH = BASE_DIR / "models" / "my_model.cbm"
METADATA_PATH = BASE_DIR / "models" / "my_model_metadata.json"


logger.info("Loading model from %s", MODEL_PATH)

model = CatBoostClassifier()
model.load_model(str(MODEL_PATH))

with open(METADATA_PATH, "r", encoding="utf-8") as metadata_file:
    metadata = json.load(metadata_file)

MODEL_FEATURES = metadata["feature_names"]
MODEL_THRESHOLD = metadata["threshold"]

logger.info("Model loaded successfully")


def make_pred(data: pd.DataFrame, source_info: str = "kafka") -> pd.DataFrame:
    missing_columns = [column for column in MODEL_FEATURES 
                       if column not in data.columns]

    if missing_columns:
        raise ValueError(f"Missing model features: {missing_columns}")

    data = data[MODEL_FEATURES]

    scores = model.predict_proba(data)[:, 1]

    result = pd.DataFrame({
        "score": scores,
        "fraud_flag": (scores >= MODEL_THRESHOLD).astype(int),
    })

    logger.info(
        "Prediction completed for data from %s",
        source_info,
    )

    return result