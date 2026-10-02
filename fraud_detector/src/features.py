import numpy as np
import pandas as pd


CATEGORICAL_FEATURES = [
    "merch",
    "cat_id",
    "gender",
    "one_city",
    "us_state",
    "post_code",
    "jobs",
]

DROPPED_FEATURES = [
    "name_1",
    "name_2",
    "street",
]


def prepare_features(data: pd.DataFrame) -> pd.DataFrame:

    result = data.copy()

    transaction_time = pd.to_datetime(
        result["transaction_time"],
        errors="coerce",
    )

    result["transaction_hour"] = transaction_time.dt.hour
    result["transaction_weekday"] = transaction_time.dt.dayofweek
    result["transaction_month"] = transaction_time.dt.month

    result = result.drop(
        columns=["transaction_time"] + DROPPED_FEATURES,
        errors="ignore",
    )

    lat_difference = result["lat"] - result["merchant_lat"]
    lon_difference = result["lon"] - result["merchant_lon"]

    result["distance"] = np.sqrt(
        lat_difference**2 + lon_difference**2
    )

    for column in CATEGORICAL_FEATURES:
        result[column] = (
            result[column]
            .fillna("__missing__")
            .astype(str)
        )

    return result