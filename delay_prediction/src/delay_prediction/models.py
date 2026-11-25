"""Models of the delay prediction, compared to the baselines on the validation set.

- logistic_regression and random_forest do not accept nulls: a null is replaced
  by the median of the training set, and an added column tells the model that
  the value was missing (SimpleImputer with add_indicator). The logistic
  regression also gets standardized features, so that it converges.
- hist_gradient_boosting and xgboost handle nulls themselves.

The train type is one-hot encoded for every model.

A model gives a late probability: a train is predicted late when it is above
the threshold with the best F1 score on the validation set.

Each model is logged as a MLflow run tagged purpose=model:

    make train-models
"""

import os

import mlflow
import polars as pl
from sklearn.compose import make_column_transformer
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBClassifier

from delay_prediction.datasets import (
    CATEGORICAL_FEATURE_COLUMNS,
    NUMERIC_FEATURE_COLUMNS,
    TARGET,
    load_dataset,
    log_dataset_inputs,
)
from delay_prediction.metrics import best_f1_threshold, classification_metrics


def median_imputer() -> SimpleImputer:
    """Return an imputer replacing nulls by the median and adding a missing indicator."""
    return SimpleImputer(strategy="median", add_indicator=True)


def preprocessing(numeric_step="passthrough"):
    """Return the step applying numeric_step to the numbers and one-hot encoding the train type."""
    return make_column_transformer(
        (numeric_step, NUMERIC_FEATURE_COLUMNS),
        (OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURE_COLUMNS),
    )


def build_models() -> dict:
    """Return the untrained models, by name."""
    return {
        "logistic_regression": make_pipeline(
            preprocessing(make_pipeline(median_imputer(), StandardScaler())),
            LogisticRegression(max_iter=1000),
        ),
        "random_forest": make_pipeline(
            preprocessing(median_imputer()),
            RandomForestClassifier(n_jobs=-1, random_state=0),
        ),
        "hist_gradient_boosting": make_pipeline(
            preprocessing(), HistGradientBoostingClassifier(random_state=0)
        ),
        "xgboost": make_pipeline(preprocessing(), XGBClassifier(random_state=0)),
    }


def features(trains: pl.DataFrame) -> pl.DataFrame:
    """Return the features, numbers as floats so that their nulls become NaN."""
    return trains.select(
        pl.col(NUMERIC_FEATURE_COLUMNS).cast(pl.Float64), *CATEGORICAL_FEATURE_COLUMNS
    )


def evaluate_model(model, train: pl.DataFrame, valid: pl.DataFrame):
    """Train the model on train; return its best F1 threshold and its metrics on valid."""
    model.fit(features(train), train[TARGET].to_numpy())
    late_probabilities = model.predict_proba(features(valid))[:, 1]
    threshold = best_f1_threshold(valid[TARGET], late_probabilities)
    predicted_late = late_probabilities >= threshold
    return threshold, classification_metrics(valid[TARGET], predicted_late)


def log_model_run(name: str, threshold: float, metrics: dict, splits, version: str):
    """Log a model evaluation as a MLflow run."""
    with mlflow.start_run(run_name=f"model_{name}"):
        mlflow.set_tags({"purpose": "model", "dataset_version": version})
        mlflow.log_param("threshold", threshold)
        mlflow.log_metrics(metrics)
        log_dataset_inputs(splits, version, os.environ["BUCKET_NAME"])


def main() -> None:
    """Train and evaluate the models on the latest dataset and log them to MLflow."""
    splits, version = load_dataset()
    mlflow.set_experiment(os.environ["MLFLOW_EXPERIMENT_NAME"])

    results = []
    for name, model in build_models().items():
        threshold, metrics = evaluate_model(model, splits["train"], splits["valid"])
        log_model_run(name, threshold, metrics, splits, version)
        results.append({"model": name, "threshold": threshold} | metrics)

    print(f"Dataset {version}, validation metrics:")
    print(pl.DataFrame(results))


if __name__ == "__main__":
    main()
