"""Models of the delay prediction, compared to the baselines on the validation set.

- logistic_regression and random_forest do not accept nulls: a null is replaced
  by the median of the training set, and an added column tells the model that
  the value was missing (SimpleImputer with add_indicator). The logistic
  regression also gets standardized features, so that it converges.
- hist_gradient_boosting and xgboost handle nulls themselves.

The train type is one-hot encoded for every model.

A model gives a late probability: a train is predicted late when it is above
the threshold with the best F1 score on the validation set.

Each model is logged as a MLflow run tagged purpose=model. Only the model with
the best F1 score is saved in its run (artifact "model"), with its threshold:
its predict() gives late or not late. Only this model is measured on the test
set (metrics test_precision, test_recall, test_f1).

This model is then promoted to champion in the Unity Catalog registry when it
beats the best baseline and the current champion (see registry.py). The
decision is the tag promotion of its run.

    make train-models
"""

import os
from datetime import datetime

import mlflow
import polars as pl
from mlflow import MlflowClient
from mlflow.models import infer_signature
from sklearn.compose import make_column_transformer
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.frozen import FrozenEstimator
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import FixedThresholdClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from xgboost import XGBClassifier

from delay_prediction.baseline import evaluate_baselines
from delay_prediction.datasets import (
    CATEGORICAL_FEATURE_COLUMNS,
    DATASET_VERSION_FORMAT,
    NUMERIC_FEATURE_COLUMNS,
    TARGET,
    load_dataset,
    log_dataset_inputs,
)
from delay_prediction.metrics import best_f1_threshold, classification_metrics
from delay_prediction.registry import champion_f1, promote, promotion_refusal


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


# Types of each model that skops, the format MLflow saves models in, does not
# load by default. Trusted because only this pipeline writes the saved models.
SKOPS_TRUSTED_TYPES = {
    "logistic_regression": ["numpy.dtype"],
    "random_forest": ["numpy.dtype", "sklearn.tree._tree.Tree"],
    "hist_gradient_boosting": [
        "sklearn.ensemble._hist_gradient_boosting.predictor.TreePredictor"
    ],
    "xgboost": ["xgboost.core.Booster", "xgboost.sklearn.XGBClassifier"],
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
    """Log a model evaluation as a MLflow run and return its id."""
    with mlflow.start_run(run_name=f"model_{name}") as run:
        mlflow.set_tags({"purpose": "model", "dataset_version": version})
        mlflow.log_param("threshold", threshold)
        mlflow.log_metrics(metrics)
        log_dataset_inputs(splits, version, os.environ["BUCKET_NAME"])
    return run.info.run_id


def with_threshold(model, threshold: float) -> FixedThresholdClassifier:
    """Return the trained model predicting late when its late probability reaches the threshold."""
    return FixedThresholdClassifier(
        FrozenEstimator(model), threshold=threshold, response_method="predict_proba"
    )


def test_metrics(model, test: pl.DataFrame) -> dict:
    """Return the precision, recall and F1 score of the model on the test set."""
    metrics = classification_metrics(test[TARGET], model.predict(features(test)))
    return {f"test_{name}": value for name, value in metrics.items()}


def dataset_metadata(version: str) -> dict:
    """Return the training dataset version and its build date and time (UTC)."""
    built_at = datetime.strptime(version, DATASET_VERSION_FORMAT)
    return {
        "training_dataset": version,
        "training_dataset_date": f"{built_at:%Y-%m-%d}",
        "training_dataset_time": f"{built_at:%H:%M:%S}",
    }


def save_best_model(evaluations: list[dict], test: pl.DataFrame, version: str) -> dict:
    """Save the model with the best F1 score in its MLflow run, with its metrics on test.

    The model metadata hold its training dataset. Return its evaluation and its
    test metrics.
    """
    best = max(evaluations, key=lambda evaluation: evaluation["f1"])
    model = with_threshold(best["trained_model"], best["threshold"])
    best_test_metrics = test_metrics(model, test)
    with mlflow.start_run(run_id=best["run_id"]):
        model_info = mlflow.sklearn.log_model(
            model,
            name="model",
            skops_trusted_types=SKOPS_TRUSTED_TYPES[best["model"]],
            metadata=dataset_metadata(version),
            # Unity Catalog only registers models with a signature
            signature=infer_signature(
                features(test).to_pandas(), model.predict(features(test))
            ),
        )
        mlflow.log_metrics(best_test_metrics)
    return best | best_test_metrics | {"model_uri": model_info.model_uri}


def promote_if_better(best: dict, splits: dict) -> str:
    """Promote the best model to champion when it deserves it; return the decision.

    The decision is also a tag of its run.
    """
    model_name = os.environ["MLFLOW_REGISTERED_MODEL_NAME"]
    baselines = evaluate_baselines(splits["train"], splits["valid"])
    best_baseline_f1 = max(metrics["f1"] for metrics, _ in baselines.values())
    valid = splits["valid"]
    current_champion_f1 = champion_f1(model_name, features(valid), valid[TARGET])
    refusal = promotion_refusal(best, best_baseline_f1, current_champion_f1)
    if refusal is None:
        decision = (
            f"promoted: {model_name} version {promote(best['model_uri'], model_name)}"
        )
    else:
        decision = f"not promoted: {refusal}"
    MlflowClient().set_tag(best["run_id"], "promotion", decision)
    return decision


def results_table(evaluations: list[dict], best: dict) -> pl.DataFrame:
    """Return the metrics of each model, each validation metric next to its test metric.

    Only the best model has test metrics.
    """
    rows = []
    for evaluation in evaluations:
        is_best = evaluation["run_id"] == best["run_id"]
        row = {"model": evaluation["model"], "threshold": evaluation["threshold"]}
        for metric in ["precision", "recall", "f1"]:
            row[metric] = evaluation[metric]
            row[f"test_{metric}"] = best[f"test_{metric}"] if is_best else None
        rows.append(row)
    return pl.DataFrame(rows)


def train_and_promote(splits: dict, version: str):
    """Train the models on a dataset, save the best and promote it when it deserves it.

    Return the metrics table, the best model evaluation and the promotion decision.
    """
    mlflow.set_experiment(os.environ["MLFLOW_EXPERIMENT_NAME"])
    evaluations = []
    for name, model in build_models().items():
        threshold, metrics = evaluate_model(model, splits["train"], splits["valid"])
        run_id = log_model_run(name, threshold, metrics, splits, version)
        evaluations.append(
            {"model": name, "threshold": threshold}
            | metrics
            | {"run_id": run_id, "trained_model": model}
        )
    best = save_best_model(evaluations, splits["test"], version)
    decision = promote_if_better(best, splits)
    return results_table(evaluations, best), best, decision


def main() -> None:
    """Train the models on the latest dataset and print their metrics."""
    splits, version = load_dataset()
    table, best, decision = train_and_promote(splits, version)

    print(f"Dataset {version}, metrics on the validation and test sets:")
    print(table)
    print(f"Saved model: {best['model']}, run {best['run_id']}, {decision}")


if __name__ == "__main__":
    main()
