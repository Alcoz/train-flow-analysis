# delay_prediction

Predicts the day before whether an SNCF train will reach its terminus **5 minutes late or more**. Features come from the Gold table `ml_delay_features` (known before day D), the target from `fact_train_trips` of day D. Rules: [`data-eng/docs/business_rules.md`](../data-eng/docs/business_rules.md).

## Training datasets

The Dagster job `sncf_ml_training_job` (every Monday at 05:00, or by hand) writes a new version with [`datasets.py`](src/delay_prediction/datasets.py):

- **Rows**: days from `first_date` (default 2026-10-13, the first with 28 days of history) that have their trains. Only trains seen running, not cancelled and with a measured delay are kept. When the SNCF changed a trip_id between two GTFS versions, the train is matched by its number.
- **Split**: the last `test_days` days (default 7) are the test set, the `valid_days` days before (default 14) the validation set, the days before the training set. The test set is kept aside for the final measure of the chosen model.
- **Storage**: `data/ml/datasets/<version>/train.parquet`, `valid.parquet` and `test.parquet` on the bucket; the version is the UTC build time.
- **MLflow**: a run tagged `purpose=training_dataset`, with the date ranges and the three sets as inputs (schema, size, digest). The data stays on the bucket.

```python
from delay_prediction.datasets import load_dataset

splits, version = load_dataset()  # latest version: splits["train"], ["valid"], ["test"]
```

## Baselines

[`baseline.py`](src/delay_prediction/baseline.py) gives the scores a trained model must beat:

- `train_rate_7d_above_28d`: a train is predicted late when it was late more often over the last 7 days than over the last 28 days (`train_late_rate_7d > train_late_rate_28d`). A train without history is predicted on time.
- `route_rate_7d_above_28d`: same rule with the late rates of the line of the train (`route_late_rate_7d > route_late_rate_28d`). A train whose line has no history is predicted on time.
- `late_rate_above_0.5`: a train is predicted late when its late rate over 28 days (else its line's over 28 days, else the network's on D-1) is above 0.5.
- `late_rate_above_best_f1_threshold`: same late rate, compared to the threshold with the best F1 score on the training set.
- `all_trains_late`: every train is predicted late, the score without any information on the trains.

Metrics on the validation set ([`metrics.py`](src/delay_prediction/metrics.py)): `precision`, `recall`, `f1`. Each baseline is a MLflow run tagged `purpose=baseline`.

```bash
make train-baseline
```

## Models

[`models.py`](src/delay_prediction/models.py) trains four models on the training set and measures them on the validation set, with the metrics of the baselines:

- `logistic_regression` and `random_forest`: they do not accept nulls. A null is replaced by the median of the training set and an added column tells the model the value was missing (`SimpleImputer(strategy="median", add_indicator=True)`). The logistic regression also gets standardized features.
- `hist_gradient_boosting` and `xgboost`: they handle nulls themselves.
- The train type is one-hot encoded for every model.

A train is predicted late when its late probability is above the threshold with the best F1 score on the validation set. Each model is a MLflow run tagged `purpose=model`. Only the model with the best F1 score is saved, as the artifact `model` of its run: a scikit-learn `FixedThresholdClassifier` holding the trained pipeline and its threshold, whose `predict()` gives late or not late. Only this model is measured on the test set (`test_precision`, `test_recall`, `test_f1` in its run): a final measure, no choice is made on it. The model metadata hold its training dataset: `training_dataset` (version), `training_dataset_date` and `training_dataset_time` (UTC build time).

The best model then becomes the champion in the Unity Catalog registry (`MLFLOW_REGISTERED_MODEL_NAME`, alias `champion`) only when ([`registry.py`](src/delay_prediction/registry.py)):

- its validation F1 beats the best baseline;
- it beats the current champion, measured again on the same validation set;
- its test F1 is not more than 0.05 below its validation F1.

Only a promoted model becomes a new version of the registered model. The decision is the tag `promotion` of its run.

The Dagster job `sncf_ml_training_job` runs this loop every Monday at 05:00: a new dataset version, then the models trained on it. `MLFLOW_REGISTERED_MODEL_NAME` must be in the `.env` passed to docker-compose. XGBoost needs OpenMP on macOS (`brew install libomp`).

```bash
make train-models
```

## MLflow

Tracking and registry are on Databricks (variables in `.env.example`, passed to the orchestrator by docker-compose). Check the connection:

```bash
uv run --env-file .env python -m delay_prediction.check_tracking
```
