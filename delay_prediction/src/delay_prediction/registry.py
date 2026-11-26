"""Champion / challenger in the Unity Catalog registry of the delay prediction.

The best model of a run is the challenger. It becomes a new version of the
registered model, with the alias champion, only when:

- its F1 on the validation set beats the best baseline;
- it beats the current champion, measured again on the same validation set;
- its F1 on the test set is not more than MAX_TEST_F1_DROP below its F1 on
  the validation set (a sign of drift or of a threshold that does not hold).
"""

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException

from delay_prediction.metrics import classification_metrics

CHAMPION_ALIAS = "champion"
MAX_TEST_F1_DROP = 0.05


def champion_f1(model_name: str, valid_features, valid_target) -> float | None:
    """Return the F1 of the current champion on the validation set, None without champion."""
    try:
        champion = mlflow.sklearn.load_model(f"models:/{model_name}@{CHAMPION_ALIAS}")
    except MlflowException:
        return None
    return classification_metrics(valid_target, champion.predict(valid_features))["f1"]


def promotion_refusal(
    challenger: dict, best_baseline_f1: float, current_champion_f1: float | None
) -> str | None:
    """Return why the challenger is not promoted, None when it is."""
    if challenger["f1"] <= best_baseline_f1:
        return f"validation F1 does not beat the best baseline ({best_baseline_f1:.3f})"
    if current_champion_f1 is not None and challenger["f1"] <= current_champion_f1:
        return f"validation F1 does not beat the champion ({current_champion_f1:.3f})"
    if challenger["test_f1"] < challenger["f1"] - MAX_TEST_F1_DROP:
        return f"test F1 is more than {MAX_TEST_F1_DROP} below the validation F1"
    return None


def promote(model_uri: str, model_name: str) -> str:
    """Register the model as a new version with the champion alias and return the version."""
    # The version is a string in Unity Catalog, an int in other registries
    version = str(mlflow.register_model(model_uri, model_name).version)
    MlflowClient().set_registered_model_alias(model_name, CHAMPION_ALIAS, version)
    return version
