"""Metrics shared by every model of the delay prediction.

All models predict a probability of being late; they are compared on the
same metrics, computed on the validation set:

- ``pr_auc``: average precision, the main metric. Late trains are the
  minority class (about 1 in 4): it measures how well they are ranked first.
- ``roc_auc``: 0.5 is random ranking.
- ``brier``: mean squared error of the probabilities; lower is better.
- ``precision`` and ``recall`` at a threshold chosen on the training set:
  of the trains flagged late, the share really late; of the late trains,
  the share flagged.
"""

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)


def choose_threshold(y_true: np.ndarray, proba: np.ndarray) -> float:
    """Return the threshold maximizing the F1 score, to choose on the training set."""
    precision, recall, thresholds = precision_recall_curve(y_true, proba)
    # The last precision and recall have no threshold
    f1 = 2 * precision[:-1] * recall[:-1] / (precision[:-1] + recall[:-1] + 1e-12)
    return float(thresholds[np.argmax(f1)])


def evaluate(y_true: np.ndarray, proba: np.ndarray, threshold: float) -> dict:
    """Return the metrics of predicted probabilities against the true labels."""
    flagged = proba >= threshold
    return {
        "pr_auc": float(average_precision_score(y_true, proba)),
        "roc_auc": float(roc_auc_score(y_true, proba)),
        "brier": float(brier_score_loss(y_true, proba)),
        "precision": float(precision_score(y_true, flagged, zero_division=0)),
        "recall": float(recall_score(y_true, flagged, zero_division=0)),
        "flagged_rate": float(flagged.mean()),
        "late_rate": float(np.mean(y_true)),
    }
