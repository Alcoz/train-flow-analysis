"""Metrics shared by every model of the delay prediction."""

from sklearn.metrics import precision_recall_fscore_support


def classification_metrics(is_late, predicted_late) -> dict:
    """Return the precision, recall and F1 score of the predictions."""
    precision, recall, f1, _ = precision_recall_fscore_support(
        is_late, predicted_late, average="binary", zero_division=0
    )
    return {"precision": precision, "recall": recall, "f1": f1}
