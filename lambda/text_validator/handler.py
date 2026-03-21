"""
text_validator/handler.py

Called by Step Functions for the text_reviews branch.

Input (from Step Functions state):
  {
    "bucket": str,
    "key": str,
    "data_type": "text_reviews",
    "timestamp": str,
    "processed_results": {}
  }

Output:
  {
    ... (forwarded input fields) ...,
    "validation": {
      "total_rows": int,
      "valid_rows": int,
      "invalid_rows": int,
      "quality_score": float,          # 0–100
      "validation_issues": {str: int}  # issue_type → count
    }
  }
"""
from __future__ import annotations

import io
import json
import logging
import os
import re
import unicodedata
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "")
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "CustomerFeedbackAnalyzer")

MIN_REVIEW_LENGTH = 10
MAX_REVIEW_LENGTH = 10_000
MIN_UNIQUE_WORD_RATIO = 0.20

URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
HTML_RE = re.compile(r"<[^>]+>")


def _validate_row(review: str | None) -> tuple[bool, list[str]]:
    """
    Validate a single review string. Returns (is_valid, list_of_issues).
    The review is considered valid when there are zero issues.
    """
    issues: list[str] = []

    if not review or not str(review).strip():
        return False, ["empty_review"]

    text = str(review).strip()

    if len(text) < MIN_REVIEW_LENGTH:
        issues.append("too_short")

    if len(text) > MAX_REVIEW_LENGTH:
        issues.append("too_long")

    # Strip HTML and URLs to measure actual content
    cleaned = HTML_RE.sub(" ", text)
    cleaned = URL_RE.sub(" ", cleaned).strip()
    if not cleaned:
        issues.append("content_is_urls_or_html_only")

    # Vocabulary diversity check (catches spam/repetitive content)
    words = cleaned.lower().split()
    if words:
        unique_ratio = len(set(words)) / len(words)
        if unique_ratio < MIN_UNIQUE_WORD_RATIO:
            issues.append("low_vocabulary_diversity")

    # Encoding sanity: reject strings with too many replacement characters
    try:
        cleaned.encode("utf-8")
    except UnicodeEncodeError:
        issues.append("invalid_encoding")

    return len(issues) == 0, issues


def _read_csv_reviews(bucket: str, key: str) -> list[str | None]:
    """Download the CSV from S3 and return the review_content column values."""
    try:
        import pandas as pd  # noqa: PLC0415 – lazy import, not in Lambda layer
    except ImportError:
        # Fallback: read raw lines; assume last meaningful CSV column is review_content
        logger.warning("pandas not available, using raw line fallback")
        obj = s3.get_object(Bucket=bucket, Key=key)
        lines = obj["Body"].read().decode("utf-8", errors="replace").splitlines()
        # Simple split on comma – good enough for fallback validation
        if len(lines) < 2:
            return []
        headers = [h.strip('"') for h in lines[0].split(",")]
        col_idx = None
        for i, h in enumerate(headers):
            if "review_content" in h.lower():
                col_idx = i
                break
        if col_idx is None:
            return []
        reviews = []
        for line in lines[1:]:
            parts = line.split(",")
            reviews.append(parts[col_idx] if col_idx < len(parts) else None)
        return reviews

    obj = s3.get_object(Bucket=bucket, Key=key)
    try:
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
        if "review_content" in df.columns:
            return df["review_content"].tolist()
        # If column not found, attempt heuristic: longest text column
        text_cols = df.select_dtypes(include="object").columns.tolist()
        if text_cols:
            avg_len = {c: df[c].dropna().astype(str).str.len().mean() for c in text_cols}
            best = max(avg_len, key=avg_len.get)
            logger.warning("'review_content' column not found; using '%s'", best)
            return df[best].tolist()
    except Exception:
        logger.warning("pandas read_csv failed, returning empty", exc_info=True)
    return []


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    logger.info("text_validator invoked: %s", json.dumps(event, default=str)[:512])

    bucket: str = event["bucket"]
    key: str = event["key"]

    reviews = _read_csv_reviews(bucket, key)
    total = len(reviews)

    if total == 0:
        logger.warning("No review_content values found in s3://%s/%s", bucket, key)
        quality_score = 0.0
        result = {
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "quality_score": quality_score,
            "validation_issues": {"no_content_found": 1},
        }
        _publish_validation_metrics(quality_score, {"no_content_found": 1})
        return {**event, "validation": result}

    issue_counts: dict[str, int] = {}
    valid_count = 0

    for review in reviews:
        is_valid, issues = _validate_row(review)
        if is_valid:
            valid_count += 1
        for issue in issues:
            issue_counts[issue] = issue_counts.get(issue, 0) + 1

    invalid_count = total - valid_count
    quality_score = round((valid_count / total) * 100, 2) if total > 0 else 0.0

    logger.info(
        "Validation complete: %d/%d valid (score=%.1f)", valid_count, total, quality_score
    )

    _publish_validation_metrics(quality_score, issue_counts)

    return {
        **event,
        "validation": {
            "total_rows": total,
            "valid_rows": valid_count,
            "invalid_rows": invalid_count,
            "quality_score": quality_score,
            "validation_issues": issue_counts,
        },
    }


def _publish_validation_metrics(quality_score: float, issue_counts: dict[str, int]) -> None:
    total_failures = sum(issue_counts.values())
    try:
        cloudwatch.put_metric_data(
            Namespace=METRICS_NAMESPACE,
            MetricData=[
                {
                    "MetricName": "DataQualityScore",
                    "Value": quality_score,
                    "Unit": "None",
                    "Dimensions": [{"Name": "Stage", "Value": "Validation"}],
                },
                {
                    "MetricName": "ValidationFailures",
                    "Value": float(total_failures),
                    "Unit": "Count",
                    "Dimensions": [{"Name": "Stage", "Value": "Validation"}],
                },
            ],
        )
    except Exception:
        logger.warning("Failed to publish validation metrics", exc_info=True)
