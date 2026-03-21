"""
survey_summarizer/handler.py

Converts tabular survey CSV data into natural-language summaries suitable
for foundation model consumption.

For each survey question column, it:
  - Computes descriptive statistics for numeric columns
  - Finds most-frequent answers for categorical/text columns
  - Builds a natural-language narrative paragraph

Input:  Step Functions state with "bucket" + "key" pointing to a survey CSV
Output: State dict + "survey_summary": str  (NL summary)  +  "survey_stats": dict
"""
from __future__ import annotations

import io
import json
import logging
import os
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

s3 = boto3.client("s3")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "")


def _numeric_summary(series) -> dict:
    desc = series.describe()
    return {
        "count": int(desc.get("count", 0)),
        "mean": round(float(desc.get("mean", 0)), 2),
        "median": round(float(series.median()), 2),
        "std": round(float(desc.get("std", 0)), 2),
        "min": round(float(desc.get("min", 0)), 2),
        "max": round(float(desc.get("max", 0)), 2),
    }


def _categorical_summary(series, top_n: int = 5) -> dict:
    counts = series.dropna().astype(str).value_counts()
    total = len(series.dropna())
    top = [
        {"value": str(val), "count": int(cnt), "pct": round(cnt / total * 100, 1)}
        for val, cnt in counts.head(top_n).items()
    ]
    return {
        "total_responses": total,
        "unique_answers": int(counts.shape[0]),
        "top_answers": top,
    }


def _build_nl_summary(col: str, col_type: str, stats: dict) -> str:
    """Build a single natural-language sentence describing a survey question's results."""
    if col_type == "numeric":
        return (
            f"For the question '{col}': {stats['count']} responses were collected. "
            f"The average score was {stats['mean']} (median {stats['median']}, "
            f"ranging from {stats['min']} to {stats['max']})."
        )
    else:
        top = stats.get("top_answers", [])
        if not top:
            return f"For the question '{col}': no valid responses were recorded."
        top_str = ", ".join(
            f'"{a["value"]}" ({a["pct"]}%)' for a in top[:3]
        )
        return (
            f"For the question '{col}': {stats['total_responses']} responses were received "
            f"with {stats['unique_answers']} unique answers. "
            f"The most common responses were {top_str}."
        )


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    bucket: str = event["bucket"]
    key: str = event["key"]
    logger.info("survey_summarizer invoked for s3://%s/%s", bucket, key)

    try:
        import pandas as pd  # noqa: PLC0415
    except ImportError:
        logger.error("pandas is required but not installed in this Lambda")
        return {
            **event,
            "survey_summary": "Survey analysis unavailable (missing pandas).",
            "survey_stats": {},
        }

    # ── Load CSV from S3 ───────────────────────────────────────────────────────
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    except Exception:
        logger.error("Failed to read survey CSV", exc_info=True)
        return {
            **event,
            "survey_summary": "Could not read survey data.",
            "survey_stats": {},
        }

    logger.info("Survey loaded: %d rows × %d columns", len(df), len(df.columns))

    # ── Skip metadata / ID columns ─────────────────────────────────────────────
    skip_keywords = {"id", "timestamp", "email", "name", "respondent", "user_id"}
    question_cols = [
        c for c in df.columns
        if not any(kw in c.lower() for kw in skip_keywords)
    ]

    summaries: list[str] = []
    stats_by_col: dict[str, Any] = {}

    for col in question_cols:
        series = df[col]
        # Determine column type
        numeric_series = pd.to_numeric(series, errors="coerce")
        not_null = numeric_series.notna().sum()

        if not_null / max(len(series), 1) > 0.7:  # mostly numeric
            col_stats = _numeric_summary(numeric_series.dropna())
            summaries.append(_build_nl_summary(col, "numeric", col_stats))
            stats_by_col[col] = {"type": "numeric", **col_stats}
        else:
            col_stats = _categorical_summary(series)
            summaries.append(_build_nl_summary(col, "categorical", col_stats))
            stats_by_col[col] = {"type": "categorical", **col_stats}

    # ── Build overall narrative ────────────────────────────────────────────────
    intro = (
        f"Survey Analysis Summary ({len(df)} respondents, {len(question_cols)} questions):\n\n"
    )
    full_summary = intro + "\n\n".join(summaries)

    logger.info("Survey summary generated: %d chars", len(full_summary))

    # ── Persist to processed bucket ────────────────────────────────────────────
    summary_key = f"survey-summaries/{key.rsplit('/', 1)[-1]}.summary.txt"
    try:
        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=summary_key,
            Body=full_summary.encode("utf-8"),
            ContentType="text/plain",
        )
        logger.info("Summary written to s3://%s/%s", PROCESSED_BUCKET, summary_key)
    except Exception:
        logger.warning("Failed to write summary to S3", exc_info=True)

    return {
        **event,
        "survey_summary": full_summary,
        "normalized_text": full_summary,   # pass-through for bedrock_formatter
        "survey_stats": stats_by_col,
        "summary_key": summary_key,
    }
