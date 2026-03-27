"""
feedback_quality_loop/handler.py

Implements a quality feedback loop: uses the Nova-generated insights to
derive data quality signals, update quality scores, and flag records that
need re-processing or human review.

Logic:
  1. Parse the insights text from bedrock_response for quality signals
  2. Calculate an improved quality score using both validation data and model signals
  3. Write a quality report to the processed S3 bucket
  4. Publish updated quality metrics to CloudWatch
  5. If quality is below threshold, tag the source object for re-processing

Input:  Full pipeline state dict (including bedrock_response, validation, comprehend_results)
Output: State dict + "quality_report": dict
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "")
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "CustomerFeedbackAnalyzer")
QUALITY_THRESHOLD = float(os.environ.get("QUALITY_THRESHOLD", "70"))

# Keywords that indicate quality issues in the model's response
QUALITY_CONCERN_PATTERNS = [
    re.compile(r"\b(insufficient|incomplete|missing|unclear|ambiguous|low quality)\b", re.I),
    re.compile(r"\b(too short|very brief|no data|no information)\b", re.I),
    re.compile(r"\b(spam|irrelevant|off[-\s]?topic)\b", re.I),
]

# Keywords that indicate high-quality signals
QUALITY_POSITIVE_PATTERNS = [
    re.compile(r"\b(detailed|comprehensive|clear|specific|actionable|rich)\b", re.I),
    re.compile(r"\b(consistent|reliable|accurate|informative)\b", re.I),
]


def _score_from_insights(insights_text: str) -> tuple[float, list[str]]:
    """
    Derive a quality adjustment signal from the model's insights text.
    Returns (adjustment: -10 to +10, issues: list of identified concerns).

    NOTE: Bedrock's business analysis routinely uses words like "missing",
    "incomplete", "unclear" to describe customer pain-points — NOT data quality
    issues. We therefore apply a very conservative, capped adjustment so that
    the language of the business report cannot drive the quality score to zero.
    """
    concerns: list[str] = []
    concern_hits = 0
    positive_hits = 0

    # Only flag explicit data-quality phrases, not general business language
    DATA_QUALITY_CONCERN_PATTERNS = [
        re.compile(r"\b(no data|no information|data is missing|data quality|empty response)\b", re.I),
        re.compile(r"\b(could not analyze|unable to analyze|insufficient data)\b", re.I),
    ]
    for pattern in DATA_QUALITY_CONCERN_PATTERNS:
        matches = pattern.findall(insights_text)
        if matches:
            concern_hits += len(matches)
            concerns.append(f"Model identified: {matches[0]}")

    for pattern in QUALITY_POSITIVE_PATTERNS:
        positive_hits += len(pattern.findall(insights_text))

    # Cap adjustment tightly so business language cannot zero-out the score
    adjustment = (positive_hits * 1) - (concern_hits * 3)
    adjustment = max(-10.0, min(10.0, float(adjustment)))

    return adjustment, concerns


def _compute_final_quality_score(event: dict) -> tuple[float, dict[str, Any]]:
    """
    Compute a final composite quality score combining:
      - Validation score (text_validator output)
      - Comprehend signal (entity/key-phrase richness)
      - Model insight signals
    """
    # Base: validation quality score (0–100)
    validation = event.get("validation", {})
    base_score = float(validation.get("quality_score", 50.0))

    # Boost from Comprehend richness
    comprehend = event.get("comprehend_results", {})
    entity_count = sum(comprehend.get("entities", {}).get("counts_by_type", {}).values())
    phrase_count = len(comprehend.get("key_phrases", []))
    comprehend_boost = min(10.0, entity_count * 0.5 + phrase_count * 0.3)

    # Adjustment from model signals
    insights = event.get("bedrock_response", {}).get("insights", "")
    model_adjustment, model_concerns = _score_from_insights(insights)

    final_score = round(min(100.0, max(0.0, base_score + comprehend_boost + model_adjustment)), 2)

    breakdown = {
        "base_score_from_validation": base_score,
        "comprehend_boost": round(comprehend_boost, 2),
        "model_adjustment": round(model_adjustment, 2),
        "final_score": final_score,
        "model_concerns_identified": model_concerns,
    }
    return final_score, breakdown


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    data_type: str = event.get("data_type", "unknown")
    bucket: str = event.get("bucket", "")
    key: str = event.get("key", "")
    logger.info("feedback_quality_loop invoked for data_type=%s, key=%s", data_type, key)

    # ── Compute final composite quality score ─────────────────────────────────
    final_score, score_breakdown = _compute_final_quality_score(event)

    needs_reprocessing = final_score < QUALITY_THRESHOLD
    status = "needs_review" if needs_reprocessing else "passed"

    quality_report = {
        "data_type": data_type,
        "source_key": key,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "quality_status": status,
        "score_breakdown": score_breakdown,
        "needs_reprocessing": needs_reprocessing,
        "threshold": QUALITY_THRESHOLD,
    }

    logger.info(
        "Quality assessment: score=%.1f, status=%s, needs_reprocessing=%s",
        final_score, status, needs_reprocessing,
    )

    # ── Write quality report to processed bucket ───────────────────────────────
    report_key = f"quality-reports/{data_type}/{key.rsplit('/', 1)[-1]}.quality.json"
    try:
        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=report_key,
            Body=json.dumps(quality_report, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Quality report written to s3://%s/%s", PROCESSED_BUCKET, report_key)
    except Exception:
        logger.warning("Failed to write quality report", exc_info=True)

    # ── Tag source object for re-processing if below threshold ────────────────
    if needs_reprocessing and bucket and key:
        try:
            s3.put_object_tagging(
                Bucket=bucket,
                Key=key,
                Tagging={
                    "TagSet": [
                        {"Key": "quality_status", "Value": "needs_review"},
                        {"Key": "quality_score", "Value": str(final_score)},
                        {"Key": "reviewed_at", "Value": datetime.now(timezone.utc).isoformat()},
                    ]
                },
            )
            logger.info("Tagged source object for review: s3://%s/%s", bucket, key)
        except Exception:
            logger.warning("Failed to tag source object", exc_info=True)

    # ── Publish CloudWatch metrics ─────────────────────────────────────────────
    try:
        cloudwatch.put_metric_data(
            Namespace=METRICS_NAMESPACE,
            MetricData=[
                {
                    "MetricName": "FinalDataQualityScore",
                    "Value": final_score,
                    "Unit": "None",
                    "Dimensions": [{"Name": "DataType", "Value": data_type}],
                },
                {
                    "MetricName": "RecordsNeedingReview",
                    "Value": 1.0 if needs_reprocessing else 0.0,
                    "Unit": "Count",
                    "Dimensions": [{"Name": "DataType", "Value": data_type}],
                },
            ],
        )
    except Exception:
        logger.warning("Failed to publish quality metrics", exc_info=True)

    return {
        **event,
        "quality_report": quality_report,
        "final_quality_score": final_score,
    }
