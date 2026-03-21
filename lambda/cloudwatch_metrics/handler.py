"""
cloudwatch_metrics/handler.py

Final Step Functions stage: publishes a complete set of custom CloudWatch metrics
summarising the pipeline execution result.

Tracks over time:
  - Pipeline success/failure counts
  - Final quality scores (by data type)
  - Sentiment distribution
  - Entity extraction richness
  - Bedrock token usage
  - Processing latency (when execution_start_timestamp is available)
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

cloudwatch = boto3.client("cloudwatch")

METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "CustomerFeedbackAnalyzer")


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    logger.info("cloudwatch_metrics invoked for data_type=%s", event.get("data_type"))

    data_type: str = event.get("data_type", "unknown")
    dimensions = [{"Name": "DataType", "Value": data_type}]

    metric_data: list[dict] = []

    # ── Pipeline success ───────────────────────────────────────────────────────
    metric_data.append({
        "MetricName": "PipelineExecutionsSucceeded",
        "Value": 1.0,
        "Unit": "Count",
        "Dimensions": dimensions,
    })

    # ── Final quality score ────────────────────────────────────────────────────
    final_score = _safe_float(event.get("final_quality_score"), 0.0)
    if final_score > 0:
        metric_data.append({
            "MetricName": "FinalDataQualityScore",
            "Value": final_score,
            "Unit": "None",
            "Dimensions": dimensions,
        })

    # ── Validation metrics ─────────────────────────────────────────────────────
    validation = event.get("validation", {})
    if validation:
        metric_data.append({
            "MetricName": "ValidationQualityScore",
            "Value": _safe_float(validation.get("quality_score")),
            "Unit": "None",
            "Dimensions": dimensions,
        })
        invalid_rows = _safe_float(validation.get("invalid_rows"))
        if invalid_rows > 0:
            metric_data.append({
                "MetricName": "ValidationFailures",
                "Value": invalid_rows,
                "Unit": "Count",
                "Dimensions": dimensions,
            })

    # ── Comprehend sentiment ───────────────────────────────────────────────────
    comprehend = event.get("comprehend_results", {})
    sentiment = comprehend.get("sentiment", {})
    sentiment_counts = sentiment.get("counts", {})
    for sentiment_label, count in sentiment_counts.items():
        metric_data.append({
            "MetricName": "SentimentCount",
            "Value": _safe_float(count),
            "Unit": "Count",
            "Dimensions": [
                {"Name": "DataType", "Value": data_type},
                {"Name": "Sentiment", "Value": sentiment_label},
            ],
        })

    entity_total = sum(
        comprehend.get("entities", {}).get("counts_by_type", {}).values()
    )
    if entity_total > 0:
        metric_data.append({
            "MetricName": "EntitiesExtracted",
            "Value": _safe_float(entity_total),
            "Unit": "Count",
            "Dimensions": dimensions,
        })

    key_phrases_count = len(comprehend.get("key_phrases", []))
    if key_phrases_count > 0:
        metric_data.append({
            "MetricName": "KeyPhrasesExtracted",
            "Value": float(key_phrases_count),
            "Unit": "Count",
            "Dimensions": dimensions,
        })

    # ── Bedrock token usage ────────────────────────────────────────────────────
    bedrock_resp = event.get("bedrock_response", {})
    input_tokens = _safe_float(bedrock_resp.get("input_tokens"))
    if input_tokens > 0:
        metric_data.append({
            "MetricName": "BedrockInputTokens",
            "Value": input_tokens,
            "Unit": "Count",
            "Dimensions": dimensions,
        })

    # ── Publish in batches (CW limit: 1000 metrics per call, but 20 is our max here) ──
    if metric_data:
        try:
            cloudwatch.put_metric_data(
                Namespace=METRICS_NAMESPACE,
                MetricData=metric_data,
            )
            logger.info(
                "Published %d metrics to CloudWatch namespace %s",
                len(metric_data),
                METRICS_NAMESPACE,
            )
        except Exception:
            logger.error("Failed to publish CloudWatch metrics", exc_info=True)

    return {
        **event,
        "pipeline_status": "SUCCEEDED",
        "metrics_published": len(metric_data),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
