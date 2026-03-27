"""
pipeline_orchestrator/handler.py

Triggered by EventBridge S3 Object Created events.
Detects the data type from the S3 key prefix, then starts a
Step Functions execution for the appropriate processing branch.
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

sfn_client = boto3.client("stepfunctions")
cloudwatch = boto3.client("cloudwatch")

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "CustomerFeedbackAnalyzer")

# Maps S3 key prefix → pipeline data_type label
PREFIX_TO_DATA_TYPE: dict[str, str] = {
    "text-reviews/": "text_reviews",
    "images/": "images",
    "audio/": "audio",
    "surveys/": "surveys",
}


def _detect_data_type(key: str) -> str:
    for prefix, data_type in PREFIX_TO_DATA_TYPE.items():
        if key.startswith(prefix):
            return data_type
    return "unknown"


def _start_execution(bucket: str, key: str, data_type: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    execution_name = f"{data_type}-{timestamp}"[:80]  # SF name limit

    payload = {
        "bucket": bucket,
        "key": key,
        "data_type": data_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "processed_results": {},
    }

    response = sfn_client.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        name=execution_name,
        input=json.dumps(payload),
    )
    return response["executionArn"]


def _publish_metric(data_type: str, metric_name: str, value: float = 1.0) -> None:
    """Publish a metric with DataType dimension AND without (for dashboard aggregation)."""
    metric_data = [
        {
            "MetricName": metric_name,
            "Value": value,
            "Unit": "Count",
            "Dimensions": [{"Name": "DataType", "Value": data_type}],
        },
        {
            "MetricName": metric_name,
            "Value": value,
            "Unit": "Count",
            "Dimensions": [],  # no-dimension copy so dashboard widgets show data
        },
    ]
    try:
        cloudwatch.put_metric_data(
            Namespace=METRICS_NAMESPACE,
            MetricData=metric_data,
        )
    except Exception:
        logger.warning("Failed to publish metric %s", metric_name, exc_info=True)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    logger.info("Received event: %s", json.dumps(event, default=str))

    execution_arns: list[str] = []
    skipped: list[str] = []

    # EventBridge S3 Object Created events have detail.bucket.name and detail.object.key
    if "detail" in event:
        records = [event]
    else:
        # Direct S3 event (e.g. from test invocation)
        records = event.get("Records", [])

    for record in records:
        try:
            # Support both EventBridge and direct S3 notification formats
            if "detail" in record:
                bucket = record["detail"]["bucket"]["name"]
                key = record["detail"]["object"]["key"]
            elif "s3" in record:
                bucket = record["s3"]["bucket"]["name"]
                key = record["s3"]["object"]["key"]
            else:
                logger.warning("Unrecognized record format: %s", record)
                continue

            data_type = _detect_data_type(key)
            if data_type == "unknown":
                logger.warning("Skipping unrecognized key prefix: %s", key)
                skipped.append(key)
                continue

            arn = _start_execution(bucket, key, data_type)
            logger.info("Started execution %s for s3://%s/%s", arn, bucket, key)
            execution_arns.append(arn)
            _publish_metric(data_type, "PipelineExecutionsStarted")

        except Exception:
            logger.error("Failed to process record", exc_info=True)
            _publish_metric("unknown", "PipelineExecutionsFailed")

    return {
        "started_executions": len(execution_arns),
        "execution_arns": execution_arns,
        "skipped_keys": skipped,
    }
