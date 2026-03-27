"""
survey_summarizer/handler.py

Handles unstructured plain-text survey responses.

Reads the text file from S3 and passes the content directly as the
``survey_summary`` field so that bedrock_formatter can analyse it with
Amazon Nova Lite without any intermediate CSV parsing.

Input:  Step Functions state with "bucket" + "key" pointing to a survey .txt file
Output: State dict + "survey_summary": str  +  "survey_stats": dict
"""
from __future__ import annotations

import logging
import os
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

s3 = boto3.client("s3")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    bucket: str = event["bucket"]
    key: str = event["key"]
    logger.info("survey_summarizer invoked for s3://%s/%s", bucket, key)

    # Read plain-text survey from S3
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        survey_text = obj["Body"].read().decode("utf-8", errors="replace").strip()
    except Exception:
        logger.error("Failed to read survey file", exc_info=True)
        return {
            **event,
            "survey_summary": "Could not read survey data.",
            "survey_stats": {},
        }

    logger.info("Survey text loaded: %d chars", len(survey_text))

    # Persist to processed bucket
    summary_key = f"survey-summaries/{key.rsplit('/', 1)[-1]}.summary.txt"
    try:
        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=summary_key,
            Body=survey_text.encode("utf-8"),
            ContentType="text/plain",
        )
        logger.info("Survey text written to s3://%s/%s", PROCESSED_BUCKET, summary_key)
    except Exception:
        logger.warning("Failed to write survey to S3", exc_info=True)

    return {
        **event,
        "survey_summary": survey_text,
        "normalized_text": survey_text,
        "survey_stats": {},
        "summary_key": summary_key,
    }
