"""
textract_processor/handler.py

Extracts text from product images stored in S3 using Amazon Textract.

Supports:
  - Synchronous DetectDocumentText for images ≤ 5 MB
  - Asynchronous StartDocumentTextDetection + polling for larger files

Input:  Step Functions state dict (bucket + key pointing to an image file)
Output: State dict + "extracted_text": str  and  "textract_results": { ... }
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

s3 = boto3.client("s3")
textract = boto3.client("textract")
cloudwatch = boto3.client("cloudwatch")

PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "")
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "CustomerFeedbackAnalyzer")

ASYNC_POLL_INTERVAL_SECONDS = 5
ASYNC_MAX_WAIT_SECONDS = 300   # 5-minute ceiling


def _object_size(bucket: str, key: str) -> int:
    resp = s3.head_object(Bucket=bucket, Key=key)
    return int(resp["ContentLength"])


def _blocks_to_text(blocks: list[dict]) -> str:
    """Concatenate LINE blocks into a readable string."""
    lines = [b["Text"] for b in blocks if b.get("BlockType") == "LINE" and "Text" in b]
    return "\n".join(lines)


def _sync_detect(bucket: str, key: str) -> tuple[str, dict]:
    """Call DetectDocumentText synchronously (images ≤ 5 MB)."""
    response = textract.detect_document_text(
        Document={"S3Object": {"Bucket": bucket, "Name": key}}
    )
    blocks = response.get("Blocks", [])
    text = _blocks_to_text(blocks)
    metadata = {
        "pages": response.get("DocumentMetadata", {}).get("Pages", 1),
        "block_count": len(blocks),
        "method": "sync",
    }
    return text, metadata


def _async_detect(bucket: str, key: str) -> tuple[str, dict]:
    """Start async text detection, poll until complete, return text."""
    job_name = f"textract-{int(time.time())}"
    start_resp = textract.start_document_text_detection(
        DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
        ClientRequestToken=job_name,
    )
    job_id = start_resp["JobId"]
    logger.info("Textract async job started: %s", job_id)

    elapsed = 0
    all_blocks: list[dict] = []
    pages = 0

    while elapsed < ASYNC_MAX_WAIT_SECONDS:
        time.sleep(ASYNC_POLL_INTERVAL_SECONDS)
        elapsed += ASYNC_POLL_INTERVAL_SECONDS

        resp = textract.get_document_text_detection(JobId=job_id)
        status = resp["JobStatus"]

        if status == "SUCCEEDED":
            all_blocks.extend(resp.get("Blocks", []))
            pages = resp.get("DocumentMetadata", {}).get("Pages", 0)

            # Paginate if more results are available
            next_token = resp.get("NextToken")
            while next_token:
                page_resp = textract.get_document_text_detection(
                    JobId=job_id, NextToken=next_token
                )
                all_blocks.extend(page_resp.get("Blocks", []))
                next_token = page_resp.get("NextToken")
            break

        elif status == "FAILED":
            raise RuntimeError(
                f"Textract async job {job_id} failed: {resp.get('StatusMessage', 'Unknown')}"
            )
        else:
            logger.debug("Waiting for Textract job %s... (%ds elapsed)", job_id, elapsed)

    else:
        raise TimeoutError(
            f"Textract job {job_id} did not complete within {ASYNC_MAX_WAIT_SECONDS}s"
        )

    text = _blocks_to_text(all_blocks)
    metadata = {
        "job_id": job_id,
        "pages": pages,
        "block_count": len(all_blocks),
        "method": "async",
    }
    return text, metadata


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    bucket: str = event["bucket"]
    key: str = event["key"]
    logger.info("textract_processor invoked for s3://%s/%s", bucket, key)

    # Choose sync vs async based on file size (5 MB threshold)
    try:
        size = _object_size(bucket, key)
    except Exception:
        logger.warning("Could not determine object size; defaulting to async", exc_info=True)
        size = 6 * 1024 * 1024  # force async path

    try:
        if size <= 5 * 1024 * 1024:
            extracted_text, metadata = _sync_detect(bucket, key)
        else:
            extracted_text, metadata = _async_detect(bucket, key)
    except Exception:
        logger.error("Textract extraction failed", exc_info=True)
        return {
            **event,
            "extracted_text": "",
            "textract_results": {"error": "extraction_failed", "key": key},
        }

    logger.info(
        "Textract extraction complete: %d chars, %d blocks, method=%s",
        len(extracted_text),
        metadata.get("block_count", 0),
        metadata.get("method"),
    )

    # Persist extracted text to processed bucket
    output_key = f"textract-output/{key.rsplit('/', 1)[-1]}.txt"
    if extracted_text:
        s3.put_object(
            Bucket=PROCESSED_BUCKET,
            Key=output_key,
            Body=extracted_text.encode("utf-8"),
            ContentType="text/plain",
        )
        logger.info("Extracted text saved to s3://%s/%s", PROCESSED_BUCKET, output_key)

    # Publish metrics
    try:
        cloudwatch.put_metric_data(
            Namespace=METRICS_NAMESPACE,
            MetricData=[
                {
                    "MetricName": "TextractCharactersExtracted",
                    "Value": float(len(extracted_text)),
                    "Unit": "Count",
                    "Dimensions": [{"Name": "DataType", "Value": "images"}],
                }
            ],
        )
    except Exception:
        logger.warning("Failed to publish Textract metrics", exc_info=True)

    return {
        **event,
        "extracted_text": extracted_text,
        "normalized_text": extracted_text,   # pass through to comprehend/bedrock
        "textract_results": {**metadata, "output_key": output_key},
    }
