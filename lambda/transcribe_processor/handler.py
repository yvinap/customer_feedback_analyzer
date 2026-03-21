"""
transcribe_processor/handler.py

Transcribes customer-service call recordings stored in S3 using Amazon Transcribe.

Flow:
  1. Start a TranscriptionJob for the S3 audio object
  2. Poll until COMPLETED or FAILED (up to Lambda timeout)
  3. Download completed transcript JSON from S3 output location
  4. Return the transcript text in the Step Functions state

Supported audio formats: mp3, mp4, wav, flac, ogg, amr, webm
"""
from __future__ import annotations

import json
import logging
import os
import time
import uuid
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

transcribe = boto3.client("transcribe")
s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "")
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "CustomerFeedbackAnalyzer")

POLL_INTERVAL = 10        # seconds between status checks
MAX_WAIT = 600            # 10 minutes maximum wait time

AUDIO_FORMAT_MAP = {
    ".mp3": "mp3",
    ".mp4": "mp4",
    ".wav": "wav",
    ".flac": "flac",
    ".ogg": "ogg",
    ".amr": "amr",
    ".webm": "webm",
}


def _infer_media_format(key: str) -> str:
    """Derive the media format from the file extension."""
    ext = "." + key.rsplit(".", 1)[-1].lower() if "." in key else ""
    fmt = AUDIO_FORMAT_MAP.get(ext)
    if not fmt:
        raise ValueError(
            f"Unsupported audio format for key '{key}'. "
            f"Supported: {list(AUDIO_FORMAT_MAP.values())}"
        )
    return fmt


def _start_transcription(bucket: str, key: str, output_bucket: str) -> tuple[str, str]:
    """Start a Transcribe job; return (job_name, output_key)."""
    media_format = _infer_media_format(key)
    job_name = f"cfa-transcribe-{uuid.uuid4().hex[:16]}"
    output_key = f"transcribe-output/{job_name}.json"

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={"MediaFileUri": f"s3://{bucket}/{key}"},
        MediaFormat=media_format,
        LanguageCode="en-US",
        OutputBucketName=output_bucket,
        OutputKey=output_key,
        Settings={
            "ShowSpeakerLabels": True,
            "MaxSpeakerLabels": 10,
        },
    )
    logger.info("Transcription job started: %s → s3://%s/%s", job_name, output_bucket, output_key)
    return job_name, output_key


def _poll_job(job_name: str) -> str:
    """Poll until the job completes; return final status."""
    elapsed = 0
    while elapsed < MAX_WAIT:
        time.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

        resp = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        status = resp["TranscriptionJob"]["TranscriptionJobStatus"]
        logger.debug("Job %s status: %s (%ds elapsed)", job_name, status, elapsed)

        if status in ("COMPLETED", "FAILED"):
            return status

    return "TIMEOUT"


def _download_transcript(bucket: str, key: str) -> str:
    """Download and parse the Transcribe JSON output; return plain transcript text."""
    obj = s3.get_object(Bucket=bucket, Key=key)
    transcript_json = json.loads(obj["Body"].read())

    # Standard Transcribe output structure
    results = transcript_json.get("results", {})
    transcripts = results.get("transcripts", [])
    if transcripts:
        return transcripts[0].get("transcript", "")

    return ""


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    bucket: str = event["bucket"]
    key: str = event["key"]
    logger.info("transcribe_processor invoked for s3://%s/%s", bucket, key)

    try:
        job_name, output_key = _start_transcription(bucket, key, PROCESSED_BUCKET)
    except ValueError as exc:
        logger.error("Cannot start transcription: %s", exc)
        return {
            **event,
            "transcript": "",
            "transcribe_results": {"error": str(exc)},
        }

    status = _poll_job(job_name)

    if status == "COMPLETED":
        transcript_text = _download_transcript(PROCESSED_BUCKET, output_key)
        logger.info(
            "Transcription complete: %d chars, job=%s", len(transcript_text), job_name
        )

        # Publish success metric
        try:
            cloudwatch.put_metric_data(
                Namespace=METRICS_NAMESPACE,
                MetricData=[
                    {
                        "MetricName": "TranscriptionJobsCompleted",
                        "Value": 1.0,
                        "Unit": "Count",
                        "Dimensions": [{"Name": "DataType", "Value": "audio"}],
                    },
                    {
                        "MetricName": "TranscriptionCharacters",
                        "Value": float(len(transcript_text)),
                        "Unit": "Count",
                        "Dimensions": [{"Name": "DataType", "Value": "audio"}],
                    },
                ],
            )
        except Exception:
            logger.warning("Failed to publish transcription metrics", exc_info=True)

        return {
            **event,
            "transcript": transcript_text,
            "raw_text": transcript_text,
            "transcribe_results": {
                "job_name": job_name,
                "output_key": output_key,
                "status": status,
                "transcript_length": len(transcript_text),
            },
        }
    else:
        error_msg = f"Transcription job {job_name} ended with status: {status}"
        logger.error(error_msg)
        return {
            **event,
            "transcript": "",
            "transcribe_results": {
                "job_name": job_name,
                "status": status,
                "error": error_msg,
            },
        }
