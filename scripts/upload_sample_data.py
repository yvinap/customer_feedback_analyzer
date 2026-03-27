"""
scripts/upload_sample_data.py

Uploads sample_data/amazon.csv (and any other files in sample_data/)
to the correct S3 input bucket prefix so the pipeline can be triggered.

Usage:
    python scripts/upload_sample_data.py
    python scripts/upload_sample_data.py --bucket my-custom-bucket
    python scripts/upload_sample_data.py --prefix cfa --profile aws_swami
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

SAMPLE_DATA_DIR = Path(__file__).parent.parent / "sample_data"

# Maps local filename patterns → S3 key prefix
# Order matters: more specific patterns must come before generic ones.
UPLOAD_RULES: list[tuple[str, str]] = [
    ("survey_*.txt", "surveys/"),    # unstructured survey text files
    ("*.csv", "text-reviews/"),
    ("*.json", "surveys/"),
    ("*.mp3", "audio/"),
    ("*.wav", "audio/"),
    ("*.flac", "audio/"),
    ("*.jpg", "images/"),
    ("*.jpeg", "images/"),
    ("*.png", "images/"),
]


def _resolve_bucket(prefix: str, profile: str) -> str:
    """Discover the input bucket name from CloudFormation stack output."""
    session = boto3.Session(profile_name=profile)
    cf = session.client("cloudformation")
    try:
        resp = cf.describe_stacks(StackName=f"CfaStorageStack")
        for output in resp["Stacks"][0].get("Outputs", []):
            if output["OutputKey"] == "InputBucketName":
                return output["OutputValue"]
    except Exception:
        logger.debug("CloudFormation lookup failed", exc_info=True)

    # Fallback: construct the predictable name
    sts = session.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    return f"{prefix}-customer-feedback-input-{account_id}"


def upload_files(bucket: str, profile: str) -> None:
    session = boto3.Session(profile_name=profile)
    s3 = session.client("s3")

    if not SAMPLE_DATA_DIR.exists():
        logger.error("Sample data directory not found: %s", SAMPLE_DATA_DIR)
        sys.exit(1)

    all_files = list(SAMPLE_DATA_DIR.iterdir())
    if not all_files:
        logger.warning("No files found in %s", SAMPLE_DATA_DIR)
        return

    uploaded = 0
    for file_path in all_files:
        if not file_path.is_file():
            continue

        # Determine target prefix
        s3_prefix = None
        for pattern, prefix in UPLOAD_RULES:
            if file_path.match(pattern):
                s3_prefix = prefix
                break

        if s3_prefix is None:
            logger.debug("Skipping unrecognised file type: %s", file_path.name)
            continue

        s3_key = f"{s3_prefix}{file_path.name}"
        logger.info("Uploading %s → s3://%s/%s", file_path.name, bucket, s3_key)

        try:
            s3.upload_file(
                str(file_path),
                bucket,
                s3_key,
                ExtraArgs={"ContentType": _content_type(file_path)},
            )
            uploaded += 1
            logger.info("  ✓ Uploaded successfully")
        except (BotoCoreError, ClientError) as exc:
            logger.error("  ✗ Upload failed: %s", exc)

    logger.info("Upload complete: %d file(s) uploaded to s3://%s", uploaded, bucket)


def _content_type(path: Path) -> str:
    ext = path.suffix.lower()
    mapping = {
        ".csv": "text/csv",
        ".json": "application/json",
        ".txt": "text/plain",
        ".mp3": "audio/mpeg",
        ".wav": "audio/wav",
        ".flac": "audio/flac",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
    }
    return mapping.get(ext, "application/octet-stream")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload sample data to S3 input bucket")
    parser.add_argument("--bucket", help="Override S3 bucket name (auto-detected if omitted)")
    parser.add_argument("--prefix", default="cfa", help="Resource prefix (default: cfa)")
    parser.add_argument("--profile", default="aws_swami", help="AWS profile (default: aws_swami)")
    args = parser.parse_args()

    bucket = args.bucket or _resolve_bucket(args.prefix, args.profile)
    logger.info("Target bucket: %s", bucket)
    upload_files(bucket, args.profile)


if __name__ == "__main__":
    main()
