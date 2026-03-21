"""
text_normalizer/handler.py

Normalises raw text content (reviews or transcriptions).
Used in two Step Functions branches:
  - After text_validator (for text_reviews)
  - After transcribe_processor (for audio transcripts)

Input:
  Forwarded Step Functions state dict with at minimum:
  {
    "bucket": str,
    "key": str,
    "data_type": str,
    "raw_text": str  (optional — used for audio transcripts)
  }

Output:
  Same dict + "normalized_text": str (cleaned, UTF-8 normalised text)
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
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "")

# ── Regex patterns ─────────────────────────────────────────────────────────────
HTML_TAG_RE = re.compile(r"<[^>]+>")
URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
REPEATED_PUNCT_RE = re.compile(r"([!?.,;:-])\1{2,}")
MULTI_SPACE_RE = re.compile(r" {2,}")
MULTI_NEWLINE_RE = re.compile(r"\n{3,}")
EMOJI_RE = re.compile(
    "["
    "\U0001F600-\U0001F64F"  # emoticons
    "\U0001F300-\U0001F5FF"  # symbols & pictographs
    "\U0001F680-\U0001F6FF"  # transport & map
    "\U0001F1E0-\U0001F1FF"  # flags
    "\U00002500-\U00002BEF"
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "]+",
    flags=re.UNICODE,
)


def normalize_text(text: str) -> str:
    """Apply a sequence of normalisation steps to raw text."""
    if not text:
        return ""

    # 1. Normalise Unicode to NFC (composed form)
    text = unicodedata.normalize("NFC", text)

    # 2. Remove HTML tags
    text = HTML_TAG_RE.sub(" ", text)

    # 3. Remove URLs
    text = URL_RE.sub(" ", text)

    # 4. Remove emojis (replace with space to preserve word boundaries)
    text = EMOJI_RE.sub(" ", text)

    # 5. Replace common HTML entities
    html_entities = {
        "&amp;": "&", "&lt;": "<", "&gt;": ">",
        "&quot;": '"', "&#39;": "'", "&nbsp;": " ",
        "&apos;": "'",
    }
    for entity, replacement in html_entities.items():
        text = text.replace(entity, replacement)

    # 6. Collapse repeated punctuation (e.g. "!!!" → "!")
    text = REPEATED_PUNCT_RE.sub(r"\1", text)

    # 7. Collapse multiple spaces/newlines
    text = MULTI_SPACE_RE.sub(" ", text)
    text = MULTI_NEWLINE_RE.sub("\n\n", text)

    return text.strip()


def _normalize_csv_column(bucket: str, key: str) -> tuple[str, str]:
    """
    Download CSV, normalise the review_content column, upload result,
    return (processed_s3_key, concatenated_sample_text).
    """
    try:
        import pandas as pd  # noqa: PLC0415
    except ImportError:
        logger.warning("pandas not available inside text_normalizer")
        return "", ""

    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))

    if "review_content" not in df.columns:
        logger.warning("review_content column missing; skipping normalisation")
        return "", ""

    df["review_content_normalized"] = df["review_content"].fillna("").apply(normalize_text)

    # Write normalised CSV back to the processed bucket
    output_key = key.replace("text-reviews/", "normalized/", 1)
    if not output_key.startswith("normalized/"):
        output_key = f"normalized/{output_key}"

    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=output_key,
        Body=buf.getvalue(),
        ContentType="text/csv",
    )
    logger.info("Normalised CSV written to s3://%s/%s", PROCESSED_BUCKET, output_key)

    # Return a concatenated sample of the first 50 reviews for downstream use
    sample_texts = df["review_content_normalized"].head(50).tolist()
    combined = "\n\n---\n\n".join(str(t) for t in sample_texts if str(t).strip())
    return output_key, combined


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    logger.info("text_normalizer invoked for data_type=%s", event.get("data_type"))

    bucket: str = event["bucket"]
    key: str = event["key"]
    data_type: str = event.get("data_type", "")

    if data_type in ("text_reviews", "surveys"):
        # Normalise a CSV file's review_content column
        processed_key, combined_text = _normalize_csv_column(bucket, key)
        return {
            **event,
            "processed_key": processed_key or key,
            "normalized_text": combined_text,
        }

    elif data_type == "audio":
        # Normalise a transcription string passed in from transcribe_processor
        raw_text: str = event.get("transcript", "") or event.get("raw_text", "")
        normalised = normalize_text(raw_text)

        # Persist normalized transcript to processed bucket
        transcript_key = f"normalized/{key.rsplit('/', 1)[-1]}.txt"
        if normalised:
            s3.put_object(
                Bucket=PROCESSED_BUCKET,
                Key=transcript_key,
                Body=normalised.encode("utf-8"),
                ContentType="text/plain",
            )

        return {
            **event,
            "processed_key": transcript_key,
            "normalized_text": normalised,
        }

    else:
        # Generic: normalise any "raw_text" field present in state
        raw_text = event.get("raw_text", "") or event.get("extracted_text", "")
        return {
            **event,
            "normalized_text": normalize_text(str(raw_text)),
        }
