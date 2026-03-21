"""
comprehend_processor/handler.py

Performs NLP analysis on normalised customer feedback text using Amazon Comprehend:
  - Batch sentiment analysis  (POSITIVE / NEGATIVE / NEUTRAL / MIXED)
  - Entity detection           (PERSON, ORGANIZATION, LOCATION, COMMERCIAL_ITEM, …)
  - Key-phrase extraction
  - Dominant language detection

Input:  Step Functions state dict (must contain "normalized_text": str)
Output: State dict + "comprehend_results": { ... }
"""
from __future__ import annotations

import json
import logging
import os
from collections import Counter
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

comprehend = boto3.client("comprehend")
cloudwatch = boto3.client("cloudwatch")

LANGUAGE_CODE = os.environ.get("COMPREHEND_LANGUAGE", "en")
BATCH_SIZE = int(os.environ.get("COMPREHEND_BATCH_SIZE", "25"))
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "CustomerFeedbackAnalyzer")

# Comprehend hard limits
MAX_BATCH_SIZE = 25
MAX_BYTES_PER_DOC = 5_000   # Comprehend limit is ~5 KB per document


def _safe_encode(text: str) -> str:
    """Truncate text to stay within the Comprehend byte limit."""
    encoded = text.encode("utf-8")
    if len(encoded) > MAX_BYTES_PER_DOC:
        encoded = encoded[:MAX_BYTES_PER_DOC]
        # Decode safely (don't split a multi-byte char)
        return encoded.decode("utf-8", errors="ignore")
    return text


def _batch_texts(texts: list[str], size: int = MAX_BATCH_SIZE) -> list[list[str]]:
    return [texts[i : i + size] for i in range(0, len(texts), size)]


def _detect_language(text: str) -> str:
    """Detect the dominant language; fallback to configured language."""
    try:
        resp = comprehend.detect_dominant_language(Text=text[:1_000])
        if resp["Languages"]:
            return resp["Languages"][0]["LanguageCode"]
    except Exception:
        logger.debug("Language detection failed, using default", exc_info=True)
    return LANGUAGE_CODE


def _aggregate_sentiments(responses: list[dict]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    score_sums: dict[str, float] = {
        "Positive": 0.0, "Negative": 0.0, "Neutral": 0.0, "Mixed": 0.0
    }
    total = 0

    for resp in responses:
        for item in resp.get("ResultList", []):
            sentiment = item.get("Sentiment", "NEUTRAL")
            counts[sentiment] += 1
            total += 1
            scores = item.get("SentimentScore", {})
            for k, v in scores.items():
                score_sums[k] = score_sums.get(k, 0.0) + v

    avg_scores = {
        k: round(v / total, 4) if total else 0.0 for k, v in score_sums.items()
    }

    dominant = counts.most_common(1)[0][0] if counts else "NEUTRAL"
    return {
        "counts": dict(counts),
        "average_scores": avg_scores,
        "dominant_sentiment": dominant,
        "total_analyzed": total,
    }


def _aggregate_entities(responses: list[dict]) -> dict[str, Any]:
    entity_counts: Counter[str] = Counter()
    top_entities: dict[str, Counter] = {}

    for resp in responses:
        for item in resp.get("ResultList", []):
            for entity in item.get("Entities", []):
                etype = entity.get("Type", "OTHER")
                etext = entity.get("Text", "")
                entity_counts[etype] += 1
                if etype not in top_entities:
                    top_entities[etype] = Counter()
                top_entities[etype][etext] += 1

    top_by_type = {
        etype: counter.most_common(10)   # top-10 per entity type
        for etype, counter in top_entities.items()
    }

    return {
        "counts_by_type": dict(entity_counts),
        "top_entities_by_type": top_by_type,
    }


def _aggregate_key_phrases(responses: list[dict]) -> list[tuple[str, int]]:
    phrase_counter: Counter[str] = Counter()
    for resp in responses:
        for item in resp.get("ResultList", []):
            for phrase in item.get("KeyPhrases", []):
                text = phrase.get("Text", "").lower().strip()
                if text:
                    phrase_counter[text] += 1
    return phrase_counter.most_common(30)


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    logger.info("comprehend_processor invoked for data_type=%s", event.get("data_type"))

    normalized_text: str = event.get("normalized_text", "")
    if not normalized_text.strip():
        logger.warning("normalized_text is empty; skipping Comprehend analysis")
        return {
            **event,
            "comprehend_results": {
                "sentiment": {},
                "entities": {},
                "key_phrases": [],
                "language": LANGUAGE_CODE,
                "skipped": True,
            },
        }

    # Split into individual reviews/segments (separated by "---" delimiter)
    segments = [
        s.strip()
        for s in normalized_text.split("---")
        if s.strip()
    ]
    if not segments:
        segments = [normalized_text]

    # Truncate each segment to Comprehend limits
    docs = [_safe_encode(seg) for seg in segments if seg]

    # Detect dominant language from a sample
    language = _detect_language(docs[0]) if docs else LANGUAGE_CODE
    logger.info("Detected language: %s | Documents to analyze: %d", language, len(docs))

    # ── Batch sentiment analysis ───────────────────────────────────────────────
    sentiment_responses: list[dict] = []
    for batch in _batch_texts(docs, MAX_BATCH_SIZE):
        try:
            resp = comprehend.batch_detect_sentiment(
                TextList=batch, LanguageCode=language
            )
            sentiment_responses.append(resp)
        except Exception:
            logger.error("batch_detect_sentiment failed", exc_info=True)

    # ── Batch entity detection ─────────────────────────────────────────────────
    entity_responses: list[dict] = []
    for batch in _batch_texts(docs, MAX_BATCH_SIZE):
        try:
            resp = comprehend.batch_detect_entities(
                TextList=batch, LanguageCode=language
            )
            entity_responses.append(resp)
        except Exception:
            logger.error("batch_detect_entities failed", exc_info=True)

    # ── Batch key-phrase extraction ────────────────────────────────────────────
    key_phrase_responses: list[dict] = []
    for batch in _batch_texts(docs, MAX_BATCH_SIZE):
        try:
            resp = comprehend.batch_detect_key_phrases(
                TextList=batch, LanguageCode=language
            )
            key_phrase_responses.append(resp)
        except Exception:
            logger.error("batch_detect_key_phrases failed", exc_info=True)

    # ── Aggregate results ──────────────────────────────────────────────────────
    sentiment_agg = _aggregate_sentiments(sentiment_responses)
    entities_agg = _aggregate_entities(entity_responses)
    key_phrases = _aggregate_key_phrases(key_phrase_responses)

    comprehend_results = {
        "language": language,
        "total_documents": len(docs),
        "sentiment": sentiment_agg,
        "entities": entities_agg,
        "key_phrases": key_phrases,
    }

    logger.info(
        "Comprehend analysis complete: dominant_sentiment=%s, entity_types=%d, key_phrases=%d",
        sentiment_agg.get("dominant_sentiment"),
        len(entities_agg.get("counts_by_type", {})),
        len(key_phrases),
    )

    # ── Publish metrics ────────────────────────────────────────────────────────
    try:
        dominant = sentiment_agg.get("dominant_sentiment", "NEUTRAL")
        cloudwatch.put_metric_data(
            Namespace=METRICS_NAMESPACE,
            MetricData=[
                {
                    "MetricName": "EntitiesFound",
                    "Value": float(sum(entities_agg.get("counts_by_type", {}).values())),
                    "Unit": "Count",
                    "Dimensions": [{"Name": "DataType", "Value": event.get("data_type", "unknown")}],
                },
                {
                    "MetricName": "SentimentDistribution",
                    "Value": float(sentiment_agg.get("counts", {}).get(dominant, 0)),
                    "Unit": "Count",
                    "Dimensions": [
                        {"Name": "Sentiment", "Value": dominant},
                        {"Name": "DataType", "Value": event.get("data_type", "unknown")},
                    ],
                },
            ],
        )
    except Exception:
        logger.warning("Failed to publish Comprehend metrics", exc_info=True)

    return {**event, "comprehend_results": comprehend_results}
