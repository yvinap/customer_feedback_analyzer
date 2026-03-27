"""
bedrock_formatter/handler.py

Formats all processed data from previous pipeline stages into structured
prompts for Amazon Nova Lite (via Amazon Bedrock) and invokes the model to generate
actionable business insights.

Supports:
  - Text-reviews path   (with Comprehend sentiment/entity enrichment)
  - Image path          (Textract extracted text)
  - Audio path          (transcribed + normalised text)
  - Survey path         (NL summaries with statistics)
  - Multimodal requests (image bytes + text in the same message)

Input:  Full Step Functions state dict from previous stages
Output: State dict + "bedrock_response": dict  +  insight written to output S3 bucket
"""
from __future__ import annotations

import base64
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

bedrock_runtime = boto3.client("bedrock-runtime", region_name=os.environ.get("BEDROCK_REGION", "us-east-1"))
s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")

BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "amazon.nova-lite-v1:0")
OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "")
MAX_TOKENS = int(os.environ.get("MAX_TOKENS", "4096"))
METRICS_NAMESPACE = os.environ.get("METRICS_NAMESPACE", "CustomerFeedbackAnalyzer")

# ── Prompt templates ───────────────────────────────────────────────────────────
SYSTEM_PROMPT = (
    "You are a senior customer-experience analyst. "
    "Your role is to analyze customer feedback data and extract clear, "
    "actionable business insights. Structure your response in well-organized "
    "sections. Be specific, data-driven, and highlight priority areas."
)

REVIEW_TEMPLATE = """\
You have been provided with customer feedback analysis for {product_context}.

## Sentiment Analysis
- Dominant sentiment: {dominant_sentiment}
- Sentiment distribution: {sentiment_counts}

## Key Entities Detected
{entities_summary}

## Top Key Phrases
{key_phrases_summary}

## Sample Reviews (normalized)
{sample_text}

---
Based on this data, please provide:
1. **Executive Summary** – overall customer sentiment in 2–3 sentences
2. **Strengths** – what customers consistently praise (top 3)
3. **Pain Points** – recurring issues and complaints (top 3)
4. **Product/Service Recommendations** – specific, actionable improvements
5. **Priority Action Items** – ranked by business impact
"""

IMAGE_TEMPLATE = """\
The following text has been extracted from product images using OCR:

{extracted_text}

---
Based on the text extracted from these product images, provide:
1. **Content Summary** – what information do the images convey?
2. **Brand & Product Signals** – notable product features, claims, or branding
3. **Quality Observations** – any inconsistencies or concerns in the image text
4. **Recommendations** – how can image content be improved for customer clarity?
"""

AUDIO_TEMPLATE = """\
The following is a transcript of a customer service call recording:

{transcript}

---
Based on this customer service call transcript, provide:
1. **Call Summary** – key topics and resolution in 2–3 sentences
2. **Customer Sentiment** – emotional tone and satisfaction signals
3. **Issue Classification** – type of customer issue raised
4. **Agent Performance** – what the agent handled well and what could improve
5. **Escalation Risk** – likelihood this customer will churn or escalate
6. **Recommended Follow-up Actions**
"""

SURVEY_TEMPLATE = """\
The following is an analysis of customer survey responses:

{survey_summary}

---
Based on this survey data, provide:
1. **Overall Customer Satisfaction** – overall sentiment and NPS signals
2. **Key Findings** – 3–5 most important insights from the data
3. **Segments of Concern** – responses that indicate dissatisfaction
4. **Opportunities** – areas where the business could improve
5. **Recommended Next Steps** – concrete actions prioritized by impact
"""

MULTIMODAL_TEMPLATE_TEXT = """\
You are analyzing a product image combined with customer feedback text.

Customer feedback text:
{text_content}

Please analyze both the visual product information and the customer feedback together.
Provide:
1. **Product-Feedback Alignment** – does the product match customer expectations?
2. **Visual Quality Assessment** – image clarity, label accuracy
3. **Customer Pain Points** linked to what is visible in the product
4. **Recommendations** for product improvement based on both signals
"""


def _build_text_reviews_prompt(event: dict) -> list[dict]:
    """Build a Nova messages list for the text_reviews data type."""
    comprehend = event.get("comprehend_results", {})
    sentiment = comprehend.get("sentiment", {})
    entities = comprehend.get("entities", {})
    key_phrases = comprehend.get("key_phrases", [])

    dominant = sentiment.get("dominant_sentiment", "UNKNOWN")
    counts = sentiment.get("counts", {})
    counts_str = ", ".join(f"{k}: {v}" for k, v in counts.items())

    # Format entities
    ents_by_type = entities.get("top_entities_by_type", {})
    ents_lines = []
    for etype, top_list in list(ents_by_type.items())[:5]:
        top_vals = ", ".join(f'"{t}"' for t, _ in top_list[:5])
        ents_lines.append(f"  - {etype}: {top_vals}")
    entities_summary = "\n".join(ents_lines) or "  (no entities detected)"

    phrases_summary = ", ".join(f'"{p}"' for p, _ in key_phrases[:15]) or "(none)"

    sample = event.get("normalized_text", "")[:3_000]

    prompt_text = REVIEW_TEMPLATE.format(
        product_context=f"key={event.get('key', 'N/A')}",
        dominant_sentiment=dominant,
        sentiment_counts=counts_str,
        entities_summary=entities_summary,
        key_phrases_summary=phrases_summary,
        sample_text=sample,
    )
    return [{"role": "user", "content": [{"text": prompt_text}]}]


def _build_image_prompt(event: dict) -> list[dict]:
    """Build prompt for image path. Optionally includes the actual image bytes (multimodal)."""
    extracted = event.get("extracted_text", "") or event.get("normalized_text", "")
    bucket = event.get("bucket", "")
    key = event.get("key", "")

    content: list[dict] = []

    # Attempt to include the actual image for multimodal analysis
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        image_bytes = obj["Body"].read()
        ext = key.rsplit(".", 1)[-1].lower() if "." in key else "jpeg"
        format_map = {"jpg": "jpeg", "jpeg": "jpeg", "png": "png", "gif": "gif", "webp": "webp"}
        img_format = format_map.get(ext, "jpeg")

        if len(image_bytes) <= 5 * 1024 * 1024:  # Nova image limit
            content.append({
                "image": {
                    "format": img_format,
                    "source": {
                        "bytes": base64.b64encode(image_bytes).decode("utf-8"),
                    },
                },
            })
            content.append({
                "text": MULTIMODAL_TEMPLATE_TEXT.format(text_content=extracted[:5_000]),
            })
            logger.info("Multimodal request: image (%d bytes) + text", len(image_bytes))
        else:
            raise ValueError("Image too large for multimodal")

    except Exception:
        logger.info("Falling back to text-only image analysis")
        content.append({
            "text": IMAGE_TEMPLATE.format(extracted_text=extracted[:5_000]),
        })

    return [{"role": "user", "content": content}]


def _build_audio_prompt(event: dict) -> list[dict]:
    transcript = event.get("transcript", "") or event.get("normalized_text", "")
    return [{
        "role": "user",
        "content": [{"text": AUDIO_TEMPLATE.format(transcript=transcript[:5_000])}],
    }]


def _build_survey_prompt(event: dict) -> list[dict]:
    summary = event.get("survey_summary", "") or event.get("normalized_text", "")
    return [{
        "role": "user",
        "content": [{"text": SURVEY_TEMPLATE.format(survey_summary=summary[:5_000])}],
    }]


# Conversation template: follow-up dialog for interactive analysis
FOLLOWUP_TEMPLATE = [
    {
        "role": "user",
        "content": [{"text": "What is the single most important action the business should take based on this feedback? Be specific and concise."}],
    }
]


def _invoke_bedrock(messages: list[dict]) -> tuple[str, int]:
    """Invoke Amazon Nova Lite via Bedrock. Returns (response_text, input_token_count)."""
    body = {
        "schemaVersion": "messages-v1",
        "messages": messages,
        "system": [{"text": SYSTEM_PROMPT}],
        "inferenceConfig": {"maxTokens": MAX_TOKENS},
    }
    response = bedrock_runtime.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps(body),
    )
    response_body = json.loads(response["body"].read())
    content = response_body.get("output", {}).get("message", {}).get("content", [])
    text = "\n".join(block.get("text", "") for block in content)
    usage = response_body.get("usage", {})
    input_tokens = usage.get("inputTokens", 0)
    return text, input_tokens


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    data_type: str = event.get("data_type", "text_reviews")
    logger.info("bedrock_formatter invoked for data_type=%s", data_type)

    # ── Build messages for the data type ──────────────────────────────────────
    if data_type == "text_reviews":
        messages = _build_text_reviews_prompt(event)
    elif data_type == "images":
        messages = _build_image_prompt(event)
    elif data_type == "audio":
        messages = _build_audio_prompt(event)
    elif data_type == "surveys":
        messages = _build_survey_prompt(event)
    else:
        messages = [{
            "role": "user",
            "content": [{"text": f"Analyze this customer feedback:\n\n{event.get('normalized_text', '')[:5_000]}"}],
        }]

    # ── Primary analysis ───────────────────────────────────────────────────────
    try:
        insights_text, input_tokens = _invoke_bedrock(messages)
    except Exception:
        logger.error("Bedrock invocation failed", exc_info=True)
        return {
            **event,
            "bedrock_response": {"error": "bedrock_invocation_failed"},
        }

    # ── Follow-up: single top action ──────────────────────────────────────────
    top_action = ""
    try:
        dialog = messages + [
            {"role": "assistant", "content": [{"text": insights_text}]},
            *FOLLOWUP_TEMPLATE,
        ]
        top_action, _ = _invoke_bedrock(dialog)
    except Exception:
        logger.warning("Follow-up Bedrock call failed", exc_info=True)

    # ── Write output to S3 ─────────────────────────────────────────────────────
    timestamp = datetime.now(timezone.utc).strftime("%Y/%m/%d")
    output_key = (
        f"insights/{data_type}/{timestamp}/"
        f"{event.get('key', 'unknown').rsplit('/', 1)[-1]}.insights.json"
    )
    output_payload = {
        "source_bucket": event.get("bucket"),
        "source_key": event.get("key"),
        "data_type": data_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "model_id": BEDROCK_MODEL_ID,
        "insights": insights_text,
        "top_priority_action": top_action,
        "comprehend_results": event.get("comprehend_results", {}),
        "validation": event.get("validation", {}),
    }
    try:
        s3.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=output_key,
            Body=json.dumps(output_payload, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Insights written to s3://%s/%s", OUTPUT_BUCKET, output_key)
    except Exception:
        logger.warning("Failed to write insights to S3", exc_info=True)

    # ── Publish metrics ────────────────────────────────────────────────────────
    try:
        cloudwatch.put_metric_data(
            Namespace=METRICS_NAMESPACE,
            MetricData=[
                {
                    "MetricName": "BedrockInputTokens",
                    "Value": float(input_tokens),
                    "Unit": "Count",
                    "Dimensions": [{"Name": "DataType", "Value": data_type}],
                },
                {
                    "MetricName": "InsightsGenerated",
                    "Value": 1.0,
                    "Unit": "Count",
                    "Dimensions": [{"Name": "DataType", "Value": data_type}],
                },
            ],
        )
    except Exception:
        logger.warning("Failed to publish Bedrock metrics", exc_info=True)

    logger.info(
        "Bedrock analysis complete: %d chars of insights, output_key=%s",
        len(insights_text),
        output_key,
    )

    return {
        **event,
        "output_key": output_key,
        "bedrock_response": {
            "insights": insights_text,
            "top_priority_action": top_action,
            "model_id": BEDROCK_MODEL_ID,
            "output_key": output_key,
            "input_tokens": input_tokens,
        },
    }
