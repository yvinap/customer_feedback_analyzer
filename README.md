# Customer Feedback Analyzer

A comprehensive, end-to-end **multimodal data validation and processing pipeline** that prepares
customer feedback from multiple sources (text reviews, product images, call recordings, and survey
responses) for analysis by foundation models (Claude on Amazon Bedrock) to generate actionable
business insights.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Project Structure](#project-structure)
3. [Prerequisites](#prerequisites)
4. [Environment Setup](#environment-setup)
5. [Deployment](#deployment)
6. [Usage](#usage)
7. [Data Sources](#data-sources)
8. [Pipeline Walkthrough](#pipeline-walkthrough)
9. [CDK Stacks Reference](#cdk-stacks-reference)
10. [Lambda Functions Reference](#lambda-functions-reference)
11. [Configuration](#configuration)
12. [Monitoring & Data Quality](#monitoring--data-quality)
13. [Cleanup](#cleanup)

---

## Architecture Overview

```
                        ┌─────────────────────────────────────────────────────┐
                        │                   INPUT S3 BUCKET                   │
                        │  text-reviews/  │  images/  │  audio/  │  surveys/ │
                        └──────────────────────┬──────────────────────────────┘
                                               │ S3 EventBridge notification
                                               ▼
                                 ┌─────────────────────────┐
                                 │  Pipeline Orchestrator   │
                                 │        (Lambda)          │
                                 └────────────┬────────────┘
                                              │ Start Execution
                                              ▼
                                 ┌─────────────────────────┐
                                 │   Step Functions        │
                                 │   State Machine         │
                                 └────────────┬────────────┘
                                              │
                    ┌─────────────────────────┼──────────────────────────────┐
                    ▼                         ▼                              ▼
          ┌──────────────────┐     ┌──────────────────┐          ┌──────────────────┐
          │  Text Reviews    │     │    Images         │          │   Audio/Survey   │
          │  ─────────────   │     │  ─────────────    │          │  ─────────────   │
          │  text_validator  │     │  textract_proc    │          │  transcribe_proc │
          │  text_normalizer │     │  (Amazon Textract)│          │  survey_summ.    │
          │  comprehend_proc │     └────────┬─────────┘          └───────┬──────────┘
          └────────┬─────────┘             │                             │
                   └─────────────────────►─┴─◄──────────────────────────┘
                                           │
                                           ▼
                              ┌────────────────────────┐
                              │   Bedrock Formatter    │   ← Formats data for Claude
                              │       (Lambda)         │
                              └────────────┬───────────┘
                                           │
                                           ▼
                              ┌────────────────────────┐
                              │  Amazon Bedrock         │   ← Claude generates insights
                              │  (Claude Sonnet)        │
                              └────────────┬───────────┘
                                           │
                              ┌────────────┴───────────┐
                              ▼                        ▼
                  ┌───────────────────┐  ┌───────────────────────┐
                  │ Feedback Quality  │  │  OUTPUT S3 BUCKET     │
                  │   Loop (Lambda)   │  │  (Insights + Reports) │
                  └────────┬──────────┘  └───────────────────────┘
                           │
                           ▼
                  ┌───────────────────┐
                  │  CloudWatch       │
                  │  Metrics/Dashboard│
                  └───────────────────┘
```

**Data Validation** is applied before processing:

- **AWS Glue Data Quality** validates structured CSV data with DQDL rules
- **Custom Lambda** (`text_validator`) validates unstructured text reviews
- **CloudWatch** tracks quality scores, validation failures, and pipeline metrics over time

---

## Project Structure

```
customer_feedback_analyzer/
├── .gitignore
├── README.md
├── requirements.txt                  # Python dependencies (installed into .venv)
├── setupenv.sh                       # One-shot environment + CDK bootstrap script
│
├── sample_data/
│   └── amazon.csv                    # Sample Amazon product reviews dataset
│
├── cdk/                              # AWS CDK infrastructure (Python)
│   ├── app.py                        # CDK App entry point
│   ├── cdk.json                      # CDK configuration
│   ├── requirements.txt              # CDK-specific pip deps
│   └── stacks/
│       ├── __init__.py
│       ├── storage_stack.py          # S3 buckets + Glue Data Catalog DB
│       ├── data_validation_stack.py  # Glue DQ, text_validator λ, CloudWatch
│       └── data_processing_stack.py  # All processing λ + Step Functions
│
├── lambda/                           # Lambda function source code
│   ├── pipeline_orchestrator/        # S3 event → Step Functions trigger
│   ├── text_validator/               # Unstructured review validation
│   ├── text_normalizer/              # Text cleaning and normalization
│   ├── comprehend_processor/         # Sentiment + entity extraction
│   ├── textract_processor/           # Image → text extraction
│   ├── transcribe_processor/         # Audio → transcript
│   ├── survey_summarizer/            # CSV survey → NL summary
│   ├── bedrock_formatter/            # Format data + invoke Claude
│   ├── feedback_quality_loop/        # Use model output to improve DQ
│   └── cloudwatch_metrics/           # Publish custom CW metrics
│
├── glue/
│   ├── data_quality_rules.py         # DQDL ruleset helper and definitions
│   └── glue_job_script.py            # Glue ETL job (Spark) for CSV processing
│
└── scripts/
    ├── upload_sample_data.py         # Upload sample_data/ to S3 input bucket
    └── trigger_pipeline.py           # Manually trigger pipeline execution
```

---

## Prerequisites

| Tool | Version | Install |
|---|---|---|
| Python | 3.11 | `brew install python@3.11` |
| Node.js | ≥ 18 | `brew install node` |
| AWS CDK CLI | ≥ 2.110 | `npm install -g aws-cdk` |
| AWS CLI | ≥ 2 | `brew install awscli` |
| AWS Profile | `aws_swami` | `aws configure --profile aws_swami` |

Your `aws_swami` IAM role/user needs permissions for:
S3, Glue, Lambda, Step Functions, Comprehend, Textract, Transcribe, Bedrock, CloudWatch,
IAM (to create roles), EventBridge.

---

## Environment Setup

```bash
# Clone / enter the project
cd customer_feedback_analyzer

# Run the setup script (creates .venv, installs all deps, validates credentials)
bash setupenv.sh

# Activate the virtual environment
source .venv/bin/activate

# Optional: also bootstrap CDK for your AWS account/region
bash setupenv.sh --cdk
```

---

## Deployment

All infrastructure is deployed via AWS CDK from the `cdk/` directory.

```bash
# Activate venv first
source .venv/bin/activate

cd cdk

# Optional: see what will be deployed
AWS_PROFILE=aws_swami cdk diff --all

# Deploy all stacks (StorageStack → DataValidationStack → DataProcessingStack)
AWS_PROFILE=aws_swami cdk deploy --all --require-approval never

# Or deploy individual stacks
AWS_PROFILE=aws_swami cdk deploy CfaStorageStack
AWS_PROFILE=aws_swami cdk deploy CfaDataValidationStack
AWS_PROFILE=aws_swami cdk deploy CfaDataProcessingStack
```

After deployment, CDK outputs the S3 bucket names and the Step Functions ARN.

---

## Usage

### 1. Upload Sample Data

```bash
# Activate venv
source .venv/bin/activate

# Upload the amazon.csv sample to the text-reviews/ prefix
python scripts/upload_sample_data.py

# Upload to a custom bucket
python scripts/upload_sample_data.py --bucket my-bucket-name
```

### 2. Trigger the Pipeline

```bash
# Text reviews (uses sample CSV)
python scripts/trigger_pipeline.py --data-type text_reviews

# Images (provide an S3 key)
python scripts/trigger_pipeline.py --data-type images --key images/product.jpg

# Audio
python scripts/trigger_pipeline.py --data-type audio --key audio/call_001.mp3

# Survey CSV
python scripts/trigger_pipeline.py --data-type surveys --key surveys/q4_survey.csv
```

### 3. Monitor Execution

```bash
# View Step Functions executions
AWS_PROFILE=aws_swami aws stepfunctions list-executions \
    --state-machine-arn $(AWS_PROFILE=aws_swami aws cloudformation describe-stacks \
    --stack-name CfaDataProcessingStack \
    --query "Stacks[0].Outputs[?OutputKey=='StateMachineArn'].OutputValue" \
    --output text)

# Check CloudWatch Dashboard
# → AWS Console → CloudWatch → Dashboards → CFA-DataQualityDashboard
```

### 4. View Insights

Pipeline outputs are written to the **output S3 bucket** under:

```
s3://<output-bucket>/insights/<data_type>/<date>/<execution_id>/insights.json
```

---

## Data Sources

### Text Reviews (`text-reviews/`)
CSV files with columns matching the Amazon dataset schema:
`product_id, product_name, category, rating, review_title, review_content, ...`

### Product Images (`images/`)
JPEG/PNG product images. Textract extracts any embedded text (labels, descriptions, etc.).

### Call Recordings (`audio/`)
MP3/WAV/FLAC audio files of customer service calls. Amazon Transcribe converts them to text.

### Survey Responses (`surveys/`)
CSV files with arbitrary question/answer columns. Converted to natural-language summaries.

---

## Pipeline Walkthrough

### Text Reviews Flow

```
CSV uploaded to s3://input/text-reviews/
    → pipeline_orchestrator fires
    → Step Functions starts
    → text_validator:   checks length, encoding, vocabulary diversity
    → text_normalizer:  strips HTML, normalises unicode, removes URLs
    → comprehend_processor: batch sentiment + entity + key-phrase extraction
    → bedrock_formatter:    formats as Claude message + invokes Bedrock
    → output written to s3://output/insights/
    → feedback_quality_loop: quality score updated from model output
    → cloudwatch_metrics:   publishes DataQualityScore, EntitiesFound, SentimentDistribution
```

### Glue Data Quality

A DQDL ruleset is applied to every structured CSV via **AWS Glue Data Quality**:

```
Rules = [
    IsComplete "review_content",
    IsComplete "rating",
    ColumnValues "rating" between 1 and 5,
    ColumnLength "review_content" > 10,
    Uniqueness "review_id" > 0.85,
    IsComplete "product_id"
]
```

Glue DQ evaluation results are stored in the Glue Data Catalog and streamed to CloudWatch.

---

## CDK Stacks Reference

| Stack | Description |
|---|---|
| `CfaStorageStack` | S3 input/processed/output buckets, Glue Data Catalog database |
| `CfaDataValidationStack` | Glue Crawler, Glue DQ Ruleset, `text_validator` + `cloudwatch_metrics` Lambdas, CloudWatch Dashboard & Alarms |
| `CfaDataProcessingStack` | All 8 processing Lambdas, Step Functions state machine, EventBridge S3 trigger, IAM roles |

---

## Lambda Functions Reference

| Function | Trigger | Description |
|---|---|---|
| `pipeline_orchestrator` | S3 EventBridge | Routes S3 uploads to the correct Step Functions path |
| `text_validator` | Step Functions | Validates text reviews (length, encoding, content quality) |
| `text_normalizer` | Step Functions | Strips HTML, normalises unicode, removes URLs/boilerplate |
| `comprehend_processor` | Step Functions | Batch sentiment analysis + entity/key-phrase extraction |
| `textract_processor` | Step Functions | Async document text detection via Amazon Textract |
| `transcribe_processor` | Step Functions | Starts + polls Amazon Transcribe transcription job |
| `survey_summarizer` | Step Functions | Converts CSV survey rows to natural-language summaries |
| `bedrock_formatter` | Step Functions | Formats payload + invokes Claude via Amazon Bedrock |
| `feedback_quality_loop` | Step Functions | Derives quality signals from Claude output; updates scores |
| `cloudwatch_metrics` | Step Functions | Publishes custom CloudWatch metrics for each execution |

---

## Configuration

CDK context values (set in `cdk/cdk.json` or with `--context`):

| Key | Default | Description |
|---|---|---|
| `prefix` | `cfa` | Resource name prefix |
| `bedrock_model_id` | `anthropic.claude-3-5-sonnet-20241022` | Bedrock model to use |
| `comprehend_language` | `en` | Language code for Comprehend |
| `log_level` | `INFO` | Lambda log level |

Override at deploy time:

```bash
cdk deploy --all --context prefix=prod --context log_level=DEBUG
```

---

## Monitoring & Data Quality

- **CloudWatch Dashboard**: `CFA-DataQualityDashboard`
  - Pipeline executions started/succeeded/failed
  - Average data quality score per data type
  - Sentiment distribution over time
  - Entity extraction counts
  - Glue DQ pass/fail rates

- **CloudWatch Alarms**:
  - `CFA-LowDataQualityScore` — fires when avg score < 70 for 3 consecutive periods
  - `CFA-PipelineFailures` — fires when ≥ 3 executions fail in 5 minutes

---

## Cleanup

```bash
cd cdk

# Destroy all stacks (S3 buckets have RemovalPolicy.RETAIN — delete manually if desired)
AWS_PROFILE=aws_swami cdk destroy --all

# Manually empty and delete S3 buckets if needed
aws s3 rb s3://<bucket-name> --force --profile aws_swami
```
