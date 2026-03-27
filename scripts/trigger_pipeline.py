"""
scripts/trigger_pipeline.py

Manually triggers a Step Functions pipeline execution for testing and development.

Supports all four data types:
  - text_reviews : uploads a CSV and fires immediately from an existing S3 key
  - images       : fires for an existing image in S3
  - audio        : fires for an existing audio file in S3
  - surveys      : fires for an existing unstructured survey text file in S3

Usage examples:
    # Default: trigger text_reviews for the sample CSV already uploaded
    python scripts/trigger_pipeline.py --data-type text_reviews

    # Trigger for a specific S3 key
    python scripts/trigger_pipeline.py --data-type images --key images/product.jpg

    # Use a specific Step Functions ARN
    python scripts/trigger_pipeline.py --data-type audio --key audio/call_001.mp3 \\
        --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:cfa-feedback-pipeline

    # Watch execution until it finishes
    python scripts/trigger_pipeline.py --data-type surveys --watch
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import BotoCoreError, ClientError

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_KEYS: dict[str, str] = {
    "text_reviews": "text-reviews/amazon.csv",
    "images": "images/sample_product.jpg",
    "audio": "audio/sample_call.mp3",
    "surveys": "surveys/sample_survey.txt",
}


def _resolve_state_machine_arn(prefix: str, profile: str) -> str:
    """Look up the Step Functions ARN from CloudFormation outputs."""
    session = boto3.Session(profile_name=profile)

    # Try CloudFormation first
    try:
        cf = session.client("cloudformation")
        resp = cf.describe_stacks(StackName="CfaDataProcessingStack")
        for output in resp["Stacks"][0].get("Outputs", []):
            if output["OutputKey"] == "StateMachineArn":
                return output["OutputValue"]
    except Exception:
        logger.debug("CloudFormation lookup failed", exc_info=True)

    # Try direct Step Functions list
    try:
        sfn = session.client("stepfunctions")
        resp = sfn.list_state_machines()
        for sm in resp.get("stateMachines", []):
            if sm["name"] == f"{prefix}-feedback-pipeline":
                return sm["stateMachineArn"]
    except Exception:
        logger.debug("Step Functions list failed", exc_info=True)

    raise RuntimeError(
        "Could not resolve the State Machine ARN. "
        "Deploy the stack first or pass --state-machine-arn explicitly."
    )


def _resolve_input_bucket(prefix: str, profile: str) -> str:
    """Look up the input bucket name from CloudFormation outputs."""
    session = boto3.Session(profile_name=profile)
    try:
        cf = session.client("cloudformation")
        resp = cf.describe_stacks(StackName="CfaStorageStack")
        for output in resp["Stacks"][0].get("Outputs", []):
            if output["OutputKey"] == "InputBucketName":
                return output["OutputValue"]
    except Exception:
        pass
    sts = session.client("sts")
    account = sts.get_caller_identity()["Account"]
    return f"{prefix}-customer-feedback-input-{account}"


def trigger_execution(
    state_machine_arn: str,
    input_bucket: str,
    key: str,
    data_type: str,
    profile: str,
) -> str:
    session = boto3.Session(profile_name=profile)
    sfn = session.client("stepfunctions")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    execution_name = f"manual-{data_type}-{timestamp}"[:80]

    payload = {
        "bucket": input_bucket,
        "key": key,
        "data_type": data_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "processed_results": {},
    }

    logger.info("Starting execution: %s", execution_name)
    logger.info("  State machine : %s", state_machine_arn)
    logger.info("  Input         : s3://%s/%s  (data_type=%s)", input_bucket, key, data_type)

    resp = sfn.start_execution(
        stateMachineArn=state_machine_arn,
        name=execution_name,
        input=json.dumps(payload),
    )

    execution_arn = resp["executionArn"]
    logger.info("  Execution ARN : %s", execution_arn)
    return execution_arn


def watch_execution(execution_arn: str, profile: str, poll_interval: int = 5) -> str:
    """Poll the execution until it reaches a terminal state."""
    session = boto3.Session(profile_name=profile)
    sfn = session.client("stepfunctions")

    logger.info("Watching execution (Ctrl+C to stop watching)...")
    while True:
        try:
            resp = sfn.describe_execution(executionArn=execution_arn)
            status = resp["status"]
            if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
                logger.info("Execution finished with status: %s", status)
                if status == "FAILED":
                    logger.error("Failure cause: %s", resp.get("cause", "N/A"))
                return status
            logger.info("  Status: %s — waiting %ds...", status, poll_interval)
            time.sleep(poll_interval)
        except KeyboardInterrupt:
            logger.info("Stopped watching (execution still running).")
            return "WATCHING_CANCELLED"
        except (BotoCoreError, ClientError) as exc:
            logger.error("Error polling execution: %s", exc)
            return "ERROR"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manually trigger the Customer Feedback Analyzer pipeline"
    )
    parser.add_argument(
        "--data-type",
        choices=["text_reviews", "images", "audio", "surveys"],
        default="text_reviews",
    )
    parser.add_argument("--key", help="S3 object key (auto-selected if omitted)")
    parser.add_argument("--bucket", help="S3 bucket name (auto-detected if omitted)")
    parser.add_argument("--state-machine-arn", help="Step Functions ARN (auto-detected if omitted)")
    parser.add_argument("--prefix", default="cfa")
    parser.add_argument("--profile", default="aws_swami")
    parser.add_argument("--watch", action="store_true", help="Poll until execution completes")
    parser.add_argument("--poll-interval", type=int, default=5, help="Polling interval in seconds")
    args = parser.parse_args()

    # Resolve resources
    try:
        sm_arn = args.state_machine_arn or _resolve_state_machine_arn(args.prefix, args.profile)
        input_bucket = args.bucket or _resolve_input_bucket(args.prefix, args.profile)
    except RuntimeError as exc:
        logger.error(str(exc))
        sys.exit(1)

    key = args.key or DEFAULT_KEYS[args.data_type]

    try:
        execution_arn = trigger_execution(
            state_machine_arn=sm_arn,
            input_bucket=input_bucket,
            key=key,
            data_type=args.data_type,
            profile=args.profile,
        )
    except (BotoCoreError, ClientError) as exc:
        logger.error("Failed to start execution: %s", exc)
        sys.exit(1)

    print(f"\nExecution ARN:\n  {execution_arn}\n")

    if args.watch:
        final_status = watch_execution(execution_arn, args.profile, args.poll_interval)
        sys.exit(0 if final_status == "SUCCEEDED" else 1)


if __name__ == "__main__":
    main()
