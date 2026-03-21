#!/usr/bin/env python3
"""
Customer Feedback Analyzer — CDK Application Entry Point

Deploys three stacks in dependency order:
  1. CfaStorageStack         — S3 buckets + Glue Data Catalog database
  2. CfaDataValidationStack  — Glue Data Quality, text-validator Lambda, CloudWatch
  3. CfaDataProcessingStack  — Processing Lambdas + Step Functions state machine
"""

import aws_cdk as cdk

from stacks.storage_stack import StorageStack
from stacks.data_validation_stack import DataValidationStack
from stacks.data_processing_stack import DataProcessingStack

app = cdk.App()

prefix = app.node.try_get_context("prefix") or "cfa"

env = cdk.Environment(
    account=app.node.try_get_context("account"),   # or use CDK_DEFAULT_ACCOUNT
    region=app.node.try_get_context("region"),     # or use CDK_DEFAULT_REGION
)

# ── Stack 1: Storage (S3 + Glue DB) ───────────────────────────────────────────
storage_stack = StorageStack(
    app,
    f"CfaStorageStack",
    prefix=prefix,
    description="Customer Feedback Analyzer — S3 Buckets and Glue Data Catalog",
    # env=env,  # uncomment to pin account/region
)

# ── Stack 2: Data Validation ───────────────────────────────────────────────────
validation_stack = DataValidationStack(
    app,
    f"CfaDataValidationStack",
    prefix=prefix,
    input_bucket=storage_stack.input_bucket,
    processed_bucket=storage_stack.processed_bucket,
    glue_database_name=storage_stack.glue_database_name,
    description="Customer Feedback Analyzer — Glue Data Quality + Validation Lambdas + CloudWatch",
    # env=env,
)
validation_stack.add_dependency(storage_stack)

# ── Stack 3: Data Processing ───────────────────────────────────────────────────
processing_stack = DataProcessingStack(
    app,
    f"CfaDataProcessingStack",
    prefix=prefix,
    input_bucket=storage_stack.input_bucket,
    processed_bucket=storage_stack.processed_bucket,
    output_bucket=storage_stack.output_bucket,
    description="Customer Feedback Analyzer — Processing Lambdas + Step Functions Pipeline",
    # env=env,
)
processing_stack.add_dependency(storage_stack)
# DataProcessingStack references the text_validator Lambda (by ARN) from the
# validation stack, so the validation stack must be deployed first.
processing_stack.add_dependency(validation_stack)

app.synth()
