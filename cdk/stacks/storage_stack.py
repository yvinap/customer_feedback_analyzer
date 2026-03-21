"""
StorageStack — S3 Buckets + AWS Glue Data Catalog Database

Resources created:
  - S3 input bucket   (raw feedback: text-reviews/, images/, audio/, surveys/)
  - S3 processed bucket (intermediate ETL output)
  - S3 output bucket  (final insights JSON)
  - Glue Data Catalog database
"""
from __future__ import annotations

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    RemovalPolicy,
    CfnOutput,
    Duration,
    aws_s3 as s3,
    aws_glue as glue,
)
from constructs import Construct


class StorageStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        prefix: str = "cfa",
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._prefix = prefix

        # ── Input bucket ───────────────────────────────────────────────────────
        self.input_bucket = s3.Bucket(
            self,
            "InputBucket",
            bucket_name=f"{prefix}-customer-feedback-input-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            event_bridge_enabled=True,   # enables EventBridge for S3 events
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ArchiveRawDataAfter90Days",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=Duration.days(90),
                        )
                    ],
                )
            ],
        )

        # ── Processed bucket ───────────────────────────────────────────────────
        self.processed_bucket = s3.Bucket(
            self,
            "ProcessedBucket",
            bucket_name=f"{prefix}-customer-feedback-processed-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ExpireProcessedAfter365Days",
                    expiration=Duration.days(365),
                )
            ],
        )

        # ── Output bucket ──────────────────────────────────────────────────────
        self.output_bucket = s3.Bucket(
            self,
            "OutputBucket",
            bucket_name=f"{prefix}-customer-feedback-output-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ── Glue Data Catalog database ─────────────────────────────────────────
        self._glue_db_name = f"{prefix}_customer_feedback_db"
        self._glue_database = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=self._glue_db_name,
                description="AWS Glue Data Catalog database for customer feedback pipeline",
            ),
        )

        # ── CloudFormation Outputs ─────────────────────────────────────────────
        CfnOutput(
            self, "InputBucketName",
            value=self.input_bucket.bucket_name,
            export_name=f"{prefix}-InputBucketName",
        )
        CfnOutput(
            self, "ProcessedBucketName",
            value=self.processed_bucket.bucket_name,
            export_name=f"{prefix}-ProcessedBucketName",
        )
        CfnOutput(
            self, "OutputBucketName",
            value=self.output_bucket.bucket_name,
            export_name=f"{prefix}-OutputBucketName",
        )
        CfnOutput(
            self, "GlueDatabaseName",
            value=self._glue_db_name,
            export_name=f"{prefix}-GlueDatabaseName",
        )

    @property
    def glue_database_name(self) -> str:
        return self._glue_db_name
