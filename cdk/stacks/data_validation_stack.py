"""
DataValidationStack — Glue Data Quality + Validation Lambda + CloudWatch

Resources created:
  - AWS Glue Crawler (discovers schema of CSV files in input bucket)
  - AWS Glue Data Quality Ruleset (DQDL rules for structured feedback CSVs)
  - IAM role for Glue
  - Lambda: text_validator  (validates unstructured text reviews)
  - Lambda: cloudwatch_metrics (publishes custom CW metrics)
  - CloudWatch Dashboard
  - CloudWatch Alarms (low quality score, pipeline failures)
"""
from __future__ import annotations

import os
from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    aws_glue as glue,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_cloudwatch as cloudwatch,
    aws_s3 as s3,
)
from constructs import Construct

# Resolve the lambda/ directory relative to this stacks/ folder
LAMBDA_ROOT = Path(__file__).parent.parent.parent / "lambda"


class DataValidationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        prefix: str,
        input_bucket: s3.Bucket,
        processed_bucket: s3.Bucket,
        glue_database_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self._prefix = prefix
        log_level = self.node.try_get_context("log_level") or "INFO"

        # ── IAM role for Glue ──────────────────────────────────────────────────
        glue_role = iam.Role(
            self,
            "GlueServiceRole",
            role_name=f"{prefix}-glue-service-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                )
            ],
        )
        input_bucket.grant_read(glue_role)
        processed_bucket.grant_read_write(glue_role)

        # ── Glue Crawler ───────────────────────────────────────────────────────
        # Crawls the text-reviews/ prefix to auto-discover CSV schema
        glue_crawler = glue.CfnCrawler(
            self,
            "FeedbackCrawler",
            name=f"{prefix}-feedback-crawler",
            role=glue_role.role_arn,
            database_name=glue_database_name,
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{input_bucket.bucket_name}/text-reviews/"
                    ),
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{input_bucket.bucket_name}/surveys/"
                    ),
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="LOG",
                delete_behavior="LOG",
            ),
            recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY"
            ),
        )

        # ── Pre-seed Glue Table (text_reviews) ────────────────────────────────
        # CfnDataQualityRuleset requires the target table to exist in the Data
        # Catalog at deploy time. We create it here with the known CSV schema;
        # the crawler will update the definition when it executes.
        text_reviews_table = glue.CfnTable(
            self,
            "TextReviewsTable",
            catalog_id=self.account,
            database_name=glue_database_name,
            table_input=glue.CfnTable.TableInputProperty(
                name="text_reviews",
                description="Amazon product reviews – pre-seeded schema; crawler updates on first run",
                table_type="EXTERNAL_TABLE",
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    location=f"s3://{input_bucket.bucket_name}/text-reviews/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        parameters={"field.delim": ","},
                    ),
                    columns=[
                        glue.CfnTable.ColumnProperty(name="product_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="product_name", type="string"),
                        glue.CfnTable.ColumnProperty(name="category", type="string"),
                        glue.CfnTable.ColumnProperty(name="discounted_price", type="string"),
                        glue.CfnTable.ColumnProperty(name="actual_price", type="string"),
                        glue.CfnTable.ColumnProperty(name="discount_percentage", type="string"),
                        glue.CfnTable.ColumnProperty(name="rating", type="double"),
                        glue.CfnTable.ColumnProperty(name="rating_count", type="string"),
                        glue.CfnTable.ColumnProperty(name="about_product", type="string"),
                        glue.CfnTable.ColumnProperty(name="user_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="user_name", type="string"),
                        glue.CfnTable.ColumnProperty(name="review_id", type="string"),
                        glue.CfnTable.ColumnProperty(name="review_title", type="string"),
                        glue.CfnTable.ColumnProperty(name="review_content", type="string"),
                        glue.CfnTable.ColumnProperty(name="img_link", type="string"),
                        glue.CfnTable.ColumnProperty(name="product_link", type="string"),
                    ],
                ),
                parameters={"classification": "csv", "skip.header.line.count": "1"},
            ),
        )

        # ── Glue Data Quality Ruleset ──────────────────────────────────────────
        # DQDL rules enforce structural quality on structured CSV feedback data
        dqdl_ruleset = "\n".join([
            "Rules = [",
            "    IsComplete \"review_content\",",
            "    IsComplete \"rating\",",
            "    IsComplete \"product_id\",",
            "    IsComplete \"user_id\",",
            "    ColumnValues \"rating\" between 1.0 and 5.0,",
            "    ColumnLength \"review_content\" > 10,",
            "    Uniqueness \"review_id\" > 0.85,",
            "    IsComplete \"review_title\",",
            "    Completeness \"user_name\" > 0.8",
            "]",
        ])

        glue_dq_ruleset = glue.CfnDataQualityRuleset(
            self,
            "FeedbackDQRuleset",
            name=f"{prefix}-feedback-dq-ruleset",
            ruleset=dqdl_ruleset,
            target_table=glue.CfnDataQualityRuleset.DataQualityTargetTableProperty(
                database_name=glue_database_name,
                table_name="text_reviews",
            ),
            description="Data quality rules for structured customer feedback CSV files",
        )
        glue_dq_ruleset.add_dependency(text_reviews_table)

        # ── Lambda: text_validator ─────────────────────────────────────────────
        validator_role = iam.Role(
            self,
            "TextValidatorRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        input_bucket.grant_read(validator_role)
        processed_bucket.grant_read_write(validator_role)
        validator_role.add_to_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )

        self.text_validator_fn = lambda_.Function(
            self,
            "TextValidatorFn",
            function_name=f"{prefix}-text-validator",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(LAMBDA_ROOT / "text_validator")),
            role=validator_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "PROCESSED_BUCKET": processed_bucket.bucket_name,
                "LOG_LEVEL": log_level,
                "METRICS_NAMESPACE": "CustomerFeedbackAnalyzer",
            },
        )

        # ── Lambda: cloudwatch_metrics ─────────────────────────────────────────
        cw_metrics_role = iam.Role(
            self,
            "CloudWatchMetricsRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        cw_metrics_role.add_to_policy(
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            )
        )

        self.cloudwatch_metrics_fn = lambda_.Function(
            self,
            "CloudWatchMetricsFn",
            function_name=f"{prefix}-cloudwatch-metrics",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(LAMBDA_ROOT / "cloudwatch_metrics")),
            role=cw_metrics_role,
            timeout=Duration.minutes(2),
            memory_size=256,
            environment={
                "METRICS_NAMESPACE": "CustomerFeedbackAnalyzer",
                "LOG_LEVEL": log_level,
            },
        )

        # ── CloudWatch Dashboard ───────────────────────────────────────────────
        dashboard = cloudwatch.Dashboard(
            self,
            "DataQualityDashboard",
            dashboard_name=f"CFA-DataQualityDashboard",
        )

        pipeline_starts = cloudwatch.Metric(
            namespace="CustomerFeedbackAnalyzer",
            metric_name="PipelineExecutionsStarted",
            statistic="Sum",
            period=Duration.minutes(5),
        )
        pipeline_successes = cloudwatch.Metric(
            namespace="CustomerFeedbackAnalyzer",
            metric_name="PipelineExecutionsSucceeded",
            statistic="Sum",
            period=Duration.minutes(5),
        )
        pipeline_failures = cloudwatch.Metric(
            namespace="CustomerFeedbackAnalyzer",
            metric_name="PipelineExecutionsFailed",
            statistic="Sum",
            period=Duration.minutes(5),
        )
        avg_quality_score = cloudwatch.Metric(
            namespace="CustomerFeedbackAnalyzer",
            metric_name="FinalDataQualityScore",
            statistic="Average",
            period=Duration.minutes(5),
        )
        validation_failures = cloudwatch.Metric(
            namespace="CustomerFeedbackAnalyzer",
            metric_name="ValidationFailures",
            statistic="Sum",
            period=Duration.minutes(5),
        )

        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Pipeline Executions (5-min)",
                left=[pipeline_starts, pipeline_successes, pipeline_failures],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="Data Quality Score (avg, 5-min)",
                left=[avg_quality_score],
                width=12,
            ),
            cloudwatch.GraphWidget(
                title="Validation Failures (5-min)",
                left=[validation_failures],
                width=12,
            ),
        )

        # ── CloudWatch Alarms ──────────────────────────────────────────────────
        cloudwatch.Alarm(
            self,
            "LowDataQualityAlarm",
            alarm_name=f"{prefix}-LowDataQualityScore",
            alarm_description="Average data quality score has dropped below 70",
            metric=avg_quality_score,
            threshold=70,
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_THRESHOLD,
            evaluation_periods=3,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        cloudwatch.Alarm(
            self,
            "PipelineFailuresAlarm",
            alarm_name=f"{prefix}-HighPipelineFailureRate",
            alarm_description="Multiple pipeline executions have failed",
            metric=pipeline_failures,
            threshold=3,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # ── Outputs ────────────────────────────────────────────────────────────
        CfnOutput(
            self, "TextValidatorFnArn",
            value=self.text_validator_fn.function_arn,
            export_name=f"{prefix}-TextValidatorFnArn",
        )
        CfnOutput(
            self, "CloudWatchMetricsFnArn",
            value=self.cloudwatch_metrics_fn.function_arn,
            export_name=f"{prefix}-CloudWatchMetricsFnArn",
        )
        CfnOutput(
            self, "GlueCrawlerName",
            value=glue_crawler.ref,
        )
        CfnOutput(
            self, "GlueDQRulesetName",
            value=glue_dq_ruleset.ref,
        )
