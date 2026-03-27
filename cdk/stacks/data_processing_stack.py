"""
DataProcessingStack — Processing Lambdas + Step Functions Pipeline

Resources created:
  - Lambda: pipeline_orchestrator  (S3 event → Step Functions)
  - Lambda: text_normalizer        (text cleaning)
  - Lambda: comprehend_processor   (sentiment + entities)
  - Lambda: textract_processor     (image → text via Textract)
  - Lambda: transcribe_processor   (audio → transcript via Transcribe)
  - Lambda: survey_summarizer      (CSV → natural-language summary)
  - Lambda: bedrock_formatter      (format data + invoke Amazon Nova Lite)
  - Lambda: feedback_quality_loop  (quality improvement from model output)
  - Step Functions State Machine   (orchestrates all processing Lambdas)
  - EventBridge Rule               (S3 PutObject → pipeline_orchestrator)
  - IAM roles for each Lambda and Step Functions
"""
from __future__ import annotations

from pathlib import Path

import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    CfnOutput,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct

LAMBDA_ROOT = Path(__file__).parent.parent.parent / "lambda"


def _lambda_role(
    scope: Construct,
    construct_id: str,
    extra_statements: list[iam.PolicyStatement] | None = None,
) -> iam.Role:
    """Helper: create a Lambda execution role with basic CW Logs + optional extras."""
    role = iam.Role(
        scope,
        construct_id,
        assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        managed_policies=[
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        ],
    )
    for stmt in extra_statements or []:
        role.add_to_policy(stmt)
    return role


class DataProcessingStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        prefix: str,
        input_bucket: s3.Bucket,
        processed_bucket: s3.Bucket,
        output_bucket: s3.Bucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        log_level = self.node.try_get_context("log_level") or "INFO"
        bedrock_model_id = (
            self.node.try_get_context("bedrock_model_id")
            or "amazon.nova-lite-v1:0"
        )
        comprehend_language = self.node.try_get_context("comprehend_language") or "en"

        # ── Common env vars shared by many Lambdas ─────────────────────────────
        common_env = {
            "INPUT_BUCKET": input_bucket.bucket_name,
            "PROCESSED_BUCKET": processed_bucket.bucket_name,
            "OUTPUT_BUCKET": output_bucket.bucket_name,
            "LOG_LEVEL": log_level,
            "METRICS_NAMESPACE": "CustomerFeedbackAnalyzer",
        }

        # ═══════════════════════════════════════════════════════════════════════
        # Lambda: text_normalizer
        # ═══════════════════════════════════════════════════════════════════════
        normalizer_role = _lambda_role(
            self, "TextNormalizerRole",
            extra_statements=[
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"],
                ),
            ],
        )
        input_bucket.grant_read(normalizer_role)
        processed_bucket.grant_read_write(normalizer_role)

        text_normalizer_fn = lambda_.Function(
            self,
            "TextNormalizerFn",
            function_name=f"{prefix}-text-normalizer",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(LAMBDA_ROOT / "text_normalizer")),
            role=normalizer_role,
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={**common_env},
        )

        # ═══════════════════════════════════════════════════════════════════════
        # Lambda: comprehend_processor
        # ═══════════════════════════════════════════════════════════════════════
        comprehend_role = _lambda_role(
            self, "ComprehendProcessorRole",
            extra_statements=[
                iam.PolicyStatement(
                    actions=[
                        "comprehend:DetectSentiment",
                        "comprehend:DetectEntities",
                        "comprehend:DetectKeyPhrases",
                        "comprehend:DetectDominantLanguage",
                        "comprehend:BatchDetectSentiment",
                        "comprehend:BatchDetectEntities",
                        "comprehend:BatchDetectKeyPhrases",
                    ],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"],
                ),
            ],
        )
        input_bucket.grant_read(comprehend_role)
        processed_bucket.grant_read_write(comprehend_role)

        comprehend_fn = lambda_.Function(
            self,
            "ComprehendProcessorFn",
            function_name=f"{prefix}-comprehend-processor",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(LAMBDA_ROOT / "comprehend_processor")),
            role=comprehend_role,
            timeout=Duration.minutes(10),
            memory_size=1024,
            environment={
                **common_env,
                "COMPREHEND_LANGUAGE": comprehend_language,
                "COMPREHEND_BATCH_SIZE": "25",
            },
        )

        # ═══════════════════════════════════════════════════════════════════════
        # Lambda: textract_processor
        # ═══════════════════════════════════════════════════════════════════════
        textract_role = _lambda_role(
            self, "TextractProcessorRole",
            extra_statements=[
                iam.PolicyStatement(
                    actions=[
                        "textract:DetectDocumentText",
                        "textract:AnalyzeDocument",
                        "textract:StartDocumentTextDetection",
                        "textract:GetDocumentTextDetection",
                    ],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"],
                ),
            ],
        )
        input_bucket.grant_read(textract_role)
        processed_bucket.grant_read_write(textract_role)

        textract_fn = lambda_.Function(
            self,
            "TextractProcessorFn",
            function_name=f"{prefix}-textract-processor",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(LAMBDA_ROOT / "textract_processor")),
            role=textract_role,
            timeout=Duration.minutes(10),
            memory_size=512,
            environment={**common_env},
        )

        # ═══════════════════════════════════════════════════════════════════════
        # Lambda: transcribe_processor
        # ═══════════════════════════════════════════════════════════════════════
        transcribe_role = _lambda_role(
            self, "TranscribeProcessorRole",
            extra_statements=[
                iam.PolicyStatement(
                    actions=[
                        "transcribe:StartTranscriptionJob",
                        "transcribe:GetTranscriptionJob",
                        "transcribe:ListTranscriptionJobs",
                    ],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"],
                ),
            ],
        )
        input_bucket.grant_read(transcribe_role)
        processed_bucket.grant_read_write(transcribe_role)

        transcribe_fn = lambda_.Function(
            self,
            "TranscribeProcessorFn",
            function_name=f"{prefix}-transcribe-processor",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(LAMBDA_ROOT / "transcribe_processor")),
            role=transcribe_role,
            timeout=Duration.minutes(12),   # polling window
            memory_size=512,
            environment={**common_env},
        )

        # ═══════════════════════════════════════════════════════════════════════
        # Lambda: survey_summarizer
        # ═══════════════════════════════════════════════════════════════════════
        survey_role = _lambda_role(
            self, "SurveySummarizerRole",
        )
        input_bucket.grant_read(survey_role)
        processed_bucket.grant_read_write(survey_role)

        survey_fn = lambda_.Function(
            self,
            "SurveySummarizerFn",
            function_name=f"{prefix}-survey-summarizer",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(LAMBDA_ROOT / "survey_summarizer")),
            role=survey_role,
            timeout=Duration.minutes(5),
            memory_size=1024,   # pandas in memory
            environment={**common_env},
        )

        # ═══════════════════════════════════════════════════════════════════════
        # Lambda: bedrock_formatter (formats payload + invokes Amazon Nova Lite)
        # ═══════════════════════════════════════════════════════════════════════
        bedrock_role = _lambda_role(
            self, "BedrockFormatterRole",
            extra_statements=[
                iam.PolicyStatement(
                    actions=[
                        "bedrock:InvokeModel",
                        "bedrock:InvokeModelWithResponseStream",
                    ],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"],
                ),
            ],
        )
        processed_bucket.grant_read(bedrock_role)
        output_bucket.grant_read_write(bedrock_role)

        bedrock_fn = lambda_.Function(
            self,
            "BedrockFormatterFn",
            function_name=f"{prefix}-bedrock-formatter",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(LAMBDA_ROOT / "bedrock_formatter")),
            role=bedrock_role,
            timeout=Duration.minutes(10),
            memory_size=1024,
            environment={
                **common_env,
                "BEDROCK_MODEL_ID": bedrock_model_id,
                "BEDROCK_REGION": self.region,
                "MAX_TOKENS": "4096",
            },
        )

        # ═══════════════════════════════════════════════════════════════════════
        # Lambda: feedback_quality_loop
        # ═══════════════════════════════════════════════════════════════════════
        feedback_role = _lambda_role(
            self, "FeedbackQualityLoopRole",
            extra_statements=[
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"],
                ),
            ],
        )
        processed_bucket.grant_read_write(feedback_role)
        output_bucket.grant_read(feedback_role)

        feedback_fn = lambda_.Function(
            self,
            "FeedbackQualityLoopFn",
            function_name=f"{prefix}-feedback-quality-loop",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(LAMBDA_ROOT / "feedback_quality_loop")),
            role=feedback_role,
            timeout=Duration.minutes(3),
            memory_size=512,
            environment={
                **common_env,
                "QUALITY_THRESHOLD": "70",
            },
        )

        # ═══════════════════════════════════════════════════════════════════════
        # Lambda: cloudwatch_metrics (re-declared in this stack for SF use)
        # ═══════════════════════════════════════════════════════════════════════
        cw_metrics_role = _lambda_role(
            self, "CWMetricsRole",
            extra_statements=[
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"],
                ),
            ],
        )

        cw_metrics_fn = lambda_.Function(
            self,
            "CWMetricsFn",
            function_name=f"{prefix}-cw-metrics",
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

        # ═══════════════════════════════════════════════════════════════════════
        # Step Functions State Machine
        # ═══════════════════════════════════════════════════════════════════════
        _RETRY_ERRORS = [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException",
            "Lambda.TooManyRequestsException",
        ]

        def _add_retry(task: sfn.TaskStateBase) -> sfn.TaskStateBase:
            """Attach standard Lambda retry configuration to a Step Functions task."""
            task.add_retry(
                errors=_RETRY_ERRORS,
                interval=Duration.seconds(2),
                max_attempts=3,
                backoff_rate=2.0,
            )
            return task

        # Terminal states (defined first — referenced downstream by earlier states)
        publish_metrics = _add_retry(sfn_tasks.LambdaInvoke(
            self, "PublishMetrics",
            lambda_function=cw_metrics_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=False,
        ))

        update_quality = _add_retry(sfn_tasks.LambdaInvoke(
            self, "UpdateQualityFeedback",
            lambda_function=feedback_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=False,
        ))
        update_quality.next(publish_metrics)

        generate_insights = _add_retry(sfn_tasks.LambdaInvoke(
            self, "GenerateInsightsWithBedrock",
            lambda_function=bedrock_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=False,
        ))
        generate_insights.next(update_quality)

        # ── Text-review branch ─────────────────────────────────────────────────
        comprehend_analysis = _add_retry(sfn_tasks.LambdaInvoke(
            self, "AnalyzeWithComprehend",
            lambda_function=comprehend_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=False,
        ))
        comprehend_analysis.next(generate_insights)

        normalize_for_reviews = _add_retry(sfn_tasks.LambdaInvoke(
            self, "NormalizeReviewText",
            lambda_function=text_normalizer_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=False,
        ))
        normalize_for_reviews.next(comprehend_analysis)

        # text_validator lives in DataValidationStack; reference by ARN to avoid
        # circular CDK dependencies between the two stacks.
        validate_text = _add_retry(sfn_tasks.LambdaInvoke(
            self,
            "ValidateTextReviews",
            lambda_function=lambda_.Function.from_function_arn(
                self,
                "ImportedTextValidatorFn",
                f"arn:aws:lambda:{self.region}:{self.account}:function:{prefix}-text-validator",
            ),
            output_path="$.Payload",
            retry_on_service_exceptions=False,
        ))
        validate_text.next(normalize_for_reviews)

        # ── Image branch ───────────────────────────────────────────────────────
        textract_process = _add_retry(sfn_tasks.LambdaInvoke(
            self, "ExtractTextFromImage",
            lambda_function=textract_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=False,
        ))
        textract_process.next(generate_insights)

        # ── Audio branch ───────────────────────────────────────────────────────
        normalize_for_audio = _add_retry(sfn_tasks.LambdaInvoke(
            self, "NormalizeTranscription",
            lambda_function=text_normalizer_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=False,
        ))
        normalize_for_audio.next(comprehend_analysis)

        transcribe_process = _add_retry(sfn_tasks.LambdaInvoke(
            self, "TranscribeAudioRecording",
            lambda_function=transcribe_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=False,
        ))
        transcribe_process.next(normalize_for_audio)

        # ── Survey branch ──────────────────────────────────────────────────────
        survey_process = _add_retry(sfn_tasks.LambdaInvoke(
            self, "SummarizeSurveyData",
            lambda_function=survey_fn,
            output_path="$.Payload",
            retry_on_service_exceptions=False,
        ))
        survey_process.next(generate_insights)

        # ── Unsupported data type ──────────────────────────────────────────────
        unsupported = sfn.Fail(
            self, "UnsupportedDataType",
            error="UnsupportedDataTypeError",
            cause="The submitted data type is not handled by this pipeline",
        )

        # ── Router (Choice state) ──────────────────────────────────────────────
        router = sfn.Choice(self, "RouteByDataType", comment="Route to processing branch")
        router.when(
            sfn.Condition.string_equals("$.data_type", "text_reviews"),
            validate_text,
        )
        router.when(
            sfn.Condition.string_equals("$.data_type", "images"),
            textract_process,
        )
        router.when(
            sfn.Condition.string_equals("$.data_type", "audio"),
            transcribe_process,
        )
        router.when(
            sfn.Condition.string_equals("$.data_type", "surveys"),
            survey_process,
        )
        router.otherwise(unsupported)

        # ── Step Functions execution role ──────────────────────────────────────
        sf_role = iam.Role(
            self,
            "StateMachineRole",
            assumed_by=iam.ServicePrincipal("states.amazonaws.com"),
        )
        for fn in [
            text_normalizer_fn, comprehend_fn, textract_fn, transcribe_fn,
            survey_fn, bedrock_fn, feedback_fn, cw_metrics_fn,
        ]:
            fn.grant_invoke(sf_role)

        # Allow invoking the cross-stack text_validator by ARN
        sf_role.add_to_policy(
            iam.PolicyStatement(
                actions=["lambda:InvokeFunction"],
                resources=[
                    f"arn:aws:lambda:{self.region}:{self.account}:function:{prefix}-text-validator"
                ],
            )
        )

        state_machine = sfn.StateMachine(
            self,
            "FeedbackPipelineSM",
            state_machine_name=f"{prefix}-feedback-pipeline",
            definition_body=sfn.DefinitionBody.from_chainable(router),
            role=sf_role,
            timeout=Duration.hours(1),
            tracing_enabled=True,
        )

        # ═══════════════════════════════════════════════════════════════════════
        # Lambda: pipeline_orchestrator
        # ═══════════════════════════════════════════════════════════════════════
        orch_role = _lambda_role(
            self, "PipelineOrchestratorRole",
            extra_statements=[
                iam.PolicyStatement(
                    actions=[
                        "states:StartExecution",
                        "states:DescribeExecution",
                    ],
                    resources=[state_machine.state_machine_arn],
                ),
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],
                    resources=["*"],
                ),
            ],
        )
        input_bucket.grant_read(orch_role)

        orchestrator_fn = lambda_.Function(
            self,
            "PipelineOrchestratorFn",
            function_name=f"{prefix}-pipeline-orchestrator",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="handler.handler",
            code=lambda_.Code.from_asset(str(LAMBDA_ROOT / "pipeline_orchestrator")),
            role=orch_role,
            timeout=Duration.minutes(2),
            memory_size=256,
            environment={
                **common_env,
                "STATE_MACHINE_ARN": state_machine.state_machine_arn,
            },
        )

        # ── EventBridge rule: S3 PutObject → pipeline_orchestrator ─────────────
        s3_put_rule = events.Rule(
            self,
            "S3PutObjectRule",
            rule_name=f"{prefix}-s3-put-object-trigger",
            description="Trigger the feedback pipeline when objects land in the input bucket",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {"name": [input_bucket.bucket_name]},
                    "object": {
                        "key": [
                            {"prefix": "text-reviews/"},
                            {"prefix": "images/"},
                            {"prefix": "audio/"},
                            {"prefix": "surveys/"},
                        ]
                    },
                },
            ),
        )
        s3_put_rule.add_target(targets.LambdaFunction(orchestrator_fn))

        # ── Outputs ────────────────────────────────────────────────────────────
        CfnOutput(
            self, "StateMachineArn",
            value=state_machine.state_machine_arn,
            export_name=f"{prefix}-StateMachineArn",
        )
        CfnOutput(
            self, "OrchestratorFnArn",
            value=orchestrator_fn.function_arn,
        )
