"""
glue/data_quality_rules.py

Helper module that defines and manages AWS Glue Data Quality (DQDL) rulesets
for customer feedback structured data.

Usage (standalone – run with appropriate AWS credentials):
    python glue/data_quality_rules.py --action create --profile aws_swami
    python glue/data_quality_rules.py --action evaluate --profile aws_swami
    python glue/data_quality_rules.py --action report --profile aws_swami
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

# ── Ruleset definitions ────────────────────────────────────────────────────────

REVIEW_CSV_RULESET = """\
Rules = [
    IsComplete "review_content",
    IsComplete "rating",
    IsComplete "product_id",
    IsComplete "user_id",
    IsComplete "review_id",
    ColumnValues "rating" between 1 and 5,
    ColumnLength "review_content" > 10,
    ColumnLength "review_content" < 10000,
    Uniqueness "review_id" > 0.85,
    IsComplete "review_title",
    Completeness "user_name" > 0.80,
    Completeness "product_name" > 0.95,
    ColumnCount > 10
]
"""

SURVEY_CSV_RULESET = """\
Rules = [
    ColumnCount >= 3,
    RowCount > 0,
    Completeness "response_id" > 0.99,
    IsUnique "response_id"
]
"""

# Map table names to their rulesets
RULESETS: dict[str, str] = {
    "text_reviews": REVIEW_CSV_RULESET,
    "surveys": SURVEY_CSV_RULESET,
}


@dataclass
class DataQualityManager:
    database_name: str
    ruleset_name_prefix: str
    glue_client: Any = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.glue_client is None:
            self.glue_client = boto3.client("glue")

    # ── Create rulesets ────────────────────────────────────────────────────────
    def create_ruleset(self, table_name: str, ruleset_dqdl: str) -> str:
        name = f"{self.ruleset_name_prefix}-{table_name}"
        try:
            self.glue_client.create_data_quality_ruleset(
                Name=name,
                Ruleset=ruleset_dqdl,
                TargetTable={
                    "TableName": table_name,
                    "DatabaseName": self.database_name,
                },
                Description=f"Data quality ruleset for {table_name} in {self.database_name}",
            )
            logger.info("Created ruleset: %s", name)
        except self.glue_client.exceptions.AlreadyExistsException:
            logger.info("Ruleset already exists, updating: %s", name)
            self.glue_client.update_data_quality_ruleset(
                Name=name,
                Ruleset=ruleset_dqdl,
            )
        return name

    def create_all_rulesets(self) -> list[str]:
        names = []
        for table, ruleset in RULESETS.items():
            names.append(self.create_ruleset(table, ruleset))
        return names

    # ── Start evaluation ───────────────────────────────────────────────────────
    def start_evaluation(self, ruleset_name: str, role_arn: str) -> str:
        response = self.glue_client.start_data_quality_ruleset_evaluation_run(
            DataSource={
                "GlueTable": {
                    "DatabaseName": self.database_name,
                    "TableName": ruleset_name.split("-")[-1],
                }
            },
            Role=role_arn,
            RulesetNames=[ruleset_name],
        )
        run_id = response["RunId"]
        logger.info("Evaluation started: RunId=%s", run_id)
        return run_id

    # ── Get evaluation results ─────────────────────────────────────────────────
    def get_evaluation_results(self, run_id: str) -> dict[str, Any]:
        response = self.glue_client.get_data_quality_ruleset_evaluation_run(RunId=run_id)
        status = response.get("Status", "UNKNOWN")
        logger.info("Evaluation run %s status: %s", run_id, status)
        return response

    # ── List all rulesets ──────────────────────────────────────────────────────
    def list_rulesets(self) -> list[dict]:
        resp = self.glue_client.list_data_quality_rulesets(
            Filter={"Name": self.ruleset_name_prefix}
        )
        return resp.get("DataQualityRulesets", [])

    # ── Print a human-readable report ─────────────────────────────────────────
    def print_rulesets(self) -> None:
        rulesets = self.list_rulesets()
        if not rulesets:
            print("No rulesets found.")
            return
        for rs in rulesets:
            print(f"\n{'─'*60}")
            print(f"  Name:        {rs.get('Name')}")
            print(f"  Table:       {rs.get('TargetTable', {}).get('TableName')}")
            print(f"  Rule count:  {rs.get('RuleCount')}")
            print(f"  Created:     {rs.get('CreatedOn')}")
            print(f"  Modified:    {rs.get('LastModifiedOn')}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage Glue Data Quality rulesets")
    parser.add_argument(
        "--action",
        choices=["create", "evaluate", "list", "report"],
        required=True,
    )
    parser.add_argument("--database", default="cfa_customer_feedback_db")
    parser.add_argument("--prefix", default="cfa-feedback-dq")
    parser.add_argument("--profile", default="aws_swami")
    parser.add_argument("--role-arn", help="IAM role ARN for evaluation runs")
    parser.add_argument("--run-id", help="Evaluation run ID for fetching results")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    session = boto3.Session(profile_name=args.profile)
    glue = session.client("glue")

    manager = DataQualityManager(
        database_name=args.database,
        ruleset_name_prefix=args.prefix,
        glue_client=glue,
    )

    if args.action == "create":
        created = manager.create_all_rulesets()
        print(f"Created/updated {len(created)} rulesets: {created}")

    elif args.action == "list":
        manager.print_rulesets()

    elif args.action == "report":
        if not args.run_id:
            print("--run-id required for 'report' action")
            sys.exit(1)
        result = manager.get_evaluation_results(args.run_id)
        print(json.dumps(result, default=str, indent=2))

    elif args.action == "evaluate":
        if not args.role_arn:
            print("--role-arn required for 'evaluate' action")
            sys.exit(1)
        rulesets = manager.list_rulesets()
        for rs in rulesets:
            run_id = manager.start_evaluation(rs["Name"], args.role_arn)
            print(f"Started evaluation: RunId={run_id} for ruleset={rs['Name']}")


if __name__ == "__main__":
    main()
