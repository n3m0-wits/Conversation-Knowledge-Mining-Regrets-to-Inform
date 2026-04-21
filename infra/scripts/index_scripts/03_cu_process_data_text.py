"""Process Outlook emails (historical or live feed) into SQL and Search."""

import argparse
import asyncio

from email_pipeline import run_email_pipeline


parser = argparse.ArgumentParser(description="Process Outlook emails")
parser.add_argument("--search_endpoint", required=True)
parser.add_argument("--ai_project_endpoint", required=True)
parser.add_argument("--deployment_model", required=True)
parser.add_argument("--embedding_model", required=True)
parser.add_argument("--storage_account_name", required=False, default="")
parser.add_argument("--sql_server", required=True)
parser.add_argument("--sql_database", required=True)
parser.add_argument("--cu_endpoint", required=True)
parser.add_argument("--cu_api_version", required=True)
parser.add_argument("--usecase", required=False, default="email")
parser.add_argument("--solution_name", required=False, default="ckm-email")

parser.add_argument("--graph_user_id", default="me")
parser.add_argument("--graph_mail_folders", default="Inbox")
parser.add_argument("--graph_backfill_limit", type=int, default=500)
parser.add_argument("--ingestion_source", choices=["historical", "live"], default="historical")
parser.add_argument("--live_emails_path", default="")
parser.add_argument("--live_source_folder", default="live")

args = parser.parse_args()

asyncio.run(run_email_pipeline(args))
