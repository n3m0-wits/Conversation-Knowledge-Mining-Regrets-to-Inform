"""Email ingestion and extraction pipeline for Outlook job-search messages."""

from __future__ import annotations

import asyncio
import json
import os
import random
import re
import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import getaddresses
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import pyodbc
import requests
from azure.ai.inference.aio import EmbeddingsClient
from azure.identity import AzureCliCredential, get_bearer_token_provider
from azure.identity.aio import AzureCliCredential as AsyncAzureCliCredential
from azure.search.documents import SearchClient

from content_understanding_client import AzureContentUnderstandingClient

INDEX_NAME = "call_transcripts_index"
ANALYZER_ID = "ckm-json"
QUALITY_REVIEW_SAMPLE_RATE = 0.05

CATEGORIES = {
    "Job Ad",
    "Recruiter Reach-out",
    "Bot/Auto-Reply",
    "Confirmation of Application",
    "Rejection",
    "Request for Information",
    "Technical Test Request",
    "Personality Test Request",
    "Interview Invitation",
    "Job Offer",
    "Unknown",
    "Not Job Related",
}

URGENCIES = {"High", "Medium", "Low"}
INTERVIEW_TYPES = {
    "Online-Live",
    "In-Person",
    "One-Way-Robot",
    "Phone-Call",
    "Unclear",
    "None",
}


@dataclass
class EmailMessage:
    message_id: str
    internet_message_id: str
    subject: str
    from_address: str
    to_addresses: str
    sent_datetime: str
    folder: str
    content: str
    raw_json: str

    @property
    def message_key(self) -> str:
        if self.internet_message_id:
            return self.internet_message_id
        return self.message_id


def _html_to_text(value: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script[^>]*>", " ", value, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style[^>]*>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_reply_and_signature(text: str) -> str:
    boundaries = [
        r"(?im)^-----Original Message-----$",
        r"(?im)^From:\s.+$",
        r"(?im)^On\s.+wrote:$",
        r"(?im)^Sent:\s.+$",
    ]
    cut_idx = len(text)
    for pattern in boundaries:
        match = re.search(pattern, text)
        if match:
            cut_idx = min(cut_idx, match.start())
    text = text[:cut_idx]

    signature_markers = [
        r"(?im)^--\s*$",
        r"(?im)^Best regards,",
        r"(?im)^Kind regards,",
        r"(?im)^Thanks,",
        r"(?im)^Sincerely,",
    ]
    sig_idx = len(text)
    for marker in signature_markers:
        match = re.search(marker, text)
        if match:
            sig_idx = min(sig_idx, match.start())
    text = text[:sig_idx]
    return re.sub(r"\s+", " ", text).strip()


def _normalize_email_text(message: dict[str, Any]) -> str:
    body = message.get("body") or {}
    body_content = body.get("content") or ""
    content_type = (body.get("contentType") or "text").lower()
    if content_type == "html":
        body_content = _html_to_text(body_content)
    normalized = _strip_reply_and_signature(body_content)
    if not normalized:
        normalized = message.get("bodyPreview") or ""
    return normalized.strip()


def _extract_from_field(message: dict[str, Any]) -> str:
    sender = (message.get("from") or {}).get("emailAddress") or {}
    return (sender.get("address") or "").strip().lower()


def _extract_to_field(message: dict[str, Any]) -> str:
    recipients = message.get("toRecipients") or []
    raw_addresses = []
    for recipient in recipients:
        addr = (recipient.get("emailAddress") or {}).get("address")
        if addr:
            raw_addresses.append(addr)
    parsed = [email.lower() for _, email in getaddresses(raw_addresses) if email]
    unique = sorted(set(parsed))
    return ", ".join(unique)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        clean_value = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def _find_deadline_datetime(text: str) -> datetime | None:
    now = datetime.now(timezone.utc)
    relative_patterns = {
        r"\bwithin\s+24\s*hours?\b": 24,
        r"\bwithin\s+48\s*hours?\b": 48,
        r"\bby\s+tomorrow\b": 24,
        r"\btoday\b": 8,
    }
    lowered = text.lower()
    for pattern, hours in relative_patterns.items():
        if re.search(pattern, lowered):
            return now + timedelta(hours=hours)

    absolute_pattern = re.search(
        r"\b(?:by|before|deadline[:\s]*)\s*(\d{4}-\d{2}-\d{2}(?:[ t]\d{2}:\d{2})?)",
        lowered,
    )
    if absolute_pattern:
        candidate = absolute_pattern.group(1).replace("t", " ")
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            return None
    return None


def _sanitize_enum(value: Any, allowed: set[str], default: str) -> str:
    if not value:
        return default
    raw = str(value).strip()
    if raw in allowed:
        return raw
    lowered_map = {item.lower(): item for item in allowed}
    return lowered_map.get(raw.lower(), default)


def _to_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "yes", "y", "1"}:
        return True
    if text in {"false", "no", "n", "0"}:
        return False
    return None


def _cap_summary(summary: str | None) -> str:
    if not summary:
        return "No action required."
    words = summary.strip().split()
    if not words:
        return "No action required."
    return " ".join(words[:25])


def _infer_automation(from_address: str, subject: str, extracted: bool | None) -> bool | None:
    if extracted is not None:
        return extracted
    sender_lower = from_address.lower()
    subject_lower = subject.lower()
    if any(token in sender_lower for token in ["noreply", "no-reply", "donotreply"]):
        return True
    if any(token in subject_lower for token in ["automated", "do not reply", "notification"]):
        return True
    return None


def _parse_test_duration(text: str, extracted_value: Any) -> int:
    if extracted_value is not None:
        try:
            return max(int(extracted_value), 0)
        except (TypeError, ValueError):
            pass
    match = re.search(r"(\d{1,3})\s*(?:minutes|mins|min)\b", text.lower())
    if match:
        return int(match.group(1))
    return 0


def _get_field(fields: dict[str, dict[str, Any]], field_name: str, default: Any = None) -> Any:
    field = fields.get(field_name) or {}
    if "valueBoolean" in field:
        return field.get("valueBoolean")
    if "valueInteger" in field:
        return field.get("valueInteger")
    if "valueNumber" in field:
        return field.get("valueNumber")
    if "valueString" in field:
        return field.get("valueString")
    return default


def _chunk_text(text: str, tokens_per_chunk: int = 1024) -> list[str]:
    cleaned = re.sub(r"\s+", " ", text).strip()
    if not cleaned:
        return []
    tokens = cleaned.split()
    chunks = []
    for idx in range(0, len(tokens), tokens_per_chunk):
        chunks.append(" ".join(tokens[idx:idx + tokens_per_chunk]))
    return chunks


class GraphMailboxClient:
    """Minimal Graph mailbox reader for backfill + delta sync."""

    def __init__(self, credential: AzureCliCredential, user_id: str):
        self._credential = credential
        self._user_id = user_id

    def _headers(self) -> dict[str, str]:
        token = self._credential.get_token("https://graph.microsoft.com/.default")
        return {
            "Authorization": f"Bearer {token.token}",
            "Content-Type": "application/json",
        }

    def _get(self, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        response = requests.get(url, headers=self._headers(), params=params, timeout=30)
        response.raise_for_status()
        return response.json()

    def resolve_folder_id(self, folder_name: str) -> str:
        if folder_name.lower() in {"inbox", "sentitems", "drafts", "archive"}:
            return folder_name
        url = f"https://graph.microsoft.com/v1.0/users/{quote(self._user_id)}/mailFolders"
        params = {"$top": 200, "$select": "id,displayName"}
        payload = self._get(url, params)
        for folder in payload.get("value", []):
            if (folder.get("displayName") or "").lower() == folder_name.lower():
                return folder.get("id", folder_name)
        return folder_name

    def fetch_historical(
        self,
        folder_id: str,
        limit: int,
    ) -> tuple[list[dict[str, Any]], str | None]:
        select_fields = (
            "id,internetMessageId,from,toRecipients,subject,sentDateTime,"
            "receivedDateTime,body,bodyPreview,parentFolderId"
        )
        url = (
            f"https://graph.microsoft.com/v1.0/users/{quote(self._user_id)}"
            f"/mailFolders/{quote(folder_id)}/messages"
        )
        params = {
            "$top": min(limit, 100),
            "$orderby": "receivedDateTime desc",
            "$select": select_fields,
        }

        messages: list[dict[str, Any]] = []
        next_url: str | None = url
        current_params = params
        while next_url and len(messages) < limit:
            payload = self._get(next_url, current_params)
            values = payload.get("value", [])
            messages.extend(values)
            next_url = payload.get("@odata.nextLink")
            current_params = None
        return messages[:limit], None

    def fetch_delta(
        self,
        folder_id: str,
        prior_delta_link: str | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        select_fields = (
            "id,internetMessageId,from,toRecipients,subject,sentDateTime,"
            "receivedDateTime,body,bodyPreview,parentFolderId"
        )
        if prior_delta_link:
            url = prior_delta_link
            params = None
        else:
            url = (
                f"https://graph.microsoft.com/v1.0/users/{quote(self._user_id)}"
                f"/mailFolders/{quote(folder_id)}/messages/delta"
            )
            params = {"$select": select_fields, "$top": 50}

        messages: list[dict[str, Any]] = []
        next_url: str | None = url
        delta_link = prior_delta_link
        current_params = params
        while next_url:
            payload = self._get(next_url, current_params)
            messages.extend(payload.get("value", []))
            next_url = payload.get("@odata.nextLink")
            if payload.get("@odata.deltaLink"):
                delta_link = payload.get("@odata.deltaLink")
            current_params = None
        return messages, delta_link


def _load_delta_state(path: str) -> dict[str, str]:
    delta_path = Path(path)
    if not delta_path.exists():
        return {}
    with delta_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _save_delta_state(path: str, state: dict[str, str]) -> None:
    delta_path = Path(path)
    delta_path.parent.mkdir(parents=True, exist_ok=True)
    with delta_path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)


def _coalesce_bool(value: bool | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value


def _compute_urgency(deadline_at: datetime | None) -> str:
    if not deadline_at:
        return "Low"
    now = datetime.now(timezone.utc)
    delta_hours = (deadline_at - now).total_seconds() / 3600
    if delta_hours <= 48:
        return "High"
    if delta_hours <= 120:
        return "Medium"
    return "Low"


def _build_email_record(message: EmailMessage, extracted_fields: dict[str, Any]) -> dict[str, Any]:
    normalized_text = message.content
    deadline_text = _get_field(extracted_fields, "deadline", "") or ""
    deadline_at = _find_deadline_datetime(f"{normalized_text} {deadline_text}")

    category = _sanitize_enum(
        _get_field(extracted_fields, "category", "Unknown"),
        CATEGORIES,
        "Unknown",
    )
    interview_type = _sanitize_enum(
        _get_field(extracted_fields, "interview_type", "None"),
        INTERVIEW_TYPES,
        "None",
    )
    extracted_urgency = _sanitize_enum(
        _get_field(extracted_fields, "urgency", "Low"),
        URGENCIES,
        "Low",
    )

    urgency = extracted_urgency
    if deadline_at:
        urgency = _compute_urgency(deadline_at)

    action_required = _to_bool(_get_field(extracted_fields, "action_required", None))
    if action_required is None:
        action_required = category in {
            "Request for Information",
            "Technical Test Request",
            "Personality Test Request",
            "Interview Invitation",
            "Job Offer",
        }

    is_automated = _infer_automation(
        message.from_address,
        message.subject,
        _to_bool(_get_field(extracted_fields, "is_automated", None)),
    )

    record = {
        "message_id": message.message_id,
        "internet_message_id": message.internet_message_id,
        "message_key": message.message_key,
        "from_address": message.from_address or "unknown",
        "to_addresses": message.to_addresses,
        "subject": message.subject,
        "sent_datetime": message.sent_datetime,
        "company": (_get_field(extracted_fields, "company", "Unknown") or "Unknown").strip() or "Unknown",
        "portal": (_get_field(extracted_fields, "portal", "Unknown") or "Unknown").strip() or "Unknown",
        "category": category,
        "is_automated": is_automated,
        "urgency": urgency,
        "interview_type": interview_type,
        "interview_platform": (
            _get_field(extracted_fields, "interview_platform", "None") or "None"
        ).strip() or "None",
        "test_platform": (
            _get_field(extracted_fields, "test_platform", "None") or "None"
        ).strip() or "None",
        "test_duration_mins": _parse_test_duration(
            normalized_text,
            _get_field(extracted_fields, "test_duration_mins", None),
        ),
        "action_required": _coalesce_bool(action_required),
        "summary": _cap_summary(_get_field(extracted_fields, "summary", "")),
        "content": normalized_text,
        "normalized_content": normalized_text,
        "deadline_at": deadline_at.isoformat() if deadline_at else None,
        "source_folder": message.folder,
        "raw_email_json": message.raw_json,
        "extracted_json": json.dumps(extracted_fields, ensure_ascii=False),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    if record["summary"] == "No action required." and record["action_required"]:
        record["summary"] = "Review this email and complete the requested next step before the deadline."

    return record


def _create_processed_data_table(cursor: pyodbc.Cursor) -> None:
    cursor.execute(
        """
        IF OBJECT_ID('dbo.processed_data', 'U') IS NULL
        CREATE TABLE dbo.processed_data (
            message_key NVARCHAR(512) NOT NULL PRIMARY KEY,
            message_id NVARCHAR(255) NULL,
            internet_message_id NVARCHAR(512) NULL,
            from_address NVARCHAR(320) NOT NULL,
            to_addresses NVARCHAR(MAX) NULL,
            subject NVARCHAR(1024) NULL,
            sent_datetime DATETIME2 NULL,
            company NVARCHAR(255) NOT NULL DEFAULT 'Unknown',
            portal NVARCHAR(255) NOT NULL DEFAULT 'Unknown',
            category NVARCHAR(255) NOT NULL DEFAULT 'Unknown',
            is_automated BIT NULL,
            urgency NVARCHAR(20) NOT NULL DEFAULT 'Low',
            interview_type NVARCHAR(50) NOT NULL DEFAULT 'None',
            interview_platform NVARCHAR(255) NOT NULL DEFAULT 'None',
            test_platform NVARCHAR(255) NOT NULL DEFAULT 'None',
            test_duration_mins INT NOT NULL DEFAULT 0,
            action_required BIT NOT NULL DEFAULT 0,
            summary NVARCHAR(4000) NOT NULL DEFAULT 'No action required.',
            content NVARCHAR(MAX) NULL,
            normalized_content NVARCHAR(MAX) NULL,
            deadline_at DATETIME2 NULL,
            source_folder NVARCHAR(255) NULL,
            raw_email_json NVARCHAR(MAX) NULL,
            extracted_json NVARCHAR(MAX) NULL,
            updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
        """
    )

    cursor.execute(
        """
        IF OBJECT_ID('dbo.extraction_review_queue', 'U') IS NULL
        CREATE TABLE dbo.extraction_review_queue (
            message_key NVARCHAR(512) NOT NULL PRIMARY KEY,
            needs_review BIT NOT NULL,
            review_reason NVARCHAR(255) NOT NULL,
            created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
        );
        """
    )


def _upsert_processed_record(cursor: pyodbc.Cursor, record: dict[str, Any]) -> None:
    sent_dt = _parse_datetime(record.get("sent_datetime"))
    deadline_dt = _parse_datetime(record.get("deadline_at"))
    cursor.execute(
        """
        MERGE dbo.processed_data AS target
        USING (SELECT ? AS message_key) AS source
        ON target.message_key = source.message_key
        WHEN MATCHED THEN
            UPDATE SET
                message_id = ?,
                internet_message_id = ?,
                from_address = ?,
                to_addresses = ?,
                subject = ?,
                sent_datetime = ?,
                company = ?,
                portal = ?,
                category = ?,
                is_automated = ?,
                urgency = ?,
                interview_type = ?,
                interview_platform = ?,
                test_platform = ?,
                test_duration_mins = ?,
                action_required = ?,
                summary = ?,
                content = ?,
                normalized_content = ?,
                deadline_at = ?,
                source_folder = ?,
                raw_email_json = ?,
                extracted_json = ?,
                updated_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (
                message_key, message_id, internet_message_id, from_address, to_addresses,
                subject, sent_datetime, company, portal, category, is_automated, urgency,
                interview_type, interview_platform, test_platform, test_duration_mins,
                action_required, summary, content, normalized_content, deadline_at,
                source_folder, raw_email_json, extracted_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
        """,
        record["message_key"],
        record["message_id"],
        record["internet_message_id"],
        record["from_address"],
        record["to_addresses"],
        record["subject"],
        sent_dt,
        record["company"],
        record["portal"],
        record["category"],
        record["is_automated"],
        record["urgency"],
        record["interview_type"],
        record["interview_platform"],
        record["test_platform"],
        record["test_duration_mins"],
        record["action_required"],
        record["summary"],
        record["content"],
        record["normalized_content"],
        deadline_dt,
        record["source_folder"],
        record["raw_email_json"],
        record["extracted_json"],
        record["message_key"],
        record["message_id"],
        record["internet_message_id"],
        record["from_address"],
        record["to_addresses"],
        record["subject"],
        sent_dt,
        record["company"],
        record["portal"],
        record["category"],
        record["is_automated"],
        record["urgency"],
        record["interview_type"],
        record["interview_platform"],
        record["test_platform"],
        record["test_duration_mins"],
        record["action_required"],
        record["summary"],
        record["content"],
        record["normalized_content"],
        deadline_dt,
        record["source_folder"],
        record["raw_email_json"],
        record["extracted_json"],
    )


def _upsert_review_record(cursor: pyodbc.Cursor, record: dict[str, Any]) -> None:
    review_reason = ""
    needs_review = False
    if record["category"] == "Unknown":
        review_reason = "Unknown category"
        needs_review = True
    elif record["company"] == "Unknown":
        review_reason = "Unknown company"
        needs_review = True
    elif random.random() < QUALITY_REVIEW_SAMPLE_RATE:
        review_reason = "Sampled quality review"
        needs_review = True

    if not needs_review:
        return

    cursor.execute(
        """
        MERGE dbo.extraction_review_queue AS target
        USING (SELECT ? AS message_key) AS source
        ON target.message_key = source.message_key
        WHEN MATCHED THEN
            UPDATE SET needs_review = 1, review_reason = ?, created_at = SYSUTCDATETIME()
        WHEN NOT MATCHED THEN
            INSERT (message_key, needs_review, review_reason, created_at)
            VALUES (?, 1, ?, SYSUTCDATETIME());
        """,
        record["message_key"],
        review_reason,
        record["message_key"],
        review_reason,
    )


async def _embed_text(embeddings_client: EmbeddingsClient, model: str, text: str) -> list[float]:
    response = await embeddings_client.embed(model=model, input=[text])
    return response.data[0].embedding


async def _upload_search_documents(
    search_client: SearchClient,
    embeddings_client: EmbeddingsClient,
    embedding_model: str,
    record: dict[str, Any],
) -> None:
    chunks = _chunk_text(record["normalized_content"])
    if not chunks:
        return

    existing_ids = []
    escaped_key = record["message_key"].replace("'", "''")
    results = search_client.search(
        search_text="*",
        filter=f"source_message_key eq '{escaped_key}'",
        select=["id"],
        top=1000,
    )
    for item in results:
        existing_ids.append({"id": item["id"]})
    if existing_ids:
        search_client.delete_documents(existing_ids)

    docs = []
    for index, chunk in enumerate(chunks, start=1):
        chunk_id = f"{record['message_key']}_{index:03d}"
        vector = await _embed_text(embeddings_client, embedding_model, chunk)
        docs.append(
            {
                "id": chunk_id,
                "chunk_id": chunk_id,
                "content": chunk,
                "sourceurl": record["subject"] or record["message_key"],
                "source_message_key": record["message_key"],
                "company": record["company"],
                "portal": record["portal"],
                "category": record["category"],
                "urgency": record["urgency"],
                "action_required": record["action_required"],
                "sent_datetime": record["sent_datetime"],
                "contentVector": vector,
            }
        )
    if docs:
        search_client.upload_documents(docs)


def _extract_fields_with_analyzer(
    cu_client: AzureContentUnderstandingClient,
    content: str,
) -> dict[str, Any]:
    payload = content.encode("utf-8")
    response = cu_client.begin_analyze(ANALYZER_ID, file_location="", file_data=payload)
    result = cu_client.poll_result(response)
    return (result.get("result", {}).get("contents", [{}])[0].get("fields", {}))


def _get_sql_connection(server: str, database: str) -> pyodbc.Connection:
    credential = AzureCliCredential(process_timeout=30)
    token = credential.get_token("https://database.windows.net/.default")
    token_bytes = token.token.encode("utf-16-LE")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256

    for driver in ["{ODBC Driver 18 for SQL Server}", "{ODBC Driver 17 for SQL Server}"]:
        try:
            conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};"
            return pyodbc.connect(conn_str, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
        except Exception:
            continue
    raise RuntimeError("Unable to connect to SQL with ODBC Driver 18 or 17")


def _fetch_messages_from_graph(args: Any) -> list[EmailMessage]:
    credential = AzureCliCredential(process_timeout=30)
    graph_client = GraphMailboxClient(credential=credential, user_id=args.graph_user_id)

    folders = [folder.strip() for folder in args.graph_mail_folders.split(",") if folder.strip()]
    if not folders:
        folders = ["inbox"]

    delta_state = _load_delta_state(args.graph_delta_link_path)
    all_messages: list[EmailMessage] = []

    for folder in folders:
        folder_id = graph_client.resolve_folder_id(folder)
        delta_key = f"{args.graph_user_id}:{folder_id}"

        if args.graph_use_delta:
            messages, new_delta = graph_client.fetch_delta(
                folder_id,
                delta_state.get(delta_key),
            )
            if new_delta:
                delta_state[delta_key] = new_delta
        else:
            messages, _ = graph_client.fetch_historical(folder_id, args.graph_backfill_limit)

        for message in messages:
            if "@removed" in message:
                continue
            content = _normalize_email_text(message)
            message_id = message.get("id") or ""
            internet_message_id = message.get("internetMessageId") or ""
            sent_dt_raw = message.get("sentDateTime") or message.get("receivedDateTime") or ""
            sent_dt = _parse_datetime(sent_dt_raw)
            all_messages.append(
                EmailMessage(
                    message_id=message_id,
                    internet_message_id=internet_message_id,
                    subject=(message.get("subject") or "").strip(),
                    from_address=_extract_from_field(message),
                    to_addresses=_extract_to_field(message),
                    sent_datetime=sent_dt.isoformat() if sent_dt else "",
                    folder=folder,
                    content=content,
                    raw_json=json.dumps(message, ensure_ascii=False),
                )
            )

    if args.graph_use_delta:
        _save_delta_state(args.graph_delta_link_path, delta_state)

    deduped: dict[str, EmailMessage] = {}
    for message in all_messages:
        deduped[message.message_key] = message
    return list(deduped.values())


async def run_email_pipeline(args: Any) -> None:
    inference_endpoint = f"https://{urlparse(args.ai_project_endpoint).netloc}/models"

    cu_credential = AzureCliCredential(process_timeout=30)
    cu_token_provider = get_bearer_token_provider(
        cu_credential,
        "https://cognitiveservices.azure.com/.default",
    )
    cu_client = AzureContentUnderstandingClient(
        endpoint=args.cu_endpoint,
        api_version=args.cu_api_version,
        token_provider=cu_token_provider,
    )

    search_credential = AzureCliCredential(process_timeout=30)
    search_client = SearchClient(args.search_endpoint, INDEX_NAME, search_credential)

    if args.graph_skip_ingestion:
        print("⚠ Graph ingestion skipped by flag.")
        return

    messages = _fetch_messages_from_graph(args)
    print(f"✓ Retrieved {len(messages)} unique emails from Graph")

    conn = _get_sql_connection(args.sql_server, args.sql_database)
    cursor = conn.cursor()
    _create_processed_data_table(cursor)
    conn.commit()

    async with (
        AsyncAzureCliCredential(process_timeout=30) as async_cred,
        EmbeddingsClient(
            endpoint=inference_endpoint,
            credential=async_cred,
            credential_scopes=["https://ai.azure.com/.default"],
        ) as embeddings_client,
    ):
        for idx, message in enumerate(messages, start=1):
            if not message.content:
                continue
            try:
                extracted_fields = _extract_fields_with_analyzer(cu_client, message.content)
                record = _build_email_record(message, extracted_fields)
                _upsert_processed_record(cursor, record)
                _upsert_review_record(cursor, record)
                await _upload_search_documents(
                    search_client,
                    embeddings_client,
                    args.embedding_model,
                    record,
                )
                if idx % 10 == 0:
                    conn.commit()
            except Exception as error:
                print(f"⚠ Failed to process message {message.message_key}: {error}")

    conn.commit()
    cursor.close()
    conn.close()
    print("✓ Email pipeline completed")
