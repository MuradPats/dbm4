"""Slack notifications. Never raises — failures are logged and swallowed."""
import logging
import os

import requests

log = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")


def _post(payload: dict) -> None:
    if not SLACK_WEBHOOK_URL:
        log.info("SLACK_WEBHOOK_URL not set — skipping notification")
        return
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
    except Exception as exc:
        log.warning("Slack notification failed (non-fatal): %s", exc)


def notify_run_started(run_id: str, dag_run_id: str, limit: int, trigger: str) -> None:
    _post({
        "text": (
            f":rocket: *RICO pipeline started*\n"
            f"• `run_id`: `{run_id}`\n"
            f"• `dag_run_id`: `{dag_run_id}`\n"
            f"• `LIMIT`: {limit}\n"
            f"• triggered by: {trigger}"
        )
    })


def notify_audit_failed(run_id: str, dag_run_id: str, duplicates: list) -> None:
    dup_text = "\n".join(f"  - {d}" for d in duplicates[:20])
    overflow = f"\n  … and {len(duplicates) - 20} more" if len(duplicates) > 20 else ""
    _post({
        "text": (
            f":rotating_light: *RICO audit FAILED — pipeline halted*\n"
            f"• `run_id`: `{run_id}`\n"
            f"• `dag_run_id`: `{dag_run_id}`\n"
            f"• Duplicate keys found:\n{dup_text}{overflow}\n"
            f"Check the Airflow task log for `audit` to investigate."
        )
    })


def notify_run_finished(run_id: str, dag_run_id: str, status: str, duration_s: float, summary: str) -> None:
    icon = ":white_check_mark:" if status == "succeeded" else ":x:"
    _post({
        "text": (
            f"{icon} *RICO pipeline {status}*\n"
            f"• `run_id`: `{run_id}`\n"
            f"• `dag_run_id`: `{dag_run_id}`\n"
            f"• Duration: {duration_s:.1f}s\n"
            f"• Summary: {summary}"
        )
    })
