#!/usr/bin/env python3
"""RICO Pipeline Backfill Agent (Agentic DataOps).

A standalone ChatOps service that listens for mentions in Slack via Socket Mode,
uses a local Ollama LLM to extract the screen backfill LIMIT parameter, and
triggers a new Airflow DAG run via the Airflow REST API.
"""

import os
import re
import json
import logging
import requests
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# Load environment variables from .env if present
load_dotenv()

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
log = logging.getLogger("backfill-agent")

# Configuration
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:3b")

AIRFLOW_URL = os.environ.get("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "admin")
DAG_ID = "rico_pipeline"

# Initialise Slack App
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_APP_TOKEN = os.environ.get("SLACK_APP_TOKEN")

if not SLACK_BOT_TOKEN or not SLACK_APP_TOKEN:
    log.error("Missing SLACK_BOT_TOKEN or SLACK_APP_TOKEN environment variables!")
    raise RuntimeError("Both SLACK_BOT_TOKEN and SLACK_APP_TOKEN must be set.")

app = App(token=SLACK_BOT_TOKEN)


def parse_limit_with_llm(user_message: str) -> int:
    """Send the Slack message text to Ollama to parse intent and extract screen count LIMIT."""
    log.info("Querying Ollama (%s) to parse limit from message: %r", OLLAMA_MODEL, user_message)
    
    prompt = f"""\
You are an AI assistant for a data engineering pipeline.
Your job is to read a Slack message and extract the requested "LIMIT" parameter (number of screens to process) as an integer.
If the user specifies a number of screens to run or backfill, return ONLY a JSON object with a single key "limit" mapping to the integer.

Examples:
- "Hey, we need more data. Can you run a backfill for 20 screens?" -> {{"limit": 20}}
- "@DataBot backfill 15 screens" -> {{"limit": 15}}
- "please run the pipeline with limit 8" -> {{"limit": 8}}
- "@DataBot run the pipeline" -> {{"limit": 5}}

If no number is specified in the message, default to 5.
Return ONLY the raw JSON object, no explanation, no markdown blocks.

Message:
"{user_message}"
"""

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
    }
    
    try:
        resp = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=25)
        resp.raise_for_status()
        raw = resp.json().get("response", "").strip()
        
        # Strip markdown json block fences if the LLM wrapped them
        raw = raw.removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        
        data = json.loads(raw)
        limit = int(data.get("limit", 5))
        log.info("LLM successfully parsed limit parameter: %d", limit)
        return limit
    except Exception as exc:
        log.warning("Ollama parsing failed or timed out (%s). Falling back to regex extraction.", exc)
        
        # Fallback regex extraction of first integer sequence
        digits = re.findall(r'\b\d+\b', user_message)
        if digits:
            limit = int(digits[0])
            log.info("Regex extraction successfully parsed limit parameter: %d", limit)
            return limit
            
        log.info("No digit sequence found in message, falling back to default limit of 5")
        return 5


def trigger_airflow_dag(limit: int) -> dict:
    """Trigger the Airflow pipeline DAG via the REST API with Basic Auth."""
    url = f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns"
    payload = {
        "conf": {
            "LIMIT": limit
        }
    }
    
    log.info("Triggering Airflow DAG %r via REST API at %s (conf: %s)", DAG_ID, url, payload)
    
    resp = requests.post(
        url,
        json=payload,
        auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
        headers={"Content-Type": "application/json"},
        timeout=15
    )
    
    if resp.status_code == 401:
        raise RuntimeError("Airflow REST API authentication failed. Verify username and password.")
    elif resp.status_code == 404:
        raise RuntimeError(f"DAG '{DAG_ID}' not found in Airflow. Ensure the DAG is active in the Airflow UI.")
        
    resp.raise_for_status()
    return resp.json()


# Event listener for mentions in the channel
@app.event("app_mention")
def handle_mention(event, say):
    text = event.get("text", "")
    thread_ts = event.get("ts")
    user_id = event.get("user")
    
    log.info("Received app_mention event from user %s in thread %s: %r", user_id, thread_ts, text)
    
    # 1. Acknowledge receipt
    say(
        text=f":brain: Reading your request, <@{user_id}>...",
        thread_ts=thread_ts
    )
    
    try:
        # 2. Query LLM to parse limit
        limit = parse_limit_with_llm(text)
        say(
            text=f":mag: Extraction engine parsed target: *{limit} screens*. Triggering Airflow pipeline...",
            thread_ts=thread_ts
        )
        
        # 3. Trigger DAG run
        dag_run = trigger_airflow_dag(limit)
        dag_run_id = dag_run.get("dag_run_id", "unknown")
        
        # 4. Success confirmation
        say(
            text=(
                f":white_check_mark: *RICO Pipeline successfully triggered!*\n"
                f"• `dag_run_id`: `{dag_run_id}`\n"
                f"• `LIMIT` parameter: `{limit}`\n"
                f"• State: `queued` / `running`\n\n"
                f"You can monitor the active run in the Airflow webserver dashboard."
            ),
            thread_ts=thread_ts
        )
        log.info("Successfully triggered DAG run %s with limit %d for user %s", dag_run_id, limit, user_id)
        
    except Exception as exc:
        log.error("Failed to execute backfill request: %s", exc, exc_info=True)
        say(
            text=(
                f":x: *Failed to execute backfill request!*\n"
                f"• *Reason:* `{str(exc)}`\n"
                f"Verify that Airflow, Postgres, and Ollama are fully running."
            ),
            thread_ts=thread_ts
        )


if __name__ == "__main__":
    log.info("Starting RICO Backfill Agent in Socket Mode...")
    try:
        handler = SocketModeHandler(app, SLACK_APP_TOKEN)
        log.info("Slack connection established! SocketModeHandler listening for mentions...")
        handler.start()
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt received. Shutting down Backfill Agent...")
    except Exception as e:
        log.critical("Failed to start SocketModeHandler: %s", e, exc_info=True)
