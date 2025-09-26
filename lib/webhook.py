from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Optional, Dict

import requests

from .db import db_conn, append_log
from .utils import now_iso


def _headers(settings) -> Dict[str, str]:
    h = {"User-Agent": "Spal.Transcriber/1.0"}
    if getattr(settings, "webhook_bearer", None):
        h["Authorization"] = f"Bearer {settings.webhook_bearer}"
    return h


def send_transcript_webhook(
    settings,
    *,
    job_id: int,
    slug: str,
    name: str,
    txt_path: Path,
    created_at: Optional[str] = None,
    updated_at: Optional[str] = None,
    recipient_group: Optional[str] = None,
) -> None:
    # POST multipart/form-data to settings.webhook_url with:
    #  - field "metadata": JSON with job info
    #  - file  "file": the transcript text/plain
    # Retries with exponential backoff. Logs success/failure into job log.

    url = getattr(settings, "webhook_url", None)
    if not url:
        return

    metadata = {
        "slug": slug,
        "name": name,
        "job_id": job_id,
        "recipient_group": recipient_group,
        "created_at": created_at,
        "updated_at": updated_at or now_iso(),
        "source": "spal.transcriber",
        "status": "done",
        "filename": txt_path.name,
    }

    headers = _headers(settings)
    timeout = int(getattr(settings, "webhook_timeout", 15))
    verify = bool(getattr(settings, "webhook_verify", True))

    # read file once
    data_bytes = txt_path.read_bytes()

    attempts = 0
    max_attempts = 4
    backoff = 2  # seconds

    while attempts < max_attempts:
        attempts += 1
        try:
            files = {
                "file": (txt_path.name, data_bytes, "text/plain"),
            }
            data = {
                "metadata": json.dumps(metadata, ensure_ascii=False),
            }
            resp = requests.post(url, headers=headers, data=data, files=files, timeout=timeout, verify=verify)
            if 200 <= resp.status_code < 300:
                with db_conn(settings.db_path) as con:
                    append_log(con, job_id, f"Webhook delivered to {url} (HTTP {resp.status_code}).")
                return
            else:
                err = f"HTTP {resp.status_code}: {resp.text[:200]}"
                raise RuntimeError(err)
        except Exception as e:
            if attempts >= max_attempts:
                with db_conn(settings.db_path) as con:
                    append_log(con, job_id, f"Webhook FAILED after {attempts} attempts: {e}")
                return
            # backoff with jitter
            sleep_s = backoff * (1.5 ** (attempts - 1))
            time.sleep(sleep_s)
