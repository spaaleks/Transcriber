import os
import time
import random
import traceback
from pathlib import Path
from typing import Optional, Callable

from .db import append_log, db_conn
from .utils import now_iso
from .emailer import send_email


def _cfg_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except Exception:
        return default


# Rate limiting and retry configuration
RATE_PER_MIN   = _cfg_int("MAIL_RATE_PER_MIN", 60)       # average messages per minute
BURST          = _cfg_int("MAIL_BURST", 30)              # token bucket size
RETRY_BASE_SEC = _cfg_int("MAIL_RETRY_BASE_SEC", 30)     # backoff base
RETRY_MAX_SEC  = _cfg_int("MAIL_RETRY_MAX_SEC", 3600)    # max backoff
SMTP_CONC      = _cfg_int("MAIL_SMTP_CONCURRENCY", 1)    # parallel senders (usually 1)


class TokenBucket:
    def __init__(self, rate_per_min: int, burst: int):
        self.rate = max(0.001, rate_per_min / 60.0)
        self.burst = max(1, burst)
        self.tokens = float(self.burst)
        self.t = time.monotonic()

    def take(self) -> bool:
        now = time.monotonic()
        self.tokens = min(self.burst, self.tokens + (now - self.t) * self.rate)
        self.t = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


def enqueue_email(db_path: Path, *, job_id: Optional[int], to_addr: str,
                  subject: str, body_text: str, body_html: Optional[str],
                  attachment_path: Optional[Path]) -> int:
    """
    Queue an email for rate-limited delivery. If job_id is provided, it is stored
    as a foreign key (expect ON DELETE CASCADE in schema).
    """
    now = now_iso()
    with db_conn(db_path) as con:
        con.execute("""
            INSERT INTO outbox (job_id, to_addr, subject, body_text, body_html,
                                attachment_path, status, attempts, last_error,
                                send_after, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, 'queued', 0, NULL, ?, ?, ?)
        """, (job_id, to_addr, subject, body_text, body_html or None,
              str(attachment_path) if attachment_path else None,
              now, now, now))
        con.commit()
        (oid,) = con.execute("SELECT last_insert_rowid()").fetchone()
        return oid


class Mailer:
    def __init__(self, db_path: Path, settings, log: Optional[Callable[[str], None]] = None):
        self.db_path = db_path
        self.settings = settings
        self.bucket = TokenBucket(RATE_PER_MIN, BURST)
        self._stop = False
        self._threads = []
        self._log = log or (lambda m: None)

    def ensure(self):
        """Start background sender threads."""
        import threading
        self._threads = [t for t in self._threads if t.is_alive()]
        need = max(1, SMTP_CONC) - len(self._threads)
        for _ in range(need):
            t = threading.Thread(target=self._loop, daemon=True)
            t.start()
            self._threads.append(t)

    def _claim(self) -> Optional[dict]:
        with db_conn(self.db_path) as con:
            con.isolation_level = None
            try:
                con.execute("BEGIN IMMEDIATE")
                row = con.execute("""
                    SELECT * FROM outbox
                    WHERE status='queued' AND send_after <= ?
                    ORDER BY id ASC LIMIT 1
                """, (now_iso(),)).fetchone()
                if not row:
                    con.execute("COMMIT")
                    return None
                changed = con.execute(
                    "UPDATE outbox SET status='sending', updated_at=? WHERE id=? AND status='queued'",
                    (now_iso(), row["id"])
                ).rowcount
                con.execute("COMMIT")
                return dict(row) if changed == 1 else None
            except Exception:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass
                return None

    def _fail(self, item: dict, msg: str, attempts: int):
        # Mark send attempt as failed and reschedule with exponential backoff.
        # Also append a per-job log line in the same transaction if the job still exists.

        delay = min(RETRY_MAX_SEC, RETRY_BASE_SEC * (2 ** min(attempts, 8)))
        jitter = delay * 0.2 * (random.random() - 0.5)
        when_epoch = time.time() + delay + jitter
        when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(when_epoch))
        log_line = f"Email to {item['to_addr']} failed: {msg[:120]}... Retrying at {when}"

        with db_conn(self.db_path) as con:
            con.isolation_level = None
            try:
                con.execute("BEGIN IMMEDIATE")
                con.execute("""
                  UPDATE outbox SET status='queued', attempts=?, last_error=?, send_after=?, updated_at=?
                  WHERE id=?""",
                  (attempts, msg[:500], when, now_iso(), item["id"]))
                job_id = item.get("job_id")
                if job_id is not None:
                    # If job was deleted, this UPDATE affects 0 rows. That is fine.
                    append_log(con, job_id, log_line)
                con.execute("COMMIT")
            except Exception:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass

    def _ok(self, item: dict):
        # Mark as sent. Append per-job log in the same transaction if the job still exists.
        log_line = f"Email sent to {item['to_addr']}"
        with db_conn(self.db_path) as con:
            con.isolation_level = None
            try:
                con.execute("BEGIN IMMEDIATE")
                con.execute("UPDATE outbox SET status='sent', updated_at=? WHERE id=?",
                            (now_iso(), item["id"]))
                job_id = item.get("job_id")
                if job_id is not None:
                    append_log(con, job_id, log_line)
                con.execute("COMMIT")
            except Exception:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass

    def _loop(self):
        while not self._stop:
            item = self._claim()
            if not item:
                time.sleep(0.5)
                continue

            # token-bucket rate limit
            if not self.bucket.take():
                time.sleep(0.5)
                self._fail(item, "rate-limit defer", item["attempts"])
                continue

            try:
                attach = Path(item["attachment_path"]) if item["attachment_path"] else None
                send_email(
                    settings=self.settings,
                    to_addr=item["to_addr"],
                    subject=item["subject"],
                    body_text=item["body_text"],
                    attachment_path=attach,
                    body_html=item["body_html"],
                )
                self._ok(item)
            except Exception as e:
                self._log(f"outbox send error id={item['id']}: {e}\n{traceback.format_exc()}")
                self._fail(item, str(e), item["attempts"] + 1)
