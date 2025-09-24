# lib/worker.py
import os
import threading
import time
import traceback
from pathlib import Path
from typing import Optional

from .db import db_conn, update_job, append_log, job_dir
from .download import download_with_resume_and_validation
from .transcribe import transcribe_to_txt
from .emailer import notify_recipients


class JobAborted(Exception):
    pass


class Worker:
    def __init__(self, data_dir: Path, db_path: Path,
                 model_size: str, device: str, compute_type: str, cpu_threads: int,
                 settings=None, concurrency: Optional[int] = None):
        self.data_dir = data_dir
        self.db_path = db_path
        self.model_size = model_size
        self.device = device
        self.compute_type = compute_type
        self.cpu_threads = cpu_threads
        self.settings = settings
        self.concurrency = concurrency or int(os.environ.get("WORKER_CONCURRENCY", "1"))
        self._lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._stop = False

    # ---------- public ----------

    def ensure(self):
        with self._lock:
            self._threads = [t for t in self._threads if t.is_alive()]
            need = max(0, self.concurrency - len(self._threads))
            for _ in range(need):
                t = threading.Thread(target=self._loop, daemon=True)
                t.start()
                self._threads.append(t)

    # ---------- loop ----------

    def _loop(self):
        while not self._stop:
            row = self._claim_next_job()
            if not row:
                time.sleep(1.0)
                continue
            try:
                self._process_job(row)
            except Exception as e:
                # never let the worker thread die
                try:
                    self._log(row["id"], f"Worker unexpected error: {e}\n{traceback.format_exc()}")
                except Exception:
                    pass

    # ---------- orchestration per job ----------

    def _process_job(self, row: dict):
        job_id = row["id"]
        slug = row["slug"]
        name = row["name"]

        self._log(job_id, "Job claimed by worker.")
        self._set(job_id, progress=0)

        jdir = job_dir(self.data_dir, slug)
        base_stem = jdir / slug
        txt_path = base_stem.with_suffix(".txt")

        uploaded_media = Path(row["media_path"]) if row.get("media_path") else None
        has_uploaded_media = bool(uploaded_media and uploaded_media.exists())

        if has_uploaded_media:
            self._abort_check(job_id)
            self._set(job_id, status="transcribing", media_path=str(uploaded_media))
            self._log(job_id, f"Using uploaded media: {uploaded_media.name}")
            final_media = uploaded_media
        else:
            final_media = self._download(job_id, row["url"], base_stem)

        try:
            self._transcribe(job_id, final_media, txt_path)
        except JobAborted as a:
            self._log(job_id, f"Job aborted during transcription: {a}")
            return
        except Exception as e:
            self._set(job_id, status="error", error=str(e))
            self._log(job_id, f"Transcription error: {e}\n{traceback.format_exc()}")
            return

        if not has_uploaded_media:
            try:
                Path(final_media).unlink(missing_ok=True)
            except Exception:
                pass

        self._set(job_id,
                  status="done",
                  txt_path=str(txt_path),
                  media_path=(str(final_media) if has_uploaded_media else None),
                  progress=100)
        kept = "kept (uploaded by user)" if has_uploaded_media else "deleted"
        self._log(job_id, f"Transcription done. Media {kept}. TXT at {txt_path.name}")

        self._maybe_notify(job_id, name, slug, txt_path, row.get("recipient_group") or "none")

    # ---------- phases ----------

    def _download(self, job_id: int, url: str, base_stem: Path) -> Path:
        self._log(job_id, "Starting download.")
        try:
            def dl_cb(frac: float):
                self._abort_check(job_id)
                with db_conn(self.db_path) as con2:
                    update_job(con2, job_id, progress=frac * 50)

            final_media = download_with_resume_and_validation(
                url, base_stem, dl_cb, lambda m: self._log(job_id, m)
            )
            self._set(job_id, status="transcribing", media_path=str(final_media))
            self._log(job_id, f"Download OK: {Path(final_media).name}")
            return final_media
        except JobAborted as a:
            self._log(job_id, f"Job aborted during download: {a}")
            raise
        except Exception as e:
            self._set(job_id, status="error", error=str(e))
            self._log(job_id, f"Download error: {e}\n{traceback.format_exc()}")
            raise

    def _transcribe(self, job_id: int, media_path: Path, txt_path: Path) -> None:
        def tr_cb(frac: float):
            self._abort_check(job_id)
            with db_conn(self.db_path) as con2:
                update_job(con2, job_id, progress=50 + frac * 50)

        transcribe_to_txt(
            media_path, txt_path,
            self.model_size, self.device, self.compute_type, self.cpu_threads,
            tr_cb, lambda m: self._log(job_id, m),
            lang_hint=None,
        )

    def _maybe_notify(self, job_id: int, name: str, slug: str, txt_path: Path, group: str) -> None:
        try:
            if self.settings and getattr(self.settings, "auto_send_email", False):
                notify_recipients(self.settings, name, slug, txt_path,
                                  lambda m: self._log(job_id, m), group=group)
            else:
                self._log(job_id, "Auto-send disabled (AUTO_SEND_EMAIL=0). Use 'Send mail' button.")
        except Exception as e:
            self._log(job_id, f"Email notification error: {e}")

    # ---------- helpers ----------

    def _claim_next_job(self) -> Optional[dict]:
        """Atomically claim the next queued job by flipping status queued->downloading."""
        with db_conn(self.db_path) as con:
            con.isolation_level = None
            try:
                con.execute("BEGIN IMMEDIATE")
                row = con.execute(
                    "SELECT * FROM jobs WHERE status='queued' ORDER BY id ASC LIMIT 1"
                ).fetchone()
                if not row:
                    con.execute("COMMIT")
                    return None
                claimed = con.execute(
                    "UPDATE jobs SET status='downloading' WHERE id=? AND status='queued'",
                    (row["id"],)
                ).rowcount
                con.execute("COMMIT")
                return dict(row) if claimed == 1 else None
            except Exception:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass
                return None

    def _abort_check(self, job_id: int):
        with db_conn(self.db_path) as con:
            r = con.execute("SELECT status FROM jobs WHERE id=?", (job_id,)).fetchone()
            if r is None:
                raise JobAborted("deleted")
            if r["status"] in ("canceled",):
                raise JobAborted(r["status"])

    def _log(self, job_id: int, msg: str):
        with db_conn(self.db_path) as con:
            append_log(con, job_id, msg)

    def _set(self, job_id: int, **fields):
        with db_conn(self.db_path) as con:
            update_job(con, job_id, **fields)
