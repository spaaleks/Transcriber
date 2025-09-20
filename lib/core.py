import re, sqlite3, threading, time, traceback, smtplib, ssl
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path
from typing import Optional, Callable
import yt_dlp
from faster_whisper import WhisperModel
import ffmpeg  # binary must be on PATH

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# -------- DB --------
def db_conn(db_path: Path):
    con = sqlite3.connect(db_path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def db_init(db_path: Path):
    with db_conn(db_path) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            slug TEXT NOT NULL,
            url TEXT NOT NULL,
            status TEXT NOT NULL, -- queued|downloading|transcribing|done|error
            progress REAL NOT NULL,
            log TEXT NOT NULL,
            media_path TEXT,
            txt_path TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            error TEXT
        )""")
        cols = {r["name"] for r in con.execute("PRAGMA table_info(jobs)").fetchall()}
        if "recipient_group" not in cols:
            con.execute("ALTER TABLE jobs ADD COLUMN recipient_group TEXT DEFAULT NULL")
        con.execute("UPDATE jobs SET status='queued' WHERE status IN ('downloading','transcribing')")
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_slug_unique ON jobs(slug)")
        con.commit()

# -------- Utils --------
def now_iso() -> str:
    # Local time, no 'Z'
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def slugify(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w\s-]", "", s)
    s = re.sub(r"[\s_-]+", "-", s)
    s = re.sub(r"^-+|-+$", "", s)
    return s or "job"

def ensure_unique_slug(con, base_slug: str, data_dir: Path) -> str:
    slug = base_slug
    i = 2
    while True:
        exists_db = con.execute("SELECT 1 FROM jobs WHERE slug=?", (slug,)).fetchone() is not None
        exists_fs = (data_dir / slug).exists()
        if not exists_db and not exists_fs:
            return slug
        slug = f"{base_slug}-{i}"
        i += 1

def job_dir(base_dir: Path, slug: str) -> Path:
    p = base_dir / slug
    p.mkdir(parents=True, exist_ok=True)
    return p

def update_job(con, job_id: int, **fields):
    if not fields: return
    fields["updated_at"] = now_iso()
    cols = ", ".join(f"{k}=?" for k in fields.keys())
    vals = list(fields.values()) + [job_id]
    con.execute(f"UPDATE jobs SET {cols} WHERE id=?", vals)
    con.commit()

def append_log(con, job_id: int, msg: str):
    (prev,) = con.execute("SELECT log FROM jobs WHERE id=?", (job_id,)).fetchone()
    newlog = (prev or "") + f"[{now_iso()}] {msg}\n"
    update_job(con, job_id, log=newlog)

def media_duration_seconds(path: Path) -> Optional[float]:
    try:
        d = float(ffmpeg.probe(str(path))["format"]["duration"])
        return d if d > 0 else None
    except Exception:
        return None

def looks_complete_and_valid(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 1_000_000 and media_duration_seconds(path) is not None

def hhmmss(seconds: float) -> str:
    h = int(seconds // 3600); m = int((seconds % 3600) // 60); s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"

# -------- SMTP --------
def load_group_recipients(settings, group: str) -> list[str]:
    path = settings.recipients_dir / f"recipients_{group}.txt"
    return load_recipients(path)

def unique_preserve_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x); out.append(x)
    return out


def load_recipients(path: Path) -> list[str]:
    if not path.exists():
        return []
    recipients = []
    for line in path.read_text(encoding="utf-8").splitlines():
        addr = line.strip()
        if not addr or addr.startswith("#"):
            continue
        if EMAIL_RE.match(addr):
            recipients.append(addr)
    return recipients

def _ssl_context(ca_file: str | None, verify: bool) -> ssl.SSLContext:
    if verify:
        return ssl.create_default_context(cafile=ca_file) if ca_file else ssl.create_default_context()
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

def send_email_file(smtp_host: str, smtp_port: int, smtp_user: str | None, smtp_pass: str | None,
                    sender: str, to_addr: str, subject: str, body: str,
                    attachment_path: Path | None = None,
                    use_tls: bool = True, use_ssl: bool = False,
                    ca_file: str | None = None, verify: bool = True):
    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.set_content(body)
    if attachment_path:
        data = attachment_path.read_bytes()
        msg.add_attachment(data, maintype="text", subtype="plain", filename=attachment_path.name)

    if use_ssl:
        context = _ssl_context(ca_file, verify)
        with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context, timeout=30) as s:
            if smtp_user and smtp_pass:
                s.login(smtp_user, smtp_pass)
            s.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as s:
            if use_tls:
                s.starttls(context=_ssl_context(ca_file, verify))
            if smtp_user and smtp_pass:
                s.login(smtp_user, smtp_pass)
            s.send_message(msg)

def notify_recipients(settings, name: str, slug: str, txt_path: Path, log_cb, group: str = "none"):
    main_list = load_recipients(settings.recipients_file)
    group_list = load_group_recipients(settings, group)
    recs = unique_preserve_order([*group_list, *main_list])

    if not recs:
        log_cb(f"No recipients found for group '{group}' and main. Skipping email.")
        return
    if not (settings.smtp_host and settings.smtp_sender):
        log_cb("SMTP not configured (SMTP_HOST or SMTP_SENDER missing). Skipping email.")
        return

    sent = 0
    for addr in recs:
        try:
            subject = settings.mail_subject.format(name=name, slug=slug)
            body = settings.mail_body.format(name=name, slug=slug)
            sender = settings.smtp_from_header or settings.smtp_sender or ""
            send_email_file(
                smtp_host=settings.smtp_host,
                smtp_port=settings.smtp_port,
                smtp_user=settings.smtp_user,
                smtp_pass=settings.smtp_pass,
                sender=sender,
                to_addr=addr,
                subject=subject,
                body=body,
                attachment_path=txt_path,
                use_tls=settings.smtp_use_tls,
                use_ssl=settings.smtp_use_ssl,
                ca_file=settings.smtp_ca_file,
                verify=settings.smtp_verify,
            )
            sent += 1
            log_cb(f"Email sent to {addr}")
            time.sleep(0.3)
        except Exception as e:
            log_cb(f"Email FAILED to {addr}: {e}")
    log_cb(f"Email summary (group='{group}'): {sent}/{len(recs)} sent")

def first_recipient(path: Path) -> Optional[str]:
    recs = load_recipients(path)
    return recs[0] if recs else None

def smtp_smoke_test(settings) -> tuple[bool, str]:
    if not (settings.smtp_host and settings.smtp_sender):
        return False, "SMTP_HOST or SMTP_SENDER not configured"
    to_addr = first_recipient(settings.recipients_file)
    if not to_addr:
        return False, f"No recipients in {settings.recipients_file}"
    try:
        subject = f"[SMTP TEST] {settings.mail_subject.format(name='TEST', slug='test')}"
        body = "This is a Spal.Transcriber SMTP test.\nIf you received this, SMTP works."
        sender = settings.smtp_from_header or settings.smtp_sender or ""
        send_email_file(
            smtp_host=settings.smtp_host,
            smtp_port=settings.smtp_port,
            smtp_user=settings.smtp_user,
            smtp_pass=settings.smtp_pass,
            sender=sender,
            to_addr=to_addr,
            subject=subject,
            body=body,
            attachment_path=None,
            use_tls=settings.smtp_use_tls,
            use_ssl=settings.smtp_use_ssl,
            ca_file=settings.smtp_ca_file,
            verify=settings.smtp_verify,
        )
        return True, f"Sent test email to {to_addr}"
    except Exception as e:
        return False, f"SMTP test failed: {e}"

# -------- Download (resumable, format-agnostic) --------
class ResumeRejected(Exception): pass

def _existing_media_for_stem(stem_path: Path) -> Optional[Path]:
    # Find any complete media file with the given stem and a common extension
    for p in stem_path.parent.glob(stem_path.name + ".*"):
        if p.suffix.lower() in {".mp4", ".webm", ".m4a", ".mp3", ".wav", ".mkv", ".mov"} and p.is_file():
            if looks_complete_and_valid(p):
                return p
    return None

def _any_part_file_for_stem(stem_path: Path) -> Optional[Path]:
    # Find any .part file for this stem regardless of final extension
    for p in stem_path.parent.glob(stem_path.name + ".*.part"):
        if p.is_file():
            return p
    # yt-dlp may also use "<stem>.part" for some cases
    p = stem_path.with_suffix(".part")
    return p if p.exists() else None

def ydl_download_resumable(url: str, base_out_stem: Path, progress_cb: Callable[[float], None], allow_resume: bool) -> Path:
    def hook(d):
        if d.get("status") == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            done = d.get("downloaded_bytes") or 0
            if total > 0: progress_cb(min(1.0, done / total))
        elif d.get("status") == "finished":
            progress_cb(1.0)

    ydl_opts = {
        "outtmpl": str(base_out_stem),   # let yt-dlp decide extension
        "retries": 10,
        "merge_output_format": "mp4",
        "concurrent_fragment_downloads": 4,
        "continuedl": bool(allow_resume),
        "nopart": False,                 # keep .part for resume
        "quiet": True,
        "progress_hooks": [hook],
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            fn = Path(ydl.prepare_filename(info))
            # Try best-effort discovery if needed
            if not fn.exists():
                cand = None
                if "requested_downloads" in info:
                    cand = info["requested_downloads"][0].get("filepath")
                if cand:
                    fn = Path(cand)
            if not fn.exists():
                # fall back to scan
                found = _existing_media_for_stem(base_out_stem)
                if found:
                    fn = found
            if not fn.exists():
                raise RuntimeError("Download finished but output file not found.")
            return fn
    except yt_dlp.utils.DownloadError as e:
        msg = str(e).lower()
        if "http error 416" in msg or "requested range not satisfiable" in msg:
            raise ResumeRejected from e
        raise

def download_with_resume_and_validation(url: str, base_out_stem: Path,
                                        progress_cb: Callable[[float],None],
                                        log_cb: Callable[[str],None]) -> Path:
    # Reuse valid existing media for this stem regardless of extension
    existing = _existing_media_for_stem(base_out_stem)
    if existing:
        log_cb(f"Found existing valid media: {existing.name}, skipping download")
        progress_cb(1.0)
        return existing

    # Try resume if there is any .part
    try:
        part = _any_part_file_for_stem(base_out_stem)
        log_cb("Starting download" + (" with resume" if part else ""))
        final_media = ydl_download_resumable(url, base_out_stem, progress_cb, allow_resume=True)
    except ResumeRejected:
        log_cb("Server rejected resume (416). Retrying fresh once.")
        # remove stale parts
        try:
            p = _any_part_file_for_stem(base_out_stem)
            if p: p.unlink(missing_ok=True)
        except Exception:
            pass
        final_media = ydl_download_resumable(url, base_out_stem, progress_cb, allow_resume=False)

    if not looks_complete_and_valid(final_media):
        log_cb("Downloaded file failed validation. Redownloading fresh once.")
        try:
            Path(final_media).unlink(missing_ok=True)
            p = _any_part_file_for_stem(base_out_stem)
            if p: p.unlink(missing_ok=True)
        except Exception:
            pass
        final_media = ydl_download_resumable(url, base_out_stem, progress_cb, allow_resume=False)
        if not looks_complete_and_valid(final_media):
            raise RuntimeError("Media corrupted after retry.")
    return Path(final_media)

# -------- Transcription --------
def transcribe_to_txt(media_path: Path, txt_path: Path,
                      model_size: str, device: str, compute_type: str, cpu_threads: int,
                      progress_cb: Callable[[float],None], log_cb: Callable[[str],None],
                      lang_hint: Optional[str] = None):
    model = WhisperModel(model_size, device=device, compute_type=compute_type, cpu_threads=cpu_threads)
    segments, info = model.transcribe(
        str(media_path),
        language=lang_hint,
        task="transcribe",
        beam_size=1,
        vad_filter=False,
        condition_on_previous_text=False,
        word_timestamps=False,
    )
    dur = media_duration_seconds(media_path) or getattr(info, "duration", None) or None

    with open(txt_path, "w", encoding="utf-8") as txt_f:
        if getattr(info, "language", None) is not None:
            log_cb(f"Detected language: {info.language} (p={getattr(info,'language_probability',None)})")
        seen_end = 0.0
        for seg in segments:
            line = seg.text.strip()
            txt_f.write(line + "\n")
            if dur and seg.end:
                pct = min(100.0, float(seg.end) / float(dur) * 100.0)
                progress_cb(pct / 100.0)
                if seg.end - seen_end >= 2.0 or pct - (seen_end / dur * 100.0 if dur else 0) >= 0.5:
                    log_cb(f"{pct:5.1f}% [{hhmmss(seg.start)} → {hhmmss(seg.end)}] {line}")
                    seen_end = seg.end
    progress_cb(1.0)
    log_cb("Transcription completed. Writing finalized outputs.")

# -------- Worker --------
class Worker:
    def __init__(self, data_dir: Path, db_path: Path,
                 model_size: str, device: str, compute_type: str, cpu_threads: int,
                 settings=None):
        self.data_dir = data_dir
        self.db_path = db_path
        self.model_size = model_size
        self.device = device
        self.compute_type = compute_type
        self.cpu_threads = cpu_threads
        self.settings = settings
        self._lock = threading.Lock()
        self._thread = None
        self._stop = False

    def ensure(self):
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(target=self._loop, daemon=True)
                self._thread.start()

    def _loop(self):
        while not self._stop:
            with db_conn(self.db_path) as con:
                row = con.execute(
                    "SELECT * FROM jobs WHERE status='queued' ORDER BY id ASC LIMIT 1"
                ).fetchone()
            if not row:
                time.sleep(1.0); continue

            job_id = row["id"]; slug = row["slug"]
            jdir = job_dir(self.data_dir, slug)
            base_stem = jdir / slug
            txt_path = base_stem.with_suffix(".txt")

            # Determine source: uploaded media (row.media_path) or remote URL
            uploaded_media = Path(row["media_path"]) if row["media_path"] else None
            has_uploaded_media = bool(uploaded_media and uploaded_media.exists())
            final_media: Optional[Path] = None

            if has_uploaded_media:
                with db_conn(self.db_path) as con:
                    update_job(con, job_id, status="transcribing", media_path=str(uploaded_media))
                    append_log(con, job_id, f"Using uploaded media: {uploaded_media.name}")
                final_media = uploaded_media
            else:
                with db_conn(self.db_path) as con:
                    update_job(con, job_id, status="downloading", media_path=None)
                    append_log(con, job_id, "Job started or resumed.")

                # Download (format-agnostic)
                try:
                    def dl_cb(frac):
                        with db_conn(self.db_path) as con2:
                            update_job(con2, job_id, progress=frac * 50)
                    final_media = download_with_resume_and_validation(
                        row["url"], base_stem, dl_cb, lambda m: append_log(db_conn(self.db_path), job_id, m)
                    )
                    with db_conn(self.db_path) as con:
                        update_job(con, job_id, status="transcribing", media_path=str(final_media))
                        append_log(con, job_id, f"Download OK: {Path(final_media).name}")
                except Exception as e:
                    with db_conn(self.db_path) as con:
                        update_job(con, job_id, status="error", error=str(e))
                        append_log(con, job_id, f"Download error: {e}\n{traceback.format_exc()}")
                    continue

            # Transcribe
            try:
                def tr_cb(frac):
                    with db_conn(self.db_path) as con2:
                        update_job(con2, job_id, progress=50 + frac * 50)

                transcribe_to_txt(
                    final_media, txt_path,
                    self.model_size, self.device, self.compute_type, self.cpu_threads,
                    tr_cb, lambda m: append_log(db_conn(self.db_path), job_id, m),
                    lang_hint=None,
                )

                # Cleanup: delete only downloaded media; keep uploaded media by default
                if not has_uploaded_media:
                    try: Path(final_media).unlink(missing_ok=True)
                    except Exception: pass

                with db_conn(self.db_path) as con:
                    update_job(
                        con, job_id,
                        status="done",
                        txt_path=str(txt_path),
                        media_path=(str(final_media) if has_uploaded_media else None),
                        progress=100
                    )
                    kept = "kept (uploaded by user)" if has_uploaded_media else "deleted"
                    append_log(con, job_id, f"Transcription done. Media {kept}. TXT at {txt_path.name}")

                # Email notifications (conditional)
                try:
                    if self.settings and getattr(self.settings, "auto_send_email", False):
                        notify_recipients(self.settings, row["name"], slug, txt_path,
                                        lambda m: append_log(db_conn(self.db_path), job_id, m),
                                        group=row["recipient_group"])
                    else:
                        with db_conn(self.db_path) as con:
                            append_log(con, job_id, "Auto-send disabled (AUTO_SEND_EMAIL=0). Use 'Send mail' button.")
                except Exception as e:
                    with db_conn(self.db_path) as con:
                        append_log(con, job_id, f"Email notification error: {e}")

            except Exception as e:
                with db_conn(self.db_path) as con:
                    update_job(con, job_id, status="error", error=str(e))
                    append_log(con, job_id, f"Transcription error: {e}\n{traceback.format_exc()}")
