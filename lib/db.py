# lib/db.py
import sqlite3
from pathlib import Path
from .utils import now_iso

def db_conn(db_path: Path):
    con = sqlite3.connect(db_path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    return con

def db_init(db_path: Path):
    with db_conn(db_path) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            slug TEXT NOT NULL,
            url TEXT NOT NULL,
            status TEXT NOT NULL, -- queued|downloading|transcribing|done|error|canceled
            progress REAL NOT NULL,
            log TEXT NOT NULL,
            media_path TEXT,
            txt_path TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            error TEXT
        )""")
        # optional columns
        cols = {r["name"] for r in con.execute("PRAGMA table_info(jobs)").fetchall()}
        if "recipient_group" not in cols:
            con.execute("ALTER TABLE jobs ADD COLUMN recipient_group TEXT DEFAULT NULL")
        con.execute("UPDATE jobs SET status='queued' WHERE status IN ('downloading','transcribing')")
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_slug_unique ON jobs(slug)")

        # outbox with FK -> jobs(id) cascade
        con.execute("""
        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER, -- may be NULL for non-job mails
            to_addr TEXT NOT NULL,
            subject TEXT NOT NULL,
            body_text TEXT NOT NULL,
            body_html TEXT,
            attachment_path TEXT,
            status TEXT NOT NULL, -- queued|sending|sent|error
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            send_after TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
        )""")
        con.execute("CREATE INDEX IF NOT EXISTS idx_outbox_ready ON outbox(status, send_after)")

        # migrate existing outbox without FK -> recreate with FK
        fk = con.execute("PRAGMA foreign_key_list(outbox)").fetchall()
        if not fk:
            con.execute("ALTER TABLE outbox RENAME TO outbox_old")
            con.execute("""
            CREATE TABLE outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER,
                to_addr TEXT NOT NULL,
                subject TEXT NOT NULL,
                body_text TEXT NOT NULL,
                body_html TEXT,
                attachment_path TEXT,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                send_after TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
            )""")
            con.execute("""
            INSERT INTO outbox (id, job_id, to_addr, subject, body_text, body_html,
                                attachment_path, status, attempts, last_error,
                                send_after, created_at, updated_at)
            SELECT id, job_id, to_addr, subject, body_text, body_html,
                   attachment_path, status, attempts, last_error,
                   send_after, created_at, updated_at
            FROM outbox_old
            """)
            con.execute("DROP TABLE outbox_old")

        con.commit()

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
    if not fields:
        return
    fields["updated_at"] = now_iso()
    cols = ", ".join(f"{k}=?" for k in fields.keys())
    vals = list(fields.values()) + [job_id]
    con.execute(f"UPDATE jobs SET {cols} WHERE id=?", vals)
    con.commit()

def append_log(con, job_id: int, msg: str):
    row = con.execute("SELECT log FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not row:
        # job was deleted; nothing to log
        return
    prev = row["log"] or ""
    newlog = prev + f"[{now_iso()}] {msg}\n"
    update_job(con, job_id, log=newlog)
