import sqlite3
from pathlib import Path
from .utils import now_iso

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
            error TEXT,
            recipient_group TEXT
        )""")
        con.execute("UPDATE jobs SET status='queued' WHERE status IN ('downloading','transcribing')")
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_slug_unique ON jobs(slug)")
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
