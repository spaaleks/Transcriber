from flask import Blueprint, request, redirect, url_for, abort, render_template, send_from_directory
from pathlib import Path
from werkzeug.utils import secure_filename
from lib.core import (
    db_conn, slugify, ensure_unique_slug, job_dir, now_iso,
    notify_recipients, append_log
)

def jobs_bp(worker, settings):
    bp = Blueprint("jobs", __name__)

    @bp.post("/upload")
    def upload_job():
        f = request.files.get("file")
        name = (request.form.get("name") or "").strip()
        if not f or not name:
            abort(400, "file and name required")

        base = slugify(name)
        with db_conn(settings.db_path) as con:
            slug = ensure_unique_slug(con, base, settings.data_dir)
            d = settings.data_dir / slug
            d.mkdir(parents=True, exist_ok=True)
            filename = secure_filename(f.filename or "media.bin")
            if "." not in filename:
                filename += ".bin"
            media_path = d / filename
            f.save(media_path)

            group = request.form.get("group").strip()

            created = now_iso()
            con.execute(
                "INSERT INTO jobs (name, slug, url, status, progress, log, media_path, created_at, updated_at, recipient_group) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (name, slug, "", "queued", 0, "", str(media_path), created, created, group),
            )
            con.commit()

        worker.ensure()
        return redirect(url_for("main.index"))

    @bp.post("/jobs")
    def create_job():
        url = (request.form.get("url") or "").strip()
        name = (request.form.get("name") or "").strip()
        group = request.form.get("group").strip()

        if not url or not name:
            abort(400, "url and name required")

        base = slugify(name)
        with db_conn(settings.db_path) as con:
            slug = ensure_unique_slug(con, base, settings.data_dir)
            created = now_iso()
            con.execute(
                "INSERT INTO jobs (name, slug, url, status, progress, log, created_at, updated_at, recipient_group) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (name, slug, url, "queued", 0, "", created, created, group),
            )
            con.commit()
        (settings.data_dir / slug).mkdir(parents=True, exist_ok=True)
        worker.ensure()
        return redirect(url_for("main.index"))

    @bp.post("/jobs/<int:job_id>/delete")
    def delete_job(job_id: int):
        with db_conn(settings.db_path) as con:
            row = con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row:
                abort(404)
            if row["txt_path"]:
                try:
                    Path(row["txt_path"]).unlink(missing_ok=True)
                except Exception:
                    pass
            if row["media_path"]:
                try:
                    Path(row["media_path"]).unlink(missing_ok=True)
                except Exception:
                    pass
            try:
                d = job_dir(settings.data_dir, row["slug"])
                if d.exists() and not any(d.iterdir()):
                    d.rmdir()
            except Exception:
                pass
            con.execute("DELETE FROM jobs WHERE id=?", (job_id,))
            con.commit()
        return redirect(url_for("main.index"))

    @bp.get("/api/logs/<int:job_id>")
    def api_log(job_id: int):
        with db_conn(settings.db_path) as con:
            row = con.execute(
                "SELECT id, status, progress, updated_at, log FROM jobs WHERE id=?", (job_id,)
            ).fetchone()
            if not row:
                abort(404)
        return {
            "id": row["id"],
            "status": row["status"],
            "progress": float(row["progress"]),
            "updated_at": row["updated_at"],
            "log": row["log"] or "",
        }

    @bp.get("/logs/<int:job_id>")
    def view_log(job_id: int):
        return render_template("log.html", job_id=job_id, settings=settings)

    @bp.get("/files/<slug>/<path:filename>")
    def download_file(slug: str, filename: str):
        d = settings.data_dir / slug
        return send_from_directory(d, filename, as_attachment=True)

    @bp.post("/jobs/<int:job_id>/sendmail")
    def send_mail(job_id: int):
        with db_conn(settings.db_path) as con:
            row = con.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
            if not row:
                abort(404)
            if row["status"] != "done" or not row["txt_path"]:
                abort(400, "job not finished or transcript missing")
        try:
            notify_recipients(
                settings=settings,
                name=row["name"],
                slug=row["slug"],
                txt_path=Path(row["txt_path"]),
                log_cb=lambda m: append_log(db_conn(settings.db_path), job_id, m),
                group=row["recipient_group"],
            )
        except Exception as e:
            with db_conn(settings.db_path) as con:
                append_log(con, job_id, f"Manual email send error: {e}")
        return redirect(url_for("jobs.view_log", job_id=job_id))

    return bp
