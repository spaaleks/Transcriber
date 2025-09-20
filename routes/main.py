from flask import Blueprint, render_template, jsonify, abort
from lib.core import db_conn

def main_bp(worker, settings):
    bp = Blueprint("main", __name__)

    @bp.get("/")
    def index():
        with db_conn(settings.db_path) as con:
            jobs = con.execute("SELECT * FROM jobs ORDER BY id DESC").fetchall()
        return render_template("index.html", jobs=jobs, settings=settings, groups=settings.available_groups)

    @bp.get("/api/jobs")
    def api_jobs():
        with db_conn(settings.db_path) as con:
            jobs = [dict(x) for x in con.execute("SELECT id,status,progress FROM jobs ORDER BY id DESC").fetchall()]
        return jsonify({"jobs": jobs})

    return bp
