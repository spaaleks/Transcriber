from flask import Blueprint, render_template, current_app
from lib.emailer import smtp_smoke_test
from lib.utils import now_iso

def smtp_bp(worker, settings):
    bp = Blueprint("smtp", __name__)

    @bp.record_once
    def init_smtp_result(setup_state):
        app = setup_state.app
        app.config.setdefault(
            "_SMTP_TEST_RESULT", {"ok": None, "message": None, "ts": None}
        )

    @bp.post("/smtp/test")
    def smtp_test():
        ok, msg = smtp_smoke_test(settings)
        current_app.config["_SMTP_TEST_RESULT"] = {
            "ok": ok,
            "message": msg,
            "ts": now_iso(),
        }
        return render_template("smtp_test.html", ok=ok, msg=msg, settings=settings)

    @bp.get("/smtp/status")
    def smtp_status():
        r = current_app.config.get("_SMTP_TEST_RESULT", {})
        return render_template("smtp_status.html", r=r, settings=settings)

    return bp
