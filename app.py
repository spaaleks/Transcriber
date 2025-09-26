#!/usr/bin/env python3
from pathlib import Path
import argparse
from flask import Flask
from lib.settings import Settings
from lib.db import db_init
from lib.worker import Worker
from lib.auth import requires_auth
from lib.outbox import Mailer
from routes import register_routes

def create_app(settings: Settings):
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    db_init(settings.db_path)

    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = settings.upload_max_mb * 1024 * 1024

    @app.before_request
    @requires_auth
    def global_protect():
        pass

    # Worker
    worker = Worker(
        data_dir=settings.data_dir,
        db_path=settings.db_path,
        model_size=settings.model_size,
        device=settings.device,
        compute_type=settings.compute_type,
        cpu_threads=settings.cpu_threads,
        settings=settings,
    )
    worker.ensure()

    # register routes (pass app + worker + settings)
    register_routes(app, worker, settings)

    mailer = Mailer(settings.db_path, settings)
    mailer.ensure()
    app.config["_mailer"] = mailer

    return app

def parse_args():
    parser = argparse.ArgumentParser(description="Spal.Transcriber")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--model", default=None)
    parser.add_argument("--device", default=None)
    parser.add_argument("--compute", default=None)
    parser.add_argument("--threads", type=int, default=None)
    return parser.parse_args()

if __name__ == "__main__":
    s = Settings.from_env()
    args = parse_args()
    if args.host: s.host = args.host
    if args.port: s.port = args.port
    if args.data_dir:
        s.data_dir = Path(args.data_dir).resolve()
        s.db_path = s.data_dir / "jobs.db"
    if args.model: s.model_size = args.model
    if args.device: s.device = args.device
    if args.compute: s.compute_type = args.compute
    if args.threads: s.cpu_threads = args.threads

    app = create_app(s)
    print(f"Listening on http://{s.host}:{s.port}")
    app.run(host=s.host, port=s.port, debug=False)
