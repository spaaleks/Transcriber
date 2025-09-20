from .jobs import jobs_bp
from .smtp import smtp_bp
from .main import main_bp

def register_routes(app, worker, settings):
    # Attach blueprints with context
    app.register_blueprint(main_bp(worker, settings))
    app.register_blueprint(jobs_bp(worker, settings))
    app.register_blueprint(smtp_bp(worker, settings))
