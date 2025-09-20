from functools import wraps
from flask import request, Response
import os

# Load credentials from environment
AUTH_USER = os.environ.get("APP_AUTH_USER")
AUTH_PASS = os.environ.get("APP_AUTH_PASS")

def check_auth(username: str, password: str) -> bool:
    return username == AUTH_USER and password == AUTH_PASS

def authenticate() -> Response:
    return Response(
        "Authentication required.\n",
        401,
        {"WWW-Authenticate": 'Basic realm="Spal.Transcriber"'},
    )

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not AUTH_USER or not AUTH_PASS:
            return f(*args, **kwargs)

        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated
