# Dockerfile
FROM python:3.11-slim

# System deps (ffmpeg for probing and audio)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
  && rm -rf /var/lib/apt/lists/*

# Runtime env
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Workdir
WORKDIR /app

# Copy and install Python deps first (better layer caching)
COPY ../lib /app/lib
COPY ../routes /app/routes
COPY ../static /app/static
COPY ../templates /app/templates
COPY ../app.py /app/app.py
COPY ../requirements.txt /app/requirements.txt

RUN python -m pip install --upgrade pip \
 && python -m pip install -r requirements.txt

# Expose default port
EXPOSE 15551

# Default envs; override at runtime
ENV APP_HOST=0.0.0.0 \
    PORT=15551

CMD python -u app.py --host "${APP_HOST:-0.0.0.0}" --port "${PORT:-15551}"
