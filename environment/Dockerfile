FROM python:3.12.15-slim-bookworm@sha256:34386ef0cb081344d7ec1c103ba398e6e9f64e9ab3a1509accc92a4e24a07258

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates coreutils git patch \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH=/app/huey \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app/huey
COPY huey/ /app/huey/

RUN python -c "import huey, sqlite3; assert huey.__version__ == '3.4.0'; assert huey.__file__.startswith('/app/huey/')"
