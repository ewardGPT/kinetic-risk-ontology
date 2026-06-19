FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECCODE=1 PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc libpq-dev curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY libs/kro_common /app/libs/kro_common
COPY services/kinetic-risk/pyproject.toml /app/services/kinetic-risk/pyproject.toml

RUN pip install --upgrade pip \
    && pip install -e /app/libs/kro_common \
    && pip install -e /app/services/kinetic-risk

COPY services/kinetic-risk/src /app/src

ENV PYTHONPATH=/app/src

CMD ["python", "-m", "kro_kinetic_risk"]
