FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
      gcc libpq-dev curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY libs/kro_common /app/libs/kro_common
COPY services/entity-resolution/pyproject.toml /app/services/entity-resolution/pyproject.toml

RUN pip install --upgrade pip \
    && pip install -e /app/libs/kro_common \
    && pip install -e /app/services/entity-resolution

COPY services/entity-resolution/src /app/src

ENV PYTHONPATH=/app/src

CMD ["python", "-m", "kro_entity_resolution"]
