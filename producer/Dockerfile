# syntax=docker/dockerfile:1
# Producer / migration / load-test image. Build context is the repo root.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY producer/requirements.txt /app/producer/requirements.txt
RUN pip install --no-cache-dir -r /app/producer/requirements.txt

COPY producer/ /app/producer/
COPY loadtest/ /app/loadtest/
COPY sql/ /app/sql/

# Non-root by default - the image only needs to read its own code.
RUN useradd --create-home --uid 10001 streampulse && chown -R streampulse:streampulse /app
USER streampulse

CMD ["python", "producer/producer.py"]
