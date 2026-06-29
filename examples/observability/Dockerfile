# Build context is the repository root (see docker-compose.yml) so the app can
# import redis_fastapi from ./src at runtime.
FROM python:3.12-slim

WORKDIR /app

COPY examples/observability/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Source is mounted/copied so app.py can add ./src to sys.path.
COPY src ./src
COPY examples ./examples

EXPOSE 8000

# `opentelemetry-instrument` sets up the OTel SDK + OTLP export (from OTEL_*
# env) and installs the FastAPI HTTP-span instrumentation (Layer 1) before the
# app boots.  The library's .otel() call (Layer 2) then reuses those providers.
CMD ["opentelemetry-instrument", "uvicorn", "examples.observability.app:app", \
     "--host", "0.0.0.0", "--port", "8000"]
