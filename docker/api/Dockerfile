# ===================================== =====
# API IMAGE (FastAPI)
# Lightweight image for REST API service
# ==========================================
FROM python:3.11-slim

WORKDIR /app

# Copiar solo requirements de API (mínimos)
COPY deps/requirements-api.txt .

# No necesitamos gcc ni libpq-dev porque usamos psycopg2-binary
RUN pip install --no-cache-dir -r requirements-api.txt

# Copiar código fuente
COPY src/ /app/src/

# Usar variable de entorno para controlar --reload
# En desarrollo: docker-compose puede setear UVICORN_FLAGS="--reload"
# En producción: dejar vacío o no setear
CMD uvicorn src.api.main:app --host 0.0.0.0 --port 8000 ${UVICORN_FLAGS:-}