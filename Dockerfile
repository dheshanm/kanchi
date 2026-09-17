# Base images come from the ECR Public mirror of the Docker Official Images
# rather than Docker Hub: Hub pulls intermittently fail with TLS handshake
# timeouts and draw on an anonymous rate limit shared by every machine behind
# the lab NAT. Same digests, different registry.
FROM public.ecr.aws/docker/library/node:20-alpine AS frontend-builder

WORKDIR /app/frontend

COPY frontend/package*.json ./
RUN npm ci

COPY frontend/ .

ARG NUXT_PUBLIC_KANCHI_VERSION=dev
ENV NUXT_PUBLIC_KANCHI_VERSION=${NUXT_PUBLIC_KANCHI_VERSION}
ENV NUXT_APP_BASE_URL=/ui/
RUN npm run generate

FROM public.ecr.aws/docker/library/python:3.12-slim

WORKDIR /app

COPY agent/pyproject.toml agent/poetry.lock* ./agent/
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && pip install poetry && \
    cd agent && \
    poetry config virtualenvs.create false && \
    poetry install --without dev --extras "db-postgres db-postgres-async db-mysql db-mysql-native" && \
    apt-get purge -y gcc pkg-config && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

COPY agent/ ./agent/
COPY --from=frontend-builder /app/frontend/.output/public ./agent/ui

ENV WS_HOST=0.0.0.0
ENV WS_PORT=8765
ENV LOG_LEVEL=INFO
ENV FRONTEND_DIST_DIR=/app/agent/ui

EXPOSE 8765

WORKDIR /app/agent

CMD ["python", "app.py"]
