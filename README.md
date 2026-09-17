# Kanchi

Kanchi is a real-time Celery task monitoring (and management) system with an enjoyable user interface. It provides insights into task execution, worker health, and task statistics.

## Features

- Real-time task monitoring via WebSocket
- Task filtering and searching (date range, status, name, worker, full-text)
- Task retry tracking and orphan detection
- Daily task statistics and history
- Worker health monitoring
- Auto-migrations with Alembic
- **Apache Airflow awareness** — see [Airflow support](#airflow-support)

## Airflow support

Airflow's Celery executor submits every task instance through a single Celery
task named `execute_workload`. A generic Celery monitor therefore shows one
repeated name for the whole deployment, with the only distinguishing
information buried in a JSON argument.

Kanchi decodes that payload and treats the task instance as a first-class
entity:

- **Real task names.** Tasks are listed as `<dag_id>.<task_id>` instead of
  `execute_workload`, so the task registry, daily statistics, failure grouping
  and search all work per DAG task rather than lumping everything together. The
  original Celery name is kept in `celery_task_name`.
- **Airflow filters.** `dag:`, `dag_task:` and `run:` join the existing
  `state:`, `worker:`, `task:`, `queue:` and `id:` filters — for example
  `dag:is:eddy_dag` or `run:starts:manual__2026`.
- **Deep links.** Task detail views link to the DAG, the DAG run, and the task
  instance's logs in the Airflow UI. Set the browser-facing Airflow URL with
  `AIRFLOW_BASE_URL` (or edit `airflow.base_url` in settings); links are hidden
  when it is unset.
- **Task-instance metadata.** Run ID, attempt, map index, pool slots, priority
  weight, Airflow queue, DAG file, bundle and log path are shown alongside the
  Celery view.
- **Token redaction.** Each workload carries a short-lived JWT that
  authenticates the worker against the Airflow execution API. Kanchi strips it
  before anything is written to the database or broadcast over the WebSocket.
- **Reruns deferred to Airflow.** Resubmitting the captured payload through
  Celery would bypass the scheduler and use an expired token, so Kanchi blocks
  rerun for Airflow tasks and points you at clearing the task instance in
  Airflow instead.

Non-Airflow Celery tasks keep their own names, their Airflow columns stay NULL,
and the Airflow panel and links do not appear for them.

One behaviour does apply to every task: redaction is keyed on the argument
**name**, so a `token`, `jwt` or `access_token` value in any task's arguments is
replaced with `<redacted by kanchi>` before storage. Kanchi cannot recover the
original, so rerunning such a task is held for review with the redacted field
flagged for replacement rather than silently resubmitting the placeholder.

Tested against Airflow 3.3 (`type: ExecuteTask` workloads). Older
`execute_command` payloads are recognised by shape where the identity is
present.

### Upgrading an existing Kanchi database

The schema migration runs automatically on startup. Two things are worth knowing
about rows captured by an earlier build:

- **Tokens at rest.** Redaction also runs on read, so old rows never expose a
  token through the API or UI, but the value is still stored. The tokens expire
  within minutes, so this is hygiene rather than an active exposure. Run
  `scripts/redact-existing-airflow-tokens.sql` to rewrite them in place.
- **No backfill.** Old rows keep `execute_workload` as their name and have NULL
  Airflow columns; only tasks seen after the upgrade are decoded.

### Known limits

- `task_name` columns are `VARCHAR(255)`. Airflow permits 250-character
  `dag_id` and `task_id` values, so a `<dag_id>.<task_id>` pair longer than 255
  characters would be rejected by PostgreSQL and the event dropped. Normal
  naming is nowhere near this.

## Screenshots

![Dashboard overview](.github/images/dashboard-overview.png)
![Failed tasks table](.github/images/failed-tasks-table.png)
![Task detail panel](.github/images/task-detail-panel.png)
![Workflow automation](.github/images/workflow-automation.png)
![Task retry chain](.github/images/task-retry-chain.png)
![Retry task modal](.github/images/retry-task-modal.png)

## Backend-hosted UI

The Docker image serves the generated Nuxt UI from FastAPI at `/ui`. The backend
serves the UI, API, and WebSocket endpoint from the same process. API,
WebSocket, frontend, and public-prefix URLs are injected into the generated UI at
request time, so they can be changed without rebuilding the Nuxt assets.

Defaults use same-origin relative paths:

```bash
export NUXT_PUBLIC_API_URL=
export NUXT_PUBLIC_WS_URL=/ws
export NUXT_PUBLIC_FRONTEND_URL=/ui
```

For reverse proxies that expose Kanchi below a path prefix, set
`KANCHI_ROOT_PATH` to the public prefix:

```bash
export KANCHI_ROOT_PATH=/kanchi
```

With the defaults above, that makes the frontend use `/kanchi/api/...`,
`/kanchi/ws`, and `/kanchi/ui/...`, generated assets use
`/kanchi/ui/_nuxt/...`, and FastAPI treats `/kanchi` as the ASGI root path.
`NUXT_PUBLIC_URL_PREFIX` is still supported for existing deployments and also
configures the root path when `KANCHI_ROOT_PATH` is not set.

## Quick Start (Docker Compose)

Run Kanchi using pre-built images from Docker Hub. No repository cloning required—just set a few environment variables and start the container. The container runs one FastAPI process and serves the generated UI at `/ui`.

### Prerequisites

- Docker Engine + Docker Compose plug-in installed on your host. Follow the [official Docker installation guide](https://docs.docker.com/engine/install/) for your OS.
- Running Celery broker (RabbitMQ or Redis) instance (and optionally PostgreSQL) reachable from the host.

1. **Create a docker-compose.yaml file**

   Create a new directory and save the following as `docker-compose.yaml`:

   ```yaml
   services:
     kanchi:
       image: getkanchi/kanchi:latest
       container_name: kanchi
       ports:
         - "8765:8765"
         - "3000:8765"
       environment:
         # Required: Your Celery broker connection string
         CELERY_BROKER_URL: ${CELERY_BROKER_URL}

         # Optional: Database (defaults to SQLite)
         DATABASE_URL: ${DATABASE_URL:-sqlite:////data/kanchi.db}

         # Optional: Logging and development
         LOG_LEVEL: ${LOG_LEVEL:-INFO}
         DEVELOPMENT_MODE: ${DEVELOPMENT_MODE:-false}
         ENABLE_PICKLE_SERIALIZATION: ${ENABLE_PICKLE_SERIALIZATION:-false}
         NUXT_PUBLIC_API_URL: ${NUXT_PUBLIC_API_URL:-}
         NUXT_PUBLIC_WS_URL: ${NUXT_PUBLIC_WS_URL:-/ws}
         NUXT_PUBLIC_FRONTEND_URL: ${NUXT_PUBLIC_FRONTEND_URL:-/ui}
         KANCHI_ROOT_PATH: ${KANCHI_ROOT_PATH:-}
         NUXT_PUBLIC_URL_PREFIX: ${NUXT_PUBLIC_URL_PREFIX:-}

         # Optional: Authentication (disabled by default)
         AUTH_ENABLED: ${AUTH_ENABLED:-false}

         # Optional: Basic HTTP authentication
         AUTH_BASIC_ENABLED: ${AUTH_BASIC_ENABLED:-false}
         BASIC_AUTH_USERNAME: ${BASIC_AUTH_USERNAME}
         BASIC_AUTH_PASSWORD_HASH: ${BASIC_AUTH_PASSWORD_HASH}

         # Optional: OAuth providers
         AUTH_GOOGLE_ENABLED: ${AUTH_GOOGLE_ENABLED:-false}
         GOOGLE_CLIENT_ID: ${GOOGLE_CLIENT_ID}
         GOOGLE_CLIENT_SECRET: ${GOOGLE_CLIENT_SECRET}
         AUTH_GITHUB_ENABLED: ${AUTH_GITHUB_ENABLED:-false}
         GITHUB_CLIENT_ID: ${GITHUB_CLIENT_ID}
         GITHUB_CLIENT_SECRET: ${GITHUB_CLIENT_SECRET}
         OAUTH_REDIRECT_BASE_URL: ${OAUTH_REDIRECT_BASE_URL}

         # Optional: Email restrictions for OAuth
         ALLOWED_EMAIL_PATTERNS: ${ALLOWED_EMAIL_PATTERNS}

         # Optional: CORS and host controls
         ALLOWED_ORIGINS: ${ALLOWED_ORIGINS}
         ALLOWED_HOSTS: ${ALLOWED_HOSTS}

         # Optional: Security tokens (generate with: openssl rand -hex 32)
         SESSION_SECRET_KEY: ${SESSION_SECRET_KEY}
         TOKEN_SECRET_KEY: ${TOKEN_SECRET_KEY}
       volumes:
         - kanchi-data:/data
       restart: unless-stopped
       healthcheck:
         test:
           [
             "CMD",
             "python",
             "-c",
             "import urllib.request; urllib.request.urlopen('http://localhost:8765/api/health').read()",
           ]
         interval: 30s
         timeout: 10s
         retries: 3
         start_period: 40s

   volumes:
     kanchi-data:
   ```

2. **Set required environment values**

   At minimum, export `CELERY_BROKER_URL` or place it in a `.env` file alongside `docker-compose.yaml`. Example:

   ```bash
   # For RabbitMQ:
   export CELERY_BROKER_URL=amqp://user:pass@rabbitmq-host:5672//

   # For Redis:
   export CELERY_BROKER_URL=redis://localhost:6379/0
   ```

   Optional overrides (see docker-compose.yaml for all available options):

   ```bash
   export DATABASE_URL=postgresql://user:pass@postgres-host:5432/kanchi
   export LOG_LEVEL=INFO
   export DEVELOPMENT_MODE=false
   export ENABLE_PICKLE_SERIALIZATION=false
   export NUXT_PUBLIC_API_URL=
   export NUXT_PUBLIC_WS_URL=/ws
   export NUXT_PUBLIC_FRONTEND_URL=/ui
   export KANCHI_ROOT_PATH=/kanchi
   # Authentication / security (all optional)
   export AUTH_ENABLED=true
   export AUTH_BASIC_ENABLED=true
   export BASIC_AUTH_USERNAME=kanchi-admin
   export BASIC_AUTH_PASSWORD_HASH=pbkdf2_sha256$260000$mysalt$N8Dk...  # see below

   # OAuth
   export AUTH_GOOGLE_ENABLED=true
   export GOOGLE_CLIENT_ID=...
   export GOOGLE_CLIENT_SECRET=...
   export AUTH_GITHUB_ENABLED=true
   export GITHUB_CLIENT_ID=...
   export GITHUB_CLIENT_SECRET=...
   export OAUTH_REDIRECT_BASE_URL=https://your-kanchi-host

   # Allowed email addresses for OAuth logins (wildcards supported)
   export ALLOWED_EMAIL_PATTERNS='*@example.com,*@example.org'

   # Trusted-header SSO behind an authenticating reverse proxy (see below)
   export AUTH_TRUSTED_HEADER_ENABLED=true
   export AUTH_TRUSTED_HEADER_SECRET=$(openssl rand -hex 32)
   export AUTH_TRUSTED_HEADER_LOGOUT_URL=/outpost.goauthentik.io/sign_out

   # CORS and host controls
   export ALLOWED_ORIGINS=https://your-kanchi-host,http://localhost:8765,http://localhost:3000
   export ALLOWED_HOSTS=your-kanchi-host,localhost,127.0.0.1

   # Token secrets (must be non-default in production)
   export SESSION_SECRET_KEY=$(openssl rand -hex 32)
   export TOKEN_SECRET_KEY=$(openssl rand -hex 32)

# Pickle payloads (advanced; defaults to off)
export ENABLE_PICKLE_SERIALIZATION=false

# Airflow deep links: the browser-facing Airflow URL (optional)
export AIRFLOW_BASE_URL=https://airflow.example.org
   ```

3. **Start or update Kanchi in one command**

   ```bash
   docker compose up -d --pull always
   ```

   Re-run the same command to pull the latest image and restart the container.

4. **Visit the app**

   - Frontend: `http://localhost:8765/ui`
   - Legacy frontend port mapping: `http://localhost:3000/ui`
   - API / Docs: `http://localhost:8765`

5. **Optional commands**

   ```bash
   docker compose logs -f kanchi      # Tail logs
   docker compose down                # Stop and remove the container
   docker compose restart kanchi      # Restart the container
   docker compose pull                # Pull latest image without restarting
   ```

Kanchi expects a Celery broker (RabbitMQ or Redis) and (if desired) PostgreSQL to be managed separately—point `CELERY_BROKER_URL` and `DATABASE_URL` to the infrastructure you already run.

Migrations run automatically on startup.

### Pickle payloads (opt-in)

Kanchi rejects pickle-serialized messages by default for safety. If your Celery producers emit `application/x-python-serialize`, set `ENABLE_PICKLE_SERIALIZATION=true` only when you fully trust all producers and the broker. See `pickle.md` for a short rundown.

### Authentication

Authentication is opt-in. When `AUTH_ENABLED=false` (the default) anyone who can reach the backend may read metrics and connect over WebSockets. Enable authentication to require access tokens for all API routes and WebSocket connections.

#### Basic HTTP authentication

1. Generate a PBKDF2 hash for the password (for example using Python):

   ```bash
   python - <<'PY'
   import os, base64, hashlib

   password = os.environ.get('KANCHI_BASIC_PASSWORD', 'change-me').encode('utf-8')
   salt = base64.b64encode(os.urandom(16)).decode('ascii').strip('=')
   iterations = 260000
   dk = hashlib.pbkdf2_hmac('sha256', password, salt.encode('utf-8'), iterations)
   print(f"pbkdf2_sha256${iterations}${salt}${base64.b64encode(dk).decode('ascii')}")
   PY
   ```

2. Set `AUTH_ENABLED=true`, `AUTH_BASIC_ENABLED=true`, `BASIC_AUTH_USERNAME`, and `BASIC_AUTH_PASSWORD_HASH` (or `BASIC_AUTH_PASSWORD` for local testing only).

#### OAuth (Google, GitHub)

1. Configure `AUTH_GOOGLE_ENABLED` and/or `AUTH_GITHUB_ENABLED` with the provider credentials.
2. Set `OAUTH_REDIRECT_BASE_URL` to the publicly reachable backend URL (e.g., `https://kanchi.example.com`).
3. Add allowed email patterns via `ALLOWED_EMAIL_PATTERNS` to restrict who can sign in.
4. The frontend exposes convenient buttons for OAuth providers once enabled.

#### Trusted proxy headers (SSO gateway)

When Kanchi sits behind a reverse proxy that already authenticates users (for
example nginx with Authentik forward auth, oauth2-proxy or Authelia), Kanchi can
accept the identity the proxy forwards instead of showing its own login page.

1. Set `AUTH_ENABLED=true` and `AUTH_TRUSTED_HEADER_ENABLED=true`.
2. Set `AUTH_TRUSTED_HEADER_SECRET` to a long random value and configure the
   proxy to add it as the `X-Neuroflow-Proxy-Secret` header on every request
   it forwards **after** authenticating the user. Kanchi ignores the identity
   headers unless this secret matches, so a client that can reach Kanchi
   without going through the proxy cannot impersonate anyone. Trusted-header
   mode stays off while the secret is empty.
3. The proxy forwards the identity in `X-authentik-username` (required),
   `X-authentik-email` and `X-authentik-name`. Other header names can be set
   with `AUTH_TRUSTED_HEADER_USERNAME`, `AUTH_TRUSTED_HEADER_EMAIL` and
   `AUTH_TRUSTED_HEADER_NAME`; `AUTH_TRUSTED_HEADER_SECRET_HEADER` renames the
   secret header. When no email is forwarded, Kanchi uses
   `<username>@<AUTH_TRUSTED_HEADER_EMAIL_DOMAIN>` (default `sso.local`).
4. Set `AUTH_TRUSTED_HEADER_LOGOUT_URL` to the proxy's sign-out URL (for
   Authentik's embedded outpost: `/outpost.goauthentik.io/sign_out`). The UI
   sends the browser there after signing out; without it the very next request
   would sign the user straight back in.

The frontend calls `POST /api/auth/header/login` on load, receives Kanchi's
usual access and refresh tokens, and everything else (WebSocket auth, token
refresh, `ALLOWED_EMAIL_PATTERNS`) works unchanged. Accounts are keyed by email,
so a person who previously signed in with basic auth or OAuth keeps the same
account. Basic auth and OAuth can stay enabled alongside this mode as a
fallback for direct (non-proxied) access.

## Local Development

### Prerequisites

- Python 3.8+
- Poetry
- Node.js 20+
- Celery broker (RabbitMQ or Redis)

### Installation

```bash
cd agent && poetry install
cd ../frontend && npm install
```

### Run

```bash
# Use our makefile:
make dev

# Or manually:

# Terminal 1: Backend
cd agent && poetry run python app.py

# Terminal 2: Frontend
cd frontend && npm run dev
```

### Testing Environment

```bash
cd scripts/test-celery-app
make start          # Start RabbitMQ, Redis, Workers
make test-mixed     # Generate test tasks
```

## Contributing

### Backend

```bash
cd agent
poetry run black .              # Format
poetry run ruff check .         # Lint
poetry run alembic revision --autogenerate -m "description"  # Migration
```

### Frontend

```bash
cd frontend
npm run build                   # Build
npx swagger-typescript-api generate -p http://localhost:8765/openapi.json -o app/src/types -n api.ts --http-client axios
```

## Getting Help

Have questions, feedback, or want to connect with other Kanchi users? Join our community on Discord:

[![Discord](https://img.shields.io/badge/Discord-Join%20us-5865F2?logo=discord&logoColor=white)](https://discord.gg/gSp9wsu3k)

## License

Licensed under [MIT](LICENSE)
