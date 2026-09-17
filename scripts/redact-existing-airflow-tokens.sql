-- Redact Airflow worker JWTs from rows written before token redaction existed.
--
-- Kanchi strips the `token` field out of every Airflow workload at ingestion,
-- and also on read, so nothing is exposed through the API or UI. Rows captured
-- by an older build still hold the token at rest, though. This rewrites them.
--
-- The tokens are short-lived (minutes), so this is hygiene rather than an
-- active exposure. Review the SELECT before running the UPDATE.
--
--   psql "$DATABASE_URL" -f scripts/redact-existing-airflow-tokens.sql
--
-- PostgreSQL. `args` holds a JSON array whose single element is a JSON *string*,
-- so the inner quotes are backslash-escaped at the ::text level.

\set pattern '\\\\"token\\\\":\\\\"[^\\\\"]*\\\\"'
\set replacement '\\\\"token\\\\":\\\\"<redacted by kanchi>\\\\"'

-- How many rows still carry a token?
SELECT 'task_events' AS table_name, count(*) AS rows_with_token
FROM task_events WHERE args::text ~ :'pattern'
UNION ALL
SELECT 'task_latest', count(*)
FROM task_latest WHERE args::text ~ :'pattern';

BEGIN;

UPDATE task_events
SET args = regexp_replace(args::text, :'pattern', :'replacement', 'g')::json
WHERE args::text ~ :'pattern';

UPDATE task_latest
SET args = regexp_replace(args::text, :'pattern', :'replacement', 'g')::json
WHERE args::text ~ :'pattern';

-- Should report 0 for both.
SELECT 'task_events' AS table_name, count(*) AS rows_still_with_token
FROM task_events WHERE args::text ~ :'pattern'
UNION ALL
SELECT 'task_latest', count(*)
FROM task_latest WHERE args::text ~ :'pattern';

COMMIT;
