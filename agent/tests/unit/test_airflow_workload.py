import json
import unittest

from utils.airflow_workload import (
    REDACTED,
    AirflowWorkload,
    looks_like_airflow_task_name,
    parse_workload,
    redact_secrets,
)

# Verbatim payload captured from an Airflow 3.3 Celery worker, with the JWT
# shortened. The real token is ~440 characters.
SAMPLE_TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIwMWEwYTZkNC0wMGEyIn0.KileoRM1ePNV"
SAMPLE_WORKLOAD = json.dumps({
    "token": SAMPLE_TOKEN,
    "dag_rel_path": "utility/generate_acqparams.py",
    "bundle_info": {"name": "dags-folder", "version": None},
    "log_path": (
        "dag_id=generate_acqparams_dag"
        "/run_id=manual__2026-09-15T20:48:26.750392+00:00"
        "/task_id=generate_acqparams_task/attempt=1.log"
    ),
    "ti": {
        "id": "01a0a6d4-00a2-7d27-8336-eeaebce54cc1",
        "task_id": "generate_acqparams_task",
        "dag_id": "generate_acqparams_dag",
        "run_id": "manual__2026-09-15T20:48:26.750392+00:00",
        "try_number": 1,
        "dag_version_id": "01a0a6ca-793b-7e0b-8cf0-8e48583de3e3",
        "map_index": -1,
        "hostname": "",
        "context_carrier": {},
        "queue": "default",
        "pool_slots": 1,
        "priority_weight": 1,
    },
    "sentry_integration": "sentry_sdk.integrations.celery.CeleryIntegration",
    "type": "ExecuteTask",
})


class ParseWorkloadTests(unittest.TestCase):

    def test_parses_complete_payload(self):
        workload = parse_workload([SAMPLE_WORKLOAD], {})

        self.assertIsNotNone(workload)
        self.assertEqual(workload.dag_id, "generate_acqparams_dag")
        self.assertEqual(workload.task_id, "generate_acqparams_task")
        self.assertEqual(
            workload.run_id, "manual__2026-09-15T20:48:26.750392+00:00"
        )
        self.assertEqual(workload.try_number, 1)
        self.assertEqual(workload.map_index, -1)
        self.assertEqual(workload.ti_id, "01a0a6d4-00a2-7d27-8336-eeaebce54cc1")
        self.assertEqual(workload.queue, "default")
        self.assertEqual(workload.pool_slots, 1)
        self.assertEqual(workload.priority_weight, 1)
        self.assertEqual(workload.dag_rel_path, "utility/generate_acqparams.py")
        self.assertEqual(workload.bundle_name, "dags-folder")
        self.assertEqual(workload.workload_type, "ExecuteTask")
        self.assertFalse(workload.partial)
        self.assertFalse(workload.is_mapped)
        self.assertEqual(
            workload.display_name, "generate_acqparams_dag.generate_acqparams_task"
        )

    def test_parses_already_decoded_payload(self):
        workload = parse_workload([json.loads(SAMPLE_WORKLOAD)], {})

        self.assertIsNotNone(workload)
        self.assertEqual(workload.dag_id, "generate_acqparams_dag")

    def test_recovers_identity_from_truncated_payload(self):
        # Celery truncates argsrepr/kwargsrepr, which can cut the JSON inside the
        # `ti` block. `log_path` is emitted earlier and survives.
        truncated = SAMPLE_WORKLOAD[:SAMPLE_WORKLOAD.index('"try_number"')]
        with self.assertRaises(ValueError):
            json.loads(truncated)

        workload = parse_workload([truncated], {})

        self.assertIsNotNone(workload)
        self.assertEqual(workload.dag_id, "generate_acqparams_dag")
        self.assertEqual(workload.task_id, "generate_acqparams_task")
        self.assertEqual(
            workload.run_id, "manual__2026-09-15T20:48:26.750392+00:00"
        )
        self.assertEqual(workload.try_number, 1)  # from log_path attempt=1
        self.assertTrue(workload.partial)

    def test_recovers_identity_when_truncated_before_ti_block(self):
        cut = SAMPLE_WORKLOAD.index('"ti"')
        workload = parse_workload([SAMPLE_WORKLOAD[:cut]], {})

        self.assertIsNotNone(workload)
        self.assertEqual(workload.dag_id, "generate_acqparams_dag")
        self.assertEqual(workload.task_id, "generate_acqparams_task")
        self.assertTrue(workload.partial)

    def test_reads_map_index_from_log_path(self):
        payload = SAMPLE_WORKLOAD.replace(
            "/task_id=generate_acqparams_task/attempt=1.log",
            "/task_id=generate_acqparams_task/map_index=3/attempt=2.log",
        )
        cut = payload.index('"ti"')

        workload = parse_workload([payload[:cut]], {})

        self.assertEqual(workload.map_index, 3)
        self.assertEqual(workload.try_number, 2)
        self.assertTrue(workload.is_mapped)

    def test_ignores_non_airflow_tasks(self):
        self.assertIsNone(parse_workload(["some-value", 42], {"foo": "bar"}))
        self.assertIsNone(parse_workload([], {}))
        self.assertIsNone(parse_workload(None, None))
        self.assertIsNone(parse_workload(['{"not": "a workload"}'], {}))

    def test_ignores_payload_without_dag_identity(self):
        payload = json.dumps({"type": "ExecuteTask", "ti": {"try_number": 1}})
        self.assertIsNone(parse_workload([payload], {}))

    def test_accepts_workload_passed_as_keyword(self):
        workload = parse_workload([], {"workload": SAMPLE_WORKLOAD})
        self.assertIsNotNone(workload)
        self.assertEqual(workload.dag_id, "generate_acqparams_dag")

    def test_to_meta_drops_empty_values(self):
        meta = AirflowWorkload(dag_id="d", task_id="t", queue="cpu").to_meta()
        self.assertEqual(meta, {"queue": "cpu"})


class RedactSecretsTests(unittest.TestCase):

    def test_redacts_token_in_raw_json_string(self):
        redacted, changed = redact_secrets([SAMPLE_WORKLOAD])

        self.assertTrue(changed)
        self.assertNotIn(SAMPLE_TOKEN, redacted[0])
        self.assertIn(REDACTED, redacted[0])
        # Everything else must survive, and the result must still be valid JSON.
        decoded = json.loads(redacted[0])
        self.assertEqual(decoded["token"], REDACTED)
        self.assertEqual(decoded["ti"]["dag_id"], "generate_acqparams_dag")

    def test_redacted_payload_still_parses(self):
        redacted, _ = redact_secrets([SAMPLE_WORKLOAD])
        workload = parse_workload(redacted, {})

        self.assertIsNotNone(workload)
        self.assertEqual(workload.dag_id, "generate_acqparams_dag")

    def test_redacts_token_in_decoded_mapping(self):
        redacted, changed = redact_secrets({"token": SAMPLE_TOKEN, "keep": 1})

        self.assertTrue(changed)
        self.assertEqual(redacted["token"], REDACTED)
        self.assertEqual(redacted["keep"], 1)

    def test_redacts_token_in_truncated_json(self):
        truncated = SAMPLE_WORKLOAD[:SAMPLE_WORKLOAD.index('"ti"')]
        redacted, changed = redact_secrets([truncated])

        self.assertTrue(changed)
        self.assertNotIn(SAMPLE_TOKEN, redacted[0])

    def test_leaves_unrelated_payloads_untouched(self):
        payload = {"args": ["a", 1, None], "nested": {"value": "token-ish"}}
        redacted, changed = redact_secrets(payload)

        self.assertFalse(changed)
        self.assertEqual(redacted, payload)

    def test_does_not_redact_empty_token(self):
        redacted, changed = redact_secrets({"token": ""})

        self.assertFalse(changed)
        self.assertEqual(redacted["token"], "")


class TaskNameTests(unittest.TestCase):

    def test_recognises_airflow_entry_points(self):
        self.assertTrue(looks_like_airflow_task_name("execute_workload"))
        self.assertTrue(looks_like_airflow_task_name(
            "airflow.providers.celery.executors.celery_executor_utils.execute_workload"
        ))
        self.assertFalse(looks_like_airflow_task_name("tasks.send_email"))
        self.assertFalse(looks_like_airflow_task_name(None))


if __name__ == "__main__":
    unittest.main()
