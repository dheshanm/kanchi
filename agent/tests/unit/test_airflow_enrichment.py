import json
from datetime import datetime, timedelta, timezone

from database import TaskEventDB, TaskLatestDB
from models import TaskEvent
from services.airflow_enrichment_service import (
    AirflowEnrichmentService,
    reset_identity_cache,
)
from services.task_service import TaskService
from tests.base import DatabaseTestCase
from tests.unit.test_airflow_workload import SAMPLE_TOKEN, SAMPLE_WORKLOAD

DAG_ID = "generate_acqparams_dag"
AIRFLOW_TASK_ID = "generate_acqparams_task"
DISPLAY_NAME = "%s.%s" % (DAG_ID, AIRFLOW_TASK_ID)
CELERY_TASK_ID = "f2f123fb-7080-48de-bfe3-d4079d30ef66"


class AirflowEnrichmentTests(DatabaseTestCase):

    def setUp(self):
        super().setUp()
        reset_identity_cache()

    def tearDown(self):
        reset_identity_cache()
        super().tearDown()

    def _event(self, event_type, args=None, task_id=CELERY_TASK_ID, offset=0):
        return TaskEvent(
            task_id=task_id,
            task_name="execute_workload",
            event_type=event_type,
            timestamp=datetime.now(timezone.utc) + timedelta(seconds=offset),
            args=args if args is not None else [],
            kwargs={},
            hostname="celery@worker-cpu",
            routing_key="default",
            queue="default",
        )

    def test_rewrites_task_name_from_workload(self):
        event = self._event("task-received", [SAMPLE_WORKLOAD])

        recognised = AirflowEnrichmentService(self.session).enrich(event)

        self.assertTrue(recognised)
        self.assertEqual(event.task_name, DISPLAY_NAME)
        self.assertEqual(event.celery_task_name, "execute_workload")
        self.assertEqual(event.airflow_dag_id, DAG_ID)
        self.assertEqual(event.airflow_task_id, AIRFLOW_TASK_ID)
        self.assertEqual(event.airflow_try_number, 1)
        self.assertEqual(event.airflow_map_index, -1)
        self.assertEqual(event.airflow_meta["queue"], "default")
        self.assertTrue(event.is_airflow)

    def test_token_is_redacted_before_anything_stores_it(self):
        event = self._event("task-received", [SAMPLE_WORKLOAD])

        # The pydantic validator redacts on construction, before enrichment.
        self.assertNotIn(SAMPLE_TOKEN, json.dumps(event.args))

        AirflowEnrichmentService(self.session).enrich(event)
        saved = TaskService(self.session).save_task_event(event)

        self.assertNotIn(SAMPLE_TOKEN, json.dumps(saved.args))
        row = self.session.query(TaskLatestDB).filter_by(task_id=CELERY_TASK_ID).one()
        self.assertNotIn(SAMPLE_TOKEN, json.dumps(row.args))

    def test_identity_carries_to_later_events_without_args(self):
        service = AirflowEnrichmentService(self.session)
        task_service = TaskService(self.session)

        received = self._event("task-received", [SAMPLE_WORKLOAD], offset=0)
        service.enrich(received)
        task_service.save_task_event(received)

        # started/succeeded arrive from Celery with no args and the raw name.
        for index, event_type in enumerate(("task-started", "task-succeeded"), start=1):
            event = self._event(event_type, [], offset=index)
            self.assertTrue(service.enrich(event), event_type)
            self.assertEqual(event.task_name, DISPLAY_NAME, event_type)
            self.assertEqual(event.airflow_dag_id, DAG_ID, event_type)
            task_service.save_task_event(event)

        names = {
            row.task_name
            for row in self.session.query(TaskEventDB).filter_by(task_id=CELERY_TASK_ID)
        }
        self.assertEqual(names, {DISPLAY_NAME})

    def test_identity_recovers_from_database_after_restart(self):
        service = AirflowEnrichmentService(self.session)
        received = self._event("task-received", [SAMPLE_WORKLOAD])
        service.enrich(received)
        TaskService(self.session).save_task_event(received)

        # A restart loses the in-process cache; the stored row must be enough.
        reset_identity_cache()

        # Terminal Celery events carry no task name, and after a restart the
        # monitor's in-memory state cannot supply one either, so it arrives as
        # "unknown" rather than "execute_workload".
        later = self._event("task-succeeded", [])
        later.task_name = "unknown"
        self.assertTrue(AirflowEnrichmentService(self.session).enrich(later))
        self.assertEqual(later.task_name, DISPLAY_NAME)
        self.assertEqual(later.airflow_run_id, received.airflow_run_id)

    def test_snapshot_keeps_identity_on_later_events(self):
        service = AirflowEnrichmentService(self.session)
        task_service = TaskService(self.session)

        received = self._event("task-received", [SAMPLE_WORKLOAD], offset=0)
        service.enrich(received)
        task_service.save_task_event(received)

        succeeded = self._event("task-succeeded", [], offset=5)
        service.enrich(succeeded)
        task_service.save_task_event(succeeded)

        row = self.session.query(TaskLatestDB).filter_by(task_id=CELERY_TASK_ID).one()
        self.assertEqual(row.event_type, "task-succeeded")
        self.assertEqual(row.airflow_dag_id, DAG_ID)
        self.assertEqual(row.airflow_task_id, AIRFLOW_TASK_ID)
        self.assertEqual(row.celery_task_name, "execute_workload")

    def test_non_airflow_tasks_are_untouched(self):
        event = TaskEvent(
            task_id="plain-task",
            task_name="tasks.send_email",
            event_type="task-received",
            timestamp=datetime.now(timezone.utc),
            args=["someone@example.org"],
            kwargs={"subject": "hi"},
        )

        recognised = AirflowEnrichmentService(self.session).enrich(event)

        self.assertFalse(recognised)
        self.assertEqual(event.task_name, "tasks.send_email")
        self.assertIsNone(event.celery_task_name)
        self.assertIsNone(event.airflow_dag_id)
        self.assertEqual(event.args, ["someone@example.org"])
        self.assertFalse(event.is_airflow)

    def test_round_trip_through_database_preserves_identity(self):
        service = AirflowEnrichmentService(self.session)
        task_service = TaskService(self.session)

        event = self._event("task-received", [SAMPLE_WORKLOAD])
        service.enrich(event)
        task_service.save_task_event(event)

        restored = task_service.get_task_events(CELERY_TASK_ID)[0]

        self.assertEqual(restored.task_name, DISPLAY_NAME)
        self.assertEqual(restored.celery_task_name, "execute_workload")
        self.assertEqual(restored.airflow_dag_id, DAG_ID)
        self.assertEqual(restored.airflow_run_id, event.airflow_run_id)
        self.assertEqual(restored.airflow_meta["dag_rel_path"],
                         "utility/generate_acqparams.py")

    def test_filters_by_dag_and_run(self):
        service = AirflowEnrichmentService(self.session)
        task_service = TaskService(self.session)

        event = self._event("task-received", [SAMPLE_WORKLOAD])
        service.enrich(event)
        task_service.save_task_event(event)

        other = TaskEvent(
            task_id="plain-task",
            task_name="tasks.send_email",
            event_type="task-received",
            timestamp=datetime.now(timezone.utc),
        )
        task_service.save_task_event(other)

        matched = task_service.get_recent_events(filters="dag:is:%s" % DAG_ID)
        self.assertEqual([e.task_id for e in matched["data"]], [CELERY_TASK_ID])

        matched = task_service.get_recent_events(filters="dag_task:is:%s" % AIRFLOW_TASK_ID)
        self.assertEqual([e.task_id for e in matched["data"]], [CELERY_TASK_ID])

        matched = task_service.get_recent_events(
            filters="run:is:%s" % event.airflow_run_id
        )
        self.assertEqual([e.task_id for e in matched["data"]], [CELERY_TASK_ID])

        matched = task_service.get_recent_events(filters="dag:is:no_such_dag")
        self.assertEqual(matched["data"], [])
