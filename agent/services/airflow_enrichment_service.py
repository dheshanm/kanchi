"""Give Airflow task events a real identity before anything else sees them.

Airflow's Celery executor submits every task instance as one Celery task named
``execute_workload``. Without this step, the task registry, daily statistics,
filters, and the task list all key on that single name, and the only thing
distinguishing one neuroimaging DAG from another sits unread inside a JSON blob
in the arguments.

Enrichment runs *before* the registry and statistics are updated, so every
consumer downstream sees ``<dag_id>.<task_id>``. The original Celery name is
preserved in ``celery_task_name``.

Only ``task-sent`` and ``task-received`` carry arguments; the later lifecycle
events (started, succeeded, failed) do not. Identity is therefore cached per
Celery task id and re-applied to those later events, with a database lookup as
the fallback after a restart.
"""

import logging
from collections import OrderedDict
from threading import Lock
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from database import TaskEventDB, TaskLatestDB
from models import TaskEvent
from utils.airflow_workload import (
    AirflowWorkload,
    looks_like_airflow_task_name,
    parse_workload,
)

logger = logging.getLogger(__name__)

# Bounded cache of Celery task id -> decoded identity, so the events that follow
# task-received do not each cost a database round trip.
_CACHE_MAX_ENTRIES = 20000
_identity_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_cache_lock = Lock()


def _cache_put(task_id: str, identity: Dict[str, Any]) -> None:
    if not task_id:
        return
    with _cache_lock:
        _identity_cache[task_id] = identity
        _identity_cache.move_to_end(task_id)
        while len(_identity_cache) > _CACHE_MAX_ENTRIES:
            _identity_cache.popitem(last=False)


def _cache_get(task_id: str) -> Optional[Dict[str, Any]]:
    if not task_id:
        return None
    with _cache_lock:
        identity = _identity_cache.get(task_id)
        if identity is not None:
            _identity_cache.move_to_end(task_id)
        return identity


def reset_identity_cache() -> None:
    """Clear the process-local identity cache (used by tests)."""
    with _cache_lock:
        _identity_cache.clear()


class AirflowEnrichmentService:
    """Decode Airflow task-instance identity onto incoming task events."""

    def __init__(self, session: Session):
        self.session = session

    def enrich(self, task_event: TaskEvent) -> bool:
        """Populate the Airflow fields on *task_event* in place.

        Returns True when the event was recognised as an Airflow workload.
        """
        try:
            workload = parse_workload(task_event.args, task_event.kwargs)
            if workload is not None:
                identity = self._identity_from_workload(workload)
                _cache_put(task_event.task_id, identity)
                self._apply(task_event, identity)
                return True

            identity = self._recover_identity(task_event)
            if identity:
                self._apply(task_event, identity)
                return True
        except Exception as exc:  # pylint: disable=broad-except
            # Enrichment is presentation only; never let it drop an event.
            logger.error(
                "Airflow enrichment failed for task %s: %s",
                task_event.task_id,
                exc,
                exc_info=True,
            )

        return False

    def _identity_from_workload(self, workload: AirflowWorkload) -> Dict[str, Any]:
        return {
            "airflow_dag_id": workload.dag_id,
            "airflow_task_id": workload.task_id,
            "airflow_run_id": workload.run_id,
            "airflow_try_number": workload.try_number,
            "airflow_map_index": workload.map_index,
            "airflow_meta": workload.to_meta(),
            "display_name": workload.display_name,
        }

    def _apply(self, task_event: TaskEvent, identity: Dict[str, Any]) -> None:
        display_name = identity.get("display_name")
        if not display_name:
            return

        if not task_event.celery_task_name:
            # Later lifecycle events arrive with the name already rewritten when
            # they are replayed from the database; keep the first Celery name.
            task_event.celery_task_name = (
                task_event.task_name
                if task_event.task_name != display_name
                else identity.get("celery_task_name")
            )

        task_event.airflow_dag_id = identity.get("airflow_dag_id")
        task_event.airflow_task_id = identity.get("airflow_task_id")
        task_event.airflow_run_id = identity.get("airflow_run_id")
        task_event.airflow_try_number = identity.get("airflow_try_number")
        task_event.airflow_map_index = identity.get("airflow_map_index")
        task_event.airflow_meta = identity.get("airflow_meta") or None
        task_event.task_name = display_name

    def _recover_identity(self, task_event: TaskEvent) -> Optional[Dict[str, Any]]:
        """Find identity for an event that arrived without arguments."""
        cached = _cache_get(task_event.task_id)
        if cached:
            return cached

        # A database lookup is only worth it for tasks that could plausibly be
        # Airflow workloads -- otherwise every event of every unrelated Celery
        # task would pay for a query.
        #
        # Terminal Celery events carry no task name; Kanchi normally fills it in
        # from the monitor's in-memory state. After a restart an in-flight task
        # is not in that state, so the name arrives as "unknown" -- which still
        # has to be looked up, or the task regresses to `unknown` with no DAG.
        name = task_event.task_name
        if not looks_like_airflow_task_name(name) and name not in (None, "", "unknown"):
            return None

        identity = self._lookup_identity(task_event.task_id)
        if identity:
            _cache_put(task_event.task_id, identity)
        return identity

    def _lookup_identity(self, task_id: str) -> Optional[Dict[str, Any]]:
        if not task_id:
            return None

        row = (
            self.session.query(TaskLatestDB)
            .filter(TaskLatestDB.task_id == task_id)
            .filter(TaskLatestDB.airflow_dag_id.isnot(None))
            .one_or_none()
        )

        if row is None:
            row = (
                self.session.query(TaskEventDB)
                .filter(TaskEventDB.task_id == task_id)
                .filter(TaskEventDB.airflow_dag_id.isnot(None))
                .order_by(TaskEventDB.timestamp.desc(), TaskEventDB.id.desc())
                .first()
            )

        if row is None:
            return None

        return {
            "airflow_dag_id": row.airflow_dag_id,
            "airflow_task_id": row.airflow_task_id,
            "airflow_run_id": row.airflow_run_id,
            "airflow_try_number": row.airflow_try_number,
            "airflow_map_index": row.airflow_map_index,
            "airflow_meta": row.airflow_meta,
            "celery_task_name": row.celery_task_name,
            "display_name": "%s.%s" % (row.airflow_dag_id, row.airflow_task_id),
        }


__all__ = ["AirflowEnrichmentService", "reset_identity_cache"]
