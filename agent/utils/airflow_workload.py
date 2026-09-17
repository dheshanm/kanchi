"""Recognise and decode Apache Airflow workloads carried by Celery tasks.

Airflow's Celery executor submits every task instance through a single Celery
task -- ``execute_workload`` -- whose sole positional argument is a JSON string
describing the work. From Celery's point of view every DAG in the deployment is
the same task with the same name, so a generic Celery monitor shows a wall of
identical rows.

This module pulls the identity back out of that payload, so Kanchi can present
``<dag_id>.<task_id>`` instead of ``execute_workload`` and offer deep links into
the Airflow UI.

A representative payload (Airflow 3.x, ``type: ExecuteTask``)::

    {
      "token": "<short-lived JWT>",
      "dag_rel_path": "utility/generate_acqparams.py",
      "bundle_info": {"name": "dags-folder", "version": null},
      "log_path": "dag_id=.../run_id=.../task_id=.../attempt=1.log",
      "ti": {"id": "...", "task_id": "...", "dag_id": "...", "run_id": "...",
             "try_number": 1, "map_index": -1, "queue": "default", ...},
      "type": "ExecuteTask"
    }

Two things make this harder than a single ``json.loads``:

* The ``token`` is a signed JWT that authenticates the worker against the
  Airflow execution API. It must never be persisted or broadcast, so it is
  redacted before the payload reaches the database or a browser.
* Celery truncates over-long ``argsrepr``/``kwargsrepr`` values, which can cut
  the JSON mid-object. ``log_path`` sits ahead of the ``ti`` block, so a regex
  fallback can still recover the full identity from a truncated payload.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# Celery task names Airflow uses to ship a task instance to a worker. Matching is
# a hint only -- detection is driven by payload shape, because the name arrives
# either bare or fully qualified depending on the Airflow version.
AIRFLOW_CELERY_TASK_NAMES = frozenset({
    "execute_workload",
    "airflow.providers.celery.executors.celery_executor_utils.execute_workload",
    "airflow.executors.celery_executor.execute_command",
    "airflow.executors.celery_executor_utils.execute_command",
})

REDACTED = "<redacted by kanchi>"

# Keys whose values are credentials rather than task metadata.
SECRET_KEYS = frozenset({"token", "jwt", "access_token"})

_SECRET_KEY_ALTERNATION = "|".join(sorted(SECRET_KEYS))
_SECRET_JSON_RE = re.compile(
    r'("(?:%s)"\s*:\s*")(?:\\.|[^"\\])*(")' % _SECRET_KEY_ALTERNATION
)

# dag_id=<dag>/run_id=<run>/task_id=<task>[/map_index=<n>]/attempt=<n>.log
_LOG_PATH_RE = re.compile(
    r"dag_id=(?P<dag_id>[^/]+)"
    r"/run_id=(?P<run_id>[^/]+)"
    r"/task_id=(?P<task_id>[^/]+)"
    r"(?:/map_index=(?P<map_index>-?\d+))?"
    r"(?:/attempt=(?P<try_number>\d+)\.log)?"
)


def _json_string_field(payload: str, key: str) -> Optional[str]:
    match = re.search(r'"%s"\s*:\s*"((?:\\.|[^"\\])*)"' % re.escape(key), payload)
    if not match:
        return None
    try:
        return json.loads('"%s"' % match.group(1))
    except ValueError:
        return match.group(1)


def _json_int_field(payload: str, key: str) -> Optional[int]:
    match = re.search(r'"%s"\s*:\s*(-?\d+)' % re.escape(key), payload)
    return int(match.group(1)) if match else None


@dataclass
class AirflowWorkload:
    """Identity and routing metadata for one Airflow task instance."""

    dag_id: str
    task_id: str
    run_id: Optional[str] = None
    try_number: Optional[int] = None
    map_index: Optional[int] = None
    ti_id: Optional[str] = None
    dag_version_id: Optional[str] = None
    queue: Optional[str] = None
    pool_slots: Optional[int] = None
    priority_weight: Optional[int] = None
    log_path: Optional[str] = None
    dag_rel_path: Optional[str] = None
    bundle_name: Optional[str] = None
    bundle_version: Optional[str] = None
    workload_type: Optional[str] = None
    # True when the payload was recovered by regex because the JSON was cut off.
    partial: bool = False

    @property
    def display_name(self) -> str:
        """The name Kanchi shows in place of the Celery task name."""
        return "%s.%s" % (self.dag_id, self.task_id)

    @property
    def is_mapped(self) -> bool:
        return self.map_index is not None and self.map_index >= 0

    def to_meta(self) -> Dict[str, Any]:
        """The long tail of fields, stored as one JSON column."""
        meta = {
            "ti_id": self.ti_id,
            "dag_version_id": self.dag_version_id,
            "queue": self.queue,
            "pool_slots": self.pool_slots,
            "priority_weight": self.priority_weight,
            "log_path": self.log_path,
            "dag_rel_path": self.dag_rel_path,
            "bundle_name": self.bundle_name,
            "bundle_version": self.bundle_version,
            "workload_type": self.workload_type,
        }
        if self.partial:
            meta["partial"] = True
        return {key: value for key, value in meta.items() if value is not None}


def redact_secrets(value: Any) -> Tuple[Any, bool]:
    """Strip credentials from *value*, returning the copy and whether it changed.

    Handles both decoded payloads (dicts with a ``token`` key) and the raw JSON
    strings Celery actually delivers, including truncated ones.
    """
    changed = False

    def _walk(item: Any) -> Any:
        nonlocal changed

        if isinstance(item, dict):
            result = {}
            for key, val in item.items():
                if isinstance(key, str) and key.lower() in SECRET_KEYS and val:
                    result[key] = REDACTED
                    changed = True
                else:
                    result[key] = _walk(val)
            return result

        if isinstance(item, list):
            return [_walk(elem) for elem in item]

        if isinstance(item, tuple):
            return tuple(_walk(elem) for elem in item)

        if isinstance(item, str):
            redacted, count = _SECRET_JSON_RE.subn(r"\g<1>%s\g<2>" % REDACTED, item)
            if count:
                changed = True
                return redacted
            return item

        return item

    return _walk(value), changed


def _from_mapping(payload: Dict[str, Any]) -> Optional[AirflowWorkload]:
    """Build a workload from an already-decoded ``ExecuteTask`` payload."""
    ti = payload.get("ti")
    if not isinstance(ti, dict):
        return None

    dag_id = ti.get("dag_id")
    task_id = ti.get("task_id")
    if not dag_id or not task_id:
        return None

    bundle = payload.get("bundle_info")
    bundle = bundle if isinstance(bundle, dict) else {}

    return AirflowWorkload(
        dag_id=str(dag_id),
        task_id=str(task_id),
        run_id=ti.get("run_id"),
        try_number=ti.get("try_number"),
        map_index=ti.get("map_index"),
        ti_id=ti.get("id"),
        dag_version_id=ti.get("dag_version_id"),
        queue=ti.get("queue"),
        pool_slots=ti.get("pool_slots"),
        priority_weight=ti.get("priority_weight"),
        log_path=payload.get("log_path"),
        dag_rel_path=payload.get("dag_rel_path"),
        bundle_name=bundle.get("name"),
        bundle_version=bundle.get("version"),
        workload_type=payload.get("type"),
    )


def _from_truncated_json(payload: str) -> Optional[AirflowWorkload]:
    """Recover identity from JSON that Celery cut short.

    ``log_path`` encodes dag/run/task/attempt and is emitted ahead of the ``ti``
    block, so it survives truncation that destroys the structured fields.
    """
    if '"ExecuteTask"' not in payload and "log_path" not in payload and '"ti"' not in payload:
        return None

    dag_id = _json_string_field(payload, "dag_id")
    task_id = _json_string_field(payload, "task_id")
    run_id = _json_string_field(payload, "run_id")
    log_path = _json_string_field(payload, "log_path")
    try_number = _json_int_field(payload, "try_number")
    map_index = _json_int_field(payload, "map_index")

    # `log_path` is a single self-consistent source; prefer it over loose keys
    # that may have been cut mid-value.
    match = _LOG_PATH_RE.search(log_path or payload)
    if match:
        dag_id = dag_id or match.group("dag_id")
        task_id = task_id or match.group("task_id")
        run_id = run_id or match.group("run_id")
        if map_index is None and match.group("map_index") is not None:
            map_index = int(match.group("map_index"))
        if try_number is None and match.group("try_number") is not None:
            try_number = int(match.group("try_number"))

    if not dag_id or not task_id:
        return None

    return AirflowWorkload(
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        try_number=try_number,
        map_index=map_index,
        ti_id=_json_string_field(payload, "id"),
        log_path=log_path,
        dag_rel_path=_json_string_field(payload, "dag_rel_path"),
        workload_type=_json_string_field(payload, "type"),
        partial=True,
    )


def _candidate_payloads(args: Any, kwargs: Any) -> List[Any]:
    """Values that might hold a workload, in the order they should be tried."""
    candidates: List[Any] = []

    if isinstance(args, (list, tuple)):
        candidates.extend(args)
    elif args:
        candidates.append(args)

    if isinstance(kwargs, dict):
        # Airflow passes the workload positionally, but a `.apply_async(kwargs=)`
        # style submission would land here instead.
        candidates.extend(kwargs.values())

    return candidates


def parse_workload(args: Any, kwargs: Any = None) -> Optional[AirflowWorkload]:
    """Extract Airflow task-instance identity from Celery call inputs.

    Returns ``None`` for anything that is not an Airflow workload, which is the
    signal to leave the task presented exactly as Celery reported it.
    """
    for candidate in _candidate_payloads(args, kwargs):
        if isinstance(candidate, dict):
            workload = _from_mapping(candidate)
            if workload:
                return workload
            continue

        if not isinstance(candidate, str):
            continue

        stripped = candidate.strip()
        if not stripped.startswith("{"):
            continue

        try:
            decoded = json.loads(stripped)
        except ValueError:
            workload = _from_truncated_json(stripped)
            if workload:
                return workload
            continue

        if isinstance(decoded, dict):
            workload = _from_mapping(decoded)
            if workload:
                return workload

    return None


AIRFLOW_RERUN_BLOCKED_MESSAGE = (
    "This is an Airflow task instance. Clear it in Airflow to rerun it -- "
    "Kanchi cannot resubmit it through Celery."
)


def is_airflow_task(obj: Any) -> bool:
    """Whether a TaskEvent or database row carries an Airflow task instance.

    Anything that resubmits a captured Celery payload must check this first:
    Airflow's workload carries a short-lived JWT that Kanchi redacts, and a
    direct Celery submission would bypass the scheduler, leaving the Airflow
    metadata database unaware the task ever ran.
    """
    return bool(getattr(obj, "airflow_dag_id", None))


def celery_name_of(obj: Any) -> Optional[str]:
    """The Celery task name for *obj*, seeing past the Airflow display name."""
    return getattr(obj, "celery_task_name", None) or getattr(obj, "task_name", None)


def looks_like_airflow_task_name(task_name: Optional[str]) -> bool:
    """Whether *task_name* is one of Airflow's Celery entry points."""
    if not task_name:
        return False
    return task_name in AIRFLOW_CELERY_TASK_NAMES or task_name.endswith(
        ".execute_workload"
    )


__all__ = [
    "AIRFLOW_CELERY_TASK_NAMES",
    "AIRFLOW_RERUN_BLOCKED_MESSAGE",
    "REDACTED",
    "AirflowWorkload",
    "celery_name_of",
    "is_airflow_task",
    "looks_like_airflow_task_name",
    "parse_workload",
    "redact_secrets",
]
