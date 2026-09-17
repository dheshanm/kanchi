/**
 * Airflow-aware view of a Celery task.
 *
 * Airflow's Celery executor submits every task instance as a single Celery task
 * named `execute_workload`. The backend decodes the DAG/run/task identity out of
 * the payload and stores it on the event; this composable turns those fields
 * into something the UI can show and link to.
 */
import { computed } from 'vue'
import { useConfigStore } from '~/stores/config'

export interface AirflowTaskFields {
  task_name?: string | null
  celery_task_name?: string | null
  airflow_dag_id?: string | null
  airflow_task_id?: string | null
  airflow_run_id?: string | null
  airflow_try_number?: number | null
  airflow_map_index?: number | null
  airflow_meta?: Record<string, any> | null
}

export interface AirflowMeta {
  ti_id?: string
  dag_version_id?: string
  queue?: string
  pool_slots?: number
  priority_weight?: number
  log_path?: string
  dag_rel_path?: string
  bundle_name?: string
  bundle_version?: string
  workload_type?: string
  /** Set when the payload was truncated and recovered by pattern matching. */
  partial?: boolean
}

/** Whether a task row carries an Airflow task instance. */
export function isAirflowTask(task?: AirflowTaskFields | null): boolean {
  return Boolean(task?.airflow_dag_id && task?.airflow_task_id)
}

/** Airflow run ids contain `:` and `+`, which must not survive into a path. */
function segment(value: string): string {
  return encodeURIComponent(value)
}

export function useAirflow() {
  const configStore = useConfigStore()

  // Only the dashboard and the settings page load the config snapshot, so fetch
  // it lazily here -- otherwise the deep links silently stay hidden on the task
  // pages, which is exactly where they are useful.
  if (import.meta.client && !configStore.config && !configStore.isLoading) {
    configStore.fetchConfig().catch(() => {})
  }

  const baseUrl = computed(() => (configStore.airflowBaseUrl || '').replace(/\/+$/, ''))
  const linksEnabled = computed(() => baseUrl.value.length > 0)

  function dagUrl(dagId?: string | null): string | null {
    if (!linksEnabled.value || !dagId) return null
    return `${baseUrl.value}/dags/${segment(dagId)}`
  }

  function runUrl(dagId?: string | null, runId?: string | null): string | null {
    const dag = dagUrl(dagId)
    if (!dag || !runId) return null
    return `${dag}/runs/${segment(runId)}`
  }

  /**
   * Link to the task instance, which opens on its logs in the Airflow 3 UI.
   * Mapped instances live under an extra `/mapped/<index>` segment.
   */
  function taskInstanceUrl(task?: AirflowTaskFields | null): string | null {
    if (!task) return null
    const run = runUrl(task.airflow_dag_id, task.airflow_run_id)
    if (!run || !task.airflow_task_id) return null

    const base = `${run}/tasks/${segment(task.airflow_task_id)}`
    const mapIndex = task.airflow_map_index
    return mapIndex !== null && mapIndex !== undefined && mapIndex >= 0
      ? `${base}/mapped/${mapIndex}`
      : base
  }

  function metaOf(task?: AirflowTaskFields | null): AirflowMeta {
    return (task?.airflow_meta as AirflowMeta) || {}
  }

  /** `dag_id` and `task_id`, split back out of the rewritten display name. */
  function identityOf(task?: AirflowTaskFields | null) {
    return {
      dagId: task?.airflow_dag_id || null,
      taskId: task?.airflow_task_id || null,
      runId: task?.airflow_run_id || null,
      tryNumber: task?.airflow_try_number ?? null,
      mapIndex: task?.airflow_map_index ?? null,
      isMapped:
        task?.airflow_map_index !== null &&
        task?.airflow_map_index !== undefined &&
        task.airflow_map_index >= 0,
    }
  }

  return {
    baseUrl,
    linksEnabled,
    isAirflowTask,
    dagUrl,
    runUrl,
    taskInstanceUrl,
    metaOf,
    identityOf,
  }
}
