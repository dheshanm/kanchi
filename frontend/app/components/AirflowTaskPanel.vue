<script setup lang="ts">
/**
 * Airflow identity for a Celery task.
 *
 * Renders nothing for ordinary Celery tasks, so it is safe to drop into any
 * task detail view whether or not the deployment runs Airflow.
 */
import { computed } from 'vue'
import { ExternalLink, GitBranch, Workflow, Layers, FileCode, AlertTriangle } from 'lucide-vue-next'
import CopyButton from '~/components/CopyButton.vue'
import { useAirflow, type AirflowTaskFields } from '~/composables/useAirflow'

const props = defineProps<{
  task?: AirflowTaskFields | null
}>()

const { linksEnabled, isAirflowTask, dagUrl, runUrl, taskInstanceUrl, metaOf, identityOf } =
  useAirflow()

const show = computed(() => isAirflowTask(props.task))
const identity = computed(() => identityOf(props.task))
const meta = computed(() => metaOf(props.task))

const links = computed(() => ({
  dag: dagUrl(identity.value.dagId),
  run: runUrl(identity.value.dagId, identity.value.runId),
  taskInstance: taskInstanceUrl(props.task),
}))

const attempt = computed(() => {
  const tryNumber = identity.value.tryNumber
  return tryNumber === null ? null : `Attempt ${tryNumber}`
})
</script>

<template>
  <div
    v-if="show"
    class="p-4 border border-border-subtle rounded-md bg-background-surface space-y-3"
  >
    <div class="flex flex-wrap items-center justify-between gap-2">
      <h4 class="flex items-center gap-1.5 text-sm font-medium text-text-primary">
        <Workflow class="h-3.5 w-3.5 text-text-muted" />
        Airflow task instance
      </h4>
      <div class="flex flex-wrap items-center gap-2">
        <a
          v-if="links.taskInstance"
          :href="links.taskInstance"
          target="_blank"
          rel="noopener noreferrer"
          class="inline-flex items-center gap-1 text-xs font-medium text-primary hover:underline"
        >
          Logs in Airflow
          <ExternalLink class="h-3 w-3" />
        </a>
        <a
          v-if="links.run"
          :href="links.run"
          target="_blank"
          rel="noopener noreferrer"
          class="inline-flex items-center gap-1 text-xs font-medium text-primary hover:underline"
        >
          DAG run
          <ExternalLink class="h-3 w-3" />
        </a>
        <a
          v-if="links.dag"
          :href="links.dag"
          target="_blank"
          rel="noopener noreferrer"
          class="inline-flex items-center gap-1 text-xs font-medium text-primary hover:underline"
        >
          DAG
          <ExternalLink class="h-3 w-3" />
        </a>
      </div>
    </div>

    <p v-if="!linksEnabled" class="text-xs text-text-muted">
      Set the Airflow base URL in settings to link straight to the DAG, run and logs.
    </p>

    <dl class="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-2 text-xs">
      <div class="flex items-start gap-2 min-w-0">
        <dt class="w-24 shrink-0 text-text-muted">DAG</dt>
        <dd class="min-w-0 font-mono text-text-primary break-all">
          <GitBranch class="inline h-3 w-3 mr-1 text-text-muted" />{{ identity.dagId }}
        </dd>
      </div>

      <div class="flex items-start gap-2 min-w-0">
        <dt class="w-24 shrink-0 text-text-muted">Task</dt>
        <dd class="min-w-0 font-mono text-text-primary break-all">{{ identity.taskId }}</dd>
      </div>

      <div v-if="identity.runId" class="flex items-start gap-2 min-w-0 sm:col-span-2">
        <dt class="w-24 shrink-0 text-text-muted">Run</dt>
        <dd class="flex min-w-0 items-center gap-1.5 font-mono text-text-primary break-all">
          <span class="break-all">{{ identity.runId }}</span>
          <CopyButton
            :text="identity.runId"
            :copy-key="`airflow-run-${identity.runId}`"
            title="Copy run ID"
            :show-text="false"
          />
        </dd>
      </div>

      <div v-if="attempt || identity.isMapped" class="flex items-start gap-2 min-w-0">
        <dt class="w-24 shrink-0 text-text-muted">Attempt</dt>
        <dd class="min-w-0 text-text-primary">
          {{ attempt || '-' }}
          <span v-if="identity.isMapped" class="ml-1 text-text-muted">
            <Layers class="inline h-3 w-3" /> map index {{ identity.mapIndex }}
          </span>
        </dd>
      </div>

      <div v-if="meta.queue" class="flex items-start gap-2 min-w-0">
        <dt class="w-24 shrink-0 text-text-muted">Airflow queue</dt>
        <dd class="min-w-0 font-mono text-text-primary break-all">{{ meta.queue }}</dd>
      </div>

      <div v-if="meta.dag_rel_path" class="flex items-start gap-2 min-w-0 sm:col-span-2">
        <dt class="w-24 shrink-0 text-text-muted">DAG file</dt>
        <dd class="min-w-0 font-mono text-text-primary break-all">
          <FileCode class="inline h-3 w-3 mr-1 text-text-muted" />{{ meta.dag_rel_path }}
        </dd>
      </div>

      <div v-if="meta.bundle_name" class="flex items-start gap-2 min-w-0">
        <dt class="w-24 shrink-0 text-text-muted">Bundle</dt>
        <dd class="min-w-0 font-mono text-text-primary break-all">
          {{ meta.bundle_name }}<span v-if="meta.bundle_version">@{{ meta.bundle_version }}</span>
        </dd>
      </div>

      <div v-if="meta.pool_slots || meta.priority_weight" class="flex items-start gap-2 min-w-0">
        <dt class="w-24 shrink-0 text-text-muted">Scheduling</dt>
        <dd class="min-w-0 text-text-primary">
          <span v-if="meta.pool_slots">{{ meta.pool_slots }} pool slot(s)</span>
          <span v-if="meta.pool_slots && meta.priority_weight" class="text-text-muted"> • </span>
          <span v-if="meta.priority_weight">priority {{ meta.priority_weight }}</span>
        </dd>
      </div>

      <div v-if="meta.ti_id" class="flex items-start gap-2 min-w-0 sm:col-span-2">
        <dt class="w-24 shrink-0 text-text-muted">Instance ID</dt>
        <dd class="min-w-0 font-mono text-text-muted break-all">{{ meta.ti_id }}</dd>
      </div>

      <div v-if="meta.log_path" class="flex items-start gap-2 min-w-0 sm:col-span-2">
        <dt class="w-24 shrink-0 text-text-muted">Log file</dt>
        <dd class="min-w-0 font-mono text-text-muted break-all">{{ meta.log_path }}</dd>
      </div>

      <div v-if="props.task?.celery_task_name" class="flex items-start gap-2 min-w-0 sm:col-span-2">
        <dt class="w-24 shrink-0 text-text-muted">Celery task</dt>
        <dd class="min-w-0 font-mono text-text-muted break-all">
          {{ props.task?.celery_task_name }}
        </dd>
      </div>
    </dl>

    <p v-if="meta.partial" class="flex items-start gap-1.5 text-xs text-status-warning">
      <AlertTriangle class="h-3.5 w-3.5 shrink-0" />
      Celery truncated this workload; the identity above was recovered from the log
      path and some fields may be missing.
    </p>

    <p class="text-xs text-text-muted">
      Reruns are handled by Airflow. Clear the task instance there rather than
      resubmitting it through Celery.
    </p>
  </div>
</template>
