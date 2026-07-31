{{/* Base name, overridable. */}}
{{- define "schematic-replicator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified app name. */}}
{{- define "schematic-replicator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "schematic-replicator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "schematic-replicator.labels" -}}
helm.sh/chart: {{ include "schematic-replicator.chart" . }}
{{ include "schematic-replicator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "schematic-replicator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "schematic-replicator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "schematic-replicator.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "schematic-replicator.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Name of the Secret holding the API key. */}}
{{- define "schematic-replicator.apiKeySecretName" -}}
{{- if .Values.schematic.existingSecret -}}
{{- .Values.schematic.existingSecret -}}
{{- else -}}
{{- include "schematic-replicator.fullname" . -}}
{{- end -}}
{{- end -}}

{{- define "schematic-replicator.apiKeySecretKey" -}}
{{- if .Values.schematic.existingSecret -}}
{{- .Values.schematic.existingSecretKey -}}
{{- else -}}
api-key
{{- end -}}
{{- end -}}

{{/* Whether a chart-managed Secret is needed at all. */}}
{{- define "schematic-replicator.createSecret" -}}
{{- if or (and (not .Values.schematic.existingSecret) .Values.schematic.apiKey) (and (not .Values.redis.existingSecret) .Values.redis.password) -}}
true
{{- end -}}
{{- end -}}

{{/*
Render one env entry, but only when the value is non-empty, so unset values fall
through to the application's own defaults instead of being pinned to "".
Emitted at column 0; the aggregate below is indented once by the caller.
*/}}
{{- define "schematic-replicator.env" -}}
{{- if not (kindIs "invalid" .value) -}}
{{- if ne (toString .value) "" }}
- name: {{ .name }}
  value: {{ toString .value | quote }}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
All optional environment variables, as one block. The caller applies a single
nindent, which keeps every entry aligned regardless of how many are set.
*/}}
{{- define "schematic-replicator.optionalEnv" -}}
{{- $v := .Values -}}

{{/* Redis: cluster mode swaps ADDR for CLUSTER_ADDRS. */}}
{{- if $v.redis.cluster.enabled -}}
{{- include "schematic-replicator.env" (dict "name" "REDIS_CLUSTER_MODE" "value" "true") -}}
{{- include "schematic-replicator.env" (dict "name" "REDIS_CLUSTER_ADDRS" "value" $v.redis.cluster.addrs) -}}
{{- else -}}
{{- include "schematic-replicator.env" (dict "name" "REDIS_ADDR" "value" $v.redis.addr) -}}
{{- end -}}
{{- include "schematic-replicator.env" (dict "name" "REDIS_DB" "value" $v.redis.db) -}}
{{- if $v.redis.tls -}}
{{- include "schematic-replicator.env" (dict "name" "REDIS_TLS" "value" "true") -}}
{{- end -}}
{{- if $v.redis.maintenanceNotifications -}}
{{- include "schematic-replicator.env" (dict "name" "REDIS_ENABLE_MAINTENANCE_NOTIFICATIONS" "value" "true") -}}
{{- end -}}

{{/* Schematic endpoints. */}}
{{- include "schematic-replicator.env" (dict "name" "SCHEMATIC_API_URL" "value" $v.schematic.apiUrl) -}}
{{- include "schematic-replicator.env" (dict "name" "SCHEMATIC_DATASTREAM_URL" "value" $v.schematic.datastreamUrl) -}}

{{/* Application behaviour. */}}
{{- include "schematic-replicator.env" (dict "name" "LOG_LEVEL" "value" $v.config.logLevel) -}}
{{- include "schematic-replicator.env" (dict "name" "HEALTH_PORT" "value" $v.config.healthPort) -}}
{{- include "schematic-replicator.env" (dict "name" "CACHE_TTL" "value" $v.config.cacheTtl) -}}
{{- include "schematic-replicator.env" (dict "name" "CACHE_CLEANUP_INTERVAL" "value" $v.config.cacheCleanupInterval) -}}

{{/* Writer lease. */}}
{{- if $v.writerLock.disabled -}}
{{- include "schematic-replicator.env" (dict "name" "WRITER_LOCK_DISABLED" "value" "true") -}}
{{- end -}}
{{- include "schematic-replicator.env" (dict "name" "WRITER_LOCK_TTL" "value" $v.writerLock.ttl) -}}
{{- include "schematic-replicator.env" (dict "name" "WRITER_LOCK_KEY" "value" $v.writerLock.key) -}}

{{/* Replay. */}}
{{- if $v.replay.disabled -}}
{{- include "schematic-replicator.env" (dict "name" "REPLAY_DISABLED" "value" "true") -}}
{{- end -}}
{{- include "schematic-replicator.env" (dict "name" "REPLAY_CURSOR_KEY" "value" $v.replay.cursorKey) -}}

{{/* WebSocket keepalive. */}}
{{- include "schematic-replicator.env" (dict "name" "WS_PING_INTERVAL" "value" $v.websocket.pingInterval) -}}
{{- include "schematic-replicator.env" (dict "name" "WS_PONG_WAIT" "value" $v.websocket.pongWait) -}}

{{/* Async loading and processing. */}}
{{- include "schematic-replicator.env" (dict "name" "USE_ASYNC_LOADING" "value" $v.async.useAsyncLoading) -}}
{{- include "schematic-replicator.env" (dict "name" "ASYNC_LOADER_PAGE_SIZE" "value" $v.async.loaderPageSize) -}}
{{- include "schematic-replicator.env" (dict "name" "ASYNC_LOADER_CIRCUIT_BREAKER_THRESHOLD" "value" $v.async.loaderCircuitBreakerThreshold) -}}
{{- include "schematic-replicator.env" (dict "name" "ASYNC_LOADER_CIRCUIT_BREAKER_TIMEOUT" "value" $v.async.loaderCircuitBreakerTimeout) -}}
{{- include "schematic-replicator.env" (dict "name" "ASYNC_LOADER_MAX_CONCURRENT_REQUESTS" "value" $v.async.loaderMaxConcurrentRequests) -}}
{{- include "schematic-replicator.env" (dict "name" "ASYNC_LOADER_RATE_LIMIT_RPS" "value" $v.async.loaderRateLimitRps) -}}
{{- include "schematic-replicator.env" (dict "name" "NUM_WORKERS" "value" $v.async.numWorkers) -}}
{{- include "schematic-replicator.env" (dict "name" "BATCH_SIZE" "value" $v.async.batchSize) -}}
{{- include "schematic-replicator.env" (dict "name" "BATCH_TIMEOUT" "value" $v.async.batchTimeout) -}}
{{- include "schematic-replicator.env" (dict "name" "COMPANY_CHANNEL_SIZE" "value" $v.async.companyChannelSize) -}}
{{- include "schematic-replicator.env" (dict "name" "USER_CHANNEL_SIZE" "value" $v.async.userChannelSize) -}}
{{- include "schematic-replicator.env" (dict "name" "FLAGS_CHANNEL_SIZE" "value" $v.async.flagsChannelSize) -}}
{{- include "schematic-replicator.env" (dict "name" "CIRCUIT_BREAKER_THRESHOLD" "value" $v.async.circuitBreakerThreshold) -}}
{{- include "schematic-replicator.env" (dict "name" "CIRCUIT_BREAKER_TIMEOUT" "value" $v.async.circuitBreakerTimeout) -}}

{{- range $key, $value := $v.extraEnv }}
- name: {{ $key }}
  value: {{ $value | quote }}
{{- end -}}
{{- with $v.extraEnvRaw }}
{{ toYaml . }}
{{- end -}}
{{- end -}}
