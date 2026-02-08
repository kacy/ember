{{/*
chart name, truncated to 63 chars.
*/}}
{{- define "ember.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
fully qualified app name, truncated to 63 chars.
*/}}
{{- define "ember.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
common labels
*/}}
{{- define "ember.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "ember.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
selector labels
*/}}
{{- define "ember.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ember.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
service account name
*/}}
{{- define "ember.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "ember.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}
