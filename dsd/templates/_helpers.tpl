{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "dsd.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "dsd.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "dsd.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "dsd.labels" -}}
app.kubernetes.io/name: {{ include "dsd.name" . }}
helm.sh/chart: {{ include "dsd.chart" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Common prefix prepended to Kubernetes resources of this chart
*/}}
{{- define "dsd.resource.prefix" -}}
{{- "dsd" }}
{{- end -}}

{{- define "image" }}
{{- $imageName := .deployment.imageName }}
{{- $imageTag := .deployment.imageTag | default "" }}
{{- if or (eq .Values.dsd.subscription.username "") (eq .Values.dsd.subscription.password "") -}}
{{- $dockerRegistry := .deployment.dockerRegistry | default "dsd" }}
image: {{ $dockerRegistry }}/{{ $imageName }}{{- if not (eq $imageTag "") }}{{- printf ":%s" $imageTag -}}{{- end }}
{{- else }}
{{- $dockerRegistry := .deployment.dockerRegistry | default "docker.dsd.com" }}
{{- $parts := len (split "." $imageTag) }}
{{- if eq $parts 3 }}
image: {{ $dockerRegistry }}/{{ $imageName }}{{- if not (eq $imageTag "") }}:{{ $imageTag }}.0{{- end }}
{{- else }}
image: {{ $dockerRegistry }}/{{ $imageName }}{{- if not (eq $imageTag "") }}:{{ $imageTag }}{{- end }}
{{- end -}}
{{- end -}}
{{- end -}}
