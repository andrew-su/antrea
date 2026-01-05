{{- define "flowAggregatorImageTag" -}}
{{- if .Values.image.tag }}
{{- .Values.image.tag -}}
{{- else if eq .Chart.AppVersion "latest" }}
{{- print "latest" -}}
{{- else }}
{{- print "v" .Chart.AppVersion -}}
{{- end }}
{{- end -}}

{{- define "flowAggregatorImage" -}}
{{- print .Values.image.repository ":" (include "flowAggregatorImageTag" .) -}}
{{- end -}}

{{/*
Create a name with `<chart_name>-<release_namespace>` prefix.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
Arguments (dict):
  - root (required): The root context ($)
  - id (optional): The identitier for the resource (ex. "auth-delegator")
  - suffix (optional): The suffix for the resource name (ex. "viewer", "reader", "role")
*/}}
{{- define "flow-aggregator.resourceName" -}}
{{- $name := default .root.Chart.Name .root.Values.nameOverride }}
{{- if not (eq $name .root.Release.Namespace) }}
{{- $name = printf "%s-%s" $name .root.Release.Namespace | trimSuffix "-" }}
{{- end -}}
{{- $name = printf "%s-%s" $name (default "" .id) | trimSuffix "-" }}
{{- $name = printf "%s-%s" $name (default "" .suffix) | trimSuffix "-" }}
{{- $name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a name for ClusterRole resource.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
Arguments (dict):
  - root (required): The root context ($)
  - id (optional): The identitier for the resource (ex. "auth-delegator")
*/}}
{{- define "flow-aggregator.clusterRole" -}}
{{- template "flow-aggregator.resourceName" (dict "root" .root "id" .id "suffix" "cluster-role") }}
{{- end }}

{{/*
Create a name for ClusterRoleBinding resource.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
Arguments (dict):
  - root (required): The root context ($)
  - id (optional): The identitier for the resource (ex. "auth-delegator")
*/}}
{{- define "flow-aggregator.clusterRoleBinding" -}}
{{- template "flow-aggregator.resourceName" (dict "root" .root "id" .id "suffix" "cluster-role-binding") }}
{{- end }}
