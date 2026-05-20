#!/usr/bin/env bash
set -Eeuo pipefail

NAMESPACE="${NAMESPACE:-pizza-pulse}"
STATE_DIR="${STATE_DIR:-.port-forward}"
PID_DIR="$STATE_DIR/pids"
LOG_DIR="$STATE_DIR/logs"

# Override these if Helm release names differ in your cluster.
POSTGRES_SERVICE="${POSTGRES_SERVICE:-pp-postgre-postgresql}"
MINIO_API_SERVICE="${MINIO_API_SERVICE:-pp-minio}"
MINIO_CONSOLE_SERVICE="${MINIO_CONSOLE_SERVICE:-pp-minio-console}"
MLFLOW_SERVICE="${MLFLOW_SERVICE:-pp-mlflow}"
KAFKA_SERVICE="${KAFKA_SERVICE:-pp-kafka-kafka-bootstrap}"
KAFKA_UI_SERVICE="${KAFKA_UI_SERVICE:-pp-kafka-ui}"
AIRFLOW_API_SERVICE="${AIRFLOW_API_SERVICE:-pp-airflow-api-server}"

# Local ports can also be changed from the environment.
POSTGRES_LOCAL_PORT="${POSTGRES_LOCAL_PORT:-5432}"
MINIO_API_LOCAL_PORT="${MINIO_API_LOCAL_PORT:-9000}"
MINIO_CONSOLE_LOCAL_PORT="${MINIO_CONSOLE_LOCAL_PORT:-9001}"
MLFLOW_LOCAL_PORT="${MLFLOW_LOCAL_PORT:-5000}"
KAFKA_LOCAL_PORT="${KAFKA_LOCAL_PORT:-9092}"
KAFKA_UI_LOCAL_PORT="${KAFKA_UI_LOCAL_PORT:-8082}"
AIRFLOW_API_LOCAL_PORT="${AIRFLOW_API_LOCAL_PORT:-8080}"

FORWARDS=(
  "postgres|$POSTGRES_SERVICE|$POSTGRES_LOCAL_PORT|5432"
  "minio-api|$MINIO_API_SERVICE|$MINIO_API_LOCAL_PORT|9000"
  "minio-console|$MINIO_CONSOLE_SERVICE|$MINIO_CONSOLE_LOCAL_PORT|9001"
  "mlflow|$MLFLOW_SERVICE|$MLFLOW_LOCAL_PORT|80"
  "kafka|$KAFKA_SERVICE|$KAFKA_LOCAL_PORT|9092"
  "kafka-ui|$KAFKA_UI_SERVICE|$KAFKA_UI_LOCAL_PORT|80"
  "airflow-api|$AIRFLOW_API_SERVICE|$AIRFLOW_API_LOCAL_PORT|8080"
)

usage() {
  cat <<'USAGE'
Usage:
  scripts/port-forward.sh start      Start all port-forwards in the background
  scripts/port-forward.sh stop       Stop port-forwards started by this script
  scripts/port-forward.sh restart    Stop, then start all port-forwards
  scripts/port-forward.sh status     Show running/stopped state
  scripts/port-forward.sh logs NAME  Tail one log, e.g. logs airflow-api

Environment overrides:
  NAMESPACE=pizza-pulse
  POSTGRES_SERVICE=pp-postgre-postgresql
  MINIO_API_SERVICE=pp-minio
  MINIO_CONSOLE_SERVICE=pp-minio-console
  MLFLOW_SERVICE=pp-mlflow
  KAFKA_SERVICE=pp-kafka-kafka-bootstrap
  KAFKA_UI_SERVICE=pp-kafka-ui
  AIRFLOW_API_SERVICE=pp-airflow-api-server

  POSTGRES_LOCAL_PORT=5432
  MINIO_API_LOCAL_PORT=9000
  MINIO_CONSOLE_LOCAL_PORT=9001
  MLFLOW_LOCAL_PORT=5000
  KAFKA_LOCAL_PORT=9092
  KAFKA_UI_LOCAL_PORT=8082
  AIRFLOW_API_LOCAL_PORT=8080
USAGE
}

die() {
  printf 'Error: %s\n' "$*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

pid_file() {
  printf '%s/%s.pid' "$PID_DIR" "$1"
}

log_file() {
  printf '%s/%s.log' "$LOG_DIR" "$1"
}

is_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1
}

existing_pid() {
  local name="$1"
  local file
  file="$(pid_file "$name")"
  [[ -f "$file" ]] || return 1
  cat "$file"
}

port_in_use() {
  local port="$1"

  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
    return $?
  fi

  if command -v ss >/dev/null 2>&1; then
    ss -ltn "sport = :$port" 2>/dev/null | awk 'NR > 1 { found = 1 } END { exit !found }'
    return $?
  fi

  if command -v nc >/dev/null 2>&1; then
    nc -z 127.0.0.1 "$port" >/dev/null 2>&1
    return $?
  fi

  return 1
}

service_exists() {
  local service="$1"
  kubectl -n "$NAMESPACE" get svc "$service" >/dev/null 2>&1
}

resolve_service() {
  local name="$1"
  local service="$2"
  local candidate
  local candidates=("$service")

  for candidate in "${candidates[@]}"; do
    if service_exists "$candidate"; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  {
    printf 'Service not found: svc/%s in namespace %s\n' "$service" "$NAMESPACE"
    printf 'Tip: check actual services with: kubectl -n %s get svc\n' "$NAMESPACE" >&2
  } >&2
  return 1
}

start_one() {
  local name="$1"
  local service="$2"
  local local_port="$3"
  local remote_port="$4"
  local pid=""

  if pid="$(existing_pid "$name" 2>/dev/null)" && is_running "$pid"; then
    printf '%-14s already running  pid=%s  localhost:%s -> svc/%s:%s\n' \
      "$name" "$pid" "$local_port" "$service" "$remote_port"
    return 0
  fi

  service="$(resolve_service "$name" "$service")"

  if port_in_use "$local_port"; then
    die "local port $local_port is already in use; change the matching *_LOCAL_PORT override or stop the process using it"
  fi

  : >"$(log_file "$name")"
  kubectl -n "$NAMESPACE" port-forward "svc/$service" "$local_port:$remote_port" \
    >"$(log_file "$name")" 2>&1 &
  pid="$!"
  printf '%s\n' "$pid" >"$(pid_file "$name")"

  sleep 0.4
  if ! is_running "$pid"; then
    printf 'Failed to start %s. Log:\n' "$name" >&2
    sed -n '1,120p' "$(log_file "$name")" >&2
    rm -f "$(pid_file "$name")"
    return 1
  fi

  printf '%-14s started          pid=%s  localhost:%s -> svc/%s:%s\n' \
    "$name" "$pid" "$local_port" "$service" "$remote_port"
}

start_all() {
  need_cmd kubectl
  mkdir -p "$PID_DIR" "$LOG_DIR"
  kubectl get namespace "$NAMESPACE" >/dev/null 2>&1 || die "namespace not found: $NAMESPACE"

  for item in "${FORWARDS[@]}"; do
    IFS='|' read -r name service local_port remote_port <<<"$item"
    start_one "$name" "$service" "$local_port" "$remote_port"
  done

  printf '\nLogs: %s/*.log\n' "$LOG_DIR"
  printf 'Stop: scripts/port-forward.sh stop\n'
}

stop_one() {
  local name="$1"
  local file pid
  file="$(pid_file "$name")"

  if [[ ! -f "$file" ]]; then
    printf '%-14s no pid file\n' "$name"
    return 0
  fi

  pid="$(cat "$file")"
  if is_running "$pid"; then
    kill "$pid" >/dev/null 2>&1 || true
    sleep 0.2
    if is_running "$pid"; then
      kill -9 "$pid" >/dev/null 2>&1 || true
    fi
    printf '%-14s stopped pid=%s\n' "$name" "$pid"
  else
    printf '%-14s not running pid=%s\n' "$name" "$pid"
  fi

  rm -f "$file"
}

stop_all() {
  mkdir -p "$PID_DIR" "$LOG_DIR"

  for item in "${FORWARDS[@]}"; do
    IFS='|' read -r name _ <<<"$item"
    stop_one "$name"
  done
}

status_all() {
  mkdir -p "$PID_DIR" "$LOG_DIR"

  for item in "${FORWARDS[@]}"; do
    IFS='|' read -r name service local_port remote_port <<<"$item"
    local pid=""
    if pid="$(existing_pid "$name" 2>/dev/null)" && is_running "$pid"; then
      printf '%-14s running  pid=%s  localhost:%s -> svc/%s:%s\n' \
        "$name" "$pid" "$local_port" "$service" "$remote_port"
    else
      printf '%-14s stopped           localhost:%s -> svc/%s:%s\n' \
        "$name" "$local_port" "$service" "$remote_port"
    fi
  done
}

tail_logs() {
  local name="${1:-}"
  [[ -n "$name" ]] || die "missing log name"
  [[ -f "$(log_file "$name")" ]] || die "log not found: $(log_file "$name")"
  tail -f "$(log_file "$name")"
}

main() {
  local command="${1:-start}"

  case "$command" in
    start)
      start_all
      ;;
    stop)
      stop_all
      ;;
    restart)
      stop_all
      start_all
      ;;
    status)
      status_all
      ;;
    logs)
      tail_logs "${2:-}"
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
