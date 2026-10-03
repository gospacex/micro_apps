#!/usr/bin/env bash
# bring-up.sh —— 按依赖序起停 hubx micro_apps 的 11 个服务。
#
# 依赖序（srv 先于 bff，bff 先于 gateway，job 独立）：
#   srv-user srv-product srv-order srv-pay srv-coupon
#   → bff-app bff-web bff-mini bff-admin
#   → gateway
#   → job（无端口，默认跑 schedule）
#
# 就绪判定不一刀切 sleep：
#   - srv（Kitex RPC，没有 HTTP 端口）→ 轮询 Consul KV hubx/svc/<name>
#   - BFF / gateway（HTTP）          → 轮询 /healthz/ready
#   - job（无端口无注册）            → 只看进程存活 + 日志无 FATAL
#
# 用法：
#   bash scripts/bring-up.sh up                       # 全量起
#   bash scripts/bring-up.sh up --services srv-order  # 只起/只重启指定服务（csv）
#   bash scripts/bring-up.sh up --reload              # 重启（改完 Nacos 配置后用）
#   bash scripts/bring-up.sh status
#   bash scripts/bring-up.sh restart srv-order
#   bash scripts/bring-up.sh down
#
# 环境变量：
#   HUBX_DB_PW      库口令（默认 hubx123，与 docker/mysql/init.sql 一致）
#   HUBX_BIN_DIR    二进制输出目录（默认 <repo>/.hubx-bin）
#   HUBX_LOG_DIR    日志目录（默认 /tmp/hubx-logs）
#   HUBX_NO_BUILD   非空则跳过 go build（用已有二进制）
#   HUBX_JOB_MODE   job 子命令（默认 schedule；consume 需 Kafka）
#   GO_BIN          go 可执行文件（默认 PATH 里的 go）
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_DIR="${HUBX_BIN_DIR:-$REPO_ROOT/.hubx-bin}"
LOG_DIR="${HUBX_LOG_DIR:-/tmp/hubx-logs}"
RUN_DIR="$LOG_DIR/pid"
GO_BIN="${GO_BIN:-go}"
HUBX_DB_PW="${HUBX_DB_PW:-hubx123}"
HUBX_JOB_MODE="${HUBX_JOB_MODE:-schedule}"
export HUBX_DB_PW

CONSUL_AGENT="127.0.0.1:8500"
READY_TIMEOUT="${HUBX_READY_TIMEOUT:-40}"

# 服务清单：<名称> <类型> <端口(无则 -)>
# 类型：rpc（Consul KV 就绪）/ http（/healthz/ready 就绪）/ none（只看进程）
ORDERED_SERVICES=(
  "srv-user:rpc:9011"
  "srv-product:rpc:9012"
  "srv-order:rpc:9013"
  "srv-pay:rpc:9014"
  "srv-coupon:rpc:9015"
  "bff-app:http:8091"
  "bff-web:http:8092"
  "bff-mini:http:8093"
  "bff-admin:http:8094"
  "gateway:http:8080"
  "job:none:-"
)

log()  { printf '%s\n' "$*"; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }

# ---------- 工具 ----------

have_go() {
  command -v "$GO_BIN" >/dev/null 2>&1 && return 0
  # macOS 上 go 常不在默认 PATH（本仓的 go 在 ~/work/gowork/go/bin）
  for c in /Users/hyx/work/gowork/go/bin/go /usr/local/go/bin/go; do
    if [ -x "$c" ]; then GO_BIN="$c"; return 0; fi
  done
  return 1
}

svc_field() { # svc_field <entry> <1=name|2=kind|3=port>
  local entry="$1" idx="$2"
  printf '%s' "$entry" | awk -F: -v i="$idx" '{print $i}'
}

svc_names() {
  local e
  for e in "${ORDERED_SERVICES[@]}"; do svc_field "$e" 1; done
}

# 过滤：--services 给出 csv，未给出则全量（保持依赖序）
selected_services() {
  local filter="$1" e name
  if [ -z "$filter" ]; then svc_names; return; fi
  local want=",$filter,"
  for e in "${ORDERED_SERVICES[@]}"; do
    name="$(svc_field "$e" 1)"
    [ "${want#*,$name,}" != "$want" ] && printf '%s\n' "$name"
  done
}

pid_file() { printf '%s/%s.pid' "$RUN_DIR" "$1"; }

is_running() {
  local pidf; pidf="$(pid_file "$1")"
  [ -f "$pidf" ] || return 1
  local pid; pid="$(cat "$pidf" 2>/dev/null || true)"
  [ -n "$pid" ] || return 1
  kill -0 "$pid" 2>/dev/null
}

# build_one <svc>：job 的二进制在 cmd/job 下，其余同构
build_one() {
  local svc="$1"
  mkdir -p "$BIN_DIR"
  (cd "$REPO_ROOT" && "$GO_BIN" build -o "$BIN_DIR/$svc" "./apps/$svc/cmd/$svc") \
    || die "build $svc failed"
}

start_one() {
  local svc="$1" kind="$2"
  if is_running "$svc"; then log "  $svc already running (pid $(cat "$(pid_file "$svc")"))"; return 0; fi
  if [ -z "${HUBX_NO_BUILD:-}" ]; then build_one "$svc"; fi
  [ -x "$BIN_DIR/$svc" ] || die "binary missing: $BIN_DIR/$svc（先跑 build）"

  mkdir -p "$LOG_DIR" "$RUN_DIR"
  local logf="$LOG_DIR/$svc.log"
  # 【关键】必须从 apps/<svc> 目录启动：本地 config.yaml 是相对路径读取的，
  # 从仓库根启动会读不到配置（静默走 fallback，排障时极难定位）。
  local args=""
  [ "$svc" = "job" ] && args="$HUBX_JOB_MODE"
  ( cd "$REPO_ROOT/apps/$svc" && nohup "$BIN_DIR/$svc" $args >>"$logf" 2>&1 & echo $! > "$(pid_file "$svc")" )
  sleep 1
  if ! is_running "$svc"; then
    die "$svc 启动即退出，看日志：$logf"
  fi
  log "  started $svc (pid $(cat "$(pid_file "$svc")"), log $logf)"
}

wait_ready() {
  local svc="$1" kind="$2" port="$3" i=0
  [ "$kind" = "none" ] && { log "  $svc ready (no probe)"; return 0; }
  while [ "$i" -lt "$READY_TIMEOUT" ]; do
    if [ "$kind" = "http" ]; then
      local code
      code="$(curl -s --noproxy '*' -o /dev/null -w '%{http_code}' "http://127.0.0.1:$port/healthz/ready" 2>/dev/null || echo 000)"
      [ "$code" = "200" ] && { log "  $svc ready (http $port)"; return 0; }
    else
      # Consul KV：服务注册成功即代表监听已起来
      local body
      body="$(curl -s --noproxy '*' "http://$CONSUL_AGENT/v1/kv/hubx/svc/$svc?raw" 2>/dev/null || true)"
      [ -n "$body" ] && { log "  $svc ready (consul kv = $body)"; return 0; }
    fi
    i=$((i+1)); sleep 1
  done
  log "  WARN $svc 未在 ${READY_TIMEOUT}s 内就绪，继续（看日志 $LOG_DIR/$svc.log）"
  return 0
}

stop_one() {
  local svc="$1" pidf pid
  pidf="$(pid_file "$svc")"
  if [ ! -f "$pidf" ]; then log "  $svc not running"; return 0; fi
  pid="$(cat "$pidf" 2>/dev/null || true)"
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    local i=0
    while kill -0 "$pid" 2>/dev/null && [ "$i" -lt 15 ]; do sleep 1; i=$((i+1)); done
    kill -9 "$pid" 2>/dev/null || true
    log "  stopped $svc (pid $pid)"
  else
    log "  $svc not running (stale pid file)"
  fi
  rm -f "$pidf"
}

# ---------- 子命令 ----------

cmd_up() {
  local filter="${1:-}" reload="${2:-}"
  have_go || die "go 不在 PATH（export PATH=/Users/hyx/work/gowork/go/bin:\$PATH 或设 GO_BIN）"
  mkdir -p "$BIN_DIR" "$LOG_DIR" "$RUN_DIR"
  log "bring-up: repo=$REPO_ROOT bin=$BIN_DIR log=$LOG_DIR"
  local svc e kind port
  for svc in $(selected_services "$filter"); do
    for e in "${ORDERED_SERVICES[@]}"; do
      [ "$(svc_field "$e" 1)" = "$svc" ] || continue
      kind="$(svc_field "$e" 2)"; port="$(svc_field "$e" 3)"
      if [ "$reload" = "reload" ] && is_running "$svc"; then
        log "reload $svc"; stop_one "$svc"
      fi
      start_one "$svc" "$kind"
      wait_ready "$svc" "$kind" "$port"
    done
  done
  cmd_status "${filter:-}"
}

cmd_status() {
  local filter="${1:-}" svc e port pid state
  printf '%-12s %-8s %-6s %s\n' SERVICE PID PORT STATE
  for e in "${ORDERED_SERVICES[@]}"; do
    svc="$(svc_field "$e" 1)"; port="$(svc_field "$e" 3)"
    if [ -n "$filter" ]; then
      local want=",$filter,"; [ "${want#*,$svc,}" != "$want" ] || continue
    fi
    pid="$(cat "$(pid_file "$svc")" 2>/dev/null || echo -)"
    if is_running "$svc"; then state="running"; else state="down"; pid="-"; fi
    printf '%-12s %-8s %-6s %s\n' "$svc" "$pid" "$port" "$state"
  done
}

cmd_down() {
  local filter="${1:-}" svc
  for svc in $(svc_names); do
    if [ -n "$filter" ]; then
      local want=",$filter,"; [ "${want#*,$svc,}" != "$want" ] || continue
    fi
    stop_one "$svc"
  done
}

cmd_restart() {
  local svc="${1:-}"
  [ -n "$svc" ] || die "用法: bring-up.sh restart <svc>"
  stop_one "$svc"
  cmd_up "$svc" ""
}

usage() {
  cat <<'USAGE'
用法: bash scripts/bring-up.sh <up|down|status|restart> [args]

  up       [--services <csv>] [--reload]   按依赖序启动（--reload 先停再起）
  down     [--services <csv>]              停止
  status   [--services <csv>]              查看存活状态
  restart  <svc>                           单服务重启

环境变量: HUBX_DB_PW / HUBX_BIN_DIR / HUBX_LOG_DIR / HUBX_NO_BUILD / HUBX_JOB_MODE / GO_BIN
USAGE
}

main() {
  local cmd="${1:-}"; shift || true
  local filter="" reload="" positional=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --services) filter="${2:-}"; shift 2 ;;
      --reload)   reload="reload"; shift ;;
      -h|--help)  usage; exit 0 ;;
      *)          positional="$1"; shift ;;
    esac
  done
  case "$cmd" in
    ""|-h|--help) usage; [ "$cmd" = "" ] && exit 1; exit 0 ;;
    up)      cmd_up "$filter" "$reload" ;;
    down)    cmd_down "$filter" ;;
    status)  cmd_status "$filter" ;;
    restart) cmd_restart "$positional" ;;
    "")      usage; exit 1 ;;
    *)       die "unknown command: $cmd" ;;
  esac
}

main "$@"
