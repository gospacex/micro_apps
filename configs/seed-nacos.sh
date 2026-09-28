#!/usr/bin/env bash
# ============================================================
# seed-nacos.sh —— 把 configs/ 灌入 Nacos（compose 起完执行一次）
#
# dataId 命名规则（group 统一 HUBX）：
#   configs/trace.yaml          -> dataId hubx.trace
#   configs/services/<svc>.yaml -> dataId hubx.<svc>
#
# Nacos 由 docker/compose.yaml 起（standalone，NACOS_AUTH_ENABLE=false，
# OpenAPI 免登录）；发布后逐个 GET 回读逐字比对，任何失败非零退出。
#
# 用法：
#   bash configs/seed-nacos.sh
#   HUBX_NACOS_SERVER=127.0.0.1:8848 HUBX_NACOS_GROUP=HUBX bash configs/seed-nacos.sh
# ============================================================
set -euo pipefail

NACOS_SERVER="${HUBX_NACOS_SERVER:-127.0.0.1:8848}"
NACOS_GROUP="${HUBX_NACOS_GROUP:-HUBX}"
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { printf '[seed-nacos] %s\n' "$*"; }
die() { printf '[seed-nacos] ERROR: %s\n' "$*" >&2; exit 1; }

# --- 等 Nacos 就绪（起得慢，最多约 2 分钟）---
wait_nacos() {
  # show=all 检索接口在无任何配置时也返回 200，可作就绪探针
  local url="http://${NACOS_SERVER}/nacos/v1/cs/configs?dataId=seed-probe&group=public&show=all"
  local i
  for i in $(seq 1 60); do
    if curl -sf --max-time 10 "$url" >/dev/null 2>&1; then
      return 0
    fi
    log "nacos not ready (${i}/60) ..."
    sleep 2
  done
  return 1
}

# --- 发布 + 回读校验（Nacos 写后即读偶发抖动，失败带退避重试）---
seed_one() {
  local file="$1" dataId="$2"
  local resp remote attempt rc
  for attempt in 1 2 3 4 5; do
    resp="$(curl -sf --max-time 30 -X POST "http://${NACOS_SERVER}/nacos/v1/cs/configs" \
      --data-urlencode "dataId=${dataId}" \
      --data-urlencode "group=${NACOS_GROUP}" \
      --data-urlencode "type=yaml" \
      --data-urlencode "content@${file}")" || resp=""
    if [ "$resp" != "true" ]; then
      log "publish retry ${attempt}/5 (resp=${resp:-curl-fail}): dataId=${dataId}"
      sleep 2
      continue
    fi

    remote="$(mktemp)"
    rc=0
    curl -sf --max-time 30 -o "$remote" \
      "http://${NACOS_SERVER}/nacos/v1/cs/configs?dataId=${dataId}&group=${NACOS_GROUP}" || rc=$?
    if [ "$rc" != 0 ]; then
      rm -f "$remote"
      log "read-back retry ${attempt}/5 (curl rc=${rc}): dataId=${dataId}"
      sleep 2
      continue
    fi
    if ! diff -q "$remote" "$file" >/dev/null 2>&1; then
      rm -f "$remote"
      log "read-back mismatch, retry ${attempt}/5: dataId=${dataId}"
      sleep 2
      continue
    fi
    rm -f "$remote"
    log "seeded ${dataId} (group=${NACOS_GROUP}) <- ${file#"${BASE_DIR}"/}"
    return 0
  done
  die "seed failed after 5 attempts: dataId=${dataId}"
}

# --- 主流程 ---
log "nacos server: ${NACOS_SERVER}, group: ${NACOS_GROUP}"
wait_nacos || die "nacos not ready at ${NACOS_SERVER} (timeout)"

seed_one "${BASE_DIR}/trace.yaml" "hubx.trace"

for f in "${BASE_DIR}"/services/*.yaml; do
  svc="$(basename "$f" .yaml)"
  seed_one "$f" "hubx.${svc}"
done

log "done: 1 + $(ls "${BASE_DIR}"/services/*.yaml | wc -l | tr -d ' ') configs published"
