#!/usr/bin/env bash
# ============================================================
# seed-nacos.sh：把 configs/trace.yaml + configs/services/*.yaml 发布进 Nacos
#
# 用法（repo 根目录执行，compose 已 up）：
#   docker compose -f docker/compose.yaml up -d nacos && bash configs/seed-nacos.sh
#
# dataId 命名规则（与配置契约对齐）：
#   configs/trace.yaml          → dataId hubx.trace   group HUBX
#   configs/services/<svc>.yaml → dataId hubx.<svc>   group HUBX
#
# curl 写法对齐 configx/example/main.go 的 publishNacosConfig：
#   POST http://127.0.0.1:8848/nacos/v1/cs/configs（form: dataId/group/type/content）
# ============================================================
set -euo pipefail

# 脚本定位 repo 根目录（configs/ 的上一级），任何目录执行均可
cd "$(dirname "${BASH_SOURCE[0]}")/.."

NACOS_API="http://127.0.0.1:8848/nacos/v1/cs/configs"
GROUP="HUBX"
NACOS_TIMEOUT_S=90

# ---------- Nacos 就绪轮询 ----------
# 探测：GET /nacos/v1/cs/configs?dataId=x&show=all
#   - Nacos 未起/未就绪 → 连接失败或 HTTP 4xx（curl -f 非 0）
#   - 就绪（即使 dataId=x 不存在）→ HTTP 200，body "config data not exist"，curl 0
wait_nacos_ready() {
  local deadline=0 now
  now() { date +%s; }
  deadline=$(( $(now) + NACOS_TIMEOUT_S ))
  echo "[seed-nacos] waiting for Nacos at ${NACOS_API} …"
  while ! curl -sf -o /dev/null "${NACOS_API}?dataId=x&show=all" 2>/dev/null; do
    if (( $(now) >= deadline )); then
      echo "[seed-nacos] FATAL: Nacos 未在 ${NACOS_TIMEOUT_S}s 内就绪（docker compose -f docker/compose.yaml up -d nacos?）" >&2
      exit 1
    fi
    sleep 3
  done
  echo "[seed-nacos] Nacos ready"
}

# ---------- 发布单个配置（写法对齐 publishNacosConfig）----------
# publish <dataId> <yamlFile>
publish() {
  local data_id="$1" file="$2" resp
  resp=$(curl -fsS -X POST \
    --data-urlencode "dataId=${data_id}" \
    --data-urlencode "group=${GROUP}" \
    --data-urlencode "type=yaml" \
    --data-urlencode "content@${file}" \
    -o /dev/null -w "%{http_code}" \
    "${NACOS_API}")
  if [[ "${resp}" != "200" ]]; then
    echo "[seed-nacos] FATAL: publish ${data_id} failed (HTTP ${resp})" >&2
    exit 1
  fi
  echo "[seed-nacos] published ${data_id} (group=${GROUP}) ← ${file}"
}

wait_nacos_ready

# 1) 统一 trace 段：11 服务共读 dataId hubx.trace
publish "hubx.trace" "configs/trace.yaml"

# 2) 各服务私有配置：dataId hubx.<svc>
for f in configs/services/*.yaml; do
  svc="$(basename "${f}" .yaml)"
  publish "hubx.${svc}" "${f}"
done

echo "[seed-nacos] done ($(ls configs/services/*.yaml | wc -l | tr -d ' ') service configs + trace.yaml)"
