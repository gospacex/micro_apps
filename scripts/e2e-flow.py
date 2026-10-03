#!/usr/bin/env python3
# e2e-flow.py —— 主链路跨进程 trace 证据（任务 16）
#
# 做什么：
#   1. （可选）按依赖序起 11 服务（scripts/bring-up.sh up）
#   2. 经 gateway :8080 打一条带 JWT 的请求，注入固定 traceparent
#   3. 等 BatchSpanProcessor flush（≥10s，已知坑：不等就拉不到完整 span 树）
#   4. 走 Jaeger API 拉该 trace，断言「同一 trace_id 出现在 ≥N 个服务」
#   5. 打印 span 树 + 落 Markdown 证据到 docs/verify/e2e-evidence.md
#
# 只依赖标准库（urllib / hmac / json），不装任何东西即可跑。
#
# 用法：
#   python3 scripts/e2e-flow.py                 # 起服务 + 打请求 + 断言
#   python3 scripts/e2e-flow.py --no-up         # 服务已起，只打请求
#   python3 scripts/e2e-flow.py --min-services 3
#   python3 scripts/e2e-flow.py --out docs/verify/e2e-evidence.md
#
# 退出码：0 = 断言通过；1 = 断言失败；2 = 环境不可用（Jaeger 不可达等）
import argparse
import base64
import hashlib
import hmac
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

JAEGER = os.environ.get("HUBX_JAEGER", "http://127.0.0.1:16686")
GATEWAY = os.environ.get("HUBX_GATEWAY", "http://127.0.0.1:8080")
JWT_SECRET = os.environ.get("HUBX_JWT_SECRET", "hubx-dev-jwt-secret")
JWT_ISSUER = "srv-user"

# 宿主有 HTTP(S) 代理时，访问 127.0.0.1 必须绕代理，否则 curl/urllib 全挂
NO_PROXY = urllib.request.ProxyHandler({})
OPENER = urllib.request.build_opener(NO_PROXY)

# 主链路请求集：(方法, 路径, 是否必须成功)
# 路径走 gateway 的路由前缀（apps/gateway/config.yaml：/api/app → bff-app）
# 路径经 gateway 前缀（apps/gateway/config.yaml：/api/app → bff-app）落到
# bff-app 的真实路由（apps/bff-app/internal/handler/handler.go Register）。
# /home 是聚合面（一次打 user/product/order/coupon 四个 srv），是主链路证据的来源。
REQUESTS = [
    ("GET", "/api/app/home", True),            # 聚合：profile + 近期订单 + 券
    ("GET", "/api/app/users/profile", False),  # 单 srv-user
    ("GET", "/api/app/products", False),       # 单 srv-product
    ("GET", "/api/app/coupons", False),        # 单 srv-coupon
    ("GET", "/api/app/orders/report", False),  # srv-order 4 表 JOIN（矩阵锚点）
]


def log(msg):
    print(msg, flush=True)


# ---------- HTTP ----------

def http_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "e2e-flow"})
    with OPENER.open(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def http_call(method, url, headers=None, body=None, timeout=20):
    data = body.encode("utf-8") if isinstance(body, str) else body
    req = urllib.request.Request(url, data=data, method=method)
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with OPENER.open(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8", "replace")
            return r.status, raw
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        return e.code, raw
    except Exception as e:  # noqa: BLE001 - 网络层错误统一成 (0, 原因)
        return 0, str(e)


# ---------- JWT（HS256，与 gateway/srv-user 同密钥同 issuer）----------

def b64u(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b"=")


def make_jwt(sub="1", ttl=3600):
    header = b64u(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    now = int(time.time())
    payload = b64u(json.dumps(
        {"sub": sub, "iss": JWT_ISSUER, "iat": now, "exp": now + ttl},
        separators=(",", ":")).encode())
    signing = header + b"." + payload
    sig = hmac.new(JWT_SECRET.encode(), signing, hashlib.sha256).digest()
    return (signing + b"." + b64u(sig)).decode()


# ---------- Jaeger ----------

def fetch_trace(trace_id, retries=6, gap=4):
    """按 traceID 拉 trace；BatchSpanProcessor 5s 一批 + 网络，需重试等待。"""
    url = f"{JAEGER}/api/traces/{trace_id}"
    last = None
    for i in range(retries):
        try:
            data = http_json(url)
            if data.get("data"):
                return data["data"][0]
        except Exception as e:  # noqa: BLE001
            last = e
        time.sleep(gap)
    if last:
        raise RuntimeError(f"拉取 trace 失败：{last}")
    return None


def span_tree(trace):
    """把 Jaeger 的 trace 结构拍平成 (服务, span 名, 父 span 名)。"""
    procs = trace.get("processes", {})
    by_id = {s["spanID"]: s for s in trace.get("spans", [])}
    rows = []
    for s in sorted(trace.get("spans", []), key=lambda x: x.get("startTime", 0)):
        svc = procs.get(s.get("processID"), {}).get("serviceName", "?")
        refs = s.get("references") or []
        parent = by_id.get(refs[0]["spanID"]) if refs else None
        if parent:
            pname = parent["operationName"]
        elif refs:
            pname = f"<MISSING {refs[0]['spanID'][:8]}>"
        else:
            pname = "<ROOT>"
        rows.append((svc, s["operationName"], pname))
    return rows


def tag_of(span, key):
    for t in span.get("tags", []):
        if t.get("key") == key:
            return t.get("value")
    return None


# ---------- 主流程 ----------

def bring_up(repo):
    script = os.path.join(repo, "scripts", "bring-up.sh")
    log("[1/5] 起 11 服务（scripts/bring-up.sh up）...")
    env = dict(os.environ)
    # 刻意不 capture_output：bring-up.sh 会把 11 个服务 nohup 到后台，用管道接
    # stdout 时子进程的 fd 继承会让 subprocess 等不到 EOF 而挂死。改落文件。
    bringup_log = os.path.join(repo, ".hubx-bin", "bring-up.log")
    os.makedirs(os.path.dirname(bringup_log), exist_ok=True)
    try:
        with open(bringup_log, "w", encoding="utf-8") as lf:
            p = subprocess.run(["bash", script, "up"], cwd=repo, env=env,
                               stdout=lf, stderr=subprocess.STDOUT, timeout=900)
    except subprocess.TimeoutExpired:
        raise SystemExit(f"bring-up.sh 超时（>{900}s），看日志 {bringup_log}")
    if p.returncode != 0:
        with open(bringup_log, encoding="utf-8", errors="replace") as f:
            log(f.read()[-2500:])
        raise SystemExit(f"bring-up.sh 失败（exit={p.returncode}），日志 {bringup_log}")
    with open(bringup_log, encoding="utf-8", errors="replace") as f:
        log(f.read().strip()[-1200:])


def main():
    ap = argparse.ArgumentParser(description="主链路跨进程 trace 证据")
    ap.add_argument("--no-up", action="store_true", help="不起服务（已起时用）")
    ap.add_argument("--min-services", type=int, default=6,
                    help="断言：同一 trace_id 至少出现在多少个服务（默认 6）")
    ap.add_argument("--trace-id", default=None, help="固定 trace-id（32 hex，默认随机）")
    ap.add_argument("--flush", type=int, default=14, help="等 span flush 的秒数（默认 14）")
    ap.add_argument("--out", default="docs/verify/e2e-evidence.md", help="Markdown 证据落盘路径")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(repo)

    try:
        http_json(f"{JAEGER}/api/services", timeout=5)
    except Exception as e:  # noqa: BLE001
        log(f"[BLOCKED] Jaeger 不可达（{JAEGER}）：{e}")
        log("        先 docker compose up -d 起 jaeger，或设 HUBX_JAEGER 指向别处")
        return 2

    if not args.no_up:
        bring_up(repo)
    else:
        log("[1/5] --no-up：跳过起服务")

    trace_id = args.trace_id or hashlib.sha1(str(time.time()).encode()).hexdigest()[:32]
    parent_span = "00f067aa0ba902b7"
    tp = f"00-{trace_id}-{parent_span}-01"
    token = make_jwt("1")

    log(f"[2/5] 打请求：gateway={GATEWAY} trace_id={trace_id}")
    results = []
    for method, path, required in REQUESTS:
        url = GATEWAY + path
        code, body = http_call(method, url, headers={
            # gateway 的 JWT 验签读 Authorization: Bearer（apps/gateway/config.yaml
            # 的 jwt.secret/issuer；本脚本用同参数自签，免外部依赖）
            "Authorization": "Bearer " + token,
            "traceparent": tp,
            "tracestate": "hubx=e2e",
            "X-User-Id": "1",
        })
        snippet = body[:160].replace("\n", " ")
        log(f"      {method} {path} -> HTTP {code}  {snippet}")
        results.append((method, path, code, required, snippet))

    failed_required = [r for r in results if r[3] and not (200 <= r[2] < 300)]
    if failed_required:
        log("[WARN] 必需请求未 2xx（链路证据可能不完整）：")
        for m, p, c, _, s in failed_required:
            log(f"       {m} {p} -> {c} {s}")

    log(f"[3/5] 等 span flush {args.flush}s ...")
    time.sleep(args.flush)

    log("[4/5] 拉 Jaeger trace 并断言")
    trace = fetch_trace(trace_id)
    if not trace:
        log(f"[FAIL] Jaeger 上查不到 trace {trace_id}")
        return 1
    rows = span_tree(trace)
    services = sorted({r[0] for r in rows})
    if not rows:
        log("[FAIL] trace 无 span")
        return 1

    log(f"      span 数 = {len(rows)}，服务数 = {len(services)}：{', '.join(services)}")
    for svc, name, parent in rows:
        log(f"      {svc:10s} | {name[:40]:40s} | parent={parent[:30]}")

    # messaging 断言：srv-order 的 send 与 job 的 receive 应同 trace
    msgs = [s for s in trace.get("spans", [])
            if s["operationName"].startswith("messaging.")]
    msg_note = "（本轮未触发 Kafka，标记 SKIP）"
    if msgs:
        names = sorted({s["operationName"] for s in msgs})
        send = [n for n in names if ".send" in n or "publish" in n]
        recv = [n for n in names if ".receive" in n or ".process" in n or "consume" in n]
        msg_note = "PASS（send+receive 同 trace）" if (send and recv) else \
                   f"PARTIAL（只有 {names}）"

    ok = len(services) >= args.min_services
    missing_parent = [r for r in rows if r[2].startswith("<MISSING")]
    degraded = [s for s in trace.get("spans", []) if tag_of(s, "degraded") is True]

    log("[5/5] 结论")
    log(f"      同 trace 服务数 {len(services)} / 门槛 {args.min_services} -> {'PASS' if ok else 'FAIL'}")
    log(f"      父 span 缺失的孤儿 span：{len(missing_parent)}")
    log(f"      降级 span（degraded=true）：{len(degraded)}")
    log(f"      messaging 续链：{msg_note}")

    # Markdown 证据
    lines = [
        "# 主链路跨进程 trace 证据（e2e-flow.py）",
        "",
        f"- 生成时间：{time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- gateway：`{GATEWAY}` / Jaeger：`{JAEGER}`",
        f"- trace_id：`{trace_id}`（注入 traceparent `00-{trace_id}-{parent_span}-01`）",
        f"- 同 trace 服务数：**{len(services)}**（门槛 {args.min_services}）→ **{'PASS' if ok else 'FAIL'}**",
        f"- span 总数：{len(rows)}；孤儿 span（父不在本 trace）：{len(missing_parent)}",
        f"- messaging 续链：{msg_note}",
        "",
        "## 请求与响应",
        "",
        "| 方法 | 路径 | HTTP | 响应摘要 |",
        "| --- | --- | --- | --- |",
    ]
    for m, p, c, req_, s in results:
        lines.append(f"| {m} | `{p}` | {c} | {s[:80]} |")
    lines += ["", "## span 树", "", "| 服务 | span | 父 span |", "| --- | --- | --- |"]
    for svc, name, parent in rows:
        lines.append(f"| {svc} | `{name}` | `{parent}` |")
    lines += ["", "## 参与服务", ""]
    for s in services:
        lines.append(f"- {s}")
    out = os.path.join(repo, args.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    log(f"      证据落盘：{out}")

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
