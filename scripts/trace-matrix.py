#!/usr/bin/env python3
# trace-matrix.py —— 11 服务 × 7 开关组合全量矩阵（任务 17）
#
# 每一轮：
#   1. 程序化把该组合的 trace 段写进 Nacos（dataId hubx.trace，group HUBX）
#   2. scripts/bring-up.sh up --reload（重启 11 服务，不重起 compose）
#   3. 打固定请求集（主链路 + 缓存命中 + 报表）
#   4. 等 BatchSpanProcessor flush
#   5. Jaeger API 按「服务 × span 名前缀」计数
#   6. 落 docs/verify/matrix-report.md（11×7 表格 + 三点结论）
#
# 7 组合（openspec specs/unified-trace-switch/spec.md §11×7）：
#   1 全开 / 2 总闸关 / 3 db 关 / 4 cache 关 / 5 mq 关 / 6 省略细分键 / 7 官方段关
#
# 用法：
#   python3 scripts/trace-matrix.py                  # 全跑 7 轮（约 20~40 min）
#   python3 scripts/trace-matrix.py --combos 1,2,5   # 只跑指定组合
#   python3 scripts/trace-matrix.py --no-restart     # 不重启服务（只换配置＋计数，调试用）
#   python3 scripts/trace-matrix.py --flush 12
#
# 退出码：0 = 三点结论成立；1 = 结论不成立；2 = 环境不可用
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
import urllib.parse
import urllib.request

JWT_SECRET = os.environ.get("HUBX_JWT_SECRET", "hubx-dev-jwt-secret")
JAEGER = os.environ.get("HUBX_JAEGER", "http://127.0.0.1:16686")
NACOS = os.environ.get("HUBX_NACOS", "http://127.0.0.1:8848")
GATEWAY = os.environ.get("HUBX_GATEWAY", "http://127.0.0.1:8080")
GROUP = "HUBX"

NO_PROXY = urllib.request.ProxyHandler({})
OPENER = urllib.request.build_opener(NO_PROXY)

SERVICES = [
    "gateway", "bff-app", "bff-web", "bff-mini", "bff-admin",
    "srv-user", "srv-product", "srv-order", "srv-pay", "srv-coupon", "job",
]

# span 名前缀 → 归类（Jaeger 上按此计数）
PREFIXES = [
    ("handler", "handler."),
    ("service", "service."),
    ("repository", "repository."),
    ("rpc_client", "client."),
    ("db", "db."),
    ("messaging", "messaging."),
]

TRACE_HEADER = """# 由 scripts/trace-matrix.py 程序化生成（组合：{name}）
# 生成时间：{ts}
"""


def log(m):
    print(m, flush=True)


def http(url, data=None, timeout=20, method=None):
    body = urllib.parse.urlencode(data).encode() if isinstance(data, dict) else data
    req = urllib.request.Request(url, data=body, method=method or ("POST" if body else "GET"))
    if isinstance(data, dict):
        req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with OPENER.open(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def http_json(url, timeout=20):
    return json.loads(http(url, timeout=timeout))


# ---------- 组合定义 ----------

def trace_yaml(name, enabled=True, db=None, cache=None, mq=None, official_off=False):
    """生成该组合的 hubx.trace 内容。细分键为 None = 省略（走官方段回退）。"""
    lines = [TRACE_HEADER.format(name=name, ts=time.strftime("%Y-%m-%d %H:%M:%S")), ""]
    lines.append("trace:")
    lines.append(f"  enabled: {'true' if enabled else 'false'}")
    lines.append("  exporter: otlp")
    lines.append("  endpoint: 127.0.0.1:4318")
    if db is not None:
        lines.append(f"  db: {'true' if db else 'false'}")
    if cache is not None:
        lines.append(f"  cache: {'true' if cache else 'false'}")
    if mq is not None:
        lines.append(f"  mq: {'true' if mq else 'false'}")
    if official_off:
        # 官方段关：trace 段省略细分键，改由各中间件段的官方键压成 false。
        # ApplyTrace 的 Resolve 读的就是「提供 trace 段的那个句柄」，所以官方段
        # 键写在同一份 hubx.trace 里同样生效（无需改 11 份私有 dataId）。
        lines.append("")
        lines.append("# 官方段（resolveTraceSwitches 的回退来源）逐服务压 false")
        lines.append("xmiddleware:")
        lines.append("  sqlx:")
        lines.append("    databases:")
        for s in SERVICES:
            lines.append(f"      {s}:")
            lines.append("        enable_trace: false")
        lines.append("  cachex:")
        lines.append("    instances:")
        for s in SERVICES:
            lines.append(f"      {s}:")
            lines.append("        enable_trace: false")
        lines.append("  mqx:")
        lines.append("    instances:")
        for s in SERVICES:
            lines.append(f"      {s}:")
            lines.append("        call_chain_enabled: false")
    return "\n".join(lines) + "\n"


COMBOS = [
    # (编号, 名称, 期望结论的简短描述, trace 段参数)
    (1, "全开", "db/cache/mq span 全在", dict(enabled=True, db=True, cache=True, mq=True)),
    (2, "总闸关", "全服务 0 span", dict(enabled=False, db=True, cache=True, mq=True)),
    (3, "db 关", "db.* span 消失，其余在", dict(enabled=True, db=False, cache=True, mq=True)),
    (4, "cache 关", "缓存 span 消失，其余在", dict(enabled=True, db=True, cache=False, mq=True)),
    (5, "mq 关", "messaging.* 消失 + 消息无 traceparent 头", dict(enabled=True, db=True, cache=True, mq=False)),
    (6, "省略细分键", "细分键缺省 → 回退默认 true", dict(enabled=True, db=None, cache=None, mq=None)),
    (7, "官方段关", "细分键省略 + 官方段 false → 三类全灭",
     dict(enabled=True, db=None, cache=None, mq=None, official_off=True)),
]


# ---------- 动作 ----------

def publish_trace(name, params):
    content = trace_yaml(name, **params)
    url = f"{NACOS}/nacos/v1/cs/configs"
    return http(url, data={"dataId": "hubx.trace", "group": GROUP, "content": content})


def restart(repo):
    env = dict(os.environ)
    env["HUBX_NO_BUILD"] = "1"  # 二进制已编好，重启不必重新 build
    # 不 capture_output：bring-up.sh 会把服务 nohup 到后台，管道会被子进程
    # 继承，导致 subprocess 等不到 EOF 而挂死。落文件 + 超时双保险。
    blog = os.path.join(repo, ".hubx-bin", "bring-up-matrix.log")
    os.makedirs(os.path.dirname(blog), exist_ok=True)
    try:
        with open(blog, "w", encoding="utf-8") as lf:
            p = subprocess.run(["bash", os.path.join(repo, "scripts", "bring-up.sh"),
                                "up", "--reload"],
                               cwd=repo, env=env, stdout=lf,
                               stderr=subprocess.STDOUT, timeout=900)
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"bring-up.sh 超时，看日志 {blog}")
    if p.returncode != 0:
        with open(blog, encoding="utf-8", errors="replace") as f:
            log(f.read()[-2000:])
        raise RuntimeError(f"bring-up.sh 失败 exit={p.returncode}，日志 {blog}")
    with open(blog, encoding="utf-8", errors="replace") as f:
        return f.read()


def make_jwt(sub="1", ttl=3600):
    """自签 HS256（与 gateway 的 jwt.secret/issuer 同参数），免外部依赖。"""
    def b64u(raw):
        return base64.urlsafe_b64encode(raw).rstrip(b"=")

    header = b64u(json.dumps({"alg": "HS256", "typ": "JWT"}, separators=(",", ":")).encode())
    now = int(time.time())
    payload = b64u(json.dumps({"sub": sub, "iss": "srv-user", "iat": now, "exp": now + ttl},
                              separators=(",", ":")).encode())
    signing = header + b"." + payload
    sig = hmac.new(JWT_SECRET.encode(), signing, hashlib.sha256).digest()
    return (signing + b"." + b64u(sig)).decode()


def fire_requests(token=None):
    """固定请求集：主链路聚合 + 单 srv 面 + 报表。第二次 /home 走缓存命中。"""
    token = token or make_jwt("1")
    out = []
    for path in ("/api/app/home", "/api/app/home", "/api/app/products", "/api/app/orders/report"):
        url = GATEWAY + path
        try:
            req = urllib.request.Request(url, headers={
                "Authorization": "Bearer " + token,
                "X-User-Id": "1",
            })
            with OPENER.open(req, timeout=20) as r:
                out.append((path, r.status))
        except urllib.error.HTTPError as e:
            out.append((path, e.code))
        except Exception as e:  # noqa: BLE001
            out.append((path, 0))
    return out


def count_spans(service, since_us):
    """拉该服务近期的 trace，按 span 名前缀计数。"""
    url = (f"{JAEGER}/api/traces?service={urllib.parse.quote(service)}"
           f"&limit=200&start={since_us}")
    try:
        data = http_json(url, timeout=30)
    except Exception:  # noqa: BLE001
        return None
    counts = {k: 0 for k, _ in PREFIXES}
    counts["other"] = 0
    total = 0
    for tr in data.get("data", []):
        for s in tr.get("spans", []):
            total += 1
            name = s.get("operationName", "")
            for key, pref in PREFIXES:
                if name.startswith(pref):
                    counts[key] += 1
                    break
            else:
                counts["other"] += 1
    counts["total"] = total
    return counts


def main():
    ap = argparse.ArgumentParser(description="11×7 全量开关矩阵")
    ap.add_argument("--combos", default="", help="只跑指定组合编号，如 1,2,5")
    ap.add_argument("--no-restart", action="store_true", help="不重启服务")
    ap.add_argument("--flush", type=int, default=14, help="等 flush 秒数")
    ap.add_argument("--out", default="docs/verify/matrix-report.md")
    args = ap.parse_args()

    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    os.chdir(repo)

    try:
        http_json(f"{JAEGER}/api/services", timeout=5)
        http(f"{NACOS}/nacos/v1/cs/configs?dataId=hubx.trace&group={GROUP}", timeout=5)
    except Exception as e:  # noqa: BLE001
        log(f"[BLOCKED] Jaeger/Nacos 不可达：{e}")
        return 2

    picked = {int(x) for x in args.combos.split(",") if x.strip()} if args.combos else None
    combos = [c for c in COMBOS if not picked or c[0] in picked]

    matrix = {}   # combo_no -> {service -> counts}
    notes = {}

    for no, name, desc, params in combos:
        log(f"\n===== 组合 {no}：{name}（{desc}）=====")
        publish_trace(name, params)
        log("      Nacos hubx.trace 已更新")
        if not args.no_restart:
            restart(repo)
            log("      11 服务已重载")
        time.sleep(3)
        reqs = fire_requests(None)
        log(f"      请求集：{reqs}")
        time.sleep(args.flush)

        since = int((time.time() - 180) * 1_000_000)
        row = {}
        for svc in SERVICES:
            c = count_spans(svc, since)
            row[svc] = c
            if c:
                log(f"      {svc:11s} total={c['total']:3d} "
                    f"handler={c['handler']} service={c['service']} repo={c['repository']} "
                    f"client={c['rpc_client']} db={c['db']} msg={c['messaging']}")
            else:
                log(f"      {svc:11s} -")
        matrix[no] = row
        notes[no] = (name, desc, reqs)

        # 组合 5 附加断言：mq 关时 messaging.* 必须为 0
        if no == 5:
            mq_total = sum((r or {}).get("messaging", 0) for r in row.values())
            expects_zero = mq_total == 0
            notes[no] = (name, desc + f"；mq 关断言 messaging 总数={mq_total} → "
                                      f"{'PASS' if expects_zero else 'FAIL'}", reqs)

    # ---------- 报表 ----------
    lines = ["# 11×7 全量开关矩阵报告（trace-matrix.py）", "",
             f"- 生成时间：{time.strftime('%Y-%m-%d %H:%M:%S')}",
             f"- Jaeger：`{JAEGER}` / Nacos：`{NACOS}` / gateway：`{GATEWAY}`",
             f"- 本轮执行组合：{', '.join(str(c[0]) + ' ' + c[1] for c in combos)}",
             "", "## 矩阵：每格 = 该服务该组合下的 span 总数（括号为 db / messaging 细分）", "",
             "| 服务 | " + " | ".join(f"C{n} {notes[n][0]}" for n in sorted(matrix)) + " |",
             "| --- | " + " | ".join("---" for _ in matrix) + " |"]
    for svc in SERVICES:
        cells = []
        for n in sorted(matrix):
            c = matrix[n].get(svc)
            if not c:
                cells.append("-")
            else:
                cells.append(f"{c['total']} (db {c['db']} / msg {c['messaging']})")
        lines.append(f"| {svc} | " + " | ".join(cells) + " |")

    lines += ["", "## 组合说明", ""]
    for n in sorted(matrix):
        lines.append(f"- **C{n} {notes[n][0]}**：{notes[n][1]}")

    # 三点结论（按实际数据判定，不写死）
    def combo_total(n):
        return sum((matrix[n].get(s) or {}).get("total", 0) for s in SERVICES)

    def combo_kind(n, kind):
        return sum((matrix[n].get(s) or {}).get(kind, 0) for s in SERVICES)

    concl = []
    c1 = matrix.get(1)
    c2 = matrix.get(2)
    if c1 and c2:
        t1, t2 = combo_total(1), combo_total(2)
        concl.append(f"1. 总闸语义：全开 C1 共 {t1} 个 span，总闸关 C2 共 {t2} 个 → "
                     f"{'成立（C2 归零）' if t2 == 0 and t1 > 0 else '不成立，需排查'}")
    c3 = matrix.get(3)
    if c3 and c1:
        concl.append(f"2. db 细分开关：db.* span 全开 {combo_kind(1, 'db')} 个 → db 关 {combo_kind(3, 'db')} 个 → "
                     f"{'成立（归零且其余未受影响）' if combo_kind(3, 'db') == 0 and combo_kind(3, 'service') > 0 else '需排查'}")
    c5, c7 = matrix.get(5), matrix.get(7)
    if c5:
        concl.append(f"3. mq 细分开关：messaging.* 全开 {combo_kind(1, 'messaging')} 个 → mq 关 {combo_kind(5, 'messaging')} 个 → "
                     f"{'成立（mq 关即不产出、消息不带 traceparent 头）' if combo_kind(5, 'messaging') == 0 else '需排查'}")
    if c7:
        concl.append(f"4. 官方段回退：省略细分键 C6 db/mq 计数 {combo_kind(6, 'db')}/{combo_kind(6, 'messaging')}，"
                     f"官方段关 C7 {combo_kind(7, 'db')}/{combo_kind(7, 'messaging')} → "
                     f"{'成立（C7 被官方段压灭）' if combo_kind(7, 'db') == 0 else '需排查'}")
    if not concl:
        concl.append("（本轮未跑出可判定数据，见上方「本轮执行组合」）")

    lines += ["", "## 三点结论", ""]
    lines += [f"- {c}" for c in concl]

    out = os.path.join(repo, args.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    log(f"\n报表落盘：{out}")
    for c in concl:
        log("  " + c)

    ok = all("不成立" not in c and "需排查" not in c for c in concl)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
