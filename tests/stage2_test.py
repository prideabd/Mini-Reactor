#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
阶段二（背压与流控）验收脚本 v2  ——  reactor::net 反向代理

拓扑:
    慢客户端 <--> proxy_server(127.0.0.1:8080) <--> 上游(127.0.0.1:9000)

v2 修正了 v1 的重大测量缺陷:
    Buffer 底层 vector 只扩容、不归还 capacity，RSS 只增不减。
    v1 在同一进程里先跑 test2（8 并发×64MiB）再测 test3，
    基线被污染到几百 MiB，导致误判。
    v2 在 test3 前重启代理，并改用【增长量】而非绝对值判定。

用法:
    python3 stage2_test.py --proxy ./build/proxy_server
    python3 stage2_test.py --proxy ./build/proxy_server --only-backpressure
"""
import argparse, hashlib, os, socket, subprocess, sys, threading, time
import http.server, socketserver

UP_PORT    = 9000
PROXY_PORT = 8080
BIG        = "big.bin"
BIG_SIZE   = 64 * 1024 * 1024


def human(n): return f"{n/1024/1024:.1f} MiB"


class Handler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a): pass


class ThreadingHTTP(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def rss_kb(pid):
    try:
        for line in open(f"/proc/{pid}/status"):
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except Exception:
        pass
    return 0


def check_port(port):
    try:
        socket.create_connection(("127.0.0.1", port), timeout=2).close()
        return True
    except Exception:
        return False


def http_get(port, path="/" + BIG, slow=False, pid=None):
    """HTTP/1.0 请求。slow=True 时限速读取，并采样代理 RSS。"""
    s = socket.create_connection(("127.0.0.1", port), timeout=180)
    s.sendall(f"GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n".encode())
    h = hashlib.sha256()
    total, hdr_done, buf = 0, False, b""
    peak, samples, last = 0, [], time.time()
    while True:
        chunk = s.recv(16384 if slow else 1 << 20)
        if not chunk:
            break
        if not hdr_done:
            buf += chunk
            if b"\r\n\r\n" in buf:
                body = buf.split(b"\r\n\r\n", 1)[1]
                h.update(body); total += len(body); hdr_done = True
            continue
        h.update(chunk); total += len(chunk)
        if slow:
            time.sleep(0.004)          # 限速，制造下游积压
            if pid and time.time() - last > 0.3:
                r = rss_kb(pid)
                peak = max(peak, r); samples.append(r); last = time.time()
    s.close()
    return h.hexdigest(), total, peak, samples


def spawn_proxy(path):
    p = subprocess.Popen([path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(1.2)
    if p.poll() is not None:
        print(f"[FAIL] 代理启动失败（退出码 {p.returncode}），检查路径与端口占用")
        sys.exit(1)
    return p


def kill_proxy(p):
    if not p:
        return
    p.terminate()
    try:
        p.wait(timeout=5)
    except Exception:
        p.kill()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proxy", default="./proxy_server")
    ap.add_argument("--no-spawn", action="store_true", help="不自动启动代理")
    ap.add_argument("--only-backpressure", action="store_true",
                    help="只跑 test3（基线最干净）")
    args = ap.parse_args()

    if not os.path.exists(BIG) or os.path.getsize(BIG) != BIG_SIZE:
        print(f"[setup] 生成 {BIG} ({human(BIG_SIZE)}) ...")
        with open(BIG, "wb") as f:
            for _ in range(BIG_SIZE // (1 << 20)):
                f.write(os.urandom(1 << 20))
    expect = hashlib.sha256(open(BIG, "rb").read()).hexdigest()
    print(f"[setup] 基准 sha256 = {expect[:20]}...")

    up = ThreadingHTTP(("127.0.0.1", UP_PORT), Handler)
    threading.Thread(target=up.serve_forever, daemon=True).start()
    print(f"[setup] 上游源站 127.0.0.1:{UP_PORT} 已启动")

    proxy = None if args.no_spawn else spawn_proxy(args.proxy)
    pid = proxy.pid if proxy else None
    if pid is None:
        try:
            out = subprocess.check_output(["pgrep", "-f", "proxy_server"], text=True)
            c = [int(x) for x in out.split() if x.strip().isdigit() and int(x) != os.getpid()]
            if c:
                pid = c[0]
                print(f"[setup] 找到外部代理进程 pid={pid}")
        except Exception:
            pass
    if proxy:
        print(f"[setup] 代理已启动 pid={pid} listen={PROXY_PORT}")

    if not check_port(PROXY_PORT):
        print(f"[FAIL] 代理端口 {PROXY_PORT} 连不上")
        sys.exit(1)

    ok = True
    try:
        if not args.only_backpressure:
            hd, ld, *_ = http_get(UP_PORT)
            hp, lp, *_ = http_get(PROXY_PORT)
            print(f"\n[test1] 直连  : {human(ld)}  {hd[:20]}")
            print(f"[test1] 经代理: {human(lp)}  {hp[:20]}")
            if hd == hp == expect:
                print("[test1] ✅ PASS 数据完整性一致")
            else:
                print("[test1] ❌ FAIL 字节流不一致"); ok = False

            res = {}
            def w(i): res[i] = http_get(PROXY_PORT)[0]
            ts = [threading.Thread(target=w, args=(i,)) for i in range(8)]
            [t.start() for t in ts]; [t.join() for t in ts]
            bad = [i for i, v in res.items() if v != expect]
            if not bad:
                print("[test2] ✅ PASS 并发 8/8 哈希一致，未串数据")
            else:
                print(f"[test2] ❌ FAIL 连接 {bad} 数据错误"); ok = False

        # ---------------- test3 背压（核心）----------------
        # ⚠️ Buffer 底层 vector 不归还 capacity，RSS 只增不减。
        # test1/test2 会把基线抬到几百 MiB，必须重启代理才能拿到干净基线。
        if proxy and not args.only_backpressure:
            kill_proxy(proxy)
            time.sleep(0.6)
            proxy = spawn_proxy(args.proxy)
            pid = proxy.pid
            print(f"\n[test3] 已重启代理 pid={pid}，获取干净内存基线")
        elif not proxy:
            print("\n[test3] ⚠️  --no-spawn 无法重启代理，基线可能被污染；"
                  "建议重启代理后配 --only-backpressure 单跑")

        base = rss_kb(pid) if pid else 0
        print(f"[test3] 基线 RSS = {base/1024:.1f} MiB")
        print(f"[test3] 慢客户端下载 {human(BIG_SIZE)}，监控代理 RSS ...")
        hs, gs, peak, samples = http_get(PROXY_PORT, slow=True, pid=pid)
        print(f"[test3] 收到 {human(gs)}  sha匹配={hs == expect}")

        if pid:
            growth = (peak - base) / 1024.0
            limit  = BIG_SIZE / 1024 / 1024 / 4      # 文件体积的 1/4
            print(f"[test3] RSS 峰值 = {peak/1024:.1f} MiB（较基线增长 {growth:.1f} MiB）")
            print(f"[test3] RSS 采样(KB) = {samples}")
            # 判定看增长量：有流控 -> 几 MiB；无流控 -> 逗近 64MiB
            if hs == expect and growth < limit:
                print(f"[test3] ✅ PASS 背压生效（增长 {growth:.1f} < 阈值 {limit:.0f} MiB）")
            else:
                print(f"[test3] ❌ FAIL 增长 {growth:.1f} MiB 超阈值 {limit:.0f} MiB，或数据错误")
                ok = False
        else:
            print("[test3] ⚠️  无 pid，跳过内存判定")
            if hs != expect:
                ok = False

        for lg in ("reactor_proxy.log", "proxy.log"):
            if os.path.exists(lg):
                txt = open(lg, encoding="utf-8", errors="ignore").read()
                p, r = txt.count("暂停读"), txt.count("恢复读")
                print(f"\n[日志] {lg}: 暂停 {p} 次，恢复 {r} 次")
                if p:
                    print("[日志] ✅ HighWaterMark -> DisableReading 实际被触发")
                break
    finally:
        kill_proxy(proxy)
        up.shutdown()

    print("\n" + "=" * 46)
    print("阶段二验收结果:", "✅ 全部通过" if ok else "❌ 存在失败项")
    print("=" * 46)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
