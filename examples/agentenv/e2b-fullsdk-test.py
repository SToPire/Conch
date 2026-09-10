"""Conch AgentENV E2B 全量接口测试。

依据最近三次提交（8936ad6 gateway/scheduler 集成、e66d1ca 单节点指南、
1ea77bb 多容器文档）所实现的功能，对 E2B SDK 暴露的接口做全量验证：

  1. 控制面 (Gateway REST)：/health /nodes /sandboxes CRUD、鉴权、
     分页 (limit/nextToken/order)、metadata 过滤、错误码、
     checkpoint 快照创建（指定名/自动名/同名更新）、
     pause/resume/connect/fork/timeout/refreshes 应返回 501。
  2. envd 数据接口 (SDK commands/files/pty)：前台/后台命令、stdin、
     流式回调、user/cwd/envs、目录与文件增删查读写、watch_dir、
     /files 上传下载、upload_url/download_url。
  3. code-interpreter (Jupyter 49999)：run_code 执行、会话状态保持、
     多 context 创建/列举/重启/删除、stdout/stderr 日志、富文本结果
     (text/html/svg/json)、执行错误回传、执行超时、envs。
  4. 数据面 (sandboxproxy)：header 路由 (E2b-Sandbox-Id/Port 与
     X-Agentenv-*)、/proxy 前缀路由、HTTP/SSE 流式（不缓冲）、
     WebSocket 双向回显与大帧、ping/pong、路由错误码。
  5. 生命周期：双节点调度分布与指标回收、创建 timeout 到期自动删除、
     kill 后 404、幂等删除。

依赖：e2b==2.46.4、e2b-code-interpreter、httpx、websockets==15.0.1。
环境变量：E2B_API_KEY、E2B_API_URL、E2B_SANDBOX_URL、E2B_TEMPLATE_ID。
模板需要 Python3 与 envd（e2b-rootfs-example 即可）。
"""

import io
import os
import re
import sys
import time
import traceback
import uuid

import httpx
from e2b import PtySize, Sandbox, SandboxQuery, Secret, Template, Volume
from e2b.exceptions import TimeoutException
from e2b.sandbox.commands.command_handle import CommandExitException
from e2b.sandbox.sandbox_api import SandboxNetworkOpts, SandboxNetworkUpdate
from websockets.sync.client import connect

from e2b_code_interpreter import Sandbox as CISandbox

# 与 integration-smoke.py 相同的访客 HTTP 应用，用于数据面验证。
APPLICATION = r'''
import base64, hashlib, json, struct, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from socketserver import TCPServer

class Server(ThreadingHTTPServer):
    def server_bind(self):
        TCPServer.server_bind(self)
        self.server_name = "localhost"
        self.server_port = self.server_address[1]

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    def do_GET(self):
        if self.path == "/ws":
            key = self.headers["Sec-WebSocket-Key"]
            accept = base64.b64encode(hashlib.sha1((key +
                "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
            self.send_response(101)
            self.send_header("Upgrade", "websocket")
            self.send_header("Connection", "Upgrade")
            self.send_header("Sec-WebSocket-Accept", accept)
            self.end_headers()
            while True:
                header = self.rfile.read(2)
                if len(header) != 2:
                    return
                opcode, length = header[0] & 15, header[1] & 127
                if length == 126:
                    length = struct.unpack("!H", self.rfile.read(2))[0]
                elif length == 127:
                    length = struct.unpack("!Q", self.rfile.read(8))[0]
                if length > 1048576:
                    return
                mask = self.rfile.read(4) if header[1] & 128 else b""
                data = self.rfile.read(length)
                if mask:
                    data = bytes(v ^ mask[i % 4] for i, v in enumerate(data))
                opcode = 10 if opcode == 9 else opcode
                if len(data) < 126:
                    size = bytes([len(data)])
                elif len(data) < 65536:
                    size = b"\x7e" + struct.pack("!H", len(data))
                else:
                    size = b"\x7f" + struct.pack("!Q", len(data))
                self.wfile.write(bytes([128 | opcode]) + size + data)
                self.wfile.flush()
                if opcode == 8:
                    return
        elif self.path == "/stream":
            self.close_connection = True
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Connection", "close")
            self.end_headers()
            for index in range(3):
                self.wfile.write(f"data: {index}\n\n".encode())
                self.wfile.flush()
                time.sleep(0.4)
        else:
            body = json.dumps({"path": self.path,
                "marker": self.headers.get("X-Conch-Marker")}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
    def log_message(self, *args):
        pass

Server(("0.0.0.0", 8080), Handler).serve_forever()
'''

PASS, FAIL = [], []
SUITE = f"conch-fullsdk-{uuid.uuid4()}"
API_URL = os.environ.get("E2B_API_URL", "").rstrip("/")


def check(name, condition, detail=""):
    if condition:
        PASS.append(name)
        print(f"  PASS {name}")
    else:
        FAIL.append((name, detail))
        print(f"  FAIL {name} :: {detail}")


def section(title):
    print(f"\n=== {title} ===")


def eventually(probe, accepts, description, timeout=30):
    deadline = time.monotonic() + timeout
    last = None
    while True:
        last = probe()
        if accepts(last):
            return last
        if time.monotonic() >= deadline:
            raise AssertionError(f"{description}: {last}")
        time.sleep(0.5)


def expect_status(name, response, status):
    check(name, response.status_code == status,
          f"HTTP {response.status_code} != {status}: {response.text[:120]}")


def sdk_error(fn):
    """运行 SDK 调用，返回 (异常类型名, 错误文本)；无异常返回 (None, None)。"""
    try:
        fn()
        return None, None
    except Exception as err:
        return type(err).__name__, str(err)


def expect_sdk_501(name, fn):
    """断言 SDK 调用报 501（或含 not supported）。"""
    etype, etext = sdk_error(fn)
    check(f"SDK {name} 501", etype is not None and ("501" in etext or "not supported" in etext),
          f"{etype}: {etext[:120] if etext else 'no exception'}")


def expect_sdk_status(name, fn, code):
    """断言 SDK 调用异常文本包含指定状态码。"""
    etype, etext = sdk_error(fn)
    check(f"SDK {name} {code}", etype is not None and str(code) in (etext or ""),
          f"{etype}: {etext[:120] if etext else 'no exception'}")


def nodes(client):
    response = client.get("/nodes")
    response.raise_for_status()
    return response.json()


def idle(ns):
    return all(n["sandboxCount"] == 0 and n["metrics"]["allocatedCPU"] == 0
               and n["metrics"]["allocatedMemoryBytes"] == 0 for n in ns)


def routing_headers(box, port):
    return {"e2b-sandbox-id": box.sandbox_id, "e2b-sandbox-port": str(port)}


# ---------------------------------------------------------------- 控制面
def test_control_plane(client, template):
    section("控制面 Gateway REST")
    expect_status("health 无需鉴权", httpx.get(f"{API_URL}/health"), 204)
    expect_status("缺 API key 401", client.get("/sandboxes", headers={"X-API-Key": ""}), 401)
    expect_status("错误 API key 401", client.get("/sandboxes", headers={"X-API-Key": "invalid"}), 401)

    ns = nodes(client)
    check("nodes 列表 >=2 节点 ready", len(ns) >= 2 and all(n["status"] == "ready" for n in ns),
          f"{[(n['id'], n['status']) for n in ns]}")

    # ---- 创建参数校验：优先 SDK 调用形态 ----
    # secure=True 是 SDK 默认值；SDK 层断言 + HTTP 层核对状态码
    expect_sdk_status("create(secure=True) 501", lambda: Sandbox.create(template, request_timeout=60), 501)
    expect_status("secure=true 501 (HTTP)", client.post("/sandboxes", json={"templateID": template, "secure": True}), 501)
    expect_sdk_status("create(未知模板) 404", lambda: Sandbox.create("does/not:exist", secure=False, request_timeout=60), 404)
    expect_sdk_status("create(负 timeout) 400", lambda: Sandbox.create(template, secure=False, timeout=-1, request_timeout=60), 400)
    expect_sdk_501("create(mcp)", lambda: Sandbox.create(template, secure=False, timeout=5, request_timeout=60, mcp={"x": {}}))
    expect_sdk_501("create(volume_mounts)", lambda: Sandbox.create(template, secure=False, timeout=5, request_timeout=60, volume_mounts={"/tmp/v": "vol"}))
    expect_sdk_501("create(lifecycle autoPause)", lambda: Sandbox.create(template, secure=False, timeout=5, request_timeout=60, lifecycle={"on_timeout": "pause"}))
    expect_sdk_501("create(network maskRequestHost)", lambda: Sandbox.create(template, secure=False, timeout=5, request_timeout=60, network=SandboxNetworkOpts(mask_request_host="example.com")))

    # SDK 无法触达的协议细节（未知字段/缺 templateID），HTTP 直测
    r = client.post("/sandboxes", json={"secure": False})
    expect_status("缺 templateID 400", r, 400)
    r = client.post("/sandboxes", json={"templateID": template, "secure": False, "bogus": 1})
    expect_status("未知字段 400", r, 400)

    # ---- 顶层模块：Template / Volume / Secret 走 SDK ----
    expect_sdk_501("Template.exists", lambda: Template.exists(template))
    expect_sdk_501("Volume.list", Volume.list)
    expect_sdk_status("Secret.create 404", lambda: Secret.create("name", {"k": "v"}), 404)
    expect_sdk_status("Secret.list 404", lambda: [s for s in Secret.list().next_items()], 404)

    # /sandboxes-cold 无 SDK 方法，HTTP 直测
    for path in ["/sandboxes-cold"]:
        expect_status(f"未实现 {path} 501", client.get(path), 501)

    box = Sandbox.create(template, secure=False, timeout=300, request_timeout=180,
                         metadata={"suite": SUITE}, envs={"CONCH_E2E_VALUE": "from-create"})
    try:
        # SDK 方法层（box.xxx()）
        expect_sdk_501("beta_pause", box.beta_pause)
        expect_sdk_501("pause", box.pause)
        expect_sdk_501("connect(resume)", box.connect)
        expect_sdk_501("fork", box.fork)
        expect_sdk_501("set_timeout", lambda: box.set_timeout(600))
        expect_sdk_501("get_metrics", box.get_metrics)
        expect_sdk_501("update_network", lambda: box.update_network(SandboxNetworkUpdate(allow_internet_access=True)))
        # refreshes 端点 SDK 无方法（仅 API client），HTTP 直测补协议层
        r = client.post(f"/sandboxes/{box.sandbox_id}/refreshes", json={})
        expect_status("POST /sandboxes/{id}/refreshes 501 (HTTP)", r, 501)

        # 详情：SDK get_info
        info = box.get_info()
        check("get_info 字段完整", all(hasattr(info, k) for k in
              ["sandbox_id", "template_id", "envd_version", "started_at",
               "end_at", "cpu_count", "memory_mb", "state", "metadata"]),
              str(sorted(vars(info).keys())))
        check("metadata 回读一致", info.metadata == {"suite": SUITE}, str(info.metadata))
        check("state=running", info.state == "running", str(info.state))

        # 未知沙箱：SDK kill() 目前因 Gateway 返回 text/plain 404（E2B 官方为 JSON）
        # 在 SDK 侧抛 JSONDecodeError —— 已知兼容性问题，如实断言"必然抛异常"
        bogus = str(uuid.uuid4())
        etype, _ = sdk_error(lambda: Sandbox.kill(bogus, request_timeout=30))
        check("SDK kill(未知沙箱) 抛异常(已知: text/plain 404)", etype is not None, etype or "no exception")
        # PUT /sandboxes 无 SDK 方法，HTTP 直测
        expect_status("PUT /sandboxes 404", client.put("/sandboxes"), 404)

        # 列表：SDK 分页器 + 查询
        paginator = Sandbox.list(query=SandboxQuery(metadata={"suite": SUITE}), limit=1)
        listed = []
        while paginator.has_next:
            listed.extend(s.sandbox_id for s in paginator.next_items())
        check("SDK 分页器 metadata 过滤", box.sandbox_id in listed, str(listed))
        # 非法 order：SDK 分页器迭代时本地校验（InvalidArgumentException），不发请求；服务端语义由 HTTP 直测覆盖
        etype, _ = sdk_error(lambda: [s for s in Sandbox.list(order="invalid").next_items()])
        check("SDK list(非法 order) 本地拦截", etype == "InvalidArgumentException", etype or "no exception")
        # limit 边界 / state 过滤 / 非法 order 的裸 HTTP 语义，SDK 不暴露 limit=0/101 这种调用面，HTTP 直测
        expect_status("limit=0 400", client.get("/sandboxes", params={"limit": 0}), 400)
        expect_status("limit=101 400", client.get("/sandboxes", params={"limit": 101}), 400)
        expect_status("state=paused 过滤 200", client.get("/sandboxes", params={"state": "paused"}), 200)
        check("paused 列表为空", client.get("/sandboxes", params={"state": "paused"}).json() == [], "")

        digest = info.template_id
        if digest.startswith("sha256:"):
            try:
                b2 = Sandbox.create(digest, secure=False, timeout=60, request_timeout=180)
                b2.kill()
                check("digest 模板创建", True)
            except Exception as err:
                check("digest 模板创建", False, str(err)[:120])
    finally:
        box.kill()
    expect_status("GET 已删除沙箱 404", client.get(f"/sandboxes/{box.sandbox_id}"), 404)
    expect_status("DELETE 已删除沙箱 404", client.delete(f"/sandboxes/{box.sandbox_id}"), 404)


# ---------------------------------------------------------------- envd
def test_envd(box):
    section("envd commands")
    result = box.commands.run('printf "%s" "$CONCH_E2E_VALUE"; printf err >&2')
    check("stdout/stderr/exit_code", result.stdout == "from-create"
          and result.stderr == "err" and result.exit_code == 0,
          f"{result.stdout!r} {result.stderr!r} {result.exit_code}")

    r = box.commands.run("pwd; printf %s \"$MYVAR\"", cwd="/etc", envs={"MYVAR": "v1"})
    check("cwd/envs", r.stdout == "/etc\nv1", repr(r.stdout))

    r = box.commands.run("id -u", user="root")
    check("user=root", r.exit_code == 0 and r.stdout.strip() == "0", r.stdout)

    try:
        box.commands.run("exit 7")
        check("非零退出码抛异常", False, "no exception")
    except CommandExitException:
        check("非零退出码抛异常", True)

    try:
        box.commands.run("sleep 10", timeout=1)
        check("命令超时异常", False, "no exception")
    except TimeoutException:
        check("命令超时异常", True)

    outs, errs = [], []
    r = box.commands.run("echo out1; echo err1 >&2", on_stdout=outs.append, on_stderr=errs.append)
    check("流式回调", outs == ["out1\n"] and errs == ["err1\n"], f"{outs} {errs}")

    r = box.commands.run("sleep 1; echo late", timeout=0)
    check("timeout=0 不限时", r.exit_code == 0 and r.stdout.strip() == "late", r.stdout)

    handle = box.commands.run("sleep 300", background=True, timeout=0)
    check("后台命令返回 pid", isinstance(handle.pid, int) and handle.pid > 0, str(handle.pid))
    check("后台命令 kill", handle.kill() is True, "")

    handle = box.commands.run("cat > /tmp/stdin.txt", stdin=True, background=True, timeout=30)
    handle.send_stdin(b"interactive-payload\n")
    handle.close_stdin()
    r = handle.wait()
    check("stdin 交互命令", r.exit_code == 0
          and box.files.read("/tmp/stdin.txt") == "interactive-payload\n", str(r))

    section("envd files")
    base = "/tmp/conch-fullsdk"
    if box.files.exists(base):
        box.files.remove(base)
    check("make_dir", box.files.make_dir(base) is True)
    info = box.files.write(f"{base}/bin.dat", b"\x00\x01\x02\xff")
    check("二进制写入", info.name == "bin.dat")
    check("二进制读取", bytes(box.files.read(f"{base}/bin.dat", format="bytes")) == b"\x00\x01\x02\xff")

    box.files.write(f"{base}/text.txt", "Conch 文件传输\n" * 512)
    check("大文本 roundtrip", box.files.read(f"{base}/text.txt") == "Conch 文件传输\n" * 512)

    filename = f"{base}/e2e file %23.txt"
    box.files.write(filename, "escaped")
    names = [e.name for e in box.files.list(base, depth=2)]
    check("特殊字符文件名列出", "e2e file %23.txt" in names, str(names))

    check("exists 正/误", box.files.exists(filename) and not box.files.exists(f"{base}/nope"))

    box.files.make_dir(f"{base}/sub/deep")
    check("嵌套 make_dir", box.files.exists(f"{base}/sub/deep"))
    box.files.remove(f"{base}/sub")
    check("remove 目录", not box.files.exists(f"{base}/sub/deep"))

    buf = io.BytesIO(b"io-stream-data")
    box.files.write(f"{base}/io.txt", buf)
    check("IO 对象写入", bytes(box.files.read(f"{base}/io.txt", format="bytes")) == b"io-stream-data")

    with box.files.read(f"{base}/io.txt", format="stream") as stream:
        chunks = list(stream)
    check("流式读取", b"".join(chunks) == b"io-stream-data", str(chunks[:2]))

    box.files.remove(base)
    check("remove 根目录", not box.files.exists(base))

    watch = box.files.watch_dir("/tmp")
    time.sleep(0.5)
    box.files.write("/tmp/watched-file.txt", "data")
    seen = eventually(watch.get_new_events, lambda evs: len(evs) > 0,
                      "watch_dir 事件到达", timeout=8)
    check("watch_dir CREATE 事件", any(
        getattr(e, "name", None) == "watched-file.txt" for e in seen), str(seen[:3]))
    watch.stop()

    section("envd /files 上传下载 URL")
    box.files.write("/tmp/dl.txt", "download-me")
    down = box.download_url("/tmp/dl.txt")
    r = httpx.get(down, headers=routing_headers(box, 49983))
    check("download_url 下载", r.status_code == 200 and r.text == "download-me",
          f"{r.status_code} {r.text[:40]}")
    up = box.upload_url("/tmp/ul.txt")
    r = httpx.post(up, headers={**routing_headers(box, 49983),
                                "Content-Type": "application/octet-stream"},
                   content=b"uploaded-bytes")
    check("upload_url 上传", r.status_code == 200
          and box.files.read("/tmp/ul.txt") == "uploaded-bytes", f"{r.status_code} {r.text[:60]}")

    section("envd pty")
    pty = box.pty.create(size=PtySize(rows=24, cols=80), timeout=30)
    box.pty.resize(pty.pid, size=PtySize(rows=35, cols=100))
    box.pty.send_stdin(pty.pid, b"printf 'PTY_OK\\n'; stty size; exit\n")
    output = []
    result = pty.wait(on_pty=output.append)
    data = b"".join(output)
    check("pty resize+回显", result.exit_code == 0 and b"PTY_OK" in data and b"35 100" in data,
          data[-120:])

    check("get_info().sandbox_id 一致", box.get_info().sandbox_id == box.sandbox_id)
    check("is_running", box.is_running() is True)

# ---------------------------------------------------------------- code-interpreter
def test_code_interpreter(box):
    section("code-interpreter run_code")
    e = box.run_code("x = 1")
    check("首条执行无结果", e.error is None and e.results == [], str(e.error))
    e = box.run_code("x += 1; x")
    check("会话状态保持", bool(e.results) and e.results[0].text == "2",
          str([r.text for r in e.results]))
    check("execution_count 递增", e.execution_count >= 2, str(e.execution_count))

    e = box.run_code("import sys; print('o1'); print('e1', file=sys.stderr)")
    check("stdout 日志", e.logs.stdout == ["o1\n"], str(e.logs))
    check("stderr 日志", e.logs.stderr == ["e1\n"], str(e.logs))

    e = box.run_code("1/0")
    check("执行错误回传", e.error is not None and e.error.name == "ZeroDivisionError"
          and e.error.value == "division by zero",
          f"{e.error.name if e.error else None} {e.error.value if e.error else None}")

    e = box.run_code("{'k': [1, 2, 3]}")
    check("json 结果", e.results[0].json == {"k": [1, 2, 3]}, str(e.results[0].json))

    e = box.run_code("from IPython.display import HTML; HTML('<b>hi</b>')")
    check("html 富文本", bool(e.results) and e.results[0].html == "<b>hi</b>",
          str(e.results[0].html if e.results else None))
    e = box.run_code("from IPython.display import SVG; "
                     "SVG('<svg xmlns=\"http://www.w3.org/2000/svg\"/>')")
    check("svg 富文本", bool(e.results) and bool(e.results[0].svg),
          str(e.results[0].svg if e.results else None)[:80])

    e = box.run_code("from IPython.display import display; display('one'); display('two')")
    check("多结果", [r.text for r in e.results] == ["one", "two"],
          str([r.text for r in e.results]))

    e = box.run_code("import os; os.environ['CI_VAR']", envs={"CI_VAR": "ci-env-ok"})
    check("run_code envs", bool(e.results) and e.results[0].text == "ci-env-ok",
          str([r.text for r in e.results]))

    outs = []
    e = box.run_code("print('cb')", on_stdout=outs.append)
    check("on_stdout 回调", bool(outs) and outs[0].line == "cb\n", str(outs))

    e = box.run_code("1 + 2", language="javascript")
    check("javascript 语言", bool(e.results) and e.results[0].text == "3"
          and e.error is None, str([r.text for r in e.results]) + str(e.error))

    try:
        box.run_code("import time; time.sleep(30)", timeout=2)
        check("run_code 执行超时", False, "no exception")
    except TimeoutException:
        check("run_code 执行超时", True)

    section("code-interpreter context")
    ctx = box.create_code_context()
    check("创建 context", isinstance(ctx.id, str) and ctx.id != "", ctx.id)
    box.run_code("ctx_var = 'in-ctx'", context=ctx)
    e = box.run_code("ctx_var", context=ctx)
    check("context 内状态", bool(e.results) and e.results[0].text == "in-ctx",
          str([r.text for r in e.results]))
    e = box.run_code("'ctx_var' in dir()")
    check("context 相互隔离", bool(e.results) and e.results[0].text == "False",
          str([r.text for r in e.results]))

    box.restart_code_context(ctx)
    e = box.run_code("'ctx_var' in dir()", context=ctx)
    check("restart 清空 context", bool(e.results) and e.results[0].text == "False",
          str([r.text for r in e.results]))

    ctxs = box.list_code_contexts()
    check("list_code_contexts", ctx.id in [c.id for c in ctxs], str([c.id for c in ctxs]))
    box.remove_code_context(ctx)
    check("remove_code_context", ctx.id not in [c.id for c in box.list_code_contexts()])



# ---------------------------------------------------------------- checkpoint
def test_checkpoint(client, template):
    section("checkpoint 快照")
    box = Sandbox.create(template, secure=False, timeout=600, request_timeout=180,
                         metadata={"suite": SUITE, "phase": "checkpoint"})
    name = f"{SUITE[:24]}-ckpt:v1"
    try:
        # 指定名称捕获
        box.files.write("/home/user/checkpoint.txt", "1")
        snap = box.create_snapshot(name=name, request_timeout=180)
        check("指定名称捕获", snap.snapshot_id == name and snap.names == [name],
              f"{snap.snapshot_id} {snap.names}")

        # 同名再次捕获（更新同一模板）
        box.files.write("/home/user/checkpoint.txt", "2")
        snap2 = box.create_snapshot(name=name, request_timeout=180)
        check("同名更新", snap2.snapshot_id == name and snap2.names == [name],
              f"{snap2.snapshot_id} {snap2.names}")

        # 捕获后源沙箱继续可用
        alive = box.commands.run("printf alive").stdout == "alive"
        check("捕获后源沙箱可用", alive and box.files.read("/home/user/checkpoint.txt") == "2", "")

        # 自动命名
        anon = box.create_snapshot(request_timeout=180)
        check("自动命名捕获", anon.snapshot_id.startswith("checkpoint-") and anon.names == [],
              f"{anon.snapshot_id} {anon.names}")

        # 错误路径
        r = client.post(f"/sandboxes/{box.sandbox_id}/snapshots", json={"name": "  "})
        expect_status("空名称 400", r, 400)
        r = client.post(f"/sandboxes/{box.sandbox_id}/snapshots",
                        json={"name": "sha256:6aaeaa67ce540956e8b09051ef5add2932c73a1f05ca4b4ff2a2d28202d20b96"})
        expect_status("digest 名称 400", r, 400)
        r = client.post(f"/sandboxes/{box.sandbox_id}/snapshots", json={"bogus": 1})
        expect_status("未知字段 400", r, 400)
        r = client.post(f"/sandboxes/{uuid.uuid4()}/snapshots", json={})
        expect_status("未知沙箱 404", r, 404)

        # list/delete 快照仍不支持
        for label, fn in [("list_snapshots", lambda: [_ for _ in _iter_snapshots(box)]),
                          ("delete_snapshot", lambda: box.delete_snapshot("nonexistent"))]:
            try:
                fn()
                check(f"SDK {label} 应不支持", False, "no exception")
            except Exception as err:
                check(f"SDK {label} 应不支持",
                      "404" in str(err) or "501" in str(err) or "not supported" in str(err),
                      str(err)[:100])
    finally:
        try:
            box.kill()
        except Exception:
            pass

    # 恢复验证：源沙箱删除后，从快照模板在归属 Node 重建并校验内容
    section("checkpoint 恢复（Node 本地模板）")
    # 找到模板所在节点：轮流在两个节点端口直接探测
    found = None
    for port in (8001, 8002):
        try:
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=180,
                              headers={"X-API-Key": os.environ["E2B_API_KEY"]}) as c:
                r = c.post("/sandboxes", json={"templateID": name, "secure": False, "timeout": 120})
                if r.status_code == 201:
                    found = (port, r.json()["sandboxID"])
                    break
        except Exception:
            continue
    if found is None:
        check("从快照模板恢复", False, "两个节点均无法用快照模板创建")
        return
    port, rid = found
    try:
        headers = {"e2b-sandbox-id": rid, "e2b-sandbox-port": "49983"}
        r = httpx.get(f"http://127.0.0.1:{port}/files",
                      params={"path": "/home/user/checkpoint.txt"}, headers=headers, timeout=30)
        check("恢复内容一致", r.status_code == 200 and r.text == "2",
              f"{r.status_code} {r.text[:40]}")
        r = httpx.get(f"http://127.0.0.1:{port}/health", headers=headers, timeout=30)
        check("恢复后 envd 健康", r.status_code == 204, str(r.status_code))
    finally:
        httpx.delete(f"http://127.0.0.1:{port}/sandboxes/{rid}",
                     headers={"X-API-Key": os.environ["E2B_API_KEY"]})


def _iter_snapshots(box):
    paginator = box.list_snapshots()
    while paginator.has_next:
        paginator.next_items()


# ---------------------------------------------------------------- 数据面
def test_dataplane(box, client):
    section("数据面 sandboxproxy")
    box.files.write("/tmp/conch-e2e-app.py", APPLICATION)
    box.commands.run("python3 /tmp/conch-e2e-app.py >/tmp/conch-e2e-app.log 2>&1",
                     background=True, timeout=0)
    routing = routing_headers(box, 8080)
    eventually(lambda: client.get("/app-ready", headers=routing),
               lambda r: r.status_code == 200, "访客 HTTP 应用就绪")

    r = client.get("/encoded%2Fpath?q=a%2Fb", headers={**routing, "X-Conch-Marker": "m1"})
    check("E2b 头路由 + 转义路径", r.status_code == 200 and r.json() == {
        "path": "/encoded%2Fpath?q=a%2Fb", "marker": "m1"}, r.text[:80])

    r = client.get("/app-ready", headers={
        "X-Agentenv-Sandbox-Id": box.sandbox_id, "X-Agentenv-Target-Port": "8080"})
    check("X-Agentenv 头路由", r.status_code == 200, str(r.status_code))

    r = client.get("/proxy/8080/app-ready", headers={
        "X-Agentenv-Sandbox-Id": box.sandbox_id, "X-Agentenv-Target-Port": "8080"})
    check("/proxy 前缀路由", r.status_code == 200, f"{r.status_code} {r.text[:80]}")

    r = client.get("/health", headers=routing_headers(box, 49983))
    check("envd /health 经代理", r.status_code == 204, str(r.status_code))

    r = client.get("/app-ready", headers={"e2b-sandbox-port": "8080"})
    check("缺 sandbox id 400/401/404", r.status_code in (400, 401, 404), str(r.status_code))
    r = client.get("/app-ready", headers={"e2b-sandbox-id": box.sandbox_id,
                                          "e2b-sandbox-port": "0"})
    check("非法端口 400", r.status_code == 400, str(r.status_code))
    r = client.get("/app-ready", headers={"e2b-sandbox-id": str(uuid.uuid4()),
                                          "e2b-sandbox-port": "8080"})
    check("未知沙箱 404", r.status_code == 404, str(r.status_code))

    started = time.monotonic()
    first = None
    lines = []
    with client.stream("GET", "/stream", headers=routing) as response:
        for line in response.iter_lines():
            if line:
                if first is None:
                    first = time.monotonic() - started
                lines.append(line)
    check("SSE 全部行", lines == ["data: 0", "data: 1", "data: 2"], str(lines))
    check("SSE 首行及时下发", first is not None and first < 0.8, str(first))

    ws_url = API_URL.replace("https://", "wss://", 1).replace("http://", "ws://", 1) + "/ws"
    with connect(ws_url, additional_headers=routing, proxy=None) as websocket:
        for message in ["Conch WebSocket 文本", b"\x00\x01\xff"]:
            websocket.send(message)
            check(f"WS 回显 {'文本' if isinstance(message, str) else '二进制'}",
                  websocket.recv() == message)
        big = b"x" * 65536
        websocket.send(big)
        check("WS 64KB 大帧回显", websocket.recv() == big)
        pong = websocket.ping(b"pingdata")
        check("WS ping/pong", pong.wait() is True)

    host = box.get_host(8080)
    check("get_host 域名格式", re.match(r"^\d+-[0-9a-f-]{36}\.\S+$", host) is not None, host)

    # Host 域名路由：仅当 create 响应携带 domain（节点已配置 sandbox_proxy_domains）时执行
    if box.sandbox_domain and box.sandbox_domain != "e2b.app":
        section("数据面 Host 域名路由")
        r = client.get("/hello", headers={"Host": host})
        check("Host 路由 GET", r.status_code == 200 and r.json()["path"] == "/hello",
              f"{r.status_code} {r.text[:60]}")
        host_lines = []
        with client.stream("GET", "/stream", headers={"Host": host}) as response:
            for line in response.iter_lines():
                if line:
                    host_lines.append(line)
        check("Host 路由 SSE", host_lines == ["data: 0", "data: 1", "data: 2"], str(host_lines))
        r = client.get("/hello", headers={"Host": host, "e2b-sandbox-id": str(uuid.uuid4()),
                                          "e2b-sandbox-port": "1"})
        check("Host 优先于冲突路由头", r.status_code == 200 and r.json()["path"] == "/hello",
              f"{r.status_code} {r.text[:60]}")
        import base64, hashlib, socket as _socket
        ws_key = base64.b64encode(os.urandom(16)).decode()
        req = (f"GET /ws HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n"
               f"Connection: Upgrade\r\nSec-WebSocket-Key: {ws_key}\r\n"
               f"Sec-WebSocket-Version: 13\r\n\r\n")
        sock = _socket.create_connection((API_URL.rsplit("//", 1)[-1].split("/")[0].split(":")[0]
                                          or "127.0.0.1",
                                          int(API_URL.rsplit(":", 1)[-1]) if API_URL.count(":") == 2 else 80),
                                         timeout=10)
        sock.sendall(req.encode())
        resp = b""
        while b"\r\n\r\n" not in resp:
            resp += sock.recv(4096)
        sock.close()
        check("Host 路由 WS 握手 101", resp.startswith(b"HTTP/1.1 101"), resp[:40].decode(errors="replace"))
    else:
        print("\n=== 数据面 Host 域名路由：跳过（节点未配置 sandbox_proxy_domains）===")


# ---------------------------------------------------------------- 生命周期
def test_lifecycle(client, template):
    section("生命周期与调度")
    boxes = []
    try:
        for _ in range(2):
            boxes.append(Sandbox.create(template, secure=False, timeout=300,
                                        request_timeout=180, metadata={"suite": SUITE}))
        observed = eventually(
            lambda: nodes(client),
            lambda ns: sum(n["sandboxCount"] for n in ns) == 2
            and sum(n["sandboxCount"] > 0 for n in ns) == 2,
            "双节点分布调度")
        busy = [n for n in observed if n["sandboxCount"] > 0]
        check("两节点各承载一个沙箱", len(busy) == 2,
              str([(n["id"], n["sandboxCount"]) for n in observed]))
        check("节点 CPU/内存分配计数", all(n["metrics"]["allocatedCPU"] > 0
              and n["metrics"]["allocatedMemoryBytes"] > 0 for n in busy),
              str([(n["id"], n["metrics"]) for n in busy]))
    finally:
        for box in boxes:
            try:
                box.kill()
            except Exception as err:
                print(f"  警告: kill {box.sandbox_id} 异常: {err}")
    try:
        eventually(lambda: nodes(client), idle, "kill 后指标回收", timeout=120)
    except AssertionError:
        leftover = client.get("/sandboxes", params={"limit": 100}).json()
        print("  诊断: 残留沙箱:", [(s["sandboxID"], s.get("metadata")) for s in leftover])
        raise

    section("timeout 到期自动删除")
    expiring = Sandbox.create(template, secure=False, timeout=3, request_timeout=180)
    status = eventually(lambda: client.get(f"/sandboxes/{expiring.sandbox_id}").status_code,
                        lambda code: code == 404, "初始 timeout 删除沙箱", timeout=20)
    check("到期 404", status == 404, str(status))
    try:
        killed = expiring.kill()
        check("到期后 kill 返回 False", killed is False, str(killed))
    except Exception as err:
        check("到期后 kill 返回 False", False, f"{type(err).__name__}: {err}")
    eventually(lambda: nodes(client), idle, "到期回收后指标归零", timeout=90)


def main():
    for variable in ["E2B_API_KEY", "E2B_API_URL", "E2B_SANDBOX_URL", "E2B_TEMPLATE_ID"]:
        if not os.getenv(variable):
            raise SystemExit(f"运行前需设置 {variable}")

    template = os.environ["E2B_TEMPLATE_ID"]
    box = None
    try:
        with httpx.Client(base_url=API_URL, timeout=180,
                          headers={"X-API-Key": os.environ["E2B_API_KEY"]}) as client:
            initial = nodes(client)
            if len(initial) < 2:
                print(f"警告: 仅 {len(initial)} 个节点，双节点分布检查可能失败")
            if not idle(initial):
                print("警告: 集群不为空，指标回收检查可能失败")

            test_control_plane(client, template)

            box = Sandbox.create(template, secure=False, timeout=900, request_timeout=180,
                                 metadata={"suite": SUITE, "phase": "envd"},
                                 envs={"CONCH_E2E_VALUE": "from-create"})
            test_envd(box)
            test_dataplane(box, client)
            box.kill()
            box = CISandbox.create(template, secure=False, timeout=900,
                                   request_timeout=180, metadata={"suite": SUITE})
            test_code_interpreter(box)
            box.kill()
            box = None
            test_checkpoint(client, template)
            test_lifecycle(client, template)
    finally:
        if box is not None:
            try:
                box.kill()
            except Exception:
                pass

    print(f"\n{'=' * 50}")
    print(f"通过: {len(PASS)}  失败: {len(FAIL)}")
    for name, detail in FAIL:
        print(f"  FAIL {name} :: {detail}")
    if FAIL:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except AssertionError:
        traceback.print_exc()
        sys.exit(2)
