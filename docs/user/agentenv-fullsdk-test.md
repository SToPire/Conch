# AgentENV E2B 全量接口测试说明

`examples/agentenv/e2b-fullsdk-test.py` 对 Conch AgentENV 集成（E2B TCP facade、非安全 envd 引导、数据代理、调度心跳）暴露的 E2B SDK 接口做全量验证。

测试原则：**凡 E2B SDK 有对应调用形态的，一律用 SDK 方法验证（无缝衔接目标）；SDK 无法触达的（无方法包装的端点、鉴权头、请求体校验、路由头等协议细节）才用 httpx 直发 HTTP 补测**。

对应功能提交：`8936ad6`（gateway/scheduler 集成）、`e66d1ca`（单节点启动指南）、`1ea77bb`（多容器部署文档）、`8b51417`（checkpoint 暴露为快照创建）。

## 1. 前置条件

- 双节点部署（参见 `docs/user/agentenv-multi-container.md`），Gateway 监听 `:8080`，两节点 Ready。
- 测试机 Python 虚拟环境安装：`e2b==2.46.4`、`e2b-code-interpreter`、`httpx`、`websockets==15.0.1`。
- 环境变量：

```bash
export E2B_API_KEY="$AENV_API_KEY"
export E2B_API_URL=http://127.0.0.1:8080
export E2B_SANDBOX_URL="$E2B_API_URL"
export E2B_TEMPLATE_ID=hub.oepkgs.net/conch/e2b-rootfs-example:conch
```

- 模板（e2b-rootfs-example）需要 Python 3 与 envd；无需额外 guest Python 包。
- 集群建议空闲：脚本检查全局节点指标回收，存在他人沙箱时相应检查会超时。

运行：

```bash
python e2b-fullsdk-test.py   # 全部通过 exit 0，任何 FAIL exit 1，断言超时 exit 2
```

脚本按阶段自建自删沙箱，结束时应将集群恢复为空闲。用 `-X utf8` 或 UTF-8 终端运行以正确显示中文检查名。

## 2. 测试维度

### 2.1 控制面（SDK 优先，HTTP 补协议细节）

| 维度 | 检查点 | 方式 |
| --- | --- | --- |
| 健康与鉴权 | `/health` 免鉴权 204；缺失/错误 `X-API-Key` 401 | HTTP（鉴权头无 SDK 形态） |
| 节点名单 | `GET /nodes` ≥2 节点 ready（调度心跳） | HTTP |
| 创建-SDK 层 | `create(secure=True)` 501（SDK 默认值）；`create(未知模板)` 404；`create(负 timeout)` 400；`create(mcp=...)`/`create(volume_mounts=...)`/`create(lifecycle={"on_timeout":"pause"})`/`create(network={"maskRequestHost":...})` 501 | SDK |
| 创建-协议层 | 缺 `templateID` 400；未知字段 400 | HTTP（SDK 客户端不会发出这两种请求体） |
| 顶层模块 | `Template.exists` 501；`Volume.list` 501；`Secret.create`/`Secret.list` 404 | SDK |
| 沙箱操作 | `beta_pause`/`pause`/`connect`（即 resume）/`fork`/`set_timeout`/`get_metrics`/`update_network` 均 501 | SDK |
| `refreshes` 端点 | 501 | HTTP（SDK 2.46.4 无高层方法，仅生成 client 存在） |
| 详情 | `get_info` 字段完整（sandbox_id/template_id/envd_version/started_at/end_at/cpu_count/memory_mb/state/metadata）、metadata 回读一致、state=running | SDK |
| 未知沙箱 | `kill(未知ID)` 抛异常（见"已知兼容性问题"）；`GET`/`DELETE` 404 | SDK + HTTP |
| 列表 | `Sandbox.list` 分页器（limit=1 翻页）+ `SandboxQuery(metadata=...)` 过滤；`list(order="invalid")` SDK 本地拦截 `InvalidArgumentException`；limit 边界（0/101）与 `state=paused` 过滤 | SDK + HTTP |
| 删除 | `kill()` 成功；删除后 `GET`/`DELETE` 404（幂等） | SDK + HTTP |
| 快照捕获 | `create_snapshot(name=...)` 返回该名称且 `names=[name]`；同名再次捕获覆盖同一模板；捕获后源沙箱继续可用；不带名称捕获返回 `checkpoint-<uuid>`、`names=[]` | SDK |
| 快照恢复 | 源沙箱删除后，用快照名作为模板 ID 在归属节点直接创建，文件内容与捕获时一致，envd `/health` 204 | SDK（节点直连） |
| 快照错误路径 | 空白名称 400；digest 形式名称（`sha256:...`）400；未知字段 400；未知沙箱 404 | HTTP |
| 快照 SDK 未实现 | `list_snapshots` 404；`delete_snapshot` 501 | SDK |
| digest 模板 | `create(sha256:...)` 创建成功 | SDK |

### 2.2 envd commands（进程 API，SDK）

| 维度 | 检查点 |
| --- | --- |
| 基本执行 | stdout/stderr/exit_code 分离正确 |
| 执行上下文 | `cwd`、`envs`、`user="root"` |
| 异常语义 | 非零退出码抛 `CommandExitException`；`timeout` 到期抛 `TimeoutException`；`timeout=0` 不限时 |
| 流式 | `on_stdout`/`on_stderr` 回调逐块到达 |
| 后台与交互 | `background=True` 返回 pid 且可 kill；`stdin=True` + `send_stdin`/`close_stdin` 交互写入 |

### 2.3 envd files（文件 API，SDK）

| 维度 | 检查点 |
| --- | --- |
| 写入 | 文本（含中文大文件）、二进制、`io.BytesIO` 对象 |
| 读取 | text/bytes/stream 三种格式 roundtrip |
| 目录 | `make_dir` 嵌套创建、`list`（含 `%`/`#`/空格特殊文件名）、`exists`、`remove` |
| 监听 | `watch_dir` 收到 CREATE 事件 |
| URL 直传 | `download_url` GET 下载、`upload_url` POST `application/octet-stream` 上传后回读一致 |

注：模板 envd 为 0.6.1，`files.write` 的 `metadata` 参数与 `watch_dir(include_entry=True)` 需 0.6.2+，脚本未覆盖这两项。

### 2.4 envd pty（SDK）

`pty.create`（24×80）→ `resize`（35×100）→ `send_stdin` → `wait(on_pty)`：退出码 0、输出含 `PTY_OK` 与 `stty size` 的 `35 100`。

### 2.5 code-interpreter（Jupyter 49999，SDK）

| 维度 | 检查点 |
| --- | --- |
| 执行 | `run_code` 状态保持（跨调用变量）、`execution_count` 递增 |
| 日志 | stdout/stderr 分别回传 |
| 结果格式 | text、json、HTML、SVG 富文本；`display()` 多结果 |
| 错误 | 执行异常回传（name/value，如 ZeroDivisionError）；执行超时抛 `TimeoutException` |
| 参数 | `envs` 注入、`on_stdout` 回调、`language="javascript"` |
| Context | 创建/独立命名空间/`restart` 清空/列举/删除 |

### 2.6 数据面（sandboxproxy）

数据面测试的背景与范围说明见附录 A。

| 维度 | 检查点 | 方式 |
| --- | --- | --- |
| SDK 隐式通路 | SDK 的 commands/files/pty/run_code 全部经数据面代理，2.2–2.5 的通过即为通路验证 | SDK |
| envd 健康 | envd 49983 `/health` 经代理 204 | HTTP |
| header 路由 | `E2b-Sandbox-Id`/`E2b-Sandbox-Port` 与 `X-Agentenv-Sandbox-Id`/`X-Agentenv-Target-Port` 两套头；百分号编码路径原样转发 | HTTP（SDK 内部拼头，无法显式指定） |
| 前缀路由 | `/proxy/{port}/...` 显式代理路径 | HTTP |
| SSE | 事件逐块下发（首行 <0.8s，不整段缓冲） | HTTP |
| WebSocket | 文本/二进制回显、64KB 大帧、ping/pong | HTTP |
| Host 域名路由 | Host 头按 `{port}-{sandboxID}.{domain}` 路由（GET/SSE/WS），优先于路由头；`get_host()` 域名格式；检测到 `sandbox_domain != e2b.app` 时自动启用 4 项检查 | SDK + HTTP |
| 错误码 | 非法端口 400、未知沙箱 404、缺 sandbox id 400/401/404 | HTTP |

### 2.7 生命周期与调度（SDK + roster 观测）

| 维度 | 检查点 |
| --- | --- |
| 双节点调度 | 2 个创建 round-robin 分布到 2 个节点（roster 心跳观测） |
| 资源计量 | 节点 `allocatedCPU`/`allocatedMemoryBytes` > 0 |
| 回收 | kill 后节点指标归零（心跳窗口内） |
| 到期删除 | create `timeout=3` 创建后自动 404；到期后 `kill()` 返回 False |

## 3. 未实现的 E2B SDK 能力（当前全部 501/404，SDK 实测）

以下能力在 Conch 当前阶段未实现，均通过 SDK 方法调用实测（异常文本含状态码）：

| SDK 调用 | 返回 | 说明 |
| --- | --- | --- |
| `Sandbox.create(secure=True)`（SDK 默认） | 501 | 非安全 envd 阶段，需显式 `secure=False` |
| `Sandbox.create(mcp=...)` | 501 | MCP 未实现 |
| `Sandbox.create(volume_mounts=...)` | 501 | 挂卷未实现 |
| `Sandbox.create(lifecycle={"on_timeout": "pause"})` | 501 | 自动 pause/resume 未实现 |
| `Sandbox.create(network={"maskRequestHost": ...})` | 501 | 私有流量/Host 伪装未实现 |
| `box.pause()` / `box.beta_pause()` | 501 | 暂停未实现 |
| `box.connect()`（resume 语义） | 501 | 恢复未实现 |
| `box.fork()` | 501 | 分叉未实现 |
| `box.set_timeout(...)` | 501 | 运行时改存活时长未实现（create 的 `timeout` 参数本身已支持） |
| `box.get_metrics()` | 501 | 指标未实现 |
| `box.update_network(...)` | 501 | 网络更新未实现 |
| `box.delete_snapshot(...)` | 501 | 删除快照未实现 |
| `box.list_snapshots()` | 404 | 列出快照端点不存在 |
| `Template.exists` / `Template.build` 系 | 501 | 模板管理未实现 |
| `Volume.list` / `Volume.create` 系 | 501 | 卷管理未实现 |
| `Secret.create` / `Secret.list` | 404 | Secret 端点不存在 |

REST 端点层补充（SDK 无方法包装，HTTP 实测）：`POST /sandboxes/{id}/refreshes` 501；`GET /templates`、`GET /volumes`、`GET /sandboxes-cold` 501；create 请求携带 `mcp`/`customExtensionParams` 501。

## 4. 已知兼容性问题

- **Gateway 404 响应为 text/plain，SDK 侧 crash**：对不存在的沙箱 `kill()` 时，Gateway（`writeSchedulerError`，`http.Error` 输出 text/plain `sandbox assignment not found`）与 E2B 官方（JSON `{"code","message"}`）格式不一致，SDK 的 OpenAPI client 解析 JSON 失败抛 `JSONDecodeError` 而非干净的 `SandboxNotFoundException`。E2B 官方云上 `kill()` 对已删除沙箱返回 `False`。修复方向：Gateway 错误响应改 JSON。
- 快照模板为节点本地（不回写镜像仓库、不同步其他节点），恢复必须发到捕获时的归属节点；经 Gateway 创建不保证成功（scheduler round-robin 选节点，落到归属节点才 201，否则 `template not found` 404），脚本通过直连节点端口（8001/8002）探测恢复。
- 快照捕获时长受节点 `sandbox.request_timeout` 约束；挂 virtiofs 卷的沙箱不支持捕获（挂卷本身也未实现）；容器化节点需预创建 loop 设备（否则恢复报 `could not open loop device`）；残留模板用 `conch template rm` 清理。

## 5. 结果判读

- `PASS/FAIL` 计数与逐项明细在结尾输出；exit 0 = 全部通过。
- `eventually` 超时（exit 2）多为环境问题：节点非 ready、集群有残留沙箱、心跳 TTL（30s）内指标未归零。
- 出现残留沙箱时可通过 `GET /sandboxes` 排查，或等待 `timeout` 到期自动回收。
- 2026-09-10 基于 conchd `8b51417` 双节点环境全量回归：122 项全部通过，结束后集群恢复空闲。

## 附录 A：数据面（sandboxproxy）测试说明

**一句话**：2.6 测的是"外部流量如何穿过 Gateway 到达沙箱内部任意端口"这条通路。它不是测某一个 API，而是测 Conch 作为 E2B 兼容层最底层的流量转发基础设施。

### A.1 为什么需要数据面

沙箱是 MicroVM，里面的端口（envd 49983、Jupyter 49999、用户自己起的 8080）外部网络根本路由不到——沙箱在独立的网络命名空间里。E2B 的解法是所有流量先到 Gateway/节点的数据面代理（sandboxproxy），由代理根据"沙箱 ID + 端口"转发进去。SDK 的每个 envd 调用（`box.commands.run`、`box.files.read`…）背后都走这条通路，只是 SDK 把路由细节隐藏了。2.6 就是把隐藏的细节掰开直接测。

### A.2 测试装置

脚本先在沙箱里起一个自带的应用（`test_dataplane` 中的 `/tmp/conch-e2e-app.py`，监听 8080，提供 `/hello`、`/stream` SSE、`/ws` WebSocket 回显等端点），作为"沙箱内用户应用"的替身。

### A.3 三种路由方式——请求怎么告诉代理"去找哪个沙箱的哪个端口"

| 路由方式 | 形式 | 对应 E2B 生态 |
| --- | --- | --- |
| E2B 头 | `E2b-Sandbox-Id` + `E2b-Sandbox-Port` | E2B 官方 SDK 用的方式 |
| AgentENV 头 | `X-Agentenv-Sandbox-Id` + `X-Agentenv-Target-Port` | AgentENV 网关的别名头 |
| 路径前缀 | `/proxy/8080/...` | 显式代理路径 |
| Host 域名（可选段） | `Host: 8080-{sandboxID}.{domain}` | 浏览器直接访问沙箱应用的方式（无需任何 SDK） |

### A.4 代理的协议保真——转发会不会破坏原始请求/响应

- **路径保真**：`/encoded%2Fpath?q=a%2Fb` 原样到达（`%2F` 不被错误解码成 `/`，query 不丢，自定义头不丢）；
- **SSE 流式**：`/stream` 逐块下发且首行 <0.8s——验证代理不缓冲整段响应（缓冲会杀死 SSE/流式 AI 场景）；
- **WebSocket**：文本/二进制回显、64KB 大帧、ping/pong——验证 `Upgrade` 握手（101）和双向透传不被代理截断；
- **envd 通路**：envd 的 `/health` 经代理 204——这就是 SDK 每次调用隐式走的路。

### A.5 错误码

路由信息错误时的行为：缺 sandbox id、非法端口（0）→ 400；未知沙箱 → 404。

### A.6 为什么 SDK 通过还不够，要专门直测

数据面是所有功能的地基：E2B 生态里浏览器直连沙箱（Host 域名）、SDK、第三方 HTTP client 全走它。SDK 测试只覆盖"SDK 那种用法"，但 SSE 缓冲、大 WS 帧、路径转义、Host 优先级这些边界只有直接构造请求才能暴露。例如"Host 优先于冲突路由头"：同时携带 `Host` 和一个错误的 `e2b-sandbox-id`，代理必须按 Host 路由——这种冲突场景 SDK 永远发不出来。SDK 层面的验证是隐式的：2.2–2.5 的 commands/files/pty/run_code 全部通过，本身就证明数据面对 SDK 流量形态是通的；HTTP 直测把这个隐式依赖变成显式、可控的验证。
