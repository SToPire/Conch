# 对接 AgentENV Gateway / Scheduler

Conch 可以作为 AgentENV Node，通过原版 Gateway 和 Scheduler 接入官方 E2B SDK。本期支持 create/get/list/delete、commands/files/PTY 与应用端口代理，SDK 必须显式使用 **non-secure**。Conch 保留 `conch-init` 的 guest 初始化与网络配置，随后等待 envd `/health` 和 `/init` 成功才返回创建结果。

本文的上游基线为 AgentENV commit `1d742e4e149092be895f2c3cf0a097201229a250`。示例使用 Python `e2b==2.46.4`、JavaScript/TypeScript `e2b@2.46.1`；guest envd 使用仓库 [e2b-rootfs](../../examples/e2b-rootfs/) 固定的 infra `2026.22` 版本。

## 1. 准备两个节点

先按[环境准备](environment-setup.md)和[快速开始](getting-started.md)准备两个可正常启动 VMM 的 Conch 主机、CNI、guest kernel 和 `conch-init` initrd。示例地址如下，请替换为实际地址：

| 角色 | 地址 | 配置 |
| --- | --- | --- |
| Scheduler / Gateway | `10.0.0.10:9090` / `10.0.0.10:8080` | [scheduler.json](../../examples/agentenv/scheduler.json)、[gateway.json](../../examples/agentenv/gateway.json) |
| Conch Node A | `10.0.0.11:8000` | [conch-node-a.yaml](../../examples/agentenv/conch-node-a.yaml) |
| Conch Node B | `10.0.0.12:8000` | [conch-node-b.yaml](../../examples/agentenv/conch-node-b.yaml) |

Gateway 必须能够访问两个 Node 的 TCP API，Node 和 Gateway 都须能连接 Scheduler。Node API、Scheduler 和 metrics 端口用于集群内部；SDK 统一访问 Gateway。`node_id` 必须与 Scheduler `nodes[].id` 完全相同，两台 Node 使用同一 `cluster_id`。

Node YAML 只列出 `e2b` / `cluster` 增量配置。将这两节**替换**到各自主机已可运行的 Conch 配置中；不要追加重复的 YAML 节。如果主机适用全部内置默认值，也可以直接加载示例文件。保存后的配置应设为 `0600` 或 `0640`。

在 Gateway 和两台 Node 上设置**同一个** `AENV_API_KEY`。可在一处生成后通过自己的密钥分发方式配置另外两处：

```bash
export AENV_API_KEY="e2b_$(openssl rand -hex 32)"
```

Conch 仅从环境变量 `AENV_API_KEY` 读取密钥，YAML 不提供密钥字段。AgentENV Gateway 要求密钥为 32–256 个 URL-safe 字符。使用 sudo 启动时保留该变量：

```bash
# 在各 Node 的 Conch 仓库根目录执行；B 节点换成自己的配置路径。
chmod 0600 config/conch-node-a.yaml
sudo --preserve-env=AENV_API_KEY ./bin/conchd --config config/conch-node-a.yaml
```

`e2b.listen_addr` 为空时禁用 TCP API；`cluster.scheduler_addr` 为空时禁用 heartbeat，可单独运行 E2B Node API。创建总时限复用 `sandbox.request_timeout`（默认 `60s`），envd 初始化使用这次创建请求的剩余时间。heartbeat 周期与 RPC 超时内部固定为 `5s`。envd 默认用户和工作目录固定为 `user`、`/home/user`，由预置镜像提供。

本机容量同时计入启动中和已运行实例，创建前预留，确认释放后归还。实例上限内部固定为 `250`，总 vCPU 配额取宿主可见逻辑 CPU 数，总内存配额取宿主总内存（MiB）；本期不开放超卖或自定义预算配置。普通 E2B 创建请求的资源仍取自 `sandbox.default_spec` 及模板实际规格。

## 2. 在两个节点预置相同模板

Scheduler 不保证选中的节点已有模板，因此**开始 SDK 流量前**，所有候选节点都必须有相同 Template Name → digest 映射及工件。本期不提供 E2B 模板管理 API，使用 Conch CLI 完成预置。

先按 [e2b-rootfs 构建说明](../../examples/e2b-rootfs/README.md)构建并推送包含 envd 的 rootfs。该镜像保留 `/etc/conch/entrypoint`，由 `conch-init` 在完成网络初始化后启动；不要将 initrd 替换成 envd。镜像中的 envd 使用 `-isnotfc`，关闭 Firecracker MMDS 和 guest port-forwarder。

在 Node A 的 Conch 仓库根目录创建一次模板，再发布到两台 Node 均可访问的 registry。以下 registry、rootfs 引用和 kernel 路径需换成实际值：

```bash
export E2B_TEMPLATE_ID=registry.example.com/conch/e2b:agentenv-v1
sudo --preserve-env=AENV_API_KEY ./bin/conch template create \
  --config config/conch-node-a.yaml \
  --name "$E2B_TEMPLATE_ID" \
  --source registry.example.com/conch/e2b-rootfs:agentenv-v1 \
  --kernel /path/to/guest/kernel \
  --initrd build-artifacts/conch-init-initramfs.cpio.gz

sudo --preserve-env=AENV_API_KEY ./bin/conch template push \
  --config config/conch-node-a.yaml \
  "$E2B_TEMPLATE_ID" "$E2B_TEMPLATE_ID"
```

在 A、B 各自执行以下命令，B 使用 `config/conch-node-b.yaml`：

```bash
export E2B_TEMPLATE_ID=registry.example.com/conch/e2b:agentenv-v1
sudo --preserve-env=AENV_API_KEY ./bin/conch template pull --config config/conch-node-a.yaml "$E2B_TEMPLATE_ID"
sudo --preserve-env=AENV_API_KEY ./bin/conch template inspect --config config/conch-node-a.yaml "$E2B_TEMPLATE_ID"
```

确认两侧输出的 Template ID 完全相同，再启用 SDK 流量。CLI registry 命令支持 `--plain-http`；仅对实际使用明文的测试 registry 添加此选项，并置于位置参数之前。SDK 的 `templateID` 可以使用上述完整名称，也可以使用已经在两台 Node 存在的不可变 `sha256:...` digest；SDK 默认的 `base` 不会自动映射到此模板。

冷启动（cold）模板的 CPU/内存取自 Node 的 `sandbox.default_spec`。从 checkpoint 恢复的 resume 模板则按捕获时的实际 CPU/内存进行容量校验与预留；新 checkpoint 会保存 CPU 元数据 `io.conch.cpu-count`。缺少 captured CPU 元数据的旧 resume 模板会被拒绝，不猜测 Node 默认 CPU 数，也不迁移已有制品；需要用当前 Conch 重新生成 checkpoint 并预置到所有 Node。这里描述从模板创建新实例，E2B `/resume` 接口仍返回 `501`。

## 3. 构建和启动原版 AgentENV

在控制面主机编译固定版本，无需更改 AgentENV 源码、重新生成 proto 或构建其 Rust Node：

```bash
git clone https://github.com/kvcache-ai/AgentENV.git AgentENV
git -C AgentENV checkout --detach 1d742e4e149092be895f2c3cf0a097201229a250
make -C AgentENV/services build
```

在分别运行 Scheduler、Gateway 的终端中执行，配置路径指向已经替换地址的示例文件：

```bash
./AgentENV/services/bin/scheduler -config /path/to/Conch/examples/agentenv/scheduler.json
```

```bash
# 此终端已配置与两个 Node 相同的 AENV_API_KEY。
./AgentENV/services/bin/gateway -config /path/to/Conch/examples/agentenv/gateway.json
```

检查 Gateway 可达以及两个节点的观测状态：

```bash
export E2B_API_KEY="$AENV_API_KEY"
export E2B_API_URL=http://10.0.0.10:8080
curl -fsS "$E2B_API_URL/health"
curl -fsS -H "X-API-Key: $E2B_API_KEY" \
  "$E2B_API_URL/nodes?clusterID=conch-agentenv-demo"
```

`/nodes` 应显示两个 Node、`ready` 状态及实际分配资源。示例使用 static discovery、round-robin 和内存 binding，未配置 Scheduler 的 `node_resource_limit`，因此没有启用其资源阈值过滤。本机请求容量校验仍由 Conch 执行。

## 4. 运行官方 SDK

SDK 客户端设置三个变量；**两个 URL 都指向 Gateway，且不加 `/proxy` 后缀**：

```bash
export E2B_API_KEY="$AENV_API_KEY"
export E2B_API_URL=http://10.0.0.10:8080
export E2B_SANDBOX_URL="$E2B_API_URL"
export E2B_TEMPLATE_ID=registry.example.com/conch/e2b:agentenv-v1
```

在 Conch 仓库根目录执行 Python 示例：

```bash
python3 -m venv .venv-agentenv
.venv-agentenv/bin/pip install 'e2b==2.46.4'
.venv-agentenv/bin/python examples/agentenv/python-sdk.py
```

使用 JavaScript/TypeScript SDK 的可直接执行示例：

```bash
npm install --prefix examples/agentenv --no-save --package-lock=false e2b@2.46.1
node examples/agentenv/js-sdk.mjs
```

两个脚本均保留两个实例，执行 get、分页 list、commands、files，最后 kill。在两个正常节点且无其他创建流量的条件下，round-robin 将两个创建分配到不同 Node；可结合 Node 日志中的 sandbox ID 确认回路由。

Python 调用使用 `Sandbox.create(template, secure=False, timeout=300)`，JS/TS 使用 `Sandbox.create(template, { secure: false, timeoutMs: 300000 })`。不要依赖 SDK 的 secure 默认值。创建时的 timeout 决定实例初始存活时间：默认 `300` 秒，到期删除；HTTP 创建请求传 `0` 表示立即到期，不是无限期保留。它与 Node 创建操作的 `sandbox.request_timeout` 不同。更新 timeout / refresh 暂不支持。

## 5. Header 和 Host 路由

默认的 `sandbox_proxy_domains: []` 配合 `E2B_SANDBOX_URL` 即可运行 commands/files。SDK 数据请求携带 `E2b-Sandbox-Id` 和 `E2b-Sandbox-Port`；Gateway 查询该实例绑定，将 `/proxy` 流量交给拥有它的 Conch，再访问 guest envd。以下请求使用一个仍存活的 sandbox ID：

```bash
export SANDBOX_ID='替换为仍存活的实例ID'
curl -i -H "X-API-Key: $E2B_API_KEY" \
  -H "E2b-Sandbox-Id: $SANDBOX_ID" \
  -H "E2b-Sandbox-Port: 49983" \
  "$E2B_API_URL/health"
```

要使用 `get_host()` / `getHost()` 生成的应用 URL，在 Gateway 和两个 Conch 的 `sandbox_proxy_domains` 中配置相同裸域，例如 `sandboxes.example.com`。域名不包含 scheme、端口或路径，须将 `*.sandboxes.example.com` 解析到 Gateway；Conch 返回第一个配置域。应用 URL 形如 `{port}-{sandboxID}.sandboxes.example.com`，guest 应用监听 `0.0.0.0:{port}`。

若 Gateway 仍使用示例端口 `8080`，浏览器/curl URL 需带 `:8080`；SDK `get_host()` 返回的主机名不会包含这个 Gateway 端口。无需 DNS 也可以先通过 Host header 验证路由：

```bash
curl -i -H "Host: 49983-$SANDBOX_ID.sandboxes.example.com" \
  "$E2B_API_URL/health"
```

non-secure 模式的应用/guest 数据流量允许公开访问；控制面仍要求 API key。这里的 `secure=false` 指 envd token 模式，和 Gateway 是否部署 HTTPS 是不同配置。

配置好相同的 proxy domain 后，可在专用、空闲的双节点集群运行完整验证脚本。Python 脚本还检查 PTY、HTTP/SSE/WebSocket、资源/roster、到期回收和未支持功能的拒绝行为；guest 只需 Python 3 及常规 envd 依赖，无需安装第三方 Python 包：

```bash
.venv-agentenv/bin/pip install 'websockets==15.0.1'
.venv-agentenv/bin/python examples/agentenv/integration-smoke.py
node examples/agentenv/integration-smoke.mjs
```

沿用前文的三个 SDK 环境变量及 `E2B_TEMPLATE_ID`；脚本通过 Host header 验证域名路由，不要求本机解析该 wildcard DNS。

## 6. 当前范围

以下能力由 Conch 返回 `501 Not Implemented`，错误体注明 `Unimplemented`：

| 能力 | 接口或触发方式 |
| --- | --- |
| 完整 pause/resume/connect | `POST /sandboxes/{id}/pause`、`/resume`、`/connect` |
| fork、snapshot、timeout 更新和 refresh | `POST /sandboxes/{id}/fork`、`/snapshots`、`/timeout`、`/refreshes` |
| cold-create、E2B 模板及卷管理 | `/sandboxes-cold`、`/templates...`、`/volumes...` |
| secure、自动暂停/恢复 | 未显式 `secure=false`，或启用 `autoPause` / `autoResume` |
| 其他扩展创建参数 | 非空 volumeMounts、MCP、customExtensionParams；私有流量或 maskRequestHost |

Conch 私有 suspend/resume 只暂停/继续 VMM，不等价于完整 E2B pause/resume。当前启动及退出仍清理 sandbox，不提供实例跨 daemon 重启恢复。

Scheduler 的 heartbeat 状态是观测结果，**不是新建调度门禁**：当前上游不会仅因未 heartbeat、超时或非 Ready 自动排除 static 节点。示例只覆盖两个正常节点的接入；节点故障避让、binding 写入失败、跨节点预留及故障恢复未作为本期验收承诺。内存 binding 在 Scheduler 重启后会暂时丢失，后续完整 heartbeat roster 可重建；这不等于 sandbox 自身的持久恢复。
