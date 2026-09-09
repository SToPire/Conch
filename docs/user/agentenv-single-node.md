# 单机运行 Conch、AgentENV Gateway 和 Scheduler

先在一台机器上运行一个 conchd、一个 Scheduler 和一个 Gateway，打通完整链路：

```text
E2B SDK -> Gateway :8080 -> conchd :8000 -> guest envd
                  |           |
                  +-> Scheduler :9090 <-+
```

以下 Conch 命令均在仓库根目录执行。三个服务各占一个终端，SDK 和模板操作使用其他终端。

## 1. 准备依赖并编译

主机需要 KVM、VMM、EROFS 工具、CNI，以及支持 EROFS 的 guest kernel。安装和主机配置方法见[环境准备](environment-setup.md)。

```bash
make build
make build-conch-init-initramfs
```

产物：

- `bin/conchd`、`bin/conch`
- `build-artifacts/conch-init-initramfs.cpio.gz`

## 2. 配置并启动 Conch

创建本地配置：

```bash
cp config/config.yaml config/config.local.yaml
chmod 0600 config/config.local.yaml
```

把其中 `e2b`、`cluster` 两节替换为以下内容，不要追加重复的 YAML 节：

```yaml
e2b:
  listen_addr: "127.0.0.1:8000"
  sandbox_proxy_domains: []

cluster:
  scheduler_addr: "127.0.0.1:9090"
  node_id: "conch-node-a"
  cluster_id: "conch"
```

同时检查 `sandbox.backend` 和对应 binary 路径。当前默认是 StratoVirt；如果使用已验证的 Cloud Hypervisor，需要修改已有 `sandbox` 节中的对应字段，保留其他配置：

```yaml
sandbox:
  backend: cloud-hypervisor
  cloud_hypervisor:
    binary: /usr/local/bin/cloud-hypervisor
  # 其余已有配置保留
```

生成一次密钥，让 Conch、Gateway 和 SDK 使用同一个值：

```bash
export AENV_API_KEY="e2b_$(openssl rand -hex 32)"

sudo --preserve-env=AENV_API_KEY \
  ./bin/conchd --config config/config.local.yaml
```

在后续终端中设置相同的 `AENV_API_KEY`，不要分别生成不同的密钥。conchd 在前台运行；Scheduler 尚未启动时，心跳会暂时报连接失败。

创建操作的总时限复用 `sandbox.request_timeout`（默认 `60s`），envd 初始化使用剩余时间。guest 默认用户和工作目录固定为 `user`、`/home/user`。

## 3. 准备包含 envd 的模板

普通 Ubuntu/nginx 镜像不够，需要包含 envd、`user` 用户和 `/home/user`。可以构建仓库的 [e2b-rootfs 示例](../../examples/e2b-rootfs/README.md)。该构建示例需要已经运行的 BuildKit 和 registry。

以下假设 rootfs 镜像已经推送到本机 registry：

```text
localhost:5000/conch/e2b-rootfs:debug
```

在设置了相同 `AENV_API_KEY` 的终端创建模板。替换实际 guest kernel 路径；如果镜像使用其他名称，也一并修改 `--source`：

```bash
sudo --preserve-env=AENV_API_KEY ./bin/conch template create \
  --config config/config.local.yaml \
  --name localhost:5000/conch/e2b:demo \
  --source localhost:5000/conch/e2b-rootfs:debug \
  --plain-http \
  --kernel /实际路径/guest-kernel \
  --initrd build-artifacts/conch-init-initramfs.cpio.gz
```

`--plain-http` 对应示例中的明文 registry；HTTPS registry 不需要它。模板创建成功后，再进行 SDK 操作。

## 4. 编译并启动原版 Scheduler、Gateway

获取已验证的 AgentENV 版本；如果目标目录已有对应源码，可直接复用：

```bash
git clone https://github.com/kvcache-ai/AgentENV.git /tmp/agentenv-control-plane
git -C /tmp/agentenv-control-plane checkout --detach \
  1d742e4e149092be895f2c3cf0a097201229a250
make -C /tmp/agentenv-control-plane/services build
```

创建 `config/scheduler.local.json`：

```json
{
  "scheduler": {
    "grpc_listen_addr": ":9090",
    "metrics_listen_addr": ":9101",
    "strategy": "round_robin",
    "discovery": {"mode": "static"},
    "nodes": [
      {"id": "conch-node-a", "endpoint": "http://127.0.0.1:8000"}
    ]
  }
}
```

创建 `config/gateway.local.json`：

```json
{
  "gateway": {
    "http_listen_addr": ":8080",
    "metrics_listen_addr": ":9102",
    "scheduler_addr": "127.0.0.1:9090",
    "request_timeout": "90s",
    "sandbox_proxy_domains": []
  }
}
```

分别在两个终端启动。Gateway 终端需要设置与 Conch 相同的 `AENV_API_KEY`：

```bash
/tmp/agentenv-control-plane/services/bin/scheduler \
  -config config/scheduler.local.json
```

```bash
/tmp/agentenv-control-plane/services/bin/gateway \
  -config config/gateway.local.json
```

检查节点上报：

```bash
curl -fsS -H "X-API-Key: $AENV_API_KEY" \
  http://127.0.0.1:8080/nodes
```

等待约一个心跳周期（`5s`），应能看到 `conch-node-a`。

## 5. 运行官方 E2B SDK

在设置了相同 `AENV_API_KEY` 的终端执行：

```bash
python3 -m venv .venv-agentenv
.venv-agentenv/bin/pip install 'e2b==2.46.4'

export E2B_API_KEY="$AENV_API_KEY"
export E2B_API_URL=http://127.0.0.1:8080
export E2B_SANDBOX_URL="$E2B_API_URL"
export E2B_TEMPLATE_ID=localhost:5000/conch/e2b:demo
```

验证一个实例：

```bash
.venv-agentenv/bin/python - <<'PY'
import os
from e2b import Sandbox

box = Sandbox.create(
    os.environ["E2B_TEMPLATE_ID"],
    secure=False,
    timeout=300,
)
try:
    print("sandbox:", box.sandbox_id)
    print(box.commands.run("echo hello-from-conch").stdout)
    box.files.write("/tmp/hello.txt", "Conch + AgentENV")
    print(box.files.read("/tmp/hello.txt"))
finally:
    box.kill()
PY
```

看到命令输出和文件内容，就说明完整链路跑通了。当前必须显式设置 `secure=False`；`pause/resume/connect` 等后续能力返回 `501 Not Implemented`。

扩展到双节点时，按照[集群运行指南](agentenv.md)配置第二台 Conch 主机，并在两边预置相同模板。
