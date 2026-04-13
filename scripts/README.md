# P2PSync 使用指南

基于 Rust 的高性能 P2P 文件分发工具，适用于从 CephFS 等集群搬运大规模数据到其他集群/节点。

---

## 快速开始

最简单的用法：serve 一个目录/文件，用 `--key` 直接下载，无需拷贝 manifest。

> **⚠️ Key 是什么？** `--key` 的值就是 serve 端的**完整绝对路径（full path）**。例如 serve 时 `--source /path/to/your/data/models`，那么下载时 `--key` 就填 `/path/to/your/data/models`。serve 启动后也会在终端输出可用的 key。

```bash
# Step 1: 在数据源节点启动 serve
bash serve.sh \
  --source /path/to/your/data/models \
  --tracker-addr http://x.x.x.x:9090 \
  --node-addr $YOUR_NODE_IP

# Step 2: 在目标节点下载
# key = serve 端 --source 的完整路径
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --key "/path/to/your/data/models" \
  --output-dir /data/models
```

> 如果需要多节点并行下载超大数据集（>1T），请使用 manifest 模式，见下方场景说明。

---

## 重要说明

### --node-addr 必须手动指定

serve 端的 `--node-addr` **必须由用户手动指定**，`hostname -I` 在容器环境中不可靠。

- **container environment**：使用 `$YOUR_NODE_IP`
- **其他环境**：填写本机可被其他节点访问的 IP

```bash
bash serve.sh \
  --source /path/to/your/data/models \
  --tracker-addr http://x.x.x.x:9090 \
  --node-addr $YOUR_NODE_IP
```

---

## 使用场景

### 场景 1：Key 模式 — 目录下载（G 级别，最常用）

适合 G 级别的目录下载，直接用目录的完整路径作为 key，一次性下载整个目录。无需拷贝 manifest，特别适合目录下有大量小文件的场景。

```bash
# Serve 端：--source 指定目录的完整路径
bash serve.sh \
  --source /path/to/your/data/training_data \
  --tracker-addr http://x.x.x.x:9090 \
  --node-addr $YOUR_NODE_IP

# Download 端：--key 填 serve 端 --source 的完整路径
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --key "/path/to/your/data/training_data" \
  --output-dir /data/output
```

也可以只下载某个子目录（key 填子目录的完整路径）：

```bash
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --key "/path/to/your/data/training_data/subdir" \
  --output-dir /data/output
```

> **注意**：
> - 目录 key 仅限 Fast Mode（不加 `--checksum`），因为 Fast Mode 下 key 就是路径，可以直接写
> - 目录 key 仅限单机下载（多机请用 manifest 模式）

### 场景 2：Key 模式 — 单文件/多文件下载

serve 一个文件，用 `--key` 直接下载。**key = serve 端 `--source` 的完整绝对路径**，serve 启动后终端会打印可用的 key。

```bash
# Serve 端：--source 指定文件的完整路径
bash serve.sh \
  --source /path/to/your/data/model.bin \
  --tracker-addr http://x.x.x.x:9090 \
  --node-addr $YOUR_NODE_IP

# Download 端：--key 填 serve 端 --source 的完整路径
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --key "/path/to/your/data/model.bin" \
  --output-dir /data/output
```

也可以一次传多个 `--key` 并行下载（受 `--parallel` 控制）：

```bash
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --key "/path/to/your/data/file1.bin" \
  --key "/path/to/your/data/file2.bin" \
  --output-dir /data/output \
  --parallel 2
```

### 场景 3：Manifest 模式 — 大数据集下载（T 级别）

当数据量超过 T 级别时，建议使用 manifest 模式。manifest 由 serve 自动生成文件列表，提供更细粒度的下载管理。需要手动拷贝 manifest 到下载节点

Manifest 模式**同时支持单节点和多节点**下载：

```bash
# Serve 端
bash serve.sh \
  --source /path/to/your/data/models \
  --tracker-addr http://x.x.x.x:9090 \
  --node-addr $YOUR_NODE_IP

# 单节点下载（同样支持，不需要设置 WORLD_SIZE）
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --manifest /tmp/p2psync_dump/manifest.txt \
  --output-dir /data/models

# 多节点并行下载（调度器设置 WORLD_SIZE 和 NODE_RANK，脚本自动分片）
# Pod 0: WORLD_SIZE=4 NODE_RANK=0
# Pod 1: WORLD_SIZE=4 NODE_RANK=1
# ...
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --manifest /tmp/p2psync_dump/manifest.txt \
  --output-dir /data/models
```

也可以通过命令行参数显式指定节点：(类似 torchrun)

```bash
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --manifest /tmp/p2psync_dump/manifest.txt \
  --output-dir /data/models \
  --world-size $WORLD_SIZE \
  --node-rank $RANK
```

> **自动检测规则**：`WORLD_SIZE > 1` 时自动进入多机模式，`WORLD_SIZE` 不设或 `= 1` 时为单机模式。

### 场景 4：多 Serve 节点（提高带宽）
多个节点同时 serve 同一份数据，下载端自动从多个 serve 节点拉取，分散带宽压力。

```bash
# 节点 A serve
bash serve.sh \
  --source /path/to/your/data/models \
  --tracker-addr http://x.x.x.x:9090 \
  --node-addr 10.0.0.10

# 节点 B serve（同一份数据，同一个 tracker）
bash serve.sh \
  --source /path/to/your/data/models \
  --tracker-addr http://x.x.x.x:9090 \
  --node-addr 10.0.0.11

# 下载端自动发现两个 serve 节点，轮询拉取
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --key "/path/to/your/data/models" \
  --output-dir /data/models
```

### 场景 5：下载中断后恢复

默认模式下重跑会覆盖重下。加 `--resume` 可以跳过已完成的文件并续传未完成的文件。

```bash
# 首次下载（中途 Ctrl+C 中断）
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --key "/path/to/your/data/models" \
  --output-dir /data/models

# 恢复下载
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --key "/path/to/your/data/models" \
  --output-dir /data/models \
  --resume
```

**`--resume` 做了什么：**
- Manifest 模式 — 脚本层：跳过 progress 文件中已记录完成的条目
- Manifest 模式 — Rust 层：对下载了一半的文件发送 Range 请求，从断点继续
- Key 模式 — Rust 层：逐个检查文件，已完成的跳过，未完成的断点续传

**不加 `--resume`（默认）：**
- 清空 progress，所有文件重新下载
- 已存在的文件会被删除并覆盖重下

#### 跨节点数 resume（m 机 → n 机）

Manifest 模式下，resume 使用全局统一的 progress 文件，**不绑定节点数量**：

| 场景 | 支持 |
|------|------|
| 单机下载一部分 → 多机 resume | ✅ 已完成的文件自动跳过 |
| 多机下载一部分 → 单机 resume | ✅ 已完成的文件自动跳过 |
| 4 机下载一部分 → 2 机 resume | ✅ 重新分片，已完成的自动跳过 |
| 2 机下载一部分 → 8 机 resume | ✅ 重新分片，已完成的自动跳过 |

```bash
# 第一次：单机下载，中途中断
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --manifest manifest.txt \
  --output-dir /data/models

# 第二次：切换到 4 机 resume，已完成的文件自动跳过
WORLD_SIZE=4 NODE_RANK=0 bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --manifest manifest.txt \
  --output-dir /data/models \
  --resume
```

### 场景 6：启用 MD5 校验

默认不做 MD5 校验（快速模式）。如果需要保证数据完整性，serve 和 download 都加 `--checksum`。

```bash
# Serve 端（启动时会计算所有文件的 MD5，大数据集会比较慢）
bash serve.sh \
  --source /path/to/your/data/models \
  --tracker-addr http://x.x.x.x:9090 \
  --node-addr $YOUR_NODE_IP \
  --checksum

# Download 端
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --manifest /tmp/p2psync_dump/manifest.txt \
  --output-dir /data/models \
  --checksum
```

**MD5 校验做了什么：**
- Serve 启动时计算所有文件的 MD5（流式计算，不占大内存）
- Manifest 中的 key 变为 MD5 值（而非文件路径）
- 下载完成后校验文件 MD5，不匹配则报错

**不加 `--checksum`（默认）：**
- Serve 秒启动，不计算 MD5
- 只通过文件大小判断是否完整
- 适合网络可靠、追求速度的场景

### 场景 7：多机 + 校验 + 恢复（全功能组合）

```bash
# WORLD_SIZE=4 NODE_RANK=0 由调度器设置
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --manifest /tmp/p2psync_dump/manifest.txt \
  --output-dir /data/models \
  --checksum \
  --resume \
  --parallel 8
```

---

## Key 模式 vs Manifest 模式

> **如何选择？** G 级别的文件/目录，单节点下载用 `--key` 最方便；T 级别超大数据集，需要多节点并行或更细粒度管理用 `--manifest`。

两种模式的核心区别：

- **Key 模式**：手动指定要下载的文件或目录（key = serve 端 `--source` 的完整路径），直接下载，无需额外操作
- **Manifest 模式**：serve 时自动生成文件列表（manifest），通过文件列表进行更细粒度的下载管理，支持多节点分片

| | Key 模式 (`--key`) | Manifest 模式 (`--manifest`) |
|---|---|---|
| **适用场景** | G 级别的文件/目录下载，日常使用 | T 级别超大数据集、需要多节点并行 |
| **工作方式** | 手动指定要下载的文件或目录路径 | 通过 serve 生成的文件列表下载，更细粒度 |
| **是否需要拷贝 manifest** | ❌ 不需要，直接填 serve 端 `--source` 的完整路径 | ✅ 需要手动拷贝到下载节点 |
| **多节点分片** | ❌ 不支持 | ✅ 支持，自动负载均衡 |
| **并发控制** | `--parallel` 控制并行 key 数，`--concurrency` 控制每个 key 内并发文件数 | `--parallel` × `--concurrency` |
| **resume** | Rust 层文件级 resume | 脚本层 + Rust 层双重 resume |

**简单规则**：
- 数据量在 G 级别，单节点下载 → 用 `--key`，最方便
- 数据量超过 T 级别，需要多节点并行 → 用 `--manifest`，支持分片和细粒度管理

---

## --parallel 和 --concurrency 的区别

download.sh 有两个并发参数，控制不同层级：

**Manifest 模式下：**

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `--parallel` | `4` | **进程级并行**：脚本层同时启动多少个 p2psync 进程，每个进程下载一个文件 |
| `--concurrency` | `32` | **连接级并发**：每个 p2psync 进程内部开几个并发连接 |

举例：`--parallel 4 --concurrency 32` = 同时 4 个文件，每个文件 32 个并发连接，总共最多 128 个连接。

**Key 模式下：**

| 参数 | 含义 |
|------|------|
| `--parallel` | **key 级并行**：指定多个 `--key` 时，同时下载几个 key（每个 key 一个 p2psync 进程）。只有一个 key 时不生效 |
| `--concurrency` | **文件级并行**：每个 key 内部，Rust 层同时下载多少个文件（目录展开后每个文件一个 task，concurrency 控制并发 task 数） |

所以 key 模式下载目录时，`--concurrency 32` 表示同时下载 32 个文件。

**调优建议：**
- 文件很大（几十 GB）、manifest 模式：提高 `--concurrency`（如 64、128）
- 文件很多但不大、manifest 模式：提高 `--parallel`（如 8、16）
- 目录 key 模式、大量小文件：提高 `--concurrency`（如 64、128）
- 多个 key 同时下载：提高 `--parallel`

**调优示例：**

```bash
# Manifest 模式：8 个文件并行，每个文件 64 并发连接
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --manifest /tmp/p2psync_dump/manifest.txt \
  --output-dir /data/models \
  --parallel 8 \
  --concurrency 64

# Key 模式（目录下载，用 --concurrency 控制并发文件数）
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --key "/path/to/your/data/training_data" \
  --output-dir /data/output \
  --concurrency 64

# Key 模式（多个 key 并行下载）
bash download.sh \
  --tracker-addr http://x.x.x.x:9090 \
  --key "/path/to/your/data/file1.bin" \
  --key "/path/to/your/data/file2.bin" \
  --key "/path/to/your/data/file3.bin" \
  --output-dir /data/output \
  --parallel 3
```

> **注意**：当前 Rust 层不支持单文件分片并发下载（一个文件只有一个 HTTP 连接）。`--concurrency` 控制的是多个文件之间的并发，不是单文件内部的分块。对于单个超大文件（如 1T+），下载速度取决于单连接带宽。

---

## 参数参考

### Tracker

```bash
p2psync tracker --port <PORT>
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--port` | `9090` | 监听端口 |

### serve.sh

```bash
bash serve.sh [OPTIONS]
```

| 参数 | 必选 | 默认值 | 说明 |
|------|------|--------|------|
| `--source` | ✅ | — | 要分发的源数据目录或单个文件 |
| `--tracker-addr` | ✅ | — | Tracker 地址，如 `http://x.x.x.x:9090` |
| `--node-addr` | ✅ | — | 本机 IP，**必须手动指定**。container environment用 `$YOUR_NODE_IP` |
| `--serve-port` | ❌ | `8081` | Serve 监听端口 |
| `--checksum` | ❌ | 不启用 | 启用 MD5 校验模式（启动较慢） |
| `--manifest-out` | ❌ | `$dump-dir/manifest.txt` | Manifest 输出路径 |
| `--dump-dir` | ❌ | `/tmp/p2psync_dump` | Dump/日志文件目录 |
| `--p2psync-bin` | ❌ | `p2psync` | p2psync 二进制路径（默认从 PATH 查找，可自定义） |

### download.sh

```bash
bash download.sh [OPTIONS]
```

| 参数 | 必选 | 默认值 | 说明 |
|------|------|--------|------|
| `--tracker-addr` | ✅ | — | Tracker 地址 |
| `--key` | 二选一 | — | 文件或目录的完整绝对路径，即 serve 端 `--source` 的路径（可多次指定），与 `--manifest` 互斥 |
| `--manifest` | 二选一 | — | Manifest 文件路径（serve 生成），与 `--key` 互斥 |
| `--output-dir` | ✅ | — | 下载输出目录 |
| `--world-size` | ❌ | `1`（环境变量 `WORLD_SIZE`） | 总节点数，>1 自动多机模式（仅 manifest 模式） |
| `--node-rank` | ❌ | `0`（环境变量 `NODE_RANK`） | 当前节点编号，从 0 开始（仅 manifest 模式） |
| `--checksum` | ❌ | 不启用 | 启用 MD5 校验 |
| `--resume` | ❌ | 不启用（覆盖模式） | 启用断点续传（支持跨节点数 resume） |
| `--parallel` | ❌ | `4` | Manifest 模式：同时下载的文件数；Key 模式：同时下载的 key 数 |
| `--concurrency` | ❌ | `32` | Manifest 模式：单文件内部并发连接数；Key 模式：同时下载的文件数 |
| `--max-retries` | ❌ | `3` | 单条目最大重试次数 |
| `--p2psync-bin` | ❌ | `p2psync` | p2psync 二进制路径（默认从 PATH 查找，可自定义） |

---

## 常见问题

### Q: Key 模式和 Manifest 模式怎么选？

- **G 级别**的文件/目录，单节点下载 → 用 `--key`，手动指定路径，最方便
- **T 级别**的超大数据集，需要多节点并行 → 用 `--manifest`，通过文件列表细粒度管理，支持自动分片

### Q: 多机模式怎么触发？

脚本自动读取环境变量 `WORLD_SIZE` 和 `NODE_RANK`：
- `WORLD_SIZE > 1` → 多机模式，自动分片
- `WORLD_SIZE` 不设或 `= 1` → 单机模式

也可以通过 `--world-size` 和 `--node-rank` 命令行参数覆盖。

### Q: Manifest 文件怎么获取？

Serve 启动后自动生成，默认在 `/tmp/p2psync_dump/manifest.txt`。需要手动拷贝到下载节点（或放在共享存储上）。

### Q: 下载失败了怎么办？

加 `--resume` 重跑即可，已完成的文件会被跳过：

```bash
bash download.sh ... --resume
```

### Q: `--checksum` 需要 serve 和 download 都加吗？

是的。两端必须一致。

### Q: 2T 的大文件会不会撑爆内存？

不会。MD5 计算和文件下载都是流式的（8MB/4MB 缓冲区），内存占用始终只有几 MB。

### Q: 多机模式下文件怎么分配？

使用贪心负载均衡算法：按文件大小降序排列，每次分配给当前总量最小的节点。比简单轮询更均衡。

### Q: 单机下载了一部分，能切换到多机 resume 吗？

可以。resume 使用全局统一的 progress 文件，不绑定节点数量。单机 → 多机、多机 → 单机、m 机 → n 机都可以无缝 resume。

### Q: Key 模式下 --concurrency 和 Manifest 模式下含义不同？

是的。
- **Manifest 模式**：`--concurrency` = 每个文件内部的并发连接数（Rust 层分块下载）
- **Key 模式（目录 key）**：`--concurrency` = 同时下载的文件数（Rust 层展开目录后的并发 task 数）

Key 模式下 `--parallel` 控制多个 key 的并行数（只有一个 key 时不生效）。
