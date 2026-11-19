# model-registry-replication

Python 3.8.2 离线模型仓库复制库。解决跨机房同步中的典型问题：链路中断、
部分分片已传完、版本清单提前可见。核心保证：**任何可读版本都对应一组完整
且通过 SHA256 校验的权重与配置**——版本指针只在全部对象校验成功后原子切换，
发布中途失败绝不暴露半成品。

## 版本模型

一个版本 = 1 个参数文件 + N 个权重分片 + 1 份 SHA256 清单（manifest）。
所有对象按内容寻址（`objects/<sha256>`），清单记录每个对象的名称、摘要、
大小和类型（`params` / `shard`）。

```
<repo>/
    objects/<sha256>                 # 内容寻址对象存储
    versions/<version>/manifest.json # 版本清单
    current                          # 可读版本指针（原子切换）
    history.json                     # 发布历史，用于回滚
    incoming/                        # 暂存区：断点状态 + 半成品分片
        state.json
        <sha256>.part
```

## 工作流程

1. **清单校验**：先从源仓库读取清单，校验结构（必须有参数文件和至少一个
   分片）以及源端每个对象的尺寸与 SHA256；清单不一致直接报错，不传输、
   不发布。
2. **差量传输**：逐个比较目标端已有对象，存在且哈希正确的跳过，只传缺失
   或损坏的分片。分块传输，每块落盘后持久化续传位置。
3. **传输预算**：`budget_bytes` 用完后本轮停止，进度保存在
   `incoming/state.json`，下次运行从断点继续（包括文件内偏移）。
4. **原子发布**：全部对象通过 SHA256 校验后，先写清单，再原子替换
   `current` 指针（临时文件 + `os.replace`）。中途失败时指针不动，
   读者永远看不到半成品版本。
5. **回滚与清理**：`rollback()` 切回上一个已发布版本；
   `delete_version()` 删除旧版本清单并垃圾回收不再被引用的对象。

## 用法

```python
from model_replication import Repository, SyncEngine

source = Repository("/mnt/source-repo")
target = Repository("/mnt/target-repo")

engine = SyncEngine(
    source, target,
    budget_bytes=1 << 30,        # 本轮最多传 1 GiB，用尽后保存断点
    bandwidth_bps=50 << 20,      # 带宽限制 50 MiB/s
)

report = engine.sync_version("v2026.11")
if not report.completed:
    # 预算用尽或链路中断，下次调用从断点续传
    report = engine.sync_version("v2026.11")

engine.rollback()                # 回滚到上一个已发布版本
engine.delete_version("v2026.10")  # 删除旧版本并回收未引用对象
```

## 网络层抽象

同步引擎不直接访问网络，只依赖两方法接口 `Transport`
（`object_size` / `fetch`，见 `model_replication/transport.py`）。
默认实现 `LocalTransport` 从本地另一个仓库目录读取，测试借此用临时目录
模拟源端和目标端机房。生产环境只需用 SSH / HTTP / 对象存储客户端实现
同一接口即可接入真实网络，同步引擎无需改动。

带宽限制由引擎内的漏桶限速器实现，`clock` / `sleep` 可注入，便于测试。

## 测试

```
python -m unittest discover -s tests
```

覆盖：断点续传（单次的与多次预算窗口）、坏分片重传、重复执行幂等、
清单不一致拒绝发布、传输中途失败不暴露半成品、回滚、旧版本删除与
对象垃圾回收、带宽限制。

仅依赖 Python 标准库，兼容 Python 3.8.2+。
