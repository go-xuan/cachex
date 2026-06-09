# cachex

缓存客户端管理工具，支持 Redis 和本地缓存，可扩展自定义后端。

## 安装

```bash
go get github.com/go-xuan/cachex
```

## 快速开始

在 `conf/cache.yaml` 中配置：

```yaml
source: "default"
driver: "redis"
enable: true
address: "localhost:6379"
password: ""
database: 0
mode: 0  # 0-单机 1-集群
```

```go
import "github.com/go-xuan/cachex"

func main() {
    cachex.Initialize()
    client := cachex.GetClient("default")
    client.Set(ctx, "key", "value", time.Hour)
    val := client.GetString(ctx, "key")
}
```

## 主要功能

- **多后端支持** — 内置 `redis`（go-redis/v9）和 `local`（go-cache）
- **多数据源** — 支持同时连接多个 Redis 实例
- **统一接口** — Get/Set/Delete/Exist/Expire 等常用缓存操作
- **可扩展** — 通过 RegisterClientBuilder 注册自定义后端
