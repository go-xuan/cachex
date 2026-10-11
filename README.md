# cachex

缓存客户端管理工具，内置 Redis（go-redis/v9）和本地缓存（go-cache）两种后端，支持多数据源与自定义扩展。

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
mode: 0  # 0-单机 1-集群 2-哨兵
```

```go
import (
    "context"
    "fmt"
    "time"

    "github.com/go-xuan/cachex"
)

func main() {
    if err := cachex.Initialize(); err != nil { // 读取 cache.yaml 并初始化客户端
        panic(err)
    }
    defer cachex.Close() // 关闭所有客户端

    ctx := context.Background()
    client := cachex.GetClient("default") // 取 default 源（不传参数时同样取 default）

    _ = client.Set(ctx, "user:1", User{Name: "quan"}, time.Hour)

    var user User
    if client.Get(ctx, "user:1", &user) { // 任意类型，内部按 marshal 反序列化
        fmt.Println(user.Name)
    }

    client.Exist(ctx, "user:1")            // 是否存在
    client.Expire(ctx, "user:1", time.Hour) // 续期
    client.Delete(ctx, "user:1")            // 删除
}
```

## 主要功能

- **多后端支持** — 内置 `redis`（go-redis/v9）和 `local`（go-cache），可注册自定义后端
- **多数据源** — 支持同时连接多个缓存实例，通过 source 名称区分；未指定源名时回退 `default`，再无则回退池中第一个，显式指定源名时只回退 `default`
- **统一接口** — Get/Set/SetNX/CompareAndDelete/Delete/Exist/Expire/TTL 与批量读写等常用操作，屏蔽大部分后端差异（见「关于本地缓存的限制」）
- **透明序列化** — 值经 `marshalx` 序列化后存储，支持 json/yaml/yml/toml/properties/msgpack
- **key 前缀** — 通过 `prefix` 统一加前缀，多环境隔离
- **配置驱动** — 配合 configx 自动从 nacos / 本地文件加载，支持单配置与多配置列表
- **延迟失败** — Redis 客户端创建时仅做一次 PING 校验，失败只记录日志不阻断初始化（缓存通常不是硬依赖），实际错误在首次读写时暴露；需要显式确认连通性（启动自检、健康检查）时调用 `Ping`

## 配置说明

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `source` | `default` | 缓存源名称，`GetClient` 的寻址键 |
| `driver` | `local` | 客户端驱动，内置 `redis` / `local`（本地缓存有若干能力限制，见「关于本地缓存的限制」） |
| `enable` | `false` | 是否启用该数据源，为 false 时跳过初始化 |
| `address` | `localhost` | 主机地址，格式为 `host:port`，多个地址用 `,` 分隔（集群模式）；`redis` 驱动必须显式配置，如 `127.0.0.1:6379`，缺失或缺少端口会在初始化阶段直接报错 |
| `username` | - | 用户名 |
| `password` | - | 密码 |
| `database` | `0` | Redis 数据库编号 |
| `prefix` | - | 缓存 key 前缀 |
| `mode` | `0` | Redis 模式：`0` 单机 / `1` 集群 / `2` 哨兵 |
| `master` | - | 哨兵模式下的主服务器名称 |
| `poolSize` | - | Redis 连接池大小 |
| `marshal` | `json` | 序列化方式：`json` / `yaml` / `yml` / `toml` / `properties` / `msgpack` |

### 多数据源

`cache.yaml` 支持写成列表，一次配置多个源：

```yaml
- source: "default"
  driver: "redis"
  enable: true
  address: "localhost:6379"
  database: 0
- source: "session"
  driver: "redis"
  enable: true
  address: "localhost:6379"
  database: 1
  prefix: "sess:"
- source: "local"
  driver: "local"
  enable: true
```

```go
cachex.GetClient("session")  // 取指定源
cachex.GetClient()           // 取 default 源
cachex.GetClient("unknown")  // 源不存在时回退 default
```

## 客户端接口

`Client` 是各后端实现的统一接口：

```go
type Client interface {
    GetInstance() interface{} // 获取底层实例（redis.UniversalClient / *cache.Cache）
    GetConfig() *Config       // 获取配置
    Close() error             // 关闭客户端
    Ping(ctx context.Context) error // 探活，本地缓存恒为 nil

    GetKey(key string) string // 获取带前缀的缓存 key

    Set(ctx context.Context, key string, value any, expiration time.Duration) error  // 更新缓存
    SetNX(ctx context.Context, key string, value any, expiration time.Duration) bool // 仅当 key 不存在时写入（可用于分布式锁）
    CompareAndDelete(ctx context.Context, key string, value any) bool                // 仅当值一致时删除缓存（安全解锁，与 SetNX 配对）
    Get(ctx context.Context, key string, value any) bool                             // 获取缓存（指针，任意类型）
    GetString(ctx context.Context, key string) string                                // 获取缓存（字符串类型）
    MSet(ctx context.Context, values map[string]any, expiration time.Duration) error // 批量更新缓存
    MGet(ctx context.Context, keys []string) map[string]string                       // 批量获取缓存（字符串类型）
    Expire(ctx context.Context, key string, expiration time.Duration) bool           // 续期缓存（仅当 key 存在）
    TTL(ctx context.Context, key string) time.Duration                               // 获取剩余有效期
    Delete(ctx context.Context, key string) bool                                     // 删除缓存
    Exist(ctx context.Context, key string) bool                                      // 是否存在缓存
}
```

只有 `Set` / `MSet` 返回 `error`（它们可能因序列化失败而失败，属编程错误）；其余方法统一返回 `bool` 表示「操作是否生效」，`Expire` 的 `bool` 表示 key 是否存在。序列化失败或后端故障时同样返回 `false`，具体原因见日志；需要严格区分业务结果与故障时，可用 `GetInstance` 取出底层客户端自行处理。

`TTL` 的返回值遵循 Redis 语义，可用两个常量判断：

```go
cachex.TTLNoExpire // -1ns，键存在但没有设置过期时间
cachex.TTLNotExist // -2ns，键不存在
```

两端都按**秒**返回剩余时长（`redis` 由 `TTL` 命令取整，`local` 对齐同一算法），剩余不足 500ms 时返回 `0`。

命令执行出错时 `TTL` 同样返回 `TTLNotExist`（与 `GetString` 出错返回空串的风格一致，需要区分错误请使用底层客户端）。

### 关于 expiration

`expiration` 以 Redis 原生语义为准，本地后端尽力对齐：

| 取值 | `Set` | `MSet` | `Expire` |
|------|-------|--------|----------|
| `> 0` | 设置过期时长 | 同 `Set` | 续期 |
| `0` | 永不过期 | 同 `Set` | 取消过期时间（转为永不过期，而非立刻删除） |
| `TTLNoExpire`（`-1`） | 等价 Redis 的 `KEEPTTL`：更新值但保留原过期时间 | 按 `0` 处理（`MSET` 不支持 `KEEPTTL`） | 同 `0` |
| 其他负数 | 同 `0`（`SetNX` 会把它拼成非法命令被服务端拒绝，统一归零） | 同 `0` | 同 `0` |

`SetNX` 总是写入新 key，不存在可保留的过期时间，故 `-1` 与其他负数一样按永不过期处理。对不存在的 key 调用 `Expire` 返回 `false`。

### 关于批量操作

`MGet` / `MSet` 在单机/哨兵模式下使用原生 `MGET`/`MSET` 命令；以下两种情况改用 pipeline 逐条收发：

- 集群模式：多 key 跨 slot 会触发 `CROSSSLOT`
- `MSet` 传了正数 `expiration`：`MSET` 本身不支持过期时间，只能逐条 `SET`

代价是这两种情况下不再具备单命令的原子性（`MGet` 传空 `keys`、`MSet` 传空 `values` 时直接返回 `nil`，不发命令）。另外 `MGet` 在 `redis` 端命令出错时同样返回 `nil`，与「key 全不存在」无法区分（`local` 端不会出错），需要区分错误请使用底层客户端。

### 关于分布式锁

`SetNX` 加锁 + `CompareAndDelete` 解锁构成完整的安全锁语义，解锁时比对持有者标识，避免删掉已被他人重新获取的锁：

```go
token := uuid.NewString() // 每个持有者的唯一标识，任意唯一值即可
if !client.SetNX(ctx, "lock:order", token, 30*time.Second) {
    return // 未抢到锁
}
defer client.CompareAndDelete(ctx, "lock:order", token) // 仅当仍是自己的锁才释放
```

Redis 端由 Lua 脚本保证「比对 + 删除」的原子性；本地缓存端由客户端自身的互斥锁保证，详见下文「关于本地缓存的限制」。

`SetNX` 返回 `false` 时无法区分「锁已被占用」与「后端故障」，`CompareAndDelete` 同理（「已不是自己的锁」与「故障」）。锁语义下这是安全方向——拿不到锁就不进临界区；但对可用性敏感的场景（例如后端不可用时应快速失败，而不是静默跳过），请先用 `Ping` 探活，或改用 `GetRedisUniversalClient` 拿到底层客户端自行区分错误。

### 关于本地缓存的限制

`local` 驱动基于 go-cache，是单进程内存缓存：**不跨进程共享，进程重启即丢失**。适合放配置、字典这类随时可重建的数据，不适合放会话状态，也不适合做跨实例需要一致的锁。

在此前提下，`local` 尽量向 `redis` 对齐：go-cache 只保证单个方法内部原子，而 `CompareAndDelete`、`Expire`、`Delete`、`MSet`、`MGet` 都需要跨多次读写保持原子，这类操作由 `LocalClient` 自身的一把互斥锁兜底——所有写路径与批量读参与该锁，单键读（`Get`/`GetString`/`Exist`/`TTL`）依赖 go-cache 自身的锁。因此这些操作在本客户端实例内与 `redis` 端语义一致，源码中对应方法也据实描述而非标注为缺陷。

真正无法对齐的只剩下面一处：

| 方法 | 本地缓存的限制 | redis 端 |
|------|---------------|---------|
| `Set`（`expiration` 传 `TTLNoExpire`） | 没有 `KEEPTTL`，只能读回原过期时间再按剩余时长重写，与原 TTL 存在纳秒级漂移 | 原生 `KEEPTTL`，服务端原样保留过期时间 |

另有几点需要注意：

- `Close` 会清空本地缓存的全部数据（数据随客户端生命周期存在），`redis` 端只断开连接、服务端数据保留；两端都支持重复调用
- 过期键的回收方式不同：`redis` 由服务端主动 + 惰性回收；`local` 未启用 go-cache 自带、且一旦启动就无法停止的 janitor，改由写路径按每分钟一次的频率调用 `DeleteExpired` 主动回收，访问过期键时也会被惰性删除，因此不会读到过期值。纯读且长时间无写入的时段不做主动回收，但 `Close` 能确定性地释放全部资源
- 通过 `GetInstance` 取出底层 `*cache.Cache` 直接读写会绕过那把锁，此时不再享有上述原子性保证
- `MSet` / `MGet` 在 `local` 端反而比 `redis` 端更强：`local` 端始终是原子写入与一致快照，而 `redis` 端在集群模式（以及 `MSet` 带过期时间）时会退化为 pipeline（见上文「关于批量操作」）

### 关于值的序列化

写入的值统一经 `marshalx` 序列化后存储，因此 `GetString` 返回的是**序列化后的原始字符串**——`Set(ctx, "k", "abc", d)` 之后 `GetString` 拿到的是带 JSON 引号的 `"abc"`。需要原始类型时用 `Get`：

```go
var s string
client.Get(ctx, "k", &s) // s == "abc"
```

反过来，`GetString` 适合读取由其他语言/服务直接写入的纯字符串。`Get` 对空字符串值会返回 false（内部以空串判断键不存在）。

`MGet` 同理，返回的也是**序列化后的原始字符串**；需要类型化批量读取时，请按 `marshal` 配置解码，或循环调用 `Get`（`Get` 内部使用同一 codec，无需关心序列化方式）。

### 包级函数

| 函数 | 说明 |
|------|------|
| `Initialize() error` | 加载配置并初始化所有启用的数据源 |
| `GetClient(source ...string) Client` | 获取缓存客户端；未指定源名时回退 `default`，再无则回退池中第一个；显式指定源名未命中时只回退 `default`，两者都不存在则返回 `nil` |
| `GetConfig(source ...string) *Config` | 获取客户端配置；客户端不存在时返回 `nil` |
| `GetInstance[T](source ...string) T` | 获取底层实例，客户端不存在或类型断言失败会 panic |
| `GetRedisUniversalClient(source ...string) redis.UniversalClient` | 直接获取 go-redis 客户端，执行原生命令 |
| `NewClient(config *Config) (Client, error)` | 按配置创建客户端（不加入池） |
| `AddClient(source string, client Client)` | 手动向客户端池添加 |
| `Pool() *typex.Enum[string, Client]` | 获取客户端池 |
| `Initialized() bool` | 是否已初始化 |
| `Close() error` | 关闭所有客户端 |

## 扩展自定义后端

cachex 通过 Builder 注册机制支持扩展任意缓存后端，内置 `local` 和 `redis`，可通过 `RegisterClientBuilder` 注册其他实现（如 etcd、memcached）。

### 核心接口

实现 `Client` 接口（见上文），以及一个 `ClientBuilder` 构造函数：

```go
type ClientBuilder func(*Config) (Client, error)
```

### 示例：接入 memcached

```go
package mycache

import (
    "context"
    "time"

    "github.com/go-xuan/cachex"
    "github.com/bradfitz/gomemcache/memcache"
)

// 1. 定义 MemcacheClient，实现 cachex.Client 接口
type MemcacheClient struct {
    config *cachex.Config
    client *memcache.Client
}

func (c *MemcacheClient) GetInstance() interface{}   { return c.client }
func (c *MemcacheClient) GetConfig() *cachex.Config  { return c.config }
func (c *MemcacheClient) Close() error               { return nil }
func (c *MemcacheClient) Ping(ctx context.Context) error { return nil }
func (c *MemcacheClient) GetKey(key string) string   { return c.config.GetKey(key) }

func (c *MemcacheClient) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
    // 实现 memcached 写入逻辑，expiration 为秒级
    return nil
}

func (c *MemcacheClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) bool {
    // 实现 memcached Add（不存在才写入）逻辑
    return false
}

func (c *MemcacheClient) Get(ctx context.Context, key string, value any) bool   { return false }
func (c *MemcacheClient) GetString(ctx context.Context, key string) string      { return "" }
func (c *MemcacheClient) Delete(ctx context.Context, key string) bool           { return false }
func (c *MemcacheClient) Exist(ctx context.Context, key string) bool            { return false }

func (c *MemcacheClient) CompareAndDelete(ctx context.Context, key string, value any) bool {
    // 实现「值一致才删除」逻辑；memcached 无原生 CAS 删除，通常用 gets + cas 命令组合
    return false
}

func (c *MemcacheClient) MSet(ctx context.Context, values map[string]any, expiration time.Duration) error {
    // 实现批量写入逻辑
    return nil
}

func (c *MemcacheClient) MGet(ctx context.Context, keys []string) map[string]string {
    // 实现批量读取逻辑，注意 key 需经 c.config.GetKeys 加前缀
    return nil
}

func (c *MemcacheClient) Expire(ctx context.Context, key string, expiration time.Duration) bool {
    // 返回 key 是否存在
    return false
}

func (c *MemcacheClient) TTL(ctx context.Context, key string) time.Duration {
    // 返回 cachex.TTLNoExpire / cachex.TTLNotExist 或剩余时长
    return cachex.TTLNotExist
}

// 2. 定义 ClientBuilder
func MemcacheClientBuilder(config *cachex.Config) (cachex.Client, error) {
    return &MemcacheClient{
        config: config,
        client: memcache.New(config.Address),
    }, nil
}

// 3. init() 中注册，之后即可在 cache.yaml 里写 driver: "memcache"
func init() {
    cachex.RegisterClientBuilder("memcache", MemcacheClientBuilder)
}
```

注册后，只要在 `cache.yaml` 中写入 `driver: "memcache"`，`Initialize()` 就会用该 Builder 创建客户端：

```yaml
source: "default"
driver: "memcache"   # 对应 RegisterClientBuilder 注册的名称
enable: true
address: "localhost:11211"
marshal: "json"
```
