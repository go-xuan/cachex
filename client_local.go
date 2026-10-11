package cachex

import (
	"context"
	"sync"
	"time"

	"github.com/go-xuan/utilx/errorx"
	"github.com/go-xuan/utilx/marshalx"
	"github.com/patrickmn/go-cache"
	log "github.com/sirupsen/logrus"
)

// LocalClientBuilder 本地缓存客户端构建器
func LocalClientBuilder(config *Config) (Client, error) {
	return NewLocalClient(config)
}

// defaultCleanupInterval 过期键的默认回收间隔
//
// go-cache 自带的 janitor 一旦启动就无法停止——它只在缓存实例被 GC 回收时由 finalizer 关闭，
// 这会让 Close 无法确定性地释放资源。因此这里传 0 不启动 janitor，
// 改由写路径按该间隔调用 DeleteExpired 主动回收，Close 时再 Flush 兜底，
// 使资源生命周期与 redis 客户端一致。代价是纯读且长时间无写入的时段不做主动回收，
// 不过过期键在下次被访问时仍会被 go-cache 惰性删除，不会读到过期值
const defaultCleanupInterval = time.Minute

// NewLocalClient 创建本地缓存客户端
func NewLocalClient(config *Config) (*LocalClient, error) {
	return &LocalClient{
		config:          config,
		marshal:         marshalx.Apply(config.Marshal),
		cache:           cache.New(cache.NoExpiration, 0), // 默认永不过期，且不启动 go-cache 的 janitor
		lastCleanup:     time.Now(),
		cleanupInterval: defaultCleanupInterval,
	}, nil
}

// LocalClient 本地缓存客户端
//
// 基于 go-cache 的单进程内存缓存，不跨进程共享、进程重启即丢失，
// 适合放配置、字典这类随时可重建的数据。
//
// go-cache 只保证单个方法内部原子，而 CompareAndDelete/Expire/Delete/MSet/MGet
// 都需要跨多次读写保持原子，因此由 LocalClient 自身的一把互斥锁兜底，
// 使这些操作在本客户端实例内与 RedisClient 语义一致；
// 受 go-cache 原语所限确实无法对齐的只剩 Set 传 TTLNoExpire 时的剩余时长漂移，
// 详见 set 方法的注释。
type LocalClient struct {
	config  *Config
	marshal marshalx.Marshal
	cache   *cache.Cache
	// mu 让"读-改-写"这类复合操作在客户端内部保持原子。
	// 所有写路径（Set/SetNX/Delete/Expire/MSet）与批量读（MGet）都参与该锁，
	// 单键读（Get/GetString/Exist/TTL）依赖 go-cache 自身的锁即可，不参与以免无谓争抢。
	// 注意：通过 GetInstance 取出底层 *cache.Cache 直接读写会绕过该锁
	mu sync.Mutex
	// lastCleanup 上次回收过期键的时间，由 mu 保护
	lastCleanup time.Time
	// cleanupInterval 过期键的回收间隔，默认 defaultCleanupInterval，测试中会调小
	cleanupInterval time.Duration
}

func (c *LocalClient) GetClient() *cache.Cache {
	return c.cache
}

func (c *LocalClient) GetConfig() *Config {
	return c.config
}

func (c *LocalClient) GetInstance() interface{} {
	return c.cache
}

// Close 关闭客户端
//
// 清空全部数据并释放资源。本地缓存没有需要停止的后台协程
// （go-cache 的 janitor 未启用，见 cleanupInterval），因此这里能确定性地完成释放
func (c *LocalClient) Close() error {
	logger := log.WithFields(c.config.LogFields())
	c.mu.Lock()
	defer c.mu.Unlock()
	// 持锁清空，避免 Flush 落在某次批量写入中间，留下清不干净的数据
	c.GetClient().Flush()
	logger.Info("close local cache client success")
	return nil
}

// Ping 探活
//
// 本地缓存没有连接概念，客户端存在即视为可用，恒返回 nil
func (c *LocalClient) Ping(_ context.Context) error {
	return nil
}

func (c *LocalClient) GetKey(key string) string {
	return c.GetConfig().GetKey(key)
}

func (c *LocalClient) Set(_ context.Context, key string, value any, expiration time.Duration) error {
	bytes, err := c.marshal.Marshal(value)
	if err != nil {
		return errorx.Wrap(err, "marshal value failed")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.set(c.GetKey(key), string(bytes), expiration)
	return nil
}

// sweep 按间隔回收过期键，调用方必须已持有 c.mu。
// 用于替代 go-cache 那个一旦启动就无法停止的 janitor
func (c *LocalClient) sweep() {
	if now := time.Now(); now.Sub(c.lastCleanup) >= c.cleanupInterval {
		c.lastCleanup = now
		c.GetClient().DeleteExpired()
	}
}

// set 写入缓存，key 需已带前缀，调用方必须已持有 c.mu
func (c *LocalClient) set(key, value string, expiration time.Duration) {
	c.sweep()
	if expiration == TTLNoExpire {
		// 对齐 Redis 的 KeepTTL：go-cache 没有该能力，只能先读原过期时间再按剩余时长重写。
		// 缺陷：锁保证了"读 + 写"的原子性，但重写仍会损失计算剩余时长与写入之间的耗时，
		// 与原 TTL 存在纳秒级漂移（Redis 端的 KEEPTTL 由服务端原样保留，无此问题）。
		// key 不存在，或原本就没有过期时间（go-cache 以零值 time.Time 表示）时按永不过期写入
		expiration = 0
		if _, old, ok := c.GetClient().GetWithExpiration(key); ok && !old.IsZero() {
			if expiration = time.Until(old); expiration <= 0 {
				// 读取到写入之间原 TTL 恰好走完：保留"随即过期"的原语义。
				// 若直接传非正数，go-cache 会当成永不过期，把这个 key 变成永久残留
				expiration = time.Nanosecond
			}
		}
	}
	c.GetClient().Set(key, value, expiration)
}

func (c *LocalClient) SetNX(_ context.Context, key string, value any, expiration time.Duration) bool {
	bytes, err := c.marshal.Marshal(value)
	if err != nil {
		log.WithFields(c.config.LogFields()).WithError(err).Error("marshal value failed")
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sweep()
	// Add 仅在 key 不存在（或已过期）时写入，与 Redis 的 SET NX 等价
	return c.GetClient().Add(c.GetKey(key), string(bytes), expiration) == nil
}

// CompareAndDelete 仅当值与缓存中一致时删除缓存
// 由 c.mu 保证「比对 + 删除」的原子性，与 Redis 端 Lua 脚本的语义一致
func (c *LocalClient) CompareAndDelete(_ context.Context, key string, value any) bool {
	bytes, err := c.marshal.Marshal(value)
	if err != nil {
		log.WithFields(c.config.LogFields()).WithError(err).Error("marshal value failed")
		return false
	}
	key = c.GetKey(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	result, ok := c.GetClient().Get(key)
	if !ok || c.valueString(key, result) != string(bytes) {
		return false
	}
	c.GetClient().Delete(key)
	return true
}

func (c *LocalClient) Get(ctx context.Context, key string, value any) bool {
	if result := c.GetString(ctx, key); result != "" {
		if err := c.marshal.Unmarshal([]byte(result), value); err == nil {
			return true
		}
	}
	return false
}

func (c *LocalClient) GetString(_ context.Context, key string) string {
	result, ok := c.GetClient().Get(c.GetKey(key))
	if !ok {
		return ""
	}
	return c.valueString(key, result)
}

// valueString 把底层缓存中的值转成字符串
func (c *LocalClient) valueString(key string, value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		// 兼容外部通过 GetInstance 直接写入的原始字节
		return string(v)
	default:
		// 其余类型不做转换，避免 fmt.Sprint 捏造出 "<nil>"、"[97 98]" 之类的假数据
		log.WithFields(c.config.LogFields()).WithField("key", key).
			Warnf("unsupported cached value type: %T", value)
		return ""
	}
}

// MSet 批量更新缓存
//
// 先完成全部序列化再统一写入，任一值序列化失败都不会留下部分写入；
// 写入过程由 c.mu 保护，其他协程不会观察到中间状态，与 Redis 端一致
func (c *LocalClient) MSet(_ context.Context, values map[string]any, expiration time.Duration) error {
	if len(values) == 0 {
		return nil
	}
	if expiration < 0 {
		// MSET 没有 KEEPTTL 选项，负数一律按永不过期处理，与 Redis 端一致
		expiration = 0
	}
	items := make(map[string]string, len(values))
	for key, value := range values {
		bytes, err := c.marshal.Marshal(value)
		if err != nil {
			return errorx.Wrap(err, "marshal value failed")
		}
		items[c.GetKey(key)] = string(bytes)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, value := range items {
		c.set(key, value, expiration)
	}
	return nil
}

// MGet 批量获取缓存
//
// 读取过程由 c.mu 保护，不会与其他写操作交错，得到的是一致快照（与 Redis 端的 MGET 一致）
func (c *LocalClient) MGet(_ context.Context, keys []string) map[string]string {
	if len(keys) == 0 {
		return nil // 与 Redis 端保持一致，空入参返回 nil 而非空 map
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	values := make(map[string]string, len(keys))
	for _, key := range keys {
		// 以取值是否命中判断而非判空，空字符串值也应出现在结果里（与 Redis 端一致）
		result, ok := c.GetClient().Get(c.GetKey(key))
		if !ok {
			continue
		}
		values[key] = c.valueString(key, result)
	}
	return values
}

// Delete 删除缓存
// 由 c.mu 保证「判断 + 删除」的原子性，与 Redis 端 DEL 返回删除数量的语义一致
func (c *LocalClient) Delete(_ context.Context, key string) bool {
	key = c.GetKey(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sweep()
	if _, ok := c.GetClient().Get(key); !ok {
		return false
	}
	c.GetClient().Delete(key)
	return true
}

func (c *LocalClient) Exist(_ context.Context, key string) bool {
	_, ok := c.GetClient().Get(c.GetKey(key))
	return ok
}

// Expire 续期缓存，返回 key 是否存在（与 Delete 的返回风格一致）
//
// go-cache 只能通过重新 Set 修改过期时间，但 c.mu 保证了「读取 + 写回」的原子性，
// 不会与并发的删除交错而复活已删除的 key（与 Redis 端 EXPIRE / PERSIST 的单命令原子一致）
func (c *LocalClient) Expire(_ context.Context, key string, expiration time.Duration) bool {
	key = c.GetKey(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sweep()
	result, ok := c.GetClient().Get(key)
	if !ok {
		return false
	}
	// 非正数表示永不过期，与 Set 保持一致
	c.GetClient().Set(key, result, expiration)
	return true
}

// TTL 获取缓存剩余有效期
//
// 与 Redis 的 TTL 命令保持一致：按秒取整，剩余不足 500ms 时返回 0。
// go-cache 的时间精度更高，这里主动降到秒级以对齐两端语义
func (c *LocalClient) TTL(_ context.Context, key string) time.Duration {
	_, expiration, ok := c.GetClient().GetWithExpiration(c.GetKey(key))
	if !ok {
		return TTLNotExist
	}
	if expiration.IsZero() {
		return TTLNoExpire
	}
	ttl := time.Until(expiration)
	if ttl <= 0 {
		return TTLNotExist
	}
	// Redis 内部按 (毫秒数+500)/1000 四舍五入到秒，这里对齐同一算法
	return time.Duration((ttl.Milliseconds()+500)/1000) * time.Second
}
