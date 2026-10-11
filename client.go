package cachex

import (
	"context"
	"errors"
	"time"

	"github.com/go-xuan/typex"
	"github.com/go-xuan/utilx/errorx"
	"github.com/redis/go-redis/v9"
)

var (
	pool     *typex.Enum[string, Client]        // 客户端池
	builders *typex.Enum[string, ClientBuilder] // 客户端构造函数池
)

// RegisterClientBuilder 注册客户端构造函数
func RegisterClientBuilder(name string, builder ClientBuilder) {
	if builders == nil {
		builders = typex.NewStringEnum[ClientBuilder]()
	}
	builders.Add(name, builder)
}

// NewClient 创建客户端
func NewClient(config *Config) (Client, error) {
	if config.Driver == "" {
		return LocalClientBuilder(config)
	}
	if builders != nil {
		if builder, ok := builders.Find(config.Driver); ok && builder != nil {
			return builder(config)
		}
	}
	return nil, errorx.Sprintf("client driver is not registered: %s", config.Driver)
}

// ClientBuilder 客户端构造函数
type ClientBuilder func(*Config) (Client, error)

const (
	TTLNoExpire = time.Duration(-1) // 键存在，但没有设置过期时间
	TTLNotExist = time.Duration(-2) // 键不存在
)

// Client 缓存客户端接口
//
// 只有 Set/MSet 返回 error：它们可能因序列化失败而失败，属编程错误；
// 其余方法返回 bool 表示"操作是否生效"，序列化失败或后端故障时同样返回 false，
// 具体原因见日志；需要严格区分业务结果与故障时，可通过 GetInstance 获取底层客户端自行处理。
type Client interface {
	GetInstance() any               // 获取实例
	GetConfig() *Config             // 获取配置
	Close() error                   // 关闭客户端,
	Ping(ctx context.Context) error // 探活，本地缓存恒为 nil

	GetKey(key string) string // 获取缓存key

	Set(ctx context.Context, key string, value any, expiration time.Duration) error  // 更新缓存
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) bool // 仅当key不存在时更新缓存（可用于实现分布式锁）
	CompareAndDelete(ctx context.Context, key string, value any) bool                // 仅当值一致时删除缓存（安全解锁，与SetNX配对使用）
	Get(ctx context.Context, key string, value any) bool                             // 获取缓存（指针，任意类型）
	GetString(ctx context.Context, key string) string                                // 获取缓存（字符串类型）
	MSet(ctx context.Context, values map[string]any, expiration time.Duration) error // 批量更新缓存
	MGet(ctx context.Context, keys []string) map[string]string                       // 批量获取缓存（字符串类型）
	Expire(ctx context.Context, key string, expiration time.Duration) bool           // 续期缓存（仅当key存在）
	TTL(ctx context.Context, key string) time.Duration                               // 获取缓存剩余有效期（TTLNoExpire/TTLNotExist，成功时按秒返回）
	Delete(ctx context.Context, key string) bool                                     // 删除缓存
	Exist(ctx context.Context, key string) bool                                      // 是否存在缓存
}

// Pool 获取客户端池
func Pool() *typex.Enum[string, Client] {
	if !Initialized() {
		panic("client pool not initialized")
	}
	return pool
}

// Initialized 是否初始化
func Initialized() bool {
	return pool != nil && pool.Len() > 0
}

// AddClient 添加客户端
func AddClient(source string, client Client) {
	if client == nil {
		return
	}
	if pool == nil {
		pool = typex.NewStringEnum[Client]()
	}
	pool.Add(source, client)
}

// GetClient 获取客户端
//   - 显式指定源名：命中则返回，未命中回退 default；两者都没有时返回 nil，
//     不再兜底到池中其他源，避免源名拼写错误时静默读写到错误的缓存
//   - 未指定源名：回退 default，再回退池中第一个客户端
func GetClient(source ...string) Client {
	if len(source) > 0 && source[0] != "" {
		if client := Pool().Get(source[0]); client != nil {
			return client
		}
		return Pool().Get("default")
	}
	if client := Pool().Get("default"); client != nil {
		return client
	}
	if clients := Pool().Values(); len(clients) > 0 {
		return clients[0]
	}
	return nil
}

// GetConfig 获取配置
//
// 客户端不存在时返回 nil，不 panic（GetClient 在源名与 default 均未命中时会返回 nil）
func GetConfig(source ...string) *Config {
	if client := GetClient(source...); client != nil {
		return client.GetConfig()
	}
	return nil
}

// GetInstance 获取实例
func GetInstance[T any](source ...string) T {
	client := GetClient(source...)
	if client == nil {
		panic("cache client not found")
	}
	instance, ok := client.GetInstance().(T)
	if !ok {
		panic("unexpected instance type")
	}
	return instance
}

// GetRedisUniversalClient 获取客户端
func GetRedisUniversalClient(source ...string) redis.UniversalClient {
	return GetInstance[redis.UniversalClient](source...)
}

// Close 关闭所有缓存客户端
func Close() error {
	var errs []error
	Pool().Range(func(source string, client Client) bool {
		if err := client.Close(); err != nil {
			errs = append(errs, errorx.Wrap(err, "close cache client failed"))
		}
		return true
	})
	return errors.Join(errs...)
}
