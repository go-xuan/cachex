package cachex

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/go-xuan/utilx/errorx"
	"github.com/go-xuan/utilx/marshalx"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

const (
	ModeStandAlone = iota // 单机
	ModeCluster           // 集群
	ModeSentinel          // 哨兵
)

// compareAndDeleteScript 安全解锁脚本：仅当缓存值与入参一致时才删除 key
// 由 Lua 保证"比对 + 删除"的原子性，避免删掉已被其他持有者重新写入的锁
var compareAndDeleteScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`)

// RedisClientBuilder redis缓存客户端构建器
func RedisClientBuilder(config *Config) (Client, error) {
	return NewRedisClient(config)
}

// NewRedisClient 创建redis缓存客户端
func NewRedisClient(config *Config) (*RedisClient, error) {
	client, err := NewRedisUniversalClient(config)
	if err != nil {
		return nil, errorx.Wrap(err, "create redis universal client failed")
	}
	return &RedisClient{
		config:  config,
		client:  client,
		marshal: marshalx.Apply(config.Marshal),
	}, nil
}

// parseRedisAddrs 解析并校验 redis 地址列表，多个地址以英文逗号分隔
//
// 必须在这里拦截空地址与缺少端口的地址：创建时的 PING 失败只记录日志不阻断
// （见下方注释），若放行，漏配 address 会得到一个"初始化成功但永远连不上"的客户端，
// 问题要推迟到首次读写才暴露
func parseRedisAddrs(address string) ([]string, error) {
	// 去除地址中的空格，避免 "host:port, host:port" 形式的配置连接失败
	var addrs []string
	for addr := range strings.SplitSeq(strings.ReplaceAll(address, " ", ""), ",") {
		if addr == "" {
			continue
		}
		if !strings.Contains(addr, ":") {
			return nil, errorx.Sprintf("redis address %q is invalid, expected host:port", addr)
		}
		addrs = append(addrs, addr)
	}
	if len(addrs) == 0 {
		return nil, errorx.New("redis address is required")
	}
	return addrs, nil
}

// NewRedisUniversalClient 创建redis universal客户端
func NewRedisUniversalClient(config *Config) (redis.UniversalClient, error) {
	addrs, err := parseRedisAddrs(config.Address)
	if err != nil {
		return nil, err
	}
	opts := &redis.UniversalOptions{
		Addrs:      addrs,
		ClientName: config.Source,
		Username:   config.Username,
		Password:   config.Password,
		PoolSize:   config.PoolSize,
		DB:         config.Database,
	}
	var client redis.UniversalClient
	switch config.Mode {
	case ModeStandAlone:
		client = redis.NewClient(opts.Simple())
	case ModeCluster:
		client = redis.NewClusterClient(opts.Cluster())
	case ModeSentinel:
		opts.MasterName = config.Master
		client = redis.NewFailoverClient(opts.Failover())
	default:
		return nil, errorx.New("redis mode is invalid")
	}
	if result, err := client.Ping(context.TODO()).Result(); err != nil || result != "PONG" {
		log.WithFields(config.LogFields()).
			WithField("ping_result", result).
			WithError(err).
			Error("ping redis failed")
	}
	return client, nil
}

// RedisClient redis缓存客户端
type RedisClient struct {
	config  *Config
	marshal marshalx.Marshal
	client  redis.UniversalClient
}

func (c *RedisClient) GetClient() redis.UniversalClient {
	return c.client
}

func (c *RedisClient) GetConfig() *Config {
	return c.config
}

func (c *RedisClient) GetInstance() any {
	return c.client
}

func (c *RedisClient) Close() error {
	logger := log.WithFields(c.config.LogFields())
	if err := c.GetClient().Close(); err != nil {
		logger.WithError(err).Error("close redis universal client failed")
		return errorx.Wrap(err, "close redis universal client failed")
	}
	logger.Info("close redis cache client success")
	return nil
}

// Ping 探活
//
// 客户端创建时的 PING 只记录日志、不阻断初始化（缓存通常不是硬依赖），
// 需要确认连通性时由调用方显式调用本方法
func (c *RedisClient) Ping(ctx context.Context) error {
	if err := c.GetClient().Ping(ctx).Err(); err != nil {
		return errorx.Wrap(err, "ping redis failed")
	}
	return nil
}

func (c *RedisClient) GetKey(key string) string {
	return c.GetConfig().GetKey(key)
}

func (c *RedisClient) Set(ctx context.Context, key string, value any, expiration time.Duration) error {
	bytes, err := c.marshal.Marshal(value)
	if err != nil {
		return errorx.Wrap(err, "marshal value failed")
	}
	if expiration != redis.KeepTTL && expiration < 0 {
		// 保留 -1（KeepTTL）的 Redis 原生语义；其余负数归一化为永不过期。
		// go-redis 对这两种情况的处理并不一致：Set 会静默丢弃过期参数（结果同永不过期），
		// SetNX 却会拼出 SET k v EX -N NX 被服务端以 ERR invalid expire time 拒绝，统一归零消除差异
		expiration = 0
	}
	if err = c.GetClient().Set(ctx, c.GetKey(key), bytes, expiration).Err(); err != nil {
		return errorx.Wrap(err, "set value failed")
	}
	return nil
}

func (c *RedisClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) bool {
	bytes, err := c.marshal.Marshal(value)
	if err != nil {
		log.WithFields(c.config.LogFields()).WithError(err).Error("marshal value failed")
		return false
	}
	if expiration < 0 {
		expiration = 0 // 非正数表示永不过期；否则会拼出 SET k v EX -N NX 被服务端拒绝
	}
	// 不能使用 Val()：它会吞掉连接超时、集群重定向失败等错误，
	// 使调用方把后端故障误判为"key 已存在"
	result, err := c.GetClient().SetNX(ctx, c.GetKey(key), bytes, expiration).Result()
	if err != nil {
		log.WithFields(c.config.LogFields()).WithError(err).Error("setnx value failed")
		return false
	}
	return result
}

func (c *RedisClient) CompareAndDelete(ctx context.Context, key string, value any) bool {
	bytes, err := c.marshal.Marshal(value)
	if err != nil {
		log.WithFields(c.config.LogFields()).WithError(err).Error("marshal value failed")
		return false
	}
	// Run 优先走 EVALSHA，脚本未缓存时自动回退 EVAL
	result, err := compareAndDeleteScript.Run(ctx, c.GetClient(), []string{c.GetKey(key)}, string(bytes)).Int64()
	if err != nil {
		log.WithFields(c.config.LogFields()).WithError(err).Error("compare and delete value failed")
		return false
	}
	return result > 0
}

func (c *RedisClient) Get(ctx context.Context, key string, value any) bool {
	if result := c.GetString(ctx, key); result != "" {
		if err := c.marshal.Unmarshal([]byte(result), value); err == nil {
			return true
		}
	}
	return false
}

func (c *RedisClient) GetString(ctx context.Context, key string) string {
	if result, err := c.GetClient().Get(ctx, c.GetKey(key)).Result(); err == nil {
		return result
	}
	return ""
}

func (c *RedisClient) MSet(ctx context.Context, values map[string]any, expiration time.Duration) error {
	if len(values) == 0 {
		return nil
	}
	pairs := make([]any, 0, len(values)*2)
	for key, value := range values {
		bytes, err := c.marshal.Marshal(value)
		if err != nil {
			return errorx.Wrap(err, "marshal value failed")
		}
		pairs = append(pairs, c.GetKey(key), bytes)
	}
	if expiration < 0 {
		expiration = 0 // 非正数表示永不过期
	}
	if expiration == 0 && c.config.Mode != ModeCluster {
		// 单机/哨兵且无过期时间：使用原生 MSET，单命令且原子
		if err := c.GetClient().MSet(ctx, pairs...).Err(); err != nil {
			return errorx.Wrap(err, "mset value failed")
		}
		return nil
	}
	// MSET 不支持过期时间；集群模式下多 key 跨 slot 会返回 CROSSSLOT，改用 pipeline 逐条写入
	pipe := c.GetClient().Pipeline()
	for i := 0; i < len(pairs); i += 2 {
		pipe.Set(ctx, pairs[i].(string), pairs[i+1], expiration)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return errorx.Wrap(err, "mset value failed")
	}
	return nil
}

func (c *RedisClient) MGet(ctx context.Context, keys []string) map[string]string {
	if len(keys) == 0 {
		return nil
	}
	values := make(map[string]string, len(keys))
	if c.config.Mode != ModeCluster {
		// 单机/哨兵：使用原生 MGET，单命令往返
		result, err := c.GetClient().MGet(ctx, c.GetConfig().GetKeys(keys)...).Result()
		if err != nil {
			log.WithFields(c.config.LogFields()).WithError(err).Error("mget value failed")
			return nil
		}
		for i, key := range keys {
			if i >= len(result) {
				break
			}
			if value, ok := result[i].(string); ok {
				values[key] = value
			}
		}
		return values
	}
	// 集群：MGET 多 key 跨 slot 会返回 CROSSSLOT，改用 pipeline 逐条读取
	cmds := make([]*redis.StringCmd, len(keys))
	pipe := c.GetClient().Pipeline()
	for i, key := range keys {
		cmds[i] = pipe.Get(ctx, c.GetKey(key))
	}
	// key 不存在时 Exec 会返回 redis.Nil，属正常情况，不视为失败
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		log.WithFields(c.config.LogFields()).WithError(err).Error("mget value failed")
		return nil
	}
	for i, key := range keys {
		if value, err := cmds[i].Result(); err == nil {
			values[key] = value
		}
	}
	return values
}

func (c *RedisClient) Delete(ctx context.Context, key string) bool {
	if result, err := c.GetClient().Del(ctx, c.GetKey(key)).Result(); err == nil {
		return result > 0
	}
	return false
}

func (c *RedisClient) Exist(ctx context.Context, key string) bool {
	if result, err := c.GetClient().Exists(ctx, c.GetKey(key)).Result(); err == nil {
		return result > 0
	}
	return false
}

// Expire 续期缓存，返回 key 是否存在（与 Delete 的返回风格一致）
func (c *RedisClient) Expire(ctx context.Context, key string, expiration time.Duration) bool {
	if expiration <= 0 {
		// 与 Set 保持一致：非正数表示永不过期（Redis 的 EXPIRE 传非正数会直接删除 key）
		result, err := c.GetClient().Persist(ctx, c.GetKey(key)).Result()
		if err != nil {
			log.WithFields(c.config.LogFields()).WithField("key", key).WithError(err).Error("redis persist failed")
			return false
		}
		if result {
			return true
		}
		// PERSIST 返回 0 有两种情况：key 不存在，或 key 本来就没有过期时间。
		// 后者已达成目标状态，属幂等成功（本地缓存端在这两种情况下都返回 true），
		// 只有确实不存在才算未生效，故再查一次加以区分
		return c.Exist(ctx, key)
	}
	result, err := c.GetClient().Expire(ctx, c.GetKey(key), expiration).Result()
	if err != nil {
		log.WithFields(c.config.LogFields()).WithField("key", key).WithError(err).Error("redis expire failed")
		return false
	}
	return result
}

func (c *RedisClient) TTL(ctx context.Context, key string) time.Duration {
	result, err := c.GetClient().TTL(ctx, c.GetKey(key)).Result()
	if err != nil {
		log.WithFields(c.config.LogFields()).WithField("key", key).WithError(err).Error("ttl value failed")
		return TTLNotExist
	}
	return result
}
