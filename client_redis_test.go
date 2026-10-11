package cachex

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// TestParseRedisAddrs 地址解析与校验：空地址与缺少端口的地址都必须在建连前被拦下
func TestParseRedisAddrs(t *testing.T) {
	for _, c := range []struct {
		address string
		want    []string
		wantErr bool
	}{
		{address: "127.0.0.1:6379", want: []string{"127.0.0.1:6379"}},
		{address: "127.0.0.1:6379, 127.0.0.2:6379", want: []string{"127.0.0.1:6379", "127.0.0.2:6379"}},
		{address: "", wantErr: true},
		{address: "localhost", wantErr: true},  // default tag 填进来的形态，缺端口
		{address: "localhost,", wantErr: true}, // 只写了主机名
		{address: " , ", wantErr: true},        // 只有分隔符
	} {
		got, err := parseRedisAddrs(c.address)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseRedisAddrs(%q) should fail, got %v", c.address, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseRedisAddrs(%q) failed: %v", c.address, err)
			continue
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("parseRedisAddrs(%q) = %v, want %v", c.address, got, c.want)
		}
	}
}

func newRedisTestClient(t *testing.T, prefix string) *RedisClient {
	t.Helper()
	server := miniredis.RunT(t)
	client, err := NewRedisClient(&Config{
		Source:  "redis-test",
		Driver:  "redis",
		Address: server.Addr(),
		Prefix:  prefix,
		Marshal: "json",
	})
	if err != nil {
		t.Fatalf("create redis client failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestRedisClientOperations(t *testing.T) {
	client := newRedisTestClient(t, "")
	testClientOperations(t, client)
	testClientConcurrency(t, client)
}

func TestRedisClientPrefix(t *testing.T) {
	client := newRedisTestClient(t, "app:")
	if got := client.GetKey("k"); got != "app:k" {
		t.Errorf("GetKey = %q, want %q", got, "app:k")
	}
	ctx := context.Background()
	if err := client.Set(ctx, "k", "v", time.Minute); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	// Redis 中的 key 应带上前缀
	if client.GetClient().Exists(ctx, "app:k").Val() != 1 {
		t.Error("key with prefix not found in redis")
	}
}

// TestRedisClientDistributedLock SetNX + CompareAndDelete 构成完整的安全锁语义
// CompareAndDelete 走 Lua 脚本，本用例同时覆盖脚本在真实 Redis 协议下的执行路径
func TestRedisClientDistributedLock(t *testing.T) {
	client := newRedisTestClient(t, "")
	ctx := context.Background()

	// 加锁：仅当 key 不存在时写入
	if !client.SetNX(ctx, "lock", "owner-1", time.Minute) {
		t.Fatal("SetNX on missing key should acquire the lock")
	}
	// 锁被占用时其他持有者加锁失败
	if client.SetNX(ctx, "lock", "owner-2", time.Minute) {
		t.Error("SetNX should fail while the lock is held")
	}
	// 非持有者解锁失败，且锁必须仍然存在（Lua 保证比对与删除原子，不会误删）
	if client.CompareAndDelete(ctx, "lock", "owner-2") {
		t.Error("CompareAndDelete by a non-owner should be false")
	}
	if !client.Exist(ctx, "lock") {
		t.Error("lock should still be held after a failed unlock")
	}
	// 持有者解锁成功
	if !client.CompareAndDelete(ctx, "lock", "owner-1") {
		t.Error("CompareAndDelete by the owner should be true")
	}
	if client.Exist(ctx, "lock") {
		t.Error("lock should be released")
	}
	// 释放后可再次加锁，确认没有残留
	if !client.SetNX(ctx, "lock", "owner-3", time.Minute) {
		t.Error("lock should be acquirable again after release")
	}
}

// TestRedisClientPingFailed 显式探活能把"后端故障"暴露成错误，而其余方法只会静默返回 false，
// 这正是 Ping 存在的意义（创建时的 PING 失败不阻断初始化，调用方需要一个主动探活的入口）
func TestRedisClientPingFailed(t *testing.T) {
	server := miniredis.RunT(t)
	client, err := NewRedisClient(&Config{
		Source:  "redis-test",
		Driver:  "redis",
		Address: server.Addr(),
		Marshal: "json",
	})
	if err != nil {
		t.Fatalf("create redis client failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping on a healthy server failed: %v", err)
	}

	// 让服务端对所有命令返回错误，模拟后端故障
	server.SetError("MASTERDOWN simulated failure")
	if err := client.Ping(ctx); err == nil {
		t.Error("Ping should surface a backend failure as an error")
	}
	// 对照组：同样处于故障状态的 Exist 只会返回 false，无法与"key 不存在"区分
	if client.Exist(ctx, "any") {
		t.Error("Exist should be false when the backend is down")
	}
}

// TestRedisClientModeInvalid 非法模式应返回错误
func TestRedisClientModeInvalid(t *testing.T) {
	if _, err := NewRedisClient(&Config{Source: "bad", Driver: "redis", Address: "127.0.0.1:6379", Mode: 9}); err == nil {
		t.Error("expected error for invalid redis mode")
	}
}

// TestNewClientUnregisteredDriver 未注册的驱动应返回错误
func TestNewClientUnregisteredDriver(t *testing.T) {
	if _, err := NewClient(&Config{Driver: "not-registered"}); err == nil {
		t.Error("expected error for unregistered driver")
	}
}

// TestGetClientFallback 未指定源名时回退 default 再到池中第一个；显式指定源名时只回退 default
func TestGetClientFallback(t *testing.T) {
	saved := pool
	pool = nil
	t.Cleanup(func() { pool = saved })

	client := newLocalTestClient(t, "")
	AddClient("session", client)

	// 未指定源名：default 不存在，回退池中第一个
	if got := GetClient(); got != client {
		t.Error("GetClient() should fall back to the first client when default is absent")
	}
	// 显式指定未知源：不回退到池中其他源，避免拼写错误时读写到错误的缓存
	if got := GetClient("unknown"); got != nil {
		t.Error("GetClient(unknown) should return nil when neither unknown nor default exists")
	}
	// 客户端缺失时 GetConfig 返回 nil 而不是 panic
	if got := GetConfig("unknown"); got != nil {
		t.Error("GetConfig(unknown) should be nil when no client resolves")
	}
	if got := GetClient("session"); got != client {
		t.Error("GetClient(session) should return the session client")
	}

	defaultClient := newLocalTestClient(t, "")
	AddClient("default", defaultClient)
	if got := GetClient(); got != defaultClient {
		t.Error("GetClient() should return the default client when it exists")
	}
	// 指定未知源时回退 default，而不是池中第一个
	if got := GetClient("unknown"); got != defaultClient {
		t.Error("GetClient(unknown) should fall back to the default client")
	}
	if got := GetClient("session"); got != client {
		t.Error("GetClient(session) should still return the session client")
	}
}
