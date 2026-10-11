package cachex

import (
	"context"
	"testing"
	"time"

	"github.com/patrickmn/go-cache"
)

func newLocalTestClient(t *testing.T, prefix string) Client {
	t.Helper()
	client, err := NewLocalClient(&Config{Source: "local-test", Driver: "local", Prefix: prefix})
	if err != nil {
		t.Fatalf("create local client failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestLocalClientOperations(t *testing.T) {
	client := newLocalTestClient(t, "")
	testClientOperations(t, client)
	testClientConcurrency(t, client)
}

func TestLocalClientPrefix(t *testing.T) {
	client := newLocalTestClient(t, "app:")
	if got := client.GetKey("k"); got != "app:k" {
		t.Errorf("GetKey = %q, want %q", got, "app:k")
	}
	ctx := context.Background()
	if err := client.Set(ctx, "k", "v", time.Minute); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	// 底层实例中的 key 应带上前缀
	if _, ok := client.GetInstance().(*cache.Cache).Get("app:k"); !ok {
		t.Error("key with prefix not found in underlying cache")
	}
}

// TestLocalClientGetStringNonStringValue 外部通过 GetInstance 写入非字符串值时的处理
func TestLocalClientGetStringNonStringValue(t *testing.T) {
	client := newLocalTestClient(t, "")
	instance := client.GetInstance().(*cache.Cache)
	ctx := context.Background()

	// []byte 按原始字节返回
	instance.Set("raw", []byte("ab"), time.Minute)
	if got := client.GetString(ctx, "raw"); got != "ab" {
		t.Errorf("GetString = %q, want %q", got, "ab")
	}

	// 其他类型不做转换，视为不存在，避免捏造 "<nil>" 之类的假数据
	instance.Set("num", 123, time.Minute)
	if got := client.GetString(ctx, "num"); got != "" {
		t.Errorf("GetString on unsupported type = %q, want empty", got)
	}
}

// TestLocalClientSweepExpired 未启用 go-cache 的 janitor 后，过期键仍由写路径主动回收
//
// 用 ItemCount 而非 Get 检查底层：Get 会对过期项做惰性删除，检查动作本身就会改变待观察的状态
func TestLocalClientSweepExpired(t *testing.T) {
	client := newLocalTestClient(t, "").(*LocalClient)
	instance := client.GetInstance().(*cache.Cache)
	ctx := context.Background()
	client.cleanupInterval = time.Millisecond // 缩短间隔，否则要等满一分钟

	if err := client.Set(ctx, "expired", "v", 50*time.Millisecond); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	// 前置条件：过期的键还留在底层，go-cache 只在被访问时才惰性删除
	if count := instance.ItemCount(); count != 1 {
		t.Fatalf("precondition: ItemCount = %d, want 1 (the expired item should still linger)", count)
	}

	// 一次写路径触发 sweep，过期的键应被回收，只剩本次写入的键
	if err := client.Set(ctx, "fresh", "v", 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if count := instance.ItemCount(); count != 1 {
		t.Errorf("ItemCount = %d, want 1 (the expired item should have been reclaimed)", count)
	}
}

func TestLocalClientExpired(t *testing.T) {
	client := newLocalTestClient(t, "")
	ctx := context.Background()

	// TTL 与 Redis 的 TTL 命令一样按秒取整，剩余不足 500ms 时返回 0。
	// 先设 600ms 再等 200ms，把剩余时间压到 500ms 以内：
	// 这样断言成立的窗口有整整 500ms（从第 100ms 到第 600ms），比直接设 400ms 后立即读更抗调度抖动
	if err := client.Set(ctx, "k", "v", 600*time.Millisecond); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if ttl := client.TTL(ctx, "k"); ttl != 0 {
		t.Errorf("TTL = %v, want 0 when under 500ms remains", ttl)
	}

	time.Sleep(500 * time.Millisecond)
	if client.Exist(ctx, "k") {
		t.Error("Exist on expired key should be false")
	}
	if ttl := client.TTL(ctx, "k"); ttl != TTLNotExist {
		t.Errorf("TTL on expired key = %v, want %v", ttl, TTLNotExist)
	}
}
