package cachex

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testClientOperations 校验各后端实现 Client 接口后行为一致
func testClientOperations(t *testing.T, client Client) {
	t.Helper()
	ctx := context.Background()

	// Ping（local 端恒为 nil，redis 端连通时也应成功）
	if err := client.Ping(ctx); err != nil {
		t.Errorf("Ping failed: %v", err)
	}

	// Set / Get / GetString
	if err := client.Set(ctx, "str", "hello", time.Minute); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	var str string
	if !client.Get(ctx, "str", &str) || str != "hello" {
		t.Errorf("Get = %q, want %q", str, "hello")
	}
	if client.GetString(ctx, "missing") != "" {
		t.Error("GetString on missing key should be empty")
	}

	// 结构体值
	type user struct{ Name string }
	if err := client.Set(ctx, "user", user{Name: "quan"}, time.Minute); err != nil {
		t.Fatalf("Set struct failed: %v", err)
	}
	var u user
	if !client.Get(ctx, "user", &u) || u.Name != "quan" {
		t.Errorf("Get struct = %+v, want {Name:quan}", u)
	}

	// 不存在的 key
	if client.Get(ctx, "missing", &str) {
		t.Error("Get on missing key should be false")
	}

	// Exist / Delete
	if !client.Exist(ctx, "str") {
		t.Error("Exist on existing key should be true")
	}
	if !client.Delete(ctx, "str") {
		t.Error("Delete on existing key should be true")
	}
	if client.Delete(ctx, "str") {
		t.Error("Delete on missing key should be false")
	}
	if client.Exist(ctx, "str") {
		t.Error("Exist after delete should be false")
	}

	// Expire / TTL
	if err := client.Set(ctx, "ttl", "v", 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if ttl := client.TTL(ctx, "ttl"); ttl != TTLNoExpire {
		t.Errorf("TTL = %v, want %v", ttl, TTLNoExpire)
	}
	if !client.Expire(ctx, "ttl", time.Minute) {
		t.Fatal("Expire on an existing key should be true")
	}
	if ttl := client.TTL(ctx, "ttl"); ttl <= 0 || ttl > time.Minute {
		t.Errorf("TTL = %v, want in (0, 1m]", ttl)
	}
	// TTL 按秒返回（如同 Redis 的 TTL 命令），本地缓存不能返回纳秒级的剩余时长
	if ttl := client.TTL(ctx, "ttl"); ttl%time.Second != 0 {
		t.Errorf("TTL = %v, want a whole-second value", ttl)
	}
	if ttl := client.TTL(ctx, "missing"); ttl != TTLNotExist {
		t.Errorf("TTL on missing key = %v, want %v", ttl, TTLNotExist)
	}
	// 续期传非正数表示取消过期时间（两端语义必须一致）
	if !client.Expire(ctx, "ttl", 0) {
		t.Fatal("Expire with zero duration on an existing key should be true")
	}
	if ttl := client.TTL(ctx, "ttl"); ttl != TTLNoExpire {
		t.Errorf("TTL after Expire(0) = %v, want %v", ttl, TTLNoExpire)
	}
	// 对已经永不过期的 key 再次 Expire(0)：Redis 的 PERSIST 此时同样返回 0，
	// 不能与"key 不存在"混为一谈，应当幂等成功并返回 true（两端语义必须一致）
	if !client.Expire(ctx, "ttl", 0) {
		t.Error("Expire(0) on a persistent key should be idempotent and return true")
	}
	if client.Expire(ctx, "missing", time.Minute) {
		t.Error("Expire on missing key should return false")
	}
	// Set 传 TTLNoExpire(-1) 等价 Redis 的 KeepTTL：更新值但保留原过期时间，两端必须一致
	if err := client.Set(ctx, "keep", "v", time.Hour); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	before := client.TTL(ctx, "keep")
	if err := client.Set(ctx, "keep", "v2", TTLNoExpire); err != nil {
		t.Fatalf("Set with KeepTTL failed: %v", err)
	}
	if after := client.TTL(ctx, "keep"); after <= 0 || after > before {
		t.Errorf("TTL after Set(-1) = %v, want in (0, %v]", after, before)
	}
	var kept string
	if !client.Get(ctx, "keep", &kept) || kept != "v2" {
		t.Errorf("value after Set(-1) = %q, want %q", kept, "v2")
	}
	// 原本就没有过期时间的 key，KeepTTL 更新后仍无过期时间
	if err := client.Set(ctx, "keep2", "v", 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if err := client.Set(ctx, "keep2", "v2", TTLNoExpire); err != nil {
		t.Fatalf("Set with KeepTTL failed: %v", err)
	}
	if ttl := client.TTL(ctx, "keep2"); ttl != TTLNoExpire {
		t.Errorf("TTL after Set(-1) on persistent key = %v, want %v", ttl, TTLNoExpire)
	}

	// SetNX
	if !client.SetNX(ctx, "lock", "1", time.Minute) {
		t.Error("SetNX on missing key should be true")
	}
	if client.SetNX(ctx, "lock", "2", time.Minute) {
		t.Error("SetNX on existing key should be false")
	}
	var lock string
	if !client.Get(ctx, "lock", &lock) || lock != "1" {
		t.Errorf("lock = %q, want %q", lock, "1")
	}

	// MSet / MGet
	if err := client.MSet(ctx, map[string]any{"a": "1", "b": "2"}, time.Minute); err != nil {
		t.Fatalf("MSet failed: %v", err)
	}
	if values := client.MGet(ctx, []string{"a", "b", "missing"}); len(values) != 2 {
		t.Errorf("MGet len = %d, want 2", len(values))
	}
	var a string
	if !client.Get(ctx, "a", &a) || a != "1" {
		t.Errorf("a = %q, want %q", a, "1")
	}
	if !client.Exist(ctx, "b") {
		t.Error("Exist on key written by MSet should be true")
	}
	// MSet 不带过期时间：Redis 端走原生 MSET 分支，写入的 key 应为永不过期
	if err := client.MSet(ctx, map[string]any{"c": "3"}, 0); err != nil {
		t.Fatalf("MSet with zero expiration failed: %v", err)
	}
	if ttl := client.TTL(ctx, "c"); ttl != TTLNoExpire {
		t.Errorf("TTL after MSet(0) = %v, want %v", ttl, TTLNoExpire)
	}
	// 空入参直接返回，不发命令
	if values := client.MGet(ctx, nil); values != nil {
		t.Errorf("MGet with empty keys = %v, want nil", values)
	}
	if err := client.MSet(ctx, nil, time.Minute); err != nil {
		t.Errorf("MSet with empty values failed: %v", err)
	}

	// CompareAndDelete：仅当值一致时才删除（安全解锁）
	if err := client.Set(ctx, "cad", "owner-1", time.Minute); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if client.CompareAndDelete(ctx, "cad", "owner-2") {
		t.Error("CompareAndDelete with mismatched value should be false")
	}
	if !client.Exist(ctx, "cad") {
		t.Error("key should survive a mismatched CompareAndDelete")
	}
	if !client.CompareAndDelete(ctx, "cad", "owner-1") {
		t.Error("CompareAndDelete with matched value should be true")
	}
	if client.Exist(ctx, "cad") {
		t.Error("key should be deleted after a matched CompareAndDelete")
	}
	if client.CompareAndDelete(ctx, "cad", "owner-1") {
		t.Error("CompareAndDelete on missing key should be false")
	}
}

// testClientConcurrency 校验并发场景下的原子性，这些语义两端必须一致
func testClientConcurrency(t *testing.T, client Client) {
	t.Helper()
	ctx := context.Background()

	// MSet 成对写入 + MGet 批量读取：MGet 是一致快照，不应读到只更新了一半的中间状态。
	// 这里必须用 0 过期时间：Redis 端 MSet 带正数 expiration 时会退化为 pipeline，
	// 本就不具备原子性（见 README「关于批量操作」），只有原生 MSET 的路径两端才可比
	t.Run("MGetSnapshot", func(t *testing.T) {
		var wg sync.WaitGroup
		var torn atomic.Bool
		done := make(chan struct{})

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(done)
			for i := 0; i < 200; i++ {
				value := strconv.Itoa(i)
				if err := client.MSet(ctx, map[string]any{"snap:a": value, "snap:b": value}, 0); err != nil {
					t.Errorf("MSet failed: %v", err)
					return
				}
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				values := client.MGet(ctx, []string{"snap:a", "snap:b"})
				// 只在两个 key 都已写入后判断，跳过写入开始前的空结果
				if len(values) == 2 && values["snap:a"] != values["snap:b"] {
					torn.Store(true)
					return
				}
			}
		}()

		wg.Wait()
		if torn.Load() {
			t.Error("MGet observed a torn state, the two keys should always hold the same value")
		}
	})

	// 并发删除同一个 key，每轮只应有一个协程删除成功。
	// 必须多轮 + 闸门同时起跑：临界区只有「判断 + 删除」两条指令，
	// 单轮且不加闸门时，缺少原子性的实现也可能因偶然串行而侥幸通过
	t.Run("DeleteOnce", func(t *testing.T) {
		assertOnlyOnce(t, 50, 16,
			func() { _ = client.Set(ctx, "del-once", "v", time.Minute) },
			func() bool { return client.Delete(ctx, "del-once") },
		)
	})

	// 并发安全解锁，每轮只应有一个协程比对成功并删除
	t.Run("CompareAndDeleteOnce", func(t *testing.T) {
		assertOnlyOnce(t, 50, 16,
			func() { _ = client.Set(ctx, "cad-once", "owner", time.Minute) },
			func() bool { return client.CompareAndDelete(ctx, "cad-once", "owner") },
		)
	})
}

// assertOnlyOnce 反复并发执行 fn，断言每一轮中恰好只有一个协程返回 true。
// prepare 在每轮开始前重置前置状态，所有协程用同一个闸门同时起跑以最大化交错概率。
//
// 注意：闸门只保证各协程"就绪"，不保证真正并行。当 GOMAXPROCS 为 1 时协程串行执行，
// 临界区又只有"判断 + 删除"两条指令，缺少原子性的实现也可能因恰好串行而侥幸通过，
// 该用例仅在多核下能可靠捕获——本地与 CI 都应保证 GOMAXPROCS 大于 1
func assertOnlyOnce(t *testing.T, rounds, workers int, prepare func(), fn func() bool) {
	t.Helper()
	var wg sync.WaitGroup
	for round := 0; round < rounds; round++ {
		prepare()
		var succeeded atomic.Int32
		start := make(chan struct{})
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if fn() {
					succeeded.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		if got := succeeded.Load(); got != 1 {
			t.Fatalf("round %d: %d goroutines succeeded, want exactly 1", round, got)
		}
	}
}
