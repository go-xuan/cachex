package cachex

import (
	"os"
	"testing"
)

// 单配置格式 + local 驱动 + 不写 address 时应能初始化成功
// （回归：Config.Valid 曾只判断 Address，导致本地缓存配置被判为无效）
func TestInitializeLocalConfigWithoutAddress(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.MkdirAll("conf", 0o755); err != nil {
		t.Fatal(err)
	}
	content := "source: \"local\"\ndriver: \"local\"\nenable: true\n"
	if err := os.WriteFile("conf/cache.yaml", []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	saved := pool
	pool = nil
	t.Cleanup(func() { pool = saved })

	if err := Initialize(); err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if !Initialized() {
		t.Fatal("client pool should be initialized")
	}
	if client := GetClient(); client == nil {
		t.Fatal("GetClient should not be nil")
	} else {
		config := client.GetConfig()
		if config.Driver != "local" {
			t.Errorf("driver = %q, want local", config.Driver)
		}
		// Config.Valid 现在只要读到 driver 就成立，file reader 之后读取即停止，TagReader 不会执行，
		// 因此 address 应保持为空。若 Valid 退回只判断 Address，TagReader 会把它填成 "localhost"，
		// 使这条断言失败——这正是它要锁住的回归
		if config.Address != "" {
			t.Errorf("address = %q, want empty (TagReader should not have run)", config.Address)
		}
	}
}

// redis 驱动漏配 address 属于配置错误，必须在初始化阶段就失败。
// 回归：Config.Valid 曾对 redis 要求 address，判定无效后放行 TagReader，
// 把 address 填成 default tag 的 "localhost"（无端口），
// 于是"初始化成功"但客户端永远连不上，问题推迟到首次读写才暴露
func TestInitializeRedisConfigWithoutAddress(t *testing.T) {
	t.Chdir(t.TempDir())
	if err := os.MkdirAll("conf", 0o755); err != nil {
		t.Fatal(err)
	}
	content := "source: \"default\"\ndriver: \"redis\"\nenable: true\n"
	if err := os.WriteFile("conf/cache.yaml", []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	saved := pool
	pool = nil
	t.Cleanup(func() { pool = saved })

	if err := Initialize(); err == nil {
		t.Fatal("Initialize should fail when the redis address is missing")
	}
	if Initialized() {
		t.Error("no client should be registered")
	}
}
