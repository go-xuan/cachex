package cachex

import "testing"

func TestConfigCopy(t *testing.T) {
	config := &Config{
		Source:   "default",
		Driver:   "redis",
		Enable:   true,
		Address:  "127.0.0.1:6379",
		Username: "user",
		Password: "pass",
		Database: 1,
		Prefix:   "app:",
		Mode:     ModeCluster,
		Master:   "mymaster",
		PoolSize: 100,
		Marshal:  "json",
	}
	if copied := config.Copy(); *copied != *config {
		t.Errorf("Copy() = %+v, want %+v", copied, config)
	}
}

func TestConfigValid(t *testing.T) {
	// 本地缓存驱动可以不配置 address，此时配置依然有效
	if config := (&Config{Source: "local", Driver: "local", Enable: true}); !config.Valid() {
		t.Error("config with local driver and no address should be valid")
	}
	if config := (&Config{Source: "default", Address: "localhost:6379"}); !config.Valid() {
		t.Error("config with address should be valid")
	}
	// 完全空的配置视为无效
	if config := (&Config{}); config.Valid() {
		t.Error("empty config should be invalid")
	}
}

func TestConfigGetKeys(t *testing.T) {
	config := &Config{Prefix: "app:"}
	if got := config.GetKey("k"); got != "app:k" {
		t.Errorf("GetKey = %q, want %q", got, "app:k")
	}
	// GetKeys 与 GetKey 的前缀规则必须一致，否则写入与批量读取的 key 对不上
	keys := config.GetKeys([]string{"a", "b"})
	if len(keys) != 2 {
		t.Fatalf("GetKeys len = %d, want 2", len(keys))
	}
	for i, key := range []string{"a", "b"} {
		if want := config.GetKey(key); keys[i] != want {
			t.Errorf("GetKeys[%d] = %q, want %q", i, keys[i], want)
		}
	}
	// 无前缀时原样返回
	if got := (&Config{}).GetKeys([]string{"a"}); len(got) != 1 || got[0] != "a" {
		t.Errorf("GetKeys without prefix = %v, want [a]", got)
	}
}
