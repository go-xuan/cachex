package cachex

import (
	"github.com/go-xuan/configx"
	log "github.com/sirupsen/logrus"
)

func init() {
	RegisterClientBuilder("local", LocalClientBuilder) // 注册本地缓存客户端构建器
	RegisterClientBuilder("redis", RedisClientBuilder) // 注册redis缓存客户端构建器
	Init()                                             // 初始化缓存
}

func Init() {
	logger := log.WithField("package", "cachex")
	if err := configx.LoadConfigurator(&Configs{}); err == nil && Initialized() {
		logger.Info("initialized success")
		return
	}
	if err := configx.LoadConfigurator(&Config{}); err == nil && Initialized() {
		logger.Info("initialized success")
		return
	}
	logger.Warn("initialized failed")
}
