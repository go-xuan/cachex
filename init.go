package cachex

import (
	"github.com/go-xuan/configx"
	"github.com/go-xuan/utilx/errorx"
	log "github.com/sirupsen/logrus"
)

func init() {
	RegisterClientBuilder("local", LocalClientBuilder) // 注册本地缓存客户端构建器
	RegisterClientBuilder("redis", RedisClientBuilder) // 注册redis缓存客户端构建器
}

func Initialize() error {
	logger := log.WithField("package", "cachex")
	if err := configx.LoadConfigurator(&Configs{}); err == nil && Initialized() {
		logger.Info("initialize success")
		return nil
	}
	if err := configx.LoadConfigurator(&Config{}); err == nil && Initialized() {
		logger.Info("initialize success")
		return nil
	}
	logger.Warn("initialize failed")
	return errorx.New("initialize cachex failed")
}
