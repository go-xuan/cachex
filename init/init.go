package init

import (
	"github.com/go-xuan/cachex"
	
	"github.com/go-xuan/configx"
	"github.com/go-xuan/utilx/errorx"
)

func init() {
	errorx.Panic(Init())
}

func Init() error {
	var err error
	if err = configx.LoadConfigurator(&cachex.Configs{}); err == nil && cachex.Initialized() {
		return nil
	} else if err = configx.LoadConfigurator(&cachex.Config{}); err == nil && cachex.Initialized() {
		return nil
	}
	return errorx.Wrap(err, "init cache failed")
}
