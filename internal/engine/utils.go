package engine

import (
	"github.com/johandrevandeventer/influxdb-consumer/internal/flags"
	"go.uber.org/zap"
)

func (e *Engine) verboseDebug(msg string, fields ...zap.Field) {
	if flags.FlagVerbose {
		e.logger.Debug(msg, fields...)
	}
}

func (e *Engine) verboseInfo(msg string, fields ...zap.Field) {
	if flags.FlagVerbose {
		e.logger.Info(msg, fields...)
	}
}
