package initializers

import (
	"github.com/johandrevandeventer/influxdb-consumer/internal/config"
	"github.com/johandrevandeventer/influxdb-consumer/internal/flags"
	"github.com/johandrevandeventer/logging"
)

// InitLogger configures the logger based on the app config
func InitLogger(cfg *config.Config) {
	// Create a new logging config with values from the app config

	var logPrefix bool

	if !flags.FlagLogPrefix || !cfg.App.Logging.AddTime {
		logPrefix = false
	} else {
		logPrefix = true
	}

	loggingConfig := logging.NewLoggingConfig(
		cfg.App.Logging.Level,
		cfg.App.Logging.FilePath,
		cfg.App.Logging.MaxSize,
		cfg.App.Logging.MaxBackups,
		cfg.App.Logging.MaxAge,
		cfg.App.Logging.Compress,
		flags.FlagDebugMode,
		logPrefix,
	)

	// Get a new logger based on the config
	_ = logging.NewLogger(loggingConfig)
}
