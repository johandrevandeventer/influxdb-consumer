package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/johandrevandeventer/influxdb-consumer/cmd"
	"github.com/johandrevandeventer/influxdb-consumer/initializers"
	"github.com/johandrevandeventer/influxdb-consumer/internal/config"
	"github.com/johandrevandeventer/influxdb-consumer/internal/engine"
	coreutils "github.com/johandrevandeventer/influxdb-consumer/utils"
	"github.com/johandrevandeventer/logging"
	"github.com/johandrevandeventer/splashscreen"
	"github.com/johandrevandeventer/textutils"
	"go.uber.org/zap"
)

func main() {
	splashscreen.PrintSplashScreen()

	cmd.Execute()

	// Initialize the environment
	coreutils.VerbosePrintln(textutils.ColorText(textutils.Green, "Loading environment variables..."))
	err := initializers.LoadEnvVariable()
	if err != nil {
		fmt.Println(textutils.ColorText(textutils.Red, err.Error()))
		return
	}
	coreutils.VerbosePrintln(textutils.ColorText(textutils.Cyan, "-> Environment variables loaded"))

	// Initialize the configuration
	coreutils.VerbosePrintln(textutils.ColorText(textutils.Green, "Initializing configuration files..."))
	err = initializers.InitConfig()
	if err != nil {
		fmt.Println(textutils.ColorText(textutils.Red, err.Error()))
		return
	}

	// Initialize the logger
	coreutils.VerbosePrintln(textutils.ColorText(textutils.Green, "Initializing Logger..."))
	cfg := config.GetConfig()
	initializers.InitLogger(cfg)
	logger := logging.GetLogger("main")
	coreutils.VerbosePrintln(textutils.ColorText(textutils.Cyan, "-> Logger initialized"))

	// Initialize the state persistence
	coreutils.VerbosePrintln(textutils.ColorText(textutils.Green, "Initializing state persistence..."))
	statePersister, err := initializers.InitPersist(cfg)
	if err != nil {
		fmt.Println(textutils.ColorText(textutils.Red, err.Error()))
		return
	}
	coreutils.VerbosePrintln(textutils.ColorText(textutils.Cyan, "-> State persistence initialized"))

	coreutils.VerbosePrintln("")

	// Graceful shutdown handling
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	// Create the engine
	engine := engine.NewEngine(ctx, cfg, logger, statePersister)

	// Recover from panics
	defer func() {
		if r := recover(); r != nil {
			logger.Error("recovered from panic", zap.Any("panic", r))
		}
	}()

	// Run the engine
	engine.Run()
}
