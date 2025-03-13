package config

import (
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/johandrevandeventer/influxdb-consumer/internal/config/app"
	"github.com/johandrevandeventer/influxdb-consumer/internal/config/system"
	"github.com/johandrevandeventer/influxdb-consumer/internal/flags"
	coreutils "github.com/johandrevandeventer/influxdb-consumer/utils"
	"github.com/johandrevandeventer/textutils"
)

var (
	appConfigFilePath    = filepath.Join(coreutils.GetConfigDir(), "app.yaml")
	systemConfigFilePath = filepath.Join(coreutils.GetConfigDir(), "system.yaml")
)

type Config struct {
	System *system.SystemConfig `mapstructure:"system" yaml:"system"`
	App    *app.AppConfig       `mapstructure:"app" yaml:"app"`
}

// InitConfig initializes the system and application configuration files
func InitConfig() (newFiles []string, existingFiles []string, err error) {
	// Initialize the system configuration file and save it to a file
	systemCfgExists := false
	systemCfgExists, err = system.InitSystemConfig(systemConfigFilePath)
	if systemCfgExists {
		existingFiles = append(existingFiles, systemConfigFilePath)
	} else {
		newFiles = append(newFiles, systemConfigFilePath)
	}

	if err != nil {
		return newFiles, existingFiles, err
	}

	// Initialize the application configuration file and save it to a file
	appCfgExists := false
	appCfgExists, err = app.InitAppConfig(appConfigFilePath)
	if appCfgExists {
		existingFiles = append(existingFiles, appConfigFilePath)
	} else {
		newFiles = append(newFiles, appConfigFilePath)
	}

	if err != nil {
		return newFiles, existingFiles, err
	}

	return newFiles, existingFiles, nil
}

// GetConfig returns the application configuration
func GetConfig() *Config {
	return &Config{
		System: system.GetSystemConfig(systemConfigFilePath),
		App:    app.GetAppConfig(appConfigFilePath),
	}
}

// SaveConfig saves the configuration
func SaveConfig() error {
	err := app.SaveAppConfig(appConfigFilePath, false)
	if err != nil {
		return err
	}

	return nil
}

// PrintInfo prints the application information
func PrintInfo(versionOnly bool) {
	systemCfg := GetConfig().System

	goVersion := strings.Replace(runtime.Version(), "go", "", 1)

	fmt.Printf("Welcome to %s!\n", textutils.ColorText(textutils.Green, (textutils.BoldText(systemCfg.AppName))))
	fmt.Printf("Built with Go %s\n", textutils.ColorText(textutils.Yellow, (textutils.BoldText(goVersion))))
	fmt.Printf("Running version %s\n", textutils.ColorText(textutils.Magenta, (textutils.BoldText(systemCfg.AppVersion))))

	contributors := strings.Join(systemCfg.Contributors, ", ")
	fmt.Printf("Developed by %s\n", textutils.ColorText(textutils.Cyan, (textutils.BoldText(contributors))))
	fmt.Printf("Release date: %s\n", textutils.ColorText(textutils.Blue, (textutils.BoldText(systemCfg.ReleaseDate))))

	fmt.Println("")

	if !versionOnly {
		switch strings.ToLower(flags.FlagEnvironment) {
		case "development":
			fmt.Println(textutils.ColorText(textutils.Red, (textutils.BoldText("Running in Development mode"))))
		case "testing":
			fmt.Println(textutils.ColorText(textutils.Yellow, (textutils.BoldText("Running in Testing mode"))))
		case "production":
			fmt.Println(textutils.ColorText(textutils.Green, (textutils.BoldText("Running in Production mode"))))
		default:
			fmt.Println(textutils.ColorText(textutils.Blue, (textutils.BoldText("Running in Default mode"))))
		}

		fmt.Println("")

		if flags.FlagDebugMode {
			fmt.Println(textutils.ColorText(textutils.Red, (textutils.BoldText("Debug mode enabled"))))
			fmt.Println("")
		}

	}
}
