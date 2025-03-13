package system

import (
	"os"
	"path/filepath"

	coreutils "github.com/johandrevandeventer/influxdb-consumer/utils"
)

var (
	systemConfig *SystemConfig

	// Default configurations
	defaultSystemConfig *SystemConfig
)

func init() {
	defaultSystemConfig = &SystemConfig{
		AppName:      "Your App Name",
		AppVersion:   "0.1.0",
		ReleaseDate:  "2025-01-01",
		Contributors: []string{"Your name"},
	}

	systemConfig = defaultSystemConfig
}

// InitSystemConfig initializes the system configuration
func InitSystemConfig(filePath string) (fileExists bool, err error) {
	// Check if the config directory exists, if not create it
	if coreutils.FileExists(filePath) {
		return true, nil
	}

	// Create the configuration directory
	dir := filepath.Dir(filePath)
	os.Mkdir(dir, 0o770)

	// Save the system configuration
	err = SaveSystemConfig(filePath, true)
	if err != nil {
		return false, err
	}

	return false, nil
}

// GetSystemConfig returns the system configuration
func GetSystemConfig(filePath string) *SystemConfig {
	err := coreutils.LoadYAMLFile(filePath, &systemConfig)
	if err != nil {
		systemConfig = defaultSystemConfig
	}
	return systemConfig
}

// SaveSystemConfig saves the system configuration
func SaveSystemConfig(filePath string, createFile bool) error {
	err := coreutils.SaveYAMLFile(filePath, systemConfig, createFile)
	if err != nil {
		return err
	}

	return nil
}
