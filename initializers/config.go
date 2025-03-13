package initializers

import (
	"fmt"

	"github.com/johandrevandeventer/influxdb-consumer/internal/config"
	coreutils "github.com/johandrevandeventer/influxdb-consumer/utils"
	"github.com/johandrevandeventer/textutils"
)

// InitConfig initializes the configuration file
func InitConfig() error {
	newFiles, existingFiles, err := config.InitConfig()
	if err != nil {
		return fmt.Errorf("error initializing configuration files: %w", err)
	}

	if len(newFiles) > 0 {
		for _, file := range newFiles {
			coreutils.VerbosePrintln(textutils.ColorText(textutils.Green, fmt.Sprintf("-> Configuration file created: %s", file)))
		}
	}

	if len(existingFiles) > 0 {
		for _, file := range existingFiles {
			coreutils.VerbosePrintln(textutils.ColorText(textutils.Yellow, fmt.Sprintf("-> Using configuration file: %s", file)))
		}
	}

	return nil
}
