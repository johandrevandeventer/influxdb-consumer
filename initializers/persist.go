package initializers

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/johandrevandeventer/influxdb-consumer/internal/config"
	"github.com/johandrevandeventer/persist"
)

// InitPersist initializes the file persister.
func InitPersist(cfg *config.Config) (*persist.FilePersister, error) {
	statePersister, err := persist.NewFilePersister(cfg.App.Runtime.PersistFilePath)
	if err != nil {
		if delErr := deletePersistDir(cfg.App.Runtime.PersistFilePath); delErr != nil {
			return nil, fmt.Errorf("failed to delete persist directory: %w", delErr)
		}

		// Retry initialization after deleting the directory
		statePersister, err = persist.NewFilePersister(cfg.App.Runtime.PersistFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to reinitialize state persister: %w", err)
		}
	}

	return statePersister, nil
}

// deletePersistDir removes the entire directory containing the persistence file.
func deletePersistDir(path string) error {
	dir := filepath.Dir(path) // Get the directory of the file
	return os.RemoveAll(dir)  // Remove the entire directory
}
