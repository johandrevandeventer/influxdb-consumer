package coreutils

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/johandrevandeventer/influxdb-consumer/internal/flags"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v2"
)

// VerbosePrintln prints a message if the verbose flag is set
func VerbosePrintln(message string) {
	if flags.FlagVerbose {
		fmt.Println(message)
	}
}

// Get the root directory
func GetRootDir() string {
	currentDir, err := filepath.Abs(".")
	if err != nil {
		panic(err)
	}

	return currentDir
}

// Get the runtime directory
func GetRuntimeDir() string {
	return filepath.Join(GetRootDir(), ".runtime")
}

// Get the config directory
func GetConfigDir() string {
	return filepath.Join(GetRuntimeDir(), "config")
}

// Get the logging directory
func GetLoggingDir() string {
	return filepath.Join(GetRuntimeDir(), "logs")
}

// Get the persistence directory
func GetPersistDir() string {
	return filepath.Join(GetRuntimeDir(), "persist")
}

// Get the temporary directory
func GetTmpDir() string {
	return filepath.Join(GetRuntimeDir(), "tmp")
}

// Get the connections directory
func GetConnectionsDir() string {
	return filepath.Join(GetRuntimeDir(), "connections")
}

// FileExists checks if a file exists
func FileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return false
	}
	return err == nil
}

// LoadYAMLFile loads the specified file path and unmarshals it into the given data structure.
func LoadYAMLFile(filePath string, target interface{}) error {
	// Check if the file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", filePath)
	}

	// Open the file for reading
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Decode the configuration from the file
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("failed to decode the data: %w", err)
	}

	return nil
}

// SaveYAMLFile saves the given data to the specified file path in YAML format.
func SaveYAMLFile(filePath string, toSave interface{}, createFile bool) error {
	// Check if the directory exists, and create it if necessary
	dir := filepath.Dir(filePath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}

	// If the file doesn't exist and createFile is false, return an error
	if _, err := os.Stat(filePath); os.IsNotExist(err) && !createFile {
		return fmt.Errorf("file does not exist: %s", filePath)
	}

	// Open the file for writing (create or overwrite)
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Encode the configuration to the file
	encoder := yaml.NewEncoder(file)
	if err := encoder.Encode(toSave); err != nil {
		return fmt.Errorf("failed to encode the data: %w", err)
	}

	return nil
}

// CreateTmpDir creates a temporary directory
func CreateTmpDir(filepath string) error {
	// Create tmp directory
	if err := os.MkdirAll(filepath, os.ModePerm); err != nil {
		return err
	}

	return nil
}

// CleanTmpDir deletes the temporary directory
func CleanTmpDir(tmpDir string) (response string, err error) {
	// Delete the `tmp` directory if it exists
	if _, err := os.Stat(tmpDir); err == nil {
		err := os.RemoveAll(tmpDir)
		if err != nil {
			return response, fmt.Errorf("failed to delete tmp directory: %w", err)
		} else {
			return "Tmp directory deleted successfully", nil
		}
	} else if os.IsNotExist(err) {
		return "Tmp directory does not exist, skipping deletion", nil
	} else {
		return response, fmt.Errorf("error checking tmp directory: %w", err)
	}
}

// WriteToLogFile writes a message to a log file
func WriteToLogFile(path string, message string) error {
	// Create the directory structure if it doesn't exist
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory for log file: %w", err)
	}

	// Open the file for appending or create it if it doesn't exist
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	// Write the message to the file
	if _, err := file.WriteString(message); err != nil {
		return fmt.Errorf("failed to write to log file: %w", err)
	}

	return nil
}

// Decode map into struct
func DecodeMapToStruct(payload interface{}, target interface{}) error {
	err := mapstructure.Decode(payload, target)
	if err != nil {
		return err
	}

	return nil
}
