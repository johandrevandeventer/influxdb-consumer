package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/johandrevandeventer/influxdb-consumer/internal/config"
	"github.com/johandrevandeventer/influxdb-consumer/internal/flags"
	coreutils "github.com/johandrevandeventer/influxdb-consumer/utils"
	"github.com/johandrevandeventer/influxdbclient"
	"github.com/johandrevandeventer/kafkaclient/consumer"
	"github.com/johandrevandeventer/kafkaclient/producer"
	"github.com/johandrevandeventer/persist"
	"go.uber.org/zap"
)

var (
	startTime time.Time
	endTime   time.Time
)

type Engine struct {
	ctx                      context.Context
	cancelFunc               context.CancelFunc
	cfg                      *config.Config
	logger                   *zap.Logger
	statePersister           *persist.FilePersister
	stopFileChan             chan struct{}
	influxdbConnectedCh      chan struct{}
	kafkaConsumerConnectedCh chan struct{}
	tmpFilePath              string
	stopFileFilePath         string
	connectionsLogFilePath   string
	wg                       sync.WaitGroup
	kafkaProducerPool        *producer.KafkaProducerPool
	kafkaConsumer            *consumer.KafkaConsumer
	influxDBClient           *influxdbclient.InfluxDBClient
}

// NewEngine creates a new Engine instance
func NewEngine(ctx context.Context, cfg *config.Config, logger *zap.Logger, statePersister *persist.FilePersister) *Engine {
	ctx, cancel := context.WithCancel(ctx)

	return &Engine{
		ctx:                      ctx,
		cancelFunc:               cancel,
		cfg:                      cfg,
		logger:                   logger,
		statePersister:           statePersister,
		stopFileChan:             make(chan struct{}),
		influxdbConnectedCh:      make(chan struct{}),
		kafkaConsumerConnectedCh: make(chan struct{}),
		tmpFilePath:              cfg.App.Runtime.TmpDir,
		stopFileFilePath:         cfg.App.Runtime.StopFileFilepath,
		connectionsLogFilePath:   cfg.App.Runtime.ConnectionsLogFilePath,
	}
}

// Run starts the Engine
func (e *Engine) Run() {
	e.logger.Info("Application started")

	// Create tmp directory
	e.verboseDebug("Creating tmp directory", zap.String("path", filepath.ToSlash(e.tmpFilePath)))
	err := coreutils.CreateTmpDir(e.tmpFilePath)
	if err != nil {
		e.logger.Error("Failed to create tmp directory", zap.Error(err))
		return
	}

	// Create connections log directory
	connectionsLogFilePathDir := filepath.Dir(e.connectionsLogFilePath)
	e.verboseDebug("Creating connections directory", zap.String("path", filepath.ToSlash(connectionsLogFilePathDir)))

	startTime = time.Now()

	// Set initial state
	e.statePersister.Set("app", map[string]any{})
	e.statePersister.Set("app.status", "running")
	e.statePersister.Set("app.name", e.cfg.System.AppName)
	e.statePersister.Set("app.version", e.cfg.System.AppVersion)
	e.statePersister.Set("app.release_date", e.cfg.System.ReleaseDate)
	e.statePersister.Set("app.environment", flags.FlagEnvironment)
	e.statePersister.Set("app.start_time", startTime.Format(time.RFC3339))

	coreutils.WriteToLogFile(e.connectionsLogFilePath, fmt.Sprintf("%s: App started\n", startTime.Format(time.RFC3339)))

	// Start background workers
	e.start()

	// Wait for shutdown signal
	select {
	case <-e.ctx.Done():
		e.logger.Warn("Received signal to stop the application")
	case <-e.stopFileChan:
		e.logger.Warn("Stop file detected, stopping operation")
	}

	// Stop the engine
	e.Stop()
}

// start starts background workers
func (e *Engine) start() {
	e.logger.Info("Background worker started")

	// Watch for stop file
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.WatchStopFile(e.stopFileFilePath)
	}()

	select {
	case <-e.ctx.Done():
		return
	default:
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()

			e.initInfluxDBClient()

			e.tryInfluxDBConnection(5)

			close(e.influxdbConnectedCh)
		}()
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		select {
		case <-e.ctx.Done():
			return
		case <-e.influxdbConnectedCh:
			e.startKafkaConsumer()
			// Once kafkaConsumer is initialized, send a signal
			close(e.kafkaConsumerConnectedCh)
		}
	}()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		select {
		case <-e.ctx.Done():
			return
		case <-e.kafkaConsumerConnectedCh:
			if e.kafkaConsumer != nil {
				e.startWorker()
			}
		}
	}()
}

// cleanup performs cleanup operations
func (e *Engine) cleanup() {
	e.verboseDebug("Cleaning up")
	defer e.verboseDebug("Cleanup complete")

	// Clean up tmp directory
	e.verboseDebug("Cleaning tmp directory", zap.String("path", filepath.ToSlash(e.tmpFilePath)))
	response, err := coreutils.CleanTmpDir(e.tmpFilePath)
	if err != nil {
		e.logger.Error("Failed to clean tmp directory", zap.Error(err))
	}
	if response != "" {
		e.verboseDebug(response)
	}

	// Close Kafka producer pool
	e.verboseDebug("Closing Kafka producer pool")
	if e.kafkaProducerPool != nil {
		e.kafkaProducerPool.Close()
	}
	e.verboseDebug("Kafka producer pool closed")

	// Close Kafka consumer
	e.verboseDebug("Closing Kafka consumer")
	if e.kafkaConsumer != nil {
		e.kafkaConsumer.Close()
	}
	e.verboseDebug("Kafka consumer closed")
}

// Stop stops the Engine
func (e *Engine) Stop() {
	e.logger.Debug("Stopping application")

	// Cancel the context to signal all goroutines to stop
	if e.cancelFunc != nil {
		e.cancelFunc()
	}

	// Wait for all goroutines to finish
	e.wg.Wait()

	// Perform cleanup
	e.cleanup()

	// Log application stop
	coreutils.WriteToLogFile(e.connectionsLogFilePath, fmt.Sprintf("%s: App stopped\n", endTime.Format(time.RFC3339)))

	endTime = time.Now()
	duration := endTime.Sub(startTime)

	e.statePersister.Set("app.status", "stopped")
	e.statePersister.Set("app.end_time", endTime.Format(time.RFC3339))
	e.statePersister.Set("app.duration", duration.String())

	e.logger.Info("Application stopped")
}

// WatchStopFile watches for the presence of a stop file
func (e *Engine) WatchStopFile(stopFileFilePath string) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-e.stopFileChan:
			return
		case <-ticker.C:
			if _, err := os.Stat(stopFileFilePath); err == nil {
				select {
				case <-e.stopFileChan: // Prevent closing channel twice
				default:
					close(e.stopFileChan)
				}
				return
			}
		}
	}
}

// StopFileDetected returns a channel that is closed when the stop file is detected
func (e *Engine) StopFileDetected() <-chan struct{} {
	return e.stopFileChan
}
