package engine

import (
	"fmt"
	"os"
	"time"

	"github.com/johandrevandeventer/influxdb-consumer/internal/flags"
	coreutils "github.com/johandrevandeventer/influxdb-consumer/utils"
	"github.com/johandrevandeventer/influxdbclient"
	"github.com/johandrevandeventer/logging"
	"go.uber.org/zap"
)

var (
	influxdbStartTime time.Time
	influxdbEndTime   time.Time
)

func (e *Engine) initInfluxDBClient() {
	e.logger.Info("Initializing InfluxDB client")
}

func (e *Engine) connectInfluxDBClient() error {
	config := influxdbclient.InfluxDBConfig{
		URL:    e.cfg.App.InfluxDB.URL,
		Org:    e.cfg.App.InfluxDB.Org,
		Bucket: e.cfg.App.InfluxDB.Bucket,
		Token:  e.cfg.App.InfluxDB.Token,
	}

	if flags.FlagEnvironment == "development" {
		url := os.Getenv("INFLUXDB_URL")
		org := os.Getenv("INFLUXDB_ORG")
		bucket := os.Getenv("INFLUXDB_BUCKET")
		token := os.Getenv("INFLUXDB_TOKEN")

		if url == "" {
			return fmt.Errorf("INFLUXDB_URL environment variable not set")
		}

		if org == "" {
			return fmt.Errorf("INFLUXDB_ORG environment variable not set")
		}

		if bucket == "" {
			return fmt.Errorf("INFLUXDB_BUCKET environment variable not set")
		}

		if token == "" {
			return fmt.Errorf("INFLUXDB_TOKEN environment variable not set")
		}

		config.URL = url
		config.Org = org
		config.Bucket = bucket
		config.Token = token
	}

	var influxdbLogger *zap.Logger // Define influxdbLogger outside if-else

	if flags.FlagInfluxDBLogging {
		influxdbLogger = logging.GetLogger("influxdb")
	} else {
		influxdbLogger = zap.NewNop() // Use zap.NewNop() to disable logging
	}

	e.influxDBClient = influxdbclient.NewInfluxDBClient(config.URL, config.Token, config.Org, config.Bucket, influxdbLogger)
	if err := e.influxDBClient.Connect(); err != nil {
		return err
	}

	return nil
}

func (e *Engine) tryInfluxDBConnection(retryInterval int) {
	e.logger.Info("Attempting to connect to InfluxDB broker")

	if retryInterval > 60 {
		e.logger.Warn("Exceeded maximum retry interval of 60 seconds. Resetting to 60 seconds")
		retryInterval = 60
	}

	select {
	case <-e.ctx.Done():
		return
	case <-e.stopFileChan:
		return
	default:
		for {
			// Check for context cancellation at the beginning of each iteration
			select {
			case <-e.ctx.Done():
				e.logger.Info("Context canceled, stopping InfluxDB connection attempts")
				return
			case <-e.stopFileChan:
				e.logger.Info("Received stop signal, stopping InfluxDB connection attempts")
				return
			default:
			}

			if e.influxDBClient != nil && e.influxDBClient.Connected() {
				e.influxDBClient.Disconnect()
			}

			if e.statePersister.Get("influxdb.status") == "connected" {
				e.influxdbStatePersistStop()
			}

			if err := e.connectInfluxDBClient(); err != nil {

				e.logger.Error("Error connecting to InfluxDB broker", zap.Error(err))
				e.logger.Info(fmt.Sprintf("Retrying in %d seconds", retryInterval))

				for range rangeInt(retryInterval * 1000) {
					select {
					case <-e.ctx.Done():
						e.logger.Info("Context canceled, stopping InfluxDB connection attempts")
						return
					case <-e.stopFileChan:
						e.logger.Info("Received stop signal, stopping InfluxDB connection attempts")
						return
					default:
						time.Sleep(time.Millisecond)
					}
				}

				// time.Sleep(time.Duration(retryInterval) * time.Second)
				continue

			}

			e.logger.Info("Connected to InfluxDB broker")
			e.influxdbStatePersistStart()
			break
		}
	}
}

// influxdbStatePersistStart persists the state of the InfluxDB connection
func (e *Engine) influxdbStatePersistStart() {
	influxdbStartTime = time.Now()

	coreutils.WriteToLogFile(e.connectionsLogFilePath, fmt.Sprintf("%s: InfluxDB connection started\n", influxdbStartTime.Format(time.RFC3339)))

	e.statePersister.Set("influxdb", map[string]interface{}{})
	e.statePersister.Set("influxdb.status", "connected")
	e.statePersister.Set("influxdb.start_time", influxdbStartTime.Format(time.RFC3339))
}

// influxdbStatePersistStop persists the state of the InfluxDB connection
func (e *Engine) influxdbStatePersistStop() {
	e.statePersister.Set("influxdb.status", "disconnected")

	if !influxdbStartTime.IsZero() {
		influxdbEndTime = time.Now()

		duration := influxdbEndTime.Sub(influxdbStartTime)

		coreutils.WriteToLogFile(e.connectionsLogFilePath, fmt.Sprintf("%s: InfluxDB connection stopped\n", influxdbEndTime.Format(time.RFC3339)))

		e.statePersister.Set("influxdb.end_time", influxdbEndTime.Format(time.RFC3339))
		e.statePersister.Set("influxdb.duration", duration.String())
	}
}

func rangeInt(n int) []struct{} {
	return make([]struct{}, n)
}
