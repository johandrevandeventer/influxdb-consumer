package engine

import (
	"encoding/json"
	"fmt"
	"time"

	influxdb_handler "github.com/johandrevandeventer/influxdb-consumer/internal/handlers/influxdb"
	"github.com/johandrevandeventer/influxdbclient"
	"github.com/johandrevandeventer/kafkaclient/payload"
	"go.uber.org/zap"
)

type Payload struct {
	MqttTopic        string    `json:"mqtt_topic"`
	Message          []byte    `json:"message"`
	MessageTimestamp time.Time `json:"message_timestamp"`
}

func (e *Engine) startWorker() {
	e.logger.Info("Starting InfluxDB workers")
	influxDBChannel := make(chan influxdbclient.Message, 1000)

	// var influxDBLogger *zap.Logger
	// if flags.FlagWorkersLogging {
	// 	influxDBLogger = logging.GetLogger("influxDB")
	// } else {
	// 	influxDBLogger = zap.NewNop()
	// }

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		select {
		case <-e.ctx.Done():
			return
		case <-e.influxdbConnectedCh:
			e.logger.Debug("Starting InfluxDB handler")
			influxdb_handler.RunInfluxDBHandler(e.ctx, influxDBChannel, e.influxDBClient, e.influxDBClient.Logger)
		}
	}()

	for {
		select {
		case <-e.ctx.Done(): // Handle context cancellation (e.g., Ctrl+C)
			e.logger.Info("Stopping worker due to context cancellation")
			return
		case data, ok := <-e.kafkaConsumer.GetOutputChannel():
			if !ok { // Channel is closed
				e.logger.Info("Kafka consumer output channel closed, stopping worker")
				return
			}
			// Deserialize data
			p, err := payload.Deserialize(data)
			if err != nil {
				e.logger.Error("Failed to deserialize message", zap.Error(err))
				continue
			}

			// Deserialize InfluxDB message
			m, err := DeserializeInfluxDBMessage(p.Message)
			if err != nil {
				e.logger.Error("Failed to deserialize InfluxDB message", zap.Error(err))
				continue
			}

			loggerMsg := fmt.Sprintf("InfluxDB -> %s :: %s :: %s :: %s :: %s :: %s :: %s", m.State, m.CustomerName, m.SiteName, m.Controller, m.ControllerIdentifier, m.DeviceType, m.DeviceIdentifier)
			e.influxDBClient.Logger.Info(loggerMsg)
			influxDBChannel <- *m
		}
	}
}

func DeserializeInfluxDBMessage(data []byte) (*influxdbclient.Message, error) {
	var m influxdbclient.Message
	err := json.Unmarshal(data, &m)
	return &m, err
}
