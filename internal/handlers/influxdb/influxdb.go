package influxdb_handler

import (
	"github.com/johandrevandeventer/influxdbclient"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

// runInfluxDBHandler runs the InfluxDB handler
func RunInfluxDBHandler(ctx context.Context, influxDBChan chan influxdbclient.Message, client *influxdbclient.InfluxDBClient, logger *zap.Logger) {
	workerPool := 100 // Number of concurrent workers
	semaphore := make(chan struct{}, workerPool)

	for {
		select {
		case <-ctx.Done(): // Handle context cancellation (e.g., Ctrl+C)
			logger.Info("Stopping worker due to context cancellation")
			return
		case data, ok := <-influxDBChan:
			if !ok { // Check if the channel is closed
				logger.Info("Stopping worker because influxDBChan is closed")
				return
			}

			// Acquire a semaphore slot
			semaphore <- struct{}{}

			// Process data in a goroutine
			go func(msg influxdbclient.Message) {
				defer func() { <-semaphore }() // Release the semaphore slot
				influxdbHandler(data, client)  // Handle the data
			}(data)
		}
	}
}

// influxdbHandler sends data to InfluxDB
func influxdbHandler(payload influxdbclient.Message, client *influxdbclient.InfluxDBClient) {
	client.WriteData(payload)
	// fmt.Println("Writing data to InfluxDB")
}
