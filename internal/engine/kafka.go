package engine

import (
	"log"

	"github.com/johandrevandeventer/influxdb-consumer/internal/flags"
	"github.com/johandrevandeventer/kafkaclient/config"
	"github.com/johandrevandeventer/kafkaclient/consumer"
	"github.com/johandrevandeventer/kafkaclient/prometheusserver"
	"github.com/johandrevandeventer/logging"
	"go.uber.org/zap"
)

func (e *Engine) startKafkaConsumer() {
	e.logger.Info("Starting Kafka consumer")

	var kafkaConsumerLogger *zap.Logger
	if flags.FlagKafkaLogging {
		kafkaConsumerLogger = logging.GetLogger("kafka.consumer")
	} else {
		kafkaConsumerLogger = zap.NewNop()
	}

	// Define Kafka consumer config
	consumerConfig := config.NewKafkaConsumerConfig("localhost:9092", "rubicon_kafka_influxdb", "influxdb-consumer-group")

	// Initialize Kafka Consumer Pool
	kafkaConsumer, err := consumer.NewKafkaConsumer(e.ctx, consumerConfig, kafkaConsumerLogger)
	if err != nil {
		log.Fatalf("Failed to create Kafka consumer pool: %v", err)
	}

	e.kafkaConsumer = kafkaConsumer

	// Start Prometheus Metrics Server
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		prometheusserver.StartPrometheusServer(":2114", e.ctx)
	}()

	// Start Kafka consumer
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.kafkaConsumer.Start()
	}()
}
