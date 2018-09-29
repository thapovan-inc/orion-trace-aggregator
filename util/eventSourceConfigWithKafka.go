// +build kafka

package util

import "github.com/confluentinc/confluent-kafka-go/kafka"

type EventSource struct {
	Type                string          `toml:"type"`
	NatsConsumerConfig  NatsConfig      `toml:"nats"`
	KafkaConsumerConfig kafka.ConfigMap `toml:"kafka"`
}
