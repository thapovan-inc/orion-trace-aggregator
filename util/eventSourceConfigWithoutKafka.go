// +build !kafka

package util

type EventSource struct {
	Type               string     `toml:"type"`
	NatsConsumerConfig NatsConfig `toml:"nats"`
}
