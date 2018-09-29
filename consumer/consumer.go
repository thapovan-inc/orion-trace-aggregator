package consumer

import (
	"errors"
	"github.com/AsynkronIT/protoactor-go/actor"
)

const (
	KAFKA = "kafka"
	NATS  = "nats"
)

type Consumer interface {
	connect() error
	isConnected() bool
	Subscribe(pid *actor.PID, topic ...string) error
	GetControlPID() *actor.PID
	UnSubscribe() error
	Close() error
}

type Message struct {
	Topic     string
	Key       []byte
	Value     []byte
	Timestamp uint64
}

var consumer Consumer

func GetConsumer() (Consumer, error) {
	if consumer == nil {
		return nil, errors.New("consumer not yet initialized. Try consumer::InitConsumerFromConfig")
	}
	return consumer, nil
}
