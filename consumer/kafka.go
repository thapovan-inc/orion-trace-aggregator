// +build kafka

package consumer

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"go.uber.org/zap"
	"reflect"
	"runtime"
	"sync/atomic"
)

type KafkaConsumer struct {
	consumerGroupName string
	ConfigMap         kafka.ConfigMap
	consumer          *kafka.Consumer
	connected         bool
	dispatcherRunning bool
	destinationPID    *actor.PID
	controlPID        *actor.PID
	//closeSignal         chan bool
	pollingEnabled      int64
	currentSubscription []string
}

func (k *KafkaConsumer) connect() error {
	logger := util.GetLogger("consumer", "KafkaConsumer::connect")
	var err error
	logger.Info("Connecting to kafka broker")
	k.consumer, err = kafka.NewConsumer(&k.ConfigMap)
	if err == nil {
		k.connected = true
	} else {
		k.connected = false
		logger.Error("Unable to connect to kafka broker", zap.Error(err))
	}
	return err
}

func (k *KafkaConsumer) GetControlPID() *actor.PID {
	if k.controlPID == nil {
		props := actor.FromProducer(func() actor.Actor {
			return k
		})
		k.controlPID = actor.Spawn(props)
	}
	return k.controlPID
}

func (k *KafkaConsumer) isConnected() bool {
	return k.connected
}

func (k *KafkaConsumer) rebalanceHandler(consumer *kafka.Consumer, event kafka.Event) error {
	logger := util.GetLogger("consumer", "KafkaConsumer::rebalanceHandler")
	logger.Debug("Received rebalance callback with event ", zap.String("event", event.String()))
	return nil
}

func (k *KafkaConsumer) dispatcher() {
	logger := util.GetLogger("consumer", "KafkaConsumer::dispatcher")
	defer k.stopDispatcher()
	logger.Debug("Starting dispatcher loop")
	//DispatchLoop:
	//	for {
	//		select {
	//		case _ = <-k.closeSignal:
	//			break DispatchLoop
	//		case ev := <-k.consumer.Events():
	//			switch e := ev.(type) {
	//			case kafka.AssignedPartitions:
	//				k.consumer.Assign(e.Partitions)
	//			case kafka.RevokedPartitions:
	//				k.consumer.Unassign()
	//			case *kafka.Message:
	//				logger.Debugf("Received message from topic %s. Key %s\n", *e.TopicPartition.Topic, e.Key)
	//				msg := &Message{Topic: *e.TopicPartition.Topic, Key: e.Key, Value: e.Value,
	//					Timestamp: uint64(e.Timestamp.UnixNano() / 1000)}
	//				k.destinationPID.Tell(msg)
	//			case kafka.Error:
	//				logger.WithError(e).Errorln("Error event received when listening for messages")
	//				k.destinationPID.Tell(&Message{})
	//				break DispatchLoop
	//			}
	//		}
	//	}
DispatchLoop:
	for k.pollingEnabled > 0 {
		ev := k.consumer.Poll(100)
		switch e := ev.(type) {
		case kafka.AssignedPartitions:
			k.consumer.Assign(e.Partitions)
		case kafka.RevokedPartitions:
			k.consumer.Unassign()
		case *kafka.Message:
			logger.Sugar().Debugf("Received message from topic %s. Key %s\n", *e.TopicPartition.Topic, e.Key)
			msg := &Message{Topic: *e.TopicPartition.Topic, Key: e.Key, Value: e.Value,
				Timestamp: uint64(e.Timestamp.UnixNano() / 1000)}
			k.destinationPID.Tell(msg)
		case kafka.Error:
			logger.Error("Error event received when listening for messages")
			k.destinationPID.Tell("sig_stop")
			break DispatchLoop
		default:
			runtime.Gosched()
		}
	}
	k.dispatcherRunning = false
}

func (k *KafkaConsumer) Subscribe(pid *actor.PID, topic ...string) error {
	//logger := util.GetLogger("consumer", "KafkaConsumer::Subscribe")
	if !k.connected {
		err := k.connect()
		if err != nil {
			return err
		}
	}
	if reflect.DeepEqual(topic, k.currentSubscription) {
		if k.dispatcherRunning {
			k.stopDispatcher()
		}
		k.startDispatcher()
		return nil
	}
	err := k.consumer.SubscribeTopics(topic, k.rebalanceHandler)
	if err == nil {
		k.currentSubscription = make([]string, len(topic))
		copy(k.currentSubscription, topic)
		if &pid != &k.destinationPID {
			k.destinationPID = pid
		}
		k.stopDispatcher()
		return nil
	}
	return err
}

func (k *KafkaConsumer) stopDispatcher() {
	if k.dispatcherRunning {
		//k.closeSignal <- true
		//k.closeSignal = nil
		shouldStopDispatcher := atomic.CompareAndSwapInt64(&k.pollingEnabled, 1, 0)
		if shouldStopDispatcher {
			k.dispatcherRunning = false
		}
	}
}

func (k *KafkaConsumer) startDispatcher() {
	if !k.dispatcherRunning {
		//k.closeSignal = make(chan bool, 1)
		shouldStartDispatcher := atomic.CompareAndSwapInt64(&k.pollingEnabled, 0, 1)
		if shouldStartDispatcher {
			runtime.GC()
			go k.dispatcher()
		}
	} else {
		logger := util.GetLogger("consumer", "KafkaConsumer::startDispatcher")
		logger.Warn("Attempting to open the start dispatcher when it is already opened")
	}
}

func (k *KafkaConsumer) UnSubscribe() error {
	k.stopDispatcher()
	return k.consumer.Unsubscribe()
}

func (k *KafkaConsumer) Close() error {
	k.stopDispatcher()
	if k.controlPID != nil {
		closeSignal := "sig_close"
		k.controlPID.Tell(closeSignal)
	}
	return k.consumer.Close()
}

func (k *KafkaConsumer) Receive(c actor.Context) {
	logger := util.GetLogger("consumer", "KafkaConsumer::Receive")
	switch msg := c.Message().(type) {
	case string:
		switch msg {
		case "sig_close":
			k.stopDispatcher()
		case "sig_pause":
			logger.Debug("Received sig_pause. Stopping dispatcher")
			k.stopDispatcher()
		case "sig_resume":
			logger.Debug("Received sig_resume. Resuming dispatcher")
			k.startDispatcher()
		}
	}
}
