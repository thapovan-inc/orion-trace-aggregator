package consumer

import (
	"errors"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/nats-io/go-nats-streaming"
	"github.com/nats-io/go-nats-streaming/pb"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"github.com/vmihailenco/msgpack"
	"go.uber.org/zap"
	"sync/atomic"
)

type natsMessage struct {
	Key   []byte
	Value []byte
}

type NatsConsumer struct {
	URL                 string
	ClusterID           string
	ClientID            string
	groupID             string
	nc                  stan.Conn
	currentSubscription stan.Subscription
	currentSubTopic     string
	aggregatorActor     *actor.PID
	controlPID          *actor.PID
	dispatcherRunning   int64
	connected           bool
}

func (nc *NatsConsumer) connect() error {
	logger := util.GetLogger("consumer", "NatsConsumer::connect")
	var err error
	if nc.nc != nil && nc.nc.NatsConn().IsConnected() {
		logger.Warn("Attempting to connect to nats server when already connected")
		return nil
	}
	logger.Info("Connecting")
	if nc.URL == "" {
		logger.Warn("nats URL not provided. Using default URL ", zap.String("defaultURL", stan.DefaultNatsURL))
		nc.URL = stan.DefaultNatsURL
	}
	nc.nc, err = stan.Connect(nc.ClusterID, nc.ClientID, stan.NatsURL(nc.URL),
		stan.SetConnectionLostHandler(func(_ stan.Conn, reason error) {
			logger.Sugar().Errorf("Connection lost, reason: %v", reason)
		}))
	if err != nil {
		logger.Sugar().Error("Error when trying to connect to nats server at ", nc.URL)
	} else {
		atomic.StoreInt64(&nc.dispatcherRunning, 0)
	}
	return err
}

func (nc *NatsConsumer) isConnected() bool {
	return nc.nc != nil && nc.nc.NatsConn().IsConnected()
}

func (nc *NatsConsumer) dispatcher(msg *stan.Msg) {
	if atomic.LoadInt64(&nc.dispatcherRunning) == 0 {
		msg.Reset()
		return
	}
	natsMessage := &natsMessage{}
	err := msgpack.Unmarshal(msg.Data, natsMessage)
	if err != nil {
		logger := util.GetLogger("consumer", "NatsConsumer::dispatcher")
		logger.Error("Error when trying to unmarshal message")
	} else {
		message := Message{Key: natsMessage.Key, Value: natsMessage.Value, Topic: msg.Subject, Timestamp: uint64(msg.Timestamp)}
		nc.aggregatorActor.Tell(&message)
		err = msg.Ack()
		if err != nil {
			logger := util.GetLogger("consumer", "NatsConsumer::dispatcher")
			logger.Error("Error when trying to ack message", zap.Error(err))
		}
	}
}

func (nc *NatsConsumer) Subscribe(pid *actor.PID, topic ...string) error {
	if nc.currentSubscription != nil && nc.currentSubscription.IsValid() {
		return errors.New("cannot create a new subscription when the exising subscription is active")
	}
	nc.aggregatorActor = pid
	nc.currentSubTopic = topic[0]
	err := nc.startDispatcher()
	return err
}

func (nc *NatsConsumer) enableSub() error {
	sub, err := nc.nc.QueueSubscribe(nc.currentSubTopic, nc.groupID, nc.dispatcher,
		stan.StartAt(pb.StartPosition_LastReceived), stan.DurableName(nc.groupID), stan.SetManualAckMode(), stan.MaxInflight(1))
	if err == nil {
		nc.currentSubscription = sub
	}
	return err
}

func (nc *NatsConsumer) GetControlPID() *actor.PID {
	if nc.controlPID == nil {
		props := actor.FromProducer(func() actor.Actor {
			return nc
		})
		nc.controlPID = actor.Spawn(props)
	}
	return nc.controlPID
}

func (nc *NatsConsumer) UnSubscribe() error {
	var err error
	if nc.currentSubscription != nil {
		err = nc.currentSubscription.Close()
	}
	return err
}

func (nc *NatsConsumer) Close() error {
	err := nc.UnSubscribe()
	if err == nil {
		err = nc.nc.Close()
	}
	if err != nil {
		logger := util.GetLogger("consumer", "NatsConsumer::Close")
		logger.Error("Unable to close nats connection", zap.Error(err))
	}
	return err
}

func (nc *NatsConsumer) isSubscriptionActive() bool {
	return nc.currentSubscription != nil && nc.currentSubscription.IsValid()
}

func (nc *NatsConsumer) stopDispatcher() {
	if nc.isSubscriptionActive() {
		ok := atomic.CompareAndSwapInt64(&nc.dispatcherRunning, 1, 0)
		if ok {
			err := nc.UnSubscribe()
			if err != nil {
				logger := util.GetLogger("consumer", "NatsConsumer::startDispatcher")
				logger.Error("Unable to unsubscribe from topic ", zap.String("topic", nc.currentSubTopic), zap.Error(err))
			}
		}
	}
}

func (nc *NatsConsumer) startDispatcher() error {
	if !nc.isSubscriptionActive() && nc.currentSubTopic != "" {
		ok := atomic.CompareAndSwapInt64(&nc.dispatcherRunning, 0, 1)
		if ok {
			err := nc.enableSub()
			return err
		} else {
			return nil
		}
	}
	return nil
	//else {
	//	logger := util.GetLogger("consumer", "NatsConsumer::startDispatcher")
	//	logger.Warningln("Attempting to open the start dispatcher when it is already opened")
	//}
}

func (nc *NatsConsumer) Receive(c actor.Context) {
	logger := util.GetLogger("consumer", "NatsConsumer::Receive")
	switch msg := c.Message().(type) {
	case string:
		switch msg {
		case "sig_close":
			nc.Close()
		case "sig_pause":
			logger.Debug("Received sig_pause. Stopping dispatcher")
			nc.stopDispatcher()
		case "sig_resume":
			logger.Debug("Received sig_resume. Resuming dispatcher")
			nc.startDispatcher()
		}
	}
}
