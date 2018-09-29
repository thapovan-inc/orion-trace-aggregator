package aggregator

import (
	"encoding/hex"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/AsynkronIT/protoactor-go/mailbox"
	"github.com/AsynkronIT/protoactor-go/router"
	"github.com/gogo/protobuf/proto"
	"github.com/thapovan-inc/orion-trace-aggregator/bookkeeper"
	"github.com/thapovan-inc/orion-trace-aggregator/consumer"
	"github.com/thapovan-inc/orion-trace-aggregator/storage"
	storage_model "github.com/thapovan-inc/orion-trace-aggregator/storage/common"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"github.com/thapovan-inc/orionproto"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"sync/atomic"
)

type AsyncAggregator interface {
	PrepareActor() *actor.PID
}

type spanMsgStruct struct {
	traceID   []byte
	spanID    []byte
	eventType storage_model.EventType
	spanData  *orionproto.Span
}
type traceMsgStruct struct {
	spanMsgStruct
	metadata string
}

type eventStruct struct {
	traceMsgStruct
	eventID uint64
	message string
}

func (msg *spanMsgStruct) Hash() string {
	return string(msg.spanID)
}

func (msg *traceMsgStruct) Hash() string {
	return string(msg.traceID)
}

type mailboxMonitor struct {
	producerPID *actor.PID
}

func (*mailboxMonitor) MailboxStarted() {

}

func (*mailboxMonitor) MessagePosted(message interface{}) {
}

func (*mailboxMonitor) MessageReceived(message interface{}) {
}

func (m *mailboxMonitor) MailboxEmpty() {
	m.producerPID.Tell("sig_resume")
}

type aggregatorActor struct {
	pid                 *actor.PID
	traceEvent          *actor.PID
	spanEvent           []*actor.PID
	rawEvent            *actor.PID
	controlPID          *actor.PID
	provider            storage.Provider
	bookKeeper          bookkeeper.BookKeeper
	eventBatchSize      int64
	currentBatchLen     int64
	busyDownStreamCount int64
	writeAsync          bool
}

func (sa *aggregatorActor) PrepareActor() *actor.PID {
	props := actor.FromProducer(func() actor.Actor {
		return sa
	})
	sa.pid = actor.Spawn(props)
	return sa.pid
}

func (sa *aggregatorActor) Receive(c actor.Context) {

	logger := util.GetLogger("aggregator", "aggregatorActor::Receive")
	switch msg := c.Message().(type) {
	case *consumer.Message:
		sa.processMessage(msg)
		atomic.AddInt64(&sa.currentBatchLen, -1)
		if sa.currentBatchLen < 1 {
			sa.controlPID.Tell("sig_pause")
		}
	case string:
		switch msg {
		case "sig_resume":
			atomic.AddInt64(&sa.busyDownStreamCount, -1)
			if sa.busyDownStreamCount < 1 {
				atomic.StoreInt64(&sa.busyDownStreamCount, 1)
				atomic.StoreInt64(&sa.currentBatchLen, sa.eventBatchSize)
				sa.bookKeeper.Discard()
				sa.controlPID.Tell("sig_resume")
			}
		case "sig_stop":
			c.Self().Poison()
		}
	case *actor.Started:
		logger.Info("Actor started")
		atomic.StoreInt64(&sa.busyDownStreamCount, 0)
		taProps := router.NewConsistentHashPool(4).WithProducer(func() actor.Actor {
			return &traceEventAggregator{provider: sa.provider, bookKeeper: sa.bookKeeper, writeAsync: sa.writeAsync}
		})
		//if sa.eventBatchSize > 0 {
		//	taProps = taProps.WithMailbox(mailbox.Bounded(int(sa.eventBatchSize/4), &mailboxMonitor{producerPID: c.Self()}))
		//}
		sa.traceEvent = c.Spawn(taProps)
		//atomic.AddInt64(&sa.busyDownStreamCount, 1)
		seaProps := actor.FromProducer(func() actor.Actor {
			return &spanEventAggregator{provider: sa.provider, bookKeeper: sa.bookKeeper, writeAsync: sa.writeAsync}
		})
		if sa.eventBatchSize > 0 {
			//seaProps = seaProps.WithMailbox(mailbox.Bounded(int(sa.eventBatchSize/8), &mailboxMonitor{producerPID: c.Self()}))
			seaProps = seaProps.WithDispatcher(mailbox.NewSynchronizedDispatcher(1))
		}
		sa.spanEvent = make([]*actor.PID, 8)
		for i := 0; i < 8; i++ {
			sa.spanEvent[i] = c.Spawn(seaProps)
		}
		//atomic.AddInt64(&sa.busyDownStreamCount, 1)
		eaProps := router.NewRoundRobinPool(8).WithProducer(func() actor.Actor {
			return &eventAppender{provider: sa.provider, bookKeeper: sa.bookKeeper, writeAsync: sa.writeAsync}
		})
		if sa.eventBatchSize > 0 {
			eaProps = eaProps.WithMailbox(mailbox.Bounded(int(sa.eventBatchSize/4), &mailboxMonitor{producerPID: c.Self()})).WithDispatcher(mailbox.NewSynchronizedDispatcher(int(sa.eventBatchSize / 4)))
		}
		sa.rawEvent = c.Spawn(eaProps)
		atomic.AddInt64(&sa.busyDownStreamCount, 1)
		atomic.StoreInt64(&sa.currentBatchLen, sa.eventBatchSize)
	case *actor.Restarting:
		logger.Info("Actor restarting")
	case *actor.Stopping:
		for _, pid := range sa.spanEvent {
			pid.Stop()
		}
		sa.traceEvent.Poison()
		sa.rawEvent.Poison()
		c.Stash()
		logger.Info("Stopping, actor is about shut down")
	case *actor.Stopped:
		logger.Info("Stopped, actor and its children are stopped")
	}
}

func (sa *aggregatorActor) processMessage(msg *consumer.Message) {
	logger := util.GetLogger("aggregator", "aggregatorActor::processMessage")
	key := string(msg.Key)
	keyParts := strings.Split(key, "_")
	//TODO: Check if namespace matches with the one provided in config toml
	_ = keyParts[0] // This extracts the namespace part
	spanData := &orionproto.Span{}
	_ = proto.Unmarshal(msg.Value, spanData)
	traceID := strings.Replace(keyParts[1], "-", "", -1)
	spanID := strings.Replace(keyParts[2], "-", "", -1)
	eventID, err := strconv.ParseUint(keyParts[3], 10, 64)
	if err != nil {
		logger.Warn("Unable to parse eventID", zap.Error(err))
	}
	var eventType storage_model.EventType
	var metadata string
	var message string
	switch event := spanData.Event.(type) {
	case *orionproto.Span_StartEvent:
		eventType = storage_model.START_SPAN
		metadata = event.StartEvent.GetJsonString()
		message = "SPAN_STARTED"
	case *orionproto.Span_EndEvent:
		eventType = storage_model.END_SPAN
		metadata = event.EndEvent.GetJsonString()
		message = "SPAN_ENDED"
	case *orionproto.Span_LogEvent:
		metadata = event.LogEvent.GetJsonString()
		message = event.LogEvent.Message
		switch event.LogEvent.Level {
		case orionproto.LogLevel_DEBUG:
			eventType = storage_model.LOG_DEBUG
		case orionproto.LogLevel_INFO:
			eventType = storage_model.LOG_INFO
		case orionproto.LogLevel_WARN:
			eventType = storage_model.LOG_WARN
		case orionproto.LogLevel_ERROR:
			eventType = storage_model.LOG_ERROR
		case orionproto.LogLevel_CRITICAL:
			eventType = storage_model.LOG_CRITICAL
		default:
			eventType = storage_model.UNKNOWN
		}
	default:
		eventType = storage_model.UNKNOWN
	}
	traceIDBinary, err := hex.DecodeString(traceID)
	if err != nil {
		logger.Warn("Unable to convert traceID from hex to binary", zap.String("traceID", traceID), zap.Error(err))
		return
	}
	spanIDBinary, err := hex.DecodeString(spanID)
	if err != nil {
		logger.Warn("Unable to convert spanID from hex to binary.", zap.String("spanID", traceID), zap.Error(err))
	}
	spanMsg := spanMsgStruct{
		traceID:   traceIDBinary,
		spanID:    spanIDBinary,
		eventType: eventType,
		spanData:  spanData,
	}
	intermediateMsg := traceMsgStruct{
		spanMsg,
		metadata,
	}
	idx := spanMsg.spanID[0] % 8
	sa.spanEvent[idx].Tell(&spanMsg)
	sa.traceEvent.Tell(&intermediateMsg)
	event := eventStruct{
		intermediateMsg,
		eventID,
		message,
	}
	sa.rawEvent.Tell(&event)
}

var aggregator AsyncAggregator

func InitAggregator(provider storage.Provider, controlPID *actor.PID) {
	logger := util.GetLogger("aggregator", "InitAggregator")
	eventBatchSize := util.GetConfig().General.EventBatchSize
	writeAsync := util.GetConfig().General.WriteAsync
	if aggregator != nil {
		logger.Warn("Aggregator already initialised")
	} else {
		aggregator = &aggregatorActor{controlPID: controlPID, provider: provider, bookKeeper: bookkeeper.GetBookKeeper(), eventBatchSize: int64(eventBatchSize), writeAsync: writeAsync}
	}
}

func GetAggregator() AsyncAggregator {
	return aggregator
}
