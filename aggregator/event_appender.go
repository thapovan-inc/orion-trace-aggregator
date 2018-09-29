package aggregator

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/thapovan-inc/orion-trace-aggregator/bookkeeper"
	"github.com/thapovan-inc/orion-trace-aggregator/storage"
	storagemodel "github.com/thapovan-inc/orion-trace-aggregator/storage/common"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"github.com/thapovan-inc/orionproto"
	"go.uber.org/zap"
	"time"
)

type eventAppender struct {
	provider   storage.Provider
	bookKeeper bookkeeper.BookKeeper
	parent     *actor.PID
	writeAsync bool
}

func (ea *eventAppender) insertEvent(traceID, spanID []byte, eventID uint64, eventType storagemodel.EventType, message, metadata string, spanData *orionproto.Span) {
	logger := util.GetLogger("aggregator", "eventAppender::insertEvent")
	event := &storagemodel.Event{
		TraceID:      traceID,
		SpanID:       spanID,
		EventID:      eventID,
		Timestamp:    time.Duration(spanData.Timestamp * 1000),
		Type:         eventType,
		Location:     spanData.EventLocation,
		Message:      message,
		MetadataJSON: []byte(metadata),
	}
	err := ea.provider.InsertEventEntry(event)
	if err != nil {
		fields := []zap.Field{zap.ByteString("traceID", traceID), zap.ByteString("spanID", spanID), zap.Uint64("eventID", eventID)}
		logger.Error("Unable to insert event entry", fields...)
	}
}

func (ea *eventAppender) Receive(c actor.Context) {
	logger := util.GetLogger("aggregator", "eventAppender::Receive")
	switch msg := c.Message().(type) {
	case *eventStruct:
		ea.insertEvent(msg.traceID, msg.spanID, msg.eventID, msg.eventType, msg.message, msg.metadata, msg.spanData)
	case *actor.Started:
		ea.parent = c.Parent()
		logger.Info("Actor started")
	case *actor.Restarting:
		ea.parent = c.Parent()
		logger.Info("Actor restarting")
	case *actor.Stopping:
		c.Stash()
		logger.Info("Stopping, actor is about shut down")
	case *actor.Stopped:
		logger.Info("Stopped, actor and its children are stopped")
	}
}
