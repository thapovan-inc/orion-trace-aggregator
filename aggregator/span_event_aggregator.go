package aggregator

import (
	"encoding/hex"
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/thapovan-inc/orion-trace-aggregator/bookkeeper"
	"github.com/thapovan-inc/orion-trace-aggregator/storage"
	storagemodel "github.com/thapovan-inc/orion-trace-aggregator/storage/common"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"github.com/thapovan-inc/orionproto"
	"go.uber.org/zap"
	"strings"
	"time"
)

type spanEventAggregator struct {
	provider   storage.Provider
	bookKeeper bookkeeper.BookKeeper
	parent     *actor.PID
	writeAsync bool
	id         int64
}

func (sea *spanEventAggregator) upsertSpan(traceID, spanID []byte, eventType storagemodel.EventType, spanData *orionproto.Span) {
	logger := util.GetLogger("aggregator", "spanEventAggregator::upsertSpan")
	if eventType < storagemodel.START_SPAN {
		//if sea.bookKeeper.SpanSeenBefore(spanID) {
		//	return
		//} else {
		//	sea.bookKeeper.SawSpan(spanID)
		//}
		return
	} else if (eventType == storagemodel.START_SPAN) || (eventType == storagemodel.END_SPAN) {
		sea.bookKeeper.SawSpan(spanID)
	}
	parentID := spanData.GetParentSpanId()
	var parentIDBinary []byte
	if parentID != "" {
		parentID = strings.Replace(parentID, "-", "", -1)
		var err error
		parentIDBinary, err = hex.DecodeString(parentID)
		if err != nil {
			fields := []zap.Field{zap.ByteString("traceID", traceID), zap.ByteString("spanID", spanID), zap.Error(err)}
			logger.Warn("Unable to parse parentID", fields...)
		}
	}
	span := &storagemodel.Span{
		TraceID:     traceID,
		SpanID:      spanID,
		ParentID:    parentIDBinary,
		ServiceName: spanData.ServiceName,
	}
	if eventType == storagemodel.START_SPAN {
		span.StartTime = time.Duration(spanData.Timestamp * 1000)
		sea.bookKeeper.MarkSpanStarted(spanID)
	} else if eventType == storagemodel.END_SPAN {
		span.EndTime = time.Duration(spanData.Timestamp * 1000)
		sea.bookKeeper.MarkSpanEnded(spanID)
	}
	writeFunc := func() {
		err := sea.provider.UpsertSpanEntry(span)
		if err != nil {
			fields := []zap.Field{zap.ByteString("traceID", traceID), zap.ByteString("spanID", spanID), zap.Error(err)}
			logger.Error("Unable to upsert span entry", fields...)
		}
	}
	if sea.writeAsync {
		go writeFunc()
	} else {
		writeFunc()
	}
}

func (sea *spanEventAggregator) Receive(c actor.Context) {
	logger := util.GetLogger("aggregator", "spanEventAggregator::Receive")
	switch msg := c.Message().(type) {
	case *spanMsgStruct:
		//if int64(msg.spanID[0])%8 == sea.id {
		sea.upsertSpan(msg.traceID, msg.spanID, msg.eventType, msg.spanData)
		//}
	case *actor.Started:
		sea.parent = c.Parent()
		logger.Info("Actor started")
	case *actor.Restarting:
		sea.parent = c.Parent()
		logger.Info("Actor restarting")
	case *actor.Stopping:
		c.Stash()
		logger.Info("Stopping, actor is about shut down")
	case *actor.Stopped:
		logger.Info("Stopped, actor and its children are stopped")
	}
}
