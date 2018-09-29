package aggregator

import (
	"github.com/AsynkronIT/protoactor-go/actor"
	"github.com/json-iterator/go"
	"github.com/thapovan-inc/orion-trace-aggregator/bookkeeper"
	"github.com/thapovan-inc/orion-trace-aggregator/storage"
	"github.com/thapovan-inc/orion-trace-aggregator/storage/common"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"github.com/thapovan-inc/orionproto"
	"go.uber.org/zap"
	"time"
)

type traceEventAggregator struct {
	provider   storage.Provider
	bookKeeper bookkeeper.BookKeeper
	writeAsync bool
}

func (ta *traceEventAggregator) Receive(c actor.Context) {
	logger := util.GetLogger("aggregator", "traceEventAggregator::Receive")
	switch msg := c.Message().(type) {
	case *traceMsgStruct:
		ta.upsertTrace(msg.traceID, msg.eventType, msg.metadata, msg.spanData)
	case *actor.Started:
		logger.Info("Actor started")
	case *actor.Restarting:
		logger.Info("Actor restarting")
	case *actor.Stopping:
		c.Stash()
		logger.Info("Stopping, actor is about shut down")
	case *actor.Stopped:
		logger.Info("Stopped, actor and its children are stopped")
	}
}

func (ta *traceEventAggregator) upsertTrace(traceID []byte, eventType common.EventType, metadata string, spanData *orionproto.Span) {
	logger := util.GetLogger("aggregator", "traceEventAggregator::upsertTrace")
	trace := &common.Trace{
		TraceID: traceID,
	}
	if eventType == common.START_SPAN && !ta.bookKeeper.TraceStarted(traceID) {
		signal := jsoniter.Get([]byte(metadata), "orion", "signal").ToString()
		if signal == "START_TRACE" {
			trace.FirstSeen = time.Duration(spanData.Timestamp * 1000)
		}
		ta.bookKeeper.TraceStarted(traceID)
	} else if ta.bookKeeper.TraceSeenBefore(traceID) {
		return
	} else {
		ta.bookKeeper.SawTrace(traceID)
	}
	if spanData.TraceContext.GetTraceName() != "" {
		trace.Name = spanData.TraceContext.GetTraceName()
	}
	writeFunc := func() {
		err := ta.provider.UpsertTraceEntry(trace)
		if err != nil {
			fields := []zap.Field{zap.ByteString("traceID", traceID), zap.Error(err)}
			logger.Error("Unable to upsert trace entry", fields...)
		}
	}
	if ta.writeAsync {
		go writeFunc()
	} else {
		writeFunc()
	}
}
