package bookkeeper

import (
	"github.com/thapovan-inc/orion-trace-aggregator/util"
)

type BookKeeper interface {
	SpanSeenBefore(spanID []byte) bool
	SpanStarted(spanID []byte) bool
	SpanEnded(spanID []byte) bool

	TraceSeenBefore(traceID []byte) bool
	TraceStarted(traceID []byte) bool
	TraceEnded(traceID []byte) bool

	SawSpan(spanID []byte)
	MarkSpanStarted(spanID []byte)
	MarkSpanEnded(spanID []byte)

	SawTrace(traceID []byte)
	MarkTraceStarted(traceID []byte)
	MarkTraceEnded(traceID []byte)

	init() error
	Flush() error
	Discard() error
	Close() error
}

var bk BookKeeper

func GetBookKeeper() BookKeeper {
	if bk == nil {
		storageType := util.GetConfig().BookKeeper.Type
		if storageType == "" || storageType == "memory" {
			bk = &bigCacheBK{}
		} else {
			bk = &badgerBK{}
		}
		bk.init()
	}
	return bk
}

func Cleanup() {
	if bk != nil {
		bk.Close()
	}
}
