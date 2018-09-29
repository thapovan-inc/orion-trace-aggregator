package bookkeeper

import (
	"github.com/allegro/bigcache"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"go.uber.org/zap"
	"time"
)

type bigCacheBK struct {
	cache *bigcache.BigCache
}

func (bc *bigCacheBK) SpanSeenBefore(spanID []byte) bool {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::SpanSeenBefore")
	result, err := bc.wasSeenBefore(append([]byte("s-"), spanID...))
	if err != nil {
		logger.Warn("Error when trying to read from cache", zap.Error(err))
	}
	return result
}

func (bc *bigCacheBK) SpanStarted(spanID []byte) bool {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::SpanStarted")
	result, err := bc.hasStarted(append([]byte("s-"), spanID...))
	if err != nil {
		logger.Warn("Error when trying to read from cache", zap.Error(err))
	}
	return result
}

func (bc *bigCacheBK) SpanEnded(spanID []byte) bool {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::SpanEnded")
	result, err := bc.hasEnded(append([]byte("s-"), spanID...))
	if err != nil {
		logger.Warn("Error when trying to read from cache", zap.Error(err))
	}
	return result
}

func (bc *bigCacheBK) TraceSeenBefore(traceID []byte) bool {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::TraceSeenBefore")
	result, err := bc.wasSeenBefore(append([]byte("t-"), traceID...))
	if err != nil {
		logger.Warn("Error when trying to read from cache", zap.Error(err))
	}
	return result
}

func (bc *bigCacheBK) TraceStarted(traceID []byte) bool {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::TraceStarted")
	result, err := bc.hasStarted(append([]byte("t-"), traceID...))
	if err != nil {
		logger.Warn("Error when trying to read from cache", zap.Error(err))
	}
	return result
}

func (bc *bigCacheBK) TraceEnded(traceID []byte) bool {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::TraceEnded")
	result, err := bc.hasEnded(append([]byte("t-"), traceID...))
	if err != nil {
		logger.Warn("Error when trying to read from cache", zap.Error(err))
	}
	return result
}

func (bc *bigCacheBK) SawSpan(spanID []byte) {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::SawSpan")
	err := bc.markKeyAsSeen(append([]byte("s-"), spanID...))
	if err != nil {
		logger.Warn("Error when trying to write to cache", zap.Error(err))
	}
}

func (bc *bigCacheBK) MarkSpanStarted(spanID []byte) {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::MarkSpanStarted")
	err := bc.markStart(append([]byte("s-"), spanID...))
	if err != nil {
		logger.Warn("Error when trying to write to cache", zap.Error(err))
	}
}

func (bc *bigCacheBK) MarkSpanEnded(spanID []byte) {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::MarkSpanStarted")
	err := bc.markEnd(append([]byte("s-"), spanID...))
	if err != nil {
		logger.Warn("Error when trying to write to cache", zap.Error(err))
	}
}

func (bc *bigCacheBK) SawTrace(traceID []byte) {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::SawTrace")
	err := bc.markKeyAsSeen(append([]byte("t-"), traceID...))
	if err != nil {
		logger.Warn("Error when trying to write to cache", zap.Error(err))
	}
}

func (bc *bigCacheBK) MarkTraceStarted(traceID []byte) {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::MarkTraceStarted")
	err := bc.markStart(append([]byte("t-"), traceID...))
	if err != nil {
		logger.Warn("Error when trying to write to cache", zap.Error(err))
	}
}

func (bc *bigCacheBK) MarkTraceEnded(traceID []byte) {
	logger := util.GetLogger("bookkeeper", "bigCacheBK::MarkTraceEnded")
	err := bc.markStart(append([]byte("t-"), traceID...))
	if err != nil {
		logger.Warn("Error when trying to write to cache", zap.Error(err))
	}
}

func (bc *bigCacheBK) init() error {
	var err error
	bc.cache, err = bigcache.NewBigCache(bigcache.DefaultConfig(1 * time.Minute))
	return err
}

func (bc *bigCacheBK) Flush() error {
	return nil
}

func (bc *bigCacheBK) Discard() error {
	return bc.cache.Reset()
}

func (bc *bigCacheBK) Close() error {
	bc.cache.Reset()
	return bc.cache.Reset()
}

func (b *bigCacheBK) wasSeenBefore(key []byte) (bool, error) {
	wasSeen := false
	item, _ := b.cache.Get(string(key))
	if item != nil {
		wasSeen = true
	}
	return wasSeen, nil
}

func (b *bigCacheBK) hasStarted(key []byte) (bool, error) {
	started := false
	data, _ := b.cache.Get(string(key))
	if data != nil {
		started = data[0] == 0x01
	}
	return started, nil
}

func (b *bigCacheBK) hasEnded(key []byte) (bool, error) {
	ended := false
	data, _ := b.cache.Get(string(key))
	if data != nil {
		ended = data[1] == 0x01
	}
	return ended, nil
}

func (b *bigCacheBK) markStart(key []byte) error {
	var err error
	data, _ := b.cache.Get(string(key))
	if data != nil {
		if data[0] != 0x01 {
			data[0] = 0x01
			err = b.cache.Set(string(key), data)
		}
	} else {
		err = b.cache.Set(string(key), []byte{0x01, 0x00})
	}
	return err
}

func (b *bigCacheBK) markEnd(key []byte) error {
	var err error
	data, _ := b.cache.Get(string(key))
	if data != nil {
		if data[1] != 0x01 {
			data[1] = 0x01
			err = b.cache.Set(string(key), data)
		}
	} else {
		err = b.cache.Set(string(key), []byte{0x00, 0x01})
	}
	return err
}

func (b *bigCacheBK) markKeyAsSeen(key []byte) error {
	var err error
	data, _ := b.cache.Get(string(key))
	if data == nil {
		err = b.cache.Set(string(key), []byte{0x00, 0x00})
	}
	return err
}
