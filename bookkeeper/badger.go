package bookkeeper

import (
	"github.com/dgraph-io/badger"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"go.uber.org/zap"
	"log"
	"os"
	"time"
)

type badgerBK struct {
	db     *badger.DB
	ticker *time.Ticker
}

func (b *badgerBK) init() error {
	opts := badger.DefaultOptions
	path := ".orion/bookkeeper"
	_ = os.Mkdir(".orion", 0777)
	opts.Dir = path
	opts.ValueDir = path
	var err error
	b.db, err = badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	b.ticker = time.NewTicker(5 * time.Minute)
	go b.gcCleanup()
	return err
}

func (b *badgerBK) Flush() error {
	return nil
}

func (b *badgerBK) Discard() error {
	return b.db.RunValueLogGC(0.5)
}

func (b *badgerBK) Close() error {
	b.ticker.Stop()
	return b.db.Close()
}

func (b *badgerBK) SpanSeenBefore(spanID []byte) bool {
	logger := util.GetLogger("bookkeeper", "badgerBK::SpanSeenBefore")
	result := make(chan bool, 1)
	err := b.db.View(func(txn *badger.Txn) error {
		spanExists := false
		defer func() { result <- spanExists }()
		key := append([]byte("s-"), spanID...)
		spanExists, err := b.wasSeenBefore(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to read from badger db", zap.Error(err))
	}
	return <-result
}

func (b *badgerBK) SpanStarted(spanID []byte) bool {
	logger := util.GetLogger("bookkeeper", "badgerBK::SpanStarted")
	result := make(chan bool, 1)
	err := b.db.View(func(txn *badger.Txn) error {
		spanStarted := false
		defer func() { result <- spanStarted }()
		key := append([]byte("s-"), spanID...)
		spanStarted, err := b.hasStarted(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to read from badger db", zap.Error(err))
	}
	return <-result
}

func (b *badgerBK) SpanEnded(spanID []byte) bool {
	logger := util.GetLogger("bookkeeper", "badgerBK::SpanEnded")
	result := make(chan bool, 1)
	err := b.db.View(func(txn *badger.Txn) error {
		spanEnded := false
		defer func() { result <- spanEnded }()
		key := append([]byte("s-"), spanID...)
		spanEnded, err := b.hasEnded(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to read from badger db", zap.Error(err))
	}
	return <-result
}

func (b *badgerBK) TraceSeenBefore(traceID []byte) bool {
	logger := util.GetLogger("bookkeeper", "badgerBK::TraceSeenBefore")
	result := make(chan bool, 1)
	err := b.db.View(func(txn *badger.Txn) error {
		traceExists := false
		defer func() { result <- traceExists }()
		key := append([]byte("t-"), traceID...)
		traceExists, err := b.wasSeenBefore(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to read from badger db", zap.Error(err))
	}
	return <-result
}

func (b *badgerBK) TraceStarted(traceID []byte) bool {
	logger := util.GetLogger("bookkeeper", "badgerBK::TraceStarted")
	result := make(chan bool, 1)
	err := b.db.View(func(txn *badger.Txn) error {
		traceStarted := false
		defer func() { result <- traceStarted }()
		key := append([]byte("t-"), traceID...)
		traceStarted, err := b.hasStarted(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to read from badger db", zap.Error(err))
	}
	return <-result
}

func (b *badgerBK) TraceEnded(traceID []byte) bool {
	logger := util.GetLogger("bookkeeper", "badgerBK::TraceEnded")
	result := make(chan bool, 1)
	err := b.db.View(func(txn *badger.Txn) error {
		traceEnded := false
		defer func() { result <- traceEnded }()
		key := append([]byte("t-"), traceID...)
		traceEnded, err := b.hasEnded(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to read from badger db", zap.Error(err))
	}
	return <-result
}

func (b *badgerBK) SawSpan(spanID []byte) {
	logger := util.GetLogger("bookkeeper", "badgerBK::SawSpan")
	err := b.db.Update(func(txn *badger.Txn) error {
		key := append([]byte("s-"), spanID...)
		err := b.markKeyAsSeen(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to write saw span to badger db", zap.Error(err))
	}
}

func (b *badgerBK) MarkSpanStarted(spanID []byte) {
	logger := util.GetLogger("bookkeeper", "badgerBK::MarkSpanStarted")
	err := b.db.Update(func(txn *badger.Txn) error {
		key := append([]byte("s-"), spanID...)
		err := b.markStart(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to write mark a started span in badger db", zap.Error(err))
	}
}

func (b *badgerBK) MarkSpanEnded(spanID []byte) {
	logger := util.GetLogger("bookkeeper", "badgerBK::MarkSpanEnded")
	err := b.db.Update(func(txn *badger.Txn) error {
		key := append([]byte("s-"), spanID...)
		err := b.markEnd(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to write mark a ended span in badger db", zap.Error(err))
	}
}

func (b *badgerBK) SawTrace(traceID []byte) {
	logger := util.GetLogger("bookkeeper", "badgerBK::SawTrace")
	err := b.db.Update(func(txn *badger.Txn) error {
		key := append([]byte("t-"), traceID...)
		err := b.markKeyAsSeen(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to write saw trace to badger db", zap.Error(err))
	}
}

func (b *badgerBK) MarkTraceStarted(traceID []byte) {
	logger := util.GetLogger("bookkeeper", "badgerBK::MarkTraceStarted")
	err := b.db.Update(func(txn *badger.Txn) error {
		key := append([]byte("t-"), traceID...)
		err := b.markStart(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to write mark a started trace in badger db", zap.Error(err))
	}
}

func (b *badgerBK) MarkTraceEnded(traceID []byte) {
	logger := util.GetLogger("bookkeeper", "badgerBK::MarkSpanEnded")
	err := b.db.Update(func(txn *badger.Txn) error {
		key := append([]byte("t-"), traceID...)
		err := b.markEnd(txn, key)
		return err
	})
	if err != nil {
		logger.Warn("Error when trying to write mark a ended trace in badger db", zap.Error(err))
	}
}

func (b *badgerBK) wasSeenBefore(txn *badger.Txn, key []byte) (bool, error) {
	wasSeen := false
	item, err := txn.Get(key)
	if item != nil {
		wasSeen = true
		err = nil
	}
	if err == badger.ErrKeyNotFound {
		err = nil
	}
	return wasSeen, err
}

func (b *badgerBK) hasStarted(txn *badger.Txn, key []byte) (bool, error) {
	started := false
	item, err := txn.Get(key)
	if (err != badger.ErrKeyNotFound) && (item != nil) {
		var data []byte
		data, err = item.Value()
		if data != nil {
			started = data[0] == 0x01
		}
	}
	if err == badger.ErrKeyNotFound {
		err = nil
	}
	return started, err
}

func (b *badgerBK) hasEnded(txn *badger.Txn, key []byte) (bool, error) {
	ended := false
	item, err := txn.Get(key)
	if (err != badger.ErrKeyNotFound) && (item != nil) {
		var data []byte
		data, err = item.Value()
		if data != nil {
			ended = data[1] == 0x01
		}
	}
	if err == badger.ErrKeyNotFound {
		err = nil
	}
	return ended, err
}

func (b *badgerBK) markStart(txn *badger.Txn, key []byte) error {
	item, err := txn.Get(key)
	predicate := (err == badger.ErrKeyNotFound) && (item == nil)
	updatedValue := make([]byte, 2)
	if !predicate {
		val, _ := item.Value()
		if val != nil {
			predicate = val[0] != 0x01
		}
		if !predicate {
			item.ValueCopy(updatedValue)
		}
	}
	updatedValue[0] = 0x01
	if predicate {
		err = txn.Set(key, updatedValue)
	}
	return err
}

func (b *badgerBK) markEnd(txn *badger.Txn, key []byte) error {
	item, err := txn.Get(key)
	predicate := (err == badger.ErrKeyNotFound) && (item == nil)
	updatedValue := make([]byte, 2)
	if !predicate {
		val, _ := item.Value()
		if val != nil {
			predicate = val[1] != 0x01
		}
		if !predicate {
			item.ValueCopy(updatedValue)
		}
	}
	updatedValue[1] = 0x01
	if predicate {
		err = txn.Set(key, updatedValue)
	}
	return err
}

func (b *badgerBK) markKeyAsSeen(txn *badger.Txn, key []byte) error {
	item, err := txn.Get(key)
	if (err == badger.ErrKeyNotFound) || (item == nil) {
		err = txn.Set(key, []byte{0, 0})
	}
	return err
}

func (b *badgerBK) gcCleanup() {
	logger := util.GetLogger("bookkeeper", "badgerBK::gcCleanup")
	var err error
	for range b.ticker.C {
	again:
		err := b.db.RunValueLogGC(0.7)
		if err == nil {
			goto again
		} else if err == badger.ErrNoRewrite {
			continue
		}
		if err != nil {
			break
		}
	}
	if err != nil {
		logger.Error("Value GC goroutine has stopped", zap.Error(err))
	}
}
