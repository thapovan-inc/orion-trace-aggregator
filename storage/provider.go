package storage

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/thapovan-inc/orion-trace-aggregator/storage/common"
	"github.com/thapovan-inc/orion-trace-aggregator/storage/mysql"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"github.com/thapovan-inc/orionproto"
	"go.uber.org/zap"
)

const (
	MYSQL = "mysql"
)

type Provider interface {
	Initialize() error
	UpsertTraceEntry(trace *common.Trace) error
	UpsertSpanEntry(span *common.Span) error
	InsertEventEntry(event *common.Event) error
	SetTag(tag *common.Tag) error
	AddToSummaryField(span *common.Span, fieldName string, delta int) error
	StoreSpanData(span *orionproto.Span) error
	Flush() error
	Close() error
}

var storageProvider Provider

func InitStorageProviderFromConfig() error {
	logger := util.GetLogger("storage", "InitStorageProviderFromConfig")
	config := util.GetConfig().Storage
	var err error
	if storageProvider != nil {
		return nil
	}
	switch config.Type {
	case MYSQL:
		mysqlProvider := &storage.MySqlProvider{MySqlConfig: config.MySQL}
		err = mysqlProvider.Initialize()
		if err != nil {
			logger.Error("unable to initialize mysql provider", zap.Error(err))
		} else {
			storageProvider = mysqlProvider
		}
	default:
		err = errors.New(fmt.Sprint("unable to find any known storage provider. configured type = ", config.Type))
	}
	return err
}

func GetStorageProvider() Provider {
	if storageProvider == nil {
		logger := util.GetLogger("storage", "GetStorageProvider")
		logger.Warn("Did you forget to call storage::InitStorageProviderFromConfig() ?")
		return nil
	} else {
		return storageProvider
	}
}
