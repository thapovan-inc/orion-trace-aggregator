package storage

import (
	"database/sql"
	"errors"
	"github.com/go-sql-driver/mysql"
	"github.com/thapovan-inc/orion-trace-aggregator/storage/common"
	"github.com/thapovan-inc/orion-trace-aggregator/util"
	"github.com/thapovan-inc/orionproto"
	"go.uber.org/zap"
	"time"
)

const (
	_ = iota
	upsertTraceEntry
	upsertSpanEntry
	insertEventEntry
	setTag
	addToSummaryField
)

const (
	sp_UPSERT_TRACE         = "call upsert_trace (?,?,?,?,?)"
	sp_UPSERT_SPAN          = "call upsert_span(?,?,?,?,?,?,?)"
	sp_INSERT_EVENT         = "call insert_event(?,?,?,?,?,?,?,?)"
	sp_SET_TAG              = "call set_tag(?,?,?,?)"
	sp_ADD_FIELD_IN_SUMMARY = "call add_field_in_span_summary(?,?,?,?)"
)

type MySqlProvider struct {
	MySqlConfig       mysql.Config
	db                *sql.DB
	preparedStatement map[int]*sql.Stmt
}

func (p *MySqlProvider) UpsertTraceEntry(trace *common.Trace) error {
	logger := util.GetLogger("storage/mysql", "MySqlProvider::UpsertTraceEntry")
	stmt, err := p.prepareStatement(upsertTraceEntry, sp_UPSERT_TRACE)
	if err != nil {
		logger.Error("Unable to prepare statement", zap.Error(err))
		return err
	}
	var firstSeen, lastSeen *time.Time
	var traceEnded int
	if trace.FirstSeen > 0 {
		fstSeen := time.Unix(int64(trace.FirstSeen/time.Second), int64(trace.FirstSeen%time.Second))
		firstSeen = &fstSeen
	} else {
		fstSeen := time.Now()
		firstSeen = &fstSeen
	}
	if trace.LastSeen > 0 {
		lstSeen := time.Unix(int64(trace.LastSeen/time.Second), int64(trace.LastSeen%time.Second))
		lastSeen = &lstSeen
		traceEnded = 1
	}
	retriesLeft := 5
tryUpsert:
	result, err := stmt.Exec(trace.TraceID, trace.Name, firstSeen, lastSeen, traceEnded)
	if err != nil {
		if mysqlError, isMySQLError := err.(*mysql.MySQLError); isMySQLError {
			if mysqlError.Number == 1213 && retriesLeft > 0 {
				retriesLeft--
				goto tryUpsert
			}
		}
		logger.Error("Error when attempting to execute prepared statement", zap.Error(err))
		return err
	}
	if rowsCount, err := result.RowsAffected(); rowsCount != 1 && rowsCount != 2 {
		logger.Warn("Number of rows affected is not 1 or 2.", zap.Error(err), zap.Int64("rowsCount", rowsCount))
	}
	logger.Debug("Upserted trace entry for traceID", zap.ByteString("traceID", trace.TraceID))
	return nil
}

func (p *MySqlProvider) UpsertSpanEntry(span *common.Span) error {
	logger := util.GetLogger("storage/mysql", "MySqlProvider::UpsertSpanEntry")
	stmt, err := p.prepareStatement(upsertSpanEntry, sp_UPSERT_SPAN)
	if err != nil {
		logger.Error("Unable to prepare statement", zap.Error(err))
		return err
	}
	var startTime, endTime *time.Time
	var serviceName *string
	var parentID []byte
	if span.StartTime > 0 {
		sttime := time.Unix(int64(span.StartTime/time.Second), int64(span.StartTime%time.Second))
		startTime = &sttime
		serviceName = &span.ServiceName
		parentID = span.ParentID
	} else if span.EndTime > 0 {
		edTime := time.Unix(int64(span.EndTime/time.Second), int64(span.EndTime%time.Second))
		endTime = &edTime
		span.ParentID = nil
	} else {
		parentID = span.ParentID
		serviceName = &span.ServiceName
	}
	retriesLeft := 5
tryUpsert:
	result, err := stmt.Exec(span.TraceID, span.SpanID, parentID, startTime, serviceName, endTime, span.SummaryJSON)
	if err != nil {
		if mysqlError, isMySQLError := err.(*mysql.MySQLError); isMySQLError {
			if mysqlError.Number == 1213 && retriesLeft > 0 {
				retriesLeft--
				logger.Debug("Transaction failed due to deadlock. Attempting retry")
				goto tryUpsert
			}
		}
		logger.Error("Error when attempting to execute prepared statement", zap.Error(err))
		return err
	}
	if rowsCount, err := result.RowsAffected(); rowsCount != 1 && rowsCount != 2 {
		logger.Error("Number of rows affected is not 1.", zap.Int64("rowsCount", rowsCount), zap.Error(err))
	}
	logger.Debug("Upserted span entry", zap.ByteString("traceID", span.TraceID), zap.ByteString("spanID", span.SpanID))
	return nil
}

func (p *MySqlProvider) InsertEventEntry(event *common.Event) error {
	logger := util.GetLogger("storage/mysql", "MySqlProvider::InsertEventEntry")
	stmt, err := p.prepareStatement(insertEventEntry, sp_INSERT_EVENT)
	if err != nil {
		logger.Error("Unable to prepare statement", zap.Error(err))
		return err
	}
	timestamp := time.Unix(int64(event.Timestamp/time.Second), int64(event.Timestamp%time.Second))
	result, err := stmt.Exec(event.TraceID, event.SpanID, event.EventID, timestamp, event.Type, event.Location, event.MetadataJSON, event.Message)
	if err != nil {
		logger.Error("Error when attempting to execute prepared statement", zap.Error(err))
		return err
	}
	if rowsCount, err := result.RowsAffected(); rowsCount != 1 {
		logger.Warn("Number of rows affected is not 1.", zap.Int64("rowsCount", rowsCount), zap.Error(err))
	}
	logger.Debug("Inserted event entry", zap.ByteString("traceID", event.TraceID), zap.ByteString("SpanID", event.SpanID), zap.Uint64("eventID", event.EventID))
	return nil
}

func (p *MySqlProvider) prepareStatement(entry int, sql string) (*sql.Stmt, error) {
	var err error
	stmt, exists := p.preparedStatement[entry]
	if !exists {
		stmt, err = p.db.Prepare(sql)
		if err == nil {
			p.preparedStatement[entry] = stmt
		}
	}
	return stmt, err
}

func (p *MySqlProvider) SetTag(tag *common.Tag) error {
	logger := util.GetLogger("storage/mysql", "MySqlProvider::SetTag")
	stmt, err := p.prepareStatement(setTag, sp_SET_TAG)
	if err != nil {
		logger.Error("Unable to prepare statement", zap.Error(err))
		return err
	}
	result, err := stmt.Exec(tag.TraceID, tag.SpanID, tag.Tag, tag.StringValue)
	if err != nil {
		logger.Error("Error when attempting to execute prepared statement", zap.Error(err))
		return err
	}
	if rowsCount, err := result.RowsAffected(); rowsCount != 1 {
		logger.Warn("Number of rows affected is not 1.", zap.Int64("rowsCount", rowsCount), zap.Error(err))
	}
	logger.Debug("Saved tag", zap.String("tag", tag.Tag), zap.String("value", tag.StringValue), zap.ByteString("traceID", tag.TraceID), zap.ByteString("spanID", tag.SpanID))
	return nil
}

func (p *MySqlProvider) AddToSummaryField(span *common.Span, fieldName string, delta int) error {
	logger := util.GetLogger("storage/mysql", "MySqlProvider::AddToSummaryField")
	stmt, err := p.prepareStatement(addToSummaryField, sp_ADD_FIELD_IN_SUMMARY)
	if err != nil {
		logger.Error("Unable to prepare statement", zap.Error(err))
		return err
	}
	result, err := stmt.Exec(span.TraceID, span.SpanID, fieldName, delta)
	if err != nil {
		logger.Error("Error when attempting to execute prepared statement", zap.Error(err))
		return err
	}
	if rowsCount, err := result.RowsAffected(); rowsCount != 1 {
		logger.Warn("Number of rows affected is not 1.", zap.Int64("rowsCount", rowsCount), zap.Error(err))
	}
	logger.Sugar().Debugf("Added delta %d to field %d for traceID %x and spanID %x", delta, fieldName, span.TraceID, span.SpanID)
	return nil
}

func (p *MySqlProvider) StoreSpanData(span *orionproto.Span) error {
	return errors.New("not implemented")
}

func (p *MySqlProvider) Flush() error {
	return nil
}

func (p *MySqlProvider) Close() error {
	logger := util.GetLogger("storage/mysql", "MySqlProvider::Close")
	for _, stmt := range p.preparedStatement {
		err := stmt.Close()
		if err != nil {
			logger.Error("Error when closing prepared statement", zap.Error(err))
			return err
		}
	}
	logger.Info("Closed all prepared staements")
	err := p.db.Close()
	if err != nil {
		logger.Error("Error when closing database", zap.Error(err))
		return err
	}
	logger.Info("Closed database")
	return nil
}

func (p *MySqlProvider) Initialize() error {
	p.preparedStatement = make(map[int]*sql.Stmt)
	return p.connect()
}
