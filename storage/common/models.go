package common

import "time"

type EventType uint

const (
	UNKNOWN      EventType = 0
	LOG_DEBUG    EventType = 10
	LOG_INFO     EventType = 100
	LOG_WARN     EventType = 200
	LOG_ERROR    EventType = 300
	LOG_CRITICAL EventType = 400
	START_SPAN   EventType = 1000
	END_SPAN     EventType = 2000
)

type Trace struct {
	TraceID   []byte
	Name      string
	FirstSeen time.Duration
	LastSeen  time.Duration
}

type Span struct {
	TraceID     []byte
	SpanID      []byte
	ParentID    []byte
	StartTime   time.Duration
	EndTime     time.Duration
	ServiceName string
	SummaryJSON []byte
}

type Event struct {
	TraceID      []byte
	SpanID       []byte
	EventID      uint64
	Timestamp    time.Duration
	Type         EventType
	Location     string
	Message      string
	MetadataJSON []byte
}

type Tag struct {
	TraceID     []byte
	SpanID      []byte
	Tag         string
	StringValue string
}
