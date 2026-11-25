// Code generated from the Trunk log Definitions by go generate. DO NOT EDIT.

package logging

type httpFactory func() (*TypedLog[*HTTPEvent], error)

// HTTP opens the TrieTrunk builder for log.http.
var HTTP httpFactory = func() (*TypedLog[*HTTPEvent], error) {
	return typedLog("log.http", TrieTrunk, func(r logRecord) *HTTPEvent { return &HTTPEvent{r} })
}

type HTTPEvent struct{ logRecord }

// Time sets time in ms.
func (e *HTTPEvent) Time(value uint64) *HTTPEvent {
	e.seed.Set("time", value)
	return e
}

// ObservedTime sets observed_time in ms.
func (e *HTTPEvent) ObservedTime(value uint64) *HTTPEvent {
	e.seed.Set("observed_time", value)
	return e
}

// Level sets level.
func (e *HTTPEvent) Level(value uint8) *HTTPEvent {
	e.seed.Set("level", value)
	return e
}

// LevelText sets level_text.
func (e *HTTPEvent) LevelText(value string) *HTTPEvent {
	e.seed.Set("level_text", value)
	return e
}

// Message sets message.
func (e *HTTPEvent) Message(value any) *HTTPEvent {
	e.seed.Set("message", value)
	return e
}

// TraceID sets trace_id.
func (e *HTTPEvent) TraceID(value [16]byte) *HTTPEvent {
	e.seed.Set("trace_id", value)
	return e
}

// SpanID sets span_id.
func (e *HTTPEvent) SpanID(value [8]byte) *HTTPEvent {
	e.seed.Set("span_id", value)
	return e
}

// TraceFlags sets trace_flags.
func (e *HTTPEvent) TraceFlags(value uint8) *HTTPEvent {
	e.seed.Set("trace_flags", value)
	return e
}

// Method sets method.
func (e *HTTPEvent) Method(value string) *HTTPEvent {
	e.seed.Set("method", value)
	return e
}

// Route sets route.
func (e *HTTPEvent) Route(value string) *HTTPEvent {
	e.seed.Set("route", value)
	return e
}

// Status sets status.
func (e *HTTPEvent) Status(value uint16) *HTTPEvent {
	e.seed.Set("status", value)
	return e
}

// Duration sets duration in us.
func (e *HTTPEvent) Duration(value uint32) *HTTPEvent {
	e.seed.Set("duration", value)
	return e
}

// Field adds an application attribute.
func (e *HTTPEvent) Field(name string, value any) *HTTPEvent {
	e.seed.Field(name, value)
	return e
}

// OpenTelemetry opens the OpenTelemetry builder for log.http.
func (httpFactory) OpenTelemetry() (*TypedLog[*OTelHTTPEvent], error) { return OTelHTTP() }

// OTelHTTP opens the OpenTelemetry builder for log.http.
func OTelHTTP() (*TypedLog[*OTelHTTPEvent], error) {
	return typedLog("log.http", OpenTelemetry, func(r logRecord) *OTelHTTPEvent { return &OTelHTTPEvent{r} })
}

type OTelHTTPEvent struct{ logRecord }

// Timestamp sets Timestamp in ns.
func (e *OTelHTTPEvent) Timestamp(value uint64) *OTelHTTPEvent {
	e.seed.Set("Timestamp", value)
	return e
}

// ObservedTimestamp sets ObservedTimestamp in ns.
func (e *OTelHTTPEvent) ObservedTimestamp(value uint64) *OTelHTTPEvent {
	e.seed.Set("ObservedTimestamp", value)
	return e
}

// SeverityNumber sets SeverityNumber.
func (e *OTelHTTPEvent) SeverityNumber(value uint8) *OTelHTTPEvent {
	e.seed.Set("SeverityNumber", value)
	return e
}

// SeverityText sets SeverityText.
func (e *OTelHTTPEvent) SeverityText(value string) *OTelHTTPEvent {
	e.seed.Set("SeverityText", value)
	return e
}

// Body sets Body.
func (e *OTelHTTPEvent) Body(value any) *OTelHTTPEvent {
	e.seed.Set("Body", value)
	return e
}

// TraceID sets TraceId.
func (e *OTelHTTPEvent) TraceID(value [16]byte) *OTelHTTPEvent {
	e.seed.Set("TraceId", value)
	return e
}

// SpanID sets SpanId.
func (e *OTelHTTPEvent) SpanID(value [8]byte) *OTelHTTPEvent {
	e.seed.Set("SpanId", value)
	return e
}

// TraceFlags sets TraceFlags.
func (e *OTelHTTPEvent) TraceFlags(value uint8) *OTelHTTPEvent {
	e.seed.Set("TraceFlags", value)
	return e
}

// HTTPRequestMethod sets http.request.method.
func (e *OTelHTTPEvent) HTTPRequestMethod(value string) *OTelHTTPEvent {
	e.seed.Set("http.request.method", value)
	return e
}

// HTTPRoute sets http.route.
func (e *OTelHTTPEvent) HTTPRoute(value string) *OTelHTTPEvent {
	e.seed.Set("http.route", value)
	return e
}

// HTTPResponseStatusCode sets http.response.status_code.
func (e *OTelHTTPEvent) HTTPResponseStatusCode(value uint16) *OTelHTTPEvent {
	e.seed.Set("http.response.status_code", value)
	return e
}

// Duration sets duration in us.
func (e *OTelHTTPEvent) Duration(value uint32) *OTelHTTPEvent {
	e.seed.Set("duration", value)
	return e
}

// Field adds an application attribute.
func (e *OTelHTTPEvent) Field(name string, value any) *OTelHTTPEvent {
	e.seed.Field(name, value)
	return e
}

type queryFactory func() (*TypedLog[*QueryEvent], error)

// Query opens the TrieTrunk builder for log.query.
var Query queryFactory = func() (*TypedLog[*QueryEvent], error) {
	return typedLog("log.query", TrieTrunk, func(r logRecord) *QueryEvent { return &QueryEvent{r} })
}

type QueryEvent struct{ logRecord }

// Time sets time in ms.
func (e *QueryEvent) Time(value uint64) *QueryEvent {
	e.seed.Set("time", value)
	return e
}

// Level sets level.
func (e *QueryEvent) Level(value uint8) *QueryEvent {
	e.seed.Set("level", value)
	return e
}

// Operation sets operation.
func (e *QueryEvent) Operation(value string) *QueryEvent {
	e.seed.Set("operation", value)
	return e
}

// Source sets source.
func (e *QueryEvent) Source(value string) *QueryEvent {
	e.seed.Set("source", value)
	return e
}

// Query sets query.
func (e *QueryEvent) Query(value string) *QueryEvent {
	e.seed.Set("query", value)
	return e
}

// Status sets status.
func (e *QueryEvent) Status(value string) *QueryEvent {
	e.seed.Set("status", value)
	return e
}

// Error sets error.
func (e *QueryEvent) Error(value string) *QueryEvent {
	e.seed.Set("error", value)
	return e
}

// UserAgent sets user_agent.
func (e *QueryEvent) UserAgent(value string) *QueryEvent {
	e.seed.Set("user_agent", value)
	return e
}

// From sets from.
func (e *QueryEvent) From(value uint64) *QueryEvent {
	e.seed.Set("from", value)
	return e
}

// To sets to.
func (e *QueryEvent) To(value uint64) *QueryEvent {
	e.seed.Set("to", value)
	return e
}

// Rows sets rows.
func (e *QueryEvent) Rows(value uint32) *QueryEvent {
	e.seed.Set("rows", value)
	return e
}

// Duration sets duration in us.
func (e *QueryEvent) Duration(value float64) *QueryEvent {
	e.seed.Set("duration", value)
	return e
}

// QueryCheckpoint preserves omitted members and explicitly supplied zeroes.
type QueryCheckpoint struct {
	Name     *string  `cbor:"name,omitempty"`
	Start    *float64 `cbor:"start,omitempty"`
	Duration *float64 `cbor:"duration,omitempty"`
}

// Checkpoints sets checkpoints in us.
func (e *QueryEvent) Checkpoints(value ...QueryCheckpoint) *QueryEvent {
	if value == nil {
		value = []QueryCheckpoint{}
	}
	e.seed.Set("checkpoints", value)
	return e
}

// Field adds an application attribute.
func (e *QueryEvent) Field(name string, value any) *QueryEvent {
	e.seed.Field(name, value)
	return e
}

// OpenTelemetry opens the OpenTelemetry builder for log.query.
func (queryFactory) OpenTelemetry() (*TypedLog[*OTelQueryEvent], error) { return OTelQuery() }

// OTelQuery opens the OpenTelemetry builder for log.query.
func OTelQuery() (*TypedLog[*OTelQueryEvent], error) {
	return typedLog("log.query", OpenTelemetry, func(r logRecord) *OTelQueryEvent { return &OTelQueryEvent{r} })
}

type OTelQueryEvent struct{ logRecord }

// Timestamp sets Timestamp in ns.
func (e *OTelQueryEvent) Timestamp(value uint64) *OTelQueryEvent {
	e.seed.Set("Timestamp", value)
	return e
}

// SeverityNumber sets SeverityNumber.
func (e *OTelQueryEvent) SeverityNumber(value uint8) *OTelQueryEvent {
	e.seed.Set("SeverityNumber", value)
	return e
}

// Operation sets operation.
func (e *OTelQueryEvent) Operation(value string) *OTelQueryEvent {
	e.seed.Set("operation", value)
	return e
}

// Source sets source.
func (e *OTelQueryEvent) Source(value string) *OTelQueryEvent {
	e.seed.Set("source", value)
	return e
}

// Query sets query.
func (e *OTelQueryEvent) Query(value string) *OTelQueryEvent {
	e.seed.Set("query", value)
	return e
}

// Status sets status.
func (e *OTelQueryEvent) Status(value string) *OTelQueryEvent {
	e.seed.Set("status", value)
	return e
}

// Error sets error.
func (e *OTelQueryEvent) Error(value string) *OTelQueryEvent {
	e.seed.Set("error", value)
	return e
}

// UserAgent sets user_agent.
func (e *OTelQueryEvent) UserAgent(value string) *OTelQueryEvent {
	e.seed.Set("user_agent", value)
	return e
}

// From sets from.
func (e *OTelQueryEvent) From(value uint64) *OTelQueryEvent {
	e.seed.Set("from", value)
	return e
}

// To sets to.
func (e *OTelQueryEvent) To(value uint64) *OTelQueryEvent {
	e.seed.Set("to", value)
	return e
}

// Rows sets rows.
func (e *OTelQueryEvent) Rows(value uint32) *OTelQueryEvent {
	e.seed.Set("rows", value)
	return e
}

// Duration sets duration in us.
func (e *OTelQueryEvent) Duration(value float64) *OTelQueryEvent {
	e.seed.Set("duration", value)
	return e
}

// Checkpoints sets checkpoints in us.
func (e *OTelQueryEvent) Checkpoints(value ...QueryCheckpoint) *OTelQueryEvent {
	if value == nil {
		value = []QueryCheckpoint{}
	}
	e.seed.Set("checkpoints", value)
	return e
}

// Field adds an application attribute.
func (e *OTelQueryEvent) Field(name string, value any) *OTelQueryEvent {
	e.seed.Field(name, value)
	return e
}
