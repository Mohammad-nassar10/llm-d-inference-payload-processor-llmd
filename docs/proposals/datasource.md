# Proposal: Data Layer

## Summary

Introduce an **Async Data Layer** — a background observation pipeline that runs
**outside the critical request path**. It collects runtime events fired by the producer
(currently `server.go`), buffers them off the hot path, and dispatches them to registered
`Extractor`s that compute aggregates and write them to the DataStore for the Model Selector.

## Goal

Track runtime information about inference requests to help make better routing decisions —
for example, which model has the least in-flight requests or the lowest average latency.
This information is read by the Model Selector (Filter / Score / Pick) when choosing
where to route each request.

## Requirements

- **Non-blocking on the critical path** - data collection must add zero latency to request handling.
- **Multiple independent extractors** - different metrics must be computable independently without coupling to each other.
- **Extensible** - adding a new metric or event type must not require changes to existing extractors or the producer.
- **Off the plugin pipeline** - the data layer is a background concern; it must not participate in the per-request plugin chain.


## Proposal

### Architecture

```mermaid
flowchart TD
    P["Producer\n(server.go)"]
    PP["Plugin Pipeline\nFilter → Score → Pick"]
    NS["NotificationSource\nchan Event  (buffered)"]
    TL["tick loop\nevery 100ms"]
    ExtA["ConcurrencyExtractor"]
    ExtB["LatencyExtractor\n(future)"]
    DS1[("DataStore\nrunning-requests")]
    DS2[("DataStore\npool-latency")]
    MS["Model Selector"]

    P -->|"Notify(event)  ~ns"| NS
    P --> PP
    NS --> TL
    TL -->|"[]Event batch"| ExtA
    TL -->|"[]Event batch"| ExtB
    ExtA --> DS1
    ExtB --> DS2
    DS1 --> MS
    DS2 --> MS
    PP -->|reads| MS
```

The **producer** (currently `server.go`) fires an `Event` on each request and response —
a non-blocking channel write (~ns). The `NotificationSource` buffers it. A background
tick loop drains the buffer every 100ms and fans the batch to all registered `Extractor`s.
Each extractor switches on `Event.Type` and handles what it understands, ignoring the rest.


### Types (`pkg/framework`)

```go
type DataSource interface {
    Plugin                            // TypedName() TypedName
    Start(ctx context.Context) error
    Stop()
}

// Event is the uniform carrier of all data layer events.
// Type identifies what happened; Payload holds the event-specific data.
type Event struct {
    Type    EventType
    Payload any
}

// NotificationSource buffers events off the hot path and dispatches
// batches to registered Extractors on each tick.
type NotificationSource interface {
    DataSource
    Notify(e Event)                // non-blocking, called by the producer
    RegisterExtractor(e Extractor)
}

// Extractor switches on Event.Type and handles only the types it understands.
type Extractor interface {
    DataSource
    Extract(ctx context.Context, events []Event) error
}
```

See [Appendix](#appendix) for payload struct definitions and a full extractor example.

### Registration (`runner.go`)

```go
src := datalayer.NewNotificationSource("notification-source")
src.RegisterExtractor(datalayer.NewConcurrencyExtractor(handle))
if err := src.Start(ctx); err != nil { ... }
// pass src to the producer so it can call src.Notify(...)
```

**Next:** move extractors registration to a config struct or CLI flags so operators can enable/disable metrics without recompiling.


## Future

- **LatencyExtractor** - handles `ResponseEventType`; per-model avg latency; owns `"pool-latency"` topic
- **PollingDataSource** - polls inference pool `/metrics` on a ticker; same `Extractor` interface

## Implementation Steps

1. Add `DataSource`, `Event`, `NotificationSource`, `Extractor`, payload types to `pkg/framework`
2. Implement `NotificationSource` (buffered channel + tick loop) in `pkg/plugins/datalayer/`
3. Implement `ConcurrencyExtractor`
4. Add `src.Notify(...)` calls to the producer alongside existing pipeline dispatch
5. Wire in `runner.go`

---

## Appendix

### Payload types

```go
// RequestPayload is the Payload for RequestEventType.
// Carries the already-parsed request — no re-parsing needed.
// StartTime is the only field the producer adds.
type RequestPayload struct {
    Request   *InferenceRequest
    StartTime time.Time
}

// ResponsePayload is the Payload for ResponseEventType.
// Duration is computed by the producer (time.Since(StartTime)).
// All response body fields are accessible via Response.Body.
type ResponsePayload struct {
    Request  *InferenceRequest
    Response *InferenceResponse
    Duration time.Duration
}
```

### Extractor definitions

#### `ConcurrencyExtractor` — owns `"running-requests"`

| | |
|---|---|
| Handles | `RequestEventType` (increment), `ResponseEventType` (decrement) |
| Reads | `model`, `max_tokens` from `RequestPayload.Request.Body` |
| State | `map[model]*atomic.Int64` — in-flight counters per model |
| Writes | `RunningRequestsCount{Requests int64, Tokens int64}` per model |

```go
type RunningRequestsCount struct {
    Requests int64
    Tokens   int64 // sum of max_tokens across in-flight requests
}
```
