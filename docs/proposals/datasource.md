# Proposal: Observation DataSource

## Summary

Introduce a `RequestNotificationSource` that collects per-request/response events
**outside the plugin pipeline** via a buffered channel, and drives registered
`RequestNotificationExtractor`s on a background tick to write aggregated statistics to the DataStore.

## Goal

Track runtime information about inference requests to help make better routing decisions —
for example, which model has the most in-flight requests, or which has the highest average
latency. This information is read by the Model Selector (Filter / Score / Pick) when
choosing where to route each request.

## Requirements

- **Non-blocking on the hot path** — collecting data must not add latency to request handling.
- **Single parse** — request and response bodies are already parsed by `server.go`; tracking logic must not re-parse them.
- **Multiple independent tracking logic** — different metrics (concurrency, latency, …) must be computable independently without coupling to each other.
- **Extensible** — adding a new metric must not require changes to existing tracking logic.
- **Off the plugin pipeline** — tracking is a background concern; it must not participate in the per-request plugin chain.


## Proposal

### Architecture

```
server.go  (request/response events already fire here)
  │  NotifyRequest(event)  →  buffered channel  (non-blocking, ~ns)
  │  NotifyResponse(event) →  buffered channel
  ▼
RequestNotificationSource
  │  holds the event buffer
  ▼
internal tick loop  (background goroutine, runs every N ms)
  │  drains buffer, calls extractors
  ├──▶ ConcurrencyExtractor  →  writes "running-requests"  topic
  └──▶ LatencyEMAExtractor   →  writes "inference-pool-latency" topic (future)
                                         │
                                    DataStore (AttributeMap per topic)
                                         │
                               Model Selector (Filter / Score / Pick)
```

### How notifications fire

`server.go` already processes every request and response. No new hooks are needed —
a non-blocking channel write is added alongside the existing pipeline dispatch:

```
request arrives in server.go
  │
  ├── notificationSrc.NotifyRequest(event)   ← non-blocking channel write (~ns)
  └── run plugin pipeline (unchanged)

response ready in server.go
  │
  ├── notificationSrc.NotifyResponse(event)  ← non-blocking channel write (~ns)
  └── run plugin pipeline (unchanged)
```


### New types in `pkg/framework`

```go
// DataSource is the base interface for all data sources.
type DataSource interface {
    Plugin                            // TypedName() TypedName
    Start(ctx context.Context) error
    Stop()
}

// RequestEvent is fired by server.go when a request arrives.
// Carries the already-parsed request so extractors can access any field
// they need (e.g. req.Body["model"], req.Body["max_tokens"]) without
// re-parsing the body. StartTime is the only field server.go adds.
type RequestEvent struct {
    Request   *InferenceRequest
    StartTime time.Time
}

// ResponseEvent is fired by server.go when a response completes.
// Duration is computed by server.go (response time − StartTime).
// All other fields are accessible via Request.Body and Response.Body.
type ResponseEvent struct {
    Request  *InferenceRequest
    Response *InferenceResponse
    Duration time.Duration
}

// RequestNotificationSource is an event-driven DataSource fired per request/response.
// It is NOT a pipeline plugin — events are written to a buffered channel by server.go,
// adding no latency to the hot path.
type RequestNotificationSource interface {
    DataSource                          // Plugin + Start + Stop
    NotifyRequest(event RequestEvent)   // non-blocking, called by server.go
    NotifyResponse(event ResponseEvent) // non-blocking, called by server.go
    RegisterExtractor(e RequestNotificationExtractor)
}

// RequestNotificationExtractor processes a batch of events collected since the
// last tick and updates the DataStore with fresh aggregates.
type RequestNotificationExtractor interface {
    DataSource
    ExtractRequests(ctx context.Context, events []RequestEvent) error
    ExtractResponses(ctx context.Context, events []ResponseEvent) error
}
```

### Intermediate data storage

Raw events live **only inside the buffered channels** of `RequestNotificationSource` —
in memory, never written to the DataStore. Once the tick loop drains them, they
are gone. Only the computed aggregates (counts, EMA) persist in the DataStore.

```
server.go fires event
  → buffered channel (raw, transient, inside DataSource)
      → tick loop drains every 100ms
          → Extractor computes aggregate (EMA, counter)
              → DataStore (persisted aggregate, read by Scorer)
                  → raw event is discarded
```


### Tick loop

Background goroutine started by `RequestNotificationSource.Start`. On each tick:
1. Drain all pending `RequestEvent`s and `ResponseEvent`s from the channels
2. Call `ExtractRequests` / `ExtractResponses` on each registered extractor
3. Extractors update their DataStore topics

Default tick interval: **100ms** (configurable).

### `ConcurrencyExtractor` — owns `"running-requests"`

Receives batches of `RequestEvent` (increment) and `ResponseEvent` (decrement).
Reads `model` and `max_tokens` from `event.Request.Body`.
Maintains `*atomic.Int64` counters per model. Writes `RunningRequestsCount` snapshots
to the DataStore after each batch.

```go
type RunningRequestsCount struct {
    Requests int64
    Tokens   int64 // max_tokens sum across in-flight requests
}
```

### Registration in `runner.go`

```go
notifySrc := observationsource.New("observation-source",
    observationsource.NewConcurrencyExtractor(handle),
)
if err := notifySrc.Start(ctx); err != nil { ... }
// pass notifySrc to the server so it can call NotifyRequest/NotifyResponse
```


## Tradeoffs

| | Current (pipeline plugin) | This proposal |
|---|---|---|
| Hot path overhead | DataStore write per request | Channel write (~ns), no DataStore write |
| Data freshness | Immediate | Up to one extractor tick (~100ms) |
| In-flight count accuracy | Exact | Nearly real-time (acceptable) |


## Future

- **LatencyExtractor** — per-model avg of end-to-end latency
- **PollingDataSource** — scrape inference pool `/metrics` for queue depth and KV-cache utilization

## Implementation Steps

1. Add `RequestEvent`, `ResponseEvent`, `RequestNotificationSource`, `RequestNotificationExtractor` to `pkg/framework`
2. Implement `RequestNotificationSource` (with internal tick loop) in `pkg/plugins`
3. Implement `ConcurrencyExtractor`
4. Add `NotifyRequest` / `NotifyResponse` calls to `server.go` (alongside existing pipeline dispatch)
5. Wire in `runner.go`; remove `RunningRequestsTrackerPlugin`
