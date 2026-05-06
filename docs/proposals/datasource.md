# Proposal: Observation DataSource

## Summary

Introduce a single `ObservationSource` plugin that parses each request/response body once and fans out to registered `ObservationExtractor` implementations. The first extractor tracks per-model in-flight request counts in the datastore.

## Problem

Every tracker plugin today independently re-parses the same fields from the request body. As more trackers are added, parsing overhead grows proportionally. There is also no shared structure for passing parsed data between them.

## Proposal

### New types in `pkg/framework`

```go
// DataSource is the common interface for all data producers.
// Embeds Plugin (TypedName) aligned with EPP's DataSource definition.
type DataSource interface {
    Plugin   // TypedName() TypedName
    Start(ctx context.Context) error
    Stop()
}

// ObservationDataSource is a DataSource driven by the request/response lifecycle.
// ObservationSource implements this interface.
// OutputType and ExtractorType enable the framework to wire compatible
// ObservationExtractors automatically — aligned with EPP's type-matching pattern.
type ObservationDataSource interface {
    DataSource
    RequestProcessor
    ResponseProcessor
    OutputType() reflect.Type    // type of observation this source produces
    ExtractorType() reflect.Type // expected extractor interface type
}

// ObservationExtractor transforms parsed request/response observations into
// per-model aggregate metrics stored in the datastore.
// Implementations must be goroutine-safe.
type ObservationExtractor interface {
    DataSource
    ExpectedInputType() reflect.Type // must match ObservationDataSource.OutputType()
    ExtractRequest(ctx context.Context, obs RequestObservation) error
    ExtractResponse(ctx context.Context, obs ResponseObservation) error
}

type RequestObservation struct {
    Model     string
    MaxTokens int
    StartTime time.Time
}

type ResponseObservation struct {
    Model            string
    MaxTokens        int // echoed from request for decrement tracking
    Duration         time.Duration
    PromptTokens     int
    CompletionTokens int
}
```

### `ObservationSource` — `pkg/plugins/observationsource/`

**ProcessRequest:**
1. Extract `model` and `max_tokens` from `request.Body` once.
2. Build `RequestObservation` and write to CycleState.
3. Fan out to all compatible `ObservationExtractor` plugins (matched by the framework via type).

**ProcessResponse:**
1. Read `RequestObservation` from CycleState. If absent, return nil.
2. Compute `Duration = time.Since(obs.StartTime)`.
3. Extract `prompt_tokens` / `completion_tokens` from `response.Body["usage"]` once.
4. Fan out to all compatible `ObservationExtractor` plugins.

Extractor errors are logged but do not fail the request.

### `ConcurrencyExtractor` — owns `"running-requests"`

Replaces `RunningRequestsTrackerPlugin`. Maintains `*atomic.Int64` counters keyed by model name. Writes `RunningRequestsCount` snapshots to the datastore after each update.

```go
type RunningRequestsCount struct {
    Requests int64
    Tokens   int64 // max_tokens sum across in-flight requests
}
```

### Registration in `runner.go`

Both `ObservationSource` and `ConcurrencyExtractor` are registered as regular plugins.
The framework wires them automatically by matching `ObservationSource.OutputType()` against `ConcurrencyExtractor.ExpectedInputType()`.

```go
framework.Register(observationsource.PluginType,    observationsource.PluginFactory)
framework.Register(concurrencyextractor.PluginType, concurrencyextractor.PluginFactory)
```

`RunningRequestsTrackerPlugin` is removed from the pipeline.

## Future

Additional extractors will be added as regular plugins without changes to existing code:
- **LatencyEMAExtractor** — per-model EMA of end-to-end latency
- **Polling-based sources** — periodic scraping of inference pool `/metrics` endpoints for queue depth, KV-cache utilization, and LoRA adapter info

## Implementation Steps

1. Add `DataSource`, `ObservationDataSource`, `ObservationExtractor`, `RequestObservation`, `ResponseObservation` to `pkg/framework`
2. Implement `ObservationSource` in `pkg/plugins/observationsource/`
3. Implement `ConcurrencyExtractor` as a standalone plugin (migrated from `RunningRequestsTrackerPlugin`)
4. Register both in `runner.go`; remove `RunningRequestsTrackerPlugin`
