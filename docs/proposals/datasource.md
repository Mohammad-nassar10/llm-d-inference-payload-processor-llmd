# Proposal: Observation DataSource

## Summary

Introduce a single `ObservationSource` plugin that parses each request/response body once and fans out to registered `ObservationExtractor` implementations. The first extractor tracks per-model in-flight request counts in the datastore.

## Problem

Every tracker plugin today independently re-parses the same fields from the request body. As more trackers are added, parsing overhead grows proportionally. There is also no shared structure for passing parsed data between them.

## Proposal


### New types in `pkg/framework`

```go
// DataSource is the common interface for all data producers.
// ObservationDataSource and PollingDataSource (future) both implement it.
// Embeds Plugin (TypedName) aligned with EPP's DataSource definition.
type DataSource interface {
    Plugin   // TypedName() TypedName
    Start(ctx context.Context) error
    Stop()
}

// ObservationDataSource is a DataSource driven by the request/response lifecycle.
// ObservationSource implements this interface.
type ObservationDataSource interface {
    DataSource
    framework.RequestProcessor
    framework.ResponseProcessor
    RegisterExtractor(e ObservationExtractor)
}

type ObservationExtractor interface {
    DataSource
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
3. Call `ExtractRequest` on each registered extractor.

**ProcessResponse:**
1. Read `RequestObservation` from CycleState. If absent, return nil.
2. Compute `Duration = time.Since(obs.StartTime)`.
3. Extract `prompt_tokens` / `completion_tokens` from `response.Body["usage"]` once.
4. Call `ExtractResponse` on each registered extractor.

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

```go
obsSrc := observationsource.New("observation-source", handle,
    observationsource.NewConcurrencyExtractor(handle),
)
r.requestPlugins  = append(r.requestPlugins,  obsSrc)
r.responsePlugins = append(r.responsePlugins, obsSrc)
```

`RunningRequestsTrackerPlugin` is removed from the pipeline.

## Future

Additional extractors will be added to the same source without changes to existing code:
- **LatencyEMAExtractor** — per-model EMA of end-to-end latency
- **Polling-based sources** — periodic scraping of inference pool `/metrics` endpoints for queue depth, KV-cache utilization, and LoRA adapter info

## Implementation Steps

1. Add `DataSource`, `ObservationDataSource`, `ObservationExtractor`, `RequestObservation`, `ResponseObservation` to `pkg/framework`
2. Implement `ObservationSource` in `pkg/plugins/observationsource/`
3. Implement `ConcurrencyExtractor` (migrated from `RunningRequestsTrackerPlugin`)
4. Register `ObservationSource` in `runner.go`; remove `RunningRequestsTrackerPlugin`
