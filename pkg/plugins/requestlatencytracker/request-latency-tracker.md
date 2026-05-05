# Request Latency Tracker Plugin

## Package
`pkg/plugins/requestlatencytracker`

## Purpose

Tracks the processing time for each inference request by recording the start time during request processing and calculating the total duration during response processing. Also captures prompt and completion token counts from the response body so that consumers can compute token-normalized latency metrics (e.g. tokens/sec, TPOT proxy). Stores latency data in the datastore for future use by scoring and routing components.

## Related Specifications

- Uses [Datastore](../../pkg/framework/datastore/datastore.md) - for storing latency data across requests
- Implements [Plugin Interfaces](../../pkg/framework/plugins.go) - RequestProcessor and ResponseProcessor

## Non-Goals

- Computing moving averages (future enhancement)
- Tracking TTFT separately — requires first-chunk detection in streaming responses (future enhancement)
- Tracking per-inference-pool latencies (future enhancement)
- KV-cache hit tracking (future enhancement)
- Request prefix hashing (future enhancement)
- Data retention/trimming policies (future enhancement)

## Core Components

### RequestLatencyTrackerPlugin (`request_latency_tracker.go`)

A plugin that implements both RequestProcessor and ResponseProcessor interfaces to track end-to-end request processing time and token counts.

```go
type RequestLatencyTrackerPlugin struct {
    typedName framework.TypedName
    handle    framework.Handle
}

func NewRequestLatencyTrackerPlugin(name string, config json.RawMessage, handle framework.Handle) (framework.Plugin, error)
func (p *RequestLatencyTrackerPlugin) TypedName() framework.TypedName
func (p *RequestLatencyTrackerPlugin) ProcessRequest(ctx context.Context, cycleState *framework.CycleState, request *framework.InferenceRequest) error
func (p *RequestLatencyTrackerPlugin) ProcessResponse(ctx context.Context, cycleState *framework.CycleState, response *framework.InferenceResponse) error
```

**Behavior:**
- Implements both RequestProcessor and ResponseProcessor interfaces
- Records start time in CycleState during request processing
- Calculates duration and extracts token counts from the response body during response processing
- Token counts (`prompt_tokens`, `completion_tokens`) are read from `response.Body["usage"]`; missing or malformed usage data is silently skipped (zero values)
- Uses datastore topic `"request-latencies"` for storing latency data
- Stores latencies keyed by timestamp nanoseconds
- No configuration required (empty JSON config accepted)
- Thread-safe through datastore's built-in concurrency protection

**NewRequestLatencyTrackerPlugin Algorithm:**
1. Validate name is non-empty, return error if empty
2. Create RequestLatencyTrackerPlugin instance with name and handle
3. Return plugin instance and nil error

**ProcessRequest Algorithm:**
1. Record current time using `time.Now()`
2. Store start time in CycleState with key `"request-start-time"`
3. Return nil (no errors expected)

**ProcessResponse Algorithm:**
1. Retrieve start time from CycleState using key `"request-start-time"`
2. If start time not found, log warning and return nil (graceful degradation)
3. Calculate duration: `time.Since(startTime)`
4. Extract token counts from `response.Body["usage"]`:
   - Cast `usage` to `map[string]any`; if absent or wrong type, use zero values
   - Extract `prompt_tokens` as int; default 0 if absent or non-numeric
   - Extract `completion_tokens` as int; default 0 if absent or non-numeric
5. Get datastore AttributeMap using `handle.GetAttributeMap("request-latencies")`
6. If error getting datastore, log error and return nil (non-blocking)
7. Create LatencyData struct with duration, timestamp, and token counts
8. Store in datastore with key format `"latency-{timestamp-nanos}"`
9. Return nil

### LatencyData Struct (`request_latency_tracker.go`)

Data structure for storing latency and token information, implements Cloneable interface.

```go
type LatencyData struct {
    Duration         time.Duration
    Timestamp        time.Time
    PromptTokens     int
    CompletionTokens int
}

func (ld LatencyData) Clone() datastore.Cloneable
```

**Behavior:**
- Implements `datastore.Cloneable` interface for data isolation
- Stores end-to-end request processing duration
- Stores timestamp when latency was recorded
- Stores prompt and completion token counts from the OpenAI-compatible `usage` response field
- All fields are value types; no deep copy required
- Enables consumers to compute: throughput (`CompletionTokens / Duration`), TPOT proxy (`Duration / CompletionTokens`), prefill cost proxy (`PromptTokens`)

**Clone Algorithm:**
1. Return new LatencyData with same Duration, Timestamp, PromptTokens, and CompletionTokens values
2. No deep copy needed as all fields are value types

### Plugin Factory (`request_latency_tracker.go`)

Factory function for plugin registration.

```go
const RequestLatencyTrackerPluginType = "RequestLatencyTracker"

func RequestLatencyTrackerPluginFactory(name string, config json.RawMessage, handle framework.Handle) (framework.Plugin, error)
```

**Behavior:**
- Registered in plugin registry with type `"RequestLatencyTracker"`
- Accepts empty or minimal JSON configuration
- Returns plugin instance implementing both RequestProcessor and ResponseProcessor

**RequestLatencyTrackerPluginFactory Algorithm:**
1. Call NewRequestLatencyTrackerPlugin with provided name, config, and handle
2. Return result from NewRequestLatencyTrackerPlugin

## Configuration

No configuration required. Plugin accepts empty JSON config:

```json
{}
```

**Usage in plugin specification:**
```bash
--plugin RequestLatencyTracker:latency-tracker:{}
```

**Registration in runner:**
```go
// In cmd/runner/runner.go registerInTreePlugins()
framework.Register(
    requestlatencytracker.RequestLatencyTrackerPluginType,
    requestlatencytracker.RequestLatencyTrackerPluginFactory,
)
```

## Unit Tests

### Plugin Tests (`request_latency_tracker_test.go`)

| Scenario | Input | Expected |
|----------|-------|----------|
| Create plugin with valid name | name="test", config={}, handle=mockHandle | Plugin created, no error |
| Create plugin with empty name | name="", config={}, handle=mockHandle | Error returned |
| ProcessRequest stores start time | Valid request, empty CycleState | Start time stored in CycleState with key `"request-start-time"` |
| ProcessResponse calculates duration and stores tokens | CycleState with start time, response with usage={prompt_tokens:10, completion_tokens:50} | Duration calculated, PromptTokens=10, CompletionTokens=50 stored in datastore |
| ProcessResponse with missing usage field | CycleState with start time, response body has no `usage` key | Duration stored, PromptTokens=0, CompletionTokens=0; no error |
| ProcessResponse with partial usage field | Response usage has `prompt_tokens` but no `completion_tokens` | PromptTokens set, CompletionTokens=0; no error |
| ProcessResponse without start time | CycleState without start time, valid response | No error, graceful degradation (logs warning) |
| ProcessResponse with datastore error | CycleState with start time, datastore returns error | No error, graceful degradation (logs error) |
| LatencyData Clone | LatencyData with all fields populated | Clone has same values, independent instance |
| Concurrent ProcessRequest calls | 10 goroutines calling ProcessRequest | All store start times, no race conditions |
| Concurrent ProcessResponse calls | 10 goroutines calling ProcessResponse with usage data | All store latency data with token counts, no race conditions |
| TypedName returns correct value | Plugin with name="test" | Returns TypedName{Type: "RequestLatencyTracker", Name: "test"} |

## Dependencies

- `context` — for context.Context in plugin methods
- `encoding/json` — for json.RawMessage config parameter
- `fmt` — for error formatting and key generation
- `time` — for time.Now(), time.Duration, time.Time
- `github.com/llm-d/llm-d-inference-payload-processor/pkg/framework` — for plugin interfaces, CycleState, Handle
- `github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datastore` — for Cloneable interface, AttributeMap
- `sigs.k8s.io/controller-runtime/pkg/log` — for structured logging
