# Running Requests Tracker Plugin

## Package
`pkg/plugins/runningrequeststracker`

## Purpose

Tracks the number of in-flight inference requests per model by atomically incrementing a counter when a request enters the system and decrementing it when the response returns. Stores snapshots of the per-model counts in the datastore so that scoring and routing components can observe current load.

## Related Specifications

- Uses [Datastore](../../pkg/framework/datastore/datastore.md) - for storing running-request snapshots across requests
- Implements [Plugin Interfaces](../../pkg/framework/plugins.go) - RequestProcessor and ResponseProcessor

## Non-Goals

- Computing moving averages over running-request counts (future enhancement)
- Tracking per-endpoint running requests (future enhancement; requires endpoint info in response)
- Evicting stale model keys from the internal counter map (future enhancement)
- Coordinating counts across multiple IPP replicas (each pod maintains its own view)

## Core Components

### RunningRequestsTrackerPlugin (`running_requests_tracker.go`)

A plugin that implements both RequestProcessor and ResponseProcessor to maintain per-model in-flight request counts.

```go
type RunningRequestsTrackerPlugin struct {
    typedName framework.TypedName
    handle    framework.Handle
    counters  sync.Map // key: model name (string), value: *atomic.Int64
}

func NewRunningRequestsTrackerPlugin(name string, config json.RawMessage, handle framework.Handle) (framework.Plugin, error)
func (p *RunningRequestsTrackerPlugin) TypedName() framework.TypedName
func (p *RunningRequestsTrackerPlugin) ProcessRequest(ctx context.Context, cycleState *framework.CycleState, request *framework.InferenceRequest) error
func (p *RunningRequestsTrackerPlugin) ProcessResponse(ctx context.Context, cycleState *framework.CycleState, response *framework.InferenceResponse) error
```

**Behavior:**
- Implements both RequestProcessor and ResponseProcessor interfaces
- Extracts the `model` field from the request body at request time; stores it in CycleState so ProcessResponse can find it
- Atomically increments the per-model counter in ProcessRequest; atomically decrements in ProcessResponse
- After each increment/decrement, writes the updated count as a `RunningRequestsCount` snapshot to the datastore topic `"running-requests"`, keyed by model name
- If the `model` field is absent or empty, logs a warning and skips tracking for that request (no increment, no decrement)
- If the datastore is unavailable, logs the error and continues without failing the request
- Thread-safe: per-model counters are `*atomic.Int64` values stored in a `sync.Map`; the AttributeMap provides additional isolation for datastore reads

### RunningRequestsCount Struct (`running_requests_tracker.go`)

Snapshot of the current running-request count for a single model, stored in the datastore.

```go
type RunningRequestsCount struct {
    Count int64
}

func (r RunningRequestsCount) Clone() datastore.Cloneable
```

**Behavior:**
- Implements `datastore.Cloneable` for data isolation
- `int64` is a value type; no deep copy is required

### Plugin Factory (`running_requests_tracker.go`)

```go
const RunningRequestsTrackerPluginType = "RunningRequestsTracker"

func RunningRequestsTrackerPluginFactory(name string, config json.RawMessage, handle framework.Handle) (framework.Plugin, error)
```

**Behavior:**
- Registered in plugin registry with type `"RunningRequestsTracker"`
- Accepts empty or minimal JSON configuration

## Configuration

No configuration required. Plugin accepts empty JSON config:

```json
{}
```

**Usage in plugin specification:**
```bash
--plugin RunningRequestsTracker:running-requests-tracker:{}
```

**Registration in runner:**
```go
// In cmd/runner/runner.go registerInTreePlugins()
framework.Register(
    runningrequeststracker.RunningRequestsTrackerPluginType,
    runningrequeststracker.RunningRequestsTrackerPluginFactory,
)
```

## Internal Design Notes

The plugin cannot perform atomic read-modify-write through `AttributeMap` because `Get()` always returns a clone. Instead, it keeps per-model `*atomic.Int64` counters in an internal `sync.Map`. Each counter is created lazily on first use via a `LoadOrStore` pattern. After updating the counter, the plugin writes a snapshot `RunningRequestsCount{Count: <current>}` to the datastore so that scorers can read it without holding any lock.

The CycleState key `"running-requests-model-key"` carries the model name from ProcessRequest to ProcessResponse within a single request lifecycle.

## Unit Tests

### Plugin Tests (`running_requests_tracker_test.go`)

| Scenario | Input | Expected |
|----------|-------|----------|
| Create plugin with valid name | name="test", config={}, handle=mockHandle | Plugin created, no error |
| Create plugin with empty name | name="", config={}, handle=mockHandle | Error returned |
| ProcessRequest increments counter | Request with model="llama3", empty CycleState | Counter for "llama3" is 1; datastore has RunningRequestsCount{Count:1} |
| ProcessResponse decrements counter | CycleState with model key, counter at 1 | Counter for "llama3" is 0; datastore has RunningRequestsCount{Count:0} |
| ProcessRequest missing model field | Request with no model field | No increment, no error; datastore unchanged |
| ProcessResponse missing model key | CycleState without model key | No decrement, no error; datastore unchanged |
| ProcessResponse with datastore error | Datastore returns error | Decrement still applied to internal counter; no error returned |
| RunningRequestsCount Clone | RunningRequestsCount{Count:5} | Clone returns independent value with Count==5 |
| Concurrent ProcessRequest calls | 10 goroutines with same model | Counter reaches 10; datastore reflects a count in [1,10] |
| Concurrent request-response pairs | 10 goroutines each increment then decrement | Counter returns to 0 after all goroutines complete |
| TypedName returns correct value | Plugin with name="test" | Returns TypedName{Type: "RunningRequestsTracker", Name: "test"} |

## Dependencies

- `context` — for context.Context in plugin methods
- `encoding/json` — for json.RawMessage config parameter
- `fmt` — for error formatting
- `sync` — for sync.Map (internal counter map)
- `sync/atomic` — for atomic.Int64 (per-model counters)
- `github.com/llm-d/llm-d-inference-payload-processor/pkg/framework` — for plugin interfaces, CycleState, Handle
- `github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datastore` — for Cloneable interface, AttributeMap
- `sigs.k8s.io/controller-runtime/pkg/log` — for structured logging
