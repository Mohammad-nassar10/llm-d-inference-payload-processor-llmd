/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package requestlatencytracker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datastore"
)

const (
	// RequestLatencyTrackerPluginType is the type identifier for this plugin
	RequestLatencyTrackerPluginType = "RequestLatencyTracker"

	cycleStateKey = "request-start-time"
	datastoreKey  = "request-latencies"
)

// compile-time type validation
var _ framework.RequestProcessor = &RequestLatencyTrackerPlugin{}
var _ framework.ResponseProcessor = &RequestLatencyTrackerPlugin{}

// LatencyData stores latency and token information for a single request.
// Token counts enable consumers to compute token-normalized metrics
// (e.g. throughput = CompletionTokens/Duration, TPOT proxy = Duration/CompletionTokens).
type LatencyData struct {
	Duration         time.Duration
	Timestamp        time.Time
	PromptTokens     int
	CompletionTokens int
}

// Clone implements datastore.Cloneable. All fields are value types; no deep copy needed.
func (ld LatencyData) Clone() datastore.Cloneable {
	return LatencyData{
		Duration:         ld.Duration,
		Timestamp:        ld.Timestamp,
		PromptTokens:     ld.PromptTokens,
		CompletionTokens: ld.CompletionTokens,
	}
}

func RequestLatencyTrackerPluginFactory(name string, rawParameters json.RawMessage, handle framework.Handle) (framework.Plugin, error) {
	// Config parameter is accepted but not used (empty JSON is valid)
	plugin, err := NewRequestLatencyTrackerPlugin(name, rawParameters, handle)
	if err != nil {
		return nil, fmt.Errorf("failed to create '%s' plugin: %w", RequestLatencyTrackerPluginType, err)
	}
	return plugin, nil
}

func NewRequestLatencyTrackerPlugin(name string, config json.RawMessage, handle framework.Handle) (framework.Plugin, error) {
	if name == "" {
		return nil, fmt.Errorf("plugin name cannot be empty")
	}

	return &RequestLatencyTrackerPlugin{
		typedName: framework.TypedName{
			Type: RequestLatencyTrackerPluginType,
			Name: name,
		},
		handle: handle,
	}, nil
}

// RequestLatencyTrackerPlugin tracks end-to-end request processing time.
type RequestLatencyTrackerPlugin struct {
	typedName framework.TypedName
	handle    framework.Handle
}

func (p *RequestLatencyTrackerPlugin) TypedName() framework.TypedName {
	return p.typedName
}

// ProcessRequest records the start time for latency tracking.
func (p *RequestLatencyTrackerPlugin) ProcessRequest(ctx context.Context, cycleState *framework.CycleState, request *framework.InferenceRequest) error {
	startTime := time.Now()
	cycleState.Write(cycleStateKey, startTime)
	return nil
}

// ProcessResponse calculates the request duration and stores it in the datastore.
func (p *RequestLatencyTrackerPlugin) ProcessResponse(ctx context.Context, cycleState *framework.CycleState, response *framework.InferenceResponse) error {
	logger := log.FromContext(ctx)

	startTimeRaw, err := cycleState.Read(cycleStateKey)
	if err != nil {
		logger.V(1).Info("start time not found in CycleState, skipping latency tracking")
		return nil
	}

	startTime, ok := startTimeRaw.(time.Time)
	if !ok {
		logger.V(1).Info("start time has unexpected type, skipping latency tracking", "type", fmt.Sprintf("%T", startTimeRaw))
		return nil
	}

	duration := time.Since(startTime)

	store, err := p.handle.GetAttributeMap(datastoreKey)
	if err != nil {
		logger.Error(err, "failed to get datastore for latency tracking")
		return nil // Non-blocking: don't fail the request
	}

	promptTokens, completionTokens := extractTokenCounts(response)

	latencyData := LatencyData{
		Duration:         duration,
		Timestamp:        time.Now(),
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
	}

	key := fmt.Sprintf("latency-%d", latencyData.Timestamp.UnixNano())
	store.Put(key, latencyData)

	logger.V(2).Info("recorded request latency", "duration", duration, "promptTokens", promptTokens, "completionTokens", completionTokens, "key", key)

	return nil
}

// extractTokenCounts reads prompt_tokens and completion_tokens from the OpenAI-compatible
// usage field in the response body. Returns zero values if the field is absent or malformed.
func extractTokenCounts(response *framework.InferenceResponse) (promptTokens, completionTokens int) {
	usageRaw, ok := response.Body["usage"]
	if !ok {
		return 0, 0
	}
	usage, ok := usageRaw.(map[string]any)
	if !ok {
		return 0, 0
	}
	promptTokens = intFromUsage(usage, "prompt_tokens")
	completionTokens = intFromUsage(usage, "completion_tokens")
	return promptTokens, completionTokens
}

// intFromUsage extracts an integer value from a usage map, returning 0 if absent or non-numeric.
func intFromUsage(usage map[string]any, key string) int {
	v, ok := usage[key]
	if !ok {
		return 0
	}
	// JSON numbers unmarshal as float64.
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	}
	return 0
}
