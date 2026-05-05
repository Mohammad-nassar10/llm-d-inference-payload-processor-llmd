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

package runningrequeststracker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datastore"
)

const (
	// RunningRequestsTrackerPluginType is the type identifier for this plugin.
	RunningRequestsTrackerPluginType = "RunningRequestsTracker"

	cycleStateKey = "running-requests-model-key"
	datastoreKey  = "running-requests"
	modelBodyKey  = "model"
)

// compile-time type validation
var _ framework.RequestProcessor = &RunningRequestsTrackerPlugin{}
var _ framework.ResponseProcessor = &RunningRequestsTrackerPlugin{}

// RunningRequestsCount is a per-model snapshot of in-flight request count, stored in the datastore.
type RunningRequestsCount struct {
	Count int64
}

// Clone implements datastore.Cloneable.
func (r RunningRequestsCount) Clone() datastore.Cloneable {
	return RunningRequestsCount{Count: r.Count}
}

func RunningRequestsTrackerPluginFactory(name string, rawParameters json.RawMessage, handle framework.Handle) (framework.Plugin, error) {
	plugin, err := NewRunningRequestsTrackerPlugin(name, rawParameters, handle)
	if err != nil {
		return nil, fmt.Errorf("failed to create '%s' plugin: %w", RunningRequestsTrackerPluginType, err)
	}
	return plugin, nil
}

func NewRunningRequestsTrackerPlugin(name string, config json.RawMessage, handle framework.Handle) (framework.Plugin, error) {
	if name == "" {
		return nil, fmt.Errorf("plugin name cannot be empty")
	}

	return &RunningRequestsTrackerPlugin{
		typedName: framework.TypedName{
			Type: RunningRequestsTrackerPluginType,
			Name: name,
		},
		handle: handle,
	}, nil
}

// RunningRequestsTrackerPlugin tracks per-model in-flight request counts.
//
// It maintains internal *atomic.Int64 counters (keyed by model name) that are
// incremented on ProcessRequest and decremented on ProcessResponse. After each
// update, a RunningRequestsCount snapshot is written to the datastore so that
// scoring plugins can read current load without holding any lock.
type RunningRequestsTrackerPlugin struct {
	typedName framework.TypedName
	handle    framework.Handle
	// counters maps model name -> *atomic.Int64 for lock-free increment/decrement.
	// AttributeMap.Get() returns a clone, so atomic read-modify-write must happen here.
	counters sync.Map
}

func (p *RunningRequestsTrackerPlugin) TypedName() framework.TypedName {
	return p.typedName
}

// ProcessRequest extracts the model name, increments its counter, and writes a snapshot to the datastore.
func (p *RunningRequestsTrackerPlugin) ProcessRequest(ctx context.Context, cycleState *framework.CycleState, request *framework.InferenceRequest) error {
	logger := log.FromContext(ctx)

	modelRaw, ok := request.Body[modelBodyKey]
	if !ok {
		logger.V(1).Info("model field not found in request body, skipping running-requests tracking")
		return nil
	}

	model, ok := modelRaw.(string)
	if !ok || model == "" {
		logger.V(1).Info("model field is not a non-empty string, skipping running-requests tracking", "value", modelRaw)
		return nil
	}

	cycleState.Write(cycleStateKey, model)

	count := p.atomicAdd(model, 1)

	p.writeSnapshot(ctx, model, count)
	logger.V(2).Info("incremented running requests", "model", model, "count", count)

	return nil
}

// ProcessResponse reads the model name from CycleState, decrements its counter, and writes a snapshot to the datastore.
func (p *RunningRequestsTrackerPlugin) ProcessResponse(ctx context.Context, cycleState *framework.CycleState, response *framework.InferenceResponse) error {
	logger := log.FromContext(ctx)

	modelRaw, err := cycleState.Read(cycleStateKey)
	if err != nil {
		logger.V(1).Info("model key not found in CycleState, skipping running-requests decrement")
		return nil
	}

	model, ok := modelRaw.(string)
	if !ok || model == "" {
		logger.V(1).Info("model key in CycleState has unexpected type, skipping decrement", "type", fmt.Sprintf("%T", modelRaw))
		return nil
	}

	count := p.atomicAdd(model, -1)
	if count < 0 {
		// Guard against decrement-without-increment; reset to 0.
		p.atomicAdd(model, -count)
		count = 0
	}

	p.writeSnapshot(ctx, model, count)
	logger.V(2).Info("decremented running requests", "model", model, "count", count)

	return nil
}

// atomicAdd loads or creates the counter for model, applies delta, and returns the new value.
func (p *RunningRequestsTrackerPlugin) atomicAdd(model string, delta int64) int64 {
	actual, _ := p.counters.LoadOrStore(model, &atomic.Int64{})
	return actual.(*atomic.Int64).Add(delta)
}

// writeSnapshot persists the current count to the datastore; errors are non-blocking.
func (p *RunningRequestsTrackerPlugin) writeSnapshot(ctx context.Context, model string, count int64) {
	logger := log.FromContext(ctx)

	store, err := p.handle.GetAttributeMap(datastoreKey)
	if err != nil {
		logger.Error(err, "failed to get datastore for running-requests tracking")
		return
	}

	store.Put(model, RunningRequestsCount{Count: count})
}
