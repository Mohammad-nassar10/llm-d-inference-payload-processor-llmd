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
	"sync"
	"sync/atomic"
	"testing"

	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datastore"
)

// mockHandle implements framework.Handle for testing.
type mockHandle struct {
	store datastore.AttributeMap
	err   error
}

func (m *mockHandle) Context() context.Context {
	return context.Background()
}

func (m *mockHandle) Client() client.Client {
	return nil
}

func (m *mockHandle) ReconcilerBuilder() *ctrlbuilder.Builder {
	return nil
}

func (m *mockHandle) GetAttributeMap(key string) (datastore.AttributeMap, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.store, nil
}

func newPlugin(t *testing.T, h *mockHandle) *RunningRequestsTrackerPlugin {
	t.Helper()
	p, err := NewRunningRequestsTrackerPlugin("test", json.RawMessage("{}"), h)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}
	return p.(*RunningRequestsTrackerPlugin)
}

func TestCreatePluginWithValidName(t *testing.T) {
	handle := &mockHandle{store: datastore.NewAttributes()}

	plugin, err := NewRunningRequestsTrackerPlugin("test", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if plugin == nil {
		t.Fatal("expected non-nil plugin")
	}

	tn := plugin.(*RunningRequestsTrackerPlugin).TypedName()
	if tn.Type != RunningRequestsTrackerPluginType {
		t.Errorf("expected type %q, got %q", RunningRequestsTrackerPluginType, tn.Type)
	}
	if tn.Name != "test" {
		t.Errorf("expected name %q, got %q", "test", tn.Name)
	}
}

func TestCreatePluginWithEmptyName(t *testing.T) {
	handle := &mockHandle{store: datastore.NewAttributes()}

	plugin, err := NewRunningRequestsTrackerPlugin("", json.RawMessage("{}"), handle)
	if err == nil {
		t.Fatal("expected error for empty name, got nil")
	}
	if plugin != nil {
		t.Error("expected nil plugin for empty name")
	}
}

func TestProcessRequestIncrementsCounter(t *testing.T) {
	store := datastore.NewAttributes()
	p := newPlugin(t, &mockHandle{store: store})
	ctx := context.Background()

	cycleState := framework.NewCycleState()
	req := framework.NewInferenceRequest()
	req.Body[modelBodyKey] = "llama3"

	if err := p.ProcessRequest(ctx, cycleState, req); err != nil {
		t.Fatalf("ProcessRequest returned error: %v", err)
	}

	// CycleState must carry the model key.
	modelRaw, err := cycleState.Read(cycleStateKey)
	if err != nil {
		t.Fatalf("model key not found in CycleState: %v", err)
	}
	if modelRaw.(string) != "llama3" {
		t.Errorf("expected model %q, got %q", "llama3", modelRaw)
	}

	// Datastore must have RunningRequestsCount{Count:1}.
	val, ok := store.Get("llama3")
	if !ok {
		t.Fatal("expected entry in datastore for model llama3")
	}
	rrc, ok := val.(RunningRequestsCount)
	if !ok {
		t.Fatalf("expected RunningRequestsCount, got %T", val)
	}
	if rrc.Count != 1 {
		t.Errorf("expected count 1, got %d", rrc.Count)
	}
}

func TestProcessResponseDecrementsCounter(t *testing.T) {
	store := datastore.NewAttributes()
	p := newPlugin(t, &mockHandle{store: store})
	ctx := context.Background()

	// Simulate a prior ProcessRequest.
	cycleState := framework.NewCycleState()
	req := framework.NewInferenceRequest()
	req.Body[modelBodyKey] = "llama3"
	_ = p.ProcessRequest(ctx, cycleState, req)

	resp := framework.NewInferenceResponse()
	if err := p.ProcessResponse(ctx, cycleState, resp); err != nil {
		t.Fatalf("ProcessResponse returned error: %v", err)
	}

	val, ok := store.Get("llama3")
	if !ok {
		t.Fatal("expected entry in datastore after decrement")
	}
	rrc := val.(RunningRequestsCount)
	if rrc.Count != 0 {
		t.Errorf("expected count 0 after decrement, got %d", rrc.Count)
	}
}

func TestProcessRequestMissingModelField(t *testing.T) {
	store := datastore.NewAttributes()
	p := newPlugin(t, &mockHandle{store: store})
	ctx := context.Background()

	cycleState := framework.NewCycleState()
	req := framework.NewInferenceRequest()
	// No model field set.

	if err := p.ProcessRequest(ctx, cycleState, req); err != nil {
		t.Fatalf("ProcessRequest returned error: %v", err)
	}

	if len(store.Keys()) != 0 {
		t.Errorf("expected empty datastore, got %d keys", len(store.Keys()))
	}
}

func TestProcessResponseMissingModelKey(t *testing.T) {
	store := datastore.NewAttributes()
	p := newPlugin(t, &mockHandle{store: store})
	ctx := context.Background()

	cycleState := framework.NewCycleState() // no model key written
	resp := framework.NewInferenceResponse()

	if err := p.ProcessResponse(ctx, cycleState, resp); err != nil {
		t.Fatalf("ProcessResponse returned error: %v", err)
	}

	if len(store.Keys()) != 0 {
		t.Errorf("expected empty datastore, got %d keys", len(store.Keys()))
	}
}

func TestProcessResponseWithDatastoreError(t *testing.T) {
	p := newPlugin(t, &mockHandle{store: nil, err: datastore.ErrEmptyDatastoreKey})
	ctx := context.Background()

	cycleState := framework.NewCycleState()
	cycleState.Write(cycleStateKey, "llama3")
	// Prime the internal counter so decrement doesn't go negative.
	p.atomicAdd("llama3", 1)

	resp := framework.NewInferenceResponse()
	if err := p.ProcessResponse(ctx, cycleState, resp); err != nil {
		t.Fatalf("ProcessResponse must not return error on datastore failure, got: %v", err)
	}

	// Internal counter should be 0 even though datastore write failed.
	actual, _ := p.counters.Load("llama3")
	if actual.(*atomic.Int64).Load() != 0 {
		t.Errorf("expected internal counter 0, got %d", actual.(*atomic.Int64).Load())
	}
}

func TestRunningRequestsCountClone(t *testing.T) {
	original := RunningRequestsCount{Count: 5}
	cloned := original.Clone()

	c, ok := cloned.(RunningRequestsCount)
	if !ok {
		t.Fatalf("Clone() returned wrong type: %T", cloned)
	}
	if c.Count != original.Count {
		t.Errorf("expected Count %d, got %d", original.Count, c.Count)
	}

	// Verify independence.
	c.Count = 99
	if original.Count == c.Count {
		t.Error("clone is not independent of original")
	}
}

func TestConcurrentProcessRequestCalls(t *testing.T) {
	store := datastore.NewAttributes()
	p := newPlugin(t, &mockHandle{store: store})
	ctx := context.Background()

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cs := framework.NewCycleState()
			req := framework.NewInferenceRequest()
			req.Body[modelBodyKey] = "llama3"
			if err := p.ProcessRequest(ctx, cs, req); err != nil {
				t.Errorf("ProcessRequest returned error: %v", err)
			}
		}()
	}
	wg.Wait()

	val, ok := store.Get("llama3")
	if !ok {
		t.Fatal("expected entry in datastore")
	}
	// Final snapshot may be any count in [1, n]; just verify it's positive.
	if val.(RunningRequestsCount).Count <= 0 {
		t.Errorf("expected positive count, got %d", val.(RunningRequestsCount).Count)
	}

	// Internal counter must be exactly n.
	actual, _ := p.counters.Load("llama3")
	if actual.(*atomic.Int64).Load() != n {
		t.Errorf("expected internal counter %d, got %d", n, actual.(*atomic.Int64).Load())
	}
}

func TestConcurrentRequestResponsePairs(t *testing.T) {
	store := datastore.NewAttributes()
	p := newPlugin(t, &mockHandle{store: store})
	ctx := context.Background()

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cs := framework.NewCycleState()
			req := framework.NewInferenceRequest()
			req.Body[modelBodyKey] = "llama3"
			_ = p.ProcessRequest(ctx, cs, req)
			_ = p.ProcessResponse(ctx, cs, framework.NewInferenceResponse())
		}()
	}
	wg.Wait()

	actual, _ := p.counters.Load("llama3")
	if actual.(*atomic.Int64).Load() != 0 {
		t.Errorf("expected counter to return to 0, got %d", actual.(*atomic.Int64).Load())
	}
}

func TestTypedNameReturnsCorrectValue(t *testing.T) {
	handle := &mockHandle{store: datastore.NewAttributes()}
	plugin, err := NewRunningRequestsTrackerPlugin("mytracker", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	tn := plugin.(*RunningRequestsTrackerPlugin).TypedName()
	if tn.Type != RunningRequestsTrackerPluginType {
		t.Errorf("expected type %q, got %q", RunningRequestsTrackerPluginType, tn.Type)
	}
	if tn.Name != "mytracker" {
		t.Errorf("expected name %q, got %q", "mytracker", tn.Name)
	}
}
