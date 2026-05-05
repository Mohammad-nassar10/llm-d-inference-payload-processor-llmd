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
	"sync"
	"testing"
	"time"

	ctrlbuilder "sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datastore"
)

// mockHandle implements framework.Handle for testing
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

func (m *mockHandle) GetAttributeMap(datastoreKey string) (datastore.AttributeMap, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.store, nil
}

func TestCreatePluginWithValidName(t *testing.T) {
	handle := &mockHandle{store: datastore.NewAttributes()}
	config := json.RawMessage("{}")

	plugin, err := NewRequestLatencyTrackerPlugin("test", config, handle)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if plugin == nil {
		t.Fatal("expected non-nil plugin")
	}

	typedName := plugin.(*RequestLatencyTrackerPlugin).TypedName()
	if typedName.Type != RequestLatencyTrackerPluginType {
		t.Errorf("expected type %q, got %q", RequestLatencyTrackerPluginType, typedName.Type)
	}
	if typedName.Name != "test" {
		t.Errorf("expected name %q, got %q", "test", typedName.Name)
	}
}

func TestCreatePluginWithEmptyName(t *testing.T) {
	handle := &mockHandle{store: datastore.NewAttributes()}
	config := json.RawMessage("{}")

	plugin, err := NewRequestLatencyTrackerPlugin("", config, handle)
	if err == nil {
		t.Fatal("expected error for empty name, got nil")
	}
	if plugin != nil {
		t.Error("expected nil plugin for empty name")
	}
}

func TestProcessRequestStoresStartTime(t *testing.T) {
	handle := &mockHandle{store: datastore.NewAttributes()}
	plugin, err := NewRequestLatencyTrackerPlugin("test", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	cycleState := framework.NewCycleState()
	request := framework.NewInferenceRequest()
	ctx := context.Background()

	beforeTime := time.Now()
	err = plugin.(*RequestLatencyTrackerPlugin).ProcessRequest(ctx, cycleState, request)
	afterTime := time.Now()

	if err != nil {
		t.Fatalf("ProcessRequest returned error: %v", err)
	}

	startTimeRaw, err := cycleState.Read(cycleStateKey)
	if err != nil {
		t.Fatalf("start time not found in CycleState: %v", err)
	}

	startTime, ok := startTimeRaw.(time.Time)
	if !ok {
		t.Fatalf("start time has wrong type: %T", startTimeRaw)
	}

	if startTime.Before(beforeTime) || startTime.After(afterTime) {
		t.Errorf("start time %v is outside expected range [%v, %v]", startTime, beforeTime, afterTime)
	}
}

func TestProcessResponseCalculatesDuration(t *testing.T) {
	store := datastore.NewAttributes()
	handle := &mockHandle{store: store}
	plugin, err := NewRequestLatencyTrackerPlugin("test", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	cycleState := framework.NewCycleState()
	response := framework.NewInferenceResponse()
	ctx := context.Background()

	startTime := time.Now().Add(-100 * time.Millisecond)
	cycleState.Write(cycleStateKey, startTime)

	err = plugin.(*RequestLatencyTrackerPlugin).ProcessResponse(ctx, cycleState, response)
	if err != nil {
		t.Fatalf("ProcessResponse returned error: %v", err)
	}

	keys := store.Keys()
	if len(keys) != 1 {
		t.Fatalf("expected 1 key in datastore, got %d", len(keys))
	}

	value, ok := store.Get(keys[0])
	if !ok {
		t.Fatal("failed to get latency data from datastore")
	}

	latencyData, ok := value.(LatencyData)
	if !ok {
		t.Fatalf("expected LatencyData, got %T", value)
	}

	if latencyData.Duration < 100*time.Millisecond {
		t.Errorf("expected duration >= 100ms, got %v", latencyData.Duration)
	}
	if time.Since(latencyData.Timestamp) > time.Second {
		t.Errorf("timestamp is too old: %v", latencyData.Timestamp)
	}
	// No usage in response body — token counts must default to zero.
	if latencyData.PromptTokens != 0 || latencyData.CompletionTokens != 0 {
		t.Errorf("expected zero token counts for missing usage, got prompt=%d completion=%d",
			latencyData.PromptTokens, latencyData.CompletionTokens)
	}
}

func TestProcessResponseStoresTokenCounts(t *testing.T) {
	store := datastore.NewAttributes()
	handle := &mockHandle{store: store}
	plugin, err := NewRequestLatencyTrackerPlugin("test", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	cycleState := framework.NewCycleState()
	cycleState.Write(cycleStateKey, time.Now().Add(-50*time.Millisecond))

	response := framework.NewInferenceResponse()
	response.Body["usage"] = map[string]any{
		"prompt_tokens":     float64(10),
		"completion_tokens": float64(50),
	}

	if err := plugin.(*RequestLatencyTrackerPlugin).ProcessResponse(context.Background(), cycleState, response); err != nil {
		t.Fatalf("ProcessResponse returned error: %v", err)
	}

	keys := store.Keys()
	if len(keys) != 1 {
		t.Fatalf("expected 1 key in datastore, got %d", len(keys))
	}

	value, _ := store.Get(keys[0])
	ld := value.(LatencyData)

	if ld.PromptTokens != 10 {
		t.Errorf("expected PromptTokens 10, got %d", ld.PromptTokens)
	}
	if ld.CompletionTokens != 50 {
		t.Errorf("expected CompletionTokens 50, got %d", ld.CompletionTokens)
	}
}

func TestProcessResponseWithPartialUsage(t *testing.T) {
	store := datastore.NewAttributes()
	handle := &mockHandle{store: store}
	plugin, err := NewRequestLatencyTrackerPlugin("test", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	cycleState := framework.NewCycleState()
	cycleState.Write(cycleStateKey, time.Now().Add(-50*time.Millisecond))

	response := framework.NewInferenceResponse()
	// Only prompt_tokens present; completion_tokens absent.
	response.Body["usage"] = map[string]any{
		"prompt_tokens": float64(20),
	}

	if err := plugin.(*RequestLatencyTrackerPlugin).ProcessResponse(context.Background(), cycleState, response); err != nil {
		t.Fatalf("ProcessResponse returned error: %v", err)
	}

	value, _ := store.Get(store.Keys()[0])
	ld := value.(LatencyData)

	if ld.PromptTokens != 20 {
		t.Errorf("expected PromptTokens 20, got %d", ld.PromptTokens)
	}
	if ld.CompletionTokens != 0 {
		t.Errorf("expected CompletionTokens 0, got %d", ld.CompletionTokens)
	}
}

func TestProcessResponseWithMalformedUsage(t *testing.T) {
	store := datastore.NewAttributes()
	handle := &mockHandle{store: store}
	plugin, err := NewRequestLatencyTrackerPlugin("test", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	cycleState := framework.NewCycleState()
	cycleState.Write(cycleStateKey, time.Now().Add(-50*time.Millisecond))

	response := framework.NewInferenceResponse()
	// usage is present but wrong type — should not cause an error.
	response.Body["usage"] = "not-a-map"

	if err := plugin.(*RequestLatencyTrackerPlugin).ProcessResponse(context.Background(), cycleState, response); err != nil {
		t.Fatalf("ProcessResponse returned error: %v", err)
	}

	value, _ := store.Get(store.Keys()[0])
	ld := value.(LatencyData)

	if ld.PromptTokens != 0 || ld.CompletionTokens != 0 {
		t.Errorf("expected zero token counts for malformed usage, got prompt=%d completion=%d",
			ld.PromptTokens, ld.CompletionTokens)
	}
}

func TestProcessResponseWithoutStartTime(t *testing.T) {
	store := datastore.NewAttributes()
	handle := &mockHandle{store: store}
	plugin, err := NewRequestLatencyTrackerPlugin("test", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	cycleState := framework.NewCycleState()
	response := framework.NewInferenceResponse()
	ctx := context.Background()

	// Don't store start time - simulate missing data

	err = plugin.(*RequestLatencyTrackerPlugin).ProcessResponse(ctx, cycleState, response)
	if err != nil {
		t.Fatalf("ProcessResponse returned error: %v", err)
	}

	keys := store.Keys()
	if len(keys) != 0 {
		t.Errorf("expected 0 keys in datastore, got %d", len(keys))
	}
}

func TestProcessResponseWithDatastoreError(t *testing.T) {
	handle := &mockHandle{
		store: nil,
		err:   datastore.ErrEmptyDatastoreKey,
	}
	plugin, err := NewRequestLatencyTrackerPlugin("test", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	cycleState := framework.NewCycleState()
	response := framework.NewInferenceResponse()
	ctx := context.Background()

	cycleState.Write(cycleStateKey, time.Now())

	// Process response should not error even though datastore fails
	err = plugin.(*RequestLatencyTrackerPlugin).ProcessResponse(ctx, cycleState, response)
	if err != nil {
		t.Fatalf("ProcessResponse returned error: %v", err)
	}
}

func TestLatencyDataClone(t *testing.T) {
	original := LatencyData{
		Duration:         100 * time.Millisecond,
		Timestamp:        time.Now(),
		PromptTokens:     10,
		CompletionTokens: 50,
	}

	cloned := original.Clone()

	clonedData, ok := cloned.(LatencyData)
	if !ok {
		t.Fatalf("Clone() returned wrong type: %T", cloned)
	}

	if clonedData.Duration != original.Duration {
		t.Errorf("expected duration %v, got %v", original.Duration, clonedData.Duration)
	}
	if !clonedData.Timestamp.Equal(original.Timestamp) {
		t.Errorf("expected timestamp %v, got %v", original.Timestamp, clonedData.Timestamp)
	}
	if clonedData.PromptTokens != original.PromptTokens {
		t.Errorf("expected PromptTokens %d, got %d", original.PromptTokens, clonedData.PromptTokens)
	}
	if clonedData.CompletionTokens != original.CompletionTokens {
		t.Errorf("expected CompletionTokens %d, got %d", original.CompletionTokens, clonedData.CompletionTokens)
	}

	// Verify independence (modifying clone doesn't affect original)
	clonedData.Duration = 200 * time.Millisecond
	if original.Duration == clonedData.Duration {
		t.Error("clone is not independent of original")
	}
}

func TestConcurrentProcessRequestCalls(t *testing.T) {
	store := datastore.NewAttributes()
	handle := &mockHandle{store: store}
	plugin, err := NewRequestLatencyTrackerPlugin("test", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cycleState := framework.NewCycleState()
			request := framework.NewInferenceRequest()
			err := plugin.(*RequestLatencyTrackerPlugin).ProcessRequest(ctx, cycleState, request)
			if err != nil {
				t.Errorf("ProcessRequest returned error: %v", err)
			}

			_, err = cycleState.Read(cycleStateKey)
			if err != nil {
				t.Errorf("start time not found in CycleState: %v", err)
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentProcessResponseCalls(t *testing.T) {
	store := datastore.NewAttributes()
	handle := &mockHandle{store: store}
	plugin, err := NewRequestLatencyTrackerPlugin("test", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	var wg sync.WaitGroup
	ctx := context.Background()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cycleState := framework.NewCycleState()
			response := framework.NewInferenceResponse()
			cycleState.Write(cycleStateKey, time.Now().Add(-10*time.Millisecond))

			err := plugin.(*RequestLatencyTrackerPlugin).ProcessResponse(ctx, cycleState, response)
			if err != nil {
				t.Errorf("ProcessResponse returned error: %v", err)
			}
		}()
	}

	wg.Wait()

	keys := store.Keys()
	if len(keys) != 10 {
		t.Errorf("expected 10 keys in datastore, got %d", len(keys))
	}
}

func TestTypedNameReturnsCorrectValue(t *testing.T) {
	handle := &mockHandle{store: datastore.NewAttributes()}
	plugin, err := NewRequestLatencyTrackerPlugin("test", json.RawMessage("{}"), handle)
	if err != nil {
		t.Fatalf("failed to create plugin: %v", err)
	}

	typedName := plugin.(*RequestLatencyTrackerPlugin).TypedName()

	if typedName.Type != RequestLatencyTrackerPluginType {
		t.Errorf("expected type %q, got %q", RequestLatencyTrackerPluginType, typedName.Type)
	}
	if typedName.Name != "test" {
		t.Errorf("expected name %q, got %q", "test", typedName.Name)
	}
}
