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

package autogroup

import (
	"context"
	"encoding/json"
	"sort"
	"testing"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
)

// candidateModels builds the datalayer models handed to the filter.
func candidateModels(names ...string) []datalayer.Model {
	models := make([]datalayer.Model, 0, len(names))
	for _, n := range names {
		models = append(models, datalayer.NewModel(n))
	}
	return models
}

// modelNames extracts the sorted model names of a filter result, for
// order-insensitive comparison against the expected names.
func modelNames(models []datalayer.Model) []string {
	out := make([]string, 0, len(models))
	for _, m := range models {
		out = append(out, m.GetName())
	}
	sort.Strings(out)
	return out
}

// requestWithModel builds an inference request whose body holds the given
// value under the "model" field; a nil value leaves the field absent.
func requestWithModel(value any) *requesthandling.InferenceRequest {
	r := requesthandling.NewInferenceRequest()
	if value != nil {
		r.Body[requestModelField] = value
	}
	return r
}

// TestAutoGroupFilterFactory verifies that the factory parses parameters
// correctly and carries the right type and instance name.
func TestAutoGroupFilterFactory(t *testing.T) {
	params := json.RawMessage(`{"qwen3models": ["qwen3-8b", "qwen3-32b"]}`)
	p, err := AutoGroupFilterFactory("my-filter", params, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	f := p.(*AutoGroupFilter)
	if got := f.TypedName().Name; got != "my-filter" {
		t.Errorf("Name = %s, want my-filter", got)
	}
	if got := f.TypedName().Type; got != AutoGroupFilterType {
		t.Errorf("Type = %s, want %s", got, AutoGroupFilterType)
	}
}

// TestAutoGroupFilterFactory_InvalidJSON verifies that malformed parameters
// cause the factory to return an error.
func TestAutoGroupFilterFactory_InvalidJSON(t *testing.T) {
	_, err := AutoGroupFilterFactory("f", json.RawMessage(`{invalid`), nil)
	if err == nil {
		t.Fatal("expected error for invalid JSON params, got nil")
	}
}

// TestAutoGroupFilterFactory_EmptyParams verifies that an empty/nil params
// payload creates a filter with no groups (pass-all on "auto").
func TestAutoGroupFilterFactory_EmptyParams(t *testing.T) {
	p, err := AutoGroupFilterFactory("f", nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil plugin")
	}
}

// TestAutoGroupFilter_Filter covers all behavioural cases described in the README.
func TestAutoGroupFilter_Filter(t *testing.T) {
	groups := map[string][]string{
		"qwen3models": {"qwen3-8b", "qwen3-32b"},
		"llama3":      {"llama3-8b", "llama3-70b"},
	}
	// All models available in the data layer.
	all := []string{"qwen3-8b", "qwen3-32b", "llama3-8b", "llama3-70b", "mistral-7b"}

	tests := []struct {
		name      string
		modelBody any
		want      []string
	}{
		// "auto/group-name" returns only models in that group that are also
		// present in the candidate list.
		{
			name:      "auto/qwen3models returns group members",
			modelBody: "auto/qwen3models",
			want:      []string{"qwen3-8b", "qwen3-32b"},
		},
		{
			name:      "auto/llama3 returns group members",
			modelBody: "auto/llama3",
			want:      []string{"llama3-8b", "llama3-70b"},
		},
		// Absent model field → all candidates pass through.
		{
			name:      "missing model field passes all through",
			modelBody: nil,
			want:      all,
		},
		// Empty string → all candidates pass through.
		{
			name:      "empty string passes all through",
			modelBody: "",
			want:      all,
		},
		// Bare "auto" (no slash) → all candidates pass through.
		{
			name:      "bare auto passes all through",
			modelBody: "auto",
			want:      all,
		},
		// "auto/" with no group name → empty result.
		{
			name:      "auto/ with empty group name yields empty",
			modelBody: "auto/",
			want:      []string{},
		},
		// Unknown group → empty result.
		{
			name:      "auto/unknown-group yields empty",
			modelBody: "auto/unknowngroup",
			want:      []string{},
		},
		// Non-auto string that doesn't match any known pattern → empty result.
		{
			name:      "plain model name (non-auto) yields empty",
			modelBody: "qwen3-8b",
			want:      []string{},
		},
		// Non-string type → malformed → empty result.
		{
			name:      "non-string model field yields empty (malformed)",
			modelBody: 42,
			want:      []string{},
		},
		// JSON array is not a string → malformed → empty result.
		{
			name:      "JSON array model field yields empty (malformed)",
			modelBody: []any{"qwen3-8b", "qwen3-32b"},
			want:      []string{},
		},
		// Group exists but none of the group models are in the candidate list.
		// This is a special case where we set the candidates to "mistral-7b".
		{
			name:      "group models not in candidates yields empty",
			modelBody: "auto/qwen3models",
			// Use a custom candidate set that excludes qwen group models.
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewAutoGroupFilter(groups)
			req := requestWithModel(tt.modelBody)

			// For the "group models not in candidates" case, use a restricted
			// candidate set; otherwise use all.
			candidates := candidateModels(all...)
			if tt.name == "group models not in candidates yields empty" {
				candidates = candidateModels("mistral-7b")
			}

			got := modelNames(f.Filter(context.Background(), nil, req, candidates))
			want := append([]string{}, tt.want...)
			sort.Strings(want)

			if len(got) != len(want) {
				t.Fatalf("Filter() = %v, want %v", got, want)
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("Filter() = %v, want %v", got, want)
					break
				}
			}
		})
	}
}

// TestAutoGroupFilter_PartialGroupInCandidates verifies that when only a
// subset of the group's models are present in the candidate list, only those
// present are returned.
func TestAutoGroupFilter_PartialGroupInCandidates(t *testing.T) {
	groups := map[string][]string{
		"qwen3models": {"qwen3-8b", "qwen3-32b", "qwen3-72b"},
	}
	// Only qwen3-8b and qwen3-72b are in the data layer; qwen3-32b is absent.
	candidates := candidateModels("qwen3-8b", "qwen3-72b", "mistral-7b")

	f := NewAutoGroupFilter(groups)
	req := requestWithModel("auto/qwen3models")

	got := modelNames(f.Filter(context.Background(), nil, req, candidates))
	want := []string{"qwen3-72b", "qwen3-8b"}
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("Filter() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Filter() = %v, want %v", got, want)
		}
	}
}
