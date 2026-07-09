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

// Package autogroup implements a modelselector filter that maps the special
// "auto" model name prefix to a configured group of candidate models.
//
// For detailed behavioral intent and configuration, see the package README.
package autogroup

import (
	"context"
	"encoding/json"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	logutil "github.com/llm-d/llm-d-inference-payload-processor/pkg/common/observability/logging"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/modelselector"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
)

const (
	// AutoGroupFilterType is the registered name of the auto-group filter plugin.
	AutoGroupFilterType = "auto-group-model-name-filter"

	// requestModelField is the request-body field holding the requested model name.
	requestModelField = "model"

	// autoPrefix is the model name value (or prefix) that triggers group-based filtering.
	autoPrefix = "auto"

	// autoGroupSeparator separates "auto" from the group name in "auto/<group-name>".
	autoGroupSeparator = "auto/"
)

// compile-time type validation
var _ modelselector.Filter = &AutoGroupFilter{}

// AutoGroupFilterFactory parses the plugin parameters as a map of group name →
// model names and returns a configured AutoGroupFilter.
func AutoGroupFilterFactory(name string, params json.RawMessage, _ plugin.Handle) (plugin.Plugin, error) {
	groups := map[string][]string{}
	if len(params) > 0 {
		if err := json.Unmarshal(params, &groups); err != nil {
			return nil, err
		}
	}
	return NewAutoGroupFilter(groups).WithName(name), nil
}

// NewAutoGroupFilter initializes a new AutoGroupFilter with the given groups map.
func NewAutoGroupFilter(groups map[string][]string) *AutoGroupFilter {
	// Build a set-per-group for O(1) membership tests.
	groupSets := make(map[string]map[string]struct{}, len(groups))
	for groupName, models := range groups {
		set := make(map[string]struct{}, len(models))
		for _, m := range models {
			set[m] = struct{}{}
		}
		groupSets[groupName] = set
	}
	return &AutoGroupFilter{
		typedName: plugin.TypedName{Type: AutoGroupFilterType, Name: AutoGroupFilterType},
		groups:    groupSets,
	}
}

// AutoGroupFilter restricts the candidate models to those that belong to the
// group identified by the "auto/<group-name>" model field in the request body.
type AutoGroupFilter struct {
	typedName plugin.TypedName
	// groups maps group name → set of model names belonging to that group.
	groups map[string]map[string]struct{}
}

// TypedName returns the type and name tuple of this plugin instance.
func (f *AutoGroupFilter) TypedName() plugin.TypedName {
	return f.typedName
}

// WithName sets the name of the plugin instance.
func (f *AutoGroupFilter) WithName(name string) *AutoGroupFilter {
	f.typedName.Name = name
	return f
}

// Filter returns the candidate models based on the "model" field of the request:
//   - absent, empty, or exactly "auto": all candidates pass through.
//   - "auto/<group-name>": candidates whose name appears in the named group.
//   - any other value or non-string type: no candidates (pipeline rejects with 429).
func (f *AutoGroupFilter) Filter(ctx context.Context, _ *plugin.CycleState, request *requesthandling.InferenceRequest, models []datalayer.Model) []datalayer.Model {
	logger := log.FromContext(ctx)

	raw := request.Body[requestModelField]
	requested, ok := raw.(string)
	if !ok && raw != nil {
		logger.V(logutil.VERBOSE).Info("malformed model field in request body, no available model candidates", "field", requestModelField)
		return []datalayer.Model{}
	}

	// Empty, absent, or bare "auto" → pass all candidates through.
	if requested == "" || requested == autoPrefix {
		logger.V(logutil.VERBOSE).Info("auto model field, all candidates kept", "field", requestModelField)
		return models
	}

	// Must start with "auto/" to be handled by this filter; anything else is
	// a model name not relevant to this filter (not prefixed with "auto").
	if !strings.HasPrefix(requested, autoGroupSeparator) {
		logger.V(logutil.VERBOSE).Info("model field is not an auto-group selector, no candidates", "requested", requested)
		return []datalayer.Model{}
	}

	groupName := strings.TrimPrefix(requested, autoGroupSeparator)
	if groupName == "" {
		logger.V(logutil.VERBOSE).Info("empty group name in auto-group selector, no candidates", "requested", requested)
		return []datalayer.Model{}
	}

	groupSet, exists := f.groups[groupName]
	if !exists {
		logger.V(logutil.VERBOSE).Info("unknown group in auto-group selector, no candidates", "group", groupName)
		return []datalayer.Model{}
	}

	var result []datalayer.Model
	for _, m := range models {
		if _, inGroup := groupSet[m.GetName()]; inGroup {
			result = append(result, m)
		}
	}

	if len(result) == 0 {
		logger.V(logutil.VERBOSE).Info("no candidates match the auto-group", "group", groupName)
		return []datalayer.Model{}
	}

	logger.V(logutil.DEBUG).Info("auto-group filter applied", "group", groupName, "candidates", len(result))
	return result
}
