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

package framework

import (
	"context"
	"time"
)

// DataSource is the common interface for all data producers.
// Both observation-driven and polling-driven sources implement it.
// Aligned with EPP's DataSource: embeds Plugin (TypedName) as the base.
type DataSource interface {
	Plugin
	Start(ctx context.Context) error
	Stop()
}

// ObservationDataSource is a DataSource driven by the request/response lifecycle.
// It is a standard framework plugin (RequestProcessor + ResponseProcessor).
// Body parsing happens once; all registered ObservationExtractors receive
// the same pre-parsed observation struct.
type ObservationDataSource interface {
	DataSource
	RequestProcessor
	ResponseProcessor
	RegisterExtractor(e ObservationExtractor)
}

// PollingDataSource is a DataSource that fetches metrics from inference pool
// endpoints on a configurable schedule. (Phase 2)
type PollingDataSource interface {
	DataSource
	RegisterExtractor(e PoolExtractor)
}

// RequestObservation is parsed once from the inference request body.
// All ObservationExtractors receive the same instance; do not mutate it.
type RequestObservation struct {
	Model     string
	MaxTokens int
	StartTime time.Time
}

// ResponseObservation is parsed once from the inference response body and CycleState.
// All ObservationExtractors receive the same instance; do not mutate it.
// MaxTokens is echoed from RequestObservation so extractors can decrement token-level counters.
type ResponseObservation struct {
	Model            string
	MaxTokens        int
	Duration         time.Duration
	PromptTokens     int
	CompletionTokens int
	// FirstChunkTime is the wall-clock time of the first streaming response chunk.
	// Zero for non-streaming responses.
	// TTFT ≈ FirstChunkTime - RequestObservation.StartTime
	FirstChunkTime time.Time
}

// ObservationExtractor transforms parsed request/response observations into
// per-model aggregate metrics stored in the datastore.
// Implementations must be goroutine-safe.
type ObservationExtractor interface {
	DataSource
	ExtractRequest(ctx context.Context, obs RequestObservation) error
	ExtractResponse(ctx context.Context, obs ResponseObservation) error
}

// PoolMetricsSnapshot is a point-in-time snapshot of metrics fetched from
// one inference pool endpoint. (Phase 2)
type PoolMetricsSnapshot struct {
	PoolID              string
	RunningRequestsSize int
	WaitingQueueSize    int
	KVCacheUsagePercent float64
	ActiveModels        []string
	UpdateTime          time.Time
}

// PoolExtractor transforms a PoolMetricsSnapshot into structured attributes
// stored in the datastore. (Phase 2)
type PoolExtractor interface {
	DataSource
	ExtractPoolMetrics(ctx context.Context, snapshot PoolMetricsSnapshot) error
}
