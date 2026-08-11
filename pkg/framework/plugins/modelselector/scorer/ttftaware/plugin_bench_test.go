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

package ttftaware

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/datalayer"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/plugin"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/interface/requesthandling"
	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/plugins/datalayer/ttftpercentile"
)

func init() {
	// Discard logs so logging cost doesn't skew results.
	log.SetLogger(logr.Discard())
}

// sink prevents dead-code elimination of the benchmarked Score result.
var sink map[datalayer.Model]float64

// BenchmarkScore measures the per-request cost of TTFTAwareScorer.Score at different
// candidate-model counts. It is a regression guard for per-request allocations on the
// scoring hot path (metrics lookup, Predict, seeding, normalisation).
//
// Run:
//
//	go test -run='^$' -bench=BenchmarkScore -benchmem -count=5 \
//	    ./pkg/framework/plugins/modelselector/scorer/ttftaware/ | tee bench.out
//	benchstat bench.out
//
// Branch coverage: the sub-benchmarks exercise the distinct paths Score takes so the
// reported cost reflects a real request mix, not just the cheapest branch:
//   - trusted:   all models calibrated with VARIED metrics → real min-max normalisation
//     (NOT the degenerate max==min tie path).
//   - mixed:     some trusted, some cold/under-observed → seeding + normalisation.
//   - allcold:   no model observed yet → all-cold explore path.
func BenchmarkScore(b *testing.B) {
	ctx := context.Background()
	scorer := NewTTFTAwareScorer()
	request := requesthandling.NewInferenceRequest()
	cycleState := plugin.NewCycleState() // Score no longer uses it, but pass a real one.

	sizes := []int{2, 5, 25, 100}

	for _, n := range sizes {
		for _, mix := range []struct {
			name  string
			build func(int) []datalayer.Model
		}{
			{"trusted", makeTrustedModels},
			{"mixed", makeMixedModels},
			{"allcold", makeColdModels},
		} {
			models := mix.build(n)
			b.Run(fmt.Sprintf("%s/models=%d", mix.name, n), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					scores := scorer.Score(ctx, cycleState, request, models)
					if len(scores) != n {
						b.Fatalf("expected %d scores, got %d", n, len(scores))
					}
					sink = scores
				}
			})
		}
	}
}

// BenchmarkScoreParallel measures Score under concurrency, surfacing any lock
// contention on the shared metrics/tracker attributes that the serial benchmark
// hides. The request path is concurrent in production.
func BenchmarkScoreParallel(b *testing.B) {
	ctx := context.Background()
	scorer := NewTTFTAwareScorer()
	models := makeTrustedModels(25)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		request := requesthandling.NewInferenceRequest()
		cycleState := plugin.NewCycleState()
		var local map[datalayer.Model]float64
		for pb.Next() {
			local = scorer.Score(ctx, cycleState, request, models)
		}
		sink = local
	})
}

// makeTrustedModels builds n calibrated models with VARIED operating points, so
// effectiveTTFT differs across models and Score runs the real min-max normalisation.
//
// NOTE: giving every model identical metrics (as an earlier version did) makes every
// effectiveTTFT equal, which triggers the degenerate max==min tie branch — a different,
// cheaper code path than production. Vary the inputs.
func makeTrustedModels(n int) []datalayer.Model {
	models := make([]datalayer.Model, n)
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < n; i++ {
		m := datalayer.NewModel(fmt.Sprintf("model-%d", i))
		p10 := 0.15 + rng.Float64()*0.1 // ~0.15..0.25
		m.GetAttributes().Put(ttftpercentile.AttributeKey, ttftpercentile.TTFTPercentileMetrics{
			Requests:      int64(50 + rng.Intn(200)),
			InflightAtHigh: float64(3 + rng.Intn(30)), // varied load point
			P10LowTTFT:    p10,
			HighTTFT:       p10 + 0.3 + rng.Float64()*2.0, // varied, always > floor
			RecentN:       50,
			Observations:  50, // >= MinRequests so the floor guard passes and Floor() is trusted
			MinRequests:   10,
		})
		models[i] = m
	}
	return models
}

// makeMixedModels builds a realistic mix: ~half trusted (varied), ~half cold/
// under-observed (RecentN below MinRequests → seeding path).
func makeMixedModels(n int) []datalayer.Model {
	models := makeTrustedModels(n)
	for i := 0; i < n; i += 2 {
		m := datalayer.NewModel(fmt.Sprintf("cold-%d", i))
		m.GetAttributes().Put(ttftpercentile.AttributeKey, ttftpercentile.TTFTPercentileMetrics{
			Requests:      2,
			InflightAtHigh: 0,   // no operating point yet → not trusted
			P10LowTTFT:    0.2, // has a floor to seed from
			HighTTFT:       0,
			RecentN:       2,  // < MinRequests, and InflightAtHigh == 0 → not trusted (SEED)
			Observations:  20, // >= MinRequests so Floor() returns the floor (seed, not cold)
			MinRequests:   10,
		})
		models[i] = m
	}
	return models
}

// makeColdModels builds n models with no observations at all → the all-cold explore
// path (every candidate cold).
func makeColdModels(n int) []datalayer.Model {
	models := make([]datalayer.Model, n)
	for i := 0; i < n; i++ {
		m := datalayer.NewModel(fmt.Sprintf("cold-%d", i))
		m.GetAttributes().Put(ttftpercentile.AttributeKey, ttftpercentile.TTFTPercentileMetrics{
			RecentN:     0,
			MinRequests: 10,
		})
		models[i] = m
	}
	return models
}
