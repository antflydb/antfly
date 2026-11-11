// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"context"
	"encoding/json"
	"github.com/antflydb/antfly/go/pkg/sources/management"
	"io"
	"testing"
)

type exactManifestAPI struct {
	*manifestAPI
	current management.Source
}

func (a *exactManifestAPI) GetSource(context.Context, string) (management.Source, error) {
	return a.current, nil
}
func TestPlanComparesNumbersExactly(t *testing.T) {
	for _, tc := range []struct {
		name, before, after string
		want                PlanAction
	}{
		{"decimal exponent", "0.000000001", "1e-9", PlanUnchanged},
		{"trailing zeros", "1000.000", "1E+3", PlanUnchanged},
		{"negative zero", "-0.000", "0", PlanUnchanged},
		{"huge coefficient", "184467440737095516170", "18446744073709551617e1", PlanUnchanged},
		{"symbolic exponent", "1e1000000000000000000000", "10e999999999999999999999", PlanUnchanged},
		{"adjacent huge integers", "18446744073709551617", "18446744073709551618", PlanUpdate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := manifestCurrent("docs")
			current.Config = map[string]any{"nested": []any{json.Number(tc.before)}}
			current.Revision = "opaque-server-revision"
			entry := manifestEntry("docs")
			entry.Config = map[string]any{"nested": []any{json.Number(tc.after)}}
			api := &exactManifestAPI{manifestAPI: newManifestAPI(t), current: current}
			plan, err := PlanManifest(t.Context(), Env{API: api, Out: io.Discard}, manifestFile(entry), PlanOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if plan.Items[0].Action != tc.want || plan.Items[0].Revision != current.Revision {
				t.Fatalf("plan=%#v", plan.Items[0])
			}
			if current.Config["nested"].([]any)[0] != json.Number(tc.before) {
				t.Fatal("comparison mutated source config")
			}
		})
	}
}

type declarationManifestAPI struct {
	*manifestAPI
	reads int
}

func (a *declarationManifestAPI) GetSource(context.Context, string) (management.Source, error) {
	a.t.Fatal("planning requested native observations")
	return management.Source{}, nil
}
func (a *declarationManifestAPI) GetSourceDeclaration(ctx context.Context, id string) (management.Source, error) {
	a.reads++
	return a.manifestAPI.GetSource(ctx, id)
}
func TestPlanPrefersDeclarationReads(t *testing.T) {
	api := &declarationManifestAPI{manifestAPI: newManifestAPI(t, manifestCurrent("docs"))}
	_, err := PlanManifest(t.Context(), Env{API: api, Out: io.Discard}, manifestFile(manifestEntry("docs")), PlanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if api.reads != 1 {
		t.Fatalf("declaration reads=%d", api.reads)
	}
}

func TestReservedConfigFailsBeforeAnyApplyWrite(t *testing.T) {
	api := newManifestAPI(t)
	invalid := manifestEntry("bad")
	invalid.Config["knowledge_status"] = map[string]any{"state": "ready"}
	_, err := ApplyManifest(t.Context(), Env{API: api, Out: io.Discard}, manifestFile(manifestEntry("first"), invalid), PlanOptions{})
	if err == nil {
		t.Fatal("reserved observation config was accepted")
	}
	if len(api.writes) != 0 {
		t.Fatal("invalid manifest wrote earlier sources")
	}
}
