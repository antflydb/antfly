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

package declarations

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/go/pkg/sources/management"
)

type KnowledgeDeclaration = management.KnowledgeDeclaration
type EntityBinding = management.EntityBinding
type GraphBinding = management.GraphBinding

// ValidateKnowledge validates discovery only. The trusted runtime supplies its
// deployment audience separately; a declaration cannot enable a security mode.
func ValidateKnowledge(k *KnowledgeDeclaration, audience string) error {
	if k == nil {
		return nil
	}
	if k.Version != 1 || k.Mode != "instance_one_audience" || !ValidBindingName(k.Audience) {
		return fmt.Errorf("invalid knowledge contract")
	}
	if len(k.Entities) > 64 || len(k.Graphs) > 64 {
		return fmt.Errorf("invalid knowledge binding count")
	}
	// An explicit empty declaration clears discovery even after an operator
	// disables the deployment boundary; it never executes native processing.
	if len(k.Entities)+len(k.Graphs) == 0 {
		return nil
	}
	if audience == "" || k.Audience != audience || !ValidBindingName(audience) {
		return fmt.Errorf("knowledge requires a runtime-enforced single-audience instance")
	}

	entities := map[string]bool{}
	namespaces := map[string]bool{}
	for _, e := range k.Entities {
		for _, v := range []string{e.Name, e.Index, e.Resolver, e.Table, e.Namespace, e.SourceArtifact, e.ResolutionArtifact} {
			if !ValidBindingName(v) {
				return fmt.Errorf("invalid entity binding")
			}
		}
		if entities[e.Name] || namespaces[e.Table+"\x00"+e.Namespace] || !strings.HasSuffix(e.Namespace, "/") || strings.ContainsAny(e.Namespace, "{}") {
			return fmt.Errorf("entity names and namespaces must be distinct stable prefixes")
		}
		identityPrefix := e.Table + "\x00" + e.Namespace
		for existing := range namespaces {
			if strings.HasPrefix(existing, identityPrefix) || strings.HasPrefix(identityPrefix, existing) {
				return fmt.Errorf("entity namespaces must not overlap within a table")
			}
		}
		entities[e.Name] = true
		namespaces[identityPrefix] = true
	}
	graphs := map[string]bool{}
	for _, g := range k.Graphs {
		if !ValidBindingName(g.Name) || !ValidBindingName(g.Index) || graphs[g.Name] || len(g.Artifacts) == 0 || len(g.Artifacts) > 64 || len(g.Entities) > 64 || g.Temporal.Mapping != "native_intervals_v1" || g.Temporal.History != "current_state" {
			return fmt.Errorf("invalid graph binding or unsupported temporal history")
		}
		graphs[g.Name] = true
		seen := map[string]bool{}
		for _, a := range g.Artifacts {
			if !ValidBindingName(a) || seen[a] {
				return fmt.Errorf("invalid graph artifact")
			}
			seen[a] = true
		}
		seen = map[string]bool{}
		for _, n := range g.Entities {
			if !entities[n] || seen[n] {
				return fmt.Errorf("unknown or duplicate graph entity binding")
			}
			seen[n] = true
			for _, e := range k.Entities {
				if e.Name == n && (e.Index != g.Index || !containsString(g.Artifacts, e.SourceArtifact)) {
					return fmt.Errorf("entity binding does not belong to graph")
				}
			}
		}
	}
	return nil
}
func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// Stable templates are intentionally restricted to native normalized mention
// fields. Candidate IDs, enumeration indices and generated display names are
// never accepted as join identities. Native curation owns redirects and splits.
func CheckEntityBinding(e EntityBinding, cfg map[string]any) error {
	values, _ := cfg["resolvers"].([]any)
	for _, v := range values {
		r, ok := v.(map[string]any)
		if !ok || r["name"] != e.Resolver {
			continue
		}
		key, _ := r["key_template"].(string)
		stable := key == e.Namespace+"{{ slug _entity.text }}" || key == e.Namespace+"{{ slug _entity.label }}/{{ slug _entity.text }}"
		if !stable || r["candidate_key_prefix"] != e.Namespace || r["table"] != e.Table || r["source_artifact"] != e.SourceArtifact || r["resolution_artifact"] != e.ResolutionArtifact {
			return fmt.Errorf("resolver identity mismatch or unstable key template")
		}
		// Native scoring breaks equal-score ties by durable table/key identity.
		// Discovery preserves that authority rather than inventing source matching.
		if search, _ := r["candidate_search"].(string); search != "" && search != "exact_key" && search != "prefix" && search != "ann" {
			return fmt.Errorf("unsupported native candidate search")
		}

		aliases, _ := r["aliases"].([]any)
		for _, v := range aliases {
			a, ok := v.(map[string]any)
			if !ok {
				return fmt.Errorf("invalid alias")
			}
			from, _ := a["key"].(string)
			to, _ := a["target"].(string)
			if !strings.HasPrefix(from, e.Namespace) || !strings.HasPrefix(to, e.Namespace) {
				return fmt.Errorf("cross-namespace aliases require explicit client composition")
			}
		}
		return nil
	}
	return fmt.Errorf("native resolver missing")
}
func CheckGraphBinding(g GraphBinding, cfg map[string]any) error {
	if cfg["type"] != "graph" {
		return fmt.Errorf("binding requires a native graph")
	}
	sources, _ := cfg["sources"].([]any)
	if single, ok := cfg["source"].(map[string]any); ok {
		if len(sources) > 0 {
			return fmt.Errorf("graph source forms are mutually exclusive")
		}
		sources = append(sources, single)
	}
	if len(sources) != len(g.Artifacts) {
		return fmt.Errorf("graph source set mismatch")
	}
	seen := map[string]bool{}
	for _, v := range sources {
		s, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid graph source")
		}
		a, _ := s["artifact"].(string)
		if seen[a] || !containsString(g.Artifacts, a) {
			return fmt.Errorf("graph source set mismatch")
		}
		seen[a] = true
		edge, _ := s["edge"].(map[string]any)
		id, ok := edge["edge_id"].(string)
		if !ok || strings.TrimSpace(id) == "" || strings.Contains(id, "_item_index") || strings.Contains(id, "@index") {
			return fmt.Errorf("relationship requires stable member identity independent of array order")
		}
		metadata, _ := edge["metadata"].(map[string]any)
		if metadata["source_id"] != "{{ _doc.value.source.id }}" || metadata["parent_revision"] != "{{ _doc.value.source.revision }}" {
			return fmt.Errorf("graph source requires revision-bound parent provenance")
		}
		context, _ := s["context"].(map[string]any)
		fields, _ := context["doc_fields"].([]any)
		hasSource := false
		for _, f := range fields {
			if f == "source" {
				hasSource = true
			}
		}
		if !hasSource {
			return fmt.Errorf("graph provenance requires source context")
		}
	}
	return nil
}

// checkKnowledgeGraph requires a complete declaration of the installed resolver
// targets before a host may execute with its runtime-owned native credentials.
// A graph may have no resolvers, but omission cannot hide an installed target.
func CheckKnowledgeGraph(k *KnowledgeDeclaration, g GraphBinding, cfg map[string]any) error {
	if err := CheckGraphBinding(g, cfg); err != nil {
		return err
	}
	declared := map[string]EntityBinding{}
	for _, e := range k.Entities {
		if !containsString(g.Entities, e.Name) {
			continue
		}
		if e.Index != g.Index {
			return fmt.Errorf("entity does not belong to graph")
		}
		if _, exists := declared[e.Resolver]; exists {
			return fmt.Errorf("duplicate graph resolver binding")
		}
		declared[e.Resolver] = e
	}
	resolvers, _ := cfg["resolvers"].([]any)
	if len(resolvers) != len(declared) {
		return fmt.Errorf("every installed graph resolver requires an entity binding")
	}
	seen := map[string]bool{}
	for _, v := range resolvers {
		r, ok := v.(map[string]any)
		if !ok {
			return fmt.Errorf("invalid native resolver")
		}
		name, _ := r["name"].(string)
		e, ok := declared[name]
		if !ok || seen[name] {
			return fmt.Errorf("undeclared installed graph resolver")
		}
		if err := CheckEntityBinding(e, cfg); err != nil {
			return err
		}
		seen[name] = true
	}
	return nil
}

// ValidateKnowledgeProcessing checks the complete desired graph before any DDL.
// Actual native admission remains authoritative for extractor/resolver schemas.
func ValidateKnowledgeProcessing(k *KnowledgeDeclaration, plan *ProcessingDeclaration, audience string) error {
	if err := ValidateKnowledge(k, audience); err != nil {
		return err
	}
	// Graph processing is subject to the same runtime boundary even when a
	// caller omits discovery. Otherwise raw native declarations could bypass
	// hosted entity-table authorization and enable mixed-audience promotion.
	if plan != nil {
		raw, err := json.Marshal(plan)
		if err != nil {
			return err
		}
		var canonical ProcessingDeclaration
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err = decoder.Decode(&canonical); err != nil {
			return err
		}
		plan = &canonical
	}
	if plan != nil {
		for _, idx := range plan.Indexes {
			resolvers, _ := idx.Config["resolvers"].([]any)
			if idx.Config["type"] != "graph" && len(resolvers) == 0 {
				continue
			}
			if k == nil || len(k.Entities)+len(k.Graphs) == 0 {
				return fmt.Errorf("source graph processing requires enabled knowledge bindings")
			}
			for _, value := range resolvers {
				r, ok := value.(map[string]any)
				if !ok {
					return fmt.Errorf("invalid native resolver")
				}
				declared := false
				for _, e := range k.Entities {
					if e.Index == idx.Name && e.Resolver == r["name"] {
						if err := CheckEntityBinding(e, idx.Config); err != nil {
							return err
						}
						declared = true
					}
				}
				if !declared {
					return fmt.Errorf("every processing resolver requires an authorized entity binding")
				}
			}
		}
	}
	if k == nil {
		return nil
	}
	// Omitted processing may reference independently administered native resources.
	// Installed readback is still verified before readiness or query admission.
	if plan == nil {
		return nil
	}
	configs := map[string]map[string]any{}
	if plan != nil {
		for _, idx := range plan.Indexes {
			configs[idx.Name] = idx.Config
		}
	}
	for _, g := range k.Graphs {
		if err := CheckKnowledgeGraph(k, g, configs[g.Index]); err != nil {
			return fmt.Errorf("graph %s: %w", g.Name, err)
		}
	}
	for _, e := range k.Entities {
		cfg := configs[e.Index]
		if cfg["type"] != "graph" {
			return fmt.Errorf("entity resolver requires its declared native graph")
		}
		if err := CheckEntityBinding(e, cfg); err != nil {
			return fmt.Errorf("entity %s: %w", e.Name, err)
		}
	}
	return nil
}
