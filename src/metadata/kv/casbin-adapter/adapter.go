// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package kvadapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/src/metadata/kv"
	"github.com/casbin/casbin/v3/model"
	"github.com/casbin/casbin/v3/persist"
	"github.com/cockroachdb/pebble/v2"
	"github.com/goccy/go-json"
)

var (
	_ persist.Adapter          = (*adapter)(nil)
	_ persist.UpdatableAdapter = (*adapter)(nil)
)

// CasbinRule represents a Casbin rule line.
type CasbinRule struct {
	Key   string `json:"key"`
	PType string `json:"p_type"`
	V0    string `json:"v0"`
	V1    string `json:"v1"`
	V2    string `json:"v2"`
	V3    string `json:"v3"`
	V4    string `json:"v4"`
	V5    string `json:"v5"`
}

func (cr *CasbinRule) Rule() []string {
	return strings.Split(cr.Key, "::")[1:]
}

type adapter struct {
	db     kv.DB
	prefix []byte
}

// NewAdapter creates a new adapter. It assumes that the Pebble DB is already open. A prefix is used if given and
// represents the Pebble prefix to save the under.
func NewAdapter(db kv.DB, prefix string) (persist.BatchAdapter, error) {
	if prefix == "" {
		return nil, errors.New("must provide a prefix")
	}

	adapter := &adapter{
		db:     db,
		prefix: []byte(prefix),
	}

	return adapter, nil
}

// prefixUpperBound returns a new slice suitable for use as an iterator upper bound.
// It avoids the append(a.prefix, 0xff) pattern which can mutate the shared prefix slice.
func (a *adapter) prefixUpperBound() []byte {
	upper := make([]byte, len(a.prefix)+1)
	copy(upper, a.prefix)
	upper[len(a.prefix)] = 0xff
	return upper
}

// ruleFromFieldValues creates a CasbinRule with fields populated from fieldValues at the given fieldIndex.
func ruleFromFieldValues(ptype string, fieldIndex int, fieldValues []string) CasbinRule {
	rule := CasbinRule{PType: ptype}
	fields := []*string{&rule.V0, &rule.V1, &rule.V2, &rule.V3, &rule.V4, &rule.V5}
	for i, field := range fields {
		if fieldIndex <= i && i < fieldIndex+len(fieldValues) {
			*field = fieldValues[i-fieldIndex]
		}
	}
	return rule
}

// filteredIter creates an iterator that skips keys not matching the given filter criteria.
func (a *adapter) filteredIter(rule CasbinRule, fieldIndex int, filterPrefix string) (*pebble.Iterator, error) {
	return a.db.NewIter(context.Background(), &pebble.IterOptions{
		LowerBound: a.prefix,
		UpperBound: a.prefixUpperBound(),
		SkipPoint: func(userKey []byte) bool {
			userKey = bytes.TrimPrefix(userKey, a.prefix)
			if !bytes.HasPrefix(userKey, []byte(rule.PType)) {
				return false
			}
			for range fieldIndex + 1 {
				i := bytes.Index(userKey, []byte("::"))
				if i < 0 {
					return false // no more fields to check
				}
				userKey = userKey[i+2:]
			}
			return !bytes.Contains(userKey, []byte(filterPrefix))
		},
	})
}

// LoadPolicy performs a scan on the bucket and individually loads every line into the Casbin model.
// Not particularity efficient but should only be required on when you application starts up as this adapter can
// leverage auto-save functionality.
func (a *adapter) LoadPolicy(model model.Model) error {
	iter, err := a.db.NewIter(context.Background(), &pebble.IterOptions{
		LowerBound: a.prefix,
		UpperBound: a.prefixUpperBound(),
	})
	if err != nil {
		return fmt.Errorf("creating db iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()
	for iter.First(); iter.Valid(); iter.Next() {
		var line CasbinRule
		if err := json.Unmarshal(iter.Value(), &line); err != nil {
			return err
		}
		if err := loadPolicy(line, model); err != nil {
			return fmt.Errorf("loading policy line %s: %w", iter.Key(), err)
		}
	}
	return nil
}

// SavePolicy is not supported for this adapter. Auto-save should be used.
func (a *adapter) SavePolicy(model model.Model) error {
	return errors.New("not supported: must use auto-save with this adapter")
}

// AddPolicy inserts or updates a rule.
func (a *adapter) AddPolicy(_ string, ptype string, rule []string) error {
	line := convertRule(ptype, rule)
	bts, err := json.Marshal(line)
	if err != nil {
		return err
	}
	return a.db.Batch(context.Background(), [][2][]byte{{a.withPrefix(line.Key), bts}}, nil)
}

// AddPolicies inserts or updates multiple rules by iterating over each one and inserting it into the bucket.
func (a *adapter) AddPolicies(_ string, ptype string, rules [][]string) error {
	writes := make([][2][]byte, 0, len(rules))
	for _, r := range rules {
		line := convertRule(ptype, r)
		bts, err := json.Marshal(line)
		if err != nil {
			return err
		}
		writes = append(writes, [2][]byte{a.withPrefix(line.Key), bts})
	}
	return a.db.Batch(context.Background(), writes, nil)
}

// Each policy rule is stored as a row in Pebble: p::subject-a::action-a::get
func (a *adapter) RemoveFilteredPolicy(
	_ string,
	ptype string,
	fieldIndex int,
	fieldValues ...string,
) error {
	rule := ruleFromFieldValues(ptype, fieldIndex, fieldValues)
	filterPrefix := a.buildFilter(rule)
	iter, err := a.filteredIter(rule, fieldIndex, filterPrefix)
	if err != nil {
		return fmt.Errorf("creating db iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()
	deletes := [][]byte{}
	for iter.First(); iter.Valid(); iter.Next() {
		deletes = append(deletes, bytes.Clone(iter.Key()))
	}
	return a.db.Batch(context.Background(), nil, deletes)
}

func (a *adapter) buildFilter(rule CasbinRule) string {
	var parts []string
	for _, v := range []string{rule.V0, rule.V1, rule.V2, rule.V3, rule.V4, rule.V5} {
		if v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, "::")
}

func (a *adapter) withPrefix(key string) []byte {
	return fmt.Append(a.prefix, key)
}

// RemovePolicy removes a policy line that matches key.
func (a *adapter) RemovePolicy(_ string, ptype string, line []string) error {
	rule := convertRule(ptype, line)
	return a.db.Batch(context.Background(), nil, [][]byte{a.withPrefix(rule.Key)})
}

// RemovePolicies removes multiple policies.
func (a *adapter) RemovePolicies(_ string, ptype string, rules [][]string) error {
	deletes := make([][]byte, 0, len(rules))
	for _, r := range rules {
		rule := convertRule(ptype, r)
		deletes = append(deletes, a.withPrefix(rule.Key))
	}
	return a.db.Batch(context.Background(), nil, deletes)
}

func (a *adapter) UpdatePolicy(_ string, ptype string, oldRule, newRule []string) error {
	old := convertRule(ptype, oldRule)
	new := convertRule(ptype, newRule)
	deletes := [][]byte{a.withPrefix(old.Key)}
	bts, err := json.Marshal(new)
	if err != nil {
		return err
	}
	writes := [][2][]byte{{a.withPrefix(new.Key), bts}}
	return a.db.Batch(context.Background(), writes, deletes)
}

func (a *adapter) UpdatePolicies(_ string, ptype string, oldRules, newRules [][]string) error {
	deletes := make([][]byte, 0, len(oldRules))
	for _, r := range oldRules {
		old := convertRule(ptype, r)
		deletes = append(deletes, a.withPrefix(old.Key))
	}

	writes := make([][2][]byte, 0, len(newRules))
	for _, r := range newRules {
		new := convertRule(ptype, r)
		bts, err := json.Marshal(new)
		if err != nil {
			return err
		}
		writes = append(writes, [2][]byte{a.withPrefix(new.Key), bts})
	}
	return a.db.Batch(context.Background(), writes, deletes)
}

func (a *adapter) UpdateFilteredPolicies(
	_ string,
	ptype string,
	newPolicies [][]string,
	fieldIndex int,
	fieldValues ...string,
) ([][]string, error) {
	rule := ruleFromFieldValues(ptype, fieldIndex, fieldValues)
	filterPrefix := a.buildFilter(rule)
	iter, err := a.filteredIter(rule, fieldIndex, filterPrefix)
	if err != nil {
		return nil, fmt.Errorf("creating db iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()
	var deletes [][]byte
	var oldRules [][]string
	for iter.First(); iter.Valid(); iter.Next() {
		r := CasbinRule{}
		if err := json.Unmarshal(iter.Value(), &r); err != nil {
			return nil, err
		}
		oldRules = append(oldRules, r.Rule())
		deletes = append(deletes, a.withPrefix(r.Key))
	}
	writes := make([][2][]byte, 0, len(newPolicies))
	for _, r := range newPolicies {
		new := convertRule(ptype, r)

		bts, err := json.Marshal(new)
		if err != nil {
			return nil, err
		}

		writes = append(writes, [2][]byte{a.withPrefix(new.Key), bts})
	}

	return oldRules, a.db.Batch(context.Background(), writes, deletes)
}

func loadPolicy(rule CasbinRule, model model.Model) error {
	parts := []string{rule.PType}
	for _, v := range []string{rule.V0, rule.V1, rule.V2, rule.V3, rule.V4, rule.V5} {
		if v != "" {
			parts = append(parts, v)
		}
	}
	return persist.LoadPolicyLine(strings.Join(parts, ", "), model)
}

func convertRule(ptype string, line []string) CasbinRule {
	rule := CasbinRule{PType: ptype}
	fields := []*string{&rule.V0, &rule.V1, &rule.V2, &rule.V3, &rule.V4, &rule.V5}
	keySlice := []string{ptype}
	for i, field := range fields {
		if i >= len(line) {
			break
		}
		*field = line[i]
		keySlice = append(keySlice, line[i])
	}
	rule.Key = strings.Join(keySlice, "::")
	return rule
}
