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

package evaluator

import (
	"encoding/json"
	"fmt"
	"strings"
)

// FilterNode is a parsed Bleve-style filter that can be evaluated against
// in-memory data. Concrete node types are exported so that consumers (e.g.
// SQL generators) can type-switch on them.
type FilterNode interface {
	Evaluate(doc map[string]any) (bool, error)
}

// ParseFilter parses a Bleve-style filter query JSON into a FilterNode AST.
// An empty/nil filter returns a MatchAllNode. The key-detection order matches
// the subset of the Bleve query DSL used by CDC route filtering and joins.
func ParseFilter(filter json.RawMessage) (FilterNode, error) {
	if len(filter) == 0 {
		return &MatchAllNode{}, nil
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(filter, &raw); err != nil {
		return nil, fmt.Errorf("invalid filter JSON: %w", err)
	}

	return parseRaw(raw)
}

func parseRaw(raw map[string]json.RawMessage) (FilterNode, error) {
	if _, ok := raw["conjuncts"]; ok {
		return parseBool(raw)
	}
	if _, ok := raw["disjuncts"]; ok {
		return parseBool(raw)
	}
	if _, ok := raw["must_not"]; ok {
		return parseBool(raw)
	}
	if q, ok := raw["query"]; ok {
		return parseQueryString(q)
	}
	if _, ok := raw["term"]; ok {
		return parseTerm(raw)
	}
	if _, ok := raw["min"]; ok {
		return parseRange(raw)
	}
	if _, ok := raw["max"]; ok {
		return parseRange(raw)
	}
	if _, ok := raw["match"]; ok {
		return parseMatch(raw)
	}
	if _, ok := raw["prefix"]; ok {
		return parsePrefix(raw)
	}
	if _, ok := raw["wildcard"]; ok {
		return parseWildcard(raw)
	}
	if _, ok := raw["match_all"]; ok {
		return &MatchAllNode{}, nil
	}
	if _, ok := raw["match_none"]; ok {
		return &MatchNoneNode{}, nil
	}

	b, _ := json.Marshal(raw)
	return nil, fmt.Errorf("unsupported filter type: %s", string(b))
}

// --- Concrete node types ---

// MatchAllNode matches all documents.
type MatchAllNode struct{}

func (n *MatchAllNode) Evaluate(map[string]any) (bool, error) { return true, nil }

// MatchNoneNode matches no documents.
type MatchNoneNode struct{}

func (n *MatchNoneNode) Evaluate(map[string]any) (bool, error) { return false, nil }

// TermNode matches documents where Field equals Value (with type coercion).
type TermNode struct {
	Field string
	Value any
}

func (n *TermNode) Evaluate(doc map[string]any) (bool, error) {
	docVal, ok := doc[n.Field]
	if !ok || docVal == nil {
		return false, nil
	}
	return ValuesEqual(docVal, n.Value), nil
}

// MatchNode matches documents where Field equals Value (string match).
// It is kept distinct from TermNode because in Bleve the "match" query
// performs full-text tokenization at query time while "term" does exact
// matching; the distinction matters for future search-backend compatibility
// even though in-memory evaluation is identical.
type MatchNode struct {
	Field string
	Value string
}

func (n *MatchNode) Evaluate(doc map[string]any) (bool, error) {
	docVal, ok := doc[n.Field]
	if !ok || docVal == nil {
		return false, nil
	}
	return ValuesEqual(docVal, n.Value), nil
}

// PrefixNode matches documents where the string field starts with Prefix.
type PrefixNode struct {
	Field  string
	Prefix string
}

func (n *PrefixNode) Evaluate(doc map[string]any) (bool, error) {
	docVal, ok := doc[n.Field]
	if !ok || docVal == nil {
		return false, nil
	}
	s, ok := docVal.(string)
	if !ok {
		return false, nil
	}
	return strings.HasPrefix(s, n.Prefix), nil
}

// WildcardNode matches documents where the string field matches a wildcard
// pattern (* = any sequence, ? = single char).
type WildcardNode struct {
	Field   string
	Pattern string
}

func (n *WildcardNode) Evaluate(doc map[string]any) (bool, error) {
	docVal, ok := doc[n.Field]
	if !ok || docVal == nil {
		return false, nil
	}
	s, ok := docVal.(string)
	if !ok {
		return false, nil
	}
	return MatchWildcard(n.Pattern, s), nil
}

// RangeNode matches documents where the field value falls within [Min, Max].
// Inclusivity is controlled by InclusiveMin/InclusiveMax.
type RangeNode struct {
	Field        string
	Min          any
	Max          any
	InclusiveMin bool
	InclusiveMax bool
}

func (n *RangeNode) Evaluate(doc map[string]any) (bool, error) {
	docVal, ok := doc[n.Field]
	if !ok || docVal == nil {
		return false, nil
	}

	if n.Min != nil {
		cmp := CompareOrdered(docVal, n.Min)
		if n.InclusiveMin {
			if cmp < 0 {
				return false, nil
			}
		} else {
			if cmp <= 0 {
				return false, nil
			}
		}
	}

	if n.Max != nil {
		cmp := CompareOrdered(docVal, n.Max)
		if n.InclusiveMax {
			if cmp > 0 {
				return false, nil
			}
		} else {
			if cmp >= 0 {
				return false, nil
			}
		}
	}

	return true, nil
}

// BoolNode combines sub-filters with AND (Conjuncts), OR (Disjuncts),
// and NOT (MustNot) logic.
type BoolNode struct {
	Conjuncts []FilterNode
	Disjuncts []FilterNode
	MustNot   FilterNode
}

func (n *BoolNode) Evaluate(doc map[string]any) (bool, error) {
	for _, c := range n.Conjuncts {
		matched, err := c.Evaluate(doc)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}

	if len(n.Disjuncts) > 0 {
		anyMatch := false
		for _, d := range n.Disjuncts {
			matched, err := d.Evaluate(doc)
			if err != nil {
				return false, err
			}
			if matched {
				anyMatch = true
				break
			}
		}
		if !anyMatch {
			return false, nil
		}
	}

	if n.MustNot != nil {
		matched, err := n.MustNot.Evaluate(doc)
		if err != nil {
			return false, err
		}
		if matched {
			return false, nil
		}
	}

	return true, nil
}

// QueryTerm represents a single field:value term from a query string.
type QueryTerm struct {
	Field string
	Value string
}

// QueryStringNode matches documents against parsed "field:value OR field:value" terms.
type QueryStringNode struct {
	Terms []QueryTerm
}

func (n *QueryStringNode) Evaluate(doc map[string]any) (bool, error) {
	if len(n.Terms) == 0 {
		return true, nil
	}

	for _, t := range n.Terms {
		docVal, exists := doc[t.Field]
		if !exists || docVal == nil {
			continue
		}
		if ValuesEqual(docVal, t.Value) {
			return true, nil
		}
	}

	return false, nil
}

// --- Parsers ---

func extractField(raw map[string]json.RawMessage) (string, error) {
	fieldRaw, ok := raw["field"]
	if !ok {
		return "", fmt.Errorf("filter query missing 'field'")
	}
	var field string
	if err := json.Unmarshal(fieldRaw, &field); err != nil {
		return "", fmt.Errorf("invalid field value: %w", err)
	}
	return field, nil
}

func parseTerm(raw map[string]json.RawMessage) (*TermNode, error) {
	var term any
	if err := json.Unmarshal(raw["term"], &term); err != nil {
		return nil, fmt.Errorf("invalid term value: %w", err)
	}
	field, err := extractField(raw)
	if err != nil {
		return nil, err
	}
	return &TermNode{Field: field, Value: term}, nil
}

func parseMatch(raw map[string]json.RawMessage) (*MatchNode, error) {
	var match string
	if err := json.Unmarshal(raw["match"], &match); err != nil {
		return nil, fmt.Errorf("invalid match value: %w", err)
	}
	field, err := extractField(raw)
	if err != nil {
		return nil, err
	}
	return &MatchNode{Field: field, Value: match}, nil
}

func parsePrefix(raw map[string]json.RawMessage) (*PrefixNode, error) {
	var prefix string
	if err := json.Unmarshal(raw["prefix"], &prefix); err != nil {
		return nil, fmt.Errorf("invalid prefix value: %w", err)
	}
	field, err := extractField(raw)
	if err != nil {
		return nil, err
	}
	return &PrefixNode{Field: field, Prefix: prefix}, nil
}

func parseWildcard(raw map[string]json.RawMessage) (*WildcardNode, error) {
	var pattern string
	if err := json.Unmarshal(raw["wildcard"], &pattern); err != nil {
		return nil, fmt.Errorf("invalid wildcard value: %w", err)
	}
	field, err := extractField(raw)
	if err != nil {
		return nil, err
	}
	return &WildcardNode{Field: field, Pattern: pattern}, nil
}

func parseRange(raw map[string]json.RawMessage) (*RangeNode, error) {
	field, err := extractField(raw)
	if err != nil {
		return nil, err
	}

	node := &RangeNode{
		Field:        field,
		InclusiveMin: true,
		InclusiveMax: true,
	}

	if minRaw, ok := raw["min"]; ok {
		var min any
		if err := json.Unmarshal(minRaw, &min); err != nil {
			return nil, fmt.Errorf("invalid min value: %w", err)
		}
		node.Min = min
	}

	if incRaw, ok := raw["inclusive_min"]; ok {
		if err := json.Unmarshal(incRaw, &node.InclusiveMin); err != nil {
			return nil, fmt.Errorf("invalid inclusive_min value: %w", err)
		}
	}

	if maxRaw, ok := raw["max"]; ok {
		var max any
		if err := json.Unmarshal(maxRaw, &max); err != nil {
			return nil, fmt.Errorf("invalid max value: %w", err)
		}
		node.Max = max
	}

	if incRaw, ok := raw["inclusive_max"]; ok {
		if err := json.Unmarshal(incRaw, &node.InclusiveMax); err != nil {
			return nil, fmt.Errorf("invalid inclusive_max value: %w", err)
		}
	}

	return node, nil
}

func parseBool(raw map[string]json.RawMessage) (*BoolNode, error) {
	node := &BoolNode{}

	if conjRaw, ok := raw["conjuncts"]; ok {
		var conjuncts []map[string]json.RawMessage
		if err := json.Unmarshal(conjRaw, &conjuncts); err != nil {
			return nil, fmt.Errorf("invalid conjuncts: %w", err)
		}
		for _, sub := range conjuncts {
			child, err := parseRaw(sub)
			if err != nil {
				return nil, err
			}
			node.Conjuncts = append(node.Conjuncts, child)
		}
	}

	if disjRaw, ok := raw["disjuncts"]; ok {
		var disjuncts []map[string]json.RawMessage
		if err := json.Unmarshal(disjRaw, &disjuncts); err != nil {
			return nil, fmt.Errorf("invalid disjuncts: %w", err)
		}
		for _, sub := range disjuncts {
			child, err := parseRaw(sub)
			if err != nil {
				return nil, err
			}
			node.Disjuncts = append(node.Disjuncts, child)
		}
	}

	if mustNotRaw, ok := raw["must_not"]; ok {
		var mustNot map[string]json.RawMessage
		if err := json.Unmarshal(mustNotRaw, &mustNot); err != nil {
			return nil, fmt.Errorf("invalid must_not: %w", err)
		}
		child, err := parseRaw(mustNot)
		if err != nil {
			return nil, err
		}
		node.MustNot = child
	}

	return node, nil
}

func parseQueryString(queryRaw json.RawMessage) (FilterNode, error) {
	var queryStr string
	if err := json.Unmarshal(queryRaw, &queryStr); err != nil {
		return nil, fmt.Errorf("invalid query string value: %w", err)
	}

	if queryStr == "*" || queryStr == "" {
		return &MatchAllNode{}, nil
	}

	terms := strings.Split(queryStr, " OR ")
	var parsed []QueryTerm
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		field, value, ok := SplitFieldValue(term)
		if !ok {
			return nil, fmt.Errorf("unsupported query string syntax: %q", term)
		}
		value = UnescapeQueryString(value)
		parsed = append(parsed, QueryTerm{Field: field, Value: value})
	}

	if len(parsed) == 0 {
		return &MatchAllNode{}, nil
	}

	return &QueryStringNode{Terms: parsed}, nil
}

// --- Helpers ---

// MatchWildcard matches a Bleve-style wildcard pattern against a string.
// * matches any sequence of characters, ? matches a single character.
// Uses two-row DP for O(m*n) time and O(n) space.
func MatchWildcard(pattern, s string) bool {
	pn, sn := len(pattern), len(s)
	prev := make([]bool, sn+1)
	curr := make([]bool, sn+1)
	prev[0] = true

	for pi := 1; pi <= pn; pi++ {
		for j := range curr {
			curr[j] = false
		}
		pc := pattern[pi-1]
		if pc == '*' {
			curr[0] = prev[0]
			for j := 1; j <= sn; j++ {
				curr[j] = prev[j] || curr[j-1]
			}
		} else {
			for j := 1; j <= sn; j++ {
				if pc == '?' || pc == s[j-1] {
					curr[j] = prev[j-1]
				}
			}
		}
		prev, curr = curr, prev
	}

	return prev[sn]
}

// SplitFieldValue splits "field:value" on the first unescaped colon.
func SplitFieldValue(s string) (field, value string, ok bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			i++ // skip escaped char
			continue
		}
		if s[i] == ':' {
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}

// UnescapeQueryString reverses the Bleve query string escaping.
func UnescapeQueryString(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			b.WriteByte(s[i])
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}
