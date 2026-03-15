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

package foreign

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/antflydb/antfly/lib/evaluator"
)

// PlaceholderFunc returns the placeholder string for the n-th parameter (1-indexed).
type PlaceholderFunc func(n int) string

// TranslateFilter converts a Bleve-style filter_query (JSON) into a SQL WHERE clause
// with parameterized values. Returns the WHERE clause (without "WHERE"), positional args,
// and any error. An empty/nil filterQuery returns ("", nil, nil).
//
// Field names are validated against knownColumns to prevent SQL injection.
// All values are parameterized via placeholderFn.
func TranslateFilter(filterQuery json.RawMessage, placeholderFn PlaceholderFunc, knownColumns []ForeignColumn) (string, []any, error) {
	if len(filterQuery) == 0 {
		return "", nil, nil
	}

	node, err := evaluator.ParseFilter(filterQuery)
	if err != nil {
		return "", nil, err
	}

	g := &sqlGen{knownColumns: knownColumns}
	argIndex := 1
	g.renderValue = func(val any) string {
		ph := placeholderFn(argIndex)
		g.args = append(g.args, val)
		argIndex++
		return ph
	}

	clause, err := g.nodeToSQL(node)
	if err != nil {
		return "", nil, err
	}
	return clause, g.args, nil
}

// FilterToLiteralSQL translates a bleve-style filter to a SQL expression with
// literal values inlined, suitable for CREATE PUBLICATION WHERE clauses.
// knownColumns may be nil to skip column validation.
func FilterToLiteralSQL(filter json.RawMessage, knownColumns []ForeignColumn) (string, error) {
	if len(filter) == 0 {
		return "", nil
	}

	node, err := evaluator.ParseFilter(filter)
	if err != nil {
		return "", err
	}

	g := &sqlGen{
		knownColumns: knownColumns,
		renderValue:  sqlLiteral,
	}
	return g.nodeToSQL(node)
}

// sqlGen walks a FilterNode AST and produces SQL. The renderValue callback
// controls whether values appear as placeholders (parameterized) or inline
// literals, eliminating the need for two separate tree walks.
type sqlGen struct {
	knownColumns []ForeignColumn
	args         []any
	renderValue  func(val any) string
}

func (g *sqlGen) validateField(field string) error {
	if len(g.knownColumns) == 0 {
		return nil
	}
	for _, col := range g.knownColumns {
		if col.Name == field {
			return nil
		}
	}
	return fmt.Errorf("unknown column %q in filter", field)
}

func (g *sqlGen) nodeToSQL(node evaluator.FilterNode) (string, error) {
	switch n := node.(type) {
	case *evaluator.MatchAllNode:
		return "", nil
	case *evaluator.MatchNoneNode:
		return "FALSE", nil
	case *evaluator.TermNode:
		return g.equalityToSQL(n.Field, n.Value)
	case *evaluator.MatchNode:
		return g.equalityToSQL(n.Field, n.Value)
	case *evaluator.PrefixNode:
		return g.prefixToSQL(n)
	case *evaluator.WildcardNode:
		return g.wildcardToSQL(n)
	case *evaluator.RangeNode:
		return g.rangeToSQL(n)
	case *evaluator.BoolNode:
		return g.boolToSQL(n)
	case *evaluator.QueryStringNode:
		return g.queryStringToSQL(n)
	default:
		return "", fmt.Errorf("unsupported filter node type: %T", node)
	}
}

// equalityToSQL handles both TermNode and MatchNode since they generate
// identical SQL (field = value).
func (g *sqlGen) equalityToSQL(field string, value any) (string, error) {
	if err := g.validateField(field); err != nil {
		return "", err
	}
	return pgQuoteIdentifier(field) + " = " + g.renderValue(value), nil
}

func (g *sqlGen) prefixToSQL(n *evaluator.PrefixNode) (string, error) {
	if err := g.validateField(n.Field); err != nil {
		return "", err
	}
	escaped := escapeLike(n.Prefix) + "%"
	return pgQuoteIdentifier(n.Field) + " LIKE " + g.renderValue(escaped) + ` ESCAPE '\'`, nil
}

func (g *sqlGen) wildcardToSQL(n *evaluator.WildcardNode) (string, error) {
	if err := g.validateField(n.Field); err != nil {
		return "", err
	}
	return pgQuoteIdentifier(n.Field) + " LIKE " + g.renderValue(wildcardToLike(n.Pattern)) + ` ESCAPE '\'`, nil
}

func (g *sqlGen) rangeToSQL(n *evaluator.RangeNode) (string, error) {
	if err := g.validateField(n.Field); err != nil {
		return "", err
	}

	var parts []string

	if n.Min != nil {
		op := ">="
		if !n.InclusiveMin {
			op = ">"
		}
		parts = append(parts, pgQuoteIdentifier(n.Field)+" "+op+" "+g.renderValue(n.Min))
	}

	if n.Max != nil {
		op := "<="
		if !n.InclusiveMax {
			op = "<"
		}
		parts = append(parts, pgQuoteIdentifier(n.Field)+" "+op+" "+g.renderValue(n.Max))
	}

	return strings.Join(parts, " AND "), nil
}

func (g *sqlGen) boolToSQL(n *evaluator.BoolNode) (string, error) {
	var parts []string

	for _, child := range n.Conjuncts {
		clause, err := g.nodeToSQL(child)
		if err != nil {
			return "", err
		}
		if clause != "" {
			parts = append(parts, "("+clause+")")
		}
	}

	if len(n.Disjuncts) > 0 {
		var orParts []string
		for _, child := range n.Disjuncts {
			clause, err := g.nodeToSQL(child)
			if err != nil {
				return "", err
			}
			if clause != "" {
				orParts = append(orParts, "("+clause+")")
			}
		}
		if len(orParts) > 0 {
			parts = append(parts, "("+strings.Join(orParts, " OR ")+")")
		}
	}

	if n.MustNot != nil {
		clause, err := g.nodeToSQL(n.MustNot)
		if err != nil {
			return "", err
		}
		if clause != "" {
			parts = append(parts, "NOT ("+clause+")")
		}
	}

	return strings.Join(parts, " AND "), nil
}

func (g *sqlGen) queryStringToSQL(n *evaluator.QueryStringNode) (string, error) {
	if len(n.Terms) == 0 {
		return "", nil
	}

	for _, t := range n.Terms {
		if err := g.validateField(t.Field); err != nil {
			return "", err
		}
	}

	// Check if all terms target the same field -> use IN clause
	allSameField := true
	for _, t := range n.Terms[1:] {
		if t.Field != n.Terms[0].Field {
			allSameField = false
			break
		}
	}

	if allSameField && len(n.Terms) > 1 {
		vals := make([]string, len(n.Terms))
		for i, t := range n.Terms {
			vals[i] = g.renderValue(t.Value)
		}
		return pgQuoteIdentifier(n.Terms[0].Field) + " IN (" + strings.Join(vals, ", ") + ")", nil
	}

	var parts []string
	for _, t := range n.Terms {
		parts = append(parts, pgQuoteIdentifier(t.Field)+" = "+g.renderValue(t.Value))
	}
	if len(parts) == 1 {
		return parts[0], nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", nil
}

// --- Shared helpers ---

// wildcardToLike converts a Bleve wildcard pattern to SQL LIKE syntax.
func wildcardToLike(pattern string) string {
	var b strings.Builder
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '*':
			b.WriteByte('%')
		case '?':
			b.WriteByte('_')
		case '%', '_':
			b.WriteByte('\\')
			b.WriteByte(pattern[i])
		default:
			b.WriteByte(pattern[i])
		}
	}
	return b.String()
}

// escapeLike escapes SQL LIKE metacharacters (%, _) in a literal string.
func escapeLike(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '%' || s[i] == '_' {
			b.WriteByte('\\')
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// sqlLiteral converts a Go value to a SQL literal string.
func sqlLiteral(v any) string {
	switch val := v.(type) {
	case string:
		return pgQuoteLiteral(val)
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case nil:
		return "NULL"
	default:
		return pgQuoteLiteral(fmt.Sprintf("%v", val))
	}
}
