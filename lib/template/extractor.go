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

package template

import (
	"fmt"
	"strings"

	"github.com/mbleigh/raymond/ast"
	"github.com/mbleigh/raymond/parser"
)

// FieldExtractor implements the raymond AST Visitor interface to extract
// all field paths referenced in a Handlebars template.
type FieldExtractor struct {
	fields [][]string
	seen   map[string]bool // for deduplication
}

// NewFieldExtractor creates a new FieldExtractor instance.
func NewFieldExtractor() *FieldExtractor {
	return &FieldExtractor{
		fields: make([][]string, 0),
		seen:   make(map[string]bool),
	}
}

// Fields returns the collected field paths.
func (f *FieldExtractor) Fields() [][]string {
	return f.fields
}

// addField adds a field path if it hasn't been seen before.
func (f *FieldExtractor) addField(parts []string) {
	if len(parts) == 0 {
		return
	}

	key := strings.Join(parts, ".")
	if !f.seen[key] {
		f.seen[key] = true
		// Make a copy to avoid sharing the slice
		fieldCopy := make([]string, len(parts))
		copy(fieldCopy, parts)
		f.fields = append(f.fields, fieldCopy)
	}
}

// VisitProgram visits the root program node and traverses all statements.
func (f *FieldExtractor) VisitProgram(node *ast.Program) any {
	if node == nil {
		return nil
	}

	for _, stmt := range node.Body {
		if stmt != nil {
			stmt.Accept(f)
		}
	}

	return nil
}

// VisitMustache visits a mustache statement {{expression}}.
func (f *FieldExtractor) VisitMustache(node *ast.MustacheStatement) any {
	if node == nil {
		return nil
	}

	if node.Expression != nil {
		node.Expression.Accept(f)
	}

	return nil
}

// VisitBlock visits a block statement {{#helper}}...{{/helper}}.
func (f *FieldExtractor) VisitBlock(node *ast.BlockStatement) any {
	if node == nil {
		return nil
	}

	if node.Expression != nil {
		node.Expression.Accept(f)
	}

	if node.Program != nil {
		node.Program.Accept(f)
	}

	if node.Inverse != nil {
		node.Inverse.Accept(f)
	}

	return nil
}

// VisitPartial visits a partial statement {{> partial}}.
func (f *FieldExtractor) VisitPartial(node *ast.PartialStatement) any {
	if node == nil {
		return nil
	}

	// Skip visiting the partial name itself - it's a template reference, not a data field
	// Exception: if Name is a SubExpression (e.g., {{> (lookup . partialName)}}),
	// we should visit it to capture field references within the subexpression
	if subExpr, ok := node.Name.(*ast.SubExpression); ok {
		subExpr.Accept(f)
	}

	// Visit parameters passed to the partial
	for _, param := range node.Params {
		if param != nil {
			param.Accept(f)
		}
	}

	// Visit hash parameters (key=value pairs)
	if node.Hash != nil {
		node.Hash.Accept(f)
	}

	return nil
}

// VisitContent visits a content statement (literal text).
func (f *FieldExtractor) VisitContent(node *ast.ContentStatement) any {
	// Content nodes contain literal text, no fields to extract
	return nil
}

// VisitComment visits a comment statement {{! comment }}.
func (f *FieldExtractor) VisitComment(node *ast.CommentStatement) any {
	// Comments don't contain field references
	return nil
}

// VisitExpression visits an expression node.
func (f *FieldExtractor) VisitExpression(node *ast.Expression) any {
	if node == nil {
		return nil
	}

	if node.Path != nil {
		node.Path.Accept(f)
	}

	// Visit parameters
	for _, param := range node.Params {
		if param != nil {
			param.Accept(f)
		}
	}

	// Visit hash
	if node.Hash != nil {
		node.Hash.Accept(f)
	}

	return nil
}

// VisitSubExpression visits a sub-expression (helper).
func (f *FieldExtractor) VisitSubExpression(node *ast.SubExpression) any {
	if node == nil {
		return nil
	}

	if node.Expression != nil {
		node.Expression.Accept(f)
	}

	return nil
}

// builtinHelpers is a set of Handlebars built-in helper names that should be
// excluded from field path extraction.
var builtinHelpers = map[string]bool{
	"if":     true,
	"unless": true,
	"each":   true,
	"with":   true,
	"lookup": true,
	"log":    true,
	"equal":  true,
	"media":  true, // Genkit helper
}

// specialContextRefs are special Handlebars context references that should be
// excluded from field path extraction.
var specialContextRefs = map[string]bool{
	"this": true,
}

// VisitPath visits a path expression and extracts the field path.
func (f *FieldExtractor) VisitPath(node *ast.PathExpression) any {
	if node == nil {
		return nil
	}

	// Skip special data variables (@root, @index, @key, etc.)
	if node.Data {
		return nil
	}

	// Skip empty paths
	if len(node.Parts) == 0 {
		return nil
	}

	// Skip built-in helpers (these are helper names, not data fields)
	if len(node.Parts) == 1 && builtinHelpers[node.Parts[0]] {
		return nil
	}

	// Skip special context references like "this"
	if len(node.Parts) == 1 && specialContextRefs[node.Parts[0]] {
		return nil
	}

	// Add the field path
	f.addField(node.Parts)

	return nil
}

// VisitString visits a string literal.
func (f *FieldExtractor) VisitString(node *ast.StringLiteral) any {
	// String literals don't contain field references
	return nil
}

// VisitBoolean visits a boolean literal.
func (f *FieldExtractor) VisitBoolean(node *ast.BooleanLiteral) any {
	// Boolean literals don't contain field references
	return nil
}

// VisitNumber visits a number literal.
func (f *FieldExtractor) VisitNumber(node *ast.NumberLiteral) any {
	// Number literals don't contain field references
	return nil
}

// VisitHash visits a hash node (key=value pairs).
func (f *FieldExtractor) VisitHash(node *ast.Hash) any {
	if node == nil {
		return nil
	}

	for _, pair := range node.Pairs {
		if pair != nil {
			pair.Accept(f)
		}
	}

	return nil
}

// VisitHashPair visits a hash pair (key=value).
func (f *FieldExtractor) VisitHashPair(node *ast.HashPair) any {
	if node == nil {
		return nil
	}

	if node.Val != nil {
		node.Val.Accept(f)
	}

	return nil
}

// ExtractFieldPaths parses a Handlebars template and returns all referenced field paths.
// Returns paths like ["title"], ["metadata", "author", "name"], etc.
//
// Example:
//
//	paths, err := ExtractFieldPaths("{{title}} {{author.name}}")
//	// Returns: [["title"], ["author", "name"]]
func ExtractFieldPaths(template string) ([][]string, error) {
	// Parse the template
	program, err := parser.Parse(template)
	if err != nil {
		return nil, fmt.Errorf("failed to parse template: %w", err)
	}

	// Create extractor and visit the AST
	extractor := NewFieldExtractor()
	program.Accept(extractor)

	return extractor.Fields(), nil
}

// IsFieldReferenced checks if a specific field path is referenced in the template.
//
// Example:
//
//	referenced := IsFieldReferenced("{{author.name}}", []string{"author", "name"})
//	// Returns: true
func IsFieldReferenced(template string, fieldPath []string) (bool, error) {
	paths, err := ExtractFieldPaths(template)
	if err != nil {
		return false, err
	}

	searchKey := strings.Join(fieldPath, ".")
	for _, path := range paths {
		if strings.Join(path, ".") == searchKey {
			return true, nil
		}
	}

	return false, nil
}

// NormalizeFieldPath removes common prefixes like "this" and "fields" for schema matching.
// This helps match template field references to schema field definitions.
//
// Example:
//
//	normalized := NormalizeFieldPath([]string{"this", "author", "name"})
//	// Returns: ["author", "name"]
func NormalizeFieldPath(path []string) []string {
	if len(path) == 0 {
		return path
	}

	// Remove "this" prefix
	if path[0] == "this" {
		path = path[1:]
	}

	// Remove "fields" prefix (common in some schema patterns)
	if len(path) > 0 && path[0] == "fields" {
		path = path[1:]
	}

	return path
}
