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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
)

func main() {
	var (
		indexPath   = flag.String("path", "", "Path to the Bleve index directory (required)")
		showMapping = flag.Bool("mapping", false, "Display index mapping details")
		analyzeText = flag.String("analyze", "", "Test analyzer on the specified text")
		queryJSON   = flag.String("query", "", "Test query execution (JSON format)")
		tokenField  = flag.String("tokens", "", "Show tokens for a specific field name")
	)
	flag.Parse()

	if *indexPath == "" {
		fmt.Fprintf(os.Stderr, "Error: --path flag is required\n")
		flag.Usage()
		os.Exit(1)
	}

	// Open the Bleve index
	index, err := bleve.Open(*indexPath)
	if err != nil {
		log.Fatalf("Failed to open index at %s: %v", *indexPath, err)
	}
	defer func() { _ = index.Close() }()

	fmt.Printf("Successfully opened Bleve index at: %s\n\n", *indexPath)

	// Execute requested operations
	if *showMapping {
		displayMapping(index)
	}

	if *analyzeText != "" {
		testAnalyzer(index, *analyzeText)
	}

	if *queryJSON != "" {
		testQuery(index, *queryJSON)
	}

	if *tokenField != "" {
		inspectTokens(index, *tokenField)
	}

	// If no specific operation requested, show basic info
	if !*showMapping && *analyzeText == "" && *queryJSON == "" && *tokenField == "" {
		showBasicInfo(index)
	}
}

func showBasicInfo(index bleve.Index) {
	fmt.Println("=== Basic Index Information ===")

	// Get document count
	docCount, err := index.GetInternal([]byte("docCount"))
	if err == nil {
		fmt.Printf("Document count: %s\n", string(docCount))
	}

	// Show mapping summary
	mappingIf := index.Mapping()
	impl, ok := mappingIf.(*mapping.IndexMappingImpl)
	if ok {
		fmt.Printf("Default document type: %s\n", impl.DefaultType)
		fmt.Printf("Document types: %v\n", getDocumentTypes(impl))
		fmt.Printf("Custom analyzers: %v\n", getAnalyzerNames(impl))
	}

	fmt.Println("\nUse --mapping to see detailed mapping information")
	fmt.Println("Use --analyze \"text\" to test analyzer token generation")
	fmt.Println("Use --query '{\"term\":\"value\",\"field\":\"fieldname\"}' to test queries")
}

func displayMapping(index bleve.Index) {
	fmt.Println("=== Index Mapping Details ===")

	mappingIf := index.Mapping()
	impl, ok := mappingIf.(*mapping.IndexMappingImpl)
	if !ok {
		fmt.Println("Cannot access detailed mapping information")
		return
	}

	// Show document type mappings
	fmt.Printf("Default Type: %s\n\n", impl.DefaultType)

	for typeName, typeMapping := range impl.TypeMapping {
		fmt.Printf("Document Type: %s\n", typeName)

		if typeMapping.Properties != nil {
			for fieldName, fieldMapping := range typeMapping.Properties {
				fmt.Printf("  Field: %s\n", fieldName)

				if fieldMapping.Fields != nil {
					for _, field := range fieldMapping.Fields {
						analyzerName := "default"
						if field.Analyzer != "" {
							analyzerName = field.Analyzer
						}
						subFieldName := fieldName
						if field.Name != "" {
							subFieldName = field.Name
						}
						fmt.Printf("    %s: type=%s, analyzer=%s, store=%t\n",
							subFieldName, field.Type, analyzerName, field.Store)
					}
				}
			}
		}
		fmt.Println()
	}

	// Show custom analyzers
	if impl.CustomAnalysis != nil && len(impl.CustomAnalysis.Analyzers) > 0 {
		fmt.Println("Custom Analyzers:")
		for name, analyzer := range impl.CustomAnalysis.Analyzers {
			fmt.Printf("  %s: %+v\n", name, analyzer)
		}
		fmt.Println()
	}
}

func testAnalyzer(index bleve.Index, text string) {
	fmt.Printf("=== Analyzer Testing: \"%s\" ===\n", text)

	mappingIf := index.Mapping()
	impl, ok := mappingIf.(*mapping.IndexMappingImpl)
	if !ok {
		fmt.Println("Cannot access analyzer information")
		return
	}

	// Test with search_as_you_type analyzer if available
	if impl.CustomAnalysis != nil && impl.CustomAnalysis.Analyzers != nil {
		for analyzerName := range impl.CustomAnalysis.Analyzers {
			if strings.Contains(analyzerName, "search_as_you_type") {
				fmt.Printf("\nTesting with %s analyzer:\n", analyzerName)
				testAnalyzerTokens(index, analyzerName, text)
			}
		}
	}

	// Test with default analyzer
	fmt.Printf("\nTesting with default analyzer:\n")
	testAnalyzerTokens(index, "", text)
}

func testAnalyzerTokens(index bleve.Index, analyzerName string, text string) {
	var analyzer analysis.Analyzer

	if analyzerName != "" {
		analyzer = index.Mapping().AnalyzerNamed(analyzerName)
	} else {
		analyzer = index.Mapping().AnalyzerNamed("standard")
	}

	if analyzer == nil {
		fmt.Printf("  Analyzer '%s' not found\n", analyzerName)
		return
	}

	tokens := analyzer.Analyze([]byte(text))

	fmt.Printf("  Generated %d tokens:\n", len(tokens))
	for i, token := range tokens {
		fmt.Printf("    [%d] \"%s\" (pos: %d, start: %d, end: %d)\n",
			i, string(token.Term), token.Position, token.Start, token.End)
	}
}

func testQuery(index bleve.Index, queryJSON string) {
	fmt.Printf("=== Query Testing ===\n")
	fmt.Printf("Query: %s\n\n", queryJSON)

	// Parse the query JSON to understand its structure
	var queryData map[string]any
	if err := json.Unmarshal([]byte(queryJSON), &queryData); err != nil {
		fmt.Printf("Error parsing query JSON: %v\n", err)
		return
	}

	// Create a Bleve query based on the JSON structure
	var query query.Query
	var err error

	if term, ok := queryData["term"].(string); ok {
		if field, ok := queryData["field"].(string); ok {
			// Term query with specific field
			termQuery := bleve.NewTermQuery(term)
			termQuery.SetField(field)
			query = termQuery
			fmt.Printf("Created term query for field '%s' with term '%s'\n", field, term)
		} else {
			// Term query without specific field
			query = bleve.NewTermQuery(term)
			fmt.Printf("Created term query with term '%s'\n", term)
		}
	} else {
		// Try to parse as query string
		query = bleve.NewQueryStringQuery(queryJSON)
		fmt.Printf("Created query string query\n")
	}

	// Execute the query
	searchReq := bleve.NewSearchRequest(query)
	searchReq.Size = 10
	searchReq.Explain = true

	result, err := index.Search(searchReq)
	if err != nil {
		fmt.Printf("Query execution error: %v\n", err)
		return
	}

	fmt.Printf("Query took: %v\n", result.Took)
	fmt.Printf("Total hits: %d\n", result.Total)
	fmt.Printf("Max score: %f\n", result.MaxScore)
	fmt.Printf("Returned hits: %d\n\n", len(result.Hits))

	for i, hit := range result.Hits {
		fmt.Printf("Hit %d:\n", i+1)
		fmt.Printf("  ID: %s\n", hit.ID)
		fmt.Printf("  Score: %f\n", hit.Score)

		if hit.Fields != nil {
			fmt.Printf("  Fields:\n")
			for field, value := range hit.Fields {
				fmt.Printf("    %s: %v\n", field, value)
			}
		}

		if hit.Expl != nil {
			fmt.Printf("  Explanation: %v\n", hit.Expl)
		}
		fmt.Println()
	}
}

func inspectTokens(index bleve.Index, fieldName string) {
	fmt.Printf("=== Token Inspection for Field: %s ===\n", fieldName)

	// This is a simplified version - in a real implementation you'd need
	// to access the underlying index storage to see actual tokens
	fmt.Printf("Note: Token inspection requires direct access to index internals.\n")
	fmt.Printf("Use --analyze with sample text to see how tokens are generated.\n")

	// Show field mapping for context
	mappingIf := index.Mapping()
	impl, ok := mappingIf.(*mapping.IndexMappingImpl)
	if !ok {
		return
	}

	for typeName, typeMapping := range impl.TypeMapping {
		if typeMapping.Properties != nil {
			if fieldMapping, exists := typeMapping.Properties[fieldName]; exists {
				fmt.Printf("\nField mapping for '%s' in type '%s':\n", fieldName, typeName)
				if fieldMapping.Fields != nil {
					for _, field := range fieldMapping.Fields {
						subFieldName := fieldName
						if field.Name != "" {
							subFieldName = field.Name
						}
						fmt.Printf(
							"  %s: analyzer=%s, type=%s\n",
							subFieldName,
							field.Analyzer,
							field.Type,
						)
					}
				}
			}
		}
	}
}

func getDocumentTypes(impl *mapping.IndexMappingImpl) []string {
	var types []string
	for typeName := range impl.TypeMapping {
		types = append(types, typeName)
	}
	return types
}

func getAnalyzerNames(impl *mapping.IndexMappingImpl) []string {
	var analyzers []string
	if impl.CustomAnalysis != nil && impl.CustomAnalysis.Analyzers != nil {
		for name := range impl.CustomAnalysis.Analyzers {
			analyzers = append(analyzers, name)
		}
	}
	return analyzers
}
