/*
Copyright 2025 The Antfly Authors

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
package cli

import (
	"fmt"
	"os"
	"strings"

	antfly "github.com/antflydb/antfly/pkg/client"
	"github.com/spf13/cobra"
)

func addQueryCommands(parent *cobra.Command) {
	queryCmd := &cobra.Command{
		Use:   "query",
		Short: "Queries data from a table",
		Long: `Queries data from a table using a Bleve query string, with options for field selection,
limits, ordering, semantic natural language search, and model selection for vector search.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			params, err := parseSearchParams(cmd)
			if err != nil {
				return err
			}

			offset, _ := cmd.Flags().GetInt("offset")
			orderByStr, _ := cmd.Flags().GetString("order-by")
			searchAfter, _ := cmd.Flags().GetStringSlice("search-after")
			searchBefore, _ := cmd.Flags().GetStringSlice("search-before")
			aggregationsStr, _ := cmd.Flags().GetString("aggregations")
			verbose, _ := cmd.Flags().GetBool("verbose")
			outputStr, _ := cmd.Flags().GetString("output")

			format, err := parseOutputFormat(outputStr)
			if err != nil {
				return err
			}

			params.Offset = offset
			params.SearchAfter = searchAfter
			params.SearchBefore = searchBefore

			if orderByStr != "" {
				for _, orderBySubStr := range splitCSV(orderByStr) {
					parts := strings.Split(orderBySubStr, ":")
					if len(parts) != 2 {
						return fmt.Errorf("invalid --order-by format %q: expected field:asc or field:desc", orderBySubStr)
					}
					var desc bool
					switch strings.ToLower(parts[1]) {
					case "asc":
						// desc defaults to false
					case "desc":
						desc = true
					default:
						return fmt.Errorf("invalid --order-by format %q: expected field:asc or field:desc", orderBySubStr)
					}
					params.OrderBy = append(params.OrderBy, antfly.SortField{Field: parts[0], Desc: desc})
				}
			}

			if aggregationsStr != "" {
				var parseErr error
				params.Aggregations, parseErr = parseJSONFlag[map[string]antfly.AggregationRequest](aggregationsStr, "aggregations")
				if parseErr != nil {
					return parseErr
				}
			}

			res, err := antflyClient.Query(cmd.Context(), params, verbose)
			if err != nil {
				return fmt.Errorf("query failed: %w", err)
			}

			if len(res.Responses) == 0 {
				fmt.Fprintln(os.Stderr, "No results.")
				return nil
			}

			return formatQueryResults(os.Stdout, res, format)
		},
	}

	lookupCmd := &cobra.Command{
		Use:   "lookup",
		Short: "Looks up a document by its key",
		Long:  `Looks up a specific document in a table using its key.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			key, _ := cmd.Flags().GetString("key")

			if key == "" {
				return fmt.Errorf("--key is required for lookup")
			}

			if err := antflyClient.LookupKey(cmd.Context(), tableName, key); err != nil {
				return fmt.Errorf("lookup failed: %w", err)
			}
			return nil
		},
	}

	parent.AddCommand(queryCmd)
	parent.AddCommand(lookupCmd)

	// query flags
	addSearchFlags(queryCmd)
	queryCmd.Flags().Int("offset", 0, "Number of results to skip (for pagination)")
	queryCmd.Flags().String("order-by", "", "Order by field:asc/desc (e.g., age:desc for descending)")
	queryCmd.Flags().StringSlice("search-after", nil, "Cursor for forward pagination (pass _sort values from last hit)")
	queryCmd.Flags().StringSlice("search-before", nil, "Cursor for backward pagination (pass _sort values from first hit)")
	queryCmd.Flags().String("aggregations", "", `JSON string defining aggregations to compute (e.g. '{"price_stats":{"type":"stats","field":"price"},"categories":{"type":"terms","field":"category","size":10}}')`)
	queryCmd.Flags().Bool("verbose", false, "Enable verbose output")
	queryCmd.Flags().StringP("output", "o", "table", `Output format: table, json, jsonl`)

	// lookup flags
	lookupCmd.Flags().StringP("table", "t", "", "Name of the table to lookup from")
	lookupCmd.Flags().StringP("key", "k", "", "Key of the document to lookup")
	_ = lookupCmd.MarkFlagRequired("table")
	_ = lookupCmd.MarkFlagRequired("key")
}
