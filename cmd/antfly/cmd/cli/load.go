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
	"runtime"

	antfly "github.com/antflydb/antfly/pkg/client"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/spf13/cobra"
)

func addLoadCommands(parent *cobra.Command) {
	loadCmd := &cobra.Command{
		Use:   "load",
		Short: "Bulk loads data from a newline-delimited JSON file into a table",
		Long: `Reads a newline-delimited JSON file and inserts the data in batches into the specified table.
You can specify which field to use as the ID or provide a template to construct the ID.
The template can use Handlebars syntax with the JSON data available as the context.
Example templates:
  - "{{user_id}}" - uses the user_id field
  - "user:{{email}}" - prefixes email with "user:"
  - "{{type}}:{{id}}" - combines type and id fields
If neither id-field nor id-template is specified, an xxhash of the line is used.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			filePath, _ := cmd.Flags().GetString("file")
			numBatches, _ := cmd.Flags().GetInt("batches")
			batchSize, _ := cmd.Flags().GetInt("size")
			concurrency, _ := cmd.Flags().GetInt("concurrency")
			idField, _ := cmd.Flags().GetString("id-field")
			idTemplate, _ := cmd.Flags().GetString("id-template")
			rateLimit, _ := cmd.Flags().GetFloat64("rate")
			verbose, _ := cmd.Flags().GetBool("verbose")

			if err := antflyClient.BatchLoad(cmd.Context(), tableName, filePath, numBatches, batchSize, concurrency, idField, idTemplate, rateLimit, verbose); err != nil {
				return fmt.Errorf("bulk load failed: %w", err)
			}
			fmt.Fprintln(os.Stderr, "Bulk load command successful.")
			return nil
		},
	}

	insertCmd := &cobra.Command{
		Use:   "insert",
		Short: "Insert a single JSON object into a table",
		Long: `Insert a single JSON object into the specified table with the given key.
Example:
  antfly insert -t users --key user123 --value '{"name": "John", "age": 30}'`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			key, _ := cmd.Flags().GetString("key")
			value, _ := cmd.Flags().GetString("value")

			// Parse the JSON value
			var jsonData map[string]any
			if err := json.Unmarshal([]byte(value), &jsonData); err != nil {
				return fmt.Errorf("failed to parse JSON value %q: %w", value, err)
			}

			// Create a batch request with a single insert
			_, err := antflyClient.Batch(cmd.Context(), tableName, antfly.BatchRequest{Inserts: map[string]any{key: jsonData}})
			if err != nil {
				return fmt.Errorf("insert failed: %w", err)
			}

			fmt.Fprintln(os.Stderr, "Insert successful.")
			return nil
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete a single key from a table",
		Long: `Delete a single key from the specified table.
Example:
  antfly delete -t users --key user123`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			key, _ := cmd.Flags().GetString("key")

			// Create a batch request with a single delete
			result, err := antflyClient.Batch(cmd.Context(), tableName, antfly.BatchRequest{Deletes: []string{key}})
			if err != nil {
				return fmt.Errorf("delete failed: %w", err)
			}

			fmt.Fprintf(os.Stderr, "Delete successful. Deleted: %d\n", result.Deleted)
			return nil
		},
	}

	parent.AddCommand(loadCmd)
	parent.AddCommand(insertCmd)
	parent.AddCommand(deleteCmd)

	// bulk-load flags
	loadCmd.Flags().StringP("table", "t", "", "Name of the table to bulk load data into")
	loadCmd.Flags().StringP("file", "f", "", "Path to the newline-delimited JSON file")
	_ = loadCmd.MarkFlagRequired("file")
	loadCmd.Flags().Int("batches", 100, "Number of batches to process from the file")
	loadCmd.Flags().Int("size", 1000, "Number of items per batch")
	loadCmd.Flags().Int("concurrency", runtime.NumCPU(), "Number of concurrent insert workers")
	loadCmd.Flags().String("id-field", "", "Field from JSON to use as document ID (e.g., 'user_id')")
	loadCmd.Flags().String("id-template", "", "Handlebars template to construct document ID (e.g., 'user:{{email}}')")
	loadCmd.Flags().Float64("rate", 0, "Rate limit for batch inserts (items per second, 0 = unlimited)")
	loadCmd.Flags().BoolP("verbose", "v", false, "Enable verbose output (per-worker and per-batch progress)")
	_ = loadCmd.MarkFlagRequired("table")

	// insert flags
	insertCmd.Flags().StringP("table", "t", "", "Name of the table to insert into")
	insertCmd.Flags().String("key", "", "Key for the document")
	insertCmd.Flags().String("value", "", "JSON value to insert")
	_ = insertCmd.MarkFlagRequired("table")
	_ = insertCmd.MarkFlagRequired("key")
	_ = insertCmd.MarkFlagRequired("value")

	// delete flags
	deleteCmd.Flags().StringP("table", "t", "", "Name of the table to delete from")
	deleteCmd.Flags().String("key", "", "Key of the document to delete")
	_ = deleteCmd.MarkFlagRequired("table")
	_ = deleteCmd.MarkFlagRequired("key")
}
