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

	"github.com/spf13/cobra"
)

func addIndexCommands(parent *cobra.Command) {
	indexCmd := &cobra.Command{
		Use:   "index",
		Short: "Manage indexes on tables",
		Long: `The index command provides subcommands for managing indexes on tables,
including creating vector and text indexes.

When called with --table/-t and no subcommand, lists all indexes for the table.
When called with --table/-t and --index/-i, shows details for the specified index.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			if tableName == "" {
				return cmd.Help()
			}
			formatStr, _ := cmd.Flags().GetString("output")
			format, err := parseOutputFormat(formatStr)
			if err != nil {
				return err
			}

			indexName, _ := cmd.Flags().GetString("index")
			if indexName != "" {
				index, err := antflyClient.GetIndex(cmd.Context(), tableName, indexName)
				if err != nil {
					return err
				}
				if format == outputJSON {
					return writeJSON(index)
				}
				printIndexStatus(index)
				return nil
			}

			indexes, err := antflyClient.ListIndexes(cmd.Context(), tableName)
			if err != nil {
				return err
			}
			if format == outputJSON {
				return writeJSON(indexes)
			}
			printIndexList(tableName, indexes)
			return nil
		},
	}
	indexCmd.Flags().StringP("output", "o", "table", "Output format: table, json")
	indexCmd.PersistentFlags().StringP("table", "t", "", "Name of the table")
	indexCmd.PersistentFlags().StringP("index", "i", "", "Name of the index")
	_ = indexCmd.MarkPersistentFlagRequired("table")

	indexCreateCmd := &cobra.Command{
		Use:   "create",
		Short: "Creates an index on a table",
		Long: `Creates a new vector or text index on a specified field within a table.
You can specify either a field name or a template to construct the indexed value.
The template can use Handlebars syntax with the document data available as the context.
Example templates:
  - "{{title}} {{body}}" - combines title and body fields
  - "Category: {{category}} - {{description}}" - prefixes and combines fields
  - "{{user.name}} ({{user.email}})" - accesses nested fields`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			indexName, _ := cmd.Flags().GetString("index")
			if indexName == "" {
				return fmt.Errorf("--index is required")
			}
			indexType, _ := cmd.Flags().GetString("type")
			field, _ := cmd.Flags().GetString("field")
			template, _ := cmd.Flags().GetString("template")
			dimension, _ := cmd.Flags().GetInt("dimension")
			embCfgJSON, _ := cmd.Flags().GetString("embedder")
			sumCfgJSON, _ := cmd.Flags().GetString("generator")
			chunkerCfgJSON, _ := cmd.Flags().GetString("chunker")

			if err := antflyClient.CreateIndex(cmd.Context(), tableName, indexName, indexType, field, template, dimension, embCfgJSON, sumCfgJSON, chunkerCfgJSON); err != nil {
				return fmt.Errorf("create index failed: %w", err)
			}
			fmt.Fprintln(os.Stderr, "Create index command successful.")
			return nil
		},
	}

	indexDropCmd := &cobra.Command{
		Use:   "drop",
		Short: "Drops an index from a table",
		Long:  `Drops an existing index from a table. This operation cannot be undone.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			indexName, _ := cmd.Flags().GetString("index")
			if indexName == "" {
				return fmt.Errorf("--index is required")
			}

			if err := antflyClient.DropIndex(cmd.Context(), tableName, indexName); err != nil {
				return fmt.Errorf("drop index failed: %w", err)
			}
			fmt.Fprintln(os.Stderr, "Drop index command successful.")
			return nil
		},
	}

	indexListCmd := &cobra.Command{
		Use:   "list",
		Short: "Lists all indexes for a table",
		Long:  `Lists all indexes for a table with their configuration details.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			formatStr, _ := cmd.Flags().GetString("output")
			format, err := parseOutputFormat(formatStr)
			if err != nil {
				return err
			}

			indexes, err := antflyClient.ListIndexes(cmd.Context(), tableName)
			if err != nil {
				return fmt.Errorf("list indexes failed: %w", err)
			}
			if format == outputJSON {
				return writeJSON(indexes)
			}
			printIndexList(tableName, indexes)
			return nil
		},
	}
	indexListCmd.Flags().StringP("output", "o", "table", "Output format: table, json")

	indexGetCmd := &cobra.Command{
		Use:   "get",
		Short: "Gets an index for a table",
		Long:  `Gets an index for a table with its configuration details.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			indexName, _ := cmd.Flags().GetString("index")
			if indexName == "" {
				return fmt.Errorf("--index is required")
			}
			formatStr, _ := cmd.Flags().GetString("output")
			format, err := parseOutputFormat(formatStr)
			if err != nil {
				return err
			}

			index, err := antflyClient.GetIndex(cmd.Context(), tableName, indexName)
			if err != nil {
				return fmt.Errorf("get index failed: %w", err)
			}
			if format == outputJSON {
				return writeJSON(index)
			}
			printIndexStatus(index)
			return nil
		},
	}
	indexGetCmd.Flags().StringP("output", "o", "table", "Output format: table, json")

	indexCmd.AddCommand(indexCreateCmd)
	indexCmd.AddCommand(indexDropCmd)
	indexCmd.AddCommand(indexListCmd)
	indexCmd.AddCommand(indexGetCmd)

	// create-index flags
	indexCreateCmd.Flags().String("type", "embeddings", "Index type (embeddings for vector/embedding indexes, full_text for text indexes, graph for graph indexes)")
	indexCreateCmd.Flags().String("field", "", "Field to index (mutually exclusive with template)")
	indexCreateCmd.Flags().String("template", "", "Handlebars template to construct indexed value (mutually exclusive with field)")
	indexCreateCmd.Flags().Int("dimension", 768, "Dimension of the vector embeddings")
	indexCreateCmd.Flags().String("embedder", `{"provider": "termite", "model": "bge-small-en-v1.5"}`, "JSON string for embedder configuration")
	indexCreateCmd.Flags().String("generator", "", `JSON string for generator configuration (e.g. {"provider": "ollama", "model": "gemma3:4b", "url": "http://localhost:11434"})`)
	indexCreateCmd.Flags().String("chunker", "", `JSON string for chunker configuration (e.g. {"provider": "termite", "model": "chonky-mmbert-small-multilingual-1", "target_tokens": 512, "overlap_tokens": 50})`)

	parent.AddCommand(indexCmd)
}
