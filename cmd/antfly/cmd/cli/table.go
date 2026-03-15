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
	"path/filepath"
	"time"

	antfly "github.com/antflydb/antfly/pkg/client"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/spf13/cobra"
)

func addTableCommands(parent *cobra.Command) {
	tableCmd := &cobra.Command{
		Use:   "table",
		Short: "Manage tables in the database",
		Long: `The table command provides subcommands for managing tables in the database,
including creating tables, backing them up, and restoring from backups.

When called with --table/-t and no subcommand, shows details for the specified table.
Without --table, lists all tables.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			formatStr, _ := cmd.Flags().GetString("output")
			format, err := parseOutputFormat(formatStr)
			if err != nil {
				return err
			}

			if tableName == "" {
				tables, err := antflyClient.ListTables(cmd.Context())
				if err != nil {
					return err
				}
				if format == outputJSON {
					return writeJSON(tables)
				}
				printTableList(tables)
				return nil
			}

			table, err := antflyClient.GetTable(cmd.Context(), tableName)
			if err != nil {
				return err
			}
			if format == outputJSON {
				return writeJSON(table)
			}
			printTableStatus(table)
			return nil
		},
	}
	tableCmd.PersistentFlags().StringP("table", "t", "", "Name of the table")
	tableCmd.Flags().StringP("output", "o", "table", "Output format: table, json")

	tableCreateCmd := &cobra.Command{
		Use:   "create",
		Short: "Creates a new table",
		Long: `Creates a new table with the specified name and number of shards.

Example with indexes:
  antfly table create --table products --index '{"name":"product_embeddings","field":"description","embedder":{"provider":"termite","model":"BAAI/bge-small-en-v1.5"}}'

Example with config file:
  antfly table create --table products --file table-config.json

The config file should be a JSON object with optional "indexes", "schema", and "shards" fields:
  {
    "indexes": {
      "title_body": {"type": "embeddings", "template": "{{title}} {{body}}", ...}
    },
    "shards": 1,
    "schema": {}
  }`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			if tableName == "" {
				return fmt.Errorf("--table is required")
			}
			numShards, _ := cmd.Flags().GetInt("shards")
			schemaJSON, _ := cmd.Flags().GetString("schema")
			indexStrings, _ := cmd.Flags().GetStringArray("index")
			configFile, _ := cmd.Flags().GetString("file")

			var indexes map[string]antfly.IndexConfig

			if configFile != "" {
				if len(indexStrings) > 0 {
					return fmt.Errorf("cannot specify both --file and --index")
				}
				data, err := os.ReadFile(filepath.Clean(configFile))
				if err != nil {
					return fmt.Errorf("reading config file %s: %w", configFile, err)
				}
				var fileConfig struct {
					Indexes map[string]antfly.IndexConfig `json:"indexes"`
					Schema  json.RawMessage               `json:"schema"`
					Shards  int                           `json:"shards"`
				}
				if err := json.Unmarshal(data, &fileConfig); err != nil {
					return fmt.Errorf("parsing config file %s: %w", configFile, err)
				}
				indexes = fileConfig.Indexes
				if fileConfig.Shards > 0 {
					numShards = fileConfig.Shards
				}
				if len(fileConfig.Schema) > 0 && schemaJSON == "" {
					schemaJSON = string(fileConfig.Schema)
				}
			} else if len(indexStrings) > 0 {
				indexes = make(map[string]antfly.IndexConfig, len(indexStrings))
				for _, indexStr := range indexStrings {
					var indexConf antfly.IndexConfig
					if err := json.UnmarshalString(indexStr, &indexConf); err != nil {
						return fmt.Errorf("invalid JSON for index: %w", err)
					}
					name := indexConf.Name
					if name == "" {
						return fmt.Errorf("index definition must have a 'name' field of type string")
					}
					indexes[name] = indexConf
				}
			}

			err := antflyClient.CreateTable(cmd.Context(), tableName, numShards, schemaJSON, indexes)
			if err != nil {
				return fmt.Errorf("create table failed: %w", err)
			}
			fmt.Fprintln(os.Stderr, "Create table command successful.")
			return nil
		},
	}

	tableDropCmd := &cobra.Command{
		Use:   "drop",
		Short: "Drops an existing table",
		Long:  `Drops an existing table and all its data. This operation cannot be undone.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			if tableName == "" {
				return fmt.Errorf("--table is required")
			}

			err := antflyClient.DropTable(cmd.Context(), tableName)
			if err != nil {
				return fmt.Errorf("drop table failed: %w", err)
			}
			fmt.Fprintln(os.Stderr, "Drop table command successful.")
			return nil
		},
	}

	tableListCmd := &cobra.Command{
		Use:   "list",
		Short: "Lists all tables",
		Long:  `Lists all tables in the database with their basic information.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			formatStr, _ := cmd.Flags().GetString("output")
			format, err := parseOutputFormat(formatStr)
			if err != nil {
				return err
			}

			tables, err := antflyClient.ListTables(cmd.Context())
			if err != nil {
				return fmt.Errorf("list tables failed: %w", err)
			}
			if format == outputJSON {
				return writeJSON(tables)
			}
			printTableList(tables)
			return nil
		},
	}
	tableListCmd.Flags().StringP("output", "o", "table", "Output format: table, json")

	tableCmd.AddCommand(tableCreateCmd)
	tableCmd.AddCommand(tableDropCmd)
	tableCmd.AddCommand(tableListCmd)

	// create-table flags
	tableCreateCmd.Flags().Int("shards", 0, "Number of shards for the table")
	tableCreateCmd.Flags().String("schema", "", "JSON schema definition")
	tableCreateCmd.Flags().StringArray("index", []string{}, "JSON index definition (can be specified multiple times)")
	tableCreateCmd.Flags().StringP("file", "f", "", "Path to a JSON config file with indexes, schema, and shards")

	backupCmd := newBackupCmd()
	restoreCmd := newRestoreCmd()

	parent.AddCommand(tableCmd)
	parent.AddCommand(backupCmd)
	parent.AddCommand(restoreCmd)
}

func newBackupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Backs up tables to a storage location",
		Long: `Backs up one or more tables to a given location with a unique backup ID.

Single table backup:
  antfly backup --table users --backup-id my-backup --location file:///tmp/backups

Bulk backup (all tables):
  antfly backup --backup-id cluster-backup --location file:///tmp/backups

Bulk backup (selected tables):
  antfly backup --tables users,products --backup-id selective-backup --location s3://bucket/path

List available backups:
  antfly backup --list --location file:///tmp/backups`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initClient(cmd, longTimeoutHTTPClient())
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			listBackups, _ := cmd.Flags().GetBool("list")
			location, _ := cmd.Flags().GetString("location")

			// Handle --list flag
			if listBackups {
				if err := antflyClient.ListBackups(cmd.Context(), location); err != nil {
					return fmt.Errorf("list backups failed: %w", err)
				}
				return nil
			}

			tableName, _ := cmd.Flags().GetString("table")
			tablesStr, _ := cmd.Flags().GetString("tables")
			backupID, _ := cmd.Flags().GetString("backup-id")

			// Single table backup (--table flag provided)
			if tableName != "" {
				if backupID == "" {
					backupID = fmt.Sprintf("backup-%s-%s", tableName, time.Now().Format("20060102150405"))
				}
				if err := antflyClient.Backup(cmd.Context(), tableName, backupID, location); err != nil {
					return fmt.Errorf("backup failed: %w", err)
				}
				fmt.Fprintln(os.Stderr, "Backup command successful.")
				return nil
			}

			// Bulk backup (no --table flag)
			if backupID == "" {
				backupID = fmt.Sprintf("cluster-backup-%s", time.Now().Format("20060102150405"))
			}

			if err := antflyClient.ClusterBackup(cmd.Context(), backupID, location, splitCSV(tablesStr)); err != nil {
				return fmt.Errorf("backup failed: %w", err)
			}
			fmt.Fprintln(os.Stderr, "Backup command successful.")
			return nil
		},
	}

	cmd.Flags().StringP("table", "t", "", "Name of a single table to backup")
	cmd.Flags().String("tables", "", "Comma-separated list of tables to backup (bulk mode)")
	cmd.Flags().String("backup-id", "", "Unique ID for this backup")
	cmd.Flags().String("location", "file:///tmp/antfly_backups", "Backup location (e.g., file:///path/to/dir or s3://bucket/path)")
	cmd.Flags().Bool("list", false, "List available backups at the location")

	return cmd
}

func newRestoreCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restores tables from a backup",
		Long: `Restores one or more tables from a backup.

Single table restore:
  antfly restore --table users --backup-id my-backup --location file:///tmp/backups

Bulk restore (all tables from backup):
  antfly restore --backup-id cluster-backup --location file:///tmp/backups

Bulk restore (selected tables):
  antfly restore --tables users,products --backup-id cluster-backup --location s3://bucket/path

Restore modes (for bulk restore):
  - fail_if_exists: Abort if any table already exists (default)
  - skip_if_exists: Skip existing tables, restore others
  - overwrite: Drop and recreate existing tables`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initClient(cmd, longTimeoutHTTPClient())
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			tableName, _ := cmd.Flags().GetString("table")
			tablesStr, _ := cmd.Flags().GetString("tables")
			backupID, _ := cmd.Flags().GetString("backup-id")
			location, _ := cmd.Flags().GetString("location")
			restoreMode, _ := cmd.Flags().GetString("mode")

			if backupID == "" {
				return fmt.Errorf("--backup-id is required for restore command")
			}

			// Single table restore (--table flag provided)
			if tableName != "" {
				if err := antflyClient.Restore(cmd.Context(), tableName, backupID, location); err != nil {
					return fmt.Errorf("restore failed: %w", err)
				}
				fmt.Fprintln(os.Stderr, "Restore command successfully initiated. It may take some time for the table to become fully available.")
				return nil
			}

			// Bulk restore (no --table flag)
			if err := antflyClient.ClusterRestore(cmd.Context(), backupID, location, splitCSV(tablesStr), restoreMode); err != nil {
				return fmt.Errorf("restore failed: %w", err)
			}
			fmt.Fprintln(os.Stderr, "Restore command successfully initiated. It may take some time for the tables to become fully available.")
			return nil
		},
	}

	cmd.Flags().StringP("table", "t", "", "Name of a single table to restore")
	cmd.Flags().String("tables", "", "Comma-separated list of tables to restore (bulk mode)")
	cmd.Flags().String("backup-id", "", "ID of the backup to restore from (required)")
	cmd.Flags().String("location", "file:///tmp/antfly_backups", "Location of the backup (e.g., file:///path/to/dir or s3://bucket/path)")
	cmd.Flags().String("mode", "fail_if_exists", "Restore mode for bulk restore: fail_if_exists, skip_if_exists, overwrite")

	return cmd
}
