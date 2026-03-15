/*
Copyright 2025 The Antfly Contributors

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

package client

import (
	"context"
	"fmt"
	"net/http"

	"github.com/antflydb/antfly/pkg/client/oapi"
	"github.com/antflydb/antfly/pkg/libaf/json"
)

// CreateTable creates a new table
func (c *AntflyClient) CreateTable(ctx context.Context, tableName string, req CreateTableRequest) error {
	resp, err := c.client.CreateTable(ctx, tableName, req)
	if err != nil {
		return fmt.Errorf("creating table: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("creating table: %w", readErrorResponse(resp))
	}
	return nil
}

// DropTable drops an existing table
func (c *AntflyClient) DropTable(ctx context.Context, tableName string) error {
	resp, err := c.client.DropTable(ctx, tableName)
	if err != nil {
		return fmt.Errorf("dropping table: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("dropping table: %w", readErrorResponse(resp))
	}

	return nil
}

func (c *AntflyClient) GetTable(ctx context.Context, tableName string) (*TableStatus, error) {
	resp, err := c.client.GetTable(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("getting table: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("getting table: %w", readErrorResponse(resp))
	}
	// Parse the response
	var table TableStatus
	if err := json.NewDecoder(resp.Body).Decode(&table); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return &table, nil
}

// ListTables lists all tables
func (c *AntflyClient) ListTables(ctx context.Context) ([]TableStatus, error) {
	resp, err := c.client.ListTables(ctx, &oapi.ListTablesParams{})
	if err != nil {
		return nil, fmt.Errorf("listing tables: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("listing tables: %w", readErrorResponse(resp))
	}

	// Parse the response
	var tables []TableStatus
	if err := json.NewDecoder(resp.Body).Decode(&tables); err != nil {
		return nil, fmt.Errorf("parsing list tables response: %w", err)
	}

	return tables, nil
}

// CreateIndex creates a new index on a table
func (c *AntflyClient) CreateIndex(ctx context.Context, tableName, indexName string, config IndexConfig) error {
	resp, err := c.client.CreateIndex(ctx, tableName, indexName, config)
	if err != nil {
		return fmt.Errorf("creating index: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("creating index: %w", readErrorResponse(resp))
	}
	return nil
}

// DropIndex drops an index from a table
func (c *AntflyClient) DropIndex(ctx context.Context, tableName, indexName string) error {
	resp, err := c.client.DropIndex(ctx, tableName, indexName)
	if err != nil {
		return fmt.Errorf("dropping index: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("dropping index: %w", readErrorResponse(resp))
	}
	return nil
}

// ListIndexes lists all indexes for a table
func (c *AntflyClient) ListIndexes(ctx context.Context, tableName string) (map[string]IndexStatus, error) {
	resp, err := c.client.ListIndexes(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("listing indexes: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("listing indexes: %w", readErrorResponse(resp))
	}
	// Parse the response - API returns an array, we convert to a map keyed by index name
	var indexList []IndexStatus
	if err := json.NewDecoder(resp.Body).Decode(&indexList); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	// Convert array to map keyed by index name
	indexes := make(map[string]IndexStatus, len(indexList))
	for _, idx := range indexList {
		indexes[idx.Config.Name] = idx
	}
	return indexes, nil
}

// GetIndex gets a specific index for a table
func (c *AntflyClient) GetIndex(ctx context.Context, tableName, indexName string) (*IndexStatus, error) {
	resp, err := c.client.GetIndex(ctx, tableName, indexName)
	if err != nil {
		return nil, fmt.Errorf("getting index: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("getting index: %w", readErrorResponse(resp))
	}
	// Parse the response
	var index IndexStatus
	if err := json.NewDecoder(resp.Body).Decode(&index); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	return &index, nil
}

// Backup backs up a table
func (c *AntflyClient) Backup(ctx context.Context, tableName, backupID, location string) error {
	if tableName == "" {
		return fmt.Errorf("empty table name")
	}

	req := oapi.BackupRequest{
		BackupId: backupID,
		Location: location,
	}

	resp, err := c.client.BackupTable(ctx, tableName, req)
	if err != nil {
		return fmt.Errorf("backup request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// API might return 201 Created or 202 Accepted
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("backup failed: %w", readErrorResponse(resp))
	}

	return nil
}

// Restore restores a table from a backup
func (c *AntflyClient) Restore(ctx context.Context, tableName, backupID, location string) error {
	if tableName == "" {
		return fmt.Errorf("empty table name")
	}

	req := oapi.RestoreRequest{
		BackupId: backupID,
		Location: location,
	}

	resp, err := c.client.RestoreTable(ctx, tableName, req)
	if err != nil {
		return fmt.Errorf("restore request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Restore API might return 202 Accepted
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("restore failed: %w", readErrorResponse(resp))
	}

	return nil
}

// ClusterBackupResult represents the result of a cluster backup operation
type ClusterBackupResult struct {
	BackupID string
	Status   string
	Tables   []TableBackupStatus
}

// TableBackupStatus represents backup status for a single table
type TableBackupStatus struct {
	Name   string
	Status string
	Error  string
}

// ClusterBackup backs up multiple tables or all tables in the cluster
func (c *AntflyClient) ClusterBackup(ctx context.Context, backupID, location string, tableNames []string) (*ClusterBackupResult, error) {
	req := oapi.ClusterBackupRequest{
		BackupId:   backupID,
		Location:   location,
		TableNames: tableNames,
	}

	resp, err := c.client.Backup(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cluster backup request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("cluster backup failed: %w", readErrorResponse(resp))
	}

	var result oapi.ClusterBackupResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	tables := make([]TableBackupStatus, len(result.Tables))
	for i, t := range result.Tables {
		tables[i] = TableBackupStatus{
			Name:   t.Name,
			Status: string(t.Status),
			Error:  t.Error,
		}
	}

	return &ClusterBackupResult{
		BackupID: result.BackupId,
		Status:   string(result.Status),
		Tables:   tables,
	}, nil
}

// ClusterRestoreResult represents the result of a cluster restore operation
type ClusterRestoreResult struct {
	Status string
	Tables []TableRestoreStatus
}

// TableRestoreStatus represents restore status for a single table
type TableRestoreStatus struct {
	Name   string
	Status string
	Error  string
}

// ClusterRestore restores multiple tables from a cluster backup
func (c *AntflyClient) ClusterRestore(ctx context.Context, backupID, location string, tableNames []string, restoreMode string) (*ClusterRestoreResult, error) {
	req := oapi.ClusterRestoreRequest{
		BackupId:   backupID,
		Location:   location,
		TableNames: tableNames,
	}
	if restoreMode != "" {
		req.RestoreMode = oapi.ClusterRestoreRequestRestoreMode(restoreMode)
	}

	resp, err := c.client.Restore(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("cluster restore request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Restore returns 202 Accepted
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("cluster restore failed: %w", readErrorResponse(resp))
	}

	var result oapi.ClusterRestoreResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	tables := make([]TableRestoreStatus, len(result.Tables))
	for i, t := range result.Tables {
		tables[i] = TableRestoreStatus{
			Name:   t.Name,
			Status: string(t.Status),
			Error:  t.Error,
		}
	}

	return &ClusterRestoreResult{
		Status: string(result.Status),
		Tables: tables,
	}, nil
}

// BackupInfo represents metadata about a stored backup
type BackupInfo struct {
	BackupID      string
	Timestamp     string
	AntflyVersion string
	Tables        []string
}

// ListBackups lists available cluster backups at the specified location
func (c *AntflyClient) ListBackups(ctx context.Context, location string) ([]BackupInfo, error) {
	params := &oapi.ListBackupsParams{
		Location: location,
	}

	resp, err := c.client.ListBackups(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("list backups request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("list backups failed: %w", readErrorResponse(resp))
	}

	var result oapi.BackupListResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing response: %w", err)
	}

	backups := make([]BackupInfo, len(result.Backups))
	for i, b := range result.Backups {
		backups[i] = BackupInfo{
			BackupID:      b.BackupId,
			Timestamp:     b.Timestamp.Format("2006-01-02T15:04:05Z07:00"),
			AntflyVersion: b.AntflyVersion,
			Tables:        b.Tables,
		}
	}

	return backups, nil
}
