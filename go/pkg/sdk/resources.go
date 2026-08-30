/*
Copyright 2026 The Antfly Contributors

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

package sdk

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"github.com/antflydb/antfly/go/pkg/sdk/oapi"
)

// CreateTable creates a new table
func (c *AntflyClient) CreateTable(ctx context.Context, tableName string, req CreateTableRequest) error {
	indexNames := make([]string, 0, len(req.Indexes))
	for indexName := range req.Indexes {
		indexNames = append(indexNames, indexName)
	}
	sort.Strings(indexNames)
	for _, indexName := range indexNames {
		indexConfig := req.Indexes[indexName]
		if err := validateCreateIndexRequest(indexConfig); err != nil {
			return fmt.Errorf("validating index %q: %w", indexName, err)
		}
	}
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

// CreateIndex creates a new index and returns its normalized effective config.
func (c *AntflyClient) CreateIndex(ctx context.Context, tableName, indexName string, config CreateIndexRequest) (*CreatedIndex, error) {
	if err := validateCreateIndexRequest(config); err != nil {
		return nil, fmt.Errorf("validating index %q: %w", indexName, err)
	}
	resp, err := c.client.CreateIndex(ctx, tableName, indexName, config)
	if err != nil {
		return nil, fmt.Errorf("creating index: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("creating index: %w", readErrorResponse(resp))
	}
	var created CreatedIndex
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return nil, fmt.Errorf("parsing create index response: %w", err)
	}
	return &created, nil
}

// validateCreateIndexRequest keeps every transport entry point aligned with
// the relationship checks used by NewCreateIndexRequest. CreateIndexRequest is
// an exported generated union, so callers may construct it directly without
// passing through the convenience builder.
func validateCreateIndexRequest(config CreateIndexRequest) error {
	data, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	discriminator, err := config.Discriminator()
	if err != nil {
		return fmt.Errorf("read type: %w", err)
	}
	indexType := IndexType(discriminator)
	if !indexType.Valid() {
		return fmt.Errorf("unknown index type %q", discriminator)
	}
	return validateIndexRequestRelationships(data, indexType)
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
		name, err := createdIndexName(idx.Config)
		if err != nil {
			return nil, fmt.Errorf("parsing response index identity: %w", err)
		}
		indexes[name] = idx
	}
	return indexes, nil
}

func createdIndexName(config oapi.CreatedIndex) (string, error) {
	value, err := config.ValueByDiscriminator()
	if err != nil {
		return "", err
	}
	var name string
	switch typed := value.(type) {
	case CreatedFullTextIndex:
		name = typed.Name
	case CreatedEmbeddingsIndex:
		name = typed.Name
	case CreatedGraphIndex:
		name = typed.Name
	case CreatedAlgebraicIndex:
		name = typed.Name
	default:
		return "", fmt.Errorf("unsupported created index type %T", value)
	}
	if name == "" {
		return "", fmt.Errorf("created index name is empty")
	}
	return name, nil
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

// ListArtifactEnrichments lists table-level generated artifact enrichments.
func (c *AntflyClient) ListArtifactEnrichments(ctx context.Context, tableName string) (*TableArtifactEnrichmentList, error) {
	resp, err := c.client.ListArtifactEnrichments(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("listing artifact enrichments: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("listing artifact enrichments: %w", readErrorResponse(resp))
	}

	var result TableArtifactEnrichmentList
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing artifact enrichment list response: %w", err)
	}
	return &result, nil
}

// PutArtifactEnrichment registers or replaces a table-level generated artifact enrichment.
func (c *AntflyClient) PutArtifactEnrichment(ctx context.Context, tableName, artifactName string, config EnrichmentConfig) error {
	resp, err := c.client.PutArtifactEnrichment(ctx, tableName, artifactName, config)
	if err != nil {
		return fmt.Errorf("putting artifact enrichment: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("putting artifact enrichment: %w", readErrorResponse(resp))
	}
	return nil
}

// DeleteArtifactEnrichment deletes a table-level generated artifact enrichment.
func (c *AntflyClient) DeleteArtifactEnrichment(ctx context.Context, tableName, artifactName string) error {
	resp, err := c.client.DeleteArtifactEnrichment(ctx, tableName, artifactName)
	if err != nil {
		return fmt.Errorf("deleting artifact enrichment: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("deleting artifact enrichment: %w", readErrorResponse(resp))
	}
	return nil
}

// ListDocumentArtifactManifests lists generated artifact manifests attached to a document.
func (c *AntflyClient) ListDocumentArtifactManifests(ctx context.Context, tableName, key string, detail string) (*DocumentArtifactManifestList, error) {
	var params *oapi.ListDocumentArtifactManifestsParams
	if detail != "" {
		params = &oapi.ListDocumentArtifactManifestsParams{
			Detail: oapi.ListDocumentArtifactManifestsParamsDetail(detail),
		}
	}
	resp, err := c.client.ListDocumentArtifactManifests(ctx, tableName, key, params)
	if err != nil {
		return nil, fmt.Errorf("listing document artifact manifests: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("listing document artifact manifests: %w", readErrorResponse(resp))
	}

	var result DocumentArtifactManifestList
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing document artifact manifest list response: %w", err)
	}
	return &result, nil
}

// GetDocumentArtifactManifest gets one generated artifact manifest attached to a document.
func (c *AntflyClient) GetDocumentArtifactManifest(ctx context.Context, tableName, key, artifactName string, detail string) (*DocumentArtifactManifest, error) {
	var params *oapi.GetDocumentArtifactManifestParams
	if detail != "" {
		params = &oapi.GetDocumentArtifactManifestParams{
			Detail: oapi.GetDocumentArtifactManifestParamsDetail(detail),
		}
	}
	resp, err := c.client.GetDocumentArtifactManifest(ctx, tableName, key, artifactName, params)
	if err != nil {
		return nil, fmt.Errorf("getting document artifact manifest: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("getting document artifact manifest: %w", readErrorResponse(resp))
	}

	var result DocumentArtifactManifest
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing document artifact manifest response: %w", err)
	}
	return &result, nil
}

// ReprocessDocumentArtifact reprocesses one generated artifact for one document.
func (c *AntflyClient) ReprocessDocumentArtifact(ctx context.Context, tableName, key, artifactName string) (*DocumentArtifactReprocessResponse, error) {
	resp, err := c.client.ReprocessDocumentArtifact(ctx, tableName, key, artifactName)
	if err != nil {
		return nil, fmt.Errorf("reprocessing document artifact: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("reprocessing document artifact: %w", readErrorResponse(resp))
	}

	var result DocumentArtifactReprocessResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing document artifact reprocess response: %w", err)
	}
	return &result, nil
}

// ReprocessDocumentArtifactRange runs one bounded table-wide reprocess pass for an artifact.
func (c *AntflyClient) ReprocessDocumentArtifactRange(ctx context.Context, tableName, artifactName string, request DocumentArtifactTableReprocessRequest) (*DocumentArtifactTableReprocessResponse, error) {
	resp, err := c.client.ReprocessDocumentArtifactRange(ctx, tableName, artifactName, request)
	if err != nil {
		return nil, fmt.Errorf("reprocessing document artifact range: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("reprocessing document artifact range: %w", readErrorResponse(resp))
	}

	var result DocumentArtifactTableReprocessResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing document artifact range reprocess response: %w", err)
	}
	return &result, nil
}

// StartDocumentArtifactReprocessJob starts a durable table-wide artifact reprocess job.
func (c *AntflyClient) StartDocumentArtifactReprocessJob(ctx context.Context, tableName, artifactName string, request DocumentArtifactReprocessJobStartRequest) (*DocumentArtifactReprocessJob, error) {
	resp, err := c.client.StartDocumentArtifactReprocessJob(ctx, tableName, artifactName, request)
	if err != nil {
		return nil, fmt.Errorf("starting document artifact reprocess job: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("starting document artifact reprocess job: %w", readErrorResponse(resp))
	}

	var result DocumentArtifactReprocessJob
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing document artifact reprocess job response: %w", err)
	}
	return &result, nil
}

// GetDocumentArtifactReprocessJob gets a durable artifact reprocess job.
func (c *AntflyClient) GetDocumentArtifactReprocessJob(ctx context.Context, tableName, artifactName, jobID string) (*DocumentArtifactReprocessJob, error) {
	resp, err := c.client.GetDocumentArtifactReprocessJob(ctx, tableName, artifactName, jobID)
	if err != nil {
		return nil, fmt.Errorf("getting document artifact reprocess job: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("getting document artifact reprocess job: %w", readErrorResponse(resp))
	}

	var result DocumentArtifactReprocessJob
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing document artifact reprocess job response: %w", err)
	}
	return &result, nil
}

// AdvanceDocumentArtifactReprocessJob advances a durable artifact reprocess job.
func (c *AntflyClient) AdvanceDocumentArtifactReprocessJob(ctx context.Context, tableName, artifactName, jobID string) (*DocumentArtifactReprocessJob, error) {
	resp, err := c.client.AdvanceDocumentArtifactReprocessJob(ctx, tableName, artifactName, jobID)
	if err != nil {
		return nil, fmt.Errorf("advancing document artifact reprocess job: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("advancing document artifact reprocess job: %w", readErrorResponse(resp))
	}

	var result DocumentArtifactReprocessJob
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing document artifact reprocess job response: %w", err)
	}
	return &result, nil
}

// CancelDocumentArtifactReprocessJob cancels a durable artifact reprocess job.
func (c *AntflyClient) CancelDocumentArtifactReprocessJob(ctx context.Context, tableName, artifactName, jobID string) (*DocumentArtifactReprocessJob, error) {
	resp, err := c.client.CancelDocumentArtifactReprocessJob(ctx, tableName, artifactName, jobID)
	if err != nil {
		return nil, fmt.Errorf("canceling document artifact reprocess job: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("canceling document artifact reprocess job: %w", readErrorResponse(resp))
	}

	var result DocumentArtifactReprocessJob
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing document artifact reprocess job response: %w", err)
	}
	return &result, nil
}

// Backup backs up a table
func (c *AntflyClient) Backup(ctx context.Context, tableName, backupID, location, connection string) error {
	if tableName == "" {
		return fmt.Errorf("empty table name")
	}

	req := oapi.BackupRequest{
		BackupId:   backupID,
		Location:   location,
		Connection: connection,
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

// TableRestoreOptions identifies a table backup and the connection authorized
// to read it. IdempotencyKey is optional; an empty value omits the header.
type TableRestoreOptions struct {
	BackupID       string
	Location       string
	Connection     string
	IdempotencyKey string
}

// Restore starts a durable asynchronous table restore.
func (c *AntflyClient) Restore(ctx context.Context, tableName string, options TableRestoreOptions) (*RestoreJob, error) {
	if tableName == "" {
		return nil, fmt.Errorf("empty table name")
	}
	if options.BackupID == "" {
		return nil, fmt.Errorf("empty backup ID")
	}
	if options.Location == "" {
		return nil, fmt.Errorf("empty backup location")
	}
	if options.Connection == "" {
		return nil, fmt.Errorf("empty external I/O connection")
	}

	req := oapi.RestoreRequest{
		BackupId:   options.BackupID,
		Location:   options.Location,
		Connection: options.Connection,
	}

	var params *oapi.RestoreTableParams
	if options.IdempotencyKey != "" {
		params = &oapi.RestoreTableParams{IdempotencyKey: &options.IdempotencyKey}
	}
	resp, err := c.client.RestoreTable(ctx, tableName, params, req)
	if err != nil {
		return nil, fmt.Errorf("restore request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Restore API might return 202 Accepted
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("restore failed: %w", readErrorResponse(resp))
	}

	return decodeRestoreJob(resp)
}

// RestoreJob is the durable asynchronous restore state returned by the API.
type RestoreJob = oapi.RestoreJob

// RestoreJobPhase is a durable restore lifecycle phase.
type RestoreJobPhase = oapi.ListRestoreJobsParamsPhase

const (
	RestoreJobPhaseQueued    = oapi.ListRestoreJobsParamsPhaseQueued
	RestoreJobPhaseRunning   = oapi.ListRestoreJobsParamsPhaseRunning
	RestoreJobPhaseSucceeded = oapi.ListRestoreJobsParamsPhaseSucceeded
	RestoreJobPhaseFailed    = oapi.ListRestoreJobsParamsPhaseFailed
	RestoreJobPhaseCancelled = oapi.ListRestoreJobsParamsPhaseCancelled
)

// RestoreJobScope selects table or cluster restore jobs.
type RestoreJobScope = oapi.ListRestoreJobsParamsScope

const (
	RestoreJobScopeTable   = oapi.ListRestoreJobsParamsScopeTable
	RestoreJobScopeCluster = oapi.ListRestoreJobsParamsScopeCluster
)

// RestoreJobListOptions filters and paginates durable restore jobs.
type RestoreJobListOptions struct {
	Limit  int
	Cursor string
	Phase  RestoreJobPhase
	Scope  RestoreJobScope
}

// RestoreJobPage is one newest-first page of durable restore jobs.
type RestoreJobPage struct {
	Jobs       []RestoreJob
	NextCursor string
}

// ClusterBackupResult represents the result of a cluster backup operation
type ClusterBackupResult struct {
	BackupID string
	Status   string
	Tables   []TableBackupStatus
}

// TableBackupStatus represents backup status for a single table
type TableBackupStatus struct {
	Name             string
	Status           string
	Error            string
	Code             string
	Retryable        bool
	BackupID         string
	ArtifactBackupID string
}

// ClusterBackup backs up multiple tables or all tables in the cluster
func (c *AntflyClient) ClusterBackup(ctx context.Context, backupID, location, connection string, tableNames []string) (*ClusterBackupResult, error) {
	req := oapi.ClusterBackupRequest{
		BackupId:   backupID,
		Location:   location,
		Connection: connection,
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
			Name:             t.Name,
			Status:           string(t.Status),
			Error:            t.Error,
			Code:             string(t.Code),
			Retryable:        t.Retryable,
			BackupID:         t.BackupId,
			ArtifactBackupID: t.ArtifactBackupId,
		}
	}

	return &ClusterBackupResult{
		BackupID: result.BackupId,
		Status:   string(result.Status),
		Tables:   tables,
	}, nil
}

// ClusterRestoreOptions configures a durable cluster restore job.
type ClusterRestoreOptions struct {
	BackupID       string
	Location       string
	Connection     string
	TableNames     []string
	RestoreMode    string
	IdempotencyKey string
}

// ClusterRestore starts a durable asynchronous cluster restore.
func (c *AntflyClient) ClusterRestore(ctx context.Context, options ClusterRestoreOptions) (*RestoreJob, error) {
	if options.BackupID == "" {
		return nil, fmt.Errorf("empty backup ID")
	}
	if options.Location == "" {
		return nil, fmt.Errorf("empty backup location")
	}
	if options.Connection == "" {
		return nil, fmt.Errorf("empty external I/O connection")
	}
	req := oapi.ClusterRestoreRequest{
		BackupId:   options.BackupID,
		Location:   options.Location,
		Connection: options.Connection,
		TableNames: options.TableNames,
	}
	if options.RestoreMode != "" {
		req.RestoreMode = oapi.ClusterRestoreRequestRestoreMode(options.RestoreMode)
	}

	var params *oapi.RestoreParams
	if options.IdempotencyKey != "" {
		params = &oapi.RestoreParams{IdempotencyKey: &options.IdempotencyKey}
	}
	resp, err := c.client.Restore(ctx, params, req)
	if err != nil {
		return nil, fmt.Errorf("cluster restore request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Restore returns 202 Accepted
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("cluster restore failed: %w", readErrorResponse(resp))
	}

	return decodeRestoreJob(resp)
}

// GetRestoreJob returns durable restore progress and terminal results.
func (c *AntflyClient) GetRestoreJob(ctx context.Context, jobID string) (*RestoreJob, error) {
	resp, err := c.client.GetRestoreJob(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("get restore job request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("get restore job failed: %w", readErrorResponse(resp))
	}
	return decodeRestoreJob(resp)
}

// CancelRestoreJob requests cancellation at the next safe restore boundary.
func (c *AntflyClient) CancelRestoreJob(ctx context.Context, jobID string) (*RestoreJob, error) {
	resp, err := c.client.CancelRestoreJob(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("cancel restore job request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("cancel restore job failed: %w", readErrorResponse(resp))
	}
	return decodeRestoreJob(resp)
}

// ListRestoreJobsPage returns one authorization-filtered page of durable restore jobs.
func (c *AntflyClient) ListRestoreJobsPage(ctx context.Context, options RestoreJobListOptions) (*RestoreJobPage, error) {
	params := &oapi.ListRestoreJobsParams{
		Limit:  options.Limit,
		Cursor: options.Cursor,
		Phase:  options.Phase,
		Scope:  options.Scope,
	}
	resp, err := c.client.ListRestoreJobs(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("list restore jobs request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("list restore jobs failed: %w", readErrorResponse(resp))
	}
	var result oapi.RestoreJobList
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parsing restore jobs response: %w", err)
	}
	return &RestoreJobPage{Jobs: result.Jobs, NextCursor: result.NextCursor}, nil
}

// ListRestoreJobs returns all authorized jobs matching the supplied filters.
func (c *AntflyClient) ListRestoreJobs(ctx context.Context, options RestoreJobListOptions) ([]RestoreJob, error) {
	var jobs []RestoreJob
	seen := make(map[string]struct{})
	for {
		page, err := c.ListRestoreJobsPage(ctx, options)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, page.Jobs...)
		if page.NextCursor == "" {
			return jobs, nil
		}
		if _, duplicate := seen[page.NextCursor]; duplicate {
			return nil, fmt.Errorf("list restore jobs returned a repeated cursor %q", page.NextCursor)
		}
		seen[page.NextCursor] = struct{}{}
		options.Cursor = page.NextCursor
	}
}

func decodeRestoreJob(resp *http.Response) (*RestoreJob, error) {
	var job RestoreJob
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, fmt.Errorf("parsing restore job response: %w", err)
	}
	return &job, nil
}

// BackupInfo represents metadata about a stored backup
type BackupInfo struct {
	BackupID      string
	Timestamp     string
	AntflyVersion string
	Tables        []string
}

// BackupPage is one stable manifest-order page of backups.
type BackupPage struct {
	Backups    []BackupInfo
	NextCursor string
}

// BackupListOptions configures one backup-list page.
type BackupListOptions struct {
	Location   string
	Connection string
	Cursor     string
	Limit      int
}

// ListBackupsPage returns one page of cluster backups at the specified location.
func (c *AntflyClient) ListBackupsPage(ctx context.Context, options BackupListOptions) (*BackupPage, error) {
	if options.Location == "" {
		return nil, fmt.Errorf("empty backup location")
	}
	if options.Connection == "" {
		return nil, fmt.Errorf("empty external I/O connection")
	}
	params := &oapi.ListBackupsParams{Location: options.Location, Connection: options.Connection}
	if options.Cursor != "" {
		params.Cursor = &options.Cursor
	}
	if options.Limit > 0 {
		params.Limit = &options.Limit
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

	return &BackupPage{Backups: backups, NextCursor: result.NextCursor}, nil
}

// ListBackups lists all backups while preserving connection and page-size options.
func (c *AntflyClient) ListBackups(ctx context.Context, options BackupListOptions) ([]BackupInfo, error) {
	var backups []BackupInfo
	seen := make(map[string]struct{})
	if options.Cursor != "" {
		seen[options.Cursor] = struct{}{}
	}
	if options.Limit <= 0 {
		options.Limit = 100
	}
	for {
		page, err := c.ListBackupsPage(ctx, options)
		if err != nil {
			return nil, err
		}
		backups = append(backups, page.Backups...)
		if page.NextCursor == "" {
			return backups, nil
		}
		if _, duplicate := seen[page.NextCursor]; duplicate {
			return nil, fmt.Errorf("list backups returned a repeated cursor %q", page.NextCursor)
		}
		seen[page.NextCursor] = struct{}{}
		options.Cursor = page.NextCursor
	}
}
