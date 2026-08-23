// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

//go:build cgo

package antflylite

import "encoding/json"

// StorageStatus describes the physical Lite database file backing a handle.
type StorageStatus struct {
	Format             string  `json:"format"`
	Engine             string  `json:"engine"`
	PrimaryLayout      string  `json:"primary_layout"`
	ReplayLayout       string  `json:"replay_layout"`
	IndexLayout        string  `json:"index_layout"`
	IndexNamespace     *string `json:"index_namespace,omitempty"`
	FormatVersion      *uint32 `json:"format_version,omitempty"`
	PageSize           *uint32 `json:"page_size,omitempty"`
	ActiveCheckpoint   *uint8  `json:"active_checkpoint,omitempty"`
	CheckpointSequence *uint64 `json:"checkpoint_sequence,omitempty"`
	PageCount          *uint64 `json:"page_count,omitempty"`
}

// InferenceStatus reports the configured inference execution mode for a Lite
// handle. A fresh core Lite database normally reports Configured=false and
// NoInferenceConfiguredOK=true.
type InferenceStatus struct {
	Mode                     string   `json:"mode"`
	AvailableModes           []string `json:"available_modes"`
	Configured               bool     `json:"configured"`
	RemoteProviderConfigured bool     `json:"remote_provider_configured"`
	LocalRuntimeConfigured   bool     `json:"local_runtime_configured"`
	LocalRuntimeAvailable    bool     `json:"local_runtime_available"`
	CallerSuppliedArtifacts  bool     `json:"caller_supplied_artifacts"`
	NoInferenceConfiguredOK  bool     `json:"no_inference_configured_ok"`
}

// Capabilities describes the Lite feature contract advertised by a handle.
type Capabilities struct {
	FreestandingBuild                  bool     `json:"freestanding_build"`
	HostedProfile                      bool     `json:"hosted_profile"`
	ManualMaintenance                  bool     `json:"manual_maintenance"`
	BackgroundEnrichmentRuntime        bool     `json:"background_enrichment_runtime"`
	TTLCleanupRuntime                  bool     `json:"ttl_cleanup_runtime"`
	TransactionRecoveryRuntime         bool     `json:"transaction_recovery_runtime"`
	LocalTemplateRendering             bool     `json:"local_template_rendering"`
	RemoteTemplateRendering            bool     `json:"remote_template_rendering"`
	RemoteTemplateHostCallbacks        bool     `json:"remote_template_host_callbacks"`
	InferenceMode                      string   `json:"inference_mode"`
	SupportedInferenceModes            []string `json:"supported_inference_modes"`
	AvailableInferenceModes            []string `json:"available_inference_modes"`
	InferenceRequired                  bool     `json:"inference_required"`
	NoInferenceConfiguredOK            bool     `json:"no_inference_configured_ok"`
	CallerSuppliedArtifacts            bool     `json:"caller_supplied_artifacts"`
	CallerSuppliedEmbeddings           bool     `json:"caller_supplied_embeddings"`
	RemoteInferenceProviders           bool     `json:"remote_inference_providers"`
	LocalInferenceRuntime              bool     `json:"local_inference_runtime"`
	GeneratedEnrichmentPlanning        bool     `json:"generated_enrichment_planning"`
	TextSearch                         bool     `json:"text_search"`
	DenseVectorSearch                  bool     `json:"dense_vector_search"`
	SparseVectorSearch                 bool     `json:"sparse_vector_search"`
	HybridSearch                       bool     `json:"hybrid_search"`
	GraphSearch                        bool     `json:"graph_search"`
	DistributedShardOwnership          bool     `json:"distributed_shard_ownership"`
	RaftReplication                    bool     `json:"raft_replication"`
	ClusterPlacement                   bool     `json:"cluster_placement"`
	CrossNodeJoins                     bool     `json:"cross_node_joins"`
	RemoteShardFanout                  bool     `json:"remote_shard_fanout"`
	DistributedTransactionCoordination bool     `json:"distributed_transaction_coordination"`
	ClusterHeartbeatStatusAggregation  bool     `json:"cluster_heartbeat_status_aggregation"`
	ServerSideAutoscaling              bool     `json:"server_side_autoscaling"`
	KubernetesOperator                 bool     `json:"kubernetes_operator"`
	ObjectStoragePrimary               bool     `json:"object_storage_primary"`
}

// PendingWorkStatus describes the stable Lite readiness fields for derived
// work. The nested maintenance telemetry is intentionally retained as raw JSON
// because it is operational detail rather than control-plane contract.
type PendingWorkStatus struct {
	DerivedTargetSequence               uint64          `json:"derived_target_sequence"`
	HasAsyncIndexes                     bool            `json:"has_async_indexes"`
	PortableImportPublicationInProgress bool            `json:"portable_import_publication_in_progress"`
	PortableImportRecoveryRequired      bool            `json:"portable_import_recovery_required"`
	PortableRuntimeActivationPending    bool            `json:"portable_runtime_activation_pending"`
	Enrichment                          json.RawMessage `json:"enrichment"`
	Resolution                          json.RawMessage `json:"resolution"`
	Promotion                           json.RawMessage `json:"promotion"`
	TextMerge                           json.RawMessage `json:"text_merge"`
}

// Status is the typed form of StatusJSON. Stats is retained as raw JSON because
// its internal shape is broader than the stable Lite control fields bindings
// need for feature branching.
type Status struct {
	Storage      StorageStatus     `json:"storage"`
	Stats        json.RawMessage   `json:"stats"`
	PendingWork  PendingWorkStatus `json:"pending_work"`
	Inference    InferenceStatus   `json:"inference"`
	Capabilities Capabilities      `json:"capabilities"`
}

// ReplayGeneratedEnrichmentsResult reports how many generated enrichment
// references were recreated from stored documents.
type ReplayGeneratedEnrichmentsResult struct {
	Replayed uint64 `json:"replayed"`
}

// Status returns the typed Lite status document for the database.
func (db *DB) Status() (*Status, error) {
	body, err := db.StatusJSON()
	if err != nil {
		return nil, err
	}
	var status Status
	if err := json.Unmarshal(body, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// PendingWorkStats returns the typed Lite pending-work readiness document.
func (db *DB) PendingWorkStats() (*PendingWorkStatus, error) {
	body, err := db.PendingWorkStatsJSON()
	if err != nil {
		return nil, err
	}
	var pending PendingWorkStatus
	if err := json.Unmarshal(body, &pending); err != nil {
		return nil, err
	}
	return &pending, nil
}

// RunUntilIdleStatus drains pending enrichment and index work and returns the
// typed post-drain readiness document.
func (db *DB) RunUntilIdleStatus() (*PendingWorkStatus, error) {
	body, err := db.RunUntilIdleJSON()
	if err != nil {
		return nil, err
	}
	var pending PendingWorkStatus
	if err := json.Unmarshal(body, &pending); err != nil {
		return nil, err
	}
	return &pending, nil
}

// ReplayGeneratedEnrichments recreates generated enrichment work from stored
// documents and returns the replay count.
func (db *DB) ReplayGeneratedEnrichments() (*ReplayGeneratedEnrichmentsResult, error) {
	body, err := db.ReplayGeneratedEnrichmentsJSON()
	if err != nil {
		return nil, err
	}
	var result ReplayGeneratedEnrichmentsResult
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Capabilities returns the typed Lite capability document for the database.
func (db *DB) Capabilities() (*Capabilities, error) {
	body, err := db.CapabilitiesJSON()
	if err != nil {
		return nil, err
	}
	var caps Capabilities
	if err := json.Unmarshal(body, &caps); err != nil {
		return nil, err
	}
	return &caps, nil
}
