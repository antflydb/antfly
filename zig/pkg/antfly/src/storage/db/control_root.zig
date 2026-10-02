// Copyright 2026 Antfly, Inc.
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

//! DB-facing types used by distributed query control. Physical consumers must
//! import `mod.zig`; this facade deliberately has no DB, backend, cache, or
//! index-manager implementation edge.

const runtime_preflight = @import("runtime_preflight.zig");
const runtime_callbacks = @import("runtime_callbacks.zig");
const structured_filter_validation = @import("query/structured_filter_validation.zig");
const replication_contract = @import("replication_contract.zig");
const document_artifact_child_range = @import("document_artifact_child_range.zig");

pub const types = @import("types.zig");
pub const coordinated_ttl = @import("../coordinated_ttl.zig");
pub const OrderedApplyReceipt = types.OrderedApplyReceipt;
pub const aggregations = @import("aggregations_contract.zig");
pub const algebraic = @import("algebraic/control_root.zig");
pub const doc_filter_wire = @import("doc_filter_wire.zig");
pub const background_runtime = @import("../background_runtime.zig");
pub const LsmOwnerKind = background_runtime.LsmOwnerKind;
pub const logical_snapshot_manifest_file_name = @import("../backup_codec.zig").logical_snapshot_manifest_file_name;
pub const query_metrics = @import("query_metrics.zig");
pub const enrichment_utf8_text = @import("enrichment/utf8_text.zig");
pub const documentExtractionStoredUnitFingerprintAlloc = @import("enrichment/document_unit_fingerprint.zig").storedPayloadLegacyFingerprintAlloc;
pub const document_query = @import("document_query.zig");

/// Physical DB values may occur in legacy-only lazy declarations in shared
/// source files. Making the type opaque keeps those declarations parseable
/// while causing any accidental control-side use to fail compilation.
pub const DB = opaque {};
pub const CandidateSource = runtime_callbacks.CandidateSource;
pub const EntityUpsert = runtime_callbacks.EntityUpsert;
pub const EntitySink = runtime_callbacks.EntitySink;
pub const PromotionOwner = runtime_callbacks.PromotionOwner;
pub const ReplicationRecordView = @import("replication_record.zig").RecordView;
pub const ReplicationAsyncEffectMirror = replication_contract.AsyncEffectMirror;
pub const ReplicationAsyncBatchMirror = replication_contract.AsyncBatchMirror;
pub const ReplicationAsyncMetadataMirror = replication_contract.AsyncMetadataMirror;
pub const MutationBarrier = @import("antfly_runtime_abi").mutation_barrier.MutationBarrier;
pub const ReplicationWriteGate = replication_contract.WriteGate;

pub const DocumentArtifactChildRangeApplyBatch = document_artifact_child_range.ApplyBatch;
pub const TextMemoryAttributionStats = @import("text_memory_stats.zig").TextMemoryAttributionStats;
pub const TextFieldStats = @import("../../search/distributed_stats.zig").TextFieldStats;
pub const TermDocFreq = @import("../../search/distributed_stats.zig").TermDocFreq;
pub const transform = @import("transform.zig");
pub const enrichment_types = @import("enrichment/enrichment_types.zig");

pub const DocIdentityNamespace = @import("doc_identity_namespace.zig").Namespace;

pub const TextIndexEstimate = runtime_preflight.TextIndexEstimate;
pub const EmbeddingIndexEstimate = runtime_preflight.EmbeddingIndexEstimate;
pub const GraphIndexEstimate = runtime_preflight.GraphIndexEstimate;
pub const RuntimePreflightSummary = runtime_preflight.RuntimePreflightSummary;
pub const RuntimePreflight = runtime_preflight.RuntimePreflight;
pub const preflightRuntimeAlloc = runtime_preflight.preflightRuntimeAlloc;
pub const preflightSearchRequestAlloc = runtime_preflight.preflightSearchRequestAlloc;
pub const deriveRuntimePreflightEstimates = runtime_preflight.deriveEstimateFields;
pub const SortRejectionDiagnostic = runtime_preflight.SortRejectionDiagnostic;
pub const resetLastSortRejectionDiagnostic = runtime_preflight.resetLastSortRejectionDiagnostic;
pub const takeLastSortRejectionDiagnostic = runtime_preflight.takeLastSortRejectionDiagnostic;
pub const peekLastSortRejectionDiagnostic = runtime_preflight.peekLastSortRejectionDiagnostic;
pub const recordSortRejectionDiagnostic = runtime_preflight.recordSortRejectionDiagnostic;
pub const searchRequestHasScoreBearingTextSource = runtime_preflight.searchRequestHasScoreBearingTextSource;
pub const searchRequestHasScoreBearingVectorSource = runtime_preflight.searchRequestHasScoreBearingVectorSource;
pub const searchRequestHasScoreBearingSource = runtime_preflight.searchRequestHasScoreBearingSource;
pub const validateStructuredFilterValueAlloc = structured_filter_validation.validateStructuredFilterValueAlloc;

pub const DenseNativeMigrationPolicySource = runtime_callbacks.DenseNativeMigrationPolicySource;

pub const merge_state = @import("merge_contract.zig");
