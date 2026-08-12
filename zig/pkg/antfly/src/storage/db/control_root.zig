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
const ha_contract = @import("ha_contract.zig");
const document_artifact_child_range = @import("document_artifact_child_range.zig");
const ha_commit_gate = @import("../ha/commit_gate.zig");
const ha_primary = @import("../ha/primary.zig");
const platform_time = @import("antfly_platform").time;

pub const types = @import("types.zig");
pub const aggregations = @import("aggregations_contract.zig");
pub const algebraic = @import("algebraic/control_root.zig");
pub const doc_filter_wire = @import("doc_filter_wire.zig");
pub const background_runtime = @import("../background_runtime.zig");
pub const query_metrics = @import("query_metrics.zig");
pub const enrichment_utf8_text = @import("enrichment/utf8_text.zig");

/// Physical DB values may occur in legacy-only lazy declarations in shared
/// source files. Making the type opaque keeps those declarations parseable
/// while causing any accidental control-side use to fail compilation.
pub const DB = opaque {};
pub const CandidateSource = runtime_callbacks.CandidateSource;
pub const EntityUpsert = runtime_callbacks.EntityUpsert;
pub const EntitySink = runtime_callbacks.EntitySink;
pub const PromotionOwner = runtime_callbacks.PromotionOwner;
pub const HAAsyncEffectMirror = ha_contract.AsyncEffectMirror;
pub const HAAsyncBatchMirror = ha_contract.AsyncBatchMirror;
pub const HAAsyncMetadataMirror = ha_contract.AsyncMetadataMirror;
pub const HAMutationBarrier = @import("../ha/mutation_barrier.zig").MutationBarrier;
pub const HASyncWaitFn = ha_contract.SyncWaitFn;
pub const HAWriteGate = ha_contract.WriteGate;
pub const HAProgressPollFn = *const fn (
    ctx: *anyopaque,
    primary: *ha_primary.Primary,
    target_lsn: u64,
    policy: ha_primary.SyncPolicy,
    round: usize,
) anyerror!void;
pub const HAPrimaryProgressSyncWait = struct {
    max_rounds: usize = 64,
    sleep_ns: u64 = 0,
    poll_ctx: ?*anyopaque = null,
    poll_fn: ?HAProgressPollFn = null,

    pub fn wait(ctx: *anyopaque, primary: *ha_primary.Primary, target_lsn: u64, policy: ha_primary.SyncPolicy) !void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (policy.mode == .async) return;
        if (self.max_rounds == 0) return error.HASyncCommitWaitLimitExceeded;

        var round: usize = 0;
        while (round < self.max_rounds) : (round += 1) {
            if (self.poll_fn) |poll| {
                const poll_ctx = self.poll_ctx orelse return error.HASyncCommitWaitMissingContext;
                try poll(poll_ctx, primary, target_lsn, policy, round);
            }

            const gate = try ha_commit_gate.evaluate(primary, target_lsn, policy);
            if (gate.shouldAcknowledge()) return;
            if (gate.action == .reject) return error.SyncPolicyUnsatisfied;
            if (self.sleep_ns > 0) platform_time.sleepNs(self.sleep_ns);
        }

        return error.HASyncCommitWouldBlock;
    }
};
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
