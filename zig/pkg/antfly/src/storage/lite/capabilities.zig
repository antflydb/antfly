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

const builtin = @import("builtin");
const lite_options = @import("antfly_lite_options");

const local_inference_runtime_available = lite_options.local_inference_runtime and builtin.os.tag != .freestanding;

pub const Profile = enum {
    native,
    hosted,
};

pub const supported_inference_modes = [_][]const u8{
    "caller_supplied_artifacts",
    "remote_provider",
    "local_embedded",
    "manual_maintenance",
    "disabled_deferred",
};

const native_available_inference_modes = [_][]const u8{
    "caller_supplied_artifacts",
    "remote_provider",
    "disabled_deferred",
};

const native_local_available_inference_modes = [_][]const u8{
    "caller_supplied_artifacts",
    "remote_provider",
    "local_embedded",
    "disabled_deferred",
};

const native_freestanding_available_inference_modes = [_][]const u8{
    "caller_supplied_artifacts",
    "disabled_deferred",
};

const hosted_available_inference_modes = [_][]const u8{
    "caller_supplied_artifacts",
    "remote_provider",
    "manual_maintenance",
    "disabled_deferred",
};

const hosted_local_available_inference_modes = [_][]const u8{
    "caller_supplied_artifacts",
    "remote_provider",
    "local_embedded",
    "manual_maintenance",
    "disabled_deferred",
};

const hosted_freestanding_available_inference_modes = [_][]const u8{
    "caller_supplied_artifacts",
    "manual_maintenance",
    "disabled_deferred",
};

pub const Capabilities = struct {
    freestanding_build: bool = builtin.os.tag == .freestanding,
    // C ABI threading contract, like sqlite3_threadsafe(): "serialized"
    // means any thread may call any function on a handle concurrently. See
    // zig/CAPI.md "Thread Safety".
    threading: []const u8 = "serialized",
    hosted_profile: bool = false,
    manual_maintenance: bool = false,
    background_enrichment_runtime: bool = true,
    ttl_cleanup_runtime: bool = true,
    transaction_recovery_runtime: bool = true,
    local_template_rendering: bool = true,
    remote_template_rendering: bool = true,
    remote_template_host_callbacks: bool = false,
    inference_mode: []const u8 = "caller_supplied_or_disabled",
    supported_inference_modes: []const []const u8 = &supported_inference_modes,
    available_inference_modes: []const []const u8 = &native_available_inference_modes,
    inference_required: bool = false,
    no_inference_configured_ok: bool = true,
    caller_supplied_artifacts: bool = true,
    caller_supplied_embeddings: bool = true,
    remote_inference_providers: bool = true,
    local_inference_runtime: bool = local_inference_runtime_available,
    generated_enrichment_planning: bool = true,
    text_search: bool = true,
    dense_vector_search: bool = true,
    sparse_vector_search: bool = true,
    hybrid_search: bool = true,
    graph_search: bool = true,
    distributed_shard_ownership: bool = false,
    raft_replication: bool = false,
    cluster_placement: bool = false,
    cross_node_joins: bool = false,
    remote_shard_fanout: bool = false,
    distributed_transaction_coordination: bool = false,
    cluster_heartbeat_status_aggregation: bool = false,
    server_side_autoscaling: bool = false,
    kubernetes_operator: bool = false,
    object_storage_primary: bool = false,
};

pub const InferenceStatus = struct {
    mode: []const u8 = "caller_supplied_or_disabled",
    available_modes: []const []const u8 = &native_available_inference_modes,
    configured: bool = false,
    remote_provider_configured: bool = false,
    local_runtime_configured: bool = false,
    local_runtime_available: bool = false,
    caller_supplied_artifacts: bool = true,
    no_inference_configured_ok: bool = true,
    // Resource budget overrides requested through `InferenceOpenOptions`, 0
    // meaning automatic/unconfigured -- mirrors the CLI's
    // `--inference-host-budget-mb`/`--inference-backend-budget-mb`/
    // `--process-memory-budget-mb` knobs (see standalone/runtime.zig and
    // inference_runtime/runtime.zig). `process_memory_limit_bytes` and
    // `process_memory_limit_source` report the policy the embedded node
    // actually resolved once started (see
    // `inference_provider.createEmbeddedInferenceNode`); they stay at their
    // zero-value/"automatic" defaults until then.
    host_budget_mb: u32 = 0,
    backend_budget_mb: u32 = 0,
    combined_budget_mb: u32 = 0,
    kv_budget_mb: u32 = 0,
    scratch_budget_mb: u32 = 0,
    process_memory_budget_mb: u32 = 0,
    process_memory_limit_bytes: u64 = 0,
    process_memory_limit_source: []const u8 = "automatic",
};

pub const InferenceOpenOptions = struct {
    remote_provider_configured: bool = false,
    local_runtime_configured: bool = false,
    // Explicit resource-budget overrides for a local embedded runtime; 0
    // requests the embedded node's own host-clamped defaults (see
    // inference_provider.zig's `default_host_budget_mb` and friends),
    // matching the CLI's `--inference-host-budget-mb`/
    // `--inference-backend-budget-mb`/`--inference-combined-budget-mb`/
    // `--inference-kv-budget-mb`/`--inference-scratch-budget-mb`/
    // `--process-memory-budget-mb` semantics.
    host_budget_mb: u32 = 0,
    backend_budget_mb: u32 = 0,
    combined_budget_mb: u32 = 0,
    kv_budget_mb: u32 = 0,
    scratch_budget_mb: u32 = 0,
    process_memory_budget_mb: u32 = 0,
};

pub fn capabilitiesForProfile(profile: Profile) Capabilities {
    const freestanding = builtin.os.tag == .freestanding;
    const hosted = profile == .hosted;
    const available_modes: []const []const u8 = if (hosted)
        if (freestanding)
            &hosted_freestanding_available_inference_modes
        else if (local_inference_runtime_available)
            &hosted_local_available_inference_modes
        else
            &hosted_available_inference_modes
    else if (freestanding)
        &native_freestanding_available_inference_modes
    else if (local_inference_runtime_available)
        &native_local_available_inference_modes
    else
        &native_available_inference_modes;
    return .{
        .hosted_profile = hosted,
        .manual_maintenance = hosted,
        .background_enrichment_runtime = !hosted and !freestanding,
        .ttl_cleanup_runtime = !hosted and !freestanding,
        .transaction_recovery_runtime = !hosted and !freestanding,
        .local_template_rendering = true,
        .remote_template_rendering = !freestanding,
        .remote_template_host_callbacks = freestanding,
        .inference_mode = "caller_supplied_or_disabled",
        .supported_inference_modes = &supported_inference_modes,
        .available_inference_modes = available_modes,
        .inference_required = false,
        .no_inference_configured_ok = true,
        .caller_supplied_artifacts = true,
        .caller_supplied_embeddings = true,
        .remote_inference_providers = !freestanding,
        .local_inference_runtime = local_inference_runtime_available,
        .generated_enrichment_planning = true,
        .text_search = true,
        .dense_vector_search = true,
        .sparse_vector_search = true,
        .hybrid_search = true,
        .graph_search = true,
        .distributed_shard_ownership = false,
        .raft_replication = false,
        .cluster_placement = false,
        .cross_node_joins = false,
        .remote_shard_fanout = false,
        .distributed_transaction_coordination = false,
        .cluster_heartbeat_status_aggregation = false,
        .server_side_autoscaling = false,
        .kubernetes_operator = false,
        .object_storage_primary = false,
    };
}

pub fn capabilitiesForProfileWithInferenceStatus(profile: Profile, status: InferenceStatus) Capabilities {
    var caps = capabilitiesForProfile(profile);
    caps.inference_mode = status.mode;
    caps.available_inference_modes = status.available_modes;
    caps.local_inference_runtime = status.local_runtime_available;
    return caps;
}

pub fn inferenceStatusForProfile(profile: Profile) InferenceStatus {
    return inferenceStatusForProfileWithOptions(profile, .{});
}

pub fn inferenceStatusForProfileWithOptions(profile: Profile, opts: InferenceOpenOptions) InferenceStatus {
    const caps = capabilitiesForProfile(profile);
    const remote_configured = opts.remote_provider_configured and caps.remote_inference_providers;
    const local_available = caps.local_inference_runtime;
    const local_configured = opts.local_runtime_configured;
    const available_modes = if (local_available)
        if (profile == .hosted) &hosted_local_available_inference_modes else &native_local_available_inference_modes
    else
        caps.available_inference_modes;
    return .{
        .mode = if (remote_configured)
            "remote_provider"
        else if (local_configured and local_available)
            "local_embedded"
        else
            caps.inference_mode,
        .available_modes = available_modes,
        .configured = remote_configured or (local_configured and local_available),
        .remote_provider_configured = remote_configured,
        .local_runtime_configured = local_configured,
        .local_runtime_available = local_available,
        .caller_supplied_artifacts = caps.caller_supplied_artifacts,
        .no_inference_configured_ok = caps.no_inference_configured_ok,
        .host_budget_mb = opts.host_budget_mb,
        .backend_budget_mb = opts.backend_budget_mb,
        .combined_budget_mb = opts.combined_budget_mb,
        .kv_budget_mb = opts.kv_budget_mb,
        .scratch_budget_mb = opts.scratch_budget_mb,
        .process_memory_budget_mb = opts.process_memory_budget_mb,
    };
}
