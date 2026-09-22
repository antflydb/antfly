// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Consumer-local adaptation for the compiled inference provider. Storage and
//! standalone use one implementation for cancellation, progress, binary media,
//! numeric results, and invocation lifetime; the inference node stays opaque.
const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const platform_time = platform.time;
const process_memory_budget = @import("../common/process_memory_budget.zig");
const inference_bridge = @import("inference_bridge.zig");
const inference_connection_abi = @import("../inference_connection_abi.zig");
const runtime_http_abi = @import("../runtime_http_abi.zig");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
const inline_inference_codegen = builtin.is_test and !@import("standalone_runtime_options").linked_inference;
const inference_host = if (inline_inference_codegen) @import("inference_host.zig") else struct {};
const inference_chunker = @import("inference_chunker");
const chunking_types = @import("../chunking/types.zig");
const inference = @import("../inference/mod.zig");
const template = @import("../template.zig");
const readers = @import("antfly_readers");
const transcribing = @import("antfly_transcribing");
const extracting = @import("antfly_extracting");
const enrichment_types = @import("../storage/db/enrichment/enrichment_types.zig");

pub const LocalInferenceConnectionContext = struct {
    handle: *anyopaque,
};

pub fn isInteractiveGeneratePath(path: []const u8) bool {
    for ([_][]const u8{ inference_bridge.ai_api_prefix, inference_bridge.public_api_prefix }) |prefix| {
        if (!std.mem.startsWith(u8, path, prefix)) continue;
        const suffix = path[prefix.len..];
        if (std.mem.eql(u8, suffix, "/generate") or
            std.mem.eql(u8, suffix, "/generate/batch") or
            std.mem.eql(u8, suffix, "/chat/completions")) return true;
    }
    return false;
}

pub const EmbeddedInferenceProviderLifetime = struct {
    const closed_bit: usize = @as(usize, 1) << (@bitSizeOf(usize) - 1);
    const count_mask: usize = closed_bit - 1;

    handle: *anyopaque,
    // Owns the resource-budget capability `createEmbeddedInferenceNode`
    // configures on `handle`; released by `destroyEmbeddedInferenceNode`
    // after the node itself is torn down. Null only if configuration failed
    // (`create()` still succeeded and the node is otherwise usable).
    resource_owner: ?*LiteInferenceResourceOwner = null,
    // Admission and borrower count share one modification order. A separate
    // accepting flag and counter would leave a check/increment window where
    // shutdown could observe zero and destroy the node before that borrower
    // committed its reference.
    state: std.atomic.Value(usize) = .init(0),
    drain_mutex: std.Io.Mutex = .init,
    drained: std.Io.Condition = .init,

    const CallGuard = struct {
        owner: *EmbeddedInferenceProviderLifetime,
        active: bool = true,

        pub fn deinit(self: *@This()) void {
            if (!self.active) return;
            const io = std.Io.Threaded.global_single_threaded.io();
            self.owner.drain_mutex.lockUncancelable(io);
            const previous = self.owner.state.fetchSub(1, .acq_rel);
            std.debug.assert(previous & count_mask > 0);
            if (previous & closed_bit != 0 and previous & count_mask == 1) {
                self.owner.drained.broadcast(io);
            }
            self.owner.drain_mutex.unlock(io);
            self.active = false;
        }
    };

    pub fn acquire(self: *EmbeddedInferenceProviderLifetime) !CallGuard {
        var observed = self.state.load(.acquire);
        while (true) {
            if (observed & closed_bit != 0) return error.InferenceProviderShuttingDown;
            if (observed & count_mask == count_mask)
                return error.InferenceProviderCallCapacityExhausted;
            if (self.state.cmpxchgWeak(observed, observed + 1, .acq_rel, .acquire)) |actual| {
                observed = actual;
                continue;
            }
            return .{ .owner = self };
        }
    }

    pub fn quiesce(self: *EmbeddedInferenceProviderLifetime) void {
        _ = self.state.fetchOr(closed_bit, .acq_rel);
        const io = std.Io.Threaded.global_single_threaded.io();
        self.drain_mutex.lockUncancelable(io);
        defer self.drain_mutex.unlock(io);
        while (self.activeCallCount() != 0) self.drained.waitUncancelable(io, &self.drain_mutex);
    }

    pub fn isAccepting(self: *const EmbeddedInferenceProviderLifetime) bool {
        return self.state.load(.acquire) & closed_bit == 0;
    }

    pub fn activeCallCount(self: *const EmbeddedInferenceProviderLifetime) usize {
        return self.state.load(.acquire) & count_mask;
    }
};

/// Creates a minimal, self-contained embedded inference node and returns its
/// opaque handle. This is the smallest equivalent of standalone/runtime.zig's
/// production `CreateContext` construction, scoped for an in-process,
/// caller-owned inference runtime such as an Antfly Lite handle: no CLI
/// configuration, warm preload, kernel JIT, or prompt cache tuning -- just
/// model auto-discovery under the default `~/.antfly/inference/models`
/// layout (when `models_dir`/`ml_dir` are null) and automatic memory budgets.
///
/// Callers must only invoke this when they know the real inference runtime
/// archive is linked into the final binary (see
/// `pkg/antfly/build/runtime.zig`'s `addCapiInferenceVariantUnits` and
/// `capi/link_anchor_inference.zig`): the default libantfly and antfly
/// executable's storage_kernel archive traps this entry point, and calling
/// through the trap aborts the process.
///
/// `io` must outlive the returned node; destroy it with
/// `destroyEmbeddedInferenceNode` before releasing `io`.
/// Result of `createEmbeddedInferenceNode`: the opaque node handle plus the
/// resource-budget owner `configureLiteInferenceResourceBudget` installed on
/// it (null only if that configuration step itself failed; the node is still
/// usable, but every provider call will fail with the same
/// `ResourceOwnerNotConfigured`-class error a caller reached before this
/// existed).
/// Explicit resource-budget overrides for `createEmbeddedInferenceNode`, 0
/// meaning automatic/host-detected sizing. These mirror the CLI's
/// `--inference-host-budget-mb`/`--inference-backend-budget-mb`/
/// `--process-memory-budget-mb` flags (see standalone/runtime.zig's
/// `CliConfig` and inference_runtime/runtime.zig's `runServer`), letting a
/// Lite handle opt into the same knobs rather than being stuck with whatever
/// the default automatic policy resolves to.
pub const EmbeddedInferenceNodeOptions = struct {
    host_budget_mb: u32 = 0,
    backend_budget_mb: u32 = 0,
    combined_budget_mb: u32 = 0,
    kv_budget_mb: u32 = 0,
    scratch_budget_mb: u32 = 0,
    process_memory_budget_mb: u32 = 0,
};

// Default embedded per-lane generation budgets (MiB), used whenever the
// caller leaves the corresponding `EmbeddedInferenceNodeOptions` field at 0.
// `antfly inference run`'s CLI flags for these same five lanes
// (`--host-budget-mb`/`--backend-budget-mb`/`--combined-budget-mb`/
// `--kv-budget-mb`/`--scratch-budget-mb`) also default to 0 (automatic), but
// that default relies on an operator supplying real values on the command
// line -- confirmed in production qualification work
// (pkg/inference/models/gliner2/GLINER25.md's "Memory budget" section) that
// a boundary-architecture model's admission estimate (encoder + boundary
// head at worst-case single-window capacity, independent of actual request
// size) exceeds what the automatic/zero-value policy admits, and that these
// exact values are known-good: with them, `antfly inference run` extracts
// real long documents against fastino/gliner2.5-base-v1 without
// MemoryBudgetExceeded. An embedded Lite handle has no operator to supply
// overrides, so it defaults to these directly instead of reproducing the
// CLI's own insufficient zero-value default. `effectiveEmbeddedBudgetMb`
// still clamps each to the host-detected envelope so a genuinely small box
// does not get an admission ceiling larger than its own memory.
const default_host_budget_mb: u32 = 16384;
const default_backend_budget_mb: u32 = 16384;
const default_combined_budget_mb: u32 = 32768;
const default_kv_budget_mb: u32 = 4096;
const default_scratch_budget_mb: u32 = 16384;

pub const EmbeddedInferenceNode = struct {
    handle: *anyopaque,
    resource_owner: ?*LiteInferenceResourceOwner,
    // The process-memory envelope this node actually resolved -- either the
    // caller's explicit `process_memory_budget_mb` override or, when that is
    // 0 (the default), the same host/cgroup-detected policy
    // `antfly inference run` and standalone report at startup (see
    // `resolveEmbeddedProcessMemoryBudget` below).
    process_memory_limit_bytes: usize,
    process_memory_limit_source: process_memory_budget.EffectiveSource,
    // Effective per-lane generation budgets actually installed (MiB): either
    // the caller's explicit override, or the host-clamped default above.
    host_budget_mb: u32,
    backend_budget_mb: u32,
    combined_budget_mb: u32,
    kv_budget_mb: u32,
    scratch_budget_mb: u32,
};

fn resolveEmbeddedProcessMemoryBudget(process_memory_budget_mb: u32) !process_memory_budget.EffectiveResolution {
    return process_memory_budget.resolveSystemDetailed(
        if (process_memory_budget_mb == 0) null else @as(usize, process_memory_budget_mb),
        platform.env.getenv(process_memory_budget.canonical_env),
        platform.env.getenv(process_memory_budget.inference_compat_env),
    );
}

// Resolves one generation-budget lane: an explicit override always wins;
// otherwise fall back to `default_mb`, clamped down to the host-detected
// envelope (`detected_limit_bytes`, 0 meaning detection was unavailable, in
// which case the generous default is kept as-is rather than clamped to
// zero).
fn effectiveEmbeddedBudgetMb(override_mb: u32, default_mb: u32, detected_limit_bytes: usize) u32 {
    if (override_mb != 0) return override_mb;
    if (detected_limit_bytes == 0) return default_mb;
    const detected_mb = detected_limit_bytes / (1024 * 1024);
    if (detected_mb == 0) return default_mb;
    return @intCast(@min(@as(usize, default_mb), detected_mb));
}

fn embeddedInferenceMemoryLimitProvenance(
    source: process_memory_budget.EffectiveSource,
) inference_bridge.ProcessMemoryLimitProvenance {
    return switch (source) {
        .explicit => .explicit,
        .cgroup_v2 => .cgroup_v2,
        .cgroup_v1 => .cgroup_v1,
        .host => .host,
        .unavailable => .unavailable,
    };
}

pub fn createEmbeddedInferenceNode(
    data_dir: []const u8,
    io: std.Io,
    options: EmbeddedInferenceNodeOptions,
) !EmbeddedInferenceNode {
    var borrowed_io = io;
    var out_handle: ?*anyopaque = null;
    // Resolve the same host/cgroup-detected process-memory policy the CLI
    // ("antfly inference run" and "antfly standalone") reports at startup
    // instead of hardcoding zero bytes with `.automatic` provenance: an
    // unresolved envelope here previously left every embedded-node sizing
    // decision (including per-request extraction scratch/KV budgets) unable
    // to distinguish "no host information available" from "this box has N
    // GiB", which is what let large in-process extraction requests trip
    // `error.MemoryBudgetExceeded` that an out-of-process `antfly inference
    // run` serving the same input did not.
    const process_memory_resolution = try resolveEmbeddedProcessMemoryBudget(options.process_memory_budget_mb);
    // Per-lane generation budgets: an explicit override always wins; absent
    // one, default to the CLI's own known-good values (see
    // `default_host_budget_mb` and friends above), clamped to the
    // host-detected envelope. Passing 0/automatic here -- this function's
    // previous behavior -- left every embedded node unable to admit a
    // boundary-architecture model's worst-case single-window estimate
    // regardless of request size (see GLINER25.md's "Memory budget"
    // section); `antfly inference run` only worked because an operator
    // supplied generous flags by hand.
    const effective_host_budget_mb = effectiveEmbeddedBudgetMb(options.host_budget_mb, default_host_budget_mb, process_memory_resolution.limit_bytes);
    const effective_backend_budget_mb = effectiveEmbeddedBudgetMb(options.backend_budget_mb, default_backend_budget_mb, process_memory_resolution.limit_bytes);
    const effective_combined_budget_mb = effectiveEmbeddedBudgetMb(options.combined_budget_mb, default_combined_budget_mb, process_memory_resolution.limit_bytes);
    const effective_kv_budget_mb = effectiveEmbeddedBudgetMb(options.kv_budget_mb, default_kv_budget_mb, process_memory_resolution.limit_bytes);
    const effective_scratch_budget_mb = effectiveEmbeddedBudgetMb(options.scratch_budget_mb, default_scratch_budget_mb, process_memory_resolution.limit_bytes);
    std.log.info(
        "lite embedded inference resource policy input_source={s} effective_source={s} configured_limit_bytes={d} effective_limit_bytes={d} host_budget_mb={d} backend_budget_mb={d} combined_budget_mb={d} kv_budget_mb={d} scratch_budget_mb={d}",
        .{
            @tagName(process_memory_resolution.source),
            @tagName(process_memory_resolution.effective_source),
            process_memory_resolution.configured_limit_bytes,
            process_memory_resolution.limit_bytes,
            effective_host_budget_mb,
            effective_backend_budget_mb,
            effective_combined_budget_mb,
            effective_kv_budget_mb,
            effective_scratch_budget_mb,
        },
    );
    const create_context = inference_bridge.CreateContext{
        .abi_version = inference_bridge.abi_version,
        .data_dir_ptr = data_dir.ptr,
        .data_dir_len = data_dir.len,
        .models_dir = .{},
        .ml_dir = .{},
        .host_limit_bytes = try process_memory_budget.mibToBytes(@as(usize, effective_host_budget_mb)),
        .backend_limit_bytes = try process_memory_budget.mibToBytes(@as(usize, effective_backend_budget_mb)),
        .combined_limit_bytes = try process_memory_budget.mibToBytes(@as(usize, effective_combined_budget_mb)),
        .kv_limit_bytes = try process_memory_budget.mibToBytes(@as(usize, effective_kv_budget_mb)),
        .scratch_limit_bytes = try process_memory_budget.mibToBytes(@as(usize, effective_scratch_budget_mb)),
        .process_memory_limit_bytes = process_memory_resolution.limit_bytes,
        .process_memory_limit_provenance = embeddedInferenceMemoryLimitProvenance(
            process_memory_resolution.effective_source,
        ),
        .preload_ptr = null,
        .preload_len = 0,
        .keep_alive = .{},
        // Explicitly unlimited (model_manager.zig: 0 means unbounded), not
        // merely unset. A Lite handle typically enrichs with a small,
        // fixed set of models (an embedder and an extractor, say); the
        // unset default of 10 would technically cover that too, but an
        // interleaved embed/extract workload that evicts and reloads either
        // model between documents is exactly the kind of self-inflicted
        // thrash this embedded node must not reproduce.
        .max_loaded_models = 0,
        .has_max_loaded_models = 1,
        .content_security_json = .{},
        .s3_credentials_json = .{},
        .runtime_config_json = inference_bridge.String.init("{}"),
        .executor = .init(&borrowed_io),
        .out_handle = &out_handle,
    };
    // Unit tests compile the inference call graph directly into the same
    // binary (no archive/trap boundary), so `linkedInferenceApi` would try
    // to resolve the real cross-archive symbol and fail to link. Route
    // through the same inline codegen path `invokeInferenceProvider` uses
    // for tests.
    const handle = if (comptime inline_inference_codegen)
        try inference_host.linkedInferenceCreate(&create_context)
    else handle: {
        const table = try linkedInferenceApi(
            inference_bridge.Capability.provider |
                inference_bridge.Capability.route_manifest |
                inference_bridge.Capability.resource_budget |
                inference_bridge.Capability.request_admission,
        );
        const status = table.create(&create_context);
        if (!status.isOk()) return inference_bridge.errorFromStatus(status);
        break :handle out_handle orelse return error.InferenceRuntimeStartupFailed;
    };
    // `Capability.resource_budget` was requested above, which the standalone
    // runtime treats as a promise that a follow-up `configure()` call will
    // install a resource-budget owner (see runtime.zig's
    // `InferenceResourceBudgetOwner`, wired to the DataServer's storage
    // ResourceManager). A Lite handle has no such storage-tied resource
    // manager to reuse, so every provider call previously failed with
    // `error.ResourceOwnerNotConfigured` the moment it tried to reserve
    // admission. Install a minimal, permissive, host-detected-by-default
    // owner instead: Lite is a single-process embedding of the runtime, not
    // a multi-tenant server, so unconditional admission is the correct
    // policy, matching "antfly inference run"'s own local/host-owned default
    // when no external resource policy is configured.
    const resource_owner = configureLiteInferenceResourceBudget(handle) catch |err| blk: {
        std.log.warn(
            "lite embedded inference resource budget configuration failed, provider calls will fail: {s}",
            .{@errorName(err)},
        );
        break :blk null;
    };
    return .{
        .handle = handle,
        .resource_owner = resource_owner,
        .process_memory_limit_bytes = process_memory_resolution.limit_bytes,
        .process_memory_limit_source = process_memory_resolution.effective_source,
        .host_budget_mb = effective_host_budget_mb,
        .backend_budget_mb = effective_backend_budget_mb,
        .combined_budget_mb = effective_combined_budget_mb,
        .kv_budget_mb = effective_kv_budget_mb,
        .scratch_budget_mb = effective_scratch_budget_mb,
    };
}

/// Counterpart to `createEmbeddedInferenceNode`. Callers must quiesce any
/// `EmbeddedInferenceProviderLifetime` wrapping `handle` before calling this.
pub fn destroyEmbeddedInferenceNode(handle: *anyopaque, resource_owner: ?*LiteInferenceResourceOwner) void {
    if (comptime inline_inference_codegen) {
        inference_host.linkedInferenceDestroy(handle);
    } else {
        linkedInferenceApiInfallible().destroy(handle);
    }
    if (resource_owner) |owner| owner.releaseBaseReference();
}

/// Minimal resource-budget owner for a Lite-embedded inference node. Unlike
/// `standalone/runtime.zig`'s `InferenceResourceBudgetOwner`, this does not
/// track real memory accounting against a shared storage `ResourceManager`:
/// Lite has no such manager, and a single embedded node with no concurrent
/// tenants does not need arbitrated admission. It exists solely to satisfy
/// the resource-budget capability contract every provider call requires.
pub const LiteInferenceResourceOwner = struct {
    references: std.atomic.Value(usize) = .init(1),
    next_lease_token: std.atomic.Value(usize) = .init(1),

    fn releaseBaseReference(self: *LiteInferenceResourceOwner) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous >= 1);
        if (previous == 1) std.heap.c_allocator.destroy(self);
    }
};

fn retainLiteInferenceResourceOwner(context: *anyopaque) callconv(.c) u8 {
    const owner: *LiteInferenceResourceOwner = @ptrCast(@alignCast(context));
    var observed = owner.references.load(.acquire);
    while (true) {
        if (observed == 0) return 0;
        if (owner.references.cmpxchgWeak(observed, observed + 1, .acq_rel, .acquire)) |actual| {
            observed = actual;
            continue;
        }
        return 1;
    }
}

fn releaseLiteInferenceResourceOwner(context: *anyopaque) callconv(.c) void {
    const owner: *LiteInferenceResourceOwner = @ptrCast(@alignCast(context));
    owner.releaseBaseReference();
}

fn reserveLiteInferenceResources(
    context: *anyopaque,
    amounts: *const inference_bridge.AdmissionAmounts,
    out_lease: *usize,
) callconv(.c) inference_bridge.Status {
    _ = amounts;
    const owner: *LiteInferenceResourceOwner = @ptrCast(@alignCast(context));
    out_lease.* = owner.next_lease_token.fetchAdd(1, .acq_rel);
    return .{};
}

fn retainLiteInferenceResources(
    context: *anyopaque,
    lease_token: usize,
    retained: *const inference_bridge.AdmissionAmounts,
) callconv(.c) inference_bridge.Status {
    _ = context;
    _ = lease_token;
    _ = retained;
    return .{};
}

fn releaseLiteInferenceResources(context: *anyopaque, lease_token: usize) callconv(.c) void {
    _ = context;
    _ = lease_token;
}

fn observeLiteInferencePromptCache(context: *anyopaque, observer_id: usize, previous: u64, next: u64) callconv(.c) u8 {
    _ = context;
    _ = observer_id;
    _ = previous;
    _ = next;
    return 1;
}

fn observeLiteInferenceTokenizerCache(context: *anyopaque, observer_id: usize, previous: u64, next: u64) callconv(.c) u8 {
    _ = context;
    _ = observer_id;
    _ = previous;
    _ = next;
    return 1;
}

fn configureLiteInferenceResourceBudget(handle: *anyopaque) !*LiteInferenceResourceOwner {
    const owner = try std.heap.c_allocator.create(LiteInferenceResourceOwner);
    owner.* = .{};
    // `Client.configure` (standalone/inference_worker.zig) calls
    // `retain_context` -- and sets its own `self.budget` field to point at
    // `owner` -- *before* it can fail (for example the `ensureWorker`
    // failure this catches at the call site below): a failure after that
    // point still leaves the client holding a retained reference it will
    // release exactly once, whenever it deinits. Unconditionally destroying
    // `owner` here on any error would be a use-after-free/double-free the
    // instant that later release runs. `releaseBaseReference` is the
    // correct unwind either way: it only frees `owner` once every retained
    // reference -- ours here, and the client's if it got far enough to take
    // one -- has been released.
    errdefer owner.releaseBaseReference();
    var budget = inference_bridge.ResourceBudget{
        .abi_version = inference_bridge.abi_version,
        .context = owner,
        .retain_context = retainLiteInferenceResourceOwner,
        .release_context = releaseLiteInferenceResourceOwner,
        .reserve_admission = reserveLiteInferenceResources,
        .retain_admission = retainLiteInferenceResources,
        .release_admission = releaseLiteInferenceResources,
        .observe_prompt_cache = observeLiteInferencePromptCache,
        .observe_tokenizer_cache = observeLiteInferenceTokenizerCache,
    };
    const configure_context = inference_bridge.ConfigureContext{
        .abi_version = inference_bridge.abi_version,
        .handle = handle,
        .resource_budget = &budget,
    };
    if (comptime inline_inference_codegen) {
        try inference_host.linkedInferenceConfigure(&configure_context);
    } else {
        const status = (try linkedInferenceApi(
            inference_bridge.Capability.resource_budget,
        )).configure(&configure_context);
        if (!status.isOk()) return inference_bridge.errorFromStatus(status);
    }
    return owner;
}

pub fn inferenceBoundaryProvider(lifetime: *EmbeddedInferenceProviderLifetime) inference.managed_embedder.AntflyProvider {
    return .{
        .ptr = lifetime,
        .owns_invocation_admission = true,
        .typed_dense_results = true,
        .embed_dense_texts = inferenceProviderEmbedDenseTexts,
        .embed_dense_texts_with_context = inferenceProviderEmbedDenseTextsWithContext,
        .embed_sparse_texts = inferenceProviderEmbedSparseTexts,
        .embed_sparse_texts_with_context = inferenceProviderEmbedSparseTextsWithContext,
        .embed_dense_parts = inferenceProviderEmbedDenseParts,
        .embed_dense_parts_with_context = inferenceProviderEmbedDensePartsWithContext,
        .embed_dense_rasters = inferenceProviderEmbedDenseRasters,
        .rerank_texts = inferenceProviderRerankTexts,
        .rerank_texts_with_context = inferenceProviderRerankTextsWithContext,
        .generate_text = inferenceProviderGenerateText,
        .generate_text_with_context = inferenceProviderGenerateTextWithContext,
        .generate_messages = inferenceProviderGenerateMessages,
        .generate_messages_with_context = inferenceProviderGenerateMessagesWithContext,
        .generate_messages_with_attachments = inferenceProviderGenerateMessagesWithAttachments,
        .generate_messages_with_attachments_with_context = inferenceProviderGenerateMessagesWithAttachmentsWithContext,
        .model_capabilities = inferenceProviderModelCapabilities,
        .model_capabilities_with_context = inferenceProviderModelCapabilitiesWithContext,
        .chunk_input = inferenceProviderChunkInput,
        .chunk_input_with_context = inferenceProviderChunkInputWithContext,
        .rewrite_texts = inferenceProviderRewriteTexts,
        .classify_texts = inferenceProviderClassifyTexts,
        .generate_json = inferenceProviderGenerateJson,
        .read_images = inferenceProviderReadImages,
        .read_images_with_context = inferenceProviderReadImagesWithContext,
        .read_encoded_images = inferenceProviderReadEncodedImages,
        .read_encoded_images_with_context = inferenceProviderReadEncodedImagesWithContext,
        .read_encoded_images_reported = inferenceProviderReadEncodedImagesReported,
        .read_encoded_images_reported_with_context = inferenceProviderReadEncodedImagesReportedWithContext,
        .read_raster_images_reported = inferenceProviderReadRasterImagesReported,
        .read_raster_images_reported_with_context = inferenceProviderReadRasterImagesReportedWithContext,
        .transcribe_audio = inferenceProviderTranscribeAudio,
        .transcribe_audio_with_context = inferenceProviderTranscribeAudioWithContext,
        .extract = inferenceProviderExtract,
        .extract_with_context = inferenceProviderExtractWithContext,
        .list_models_json = inferenceProviderListModelsJson,
    };
}

pub fn invokeInferenceProvider(
    comptime Result: type,
    alloc: std.mem.Allocator,
    provider_context: *anyopaque,
    operation: inference_bridge.ProviderOperation,
    request: anytype,
    request_context: ?inference.RequestContext,
) !Result {
    return try invokeInferenceProviderWithBinaryContext(Result, alloc, provider_context, operation, request, request_context, &.{}, &.{});
}

pub fn invokeInferenceProviderControlled(
    comptime Result: type,
    alloc: std.mem.Allocator,
    provider_context: *anyopaque,
    operation: inference_bridge.ProviderOperation,
    request: anytype,
    deadline_ns: ?u64,
    cancellation: CancellationToken,
) !Result {
    return try invokeInferenceProviderWithBinaryContext(Result, alloc, provider_context, operation, request, requestContextFromControls(deadline_ns, cancellation), &.{}, &.{});
}

pub fn invokeInferenceProviderWithBinaryControlled(
    comptime Result: type,
    alloc: std.mem.Allocator,
    provider_context: *anyopaque,
    operation: inference_bridge.ProviderOperation,
    request: anytype,
    deadline_ns: ?u64,
    binary_payloads: []const inference_bridge.ProviderBinaryPayload,
    attachment_refs: []const inference_bridge.ProviderAttachmentRef,
    cancellation: CancellationToken,
) !Result {
    return try invokeInferenceProviderWithBinaryContext(Result, alloc, provider_context, operation, request, requestContextFromControls(deadline_ns, cancellation), binary_payloads, attachment_refs);
}

pub fn requestContextFromControls(deadline_ns: ?u64, cancellation: CancellationToken) ?inference.RequestContext {
    if (deadline_ns == null and cancellation.ptr == null) return null;
    return .{
        .io = std.Io.Threaded.global_single_threaded.io(),
        .deadline_ns = deadline_ns,
        .cancellation = if (cancellation.ptr != null) cancellation else null,
    };
}

pub fn invokeInferenceProviderWithBinaryContext(
    comptime Result: type,
    alloc: std.mem.Allocator,
    provider_context: *anyopaque,
    operation: inference_bridge.ProviderOperation,
    request: anytype,
    request_context: ?inference.RequestContext,
    binary_payloads: []const inference_bridge.ProviderBinaryPayload,
    attachment_refs: []const inference_bridge.ProviderAttachmentRef,
) !Result {
    if (request_context) |context| try context.check();
    const lifetime: *EmbeddedInferenceProviderLifetime = @ptrCast(@alignCast(provider_context));
    var call_guard = try lifetime.acquire();
    defer call_guard.deinit();
    const handle = lifetime.handle;
    const request_json = try std.json.Stringify.valueAlloc(alloc, request, .{});
    defer alloc.free(request_json);
    var response_handle: ?*anyopaque = null;
    var response_json: inference_bridge.String = undefined;
    var numeric_result = inference_bridge.NumericResult{};
    const effective_deadline_ns = if (request_context) |active|
        active.deadline_ns orelse platform_time.monotonicNs() +| 5 * std.time.ns_per_min
    else
        platform_time.monotonicNs() +| 5 * std.time.ns_per_min;
    const RequestCancellation = struct {
        pub fn requested(raw: ?*const anyopaque) callconv(.c) u8 {
            const context: *const ?inference.RequestContext = @ptrCast(@alignCast(raw orelse return 1));
            const active = context.* orelse return 0;
            return @intFromBool(if (active.cancellation) |token| token.isCancelled() else false);
        }
    };
    const RequestProgress = struct {
        pub fn update(raw: ?*anyopaque, phase: u8, completed: u64, total: u64, model: inference_bridge.String, backend: inference_bridge.String) callconv(.c) void {
            const context: *const ?inference.RequestContext = @ptrCast(@alignCast(raw orelse return));
            const active = context.* orelse return;
            const progress = active.progress orelse return;
            const typed_phase = std.enums.fromInt(inference.request_context.Phase, phase) orelse return;
            progress.update(.{
                .phase = typed_phase,
                .completed = completed,
                .total = total,
                .model = model.slice(),
                .backend = backend.slice(),
                .deadline_ns = active.deadline_ns,
            });
        }
    };
    const context = inference_bridge.ProviderInvokeContext{
        .abi_version = inference_bridge.abi_version,
        .handle = handle,
        .operation = @intFromEnum(operation),
        .request_json = inference_bridge.String.init(request_json),
        .deadline_ns = effective_deadline_ns,
        .has_deadline = 1,
        .out_response_handle = &response_handle,
        .out_response_json = &response_json,
        .out_numeric_result = if (Result == [][]f32 or Result == []f32) &numeric_result else null,
        .binary_payloads = if (binary_payloads.len > 0) binary_payloads.ptr else null,
        .binary_payloads_len = binary_payloads.len,
        .attachment_refs = if (attachment_refs.len > 0) attachment_refs.ptr else null,
        .attachment_refs_len = attachment_refs.len,
        .cancellation = if (request_context != null and request_context.?.cancellation != null)
            .{ .context = &request_context, .is_cancelled = RequestCancellation.requested }
        else
            .{},
        .progress = if (request_context != null and request_context.?.progress != null)
            .{ .context = @constCast(&request_context), .update_progress = RequestProgress.update }
        else
            .{},
    };
    if (comptime inline_inference_codegen) {
        try inference_host.linkedInferenceInvokeProvider(&context);
    } else {
        const status = (try linkedInferenceApi(
            inference_bridge.Capability.provider,
        )).invoke_provider(&context);
        if (!status.isOk()) return inference_bridge.errorFromStatus(status);
    }
    const owned_response = response_handle orelse return error.InferenceRuntimeResponseMissing;
    defer if (comptime inline_inference_codegen)
        inference_host.linkedInferenceDestroyProviderResponse(owned_response)
    else
        linkedInferenceApiInfallible().destroy_provider_response(owned_response);
    if (request_context) |active| try active.check();
    if (comptime Result == [][]f32 or Result == []f32) {
        if (numeric_result.kind != .absent) {
            if (comptime Result == [][]f32) {
                if (numeric_result.kind != .dense_vectors) return error.InvalidInferenceNumericResult;
                return numeric_result.copyRows(alloc);
            } else {
                if (numeric_result.kind != .scores or numeric_result.len != 1) return error.InvalidInferenceNumericResult;
                const rows = try numeric_result.copyRows(alloc);
                defer alloc.free(rows);
                return rows[0];
            }
        }
    }
    return try std.json.parseFromSliceLeaky(Result, alloc, response_json.slice(), .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

pub fn linkedInferenceApi(required_capabilities: u64) !*const inference_bridge.FunctionTable {
    const table = inference_bridge.antfly_standalone_inference_get_function_table();
    if (!inference_bridge.validFunctionTable(table, required_capabilities))
        return error.UnsupportedVersion;
    return table;
}

pub fn linkedInferenceApiInfallible() *const inference_bridge.FunctionTable {
    return linkedInferenceApi(0) catch @panic("linked inference ABI changed after startup");
}

pub const LocalInferenceInvocationLifetime = struct {
    upstream: runtime_http_abi.CancellationView,
    deadline_ns: u64,

    pub fn expired(self: *const LocalInferenceInvocationLifetime) bool {
        return self.deadline_ns != 0 and platform_time.monotonicNs() >= self.deadline_ns;
    }

    pub fn check(self: *const LocalInferenceInvocationLifetime) !void {
        if (self.upstream.requested()) return error.Canceled;
        if (self.expired()) return error.Timeout;
    }

    pub fn isCancelled(raw: ?*const anyopaque) callconv(.c) u8 {
        const self: *const LocalInferenceInvocationLifetime = @ptrCast(@alignCast(raw orelse return 1));
        return @intFromBool(self.upstream.requested() or self.expired());
    }

    pub fn cancellation(self: *const LocalInferenceInvocationLifetime) runtime_http_abi.CancellationView {
        return .{ .context = self, .is_cancelled = isCancelled };
    }
};

pub fn ownedInferenceConnectionBytes(alloc: std.mem.Allocator, value: []const u8) !inference_connection_abi.OwnedBytes {
    const owned = try alloc.dupe(u8, value);
    return .{
        .ptr = if (owned.len == 0) null else owned.ptr,
        .len = owned.len,
    };
}

pub fn optionalOwnedInferenceConnectionBytes(
    alloc: std.mem.Allocator,
    value: ?[]const u8,
) !inference_connection_abi.OptionalOwnedBytes {
    const present = value orelse return .{};
    return .{
        .bytes = try ownedInferenceConnectionBytes(alloc, present),
        .present = 1,
    };
}

pub fn invokeLocalInferenceConnectionFallible(context: *const inference_connection_abi.InvokeContext) !void {
    if (!inference_connection_abi.validInvokeContext(context)) return error.UnsupportedVersion;
    const local_context: *LocalInferenceConnectionContext = @ptrCast(@alignCast(context.target_context));
    const alloc = context.allocator.asStd();
    const operation = context.operation.slice();
    const body = context.body.slice();
    var lifetime = LocalInferenceInvocationLifetime{
        .upstream = context.cancellation,
        .deadline_ns = context.deadline_ns,
    };
    try lifetime.check();
    const functions: ?*const inference_bridge.FunctionTable = if (comptime inline_inference_codegen)
        null
    else
        try linkedInferenceApi(inference_bridge.Capability.route_manifest);

    var entries_ptr: ?[*]const inference_bridge.RouteManifestEntry = null;
    var entries_len: usize = 0;
    const manifest_context = inference_bridge.RouteManifestContext{
        .abi_version = inference_bridge.abi_version,
        .handle = local_context.handle,
        .out_entries = &entries_ptr,
        .out_len = &entries_len,
    };
    if (comptime inline_inference_codegen) {
        try inference_host.linkedInferenceRouteManifest(&manifest_context);
    } else {
        const status = functions.?.route_manifest(&manifest_context);
        if (!status.isOk()) return inference_bridge.errorFromStatus(status);
    }

    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ inference_bridge.ai_api_prefix, operation });
    defer alloc.free(path);
    const interactive_generate = isInteractiveGeneratePath(path);
    if (interactive_generate)
        _ = enrichment_types.interactive_generate_inflight.fetchAdd(1, .monotonic);
    defer {
        if (interactive_generate)
            _ = enrichment_types.interactive_generate_inflight.fetchSub(1, .monotonic);
    }
    const entries = if (entries_ptr) |ptr| ptr[0..entries_len] else &.{};
    const route_handle = for (entries) |entry| {
        if (entry.method == .post and std.mem.eql(u8, entry.path.slice(), path))
            break entry.route_handle;
    } else return error.UnsupportedInferenceOperation;

    const headers = [_]runtime_http_abi.HeaderView{.{
        .name = runtime_http_abi.Bytes.init("Content-Type"),
        .value = runtime_http_abi.Bytes.init("application/json"),
    }};
    const request = runtime_http_abi.HttpRequestView{
        .method = .post,
        .path = runtime_http_abi.Bytes.init(path),
        .headers_ptr = &headers,
        .headers_len = headers.len,
        .body = runtime_http_abi.OptionalBytes.init(body),
        .content_type = runtime_http_abi.OptionalBytes.init("application/json"),
    };
    var response_handle: ?*anyopaque = null;
    var response_view: runtime_http_abi.HttpResponseView = undefined;
    const handle_context = inference_bridge.HttpHandleContext{
        .abi_version = inference_bridge.abi_version,
        .route_handle = route_handle,
        .request = &request,
        .cancellation = lifetime.cancellation(),
        .stream = context.stream,
        .out_response_handle = &response_handle,
        .out_response = &response_view,
    };
    if (comptime inline_inference_codegen) {
        inference_host.linkedInferenceHandleHttp(&handle_context) catch |err| {
            try lifetime.check();
            return err;
        };
    } else {
        const status = functions.?.handle_http(&handle_context);
        if (!status.isOk()) {
            try lifetime.check();
            return inference_bridge.errorFromStatus(status);
        }
    }
    try lifetime.check();
    const owned_response = response_handle orelse return error.InferenceRuntimeResponseMissing;
    defer if (comptime inline_inference_codegen)
        inference_host.linkedInferenceDestroyHttpResponse(owned_response)
    else
        functions.?.destroy_http_response(owned_response);

    var response: inference_connection_abi.InvokeResponse = .{
        .status = response_view.status,
        .body = try ownedInferenceConnectionBytes(alloc, response_view.body.slice()),
    };
    errdefer alloc.free(response.body.slice());
    var retry_after: ?[]const u8 = null;
    const response_headers = if (response_view.headers_ptr) |ptr| ptr[0..response_view.headers_len] else &.{};
    for (response_headers) |header| {
        if (std.ascii.eqlIgnoreCase(header.name.slice(), "Retry-After")) {
            retry_after = header.value.slice();
            break;
        }
    }
    response.retry_after = try optionalOwnedInferenceConnectionBytes(alloc, retry_after);
    errdefer if (response.retry_after.present != 0) alloc.free(response.retry_after.bytes.slice());
    response.content_type = try optionalOwnedInferenceConnectionBytes(alloc, response_view.content_type.slice());
    context.out_response.* = response;
}

pub fn inferenceProviderEmbedDenseTexts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
) anyerror![][]f32 {
    return try invokeInferenceProvider([][]f32, alloc, handle, .embed_dense_texts, .{
        .model = model,
        .texts = texts,
    }, null);
}

pub fn inferenceProviderEmbedDenseTextsWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
    context: inference.managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    try context.check();
    return try invokeInferenceProviderControlled([][]f32, alloc, handle, .embed_dense_texts_with_context, .{
        .model = model,
        .texts = texts,
        .task_type = context.task_type.canonical(),
        .instruction = context.instruction,
    }, context.request.deadline_ns, context.request.cancellation orelse .none);
}

pub fn inferenceProviderEmbedSparseTexts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
) anyerror![]inference.managed_embedder.SparseEmbedding {
    return try invokeInferenceProvider([]inference.managed_embedder.SparseEmbedding, alloc, handle, .embed_sparse_texts, .{
        .model = model,
        .texts = texts,
    }, null);
}

pub fn inferenceProviderEmbedSparseTextsWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    texts: []const []const u8,
    context: inference.managed_embedder.EmbeddingRequestContext,
) anyerror![]inference.managed_embedder.SparseEmbedding {
    try context.check();
    return try invokeInferenceProvider([]inference.managed_embedder.SparseEmbedding, alloc, handle, .embed_sparse_texts, .{
        .model = model,
        .texts = texts,
    }, context.request);
}

pub fn inferenceProviderEmbedDenseParts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const template.ContentPart,
) anyerror![][]f32 {
    return try inferenceProviderEmbedDensePartsBorrowed(
        handle,
        alloc,
        model,
        parts,
        .embed_dense_parts,
        null,
        null,
        null,
        .none,
    );
}

pub fn inferenceProviderEmbedDensePartsWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const template.ContentPart,
    context: inference.managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    try context.check();
    return try inferenceProviderEmbedDensePartsBorrowed(
        handle,
        alloc,
        model,
        parts,
        .embed_dense_parts_with_context,
        context.task_type.canonical(),
        context.instruction,
        context.request.deadline_ns,
        context.request.cancellation orelse .none,
    );
}

pub fn inferenceProviderEmbedDensePartsBorrowed(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    parts: []const template.ContentPart,
    operation: inference_bridge.ProviderOperation,
    task_type: ?[]const u8,
    instruction: ?[]const u8,
    deadline_ns: ?u64,
    cancellation: CancellationToken,
) ![][]f32 {
    const embedding_wire = @import("../inference/embedding_wire.zig");
    const wire_parts = try alloc.alloc(template.ContentPart, parts.len);
    defer alloc.free(wire_parts);
    const payload_storage = try alloc.alloc(inference_bridge.ProviderBinaryPayload, parts.len);
    defer alloc.free(payload_storage);
    const ref_storage = try alloc.alloc(inference_bridge.ProviderAttachmentRef, parts.len);
    defer alloc.free(ref_storage);
    var payload_count: usize = 0;
    for (parts, wire_parts, 0..) |part, *wire_part, item_index| switch (part) {
        .binary => |binary| {
            payload_storage[payload_count] = .{
                .bytes = inference_bridge.String.init(binary.data),
                .content_type = inference_bridge.String.init(binary.mime_type),
            };
            ref_storage[payload_count] = .{ .attachment_index = payload_count, .item_index = item_index };
            payload_count += 1;
            wire_part.* = embedding_wire.metadataPart(part);
        },
        else => wire_part.* = part,
    };
    return try invokeInferenceProviderWithBinaryControlled(
        [][]f32,
        alloc,
        handle,
        operation,
        embedding_wire.Request(template.ContentPart){
            .model = model,
            .parts = wire_parts,
            .attachment_count = payload_count,
            .task_type = task_type,
            .instruction = instruction,
        },
        deadline_ns,
        payload_storage[0..payload_count],
        ref_storage[0..payload_count],
        cancellation,
    );
}

pub fn inferenceProviderRerankTexts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    query: []const u8,
    documents: []const []const u8,
) anyerror![]f32 {
    return try invokeInferenceProvider([]f32, alloc, handle, .rerank_texts, .{
        .model = model,
        .query = query,
        .documents = documents,
    }, null);
}

pub fn inferenceProviderRerankTextsWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    query: []const u8,
    documents: []const []const u8,
    context: inference.RequestContext,
) anyerror![]f32 {
    try context.check();
    const result = try invokeInferenceProviderControlled([]f32, alloc, handle, .rerank_texts, .{
        .model = model,
        .query = query,
        .documents = documents,
    }, context.deadline_ns, context.cancellation orelse .none);
    errdefer alloc.free(result);
    try context.check();
    return result;
}

pub fn inferenceProviderGenerateText(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    roles: []const []const u8,
    contents: []const []const u8,
    options: inference.GenerationOptions,
) anyerror![]u8 {
    return try invokeInferenceProvider([]u8, alloc, handle, .generate_text, inference.types.GenerateTextRequest{
        .model = model,
        .roles = roles,
        .contents = contents,
        .options = options,
    }, null);
}

pub fn inferenceProviderGenerateTextWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    roles: []const []const u8,
    contents: []const []const u8,
    options: inference.GenerationOptions,
    context: inference.RequestContext,
) anyerror![]u8 {
    return try invokeInferenceProvider([]u8, alloc, handle, .generate_text, inference.types.GenerateTextRequest{
        .model = model,
        .roles = roles,
        .contents = contents,
        .options = options,
    }, context);
}

pub fn inferenceProviderGenerateMessages(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const inference.ChatMessage,
    options: inference.GenerationOptions,
) anyerror![]u8 {
    return try invokeInferenceProvider([]u8, alloc, handle, .generate_messages, inference.types.GenerateMessagesRequest{
        .model = model,
        .messages = messages,
        .options = options,
    }, null);
}

pub fn inferenceProviderGenerateJson(
    ptr: *anyopaque,
    alloc: std.mem.Allocator,
    body: []const u8,
    request: ?inference.RequestContext,
) ![]u8 {
    if (request) |context| try context.check();
    const lifetime: *EmbeddedInferenceProviderLifetime = @ptrCast(@alignCast(ptr));
    var guard = try lifetime.acquire();
    defer guard.deinit();
    var target = LocalInferenceConnectionContext{ .handle = lifetime.handle };
    var abi_alloc = inference_connection_abi.Allocator.fromStd(&alloc);
    var response: inference_connection_abi.InvokeResponse = .{};
    defer response.deinit(&abi_alloc);
    try invokeLocalInferenceConnectionFallible(&.{
        .abi_version = inference_connection_abi.abi_version,
        .target_context = &target,
        .allocator = &abi_alloc,
        .operation = .init("generate"),
        .body = .init(body),
        .deadline_ns = if (request) |context| context.deadline_ns orelse 0 else platform_time.monotonicNs() +| 5 * std.time.ns_per_min,
        .cancellation = .{ .context = &request, .is_cancelled = struct {
            pub fn cancelled(raw: ?*const anyopaque) callconv(.c) u8 {
                const source: *const ?inference.RequestContext = @ptrCast(@alignCast(raw orelse return 1));
                const active = source.* orelse return 0;
                return @intFromBool(if (active.cancellation) |token| token.isCancelled() else false);
            }
        }.cancelled },
        .out_response = &response,
    });
    if (request) |context| try context.check();
    if (!response.valid()) return error.RuntimeBoundaryFailure;
    if (response.status >= 300) return inference.types.localGenerationStatusError(alloc, response.status, response.body.slice());
    return alloc.dupe(u8, response.body.slice());
}

pub fn inferenceProviderGenerateMessagesWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const inference.ChatMessage,
    options: inference.GenerationOptions,
    context: inference.RequestContext,
) anyerror![]u8 {
    return try invokeInferenceProvider([]u8, alloc, handle, .generate_messages, inference.types.GenerateMessagesRequest{
        .model = model,
        .messages = messages,
        .options = options,
    }, context);
}

pub fn inferenceProviderGenerateMessagesWithAttachments(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const inference.ChatMessage,
    attachments: []const inference.work.Attachment,
) anyerror![]u8 {
    return try inferenceProviderGenerateMessagesWithAttachmentsControlled(
        handle,
        alloc,
        model,
        messages,
        attachments,
        null,
    );
}

pub fn inferenceProviderGenerateMessagesWithAttachmentsWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const inference.ChatMessage,
    attachments: []const inference.work.Attachment,
    context: inference.RequestContext,
) anyerror![]u8 {
    try context.check();
    const result = try inferenceProviderGenerateMessagesWithAttachmentsControlled(
        handle,
        alloc,
        model,
        messages,
        attachments,
        context,
    );
    errdefer alloc.free(result);
    try context.check();
    return result;
}

pub fn inferenceProviderGenerateMessagesWithAttachmentsControlled(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    messages: []const inference.ChatMessage,
    attachments: []const inference.work.Attachment,
    request_context: ?inference.RequestContext,
) anyerror![]u8 {
    const payloads = try alloc.alloc(inference_bridge.ProviderBinaryPayload, attachments.len);
    defer alloc.free(payloads);
    const refs = try alloc.alloc(inference_bridge.ProviderAttachmentRef, attachments.len);
    defer alloc.free(refs);
    for (attachments, 0..) |attachment, i| {
        try attachment.validate();
        payloads[i] = .{
            .bytes = inference_bridge.String.init(attachment.bytes),
            .content_type = inference_bridge.String.init(attachment.content_type),
        };
        refs[i] = .{
            .attachment_index = i,
            .item_index = 0,
            .item_id = inference_bridge.OptionalString.init(if (attachment.identity.item_id.len > 0) attachment.identity.item_id else null),
            .source_fingerprint = inference_bridge.OptionalString.init(attachment.identity.source_fingerprint),
            .page_number = attachment.identity.page_number orelse 0,
            .has_page_number = @intFromBool(attachment.identity.page_number != null),
        };
    }
    return try invokeInferenceProviderWithBinaryContext(
        []u8,
        alloc,
        handle,
        .generate_messages_with_attachments,
        .{ .model = model, .messages = messages, .attachment_count = attachments.len },
        request_context,
        payloads,
        refs,
    );
}

pub fn inferenceProviderModelCapabilities(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    task: inference.work.Task,
) anyerror!inference.work.InferenceCapabilities {
    return try invokeInferenceProvider(
        inference.work.InferenceCapabilities,
        alloc,
        handle,
        .model_capabilities,
        .{ .model = model, .task = task },
        null,
    );
}

pub fn inferenceProviderModelCapabilitiesWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    task: inference.work.Task,
    context: inference.RequestContext,
) anyerror!inference.work.InferenceCapabilities {
    try context.check();
    const result = try invokeInferenceProvider(
        inference.work.InferenceCapabilities,
        alloc,
        handle,
        .model_capabilities,
        .{ .model = model, .task = task },
        context,
    );
    try context.check();
    return result;
}

pub fn inferenceProviderChunkInput(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    input: inference_chunker.Input,
    config: chunking_types.Config,
) anyerror![]inference_chunker.Chunk {
    return try inferenceProviderChunkInputControlled(handle, alloc, model, input, config, null, .none);
}

pub fn inferenceProviderChunkInputWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    input: inference_chunker.Input,
    config: chunking_types.Config,
    context: inference.execution_context.RequestContext,
) anyerror![]inference_chunker.Chunk {
    try context.check();
    const result = try inferenceProviderChunkInputControlled(handle, alloc, model, input, config, context.deadline_ns, context.cancellation orelse .none);
    errdefer inference_chunker.types.freeChunks(alloc, result);
    try context.check();
    return result;
}

pub fn inferenceProviderChunkInputControlled(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    input: inference_chunker.Input,
    config: chunking_types.Config,
    deadline_ns: ?u64,
    cancellation: CancellationToken,
) anyerror![]inference_chunker.Chunk {
    return switch (input) {
        .text => try invokeInferenceProviderControlled([]inference_chunker.Chunk, alloc, handle, .chunk_input, .{
            .model = model,
            .input = input,
            .config = config,
            .attachment_count = @as(usize, 0),
        }, deadline_ns, cancellation),
        .binary => |binary| blk: {
            const payloads = [_]inference_bridge.ProviderBinaryPayload{.{
                .bytes = inference_bridge.String.init(binary.data),
                .content_type = inference_bridge.String.init(binary.mime_type),
            }};
            const refs = [_]inference_bridge.ProviderAttachmentRef{.{ .attachment_index = 0, .item_index = 0 }};
            break :blk try invokeInferenceProviderWithBinaryControlled(
                []inference_chunker.Chunk,
                alloc,
                handle,
                .chunk_input,
                .{
                    .model = model,
                    .input = inference_chunker.Input{ .binary = .{ .mime_type = binary.mime_type, .data = &.{} } },
                    .config = config,
                    .attachment_count = @as(usize, 1),
                },
                deadline_ns,
                &payloads,
                &refs,
                cancellation,
            );
        },
    };
}

pub fn inferenceProviderRewriteTexts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    inputs: []const []const u8,
) anyerror![][]const u8 {
    return try invokeInferenceProvider([][]const u8, alloc, handle, .rewrite_texts, .{
        .model = model,
        .inputs = inputs,
    }, null);
}

pub fn inferenceProviderClassifyTexts(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: inference.managed_embedder.ClassificationRequest,
) anyerror![]const []const inference.managed_embedder.ClassificationScore {
    return try invokeInferenceProvider(
        []const []const inference.managed_embedder.ClassificationScore,
        alloc,
        handle,
        .classify_texts,
        .{ .model = model, .request = request },
        null,
    );
}

pub fn inferenceProviderReadImages(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: readers.Request,
) anyerror![]readers.Result {
    return try invokeInferenceProvider([]readers.Result, alloc, handle, .read_images, .{
        .model = model,
        .request = request,
    }, null);
}

pub fn inferenceProviderReadImagesWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: readers.Request,
    context: inference.RequestContext,
) anyerror![]readers.Result {
    return try invokeInferenceProvider([]readers.Result, alloc, handle, .read_images, .{
        .model = model,
        .request = request,
    }, context);
}

pub fn inferenceProviderReadEncodedImages(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: readers.EncodedRequest,
) anyerror![]readers.Result {
    return try inferenceProviderReadEncodedImagesControlled(handle, alloc, model, request, null);
}

pub fn inferenceProviderReadEncodedImagesWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: readers.EncodedRequest,
    context: inference.RequestContext,
) anyerror![]readers.Result {
    try context.check();
    const results = try inferenceProviderReadEncodedImagesControlled(
        handle,
        alloc,
        model,
        request,
        context,
    );
    errdefer {
        for (results) |*result| readers.deinitResult(alloc, result);
        alloc.free(results);
    }
    try context.check();
    return results;
}

pub fn inferenceProviderReadEncodedImagesControlled(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: readers.EncodedRequest,
    request_context: ?inference.RequestContext,
) anyerror![]readers.Result {
    if (request.images.len == 0) return error.ReadBatchTooLarge;
    var encoded = try encodedImageProviderPayloadsAlloc(alloc, request.images);
    defer encoded.deinit(alloc);
    return try invokeInferenceProviderWithBinaryContext(
        []readers.Result,
        alloc,
        handle,
        .read_encoded_images,
        encodedImageProviderMetadata(model, request),
        request_context,
        encoded.payloads,
        encoded.refs,
    );
}

pub fn inferenceProviderReadEncodedImagesReported(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: readers.EncodedRequest,
) anyerror!readers.BatchResult {
    return try inferenceProviderReadEncodedImagesReportedControlled(handle, alloc, model, request, null);
}

pub fn inferenceProviderReadEncodedImagesReportedWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: readers.EncodedRequest,
    context: inference.RequestContext,
) anyerror!readers.BatchResult {
    try context.check();
    var result = try inferenceProviderReadEncodedImagesReportedControlled(
        handle,
        alloc,
        model,
        request,
        context,
    );
    errdefer result.deinit(alloc);
    try context.check();
    return result;
}

pub fn inferenceProviderReadEncodedImagesReportedControlled(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: readers.EncodedRequest,
    request_context: ?inference.RequestContext,
) anyerror!readers.BatchResult {
    if (request.images.len == 0) return error.ReadBatchTooLarge;
    var encoded = try encodedImageProviderPayloadsAlloc(alloc, request.images);
    defer encoded.deinit(alloc);
    return try invokeInferenceProviderWithBinaryContext(
        readers.BatchResult,
        alloc,
        handle,
        .read_encoded_images_reported,
        encodedImageProviderMetadata(model, request),
        request_context,
        encoded.payloads,
        encoded.refs,
    );
}

pub fn encodedImageProviderMetadata(
    model: []const u8,
    request: readers.EncodedRequest,
) inference_bridge.ReadEncodedImagesRequest {
    return .{
        .model = model,
        .image_count = request.images.len,
        .prompt = request.prompt,
        .max_tokens = request.max_tokens,
        .source_fingerprint = request.source_fingerprint,
    };
}

pub const EncodedImageProviderPayloads = struct {
    payloads: []inference_bridge.ProviderBinaryPayload,
    refs: []inference_bridge.ProviderAttachmentRef,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.payloads);
        alloc.free(self.refs);
        self.* = undefined;
    }
};

pub fn encodedImageProviderPayloadsAlloc(
    alloc: std.mem.Allocator,
    images: []const readers.EncodedImage,
) !EncodedImageProviderPayloads {
    const payloads = try alloc.alloc(inference_bridge.ProviderBinaryPayload, images.len);
    errdefer alloc.free(payloads);
    const refs = try alloc.alloc(inference_bridge.ProviderAttachmentRef, images.len);
    for (images, 0..) |image, i| {
        payloads[i] = .{
            .bytes = inference_bridge.String.init(image.bytes),
            .content_type = inference_bridge.String.init(image.mime_type),
        };
        refs[i] = .{
            .attachment_index = i,
            .item_index = i,
            .item_id = inference_bridge.OptionalString.init(if (image.item_id.len > 0) image.item_id else null),
            .source_fingerprint = inference_bridge.OptionalString.init(image.source_fingerprint),
            .page_number = image.page_number orelse 0,
            .has_page_number = @intFromBool(image.page_number != null),
        };
    }
    return .{ .payloads = payloads, .refs = refs };
}

pub fn inferenceProviderReadRasterImagesReported(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: readers.RasterRequest,
) anyerror!readers.BatchResult {
    return inferenceProviderReadRasterImagesReportedControlled(
        handle,
        alloc,
        model,
        request,
        null,
    );
}

pub fn inferenceProviderReadRasterImagesReportedWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: readers.RasterRequest,
    context: inference.RequestContext,
) anyerror!readers.BatchResult {
    try context.check();
    var result = try inferenceProviderReadRasterImagesReportedControlled(
        handle,
        alloc,
        model,
        request,
        context,
    );
    errdefer result.deinit(alloc);
    try context.check();
    return result;
}

pub fn inferenceProviderReadRasterImagesReportedControlled(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: readers.RasterRequest,
    request_context: ?inference.RequestContext,
) !readers.BatchResult {
    try readers.validateRasterRequest(request);
    var borrowed = try rasterProviderPayloadsAlloc(alloc, request.images);
    defer borrowed.deinit(alloc);
    return invokeInferenceProviderWithBinaryContext(
        readers.BatchResult,
        alloc,
        handle,
        .read_raster_images_reported,
        inference_bridge.ReadRasterImagesRequest{
            .model = model,
            .raster_count = request.images.len,
            .rasters = borrowed.metadata,
            .prompt = request.prompt,
            .max_tokens = request.max_tokens,
            .source_fingerprint = request.source_fingerprint,
        },
        request_context,
        borrowed.payloads,
        borrowed.refs,
    );
}

pub fn inferenceProviderEmbedDenseRasters(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    rasters: []const readers.RasterImage,
    context: inference.managed_embedder.EmbeddingRequestContext,
) anyerror![][]f32 {
    try context.check();
    if (rasters.len == 0) return try alloc.alloc([]f32, 0);
    var borrowed = try rasterProviderPayloadsAlloc(alloc, rasters);
    defer borrowed.deinit(alloc);
    const vectors = try invokeInferenceProviderWithBinaryControlled(
        [][]f32,
        alloc,
        handle,
        .embed_dense_rasters,
        inference_bridge.ReadRasterImagesRequest{
            .model = model,
            .raster_count = rasters.len,
            .rasters = borrowed.metadata,
        },
        context.request.deadline_ns,
        borrowed.payloads,
        borrowed.refs,
        context.request.cancellation orelse .none,
    );
    errdefer {
        for (vectors) |vector| alloc.free(vector);
        alloc.free(vectors);
    }
    try context.check();
    return vectors;
}

pub const RasterProviderPayloads = struct {
    metadata: []inference_bridge.RasterImageMetadata,
    payloads: []inference_bridge.ProviderBinaryPayload,
    refs: []inference_bridge.ProviderAttachmentRef,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.metadata);
        alloc.free(self.payloads);
        alloc.free(self.refs);
        self.* = undefined;
    }
};

pub fn rasterProviderPayloadsAlloc(
    alloc: std.mem.Allocator,
    images: []const readers.RasterImage,
) !RasterProviderPayloads {
    const metadata = try alloc.alloc(inference_bridge.RasterImageMetadata, images.len);
    errdefer alloc.free(metadata);
    const payloads = try alloc.alloc(inference_bridge.ProviderBinaryPayload, images.len);
    errdefer alloc.free(payloads);
    const refs = try alloc.alloc(inference_bridge.ProviderAttachmentRef, images.len);
    for (images, 0..) |image, i| {
        try image.validate();
        metadata[i] = .{
            .width = image.width,
            .height = image.height,
            .stride_bytes = image.stride_bytes,
            .format = image.format,
        };
        payloads[i] = .{
            .bytes = inference_bridge.String.init(image.bytes),
            .content_type = inference_bridge.String.init(image.mime_type),
        };
        refs[i] = .{
            .attachment_index = i,
            .item_index = i,
            .item_id = inference_bridge.OptionalString.init(if (image.item_id.len > 0) image.item_id else null),
            .source_fingerprint = inference_bridge.OptionalString.init(image.source_fingerprint),
            .page_number = image.page_number orelse 0,
            .has_page_number = @intFromBool(image.page_number != null),
        };
    }
    return .{ .metadata = metadata, .payloads = payloads, .refs = refs };
}

pub fn inferenceProviderTranscribeAudio(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: transcribing.Request,
) anyerror!transcribing.Response {
    return try invokeInferenceProvider(transcribing.Response, alloc, handle, .transcribe_audio, .{
        .model = model,
        .request = request,
    }, null);
}

pub fn inferenceProviderTranscribeAudioWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: transcribing.Request,
    context: inference.RequestContext,
) anyerror!transcribing.Response {
    return try invokeInferenceProvider(transcribing.Response, alloc, handle, .transcribe_audio, .{
        .model = model,
        .request = request,
    }, context);
}

pub fn inferenceProviderExtract(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: extracting.Request,
) anyerror!extracting.Response {
    return try inferenceProviderExtractControlled(handle, alloc, model, request, null);
}

pub fn inferenceProviderExtractWithContext(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: extracting.Request,
    context: inference.RequestContext,
) anyerror!extracting.Response {
    try context.check();
    var result = try inferenceProviderExtractControlled(
        handle,
        alloc,
        model,
        request,
        context,
    );
    errdefer result.deinit();
    try context.check();
    return result;
}

pub fn inferenceProviderExtractControlled(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
    model: []const u8,
    request: extracting.Request,
    request_context: ?inference.RequestContext,
) anyerror!extracting.Response {
    const payloads = try alloc.alloc(inference_bridge.ProviderBinaryPayload, request.attachments.len);
    defer alloc.free(payloads);
    const refs = try alloc.alloc(inference_bridge.ProviderAttachmentRef, request.attachments.len);
    defer alloc.free(refs);
    for (request.attachments, 0..) |attachment, i| {
        if (attachment.input_index >= request.inputs.len or attachment.mime_type.len == 0)
            return error.InvalidExtractionAttachment;
        payloads[i] = .{
            .bytes = inference_bridge.String.init(attachment.bytes),
            .content_type = inference_bridge.String.init(attachment.mime_type),
        };
        refs[i] = .{ .attachment_index = i, .item_index = attachment.input_index };
    }
    const wire_request = extracting.Request{
        .inputs = request.inputs,
        .schema_json = request.schema_json,
        .options_json = request.options_json,
    };
    const json = try invokeInferenceProviderWithBinaryContext([]u8, alloc, handle, .extract, .{
        .model = model,
        .request = wire_request,
        .attachment_count = request.attachments.len,
    }, request_context, payloads, refs);
    return .{ .allocator = alloc, .json = json };
}

pub fn inferenceProviderListModelsJson(
    handle: *anyopaque,
    alloc: std.mem.Allocator,
) anyerror![]u8 {
    return try invokeInferenceProvider([]u8, alloc, handle, .list_models_json, .{}, null);
}
