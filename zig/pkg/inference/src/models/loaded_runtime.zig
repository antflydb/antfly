// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Loaded model generation: component sessions, resident caches, identity and
//! task construction. Serving supplies retirement and admission services through
//! Host, retaining registry/handle/health ownership outside this module.
const session_loader = @import("session_loader.zig");

pub const Host = struct {
    context: *anyopaque,
    session_loading: session_loader.Host,
    retire: *const fn (*anyopaque, *LoadedModel) void,
    reserve: *const fn (*anyopaque, @import("../runtime/tier/memory.zig").BackendClass, @import("../runtime/tier/memory.zig").Limits, @import("../runtime/tier/memory.zig").AdmissionAmounts) anyerror!@import("../runtime/tier/memory.zig").AdmissionLease,
};

const ClassificationConfig = @import("../pipelines/classification.zig").ClassificationConfig;
const ClassificationPipeline = @import("../pipelines/classification.zig").ClassificationPipeline;
const GlinerPipeline = @import("../pipelines/gliner.zig").GlinerPipeline;
const NerPipeline = @import("../pipelines/ner.zig").NerPipeline;
const RerankingPipeline = @import("../pipelines/reranking.zig").RerankingPipeline;
const ScoringMode = @import("../pipelines/reranking.zig").ScoringMode;
const backends = @import("../backends/backends.zig");
const builtin = @import("builtin");
const cleanup_model_mod = @import("../finetune/entity_cleanup_model.zig");
const embedding_mod = @import("../pipelines/embedding.zig");
const execution_control_mod = @import("../execution_control.zig");
const gemma4_projector = @import("../architectures/gemma4_projector.zig");
const generation = @import("../pipelines/generation.zig");
const graph_mod = @import("../graph/root.zig");
const hf_tokenizer = @import("inference_hf_tokenizer");
const kernel_jit_profile_output = @import("../kernel_jit_profile_output.zig");
const managed_receipt = @import("../registry/managed_receipt.zig");
const manifest_mod = @import("../models/manifest.zig");
const model_caps = @import("../models/capabilities.zig");
const native_kv_identity = @import("../runtime/kv/attention_match_identity.zig");
const platform = @import("antfly_platform");
const qwen3vl_reranker = @import("../architectures/qwen3vl_reranker.zig");
const runtime = @import("../runtime/root.zig");
const sentencepiece = @import("inference_tokenizer").sentencepiece;
const session_factory = @import("../architectures/session_factory.zig");
const std = @import("std");
const tokenizer_mod = @import("inference_tokenizer");
const vision_config = @import("../readers/vision_config.zig");
const whisper_prompt = @import("../pipelines/whisper_prompt.zig");

pub const InferenceExecutionControl = execution_control_mod.InferenceExecutionControl;

pub const EmbeddingPipeline = embedding_mod.EmbeddingPipeline;

pub const ChatTemplate = generation.ChatTemplate;

pub const ComponentPlanKey = [std.crypto.hash.sha2.Sha256.digest_length]u8;

pub fn updateComponentPlanKeySlice(
    hash: *std.crypto.hash.sha2.Sha256,
    value: []const u8,
) void {
    const len: u64 = @intCast(value.len);
    hash.update(std.mem.asBytes(&len));
    hash.update(value);
}

pub fn componentDependencySignature(
    io: std.Io,
    dependencies: []const []const u8,
) !ComponentPlanKey {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (dependencies) |path| {
        updateComponentPlanKeySlice(&hash, path);
        const stat = std.Io.Dir.cwd().statFile(io, path, .{}) catch {
            hash.update("missing");
            continue;
        };
        hash.update(std.mem.asBytes(&stat.inode));
        hash.update(std.mem.asBytes(&stat.size));
        const mtime_ns = stat.mtime.toNanoseconds();
        const ctime_ns = stat.ctime.toNanoseconds();
        // std.Io.Timestamp uses i96, whose ABI storage contains padding on
        // common targets. Widen before hashing so fingerprints never include
        // indeterminate padding bytes.
        const canonical_mtime_ns: i128 = mtime_ns;
        const canonical_ctime_ns: i128 = ctime_ns;
        hash.update(std.mem.asBytes(&canonical_mtime_ns));
        hash.update(std.mem.asBytes(&canonical_ctime_ns));
        const final_stat = std.Io.Dir.cwd().statFile(io, path, .{}) catch {
            hash.update("changed-during-read");
            continue;
        };
        if (final_stat.inode != stat.inode or
            final_stat.size != stat.size or
            final_stat.mtime.toNanoseconds() != mtime_ns or
            final_stat.ctime.toNanoseconds() != ctime_ns)
            hash.update("changed-during-read");
    }
    var digest: ComponentPlanKey = undefined;
    hash.final(&digest);
    return digest;
}

pub fn spinLock(m: *std.atomic.Mutex) void {
    platform.sync.lockYielding(m);
}

pub const EmbeddingAssetGate = struct {
    reader_gate: std.atomic.Mutex = .unlocked,
    reader_mutex: std.atomic.Mutex = .unlocked,
    resource_mutex: std.atomic.Mutex = .unlocked,
    reader_count: usize = 0,

    pub fn lockShared(self: *EmbeddingAssetGate) void {
        spinLock(&self.reader_gate);
        defer self.reader_gate.unlock();
        spinLock(&self.reader_mutex);
        defer self.reader_mutex.unlock();

        self.reader_count += 1;
        if (self.reader_count == 1) spinLock(&self.resource_mutex);
    }

    pub fn tryLockShared(self: *EmbeddingAssetGate) bool {
        if (!self.reader_gate.tryLock()) return false;
        defer self.reader_gate.unlock();
        if (!self.reader_mutex.tryLock()) return false;
        defer self.reader_mutex.unlock();

        if (self.reader_count == 0 and !self.resource_mutex.tryLock()) return false;
        self.reader_count += 1;
        return true;
    }

    pub fn unlockShared(self: *EmbeddingAssetGate) void {
        spinLock(&self.reader_mutex);
        defer self.reader_mutex.unlock();

        std.debug.assert(self.reader_count > 0);
        self.reader_count -= 1;
        if (self.reader_count == 0) self.resource_mutex.unlock();
    }

    pub fn lockExclusive(self: *EmbeddingAssetGate) void {
        spinLock(&self.reader_gate);
        spinLock(&self.resource_mutex);
    }

    pub fn tryLockExclusive(self: *EmbeddingAssetGate) bool {
        if (!self.reader_gate.tryLock()) return false;
        if (!self.resource_mutex.tryLock()) {
            self.reader_gate.unlock();
            return false;
        }
        return true;
    }

    pub fn unlockExclusive(self: *EmbeddingAssetGate) void {
        self.resource_mutex.unlock();
        self.reader_gate.unlock();
    }

    /// Convert an exclusive owner into the first shared reader without opening
    /// a gap in which another writer could mutate optional-session slots.
    pub fn downgradeExclusiveToShared(self: *EmbeddingAssetGate) void {
        spinLock(&self.reader_mutex);
        std.debug.assert(self.reader_count == 0);
        self.reader_count = 1;
        self.reader_mutex.unlock();

        // Keep resource_mutex held on behalf of the new reader cohort. New
        // readers may now join, while queued writers remain excluded until the
        // final reader releases the resource.
        self.reader_gate.unlock();
    }
};

pub const EmbeddingAssetLease = struct {
    gate: *EmbeddingAssetGate,
    access: enum { shared, exclusive },
    held: bool = true,

    pub fn release(self: *EmbeddingAssetLease) void {
        if (!self.held) return;
        switch (self.access) {
            .shared => self.gate.unlockShared(),
            .exclusive => self.gate.unlockExclusive(),
        }
        self.held = false;
    }

    pub fn downgradeExclusiveToShared(self: *EmbeddingAssetLease) void {
        std.debug.assert(self.held);
        std.debug.assert(self.access == .exclusive);
        self.gate.downgradeExclusiveToShared();
        self.access = .shared;
    }
};

pub const DeclaredOptionalSessionKind = enum {
    vision,
    audio,
    text_projection,
    visual_projection,
    audio_projection,
};

pub const DeclaredOptionalSession = struct {
    kind: DeclaredOptionalSessionKind,
    path: ?[]const u8,
};

pub const declared_optional_session_count = @typeInfo(DeclaredOptionalSessionKind).@"enum".field_names.len;

pub fn declaredOptionalSessions(manifest: *const manifest_mod.ModelManifest) [declared_optional_session_count]DeclaredOptionalSession {
    return .{
        .{ .kind = .vision, .path = manifest.visual_model_path },
        .{ .kind = .audio, .path = manifest.audio_model_path },
        .{ .kind = .text_projection, .path = manifest.text_projection_path },
        .{ .kind = .visual_projection, .path = manifest.visual_projection_path },
        .{ .kind = .audio_projection, .path = manifest.audio_projection_path },
    };
}

pub fn declaredOptionalSessionsComplete(
    manifest: *const manifest_mod.ModelManifest,
    loaded: [declared_optional_session_count]bool,
) bool {
    for (declaredOptionalSessions(manifest), loaded) |declared, is_loaded| {
        if (declared.path != null and !is_loaded) return false;
    }
    return true;
}

pub fn bindOptionalSessionProfile(
    session_manager: *backends.SessionManager,
    kind: DeclaredOptionalSessionKind,
    profile_bundle: ?graph_mod.kernel_jit.QualifiedProfileBundle,
) !void {
    if (profile_bundle) |bundle| {
        const component_path = switch (kind) {
            .vision => bundle.vision,
            .audio => bundle.audio,
            .text_projection => bundle.text_projection,
            .visual_projection => bundle.visual_projection,
            .audio_projection => bundle.audio_projection,
        };
        if (component_path) |path| {
            session_manager.kernel_jit.qualified_profile_path = path;
        } else {
            if (session_manager.kernel_jit.mode.failClosed()) {
                return error.MissingKernelJitProfileBundleComponent;
            }
            std.log.warn(
                "kernel JIT profile bundle has no {s} member; using bundled kernels",
                .{@tagName(kind)},
            );
            session_manager.kernel_jit.qualified_profile_path = null;
            session_manager.kernel_jit.mode = .off;
        }
    } else if (session_manager.kernel_jit.qualified_profile_path != null) {
        if (session_manager.kernel_jit.mode.failClosed()) {
            return error.KernelJitQualifiedProfileOptionalSessionUnsupported;
        }
        // One exact profile is bound to one model fingerprint. Never apply
        // the primary model's profile to an optional submodel.
        session_manager.kernel_jit.qualified_profile_path = null;
        session_manager.kernel_jit.mode = .off;
    }
}

pub const ArtifactDigestCache = struct {
    const Entry = struct { signature: [32]u8, digest: [32]u8 };
    pub var mutex: std.atomic.Mutex = .unlocked;
    pub var entries: [256]?Entry = @splat(null);
    pub var next: usize = 0;
    pub var forbid_hashing_for_test: bool = false;

    pub fn digest(io: std.Io, artifact: managed_receipt.ValidatedArtifact) ![32]u8 {
        return digestWithControl(io, artifact, .{});
    }

    pub fn digestWithControl(io: std.Io, artifact: managed_receipt.ValidatedArtifact, control: InferenceExecutionControl) ![32]u8 {
        try control.check();
        const signature = try componentDependencySignature(io, &.{artifact.canonical_path});
        spinLock(&mutex);
        for (entries) |item| if (item) |entry| {
            if (std.mem.eql(u8, &entry.signature, &signature)) {
                mutex.unlock();
                return entry.digest;
            }
        };
        mutex.unlock();
        const memo_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.antfly-sha256-cache", .{artifact.canonical_path});
        defer std.heap.page_allocator.free(memo_path);
        const memo_bytes = std.Io.Dir.cwd().readFileAlloc(io, memo_path, std.heap.page_allocator, .limited(1024)) catch null;
        defer if (memo_bytes) |bytes| std.heap.page_allocator.free(bytes);
        if (memo_bytes) |bytes| {
            if (std.json.parseFromSlice(Entry, std.heap.page_allocator, bytes, .{})) |memo| {
                defer memo.deinit();
                if (std.mem.eql(u8, &signature, &memo.value.signature)) {
                    spinLock(&mutex);
                    defer mutex.unlock();
                    entries[next] = memo.value;
                    next = (next + 1) % entries.len;
                    return memo.value.digest;
                }
            } else |_| {}
        }
        if (builtin.is_test and forbid_hashing_for_test) return error.UnexpectedModelRehash;
        var file = try std.Io.Dir.cwd().openFile(io, artifact.canonical_path, .{});
        defer file.close(io);
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        var buffer: [64 * 1024]u8 = undefined;
        while (true) {
            try control.check();
            const count = file.readStreaming(io, &.{&buffer}) catch |err| switch (err) {
                error.EndOfStream => 0,
                else => return err,
            };
            if (count == 0) break;
            hasher.update(buffer[0..count]);
        }
        var result: [32]u8 = undefined;
        hasher.final(&result);
        if (!std.mem.eql(u8, &signature, &try componentDependencySignature(io, &.{artifact.canonical_path}))) return error.ModelArtifactsChanging;
        const memo = std.json.Stringify.valueAlloc(std.heap.page_allocator, Entry{ .signature = signature, .digest = result }, .{}) catch null;
        defer if (memo) |bytes| std.heap.page_allocator.free(bytes);
        if (memo) |bytes| std.Io.Dir.cwd().writeFile(io, .{ .sub_path = memo_path, .data = bytes }) catch {};
        spinLock(&mutex);
        defer mutex.unlock();
        entries[next] = .{ .signature = signature, .digest = result };
        next = (next + 1) % entries.len;
        return result;
    }
};

pub const NativeKvFiles = struct {
    arena: std.heap.ArenaAllocator,
    entries: [8]native_kv_identity.FileArtifact,

    pub fn init(a: std.mem.Allocator, directory: []const u8, manifest: manifest_mod.ModelManifest) !NativeKvFiles {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const alloc = arena.allocator();
        if (!manifest.usesGgufWeights()) return error.UnsupportedKvIdentity;
        const weights = manifest.gguf_path orelse return error.UnsupportedKvIdentity;
        const entries = [8]native_kv_identity.FileArtifact{
            .{ .role = "added_tokens", .path = try std.fs.path.join(alloc, &.{ directory, "added_tokens.json" }), .optional = true, .max_bytes = 64 * 1024 * 1024 },
            .{ .role = "config", .path = manifest.config_path orelse try std.fs.path.join(alloc, &.{ directory, "config.json" }), .optional = true, .max_bytes = 64 * 1024 * 1024 },
            .{ .role = "special_tokens_map", .path = manifest.special_tokens_map_path orelse try std.fs.path.join(alloc, &.{ directory, "special_tokens_map.json" }), .optional = true, .max_bytes = 64 * 1024 * 1024 },
            .{ .role = "tokenizer_config", .path = manifest.tokenizer_config_path orelse try std.fs.path.join(alloc, &.{ directory, "tokenizer_config.json" }), .optional = true, .max_bytes = 64 * 1024 * 1024 },
            .{ .role = "tokenizer_json", .path = manifest.tokenizer_json_path orelse try std.fs.path.join(alloc, &.{ directory, "tokenizer.json" }), .optional = true, .max_bytes = 64 * 1024 * 1024 },
            .{ .role = "tokenizer_model", .path = try std.fs.path.join(alloc, &.{ directory, "tokenizer.model" }), .optional = true, .max_bytes = 64 * 1024 * 1024 },
            .{ .role = "vocab", .path = manifest.vocab_txt_path orelse try std.fs.path.join(alloc, &.{ directory, "vocab.txt" }), .optional = true, .max_bytes = 64 * 1024 * 1024 },
            .{ .role = "weights", .path = weights, .max_bytes = 16 * 1024 * 1024 * 1024 },
        };
        return .{ .arena = arena, .entries = entries };
    }
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const RuntimeId = u128;

pub const RuntimeResidencyMode = enum {
    backend_managed,
    streamed,
    resident,
};

pub const ChunkPlanningSnapshot = struct {
    digest: [32]u8,
    signature: [32]u8,
    // Evidence for the embedded tokenizer, separate from the stable public
    // planning identity so unchanged models keep their existing chunk keys.
    embedded_tokenizer_digest: ?[32]u8 = null,
};

pub fn chunkPlanningPaths(manifest: manifest_mod.ModelManifest, out: *[9][]const u8) []const []const u8 {
    var count: usize = 0;
    inline for (.{ "config_path", "tokenizer_json_path", "vocab_txt_path", "tokenizer_config_path", "special_tokens_map_path", "preprocessor_config_path", "processor_config_path" }) |name| if (@field(manifest, name)) |path| {
        out[count] = path;
        count += 1;
    };
    if (manifest.tokenizer_json_path == null and manifest.vocab_txt_path == null) if (manifest.gguf_path) |path| {
        out[count] = path;
        count += 1;
    };
    return out[0..count];
}

pub fn chunkPlanningSnapshotWithControl(a: std.mem.Allocator, io: std.Io, manifest: manifest_mod.ModelManifest, fallback: [32]u8, control: InferenceExecutionControl) !ChunkPlanningSnapshot {
    try control.check();
    var storage: [9][]const u8 = undefined;
    const paths = chunkPlanningPaths(manifest, &storage);
    const signature = try componentDependencySignature(io, paths);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-model-chunk-planning-v1");
    const recipe = try std.json.Stringify.valueAlloc(a, .{ .style = manifest.embedding_style, .tokenizer = manifest.tokenizer_type, .context = manifest.maxTextSequenceLength() }, .{});
    defer a.free(recipe);
    hash.update(recipe);
    if (manifest.tokenizer_json_path == null and manifest.vocab_txt_path == null) hash.update(&fallback);
    var embedded_tokenizer_digest: ?[32]u8 = null;
    for (paths) |path| {
        try control.check();
        // Reuse the content verification performed during managed model load.
        // After a metadata change, prove GGUF byte identity rather than trusting
        // the loaded model's old fallback revision. Warm planning only stats.
        if (manifest.gguf_path) |gguf| if (std.mem.eql(u8, path, gguf)) {
            const canonical = try managed_receipt.resolveRegularFilePath(a, io, path);
            defer a.free(canonical);
            const stat = try std.Io.Dir.cwd().statFile(io, canonical, .{});
            embedded_tokenizer_digest = try ArtifactDigestCache.digestWithControl(io, .{
                .path = std.fs.path.basename(path),
                .canonical_path = canonical,
                .size = stat.size,
                .sha256 = null,
            }, control);
            continue;
        };
        hash.update(std.fs.path.basename(path));
        hash.update(&.{0});
        var file = try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);
        var file_hash = std.crypto.hash.sha2.Sha256.init(.{});
        var buffer: [64 * 1024]u8 = undefined;
        while (true) {
            try control.check();
            const count = file.readStreaming(io, &.{&buffer}) catch |err| switch (err) {
                error.EndOfStream => 0,
                else => return err,
            };
            if (count == 0) break;
            file_hash.update(buffer[0..count]);
        }
        hash.update(&file_hash.finalResult());
    }
    if (!std.mem.eql(u8, &signature, &try componentDependencySignature(io, paths))) return error.ModelArtifactsChanging;
    return .{ .digest = hash.finalResult(), .signature = signature, .embedded_tokenizer_digest = embedded_tokenizer_digest };
}

const LoadedModel = Runtime;

pub const Runtime = struct {
    runtime_id: RuntimeId = 0,
    residency_mode: RuntimeResidencyMode = .backend_managed,
    chunk_planning: ?ChunkPlanningSnapshot = null,
    resolution_revision: ?[32]u8 = null,
    /// Portable checkpoint identity is bound to this loaded asset generation.
    /// Unsupported bundles keep this optional capability disabled.
    native_kv_asset_generation: ?[32]u8 = null,
    native_kv_artifact_digest: ?[32]u8 = null,
    native_kv_tokenizer_contract: ?native_kv_identity.TokenizerContract = null,
    native_kv_identity_mutex: std.atomic.Mutex = .unlocked,
    embedding_identity: ?[64]u8 = null,
    embedding_validation: ?@import("../models/embedding_gemma2_identity.zig").SnapshotValidation = null,
    embedding_identity_mutex: std.atomic.Mutex = .unlocked,
    chunk_planning_lane: execution_control_mod.ExecutionLane = .{},
    embedding_execution_lane: execution_control_mod.ExecutionLane = .{},
    /// Queue OCR cohorts before taking their memory leases. Waiting groups
    /// must not reserve decoder state that only one GPU cohort can use.
    native_reader_execution_lane: execution_control_mod.ExecutionLane = .{},
    native_reader_stream: @import("../execution/reader_pool.zig").Pool = .{},
    manifest: manifest_mod.ModelManifest,
    hf_tok: ?*hf_tokenizer.HfTokenizer,
    sp_tok: ?*sentencepiece.Processor,
    session: backends.Session,
    session_manager: *backends.SessionManager,
    host: Host,
    model_dir: []const u8,
    allocator: std.mem.Allocator,

    /// Borrowed from the serving BackendRuntime and valid for the model's
    /// loaded lifetime. Offline model owners leave this null.
    executor_io: ?std.Io = null,
    /// Immutable Florence reader sidecars, parsed before this generation is
    /// published so request microbatches perform no filesystem discovery.
    florence_reader_config: ?vision_config.RuntimeConfig = null,
    chat_tmpl: ?*ChatTemplate = null,
    whisper_prompt_cache: ?whisper_prompt.PromptCache = null,
    /// The model shipped a chat template that we could not parse, so chat requests fall
    /// back to raw prompting. Distinct from `chat_tmpl == null` with no template at all.
    chat_template_failed: bool = false,
    shared_moe_cache: ?*runtime.moe.shared.SharedExpertCache = null,
    shared_prefetch: ?*runtime.tier.shared.SharedPrefetchState = null,
    prompt_prefix_cache: runtime.kv.prompt_cache.PromptPrefixCache,
    native_generate_coordinator: ?*runtime.scheduler.native_generate.NativeGenerateCoordinator = null,
    native_generation_graph_cache: graph_mod.cache.GraphCache,
    // ponytail: model-wide safety lock; replace with per-request backend state only when continuous batching is proven safe.
    native_generate_lock: std.atomic.Mutex = .unlocked,
    /// The GGUF projector kept open for Gemma media prompts, opened on the
    /// first request that needs it and closed with the model.
    projector_store: ?*gemma4_projector.ProjectorStore = null,
    projector_store_mutex: std.atomic.Mutex = .unlocked,
    // Multimodal sessions (CLIP/CLAP/CLIPCLAP). The gate protects session
    // lifetime during execution; the mutex protects short slot mutations.
    embedding_asset_gate: EmbeddingAssetGate = .{},
    // Stateful GPU backends retain one model-level execution lane until
    // per-request resident frames and caches are independently owned.
    embedding_session_lock: std.atomic.Mutex = .unlocked,
    /// Stateful GPU pipelines share mutable command-frame and resident-slot
    /// state within a loaded model. Keep one model-local lane while allowing
    /// independent models to overlap.
    target_inference_run_lock: std.atomic.Mutex = .unlocked,
    embedding_cohort_scheduler: @import("../runtime/embedding_scheduler.zig").Scheduler = .{},
    /// CUDA boundary requests execute serially on this session. Queue before
    /// reserving their full device scratch ceiling, without holding a provider
    /// lock during admission/eviction. Every waiter retains a loaded-model handle.
    gliner_boundary_admission_lock: std.atomic.Mutex = .unlocked,
    vision_session: ?backends.Session = null,
    audio_session: ?backends.Session = null,
    text_projection: ?backends.Session = null,
    visual_projection: ?backends.Session = null,
    audio_projection: ?backends.Session = null,
    /// Owns the strings backing the qualified JIT profile paths retained by
    /// the primary and lazily loaded component sessions.
    kernel_jit_profile_bundle: ?kernel_jit_profile_output.LoadedProfileBundle = null,
    vision_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    audio_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    text_projection_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    visual_projection_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    audio_projection_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    embedding_prototype_cache: ?*@import("../extractors/embedding_prototypes.zig").Cache = null,
    embedding_prototype_cache_lease: ?runtime.tier.memory.AdmissionLease = null,
    embedding_prototype_cache_mutex: std.atomic.Mutex = .unlocked,
    /// Last complete optional-session JIT snapshot. Metrics use it when a
    /// cold sidecar load owns embedding_session_lock, keeping scrapes bounded
    /// without racing publication of the optional session handles.
    optional_metal_jit_q4_0_hits: std.atomic.Value(u64) = .init(0),
    optional_metal_jit_q4_k_hits: std.atomic.Value(u64) = .init(0),
    /// Accounts for the tokenizer's resident vocabulary, maps, tries, and
    /// parser-owned state independently of backend weight residency.
    tokenizer_resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    resident_projection_stats: embedding_mod.AtomicResidentProjectionStats = .{},
    cleanup_head: ?*cleanup_model_mod.CleanupHead = null,
    cleanup_head_loaded: bool = false,
    resource_lease: ?runtime.tier.memory.AdmissionLease = null,
    /// Protected by the serving registry lock. A model can only be evicted while
    /// it has no active handles and is not pinned by preload configuration.
    active_handles: usize = 0,
    last_used_ns: u64 = 0,
    pinned: bool = false,
    /// Removed from the lookup maps after a recoverable runtime-integrity
    /// fault. Existing handles may finish unwinding; the last one destroys the
    /// retired model while new requests load a fresh session.
    retired: bool = false,

    /// Caller owns a loaded-model handle for this entire operation. Identity is
    /// cached only in this loaded generation, never in a user-writable memo.
    /// The pre-load generation marker ties the content hash to admitted weights
    /// and tokenizer state; edited assets cannot retroactively label old state.
    pub fn nativeKvIdentity(self: *LoadedModel, dtype: runtime.kv.pool.KvDType, control: ?InferenceExecutionControl) !runtime.kv.attention_match_checkpoint.Identity {
        if ((dtype != .f16 and dtype != .f32) or self.chat_template_failed or !self.manifest.usesGgufWeights()) return error.UnsupportedKvIdentity;
        const config = session_factory.getGptConfig(self.session) orelse return error.UnsupportedKvIdentity;
        if (config.family != .gemma or !config.usesGemma4Channels() or config.gemma4_mtp_assistant) return error.UnsupportedKvIdentity;
        const expected_generation = self.native_kv_asset_generation orelse return error.UnsupportedKvIdentity;
        const tokenizer_contract = self.native_kv_tokenizer_contract orelse return error.UnsupportedKvIdentity;
        if (control) |active| try active.lock(&self.native_kv_identity_mutex) else platform.sync.lockYielding(&self.native_kv_identity_mutex);
        defer self.native_kv_identity_mutex.unlock();
        if (control) |active| try active.check();
        const identity_started = std.Io.Timestamp.now(self.executor_io orelse std.Options.debug_io, .awake);
        const identity_cache_hit = self.native_kv_artifact_digest != null;
        if (self.native_kv_artifact_digest == null) {
            var files = try NativeKvFiles.init(self.allocator, self.model_dir, self.manifest);
            defer files.deinit();
            const Callback = struct {
                active: ?InferenceExecutionControl,
                fn check(raw: ?*anyopaque) !void {
                    const self_callback: *@This() = @ptrCast(@alignCast(raw.?));
                    if (self_callback.active) |active| try active.check();
                }
            };
            var callback: Callback = .{ .active = control };
            const digest = try native_kv_identity.hashFiles(std.Io.Dir.cwd(), self.executor_io orelse std.Options.debug_io, &files.entries, expected_generation, "antfly.gemma4.loaded-bundle-v1", .{ .context = &callback, .check_fn = Callback.check });
            if (control) |active| try active.check();
            self.native_kv_artifact_digest = digest;
        }
        const artifacts_digest = self.native_kv_artifact_digest.?;
        const identity: runtime.kv.attention_match_checkpoint.Identity = .{
            // Conservative full-bundle content binding includes weight and
            // tokenizer sidecars; filenames and inode metadata are excluded.
            .weights = artifacts_digest,
            .tokenizer = native_kv_identity.tokenizer(artifacts_digest, tokenizer_contract.route, tokenizer_contract.preserve_inline_specials),
            .template = native_kv_identity.template(if (self.chat_tmpl != null) self.manifest.chat_template orelse return error.UnsupportedKvIdentity else "", self.manifest.bos_token, self.manifest.eos_token, self.manifest.unk_token, self.manifest.pad_token),
            .layout = try native_kv_identity.layout(config, @tagName(dtype)),
        };
        std.log.info("native_kv_identity cache_hit={d} elapsed_ns={d} weights_sha256={s} tokenizer_sha256={s} template_sha256={s} layout_sha256={s}", .{
            @intFromBool(identity_cache_hit),              identity_started.untilNow(self.executor_io orelse std.Options.debug_io, .awake).nanoseconds,
            std.fmt.bytesToHex(identity.weights, .lower),  std.fmt.bytesToHex(identity.tokenizer, .lower),
            std.fmt.bytesToHex(identity.template, .lower), std.fmt.bytesToHex(identity.layout, .lower),
        });
        return identity;
    }

    pub fn chunkPlanningIdentity(self: *LoadedModel) ![32]u8 {
        return self.chunkPlanningIdentityWithControl(.{});
    }

    pub fn chunkPlanningIdentityWithControl(self: *LoadedModel, control: InferenceExecutionControl) ![32]u8 {
        var active = control;
        const io = self.executor_io orelse std.Options.debug_io;
        if (active.io == null) active.io = io;
        try active.lock(&self.embedding_identity_mutex);
        const previous = self.chunk_planning;
        self.embedding_identity_mutex.unlock();
        var snapshot = previous orelse return self.resolution_revision orelse error.InvalidEmbeddingIdentity;
        var paths: [9][]const u8 = undefined;
        const signature = try componentDependencySignature(io, chunkPlanningPaths(self.manifest, &paths));
        if (std.mem.eql(u8, &signature, &snapshot.signature)) return snapshot.digest;
        const lease = try self.chunk_planning_lane.acquire(active);
        defer lease.release();
        // A preceding validator may already have refreshed this generation.
        try active.lock(&self.embedding_identity_mutex);
        snapshot = self.chunk_planning.?;
        self.embedding_identity_mutex.unlock();
        if (std.mem.eql(u8, &try componentDependencySignature(io, chunkPlanningPaths(self.manifest, &paths)), &snapshot.signature)) return snapshot.digest;
        const current = chunkPlanningSnapshotWithControl(self.allocator, io, self.manifest, self.resolution_revision orelse snapshot.digest, active) catch |err| switch (err) {
            error.FileNotFound, error.NotDir, error.ModelArtifactsChanging => return error.ResourceTemporarilyUnavailable,
            else => return err,
        };
        if (!std.mem.eql(u8, &current.digest, &snapshot.digest) or
            !std.meta.eql(current.embedded_tokenizer_digest, snapshot.embedded_tokenizer_digest))
        {
            self.host.retire(self.host.context, self);
            return error.ModelArtifactsChanging;
        }
        try active.lock(&self.embedding_identity_mutex);
        self.chunk_planning = current;
        self.embedding_identity_mutex.unlock();
        return current.digest;
    }

    pub fn verifyEmbeddingIdentity(self: *const LoadedModel) !void {
        return self.verifyEmbeddingIdentityWithControl(.{});
    }

    pub fn verifyEmbeddingIdentityWithControl(self: *const LoadedModel, control: InferenceExecutionControl) !void {
        const mutable = @constCast(self);
        try control.check();
        const validation = if (mutable.embedding_validation) |*value| value else return;
        const identity = @import("../models/embedding_gemma2_identity.zig");
        const snapshot = validation.refresh(self.allocator, self.executor_io orelse std.Options.debug_io, self.model_dir, self.manifest.safetensors_path.?, control) catch |err| {
            // An unstable filesystem snapshot does not prove this loaded
            // runtime is stale. Preserve it and retry publication instead of
            // forcing a second resident session under backfill memory pressure.
            switch (err) {
                error.ModelArtifactsChanging, error.FileNotFound, error.NotDir => return error.ResourceTemporarilyUnavailable,
                error.Cancelled, error.Canceled, error.Timeout => return err,
                else => {
                    self.host.retire(self.host.context, mutable);
                    return err;
                },
            }
        };
        const digest = identity.forBackend(snapshot.digest, self.session.backend());
        if (self.embedding_identity) |loaded| if (std.mem.eql(u8, &loaded, &digest)) return;
        // Active handles retain rejected assets until they unwind; later
        // calls reload instead of reusing a permanently stale generation.
        self.host.retire(self.host.context, mutable);
        return error.ModelArtifactsChanging;
    }

    pub fn getTokenizer(self: *LoadedModel) tokenizer_mod.Tokenizer {
        var tokenizer = if (self.hf_tok) |ht| ht.tokenizer() else if (self.sp_tok) |sp| sp.tokenizer() else unreachable;
        // Managed tokenizers are immutable for this loaded model generation.
        // A pinned handle keeps both tokenizer and backend provider alive.
        tokenizer.cache_identity = if (self.embedding_identity) |signature| std.hash.Wyhash.hash(0, &signature) else null;
        return tokenizer;
    }

    pub fn getEmbeddingPrototypeCache(self: *LoadedModel, limits: runtime.tier.memory.Limits, control: InferenceExecutionControl) !*@import("../extractors/embedding_prototypes.zig").Cache {
        const prototypes = @import("../extractors/embedding_prototypes.zig");
        try control.lock(&self.embedding_prototype_cache_mutex);
        if (self.embedding_prototype_cache) |cache| {
            self.embedding_prototype_cache_mutex.unlock();
            return cache;
        }
        self.embedding_prototype_cache_mutex.unlock();
        // Admission may evict other models. Do it without cache/execution
        // locks, while the caller's loaded-model handle pins this model generation.
        var lease = try self.host.reserve(self.host.context, .cpu, limits, .{ .host_weight_bytes = prototypes.admitted_bytes });
        errdefer lease.release();
        const cache = try self.allocator.create(prototypes.Cache);
        errdefer self.allocator.destroy(cache);
        cache.* = .{};
        try control.lock(&self.embedding_prototype_cache_mutex);
        defer self.embedding_prototype_cache_mutex.unlock();
        if (self.embedding_prototype_cache) |published| {
            self.allocator.destroy(cache);
            lease.release();
            return published;
        }
        self.embedding_prototype_cache = cache;
        self.embedding_prototype_cache_lease = lease;
        return cache;
    }

    pub fn attachIo(self: *LoadedModel, io: std.Io) void {
        self.executor_io = io;
        session_factory.attachIo(self.session, io);
        if (self.vision_session) |session| session_factory.attachIo(session, io);
        if (self.audio_session) |session| session_factory.attachIo(session, io);
        if (self.text_projection) |session| session_factory.attachIo(session, io);
        if (self.visual_projection) |session| session_factory.attachIo(session, io);
        if (self.audio_projection) |session| session_factory.attachIo(session, io);
    }

    /// Snapshot all model-component exact-JIT counters while optional-session
    /// publication is stable. A cold optional-session load can take seconds,
    /// so metrics fall back to the last stable optional snapshot rather than
    /// waiting behind disk I/O. The primary session is immutable and can
    /// always be sampled directly.
    pub fn metalExactJitDispatchStats(self: *LoadedModel) session_factory.MetalExactJitDispatchStats {
        var total = session_factory.MetalExactJitDispatchStats{};
        if (session_factory.getMetalExactJitDispatchStats(self.session)) |stats| total.add(stats);

        if (!self.embedding_session_lock.tryLock()) {
            total.add(.{
                .q4_0_hits = self.optional_metal_jit_q4_0_hits.load(.acquire),
                .q4_k_hits = self.optional_metal_jit_q4_k_hits.load(.acquire),
            });
            return total;
        }
        defer self.embedding_session_lock.unlock();

        var optional = session_factory.MetalExactJitDispatchStats{};
        const sessions = [_]?backends.Session{
            self.vision_session,
            self.audio_session,
            self.text_projection,
            self.visual_projection,
            self.audio_projection,
        };
        for (sessions) |maybe_session| {
            const session = maybe_session orelse continue;
            if (session_factory.getMetalExactJitDispatchStats(session)) |stats| optional.add(stats);
        }
        self.optional_metal_jit_q4_0_hits.store(optional.q4_0_hits, .release);
        self.optional_metal_jit_q4_k_hits.store(optional.q4_k_hits, .release);
        total.add(optional);
        return total;
    }

    pub fn lockNativeGeneration(self: *LoadedModel, io: std.Io) void {
        platform.sync.lockYieldingIo(&self.native_generate_lock, io);
    }

    pub fn unlockNativeGeneration(self: *LoadedModel) void {
        self.native_generate_lock.unlock();
    }

    pub fn nativeGenerationMutex(self: *LoadedModel) *std.atomic.Mutex {
        return &self.native_generate_lock;
    }

    pub fn wholeModelExecutor(self: *LoadedModel, allocator: std.mem.Allocator, kv_dtype: ?runtime.kv.pool.KvDType) !?graph_mod.model_runtime.ModelExecutor {
        return @import("../execution/model_candidate.zig").createSessionExecutor(allocator, self.session, .{
            .kv_dtype = kv_dtype,
            .shared_moe_cache = self.shared_moe_cache,
        });
    }

    pub fn ensureOptionalSessionWithControl(
        self: *LoadedModel,
        kind: DeclaredOptionalSessionKind,
        slot: *?backends.Session,
        lease_slot: *?runtime.tier.memory.AdmissionLease,
        path: ?[]const u8,
        control: ?InferenceExecutionControl,
    ) !bool {
        if (control) |active| try active.check();
        if (slot.* != null) return false;
        const session_path = path orelse return false;
        const shared_ctx = backends.imported_onnx_session.sharedBackendContext(self.session);
        const strict_backend = [_]backends.BackendType{if (shared_ctx) |shared|
            shared.backendType()
        else
            self.session.backend()};
        var session_manager = self.session_manager.*.withPreferredBackends(
            self.allocator,
            strict_backend[0..],
        );
        const profile_bundle = if (self.kernel_jit_profile_bundle) |*bundle| blk: {
            const mapped = bundle.kernelJitBundleForMode(session_manager.kernel_jit.mode);
            session_manager.kernel_jit.qualified_profile_path = mapped.primary;
            break :blk mapped;
        } else null;
        try bindOptionalSessionProfile(&session_manager, kind, profile_bundle);
        var loaded = try session_loader.loadComponent(
            self.allocator,
            self.host.session_loading,
            session_path,
            strict_backend[0..],
            shared_ctx,
            &session_manager,
            control,
        );
        defer loaded.deinit();
        if (control) |active| try active.check();
        const owned = loaded.take();
        slot.* = owned.session;
        lease_slot.* = owned.resource_lease;
        return true;
    }

    pub fn ensureOptionalSession(
        self: *LoadedModel,
        kind: DeclaredOptionalSessionKind,
        slot: *?backends.Session,
        lease_slot: *?runtime.tier.memory.AdmissionLease,
        path: ?[]const u8,
    ) !bool {
        return self.ensureOptionalSessionWithControl(kind, slot, lease_slot, path, null);
    }

    pub fn releaseOptionalSession(
        slot: *?backends.Session,
        lease_slot: *?runtime.tier.memory.AdmissionLease,
    ) void {
        if (slot.*) |session| session.close();
        slot.* = null;
        if (lease_slot.*) |*lease| lease.release();
        lease_slot.* = null;
    }

    /// Serialize short optional-session slot mutations. Request execution is
    /// protected separately by embedding_asset_gate so primary-only callers
    /// retain shared concurrency.
    pub fn lockEmbeddingAssets(self: *LoadedModel) void {
        spinLock(&self.embedding_session_lock);
    }

    pub fn lockEmbeddingAssetsWithControl(self: *LoadedModel, control: InferenceExecutionControl) !void {
        try control.lock(&self.embedding_session_lock);
    }

    pub fn unlockEmbeddingAssets(self: *LoadedModel) void {
        self.embedding_session_lock.unlock();
    }

    pub fn acquireEmbeddingAssetLease(self: *LoadedModel, include_audio: bool) EmbeddingAssetLease {
        if (include_audio) {
            self.embedding_asset_gate.lockExclusive();
            return .{ .gate = &self.embedding_asset_gate, .access = .exclusive };
        }
        self.embedding_asset_gate.lockShared();
        return .{ .gate = &self.embedding_asset_gate, .access = .shared };
    }

    /// Load every optional model/projection declared by the manifest without
    /// running media inference. Startup preload calls this while the node owns
    /// the exclusive JIT qualification phase.
    pub fn materializeDeclaredOptionalSessions(self: *LoadedModel) !void {
        spinLock(&self.embedding_session_lock);
        defer self.embedding_session_lock.unlock();

        for (declaredOptionalSessions(&self.manifest)) |declared| {
            _ = switch (declared.kind) {
                .vision => try self.ensureOptionalSession(.vision, &self.vision_session, &self.vision_resource_lease, declared.path),
                .audio => try self.ensureOptionalSession(.audio, &self.audio_session, &self.audio_resource_lease, declared.path),
                .text_projection => try self.ensureOptionalSession(.text_projection, &self.text_projection, &self.text_projection_resource_lease, declared.path),
                .visual_projection => try self.ensureOptionalSession(.visual_projection, &self.visual_projection, &self.visual_projection_resource_lease, declared.path),
                .audio_projection => try self.ensureOptionalSession(.audio_projection, &self.audio_projection, &self.audio_projection_resource_lease, declared.path),
            };
        }
        if (!self.declaredOptionalSessionsMaterializedUnlocked())
            return error.OptionalSessionMaterializationIncomplete;
    }

    pub fn declaredOptionalSessionsMaterialized(self: *LoadedModel) bool {
        spinLock(&self.embedding_session_lock);
        defer self.embedding_session_lock.unlock();
        return self.declaredOptionalSessionsMaterializedUnlocked();
    }

    pub fn declaredOptionalSessionsMaterializedUnlocked(self: *const LoadedModel) bool {
        return declaredOptionalSessionsComplete(&self.manifest, .{
            self.vision_session != null,
            self.audio_session != null,
            self.text_projection != null,
            self.visual_projection != null,
            self.audio_projection != null,
        });
    }

    pub fn ensureVisionSession(self: *LoadedModel) !void {
        self.lockEmbeddingAssets();
        defer self.unlockEmbeddingAssets();
        _ = try self.ensureOptionalSession(
            .vision,
            &self.vision_session,
            &self.vision_resource_lease,
            self.manifest.visual_model_path,
        );
    }

    pub fn ensureVisionSessionWithControl(self: *LoadedModel, control: InferenceExecutionControl) !void {
        try self.lockEmbeddingAssetsWithControl(control);
        defer self.unlockEmbeddingAssets();
        _ = try self.ensureOptionalSessionWithControl(
            .vision,
            &self.vision_session,
            &self.vision_resource_lease,
            self.manifest.visual_model_path,
            control,
        );
    }

    pub fn ensureEmbeddingAssets(self: *LoadedModel, include_text: bool, include_image: bool, include_audio: bool) !void {
        self.lockEmbeddingAssets();
        defer self.unlockEmbeddingAssets();
        try self.ensureEmbeddingAssetsLocked(include_text, include_image, include_audio);
    }

    pub fn ensureEmbeddingAssetsLocked(self: *LoadedModel, include_text: bool, include_image: bool, include_audio: bool) !void {
        try self.ensurePrimaryEmbeddingAssetsLocked(include_text, include_image);
        if (include_audio) try self.ensureAudioEmbeddingAssetsLocked();
    }

    pub fn ensureEmbeddingAssetsLockedWithControl(
        self: *LoadedModel,
        include_text: bool,
        include_image: bool,
        include_audio: bool,
        control: InferenceExecutionControl,
    ) !void {
        try self.ensurePrimaryEmbeddingAssetsLockedWithControl(include_text, include_image, control);
        if (include_audio) try self.ensureAudioEmbeddingAssetsLockedWithControl(control);
    }

    const PrimaryEmbeddingAssetAcquisitions = struct {
        text_projection: bool = false,
        vision: bool = false,
        visual_projection: bool = false,
    };

    pub fn rollbackPrimaryEmbeddingAssetsLocked(self: *LoadedModel, acquired: PrimaryEmbeddingAssetAcquisitions) void {
        if (acquired.visual_projection) releaseOptionalSession(
            &self.visual_projection,
            &self.visual_projection_resource_lease,
        );
        if (acquired.vision) releaseOptionalSession(
            &self.vision_session,
            &self.vision_resource_lease,
        );
        if (acquired.text_projection) releaseOptionalSession(
            &self.text_projection,
            &self.text_projection_resource_lease,
        );
        _ = platform.allocator.reclaimUnusedProcessMemory();
    }

    /// Admit the reusable text/image sidecars transactionally. A failed
    /// multi-session admission releases only sessions acquired by this call,
    /// preserving already-warm assets for concurrent request efficiency.
    pub fn ensurePrimaryEmbeddingAssetsLocked(self: *LoadedModel, include_text: bool, include_image: bool) !void {
        var acquired = PrimaryEmbeddingAssetAcquisitions{};
        errdefer self.rollbackPrimaryEmbeddingAssetsLocked(acquired);

        if (include_text) {
            acquired.text_projection = try self.ensureOptionalSession(
                .text_projection,
                &self.text_projection,
                &self.text_projection_resource_lease,
                self.manifest.text_projection_path,
            );
        }
        if (include_image) {
            acquired.vision = try self.ensureOptionalSession(
                .vision,
                &self.vision_session,
                &self.vision_resource_lease,
                self.manifest.visual_model_path,
            );
            acquired.visual_projection = try self.ensureOptionalSession(
                .visual_projection,
                &self.visual_projection,
                &self.visual_projection_resource_lease,
                self.manifest.visual_projection_path,
            );
        }
    }

    pub fn ensurePrimaryEmbeddingAssetsLockedWithControl(
        self: *LoadedModel,
        include_text: bool,
        include_image: bool,
        control: InferenceExecutionControl,
    ) !void {
        var acquired = PrimaryEmbeddingAssetAcquisitions{};
        errdefer self.rollbackPrimaryEmbeddingAssetsLocked(acquired);

        if (include_text) {
            acquired.text_projection = try self.ensureOptionalSessionWithControl(
                .text_projection,
                &self.text_projection,
                &self.text_projection_resource_lease,
                self.manifest.text_projection_path,
                control,
            );
        }
        if (include_image) {
            acquired.vision = try self.ensureOptionalSessionWithControl(
                .vision,
                &self.vision_session,
                &self.vision_resource_lease,
                self.manifest.visual_model_path,
                control,
            );
            acquired.visual_projection = try self.ensureOptionalSessionWithControl(
                .visual_projection,
                &self.visual_projection,
                &self.visual_projection_resource_lease,
                self.manifest.visual_projection_path,
                control,
            );
        }
    }

    /// Admit the ephemeral audio sidecars as one phase. Any partial admission
    /// is rolled back immediately so a failed request cannot strand a lease
    /// and prevent subsequent text/image work from making progress.
    pub fn ensureAudioEmbeddingAssetsLocked(self: *LoadedModel) !void {
        errdefer self.releaseAudioEmbeddingAssetsLocked();
        _ = try self.ensureOptionalSession(
            .audio,
            &self.audio_session,
            &self.audio_resource_lease,
            self.manifest.audio_model_path,
        );
        _ = try self.ensureOptionalSession(
            .audio_projection,
            &self.audio_projection,
            &self.audio_projection_resource_lease,
            self.manifest.audio_projection_path,
        );
    }

    pub fn ensureAudioEmbeddingAssetsLockedWithControl(self: *LoadedModel, control: InferenceExecutionControl) !void {
        errdefer self.releaseAudioEmbeddingAssetsLocked();
        _ = try self.ensureOptionalSessionWithControl(
            .audio,
            &self.audio_session,
            &self.audio_resource_lease,
            self.manifest.audio_model_path,
            control,
        );
        _ = try self.ensureOptionalSessionWithControl(
            .audio_projection,
            &self.audio_projection,
            &self.audio_projection_resource_lease,
            self.manifest.audio_projection_path,
            control,
        );
    }

    /// Release the lazily loaded audio branch after its outputs have been
    /// materialized. The primary CLIP session can then admit text/image work
    /// without counting an idle CLAP session against the same host budget.
    pub fn releaseAudioEmbeddingAssetsLocked(self: *LoadedModel) void {
        releaseOptionalSession(
            &self.audio_projection,
            &self.audio_projection_resource_lease,
        );
        releaseOptionalSession(
            &self.audio_session,
            &self.audio_resource_lease,
        );
        // Audio sidecars are intentionally request-phased rather than cached.
        // Their backend allocations can be large and differently shaped from
        // the persistent vision/text sessions, so complete the physical
        // release at this explicit cold boundary before admitting later work.
        _ = platform.allocator.reclaimUnusedProcessMemory();
    }

    pub fn embeddingPipeline(self: *LoadedModel, allocator: std.mem.Allocator) EmbeddingPipeline {
        self.lockEmbeddingAssets();
        defer self.unlockEmbeddingAssets();
        return self.embeddingPipelineLocked(allocator);
    }

    pub fn embeddingPipelineLocked(self: *LoadedModel, allocator: std.mem.Allocator) EmbeddingPipeline {
        const tok = self.getTokenizer();
        const contract = if (self.session.model_contract.resolved) self.session.model_contract else @import("../models/session_contract.zig").resolve(self.session.model_contract, &self.manifest, self.session.backend());
        const semantics = contract.embedding.?;
        var pipeline = EmbeddingPipeline.init(allocator, self.session, tok, .{
            .max_length = semantics.max_length,
            .reject_truncation = semantics.reject_truncation,
            .normalize = semantics.normalize,
            .pooling = switch (semantics.pooling) {
                .mean => .mean,
                .cls => .cls,
                .max => .max,
                .last => .last,
            },
            .text_prefix = semantics.document_prefix,
            .trim_padding_to_batch_max = contract.padding == .batch_max,
            .resident_qwen3_embedding = semantics.family == .last_token_decoder,
            .resident_text_encoder = contract.resident_text_embeddings,
            .resident_embedding_gemma2 = semantics.family == .gemma2 and self.session.backend() == .cuda,
            .allow_image = semantics.image,
            .allow_video = semantics.video,
            .allow_audio = semantics.audio,
            .strict_max_length = semantics.reject_truncation,
            .preprocess_io = self.executor_io,
            // Last-token pooling reads the EOS position; guarantee exactly
            // one trailing EOS regardless of tokenizer.json snapshot age.
            .ensure_trailing_eos_id = if (semantics.family == .last_token_decoder and tok.specialTokens().sep_id >= 0)
                tok.specialTokens().sep_id
            else
                null,
        });
        if (usesClipImagePreprocessProfile(&self.manifest)) {
            pipeline.config.image_preprocess_profile = .clip;
        }
        if (session_factory.getClipConfig(self.session)) |cfg| {
            pipeline.config.image_size = cfg.image_size;
            if (cfg.family == .clip) pipeline.config.image_preprocess_profile = .clip;
        }
        self.bindEmbeddingPipelineAssetsLocked(&pipeline);
        pipeline.resident_projection_stats = &self.resident_projection_stats;
        pipeline.execution_lock = self.embeddingExecutionLock();
        pipeline.execution_lane = if (self.manifest.embedding_style == .embedding_gemma2) &self.embedding_execution_lane else null;
        return pipeline;
    }

    /// Refresh borrowed optional-session handles after a phased admission.
    /// Callers hold embedding_session_lock while rebinding and retain an
    /// embedding asset lease for the pipeline's full use.
    pub fn bindEmbeddingPipelineAssetsLocked(self: *LoadedModel, pipeline: *EmbeddingPipeline) void {
        if (session_factory.getClipConfig(self.session) == null) {
            if (self.vision_session) |vs| {
                if (session_factory.getClipConfig(vs)) |cfg| {
                    pipeline.config.image_size = cfg.image_size;
                    if (cfg.family == .clip) pipeline.config.image_preprocess_profile = .clip;
                }
            }
        }
        pipeline.vision_session = self.vision_session;
        pipeline.audio_session = self.audio_session;
        pipeline.text_projection = self.text_projection;
        pipeline.visual_projection = self.visual_projection;
        pipeline.audio_projection = self.audio_projection;
    }

    pub fn embeddingExecutionLock(self: *LoadedModel) ?*std.atomic.Mutex {
        return self.targetInferenceExecutionMutex();
    }

    pub fn targetInferenceExecutionMutex(self: *LoadedModel) ?*std.atomic.Mutex {
        return targetInferenceExecutionMutexForBackend(self.session.backend(), &self.target_inference_run_lock);
    }

    pub fn rerankingPipeline(self: *LoadedModel, allocator: std.mem.Allocator) RerankingPipeline {
        const tok = self.getTokenizer();
        const is_qwen3_generative_reranker = self.manifest.isQwen3GenerativeReranker();
        var pipeline = RerankingPipeline.init(allocator, self.session, tok, .{
            .max_length = if (is_qwen3_generative_reranker)
                @min(self.manifest.maxTextSequenceLength(), qwen3vl_reranker.default_max_length)
            else
                self.manifest.maxTextSequenceLength(),
            .mode = if (is_qwen3_generative_reranker)
                ScoringMode.generative_yes_no
            else if (self.manifest.hasCapability("late_interaction") or
                self.manifest.hasCapability("colbert") or
                self.manifest.hasCapability("colqwen") or
                self.manifest.hasCapability("multimodal_late_interaction"))
                ScoringMode.late_interaction
            else
                ScoringMode.cross_encoder,
            .single_text_encoding = if (is_qwen3_generative_reranker or self.manifest.prefersGenerationEncodingForLateInteraction()) .generation else .encoder,
            .generative_prompt_profile = if (self.manifest.isQwen3TextReranker()) .qwen3_text else .qwen3_vl,
            .add_bos_token = self.manifest.add_bos_token,
            .distributed = runtime.distributed.configFromEnv(),
        });
        pipeline.execution_lock = self.targetInferenceExecutionMutex();
        return pipeline;
    }

    pub fn classificationPipeline(self: *LoadedModel, allocator: std.mem.Allocator, config: ClassificationConfig) ClassificationPipeline {
        const tok = self.getTokenizer();
        var effective = config;
        effective.max_length = @min(effective.max_length, self.manifest.maxTextSequenceLength());
        effective.distributed = runtime.distributed.configFromEnv();
        return ClassificationPipeline.init(allocator, self.session, tok, effective);
    }

    pub fn nerPipeline(self: *LoadedModel, allocator: std.mem.Allocator) NerPipeline {
        const tok = self.getTokenizer();
        // Cast id2label from ?[][]const u8 to ?[]const []const u8
        const id2label: ?[]const []const u8 = if (self.manifest.id2label) |labels| labels else null;
        return NerPipeline.init(allocator, self.session, tok, .{
            .max_length = self.manifest.maxTextSequenceLength(),
            .id2label = id2label,
            .distributed = runtime.distributed.configFromEnv(),
        });
    }

    pub fn isGlinerModel(self: *LoadedModel) bool {
        return self.manifest.gliner_model_type.len > 0;
    }

    pub fn supportsClassification(self: *LoadedModel) bool {
        return model_caps.modelSupportsCapability(
            @tagName(self.manifest.model_type),
            self.manifest.gliner_model_type,
            self.manifest.capabilities,
            "classification",
        );
    }

    pub fn supportsExtraction(self: *LoadedModel) bool {
        return model_caps.modelSupportsCapability(
            @tagName(self.manifest.model_type),
            self.manifest.gliner_model_type,
            self.manifest.capabilities,
            "extraction",
        );
    }

    pub fn supportsRelationExtraction(self: *LoadedModel) bool {
        return model_caps.modelSupportsCapability(
            @tagName(self.manifest.model_type),
            self.manifest.gliner_model_type,
            self.manifest.capabilities,
            "relations",
        );
    }

    pub fn glinerPipeline(self: *LoadedModel, allocator: std.mem.Allocator) GlinerPipeline {
        const tok = self.getTokenizer();
        return .{
            .allocator = allocator,
            .session = self.session,
            .tok = tok,
            .execution_lock = self.targetInferenceExecutionMutex(),
            .config = .{
                .max_width = self.manifest.gliner_max_width,
                .max_length = if (self.manifest.gliner_architecture == .span and
                    self.manifest.gliner_span_encoder_family == .modern_bert and
                    self.session.backend() == .cuda)
                    @min(self.manifest.max_position_embeddings, 512)
                else
                    self.manifest.max_position_embeddings,
                .threshold = self.manifest.gliner_threshold,
                .flat_ner = self.manifest.gliner_flat_ner,
                .default_labels = self.manifest.gliner_default_labels,
                .relation_labels = self.manifest.gliner_relation_labels,
                .relation_threshold = self.manifest.gliner_relation_threshold,
                .model_type = self.manifest.gliner_model_type,
                .capabilities = self.manifest.capabilities,
                .classification_head = switch (self.manifest.gliner_classification_head) {
                    .none => .none,
                    .label_marker_mlp => .label_marker_mlp,
                },
                .token_p = self.manifest.gliner_token_p,
                .token_c = self.manifest.gliner_token_c,
                .token_l = self.manifest.gliner_token_l,
                .token_e = self.manifest.gliner_token_e,
                .token_r = self.manifest.gliner_token_r,
                .token_sep_struct = self.manifest.gliner_token_sep_struct,
                .token_sep_text = self.manifest.gliner_token_sep_text,
                .distributed = runtime.distributed.configFromEnv(),
            },
        };
    }

    pub fn getCleanupHead(self: *LoadedModel) !?*const cleanup_model_mod.CleanupHead {
        if (self.cleanup_head_loaded) return self.cleanup_head;

        var loaded = (try cleanup_model_mod.loadHeadIfPresent(self.allocator, self.model_dir)) orelse {
            self.cleanup_head_loaded = true;
            return null;
        };
        errdefer loaded.deinit();
        const head = try self.allocator.create(cleanup_model_mod.CleanupHead);
        head.* = loaded;
        self.cleanup_head = head;
        self.cleanup_head_loaded = true;
        return head;
    }

    /// The model's open projector file, opened on first use. Null when the
    /// manifest has no GGUF projector.
    pub fn ensureProjectorStore(self: *LoadedModel) !?*gemma4_projector.ProjectorStore {
        const path = self.manifest.gguf_projector_path orelse return null;
        platform.sync.lockYielding(&self.projector_store_mutex);
        defer self.projector_store_mutex.unlock();
        if (self.projector_store) |store| return store;
        const store = try gemma4_projector.ProjectorStore.open(self.allocator, path);
        self.projector_store = store;
        return store;
    }

    pub fn deinit(self: *LoadedModel) void {
        std.debug.assert(self.native_reader_stream.context == null and self.native_reader_stream.users == 0);
        if (self.embedding_prototype_cache) |cache| self.allocator.destroy(cache);
        if (self.embedding_prototype_cache_lease) |*lease| lease.release();
        if (self.projector_store) |store| {
            store.close();
            self.projector_store = null;
        }
        // Cache destruction may enter a driver before Session.close. Protect
        // every retained component first, without borrowing request controls.
        var close_scopes: [6]backends.Session.CloseScope = @splat(.{});
        close_scopes[0] = self.session.beginClose();
        const optional_sessions = [_]?backends.Session{ self.vision_session, self.audio_session, self.text_projection, self.visual_projection, self.audio_projection };
        for (optional_sessions, 1..) |session, index| {
            if (session) |component| close_scopes[index] = component.beginClose();
        }
        self.native_generation_graph_cache.deinit();
        self.prompt_prefix_cache.deinit();
        self.session.close();
        if (self.vision_session) |vs| vs.close();
        if (self.audio_session) |as_| as_.close();
        if (self.text_projection) |tp| tp.close();
        if (self.visual_projection) |vp| vp.close();
        if (self.audio_projection) |ap| ap.close();
        if (self.kernel_jit_profile_bundle) |*bundle| bundle.deinit();
        if (self.vision_resource_lease) |*lease| lease.release();
        if (self.audio_resource_lease) |*lease| lease.release();
        if (self.text_projection_resource_lease) |*lease| lease.release();
        if (self.visual_projection_resource_lease) |*lease| lease.release();
        if (self.audio_projection_resource_lease) |*lease| lease.release();
        if (self.whisper_prompt_cache) |*cache| cache.deinit();
        if (self.shared_moe_cache) |cache| {
            cache.deinit();
            self.allocator.destroy(cache);
        }
        if (self.shared_prefetch) |state| {
            state.deinit();
            self.allocator.destroy(state);
        }
        if (self.native_generate_coordinator) |coordinator| {
            coordinator.deinit();
            self.allocator.destroy(coordinator);
        }
        if (self.cleanup_head) |head| {
            head.deinit();
            self.allocator.destroy(head);
        }
        // All session/cache/prefetch driver cleanup has returned. End protection
        // before unrelated host tokenizer/template destruction: large vocabularies
        // under Debug allocation tracking can otherwise trigger a false restart.
        for (&close_scopes) |*scope| scope.deinit();
        if (self.hf_tok) |ht| ht.deinitSelf();
        if (self.sp_tok) |sp| {
            sp.deinit();
            self.allocator.destroy(sp);
        }
        if (self.tokenizer_resource_lease) |*lease| lease.release();
        if (self.chat_tmpl) |ct| {
            var ct_mut = @constCast(ct);
            ct_mut.deinit();
            self.allocator.destroy(ct_mut);
        }
        if (self.resource_lease) |*lease| lease.release();
        self.manifest.deinit();
        self.allocator.free(self.model_dir);
    }
};

pub fn targetInferenceExecutionMutexForBackend(
    backend: backends.BackendType,
    model_mutex: *std.atomic.Mutex,
) ?*std.atomic.Mutex {
    return switch (backend) {
        .metal, .cuda => model_mutex,
        else => null,
    };
}

pub fn usesClipImagePreprocessProfile(manifest: *const manifest_mod.ModelManifest) bool {
    return std.mem.eql(u8, manifest.config_model_arch, "clip") or
        std.mem.eql(u8, manifest.config_model_arch, "clipclap") or
        manifest.isClipclapGgufBundle();
}

test "loaded runtime cleanup head publication unwinds every allocation failure" {
    const a = std.testing.allocator;
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    try temp.dir.writeFile(std.testing.io, .{ .sub_path = cleanup_model_mod.head_file_name, .data = "{\"feature_dim\":2,\"embedding_dim\":1,\"context_window\":3,\"validity_weight\":[1,2],\"validity_bias\":0,\"representative_weight\":[3,4],\"representative_bias\":0,\"embedding_proj\":[5,6]}" });
    const directory = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temp.sub_path[0..] });
    defer a.free(directory);
    const Case = struct {
        fn run(allocator: std.mem.Allocator, path: []const u8) !void {
            var model: LoadedModel = undefined;
            model.allocator = allocator;
            model.model_dir = path;
            model.cleanup_head_loaded = false;
            model.cleanup_head = null;
            defer if (model.cleanup_head) |head| {
                head.deinit();
                allocator.destroy(head);
            };
            const head = (try model.getCleanupHead()) orelse return error.TestUnexpectedResult;
            try std.testing.expectEqualSlices(f32, &.{ 5, 6 }, head.embedding_proj);
            try std.testing.expectEqual(head, (try model.getCleanupHead()).?);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Case.run, .{directory});
}
