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

// Model manager: lazy-loads models and caches ready-to-use pipelines.
//
// Given a model directory path, loads the manifest, creates a tokenizer
// and backend session, and returns a pipeline ready for inference.

const loaded_runtime = @import("../models/loaded_runtime.zig");
const backend_compatibility = @import("../models/backend_compatibility.zig");
const tokenizer_loader = @import("../models/tokenizer_loader.zig");
const session_loader = @import("../models/session_loader.zig");
const load_plan = @import("../models/load_plan.zig");
const std = @import("std");
const admission_diagnostics = @import("admission_diagnostics.zig");
const execution_control_mod = @import("../execution_control.zig");
const InferenceExecutionControl = loaded_runtime.InferenceExecutionControl;
const HardCancellationWatchdog = @import("../hard_cancellation_watchdog.zig").HardCancellationWatchdog;
const builtin = @import("builtin");
const build_options = @import("build_options");
const platform = @import("antfly_platform");

const backends = @import("../backends/backends.zig");
const manifest_mod = @import("../models/manifest.zig");
const model_compatibility = @import("../models/compatibility.zig");
const gliner_decide_qualification = @import("../models/gliner_decide_qualification.zig");
const gliner_specialty = @import("../models/gliner_specialty_qualification.zig");
const managed_receipt = @import("../registry/managed_receipt.zig");
const ModelSource = @import("../models/model_source.zig").ModelSource;
const PublishedArtifacts = @import("../registry/published_artifacts.zig").PublishedArtifacts;
const ResolvedModel = @import("../registry/model_selection.zig").ResolvedModel;

const ComponentConstruction = session_loader.ComponentConstruction;
const BoundDirectoryManifest = session_loader.BoundDirectoryManifest;
const BoundModelSource = session_loader.BoundModelSource;

/// Receipt membership is immutable within a publication. Cache positive and
/// negative answers without retaining request memory or walking weight files
/// on warm hits. Unmanaged directories deliberately bypass this cache.
const MergedDecoderMembershipCache = struct {
    const capacity = 128;
    const Location = enum {
        absent,
        root,
        onnx,
        fn relative(self: Location) ?[]const u8 {
            return switch (self) {
                .absent => null,
                .root => "decoder_model_merged.onnx",
                .onnx => "onnx/decoder_model_merged.onnx",
            };
        }
    };
    const Entry = struct { key: [32]u8, location: Location, used: u64 };
    const Lookup = union(enum) { miss, hit: Location };
    mutex: std.atomic.Mutex = .unlocked,
    builders: [8]std.atomic.Mutex = @splat(.unlocked),
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    clock: u64 = 0,

    fn keyFor(selected: ResolvedModel) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&selected.binding().directory);
        hash.update(&selected.publication.?);
        return hash.finalResult();
    }
    fn get(self: *MergedDecoderMembershipCache, key: [32]u8) Lookup {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |*entry| if (std.mem.eql(u8, &entry.key, &key)) {
            self.clock +|= 1;
            entry.used = self.clock;
            return .{ .hit = entry.location };
        };
        return .miss;
    }
    fn put(self: *MergedDecoderMembershipCache, a: std.mem.Allocator, key: [32]u8, location: Location) !void {
        spinLock(&self.mutex);
        defer self.mutex.unlock();
        self.clock +|= 1;
        const next = Entry{ .key = key, .location = location, .used = self.clock };
        for (self.entries.items) |*entry| if (std.mem.eql(u8, &entry.key, &key)) {
            entry.* = next;
            return;
        };
        if (self.entries.items.len < capacity) return self.entries.append(a, next);
        var oldest: usize = 0;
        for (self.entries.items, 0..) |entry, i| if (entry.used < self.entries.items[oldest].used) {
            oldest = i;
        };
        self.entries.items[oldest] = next;
    }
    fn deinit(self: *MergedDecoderMembershipCache, a: std.mem.Allocator) void {
        self.entries.deinit(a);
    }
};

const safetensors_mod = @import("../models/safetensors.zig");
const c_file = @import("../util/c_file.zig");
const gguf_format = @import("../gguf/format.zig");
const gguf_tensor_types = @import("../gguf/tensor_types.zig");
const gguf_writer = @import("../gguf/writer.zig");

const gemma4_projector = @import("../architectures/gemma4_projector.zig");
const qwen3vl_reranker = @import("../architectures/qwen3vl_reranker.zig");
const tokenizer_mod = @import("inference_tokenizer");
const hf_tokenizer = @import("inference_hf_tokenizer");
const sentencepiece = @import("inference_tokenizer").sentencepiece;
const whisper_prompt = @import("../pipelines/whisper_prompt.zig");
const encoder_decoder = @import("../pipelines/encoder_decoder.zig");
const vision_config = @import("../readers/vision_config.zig");
const embedding_mod = @import("../pipelines/embedding.zig");

const EmbeddingPipeline = loaded_runtime.EmbeddingPipeline;
const EmbeddingConfig = embedding_mod.EmbeddingConfig;
const PoolingStrategy = embedding_mod.PoolingStrategy;
const RerankingPipeline = @import("../pipelines/reranking.zig").RerankingPipeline;
const RerankingConfig = @import("../pipelines/reranking.zig").RerankingConfig;
const ScoringMode = @import("../pipelines/reranking.zig").ScoringMode;
const ClassificationPipeline = @import("../pipelines/classification.zig").ClassificationPipeline;
const ClassificationConfig = @import("../pipelines/classification.zig").ClassificationConfig;
const NerPipeline = @import("../pipelines/ner.zig").NerPipeline;
const NerConfig = @import("../pipelines/ner.zig").NerConfig;
const GlinerPipeline = @import("../pipelines/gliner.zig").GlinerPipeline;
const GlinerConfig = @import("../pipelines/gliner.zig").GlinerConfig;
const generation = @import("../pipelines/generation.zig");
const ChatTemplate = loaded_runtime.ChatTemplate;
const session_factory = @import("../architectures/session_factory.zig");
const backend_contracts = @import("../graph/backend_contracts.zig");
const graph_mod = @import("../graph/root.zig");
const kernel_jit_profile_output = @import("../kernel_jit_profile_output.zig");
const runtime = @import("../runtime/root.zig");
const onnx_graph = @import("onnx_graph");
const ml = @import("ml");

const manifestHasNativeAssets = backend_compatibility.manifestHasNativeAssets;

const ArtifactCandidateKind = backend_compatibility.ArtifactCandidateKind;

const artifactCandidateForBackend = backend_compatibility.artifactCandidateForBackend;

pub const CompatibilitySummary = backend_compatibility.CompatibilitySummary;

/// Describes the runtime contract represented by a set of independently loaded
/// model components. A component bundle must not inherit an incompatible
/// whole-model classification when a dedicated pipeline owns its runtime.
pub const ComponentContract = backend_compatibility.ComponentContract;

const ProjectorDecoderContract = backend_compatibility.ProjectorDecoderContract;

const projectorMatchesDecoder = backend_compatibility.projectorMatchesDecoder;

test "Qwen3-VL projectors are paired only with Qwen3-VL decoders" {
    const qwen = ProjectorDecoderContract{ .family = .qwen3vl };
    const gemma = ProjectorDecoderContract{ .family = .gemma4 };
    try std.testing.expect(projectorMatchesDecoder(.clip_qwen3vl_image, qwen));
    try std.testing.expect(!projectorMatchesDecoder(.clip_qwen3vl_image, gemma));
    try std.testing.expect(!projectorMatchesDecoder(.clip_gemma4_image, qwen));
}

const validateNativeCompanionsForBackend = backend_compatibility.validateNativeCompanionsForBackend;

const validateClipclapBundleForBackend = backend_compatibility.validateClipclapBundleForBackend;

/// Assess exactly the artifact route that `backend` would load. Optional
/// artifacts for other backends must not poison this result.
pub const compatibilitySummaryForBackend = backend_compatibility.compatibilitySummaryForBackend;

const qwen3EmbeddingSupportsBackend = backend_compatibility.qwen3EmbeddingSupportsBackend;

test "Qwen3 embedding production backends include native CPU and CUDA" {
    try std.testing.expect(qwen3EmbeddingSupportsBackend(.metal));
    try std.testing.expect(qwen3EmbeddingSupportsBackend(.native));
    try std.testing.expect(qwen3EmbeddingSupportsBackend(.cuda));
}

/// Aggregate candidate compatibility as an OR: a model bundle is usable when
/// any configured backend has a compatible artifact. Unknown is returned only
/// when no compatible route exists, and incompatible only when every candidate
/// is known to be unusable.
pub const compatibilitySummaryForBackends = backend_compatibility.compatibilitySummaryForBackends;

const selectBetterCompatibility = backend_compatibility.selectBetterCompatibility;

const policyAllowedBackends = backend_compatibility.policyAllowedBackends;

const ComponentPlanKey = loaded_runtime.ComponentPlanKey;
const component_plan_cache_capacity = 256;
const composite_assets_cache_capacity = 256;

const updateComponentPlanKeySlice = loaded_runtime.updateComponentPlanKeySlice;

const ComponentPlanCacheEntry = struct {
    allocator: std.mem.Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    signature: ComponentPlanKey,
    allowed_backends: [7]backends.BackendType = undefined,
    allowed_backend_count: usize = 0,
    dependencies: [][]u8,

    fn create(
        allocator: std.mem.Allocator,
        signature: ComponentPlanKey,
        allowed: []const backends.BackendType,
        dependencies: []const []const u8,
    ) !*ComponentPlanCacheEntry {
        const self = try allocator.create(ComponentPlanCacheEntry);
        errdefer allocator.destroy(self);
        const owned_dependencies = try allocator.alloc([]u8, dependencies.len);
        errdefer allocator.free(owned_dependencies);
        var initialized: usize = 0;
        errdefer for (owned_dependencies[0..initialized]) |path| allocator.free(path);
        for (dependencies, 0..) |path, index| {
            owned_dependencies[index] = try allocator.dupe(u8, path);
            initialized += 1;
        }
        self.* = .{
            .allocator = allocator,
            .signature = signature,
            .dependencies = owned_dependencies,
        };
        @memcpy(self.allowed_backends[0..allowed.len], allowed);
        self.allowed_backend_count = allowed.len;
        return self;
    }

    fn retain(self: *ComponentPlanCacheEntry) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }

    fn release(self: *ComponentPlanCacheEntry) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        for (self.dependencies) |path| self.allocator.free(path);
        self.allocator.free(self.dependencies);
        const allocator = self.allocator;
        allocator.destroy(self);
    }
};

fn componentPlanKey(
    model_dir: []const u8,
    man: *const manifest_mod.ModelManifest,
    preferred_backends: []const backends.BackendType,
    component_paths: []const []const u8,
    policy: model_compatibility.Policy,
    contract: ComponentContract,
) ComponentPlanKey {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    updateComponentPlanKeySlice(&hash, model_dir);
    hash.update(&.{
        @backingInt(man.model_type),
        @backingInt(man.model_type_origin),
        @backingInt(man.native_arch_hint),
        @backingInt(contract),
        @intFromBool(policy.allow_unknown),
        @intFromBool(manifestHasNativeAssets(man.*)),
        @intFromBool(man.hasIncompleteGlinerBundle()),
        @intFromBool(man.hasIncompleteColqwenBundle()),
        @intFromBool(man.hasIncompleteClipclapGgufBundle()),
        @intFromBool(man.hasIncompleteFlorence2GgufBundle()),
    });
    updateComponentPlanKeySlice(&hash, man.config_model_arch);
    updateComponentPlanKeySlice(&hash, man.gliner_model_type);
    updateComponentPlanKeySlice(&hash, man.inference_bundle_family);
    const backend_count: u64 = @intCast(preferred_backends.len);
    hash.update(std.mem.asBytes(&backend_count));
    for (preferred_backends) |backend| hash.update(&.{@backingInt(backend)});
    const component_count: u64 = @intCast(component_paths.len);
    hash.update(std.mem.asBytes(&component_count));
    for (component_paths) |path| {
        updateComponentPlanKeySlice(&hash, path);
    }
    var digest: ComponentPlanKey = undefined;
    hash.final(&digest);
    return digest;
}

/// Build a cheap cache-coherency identity from filesystem metadata. Full
/// artifact validation populates the cache; cache hits only need to detect
/// replacement or mutation. inode/size/mtime/ctime cover those transitions
/// without rereading and hashing model contents on request paths.
const componentDependencySignature = loaded_runtime.componentDependencySignature;

const inspectComponentArtifacts = backend_compatibility.inspectComponentArtifacts;

const validateComponentNativeArtifacts = backend_compatibility.validateComponentNativeArtifacts;

const validateComponentImportedGraphs = backend_compatibility.validateComponentImportedGraphs;

const componentCompatibilityForBackend = backend_compatibility.componentCompatibilityForBackend;

const policyAllowedComponentBackendsFromInspection = backend_compatibility.policyAllowedComponentBackendsFromInspection;

const spinLock = loaded_runtime.spinLock;

/// Writer-preferring process-local gate for optional embedding assets.
/// Primary-only requests take shared access and retain full backend concurrency;
/// requests that swap ephemeral audio sessions take exclusive access.
const EmbeddingAssetGate = loaded_runtime.EmbeddingAssetGate;

pub const EmbeddingAssetLease = loaded_runtime.EmbeddingAssetLease;

pub const shouldPreferSentencePieceOverride = tokenizer_loader.shouldPreferSentencePieceOverride;
pub const shouldEnableGemmaSentencePieceCompat = tokenizer_loader.shouldEnableGemmaSentencePieceCompat;
pub const loadSentencePieceAddedTokens = tokenizer_loader.loadSentencePieceAddedTokens;
pub const loadHuggingFaceTokenizerFromDir = tokenizer_loader.loadHuggingFaceTokenizerFromDir;
pub const loadHuggingFaceTokenizerFromDirOrGguf = tokenizer_loader.loadHuggingFaceTokenizerFromDirOrGguf;
const loadHuggingFaceTokenizerFromManifest = tokenizer_loader.loadHuggingFaceTokenizerFromManifest;
const unigramTokenizerJsonFromGguf = tokenizer_loader.unigramTokenizerJsonFromGguf;
pub const loadSentencePieceTokenizerFromDirOrGguf = tokenizer_loader.loadSentencePieceTokenizerFromDirOrGguf;
const adoptAndConfigureSentencePieceTokenizer = tokenizer_loader.adoptAndConfigureSentencePieceTokenizer;

pub const isModelDirPotentiallyLoadableInCurrentBuild = backend_compatibility.isModelDirPotentiallyLoadableInCurrentBuild;

pub const isManifestPotentiallyLoadableInCurrentBuild = backend_compatibility.isManifestPotentiallyLoadableInCurrentBuild;

const DeclaredOptionalSessionKind = loaded_runtime.DeclaredOptionalSessionKind;

const DeclaredOptionalSession = loaded_runtime.DeclaredOptionalSession;

const declared_optional_session_count = loaded_runtime.declared_optional_session_count;

const declaredOptionalSessions = loaded_runtime.declaredOptionalSessions;

const declaredOptionalSessionsComplete = loaded_runtime.declaredOptionalSessionsComplete;

const bindOptionalSessionProfile = loaded_runtime.bindOptionalSessionProfile;

fn ownSelectedFirstBackendPreference(
    allocator: std.mem.Allocator,
    preferred: []const backends.BackendType,
    selected: backends.BackendType,
) ![]backends.BackendType {
    var count: usize = 1;
    for (preferred) |backend| {
        if (backend != selected) count += 1;
    }
    const result = try allocator.alloc(backends.BackendType, count);
    result[0] = selected;
    var index: usize = 1;
    for (preferred) |backend| {
        if (backend == selected) continue;
        result[index] = backend;
        index += 1;
    }
    return result;
}

const resolutionModelRevision = loaded_runtime.resolutionModelRevision;

// Legacy managed receipts may lack a digest. Hash those files once per local
// file generation, keeping the resulting identity content-based across copies.
const LegacyArtifactDigestCache = loaded_runtime.ArtifactDigestCache;

/// Stable identity before loading weights. Managed receipts use relative paths
/// and pinned content digests, so copying/restoring a model preserves reuse.
pub const modelIdentity = loaded_runtime.modelIdentity;

/// Include optional tokenizer sidecars in the generation marker even if absent.
/// Their appearance during/after loading must not authenticate different bytes.
const NativeKvFiles = loaded_runtime.NativeKvFiles;

pub const RuntimeId = loaded_runtime.RuntimeId;

pub const RuntimeResidencyMode = loaded_runtime.RuntimeResidencyMode;

pub const RuntimeKind = enum {
    primary,
    composite,
};

pub const RuntimeState = enum {
    loading,
    loaded,
    in_use,
};

pub const RuntimeSnapshotEntry = struct {
    managed_publication: ?[32]u8 = null,
    runtime_id: RuntimeId,
    unloadable: bool = false,
    model_dir: []u8,
    state: RuntimeState,
    runtime_kind: RuntimeKind,
    model_type: ?manifest_mod.ModelType,
    task_mask: u16,
    active_handles: usize,
    startup_managed: bool,
    backend: ?backends.BackendType,
    residency_mode: RuntimeResidencyMode,
    last_used_ms: u64,
};

pub const RuntimeManifestTaskMaskFn = *const fn (*const manifest_mod.ModelManifest) u16;

pub const RuntimeSnapshot = struct {
    allocator: std.mem.Allocator,
    entries: []RuntimeSnapshotEntry,

    pub fn deinit(self: *RuntimeSnapshot) void {
        for (self.entries) |entry| self.allocator.free(entry.model_dir);
        self.allocator.free(self.entries);
        self.* = undefined;
    }
};

pub const UnloadRuntimeResult = union(enum) {
    unloaded,
    already_unloaded,
    loading,
    in_use: usize,
    startup_managed,
};

pub fn formatRuntimeId(buffer: *[32]u8, runtime_id: RuntimeId) []const u8 {
    return std.fmt.bufPrint(buffer, "{x:0>32}", .{runtime_id}) catch unreachable;
}

pub fn parseRuntimeId(value: []const u8) !RuntimeId {
    if (value.len != 32) return error.InvalidRuntimeId;
    return std.fmt.parseInt(RuntimeId, value, 16) catch error.InvalidRuntimeId;
}

const ChunkPlanningSnapshot = loaded_runtime.ChunkPlanningSnapshot;
const chunkPlanningPaths = loaded_runtime.chunkPlanningPaths;
fn chunkPlanningSnapshot(a: std.mem.Allocator, io: std.Io, manifest: manifest_mod.ModelManifest, fallback: [32]u8) !ChunkPlanningSnapshot {
    return chunkPlanningSnapshotWithControl(a, io, manifest, fallback, .{});
}

const chunkPlanningSnapshotWithControl = loaded_runtime.chunkPlanningSnapshotWithControl;

/// Tokenizer/processor-only ownership for planning. Never constructs a backend
/// session or loads model tensors. Immutable identity checks still apply.
pub const ChunkPlanningAssets = loaded_runtime.ChunkPlanningAssets;

pub const LoadedModel = loaded_runtime.Runtime;

const targetInferenceExecutionMutexForBackend = loaded_runtime.targetInferenceExecutionMutexForBackend;

test "embedding asset gate admits concurrent readers and excludes writers" {
    var gate = EmbeddingAssetGate{};

    gate.lockShared();
    try std.testing.expect(gate.tryLockShared());
    gate.unlockShared();
    try std.testing.expect(!gate.tryLockExclusive());
    gate.unlockShared();

    try std.testing.expect(gate.tryLockExclusive());
    try std.testing.expect(!gate.tryLockShared());
    gate.unlockExclusive();

    try std.testing.expect(gate.tryLockShared());
    gate.unlockShared();

    gate.lockShared();
    var lease = EmbeddingAssetLease{ .gate = &gate, .access = .shared };
    lease.release();
    lease.release();
    try std.testing.expect(gate.tryLockExclusive());
    gate.unlockExclusive();
}

test "embedding asset lease downgrades exclusive access without an ownership gap" {
    var gate = EmbeddingAssetGate{};
    gate.lockExclusive();
    var lease = EmbeddingAssetLease{ .gate = &gate, .access = .exclusive };

    lease.downgradeExclusiveToShared();
    try std.testing.expect(lease.held);
    try std.testing.expect(lease.access == .shared);
    try std.testing.expect(gate.tryLockShared());
    gate.unlockShared();
    try std.testing.expect(!gate.tryLockExclusive());

    lease.release();
    try std.testing.expect(gate.tryLockExclusive());
    gate.unlockExclusive();
}

test "embedding asset gate blocks late readers behind a queued writer" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding)
        return error.SkipZigTest;

    const Writer = struct {
        gate: *EmbeddingAssetGate,
        acquired: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.gate.lockExclusive();
            self.acquired.store(true, .release);
            while (!self.release.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
            self.gate.unlockExclusive();
        }
    };

    var gate = EmbeddingAssetGate{};
    gate.lockShared();
    var shared_held = true;
    defer if (shared_held) gate.unlockShared();

    var writer = Writer{ .gate = &gate };
    var writer_thread = try std.testing.io.concurrent(Writer.run, .{&writer});
    var writer_joined = false;
    defer if (!writer_joined) {
        if (shared_held) {
            gate.unlockShared();
            shared_held = false;
        }
        writer.release.store(true, .release);
        writer_thread.await(std.testing.io);
    };

    var writer_queued = false;
    for (0..200_000) |_| {
        if (!gate.tryLockShared()) {
            writer_queued = true;
            break;
        }
        gate.unlockShared();
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    if (!writer_queued) return error.TestTimeout;

    // Once the writer owns the reader gate, a newly arriving reader cannot
    // join the active reader cohort and bypass it.
    try std.testing.expect(!gate.tryLockShared());
    gate.unlockShared();
    shared_held = false;

    var writer_acquired = false;
    for (0..200_000) |_| {
        if (writer.acquired.load(.acquire)) {
            writer_acquired = true;
            break;
        }
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    if (!writer_acquired) return error.TestTimeout;
    try std.testing.expect(!gate.tryLockShared());

    writer.release.store(true, .release);
    writer_thread.await(std.testing.io);
    writer_joined = true;

    try std.testing.expect(gate.tryLockShared());
    gate.unlockShared();
}

test "embedding asset rollback closes only newly acquired primary sessions" {
    const CloseProbe = struct {
        close_count: usize = 0,

        fn run(_: *anyopaque, _: []const backends.Tensor, allocator: std.mem.Allocator) ![]backends.Tensor {
            return allocator.alloc(backends.Tensor, 0);
        }
        fn runWithControl(ptr: *anyopaque, inputs: []const backends.Tensor, alloc: std.mem.Allocator, control: InferenceExecutionControl) ![]backends.Tensor {
            try control.check();
            return run(ptr, inputs, alloc);
        }
        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.close_count += 1;
        }
        fn session(self: *@This()) backends.Session {
            return .{ .ptr = self, .vtable = &vtable };
        }

        const vtable = backends.Session.VTable{
            .run = run,
            .runWithControl = runWithControl,
            .inputInfo = inputInfo,
            .outputInfo = outputInfo,
            .backend = backend,
            .close = close,
        };
    };

    var text_probe = CloseProbe{};
    var existing_vision_probe = CloseProbe{};
    var visual_probe = CloseProbe{};
    var model: LoadedModel = undefined;
    model.managed_publication = null;
    model.text_projection = text_probe.session();
    model.vision_session = existing_vision_probe.session();
    model.visual_projection = visual_probe.session();
    model.text_projection_resource_lease = null;
    model.vision_resource_lease = null;
    model.visual_projection_resource_lease = null;

    model.rollbackPrimaryEmbeddingAssetsLocked(.{
        .text_projection = true,
        .vision = false,
        .visual_projection = true,
    });

    try std.testing.expectEqual(@as(usize, 1), text_probe.close_count);
    try std.testing.expectEqual(@as(usize, 0), existing_vision_probe.close_count);
    try std.testing.expectEqual(@as(usize, 1), visual_probe.close_count);
    try std.testing.expect(model.text_projection == null);
    try std.testing.expect(model.vision_session != null);
    try std.testing.expect(model.visual_projection == null);
}

test "audio asset rollback closes every ephemeral session" {
    const CloseProbe = struct {
        close_count: usize = 0,

        fn run(_: *anyopaque, _: []const backends.Tensor, allocator: std.mem.Allocator) ![]backends.Tensor {
            return allocator.alloc(backends.Tensor, 0);
        }
        fn runWithControl(ptr: *anyopaque, inputs: []const backends.Tensor, allocator: std.mem.Allocator, control: InferenceExecutionControl) ![]backends.Tensor {
            try control.check();
            return run(ptr, inputs, allocator);
        }
        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.close_count += 1;
        }
        fn session(self: *@This()) backends.Session {
            return .{ .ptr = self, .vtable = &vtable };
        }

        const vtable = backends.Session.VTable{
            .run = run,
            .runWithControl = runWithControl,
            .inputInfo = inputInfo,
            .outputInfo = outputInfo,
            .backend = backend,
            .close = close,
        };
    };

    var audio_probe = CloseProbe{};
    var projection_probe = CloseProbe{};
    var model: LoadedModel = undefined;
    model.managed_publication = null;
    model.audio_session = audio_probe.session();
    model.audio_projection = projection_probe.session();
    model.audio_resource_lease = null;
    model.audio_projection_resource_lease = null;

    model.releaseAudioEmbeddingAssetsLocked();

    try std.testing.expectEqual(@as(usize, 1), audio_probe.close_count);
    try std.testing.expectEqual(@as(usize, 1), projection_probe.close_count);
    try std.testing.expect(model.audio_session == null);
    try std.testing.expect(model.audio_projection == null);
}

test "target inference gates are GPU-only and model-local" {
    var first: std.atomic.Mutex = .unlocked;
    var second: std.atomic.Mutex = .unlocked;

    try std.testing.expectEqual(&first, targetInferenceExecutionMutexForBackend(.metal, &first).?);
    try std.testing.expectEqual(&second, targetInferenceExecutionMutexForBackend(.metal, &second).?);
    try std.testing.expectEqual(&first, targetInferenceExecutionMutexForBackend(.cuda, &first).?);
    try std.testing.expect(targetInferenceExecutionMutexForBackend(.native, &first) == null);
    try std.testing.expect(targetInferenceExecutionMutexForBackend(.wasm, &first) == null);
}

const usesClipImagePreprocessProfile = loaded_runtime.usesClipImagePreprocessProfile;

const LoadFlight = struct {
    managed_publication: ?[32]u8 = null,
    runtime_id: RuntimeId = 0,
    model_dir: ?[]u8 = null,
    completed: std.Io.Event = .unset,
    io: std.Io,
    hard_cancellation: ?execution_control_mod.HardCancellationBoundary = null,
    native_call_policy: execution_control_mod.NativeCallPolicy = .require_supervision,
    model: ?*LoadedModel = null,
    err: ?anyerror = null,
    /// Protected by ModelManager.load_lock. The manager-owned task holds one
    /// reference and every request waiter holds another.
    refs: usize = 2,
    /// Active request waiters. The terminal `abandoned` state prevents a new
    /// request from joining initialization after the final waiter has left.
    waiters: std.atomic.Value(usize) = .init(1),
    /// Distinguishes the manager task reference so retirement reserves handles
    /// only for request waiters that have not adopted theirs yet.
    task_ref_pending: bool = true,
    /// A retired model detaches its completed flight so a replacement can use
    /// the same key. Detached flights live until existing waiters consume them.
    registered: bool = true,
    /// Retirement reserves one active model handle for every remaining flight
    /// waiter. Waiters adopt those reservations instead of incrementing the
    /// handle count a second time.
    handles_reserved: bool = false,

    const abandoned = std.math.maxInt(usize);

    fn tryAddWaiter(self: *LoadFlight) bool {
        var current = self.waiters.load(.acquire);
        while (current != abandoned) {
            std.debug.assert(current < abandoned - 1);
            if (self.waiters.cmpxchgWeak(current, current + 1, .acq_rel, .acquire)) |observed| {
                current = observed;
            } else return true;
        }
        return false;
    }

    fn releaseWaiter(self: *LoadFlight, abandon_if_last: bool) void {
        var current = self.waiters.load(.acquire);
        while (true) {
            std.debug.assert(current != abandoned and current > 0);
            const next = if (abandon_if_last and current == 1) abandoned else current - 1;
            if (self.waiters.cmpxchgWeak(current, next, .acq_rel, .acquire)) |observed| {
                current = observed;
            } else return;
        }
    }

    /// Observational count, excluding the terminal cancellation sentinel.
    /// Read once: composite waiters can depart without taking load_lock.
    fn activeWaiterCount(self: *const LoadFlight) usize {
        const count = self.waiters.load(.acquire);
        return if (count == abandoned) 0 else count;
    }

    fn unadoptedWaiterRefs(self: *const LoadFlight) usize {
        std.debug.assert(self.refs >= @intFromBool(self.task_ref_pending));
        return self.refs - @intFromBool(self.task_ref_pending);
    }
};

const LoadFlightControl = struct {
    flight: *LoadFlight,

    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.flight.waiters.load(.acquire) == LoadFlight.abandoned)
            return error.Cancelled;
    }

    fn control(self: *@This()) InferenceExecutionControl {
        return .{
            .io = self.flight.io,
            .ptr = self,
            .check_fn = check,
            .hard_cancellation = self.flight.hard_cancellation,
            .native_call_policy = self.flight.native_call_policy,
        };
    }
};

const LoadTask = struct {
    managed_publication: ?[32]u8 = null,
    manager: *ModelManager,
    flight: *LoadFlight,
    flight_key: []u8,
    model_dir: []u8,
    preferred_backends: []backends.BackendType,
    session_manager: backends.SessionManager,
    cache_default_alias: bool,
    a4b_request: ?backend_contracts.A4bInferenceRequest,

    pub fn deinit(self: *@This()) void {
        const allocator = self.manager.allocator;
        allocator.free(self.flight_key);
        allocator.free(self.model_dir);
        allocator.free(self.preferred_backends);
        allocator.destroy(self);
    }
};

test "manager load flight remains active until its final waiter leaves" {
    var flight = LoadFlight{ .io = std.testing.io };
    var load_control = LoadFlightControl{ .flight = &flight };
    try load_control.control().check();
    try std.testing.expect(flight.tryAddWaiter());
    flight.releaseWaiter(true);
    try load_control.control().check();
    flight.releaseWaiter(true);
    try std.testing.expectError(error.Cancelled, load_control.control().check());
    try std.testing.expect(!flight.tryAddWaiter());
}

test "runtime snapshots normalize cancelled primary and composite load waiters" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();
    const primary = try allocator.create(LoadFlight);
    primary.* = .{
        .runtime_id = 1,
        .model_dir = try allocator.dupe(u8, "/models/owner/primary"),
        .io = std.testing.io,
    };
    try manager.in_flight_loads.put(allocator, try allocator.dupe(u8, "primary"), primary);
    defer manager.releaseLoadFlight("primary", primary);
    const composite = try allocator.create(CompositeAssetsLoadFlight);
    composite.* = .{
        .runtime_id = 2,
        .model_dir = try allocator.dupe(u8, "/models/owner/composite"),
        .io = std.testing.io,
        .refs = 2,
        .load_state = .{ .io = std.testing.io },
    };
    const composite_key: ComponentPlanKey = @splat(0);
    try manager.in_flight_composite_assets.put(allocator, composite_key, composite);
    defer manager.releaseCompositeAssetsLoadFlight(composite_key, composite);
    const Cancelled = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    const control = InferenceExecutionControl{ .check_fn = Cancelled.check };

    // Two callers share each manager-owned load. Cancelling one must leave
    // the other counted and keep the shared task joinable.
    try std.testing.expect(primary.tryAddWaiter());
    primary.refs += 1;
    try std.testing.expect(composite.load_state.tryAddWaiter());
    composite.refs += 1;
    try std.testing.expectError(error.Cancelled, manager.waitForLoadFlight("primary", primary, control));
    try std.testing.expectError(error.Cancelled, manager.waitForCompositeAssetsLoadFlight(composite_key, composite, control));
    {
        var snapshot = try manager.runtimeSnapshot(allocator, null);
        defer snapshot.deinit();
        try std.testing.expectEqual(@as(usize, 2), snapshot.entries.len);
        for (snapshot.entries) |entry| try std.testing.expectEqual(@as(usize, 1), entry.active_handles);
    }

    // The final callers leave while their load tasks still own the flights.
    // Poll repeatedly through that real cancellation window, including the
    // signed count conversion used by the model-instance API.
    try std.testing.expectError(error.Cancelled, manager.waitForLoadFlight("primary", primary, control));
    try std.testing.expectError(error.Cancelled, manager.waitForCompositeAssetsLoadFlight(composite_key, composite, control));
    for (0..3) |_| {
        var snapshot = try manager.runtimeSnapshot(allocator, null);
        defer snapshot.deinit();
        try std.testing.expectEqual(@as(usize, 2), snapshot.entries.len);
        for (snapshot.entries) |entry| {
            const api_active_handles: i64 = @intCast(entry.active_handles);
            try std.testing.expectEqual(@as(i64, 0), api_active_handles);
            try std.testing.expectEqual(RuntimeState.loading, entry.state);
            try std.testing.expect(!entry.unloadable);
            try std.testing.expectEqual(UnloadRuntimeResult.loading, manager.unloadRuntime(entry.runtime_id));
        }
    }
    try std.testing.expect(!primary.tryAddWaiter());
    try std.testing.expect(!composite.load_state.tryAddWaiter());
    // Observation must not change cancellation or task ownership.
    try std.testing.expectEqual(@as(usize, 1), primary.refs);
    try std.testing.expectEqual(@as(usize, 1), composite.refs);
    var primary_control = LoadFlightControl{ .flight = primary };
    var composite_control = LoadFlightControl{ .flight = &composite.load_state };
    try std.testing.expectError(error.Cancelled, primary_control.control().check());
    try std.testing.expectError(error.Cancelled, composite_control.control().check());
    manager.finishLoadFlight(primary, null, error.Cancelled);
    manager.finishCompositeAssetsLoadFlight(composite, null, error.Cancelled);
    var completed = try manager.runtimeSnapshot(allocator, null);
    defer completed.deinit();
    try std.testing.expectEqual(@as(usize, 0), completed.entries.len);
}

test "runtime identifiers round trip and exact loading unload is rejected" {
    const allocator = std.testing.allocator;
    const runtime_id: RuntimeId = (@as(RuntimeId, 0x1234) << 64) | 0x5678;
    var buffer: [32]u8 = undefined;
    const encoded = formatRuntimeId(&buffer, runtime_id);
    try std.testing.expectEqual(@as(usize, 32), encoded.len);
    try std.testing.expectEqual(runtime_id, try parseRuntimeId(encoded));
    try std.testing.expectError(error.InvalidRuntimeId, parseRuntimeId("1234"));

    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        manager.in_flight_loads.deinit(allocator);
        manager.loaded.deinit(allocator);
        manager.loaded_aliases.deinit(allocator);
    }
    const flight = try allocator.create(LoadFlight);
    flight.* = .{
        .runtime_id = runtime_id,
        .model_dir = try allocator.dupe(u8, "/models/generators/example"),
        .io = std.testing.io,
    };
    const key = try allocator.dupe(u8, "flight");
    try manager.in_flight_loads.put(allocator, key, flight);

    try std.testing.expectEqual(UnloadRuntimeResult.loading, manager.unloadRuntime(runtime_id));
    try std.testing.expectEqual(UnloadRuntimeResult.already_unloaded, manager.unloadRuntime(runtime_id + 1));

    const removed = manager.in_flight_loads.fetchRemove("flight").?;
    allocator.free(removed.key);
    manager.destroyLoadFlight(flight);
}

test "load flight retirement reservations exclude the manager task" {
    var flight = LoadFlight{ .io = std.testing.io, .refs = 3 };
    try std.testing.expectEqual(@as(usize, 2), flight.unadoptedWaiterRefs());
    flight.task_ref_pending = false;
    try std.testing.expectEqual(@as(usize, 3), flight.unadoptedWaiterRefs());
}

const CompositeAssetsLoadFlight = struct {
    managed_publication: ?[32]u8 = null,
    runtime_id: RuntimeId = 0,
    model_dir: ?[]u8 = null,
    kind: CompositeKind = .whisper_metadata,
    model_type: manifest_mod.ModelType = .transcriber,
    completed: std.Io.Event = .unset,
    io: std.Io,
    assets: ?*CompositeAssets = null,
    err: ?anyerror = null,
    /// Protected by ModelManager.load_lock. The owner starts with one reference;
    /// every waiter takes one before dropping the manager lock.
    refs: usize = 1,
    registered: bool = true,
    // Reuse the model-loader waiter/cancellation state machine. The component
    // task is manager-owned; no individual request controls a shared cold load.
    load_state: LoadFlight,
};

const CompositeLoadTask = struct {
    manager: *ModelManager,
    flight: *CompositeAssetsLoadFlight,
    key: ComponentPlanKey,
    model_dir: []u8,
    paths: [][]const u8,
    preferred_backends: []backends.BackendType,
    kind: CompositeKind,

    fn create(manager: *ModelManager, flight: *CompositeAssetsLoadFlight, key: ComponentPlanKey, model_dir: []const u8, paths: []const []const u8, preferred_backends: []const backends.BackendType, kind: CompositeKind) !*@This() {
        const allocator = manager.allocator;
        const task = try allocator.create(@This());
        errdefer allocator.destroy(task);
        const owned_dir = try allocator.dupe(u8, model_dir);
        errdefer allocator.free(owned_dir);
        const owned_paths = try allocator.alloc([]const u8, paths.len);
        var count: usize = 0;
        errdefer {
            for (owned_paths[0..count]) |path| allocator.free(path);
            allocator.free(owned_paths);
        }
        for (paths, owned_paths) |path, *owned| {
            owned.* = try allocator.dupe(u8, path);
            count += 1;
        }
        const owned_backends = try allocator.dupe(backends.BackendType, preferred_backends);
        errdefer allocator.free(owned_backends);
        task.* = .{ .manager = manager, .flight = flight, .key = key, .model_dir = owned_dir, .paths = owned_paths, .preferred_backends = owned_backends, .kind = kind };
        return task;
    }

    pub fn deinit(self: *@This()) void {
        const allocator = self.manager.allocator;
        for (self.paths) |path| allocator.free(path);
        allocator.free(self.paths);
        allocator.free(self.preferred_backends);
        allocator.free(self.model_dir);
        allocator.destroy(self);
    }
};

const admissionBackendClassForRuntime = load_plan.admissionBackendClassForRuntime;

const attachSessionRunAdmission = load_plan.attachSessionRunAdmission;

const sessionRunAdmissionModelProfile = load_plan.sessionRunAdmissionModelProfile;

test "Qwen embedding admission preserves reranker capacity and gates dense workspace" {
    const Session = @import("../backends/session.zig").Session;
    const Probe = struct {
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "logits", .dtype = .f32, .shape = &.{ -1, 1 } }};
        }
    };
    var cfg = @import("../architectures/gpt.zig").Config{
        .family = .qwen3,
        .norm_type = .rms_norm,
        .position_encoding = .rope,
        .activation = .silu,
        .hidden_size = 1024,
        .intermediate_size = 3072,
        .num_hidden_layers = 28,
        .num_attention_heads = 16,
        .num_key_value_heads = 8,
        .attention_head_dim = 128,
    };
    var manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .reranker,
        .hidden_size = 1024,
        .intermediate_size = 3072,
        .num_attention_heads = 16,
        .pooling = .last,
        // Even a legacy last-pooling/document-prefix reranker must not qualify.
        .embedding_profile = .{ .document = .{ .prefix = "Document: " } },
    };
    var controller = runtime.tier.memory.AdmissionController{};
    var session = Session{
        .ptr = &controller,
        .vtable = &.{ .run = undefined, .inputInfo = undefined, .outputInfo = Probe.outputInfo, .backend = undefined, .close = undefined },
        .run_admission = .{
            .controller = &controller,
            .backend_class = .gpu,
            .limits = .{ .scratch_limit_bytes = 384 * runtime.tier.memory.bytes_per_mib },
            .static_workspace_bytes = 80 * runtime.tier.memory.bytes_per_mib,
            .check_live_memory = false,
            .model_profile = sessionRunAdmissionModelProfile(.{ .backend = .metal }, &manifest, cfg),
        },
    };
    try std.testing.expect(!session.run_admission.?.model_profile.qwen_dense_workspace);
    var permit = try session.admit(.{ .batch = 4, .sequence = 256, .input_bytes = 4 * 256 * 24 });
    defer permit.deinit();
    permit.deinit();
    try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, controller.snapshot());

    manifest.model_type = .generator;
    try std.testing.expect(!sessionRunAdmissionModelProfile(.{ .backend = .metal }, &manifest, cfg).qwen_dense_workspace);
    manifest.model_type = .embedder;
    manifest.embedding_style = .qwen3_embedding;
    const embedding_profile = sessionRunAdmissionModelProfile(.{ .backend = .metal }, &manifest, cfg);
    try std.testing.expect(embedding_profile.qwen_dense_workspace);
    try std.testing.expectEqual(@as(usize, 2048), embedding_profile.query_width);
    try std.testing.expectEqual(@as(usize, 1024), embedding_profile.kv_width);
    session.run_admission.?.model_profile = embedding_profile;
    try std.testing.expectError(error.ResourceLimitExceeded, session.admit(.{ .batch = 4, .sequence = 256, .input_bytes = 4 * 256 * 24 }));
    try std.testing.expect(!sessionRunAdmissionModelProfile(.{ .backend = .native }, &manifest, cfg).qwen_dense_workspace);
    try std.testing.expect(!sessionRunAdmissionModelProfile(.{ .backend = .metal }, &manifest, null).qwen_dense_workspace);
    try std.testing.expect(!sessionRunAdmissionModelProfile(.{ .backend = .metal }, null, cfg).qwen_dense_workspace);
    cfg.family = .qwen3_vl;
    try std.testing.expect(!sessionRunAdmissionModelProfile(.{ .backend = .metal }, &manifest, cfg).qwen_dense_workspace);
}

pub const ModelHandle = struct {
    manager: *ModelManager,
    model: ?*LoadedModel,

    pub fn get(self: *const ModelHandle) *LoadedModel {
        return self.model orelse unreachable;
    }

    /// Create another lifetime pin for the exact same published generation.
    /// This is used when admission metadata must remain borrowed for the whole
    /// request while execution independently transfers a handle into recovery.
    pub fn duplicate(self: *const ModelHandle) ModelHandle {
        const model = self.model orelse unreachable;
        self.manager.lockLoadedModels();
        std.debug.assert(model.active_handles > 0);
        model.active_handles += 1;
        self.manager.unlockLoadedModels();
        return .{ .manager = self.manager, .model = model };
    }

    pub fn pin(self: *ModelHandle) void {
        const model = self.model orelse return;
        self.manager.lockLoadedModels();
        model.pinned = true;
        self.manager.unlockLoadedModels();
    }

    pub fn release(self: *ModelHandle) void {
        self.releaseWithUsage(true);
    }

    // Observation pins share exactly the same final-retired-owner cleanup,
    // but are not inference usage and must not renew idle cache residency.
    fn releaseWithUsage(self: *ModelHandle, used: bool) void {
        const model = self.model orelse return;
        var destroy_retired = false;
        self.manager.lockLoadedModels();
        std.debug.assert(model.active_handles > 0);
        model.active_handles -= 1;
        if (model.retired) {
            destroy_retired = model.active_handles == 0;
        } else if (used) {
            model.last_used_ns = platform.time.monotonicNs();
        }
        self.manager.unlockLoadedModels();
        self.model = null;
        if (destroy_retired) self.manager.destroyRetiredModel(model);
    }

    /// Stop publishing this cached runtime and release this handle. A fresh
    /// request can reload the same artifact without waiting for process
    /// restart; concurrent users retain the retired object until they unwind.
    pub fn retire(self: *ModelHandle) void {
        const model = self.model orelse return;
        self.manager.retireLoadedModel(model);
        self.release();
    }

    /// Quarantine the exact model/backend pair before retiring its in-process
    /// runtime. A later durable retry can select the next healthy backend
    /// without penalizing unrelated models that use the same backend.
    pub fn quarantine(self: *ModelHandle) void {
        const model = self.model orelse return;
        markModelBackendUnhealthy(self.manager, model.model_dir, model.session.backend());
        self.manager.retireLoadedModel(model);
        self.release();
    }
};

pub const LoadedModelSnapshot = struct {
    allocator: std.mem.Allocator,
    /// Borrowed observation pins; release them only through this owner.
    handles: []ModelHandle,

    pub fn deinit(self: *LoadedModelSnapshot) void {
        for (self.handles) |*handle| handle.releaseWithUsage(false);
        self.allocator.free(self.handles);
        self.handles = &.{};
    }
};

pub const CompositeKind = @import("../models/composite_runtime.zig").CompositeKind;
pub const CompositeAssets = @import("../models/composite_runtime.zig").CompositeAssets;

pub const CompositeAssetsHandle = struct {
    manager: *ModelManager,
    assets: ?*CompositeAssets,

    pub fn get(self: *const CompositeAssetsHandle) *const CompositeAssets {
        return self.assets orelse unreachable;
    }

    pub fn markStartupManaged(self: *CompositeAssetsHandle) !void {
        try self.applyStartupPolicy(true);
    }

    pub fn applyStartupPolicy(self: *CompositeAssetsHandle, pin: bool) !void {
        const assets = self.assets orelse return;
        self.manager.lockLoadedModels();
        defer self.manager.unlockLoadedModels();
        // Startup policy selects the default request backend for this artifact.
        // Keep explicit backend acquisitions separate; never alias cache entries
        // with different placement constraints or artifact generations.
        if (assets.preferred_backends.len == 1)
            try self.manager.setStartupLoadPolicyLocked(assets.model_dir, .{ .backend = assets.preferred_backends[0] });
        // Explicit startup backend selection also applies to evictable loads.
        // Cache identity must not depend on whether the model is pinned.
        if (pin) assets.startup_managed = true;
    }

    pub fn release(self: *CompositeAssetsHandle) void {
        const assets = self.assets orelse return;
        self.manager.lockLoadedModels();
        std.debug.assert(assets.active_handles > 0);
        assets.active_handles -= 1;
        assets.last_used_ns = platform.time.monotonicNs();
        self.manager.unlockLoadedModels();
        self.assets = null;
    }
};

/// Declares who owns the process-wide physical memory budget. Direct inference
/// uses its local admission controller. Embedded inference must attach the
/// node ResourceManager bridge before any model can be loaded or served.
pub const ResourceOwnership = enum {
    local,
    external_required,
};

/// Offline sessions may retain this runtime after a raw Session ownership
/// transfer. The manager and teardown domain therefore hold independent refs.
const OwnedManagerIo = struct {
    allocator: std.mem.Allocator,
    runtime: std.Io.Threaded,
    refs: std.atomic.Value(usize) = .init(1),

    fn create(allocator: std.mem.Allocator) !*OwnedManagerIo {
        const self = try allocator.create(OwnedManagerIo);
        self.* = .{ .allocator = allocator, .runtime = std.Io.Threaded.init(allocator, .{}) };
        return self;
    }

    fn retain(self: *OwnedManagerIo) void {
        const old = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(old > 0 and old < std.math.maxInt(usize));
    }

    fn release(self: *OwnedManagerIo) void {
        const old = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(old > 0);
        if (old != 1) return;
        self.runtime.deinit();
        self.allocator.destroy(self);
    }
};

/// Separate from request/load watchdogs: dormant lifetime entries must not
/// obscure their idle counts. Its private monitor runtime never borrows Node
/// or request state. A domain survives its manager until its final ticket.
const TeardownDomain = struct {
    allocator: std.mem.Allocator,
    monitor_io: *OwnedManagerIo,
    driver_io: ?*OwnedManagerIo,
    watchdog: *HardCancellationWatchdog,
    refs: std.atomic.Value(usize) = .init(1),

    fn create(allocator: std.mem.Allocator, driver_io: ?*OwnedManagerIo) !*TeardownDomain {
        const self = try allocator.create(TeardownDomain);
        errdefer allocator.destroy(self);
        const monitor_io = try OwnedManagerIo.create(allocator);
        errdefer monitor_io.release();
        const watchdog = try HardCancellationWatchdog.create(allocator);
        errdefer watchdog.destroy();
        try watchdog.start(monitor_io.runtime.io());
        if (driver_io) |owner| owner.retain();
        self.* = .{
            .allocator = allocator,
            .monitor_io = monitor_io,
            .driver_io = driver_io,
            .watchdog = watchdog,
        };
        return self;
    }

    fn retain(self: *TeardownDomain) void {
        const old = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(old > 0 and old < std.math.maxInt(usize));
    }

    fn release(self: *TeardownDomain) void {
        const old = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(old > 0);
        if (old != 1) return;
        // Final ticket disarms synchronously before it releases this ref.
        self.watchdog.destroy();
        self.monitor_io.release();
        if (self.driver_io) |owner| owner.release();
        self.allocator.destroy(self);
    }
};

const TeardownTicket = struct {
    const timeout_ns: u64 = 30 * std.time.ns_per_s;

    domain: *TeardownDomain,
    token: u64 = 0,
    refs: std.atomic.Value(usize) = .init(1),
    deadline_ns: std.atomic.Value(u64) = .init(0),
    close_timeout_ns: u64 = timeout_ns,
    scope_mutex: std.atomic.Mutex = .unlocked,
    active_scopes: usize = 0,

    fn create(domain: *TeardownDomain) !backends.Session.CloseProtection {
        const self = try domain.allocator.create(TeardownTicket);
        errdefer domain.allocator.destroy(self);
        self.* = .{ .domain = domain };
        // All fallible work occurs before backend entry. This callback borrows
        // only the stable ticket, never a caller, manager or request control.
        const boundary = domain.watchdog.boundary();
        self.token = try boundary.arm_fn(boundary.ptr, .{ .ptr = self, .check_fn = check });
        domain.retain();
        return .{ .ptr = self, .begin_fn = begin, .release_fn = release };
    }

    fn check(raw: ?*anyopaque) !void {
        const self: *TeardownTicket = @ptrCast(@alignCast(raw.?));
        spinLock(&self.scope_mutex);
        defer self.scope_mutex.unlock();
        try self.checkDeadline();
    }

    fn checkDeadline(self: *TeardownTicket) !void {
        const deadline = self.deadline_ns.load(.acquire);
        if (deadline != 0 and platform.time.monotonicNs() >= deadline)
            return error.Timeout;
    }

    fn begin(raw: *anyopaque) backends.Session.CloseScope {
        const self: *TeardownTicket = @ptrCast(@alignCast(raw));
        const old = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(old > 0 and old < std.math.maxInt(usize));
        spinLock(&self.scope_mutex);
        defer self.scope_mutex.unlock();
        // Nested active closes keep the first deadline. A retained but idle
        // session does not spend its future physical-cleanup timeout.
        if (self.active_scopes == 0) {
            const deadline = platform.time.monotonicNs() +| self.close_timeout_ns;
            self.deadline_ns.store(@max(deadline, 1), .release);
        }
        std.debug.assert(self.active_scopes < std.math.maxInt(usize));
        self.active_scopes += 1;
        return .{ .ptr = self, .release_fn = releaseScope };
    }

    fn releaseScope(raw: *anyopaque) void {
        const self: *TeardownTicket = @ptrCast(@alignCast(raw));
        spinLock(&self.scope_mutex);
        std.debug.assert(self.active_scopes > 0);
        // Enforce an over-deadline close even if the watcher has not polled it.
        self.checkDeadline() catch platform.inference_process_supervisor.restartWorker();
        self.active_scopes -= 1;
        if (self.active_scopes == 0) self.deadline_ns.store(0, .release);
        self.scope_mutex.unlock();
        release(raw);
    }

    fn release(raw: *anyopaque) void {
        const self: *TeardownTicket = @ptrCast(@alignCast(raw));
        const old = self.refs.fetchSub(1, .acq_rel);
        std.debug.assert(old > 0);
        if (old != 1) return;
        const domain = self.domain;
        // The watcher may be between polls when close returns. A completed
        // over-deadline close still cannot report successful cleanup.
        check(self) catch {
            // Fatal progress cannot depend on stderr locks or pipe consumers.
            // The supervisor observes exit86; this thread must not log first.
            platform.inference_process_supervisor.restartWorker();
        };
        const boundary = domain.watchdog.boundary();
        boundary.disarm_fn(boundary.ptr, self.token);
        domain.allocator.destroy(self);
        domain.release();
    }
};
/// A caller may keep its lightweight weight preflight or delegate to the
/// manager's exact construction plan and retained residency/workspace lease.
pub const ModelLoadAdmission = session_loader.ModelLoadAdmission;

pub const ModelLoadPreflight = session_loader.ModelLoadPreflight;
/// Borrowed invocation policy, also used by artifact-created host managers.
/// Its shared owner and preflight context must outlive configured managers.
pub const ModelLoadPolicy = struct {
    limits: runtime.tier.memory.Limits = .{},
    resources: ?*ModelManager.SharedResources = null,
    preflight: ?ModelLoadPreflight = null,

    pub fn configure(self: @This(), manager: *ModelManager) !void {
        manager.configureAdmissionLimits(self.limits);
        if (self.resources) |resources| try resources.attach(manager);
        manager.model_load_preflight = self.preflight;
        // Admission is an execution policy, independent of serving/model
        // compatibility policy. Include tokenizer construction and metadata,
        // not just weights, in the invocation's retained resource domain.
        manager.admission_enabled = true;
        (try manager.ensureResourceDomain()).admission.tightenInvocationQuotas(self.limits);
        if (self.limits.combined_limit_bytes != 0)
            (try manager.ensureResourceDomain()).admission.tightenUnifiedLimit(self.limits.combined_limit_bytes);
    }
};

pub const ModelManager = struct {
    /// One physical ledger for an invocation's target, draft and artifact
    /// managers. Managers retain the stable domain; cache/lease lifetimes can
    /// escape a manager without borrowing this handle's address.
    pub const SharedResources = struct {
        domain: *ResourceDomain,
        limits: runtime.tier.memory.Limits,

        pub fn init(allocator: std.mem.Allocator, limits: runtime.tier.memory.Limits) !@This() {
            const domain = try ResourceDomain.create(allocator);
            domain.tokenizer_cache_budget_source = .local_manager;
            var policy = ModelManager.init(allocator, backends.SessionManager.init(allocator));
            policy.admission_limit_overrides = limits;
            policy.configureAdmissionController(domain);
            domain.admission.tightenInvocationQuotas(limits);
            // An invocation cap spans CPU tokenizers, GPU sessions and host
            // staging even on discrete-memory systems. Physical topology only
            // determines the automatic limit, never an explicit operator cap.
            if (limits.combined_limit_bytes != 0)
                domain.admission.tightenUnifiedLimit(limits.combined_limit_bytes);
            return .{ .domain = domain, .limits = limits };
        }

        pub fn attach(self: *@This(), manager: *ModelManager) !void {
            if (manager.resource_domain != null or manager.tokenizer_cache_budget_source != .none or
                manager.resource_ownership != .local or manager.loaded.count() != 0 or
                manager.composite_assets.count() != 0)
                return error.ResourceOwnerAlreadyConfigured;
            if (!std.meta.eql(manager.admission_limit_overrides, self.limits) or manager.process_memory_limit_bytes != 0)
                return error.SharedResourcePolicyMismatch;
            if (!self.domain.retain()) return error.ResourceOwnerShuttingDown;
            manager.resource_domain = self.domain;
            manager.owns_resource_domain = false;
            manager.tokenizer_cache_config.resource_budget = self.domain.resourceBudget();
            manager.tokenizer_cache_budget_source = .local_manager;
        }

        /// Call after all attached managers have closed. Escaped tokenizers
        /// retain their domain until their final cache/metadata lease releases.
        pub fn deinit(self: *@This()) void {
            self.domain.close();
            self.domain.release();
            self.* = undefined;
        }
    };
    /// Tokenizer-only preparation for offline artifacts. The retained domain
    /// makes both metadata and BPE cache ownership independent of manager moves.
    pub const TokenizerAssets = struct {
        allocator: std.mem.Allocator,
        hf_tok: ?*hf_tokenizer.HfTokenizer = null,
        sp_tok: ?*sentencepiece.Processor = null,
        chat_tmpl: ?*generation.ChatTemplate = null,
        lease: ?runtime.tier.memory.AdmissionLease = null,
        domain: *ResourceDomain,

        pub fn tokenizer(self: *const @This()) tokenizer_mod.Tokenizer {
            if (self.hf_tok) |tok| return tok.tokenizer();
            return self.sp_tok.?.tokenizer();
        }

        pub fn deinit(self: *@This()) void {
            if (self.hf_tok) |tok| tok.deinitSelf();
            if (self.sp_tok) |tok| {
                tok.deinit();
                self.allocator.destroy(tok);
            }
            if (self.chat_tmpl) |ct| {
                ct.deinit();
                self.allocator.destroy(ct);
            }
            if (self.lease) |*lease| lease.release();
            self.domain.release();
        }
    };

    fn verifyArtifactPublication(self: *ModelManager, model_dir: []const u8, man: *const manifest_mod.ModelManifest) !void {
        const io = self.session_manager.io orelse std.Options.debug_io;
        if (!std.meta.eql(man.managed_publication, try managed_receipt.publicationGeneration(self.allocator, io, model_dir))) return error.ModelArtifactsChanging;
    }

    pub fn loadGenerationTokenizerAssets(self: *ModelManager, model_dir: []const u8, provided_man: *const manifest_mod.ModelManifest) !TokenizerAssets {
        const io = self.session_manager.io orelse std.Options.debug_io;
        var publication_lease = try @import("../registry/publication_lease.zig").Lease.shared(self.allocator, io, model_dir);
        defer publication_lease.deinit(io);
        try self.verifyArtifactPublication(model_dir, provided_man);
        try self.ensureResourceOwnerReady();
        var artifacts = try PublishedArtifacts.init(self.allocator, io, model_dir, .{ .publication = provided_man.managed_publication });
        defer artifacts.deinit();
        artifacts.verification_cache = &self.verified_artifacts;
        var proof = ComponentConstruction.borrowingUnderLease(io, &artifacts);
        defer proof.deinit();
        // Tokenizer-only preparation shares session construction's identity,
        // provenance, and scope checks without opening unused weight sources.
        var bound = try BoundModelSource.init(&proof, model_dir, ModelSource.kindForPath(model_dir), provided_man, null);
        defer bound.deinit();
        const man = bound.view().manifest.?;
        const gemma_compat = tokenizer_loader.tokenizerGemmaCompat(man.*, model_dir);
        const kind = try tokenizer_loader.tokenizerKindWithArtifacts(man.*, &artifacts, gemma_compat);
        const plan = try load_plan.tokenizerLoadAdmissionPlanWithArtifacts(self.allocator, model_dir, man, kind, &artifacts);
        var lease = try self.acquireTokenizerAdmission(plan);
        errdefer if (lease) |*active| active.release();
        const domain = self.resource_domain.?;
        if (!domain.retain()) return error.ResourceOwnerShuttingDown;
        var assets = TokenizerAssets{ .allocator = self.allocator, .domain = domain };
        errdefer assets.deinit();
        // assets takes its domain reference immediately; keep the metadata
        // lease separate until all parsing and cache configuration succeeds.
        switch (kind) {
            .huggingface => {
                assets.hf_tok = try tokenizer_loader.loadHuggingFaceTokenizerFromManifestWithArtifacts(self.allocator, man, &artifacts);
                try assets.hf_tok.?.configureBpeCache(self.tokenizer_cache_config);
                try assets.hf_tok.?.configureParallelBpe(self.tokenizer_parallel_bpe_config);
            },
            .sentencepiece => {
                assets.sp_tok = try tokenizer_loader.loadSentencePieceTokenizerWithArtifacts(self.allocator, man, &artifacts);
                if (gemma_compat)
                    assets.sp_tok.?.setPreserveInlineSpecialsAfterLiteralBos(true);
                try tokenizer_loader.loadSentencePieceAddedTokensWithArtifacts(self.allocator, assets.sp_tok.?, &artifacts);
            },
        }
        if (man.chat_template) |source| {
            const ct = try self.allocator.create(generation.ChatTemplate);
            errdefer self.allocator.destroy(ct);
            ct.* = try generation.ChatTemplate.init(self.allocator, source, man.bos_token, man.eos_token, man.unk_token, man.pad_token);
            assets.chat_tmpl = ct;
        }
        try bound.finish(&proof);
        try self.verifyArtifactPublication(model_dir, man);
        if (lease) |*active| try active.retain(plan.resident);
        assets.lease = lease;
        return assets;
    }

    const ManagedWhisperPrompt = loaded_runtime.ManagedWhisperPrompt;
    fn loadNativeWhisperPrompt(self: *ModelManager, man: *const manifest_mod.ModelManifest, artifacts: *PublishedArtifacts, tokenizer: tokenizer_mod.Tokenizer) !ManagedWhisperPrompt {
        return loaded_runtime.loadNativeWhisperPrompt(self.allocator, self.primaryPreparation().tokenizer_admission, man, artifacts, tokenizer);
    }
    /// Load only the admitted backend session. Artifact callers already own
    /// tokenizer assets and must not parse or retain a second tokenizer.
    pub fn loadArtifactSession(self: *ModelManager, model_dir: []const u8, man: manifest_mod.ModelManifest) !ArtifactSession {
        const io = self.session_manager.io orelse std.Options.debug_io;
        var publication_lease = try @import("../registry/publication_lease.zig").Lease.shared(self.allocator, io, model_dir);
        defer publication_lease.deinit(io);
        try self.verifyArtifactPublication(model_dir, &man);
        var managed = try loadSessionForPreferredBackends(self, &.{.native}, model_dir, man, &self.session_manager, null);
        var result = ArtifactSession{ .allocator = self.allocator, .managed = managed.take(), .session = managed.session };
        errdefer result.deinit();
        // Preserve the normal generator's weight-prefetch and expert-cache
        // behavior without allocating another tokenizer or serving scheduler.
        if (session_factory.getGptConfig(result.session)) |cfg| {
            if (cfg.usesMoe()) {
                const cache = try self.allocator.create(runtime.moe.shared.SharedExpertCache);
                cache.* = runtime.moe.shared.SharedExpertCache.init(self.allocator);
                result.shared_moe_cache = cache;
            }
            const state = try self.allocator.create(runtime.tier.shared.SharedPrefetchState);
            state.* = runtime.tier.shared.SharedPrefetchState.init(self.allocator);
            result.shared_prefetch = state;
            try session_factory.attachSharedPrefetchState(result.session, state);
        }
        try self.verifyArtifactPublication(model_dir, &man);
        return result;
    }

    pub fn loadedRuntimeHost(self: *ModelManager) loaded_runtime.Host {
        const Services = struct {
            fn retire(context: *anyopaque, model: *LoadedModel) void {
                const manager: *ModelManager = @ptrCast(@alignCast(context));
                manager.retireLoadedModel(model);
            }
            fn reserve(context: *anyopaque, backend: runtime.tier.memory.BackendClass, limits: runtime.tier.memory.Limits, amounts: runtime.tier.memory.AdmissionAmounts) !runtime.tier.memory.AdmissionLease {
                const manager: *ModelManager = @ptrCast(@alignCast(context));
                return manager.acquireRunResourceAmounts(backend, limits, amounts);
            }
        };
        return .{ .context = self, .session_loading = SessionLoadHost.services(self), .retire = Services.retire, .reserve = Services.reserve };
    }

    pub const HostCacheReclaimer = struct {
        context: *anyopaque,
        reclaim: *const fn (*anyopaque, usize) usize,
    };

    pub const MemoryAdmissionDiagnostic = admission_diagnostics.Diagnostic;

    const LoadedModelMap = std.StringHashMapUnmanaged(*LoadedModel);
    const tokenizer_cache_budget_shard_count = 16;
    const tokenizer_cache_admission_quantum_bytes = 1024 * 1024;

    const TokenizerCacheBudgetSource = enum {
        none,
        local_manager,
        external_pair,
    };

    const TokenizerCacheBudgetRecord = struct {
        actual_bytes: usize = 0,
        reserved_bytes: usize = 0,
        transitioning: bool = false,
        credits: std.ArrayListUnmanaged(runtime.tier.memory.AdmissionLease) = .empty,
    };

    const TokenizerCacheBudgetShard = struct {
        mutex: std.atomic.Mutex = .unlocked,
        records: std.AutoHashMapUnmanaged(usize, *TokenizerCacheBudgetRecord) = .empty,
    };

    /// Ref-counted resource domain shared by ModelManager and every tokenizer
    /// that adopts its budget capability. Admission state and tokenizer-cache
    /// ledgers live here, rather than inside ModelManager, so physical memory
    /// remains charged until the last escaped tokenizer is destroyed.
    const ResourceDomain = struct {
        allocator: std.mem.Allocator,
        admission_owner: *runtime.tier.admission_domain.Domain,
        admission: *runtime.tier.memory.AdmissionController,
        admission_limits: runtime.tier.memory.Limits = .{},
        tokenizer_cache_budget_source: TokenizerCacheBudgetSource = .none,
        external_tokenizer_cache_budget: ?hf_tokenizer.HfTokenizer.BpeCacheResourceBudget = null,
        tokenizer_cache_budget_shards: [tokenizer_cache_budget_shard_count]TokenizerCacheBudgetShard =
            @as([tokenizer_cache_budget_shard_count]TokenizerCacheBudgetShard, @splat(.{})),
        references: std.atomic.Value(usize) = .init(1),
        closing: std.atomic.Value(bool) = .init(false),
        managed_mutex: std.atomic.Mutex = .unlocked,
        managed_head: ?*ManagedTokenizerLifetime = null,

        fn create(allocator: std.mem.Allocator) !*@This() {
            const lifetime = try allocator.create(@This());
            errdefer allocator.destroy(lifetime);
            const admission = try runtime.tier.admission_domain.Domain.create();
            lifetime.* = .{
                .allocator = allocator,
                .admission_owner = admission,
                .admission = &admission.controller,
            };
            return lifetime;
        }

        fn retain(self: *@This()) bool {
            if (self.closing.load(.acquire)) return false;
            var current = self.references.load(.acquire);
            while (current != 0 and current != std.math.maxInt(usize)) {
                if (self.closing.load(.acquire)) return false;
                if (self.references.cmpxchgWeak(
                    current,
                    current + 1,
                    .acq_rel,
                    .acquire,
                )) |observed| {
                    current = observed;
                } else {
                    if (self.closing.load(.acquire)) {
                        self.release();
                        return false;
                    }
                    return true;
                }
            }
            return false;
        }

        fn release(self: *@This()) void {
            const previous = self.references.fetchSub(1, .acq_rel);
            std.debug.assert(previous > 0);
            if (previous == 1) {
                if (!self.closing.load(.acquire))
                    @panic("inference resource domain lost its final live reference");
                if (self.managed_head != null)
                    @panic("inference resource domain closed with managed tokenizers");
                for (&self.tokenizer_cache_budget_shards) |*shard| {
                    if (shard.records.count() != 0)
                        @panic("inference resource domain closed with tokenizer observers");
                    shard.records.deinit(self.allocator);
                }
                // Ordinary owners and IO may disappear while independently
                // pinned SDK retirement still owns its exact admission lease.
                self.admission_owner.release();
                if (self.external_tokenizer_cache_budget) |budget| budget.releaseContext();
                self.allocator.destroy(self);
            }
        }

        fn close(self: *@This()) void {
            const was_closing = self.closing.swap(true, .acq_rel);
            if (was_closing) @panic("inference resource domain closed twice");
        }

        fn registerManagedTokenizer(
            self: *@This(),
            resource_lease: ?runtime.tier.memory.AdmissionLease,
        ) !*ManagedTokenizerLifetime {
            if (self.closing.load(.acquire)) return error.ResourceOwnerShuttingDown;
            const managed = try self.allocator.create(ManagedTokenizerLifetime);
            managed.* = .{
                .lifetime = self,
                .resource_lease = resource_lease,
            };
            if (!self.retain()) {
                self.allocator.destroy(managed);
                return error.ResourceOwnerShuttingDown;
            }

            spinLock(&self.managed_mutex);
            if (self.closing.load(.acquire)) {
                self.managed_mutex.unlock();
                self.allocator.destroy(managed);
                self.release();
                return error.ResourceOwnerShuttingDown;
            }
            managed.next = self.managed_head;
            if (self.managed_head) |head| head.previous = managed;
            self.managed_head = managed;
            self.managed_mutex.unlock();
            return managed;
        }

        fn unregisterManagedTokenizer(
            self: *@This(),
            managed: *ManagedTokenizerLifetime,
        ) void {
            spinLock(&self.managed_mutex);
            if (managed.previous) |previous| {
                previous.next = managed.next;
            } else {
                std.debug.assert(self.managed_head == managed);
                self.managed_head = managed.next;
            }
            if (managed.next) |next| next.previous = managed.previous;
            if (managed.resource_lease) |*lease| lease.release();
            managed.resource_lease = null;
            managed.previous = null;
            managed.next = null;
            self.managed_mutex.unlock();

            self.allocator.destroy(managed);
            self.release();
        }

        fn resourceBudget(
            self: *@This(),
        ) hf_tokenizer.HfTokenizer.BpeCacheResourceBudget {
            return .{
                .context = self,
                .retain_context = retainContext,
                .release_context = releaseContext,
                .observe = observe,
            };
        }

        fn retainContext(context: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            return self.retain();
        }

        fn releaseContext(context: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.release();
        }

        fn observe(
            context: *anyopaque,
            observer_id: usize,
            previous: usize,
            next: usize,
        ) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (self.closing.load(.acquire) and next > previous) return false;
            return ModelManager.reconcileTokenizerCacheUsage(self, observer_id, previous, next);
        }
    };

    const ManagedTokenizerLifetime = struct {
        lifetime: *ResourceDomain,
        resource_lease: ?runtime.tier.memory.AdmissionLease = null,
        previous: ?*ManagedTokenizerLifetime = null,
        next: ?*ManagedTokenizerLifetime = null,

        fn capability(self: *@This()) @import("../models/managed_tokenizer.zig").Lifetime {
            const Adapter = struct {
                fn release(raw: *anyopaque) void {
                    const owner: *ManagedTokenizerLifetime = @ptrCast(@alignCast(raw));
                    owner.release();
                }
                fn admission(raw: *anyopaque) ?runtime.tier.memory.AdmissionLease {
                    const owner: *ManagedTokenizerLifetime = @ptrCast(@alignCast(raw));
                    spinLock(&owner.lifetime.managed_mutex);
                    defer owner.lifetime.managed_mutex.unlock();
                    return owner.resource_lease;
                }
            };
            return .{ .context = self, .release_fn = Adapter.release, .admission_fn = Adapter.admission };
        }

        fn release(self: *@This()) void {
            self.lifetime.unregisterManagedTokenizer(self);
        }
    };

    const EvictedModel = struct {
        key: []const u8,
        model: *LoadedModel,
    };

    const EvictedCompositeAssets = struct {
        key: ComponentPlanKey,
        assets: *CompositeAssets,
    };

    allocator: std.mem.Allocator,
    session_manager: backends.SessionManager,
    /// Null for diagnostic/offline CLIs. Serving Nodes set this before any model load.
    serving_policy: ?model_compatibility.Policy = null,
    admission_diagnostics: admission_diagnostics.Recorder = .{},
    admission_enabled: bool = false,
    /// Optional caller policy for full-model loads. Borrowed until deinit;
    /// checked per resolved backend before construction, with normal fallback
    /// and CUDA A4B fail-closed rules. No device probing belongs in this hook.
    model_load_preflight: ?ModelLoadPreflight = null,
    admission_limit_overrides: runtime.tier.memory.Limits = .{},
    process_memory_limit_bytes: usize = 0,
    process_memory_limit_provenance: runtime.tier.memory.ProcessMemoryLimitProvenance = .automatic,
    forced_run_admission_denials_for_testing: usize = 0,
    /// Test-only stable-limit provider, allowing unified-CUDA coverage without
    /// opening a driver or adding per-allocation instrumentation in production.
    test_unified_limit_probe: if (builtin.is_test) ?*const fn (usize, usize) usize else void = if (builtin.is_test) null else {},
    resource_ownership: ResourceOwnership = .local,
    tokenizer_cache_budget_source: TokenizerCacheBudgetSource = .none,
    loaded: std.StringHashMapUnmanaged(*LoadedModel),
    loaded_aliases: std.StringHashMapUnmanaged(*LoadedModel),
    /// Durable startup load policies, keyed by resolved model directory.
    /// Values are copied under load_lock; request lookup is O(1).
    startup_load_policies: std.StringHashMapUnmanaged(StartupLoadPolicy) = .{},
    runtime_epoch: u64,
    next_runtime_sequence: u64 = 1,
    /// Protects loaded-model maps and the in-flight registry. Expensive tokenizer,
    /// weight, and backend initialization always runs outside this lock.
    load_lock: std.atomic.Mutex = .unlocked,
    /// Serializes selecting and destroying eviction victims. Destruction
    /// releases admission leases outside load_lock and may be expensive.
    eviction_lock: std.atomic.Mutex = .unlocked,
    host_cache_mutex: std.atomic.Mutex = .unlocked,
    host_cache_reclaimer: ?HostCacheReclaimer = null,
    keep_alive_ms: u64 = 0,
    max_loaded_models: usize = 0,
    eviction_group: std.Io.Group = .init,
    eviction_stop: std.Io.Event = .unset,
    eviction_io: ?std.Io = null,
    eviction_loop_started: bool = false,
    /// Cold initialization is owned by the manager rather than whichever
    /// request happened to miss the cache first. Request waiters may therefore
    /// cancel independently without invalidating shared work.
    load_group: std.Io.Group = .init,
    load_io: ?std.Io = null,
    /// Lazily allocated at a stable address for offline/direct callers. Never
    /// borrow a request's Io: shared loads and resident sessions outlive it.
    owned_load_runtime: ?*std.Io.Threaded = null,
    owned_load_io_owner: ?*OwnedManagerIo = null,
    teardown_domain: ?*TeardownDomain = null,
    owned_load_watchdog: ?*HardCancellationWatchdog = null,
    in_flight_loads: std.StringHashMapUnmanaged(*LoadFlight) = .empty,
    composite_assets: std.AutoHashMapUnmanaged(ComponentPlanKey, *CompositeAssets) = .empty,
    in_flight_composite_assets: std.AutoHashMapUnmanaged(ComponentPlanKey, *CompositeAssetsLoadFlight) = .empty,
    component_plan_cache: std.AutoHashMapUnmanaged(
        ComponentPlanKey,
        *ComponentPlanCacheEntry,
    ) = .empty,
    component_plan_cache_lock: std.atomic.Mutex = .unlocked,
    merged_decoder_membership: MergedDecoderMembershipCache = .{},
    verified_artifacts: @import("../registry/published_artifacts.zig").VerificationCache = .{},
    unhealthy_backend_lock: std.atomic.Mutex = .unlocked,
    unhealthy_model_backends: std.AutoHashMapUnmanaged(ModelBackendHealthKey, u64) = .empty,
    tokenizer_cache_config_mutex: std.atomic.Mutex = .unlocked,
    resource_domain: ?*ResourceDomain = null,
    owns_resource_domain: bool = true,
    tokenizer_cache_config: hf_tokenizer.HfTokenizer.BpeCacheConfig = .{},
    tokenizer_parallel_bpe_config: hf_tokenizer.HfTokenizer.ParallelBpeConfig = .{},

    pub fn init(allocator: std.mem.Allocator, session_manager: backends.SessionManager) ModelManager {
        return initWithRuntimeEpoch(
            allocator,
            session_manager,
            platform.time.monotonicNs() ^ @as(u64, @truncate(@intFromPtr(allocator.ptr))),
        );
    }

    pub fn initWithRuntimeEpoch(
        allocator: std.mem.Allocator,
        session_manager: backends.SessionManager,
        runtime_epoch: u64,
    ) ModelManager {
        return .{
            .allocator = allocator,
            .session_manager = session_manager,
            .loaded = LoadedModelMap{},
            .loaded_aliases = LoadedModelMap{},
            .runtime_epoch = runtime_epoch,
            .verified_artifacts = .{ .owner_allocator = allocator },
        };
    }

    fn nextRuntimeIdLocked(self: *ModelManager) RuntimeId {
        const sequence = self.next_runtime_sequence;
        self.next_runtime_sequence +%= 1;
        if (self.next_runtime_sequence == 0) self.next_runtime_sequence = 1;
        return (@as(RuntimeId, self.runtime_epoch) << 64) | sequence;
    }

    pub fn configureServingPolicy(self: *ModelManager, policy: model_compatibility.Policy) void {
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(self.composite_assets.count() == 0);
        self.serving_policy = policy;
        self.admission_enabled = true;
        if (self.resource_domain) |domain|
            self.configureAdmissionController(domain);
    }

    pub fn configureResourceOwnership(
        self: *ModelManager,
        ownership: ResourceOwnership,
    ) !void {
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(self.composite_assets.count() == 0);
        spinLock(&self.tokenizer_cache_config_mutex);
        defer self.tokenizer_cache_config_mutex.unlock();
        if (self.tokenizer_cache_budget_source != .none)
            return error.ResourceOwnershipAfterTokenizerBudgetConfiguration;
        self.resource_ownership = ownership;
    }

    pub fn configureProcessMemoryLimit(
        self: *ModelManager,
        limit_bytes: usize,
        provenance: runtime.tier.memory.ProcessMemoryLimitProvenance,
    ) void {
        std.debug.assert(self.owns_resource_domain);
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(self.composite_assets.count() == 0);
        self.process_memory_limit_bytes = limit_bytes;
        self.process_memory_limit_provenance = provenance;
        if (self.resource_domain) |domain|
            self.configureAdmissionController(domain);
    }

    pub fn ensureResourceOwnerReady(self: *ModelManager) !void {
        spinLock(&self.tokenizer_cache_config_mutex);
        defer self.tokenizer_cache_config_mutex.unlock();
        switch (self.resource_ownership) {
            .local => {
                switch (self.tokenizer_cache_budget_source) {
                    .none => {},
                    .local_manager => return,
                    .external_pair => return error.ExternalTokenizerBudgetInLocalOwnership,
                }
                if (self.tokenizer_cache_config.resource_budget != null)
                    return error.UntrackedTokenizerResourceBudget;
                const lifetime = try self.ensureResourceDomain();
                lifetime.tokenizer_cache_budget_source = .local_manager;
                self.tokenizer_cache_config.resource_budget = lifetime.resourceBudget();
                self.tokenizer_cache_budget_source = .local_manager;
            },
            .external_required => {
                if (self.tokenizer_cache_budget_source != .external_pair)
                    return error.ExternalResourceManagerNotConfigured;
                if (self.tokenizer_cache_config.resource_budget == null)
                    return error.ExternalTokenizerResourceManagerNotConfigured;
            },
        }
    }

    fn ensureResourceDomain(
        self: *ModelManager,
    ) !*ResourceDomain {
        if (self.resource_domain) |lifetime| return lifetime;
        const lifetime = try ResourceDomain.create(self.allocator);
        self.configureAdmissionController(lifetime);
        self.resource_domain = lifetime;
        return lifetime;
    }

    fn configureAdmissionController(
        self: *const ModelManager,
        domain: *ResourceDomain,
    ) void {
        // An attached manager can tighten its own request policy, but only the
        // invocation owner configures the shared physical envelope.
        if (!self.owns_resource_domain) return;
        domain.admission.configureSharedLimits(
            runtime.tier.memory.sharedAdmissionLimitsWithOverridesAndProcessLimit(
                self.admission_limit_overrides,
                self.process_memory_limit_bytes,
            ),
        );
        domain.admission.configureProcessMemoryLimit(
            self.process_memory_limit_bytes,
            self.process_memory_limit_provenance,
        );
        domain.admission.configureForcedRunDenialsForTesting(
            self.forced_run_admission_denials_for_testing,
        );
        domain.admission_limits = self.admissionLimitsForBackend(.{ .backend = .native });
    }

    fn admissionController(
        self: *const ModelManager,
    ) *runtime.tier.memory.AdmissionController {
        return self.resource_domain.?.admission;
    }

    /// Keep the accounting domain alive for an executor whose handles outlive
    /// its manager. This does not keep the manager itself or model caches alive.
    pub const ResourceLifetime = struct {
        domain: *ResourceDomain,
        pub fn deinit(self: @This()) void {
            self.domain.release();
        }

        /// Metadata allocations participate in the same process-wide ledger.
        /// The allocator separately reserves batched physical headroom, so
        /// every byte participates without a kernel probe per parser node.
        pub fn acquireMetadata(self: @This(), limits: runtime.tier.memory.Limits, bytes: usize) !runtime.tier.memory.AdmissionLease {
            return self.domain.admission.tryAcquire(.cpu, limits, .{ .host_weight_bytes = bytes }, false);
        }

        pub fn reserveMetadataCapacity(self: @This(), bytes: usize) !runtime.tier.memory.LiveReservation {
            return self.domain.admission.reserveLiveCapacity(bytes);
        }
    };

    pub fn retainResourceLifetime(self: *ModelManager) !ResourceLifetime {
        try self.ensureResourceOwnerReady();
        const domain = self.resource_domain.?;
        if (!domain.retain()) return error.ResourceOwnerShuttingDown;
        return .{ .domain = domain };
    }

    /// A provider discovered after tokenizer construction can tighten the
    /// shared physical envelope without replacing any outstanding leases.
    pub fn tightenUnifiedDeviceCapacity(self: *ModelManager, bytes: usize) !void {
        try self.ensureResourceOwnerReady();
        self.admissionController().tightenUnifiedLimit(bytes);
    }

    /// Read the physical admission ledger without holding model/catalog locks.
    /// ResourceDomain stays owned by the live manager until requests drain.
    pub fn currentAdmissionAmounts(self: *ModelManager) runtime.tier.memory.AdmissionAmounts {
        spinLock(&self.tokenizer_cache_config_mutex);
        const domain = self.resource_domain;
        self.tokenizer_cache_config_mutex.unlock();
        return if (domain) |owner| owner.admission.snapshot() else .{};
    }

    /// Install at the owner's stable address. Clearing the hook waits for any
    /// running callback before its borrowed cache context may be destroyed.
    pub fn setHostCacheReclaimer(self: *ModelManager, hook: ?HostCacheReclaimer) void {
        spinLock(&self.host_cache_mutex);
        defer self.host_cache_mutex.unlock();
        self.host_cache_reclaimer = hook;
    }
    fn reclaimHostCache(self: *ModelManager, target_bytes: usize) usize {
        if (!self.host_cache_mutex.tryLock()) return 0;
        defer self.host_cache_mutex.unlock();
        const hook = self.host_cache_reclaimer orelse return 0;
        const freed = hook.reclaim(hook.context, target_bytes);
        if (freed != 0) _ = platform.allocator.reclaimUnusedProcessMemory();
        return freed;
    }

    /// Account bounded metadata/codec inspection before a model is loaded.
    /// Uses the same physical host domain as loaded models and execution.
    pub fn admitHostPreflight(self: *ModelManager, bytes: usize) !runtime.tier.memory.AdmissionLease {
        return self.admitHostPreflightChecked(bytes, true);
    }
    fn admitHostPreflightChecked(self: *ModelManager, bytes: usize, check_live_memory: bool) !runtime.tier.memory.AdmissionLease {
        try self.ensureResourceOwnerReady();
        const domain = self.resource_domain.?;
        return domain.admission.tryAcquire(.cpu, domain.admission_limits, .{ .host_scratch_bytes = bytes }, check_live_memory) catch |err| {
            if (err != error.ResourceTemporarilyUnavailable or self.reclaimHostCache(bytes) == 0) return err;
            return domain.admission.tryAcquire(.cpu, domain.admission_limits, .{ .host_scratch_bytes = bytes }, check_live_memory);
        };
    }

    /// Auxiliary resident models share the physical host domain with primary
    /// sessions. The constructor shrinks this lease to actual owned weights.
    pub fn admitHostWeights(self: *ModelManager, peak_bytes: usize) !runtime.tier.memory.AdmissionLease {
        try self.ensureResourceOwnerReady();
        const domain = self.resource_domain.?;
        return domain.admission.tryAcquire(.cpu, domain.admission_limits, .{ .host_weight_bytes = peak_bytes }, true) catch |err| {
            if (err != error.ResourceTemporarilyUnavailable or self.reclaimHostCache(peak_bytes) == 0) return err;
            return domain.admission.tryAcquire(.cpu, domain.admission_limits, .{ .host_weight_bytes = peak_bytes }, true);
        };
    }

    /// Exact admission ownership held by the live Hugging Face tokenizer
    /// caches. Cache growth is charged in independent quantum leases owned by
    /// ResourceDomain records, rather than by LoadedModel.tokenizer_resource_lease.
    /// Callers that reconcile an idle domain must include both sources.
    pub fn tokenizerCacheAdmissionAmounts(
        self: *const ModelManager,
    ) !runtime.tier.memory.AdmissionAmounts {
        const domain = self.resource_domain orelse return .{};
        var total: runtime.tier.memory.AdmissionAmounts = .{};
        for (&domain.tokenizer_cache_budget_shards) |*shard| {
            spinLock(&shard.mutex);
            {
                defer shard.mutex.unlock();
                var records = shard.records.valueIterator();
                while (records.next()) |record_ptr| {
                    for (record_ptr.*.credits.items) |credit|
                        total = try total.merge(credit.amounts);
                }
            }
        }
        return total;
    }

    fn tokenizerCacheBudgetShard(
        self: *ResourceDomain,
        observer_id: usize,
    ) *TokenizerCacheBudgetShard {
        return &self.tokenizer_cache_budget_shards[
            observer_id % tokenizer_cache_budget_shard_count
        ];
    }

    fn tokenizerCacheCreditTarget(actual_bytes: usize) usize {
        if (actual_bytes == 0) return 0;
        const remainder = actual_bytes % tokenizer_cache_admission_quantum_bytes;
        if (remainder == 0) return actual_bytes;
        return std.math.add(
            usize,
            actual_bytes,
            tokenizer_cache_admission_quantum_bytes - remainder,
        ) catch actual_bytes;
    }

    fn acquireTokenizerCacheCredit(
        self: *ResourceDomain,
        bytes: usize,
    ) !runtime.tier.memory.AdmissionLease {
        std.debug.assert(bytes > 0);
        const amounts = runtime.tier.memory.AdmissionAmounts{
            .host_weight_bytes = bytes,
        };
        var pressure: ?runtime.tier.memory.AdmissionPressure = null;
        var lease = try self.admission.tryAcquireWithPressure(
            .cpu,
            self.admission_limits,
            amounts,
            true,
            &pressure,
        );
        errdefer lease.release();
        // Settle the transient live-memory epoch while retaining ordinary lease
        // ownership for the admitted credit. The ModelManager record, not a raw
        // byte total, remains the sole release authority.
        try lease.retain(amounts);
        return lease;
    }

    fn releaseTokenizerCacheCredit(
        record: *TokenizerCacheBudgetRecord,
        released_bytes: usize,
    ) void {
        std.debug.assert(released_bytes <= record.reserved_bytes);
        var remaining = released_bytes;
        while (remaining > 0) {
            const lease = &record.credits.items[record.credits.items.len - 1];
            const owned_bytes = lease.amounts.host_weight_bytes;
            std.debug.assert(owned_bytes > 0);
            if (owned_bytes <= remaining) {
                remaining -= owned_bytes;
                record.reserved_bytes -= owned_bytes;
                lease.release();
                _ = record.credits.pop();
            } else {
                lease.retain(.{
                    .host_weight_bytes = owned_bytes - remaining,
                }) catch unreachable;
                record.reserved_bytes -= remaining;
                remaining = 0;
            }
        }
    }

    fn removeTokenizerCacheBudgetRecord(
        self: *ResourceDomain,
        shard: *TokenizerCacheBudgetShard,
        observer_id: usize,
        record: *TokenizerCacheBudgetRecord,
    ) void {
        const removed = shard.records.fetchRemove(observer_id) orelse return;
        std.debug.assert(removed.value == record);
        std.debug.assert(record.actual_bytes == 0);
        std.debug.assert(record.reserved_bytes == 0);
        std.debug.assert(record.credits.items.len == 0);
        record.credits.deinit(self.allocator);
        self.allocator.destroy(record);
    }

    /// Track exact tokenizer usage while admitting capacity in bounded chunks.
    /// The shard lock protects only identity transitions; potentially blocking
    /// live-memory probes run after marking the record in flight and releasing
    /// that lock. At most one quantum per tokenizer is conservatively unused.
    fn reconcileLocalTokenizerCacheUsage(
        self: *ResourceDomain,
        observer_id: usize,
        previous: usize,
        next: usize,
    ) bool {
        if (observer_id == 0) return false;
        const shard = tokenizerCacheBudgetShard(self, observer_id);
        spinLock(&shard.mutex);

        var inserted = false;
        const record = shard.records.get(observer_id) orelse blk: {
            if (previous != 0) {
                shard.mutex.unlock();
                return false;
            }
            if (next == 0) {
                shard.mutex.unlock();
                return true;
            }
            const created = self.allocator.create(TokenizerCacheBudgetRecord) catch {
                shard.mutex.unlock();
                return false;
            };
            created.* = .{};
            shard.records.put(self.allocator, observer_id, created) catch {
                self.allocator.destroy(created);
                shard.mutex.unlock();
                return false;
            };
            inserted = true;
            break :blk created;
        };
        if (record.actual_bytes != previous or record.transitioning) {
            shard.mutex.unlock();
            return false;
        }

        const reserved = record.reserved_bytes;
        const rounded_target = tokenizerCacheCreditTarget(next);
        const credit_target = if (next > reserved)
            rounded_target
        else if (next < previous)
            @min(reserved, rounded_target)
        else
            reserved;
        if (credit_target == reserved) {
            record.actual_bytes = next;
            if (next == 0) removeTokenizerCacheBudgetRecord(
                self,
                shard,
                observer_id,
                record,
            );
            shard.mutex.unlock();
            return true;
        }

        if (credit_target > reserved) {
            record.credits.ensureUnusedCapacity(self.allocator, 1) catch {
                if (inserted) removeTokenizerCacheBudgetRecord(
                    self,
                    shard,
                    observer_id,
                    record,
                );
                shard.mutex.unlock();
                return false;
            };
        }
        record.transitioning = true;
        shard.mutex.unlock();

        if (credit_target > reserved) {
            const preferred_growth = credit_target - reserved;
            const required_growth = next -| reserved;
            const credit = acquireTokenizerCacheCredit(self, preferred_growth) catch retry: {
                if (preferred_growth == required_growth) {
                    spinLock(&shard.mutex);
                    record.transitioning = false;
                    if (inserted) removeTokenizerCacheBudgetRecord(
                        self,
                        shard,
                        observer_id,
                        record,
                    );
                    shard.mutex.unlock();
                    return false;
                }
                break :retry acquireTokenizerCacheCredit(self, required_growth) catch {
                    spinLock(&shard.mutex);
                    record.transitioning = false;
                    if (inserted) removeTokenizerCacheBudgetRecord(
                        self,
                        shard,
                        observer_id,
                        record,
                    );
                    shard.mutex.unlock();
                    return false;
                };
            };

            spinLock(&shard.mutex);
            record.credits.appendAssumeCapacity(credit);
            record.reserved_bytes += credit.amounts.host_weight_bytes;
            record.actual_bytes = next;
            record.transitioning = false;
            shard.mutex.unlock();
            return true;
        }

        const released = reserved - credit_target;
        releaseTokenizerCacheCredit(record, released);
        spinLock(&shard.mutex);
        record.actual_bytes = next;
        record.transitioning = false;
        if (next == 0) removeTokenizerCacheBudgetRecord(
            self,
            shard,
            observer_id,
            record,
        );
        shard.mutex.unlock();
        return true;
    }

    fn reconcileExternalTokenizerCacheUsage(
        self: *ResourceDomain,
        observer_id: usize,
        previous: usize,
        next: usize,
    ) bool {
        if (observer_id == 0) return false;
        const upstream = self.external_tokenizer_cache_budget orelse return false;
        const shard = tokenizerCacheBudgetShard(self, observer_id);
        spinLock(&shard.mutex);

        var inserted = false;
        const record = shard.records.get(observer_id) orelse blk: {
            if (previous != 0) {
                shard.mutex.unlock();
                return false;
            }
            if (next == 0) {
                shard.mutex.unlock();
                return true;
            }
            const created = self.allocator.create(TokenizerCacheBudgetRecord) catch {
                shard.mutex.unlock();
                return false;
            };
            created.* = .{};
            shard.records.put(self.allocator, observer_id, created) catch {
                self.allocator.destroy(created);
                shard.mutex.unlock();
                return false;
            };
            inserted = true;
            break :blk created;
        };
        if (record.actual_bytes != previous or record.transitioning) {
            shard.mutex.unlock();
            return false;
        }
        record.transitioning = true;
        shard.mutex.unlock();

        if (!upstream.observe(upstream.context, observer_id, previous, next)) {
            spinLock(&shard.mutex);
            record.transitioning = false;
            if (inserted) removeTokenizerCacheBudgetRecord(
                self,
                shard,
                observer_id,
                record,
            );
            shard.mutex.unlock();
            return false;
        }

        spinLock(&shard.mutex);
        record.actual_bytes = next;
        record.transitioning = false;
        if (next == 0) removeTokenizerCacheBudgetRecord(
            self,
            shard,
            observer_id,
            record,
        );
        shard.mutex.unlock();
        return true;
    }

    fn reconcileTokenizerCacheUsage(
        self: *ResourceDomain,
        observer_id: usize,
        previous: usize,
        next: usize,
    ) bool {
        return switch (self.tokenizer_cache_budget_source) {
            .none => false,
            .local_manager => reconcileLocalTokenizerCacheUsage(
                self,
                observer_id,
                previous,
                next,
            ),
            .external_pair => reconcileExternalTokenizerCacheUsage(
                self,
                observer_id,
                previous,
                next,
            ),
        };
    }

    pub fn configureModelCache(
        self: *ModelManager,
        keep_alive_ms: u64,
        max_loaded_models: usize,
    ) void {
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(self.composite_assets.count() == 0);
        std.debug.assert(!self.eviction_loop_started);
        self.keep_alive_ms = keep_alive_ms;
        self.max_loaded_models = max_loaded_models;
    }

    pub fn acquireRunResources(
        self: *ModelManager,
        backend_class: runtime.tier.memory.BackendClass,
        limits: runtime.tier.memory.Limits,
        estimate: runtime.tier.memory.Estimate,
    ) !runtime.tier.memory.AdmissionLease {
        return self.acquireRunResourceAmounts(
            backend_class,
            limits,
            try runtime.tier.memory.AdmissionAmounts.fromEstimate(estimate),
        );
    }

    /// Admit already-classified transient amounts. Callers use this when an
    /// execution owns request-scoped artifacts in addition to KV and scratch.
    pub fn acquireRunResourceAmounts(
        self: *ModelManager,
        backend_class: runtime.tier.memory.BackendClass,
        limits: runtime.tier.memory.Limits,
        amounts: runtime.tier.memory.AdmissionAmounts,
    ) !runtime.tier.memory.AdmissionLease {
        return self.acquireAmountsWithEviction(backend_class, limits, amounts);
    }

    /// Atomically admits all transient resources for one execution. Speculative
    /// generation must acquire target and draft KV/scratch together so concurrent
    /// requests cannot each pass admission using only a partial estimate.
    pub fn acquireRunResourceEstimates(
        self: *ModelManager,
        requests: []const runtime.tier.memory.AdmissionRequest,
    ) !runtime.tier.memory.AdmissionLease {
        return self.acquireRequestsWithEviction(requests);
    }

    /// Borrow reserved generation workspace before charging the remaining
    /// request bytes. Every failure unwinds the sub-ledgers, including a failed
    /// draft admission, process cap, live-memory check, or cancellation later.
    pub fn acquireGenerationResources(
        self: *ModelManager,
        requests: []const GenerationAdmissionRequest,
    ) !GenerationAdmissionLease {
        if (requests.len == 0 or requests.len > 2) return error.ResourceLimitExceeded;
        var result = GenerationAdmissionLease{};
        errdefer result.release();
        var remaining: [2]runtime.tier.memory.AdmissionRequest = undefined;
        for (requests, 0..) |request, i| {
            remaining[i] = request.resources;
            if (request.session.generation_workspace) |workspace| {
                result.workspace[i] = try workspace.acquire(&remaining[i].amounts);
            }
        }
        // This call also checks hard caps when all KV/scratch is prepaid.
        result.resources = try self.acquireRunResourceEstimates(remaining[0..requests.len]);
        return result;
    }

    pub fn configureAdmissionLimits(
        self: *ModelManager,
        overrides: runtime.tier.memory.Limits,
    ) void {
        std.debug.assert(self.owns_resource_domain);
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(self.composite_assets.count() == 0);
        self.admission_limit_overrides = overrides;
        if (self.resource_domain) |domain|
            self.configureAdmissionController(domain);
    }

    /// Attach both halves of one external process owner atomically. Requiring a
    /// paired configuration prevents a model lease and tokenizer cache from
    /// silently charging different managers.
    pub fn configureExternalResourceBudgets(
        self: *ModelManager,
        admission_budget: runtime.tier.memory.AdmissionResourceBudget,
        tokenizer_budget: hf_tokenizer.HfTokenizer.BpeCacheResourceBudget,
    ) !void {
        std.debug.assert(self.loaded.count() == 0);
        std.debug.assert(self.composite_assets.count() == 0);
        if (self.resource_ownership != .external_required)
            return error.ExternalResourceBudgetInLocalOwnership;
        spinLock(&self.tokenizer_cache_config_mutex);
        defer self.tokenizer_cache_config_mutex.unlock();
        if (self.tokenizer_cache_budget_source != .none)
            return error.ExternalResourceBudgetsAlreadyConfigured;
        if (!admission_budget.hasValidLifetimeHooks() or
            !tokenizer_budget.hasValidLifetimeHooks() or
            admission_budget.retain_context == null or
            tokenizer_budget.retain_context == null)
            return error.ExternalResourceBudgetRequiresLifetimeHooks;
        if (!tokenizer_budget.retainContext())
            return error.ExternalResourceOwnerShuttingDown;
        errdefer tokenizer_budget.releaseContext();
        const lifetime = try self.ensureResourceDomain();
        try lifetime.admission.configureResourceBudget(admission_budget);
        lifetime.external_tokenizer_cache_budget = tokenizer_budget;
        lifetime.tokenizer_cache_budget_source = .external_pair;
        // Ownership pairing changes only provenance. Cache geometry remains
        // the policy selected in NodeConfig/configureTokenizerCaches.
        self.tokenizer_cache_config.resource_budget = lifetime.resourceBudget();
        self.tokenizer_cache_budget_source = .external_pair;
    }

    pub fn configureForcedRunAdmissionDenialsForTesting(
        self: *ModelManager,
        count: usize,
    ) void {
        self.forced_run_admission_denials_for_testing = count;
        if (self.resource_domain) |domain|
            domain.admission.configureForcedRunDenialsForTesting(count);
    }

    fn modelIsInFlightLocked(self: *ModelManager, model: *LoadedModel) bool {
        var it = self.in_flight_loads.valueIterator();
        while (it.next()) |flight_ptr| {
            if (flight_ptr.*.model == model) return true;
        }
        return false;
    }

    fn takeLruModelLocked(
        self: *ModelManager,
        now_ns: u64,
        expired_only: bool,
        admission_pressure: ?runtime.tier.memory.AdmissionPressure,
    ) ?EvictedModel {
        return self.takeLruModelWithWorkspaceLocked(now_ns, expired_only, admission_pressure, session_factory.glinerBoundaryWorkspaceAdmissionAmounts);
    }

    /// The private compile-time reader permits model-free ownership tests.
    /// Production always selects the concrete session factory reader above.
    fn takeLruModelWithWorkspaceLocked(
        self: *ModelManager,
        now_ns: u64,
        expired_only: bool,
        admission_pressure: ?runtime.tier.memory.AdmissionPressure,
        workspace_amounts: anytype,
    ) ?EvictedModel {
        const ttl_ns = std.math.mul(
            u64,
            self.keep_alive_ms,
            std.time.ns_per_ms,
        ) catch std.math.maxInt(u64);
        var victim: ?*LoadedModel = null;
        var oldest_ns: u64 = std.math.maxInt(u64);
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model.active_handles != 0 or model.pinned or
                self.modelIsInFlightLocked(model))
            {
                continue;
            }
            if (admission_pressure) |pressure| {
                if (!loadedModelAdmissionReclaimRelevant(model, pressure)) {
                    // This slot can change during execution. Only inspect it
                    // after proving no active/pending owner under this cache
                    // lock, which also prevents any new handle acquisition.
                    const amounts = workspace_amounts(model.session);
                    var by_backend: @FieldType(runtime.tier.memory.AdmissionLease, "amounts_by_backend") = @splat(.{});
                    by_backend[@backingInt(runtime.tier.memory.BackendClass.gpu)] = amounts;
                    if (!admissionAmountsReclaimRelevant(amounts, by_backend, pressure)) continue;
                }
            }
            const age_ns = if (now_ns >= model.last_used_ns)
                now_ns - model.last_used_ns
            else
                0;
            if (expired_only and
                (self.keep_alive_ms == 0 or age_ns < ttl_ns))
            {
                continue;
            }
            if (victim == null or model.last_used_ns < oldest_ns) {
                victim = model;
                oldest_ns = model.last_used_ns;
            }
        }
        return self.takeLoadedModelLocked(victim orelse return null);
    }

    fn takeLoadedModelLocked(
        self: *ModelManager,
        selected: *LoadedModel,
    ) ?EvictedModel {
        var canonical_key: ?[]const u8 = null;
        var loaded_it = self.loaded.iterator();
        while (loaded_it.next()) |entry| {
            if (entry.value_ptr.* == selected) {
                canonical_key = entry.key_ptr.*;
                break;
            }
        }
        const removed = self.loaded.fetchRemove(canonical_key orelse return null) orelse unreachable;
        std.debug.assert(removed.value == selected);

        // Aliases never own the model, but every alias must disappear in the
        // same map critical section as its canonical backend-variant key.
        while (true) {
            var alias_key: ?[]const u8 = null;
            var alias_it = self.loaded_aliases.iterator();
            while (alias_it.next()) |entry| {
                if (entry.value_ptr.* == selected) {
                    alias_key = entry.key_ptr.*;
                    break;
                }
            }
            const key = alias_key orelse break;
            const alias = self.loaded_aliases.fetchRemove(key) orelse unreachable;
            self.allocator.free(alias.key);
        }
        return .{ .key = removed.key, .model = selected };
    }

    /// Remove a runtime that returned an integrity error from future lookup.
    /// Destruction is deferred to the final active handle, so concurrent
    /// requests never observe freed session state.
    fn retireLoadedModel(self: *ModelManager, model: *LoadedModel) void {
        spinLock(&self.eviction_lock);
        defer self.eviction_lock.unlock();

        self.lockLoadedModels();
        if (model.retired) {
            self.unlockLoadedModels();
            return;
        }
        const retired = self.takeLoadedModelLocked(model) orelse {
            self.unlockLoadedModels();
            return;
        };
        std.debug.assert(model.active_handles > 0);
        model.retired = true;
        model.pinned = false;

        // A completed cold-load flight may still have waiters that have not
        // adopted their model handles. Reserve those handles before detaching
        // the old flight, so a replacement load can publish under the same key
        // without freeing state an existing waiter is about to use.
        while (true) {
            var flight_key: ?[]const u8 = null;
            var flight: ?*LoadFlight = null;
            var flight_it = self.in_flight_loads.iterator();
            while (flight_it.next()) |entry| {
                if (entry.value_ptr.*.model == model) {
                    flight_key = entry.key_ptr.*;
                    flight = entry.value_ptr.*;
                    break;
                }
            }
            const pending = flight orelse break;
            std.debug.assert(!pending.handles_reserved);
            const waiter_refs = pending.unadoptedWaiterRefs();
            std.debug.assert(std.math.maxInt(usize) - model.active_handles >= waiter_refs);
            model.active_handles += waiter_refs;
            pending.handles_reserved = true;
            const detached = self.in_flight_loads.fetchRemove(flight_key.?) orelse unreachable;
            std.debug.assert(detached.value == pending);
            pending.registered = false;
            self.allocator.free(detached.key);
        }
        self.unlockLoadedModels();

        std.log.warn("retiring failed inference model path={s} backend={s}", .{
            model.model_dir,
            @tagName(model.session.backend()),
        });
        self.allocator.free(retired.key);
    }

    fn destroyRetiredModel(self: *ModelManager, model: *LoadedModel) void {
        std.debug.assert(model.retired);
        model.deinit();
        self.allocator.destroy(model);
        _ = platform.allocator.reclaimUnusedProcessMemory();
    }

    fn destroyEvictedModel(self: *ModelManager, evicted: EvictedModel) void {
        std.log.info("evicting inference model path={s} backend={s}", .{
            evicted.model.model_dir,
            @tagName(evicted.model.session.backend()),
        });
        evicted.model.deinit();
        self.allocator.destroy(evicted.model);
        self.allocator.free(evicted.key);
        _ = platform.allocator.reclaimUnusedProcessMemory();
    }

    fn takeLruCompositeAssetsLocked(
        self: *ModelManager,
        now_ns: u64,
        expired_only: bool,
    ) ?EvictedCompositeAssets {
        const ttl_ns = std.math.mul(
            u64,
            self.keep_alive_ms,
            std.time.ns_per_ms,
        ) catch std.math.maxInt(u64);
        var victim_key: ?ComponentPlanKey = null;
        var victim: ?*CompositeAssets = null;
        var oldest_ns: u64 = std.math.maxInt(u64);
        var it = self.composite_assets.iterator();
        while (it.next()) |entry| {
            const assets = entry.value_ptr.*;
            if (assets.startup_managed or assets.active_handles != 0 or self.compositeAssetsIsInFlightLocked(assets)) continue;
            const age_ns = if (now_ns >= assets.last_used_ns)
                now_ns - assets.last_used_ns
            else
                0;
            if (expired_only and
                (self.keep_alive_ms == 0 or age_ns < ttl_ns))
            {
                continue;
            }
            if (victim == null or assets.last_used_ns < oldest_ns) {
                victim_key = entry.key_ptr.*;
                victim = assets;
                oldest_ns = assets.last_used_ns;
            }
        }
        const selected = victim orelse return null;
        const removed = self.composite_assets.fetchRemove(victim_key.?) orelse unreachable;
        std.debug.assert(removed.value == selected);
        return .{ .key = removed.key, .assets = selected };
    }

    fn compositeAssetsIsInFlightLocked(
        self: *ModelManager,
        assets: *CompositeAssets,
    ) bool {
        var it = self.in_flight_composite_assets.valueIterator();
        while (it.next()) |flight_ptr| {
            if (flight_ptr.*.assets == assets) return true;
        }
        return false;
    }

    fn destroyEvictedCompositeAssets(
        self: *ModelManager,
        evicted: EvictedCompositeAssets,
    ) void {
        evicted.assets.deinit();
        self.allocator.destroy(evicted.assets);
        _ = platform.allocator.reclaimUnusedProcessMemory();
    }

    fn admissionAmountsPresent(amounts: runtime.tier.memory.AdmissionAmounts) bool {
        return amounts.hostTotalBytes() > 0 or amounts.backendTotalBytes() > 0;
    }

    fn admissionReclaimRelevant(
        reclaimable: runtime.tier.memory.AdmissionLease,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        return admissionAmountsReclaimRelevant(reclaimable.amounts, reclaimable.amounts_by_backend, pressure);
    }

    fn admissionAmountsReclaimRelevant(
        amounts: runtime.tier.memory.AdmissionAmounts,
        amounts_by_backend: @FieldType(runtime.tier.memory.AdmissionLease, "amounts_by_backend"),
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        return switch (pressure) {
            .shared_host => amounts.hostTotalBytes() > 0,
            .shared_unified => admissionAmountsPresent(amounts),
            .shared_kv => amounts.kvTotalBytes() > 0,
            .shared_scratch => amounts.scratchTotalBytes() > 0,
            .live_host => amounts.hostTotalBytes() > 0 or
                (builtin.os.tag == .macos and amounts.backendTotalBytes() > 0),
            .domain_host => |backend_class| amounts_by_backend[@backingInt(backend_class)].hostTotalBytes() > 0,
            .domain_backend => |backend_class| amounts_by_backend[@backingInt(backend_class)].backendTotalBytes() > 0,
            .domain_combined => |backend_class| admissionAmountsPresent(
                amounts_by_backend[@backingInt(backend_class)],
            ),
            .domain_kv => |backend_class| amounts_by_backend[@backingInt(backend_class)].kvTotalBytes() > 0,
            .domain_scratch => |backend_class| amounts_by_backend[@backingInt(backend_class)].scratchTotalBytes() > 0,
            // The process-owner budget is intentionally opaque. Any resident
            // admission released from the aggregate can potentially satisfy it.
            .external_budget => admissionAmountsPresent(amounts),
        };
    }

    fn loadedModelAdmissionReclaimRelevant(
        model: *const LoadedModel,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        const lease_slots = [_]*const ?runtime.tier.memory.AdmissionLease{
            &model.resource_lease,
            &model.tokenizer_resource_lease,
            &model.whisper_prompt_admission,
            &model.vision_resource_lease,
            &model.audio_resource_lease,
            &model.text_projection_resource_lease,
            &model.visual_projection_resource_lease,
            &model.audio_projection_resource_lease,
        };
        for (lease_slots) |lease_slot| {
            const lease = lease_slot.* orelse continue;
            if (admissionReclaimRelevant(lease, pressure)) return true;
        }
        return false;
    }

    fn takeLruCompositeAssetsForAdmissionLocked(
        self: *ModelManager,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) ?EvictedCompositeAssets {
        var victim_key: ?ComponentPlanKey = null;
        var victim: ?*CompositeAssets = null;
        var oldest_ns: u64 = std.math.maxInt(u64);
        var it = self.composite_assets.iterator();
        while (it.next()) |entry| {
            const assets = entry.value_ptr.*;
            if (assets.startup_managed or assets.active_handles != 0 or self.compositeAssetsIsInFlightLocked(assets)) continue;
            var relevant = false;
            for (assets.reclaimableAdmissions()) |maybe_lease| {
                if (maybe_lease) |lease| if (admissionReclaimRelevant(lease, pressure)) {
                    relevant = true;
                    break;
                };
            }
            if (!relevant) continue;
            if (victim == null or assets.last_used_ns < oldest_ns) {
                victim_key = entry.key_ptr.*;
                victim = assets;
                oldest_ns = assets.last_used_ns;
            }
        }
        const selected = victim orelse return null;
        const removed = self.composite_assets.fetchRemove(victim_key.?) orelse unreachable;
        std.debug.assert(removed.value == selected);
        return .{ .key = removed.key, .assets = selected };
    }

    /// Called with eviction_lock held. Reclaim matching lightweight tokenizer
    /// state before tearing down a heavyweight model. The admission attempt is
    /// retried after every victim, so a model is still selected immediately when
    /// tokenizer residency cannot relieve the constrained resource class.
    fn evictOneIdleForAdmission(
        self: *ModelManager,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        self.lockLoadedModels();
        const now_ns = platform.time.monotonicNs();
        const assets = self.takeLruCompositeAssetsForAdmissionLocked(pressure);
        const model = if (assets == null)
            self.takeLruModelLocked(now_ns, false, pressure)
        else
            null;
        self.unlockLoadedModels();
        if (assets) |evicted| {
            self.destroyEvictedCompositeAssets(evicted);
            return true;
        }
        if (model) |evicted| {
            self.destroyEvictedModel(evicted);
            return true;
        }
        return false;
    }

    fn pressureCanBeRelievedByHostCache(
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        return switch (pressure) {
            .shared_host, .shared_unified, .external_budget, .live_host => true,
            .domain_host, .domain_combined => true,
            .domain_backend, .domain_kv, .domain_scratch, .shared_kv, .shared_scratch => false,
        };
    }

    /// Called with eviction_lock held after no idle model remains. Active
    /// native/PJRT sessions may still own cold, unpinned lazy weights. Reclaim
    /// one entry and let the caller re-probe the authoritative controller;
    /// never infer success from cache counters alone.
    fn reclaimOneActiveCacheForAdmission(
        self: *ModelManager,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        if (!pressureCanBeRelievedByHostCache(pressure)) return false;

        self.lockLoadedModels();
        var loaded_locked = true;
        defer if (loaded_locked) self.unlockLoadedModels();
        var candidate: ?*LoadedModel = null;
        var oldest_ns: u64 = std.math.maxInt(u64);
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model.retired or self.modelIsInFlightLocked(model)) continue;
            if (!loadedModelAdmissionReclaimRelevant(model, pressure)) continue;
            if (candidate == null or model.last_used_ns < oldest_ns) {
                candidate = model;
                oldest_ns = model.last_used_ns;
            }
        }
        var model = candidate orelse return false;
        var released_bytes = if (model.session.model_lifetime != null)
            session_factory.reclaimOneHostCacheEntryForSession(model.session)
        else
            0;
        var settlement_pending = session_factory.hostCacheCreditSettlementPendingForSession(model.session);
        if (released_bytes == 0 and !settlement_pending) {
            // A session may have no unpinned victim at this instant. Do not let
            // one busy model hide reclaimable capacity in another model.
            var fallback_it = self.loaded.iterator();
            while (fallback_it.next()) |entry| {
                const fallback = entry.value_ptr.*;
                if (fallback == model or fallback.retired or
                    self.modelIsInFlightLocked(fallback) or
                    !loadedModelAdmissionReclaimRelevant(fallback, pressure))
                {
                    continue;
                }
                if (fallback.session.model_lifetime == null) continue;
                released_bytes = session_factory.reclaimOneHostCacheEntryForSession(fallback.session);
                const pending = session_factory.hostCacheCreditSettlementPendingForSession(fallback.session);
                if (released_bytes != 0 or pending) {
                    settlement_pending = pending;
                    model = fallback;
                    break;
                }
            }
        }
        if (released_bytes == 0 and !settlement_pending) return false;
        std.log.info(
            "reclaimed inference host cache path={s} backend={s} bytes={d} pressure={s}",
            .{
                model.model_dir,
                @tagName(model.session.backend()),
                released_bytes,
                @tagName(pressure),
            },
        );
        // LoadedModel/path metadata may retire during a reentrant callback.
        // Copy only its existing Session and retain the actual Model BEFORE
        // unlocking. Payload has already died under Native's residency guard;
        // its exact ledger remains backed until this outside settlement.
        const reclaimed_session = model.session;
        const model_pin = reclaimed_session.model_lifetime.?.retain();
        loaded_locked = false;
        const settled = self.finishReclaimedHostCacheOutsideLocks(reclaimed_session, model_pin, session_factory.flushHostCacheReleasesOutsideExecutionForSession);
        return released_bytes != 0 or settled;
    }

    /// Entry owns loaded-models+eviction locks and one actual Model pin. Exit
    /// owns eviction only. Payload selection/removal is already complete; the
    /// synchronous pin protects metadata through a callback that can retire the
    /// loaded identity. No callbacks may run until BOTH Manager locks are off.
    fn finishReclaimedHostCacheOutsideLocks(self: *ModelManager, reclaimed_session: backends.Session, model_pin: *backends.Session.ModelLifetime, comptime settle: anytype) bool {
        self.unlockLoadedModels();
        self.eviction_lock.unlock();
        defer spinLock(&self.eviction_lock);
        const settled = settle(reclaimed_session) catch false;
        const final_model_drop = model_pin.references.load(.acquire) == 1;
        // This may be the actual final Model drop after reentrant retirement.
        // It too occurs outside both Manager locks, before the loop resumes.
        model_pin.release();
        _ = platform.allocator.reclaimUnusedProcessMemory();
        return settled or final_model_drop;
    }

    fn reclaimOneForAdmission(
        self: *ModelManager,
        pressure: runtime.tier.memory.AdmissionPressure,
    ) bool {
        const cold_host_cache_can_help = switch (pressure) {
            .domain_scratch => true, // retained preflight/index leases
            else => pressureCanBeRelievedByHostCache(pressure),
        };
        if (cold_host_cache_can_help and self.reclaimHostCache(1) != 0) return true;
        if (self.evictOneIdleForAdmission(pressure)) return true;
        return self.reclaimOneActiveCacheForAdmission(pressure);
    }

    fn tightenUnifiedDeviceAdmission(self: *const ModelManager) void {
        if (builtin.is_test) if (self.test_unified_limit_probe) |probe| {
            self.admissionController().tightenUnifiedLimit(probe(self.process_memory_limit_bytes, self.admission_limit_overrides.combined_limit_bytes));
            return;
        };
        if (backends.cudaUsesUnifiedMemory())
            self.admissionController().tightenUnifiedLimit(unifiedDeviceAdmissionLimit(self.process_memory_limit_bytes, self.admission_limit_overrides.combined_limit_bytes));
    }

    fn physicalRunLimits(self: *const ModelManager, backend_class: runtime.tier.memory.BackendClass, limits: runtime.tier.memory.Limits) runtime.tier.memory.Limits {
        if (backend_class != .gpu) return limits;
        return backends.clampPhysicalMemoryLimits(.cuda, limits, self.process_memory_limit_bytes);
    }

    fn acquireAmountsWithEviction(
        self: *ModelManager,
        backend_class: runtime.tier.memory.BackendClass,
        requested_limits: runtime.tier.memory.Limits,
        amounts: runtime.tier.memory.AdmissionAmounts,
    ) !runtime.tier.memory.AdmissionLease {
        return self.acquireAmountsWithEvictionMode(backend_class, requested_limits, amounts, true);
    }

    pub fn acquireMetadataAmounts(self: *ModelManager, limits: runtime.tier.memory.Limits, bytes: usize) !runtime.tier.memory.AdmissionLease {
        return self.acquireAmountsWithEvictionMode(.cpu, limits, .{ .host_weight_bytes = bytes }, false);
    }

    pub fn reserveMetadataCapacity(self: *ModelManager, bytes: usize) !runtime.tier.memory.LiveReservation {
        try self.ensureResourceOwnerReady();
        return self.admissionController().reserveLiveCapacity(bytes) catch |err| {
            if (err != error.ResourceTemporarilyUnavailable) return err;
            spinLock(&self.eviction_lock);
            defer self.eviction_lock.unlock();
            while (true) {
                if (self.admissionController().reserveLiveCapacity(bytes)) |reservation| return reservation else |retry| {
                    if (retry != error.ResourceTemporarilyUnavailable) return retry;
                }
                if (!self.reclaimOneForAdmission(.live_host)) return error.ResourceTemporarilyUnavailable;
            }
        };
    }

    fn acquireAmountsWithEvictionMode(self: *ModelManager, backend_class: runtime.tier.memory.BackendClass, requested_limits: runtime.tier.memory.Limits, amounts: runtime.tier.memory.AdmissionAmounts, check_live: bool) !runtime.tier.memory.AdmissionLease {
        try self.ensureResourceOwnerReady();
        const limits = self.physicalRunLimits(backend_class, requested_limits);
        // Metadata is a suballocation of an enclosing, already configured
        // envelope (Budget.create acquires one with check_live=true). Avoid
        // repeating stable-limit OS probes for every parser node; live physical
        // capacity is checked separately by metadata's batched reservations.
        if (check_live) self.tightenUnifiedDeviceAdmission();
        var pressure: ?runtime.tier.memory.AdmissionPressure = null;
        const requests = [_]runtime.tier.memory.AdmissionRequest{.{
            .backend_class = backend_class,
            .limits = limits,
            .amounts = amounts,
        }};
        return self.admissionController().tryAcquireWithPressure(
            backend_class,
            limits,
            amounts,
            check_live,
            &pressure,
        ) catch |first_err| switch (first_err) {
            error.ResourceTemporarilyUnavailable => {
                spinLock(&self.eviction_lock);
                defer self.eviction_lock.unlock();
                while (true) {
                    if (self.admissionController().tryAcquireWithPressure(
                        backend_class,
                        limits,
                        amounts,
                        check_live,
                        &pressure,
                    )) |lease| return lease else |retry_err| switch (retry_err) {
                        error.ResourceTemporarilyUnavailable => {},
                        else => {
                            self.logRunAdmissionDenied(retry_err, pressure, &requests);
                            return retry_err;
                        },
                    }
                    if (!self.reclaimOneForAdmission(
                        pressure orelse return error.ResourceTemporarilyUnavailable,
                    )) {
                        self.logRunAdmissionDenied(error.ResourceTemporarilyUnavailable, pressure, &requests);
                        return error.ResourceTemporarilyUnavailable;
                    }
                }
            },
            else => {
                self.logRunAdmissionDenied(first_err, pressure, &requests);
                return first_err;
            },
        };
    }

    fn acquireRequestsWithEviction(
        self: *ModelManager,
        requested: []const runtime.tier.memory.AdmissionRequest,
    ) !runtime.tier.memory.AdmissionLease {
        try self.ensureResourceOwnerReady();
        const requests = try self.allocator.dupe(runtime.tier.memory.AdmissionRequest, requested);
        defer self.allocator.free(requests);
        for (requests) |*request| request.limits = self.physicalRunLimits(request.backend_class, request.limits);
        self.tightenUnifiedDeviceAdmission();
        var pressure: ?runtime.tier.memory.AdmissionPressure = null;
        return self.admissionController().tryAcquireRequestsWithPressure(
            requests,
            true,
            &pressure,
        ) catch |first_err| switch (first_err) {
            error.ResourceTemporarilyUnavailable => {
                spinLock(&self.eviction_lock);
                defer self.eviction_lock.unlock();
                while (true) {
                    if (self.admissionController().tryAcquireRequestsWithPressure(
                        requests,
                        true,
                        &pressure,
                    )) |lease|
                        return lease
                    else |retry_err| switch (retry_err) {
                        error.ResourceTemporarilyUnavailable => {},
                        else => {
                            self.logRunAdmissionDenied(retry_err, pressure, requests);
                            return retry_err;
                        },
                    }
                    if (!self.reclaimOneForAdmission(
                        pressure orelse return error.ResourceTemporarilyUnavailable,
                    )) {
                        self.logRunAdmissionDenied(error.ResourceTemporarilyUnavailable, pressure, requests);
                        return error.ResourceTemporarilyUnavailable;
                    }
                }
            },
            else => {
                self.logRunAdmissionDenied(first_err, pressure, requests);
                return first_err;
            },
        };
    }

    /// Keep client responses deliberately generic while leaving enough
    /// operator evidence to distinguish stable-policy saturation from current
    /// live-memory pressure. Artifact paths and request contents are excluded.
    fn observeSessionAdmissionDenied(raw: *anyopaque, err: anyerror, pressure: ?runtime.tier.memory.AdmissionPressure, request: runtime.tier.memory.AdmissionRequest) void {
        const self: *ModelManager = @ptrCast(@alignCast(raw));
        self.logRunAdmissionDenied(err, pressure, &.{request});
    }

    /// Count every denial; sample memory and emit at most one batch per second.
    fn logRunAdmissionDenied(
        self: *ModelManager,
        err: anyerror,
        pressure: ?runtime.tier.memory.AdmissionPressure,
        requests: []const runtime.tier.memory.AdmissionRequest,
    ) void {
        if (err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable) return;
        const admitted = self.admissionController().snapshot();
        const shared_limits = self.admissionController().sharedLimitsSnapshot();
        var diagnostic: MemoryAdmissionDiagnostic = .{
            .shared_host_limit_bytes = shared_limits.host_limit_bytes,
            .shared_unified_limit_bytes = shared_limits.unified_limit_bytes,
            .error_code = @errorName(err),
            .retryable = err == error.ResourceTemporarilyUnavailable,
            .pressure = if (pressure) |value| @tagName(std.meta.activeTag(value)) else "unknown",
            .last_denied_ns = platform.time.monotonicNs(),
            .request_count = requests.len,
            .admitted_host_bytes = admitted.hostTotalBytes(),
            .admitted_backend_bytes = admitted.backendTotalBytes(),
        };
        for (requests) |request| {
            diagnostic.requested_host_bytes +|= request.amounts.hostTotalBytes();
            diagnostic.requested_backend_bytes +|= request.amounts.backendTotalBytes();
            diagnostic.requested_kv_bytes +|= request.amounts.kvTotalBytes();
            diagnostic.requested_scratch_bytes +|= request.amounts.scratchTotalBytes();
        }
        if (requests.len > 0) {
            diagnostic.host_limit_bytes = requests[0].limits.host_limit_bytes;
            diagnostic.backend_limit_bytes = requests[0].limits.backend_limit_bytes;
            diagnostic.combined_limit_bytes = requests[0].limits.combined_limit_bytes;
            diagnostic.kv_limit_bytes = requests[0].limits.kv_limit_bytes;
            diagnostic.scratch_limit_bytes = requests[0].limits.scratch_limit_bytes;
        }
        diagnostic = self.admission_diagnostics.record(diagnostic, self, sampleAdmissionAvailableMemory) orelse return;
        if (builtin.is_test) return;
        std.log.warn(
            "inference run admission denied error={s} retryable={} pressure={s} denied_count={d} request_count={d} admitted_host_bytes={d} admitted_backend_bytes={d} admitted_kv_bytes={d} admitted_scratch_bytes={d} shared_host_limit_bytes={d} shared_unified_limit_bytes={d}",
            .{
                @errorName(err),
                diagnostic.retryable,
                if (pressure) |value| @tagName(std.meta.activeTag(value)) else "unknown",
                diagnostic.denied_count,
                requests.len,
                admitted.hostTotalBytes(),
                admitted.backendTotalBytes(),
                admitted.kvTotalBytes(),
                admitted.scratchTotalBytes(),
                shared_limits.host_limit_bytes,
                shared_limits.unified_limit_bytes,
            },
        );
        for (requests, 0..) |request, index| {
            std.log.warn(
                "inference run admission request index={d} backend={s} host_bytes={d} backend_bytes={d} kv_bytes={d} scratch_bytes={d} host_limit_bytes={d} backend_limit_bytes={d} combined_limit_bytes={d} kv_limit_bytes={d} scratch_limit_bytes={d}",
                .{
                    index,
                    @tagName(request.backend_class),
                    request.amounts.hostTotalBytes(),
                    request.amounts.backendTotalBytes(),
                    request.amounts.kvTotalBytes(),
                    request.amounts.scratchTotalBytes(),
                    request.limits.host_limit_bytes,
                    request.limits.backend_limit_bytes,
                    request.limits.combined_limit_bytes,
                    request.limits.kv_limit_bytes,
                    request.limits.scratch_limit_bytes,
                },
            );
        }
    }

    fn sampleAdmissionAvailableMemory(self: *ModelManager) ?u64 {
        if (builtin.is_test) return null;
        const info = runtime.tier.memory.currentSystemMemoryInfoForLimit(self.process_memory_limit_bytes, self.process_memory_limit_provenance) orelse return null;
        const available = info.available_bytes orelse return null;
        return @intCast(available);
    }

    pub fn memoryAdmissionDiagnostic(self: *ModelManager) MemoryAdmissionDiagnostic {
        return self.admission_diagnostics.snapshot();
    }

    fn evictExpired(self: *ModelManager) void {
        self.evictExpiredAt(null);
    }

    /// The ordinary maintenance loop samples the real clock under the same
    /// eviction lock as before. A private test bridge may supply an exact
    /// timestamp without changing model timestamps or exposing a runtime knob.
    fn evictExpiredAt(self: *ModelManager, supplied_now_ns: ?u64) void {
        if (self.keep_alive_ms == 0) return;
        spinLock(&self.eviction_lock);
        defer self.eviction_lock.unlock();
        const now_ns = supplied_now_ns orelse platform.time.monotonicNs();
        while (true) {
            self.lockLoadedModels();
            const evicted = self.takeLruModelLocked(now_ns, true, null);
            self.unlockLoadedModels();
            if (evicted == null) break;
            self.destroyEvictedModel(evicted.?);
        }
        while (true) {
            self.lockLoadedModels();
            const evicted = self.takeLruCompositeAssetsLocked(now_ns, true);
            self.unlockLoadedModels();
            if (evicted == null) break;
            self.destroyEvictedCompositeAssets(evicted.?);
        }
    }

    fn evictionLoop(self: *ModelManager, io: std.Io) std.Io.Cancelable!void {
        const interval_ms = @max(
            @as(u64, 10),
            @min(@max(self.keep_alive_ms / 4, 1), @as(u64, 30_000)),
        );
        while (true) {
            self.eviction_stop.waitTimeout(io, .{ .duration = .{
                .raw = .fromMilliseconds(@intCast(interval_ms)),
                .clock = .awake,
            } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => return error.Canceled,
            };
            if (self.eviction_stop.isSet()) return;
            self.evictExpired();
        }
    }

    /// A short-lived, policy-validated handle for loading every graph in a
    /// composite model bundle. Creating the loader evaluates serving policy
    /// once; every session then uses the same allowed backend order, injected
    /// graph runtime, and process-wide admission controller.
    /// Resource-protected constructors used by immutable model component plans.
    /// Serving policy validation remains in componentLoaderForPaths.
    pub fn componentLoadServices(self: *ModelManager) @import("../models/component_loader.zig").Services {
        const Adapter = struct {
            fn load(raw: *anyopaque, path: []const u8, preferred: []const backends.BackendType, shared: ?*backends.imported_onnx_session.SharedBackendContext, control: ?InferenceExecutionControl, selected: ResolvedModel, artifacts: ?*PublishedArtifacts) !ManagedSession {
                const manager: *ModelManager = @ptrCast(@alignCast(raw));
                return manager.loadManagedSessionWithAdmission(path, preferred, shared, control, selected, artifacts);
            }
            fn tokenizer(raw: *anyopaque, path: []const u8, artifacts: ?*const PublishedArtifacts) !ManagedHfTokenizer {
                const manager: *ModelManager = @ptrCast(@alignCast(raw));
                return manager.loadManagedHfTokenizerFileWithArtifacts(path, artifacts);
            }
        };
        return .{ .allocator = self.allocator, .io = self.session_manager.io orelse std.Options.debug_io, .context = self, .load_session_fn = Adapter.load, .load_tokenizer_fn = Adapter.tokenizer };
    }

    pub const ComponentLoader = @import("../models/component_loader.zig").ComponentLoader;

    fn componentPlanIo(self: *ModelManager) !std.Io {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        return self.session_manager.io orelse error.MissingIoRuntime;
    }

    fn applyCachedComponentPlan(
        self: *ModelManager,
        key: ComponentPlanKey,
        loader: *ComponentLoader,
    ) !bool {
        const entry = try self.cachedComponentPlan(key) orelse return false;
        defer entry.release();
        @memcpy(loader.allowed_backends[0..entry.allowed_backend_count], entry.allowed_backends[0..entry.allowed_backend_count]);
        loader.allowed_backend_count = entry.allowed_backend_count;
        loader.validated_generation = entry.signature;
        return true;
    }

    fn cachedComponentPlan(self: *ModelManager, key: ComponentPlanKey) !?*ComponentPlanCacheEntry {
        spinLock(&self.component_plan_cache_lock);
        const entry = self.component_plan_cache.get(key) orelse {
            self.component_plan_cache_lock.unlock();
            return null;
        };
        entry.retain();
        self.component_plan_cache_lock.unlock();
        errdefer entry.release();

        const signature = try componentDependencySignature(
            try self.componentPlanIo(),
            entry.dependencies,
        );
        if (!std.mem.eql(u8, signature[0..], entry.signature[0..])) {
            entry.release();
            return null;
        }
        return entry;
    }

    fn publishComponentPlan(
        self: *ModelManager,
        key: ComponentPlanKey,
        signature: ComponentPlanKey,
        allowed: []const backends.BackendType,
        dependencies: []const []const u8,
    ) !void {
        const entry = try ComponentPlanCacheEntry.create(
            self.allocator,
            signature,
            allowed,
            dependencies,
        );
        var replaced: ?*ComponentPlanCacheEntry = null;
        var evicted: ?*ComponentPlanCacheEntry = null;

        spinLock(&self.component_plan_cache_lock);
        if (!self.component_plan_cache.contains(key) and
            self.component_plan_cache.count() >= component_plan_cache_capacity)
        {
            var it = self.component_plan_cache.iterator();
            if (it.next()) |candidate| {
                const removed = self.component_plan_cache.fetchRemove(candidate.key_ptr.*);
                if (removed) |old| evicted = old.value;
            }
        }
        const previous = self.component_plan_cache.fetchPut(
            self.allocator,
            key,
            entry,
        ) catch |err| {
            self.component_plan_cache_lock.unlock();
            entry.release();
            if (evicted) |old| old.release();
            return err;
        };
        if (previous) |old| replaced = old.value;
        self.component_plan_cache_lock.unlock();
        if (replaced) |old| old.release();
        if (evicted) |old| old.release();
    }

    pub fn componentLoaderForPaths(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        component_paths: []const []const u8,
    ) !ComponentLoader {
        return self.componentLoaderForPathsWithContract(
            model_dir,
            preferred_backends,
            component_paths,
            .manifest,
        );
    }

    pub fn componentLoaderForPathsWithContract(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        component_paths: []const []const u8,
        contract: ComponentContract,
    ) !ComponentLoader {
        var required_backend_scratch: [1]backends.BackendType = undefined;
        const effective_backends = try self.session_manager.requiredBackendCandidates(
            preferred_backends,
            &required_backend_scratch,
        );
        var loader = ComponentLoader{ .services = self.componentLoadServices() };
        for (component_paths) |path| try loader.addComponentPath(path);
        if (loader.component_path_count == 0) return error.IncompleteModelBundle;
        var man = try manifest_mod.loadFromDir(self.allocator, model_dir);
        defer man.deinit();
        try loader.bindOwner(model_dir, man.managed_publication);
        const allowed = if (self.serving_policy) |policy| blk: {
            const key = componentPlanKey(
                model_dir,
                &man,
                effective_backends,
                component_paths,
                policy,
                contract,
            );
            if (try self.applyCachedComponentPlan(key, &loader))
                return loader;

            var inspection = try inspectComponentArtifacts(
                self.allocator,
                &man,
                component_paths,
                contract,
            );
            defer inspection.deinit();
            const signature_before = try componentDependencySignature(
                try self.componentPlanIo(),
                inspection.dependencies.items,
            );
            try validateComponentNativeArtifacts(
                self.allocator,
                component_paths,
                effective_backends,
                &inspection,
            );
            try validateComponentImportedGraphs(
                self.allocator,
                component_paths,
                effective_backends,
                &inspection,
            );
            const signature_after = try componentDependencySignature(
                try self.componentPlanIo(),
                inspection.dependencies.items,
            );
            if (!std.mem.eql(
                u8,
                signature_before[0..],
                signature_after[0..],
            )) return error.ModelArtifactsChanging;
            const validated = try policyAllowedComponentBackendsFromInspection(
                &loader.allowed_backends,
                effective_backends,
                policy,
                &inspection,
            );
            self.publishComponentPlan(
                key,
                signature_after,
                validated,
                inspection.dependencies.items,
            ) catch {};
            loader.validated_generation = signature_after;
            break :blk validated;
        } else effective_backends;
        for (allowed) |backend| {
            if (!backend.supportsDirectSessionLoad()) continue;
            if (loader.allowed_backend_count == loader.allowed_backends.len) break;
            loader.allowed_backends[loader.allowed_backend_count] = backend;
            loader.allowed_backend_count += 1;
        }
        if (loader.allowed_backend_count == 0) return error.NoBackendAvailable;
        return loader;
    }

    fn admissionLimitsForBackend(
        self: *const ModelManager,
        backend_runtime: backends.BackendRuntime,
    ) runtime.tier.memory.Limits {
        var limits = backends.defaultMemoryLimits(
            backend_runtime.backend,
            self.process_memory_limit_bytes,
        );
        limits = runtime.tier.memory.applyLimitOverrides(limits, self.admission_limit_overrides);
        return backends.clampPhysicalMemoryLimits(backend_runtime.backend, limits, self.process_memory_limit_bytes);
    }

    /// Apply architecture/artifact residency floors before attaching the hard
    /// serving envelope. Explicit per-bucket operator overrides remain the
    /// final authority; the model floor only widens automatically derived
    /// defaults such as the generic Metal host-cache allowance.
    fn admissionLimitsForModelSource(
        self: *const ModelManager,
        source: ModelSource,
        backend_runtime: backends.BackendRuntime,
        load_plan_floor: runtime.tier.memory.Limits,
    ) !runtime.tier.memory.Limits {
        var limits = backends.defaultMemoryLimits(
            backend_runtime.backend,
            self.process_memory_limit_bytes,
        );
        if (source.kind != .onnx) {
            if (source.manifest) |manifest| limits = try session_factory.widenBudgetLimitsForManifest(self.allocator, source.path, manifest.*, limits, backend_runtime.backend);
        }
        limits = runtime.tier.memory.maxCompositeLimits(limits, load_plan_floor);
        const resolved = backends.clampPhysicalMemoryLimits(backend_runtime.backend, runtime.tier.memory.applyLimitOverrides(limits, self.admission_limit_overrides), self.process_memory_limit_bytes);
        std.log.info("inference memory policy backend={s} requested={any} resolved={any} process_max_bytes={d} cuda_device_bytes={d} unified={} automatic_fallback={}", .{
            @tagName(backend_runtime.backend),                                               self.admission_limit_overrides,                          resolved,
            self.process_memory_limit_bytes,                                                 backends.cudaDeviceMemoryBytes(backend_runtime.backend), backends.cudaUsesUnifiedMemory(),
            backend_runtime.backend == .cuda and backends.cudaDeviceMemoryBytes(.cuda) == 0,
        });
        return resolved;
    }

    /// Architecture sessions may carry a minimum safe cache/workspace floor
    /// that cannot be known until their config has been parsed. Apply that
    /// floor before attaching serving admission, then reapply explicit node
    /// overrides so an operator hard cap always remains authoritative.
    fn admissionLimitsForSession(
        self: *const ModelManager,
        backend_runtime: backends.BackendRuntime,
        session: backends.Session,
    ) !runtime.tier.memory.Limits {
        const limits = try session_factory.resolveBudgetLimitsForSessions(&.{session}, backends.defaultMemoryLimits(
            backend_runtime.backend,
            self.process_memory_limit_bytes,
        ), self.admission_limit_overrides);
        return backends.clampPhysicalMemoryLimits(backend_runtime.backend, limits, self.process_memory_limit_bytes);
    }

    /// Lazily loaded composite-model components participate in the same
    /// process-wide admission accounting as the primary session. Reserve their
    /// construction peak immediately before import, then retain only completed
    /// residency for the component lifetime.
    fn loadManagedSessionWithAdmission(
        self: *ModelManager,
        model_path: []const u8,
        preferred_backends: []const backends.BackendType,
        shared_backend_ctx: ?*backends.imported_onnx_session.SharedBackendContext,
        control: ?InferenceExecutionControl,
        selected: ResolvedModel,
        borrowed: ?*PublishedArtifacts,
    ) !ManagedSession {
        return self.loadManagedSessionWithAdmissionUsingManager(
            model_path,
            preferred_backends,
            shared_backend_ctx,
            &self.session_manager,
            control,
            selected,
            borrowed,
        );
    }

    fn loadManagedSessionWithAdmissionUsingManager(
        self: *ModelManager,
        model_path: []const u8,
        preferred_backends: []const backends.BackendType,
        shared_backend_ctx: ?*backends.imported_onnx_session.SharedBackendContext,
        source_session_manager: *const backends.SessionManager,
        control: ?InferenceExecutionControl,
        selected: ResolvedModel,
        borrowed: ?*PublishedArtifacts,
    ) !ManagedSession {
        return session_loader.loadComponent(self.allocator, SessionLoadHost.services(self), model_path, preferred_backends, shared_backend_ctx, source_session_manager, control, selected, borrowed);
    }

    fn loadManagedHfTokenizerFile(
        self: *ModelManager,
        tokenizer_path: []const u8,
    ) !ManagedHfTokenizer {
        return self.loadManagedHfTokenizerFileWithArtifacts(tokenizer_path, null);
    }

    fn loadManagedHfTokenizerFileWithArtifacts(
        self: *ModelManager,
        tokenizer_path: []const u8,
        artifacts: ?*const PublishedArtifacts,
    ) !ManagedHfTokenizer {
        const plan = try tokenizerFileAdmissionPlan(
            self.allocator,
            tokenizer_path,
            .huggingface,
        );
        var resource_lease = try self.acquireTokenizerAdmission(plan);
        errdefer if (resource_lease) |*lease| lease.release();

        const tok_bytes = if (artifacts) |verified|
            try verified.readPath(tokenizer_path, std.math.maxInt(usize))
        else
            try c_file.readFile(self.allocator, tokenizer_path);
        defer self.allocator.free(tok_bytes);
        const tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(self.allocator, tok_bytes);
        errdefer tokenizer.deinitSelf();
        try tokenizer.configureBpeCache(self.tokenizer_cache_config);
        try tokenizer.configureParallelBpe(self.tokenizer_parallel_bpe_config);
        if (resource_lease) |*lease| try lease.retain(plan.resident);

        const managed_lifetime = if (self.resource_domain) |lifetime|
            try lifetime.registerManagedTokenizer(resource_lease)
        else
            null;
        if (managed_lifetime != null) resource_lease = null;

        return .{
            .tokenizer = tokenizer,
            .resource_lease = resource_lease,
            .managed_lifetime = if (managed_lifetime) |owner| owner.capability() else null,
        };
    }

    fn acquireTokenizerAdmission(
        self: *ModelManager,
        plan: ModelLoadAdmissionPlan,
    ) !?runtime.tier.memory.AdmissionLease {
        if (!self.admission_enabled) return null;
        return try self.acquireAmountsWithEviction(
            .cpu,
            self.admissionLimitsForBackend(.{ .backend = .native }),
            plan.peak,
        );
    }

    pub fn lockLoadedModels(self: *ModelManager) void {
        spinLock(&self.load_lock);
    }

    pub fn unlockLoadedModels(self: *ModelManager) void {
        self.load_lock.unlock();
    }

    fn canPrewarmBeforeModelPublication(self: *ModelManager) bool {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        return modelCacheHasPublicationCapacity(self.loaded.count(), self.max_loaded_models);
    }

    pub fn publicationForRequest(self: *ModelManager, model_dir: []const u8, control: ?InferenceExecutionControl) !?[32]u8 {
        if (control) |active| try active.check();
        const current = try managed_receipt.publicationGeneration(self.allocator, if (control) |active| active.io orelse self.session_manager.io orelse std.Options.debug_io else self.session_manager.io orelse std.Options.debug_io, model_dir);
        if (control) |active| if (active.model_selection) |selected| {
            if (!selected.matches(model_dir) or !std.meta.eql(selected.publication, current)) return error.ModelArtifactsChanging;
        };
        return current;
    }

    pub fn acquireLoadedModel(self: *ModelManager, model_dir: []const u8) ?ModelHandle {
        return self.acquireLoadedModelWithControl(model_dir, .{}) catch null;
    }

    pub fn acquireLoadedModelWithControl(self: *ModelManager, model_dir: []const u8, control: InferenceExecutionControl) !?ModelHandle {
        const expected = try self.publicationForRequest(model_dir, control);
        self.lockLoadedModels();
        const model = self.loaded.get(model_dir) orelse self.loaded_aliases.get(model_dir) orelse {
            self.unlockLoadedModels();
            return null;
        };
        model.active_handles += 1;
        self.unlockLoadedModels();
        var handle: ModelHandle = .{ .manager = self, .model = model };
        try self.requireManagedPublication(&handle, expected);
        return handle;
    }

    /// Pin the same cached publication selected by ordinary request loading,
    /// including required-backend and A4B policy keys. Never load cold assets
    /// or join a load flight while reading request-admission metadata.
    pub fn acquireCachedFromDir(self: *ModelManager, model_dir: []const u8) !?ModelHandle {
        return self.acquireCachedFromDirWithControl(model_dir, .{});
    }

    pub fn acquireCachedFromDirWithControl(self: *ModelManager, model_dir: []const u8, control: InferenceExecutionControl) !?ModelHandle {
        const publication = try self.publicationForRequest(model_dir, control);
        const defaults = self.defaultModelLoadPolicy(model_dir);
        var required_backend_scratch: [1]backends.BackendType = undefined;
        const effective_backends = try self.session_manager.requiredBackendCandidates(
            defaults.preferred_backends,
            &required_backend_scratch,
        );
        self.lockLoadedModels();
        const model = (self.lookupLoadedModelLocked(
            model_dir,
            effective_backends,
            true,
            defaults.cache_policy,
        ) catch |err| {
            self.unlockLoadedModels();
            return err;
        }) orelse {
            self.unlockLoadedModels();
            return null;
        };
        model.active_handles += 1;
        self.unlockLoadedModels();
        var handle: ModelHandle = .{ .manager = self, .model = model };
        try self.requireManagedPublication(&handle, publication);
        return handle;
    }

    fn requireManagedPublication(self: *ModelManager, handle: *ModelHandle, expected: ?[32]u8) !void {
        _ = self;
        if (std.meta.eql(handle.get().managed_publication, expected)) return;
        handle.retire();
        return error.ModelArtifactsChanging;
    }

    /// Return owned warm-request metadata without holding a second runtime
    /// owner through execution or integrity recovery. Cold paths still inspect
    /// the manifest before loading assets.
    pub fn copyCachedQwenEmbeddingManifest(self: *ModelManager, allocator: std.mem.Allocator, model_dir: []const u8) !?manifest_mod.ModelManifest {
        return self.copyCachedQwenEmbeddingManifestWithControl(allocator, model_dir, .{});
    }

    pub fn copyCachedQwenEmbeddingManifestWithControl(self: *ModelManager, allocator: std.mem.Allocator, model_dir: []const u8, control: InferenceExecutionControl) !?manifest_mod.ModelManifest {
        var handle = (try self.acquireCachedFromDirWithControl(model_dir, control)) orelse return null;
        defer handle.release();
        const model = handle.get();
        if (model.manifest.embedding_style != .qwen3_embedding or model.session.backend() != .metal) return null;
        return try model.manifest.clone(allocator);
    }

    /// Admission metadata belongs to the loaded immutable generation. A clone
    /// outlives the cache pin; execution independently pins and verifies its
    /// generation before consuming input. Explicit retirement/reload publishes
    /// a different manifest instead of reparsing the tokenizer per request.
    pub fn copyCachedEmbeddingGemma2Manifest(self: *ModelManager, allocator: std.mem.Allocator, model_dir: []const u8) !?manifest_mod.ModelManifest {
        return self.copyCachedEmbeddingGemma2ManifestWithControl(allocator, model_dir, .{});
    }

    pub fn copyCachedEmbeddingGemma2ManifestWithControl(self: *ModelManager, allocator: std.mem.Allocator, model_dir: []const u8, control: InferenceExecutionControl) !?manifest_mod.ModelManifest {
        var handle = (try self.acquireCachedFromDirWithControl(model_dir, control)) orelse return null;
        defer handle.release();
        const model = handle.get();
        if (model.manifest.embedding_style != .embedding_gemma2) return null;
        return try model.manifest.clone(allocator);
    }

    /// Request admission may reuse the selected immutable generator's metadata.
    /// Clone under a short cache pin so invalid requests and slow media uploads
    /// do not prevent eviction or recovery. Execution rechecks its own manifest.
    pub fn copyCachedGenerationManifest(self: *ModelManager, allocator: std.mem.Allocator, model_dir: []const u8) !?manifest_mod.ModelManifest {
        return self.copyCachedGenerationManifestWithControl(allocator, model_dir, .{});
    }

    pub fn copyCachedGenerationManifestWithControl(self: *ModelManager, allocator: std.mem.Allocator, model_dir: []const u8, control: InferenceExecutionControl) !?manifest_mod.ModelManifest {
        var handle = (try self.acquireCachedFromDirWithControl(model_dir, control)) orelse return null;
        defer handle.release();
        const model = handle.get();
        if (model.manifest.model_type != .generator) return null;
        return try model.manifest.clone(allocator);
    }

    /// Pin one handle for every currently published model. Callers may release
    /// load_lock before taking per-model locks without racing model eviction or
    /// retirement destruction. Metrics/listing observation does not renew TTL;
    /// ordinary inference handles still record use when they are released.
    pub fn acquireLoadedModelSnapshot(
        self: *ModelManager,
        allocator: std.mem.Allocator,
    ) !LoadedModelSnapshot {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();

        const handles = try allocator.alloc(ModelHandle, self.loaded.count());
        var index: usize = 0;
        var it = self.loaded.valueIterator();
        while (it.next()) |model_ptr| {
            const model = model_ptr.*;
            model.active_handles += 1;
            handles[index] = .{ .manager = self, .model = model };
            index += 1;
        }
        std.debug.assert(index == handles.len);
        return .{ .allocator = allocator, .handles = handles };
    }

    fn runtimeIdPublishedLocked(self: *const ModelManager, runtime_id: RuntimeId) bool {
        var loaded_it = self.loaded.valueIterator();
        while (loaded_it.next()) |model_ptr| {
            if (model_ptr.*.runtime_id == runtime_id) return true;
        }
        var composite_it = self.composite_assets.valueIterator();
        while (composite_it.next()) |assets_ptr| {
            if (assets_ptr.*.runtime_id == runtime_id) return true;
        }
        return false;
    }

    /// Copy runtime state without acquiring model handles or renewing idle TTL.
    /// Filesystem paths are internal join keys; callers must map them to safe
    /// advertised identifiers before serializing a response.
    pub fn runtimeSnapshot(
        self: *ModelManager,
        allocator: std.mem.Allocator,
        manifest_task_mask: ?RuntimeManifestTaskMaskFn,
    ) !RuntimeSnapshot {
        var entries = std.ArrayList(RuntimeSnapshotEntry).empty;
        errdefer {
            for (entries.items) |entry| allocator.free(entry.model_dir);
            entries.deinit(allocator);
        }

        const now_ns = platform.time.monotonicNs();
        self.lockLoadedModels();
        defer self.unlockLoadedModels();

        var loaded_it = self.loaded.valueIterator();
        while (loaded_it.next()) |model_ptr| {
            const model = model_ptr.*;
            const owned_model_dir = try allocator.dupe(u8, model.model_dir);
            errdefer allocator.free(owned_model_dir);
            try entries.append(allocator, .{
                .runtime_id = model.runtime_id,
                .managed_publication = model.managed_publication,
                .unloadable = !model.pinned and model.active_handles == 0 and !self.modelIsInFlightLocked(model),
                .model_dir = owned_model_dir,
                .state = if (model.active_handles == 0) .loaded else .in_use,
                .runtime_kind = .primary,
                .model_type = model.manifest.model_type,
                .task_mask = if (manifest_task_mask) |classify| classify(&model.manifest) else 0,
                .active_handles = model.active_handles,
                .startup_managed = model.pinned,
                .backend = model.session.backend(),
                .residency_mode = model.residency_mode,
                .last_used_ms = if (now_ns >= model.last_used_ns)
                    (now_ns - model.last_used_ns) / std.time.ns_per_ms
                else
                    0,
            });
        }

        var composite_it = self.composite_assets.valueIterator();
        while (composite_it.next()) |assets_ptr| {
            const assets = assets_ptr.*;
            const backend = if (assets.encoder) |managed|
                managed.session.backend()
            else if (assets.decoder) |managed|
                managed.session.backend()
            else
                null;
            const owned_model_dir = try allocator.dupe(u8, assets.model_dir);
            errdefer allocator.free(owned_model_dir);
            try entries.append(allocator, .{
                .runtime_id = assets.runtime_id,
                .managed_publication = assets.managed_publication,
                .unloadable = !assets.startup_managed and assets.active_handles == 0 and !self.compositeAssetsIsInFlightLocked(assets),
                .model_dir = owned_model_dir,
                .state = if (assets.active_handles == 0) .loaded else .in_use,
                .runtime_kind = .composite,
                .model_type = assets.model_type,
                .task_mask = 0,
                .active_handles = assets.active_handles,
                .startup_managed = assets.startup_managed,
                .backend = backend,
                .residency_mode = .backend_managed,
                .last_used_ms = if (now_ns >= assets.last_used_ns)
                    (now_ns - assets.last_used_ns) / std.time.ns_per_ms
                else
                    0,
            });
        }

        var flight_it = self.in_flight_loads.valueIterator();
        while (flight_it.next()) |flight_ptr| {
            const flight = flight_ptr.*;
            if (flight.completed.isSet()) continue;
            if (self.runtimeIdPublishedLocked(flight.runtime_id)) continue;
            const model_dir = flight.model_dir orelse continue;
            const owned_model_dir = try allocator.dupe(u8, model_dir);
            errdefer allocator.free(owned_model_dir);
            try entries.append(allocator, .{
                .runtime_id = flight.runtime_id,
                .managed_publication = flight.managed_publication,
                .model_dir = owned_model_dir,
                .state = .loading,
                .runtime_kind = .primary,
                .model_type = null,
                .task_mask = 0,
                .active_handles = flight.activeWaiterCount(),
                .startup_managed = false,
                .backend = null,
                .residency_mode = .backend_managed,
                .last_used_ms = 0,
            });
        }

        var composite_flight_it = self.in_flight_composite_assets.valueIterator();
        while (composite_flight_it.next()) |flight_ptr| {
            const flight = flight_ptr.*;
            if (flight.completed.isSet()) continue;
            if (self.runtimeIdPublishedLocked(flight.runtime_id)) continue;
            const model_dir = flight.model_dir orelse continue;
            const owned_model_dir = try allocator.dupe(u8, model_dir);
            errdefer allocator.free(owned_model_dir);
            try entries.append(allocator, .{
                .runtime_id = flight.runtime_id,
                .managed_publication = flight.managed_publication,
                .model_dir = owned_model_dir,
                .state = .loading,
                .runtime_kind = .composite,
                .model_type = flight.model_type,
                .task_mask = 0,
                .active_handles = flight.load_state.activeWaiterCount(),
                .startup_managed = false,
                .backend = null,
                .residency_mode = .backend_managed,
                .last_used_ms = 0,
            });
        }

        return .{ .allocator = allocator, .entries = try entries.toOwnedSlice(allocator) };
    }

    /// Evict one exact published incarnation. A stale identifier is a safe,
    /// idempotent miss and can never select a replacement runtime.
    pub fn unloadRuntime(self: *ModelManager, runtime_id: RuntimeId) UnloadRuntimeResult {
        spinLock(&self.eviction_lock);
        defer self.eviction_lock.unlock();

        self.lockLoadedModels();
        var load_flights = self.in_flight_loads.valueIterator();
        while (load_flights.next()) |flight_ptr| {
            const flight = flight_ptr.*;
            if (flight.runtime_id == runtime_id and !flight.completed.isSet()) {
                self.unlockLoadedModels();
                return .loading;
            }
        }
        var composite_flights = self.in_flight_composite_assets.valueIterator();
        while (composite_flights.next()) |flight_ptr| {
            const flight = flight_ptr.*;
            if (flight.runtime_id == runtime_id and !flight.completed.isSet()) {
                self.unlockLoadedModels();
                return .loading;
            }
        }

        var loaded_it = self.loaded.valueIterator();
        while (loaded_it.next()) |model_ptr| {
            const model = model_ptr.*;
            if (model.runtime_id != runtime_id) continue;
            if (model.pinned) {
                self.unlockLoadedModels();
                return .startup_managed;
            }
            if (model.active_handles != 0 or self.modelIsInFlightLocked(model)) {
                const active = model.active_handles;
                self.unlockLoadedModels();
                return .{ .in_use = active };
            }
            const evicted = self.takeLoadedModelLocked(model).?;
            self.unlockLoadedModels();
            self.destroyEvictedModel(evicted);
            return .unloaded;
        }

        var composite_it = self.composite_assets.iterator();
        while (composite_it.next()) |entry| {
            const assets = entry.value_ptr.*;
            if (assets.runtime_id != runtime_id) continue;
            if (assets.startup_managed) {
                self.unlockLoadedModels();
                return .startup_managed;
            }
            if (assets.active_handles != 0 or self.compositeAssetsIsInFlightLocked(assets)) {
                const active = assets.active_handles;
                self.unlockLoadedModels();
                return .{ .in_use = active };
            }
            const removed = self.composite_assets.fetchRemove(entry.key_ptr.*).?;
            const evicted = EvictedCompositeAssets{ .key = removed.key, .assets = removed.value };
            self.unlockLoadedModels();
            self.destroyEvictedCompositeAssets(evicted);
            return .unloaded;
        }
        self.unlockLoadedModels();
        return .already_unloaded;
    }

    pub fn loadedChatTemplateFailed(
        self: *ModelManager,
        model_dir: []const u8,
    ) ?bool {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        const model = self.loaded.get(model_dir) orelse
            self.loaded_aliases.get(model_dir) orelse return null;
        return model.chat_template_failed;
    }

    /// Configure tokenizer-cache admission before any model is loaded. The
    /// same process-wide budget may be shared by every loaded tokenizer.
    pub fn configureTokenizerCaches(
        self: *ModelManager,
        config: hf_tokenizer.HfTokenizer.BpeCacheConfig,
    ) !void {
        if (self.loaded.count() != 0 or self.composite_assets.count() != 0)
            return error.TokenizerCacheConfigAfterModelLoad;
        spinLock(&self.tokenizer_cache_config_mutex);
        defer self.tokenizer_cache_config_mutex.unlock();
        if (config.resource_budget != null) {
            return switch (self.resource_ownership) {
                .local => error.ExternalTokenizerBudgetInLocalOwnership,
                .external_required => error.ExternalBudgetsMustBeConfiguredTogether,
            };
        }
        var next = config;
        switch (self.tokenizer_cache_budget_source) {
            .none => {},
            .local_manager, .external_pair => next.resource_budget = self.tokenizer_cache_config.resource_budget,
        }
        self.tokenizer_cache_config = next;
    }

    /// Configure persistent std.Io-consumer tokenizer state before loading
    /// models. Private tables use the resource budget supplied through
    /// `configureTokenizerCaches`, so this is a capacity request rather than
    /// an unconditional allocation.
    pub fn configureTokenizerParallelBpe(
        self: *ModelManager,
        config: hf_tokenizer.HfTokenizer.ParallelBpeConfig,
    ) !void {
        if (self.loaded.count() != 0 or self.composite_assets.count() != 0)
            return error.TokenizerParallelConfigAfterModelLoad;
        self.tokenizer_parallel_bpe_config = config;
    }

    /// External export is allowed only after numerical/resource lanes retire.
    pub fn flushExecutionDiagnostics(self: *const ModelManager) void {
        if (self.session_manager.execution_diagnostics.enabled) @import("../execution/diagnostic_export.zig").flush();
    }

    pub fn deinit(self: *ModelManager) void {
        defer self.flushExecutionDiagnostics();
        // Sessions can retain the load runtime. Join work and destroy all
        // resident resources before tearing down the manager-owned fallback.
        defer if (self.owned_load_io_owner) |owner| owner.release();
        defer if (self.owned_load_watchdog) |watchdog| watchdog.destroy();
        // Escaped raw sessions retain this domain and any manager-owned driver
        // IO until their final close scope, even after the cache owner exits.
        defer if (self.teardown_domain) |domain| domain.release();
        if (self.load_io) |io| self.load_group.cancel(io);
        if (self.eviction_io) |io| {
            self.eviction_stop.set(io);
            self.eviction_group.cancel(io);
        }
        std.debug.assert(self.in_flight_loads.count() == 0);
        std.debug.assert(self.in_flight_composite_assets.count() == 0);
        self.in_flight_loads.deinit(self.allocator);
        self.in_flight_composite_assets.deinit(self.allocator);
        var component_plan_it = self.component_plan_cache.iterator();
        while (component_plan_it.next()) |entry| entry.value_ptr.*.release();
        self.component_plan_cache.deinit(self.allocator);
        self.merged_decoder_membership.deinit(self.allocator);
        self.verified_artifacts.deinit(self.allocator);
        self.unhealthy_model_backends.deinit(self.allocator);
        var composite_assets_it = self.composite_assets.iterator();
        while (composite_assets_it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.composite_assets.deinit(self.allocator);
        var startup_keys = self.startup_load_policies.keyIterator();
        while (startup_keys.next()) |key| self.allocator.free(key.*);
        self.startup_load_policies.deinit(self.allocator);
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.loaded.deinit(self.allocator);
        var alias_it = self.loaded_aliases.iterator();
        while (alias_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.loaded_aliases.deinit(self.allocator);
        if (self.resource_domain) |lifetime| {
            if (self.owns_resource_domain) lifetime.close();
            self.tokenizer_cache_config.resource_budget = null;
            self.resource_domain = null;
            lifetime.release();
        }
        _ = platform.allocator.reclaimUnusedProcessMemory();
    }

    pub fn attachIo(self: *ModelManager, io: std.Io) std.Io.ConcurrentError!void {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        self.session_manager.io = io;
        var it = self.loaded.iterator();
        while (it.next()) |entry| entry.value_ptr.*.attachIo(io);
        const start_eviction_loop = self.keep_alive_ms > 0 and
            !self.eviction_loop_started;
        if (start_eviction_loop) {
            // This loop waits for shutdown. async may execute inline
            // when the CPU-bound worker limit is zero or exhausted, hanging
            // startup. concurrent must either spawn it or report failure.
            try self.eviction_group.concurrent(io, evictionLoop, .{ self, io });
            self.eviction_loop_started = true;
            self.eviction_io = io;
        }
    }

    pub fn detachPromptCacheResourceUsageObserver(self: *ModelManager) void {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        var it = self.loaded.valueIterator();
        while (it.next()) |model| model.*.prompt_prefix_cache.detachResourceUsageObserver();
    }

    /// Counts loaded models participating in the prompt-cache accounting target.
    /// Used to split that target evenly across active model caches.
    /// `include` is always counted even if its cache has not activated yet.
    fn participatingPromptCacheCount(self: *ModelManager, include: *LoadedModel) usize {
        var count: usize = 0;
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model == include) continue;
            if (model.prompt_prefix_cache.isParticipating()) count += 1;
        }
        return count + 1;
    }

    /// Apply one node-wide prompt-cache target to the cache being activated and
    /// schedule that target for every other active cache. The caller owns the
    /// included model's generation lock, so its configure() may evict safely.
    /// Foreign caches must defer eviction until their own serialized request
    /// boundary; their KvManager pools are otherwise concurrently mutable.
    pub fn rebalancePromptCaches(
        self: *ModelManager,
        include: *LoadedModel,
        node_config: runtime.kv.prompt_cache.Config,
    ) void {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        include.prompt_prefix_cache.reserveActivation();
        var per_cache = node_config;
        per_cache.max_bytes /= self.participatingPromptCacheCount(include);

        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model != include and model.prompt_prefix_cache.isParticipating()) {
                model.prompt_prefix_cache.scheduleConfigure(per_cache);
            }
        }
        // Existing foreign entries are an eventual-eviction overage, not the
        // active model's allocation. scheduleConfigure() immediately prevents
        // those caches from growing past their new shares; charging their old
        // bytes again here can permanently collapse a busy model's share to
        // zero when an idle owner never reaches another request boundary.
        // Prompt-cache max_bytes is an eviction target rather than a hard RSS
        // limit, so let the active owner use its fair share while foreign
        // owners converge safely on their serialized generation lanes.
        include.prompt_prefix_cache.configure(per_cache);
    }

    /// Remove a failed first-use reservation and restore the full node target
    /// across the remaining caches. If a pool was already created, the cache
    /// remains a participant and no rollback is needed.
    pub fn cancelPromptCacheActivation(
        self: *ModelManager,
        include: *LoadedModel,
        node_config: runtime.kv.prompt_cache.Config,
    ) void {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        if (!include.prompt_prefix_cache.cancelPendingActivation()) return;

        var count: usize = 0;
        var it = self.loaded.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.*.prompt_prefix_cache.isParticipating()) count += 1;
        }
        if (count == 0) return;

        var per_cache = node_config;
        per_cache.max_bytes /= count;
        var rebalance_it = self.loaded.iterator();
        while (rebalance_it.next()) |entry| {
            const model = entry.value_ptr.*;
            if (model.prompt_prefix_cache.isParticipating()) {
                model.prompt_prefix_cache.scheduleConfigure(per_cache);
            }
        }
    }

    fn finishCompositeAssetsLoadFlight(
        self: *ModelManager,
        flight: *CompositeAssetsLoadFlight,
        assets: ?*CompositeAssets,
        err: ?anyerror,
    ) void {
        self.lockLoadedModels();
        flight.assets = assets;
        if (assets) |loaded| flight.runtime_id = loaded.runtime_id;
        flight.err = err;
        self.unlockLoadedModels();
        flight.completed.set(flight.io);
    }

    fn destroyCompositeAssetsLoadFlight(self: *ModelManager, flight: *CompositeAssetsLoadFlight) void {
        if (flight.model_dir) |model_dir| self.allocator.free(model_dir);
        self.allocator.destroy(flight);
    }

    fn releaseCompositeAssetsLoadFlight(
        self: *ModelManager,
        flight_key: ComponentPlanKey,
        flight: *CompositeAssetsLoadFlight,
    ) void {
        self.lockLoadedModels();
        std.debug.assert(flight.refs > 0);
        flight.refs -= 1;
        if (flight.refs != 0) {
            self.unlockLoadedModels();
            return;
        }
        if (flight.registered) {
            const removed = self.in_flight_composite_assets.fetchRemove(flight_key) orelse unreachable;
            std.debug.assert(removed.value == flight);
            flight.registered = false;
        }
        self.unlockLoadedModels();

        self.destroyCompositeAssetsLoadFlight(flight);
    }

    fn waitForCompositeAssetsLoadFlight(
        self: *ModelManager,
        flight_key: ComponentPlanKey,
        flight: *CompositeAssetsLoadFlight,
        control: ?InferenceExecutionControl,
    ) !CompositeAssetsHandle {
        defer self.releaseCompositeAssetsLoadFlight(flight_key, flight);
        defer flight.load_state.releaseWaiter(!flight.completed.isSet());
        while (!flight.completed.isSet()) {
            if (control) |active| try active.update(.loading_model, 0, 1);
            flight.completed.waitTimeout(flight.io, .{
                .duration = .{ .raw = std.Io.Duration.fromNanoseconds(10 * std.time.ns_per_ms), .clock = .awake },
            }) catch |err| switch (err) {
                error.Timeout => continue,
                else => return err,
            };
        }
        if (control) |active| try active.check();
        const assets = flight.assets;
        const maybe_err = flight.err;

        if (assets) |loaded| {
            self.lockLoadedModels();
            loaded.active_handles += 1;
            self.unlockLoadedModels();
        }
        if (maybe_err) |err| return err;
        return .{
            .manager = self,
            .assets = assets orelse return error.TokenizerLoadFailed,
        };
    }

    fn runCompositeLoadTask(task: *CompositeLoadTask) std.Io.Cancelable!void {
        defer task.deinit();
        const manager = task.manager;
        defer manager.releaseCompositeAssetsLoadFlight(task.key, task.flight);
        var active = LoadFlightControl{ .flight = &task.flight.load_state };
        const assets = manager.loadCompositeAssetsUncached(task.model_dir, task.paths, task.preferred_backends, task.key, task.flight.runtime_id, task.flight.model_type, task.kind, (@import("../registry/model_selection.zig").ResolvedModel{ .path = task.model_dir, .publication = task.flight.managed_publication }).bind(active.control())) catch |err| {
            manager.finishCompositeAssetsLoadFlight(task.flight, null, err);
            return;
        };
        active.control().check() catch |err| {
            assets.deinit();
            manager.allocator.destroy(assets);
            manager.finishCompositeAssetsLoadFlight(task.flight, null, err);
            return;
        };
        if (!std.meta.eql(assets.managed_publication, task.flight.managed_publication)) {
            assets.deinit();
            manager.allocator.destroy(assets);
            manager.finishCompositeAssetsLoadFlight(task.flight, null, error.ModelArtifactsChanging);
            return;
        }
        var handle = manager.publishCompositeAssets(task.key, assets) catch |err| {
            manager.finishCompositeAssetsLoadFlight(task.flight, null, err);
            return;
        };
        manager.finishCompositeAssetsLoadFlight(task.flight, handle.assets, null);
        handle.release();
    }

    fn compositeAssetGenerationSignature(
        self: *ModelManager,
        model_dir: []const u8,
        component_paths: []const []const u8,
    ) !ComponentPlanKey {
        var dependencies = std.ArrayListUnmanaged([]const u8).empty;
        defer dependencies.deinit(self.allocator);
        try dependencies.appendSlice(self.allocator, component_paths);

        const sidecar_names = [_][]const u8{
            "tokenizer.json",
            "config.json",
            "generation_config.json",
            "clip_config.json",
            "model_manifest.json",
            "antfly_inference_bundle.json",
            "antfly_inference_variants.json",
            "gliner_config.json",
            "added_tokens.json",
            "1_SpladePooling/config.json",
            managed_receipt.complete_filename,
            managed_receipt.in_progress_filename,
            managed_receipt.plan_filename,
        };
        var owned_paths = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (owned_paths.items) |path| self.allocator.free(path);
            owned_paths.deinit(self.allocator);
        }
        try owned_paths.ensureTotalCapacity(self.allocator, sidecar_names.len);
        for (sidecar_names) |name| {
            const path = try std.fs.path.join(self.allocator, &.{ model_dir, name });
            owned_paths.appendAssumeCapacity(path);
            try dependencies.append(self.allocator, path);
        }
        const direct = try componentDependencySignature(try self.componentPlanIo(), dependencies.items);
        if (component_paths.len == 0) return direct;
        // Artifact identity is backend-neutral. Optional graphs participate in
        // invalidation, but must not restrict the backend of the selected pair.
        // Cache the closure under a distinct namespace in the bounded plan cache.
        var closure_hash = std.crypto.hash.sha2.Sha256.init(.{});
        closure_hash.update("composite-artifact-closure-v1");
        closure_hash.update(&direct);
        const closure_key = closure_hash.finalResult();
        const closure = if (try self.cachedComponentPlan(closure_key)) |entry| blk: {
            defer entry.release();
            break :blk entry.signature;
        } else blk: {
            var man = try manifest_mod.loadFromDir(self.allocator, model_dir);
            defer man.deinit();
            var inspection = try inspectComponentArtifacts(self.allocator, &man, component_paths, .multistage_ocr);
            defer inspection.deinit();
            if (inspection.invalid_summary != null) return error.IncompatibleModel;
            const signature = try componentDependencySignature(try self.componentPlanIo(), inspection.dependencies.items);
            if (!std.mem.eql(u8, &direct, &try componentDependencySignature(try self.componentPlanIo(), dependencies.items))) return error.ModelArtifactsChanging;
            try self.publishComponentPlan(closure_key, signature, &.{}, inspection.dependencies.items);
            break :blk signature;
        };
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&direct);
        hash.update(&closure);
        return hash.finalResult();
    }

    fn compositeLoadHost(self: *ModelManager) !@import("../models/composite_runtime.zig").Host {
        const Adapter = struct {
            fn tokenizer(raw: *anyopaque, path: []const u8, artifacts: ?*const PublishedArtifacts) !ManagedHfTokenizer {
                const manager: *ModelManager = @ptrCast(@alignCast(raw));
                return manager.loadManagedHfTokenizerFileWithArtifacts(path, artifacts);
            }
            fn plan(raw: *anyopaque, directory: []const u8, preferred: []const backends.BackendType, paths: []const []const u8) !ComponentLoader {
                const manager: *ModelManager = @ptrCast(@alignCast(raw));
                return manager.componentLoaderForPaths(directory, preferred, paths);
            }
            fn identity(raw: *anyopaque, request: @import("../models/composite_runtime.zig").Request) !ComponentPlanKey {
                const manager: *ModelManager = @ptrCast(@alignCast(raw));
                return manager.compositeRuntimeKey(request.model_dir, request.component_paths, request.kind, request.preferred_backends, request.model_type);
            }
        };
        return .{ .context = self, .io = try self.componentPlanIo(), .load_tokenizer_fn = Adapter.tokenizer, .plan_components_fn = Adapter.plan, .identity_fn = Adapter.identity };
    }

    fn loadCompositeAssetsUncached(
        self: *ModelManager,
        model_dir: []const u8,
        component_paths: []const []const u8,
        preferred_backends: []const backends.BackendType,
        asset_generation: ComponentPlanKey,
        runtime_id: RuntimeId,
        model_type: manifest_mod.ModelType,
        kind: CompositeKind,
        control: ?InferenceExecutionControl,
    ) !*CompositeAssets {
        if (control) |active| try active.check();
        return @import("../models/composite_runtime.zig").load(self.allocator, try self.compositeLoadHost(), .{
            .model_dir = model_dir,
            .component_paths = component_paths,
            .preferred_backends = preferred_backends,
            .asset_generation = asset_generation,
            .publication = try self.publicationForRequest(model_dir, control),
            .runtime_id = runtime_id,
            .model_type = model_type,
            .kind = kind,
        }, control);
    }

    fn publishCompositeAssets(
        self: *ModelManager,
        asset_generation: ComponentPlanKey,
        assets: *CompositeAssets,
    ) !CompositeAssetsHandle {
        var assets_owned = true;
        errdefer if (assets_owned) {
            assets.deinit();
            self.allocator.destroy(assets);
            _ = platform.allocator.reclaimUnusedProcessMemory();
        };
        spinLock(&self.eviction_lock);
        defer self.eviction_lock.unlock();
        while (true) {
            self.lockLoadedModels();
            if (self.composite_assets.get(asset_generation)) |existing| {
                existing.active_handles += 1;
                self.unlockLoadedModels();
                assets.deinit();
                self.allocator.destroy(assets);
                _ = platform.allocator.reclaimUnusedProcessMemory();
                assets_owned = false;
                return .{ .manager = self, .assets = existing };
            }

            const configured_capacity = if (self.max_loaded_models > 0)
                self.max_loaded_models
            else
                composite_assets_cache_capacity;
            const capacity = @max(
                @as(usize, 1),
                @min(configured_capacity, composite_assets_cache_capacity),
            );
            if (self.composite_assets.count() >= capacity) {
                const evicted = self.takeLruCompositeAssetsLocked(
                    platform.time.monotonicNs(),
                    false,
                );
                self.unlockLoadedModels();
                if (evicted == null) return error.ResourceTemporarilyUnavailable;
                self.destroyEvictedCompositeAssets(evicted.?);
                continue;
            }

            self.composite_assets.ensureUnusedCapacity(self.allocator, 1) catch |err| {
                self.unlockLoadedModels();
                return err;
            };
            if (assets.runtime_id == 0) assets.runtime_id = self.nextRuntimeIdLocked();
            assets.active_handles = 1;
            assets.last_used_ns = platform.time.monotonicNs();
            self.composite_assets.putAssumeCapacity(asset_generation, assets);
            self.unlockLoadedModels();
            assets_owned = false;
            return .{ .manager = self, .assets = assets };
        }
    }

    /// Acquire immutable tokenizer and decoder metadata for a split Whisper
    /// bundle. Cold construction is single-flight; warm requests perform one
    /// map lookup and keep the entry pinned through the returned handle.
    pub fn acquireCompositeAssets(
        self: *ModelManager,
        model_dir: []const u8,
        component_paths: []const []const u8,
    ) !CompositeAssetsHandle {
        return self.acquireCompositeRuntime(model_dir, component_paths, .whisper_metadata, null);
    }

    fn compositeRuntimeKey(self: *ModelManager, model_dir: []const u8, paths: []const []const u8, kind: CompositeKind, preferred_backends: []const backends.BackendType, model_type: manifest_mod.ModelType) !ComponentPlanKey {
        const generation_key = try self.compositeAssetGenerationSignature(model_dir, paths);
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(&generation_key);
        if (try managed_receipt.publicationGeneration(self.allocator, self.session_manager.io orelse std.Options.debug_io, model_dir)) |publication| hash.update(&publication);
        hash.update(@tagName(kind));
        hash.update(@tagName(model_type));
        for (preferred_backends) |backend| {
            hash.update("/");
            hash.update(@tagName(backend));
        }
        return hash.finalResult();
    }

    /// Startup selection is configuration, not cache residency. Copy policy
    /// values under the lock; returned backend slices have static lifetime.
    pub const StartupLoadPolicy = struct {
        backend: ?backends.BackendType = null,
        gliner_cuda_precision: ?session_factory.GlinerCudaPrecision = null,
        a4b_request: ?backend_contracts.A4bInferenceRequest = null,
    };

    pub fn setStartupLoadPolicy(self: *ModelManager, model_dir: []const u8, policy: StartupLoadPolicy) !void {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        try self.setStartupLoadPolicyLocked(model_dir, policy);
    }

    fn setStartupLoadPolicyLocked(self: *ModelManager, model_dir: []const u8, policy: StartupLoadPolicy) !void {
        const entry = try self.startup_load_policies.getOrPut(self.allocator, model_dir);
        if (!entry.found_existing) {
            errdefer _ = self.startup_load_policies.remove(model_dir);
            entry.key_ptr.* = try self.allocator.dupe(u8, model_dir);
        }
        entry.value_ptr.* = policy;
    }

    fn startupLoadPolicy(self: *ModelManager, model_dir: []const u8) StartupLoadPolicy {
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        return self.startup_load_policies.get(model_dir) orelse .{};
    }

    fn startupBackends(self: *ModelManager, policy: StartupLoadPolicy) []const backends.BackendType {
        return switch (policy.backend orelse return self.session_manager.preferred_backends) {
            .native => &.{.native},
            .metal => &.{.metal},
            .cuda => &.{.cuda},
            .pjrt => &.{.pjrt},
            .wasm => &.{.wasm},
        };
    }

    fn defaultModelBackends(self: *ModelManager, model_dir: []const u8) []const backends.BackendType {
        return self.startupBackends(self.startupLoadPolicy(model_dir));
    }

    /// Overrides describe only fields the caller selected. Placement is
    /// independent of the model's persistent load contract.
    const ModelLoadOverrides = struct {
        preferred_backends: ?[]const backends.BackendType = null,
        a4b_request: ?backend_contracts.A4bInferenceRequest = null,
        gliner_cuda_precision: ?session_factory.GlinerCudaPrecision = null,
    };

    const ResolvedModelLoadPolicy = struct {
        preferred_backends: []const backends.BackendType,
        cache_policy: ModelLoadCachePolicy,
    };

    fn resolveModelLoadPolicy(self: *ModelManager, model_dir: []const u8, overrides: ModelLoadOverrides) ResolvedModelLoadPolicy {
        const startup = self.startupLoadPolicy(model_dir);
        const a4b_request = overrides.a4b_request orelse startup.a4b_request orelse self.session_manager.a4b_inference_request;
        const precision = overrides.gliner_cuda_precision orelse startup.gliner_cuda_precision;
        var policy = inheritedA4bCachePolicy(a4b_request, overrides.preferred_backends != null or startup.backend != null);
        policy.gliner_cuda_precision = precision;
        if (precision != null) policy.accept_default_alias = false;
        return .{ .preferred_backends = overrides.preferred_backends orelse self.startupBackends(startup), .cache_policy = policy };
    }

    fn defaultModelLoadPolicy(self: *ModelManager, model_dir: []const u8) ResolvedModelLoadPolicy {
        return self.resolveModelLoadPolicy(model_dir, .{});
    }

    fn acquireResolvedModel(self: *ModelManager, model_dir: []const u8, overrides: ModelLoadOverrides, cache_default_alias: bool, control: ?InferenceExecutionControl) !ModelHandle {
        const resolved = self.resolveModelLoadPolicy(model_dir, overrides);
        return self.loadFromDirCoordinated(model_dir, resolved.preferred_backends, cache_default_alias, resolved.cache_policy, control);
    }

    /// Lazy, single-flight component runtime. The handle pins both sessions,
    /// tokenizer and metadata; callers must not close the borrowed sessions.
    pub fn acquireCompositeRuntime(
        self: *ModelManager,
        model_dir: []const u8,
        component_paths: []const []const u8,
        kind: CompositeKind,
        control: ?InferenceExecutionControl,
    ) !CompositeAssetsHandle {
        return self.acquireCompositeRuntimeWithPreferredBackends(
            model_dir,
            component_paths,
            self.defaultModelBackends(model_dir),
            if (kind == .seq2seq) .rewriter else .transcriber,
            kind,
            control,
        );
    }

    pub fn acquireCompositeRuntimeForModelType(
        self: *ModelManager,
        model_dir: []const u8,
        component_paths: []const []const u8,
        model_type: manifest_mod.ModelType,
        kind: CompositeKind,
        control: ?InferenceExecutionControl,
    ) !CompositeAssetsHandle {
        return self.acquireCompositeRuntimeWithPreferredBackends(
            model_dir,
            component_paths,
            self.defaultModelBackends(model_dir),
            model_type,
            kind,
            control,
        );
    }

    pub fn acquireCompositeRuntimeWithPreferredBackends(
        self: *ModelManager,
        model_dir: []const u8,
        component_paths: []const []const u8,
        preferred_backends: []const backends.BackendType,
        model_type: manifest_mod.ModelType,
        kind: CompositeKind,
        control: ?InferenceExecutionControl,
    ) !CompositeAssetsHandle {
        if (control) |active| try active.check();
        const publication = try self.publicationForRequest(model_dir, control);
        if (kind != .whisper_metadata and component_paths.len == 2 and std.mem.endsWith(u8, component_paths[1], ".onnx")) {
            if (try self.findMergedDecoderCached(model_dir, control)) |candidate| {
                defer self.allocator.free(candidate);
                if (!std.mem.eql(u8, candidate, component_paths[1])) {
                    const planned = [_][]const u8{ component_paths[0], component_paths[1], candidate };
                    // The runtime single-flight pins metadata-only selection.
                    // Artifact identity includes the optional graph; backend
                    // policy is applied only to the selected executable pair.
                    return self.acquireCompositeRuntimePlanned(model_dir, &planned, preferred_backends, model_type, kind, control, publication) catch |err| switch (err) {
                        error.IncompatibleModel, error.UnsupportedArchitecture, error.UnsupportedOperation, error.UnsupportedOperator, error.NoCompatibleBackend, error.NoBackendAvailable, error.UnsupportedBackend => {
                            // Planning can reject a graph before its constructor
                            // runs. Compatibility fallback must not conceal a
                            // corrupt receipt-listed graph or external tensor.
                            try self.verifyOptionalOnnxForFallback(model_dir, candidate, control);
                            return self.acquireCompositeRuntimePlanned(model_dir, component_paths, preferred_backends, model_type, kind, control, publication);
                        },
                        else => return err,
                    };
                }
            }
        }
        return self.acquireCompositeRuntimePlanned(model_dir, component_paths, preferred_backends, model_type, kind, control, publication);
    }

    fn findMergedDecoderCached(self: *ModelManager, model_dir: []const u8, supplied: ?InferenceExecutionControl) !?[]const u8 {
        const io = if (supplied) |active| active.io orelse self.session_manager.io orelse std.Options.debug_io else self.session_manager.io orelse std.Options.debug_io;
        const control = supplied orelse InferenceExecutionControl{ .io = io };
        try control.check();
        const selected = ResolvedModel{ .path = model_dir, .publication = try self.publicationForRequest(model_dir, supplied) };
        const key: ?[32]u8 = if (selected.publication != null) MergedDecoderMembershipCache.keyFor(selected) else null;
        var builder: ?*std.atomic.Mutex = null;
        defer if (builder) |mutex| mutex.unlock();
        if (key) |value| {
            switch (self.merged_decoder_membership.get(value)) {
                .hit => |location| return self.mergedDecoderPath(model_dir, location),
                .miss => {},
            }
            const mutex = &self.merged_decoder_membership.builders[value[0] % self.merged_decoder_membership.builders.len];
            try control.lockWithPoll(mutex, 100 * std.time.ns_per_us);
            builder = mutex;
            switch (self.merged_decoder_membership.get(value)) {
                .hit => |location| return self.mergedDecoderPath(model_dir, location),
                .miss => {},
            }
        }
        var lease = try @import("../registry/publication_lease.zig").Lease.shared(self.allocator, io, model_dir);
        defer lease.deinit(io);
        var artifacts = try PublishedArtifacts.init(self.allocator, io, model_dir, .{ .publication = selected.publication });
        defer artifacts.deinit();
        var location: MergedDecoderMembershipCache.Location = .absent;
        for ([_]MergedDecoderMembershipCache.Location{ .root, .onnx }) |candidate| {
            if (try artifacts.resolve(candidate.relative().?)) |path| {
                self.allocator.free(path);
                location = candidate;
                break;
            }
        }
        try artifacts.verify();
        try control.check();
        if (key) |value| self.merged_decoder_membership.put(self.allocator, value, location) catch {};
        return self.mergedDecoderPath(model_dir, location);
    }

    fn mergedDecoderPath(self: *ModelManager, root: []const u8, location: MergedDecoderMembershipCache.Location) !?[]const u8 {
        const relative = location.relative() orelse return null;
        // Keep the logical path. The constructor resolves it under the current
        // installation fence, so an alias retarget cannot leave a stale target
        // cached even when both installations have the same receipt digest.
        return try std.fs.path.join(self.allocator, &.{ root, relative });
    }

    fn verifyOptionalOnnxForFallback(self: *ModelManager, model_dir: []const u8, path: []const u8, control: ?InferenceExecutionControl) !void {
        const io = if (control) |active| active.io orelse self.session_manager.io orelse std.Options.debug_io else self.session_manager.io orelse std.Options.debug_io;
        var lease = try @import("../registry/publication_lease.zig").Lease.shared(self.allocator, io, model_dir);
        defer lease.deinit(io);
        var artifacts = try @import("../registry/published_artifacts.zig").PublishedArtifacts.init(self.allocator, io, model_dir, .{ .publication = try self.publicationForRequest(model_dir, control) });
        defer artifacts.deinit();
        try artifacts.verifyOnnx(path);
        try artifacts.verify();
    }

    fn acquireCompositeRuntimePlanned(self: *ModelManager, model_dir: []const u8, component_paths: []const []const u8, preferred_backends: []const backends.BackendType, model_type: manifest_mod.ModelType, kind: CompositeKind, control: ?InferenceExecutionControl, publication: ?[32]u8) !CompositeAssetsHandle {
        if (!std.meta.eql(publication, try self.publicationForRequest(model_dir, control))) return error.ModelArtifactsChanging;
        var handle = try self.acquireCompositeRuntimePlannedInner(model_dir, component_paths, preferred_backends, model_type, kind, control, publication);
        errdefer handle.release();
        if (!std.meta.eql(publication, handle.get().managed_publication)) return error.ModelArtifactsChanging;
        return handle;
    }

    fn acquireCompositeRuntimePlannedInner(self: *ModelManager, model_dir: []const u8, component_paths: []const []const u8, preferred_backends: []const backends.BackendType, model_type: manifest_mod.ModelType, kind: CompositeKind, control: ?InferenceExecutionControl, publication: ?[32]u8) !CompositeAssetsHandle {
        const asset_generation = try self.compositeRuntimeKey(
            model_dir,
            component_paths,
            kind,
            preferred_backends,
            model_type,
        );
        self.lockLoadedModels();
        if (self.composite_assets.get(asset_generation)) |assets| {
            assets.active_handles += 1;
            self.unlockLoadedModels();
            return .{ .manager = self, .assets = assets };
        }
        if (self.joinableCompositeLoadFlightLocked(asset_generation)) |flight| {
            if (!flight.load_state.tryAddWaiter()) {
                self.unlockLoadedModels();
                return error.ResourceTemporarilyUnavailable;
            }
            flight.refs += 1;
            self.unlockLoadedModels();
            return self.waitForCompositeAssetsLoadFlight(asset_generation, flight, control);
        }

        const coordination_io = self.loadCoordinationIoLocked() catch |err| {
            self.unlockLoadedModels();
            return err;
        };
        const flight = self.allocator.create(CompositeAssetsLoadFlight) catch |err| {
            self.unlockLoadedModels();
            return err;
        };
        const flight_model_dir = self.allocator.dupe(u8, model_dir) catch |err| {
            self.allocator.destroy(flight);
            self.unlockLoadedModels();
            return err;
        };
        flight.* = .{
            .managed_publication = publication,
            .runtime_id = self.nextRuntimeIdLocked(),
            .model_dir = flight_model_dir,
            .kind = kind,
            .model_type = model_type,
            .io = coordination_io,
            .refs = 2,
            .load_state = .{
                .io = coordination_io,
                .hard_cancellation = if (control) |active| active.hard_cancellation else null,
                .native_call_policy = self.session_manager.native_call_policy,
            },
        };
        const task = CompositeLoadTask.create(self, flight, asset_generation, model_dir, component_paths, preferred_backends, kind) catch |err| {
            self.destroyCompositeAssetsLoadFlight(flight);
            self.unlockLoadedModels();
            return err;
        };
        self.in_flight_composite_assets.put(self.allocator, asset_generation, flight) catch |err| {
            task.deinit();
            self.destroyCompositeAssetsLoadFlight(flight);
            self.unlockLoadedModels();
            return err;
        };
        self.unlockLoadedModels();
        if (control != null and control.?.request_owned) {
            self.load_group.concurrent(coordination_io, runCompositeLoadTask, .{task}) catch |err| {
                self.finishCompositeAssetsLoadFlight(flight, null, err);
                task.deinit();
                self.releaseCompositeAssetsLoadFlight(asset_generation, flight);
            };
        } else {
            self.load_group.async(coordination_io, runCompositeLoadTask, .{task});
        }
        return self.waitForCompositeAssetsLoadFlight(asset_generation, flight, control);
    }

    /// Verify that sessions loaded after asset acquisition still describe the
    /// same immutable publication. A model pull may atomically replace the
    /// directory between those operations; fail the request instead of pairing
    /// sessions and token IDs from different generations.
    pub fn validateCompositeAssetsCurrent(
        self: *ModelManager,
        handle: *const CompositeAssetsHandle,
        model_dir: []const u8,
        component_paths: []const []const u8,
    ) !void {
        const assets = handle.get();
        const current = if (assets.considered_merged_decoder and component_paths.len == 2) blk: {
            const selected = ResolvedModel{ .path = model_dir, .publication = assets.managed_publication };
            const candidate = try self.findMergedDecoderCached(model_dir, selected.bind(InferenceExecutionControl{ .io = self.session_manager.io orelse std.Options.debug_io })) orelse return error.ModelArtifactsChanging;
            defer self.allocator.free(candidate);
            break :blk try self.compositeRuntimeKey(model_dir, &.{ component_paths[0], component_paths[1], candidate }, assets.kind, assets.preferred_backends, assets.model_type);
        } else try self.compositeRuntimeKey(model_dir, component_paths, assets.kind, assets.preferred_backends, assets.model_type);
        if (!std.mem.eql(u8, assets.generation[0..], current[0..]))
            return error.ModelArtifactsChanging;
    }

    /// Acquire a model for the duration of one operation. The returned handle
    /// prevents eviction until release.
    pub fn acquireFromDir(self: *ModelManager, model_dir: []const u8) !ModelHandle {
        return self.acquireResolvedModel(model_dir, .{}, true, null);
    }

    pub fn acquireFromDirWithControl(
        self: *ModelManager,
        model_dir: []const u8,
        control: InferenceExecutionControl,
    ) !ModelHandle {
        return self.acquireResolvedModel(model_dir, .{}, true, control);
    }

    pub fn acquireFromDirWithPreferredBackends(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
    ) !ModelHandle {
        return self.acquireResolvedModel(model_dir, .{ .preferred_backends = preferred_backends }, cache_default_alias, null);
    }

    pub fn acquireFromDirWithPreferredBackendsAndControl(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
        control: InferenceExecutionControl,
    ) !ModelHandle {
        return self.acquireResolvedModel(model_dir, .{ .preferred_backends = preferred_backends }, cache_default_alias, control);
    }

    /// Explicit GLiNER precision is immutable and part of both the resident
    /// variant and in-flight load identity. Later ordinary requests may reuse
    /// the default alias published by this startup preload.
    pub fn acquireFromDirWithGlinerCudaPrecisionAndControl(self: *ModelManager, model_dir: []const u8, precision: session_factory.GlinerCudaPrecision, control: InferenceExecutionControl) !ModelHandle {
        return self.acquireResolvedModel(model_dir, .{ .preferred_backends = &.{.cuda}, .gliner_cuda_precision = precision }, true, control);
    }

    pub fn acquireFromDirWithGlinerCudaPrecision(self: *ModelManager, model_dir: []const u8, precision: session_factory.GlinerCudaPrecision) !ModelHandle {
        return self.acquireResolvedModel(model_dir, .{ .preferred_backends = &.{.cuda}, .gliner_cuda_precision = precision }, true, null);
    }

    /// Acquire a model using an immutable A4B policy for this load. Explicit
    /// policies do not accept an existing unqualified alias: the qualified
    /// cache key must match before a model can be reused. A newly qualified
    /// preload may still publish the default alias so later policy-free
    /// requests reuse the warmed session; its policy-specific variant key
    /// remains the ownership key.
    pub fn acquireFromDirWithA4bRequest(
        self: *ModelManager,
        model_dir: []const u8,
        a4b_request: backend_contracts.A4bInferenceRequest,
    ) !ModelHandle {
        return self.acquireResolvedModel(model_dir, .{ .a4b_request = a4b_request }, true, null);
    }

    pub fn acquireFromDirWithA4bRequestAndControl(
        self: *ModelManager,
        model_dir: []const u8,
        a4b_request: backend_contracts.A4bInferenceRequest,
        control: InferenceExecutionControl,
    ) !ModelHandle {
        return self.acquireResolvedModel(model_dir, .{ .a4b_request = a4b_request }, true, control);
    }

    pub fn acquireFromDirWithPreferredBackendsAndA4bRequest(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
        a4b_request: backend_contracts.A4bInferenceRequest,
    ) !ModelHandle {
        return self.acquireResolvedModel(model_dir, .{ .preferred_backends = preferred_backends, .a4b_request = a4b_request }, cache_default_alias, null);
    }

    pub fn acquireFromDirWithPreferredBackendsAndA4bRequestAndControl(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
        a4b_request: backend_contracts.A4bInferenceRequest,
        control: InferenceExecutionControl,
    ) !ModelHandle {
        return self.acquireResolvedModel(model_dir, .{ .preferred_backends = preferred_backends, .a4b_request = a4b_request }, cache_default_alias, control);
    }

    /// Compatibility API for offline tools that keep a raw model pointer. Such
    /// callers explicitly pin the model because their pointer lifetime cannot
    /// be observed by the cache.
    pub fn loadFromDirWithControl(self: *ModelManager, path: []const u8, control: InferenceExecutionControl) !*LoadedModel {
        var handle = try self.acquireFromDirWithControl(path, control);
        handle.pin();
        const model = handle.get();
        handle.release();
        return model;
    }
    pub fn loadFromDirWithPreferredBackendsAndControl(self: *ModelManager, path: []const u8, backends_: []const backends.BackendType, cache_default_alias: bool, control: InferenceExecutionControl) !*LoadedModel {
        var handle = try self.acquireFromDirWithPreferredBackendsAndControl(path, backends_, cache_default_alias, control);
        handle.pin();
        const model = handle.get();
        handle.release();
        return model;
    }
    pub fn acquireCompositeAssetsWithControl(self: *ModelManager, path: []const u8, components: []const []const u8, control: InferenceExecutionControl) !CompositeAssetsHandle {
        return self.acquireCompositeRuntime(path, components, .whisper_metadata, control);
    }

    pub fn loadFromDir(self: *ModelManager, model_dir: []const u8) !*LoadedModel {
        var handle = try self.acquireFromDir(model_dir);
        handle.pin();
        const model = handle.get();
        handle.release();
        return model;
    }

    pub fn loadFromDirWithPreferredBackends(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
    ) !*LoadedModel {
        var handle = try self.acquireFromDirWithPreferredBackends(
            model_dir,
            preferred_backends,
            cache_default_alias,
        );
        handle.pin();
        const model = handle.get();
        handle.release();
        return model;
    }

    fn lookupLoadedModelLocked(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        _: bool,
        policy: ModelLoadCachePolicy,
    ) !?*LoadedModel {
        for (preferred_backends) |backend| {
            if (!backend.supportsDirectSessionLoad()) continue;
            if (modelBackendIsUnhealthy(self, model_dir, backend)) continue;
            const variant_key = try backendVariantCacheKeyWithPrecision(
                self.allocator,
                model_dir,
                backend,
                policy.a4b_request,
                policy.gliner_cuda_precision,
            );
            defer self.allocator.free(variant_key);
            if (self.loaded.get(variant_key)) |model| return model;
            if (self.loaded_aliases.get(variant_key)) |model| return model;
        }
        if (policy.accept_default_alias) {
            if (self.loaded.get(model_dir)) |model|
                if (!modelBackendIsUnhealthy(self, model_dir, model.session.backend()) and
                    (!policy.require_default_alias_backend_match or
                        loadedModelUsesPreferredBackend(model, preferred_backends))) return model;
            if (self.loaded_aliases.get(model_dir)) |model|
                if (!modelBackendIsUnhealthy(self, model_dir, model.session.backend()) and
                    (!policy.require_default_alias_backend_match or
                        loadedModelUsesPreferredBackend(model, preferred_backends))) return model;
        }
        return null;
    }

    fn loadFlightKey(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
        policy: ModelLoadCachePolicy,
    ) ![]u8 {
        const a4b_identity = if (policy.a4b_request) |request|
            try a4bRequestCacheIdentity(self.allocator, request)
        else
            null;
        defer if (a4b_identity) |identity| self.allocator.free(identity);
        const prefix = if (a4b_identity) |identity|
            try std.fmt.allocPrint(
                self.allocator,
                "{d}:{s}:{d}:{d}:{d}:a4b={s}:",
                .{
                    model_dir.len,
                    model_dir,
                    @intFromBool(cache_default_alias),
                    @intFromBool(policy.accept_default_alias),
                    @intFromBool(policy.require_default_alias_backend_match),
                    identity,
                },
            )
        else
            try std.fmt.allocPrint(
                self.allocator,
                "{d}:{s}:{d}:{d}:{d}:a4b=none:",
                .{
                    model_dir.len,
                    model_dir,
                    @intFromBool(cache_default_alias),
                    @intFromBool(policy.accept_default_alias),
                    @intFromBool(policy.require_default_alias_backend_match),
                },
            );
        defer self.allocator.free(prefix);
        const publication_hex = if (policy.managed_publication) |value| std.fmt.bytesToHex(value, .lower) else @as([64]u8, @splat('-'));
        const precision_prefix = try std.fmt.allocPrint(self.allocator, "{s}publication={s}:gliner_cuda_precision={s}:", .{ prefix, publication_hex, if (policy.gliner_cuda_precision) |value| @tagName(value) else "none" });
        defer self.allocator.free(precision_prefix);
        const key = try self.allocator.alloc(u8, precision_prefix.len + preferred_backends.len);
        @memcpy(key[0..precision_prefix.len], precision_prefix);
        for (preferred_backends, 0..) |backend, idx| {
            key[precision_prefix.len + idx] = @intCast(@backingInt(backend));
        }
        return key;
    }

    fn finishLoadFlight(
        self: *ModelManager,
        flight: *LoadFlight,
        model: ?*LoadedModel,
        err: ?anyerror,
    ) void {
        self.lockLoadedModels();
        flight.model = model;
        if (model) |loaded| flight.runtime_id = loaded.runtime_id;
        flight.err = err;
        self.unlockLoadedModels();
        flight.completed.set(flight.io);
    }

    fn destroyLoadFlight(self: *ModelManager, flight: *LoadFlight) void {
        if (flight.model_dir) |model_dir| self.allocator.free(model_dir);
        self.allocator.destroy(flight);
    }

    /// Completed failures are not negative cache entries. The task and its old
    /// waiters may still own the flight, but a new request must be able to retry
    /// after admission or artifacts change. Detachment does not destroy it, and
    /// old-reference cleanup must never remove a replacement with the same key.
    fn joinableCompositeLoadFlightLocked(self: *ModelManager, key: ComponentPlanKey) ?*CompositeAssetsLoadFlight {
        const flight = self.in_flight_composite_assets.get(key) orelse return null;
        if (!flight.completed.isSet() or flight.err == null) return flight;
        const removed = self.in_flight_composite_assets.fetchRemove(key) orelse unreachable;
        std.debug.assert(removed.value == flight);
        flight.registered = false;
        return null;
    }

    fn joinableLoadFlightLocked(self: *ModelManager, key: []const u8) ?*LoadFlight {
        const flight = self.in_flight_loads.get(key) orelse return null;
        if (!flight.completed.isSet() or flight.err == null) return flight;
        const removed = self.in_flight_loads.fetchRemove(key) orelse unreachable;
        std.debug.assert(removed.value == flight);
        flight.registered = false;
        self.allocator.free(removed.key);
        return null;
    }

    fn runLoadTask(task: *LoadTask) std.Io.Cancelable!void {
        defer task.deinit();
        const manager = task.manager;
        defer manager.releaseLoadFlight(task.flight_key, task.flight);

        var load_control = LoadFlightControl{ .flight = task.flight };
        const publication = managed_receipt.publicationGeneration(manager.allocator, task.session_manager.io orelse std.Options.debug_io, task.model_dir) catch |err| {
            manager.finishLoadFlight(task.flight, null, err);
            return;
        };
        if (!std.meta.eql(publication, task.managed_publication)) {
            manager.finishLoadFlight(task.flight, null, error.ModelArtifactsChanging);
            return;
        }
        var handle = manager.loadFromDirUncached(
            task.model_dir,
            &task.session_manager,
            task.cache_default_alias,
            task.a4b_request,
            task.flight.runtime_id,
            (@import("../registry/model_selection.zig").ResolvedModel{ .path = task.model_dir, .publication = task.managed_publication }).bind(load_control.control()),
        ) catch |err| {
            manager.finishLoadFlight(task.flight, null, err);
            return;
        };
        manager.requireManagedPublication(&handle, task.managed_publication) catch |err| {
            manager.finishLoadFlight(task.flight, null, err);
            return;
        };
        load_control.control().check() catch |err| {
            // loadFromDirUncached publishes before returning. If every waiter
            // left during its final non-suspending section, retire that orphan
            // instead of leaving an unused runtime resident.
            handle.retire();
            manager.finishLoadFlight(task.flight, null, err);
            return;
        };
        manager.finishLoadFlight(task.flight, handle.get(), null);
        // The task owns only the construction handle. Request waiters adopt
        // their own handles from the completed flight.
        handle.release();
    }

    fn releaseLoadFlight(
        self: *ModelManager,
        flight_key: []const u8,
        flight: *LoadFlight,
    ) void {
        var removed_key: ?[]const u8 = null;
        self.lockLoadedModels();
        std.debug.assert(flight.task_ref_pending);
        flight.task_ref_pending = false;
        std.debug.assert(flight.refs > 0);
        flight.refs -= 1;
        if (flight.refs != 0) {
            self.unlockLoadedModels();
            return;
        }
        if (flight.registered) {
            const removed = self.in_flight_loads.fetchRemove(flight_key) orelse unreachable;
            std.debug.assert(removed.value == flight);
            flight.registered = false;
            removed_key = removed.key;
        }
        self.unlockLoadedModels();

        if (removed_key) |key| self.allocator.free(key);
        self.destroyLoadFlight(flight);
    }

    fn waitForLoadFlight(
        self: *ModelManager,
        flight_key: []const u8,
        flight: *LoadFlight,
        control: ?InferenceExecutionControl,
    ) !ModelHandle {
        var waiter_consumed = false;
        defer if (!waiter_consumed) {
            var result = self.consumeLoadFlightWaiter(flight_key, flight, .abandon);
            result.handle.release();
        };
        while (!flight.completed.isSet()) {
            if (control) |active| try active.update(.loading_model, 0, 1);
            flight.completed.waitTimeout(flight.io, .{
                .duration = .{
                    .raw = std.Io.Duration.fromNanoseconds(10 * std.time.ns_per_ms),
                    .clock = .awake,
                },
            }) catch |err| switch (err) {
                error.Timeout => continue,
                else => return err,
            };
        }
        var result = self.consumeLoadFlightWaiter(flight_key, flight, .adopt);
        waiter_consumed = true;
        errdefer result.handle.release();
        if (control) |active| try active.check();
        if (result.err) |err| return err;
        if (result.handle.model == null) return error.NoBackendAvailable;
        return result.handle;
    }

    /// Consume exactly one waiter reference and transfer its model ownership,
    /// if any, to the caller. Retirement reserves handles for outstanding
    /// waiters; even an abandoning waiter must release that reservation. Keep
    /// this accounting under the same lock as retirement, and release returned
    /// handles outside the lock because the last release can destroy a model.
    fn consumeLoadFlightWaiter(
        self: *ModelManager,
        flight_key: []const u8,
        flight: *LoadFlight,
        disposition: enum { adopt, abandon },
    ) struct { handle: ModelHandle, err: ?anyerror } {
        var removed_key: ?[]const u8 = null;
        var destroy_flight = false;
        self.lockLoadedModels();
        var handle = ModelHandle{ .manager = self, .model = null };
        const maybe_err = flight.err;
        if (flight.model) |loaded| {
            if (flight.handles_reserved or disposition == .adopt) {
                if (!flight.handles_reserved) loaded.active_handles += 1;
                handle.model = loaded;
            }
        }
        std.debug.assert(flight.refs > 0);
        flight.refs -= 1;
        flight.releaseWaiter(disposition == .abandon);
        if (flight.refs == 0) {
            if (flight.registered) {
                const removed = self.in_flight_loads.fetchRemove(flight_key) orelse unreachable;
                std.debug.assert(removed.value == flight);
                flight.registered = false;
                removed_key = removed.key;
            }
            destroy_flight = true;
        }
        self.unlockLoadedModels();
        if (removed_key) |key| self.allocator.free(key);
        if (destroy_flight) self.destroyLoadFlight(flight);
        return .{ .handle = handle, .err = maybe_err };
    }

    /// Called under load_lock. Once selected, the group's runtime never changes,
    /// including when attachIo is called after an offline load has started.
    fn loadCoordinationIoLocked(self: *ModelManager) !std.Io {
        if (self.load_io) |io| return io;
        const io = self.session_manager.io orelse blk: {
            const owner = try OwnedManagerIo.create(self.allocator);
            self.owned_load_io_owner = owner;
            self.owned_load_runtime = &owner.runtime;
            break :blk owner.runtime.io();
        };
        self.load_io = io;
        return io;
    }

    /// Establish a lifetime ticket before entering an uninterruptible backend.
    /// Embedded runtimes accept waiting for native close; supervised runtimes
    /// retain a watchdog ticket. Nothing arms a monitor when close starts.
    fn prepareSessionClose(self: *ModelManager, backend_runtime: backends.BackendRuntime) !?backends.Session.CloseProtection {
        if (!backend_runtime.hasUninterruptibleCalls()) return null;
        self.lockLoadedModels();
        defer self.unlockLoadedModels();
        if (!self.session_manager.process_termination_available) {
            // In-process hosts accept that a wedged close blocks rather than
            // being ended by killing the worker.
            if (self.session_manager.native_call_policy == .in_process) return null;
            return error.ProcessIsolationRequired;
        }
        if (self.teardown_domain == null) {
            // Stabilize a possible manager-owned offline driver runtime before
            // the independent teardown owner takes its lifetime reference.
            _ = try self.loadCoordinationIoLocked();
            self.teardown_domain = try TeardownDomain.create(self.allocator, self.owned_load_io_owner);
        }
        return try TeardownTicket.create(self.teardown_domain.?);
    }

    fn offlineLoadBoundaryLocked(self: *ModelManager, io: std.Io) !?execution_control_mod.HardCancellationBoundary {
        if (!self.session_manager.process_termination_available) return null;
        if (self.owned_load_watchdog == null) {
            const watchdog = try HardCancellationWatchdog.create(self.allocator);
            errdefer watchdog.destroy();
            try watchdog.start(io);
            self.owned_load_watchdog = watchdog;
        }
        return self.owned_load_watchdog.?.boundary();
    }

    fn loadFromDirCoordinated(
        self: *ModelManager,
        model_dir: []const u8,
        preferred_backends: []const backends.BackendType,
        cache_default_alias: bool,
        policy: ModelLoadCachePolicy,
        control: ?InferenceExecutionControl,
    ) !ModelHandle {
        if (control) |active| try active.update(.loading_model, 0, 1);
        var required_backend_scratch: [1]backends.BackendType = undefined;
        const effective_backends = try self.session_manager.requiredBackendCandidates(
            preferred_backends,
            &required_backend_scratch,
        );
        var checked_policy = policy;
        checked_policy.managed_publication = try self.publicationForRequest(model_dir, control);
        const flight_key = try self.loadFlightKey(
            model_dir,
            effective_backends,
            cache_default_alias,
            checked_policy,
        );
        defer self.allocator.free(flight_key);

        self.lockLoadedModels();
        const cached = self.lookupLoadedModelLocked(
            model_dir,
            effective_backends,
            cache_default_alias,
            checked_policy,
        ) catch |err| {
            self.unlockLoadedModels();
            return err;
        };
        if (cached) |model| {
            model.active_handles += 1;
            self.unlockLoadedModels();
            var handle: ModelHandle = .{ .manager = self, .model = model };
            try self.requireManagedPublication(&handle, checked_policy.managed_publication);
            return handle;
        }
        if (self.joinableLoadFlightLocked(flight_key)) |flight| {
            flight.refs += 1;
            if (!flight.tryAddWaiter()) {
                flight.refs -= 1;
                self.unlockLoadedModels();
                return error.ResourceTemporarilyUnavailable;
            }
            self.unlockLoadedModels();
            return self.waitForLoadFlight(flight_key, flight, control);
        }

        const coordination_io = self.loadCoordinationIoLocked() catch |err| {
            self.unlockLoadedModels();
            return err;
        };
        // Offline CLI loads also run in a cancellable manager-owned task.
        // Their process is disposable, but the task still needs a real monitor
        // for a driver call that cannot observe its final waiter's cancellation.
        const hard_cancellation = if (control) |active| active.hard_cancellation else self.offlineLoadBoundaryLocked(coordination_io) catch |err| {
            self.unlockLoadedModels();
            return err;
        };
        const flight = self.allocator.create(LoadFlight) catch |err| {
            self.unlockLoadedModels();
            return err;
        };
        const flight_model_dir = self.allocator.dupe(u8, model_dir) catch |err| {
            self.allocator.destroy(flight);
            self.unlockLoadedModels();
            return err;
        };
        flight.* = .{
            .managed_publication = checked_policy.managed_publication,
            .runtime_id = self.nextRuntimeIdLocked(),
            .model_dir = flight_model_dir,
            .io = coordination_io,
            .hard_cancellation = hard_cancellation,
            .native_call_policy = self.session_manager.native_call_policy,
        };
        const owned_flight_key = self.allocator.dupe(u8, flight_key) catch |err| {
            self.destroyLoadFlight(flight);
            self.unlockLoadedModels();
            return err;
        };
        const task = self.allocator.create(LoadTask) catch |err| {
            self.allocator.free(owned_flight_key);
            self.destroyLoadFlight(flight);
            self.unlockLoadedModels();
            return err;
        };
        const task_flight_key = self.allocator.dupe(u8, flight_key) catch |err| {
            self.allocator.destroy(task);
            self.allocator.free(owned_flight_key);
            self.destroyLoadFlight(flight);
            self.unlockLoadedModels();
            return err;
        };
        const task_model_dir = self.allocator.dupe(u8, model_dir) catch |err| {
            self.allocator.free(task_flight_key);
            self.allocator.destroy(task);
            self.allocator.free(owned_flight_key);
            self.destroyLoadFlight(flight);
            self.unlockLoadedModels();
            return err;
        };
        const task_backends = self.allocator.dupe(backends.BackendType, effective_backends) catch |err| {
            self.allocator.free(task_model_dir);
            self.allocator.free(task_flight_key);
            self.allocator.destroy(task);
            self.allocator.free(owned_flight_key);
            self.destroyLoadFlight(flight);
            self.unlockLoadedModels();
            return err;
        };
        // Snapshot the injected runtime while holding load_lock. attachIo may
        // update the manager between cold loads, but a load must use one stable
        // runtime for its complete construction.
        var session_manager = sessionManagerForPreferredBackends(
            self.allocator,
            task_backends,
            &self.session_manager,
        );
        session_manager.io = coordination_io;
        session_manager.a4b_inference_request = policy.a4b_request;
        session_manager.gliner_cuda_precision = policy.gliner_cuda_precision;
        task.* = .{
            .manager = self,
            .flight = flight,
            .flight_key = task_flight_key,
            .model_dir = task_model_dir,
            .preferred_backends = task_backends,
            .session_manager = session_manager,
            .cache_default_alias = cache_default_alias,
            .a4b_request = policy.a4b_request,
            .managed_publication = checked_policy.managed_publication,
        };
        self.in_flight_loads.put(self.allocator, owned_flight_key, flight) catch |err| {
            task.deinit();
            self.allocator.free(owned_flight_key);
            self.destroyLoadFlight(flight);
            self.unlockLoadedModels();
            return err;
        };
        self.unlockLoadedModels();
        if (control != null and control.?.request_owned) {
            self.load_group.concurrent(coordination_io, runLoadTask, .{task}) catch |err| {
                // `Group.async` is allowed to execute eagerly when its async pool is
                // saturated. A request-scoped cold load must never run on the request
                // lane: the waiter is what translates request cancellation into an
                // abandoned flight. Fail closed when guaranteed concurrency is not
                // available and retire the task reference exactly as runLoadTask
                // would have done.
                self.finishLoadFlight(flight, null, err);
                task.deinit();
                self.releaseLoadFlight(flight_key, flight);
            };
        } else {
            // Startup preloads and direct/offline callers have no request lifetime
            // to protect. Retain the permissive path for executors such as
            // std.testing.io that intentionally do not offer concurrency.
            self.load_group.async(coordination_io, runLoadTask, .{task});
        }
        return self.waitForLoadFlight(flight_key, flight, control);
    }

    pub fn loadChunkPlanningAssets(self: *ModelManager, model_dir: []const u8, io: std.Io, control: InferenceExecutionControl) !ChunkPlanningAssets {
        var publication_lease = try @import("../registry/publication_lease.zig").Lease.shared(self.allocator, io, model_dir);
        defer publication_lease.deinit(io);
        const publication = try self.publicationForRequest(model_dir, control);
        // Capture the actual root while this manager's publication lease is
        // held, before another cancellation/progress callback can replace it.
        var artifacts = try PublishedArtifacts.init(self.allocator, io, model_dir, .{ .publication = publication });
        defer artifacts.deinit();
        artifacts.verification_cache = &self.verified_artifacts;
        try control.check();
        self.lockLoadedModels();
        const backend_snapshot = self.session_manager;
        self.unlockLoadedModels();
        const resources = self.primaryPreparation();
        return loaded_runtime.loadPlanningAssets(self.allocator, model_dir, io, .{
            .backend_snapshot = backend_snapshot,
            .identity_io = backend_snapshot.io,
            .tokenizer_options = resources.tokenizer_options,
            .tokenizer_admission = resources.tokenizer_admission,
            .admission_enabled = resources.host.session_loading.admission_enabled,
            .expected_publication = publication,
            .verification_cache = &self.verified_artifacts,
            .borrowed_artifacts = &artifacts,
        }, control);
    }

    pub fn modelIdentityForManifest(self: *ModelManager, model_dir: []const u8, manifest: manifest_mod.ModelManifest) ![32]u8 {
        return modelIdentity(self.allocator, try self.componentPlanIo(), model_dir, manifest);
    }

    const RetainedSessionResources = struct {
        references: std.atomic.Value(usize) = .init(1),
        allocator: std.mem.Allocator,
        domain: *ResourceDomain,
        io_owner: ?*OwnedManagerIo,

        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const old = self.references.fetchAdd(1, .monotonic);
            std.debug.assert(old > 0 and old < std.math.maxInt(usize));
        }

        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const old = self.references.fetchSub(1, .acq_rel);
            std.debug.assert(old > 0);
            if (old != 1) return;
            self.domain.release();
            if (self.io_owner) |owner| owner.release();
            self.allocator.destroy(self);
        }
    };

    fn attachSessionResourcePin(self: *ModelManager, session: *backends.Session) !void {
        self.lockLoadedModels();
        const domain = self.resource_domain orelse {
            self.unlockLoadedModels();
            return error.MissingResourceOwner;
        };
        if (!domain.retain()) {
            self.unlockLoadedModels();
            return error.ResourceOwnerShuttingDown;
        }
        const io_owner = self.owned_load_io_owner;
        if (io_owner) |owner| owner.retain();
        self.unlockLoadedModels();
        errdefer domain.release();
        errdefer if (io_owner) |owner| owner.release();
        if (session.model_lifetime) |lifetime| if (lifetime.admission_domain) |pin| {
            if (pin.controller() != domain.admission) return error.ForeignAdmissionDomain;
        };
        // The tier ledger's final metadata must retain the actual owner before
        // serving publication. This happens outside Model/lane/cache gates and
        // before fallible resource-pin creation, so failure closes normally.
        const cache_domain = domain.admission_owner.pin();
        defer cache_domain.release();
        try session_factory.installNativeCacheRetirementDomainForSession(session.*, &cache_domain);
        const holder = try self.allocator.create(RetainedSessionResources);
        errdefer self.allocator.destroy(holder);
        holder.* = .{ .allocator = self.allocator, .domain = domain, .io_owner = io_owner };
        try session.attachResourcePin(self.allocator, .{ .ptr = holder, .release_fn = RetainedSessionResources.release, .retain_fn = RetainedSessionResources.retain });
        // attachResourcePin has published a stable lifetime; nothing fallible
        // follows, and matching preinstalled domain ownership is preserved.
        if (session.model_lifetime.?.admission_domain == null)
            session.model_lifetime.?.admission_domain = domain.admission_owner.pin();
    }

    fn primaryPreparation(self: *ModelManager) loaded_runtime.PrimaryPreparation {
        const Adapter = struct {
            fn reserve(raw: *anyopaque, plan: ModelLoadAdmissionPlan) !?runtime.tier.memory.AdmissionLease {
                const manager: *ModelManager = @ptrCast(@alignCast(raw));
                return manager.acquireTokenizerAdmission(plan);
            }
        };
        return .{
            .host = self.loadedRuntimeHost(),
            .component_sessions = &self.session_manager,
            .executor_io = self.session_manager.io,
            .serving_policy = self.serving_policy,
            .tokenizer_options = .{ .cache = self.tokenizer_cache_config, .parallel = self.tokenizer_parallel_bpe_config },
            .tokenizer_admission = .{ .context = self, .reserve_fn = Adapter.reserve },
        };
    }

    fn loadFromDirUncached(
        self: *ModelManager,
        model_dir: []const u8,
        sm: *backends.SessionManager,
        cache_default_alias: bool,
        a4b_request: ?backend_contracts.A4bInferenceRequest,
        runtime_id: RuntimeId,
        control: ?InferenceExecutionControl,
    ) !ModelHandle {
        const model = try loaded_runtime.preparePrimary(self.allocator, model_dir, sm, self.primaryPreparation(), control);
        var unpublished_model_owned = true;
        errdefer if (unpublished_model_owned) {
            model.deinit();
            self.allocator.destroy(model);
        };
        // A cancelled manager task must close its fully constructed runtime
        // without exposing a transient loaded generation to other requests.
        if (control) |active| try active.update(.loading_model, 4, 4);

        // Publication performs max-loaded eviction. When the cache is already
        // full, prewarming first makes the incoming runtime compete with the
        // still-resident eviction victim and emits a predictable resource
        // warning. Skip that speculative work; generation prepares the runtime
        // lazily after publication has reclaimed the victim.
        if (self.canPrewarmBeforeModelPublication())
            try model.prewarmWithControl(control);

        if (control) |active| try active.check();
        if (!std.meta.eql(model.managed_publication, try managed_receipt.publicationGeneration(self.allocator, sm.io orelse std.Options.debug_io, model_dir))) return error.ModelArtifactsChanging;
        try model.finishPrimaryConstruction();
        unpublished_model_owned = false;
        return self.publishLoadedModel(model, cache_default_alias, a4b_request, sm.gliner_cuda_precision, runtime_id);
    }

    /// Publish a fully constructed model with only a short map critical section.
    /// Distinct request keys can occasionally converge on the same actual backend;
    /// in that case retain the first model and dispose of the duplicate outside
    /// the lock.
    fn publishLoadedModel(
        self: *ModelManager,
        model: *LoadedModel,
        cache_default_alias: bool,
        a4b_request: ?backend_contracts.A4bInferenceRequest,
        gliner_cuda_precision: ?session_factory.GlinerCudaPrecision,
        runtime_id: RuntimeId,
    ) !ModelHandle {
        var model_owned = true;
        errdefer if (model_owned) {
            model.deinit();
            self.allocator.destroy(model);
            _ = platform.allocator.reclaimUnusedProcessMemory();
        };

        const variant_key = try backendVariantCacheKeyWithPrecision(
            self.allocator,
            model.model_dir,
            model.session.backend(),
            a4b_request,
            gliner_cuda_precision,
        );
        var variant_key_owned = true;
        defer if (variant_key_owned) self.allocator.free(variant_key);

        const alias_key = if (cache_default_alias)
            try self.allocator.dupe(u8, model.model_dir)
        else
            null;
        var alias_key_owned = alias_key != null;
        defer if (alias_key_owned) self.allocator.free(alias_key.?);

        spinLock(&self.eviction_lock);
        defer self.eviction_lock.unlock();
        while (true) {
            self.lockLoadedModels();
            if (self.loaded.get(variant_key)) |existing| {
                if (!std.meta.eql(existing.managed_publication, model.managed_publication)) {
                    self.unlockLoadedModels();
                    return error.ModelArtifactsChanging;
                }
                if (alias_key != null and
                    self.loaded.get(model.model_dir) == null and
                    self.loaded_aliases.get(model.model_dir) == null)
                {
                    self.loaded_aliases.ensureUnusedCapacity(self.allocator, 1) catch |err| {
                        self.unlockLoadedModels();
                        return err;
                    };
                    self.loaded_aliases.putAssumeCapacity(alias_key.?, existing);
                    alias_key_owned = false;
                }
                existing.active_handles += 1;
                self.unlockLoadedModels();

                model.deinit();
                self.allocator.destroy(model);
                _ = platform.allocator.reclaimUnusedProcessMemory();
                model_owned = false;
                return .{ .manager = self, .model = existing };
            }

            if (self.max_loaded_models > 0 and
                self.loaded.count() >= self.max_loaded_models)
            {
                const evicted = self.takeLruModelLocked(
                    platform.time.monotonicNs(),
                    false,
                    null,
                );
                self.unlockLoadedModels();
                if (evicted == null)
                    return error.ResourceTemporarilyUnavailable;
                self.destroyEvictedModel(evicted.?);
                continue;
            }

            const needs_alias = alias_key != null and
                self.loaded.get(model.model_dir) == null and
                self.loaded_aliases.get(model.model_dir) == null;
            self.loaded.ensureUnusedCapacity(self.allocator, 1) catch |err| {
                self.unlockLoadedModels();
                return err;
            };
            if (needs_alias) {
                self.loaded_aliases.ensureUnusedCapacity(self.allocator, 1) catch |err| {
                    self.unlockLoadedModels();
                    return err;
                };
            }
            model.active_handles = 1;
            model.last_used_ns = platform.time.monotonicNs();
            if (model.runtime_id == 0)
                model.runtime_id = if (runtime_id != 0) runtime_id else self.nextRuntimeIdLocked();
            model.residency_mode = if (a4b_request) |request| switch (request.residency_mode) {
                .streamed => .streamed,
                .resident => .resident,
                .auto => .backend_managed,
            } else .backend_managed;
            self.loaded.putAssumeCapacity(variant_key, model);
            variant_key_owned = false;
            if (needs_alias) {
                self.loaded_aliases.putAssumeCapacity(alias_key.?, model);
                alias_key_owned = false;
            }
            self.unlockLoadedModels();

            model_owned = false;
            return .{ .manager = self, .model = model };
        }
    }

    fn destroyLoadedModel(self: *ModelManager, model: *LoadedModel) void {
        model.deinit();
        self.allocator.destroy(model);
    }
};

fn modelCacheHasPublicationCapacity(loaded_count: usize, max_loaded_models: usize) bool {
    return max_loaded_models == 0 or loaded_count < max_loaded_models;
}

const ModelLoadCachePolicy = struct {
    managed_publication: ?[32]u8 = null,
    gliner_cuda_precision: ?session_factory.GlinerCudaPrecision = null,
    a4b_request: ?backend_contracts.A4bInferenceRequest = null,
    accept_default_alias: bool = true,
    /// Policy-free acquisition consumes the model's explicitly published
    /// default alias regardless of current node preference. A caller that
    /// supplied an explicit backend order still requires an exact match.
    require_default_alias_backend_match: bool = true,
};

fn inheritedA4bCachePolicy(
    request: ?backend_contracts.A4bInferenceRequest,
    require_default_alias_backend_match: bool,
) ModelLoadCachePolicy {
    return .{
        .a4b_request = request,
        // Publishing a new default alias and consuming an existing one are
        // separate decisions. Policy-free requests may reuse a matching
        // qualified preload even when their call site does not publish new
        // aliases. Explicit A4B policies still require their exact variant key.
        .accept_default_alias = request == null,
        .require_default_alias_backend_match = require_default_alias_backend_match,
    };
}

fn loadedModelUsesPreferredBackend(
    model: *const LoadedModel,
    preferred_backends: []const backends.BackendType,
) bool {
    const actual = model.session.backend();
    for (preferred_backends) |preferred| {
        if (preferred == actual) return true;
    }
    return false;
}

fn a4bRequestCacheIdentity(
    allocator: std.mem.Allocator,
    request: backend_contracts.A4bInferenceRequest,
) ![]u8 {
    return std.fmt.allocPrint(
        allocator,
        "residency={s};memory_mb={d};load_strategy={s};load_workers={d};load_staging_mb={d};prepared_pack={s};drop_host_cache={d}",
        .{
            @tagName(request.residency_mode),
            request.memory_budget_mb,
            @tagName(request.load_strategy),
            request.load_workers,
            request.load_staging_mb,
            @tagName(request.prepared_pack),
            @intFromBool(request.drop_host_cache_after_load),
        },
    );
}

const model_backend_quarantine_ns: u64 = 30 * std.time.ns_per_s;
const ModelBackendHealthKey = [std.crypto.hash.sha2.Sha256.digest_length]u8;

fn modelBackendHealthKey(model_dir: []const u8, backend: backends.BackendType) ModelBackendHealthKey {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(model_dir);
    hasher.update(&[_]u8{@backingInt(backend)});
    var digest: ModelBackendHealthKey = undefined;
    hasher.final(&digest);
    return digest;
}

fn markModelBackendUnhealthy(
    self: *ModelManager,
    model_dir: []const u8,
    backend: backends.BackendType,
) void {
    const retry_after_ns = platform.time.monotonicNs() +| model_backend_quarantine_ns;
    spinLock(&self.unhealthy_backend_lock);
    self.unhealthy_model_backends.put(
        self.allocator,
        modelBackendHealthKey(model_dir, backend),
        retry_after_ns,
    ) catch {
        self.unhealthy_backend_lock.unlock();
        return;
    };
    self.unhealthy_backend_lock.unlock();
    std.log.warn("quarantined inference model path={s} backend={s} for {d}ms", .{
        model_dir,
        @tagName(backend),
        model_backend_quarantine_ns / std.time.ns_per_ms,
    });
}

fn modelBackendIsUnhealthy(
    self: *ModelManager,
    model_dir: []const u8,
    backend: backends.BackendType,
) bool {
    const key = modelBackendHealthKey(model_dir, backend);
    const now_ns = platform.time.monotonicNs();
    spinLock(&self.unhealthy_backend_lock);
    defer self.unhealthy_backend_lock.unlock();
    const retry_after_ns = self.unhealthy_model_backends.get(key) orelse return false;
    if (now_ns < retry_after_ns) return true;
    _ = self.unhealthy_model_backends.remove(key);
    return false;
}

fn backendVariantCacheKeyWithPrecision(allocator: std.mem.Allocator, model_dir: []const u8, backend: backends.BackendType, a4b_request: ?backend_contracts.A4bInferenceRequest, precision: ?session_factory.GlinerCudaPrecision) ![]u8 {
    const base = try backendVariantCacheKey(allocator, model_dir, backend, a4b_request);
    if (precision) |value| {
        defer allocator.free(base);
        return std.fmt.allocPrint(allocator, "{s}\ngliner_cuda_precision={s}", .{ base, @tagName(value) });
    }
    return base;
}

test "GLiNER CUDA precision isolates resident and in-flight loads" {
    const a = std.testing.allocator;
    const base = try backendVariantCacheKeyWithPrecision(a, "model", .cuda, null, null);
    defer a.free(base);
    var manager: ModelManager = undefined;
    manager.allocator = a;
    const values = [_]session_factory.GlinerCudaPrecision{ .auto, .fp32, .fp16, .bf16 };
    for (values, 0..) |value, index| {
        const key = try backendVariantCacheKeyWithPrecision(a, "model", .cuda, null, value);
        defer a.free(key);
        try std.testing.expect(!std.mem.eql(u8, base, key));
        const flight = try manager.loadFlightKey("model", &.{.cuda}, true, .{ .gliner_cuda_precision = value, .accept_default_alias = false });
        defer a.free(flight);
        for (values[0..index]) |other| {
            const other_key = try backendVariantCacheKeyWithPrecision(a, "model", .cuda, null, other);
            defer a.free(other_key);
            try std.testing.expect(!std.mem.eql(u8, key, other_key));
            const other_flight = try manager.loadFlightKey("model", &.{.cuda}, true, .{ .gliner_cuda_precision = other, .accept_default_alias = false });
            defer a.free(other_flight);
            try std.testing.expect(!std.mem.eql(u8, flight, other_flight));
        }
    }
}

test "GLiNER CUDA managed Ettin full context retains admission and recovers after denial" {
    if (!build_options.enable_cuda) return error.SkipZigTest;
    const directory = platform.env.getenv("ANTFLY_GLINER25_DECIDE_1B_MODEL_DIR") orelse return error.SkipZigTest;
    const precision = std.meta.stringToEnum(session_factory.GlinerCudaPrecision, platform.env.getenv("ANTFLY_GLINER25_FAMILY_CUDA_PRECISION") orelse "fp32") orelse return error.InvalidPrecision;
    const a = std.testing.allocator;
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, std.testing.io));
    manager.session_manager.test_allow_unqualified_gliner_cuda_precision = true;
    manager.configureServingPolicy(.{ .allow_unknown = true });
    defer manager.deinit();
    var handle = try manager.acquireFromDirWithGlinerCudaPrecision(directory, precision);
    defer handle.release();
    const loaded = handle.get();
    try qualifyGlinerEttinForTest(&manager, loaded, directory, precision);
}

/// Reused by the socket qualification test after its cold preload. Keeping
/// the same owner also verifies serving survives the failed managed runs.
pub fn qualifyGlinerEttinForTest(manager: *ModelManager, loaded: *LoadedModel, directory: []const u8, precision: session_factory.GlinerCudaPrecision) !void {
    if (!builtin.is_test) @compileError("GLiNER managed qualification is test-only");
    const a = std.testing.allocator;
    try std.testing.expectEqual(.cuda, loaded.session.backend());
    try std.testing.expect(loaded.session.run_admission != null);
    var alias = try manager.acquireFromDir(directory);
    defer alias.release();
    try std.testing.expectEqual(loaded, alias.get());
    var pipeline = loaded.glinerPipeline(a);
    try std.testing.expectEqual(@as(u32, 7999), pipeline.config.max_length);
    const bytes = try @import("../architectures/gliner/boundary_parity_test.zig").fixtureBytes(a, "family/decide_1b/capacity_cases.json");
    defer a.free(bytes);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer parsed.deinit();
    const cases = parsed.value.object.get("cases").?.array.items;
    const text = cases[cases.len - 1].object.get("texts").?.array.items[0].string;
    const gliner = @import("../pipelines/gliner.zig");
    const labels = [_]gliner.DecisionLabel{ .{ .name = "refund" }, .{ .name = "support" }, .{ .name = "cancel" } };
    const tasks = [_]gliner.DecisionTask{.{ .name = "intent", .labels = &labels }};
    const requests = [_]gliner.DecisionRequest{.{ .text = text, .tasks = &tasks }};
    const watchdog = try HardCancellationWatchdog.create(a);
    defer watchdog.destroy();
    try watchdog.start(std.testing.io);
    const Cancellation = struct {
        session: backends.Session,
        cancelled: bool = false,
        after_launch: ?usize = null,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.cancelled) return error.Cancelled;
            if (comptime session_factory.CudaRuntimeStats != void) {
                if (self.after_launch) |limit| {
                    if (session_factory.getCudaRuntimeStats(self.session).?.kernel_launches >= limit) return error.Cancelled;
                }
            }
        }
    };
    var cancellation = Cancellation{ .session = loaded.session };
    pipeline.execution_control = .{ .ptr = &cancellation, .check_fn = Cancellation.check, .hard_cancellation = watchdog.boundary(), .deadline_ns = platform.time.monotonicNs() + 180 * std.time.ns_per_s };
    manager.configureForcedRunAdmissionDenialsForTesting(1);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, pipeline.decideBatch(&requests));
    try std.testing.expectEqual(@as(usize, 0), manager.admissionController().snapshot().scratchTotalBytes());
    cancellation.cancelled = true;
    try std.testing.expectError(error.Cancelled, pipeline.decideBatch(&requests));
    try std.testing.expectEqual(@as(usize, 0), manager.admissionController().snapshot().scratchTotalBytes());
    cancellation.cancelled = false;
    if (comptime session_factory.CudaRuntimeStats != void) {
        const limit = session_factory.getCudaRuntimeStats(loaded.session).?.kernel_launches + 8;
        cancellation.after_launch = limit;
        try std.testing.expectError(error.Cancelled, pipeline.decideBatch(&requests));
        try std.testing.expect(session_factory.getCudaRuntimeStats(loaded.session).?.kernel_launches >= limit);
        try std.testing.expectEqual(@as(usize, 0), manager.admissionController().snapshot().scratchTotalBytes());
        cancellation.after_launch = null;
    }
    const results = try pipeline.decideBatch(&requests);
    defer {
        for (results) |*result| result.deinit(a);
        a.free(results);
    }
    try std.testing.expectEqual(@as(usize, 1), results.len);
    try std.testing.expectEqual(@as(usize, 7999), results[0].prompt_tokens);
    try std.testing.expectEqual(@as(usize, 0), results[0].tasks[0].selections[0].label_index);
    try std.testing.expectApproxEqAbs(@as(f32, 0.5019554), results[0].tasks[0].selections[0].probability, if (precision == .auto or precision == .fp32) @as(f32, 5e-4) else @as(f32, 5e-3));
    try std.testing.expectEqual(@as(usize, 0), manager.admissionController().snapshot().scratchTotalBytes());
    std.debug.print("family_cuda_managed: precision={s} tokens=7999 denial_recovered=true cancellation_recovered=true leases_released=true\n", .{@tagName(precision)});
}

fn backendVariantCacheKey(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    backend: backends.BackendType,
    a4b_request: ?backend_contracts.A4bInferenceRequest,
) ![]u8 {
    if (a4b_request) |request| {
        const identity = try a4bRequestCacheIdentity(allocator, request);
        defer allocator.free(identity);
        return std.fmt.allocPrint(
            allocator,
            "{s}\nbackend={s}\na4b={s}",
            .{
                model_dir,
                @tagName(backend),
                identity,
            },
        );
    }
    return std.fmt.allocPrint(allocator, "{s}\nbackend={s}", .{ model_dir, @tagName(backend) });
}

test "model prewarm defers while max-loaded eviction is pending" {
    try std.testing.expect(modelCacheHasPublicationCapacity(10, 0));
    try std.testing.expect(modelCacheHasPublicationCapacity(0, 1));
    try std.testing.expect(!modelCacheHasPublicationCapacity(1, 1));
    try std.testing.expect(!modelCacheHasPublicationCapacity(2, 1));
}

test "A4B model and load-flight cache keys isolate every load policy field" {
    const allocator = std.testing.allocator;
    const default_key = try backendVariantCacheKey(allocator, "model", .metal, null);
    defer allocator.free(default_key);
    const request = backend_contracts.A4bInferenceRequest{
        .residency_mode = .resident,
        .memory_budget_mb = 16384,
        .load_strategy = .pipeline,
        .load_workers = 4,
        .load_staging_mb = 256,
        .prepared_pack = .required,
        .drop_host_cache_after_load = true,
    };
    const variants = [_]backend_contracts.A4bInferenceRequest{
        .{ .residency_mode = .streamed, .memory_budget_mb = 16384, .load_strategy = .pipeline, .load_workers = 4, .load_staging_mb = 256, .prepared_pack = .required, .drop_host_cache_after_load = true },
        .{ .residency_mode = .resident, .memory_budget_mb = 4096, .load_strategy = .pipeline, .load_workers = 4, .load_staging_mb = 256, .prepared_pack = .required, .drop_host_cache_after_load = true },
        .{ .residency_mode = .resident, .memory_budget_mb = 16384, .load_strategy = .legacy, .load_workers = 4, .load_staging_mb = 256, .prepared_pack = .required, .drop_host_cache_after_load = true },
        .{ .residency_mode = .resident, .memory_budget_mb = 16384, .load_strategy = .pipeline, .load_workers = 6, .load_staging_mb = 256, .prepared_pack = .required, .drop_host_cache_after_load = true },
        .{ .residency_mode = .resident, .memory_budget_mb = 16384, .load_strategy = .pipeline, .load_workers = 4, .load_staging_mb = 384, .prepared_pack = .required, .drop_host_cache_after_load = true },
        .{ .residency_mode = .resident, .memory_budget_mb = 16384, .load_strategy = .pipeline, .load_workers = 4, .load_staging_mb = 256, .prepared_pack = .off, .drop_host_cache_after_load = true },
        .{ .residency_mode = .resident, .memory_budget_mb = 16384, .load_strategy = .pipeline, .load_workers = 4, .load_staging_mb = 256, .prepared_pack = .required, .drop_host_cache_after_load = false },
    };

    const model_key = try backendVariantCacheKey(allocator, "model", .cuda, request);
    defer allocator.free(model_key);
    try std.testing.expect(!std.mem.eql(u8, default_key, model_key));

    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    const flight_key = try manager.loadFlightKey(
        "model",
        &.{.cuda},
        false,
        inheritedA4bCachePolicy(request, true),
    );
    defer allocator.free(flight_key);

    for (variants) |variant| {
        const variant_model_key = try backendVariantCacheKey(allocator, "model", .cuda, variant);
        defer allocator.free(variant_model_key);
        try std.testing.expect(!std.mem.eql(u8, model_key, variant_model_key));

        const variant_flight_key = try manager.loadFlightKey(
            "model",
            &.{.cuda},
            false,
            inheritedA4bCachePolicy(variant, true),
        );
        defer allocator.free(variant_flight_key);
        try std.testing.expect(!std.mem.eql(u8, flight_key, variant_flight_key));
    }
}

test "inherited A4B policy isolates explicit loads but reuses policy-free aliases" {
    const request = backend_contracts.A4bInferenceRequest{
        .residency_mode = .streamed,
        .memory_budget_mb = 4096,
    };
    const isolated = inheritedA4bCachePolicy(request, false);
    try std.testing.expectEqual(request, isolated.a4b_request.?);
    try std.testing.expect(!isolated.accept_default_alias);
    try std.testing.expect(inheritedA4bCachePolicy(null, false).accept_default_alias);
}

test "explicit backend lookup reuses only a matching default alias" {
    const BackendProbe = struct {
        backend_type: backends.BackendType,

        fn run(_: *anyopaque, _: []const backends.Tensor, allocator: std.mem.Allocator) ![]backends.Tensor {
            return allocator.alloc(backends.Tensor, 0);
        }
        fn runWithControl(ptr: *anyopaque, inputs: []const backends.Tensor, allocator: std.mem.Allocator, control: InferenceExecutionControl) ![]backends.Tensor {
            try control.check();
            return run(ptr, inputs, allocator);
        }
        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn backend(ptr: *anyopaque) backends.BackendType {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backend_type;
        }
        fn close(_: *anyopaque) void {}
        fn session(self: *@This()) backends.Session {
            return .{ .ptr = self, .vtable = &vtable };
        }

        const vtable = backends.Session.VTable{
            .run = run,
            .runWithControl = runWithControl,
            .inputInfo = inputInfo,
            .outputInfo = outputInfo,
            .backend = backend,
            .close = close,
        };
    };

    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var aliases = manager.loaded_aliases.iterator();
        while (aliases.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded_aliases.deinit(allocator);
        var loaded = manager.loaded.iterator();
        while (loaded.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    var probe = BackendProbe{ .backend_type = .metal };
    var model: LoadedModel = undefined;
    model.managed_publication = null;
    model.session = probe.session();
    try manager.loaded_aliases.put(
        allocator,
        try allocator.dupe(u8, "model"),
        &model,
    );

    manager.lockLoadedModels();
    const matching = try manager.lookupLoadedModelLocked(
        "model",
        &.{.metal},
        false,
        inheritedA4bCachePolicy(null, true),
    );
    const mismatching = try manager.lookupLoadedModelLocked(
        "model",
        &.{.native},
        false,
        inheritedA4bCachePolicy(null, true),
    );
    const policy_free = try manager.lookupLoadedModelLocked(
        "model",
        &.{.native},
        true,
        inheritedA4bCachePolicy(null, false),
    );
    manager.unlockLoadedModels();

    try std.testing.expect(matching == &model);
    try std.testing.expect(mismatching == null);
    try std.testing.expect(policy_free == &model);

    // Startup preloads need not publish the path-only alias. Warm admission
    // must use the identical backend-qualified key used by request execution.
    const alias = manager.loaded_aliases.fetchRemove("model").?;
    allocator.free(alias.key);
    try manager.loaded.put(allocator, try backendVariantCacheKey(allocator, "model", .metal, null), &model);
    manager.session_manager.preferred_backends = &.{.metal};
    model.active_handles = 0;
    model.retired = false;
    var cached = (try manager.acquireCachedFromDir("model")).?;
    try std.testing.expect(cached.get() == &model);
    try std.testing.expectEqual(@as(usize, 1), model.active_handles);
    cached.release();
    try std.testing.expectEqual(@as(usize, 0), model.active_handles);
    try std.testing.expect((try manager.acquireCachedFromDir("cold model")) == null);
}

test "explicit A4B loads ignore unqualified model aliases" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var loaded = manager.loaded.iterator();
        while (loaded.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        var aliases = manager.loaded_aliases.iterator();
        while (aliases.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    var default_model: LoadedModel = undefined;
    default_model.managed_publication = null;
    try manager.loaded_aliases.put(
        allocator,
        try allocator.dupe(u8, "model"),
        &default_model,
    );
    const request = backend_contracts.A4bInferenceRequest{
        .residency_mode = .streamed,
        .memory_budget_mb = 4096,
    };
    manager.lockLoadedModels();
    const alias_miss = try manager.lookupLoadedModelLocked(
        "model",
        &.{.metal},
        true,
        .{ .a4b_request = request, .accept_default_alias = false },
    );
    manager.unlockLoadedModels();
    try std.testing.expect(alias_miss == null);

    var streamed_model: LoadedModel = undefined;
    streamed_model.managed_publication = null;
    const streamed_key = try backendVariantCacheKey(allocator, "model", .metal, request);
    try manager.loaded.put(allocator, streamed_key, &streamed_model);
    manager.lockLoadedModels();
    const qualified_hit = try manager.lookupLoadedModelLocked(
        "model",
        &.{.metal},
        true,
        .{ .a4b_request = request, .accept_default_alias = false },
    );
    manager.unlockLoadedModels();
    try std.testing.expect(qualified_hit == &streamed_model);
}

fn admissionEvictionTestModel(last_used_ns: u64) LoadedModel {
    const Stub = struct {
        var probe: TeardownTestProbe = .{};
    };
    var model: LoadedModel = undefined;
    model.managed_publication = null;
    // Metadata-only tests never enter or close this foreign architecture.
    model.session = .{ .ptr = &Stub.probe, .vtable = &TeardownTestProbe.vtable };
    model.active_handles = 0;
    model.last_used_ns = last_used_ns;
    model.pinned = false;
    model.retired = false;
    model.resource_lease = null;
    model.tokenizer_resource_lease = null;
    model.whisper_prompt_admission = null;
    model.vision_resource_lease = null;
    model.audio_resource_lease = null;
    model.text_projection_resource_lease = null;
    model.visual_projection_resource_lease = null;
    model.audio_projection_resource_lease = null;
    return model;
}

test "model cache eviction skips active and pinned models and removes aliases" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var it = manager.loaded.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        var aliases = manager.loaded_aliases.iterator();
        while (aliases.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    var idle: LoadedModel = undefined;
    idle.managed_publication = null;
    idle.active_handles = 0;
    idle.last_used_ns = 10;
    idle.pinned = false;
    var active: LoadedModel = undefined;
    active.managed_publication = null;
    active.active_handles = 1;
    active.last_used_ns = 1;
    active.pinned = false;
    var pinned: LoadedModel = undefined;
    pinned.managed_publication = null;
    pinned.active_handles = 0;
    pinned.last_used_ns = 0;
    pinned.pinned = true;

    try manager.loaded.put(allocator, try allocator.dupe(u8, "idle"), &idle);
    try manager.loaded.put(allocator, try allocator.dupe(u8, "active"), &active);
    try manager.loaded.put(allocator, try allocator.dupe(u8, "pinned"), &pinned);
    try manager.loaded_aliases.put(
        allocator,
        try allocator.dupe(u8, "idle-alias"),
        &idle,
    );

    manager.lockLoadedModels();
    const evicted = manager.takeLruModelLocked(100, false, null);
    manager.unlockLoadedModels();
    try std.testing.expect(evicted != null);
    try std.testing.expect(evicted.?.model == &idle);
    try std.testing.expectEqual(@as(usize, 2), manager.loaded.count());
    try std.testing.expectEqual(@as(usize, 0), manager.loaded_aliases.count());
    manager.lockLoadedModels();
    const after_eviction = try manager.lookupLoadedModelLocked(
        "idle-alias",
        manager.session_manager.preferred_backends,
        true,
        .{},
    );
    manager.unlockLoadedModels();
    try std.testing.expect(after_eviction == null);
    allocator.free(evicted.?.key);
}

test "loaded model snapshot preserves TTL while ordinary inference release records use" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var it = manager.loaded.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }
    manager.configureModelCache(1, 1);
    var model: LoadedModel = undefined;
    model.managed_publication = null;
    model.active_handles = 0;
    model.last_used_ns = 1;
    model.pinned = false;
    model.retired = false;
    try manager.loaded.put(allocator, try allocator.dupe(u8, "model"), &model);
    const old_expiry = model.last_used_ns + std.time.ns_per_ms;

    for (0..3) |_| {
        var snapshot = try manager.acquireLoadedModelSnapshot(allocator);
        defer snapshot.deinit();
        try std.testing.expectEqual(@as(usize, 1), snapshot.handles.len);
        try std.testing.expectEqual(@as(usize, 1), model.active_handles);
        try std.testing.expect(snapshot.handles[0].get() == &model);
        manager.lockLoadedModels();
        const held = manager.takeLruModelLocked(old_expiry, true, null);
        manager.unlockLoadedModels();
        defer if (held) |removed| allocator.free(removed.key);
        try std.testing.expect(held == null);
        snapshot.deinit();
        try std.testing.expectEqual(@as(usize, 0), model.active_handles);
        try std.testing.expectEqual(@as(u64, 1), model.last_used_ns);
    }

    // A real inference handle can finish while observation remains pinned.
    // Releasing that observer must neither reset nor renew the inference time.
    var observation = try manager.acquireLoadedModelSnapshot(allocator);
    defer observation.deinit();
    var inference = manager.acquireLoadedModel("model") orelse return error.MissingTestModel;
    defer inference.release();
    try std.testing.expectEqual(@as(usize, 2), model.active_handles);
    var duplicate = inference.duplicate();
    defer duplicate.release();
    try std.testing.expect(duplicate.get() == inference.get());
    try std.testing.expectEqual(@as(usize, 3), model.active_handles);
    const before_release = platform.time.monotonicNs();
    inference.release();
    const used_at = model.last_used_ns;
    try std.testing.expect(used_at >= before_release and used_at > 1);
    try std.testing.expectEqual(@as(usize, 2), model.active_handles);
    duplicate.release();
    const duplicate_used_at = model.last_used_ns;
    try std.testing.expect(duplicate_used_at >= used_at);
    try std.testing.expectEqual(@as(usize, 1), model.active_handles);
    observation.deinit();
    try std.testing.expectEqual(duplicate_used_at, model.last_used_ns);
    try std.testing.expectEqual(@as(usize, 0), model.active_handles);

    manager.lockLoadedModels();
    const not_yet = manager.takeLruModelLocked(duplicate_used_at + std.time.ns_per_ms - 1, true, null);
    manager.unlockLoadedModels();
    defer if (not_yet) |removed| allocator.free(removed.key);
    try std.testing.expect(not_yet == null);
    manager.lockLoadedModels();
    const expired = manager.takeLruModelLocked(duplicate_used_at + std.time.ns_per_ms, true, null);
    manager.unlockLoadedModels();
    defer if (expired) |removed| allocator.free(removed.key);
    try std.testing.expect(expired != null and expired.?.model == &model);
    try std.testing.expectEqual(@as(usize, 0), manager.loaded.count());
}

test "loaded model snapshot allocation failure preserves lifetime usage and retry" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var it = manager.loaded.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }
    var model: LoadedModel = undefined;
    model.managed_publication = null;
    model.active_handles = 0;
    model.last_used_ns = 123;
    model.pinned = false;
    model.retired = false;
    try manager.loaded.put(allocator, try allocator.dupe(u8, "model"), &model);
    var failure = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, manager.acquireLoadedModelSnapshot(failure.allocator()));
    try std.testing.expectEqual(@as(usize, 0), model.active_handles);
    try std.testing.expectEqual(@as(u64, 123), model.last_used_ns);
    try std.testing.expectEqual(@as(usize, 1), manager.loaded.count());
    // The failed acquisition also releases load_lock before this retry.
    var retry = try manager.acquireLoadedModelSnapshot(allocator);
    defer retry.deinit();
    try std.testing.expectEqual(@as(usize, 1), model.active_handles);
    retry.deinit();
    try std.testing.expectEqual(@as(usize, 0), model.active_handles);
    try std.testing.expectEqual(@as(u64, 123), model.last_used_ns);
}

test "loaded model snapshot final retired observation keeps protected cleanup and admission" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 64 });
    try manager.ensureResourceOwnerReady();
    var probe = TeardownTestProbe{};
    const model = try teardownTestModel(&manager, &probe);
    var inference = try manager.publishLoadedModel(model, true, null, null, 0);
    defer inference.release();
    var snapshot = try manager.acquireLoadedModelSnapshot(allocator);
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 2), model.active_handles);
    const used_at = model.last_used_ns;
    inference.retire();
    try std.testing.expectEqual(@as(usize, 0), manager.loaded.count());
    try std.testing.expectEqual(@as(usize, 0), manager.loaded_aliases.count());
    try std.testing.expect(model.retired and !probe.closed);
    try std.testing.expectEqual(@as(usize, 1), model.active_handles);
    try std.testing.expectEqual(used_at, model.last_used_ns);
    try std.testing.expectEqual(@as(usize, 64), manager.admissionController().snapshot().host_weight_bytes);
    // This is the final owner. The existing fake process-required session
    // asserts active close protection and its 64-byte lease inside close().
    snapshot.deinit();
    try std.testing.expect(probe.closed);
    try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
    const watchdog = manager.teardown_domain.?.watchdog;
    spinLock(&watchdog.mutex);
    defer watchdog.mutex.unlock();
    try std.testing.expectEqual(@as(usize, 0), watchdog.entries.items.len);
}

test "warm Qwen admission copy releases its pin and permits single-model recovery" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.metal} });
    defer manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 64 });
    try manager.ensureResourceOwnerReady();
    var probe = TeardownTestProbe{};
    const model = try teardownTestModel(&manager, &probe);
    model.manifest.embedding_style = .qwen3_embedding;
    model.manifest.hidden_size = 1024;
    model.manifest.embedding_profile.query.prefix = try allocator.dupe(u8, "Instruct: retrieve\nQuery: ");
    model.manifest.inputs = try allocator.alloc([]const u8, 1);
    model.manifest.inputs[0] = try allocator.dupe(u8, "text");
    var inference = try manager.publishLoadedModel(model, true, null, null, 0);
    defer inference.release();

    var failure = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, manager.copyCachedQwenEmbeddingManifest(failure.allocator(), "teardown-fixture"));
    try std.testing.expectEqual(@as(usize, 1), model.active_handles);
    try std.testing.expect(!probe.closed);
    var copy = (try manager.copyCachedQwenEmbeddingManifest(allocator, "teardown-fixture")) orelse return error.MissingTestModel;
    defer copy.deinit();
    try std.testing.expectEqual(@as(usize, 1), model.active_handles);
    inference.retire();
    try std.testing.expect(probe.closed);
    try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
    // Metadata remains usable after all old runtime storage has been freed.
    try std.testing.expectEqual(@as(u32, 1024), copy.hidden_size);
    try std.testing.expectEqualStrings("Instruct: retrieve\nQuery: ", copy.queryPrefix());
    try std.testing.expectEqualStrings("text", copy.inputs[0]);
    // The same hard cap that admitted one model admits its replacement now.
    var replacement = try manager.acquireAmountsWithEviction(.cpu, .{ .host_limit_bytes = 64 }, .{ .host_weight_bytes = 64 });
    defer replacement.release();
    try std.testing.expectEqual(@as(usize, 64), manager.admissionController().snapshot().host_weight_bytes);
    try std.testing.expect((try manager.copyCachedQwenEmbeddingManifest(allocator, "teardown-fixture")) == null);
}

test "failed loaded model retires from lookup while active handles unwind" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        manager.loaded.deinit(allocator);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    const SessionProbe = struct {
        fn run(_: *anyopaque, _: []const backends.Tensor, alloc: std.mem.Allocator) ![]backends.Tensor {
            return alloc.alloc(backends.Tensor, 0);
        }
        fn runWithControl(ptr: *anyopaque, inputs: []const backends.Tensor, alloc: std.mem.Allocator, control: InferenceExecutionControl) ![]backends.Tensor {
            try control.check();
            return run(ptr, inputs, alloc);
        }
        fn inputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn outputInfo(_: *anyopaque) []const backends.TensorInfo {
            return &.{};
        }
        fn backend(_: *anyopaque) backends.BackendType {
            return .metal;
        }
        fn close(_: *anyopaque) void {}

        var state: u8 = 0;
        const vtable = backends.Session.VTable{
            .run = run,
            .runWithControl = runWithControl,
            .inputInfo = inputInfo,
            .outputInfo = outputInfo,
            .backend = backend,
            .close = close,
        };
    };

    var model: LoadedModel = undefined;
    model.managed_publication = null;
    model.model_dir = "owner/model";
    model.session = .{ .ptr = @ptrCast(&SessionProbe.state), .vtable = &SessionProbe.vtable };
    model.active_handles = 2;
    model.last_used_ns = 1;
    model.pinned = true;
    model.retired = false;

    try manager.loaded.put(allocator, try allocator.dupe(u8, "owner/model\nbackend=metal"), &model);
    try manager.loaded_aliases.put(allocator, try allocator.dupe(u8, "owner/model"), &model);
    const flight = try allocator.create(LoadFlight);
    defer allocator.destroy(flight);
    flight.* = .{
        .io = std.testing.io,
        .model = &model,
        .refs = 2,
        .task_ref_pending = false,
    };
    try manager.in_flight_loads.put(allocator, try allocator.dupe(u8, "flight"), flight);

    manager.retireLoadedModel(&model);

    try std.testing.expect(model.retired);
    try std.testing.expect(!model.pinned);
    try std.testing.expectEqual(@as(usize, 0), manager.loaded.count());
    try std.testing.expectEqual(@as(usize, 0), manager.loaded_aliases.count());
    try std.testing.expectEqual(@as(usize, 0), manager.in_flight_loads.count());
    try std.testing.expectEqual(@as(usize, 4), model.active_handles);
    try std.testing.expect(!flight.registered);
    try std.testing.expect(flight.handles_reserved);
}

test "admission eviction skips older models outside the rejected domain" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var it = manager.loaded.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    const cpu_host = runtime.tier.memory.AdmissionAmounts{ .host_weight_bytes = 64 };
    const gpu_backend = runtime.tier.memory.AdmissionAmounts{ .backend_weight_bytes = 64 };
    var older_cpu = admissionEvictionTestModel(1);
    older_cpu.resource_lease = .{
        .controller = null,
        .amounts = cpu_host,
        .amounts_by_backend = .{ cpu_host, .{} },
        .retain_backend_class = null,
        .live_reserved_bytes = 0,
    };
    var newer_gpu = admissionEvictionTestModel(10);
    // Sidecar leases must participate in victim selection just like the primary
    // model lease; multimodal models can hold most of their GPU residency here.
    newer_gpu.vision_resource_lease = .{
        .controller = null,
        .amounts = gpu_backend,
        .amounts_by_backend = .{ .{}, gpu_backend },
        .retain_backend_class = null,
        .live_reserved_bytes = 0,
    };

    try manager.loaded.put(allocator, try allocator.dupe(u8, "older-cpu"), &older_cpu);
    try manager.loaded.put(allocator, try allocator.dupe(u8, "newer-gpu"), &newer_gpu);

    manager.lockLoadedModels();
    const evicted = manager.takeLruModelLocked(
        100,
        false,
        .{ .domain_backend = .gpu },
    );
    manager.unlockLoadedModels();
    try std.testing.expect(evicted != null);
    try std.testing.expect(evicted.?.model == &newer_gpu);
    allocator.free(evicted.?.key);

    manager.lockLoadedModels();
    const no_relevant_victim = manager.takeLruModelLocked(
        100,
        false,
        .{ .domain_backend = .gpu },
    );
    manager.unlockLoadedModels();
    try std.testing.expect(no_relevant_victim == null);
    try std.testing.expectEqual(@as(usize, 1), manager.loaded.count());
    try std.testing.expect(manager.loaded.get("older-cpu") == &older_cpu);
}

test "admission eviction selects idle GPU workspace and never reads active workspace state" {
    const Snapshot = struct {
        amounts: runtime.tier.memory.AdmissionAmounts = .{},
        readable: bool = true,
        reads: usize = 0,

        pub fn read(session: backends.Session) runtime.tier.memory.AdmissionAmounts {
            const self: *@This() = @ptrCast(@alignCast(session.ptr));
            std.debug.assert(self.readable);
            self.reads += 1;
            return self.amounts;
        }
    };
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var it = manager.loaded.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        var aliases = manager.loaded_aliases.iterator();
        while (aliases.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }
    const owned_workspace = runtime.tier.memory.AdmissionAmounts{ .host_scratch_bytes = 8, .backend_scratch_bytes = 64 };
    var active_snapshot = Snapshot{ .amounts = owned_workspace, .readable = false };
    var idle_snapshot = Snapshot{ .amounts = owned_workspace };
    var empty_snapshot = Snapshot{};
    var active = admissionEvictionTestModel(1);
    active.session.ptr = &active_snapshot;
    active.active_handles = 1;
    var idle = admissionEvictionTestModel(2);
    idle.session.ptr = &idle_snapshot;
    var empty = admissionEvictionTestModel(0);
    empty.session.ptr = &empty_snapshot;
    try manager.loaded.put(allocator, try allocator.dupe(u8, "active"), &active);
    try manager.loaded.put(allocator, try allocator.dupe(u8, "idle"), &idle);
    try manager.loaded.put(allocator, try allocator.dupe(u8, "empty"), &empty);
    try manager.loaded_aliases.put(allocator, try allocator.dupe(u8, "idle-alias"), &idle);
    // GPU workspace cannot relieve CPU-domain or KV pressure. Active state
    // must remain unread even while these unsuccessful searches scan the map.
    for ([_]runtime.tier.memory.AdmissionPressure{ .{ .domain_scratch = .cpu }, .{ .domain_backend = .cpu }, .{ .domain_kv = .gpu } }) |pressure| {
        manager.lockLoadedModels();
        const absent = manager.takeLruModelWithWorkspaceLocked(100, false, pressure, Snapshot.read);
        manager.unlockLoadedModels();
        defer if (absent) |found| allocator.free(found.key);
        try std.testing.expect(absent == null);
    }
    manager.lockLoadedModels();
    const chosen = manager.takeLruModelWithWorkspaceLocked(100, false, .{ .domain_scratch = .gpu }, Snapshot.read);
    manager.unlockLoadedModels();
    defer if (chosen) |found| allocator.free(found.key);
    try std.testing.expect(chosen != null and chosen.?.model == &idle);
    try std.testing.expectEqual(@as(usize, 0), manager.loaded_aliases.count());
    try std.testing.expectEqual(@as(usize, 0), active_snapshot.reads);
    try std.testing.expect(idle_snapshot.reads > 0);
    // Release through the real handle owner; only then may eviction inspect
    // the formerly active model's stable workspace accounting.
    var handle = ModelHandle{ .manager = &manager, .model = &active };
    handle.release();
    active_snapshot.readable = true;
    manager.lockLoadedModels();
    const released = manager.takeLruModelWithWorkspaceLocked(100, false, .{ .domain_scratch = .gpu }, Snapshot.read);
    manager.unlockLoadedModels();
    defer if (released) |found| allocator.free(found.key);
    try std.testing.expect(released != null and released.?.model == &active);
    try std.testing.expectEqual(@as(usize, 1), active_snapshot.reads);
    try std.testing.expectEqual(@as(usize, 1), manager.loaded.count());
}

test "model cache idle expiration can be disabled" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer {
        var it = manager.loaded.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        manager.loaded.deinit(allocator);
        manager.loaded_aliases.deinit(allocator);
        manager.in_flight_loads.deinit(allocator);
    }

    var idle: LoadedModel = undefined;
    idle.managed_publication = null;
    idle.active_handles = 0;
    idle.last_used_ns = 1;
    idle.pinned = false;
    try manager.loaded.put(allocator, try allocator.dupe(u8, "idle"), &idle);

    manager.keep_alive_ms = 0;
    manager.lockLoadedModels();
    const disabled = manager.takeLruModelLocked(std.time.ns_per_s, true, null);
    manager.unlockLoadedModels();
    try std.testing.expect(disabled == null);

    manager.keep_alive_ms = 1;
    manager.lockLoadedModels();
    const expired = manager.takeLruModelLocked(std.time.ns_per_s, true, null);
    manager.unlockLoadedModels();
    try std.testing.expect(expired != null);
    try std.testing.expect(expired.?.model == &idle);
    allocator.free(expired.?.key);
}

const preferredModelPathForBackend = session_loader.preferredModelPathForBackend;

fn sessionManagerForPreferredBackends(
    allocator: std.mem.Allocator,
    preferred_backends: []const backends.BackendType,
    source: *const backends.SessionManager,
) backends.SessionManager {
    return source.withPreferredBackends(allocator, preferred_backends);
}

test "session manager load clones preserve process isolation policy" {
    var source = backends.SessionManager.init(std.testing.allocator);
    source.process_termination_available = false;
    source.native_call_policy = .in_process;
    const clone = sessionManagerForPreferredBackends(
        std.testing.allocator,
        &.{.native},
        &source,
    );
    try std.testing.expect(!clone.process_termination_available);
    try std.testing.expectEqual(execution_control_mod.NativeCallPolicy.in_process, clone.native_call_policy);
}

test "GLiNER CUDA load clones retain per-manager qualification policy" {
    var source = backends.SessionManager.init(std.testing.allocator);
    const ordinary = sessionManagerForPreferredBackends(std.testing.allocator, &.{.cuda}, &source);
    try std.testing.expect(!ordinary.test_allow_unqualified_gliner_cuda_precision);
    source.gliner_cuda_precision = .fp16;
    source.test_allow_unqualified_gliner_cuda_precision = true;
    const candidate = sessionManagerForPreferredBackends(std.testing.allocator, &.{.cuda}, &source);
    try std.testing.expect(candidate.test_allow_unqualified_gliner_cuda_precision);
    try std.testing.expectEqual(.fp16, candidate.gliner_cuda_precision.?);
    try std.testing.expect(!ordinary.test_allow_unqualified_gliner_cuda_precision);
}

pub const ManagedSession = session_loader.ManagedSession;
pub const ArtifactSession = session_loader.ArtifactSession;

pub const ManagedHfTokenizer = @import("../models/managed_tokenizer.zig").Managed;

const LoadedSessionPlan = ManagedSession;

pub const GenerationAdmissionRequest = struct {
    session: backends.Session,
    resources: runtime.tier.memory.AdmissionRequest,
};

pub const GenerationAdmissionLease = struct {
    resources: ?runtime.tier.memory.AdmissionLease = null,
    workspace: [2]?runtime.tier.memory.AdmissionLease = .{ null, null },

    pub fn release(self: *@This()) void {
        if (self.resources) |*lease| lease.release();
        for (&self.workspace) |*slot| if (slot.*) |*lease| lease.release();
    }
};

const ModelLoadAdmissionPlan = load_plan.ModelLoadAdmissionPlan;

const tokenizer_fixed_resident_bytes = load_plan.tokenizer_fixed_resident_bytes;
const tokenizer_fixed_peak_bytes = load_plan.tokenizer_fixed_peak_bytes;

/// Tokenizer parsers expand compact JSON/protobuf vocabularies into multiple
/// owned hash maps and tries. Reserve a deliberately conservative construction
/// peak before reading the artifact, then retain the estimated live structures.
const tokenizerAdmissionPlan = load_plan.tokenizerAdmissionPlan;

const tokenizerFileAdmissionPlan = load_plan.tokenizerFileAdmissionPlan;

const tokenizerLoadAdmissionPlan = load_plan.tokenizerLoadAdmissionPlan;

const ComponentArtifactEstimate = load_plan.ComponentArtifactEstimate;

test "tokenizer admission reserves parse peak and retains live structures" {
    const encoded: usize = 2 * 1024 * 1024;
    const plan = try tokenizerAdmissionPlan(encoded, .huggingface);
    try std.testing.expectEqual(
        tokenizer_fixed_peak_bytes + 13 * encoded,
        plan.peak.host_weight_bytes,
    );
    try std.testing.expectEqual(
        tokenizer_fixed_resident_bytes + 10 * encoded,
        plan.resident.host_weight_bytes,
    );
    try std.testing.expect(plan.peak.host_weight_bytes > plan.resident.host_weight_bytes);
}

test "admission eviction only selects leases that relieve the rejected constraint" {
    const cpu_host = runtime.tier.memory.AdmissionAmounts{ .host_weight_bytes = 64 };
    const tokenizer_lease = runtime.tier.memory.AdmissionLease{
        .controller = null,
        .amounts = cpu_host,
        .amounts_by_backend = .{ cpu_host, .{} },
        .retain_backend_class = null,
        .live_reserved_bytes = 0,
    };

    try std.testing.expect(ModelManager.admissionReclaimRelevant(
        tokenizer_lease,
        .shared_host,
    ));
    try std.testing.expect(ModelManager.admissionReclaimRelevant(
        tokenizer_lease,
        .{ .domain_host = .cpu },
    ));
    try std.testing.expect(!ModelManager.admissionReclaimRelevant(
        tokenizer_lease,
        .{ .domain_host = .gpu },
    ));
    try std.testing.expect(!ModelManager.admissionReclaimRelevant(
        tokenizer_lease,
        .{ .domain_backend = .cpu },
    ));
    try std.testing.expect(!ModelManager.admissionReclaimRelevant(
        tokenizer_lease,
        .{ .domain_scratch = .cpu },
    ));
}

const estimateModelArtifactBytes = load_plan.estimateModelArtifactBytes;

/// A multimodal projector is opened, mapped, and consumed by an individual
/// generation request; it is not part of the decoder session's retained
/// residency. Charge its mapped artifact to the request that owns that mapping.
/// Integrated vision weights are already resident, but their request scratch
/// must still be reserved even when there is no separate projector artifact.
pub fn projectorRunAdmissionAmounts(
    man: manifest_mod.ModelManifest,
    backend: runtime.kv.pool.BackendKind,
    media: generation.NativeGenerationMediaAdmission,
) !runtime.tier.memory.AdmissionAmounts {
    const bytes = if (man.gguf_projector_path) |path| std.math.cast(
        usize,
        try c_file.fileSize(man.allocator, path),
    ) orelse return error.ResourceLimitExceeded else 0;
    return switch (backend) {
        .native => .{
            .host_weight_bytes = bytes,
            .host_scratch_bytes = std.math.add(
                usize,
                media.host_scratch_bytes,
                media.backend_scratch_bytes,
            ) catch return error.ResourceLimitExceeded,
        },
        .metal, .cuda => .{
            // The GGUF mapping remains resident on the host while the request
            // weight cache exposes the same tensors to the accelerator.
            .host_weight_bytes = bytes,
            .backend_weight_bytes = bytes,
            .host_scratch_bytes = media.host_scratch_bytes,
            .backend_scratch_bytes = media.backend_scratch_bytes,
        },
    };
}

const a4bGpuModelLoadAdmission = load_plan.a4bGpuModelLoadAdmission;

test "A4B GPU admission lease equals the configured memory envelope" {
    const config = try backend_contracts.buildCudaA4bInferenceConfig(
        null,
        backend_contracts.qualified_a4b_geometries[0],
    );
    try std.testing.expectEqual(
        @as(u64, backend_contracts.qualified_cuda_a4b_memory_budget_mb) * 1024 * 1024,
        config.memory_budget_bytes,
    );
    const metal_plan = a4bGpuModelLoadAdmission(config, 1234, .metal);
    try std.testing.expectEqual(@as(usize, @intCast(config.memory_budget_bytes)), metal_plan.resident.backendTotalBytes());
    try std.testing.expectEqual(metal_plan.resident, metal_plan.peak);
    const cuda_plan = a4bGpuModelLoadAdmission(config, 1234, .cuda);
    try std.testing.expectEqual(cuda_plan.resident.backendTotalBytes(), cuda_plan.peak.backendTotalBytes());
    try std.testing.expectEqual(@as(usize, 1234), cuda_plan.peak.host_weight_bytes);
    try std.testing.expectEqual(@as(usize, 0), cuda_plan.resident.host_weight_bytes);
}

test "CUDA A4B serving reuses retained workspace and unwinds failed requests" {
    const memory = runtime.tier.memory;
    const Reject = struct {
        // A failing assertion must still retire an unexpectedly successful
        // resource/workspace lease before the fixture tears down its owners.
        fn expect(expected: anyerror, result: anytype) !void {
            if (result) |value| {
                var lease = value;
                lease.release();
                std.testing.failPrint("expected error.{s}, found a successful lease\n", .{@errorName(expected)});
                return error.TestExpectedError;
            } else |actual| {
                try std.testing.expectError(expected, @as(anyerror!void, actual));
            }
        }
    };
    for ([_]?backend_contracts.A4bInferenceRequest{ null, .{ .residency_mode = .resident, .memory_budget_mb = 24 * 1024 } }) |request| {
        const config = try backend_contracts.buildCudaA4bInferenceConfig(request, backend_contracts.qualified_a4b_geometries[0]);
        const plan = a4bGpuModelLoadAdmission(config, 4 * 1024 * 1024 * 1024, .cuda);
        var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
        defer manager.deinit();
        try manager.ensureResourceOwnerReady();
        const controller = manager.admissionController();
        // No physical GPU allocation: the independent host metadata bound
        // still belongs to the request, outside the credited GPU workspace.
        controller.configureSharedLimits(.{});
        var model_lease = try controller.tryAcquire(.gpu, plan.limit_floor, plan.peak, false);
        defer model_lease.release();
        try model_lease.retain(plan.resident);
        var workspace = memory.ReservedGenerationWorkspace{
            .floor = plan.serving_floor.?,
            .capacity = .{ .backend_kv_bytes = plan.resident.backend_kv_bytes, .backend_scratch_bytes = plan.resident.backend_scratch_bytes },
        };
        defer workspace.borrowed.deinit();
        const session = backends.Session{ .ptr = &workspace, .vtable = undefined, .generation_workspace = &workspace };
        const strict_host_limits = try session_factory.resolveBudgetLimitsForSessions(&.{session}, .{ .backend_limit_bytes = 9 * 1024 * 1024 * 1024 }, .{ .host_limit_bytes = 1024 });
        try std.testing.expectEqual(@as(usize, 1024), strict_host_limits.host_limit_bytes);
        const replay_capacity = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY") orelse 0;
        const view_metadata = try @import("../runtime/kv/view_metadata.zig").hostBytes(@max(@as(usize, 2049), replay_capacity), memory.generation_kv_page_size_tokens, false);
        const owner_spills = try @import("../runtime/kv/writable_blocks.zig").spillHostBytes(2049, 2048, replay_capacity, memory.generation_kv_page_size_tokens, false);
        const host_metadata = try std.math.add(usize, view_metadata, try std.math.mul(usize, 2, owner_spills));
        const operator_host_limit = @max(@as(usize, 1024), host_metadata);
        const limits = try session_factory.resolveBudgetLimitsForSessions(&.{session}, .{ .backend_limit_bytes = 9 * 1024 * 1024 * 1024 }, .{ .host_limit_bytes = operator_host_limit });
        try std.testing.expectEqual(operator_host_limit, limits.host_limit_bytes);
        try std.testing.expectEqual(@as(usize, @intCast(config.memory_budget_bytes)), limits.backend_limit_bytes);
        try std.testing.expectEqual(limits.host_limit_bytes + limits.backend_limit_bytes, limits.combined_limit_bytes);
        // Plan a real long prompt before borrowing. The generic scratch cap
        // permits a chunk larger than the session's retained workspace.
        const gpt_config = @import("../models/gpt.zig").Config{
            .family = .gemma,
            .hidden_size = 2816,
            .num_hidden_layers = 30,
            .num_attention_heads = 16,
            .num_key_value_heads = 1,
            .vocab_size = 262144,
            .num_local_experts = 128,
            .num_experts_per_tok = 8,
            .num_shared_experts = 1,
            .expert_intermediate_size = 704,
        };
        const components = [_]memory.GptGenerationBudgetComponent{.{
            .backend = .cuda,
            .kv_dtype = .f16,
            .config = gpt_config,
            .workspace_capacity = session.generationWorkspaceCapacity(),
        }};
        var sizing_limits = limits;
        sizing_limits.scratch_limit_bytes = 384 * 1024 * 1024;
        var sizing = memory.RunBudget.init(sizing_limits);
        const prefill = try memory.reserveGptGenerationPrefill(&sizing, &components, 2048, 1, 2048);
        try std.testing.expect(prefill.max_chunk_rows < 2048);
        const estimate = try memory.estimateGptGeneration(.cuda, .f16, gpt_config, 2048, 1, prefill.max_chunk_rows);
        try std.testing.expect(estimate.scratch_bytes <= workspace.capacity.backend_scratch_bytes);
        // The old strict operator cap remains strict: actual mandatory host
        // metadata cannot be paid by the precharged device workspace.
        try std.testing.expect(host_metadata > strict_host_limits.host_limit_bytes);
        try Reject.expect(error.ResourceLimitExceeded, manager.acquireGenerationResources(&.{.{
            .session = session,
            .resources = .{ .backend_class = .gpu, .limits = strict_host_limits, .amounts = try memory.AdmissionAmounts.fromEstimate(estimate) },
        }}));
        try std.testing.expectEqualDeep(plan.resident, controller.snapshot());
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, workspace.borrowed.snapshot());
        var prompt_lease = try manager.acquireGenerationResources(&.{.{
            .session = session,
            .resources = .{ .backend_class = .gpu, .limits = limits, .amounts = try runtime.tier.memory.AdmissionAmounts.fromEstimate(estimate) },
        }});
        defer prompt_lease.release();
        try std.testing.expectEqual(host_metadata, estimate.host_metadata_bytes);
        const with_request_host = try plan.resident.merge(.{ .host_scratch_bytes = estimate.host_metadata_bytes });
        try std.testing.expectEqualDeep(with_request_host, controller.snapshot());
        prompt_lease.release();
        try std.testing.expectEqualDeep(plan.resident, controller.snapshot());
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, workspace.borrowed.snapshot());
        const one_byte = memory.AdmissionAmounts{ .backend_scratch_bytes = 1 };
        // AJ's reproduction: preserving the floor alone still double-charges.
        try Reject.expect(error.ResourceTemporarilyUnavailable, controller.tryAcquire(.gpu, limits, one_byte, false));
        var requests = [_]GenerationAdmissionRequest{.{ .session = session, .resources = .{ .backend_class = .gpu, .limits = limits, .amounts = one_byte } }};
        var ordinary = try manager.acquireGenerationResources(&requests);
        defer ordinary.release();
        try std.testing.expectEqualDeep(plan.resident, controller.snapshot());
        ordinary.release();
        // A streaming lifetime can hold the complete KV/scratch allowance;
        // another borrower cannot spend the same bytes until it releases.
        requests[0].resources.amounts = workspace.capacity;
        var streaming = try manager.acquireGenerationResources(&requests);
        defer streaming.release();
        try std.testing.expectEqualDeep(plan.resident, controller.snapshot());
        requests[0].resources.amounts = one_byte;
        try Reject.expect(error.ResourceTemporarilyUnavailable, manager.acquireGenerationResources(&requests));
        streaming.release();
        var retry = try manager.acquireGenerationResources(&requests);
        defer retry.release();
        retry.release();
        // Operator caps still apply even when the global request increment is zero.
        inline for (.{ memory.Limits{ .backend_limit_bytes = limits.backend_limit_bytes - 1 }, memory.Limits{ .combined_limit_bytes = limits.backend_limit_bytes - 1 }, memory.Limits{ .kv_limit_bytes = plan.resident.backend_kv_bytes - 1 }, memory.Limits{ .scratch_limit_bytes = plan.resident.backend_scratch_bytes - 1 } }) |cap| {
            requests[0].resources.limits = memory.applyLimitOverrides(limits, cap);
            try Reject.expect(error.ResourceTemporarilyUnavailable, manager.acquireGenerationResources(&requests));
            try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, workspace.borrowed.snapshot());
        }
        requests[0].resources.limits = limits;
        requests[0].resources.amounts = .{ .backend_kv_bytes = workspace.capacity.backend_kv_bytes + 1 };
        try Reject.expect(error.ResourceLimitExceeded, manager.acquireGenerationResources(&requests));
        requests[0].resources.amounts = .{ .backend_scratch_bytes = workspace.capacity.backend_scratch_bytes + 1 };
        try Reject.expect(error.ResourceLimitExceeded, manager.acquireGenerationResources(&requests));
        // The original 1024-byte operator cap rejects 1025 host bytes,
        // independently of the larger mandatory-metadata positive policy.
        requests[0].resources.limits = strict_host_limits;
        requests[0].resources.amounts = .{ .backend_scratch_bytes = 1, .host_weight_bytes = 1025 };
        try Reject.expect(error.ResourceLimitExceeded, manager.acquireGenerationResources(&requests));
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, workspace.borrowed.snapshot());
        requests[0].resources.limits = limits;
        requests[0].resources.amounts = .{ .backend_scratch_bytes = 1, .host_weight_bytes = 8 };
        var host = try manager.acquireGenerationResources(&requests);
        defer host.release();
        try std.testing.expectEqual(@as(usize, 8), controller.snapshot().host_weight_bytes);
        host.release();
        // Target borrowing must unwind if admission of a second session fails.
        requests[0].resources.amounts = one_byte;
        const pair = [_]GenerationAdmissionRequest{ requests[0], .{
            .session = .{ .ptr = &workspace, .vtable = undefined },
            .resources = .{ .backend_class = .gpu, .limits = limits, .amounts = .{ .backend_weight_bytes = 1 } },
        } };
        try Reject.expect(error.ResourceTemporarilyUnavailable, manager.acquireGenerationResources(&pair));
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, workspace.borrowed.snapshot());
        try std.testing.expectEqualDeep(plan.resident, controller.snapshot());
    }
}

test "CUDA A4B load plan widens explicit budget above default" {
    const requested_budget_mb: u64 = 24 * 1024;
    const config = try backend_contracts.buildCudaA4bInferenceConfig(
        .{
            .residency_mode = .resident,
            .memory_budget_mb = requested_budget_mb,
        },
        backend_contracts.qualified_a4b_geometries[0],
    );
    const encoded_artifact_bytes = 4 * 1024 * 1024 * 1024;
    const plan = a4bGpuModelLoadAdmission(config, encoded_artifact_bytes, .cuda);
    try std.testing.expectEqual(
        @as(usize, requested_budget_mb) * 1024 * 1024,
        plan.limit_floor.backend_limit_bytes,
    );
    try std.testing.expectEqual(
        plan.limit_floor.backend_limit_bytes + encoded_artifact_bytes,
        plan.limit_floor.combined_limit_bytes,
    );
}

test "CUDA A4B operator cap remains authoritative over load-plan floor" {
    const config = try backend_contracts.buildCudaA4bInferenceConfig(
        null,
        backend_contracts.qualified_a4b_geometries[0],
    );
    const plan = a4bGpuModelLoadAdmission(config, 4 * 1024 * 1024 * 1024, .cuda);
    const capped = runtime.tier.memory.applyLimitOverrides(
        plan.limit_floor,
        .{ .backend_limit_bytes = 8 * 1024 * 1024 * 1024 },
    );
    try std.testing.expectEqual(@as(usize, 8 * 1024 * 1024 * 1024), capped.backend_limit_bytes);
    try std.testing.expectEqual(
        capped.host_limit_bytes + capped.backend_limit_bytes,
        capped.combined_limit_bytes,
    );
    var run_budget = runtime.tier.memory.RunBudget.init(capped);
    try std.testing.expectError(
        error.MemoryBudgetExceeded,
        run_budget.tryReserveWeight(.backend, plan.resident.backendTotalBytes()),
    );
}

const onnxModelLoadAdmission = load_plan.onnxModelLoadAdmission;

const nativeModelLoadAdmission = load_plan.nativeModelLoadAdmission;
const metalQualifiedSpecialtyF32HostSourceBytes = load_plan.metalQualifiedSpecialtyF32HostSourceBytes;
const addMetalResidentHostSource = load_plan.addMetalResidentHostSource;

/// Serving services borrowed by the independent loader during construction.
const SessionLoadHost = struct {
    fn services(manager: *ModelManager) session_loader.Host {
        return .{
            .context = manager,
            .admission_enabled = manager.admission_enabled,
            .verification_cache = &manager.verified_artifacts,
            .preflight = manager.model_load_preflight,
            .unhealthy_fn = unhealthy,
            .limits_fn = limitsForPath,
            .reserve_fn = reserve,
            .protect_close_fn = protectClose,
            .attach_admission_fn = attachAdmission,
        };
    }

    fn unhealthy(context: *anyopaque, model_dir: []const u8, backend: backends.BackendType) bool {
        const manager: *ModelManager = @ptrCast(@alignCast(context));
        return modelBackendIsUnhealthy(manager, model_dir, backend);
    }

    fn limitsForPath(context: *anyopaque, source: ModelSource, backend_runtime: backends.BackendRuntime, floor: runtime.tier.memory.Limits) !runtime.tier.memory.Limits {
        const manager: *ModelManager = @ptrCast(@alignCast(context));
        return manager.admissionLimitsForModelSource(source, backend_runtime, floor);
    }

    fn reserve(context: *anyopaque, model_dir: []const u8, backend_runtime: backends.BackendRuntime, limits: runtime.tier.memory.Limits, plan: load_plan.ModelLoadAdmissionPlan) !session_loader.Reservation {
        const manager: *ModelManager = @ptrCast(@alignCast(context));
        // No construction or driver call begins before the full peak is leased.
        const lease = manager.acquireAmountsWithEviction(admissionBackendClassForRuntime(backend_runtime), limits, plan.peak) catch |err| {
            std.log.warn(
                "loadModel({s}) backend {s} admission failed: {s}; peak_host={d} peak_backend={d} host_limit={d} backend_limit={d} combined_limit={d}",
                .{ model_dir, @tagName(backend_runtime.backend), @errorName(err), plan.peak.host_weight_bytes +| plan.peak.host_kv_bytes +| plan.peak.host_scratch_bytes, plan.peak.backend_weight_bytes +| plan.peak.backend_kv_bytes +| plan.peak.backend_scratch_bytes, limits.host_limit_bytes, limits.backend_limit_bytes, limits.combined_limit_bytes },
            );
            return err;
        };
        return .{ .lease = lease, .resident = plan.resident, .serving_floor = plan.serving_floor };
    }

    fn protectClose(context: *anyopaque, backend_runtime: backends.BackendRuntime) !?backends.Session.CloseProtection {
        const manager: *ModelManager = @ptrCast(@alignCast(context));
        return manager.prepareSessionClose(backend_runtime);
    }

    fn attachAdmission(context: *anyopaque, session: *backends.Session, backend_runtime: backends.BackendRuntime, resident: runtime.tier.memory.AdmissionAmounts, man: ?*const manifest_mod.ModelManifest) !void {
        const manager: *ModelManager = @ptrCast(@alignCast(context));
        try attachSessionRunAdmission(manager.allocator, session, manager.admissionController(), backend_runtime, try manager.admissionLimitsForSession(backend_runtime, session.*), resident, man);
        if (session.run_admission) |*admission| admission.denial_observer = .{ .context = manager, .record = ModelManager.observeSessionAdmissionDenied };
        try manager.attachSessionResourcePin(session);
    }
};

fn loadSessionForPreferredBackends(
    manager: *ModelManager,
    preferred_backends: []const backends.BackendType,
    model_dir: []const u8,
    man: manifest_mod.ModelManifest,
    source_session_manager: *const backends.SessionManager,
    control: ?InferenceExecutionControl,
) !LoadedSessionPlan {
    return loadSessionForPreferredBackendsWithArtifacts(manager, preferred_backends, model_dir, man, source_session_manager, control, null);
}
fn writeCancellationTestModel(dir: std.Io.Dir, allocator: std.mem.Allocator) !void {
    try writeTinyDebertaEncoderGgufForModelManagerTest(dir, allocator, "model.gguf");
    try dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"deberta","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"position_buckets":16}
        ,
    });
    try dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data =
        \\{"version":"1.0","model":{"type":"BPE","vocab":{"a":0,"b":1},"merges":[]}}
        ,
    });
}

/// Record checkpoints on a successful cold load, then inject cancellation at
/// those exact checkpoints without depending on hard-coded check counts.
const LoadCancellationTrace = struct {
    checks: usize = 0,
    fail_at: ?usize = null,
    after_manifest: usize = 0,
    after_session: usize = 0,

    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.checks += 1;
        if (self.fail_at == self.checks) return error.Cancelled;
    }

    fn progress(raw: ?*anyopaque, update: execution_control_mod.Progress) void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (update.phase != .loading_model or update.total != 4) return;
        if (update.completed == 1) self.after_manifest = self.checks;
        if (update.completed == 3) self.after_session = self.checks;
    }

    fn control(self: *@This()) InferenceExecutionControl {
        return .{ .ptr = self, .check_fn = check, .progress = .{ .ptr = self, .update_fn = progress } };
    }
};

test "cold direct loads own a concurrent runtime beyond the request lifetime" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, allocator);
    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(root);
    var manager = ModelManager.initWithRuntimeEpoch(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} }, 0xfeed);
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    {
        var request_io = std.Io.Threaded.init(allocator, .{});
        defer request_io.deinit();
        var handle = try manager.loadFromDirCoordinated(root, &.{.native}, true, .{}, .{ .io = request_io.io() });
        defer handle.release();
        try std.testing.expectEqual((@as(RuntimeId, 0xfeed) << 64) | 1, handle.get().runtime_id);
        var cached = try manager.loadFromDirCoordinated(root, &.{.native}, true, .{}, .{});
        defer cached.release();
        try std.testing.expectEqual(handle.get().runtime_id, cached.get().runtime_id);
        try std.testing.expect(manager.owned_load_runtime != null);
        try std.testing.expect(manager.load_io.?.userdata != request_io.io().userdata);
    }
    // The request runtime has been destroyed. Both cached access and another
    // cold load must still work; retiring the first handle forces the latter.
    {
        var cached = try manager.loadFromDirCoordinated(root, &.{.native}, true, .{}, .{});
        cached.retire();
    }
    var reloaded = try manager.loadFromDirCoordinated(root, &.{.native}, true, .{}, .{});
    defer reloaded.release();
    try std.testing.expectEqual((@as(RuntimeId, 0xfeed) << 64) | 2, reloaded.get().runtime_id);
    // A late attachment must not move an existing Group to a different Io.
    const owned_io = manager.load_io.?;
    try manager.attachIo(std.testing.io);
    manager.lockLoadedModels();
    defer manager.unlockLoadedModels();
    try std.testing.expectEqual(owned_io.userdata, (try manager.loadCoordinationIoLocked()).userdata);
}

test "model eviction attachment returns with zero async workers and cancels on teardown" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .limited(1),
    });
    defer io_impl.deinit();
    var manager = ModelManager.init(std.testing.allocator, .{ .allocator = std.testing.allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    manager.configureModelCache(1, 0);
    try manager.attachIo(io_impl.io());
    try std.testing.expect(manager.eviction_loop_started);
    try std.testing.expectEqual(io_impl.io().userdata, manager.eviction_io.?.userdata);
    // Reattachment must not schedule a second infinite task on the one-worker lane.
    try manager.attachIo(io_impl.io());
}

test "model eviction attachment failure leaves maintenance retryable" {
    var unavailable_io = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .nothing,
    });
    defer unavailable_io.deinit();
    var available_io = std.Io.Threaded.init(std.testing.allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .limited(1),
    });
    defer available_io.deinit();
    var manager = ModelManager.init(std.testing.allocator, .{ .allocator = std.testing.allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    manager.configureModelCache(1, 0);
    try std.testing.expectError(error.ConcurrencyUnavailable, manager.attachIo(unavailable_io.io()));
    try std.testing.expect(!manager.eviction_loop_started);
    try std.testing.expect(manager.eviction_io == null);
    try manager.attachIo(available_io.io());
    try std.testing.expect(manager.eviction_loop_started);
}

test "cold load coordination reuses an attached runtime without a fallback" {
    var manager = ModelManager.init(std.testing.allocator, .{ .allocator = std.testing.allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    try manager.attachIo(std.testing.io);
    manager.lockLoadedModels();
    defer manager.unlockLoadedModels();
    try std.testing.expectEqual(std.testing.io.userdata, (try manager.loadCoordinationIoLocked()).userdata);
    try std.testing.expect(manager.owned_load_runtime == null);
}

test "ColQwen query forward observes managed backend cancellation after tokenization" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, allocator);
    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(root);
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    var handle = try manager.loadFromDirCoordinated(root, &.{.native}, true, .{}, .{});
    defer handle.release();
    var trace = LoadCancellationTrace{};
    var compute = try session_factory.getComputeBackendWithControl(handle.get().session, allocator, trace.control());
    defer compute.deinit();
    trace.fail_at = trace.checks + 1;
    // No pipeline-level control: only the managed backend can stop this
    // forward after tokenization, at its first weight operation.
    try std.testing.expectError(error.Cancelled, @import("../pipelines/multimodal_reranker_colqwen_impl.zig").encodeQuery(
        &compute.backend,
        allocator,
        handle.get().getTokenizer(),
        .{},
        .{ .hidden_size = 4, .vocab_size = 16, .num_hidden_layers = 1, .num_attention_heads = 2 },
        "a",
        16,
        false,
        null,
    ));
    try std.testing.expectEqual(trace.fail_at.?, trace.checks);
}

test "model load preflight checks resolved backends before construction and preserves fallback" {
    const a = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, a);
    const root = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer a.free(root);
    const Probe = struct {
        attempts: [2]backends.BackendType = undefined,
        count: usize = 0,
        deny_all: bool = false,
        fn check(raw: *anyopaque, _: []const u8, _: *const manifest_mod.ModelManifest, backend: backends.BackendType, _: ?backend_contracts.A4bInferenceRequest) !ModelLoadAdmission {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.attempts[self.count] = backend;
            self.count += 1;
            if (self.deny_all or backend == .metal) return error.MemoryBudgetExceeded;
            return .preflight_checked;
        }
    };
    var probe: Probe = .{};
    var manager = ModelManager.init(a, .{ .allocator = a, .preferred_backends = &.{ .cuda, .metal, .native }, .process_termination_available = false });
    defer manager.deinit();
    manager.model_load_preflight = .{ .context = &probe, .check = Probe.check };
    var manifest = try manifest_mod.loadFromDir(a, root);
    defer manifest.deinit();
    // Reject Metal before driver/session construction, then load the same real
    // fixture on the permitted native candidate. No accelerator is required.
    var loaded = try loadSessionForPreferredBackends(&manager, manager.session_manager.preferred_backends, root, manifest, &manager.session_manager, null);
    loaded.deinit();
    const expected: []const backends.BackendType = if (build_options.enable_metal) &.{ .metal, .native } else &.{.native};
    try std.testing.expectEqualSlices(backends.BackendType, expected, probe.attempts[0..probe.count]);
    probe.count = 0;
    probe.deny_all = true;
    try std.testing.expectError(error.MemoryBudgetExceeded, loadSessionForPreferredBackends(&manager, manager.session_manager.preferred_backends, root, manifest, &manager.session_manager, null));
    try std.testing.expectEqualSlices(backends.BackendType, expected, probe.attempts[0..probe.count]);
}

fn sharedResourceTokenizerLifetimeScenario(a: std.mem.Allocator) !void {
    const limits = runtime.tier.memory.Limits{ .host_limit_bytes = 16 * 1024 * 1024 };
    var resources = try ModelManager.SharedResources.init(a, limits);
    var resources_owned = true;
    defer if (resources_owned) resources.deinit();
    var budget: hf_tokenizer.HfTokenizer.BpeCacheResourceBudget = undefined;
    {
        var manager = ModelManager.init(a, backends.SessionManager.init(a));
        defer manager.deinit();
        try (ModelLoadPolicy{ .limits = limits, .resources = &resources }).configure(&manager);
        budget = manager.tokenizer_cache_config.resource_budget.?;
        try std.testing.expect(budget.retainContext());
    }
    defer budget.releaseContext();
    // A cache can escape every manager. Its retained domain must continue to
    // account growth, and refunds must still work after the owner closes.
    if (!budget.observe(budget.context, 123, 0, 4096)) return error.OutOfMemory;
    defer {
        const refunded = budget.observe(budget.context, 123, 4096, 0);
        std.debug.assert(refunded);
    }
    resources.deinit();
    resources_owned = false;
    try std.testing.expect(!budget.observe(budget.context, 123, 4096, 8192));
}

test "shared model load admission owns escaped tokenizer caches across every allocation failure" {
    try sharedResourceTokenizerLifetimeScenario(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, sharedResourceTokenizerLifetimeScenario, .{});
}

test "shared model load admission denies aggregate residency before a second constructor and refunds" {
    const a = std.testing.allocator;
    const memory = runtime.tier.memory;
    const mib: usize = 1024 * 1024;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, a);
    // Pad the valid tiny GGUF to exercise real canonical residency plans
    // without allocating a large model or substituting a fake load plan.
    const original = try dir.dir.readFileAlloc(std.testing.io, "model.gguf", a, .limited(mib));
    defer a.free(original);
    const padded = try a.alloc(u8, 2 * mib);
    defer a.free(padded);
    @memset(padded, 0);
    @memcpy(padded[0..original.len], original);
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = padded });
    const root = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer a.free(root);
    var manifest = try manifest_mod.loadFromDir(a, root);
    defer manifest.deinit();
    const Probe = struct {
        fn check(_: *anyopaque, _: []const u8, _: *const manifest_mod.ModelManifest, _: backends.BackendType, _: ?backend_contracts.A4bInferenceRequest) !ModelLoadAdmission {
            return .managed;
        }
    };
    var context: u8 = 0;
    const limits = memory.Limits{ .host_limit_bytes = 3 * mib, .combined_limit_bytes = 3 * mib };
    var resources = try ModelManager.SharedResources.init(a, limits);
    defer resources.deinit();
    const policy = ModelLoadPolicy{ .limits = limits, .resources = &resources, .preflight = .{ .context = &context, .check = Probe.check } };
    var draft = ModelManager.init(a, .{ .allocator = a, .preferred_backends = &.{.native} });
    defer draft.deinit();
    try policy.configure(&draft);
    {
        var target = ModelManager.init(a, .{ .allocator = a, .preferred_backends = &.{.native} });
        defer target.deinit();
        try policy.configure(&target);
        var loaded = try loadSessionForPreferredBackends(&target, &.{.native}, root, manifest, &target.session_manager, null);
        defer loaded.deinit();
        try std.testing.expect(loaded.resource_lease != null);
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, loadSessionForPreferredBackends(&draft, &.{.native}, root, manifest, &draft.session_manager, null));
        try std.testing.expectEqual(2 * mib, draft.currentAdmissionAmounts().host_weight_bytes);
        loaded.deinit();
        try std.testing.expectEqual(memory.AdmissionAmounts{}, draft.currentAdmissionAmounts());
    }
    var retry = try loadSessionForPreferredBackends(&draft, &.{.native}, root, manifest, &draft.session_manager, null);
    defer retry.deinit();
    try std.testing.expectEqual(2 * mib, draft.currentAdmissionAmounts().host_weight_bytes);
    retry.deinit();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, draft.currentAdmissionAmounts());
}

test "managed load preflight enforces caps and retains residency without a serving policy" {
    const a = std.testing.allocator;
    const memory = runtime.tier.memory;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, a);
    const root = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer a.free(root);
    const Probe = struct {
        fn check(_: *anyopaque, _: []const u8, _: *const manifest_mod.ModelManifest, _: backends.BackendType, _: ?backend_contracts.A4bInferenceRequest) !ModelLoadAdmission {
            return .managed;
        }
    };
    var context: u8 = 0;
    var manager = ModelManager.init(a, .{ .allocator = a, .preferred_backends = &.{.native} });
    defer manager.deinit();
    manager.model_load_preflight = .{ .context = &context, .check = Probe.check };
    try std.testing.expect(!manager.admission_enabled);
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 1 });
    try manager.ensureResourceOwnerReady();
    var manifest = try manifest_mod.loadFromDir(a, root);
    defer manifest.deinit();
    // Exercise the actual constructor gate with a real CPU model, so this
    // regression runs even on machines without an A4B accelerator or weights.
    try std.testing.expectError(error.ResourceLimitExceeded, loadSessionForPreferredBackends(&manager, &.{.native}, root, manifest, &manager.session_manager, null));
    try std.testing.expectEqual(memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());

    manager.configureAdmissionLimits(.{ .host_limit_bytes = 128 * 1024 * 1024 });
    var loaded = try loadSessionForPreferredBackends(&manager, &.{.native}, root, manifest, &manager.session_manager, null);
    defer loaded.deinit();
    try std.testing.expect(!manager.admission_enabled);
    try std.testing.expect(loaded.resource_lease != null);
    try std.testing.expect(loaded.session.run_admission != null);
    try std.testing.expect(manager.currentAdmissionAmounts().host_weight_bytes > 0);
    loaded.deinit();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
}

test "cold load rollback owns manifest and constructed session before cancellation checkpoints" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, allocator);
    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(root);
    var checkpoints: [3]usize = undefined;
    for (0..4) |iteration| {
        var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
        defer manager.deinit();
        manager.configureServingPolicy(.{ .allow_unknown = true });
        manager.configureAdmissionLimits(.{ .host_limit_bytes = 128 * 1024 * 1024 });
        try manager.ensureResourceOwnerReady();
        var trace = LoadCancellationTrace{ .fail_at = if (iteration == 0) null else checkpoints[iteration - 1] };
        const result = manager.loadFromDirUncached(root, &manager.session_manager, true, null, 0, trace.control());
        if (iteration == 0) {
            var handle = try result;
            defer handle.release();
            try std.testing.expect(manager.admissionController().snapshot().host_weight_bytes > 0);
            // The final checkpoint is after ownership has moved into the
            // unpublished model, including any speculative preparation.
            checkpoints = .{ trace.after_manifest, trace.after_session, trace.checks };
            try std.testing.expect(checkpoints[0] > 0 and checkpoints[1] > checkpoints[0]);
            try std.testing.expect(checkpoints[2] > checkpoints[1]);
            var compute = try session_factory.getComputeBackendWithControl(handle.get().session, allocator, trace.control());
            defer compute.deinit();
            trace.fail_at = trace.checks + 1;
            try std.testing.expectError(error.Cancelled, compute.backend.checkExecutionControl());
        } else {
            try std.testing.expectError(error.Cancelled, result);
            try std.testing.expectEqual(@as(usize, 0), manager.loaded.count());
            try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
        }
    }
}

test "load flight waiter exits release retirement reservations and admission" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, allocator);
    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(root);

    const Exit = enum { cancel_during_wait, cancel_after_completion, adopt };
    const Cancellation = struct {
        manager: *ModelManager,
        flight: *LoadFlight,
        model: *LoadedModel,
        publish_before_cancel: bool,

        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.publish_before_cancel) {
                // Deterministically interleave completion and retirement after
                // the waiter observed an incomplete flight, before it exits.
                self.manager.finishLoadFlight(self.flight, self.model, null);
                self.manager.retireLoadedModel(self.model);
            }
            return error.Cancelled;
        }
    };

    for ([_]bool{ false, true }) |task_ref_pending| {
        for ([_]Exit{ .cancel_during_wait, .cancel_after_completion, .adopt }) |exit| {
            var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
            defer manager.deinit();
            manager.configureServingPolicy(.{ .allow_unknown = true });
            manager.configureAdmissionLimits(.{ .host_limit_bytes = 128 * 1024 * 1024 });
            try manager.ensureResourceOwnerReady();
            var keeper = try manager.loadFromDirUncached(root, &manager.session_manager, true, null, 0, null);
            defer keeper.release();
            const model = keeper.get();
            try std.testing.expect(manager.admissionController().snapshot().host_weight_bytes > 0);

            const flight = try allocator.create(LoadFlight);
            flight.* = .{
                .io = std.testing.io,
                .refs = 1 + @as(usize, @intFromBool(task_ref_pending)),
                .task_ref_pending = task_ref_pending,
            };
            try manager.in_flight_loads.put(allocator, try allocator.dupe(u8, "flight"), flight);
            // The task can drop its reference either before or after the
            // waiter. Only waiter references own retirement reservations.
            defer if (task_ref_pending) manager.releaseLoadFlight("flight", flight);
            var cancellation = Cancellation{
                .manager = &manager,
                .flight = flight,
                .model = model,
                .publish_before_cancel = exit == .cancel_during_wait,
            };
            if (exit != .cancel_during_wait) {
                manager.finishLoadFlight(flight, model, null);
                manager.retireLoadedModel(model);
            }
            const result = manager.waitForLoadFlight("flight", flight, if (exit == .adopt) null else .{
                .ptr = &cancellation,
                .check_fn = Cancellation.check,
            });
            if (exit == .adopt) {
                var adopted = try result;
                try std.testing.expectEqual(@as(usize, 2), model.active_handles);
                adopted.release();
            } else {
                try std.testing.expectError(error.Cancelled, result);
            }
            try std.testing.expectEqual(@as(usize, 0), manager.in_flight_loads.count());
            try std.testing.expectEqual(@as(usize, 0), manager.loaded.count());
            try std.testing.expectEqual(@as(usize, 1), model.active_handles);
            keeper.release();
            // This proves that cancellation releases the native model and its
            // lease, not merely that it removes the flight from the registry.
            try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
        }
    }
}

test "optional session adoption owns rollback and transfers admission exactly once" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, allocator);
    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(root);
    var final_check: usize = 0;
    for (0..2) |iteration| {
        var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
        defer manager.deinit();
        manager.configureServingPolicy(.{ .allow_unknown = true });
        manager.configureAdmissionLimits(.{ .host_limit_bytes = 128 * 1024 * 1024 });
        try manager.ensureResourceOwnerReady();
        var primary = try manager.loadManagedSessionWithAdmission(root, &.{.native}, null, null, .{ .path = root, .publication = try managed_receipt.publicationGeneration(allocator, std.testing.io, root) }, null);
        defer primary.deinit();
        const baseline = manager.admissionController().snapshot();
        var model: LoadedModel = undefined;
        model.managed_publication = null;
        model.model_dir = root;
        model.allocator = allocator;
        model.session = primary.session;
        model.session_manager = &manager.session_manager;
        model.host = manager.loadedRuntimeHost();
        model.kernel_jit_profile_bundle = null;
        var slot: ?backends.Session = null;
        var lease_slot: ?runtime.tier.memory.AdmissionLease = null;
        defer if (lease_slot) |*lease| lease.release();
        defer if (slot) |session| session.close();
        var trace = LoadCancellationTrace{ .fail_at = if (iteration == 0) null else final_check };
        const result = model.ensureOptionalSessionWithControl(.text_projection, &slot, &lease_slot, root, trace.control());
        if (iteration == 0) {
            try std.testing.expect(try result);
            final_check = trace.checks;
            try std.testing.expect(slot != null and lease_slot != null);
            try std.testing.expect(manager.admissionController().snapshot().host_weight_bytes > baseline.host_weight_bytes);
            // Already-adopted components must not load or reserve again.
            try std.testing.expect(!try model.ensureOptionalSessionWithControl(.text_projection, &slot, &lease_slot, root, .{}));
        } else {
            try std.testing.expectError(error.Cancelled, result);
            try std.testing.expect(slot == null and lease_slot == null);
            try std.testing.expectEqual(baseline, manager.admissionController().snapshot());
        }
    }
}

test "component construction arms cancellation before backend entry and releases admission on failure" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, allocator);
    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(root);
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.metal} });
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 128 * 1024 * 1024, .backend_limit_bytes = 128 * 1024 * 1024 });
    try manager.ensureResourceOwnerReady();
    const Boundary = struct {
        manager: *ModelManager,
        arms: usize = 0,
        fn arm(raw: *anyopaque, _: execution_control_mod.MonitorControl) !u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.manager.admissionController().snapshot().host_weight_bytes > 0);
            self.arms += 1;
            return error.OutOfMemory;
        }
        fn disarm(_: *anyopaque, _: u64) void {
            @panic("failed constructor guard cannot be disarmed");
        }
    };
    var boundary = Boundary{ .manager = &manager };
    for (0..3) |_| {
        try std.testing.expectError(error.ProcessIsolationRequired, manager.loadManagedSessionWithAdmission(root, &.{.metal}, null, .{}, .{ .path = root, .publication = try managed_receipt.publicationGeneration(allocator, std.testing.io, root) }, null));
        try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
        try std.testing.expectError(error.OutOfMemory, manager.loadManagedSessionWithAdmission(root, &.{.metal}, null, .{
            .hard_cancellation = .{ .ptr = &boundary, .arm_fn = Boundary.arm, .disarm_fn = Boundary.disarm },
        }, .{ .path = root, .publication = try managed_receipt.publicationGeneration(allocator, std.testing.io, root) }, null));
        try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
    }
    try std.testing.expectEqual(@as(usize, 3), boundary.arms);
}

test "managed constructors retain cooperative fallback without a process boundary" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, allocator);
    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(root);
    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{ .metal, .native },
        .process_termination_available = false,
    });
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 128 * 1024 * 1024, .backend_limit_bytes = 128 * 1024 * 1024 });
    try manager.ensureResourceOwnerReady();
    var man = try manifest_mod.loadFromDir(allocator, root);
    defer man.deinit();
    for ([_]bool{ false, true }) |component| {
        var loaded = if (component)
            try manager.loadManagedSessionWithAdmission(root, &.{ .metal, .native }, null, .{}, .{ .path = root, .publication = try managed_receipt.publicationGeneration(allocator, std.testing.io, root) }, null)
        else
            try loadSessionForPreferredBackends(&manager, &.{ .metal, .native }, root, man, &manager.session_manager, .{});
        try std.testing.expectEqual(backends.BackendType.native, loaded.session.backend());
        loaded.deinit();
        try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
    }
}

test "preferred load attempt releases admission on cancellation and guard failure" {
    // This exercises Metal's process-required construction contract, which
    // cannot be entered when Metal is excluded from the build.
    if (!backends.BackendType.metal.available()) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    // The manifest explicitly selects ONNX even though the physical filename
    // has no format suffix. Guards must fail before backend construction.
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    try graph.markOutput(try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4})));
    const bytes = try onnx_graph.exportGraph(allocator, &graph, .{});
    defer allocator.free(bytes);
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "weights.bin", .data = bytes });
    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(root);
    const path = try std.fs.path.join(allocator, &.{ root, "weights.bin" });
    defer allocator.free(path);
    const man = manifest_mod.ModelManifest{ .allocator = allocator, .onnx_path = path };
    var sessions = backends.SessionManager.init(allocator);
    sessions.required_backend = null;
    sessions.required_backend_invalid = false;
    var manager = ModelManager.init(allocator, sessions);
    defer manager.deinit();
    manager.configureServingPolicy(.{});
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 1024, .backend_limit_bytes = 1024 });
    try manager.ensureResourceOwnerReady();

    const Hooks = struct {
        fn cancelAfterAdmission(raw: ?*anyopaque) !void {
            const owner: *ModelManager = @ptrCast(@alignCast(raw.?));
            if (owner.admissionController().snapshot().host_weight_bytes > 0) return error.Timeout;
        }
        fn failArm(raw: *anyopaque, _: execution_control_mod.MonitorControl) !u64 {
            const owner: *ModelManager = @ptrCast(@alignCast(raw));
            try std.testing.expect(owner.admissionController().snapshot().host_weight_bytes > 0);
            return error.OutOfMemory;
        }
        fn disarm(_: *anyopaque, _: u64) void {
            @panic("failed arm must not be disarmed");
        }
    };
    const controls = [_]InferenceExecutionControl{
        .{},
        .{ .native_call_policy = .in_process, .ptr = &manager, .check_fn = Hooks.cancelAfterAdmission },
        .{ .hard_cancellation = .{ .ptr = &manager, .arm_fn = Hooks.failArm, .disarm_fn = Hooks.disarm } },
    };
    const errors = [_]anyerror{ error.ProcessIsolationRequired, error.Timeout, error.OutOfMemory };
    // Use Metal's process-required construction contract for this test.
    for (0..3) |_| for (controls, errors) |control, expected| {
        try std.testing.expectError(expected, loadSessionForPreferredBackends(
            &manager,
            &.{.metal},
            root,
            man,
            &sessions,
            control,
        ));
        try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
    };
}

test "preferred load cancellation after construction releases retained admission" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, allocator);
    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(root);
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 128 * 1024 * 1024 });
    try manager.ensureResourceOwnerReady();
    var man = try manifest_mod.loadFromDir(allocator, root);
    defer man.deinit();
    const State = struct {
        manager: *ModelManager,
        observed: bool = false,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.manager.admissionController().snapshot().host_weight_bytes != 0) {
                self.observed = true;
                return error.Timeout;
            }
        }
    };
    for (0..3) |_| {
        var state = State{ .manager = &manager };
        try std.testing.expectError(error.Timeout, loadSessionForPreferredBackends(&manager, &.{.native}, root, man, &manager.session_manager, .{ .ptr = &state, .check_fn = State.check }));
        try std.testing.expect(state.observed);
        try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
    }
}

const rememberPreferredLoadError = session_loader.rememberPreferredLoadError;

test "external resource ownership fails closed until budget is attached" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();

    try manager.configureResourceOwnership(.external_required);
    try std.testing.expectError(
        error.ExternalResourceManagerNotConfigured,
        manager.ensureResourceOwnerReady(),
    );

    const Bridge = struct {
        fn retainContext(_: *anyopaque) bool {
            return true;
        }
        fn releaseContext(_: *anyopaque) void {}
        fn reserve(_: *anyopaque, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!void {}
        fn release(_: *anyopaque, _: usize) void {}
    };
    const CacheBridge = struct {
        fn observe(_: *anyopaque, _: usize, _: usize, _: usize) bool {
            return true;
        }
    };
    var context: u8 = 0;
    try std.testing.expectError(
        error.ExternalBudgetsMustBeConfiguredTogether,
        manager.configureTokenizerCaches(.{
            .resource_budget = .{
                .context = &context,
                .observe = CacheBridge.observe,
            },
        }),
    );
    try manager.configureExternalResourceBudgets(
        .{
            .context = &context,
            .retain_context = Bridge.retainContext,
            .release_context = Bridge.releaseContext,
            .try_reserve = Bridge.reserve,
            .retain = Bridge.retain,
            .release = Bridge.release,
        },
        .{
            .context = &context,
            .retain_context = Bridge.retainContext,
            .release_context = Bridge.releaseContext,
            .observe = CacheBridge.observe,
        },
    );
    try manager.ensureResourceOwnerReady();
}

test "local resource ownership rejects external tokenizer provenance" {
    const CacheBridge = struct {
        fn observe(_: *anyopaque, _: usize, _: usize, _: usize) bool {
            return true;
        }
    };
    var manager = ModelManager.init(
        std.testing.allocator,
        backends.SessionManager.init(std.testing.allocator),
    );
    defer manager.deinit();
    var context: u8 = 0;

    try std.testing.expectError(
        error.ExternalTokenizerBudgetInLocalOwnership,
        manager.configureTokenizerCaches(.{
            .resource_budget = .{
                .context = &context,
                .observe = CacheBridge.observe,
            },
        }),
    );
    try manager.ensureResourceOwnerReady();
    try std.testing.expectError(
        error.ResourceOwnershipAfterTokenizerBudgetConfiguration,
        manager.configureResourceOwnership(.external_required),
    );
}

test "local resource ownership amortizes exact tokenizer cache usage" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();

    manager.configureServingPolicy(.{});
    const quantum = ModelManager.tokenizer_cache_admission_quantum_bytes;
    manager.configureAdmissionLimits(.{
        .host_limit_bytes = quantum + 20,
        .combined_limit_bytes = quantum + 20,
    });
    try manager.ensureResourceOwnerReady();
    const budget = manager.tokenizer_cache_config.resource_budget.?;

    try std.testing.expect(budget.observe(budget.context, 71, 0, 16));
    try std.testing.expectEqual(
        quantum,
        manager.admissionController().snapshot().host_weight_bytes,
    );
    // Growth inside the admitted quantum performs no additional admission or
    // live-memory probe, but the observer retains the exact current value.
    try std.testing.expect(budget.observe(budget.context, 71, 16, quantum - 1));
    try std.testing.expectEqual(
        quantum,
        manager.admissionController().snapshot().host_weight_bytes,
    );
    try std.testing.expect(!budget.observe(budget.context, 71, 0, 0));
    // The preferred second quantum exceeds policy, so admission retries only
    // the required delta instead of denying useful cache capacity.
    try std.testing.expect(budget.observe(
        budget.context,
        71,
        quantum - 1,
        quantum + 16,
    ));
    try std.testing.expectEqual(
        quantum + 16,
        manager.admissionController().snapshot().host_weight_bytes,
    );
    try std.testing.expect(budget.observe(
        budget.context,
        71,
        quantum + 16,
        quantum + 15,
    ));
    try std.testing.expectEqual(
        quantum + 16,
        manager.admissionController().snapshot().host_weight_bytes,
    );
    try std.testing.expect(!budget.observe(
        budget.context,
        71,
        quantum + 15,
        quantum + 21,
    ));
    try std.testing.expect(budget.observe(budget.context, 71, quantum + 15, 7));
    try std.testing.expectEqual(quantum, manager.admissionController().snapshot().host_weight_bytes);
    try std.testing.expect(budget.observe(budget.context, 71, 7, 0));
    try std.testing.expectEqual(
        runtime.tier.memory.AdmissionAmounts{},
        manager.admissionController().snapshot(),
    );
}

test "local tokenizer lifecycle releases its persistent admission credit" {
    const allocator = std.testing.allocator;
    const json_str =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1, "b": 2, "c": 3, "ab": 4, "abc": 5},
        \\    "merges": ["a b", "ab c"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    defer manager.deinit();
    manager.configureServingPolicy(.{});
    manager.configureAdmissionLimits(.{
        .host_limit_bytes = 4 * ModelManager.tokenizer_cache_admission_quantum_bytes,
        .combined_limit_bytes = 4 * ModelManager.tokenizer_cache_admission_quantum_bytes,
    });
    try manager.ensureResourceOwnerReady();

    var tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, json_str);
    errdefer tokenizer.deinitSelf();
    try tokenizer.configureBpeCache(manager.tokenizer_cache_config);
    for (0..4) |_| {
        const ids = try tokenizer.tokenizer().encode(allocator, "abc");
        allocator.free(ids);
    }
    const actual_cache_bytes = tokenizer.bpeCacheStats().used_bytes;
    try std.testing.expectEqual(
        ModelManager.tokenizerCacheCreditTarget(actual_cache_bytes),
        manager.admissionController().snapshot().host_weight_bytes,
    );

    tokenizer.deinitSelf();
    try std.testing.expectEqual(
        runtime.tier.memory.AdmissionAmounts{},
        manager.admissionController().snapshot(),
    );
    for (&manager.resource_domain.?.tokenizer_cache_budget_shards) |*shard|
        try std.testing.expectEqual(@as(usize, 0), shard.records.count());
}

test "managed tokenizer safely outlives ModelManager shutdown" {
    const allocator = std.testing.allocator;
    const tokenizer_json =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1, "b": 2, "ab": 3},
        \\    "merges": ["a b"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data = tokenizer_json,
    });
    const tokenizer_path = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "tokenizer.json" },
    );
    defer allocator.free(tokenizer_path);

    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    manager.configureServingPolicy(.{});
    try manager.ensureResourceOwnerReady();
    var managed = try manager.loadManagedHfTokenizerFile(tokenizer_path);
    try std.testing.expect(managed.managed_lifetime != null);
    try std.testing.expect(manager.admissionController().snapshot().host_weight_bytes > 0);

    const domain = manager.resource_domain.?;
    try std.testing.expect(domain.retain());
    defer domain.release();

    // Manager shutdown closes new growth but keeps physical tokenizer memory
    // charged in the independently owned resource domain.
    manager.deinit();
    try std.testing.expect(domain.admission.snapshot().host_weight_bytes > 0);
    managed.deinit();
    try std.testing.expectEqual(
        runtime.tier.memory.AdmissionAmounts{},
        domain.admission.snapshot(),
    );
}

test "raw tokenizer budget capability pins resource domain through teardown" {
    const allocator = std.testing.allocator;
    const tokenizer_json =
        \\{
        \\  "model": {"type": "BPE", "vocab": {"a": 1}, "merges": []},
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    var manager = ModelManager.init(allocator, backends.SessionManager.init(allocator));
    manager.configureServingPolicy(.{});
    try manager.ensureResourceOwnerReady();

    var tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
    try tokenizer.configureBpeCache(manager.tokenizer_cache_config);
    const domain = manager.resource_domain.?;
    try std.testing.expect(domain.retain());
    defer domain.release();
    try std.testing.expect(domain.admission.snapshot().host_weight_bytes > 0);

    manager.deinit();
    try std.testing.expect(domain.admission.snapshot().host_weight_bytes > 0);
    tokenizer.deinitSelf();
    try std.testing.expectEqual(
        runtime.tier.memory.AdmissionAmounts{},
        domain.admission.snapshot(),
    );
}

test "external ownership pairing preserves cache policy and drains observers" {
    const AdmissionBridge = struct {
        fn retainContext(_: *anyopaque) bool {
            return true;
        }
        fn releaseContext(_: *anyopaque) void {}
        fn reserve(_: *anyopaque, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!void {}
        fn release(_: *anyopaque, _: usize) void {}
    };
    const CacheBridge = struct {
        current: usize = 0,
        transitions: usize = 0,
        alive: bool = true,
        callbacks_after_shutdown: usize = 0,

        fn observe(
            context: *anyopaque,
            _: usize,
            previous: usize,
            next: usize,
        ) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (!self.alive) {
                self.callbacks_after_shutdown += 1;
                return false;
            }
            if (self.current != previous) return false;
            self.current = next;
            self.transitions += 1;
            return true;
        }
    };

    var admission_context: u8 = 0;
    var cache_context = CacheBridge{};
    var manager = ModelManager.init(
        std.testing.allocator,
        backends.SessionManager.init(std.testing.allocator),
    );
    var manager_closed = false;
    defer if (!manager_closed) manager.deinit();
    try manager.configureResourceOwnership(.external_required);
    try manager.configureTokenizerCaches(.{
        .max_bytes = 7 * 1024 * 1024,
        .bulk_slots_per_shard = 1024,
    });
    try manager.configureExternalResourceBudgets(
        .{
            .context = &admission_context,
            .retain_context = AdmissionBridge.retainContext,
            .release_context = AdmissionBridge.releaseContext,
            .try_reserve = AdmissionBridge.reserve,
            .retain = AdmissionBridge.retain,
            .release = AdmissionBridge.release,
        },
        .{
            .context = &cache_context,
            .retain_context = AdmissionBridge.retainContext,
            .release_context = AdmissionBridge.releaseContext,
            .observe = CacheBridge.observe,
        },
    );
    try manager.ensureResourceOwnerReady();
    try std.testing.expectEqual(
        @as(usize, 7 * 1024 * 1024),
        manager.tokenizer_cache_config.max_bytes,
    );
    try std.testing.expectEqual(
        @as(usize, 1024),
        manager.tokenizer_cache_config.bulk_slots_per_shard,
    );

    const tokenizer_json =
        \\{
        \\  "model": {
        \\    "type": "BPE",
        \\    "vocab": {"a": 1, "b": 2, "ab": 3},
        \\    "merges": ["a b"]
        \\  },
        \\  "pre_tokenizer": {"type": "ByteLevel", "add_prefix_space": false}
        \\}
    ;
    const tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(
        std.testing.allocator,
        tokenizer_json,
    );
    var tokenizer_owned = true;
    errdefer if (tokenizer_owned) tokenizer.deinitSelf();
    try tokenizer.configureBpeCache(manager.tokenizer_cache_config);
    const managed_lifetime = try manager.resource_domain.?
        .registerManagedTokenizer(null);
    var managed = ManagedHfTokenizer{
        .tokenizer = tokenizer,
        .managed_lifetime = managed_lifetime.capability(),
    };
    tokenizer_owned = false;
    defer managed.deinit();
    try std.testing.expect(cache_context.current > 0);

    manager.deinit();
    manager_closed = true;
    try std.testing.expect(cache_context.current > 0);

    // The external owner stays retained until physical tokenizer destruction,
    // which performs the exact final observer transition.
    managed.deinit();
    try std.testing.expectEqual(@as(usize, 0), cache_context.current);
    try std.testing.expect(cache_context.transitions >= 2);
    cache_context.alive = false;
    try std.testing.expectEqual(@as(usize, 0), cache_context.callbacks_after_shutdown);
}

test "model loading preserves retryable admission errors across backend probes" {
    var selected: ?anyerror = null;
    rememberPreferredLoadError(&selected, error.NoBackendAvailable);
    rememberPreferredLoadError(&selected, error.MissingWeight);
    rememberPreferredLoadError(&selected, error.ResourceTemporarilyUnavailable);
    rememberPreferredLoadError(&selected, error.ResourceLimitExceeded);
    try std.testing.expectEqual(
        @as(?anyerror, error.ResourceTemporarilyUnavailable),
        selected,
    );
}

test "model loading prefers actionable import errors over backend probe misses" {
    var selected: ?anyerror = error.UnsupportedBackend;
    rememberPreferredLoadError(&selected, error.ShapeMismatch);
    rememberPreferredLoadError(&selected, error.NoBackendAvailable);
    try std.testing.expectEqual(@as(?anyerror, error.ShapeMismatch), selected);
}

test "serving admission applies the node-wide host-memory override" {
    var manager = ModelManager.init(
        std.testing.allocator,
        backends.SessionManager.init(std.testing.allocator),
    );
    defer manager.deinit();

    manager.configureServingPolicy(.{});
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 100 });
    try manager.ensureResourceOwnerReady();
    try std.testing.expectEqual(
        @as(usize, 100),
        manager.admissionController().shared_limits.host_limit_bytes,
    );
    try std.testing.expectEqual(
        @as(usize, 100),
        manager.resource_domain.?.admission_limits.host_limit_bytes,
    );
    try std.testing.expectEqual(
        @as(usize, 100),
        manager.resource_domain.?.admission_limits.combined_limit_bytes,
    );
}

test "optional sessions retain the selected backend before fallbacks" {
    const preferred = [_]backends.BackendType{ .metal, .native };
    const owned = try ownSelectedFirstBackendPreference(
        std.testing.allocator,
        &preferred,
        .native,
    );
    defer std.testing.allocator.free(owned);
    try std.testing.expectEqualSlices(backends.BackendType, &.{ .native, .metal }, owned);

    const added = try ownSelectedFirstBackendPreference(
        std.testing.allocator,
        &preferred,
        .cuda,
    );
    defer std.testing.allocator.free(added);
    try std.testing.expectEqualSlices(backends.BackendType, &.{ .cuda, .metal, .native }, added);
}

test "required preload completeness includes every declared optional session" {
    const manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .visual_model_path = "vision.gguf",
        .audio_model_path = "audio.gguf",
        .text_projection_path = "text-projection.onnx",
        .visual_projection_path = "visual-projection.onnx",
        .audio_projection_path = "audio-projection.onnx",
    };
    const declared = declaredOptionalSessions(&manifest);
    try std.testing.expectEqual(@as(usize, 5), declared.len);
    try std.testing.expect(!declaredOptionalSessionsComplete(
        &manifest,
        .{ true, true, true, true, false },
    ));
    try std.testing.expect(declaredOptionalSessionsComplete(
        &manifest,
        .{ true, true, true, true, true },
    ));

    const text_only = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    try std.testing.expect(declaredOptionalSessionsComplete(
        &text_only,
        .{ false, false, false, false, false },
    ));
}

test "optional sessions bind only their component-qualified profile" {
    var manager = backends.SessionManager.init(std.testing.allocator);
    manager.kernel_jit = .{ .mode = .on, .qualified_profile_path = "primary.json" };
    const bundle: graph_mod.kernel_jit.QualifiedProfileBundle = .{
        .primary = "primary.json",
        .vision = "vision.json",
        .audio = "audio.json",
    };

    try bindOptionalSessionProfile(&manager, .audio, bundle);
    try std.testing.expectEqualStrings("audio.json", manager.kernel_jit.qualified_profile_path.?);
    try bindOptionalSessionProfile(&manager, .text_projection, bundle);
    try std.testing.expect(manager.kernel_jit.qualified_profile_path == null);
    try std.testing.expectEqual(graph_mod.kernel_jit.Mode.off, manager.kernel_jit.mode);

    manager.kernel_jit = .{ .mode = .required, .qualified_profile_path = "primary.json" };
    try std.testing.expectError(
        error.MissingKernelJitProfileBundleComponent,
        bindOptionalSessionProfile(&manager, .text_projection, bundle),
    );

    manager.kernel_jit = .{ .mode = .on, .qualified_profile_path = "primary.json" };
    try bindOptionalSessionProfile(&manager, .vision, null);
    try std.testing.expect(manager.kernel_jit.qualified_profile_path == null);
    try std.testing.expectEqual(graph_mod.kernel_jit.Mode.off, manager.kernel_jit.mode);
}

test "isManifestPotentiallyLoadableInCurrentBuild rejects incomplete GLiNER bundle" {
    const allocator = std.testing.allocator;
    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    man.gliner_model_type = try allocator.dupe(u8, "gliner2");
    man.gguf_path = try allocator.dupe(u8, "encoder.gguf");
    try std.testing.expect(!isManifestPotentiallyLoadableInCurrentBuild(man));
}

test "required backend is applied before model manager backend planning" {
    const allocator = std.testing.allocator;
    var source = backends.SessionManager.init(allocator);
    source.preferred_backends = &.{ .native, .metal };
    source.required_backend = .cuda;
    source.required_backend_invalid = false;

    var required_scratch: [1]backends.BackendType = undefined;
    const policy_backends = try source.requiredBackendCandidates(
        source.preferred_backends,
        &required_scratch,
    );

    try std.testing.expectEqualSlices(backends.BackendType, &.{.cuda}, policy_backends);
}

test "model manager rejects an unavailable required backend before artifact selection" {
    const allocator = std.testing.allocator;
    var session_manager = backends.SessionManager.init(allocator);
    session_manager.required_backend = .pjrt;
    session_manager.required_backend_invalid = false;
    var manager = ModelManager.init(allocator, session_manager);
    defer manager.deinit();

    try std.testing.expectError(
        error.RequiredBackendUnavailable,
        manager.acquireFromDir("/nonexistent/required-backend-model"),
    );
    try std.testing.expectError(
        error.RequiredBackendUnavailable,
        manager.componentLoaderForPaths(
            "/nonexistent/required-backend-model",
            &.{.native},
            &.{"/nonexistent/component"},
        ),
    );
}

test "isManifestPotentiallyLoadableInCurrentBuild accepts onnx-only models when onnx model support is enabled" {
    const allocator = std.testing.allocator;

    var onnx_only = manifest_mod.ModelManifest{ .allocator = allocator };
    defer onnx_only.deinit();
    onnx_only.onnx_path = try allocator.dupe(u8, "model.onnx");

    try std.testing.expect(isManifestPotentiallyLoadableInCurrentBuild(onnx_only));

    var native_model = manifest_mod.ModelManifest{ .allocator = allocator };
    defer native_model.deinit();
    native_model.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    try std.testing.expect(isManifestPotentiallyLoadableInCurrentBuild(native_model));
}

test "hybrid artifact candidates are isolated by backend" {
    const allocator = std.testing.allocator;
    var hybrid = manifest_mod.ModelManifest{ .allocator = allocator };
    defer hybrid.deinit();
    hybrid.gguf_path = try allocator.dupe(u8, "optional.gguf");
    hybrid.safetensors_path = try allocator.dupe(u8, "model.safetensors");
    hybrid.onnx_path = try allocator.dupe(u8, "model.onnx");

    try std.testing.expectEqual(
        ArtifactCandidateKind.safetensors,
        artifactCandidateForBackend(hybrid, .native).?,
    );
    try std.testing.expectEqual(
        ArtifactCandidateKind.safetensors,
        artifactCandidateForBackend(hybrid, .metal).?,
    );
}

test "native compatibility ignores an unrelated primary ONNX graph" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "stale.onnx",
        .data = "not an ONNX model",
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "llama"),
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .onnx_path = try std.fs.path.join(allocator, &.{ root, "stale.onnx" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.compatible, summary.level);
}

test "unknown opt in rejects a structurally invalid primary GGUF" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.gguf",
        .data = "not a GGUF",
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "brand_new_decoder"),
        .gguf_path = try std.fs.path.join(allocator, &.{ root, "model.gguf" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);

    var allowed_scratch: [7]backends.BackendType = undefined;
    try std.testing.expectError(
        error.IncompatibleModel,
        policyAllowedBackends(
            allocator,
            &allowed_scratch,
            root,
            &man,
            &.{.native},
            .{ .allow_unknown = true },
        ),
    );
}

test "safetensors compatibility rejects a missing referenced shard" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.safetensors.index.json",
        .data =
        \\{"weight_map":{"model.embed_tokens.weight":"missing-00001-of-00001.safetensors"}}
        ,
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "llama"),
        .safetensors_index_path = try std.fs.path.join(
            allocator,
            &.{ root, "model.safetensors.index.json" },
        ),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);
}

test "native compatibility rejects a missing lazy GGUF companion" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "llama"),
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .audio_model_path = try std.fs.path.join(allocator, &.{ root, "missing-clap.gguf" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);
}

test "native compatibility rejects a corrupt GGUF projector" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "mmproj.gguf",
        .data = "not a GGUF",
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "llama"),
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);
}

test "native compatibility rejects an unrecognized GGUF projector format" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try writeTinyHeadGgufForModelManagerTest(
        dir.dir,
        allocator,
        "mmproj.gguf",
    );
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma3"),
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.unsupported_backend, summary.code);
}

test "native compatibility rejects a projector for a different decoder family" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try writeTinyProjectorGgufForModelManagerTest(
        dir.dir,
        allocator,
        "mmproj.gguf",
        .gemma4_image,
    );
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var mismatched = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma3"),
        .hidden_size = 4,
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer mismatched.deinit();

    const mismatched_summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &mismatched,
        .native,
    )).?;
    try std.testing.expectEqual(
        model_compatibility.Level.incompatible,
        mismatched_summary.level,
    );
    try std.testing.expectEqual(
        model_compatibility.Code.unsupported_backend,
        mismatched_summary.code,
    );

    var matched = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma4"),
        .hidden_size = 4,
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer matched.deinit();
    const matched_summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &matched,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.compatible, matched_summary.level);

    var wrong_width = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma4"),
        .hidden_size = 8,
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer wrong_width.deinit();
    const wrong_width_summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &wrong_width,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, wrong_width_summary.level);
    try std.testing.expectEqual(model_compatibility.Code.missing_required_tensor, wrong_width_summary.code);

    var audio_inputs = [_][]const u8{"audio"};
    var wrong_modality = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma4"),
        .hidden_size = 4,
        .inputs = &audio_inputs,
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer {
        // The input slice and its literal are borrowed by this fixture.
        wrong_modality.inputs = &.{};
        wrong_modality.deinit();
    }
    const wrong_modality_summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &wrong_modality,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, wrong_modality_summary.level);
    try std.testing.expectEqual(model_compatibility.Code.unsupported_backend, wrong_modality_summary.code);
}

test "native compatibility rejects unsupported companion GGUF tensor types" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try writeTinyHeadGgufWithTensorTypeForModelManagerTest(
        dir.dir,
        allocator,
        "clap.gguf",
        .{ .known = .F64 },
    );
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "llama"),
        .safetensors_path = try std.fs.path.join(allocator, &.{ root, "model.safetensors" }),
        .audio_model_path = try std.fs.path.join(allocator, &.{ root, "clap.gguf" }),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        root,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.unsupported_tensor_type, summary.code);
}

test "published Gemma 4 E2B decoder and projector pass native admission when configured" {
    const allocator = std.testing.allocator;
    const decoder_env = std.c.getenv("ANTFLY_TEST_GEMMA4_E2B_DECODER_GGUF") orelse
        return error.SkipZigTest;
    const projector_env = std.c.getenv("ANTFLY_TEST_GEMMA4_E2B_PROJECTOR_GGUF") orelse
        return error.SkipZigTest;
    const decoder_path = std.mem.span(decoder_env);
    const projector_path = std.mem.span(projector_env);
    const model_dir = std.fs.path.dirname(decoder_path) orelse ".";

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .config_model_arch = try allocator.dupe(u8, "gemma4"),
        .gguf_path = try allocator.dupe(u8, decoder_path),
        .gguf_projector_path = try allocator.dupe(u8, projector_path),
    };
    defer man.deinit();

    const summary = (try compatibilitySummaryForBackend(
        allocator,
        model_dir,
        &man,
        .native,
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.compatible, summary.level);
}

test "published ClipClap GGUF pair passes native admission when configured" {
    const allocator = std.testing.allocator;
    const clip_env = std.c.getenv("ANTFLY_TEST_CLIPCLAP_CLIP_GGUF") orelse
        return error.SkipZigTest;
    const clap_env = std.c.getenv("ANTFLY_TEST_CLIPCLAP_CLAP_GGUF") orelse
        return error.SkipZigTest;

    const summary = try validateClipclapBundleForBackend(
        allocator,
        std.mem.span(clip_env),
        std.mem.span(clap_env),
        .native,
    );
    try std.testing.expect(summary == null);
}

test "native compatibility rejects a malformed split GLiNER safetensors head" {
    const allocator = std.testing.allocator;
    const summary = (try validateNativeCompanionsForBackend(
        allocator,
        &.{
            .allocator = allocator,
            .gliner_model_type = "gliner",
            .gguf_path = "/missing/encoder.gguf",
            .gliner_head_safetensors_path = "/missing/gliner_head.safetensors",
        },
        .gguf,
        .native,
        .{ .family = .unsupported },
    )).?;
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);
}

test "compatible artifact candidate wins aggregate compatibility" {
    const incompatible = CompatibilitySummary{
        .level = .incompatible,
        .code = .unsupported_tensor_type,
        .message = "bad optional artifact",
    };
    const unknown = CompatibilitySummary{
        .level = .unknown,
        .code = .unknown_architecture,
        .message = "unknown candidate",
    };
    const compatible = CompatibilitySummary{
        .level = .compatible,
        .code = .compatible,
        .message = "valid selected artifact",
    };

    try std.testing.expectEqual(
        model_compatibility.Level.unknown,
        selectBetterCompatibility(incompatible, unknown).level,
    );
    try std.testing.expectEqual(
        model_compatibility.Level.compatible,
        selectBetterCompatibility(incompatible, compatible).level,
    );
}

test "component compatibility validates explicit split ONNX graphs" {
    const allocator = std.testing.allocator;
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const bias = try builder.tensorConst(
        &.{ 0.1, 0.2, 0.3, 0.4 },
        ml.graph.Shape.init(.f32, &.{4}),
    );
    const output = try builder.add(input, bias);
    try graph.markOutput(output);
    const model_bytes = try onnx_graph.exportGraph(allocator, &graph, .{});
    defer allocator.free(model_bytes);

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "encoder_model.onnx",
        .data = model_bytes,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "decoder_model.onnx",
        .data = model_bytes,
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);
    const encoder = try std.fs.path.join(allocator, &.{ root, "encoder_model.onnx" });
    defer allocator.free(encoder);
    const decoder = try std.fs.path.join(allocator, &.{ root, "decoder_model.onnx" });
    defer allocator.free(decoder);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .transcriber,
        .native_arch_hint = .whisper,
        .config_model_arch = try allocator.dupe(u8, "whisper"),
    };
    defer man.deinit();
    const summary = try componentCompatibilityForBackend(
        allocator,
        &man,
        .native,
        &.{ encoder, decoder },
        .manifest,
    );
    try std.testing.expectEqual(model_compatibility.Level.compatible, summary.level);

    man.model_type = .reader;
    man.native_arch_hint = .none;
    const whole_reader_summary = try componentCompatibilityForBackend(
        allocator,
        &man,
        .native,
        &.{ encoder, decoder },
        .manifest,
    );
    try std.testing.expectEqual(
        model_compatibility.Level.incompatible,
        whole_reader_summary.level,
    );
    const multistage_summary = try componentCompatibilityForBackend(
        allocator,
        &man,
        .native,
        &.{ encoder, decoder },
        .multistage_ocr,
    );
    try std.testing.expectEqual(model_compatibility.Level.compatible, multistage_summary.level);

    var manager = ModelManager.init(allocator, backends.SessionManager.initWithIo(allocator, std.testing.io));
    defer manager.deinit();
    manager.configureServingPolicy(.{});
    _ = try manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{ encoder, decoder },
        .multistage_ocr,
    );
    _ = try manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{ encoder, decoder },
        .multistage_ocr,
    );
    try std.testing.expectEqual(@as(usize, 1), manager.component_plan_cache.count());

    var required_session_manager = backends.SessionManager.init(allocator);
    required_session_manager.required_backend = .cuda;
    required_session_manager.required_backend_invalid = false;
    var required_manager = ModelManager.init(allocator, required_session_manager);
    defer required_manager.deinit();
    const required_loader = try required_manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{ encoder, decoder },
        .multistage_ocr,
    );
    try std.testing.expectEqual(@as(usize, 1), required_loader.allowed_backend_count);
    try std.testing.expectEqual(backends.BackendType.cuda, required_loader.allowed_backends[0]);

    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "encoder_model.onnx",
        .data = "invalidated",
    });
    try std.testing.expectError(
        error.IncompatibleModel,
        manager.componentLoaderForPathsWithContract(
            root,
            &.{.native},
            &.{ encoder, decoder },
            .multistage_ocr,
        ),
    );
}

test "composite decoder selection preserves startup backend policy and pins generation fallback" {
    const allocator = std.testing.allocator;
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const bias = try builder.tensorConst(&.{ 1, 1, 1, 1 }, ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try builder.add(input, bias));
    const bytes = try onnx_graph.exportGraph(allocator, &graph, .{});
    defer allocator.free(bytes);
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    for ([_][]const u8{ "encoder_model.onnx", "decoder_model.onnx", "decoder_model_merged.onnx" }) |name|
        try dir.dir.writeFile(std.testing.io, .{ .sub_path = name, .data = bytes });
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data =
        \\{"model_type":"t5","vocab_size":4,"d_model":4,"decoder_start_token_id":0}
    });
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "tokenizer.json", .data =
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<unk>"}],"model":{"type":"BPE","vocab":{"<unk>":0},"merges":[]}}
    });
    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer allocator.free(root);
    const paths = try encoder_decoder.findEncoderDecoderPaths(allocator, root);
    defer allocator.free(paths.encoder);
    defer allocator.free(paths.decoder);
    var sessions = backends.SessionManager.initWithIo(allocator, std.testing.io);
    sessions.preferred_backends = &.{.native};
    var manager = ModelManager.init(allocator, sessions);
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    var first = try manager.acquireCompositeRuntime(root, &.{ paths.encoder, paths.decoder }, .seq2seq, null);
    defer first.release();
    try std.testing.expect(first.get().considered_merged_decoder);
    try std.testing.expectEqual(.full_prefix, first.get().decoder_execution);
    // The unqualified graph is fingerprinted, but never enters executable
    // backend policy (or its heavyweight session construction).
    var plans = manager.component_plan_cache.valueIterator();
    while (plans.next()) |entry| {
        if (entry.*.allowed_backend_count == 0) continue;
        for (entry.*.dependencies) |dependency|
            try std.testing.expect(!std.mem.endsWith(u8, dependency, "decoder_model_merged.onnx"));
    }
    // A startup-pinned native instance must stay the default even when the
    // server's fallback list is broader. Explicit policies still have their
    // own keys; default acquisition must reuse both sessions and the ID.
    try first.markStartupManaged();
    manager.session_manager.preferred_backends = &.{ .native, .metal };
    var second = try manager.acquireCompositeRuntime(root, &.{ paths.encoder, paths.decoder }, .seq2seq, null);
    defer second.release();
    try std.testing.expect(first.get() == second.get());
    try std.testing.expectEqual(first.get().runtime_id, second.get().runtime_id);
    try std.testing.expectEqual(@as(usize, 1), manager.composite_assets.count());
    const strict_other = try manager.compositeRuntimeKey(root, &.{ paths.encoder, paths.decoder }, .seq2seq, &.{.metal}, .rewriter);
    try std.testing.expect(!std.mem.eql(u8, &strict_other, &first.get().generation));

    try manager.validateCompositeAssetsCurrent(&first, root, &.{ paths.encoder, paths.decoder });
    // Invalid optional publication cannot displace the working ordinary graph.
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "decoder_model_merged.onnx", .data = "invalid" });
    var fallback = try manager.acquireCompositeRuntime(root, &.{ paths.encoder, paths.decoder }, .seq2seq, null);
    defer fallback.release();
    try std.testing.expect(!fallback.get().considered_merged_decoder);
    try std.testing.expectEqual(.full_prefix, fallback.get().decoder_execution);
    try std.testing.expect(first.get() != fallback.get());
}

test "split Whisper assets remain model-lifetime cached across request handles" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data =
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<unk>"},{"id":10,"content":"<|en|>"},{"id":11,"content":"<|es|>"},{"id":12,"content":"<|transcribe|>"},{"id":13,"content":"<|notimestamps|>"}],"model":{"type":"BPE","vocab":{"<unk>":0,"<|en|>":10,"<|es|>":11,"<|transcribe|>":12,"<|notimestamps|>":13},"merges":[]}}
        ,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"decoder_start_token_id":7,"eos_token_id":8,"max_length":99}
        ,
    });
    try dir.dir.createDir(std.testing.io, "other", .default_dir);
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "other/tokenizer.json",
        .data =
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<unk>"},{"id":10,"content":"<|en|>"},{"id":11,"content":"<|es|>"},{"id":12,"content":"<|transcribe|>"},{"id":13,"content":"<|notimestamps|>"}],"model":{"type":"BPE","vocab":{"<unk>":0,"<|en|>":10,"<|es|>":11,"<|transcribe|>":12,"<|notimestamps|>":13},"merges":[]}}
        ,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "other/config.json",
        .data =
        \\{"decoder_start_token_id":9,"eos_token_id":8,"max_length":88}
        ,
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);
    const other_root = try std.fs.path.join(allocator, &.{ root, "other" });
    defer allocator.free(other_root);

    var manager = ModelManager.init(allocator, backends.SessionManager.initWithIo(allocator, std.testing.io));
    defer manager.deinit();
    manager.configureModelCache(0, 2);

    var first = try manager.acquireCompositeAssets(root, &.{});
    defer first.release();
    const cached = first.get();
    try std.testing.expectEqual(@as(usize, 99), cached.decoder_config.max_length);
    try std.testing.expectEqual(@as(i32, 7), cached.decoder_config.decoder_start_token_id);
    try std.testing.expectEqual(@as(usize, 2), cached.prompt_cache.?.language_tokens.len);

    // A republished directory gets a distinct immutable generation even while
    // requests still hold the previous tokenizer and prompt metadata.
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data =
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<unk>"},{"id":10,"content":"<|en|>"},{"id":11,"content":"<|es|>"},{"id":12,"content":"<|transcribe|>"},{"id":13,"content":"<|notimestamps|>"},{"id":14,"content":"new-generation"}],"model":{"type":"BPE","vocab":{"<unk>":0,"<|en|>":10,"<|es|>":11,"<|transcribe|>":12,"<|notimestamps|>":13,"new-generation":14},"merges":[]}}
        ,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"decoder_start_token_id":17,"eos_token_id":8,"max_length":77}
        ,
    });
    var second = try manager.acquireCompositeAssets(root, &.{});
    defer second.release();
    try std.testing.expect(cached != second.get());
    try std.testing.expectEqual(@as(i32, 17), second.get().decoder_config.decoder_start_token_id);
    try std.testing.expectEqual(@as(usize, 77), second.get().decoder_config.max_length);
    try std.testing.expectEqual(@as(usize, 2), manager.composite_assets.count());
    try std.testing.expectError(
        error.ModelArtifactsChanging,
        manager.validateCompositeAssetsCurrent(&first, root, &.{}),
    );
    try manager.validateCompositeAssetsCurrent(&second, root, &.{});

    // Capacity pressure cannot invalidate an entry while either request holds it.
    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        manager.acquireCompositeAssets(other_root, &.{}),
    );
    second.release();
    first.release();

    // Once idle, the oldest generation is reclaimable and all admission-owned
    // tokenizer memory moves to the replacement entry.
    var replacement = try manager.acquireCompositeAssets(other_root, &.{});
    defer replacement.release();
    try std.testing.expectEqual(@as(usize, 2), manager.composite_assets.count());
    try std.testing.expectEqual(@as(usize, 88), replacement.get().decoder_config.max_length);

    // Publish a fake component pair through the real runtime cache. Warm
    // acquisition must lend the exact sessions, and device pressure must see
    // component leases even though the tokenizer resides in the CPU domain.
    const Probe = struct {
        closes: usize = 0,
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.closes += 1;
        }
        fn session(self: *@This()) backends.Session {
            return .{ .ptr = self, .vtable = &.{ .run = undefined, .runWithControl = undefined, .inputInfo = undefined, .outputInfo = undefined, .backend = undefined, .close = close } };
        }
    };
    var probe = Probe{};
    var controller = runtime.tier.memory.AdmissionController{};
    const metadata_key = try manager.compositeRuntimeKey(root, &.{}, .whisper_metadata, manager.session_manager.preferred_backends, .transcriber);
    const runtime_key = try manager.compositeRuntimeKey(root, &.{}, .seq2seq, manager.session_manager.preferred_backends, .rewriter);
    const metal_runtime_key = try manager.compositeRuntimeKey(root, &.{}, .seq2seq, &.{.metal}, .rewriter);
    const extractor_runtime_key = try manager.compositeRuntimeKey(root, &.{}, .seq2seq, manager.session_manager.preferred_backends, .extractor);
    try std.testing.expect(!std.mem.eql(u8, &metadata_key, &runtime_key));
    try std.testing.expect(!std.mem.eql(u8, &runtime_key, &metal_runtime_key));
    try std.testing.expect(!std.mem.eql(u8, &runtime_key, &extractor_runtime_key));
    const composite_flight_runtime_id: RuntimeId = 0xabc;
    const candidate = try manager.loadCompositeAssetsUncached(root, &.{}, manager.session_manager.preferred_backends, metadata_key, composite_flight_runtime_id, .transcriber, .whisper_metadata, null);
    try std.testing.expectEqualSlices(backends.BackendType, manager.session_manager.preferred_backends, candidate.preferred_backends);
    try std.testing.expectEqual(composite_flight_runtime_id, candidate.runtime_id);
    candidate.prompt_cache.?.deinit();
    candidate.prompt_cache = null;
    candidate.generation = runtime_key;
    candidate.kind = .seq2seq;
    candidate.model_type = .rewriter;
    candidate.encoder = .{ .session = probe.session(), .resource_lease = try controller.tryAcquire(.gpu, .{}, .{ .backend_weight_bytes = 256 }, false) };
    candidate.decoder = .{ .session = probe.session(), .resource_lease = null };
    var published = try manager.publishCompositeAssets(runtime_key, candidate);
    defer published.release();
    try std.testing.expectEqual(composite_flight_runtime_id, published.get().runtime_id);
    var warm = try manager.acquireCompositeRuntime(root, &.{}, .seq2seq, null);
    defer warm.release();
    try std.testing.expectEqual(published.get(), warm.get());
    try std.testing.expectEqual(composite_flight_runtime_id, warm.get().runtime_id);
    try std.testing.expectEqual(candidate.encoder.?.session.ptr, warm.get().encoder.?.session.ptr);
    manager.lockLoadedModels();
    const pinned = manager.takeLruCompositeAssetsForAdmissionLocked(.{ .domain_backend = .gpu });
    manager.unlockLoadedModels();
    try std.testing.expect(pinned == null);
    warm.release();
    published.release();
    manager.lockLoadedModels();
    const evicted = manager.takeLruCompositeAssetsForAdmissionLocked(.{ .domain_backend = .gpu });
    manager.unlockLoadedModels();
    try std.testing.expect(evicted != null);
    manager.destroyEvictedCompositeAssets(evicted.?);
    try std.testing.expectEqual(@as(usize, 2), probe.closes);
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, controller.snapshot());
}

test "composite cold load cancellation abandons only the departing waiter" {
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();
    const flight = try std.testing.allocator.create(CompositeAssetsLoadFlight);
    const key = @as([32]u8, @splat(0));
    flight.* = .{ .io = std.testing.io, .refs = 3, .load_state = .{ .io = std.testing.io } };
    try std.testing.expect(flight.load_state.tryAddWaiter());
    try manager.in_flight_composite_assets.put(std.testing.allocator, key, flight);
    const Canceled = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, manager.waitForCompositeAssetsLoadFlight(key, flight, .{ .check_fn = Canceled.check }));
    var load_control = LoadFlightControl{ .flight = &flight.load_state };
    try load_control.control().check();
    try std.testing.expectEqual(@as(usize, 2), flight.refs);
    manager.finishCompositeAssetsLoadFlight(flight, null, error.TestLoadFailure);
    try std.testing.expectError(error.TestLoadFailure, manager.waitForCompositeAssetsLoadFlight(key, flight, null));
    manager.releaseCompositeAssetsLoadFlight(key, flight);
    try std.testing.expectEqual(@as(usize, 0), manager.in_flight_composite_assets.count());
}

test "failed load flights allow immediate retry before the old task releases" {
    const alloc = std.testing.allocator;
    var manager = ModelManager.init(alloc, backends.SessionManager.init(alloc));
    defer manager.deinit();
    const key = @as([32]u8, @splat(1));
    const old = try alloc.create(CompositeAssetsLoadFlight);
    old.* = .{ .io = std.testing.io, .refs = 2, .load_state = .{ .io = std.testing.io } };
    try manager.in_flight_composite_assets.put(alloc, key, old);
    manager.finishCompositeAssetsLoadFlight(old, null, error.ResourceTemporarilyUnavailable);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, manager.waitForCompositeAssetsLoadFlight(key, old, null));
    manager.lockLoadedModels();
    const joinable = manager.joinableCompositeLoadFlightLocked(key);
    manager.unlockLoadedModels();
    try std.testing.expect(joinable == null);
    try std.testing.expect(!old.registered);
    const replacement = try alloc.create(CompositeAssetsLoadFlight);
    replacement.* = .{ .io = std.testing.io, .load_state = .{ .io = std.testing.io } };
    try manager.in_flight_composite_assets.put(alloc, key, replacement);
    manager.releaseCompositeAssetsLoadFlight(key, old);
    try std.testing.expect(manager.in_flight_composite_assets.get(key).? == replacement);
    manager.releaseCompositeAssetsLoadFlight(key, replacement);

    const old_model = try alloc.create(LoadFlight);
    old_model.* = .{ .io = std.testing.io };
    try manager.in_flight_loads.put(alloc, try alloc.dupe(u8, "retry"), old_model);
    manager.finishLoadFlight(old_model, null, error.ResourceTemporarilyUnavailable);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, manager.waitForLoadFlight("retry", old_model, null));
    manager.lockLoadedModels();
    const model_joinable = manager.joinableLoadFlightLocked("retry");
    manager.unlockLoadedModels();
    try std.testing.expect(model_joinable == null);
    try std.testing.expect(!old_model.registered);
    const new_model = try alloc.create(LoadFlight);
    new_model.* = .{ .io = std.testing.io, .refs = 1 };
    try manager.in_flight_loads.put(alloc, try alloc.dupe(u8, "retry"), new_model);
    manager.releaseLoadFlight("retry", old_model);
    try std.testing.expect(manager.in_flight_loads.get("retry").? == new_model);
    manager.releaseLoadFlight("retry", new_model);
}

test "component compatibility rejects malformed directory-backed native artifacts" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.safetensors",
        .data = "not a safetensors artifact",
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var parent = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .reader,
    };
    defer parent.deinit();
    const summary = try componentCompatibilityForBackend(
        allocator,
        &parent,
        .native,
        &.{root},
        .multistage_ocr,
    );
    try std.testing.expectEqual(model_compatibility.Level.incompatible, summary.level);
    try std.testing.expectEqual(model_compatibility.Code.artifact_unreadable, summary.code);

    var manager = ModelManager.init(allocator, backends.SessionManager.initWithIo(allocator, std.testing.io));
    defer manager.deinit();
    // Structurally invalid artifacts remain incompatible even when unknown
    // model contracts are explicitly permitted.
    manager.configureServingPolicy(.{ .allow_unknown = true });
    try std.testing.expectError(
        error.IncompatibleModel,
        manager.componentLoaderForPathsWithContract(
            root,
            &.{.native},
            &.{root},
            .multistage_ocr,
        ),
    );
}

test "component plan invalidates when a referenced safetensors shard changes" {
    const allocator = std.testing.allocator;
    const shard_json =
        \\{"weight":{"dtype":"F32","shape":[2],"data_offsets":[0,8]}}
    ;
    var shard_bytes: [8 + shard_json.len + 8]u8 = undefined;
    std.mem.writeInt(u64, shard_bytes[0..8], shard_json.len, .little);
    @memcpy(shard_bytes[8..][0..shard_json.len], shard_json);
    @memset(shard_bytes[8 + shard_json.len ..], 0);

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model-00001-of-00001.safetensors",
        .data = &shard_bytes,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.safetensors.index.json",
        .data =
        \\{"weight_map":{"weight":"model-00001-of-00001.safetensors"}}
        ,
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var manager = ModelManager.init(allocator, backends.SessionManager.initWithIo(allocator, std.testing.io));
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    _ = try manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{root},
        .multistage_ocr,
    );
    try std.testing.expectEqual(@as(usize, 1), manager.component_plan_cache.count());

    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model-00001-of-00001.safetensors",
        .data = "invalidated shard",
    });
    try std.testing.expectError(
        error.IncompatibleModel,
        manager.componentLoaderForPathsWithContract(
            root,
            &.{.native},
            &.{root},
            .multistage_ocr,
        ),
    );
}

test "component plan invalidates lazy ONNX graphs and their external data" {
    const allocator = std.testing.allocator;
    var graph = ml.graph.Graph.init(allocator);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const weight = try builder.parameter("weight", ml.graph.Shape.init(.f32, &.{4}));
    const output = try builder.add(input, weight);
    try graph.markOutput(output);
    const values = [_]f32{ 0.1, 0.2, 0.3, 0.4 };
    const initializer = onnx_graph.ParameterInitializer{
        .name = "weight",
        .shape = ml.graph.Shape.init(.f32, &.{4}),
        .data = .{ .raw_bytes = std.mem.asBytes(&values) },
    };
    var exported = try onnx_graph.exportGraphWithExternalData(
        allocator,
        &graph,
        .{ .parameter_initializers = &.{initializer} },
        "visual_model.data",
    );
    defer exported.deinit(allocator);

    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(
        dir.dir,
        allocator,
        "model.safetensors",
    );
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "visual_model.onnx",
        .data = exported.model_bytes,
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "visual_model.data",
        .data = exported.external_data.?.bytes,
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var manager = ModelManager.init(allocator, backends.SessionManager.initWithIo(allocator, std.testing.io));
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    _ = try manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{root},
        .multistage_ocr,
    );
    try std.testing.expectEqual(@as(usize, 1), manager.component_plan_cache.count());

    // Updating a child in place does not change the component directory's
    // identity. The external-data file itself must invalidate the cached plan.
    const composite_generation = try manager.compositeAssetGenerationSignature(root, &.{root});
    try std.testing.expectEqualSlices(u8, &composite_generation, &(try manager.compositeAssetGenerationSignature(root, &.{root})));
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "visual_model.data",
        .data = "truncated",
    });
    try std.testing.expectError(error.IncompatibleModel, manager.compositeAssetGenerationSignature(root, &.{root}));
    try std.testing.expectError(
        error.IncompatibleModel,
        manager.componentLoaderForPathsWithContract(
            root,
            &.{.native},
            &.{root},
            .multistage_ocr,
        ),
    );

    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "visual_model.data",
        .data = exported.external_data.?.bytes,
    });
    _ = try manager.componentLoaderForPathsWithContract(
        root,
        &.{.native},
        &.{root},
        .multistage_ocr,
    );

    // The lazy graph itself is also a dependency, independently of its external
    // tensor storage.
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "visual_model.onnx",
        .data = "invalidated graph",
    });
    try std.testing.expectError(
        error.IncompatibleModel,
        manager.componentLoaderForPathsWithContract(
            root,
            &.{.native},
            &.{root},
            .multistage_ocr,
        ),
    );
}

test "component loader rejects paths outside its validated plan" {
    const session_manager = backends.SessionManager.init(std.testing.allocator);
    var manager = ModelManager.init(std.testing.allocator, session_manager);
    defer manager.deinit();

    var loader = ModelManager.ComponentLoader{ .services = manager.componentLoadServices() };
    try loader.addComponentPath("/models/encoder_model.onnx");
    loader.allowed_backends[0] = .native;
    loader.allowed_backend_count = 1;
    try std.testing.expectError(
        error.UnvalidatedModelComponent,
        loader.load("/models/substituted.onnx"),
    );

    const restricted = try loader.restrictToBackend(.native);
    try std.testing.expectError(
        error.UnvalidatedModelComponent,
        restricted.load("/models/substituted.onnx"),
    );
}

test "gpu model admission classifies weights and persistent scratch" {
    const weights: usize = 1024 * 1024;
    const cuda = try nativeModelLoadAdmission(weights, .cuda, .{});
    try std.testing.expectEqual(weights, cuda.peak.host_weight_bytes);
    try std.testing.expect(cuda.peak.backend_weight_bytes >= weights);
    try std.testing.expectEqual(@as(usize, 0), cuda.resident.host_weight_bytes);
    try std.testing.expectEqual(
        cuda.peak.backend_weight_bytes,
        cuda.resident.backend_weight_bytes,
    );

    const extra_backend_resident = runtime.tier.memory.AdmissionAmounts{
        .backend_weight_bytes = 384 * 1024,
        .backend_scratch_bytes = 64 * 1024,
    };
    const metal = try nativeModelLoadAdmission(weights, .metal, extra_backend_resident);
    try std.testing.expectEqual(weights / 4, metal.peak.host_weight_bytes);
    try std.testing.expectEqual(weights + extra_backend_resident.backend_weight_bytes, metal.peak.backend_weight_bytes);
    try std.testing.expectEqual(extra_backend_resident.backend_scratch_bytes, metal.peak.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), metal.resident.host_weight_bytes);
    try std.testing.expectEqual(weights + extra_backend_resident.backend_weight_bytes, metal.resident.backend_weight_bytes);
    try std.testing.expectEqual(extra_backend_resident.backend_scratch_bytes, metal.resident.backend_scratch_bytes);
}

test "qualified F32 GLiNER specialty Metal admission retains host source without broadening generic models" {
    const pii_bytes: usize = @intCast(gliner_specialty.pii_weight.size_bytes);
    const guard_bytes: usize = @intCast(gliner_specialty.gliguard_weight.size_bytes);
    const specialty = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .safetensors_path = "model.safetensors",
        .gliner_architecture = .span,
        .gliner_span_encoder_family = .deberta,
        .gliner_specialty_profile = .pii,
    };
    try std.testing.expectEqual(pii_bytes, metalQualifiedSpecialtyF32HostSourceBytes(specialty, pii_bytes));
    try std.testing.expectEqual(@as(usize, 0), metalQualifiedSpecialtyF32HostSourceBytes(specialty, guard_bytes));

    var guard = specialty;
    guard.gliner_specialty_profile = .gliguard;
    try std.testing.expectEqual(guard_bytes, metalQualifiedSpecialtyF32HostSourceBytes(guard, guard_bytes));

    var generic = specialty;
    generic.gliner_specialty_profile = null;
    try std.testing.expectEqual(@as(usize, 0), metalQualifiedSpecialtyF32HostSourceBytes(generic, pii_bytes));
    var legacy_decide = specialty;
    legacy_decide.gliner_specialty_profile = null;
    legacy_decide.gliner_span_declared = true;
    try std.testing.expectEqual(@as(usize, 0), metalQualifiedSpecialtyF32HostSourceBytes(legacy_decide, pii_bytes));
    var reduced = specialty;
    reduced.safetensors_path = null;
    reduced.gguf_path = "model.gguf";
    try std.testing.expectEqual(@as(usize, 0), metalQualifiedSpecialtyF32HostSourceBytes(reduced, pii_bytes));

    var plan = try nativeModelLoadAdmission(1024, .metal, .{ .backend_scratch_bytes = 64 });
    const old_peak_host = plan.peak.host_weight_bytes;
    try addMetalResidentHostSource(&plan, pii_bytes);
    try std.testing.expectEqual(pii_bytes, plan.resident.host_weight_bytes);
    try std.testing.expectEqual(old_peak_host + pii_bytes, plan.peak.host_weight_bytes);
    try std.testing.expectEqual(@as(usize, 64), plan.resident.backend_scratch_bytes);

    var overflow = ModelLoadAdmissionPlan{
        .peak = .{ .host_weight_bytes = std.math.maxInt(usize) },
        .resident = .{},
    };
    try std.testing.expectError(error.ResourceLimitExceeded, addMetalResidentHostSource(&overflow, 1));
    try std.testing.expectEqual(@as(usize, 0), overflow.resident.host_weight_bytes);
    try std.testing.expectEqual(std.math.maxInt(usize), overflow.peak.host_weight_bytes);
}

test "specialty Metal host baseline evicts prior model and avoids lazy cache double charge" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.initWithRuntimeEpoch(
        allocator,
        .{ .allocator = allocator, .preferred_backends = &.{.metal} },
        0x51ec,
    );
    defer manager.deinit();
    try manager.ensureResourceOwnerReady();

    // Build a lifecycle-complete idle model, then replace its fixture CPU
    // lease with the GPU-host residency used by a Metal specialty source.
    var pii_probe = TeardownTestProbe{};
    const pii = try teardownTestModel(&manager, &pii_probe);
    pii.resource_lease.?.release();
    pii.resource_lease = try manager.acquireAmountsWithEviction(
        .gpu,
        .{ .host_limit_bytes = 64 },
        .{ .host_weight_bytes = 64 },
    );
    pii.active_handles = 0;
    pii.last_used_ns = 1;
    try manager.loaded.put(allocator, try allocator.dupe(u8, "specialty-pii-metal"), pii);

    // A second qualified model cannot share this deliberately tiny host
    // domain. Acquiring its model-owned source reservation must evict PII
    // before any request-time lazy tensor asks the tier cache for credit.
    var guard_lease = try manager.acquireAmountsWithEviction(
        .gpu,
        .{ .host_limit_bytes = 64 },
        .{ .host_weight_bytes = 64 },
    );
    defer guard_lease.release();
    try std.testing.expect(pii_probe.closed);
    try std.testing.expectEqual(@as(usize, 0), manager.loaded.count());
    try std.testing.expectEqual(@as(usize, 64), manager.admissionController().snapshot().host_weight_bytes);

    // The session tier cache receives the same resident amount as its baseline.
    // Materializing the complete borrowed F32 source therefore owns no second
    // aggregate credit and remains admissible after the eviction transition.
    var cache = runtime.tier.cache.SharedCache.init(.{ .host_limit_bytes = 64 });
    defer cache.deinitAdmission();
    try cache.configureAdmission(
        allocator,
        manager.admissionController(),
        .gpu,
        .{ .host_limit_bytes = 64 },
        .{ .host_limit_bytes = 64 },
    );
    try cache.reserve(.host, 64);
    try std.testing.expectEqual(@as(usize, 64), manager.admissionController().snapshot().host_weight_bytes);
    try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, cache.admissionAmounts());
    cache.noteRelease(.host, 64);
}

test "projector residency follows its request-scoped lifecycle" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const decoder_bytes: usize = 8192;
    const projector_bytes: usize = 4096;
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.gguf",
        .data = &(@as([decoder_bytes]u8, @splat(0x31))),
    });
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "mmproj.gguf",
        .data = &(@as([projector_bytes]u8, @splat(0x32))),
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .gguf_path = try std.fs.path.join(allocator, &.{ root, "model.gguf" }),
        .gguf_projector_path = try std.fs.path.join(allocator, &.{ root, "mmproj.gguf" }),
    };
    defer man.deinit();

    try std.testing.expectEqual(
        decoder_bytes,
        try estimateModelArtifactBytes(man),
    );
    const request = try projectorRunAdmissionAmounts(man, .native, .{});
    try std.testing.expectEqual(projector_bytes, request.host_weight_bytes);
    try std.testing.expectEqual(projector_bytes, request.hostTotalBytes());

    const metal_request = try projectorRunAdmissionAmounts(man, .metal, .{
        .host_scratch_bytes = 32,
        .backend_scratch_bytes = 64,
    });
    try std.testing.expectEqual(projector_bytes, metal_request.host_weight_bytes);
    try std.testing.expectEqual(projector_bytes, metal_request.backend_weight_bytes);
    try std.testing.expectEqual(@as(usize, 32), metal_request.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 64), metal_request.backend_scratch_bytes);
}

test "LightOnOCR3 integrated vision scratch is admitted without charging resident weights again" {
    const allocator = std.testing.allocator;
    const memory = runtime.tier.memory;
    const cfg = try @import("../models/gpt.zig").parseConfig(allocator, @embedFile("../models/testdata/lightonocr3/config.json"));
    const estimate = try @import("../architectures/qwen3vl_projector.zig").estimateAdmission(
        &.{@embedFile("../models/testdata/lightonocr3/invoice.png")},
        cfg,
        .{},
    );
    try std.testing.expect(estimate.host_scratch_bytes > 0 and estimate.backend_scratch_bytes > 0);
    var man = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .generator,
        .safetensors_path = try allocator.dupe(u8, "model.safetensors"),
    };
    defer man.deinit();
    const media = generation.NativeGenerationMediaAdmission{
        .host_scratch_bytes = estimate.host_scratch_bytes,
        .backend_scratch_bytes = estimate.backend_scratch_bytes,
    };
    const scratch = try std.math.add(usize, media.host_scratch_bytes, media.backend_scratch_bytes);
    inline for (.{ runtime.kv.pool.BackendKind.native, .metal, .cuda }) |backend| {
        const amounts = try projectorRunAdmissionAmounts(man, backend, media);
        try std.testing.expectEqual(@as(usize, 0), amounts.host_weight_bytes);
        try std.testing.expectEqual(@as(usize, 0), amounts.backend_weight_bytes);
        try std.testing.expectEqual(if (backend == .native) scratch else media.host_scratch_bytes, amounts.host_scratch_bytes);
        try std.testing.expectEqual(if (backend == .native) @as(usize, 0) else media.backend_scratch_bytes, amounts.backend_scratch_bytes);
        const backend_class: memory.BackendClass = if (backend == .native) .cpu else .gpu;
        var controller = memory.AdmissionController{};
        defer controller.deinit();
        // A one-byte-short operator cap must reject the actual image work;
        // the exact envelope admits it and releases every byte afterwards.
        try std.testing.expectError(error.ResourceLimitExceeded, controller.tryAcquire(backend_class, .{ .scratch_limit_bytes = scratch - 1 }, amounts, false));
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
        var lease = try controller.tryAcquire(backend_class, .{ .scratch_limit_bytes = scratch }, amounts, false);
        try std.testing.expectEqualDeep(amounts, controller.snapshot());
        lease.release();
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    }
}

test "onnx admission separates encoded staging from completed residency" {
    const weights: usize = 1024 * 1024;
    const cpu = try onnxModelLoadAdmission(weights, .{ .backend = .native });
    try std.testing.expectEqual(weights * 2, cpu.peak.host_weight_bytes);
    try std.testing.expectEqual(weights, cpu.resident.host_weight_bytes);

    const gpu = try onnxModelLoadAdmission(weights, .{ .backend = .metal });
    try std.testing.expectEqual(weights, gpu.peak.host_weight_bytes);
    try std.testing.expectEqual(weights, gpu.peak.backend_weight_bytes);
    try std.testing.expectEqual(@as(usize, 0), gpu.resident.host_weight_bytes);
    try std.testing.expectEqual(weights, gpu.resident.backend_weight_bytes);
    try std.testing.expectEqual(
        runtime.tier.memory.BackendClass.gpu,
        admissionBackendClassForRuntime(.{ .backend = .metal }),
    );
}

test "directory-backed component admission charges native model artifacts" {
    const allocator = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    const weight_bytes = 8192;
    try dir.dir.writeFile(std.testing.io, .{
        .sub_path = "model.gguf",
        .data = &(@as([weight_bytes]u8, @splat(0x5a))),
    });
    const root = try std.fs.path.join(
        allocator,
        &.{ ".zig-cache", "tmp", dir.sub_path[0..] },
    );
    defer allocator.free(root);

    var manifest = try manifest_mod.loadFromDir(allocator, root);
    defer manifest.deinit();
    var estimate = try ComponentArtifactEstimate.init(allocator, .{ .path = root, .kind = .directory, .manifest = &manifest });
    defer estimate.deinit();
    try std.testing.expectEqual(
        @as(usize, weight_bytes),
        try estimate.encodedBytes(),
    );
    const plan = try nativeModelLoadAdmission(
        try estimate.encodedBytes(),
        .native,
        .{},
    );
    try std.testing.expectEqual(@as(usize, weight_bytes), plan.resident.host_weight_bytes);
}

test "isManifestPotentiallyLoadableInCurrentBuild hides incomplete colqwen bundles" {
    const allocator = std.testing.allocator;
    var colqwen = manifest_mod.ModelManifest{ .allocator = allocator };
    defer colqwen.deinit();
    colqwen.inference_bundle_family = try allocator.dupe(u8, "colqwen2_gguf_bundle/v1");
    colqwen.config_model_arch = try allocator.dupe(u8, "qwen2");
    colqwen.gguf_path = try allocator.dupe(u8, "model.gguf");
    colqwen.config_path = try allocator.dupe(u8, "config.json");
    colqwen.model_manifest_path = try allocator.dupe(u8, "model_manifest.json");
    colqwen.tokenizer_json_path = try allocator.dupe(u8, "tokenizer.json");
    colqwen.tokenizer_config_path = try allocator.dupe(u8, "tokenizer_config.json");
    colqwen.preprocessor_config_path = try allocator.dupe(u8, "preprocessor_config.json");
    try std.testing.expect(!isManifestPotentiallyLoadableInCurrentBuild(colqwen));

    colqwen.processor_config_path = try allocator.dupe(u8, "processor_config.json");
    try std.testing.expect(isManifestPotentiallyLoadableInCurrentBuild(colqwen));
}

test "ClipClap manifest selects CLIP image preprocessing profile" {
    const allocator = std.testing.allocator;
    var clipclap = manifest_mod.ModelManifest{ .allocator = allocator };
    defer clipclap.deinit();

    clipclap.config_model_arch = try allocator.dupe(u8, "clipclap");
    try std.testing.expect(usesClipImagePreprocessProfile(&clipclap));

    var siglip = manifest_mod.ModelManifest{ .allocator = allocator };
    defer siglip.deinit();
    siglip.config_model_arch = try allocator.dupe(u8, "siglip");
    try std.testing.expect(!usesClipImagePreprocessProfile(&siglip));
}

test "ModelManager loads split gliner bundle and exposes runtime pipeline" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"extractor","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"position_buckets":16}
        ,
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "gliner_config.json",
        .data = "{\"model_type\":\"gliner2\",\"max_width\":4,\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"extractor\",\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "antfly_inference_bundle.json",
        .data = "{\"family\":\"gliner2_split_bundle/v1\",\"wrapper\":\"gliner2\",\"encoder\":\"encoder.gguf\",\"head\":\"gliner_head.safetensors\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data =
        \\{
        \\  "version":"1.0",
        \\  "normalizer":{"type":"BertNormalizer","lowercase":true},
        \\  "pre_tokenizer":{"type":"BertPreTokenizer"},
        \\  "post_processor":{"type":"BertProcessing","sep":["[SEP]",3],"cls":["[CLS]",2]},
        \\  "added_tokens":[
        \\    {"id":0,"content":"[PAD]"},
        \\    {"id":1,"content":"[UNK]"},
        \\    {"id":2,"content":"[CLS]"},
        \\    {"id":3,"content":"[SEP]"}
        \\  ],
        \\  "model":{
        \\    "type":"WordPiece",
        \\    "unk_token":"[UNK]",
        \\    "continuing_subword_prefix":"##",
        \\    "max_input_chars_per_word":100,
        \\    "vocab":{"[PAD]":0,"[UNK]":1,"[CLS]":2,"[SEP]":3,"hello":4,"person":5}
        \\  }
        \\}
        ,
    });
    try writeTinyDebertaEncoderGgufForModelManagerTest(tmp.dir, allocator, "encoder.gguf");
    try writeTinyHeadSafetensorsForModelManagerTest(tmp.dir, allocator, "gliner_head.safetensors");

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();

    const ColdLoadWorker = struct {
        manager: *ModelManager,
        path: []const u8,
        model: ?*LoadedModel = null,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.model = self.manager.loadFromDir(self.path) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var workers = [_]ColdLoadWorker{
        .{ .manager = &manager, .path = dir_path },
        .{ .manager = &manager, .path = dir_path },
        .{ .manager = &manager, .path = dir_path },
        .{ .manager = &manager, .path = dir_path },
    };
    var threads: [workers.len]std.Io.Future(void) = undefined;
    var started_tasks: usize = 0;
    defer {
        for (threads[0..started_tasks]) |*task| task.await(std.testing.io);
    }
    for (&workers, 0..) |*worker, i| {
        threads[i] = try std.testing.io.concurrent(ColdLoadWorker.run, .{worker});
        started_tasks += 1;
    }
    for (&threads) |*thread| thread.await(std.testing.io);
    for (workers) |worker| {
        try std.testing.expect(worker.err == null);
        try std.testing.expect(worker.model == workers[0].model);
    }

    const model = workers[0].model.?;
    try std.testing.expect(model.isGlinerModel());
    try std.testing.expect(model.supportsExtraction());
    try std.testing.expectEqualStrings("gliner2_split_bundle/v1", model.manifest.inference_bundle_family);

    var pipeline = model.glinerPipeline(allocator);
    try std.testing.expectEqualStrings("gliner2", pipeline.config.model_type);
    try std.testing.expectError(error.MissingSpecialTokenIds, pipeline.recognizeBatch(&.{"hello"}, &.{"person"}));
}

test "ModelManager strict serving policy rejects unknown generator architectures" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"generator\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\"model_type\":\"brand_new_decoder\"}",
    });

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = false });

    try std.testing.expectError(error.UnknownModelCompatibility, manager.loadFromDir(dir_path));
}

test "ModelManager default serving policy still rejects a generator without a loadable artifact" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"generator\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\"model_type\":\"brand_new_decoder\"}",
    });

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();
    manager.configureServingPolicy(.{});

    try std.testing.expectError(error.IncompatibleModel, manager.loadFromDir(dir_path));
}

test "ModelManager default serving policy does not enable a known incompatible generator" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"generator\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\"model_type\":\"deepseek4\"}",
    });

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();
    manager.configureServingPolicy(.{});

    try std.testing.expectError(error.IncompatibleModel, manager.loadFromDir(dir_path));
}

test "ModelManager loads split gliner gguf-head bundle and exposes runtime pipeline" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"extractor","hidden_size":4,"num_hidden_layers":1,"num_attention_heads":2,"intermediate_size":8,"vocab_size":16,"max_position_embeddings":16,"position_buckets":16}
        ,
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "gliner_config.json",
        .data = "{\"model_type\":\"gliner2\",\"max_width\":4,\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "model_manifest.json",
        .data = "{\"type\":\"extractor\",\"capabilities\":[\"extraction\"]}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "antfly_inference_bundle.json",
        .data = "{\"family\":\"gliner2_split_bundle/v1\",\"wrapper\":\"gliner2\",\"encoder\":\"encoder.gguf\",\"head\":\"gliner_head.gguf\"}",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "tokenizer.json",
        .data =
        \\{
        \\  "version":"1.0",
        \\  "normalizer":{"type":"BertNormalizer","lowercase":true},
        \\  "pre_tokenizer":{"type":"BertPreTokenizer"},
        \\  "post_processor":{"type":"BertProcessing","sep":["[SEP]",3],"cls":["[CLS]",2]},
        \\  "added_tokens":[
        \\    {"id":0,"content":"[PAD]"},
        \\    {"id":1,"content":"[UNK]"},
        \\    {"id":2,"content":"[CLS]"},
        \\    {"id":3,"content":"[SEP]"}
        \\  ],
        \\  "model":{
        \\    "type":"WordPiece",
        \\    "unk_token":"[UNK]",
        \\    "continuing_subword_prefix":"##",
        \\    "max_input_chars_per_word":100,
        \\    "vocab":{"[PAD]":0,"[UNK]":1,"[CLS]":2,"[SEP]":3,"hello":4,"person":5}
        \\  }
        \\}
        ,
    });
    try writeTinyDebertaEncoderGgufForModelManagerTest(tmp.dir, allocator, "encoder.gguf");
    try writeTinyHeadGgufForModelManagerTest(tmp.dir, allocator, "gliner_head.gguf");

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    var manager = ModelManager.init(allocator, .{
        .allocator = allocator,
        .preferred_backends = &.{.native},
    });
    defer manager.deinit();

    const model = try manager.loadFromDir(dir_path);
    try std.testing.expect(model.isGlinerModel());
    try std.testing.expect(model.supportsExtraction());
    try std.testing.expectEqualStrings("gliner2_split_bundle/v1", model.manifest.inference_bundle_family);

    var pipeline = model.glinerPipeline(allocator);
    try std.testing.expectEqualStrings("gliner2", pipeline.config.model_type);
    try std.testing.expectError(error.MissingSpecialTokenIds, pipeline.recognizeBatch(&.{"hello"}, &.{"person"}));
}

test "shouldPreferSentencePieceOverride still prefers sentencepiece for multimodal gemma" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "tokenizer.model", .data = "fake-spm" });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "added_tokens.json",
        .data = "{\n  \"<image_soft_token>\": 262144\n}\n",
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data = "{\n  \"model_type\": \"gemma3\"\n}\n",
    });

    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);

    try std.testing.expect(shouldPreferSentencePieceOverride(man, dir_path, allocator));
}

test "shouldEnableGemmaSentencePieceCompat applies to gguf-only gemma dirs" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var man = manifest_mod.ModelManifest{ .allocator = allocator };
    defer man.deinit();
    man.tokenizer_type = .sentencepiece;

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..], "gemma-4-e2b-it-gguf" });
    defer allocator.free(dir_path);

    try std.testing.expect(shouldEnableGemmaSentencePieceCompat(man, dir_path, allocator));
    try std.testing.expect(!shouldPreferSentencePieceOverride(man, dir_path, allocator));
}

test "sentencepiece tokenizer is owned before added-token failure" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "added_tokens.json",
        .data = "]",
    });

    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(dir_path);
    const man = manifest_mod.ModelManifest{ .allocator = allocator };

    const Harness = struct {
        fn run(a: std.mem.Allocator, model_dir: []const u8, manifest: manifest_mod.ModelManifest) !void {
            var owned: ?*sentencepiece.Processor = null;
            errdefer if (owned) |sp| {
                sp.deinit();
                a.destroy(sp);
            };

            const sp = try a.create(sentencepiece.Processor);
            errdefer if (owned == null) a.destroy(sp);
            sp.* = try sentencepiece.Processor.initFromPieces(a, &.{
                .{ .text = "<unk>", .score = 0, .piece_type = 2 },
                .{ .text = "token", .score = -1, .piece_type = 1 },
            }, .{});

            try adoptAndConfigureSentencePieceTokenizer(&owned, sp, manifest, model_dir, a);

            // Keep an unexpected success leak-free so expectError reports only
            // the missing post-load failure.
            sp.deinit();
            a.destroy(sp);
            owned = null;
        }
    };

    try std.testing.expectError(error.SyntaxError, Harness.run(allocator, dir_path, man));
}

test "loadSentencePieceAddedTokens overlays gemma special tokens from tokenizer json" {
    const allocator = std.testing.allocator;
    const model_dir = "models/google/gemma-3-4b-it";
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) return error.SkipZigTest;
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.json")) return error.SkipZigTest;

    const sp_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.model", .{model_dir});
    defer allocator.free(sp_path);
    var sp = try sentencepiece.Processor.initFromPath(allocator, sp_path);
    defer sp.deinit();

    try loadSentencePieceAddedTokens(model_dir, allocator, &sp);
    try std.testing.expectEqual(@as(?i32, 105), sp.piece_map.get("<start_of_turn>"));
    try std.testing.expectEqual(@as(?i32, 262144), sp.extra_reserved_map.get("<image_soft_token>"));
    try std.testing.expectEqual("<start_of_turn>".len, sp.special_matcher.findPrefixLen("<start_of_turn>"));

    const encoded = try sp.tokenizer().encodeForGenerationConfigured(allocator, "<start_of_turn>", 16, false);
    defer {
        var encoded_mut = encoded;
        encoded_mut.deinit();
    }
    var found = false;
    for (encoded.ids[0..encoded.attention_mask.len], 0..) |id, idx| {
        if (encoded.attention_mask[idx] == 0) break;
        if (id == 105) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
}

test "gemma sentencepiece prompt parity against hf tokenizer" {
    const allocator = std.testing.allocator;
    const model_dir = "models/google/gemma-3-4b-it";
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.model")) return error.SkipZigTest;
    if (!c_file.fileExistsInDir(allocator, model_dir, "tokenizer.json")) return error.SkipZigTest;

    const prompt =
        "<bos><start_of_turn>user\n" ++
        "<start_of_image>Describe this image.<end_of_turn>\n" ++
        "<start_of_turn>model\n";

    const sp_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.model", .{model_dir});
    defer allocator.free(sp_path);
    var sp = try sentencepiece.Processor.initFromPath(allocator, sp_path);
    defer sp.deinit();
    try loadSentencePieceAddedTokens(model_dir, allocator, &sp);

    const tokenizer_path = try std.fmt.allocPrint(allocator, "{s}/tokenizer.json", .{model_dir});
    defer allocator.free(tokenizer_path);
    const tokenizer_bytes = try c_file.readFile(allocator, tokenizer_path);
    defer allocator.free(tokenizer_bytes);
    var hf = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_bytes);
    defer hf.deinitSelf();

    var sp_encoded = try sp.tokenizer().encodeForGenerationConfigured(allocator, prompt, 512, false);
    defer sp_encoded.deinit();
    var hf_encoded = try hf.tokenizer().encodeForGenerationConfigured(allocator, prompt, 512, false);
    defer hf_encoded.deinit();

    var sp_count: usize = 0;
    while (sp_count < sp_encoded.attention_mask.len and sp_encoded.attention_mask[sp_count] != 0) : (sp_count += 1) {}
    var hf_count: usize = 0;
    while (hf_count < hf_encoded.attention_mask.len and hf_encoded.attention_mask[hf_count] != 0) : (hf_count += 1) {}
    try std.testing.expectEqual(sp_count, hf_count);
    try std.testing.expectEqualSlices(i32, sp_encoded.ids[0..sp_count], hf_encoded.ids[0..hf_count]);
}

fn writeTinyDebertaEncoderGgufForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
) !void {
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "deberta" } },
        .{ .key = "general.alignment", .value = .{ .u32 = @intCast(gguf_format.default_alignment) } },
        .{ .key = "deberta.vocab_size", .value = .{ .u32 = 16 } },
        .{ .key = "deberta.embedding_length", .value = .{ .u32 = 4 } },
        .{ .key = "deberta.block_count", .value = .{ .u32 = 1 } },
        .{ .key = "deberta.attention.head_count", .value = .{ .u32 = 2 } },
        .{ .key = "deberta.feed_forward_length", .value = .{ .u32 = 8 } },
        .{ .key = "deberta.context_length", .value = .{ .u32 = 16 } },
        .{ .key = "deberta.position_buckets", .value = .{ .u32 = 16 } },
        .{ .key = "deberta.label_count", .value = .{ .u32 = 1 } },
    };
    const dims_vocab = [_]u64{ 4, 16 };
    const dims_hidden = [_]u64{4};
    const dims_rel = [_]u64{ 4, 16 };
    const dims_dense = [_]u64{ 4, 4 };
    const dims_intermediate = [_]u64{ 4, 8 };
    const dims_output = [_]u64{ 8, 4 };
    const dims_intermediate_bias = [_]u64{8};
    const tensors = [_]gguf_writer.TensorSpec{
        .{ .name = "embeddings.word_embeddings.weight", .dimensions = &dims_vocab, .tensor_type = .{ .known = .F32 } },
        .{ .name = "embeddings.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "embeddings.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.rel_embeddings.weight", .dimensions = &dims_rel, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.query_proj.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.query_proj.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.key_proj.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.key_proj.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.value_proj.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.self.value_proj.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.dense.weight", .dimensions = &dims_dense, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.dense.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.attention.output.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.intermediate.dense.weight", .dimensions = &dims_intermediate, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.intermediate.dense.bias", .dimensions = &dims_intermediate_bias, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.dense.weight", .dimensions = &dims_output, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.dense.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.LayerNorm.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
        .{ .name = "encoder.layer.0.output.LayerNorm.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
    };

    var layout = try gguf_writer.buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);
    try data.appendSlice(allocator, layout.header_bytes);
    const data_region_offset = std.mem.alignForward(usize, layout.header_bytes.len, @intCast(layout.alignment));
    try data.appendNTimes(allocator, 0, data_region_offset - layout.header_bytes.len);

    var written_offset: u64 = 0;
    for (tensors, layout.offsets) |tensor, offset| {
        if (offset > written_offset) {
            try data.appendNTimes(allocator, 0, @intCast(offset - written_offset));
            written_offset = offset;
        }
        const byte_len = gguf_tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType;
        try data.appendNTimes(allocator, 0, byte_len);
        written_offset += @intCast(byte_len);
    }

    try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = data.items });
}

fn writeTinyHeadSafetensorsForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
) !void {
    const json =
        \\{"span_rep.test":{"dtype":"F32","shape":[2],"data_offsets":[0,8]}}
    ;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);
    try appendLeModelManagerTest(u64, allocator, &data, json.len);
    try data.appendSlice(allocator, json);
    try data.appendSlice(allocator, std.mem.asBytes(&[_]f32{ 0.0, 0.0 }));
    try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = data.items });
}

fn writeTinyHeadGgufForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
) !void {
    return writeTinyHeadGgufWithTensorTypeForModelManagerTest(
        dir,
        allocator,
        sub_path,
        .{ .known = .F32 },
    );
}

fn writeTinyHeadGgufWithTensorTypeForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
    tensor_type: gguf_tensor_types.TensorType,
) !void {
    const metadata = [_]gguf_format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "antfly-gliner-head" } },
        .{ .key = "general.alignment", .value = .{ .u32 = @intCast(gguf_format.default_alignment) } },
    };
    return writeTinyGgufForModelManagerTest(
        dir,
        allocator,
        sub_path,
        &metadata,
        tensor_type,
    );
}

const TestProjectorKind = enum {
    gemma3,
    gemma4_image,
};

fn writeTinyProjectorGgufForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
    kind: TestProjectorKind,
) !void {
    const metadata: []const gguf_format.MetadataEntry = switch (kind) {
        .gemma3 => &[_]gguf_format.MetadataEntry{
            .{ .key = "general.architecture", .value = .{ .string = "antfly-projector" } },
            .{ .key = "inference.projector.source_architecture", .value = .{ .string = "gemma3" } },
            .{ .key = "general.alignment", .value = .{ .u32 = @intCast(gguf_format.default_alignment) } },
        },
        .gemma4_image => &[_]gguf_format.MetadataEntry{
            .{ .key = "general.architecture", .value = .{ .string = "clip" } },
            .{ .key = "clip.vision.projector_type", .value = .{ .string = "gemma4uv" } },
            .{ .key = "clip.vision.projection_dim", .value = .{ .u32 = 4 } },
            .{ .key = "clip.vision.embedding_length", .value = .{ .u32 = 4 } },
            .{ .key = "clip.vision.feed_forward_length", .value = .{ .u32 = 0 } },
            .{ .key = "clip.vision.block_count", .value = .{ .u32 = 0 } },
            .{ .key = "clip.vision.attention.head_count", .value = .{ .u32 = 0 } },
            .{ .key = "clip.vision.image_size", .value = .{ .u32 = 224 } },
            .{ .key = "clip.vision.patch_size", .value = .{ .u32 = 14 } },
            .{ .key = "clip.vision.projector_scale_factor", .value = .{ .u32 = 1 } },
            .{ .key = "general.alignment", .value = .{ .u32 = @intCast(gguf_format.default_alignment) } },
        },
    };
    if (kind == .gemma4_image) {
        const dims_patch = [_]u64{588};
        const dims_hidden = [_]u64{4};
        const dims_patch_projection = [_]u64{ 4, 588 };
        const dims_position = [_]u64{ 2, 16, 4 };
        const dims_projection = [_]u64{ 4, 4 };
        const tensors = [_]gguf_writer.TensorSpec{
            .{ .name = "v.patch_norm.1.weight", .dimensions = &dims_patch, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_norm.1.bias", .dimensions = &dims_patch, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_embd.weight", .dimensions = &dims_patch_projection, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_embd.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_norm.2.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_norm.2.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.position_embd.weight", .dimensions = &dims_position, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_norm.3.weight", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
            .{ .name = "v.patch_norm.3.bias", .dimensions = &dims_hidden, .tensor_type = .{ .known = .F32 } },
            .{ .name = "mm.input_projection.weight", .dimensions = &dims_projection, .tensor_type = .{ .known = .F32 } },
        };
        var layout = try gguf_writer.buildLayout(allocator, metadata, &tensors);
        defer layout.deinit(allocator);
        var data = std.ArrayListUnmanaged(u8).empty;
        defer data.deinit(allocator);
        try data.appendSlice(allocator, layout.header_bytes);
        const data_region_offset = std.mem.alignForward(
            usize,
            layout.header_bytes.len,
            @intCast(layout.alignment),
        );
        try data.appendNTimes(allocator, 0, data_region_offset - layout.header_bytes.len);
        var written_offset: u64 = 0;
        for (tensors, layout.offsets) |tensor, offset| {
            if (offset > written_offset) {
                try data.appendNTimes(allocator, 0, @intCast(offset - written_offset));
                written_offset = offset;
            }
            const byte_len = gguf_tensor_types.byteLen(
                tensor.tensor_type,
                tensor.dimensions,
            ) orelse return error.UnsupportedTensorType;
            try data.appendNTimes(allocator, 0, byte_len);
            written_offset += @intCast(byte_len);
        }
        try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = data.items });
        return;
    }
    return writeTinyGgufForModelManagerTest(
        dir,
        allocator,
        sub_path,
        metadata,
        .{ .known = .F32 },
    );
}

fn writeTinyGgufForModelManagerTest(
    dir: anytype,
    allocator: std.mem.Allocator,
    sub_path: []const u8,
    metadata: []const gguf_format.MetadataEntry,
    tensor_type: gguf_tensor_types.TensorType,
) !void {
    const dims = [_]u64{2};
    const tensors = [_]gguf_writer.TensorSpec{
        .{ .name = "span_rep.test", .dimensions = &dims, .tensor_type = tensor_type },
    };

    var layout = try gguf_writer.buildLayout(allocator, metadata, &tensors);
    defer layout.deinit(allocator);

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);
    try data.appendSlice(allocator, layout.header_bytes);
    const data_region_offset = std.mem.alignForward(usize, layout.header_bytes.len, @intCast(layout.alignment));
    try data.appendNTimes(allocator, 0, data_region_offset - layout.header_bytes.len);
    const tensor_byte_len = gguf_tensor_types.byteLen(tensor_type, &dims) orelse
        return error.UnsupportedTensorType;
    try data.appendNTimes(allocator, 0, tensor_byte_len);

    try dir.writeFile(std.testing.io, .{ .sub_path = sub_path, .data = data.items });
}

fn appendLeModelManagerTest(
    comptime T: type,
    allocator: std.mem.Allocator,
    data: *std.ArrayListUnmanaged(u8),
    value: T,
) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try data.appendSlice(allocator, &buf);
}

test "load huggingface tokenizer from gguf gpt2 metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithGpt2Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "ggml-model-i2_s.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    const gguf_path = try std.fs.path.join(allocator, &.{ model_dir, "ggml-model-i2_s.gguf" });
    defer allocator.free(gguf_path);

    var tok = try loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, gguf_path);
    defer tok.deinitSelf();

    var encoded = try tok.tokenizer().encodeForGenerationConfigured(allocator, "hello", 8, true);
    defer encoded.deinit();

    try std.testing.expectEqual(@as(i32, 0), encoded.ids[0]);
    try std.testing.expectEqual(@as(i32, 1), encoded.ids[1]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[0]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[1]);
}

test "load huggingface tokenizer from gguf gemma4 bpe metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithGemma4Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "gemma4-q4_0.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    const gguf_path = try std.fs.path.join(allocator, &.{ model_dir, "gemma4-q4_0.gguf" });
    defer allocator.free(gguf_path);

    var tok = try loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, gguf_path);
    defer tok.deinitSelf();

    var encoded = try tok.tokenizer().encodeForGenerationConfigured(allocator, "hello world", 8, true);
    defer encoded.deinit();

    try std.testing.expectEqual(@as(i32, 2), encoded.ids[0]);
    try std.testing.expectEqual(@as(i32, 7), encoded.ids[1]);
    try std.testing.expectEqual(@as(i32, 5), encoded.ids[2]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[0]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[1]);
    try std.testing.expectEqual(@as(i32, 1), encoded.attention_mask[2]);

    const special_ids = try tok.tokenizer().encode(allocator, "<|turn>hello");
    defer allocator.free(special_ids);
    try std.testing.expectEqualSlices(i32, &.{ 6, 7 }, special_ids);

    const decoded = try tok.tokenizer().decode(allocator, &.{ 4, 7 });
    defer allocator.free(decoded);
    try std.testing.expectEqualStrings("hellohello", decoded);
    try std.testing.expectEqual(@as(usize, 8), tok.tokenizer().vocabSize());
}

test "load huggingface tokenizer from gguf t5 unigram metadata" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithT5Tokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "bge-m3-q4_k_m.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    const gguf_path = try std.fs.path.join(allocator, &.{ model_dir, "bge-m3-q4_k_m.gguf" });
    defer allocator.free(gguf_path);

    var tok = try loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, gguf_path);
    defer tok.deinitSelf();

    var encoded = try tok.tokenizer().encodeForModel(allocator, "hello world", 8);
    defer encoded.deinit();
    try std.testing.expectEqualSlices(i32, &.{ 0, 4, 5, 2, 1, 1, 1, 1 }, encoded.ids);
    try std.testing.expectEqualSlices(i32, &.{ 1, 1, 1, 1, 0, 0, 0, 0 }, encoded.attention_mask);
}

fn buildTestGgufWithGpt2Tokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 7);

    try appendTestMetadataString(allocator, &data, "general.architecture", "bitnet-b1.58");
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "gpt2");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{
        "<|begin_of_text|>",
        "hello",
        "<|end_of_text|>",
    });
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.merges", &.{});
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 2);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", true);

    return data.toOwnedSlice(allocator);
}

/// Qwen3-Embedding-shaped tokenizer metadata: bos and eos share one id
/// (<|endoftext|>), add_bos_token=false, add_eos_token=true. The historical
/// bos->cls mapping would wrap encodeForModel sequences with a leading eos
/// the HF reference never emits.
fn buildTestGgufWithQwen3EmbeddingTokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 9);

    try appendTestMetadataString(allocator, &data, "general.architecture", "qwen3");
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "gpt2");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{
        "hello",
        "world",
        "<|endoftext|>",
    });
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.merges", &.{});
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 2);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 2);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.padding_token_id", 2);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", false);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_eos_token", true);

    return data.toOwnedSlice(allocator);
}

test "load huggingface tokenizer from gguf qwen3 embedding metadata wraps eos only" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const gguf_bytes = try buildTestGgufWithQwen3EmbeddingTokenizer(allocator);
    defer allocator.free(gguf_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "qwen3-embedding-q8_0.gguf", .data = gguf_bytes });

    const model_dir = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp.sub_path[0..] });
    defer allocator.free(model_dir);
    const gguf_path = try std.fs.path.join(allocator, &.{ model_dir, "qwen3-embedding-q8_0.gguf" });
    defer allocator.free(gguf_path);

    var tok = try loadHuggingFaceTokenizerFromDirOrGguf(allocator, model_dir, gguf_path);
    defer tok.deinitSelf();

    var encoded = try tok.tokenizer().encodeForModel(allocator, "hello", 4);
    defer encoded.deinit();

    // No leading token; one trailing <|endoftext|>; padding masked out.
    try std.testing.expectEqualSlices(i32, &.{ 0, 2, 2, 2 }, encoded.ids);
    try std.testing.expectEqualSlices(i32, &.{ 1, 1, 0, 0 }, encoded.attention_mask);
}

fn buildTestGgufWithGemma4Tokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 10);

    try appendTestMetadataString(allocator, &data, "general.architecture", "gemma4");
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "gemma4");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{
        "<pad>",
        "<eos>",
        "<bos>",
        "<unk>",
        "hello",
        "▁world",
        "<|turn>",
        "hello",
    });
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.merges", &.{});
    try appendTestMetadataI32Array(allocator, &data, "tokenizer.ggml.token_type", &.{ 3, 3, 3, 2, 1, 1, 3, 1 });
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 2);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 1);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.padding_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.unknown_token_id", 3);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", true);

    return data.toOwnedSlice(allocator);
}

fn buildTestGgufWithT5Tokenizer(allocator: std.mem.Allocator) ![]u8 {
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, gguf_format.magic);
    try appendTestLe(u32, allocator, &data, 3);
    try appendTestLe(u64, allocator, &data, 0);
    try appendTestLe(u64, allocator, &data, 11);

    try appendTestMetadataString(allocator, &data, "general.architecture", "bert");
    try appendTestMetadataString(allocator, &data, "tokenizer.ggml.model", "t5");
    try appendTestMetadataStringArray(allocator, &data, "tokenizer.ggml.tokens", &.{
        "<s>", "<pad>", "</s>", "<unk>", "\u{2581}hello", "\u{2581}world",
    });
    try appendTestMetadataF32Array(allocator, &data, "tokenizer.ggml.scores", &.{ 0, 0, 0, 0, -1, -1 });
    try appendTestMetadataI32Array(allocator, &data, "tokenizer.ggml.token_type", &.{ 3, 3, 3, 2, 1, 1 });
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.bos_token_id", 0);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.eos_token_id", 2);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.padding_token_id", 1);
    try appendTestMetadataU32(allocator, &data, "tokenizer.ggml.unknown_token_id", 3);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_bos_token", true);
    try appendTestMetadataBool(allocator, &data, "tokenizer.ggml.add_eos_token", true);

    return data.toOwnedSlice(allocator);
}

fn appendTestLe(comptime T: type, allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: T) !void {
    const bytes = std.mem.asBytes(&std.mem.nativeToLittle(T, value));
    try data.appendSlice(allocator, bytes);
}

fn appendTestString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try appendTestLe(u64, allocator, data, value.len);
    try data.appendSlice(allocator, value);
}

fn appendTestMetadataString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: []const u8) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.string));
    try appendTestString(allocator, data, value);
}

fn appendTestMetadataU32(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: u32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.u32));
    try appendTestLe(u32, allocator, data, value);
}

fn appendTestMetadataBool(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: bool) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.bool_));
    try appendTestLe(u8, allocator, data, @intFromBool(value));
}

fn appendTestMetadataStringArray(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const []const u8) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.string));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestString(allocator, data, value);
}

fn appendTestMetadataI32Array(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const i32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.i32));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestLe(i32, allocator, data, value);
}

fn appendTestMetadataF32Array(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const f32) !void {
    try appendTestString(allocator, data, key);
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.array));
    try appendTestLe(u32, allocator, data, @backingInt(gguf_format.MetadataValueType.f32));
    try appendTestLe(u64, allocator, data, values.len);
    for (values) |value| try appendTestLe(u32, allocator, data, @bitCast(value));
}

test "gliner boundary cache pinned small Metal handle retention eviction and reload" {
    try @import("gliner_boundary_cache_lifecycle_test.zig").exercise(.{ .run = ModelManager.evictExpiredAt, .close_timeout_ns = TeardownTicket.timeout_ns });
}

test "offline load runtime supplies a real hard cancellation boundary only for disposable processes" {
    const alloc = std.testing.allocator;
    var manager = ModelManager.init(alloc, .{ .allocator = alloc, .preferred_backends = &.{.metal}, .process_termination_available = false });
    defer manager.deinit();
    try std.testing.expect(try manager.offlineLoadBoundaryLocked(std.testing.io) == null);
    try std.testing.expect(manager.owned_load_watchdog == null);
    manager.session_manager.process_termination_available = true;
    const boundary = (try manager.offlineLoadBoundaryLocked(std.testing.io)).?;
    const again = (try manager.offlineLoadBoundaryLocked(std.testing.io)).?;
    try std.testing.expectEqual(boundary.ptr, again.ptr);
    const control = InferenceExecutionControl{ .hard_cancellation = boundary };
    var guard = try control.enterUninterruptible(.uninterruptible);
    guard.deinit();
    try std.testing.expectEqual(@as(usize, 0), manager.owned_load_watchdog.?.entries.items.len);
}

// One bounded test-only OS thread owns stderr while the actual watchdog must
// terminate another thread. Ready is published only after the lock is held.
const TeardownStderrBlocker = struct {
    ready: std.atomic.Value(bool) = .init(false),
    stopping: std.atomic.Value(bool) = .init(false),
    thread: ?std.Thread = null,

    fn hold(self: *@This()) void {
        const locked = std.debug.lockStderr(&.{});
        defer std.debug.unlockStderr();
        locked.file_writer.interface.writeAll("teardown-fixture stderr-lock-held\n") catch return;
        locked.file_writer.interface.flush() catch return;
        self.ready.store(true, .release);
        while (!self.stopping.load(.acquire)) std.atomic.spinLoopHint();
    }

    fn start(self: *@This()) !void {
        self.thread = try std.Thread.spawn(.{}, hold, .{self});
        const deadline = platform.time.monotonicNs() + std.time.ns_per_s;
        while (!self.ready.load(.acquire)) {
            if (platform.time.monotonicNs() >= deadline) return error.StderrLockFixtureNotReady;
            std.atomic.spinLoopHint();
        }
    }

    pub fn deinit(self: *@This()) void {
        self.stopping.store(true, .release);
        if (self.thread) |thread| thread.join();
        self.thread = null;
    }
};

const TeardownTestProbe = struct {
    ticket: ?*TeardownTicket = null,
    controller: ?*runtime.tier.memory.AdmissionController = null,
    peer_ticket: ?*TeardownTicket = null,
    constructor_entries: usize = 0,
    closed: bool = false,
    block: bool = false,
    stderr_locked: bool = false,
    wait_until_deadline: bool = false,

    fn run(_: *anyopaque, _: []const backends.Tensor, allocator: std.mem.Allocator) ![]backends.Tensor {
        return allocator.alloc(backends.Tensor, 0);
    }
    fn info(_: *anyopaque) []const backends.TensorInfo {
        return &.{};
    }
    fn backend(_: *anyopaque) backends.BackendType {
        return .metal;
    }
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.closed) @panic("teardown fixture session closed twice");
        const ticket = self.ticket.?;
        if (ticket.deadline_ns.load(.acquire) == 0 or ticket.refs.load(.acquire) < 2)
            @panic("teardown fixture session has no active close protection");
        if (self.peer_ticket) |peer| {
            if (peer.deadline_ns.load(.acquire) == 0)
                @panic("teardown fixture peer protection is not active");
        }
        if (self.controller) |controller| {
            if (controller.snapshot().host_weight_bytes != 64)
                @panic("teardown fixture released admission before physical close");
        }
        if (self.block or self.wait_until_deadline) {
            const marker: []const u8 = if (self.controller != null)
                "teardown-fixture close-entered lease-held64\n"
            else
                "teardown-fixture close-entered no-lease\n";
            if (self.stderr_locked) {
                // Separate bounded regular-file output supplied by the child
                // runner; never reacquire the deliberately held stderr lock.
                std.Io.File.stdout().writeStreamingAll(std.testing.io, marker) catch
                    @panic("teardown fixture marker write failed");
            } else std.debug.print("{s}", .{marker});
            if (self.block) while (true) std.atomic.spinLoopHint();
            while (platform.time.monotonicNs() < ticket.deadline_ns.load(.acquire)) std.atomic.spinLoopHint();
        }
        self.closed = true;
    }
    const vtable = backends.Session.VTable{
        .run = run,
        .inputInfo = info,
        .outputInfo = info,
        .backend = backend,
        .close = close,
    };

    fn session(self: *@This(), manager: *ModelManager) !backends.Session {
        // The fake backend must not be entered before fallible protection.
        const protection = (try manager.prepareSessionClose(.{ .backend = .metal })).?;
        self.constructor_entries += 1;
        self.ticket = @ptrCast(@alignCast(protection.ptr));
        return .{ .ptr = self, .vtable = &vtable, .close_protection = protection };
    }
};

fn teardownTestModel(manager: *ModelManager, probe: *TeardownTestProbe) !*LoadedModel {
    return teardownTestModelWithHostLimit(manager, probe, 64);
}

fn teardownTestModelWithHostLimit(manager: *ModelManager, probe: *TeardownTestProbe, host_limit_bytes: usize) !*LoadedModel {
    const allocator = manager.allocator;
    const model = try allocator.create(LoadedModel);
    errdefer allocator.destroy(model);
    const path = try allocator.dupe(u8, "teardown-fixture");
    errdefer allocator.free(path);
    var lease = try manager.acquireAmountsWithEviction(.cpu, .{ .host_limit_bytes = host_limit_bytes }, .{ .host_weight_bytes = 64 });
    errdefer lease.release();
    probe.controller = manager.admissionController();
    const session = try probe.session(manager);
    model.* = .{
        .manifest = .{ .allocator = allocator },
        .hf_tok = null,
        .sp_tok = null,
        .session = session,
        .session_manager = &manager.session_manager,
        .host = manager.loadedRuntimeHost(),
        .model_dir = path,
        .allocator = allocator,
        .prompt_prefix_cache = runtime.kv.prompt_cache.PromptPrefixCache.init(allocator),
        .native_generation_graph_cache = graph_mod.cache.GraphCache.init(allocator),
        .resource_lease = lease,
    };
    return model;
}

test "runtime snapshot is observational and exact primary unload honors lifecycle guards" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.initWithRuntimeEpoch(
        allocator,
        .{ .allocator = allocator, .preferred_backends = &.{.native} },
        0x1234,
    );
    defer manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 64 });
    try manager.ensureResourceOwnerReady();

    var first_probe = TeardownTestProbe{};
    const first = try teardownTestModel(&manager, &first_probe);
    first.runtime_id = manager.nextRuntimeIdLocked();
    first.last_used_ns = 123;
    try manager.loaded.put(allocator, try allocator.dupe(u8, "primary-first"), first);

    // Publication precedes signaling the shared flight. The snapshot must not
    // briefly report both states for the same incarnation in that window.
    const completing_flight = try allocator.create(LoadFlight);
    completing_flight.* = .{
        .runtime_id = first.runtime_id,
        .model_dir = try allocator.dupe(u8, first.model_dir),
        .io = std.testing.io,
    };
    const completing_key = try allocator.dupe(u8, "completing-runtime-flight");
    try manager.in_flight_loads.put(allocator, completing_key, completing_flight);
    var completing_flight_owned = true;
    defer if (completing_flight_owned) {
        const removed = manager.in_flight_loads.fetchRemove("completing-runtime-flight").?;
        allocator.free(removed.key);
        manager.destroyLoadFlight(completing_flight);
    };

    const SnapshotClassifier = struct {
        fn classify(_: *const manifest_mod.ModelManifest) u16 {
            return 0x12a;
        }
    };
    var snapshot = try manager.runtimeSnapshot(allocator, SnapshotClassifier.classify);
    try std.testing.expectEqual(@as(usize, 1), snapshot.entries.len);
    try std.testing.expectEqual(first.runtime_id, snapshot.entries[0].runtime_id);
    try std.testing.expectEqual(RuntimeState.loaded, snapshot.entries[0].state);
    try std.testing.expectEqual(@as(u16, 0x12a), snapshot.entries[0].task_mask);
    try std.testing.expectEqualStrings("teardown-fixture", snapshot.entries[0].model_dir);
    snapshot.deinit();
    const removed_flight = manager.in_flight_loads.fetchRemove("completing-runtime-flight").?;
    allocator.free(removed_flight.key);
    manager.destroyLoadFlight(completing_flight);
    completing_flight_owned = false;
    try std.testing.expectEqual(@as(usize, 0), first.active_handles);
    try std.testing.expectEqual(@as(u64, 123), first.last_used_ns);

    const composite_cases = [_]manifest_mod.ModelType{ .rewriter, .extractor };
    for (composite_cases, 0..) |model_type, index| {
        const flight = try allocator.create(CompositeAssetsLoadFlight);
        flight.* = .{
            .runtime_id = first.runtime_id + @as(RuntimeId, @intCast(index + 1)),
            .model_dir = try allocator.dupe(u8, first.model_dir),
            .kind = .seq2seq,
            .model_type = model_type,
            .io = std.testing.io,
            .load_state = .{ .io = std.testing.io },
        };
        const key: ComponentPlanKey = @splat(@intCast(index + 1));
        try manager.in_flight_composite_assets.put(allocator, key, flight);
        defer {
            _ = manager.in_flight_composite_assets.remove(key);
            manager.destroyCompositeAssetsLoadFlight(flight);
        }
        var composite_snapshot = try manager.runtimeSnapshot(allocator, SnapshotClassifier.classify);
        defer composite_snapshot.deinit();
        var found = false;
        for (composite_snapshot.entries) |entry| {
            if (entry.runtime_id != flight.runtime_id) continue;
            try std.testing.expectEqual(RuntimeKind.composite, entry.runtime_kind);
            try std.testing.expectEqual(model_type, entry.model_type.?);
            found = true;
        }
        try std.testing.expect(found);
    }

    first.active_handles = 1;
    try std.testing.expectEqual(UnloadRuntimeResult{ .in_use = 1 }, manager.unloadRuntime(first.runtime_id));
    first.active_handles = 0;
    first.pinned = true;
    try std.testing.expectEqual(UnloadRuntimeResult.startup_managed, manager.unloadRuntime(first.runtime_id));
    first.pinned = false;
    const stale_runtime_id = first.runtime_id;
    try std.testing.expectEqual(UnloadRuntimeResult.unloaded, manager.unloadRuntime(stale_runtime_id));
    try std.testing.expect(first_probe.closed);

    var replacement_probe = TeardownTestProbe{};
    const replacement = try teardownTestModel(&manager, &replacement_probe);
    replacement.runtime_id = manager.nextRuntimeIdLocked();
    try manager.loaded.put(allocator, try allocator.dupe(u8, "primary-replacement"), replacement);
    try std.testing.expectEqual(UnloadRuntimeResult.already_unloaded, manager.unloadRuntime(stale_runtime_id));
    try std.testing.expectEqual(@as(usize, 1), manager.loaded.count());
    try std.testing.expectEqual(UnloadRuntimeResult.unloaded, manager.unloadRuntime(replacement.runtime_id));
    try std.testing.expect(replacement_probe.closed);
}

test "exact Whisper composite unload honors active and startup guards" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.initWithRuntimeEpoch(
        allocator,
        .{ .allocator = allocator, .preferred_backends = &.{.native} },
        0x5678,
    );
    defer manager.deinit();

    var tokenizer = ManagedHfTokenizer{ .tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator,
        \\{"version":"1.0","model":{"type":"BPE","vocab":{"<unk>":0},"merges":[]}}
    ) };
    defer tokenizer.deinit();
    var encoder_probe = TeardownTestProbe{};
    var encoder = ManagedSession{ .session = try encoder_probe.session(&manager) };
    defer encoder.deinit();
    var decoder_probe = TeardownTestProbe{};
    var decoder = ManagedSession{ .session = try decoder_probe.session(&manager) };
    defer decoder.deinit();
    const assets = try allocator.create(CompositeAssets);
    assets.* = .{
        .runtime_id = manager.nextRuntimeIdLocked(),
        .allocator = allocator,
        .model_dir = try allocator.dupe(u8, "/models/transcribers/whisper-test"),
        .model_dir_owned = true,
        .managed_tokenizer = tokenizer.take(),
        .prompt_cache = null,
        .encoder = encoder.take(),
        .decoder = decoder.take(),
        .kind = .whisper,
        .decoder_config = .{},
        .generation = @splat(0),
        .active_handles = 1,
        .last_used_ns = 456,
    };
    encoder_probe.peer_ticket = decoder_probe.ticket;
    decoder_probe.peer_ticket = encoder_probe.ticket;
    try manager.composite_assets.put(allocator, @splat(7), assets);

    var snapshot = try manager.runtimeSnapshot(allocator, null);
    try std.testing.expectEqual(@as(usize, 1), snapshot.entries.len);
    try std.testing.expectEqual(RuntimeKind.composite, snapshot.entries[0].runtime_kind);
    try std.testing.expectEqual(RuntimeState.in_use, snapshot.entries[0].state);
    snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 1), assets.active_handles);
    try std.testing.expectEqual(@as(u64, 456), assets.last_used_ns);
    try std.testing.expectEqual(UnloadRuntimeResult{ .in_use = 1 }, manager.unloadRuntime(assets.runtime_id));
    assets.active_handles = 0;
    assets.startup_managed = true;
    try std.testing.expectEqual(UnloadRuntimeResult.startup_managed, manager.unloadRuntime(assets.runtime_id));
    manager.lockLoadedModels();
    const startup_eviction = manager.takeLruCompositeAssetsLocked(platform.time.monotonicNs(), false);
    manager.unlockLoadedModels();
    try std.testing.expect(startup_eviction == null);
    assets.startup_managed = false;
    try std.testing.expectEqual(UnloadRuntimeResult.unloaded, manager.unloadRuntime(assets.runtime_id));
    try std.testing.expect(encoder_probe.closed and decoder_probe.closed);
}

const TeardownCacheProbe = struct {
    primary: *TeardownTestProbe,
    optional: *TeardownTestProbe,
    destroyed: bool = false,
    block: bool = false,

    fn destroy(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.destroyed or self.primary.closed or self.optional.closed)
            @panic("teardown fixture cache destroyed after physical session close");
        for ([_]*TeardownTestProbe{ self.primary, self.optional }) |session| {
            const ticket = session.ticket.?;
            if (ticket.deadline_ns.load(.acquire) == 0 or ticket.refs.load(.acquire) < 2)
                @panic("teardown fixture cache has no active aggregate protection");
        }
        if (self.primary.controller.?.snapshot().host_weight_bytes != 64)
            @panic("teardown fixture released admission before cached executor close");
        if (self.block) {
            std.debug.print("teardown-fixture cache-entered primary-and-optional-active lease-held64\n", .{});
            while (true) std.atomic.spinLoopHint();
        }
        self.destroyed = true;
    }

    fn attach(self: *@This(), model: *LoadedModel) !void {
        const key = graph_mod.cache.CacheKey{ .config_hash = 1, .batch = 1, .seq_len = 1, .attention_mode = .paged_decode };
        var graph = ml.graph.Graph.init(model.allocator);
        errdefer graph.deinit();
        try model.native_generation_graph_cache.put(key, graph);
        model.native_generation_graph_cache.getEntry(key).?.compiled_model_executor = .{ .ptr = self, .deinit = destroy };
    }
};

test "model manager teardown starts both tickets before cached executor destruction" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 64 });
    try manager.ensureResourceOwnerReady();
    var primary = TeardownTestProbe{};
    const model = try teardownTestModel(&manager, &primary);
    var model_owned = true;
    defer if (model_owned) manager.destroyLoadedModel(model);
    var optional = TeardownTestProbe{};
    model.vision_session = try optional.session(&manager);
    var cached = TeardownCacheProbe{ .primary = &primary, .optional = &optional };
    try cached.attach(model);
    try std.testing.expectEqual(@as(u64, 0), primary.ticket.?.deadline_ns.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), optional.ticket.?.deadline_ns.load(.acquire));
    // No externally entered scope: only LoadedModel's production deinit can
    // activate both tickets before the cached executor's destructor runs.
    manager.destroyLoadedModel(model);
    model_owned = false;
    try std.testing.expect(cached.destroyed and primary.closed and optional.closed);
    try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
    try std.testing.expectEqual(@as(usize, 0), manager.teardown_domain.?.watchdog.entries.items.len);
}

test "model manager teardown composite keeps both peer tickets through ordered session cleanup" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    var tokenizer = ManagedHfTokenizer{ .tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator,
        \\{"version":"1.0","model":{"type":"BPE","vocab":{"<unk>":0},"merges":[]}}
    ) };
    defer tokenizer.deinit();
    var encoder_probe = TeardownTestProbe{};
    var encoder = ManagedSession{ .session = try encoder_probe.session(&manager) };
    defer encoder.deinit();
    var decoder_probe = TeardownTestProbe{};
    var decoder = ManagedSession{ .session = try decoder_probe.session(&manager) };
    defer decoder.deinit();
    var assets = CompositeAssets{
        .managed_tokenizer = tokenizer.take(),
        .prompt_cache = null,
        .encoder = encoder.take(),
        .decoder = decoder.take(),
        .kind = .seq2seq,
        .decoder_config = .{},
        .generation = @splat(0),
    };
    encoder_probe.peer_ticket = decoder_probe.ticket;
    decoder_probe.peer_ticket = encoder_probe.ticket;
    try std.testing.expectEqual(@as(u64, 0), encoder_probe.ticket.?.deadline_ns.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), decoder_probe.ticket.?.deadline_ns.load(.acquire));
    assets.deinit();
    try std.testing.expect(encoder_probe.closed and decoder_probe.closed);
    try std.testing.expectEqual(@as(usize, 0), manager.teardown_domain.?.watchdog.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), manager.teardown_domain.?.refs.load(.acquire));
}

test "model manager teardown dormant ticket covers nested close without allocation or borrowed request" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 64 });
    try manager.ensureResourceOwnerReady();
    var probe = TeardownTestProbe{};
    const model = try teardownTestModel(&manager, &probe);
    var optional_probe = TeardownTestProbe{};
    model.vision_session = try optional_probe.session(&manager);
    probe.peer_ticket = optional_probe.ticket;
    const ticket = probe.ticket.?;
    try std.testing.expectEqual(@as(u64, 0), ticket.deadline_ns.load(.acquire));
    try std.testing.expect(manager.owned_load_watchdog == null);
    try std.testing.expectEqual(@as(usize, 2), manager.teardown_domain.?.watchdog.entries.items.len);

    // beginClose cannot allocate even when every future allocation is denied.
    // The aggregate scope must preserve the ticket after Session.close drops
    // its owning reference, until dependent-cache cleanup has also returned.
    var scope = model.session.beginClose();
    const deadline = ticket.deadline_ns.load(.acquire);
    const domain = manager.teardown_domain.?;
    var failure = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    const original_allocator = domain.watchdog.allocator;
    domain.watchdog.allocator = failure.allocator();
    manager.destroyLoadedModel(model);
    domain.watchdog.allocator = original_allocator;
    try std.testing.expect(probe.closed and optional_probe.closed);
    try std.testing.expectEqual(deadline, ticket.deadline_ns.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), failure.alloc_index);
    try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
    try std.testing.expectEqual(@as(usize, 1), domain.watchdog.entries.items.len);
    scope.deinit();
    try std.testing.expectEqual(@as(usize, 0), domain.watchdog.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), domain.refs.load(.acquire));
}

test "model manager teardown disarms driver protection before host tokenizer destruction" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 64 });
    try manager.ensureResourceOwnerReady();
    var probe = TeardownTestProbe{};
    const model = try teardownTestModel(&manager, &probe);
    const Observer = struct {
        domain: *TeardownDomain,
        checking: bool = false,
        frees: usize = 0,
        fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            return std.testing.allocator.rawAlloc(len, alignment, ret);
        }
        fn resize(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) bool {
            return std.testing.allocator.rawResize(memory, alignment, len, ret);
        }
        fn remap(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) ?[*]u8 {
            return std.testing.allocator.rawRemap(memory, alignment, len, ret);
        }
        fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.checking) {
                if (self.domain.watchdog.entries.items.len != 0)
                    @panic("driver teardown protection extends into host tokenizer destruction");
                self.frees += 1;
            }
            std.testing.allocator.rawFree(memory, alignment, ret);
        }
        const vtable = std.mem.Allocator.VTable{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
    };
    var observer = Observer{ .domain = manager.teardown_domain.? };
    model.hf_tok = try hf_tokenizer.HfTokenizer.loadFromBytes(.{ .ptr = &observer, .vtable = &Observer.vtable },
        \\{"version":"1.0","model":{"type":"BPE","vocab":{"<unk>":0},"merges":[]}}
    );
    observer.checking = true;
    manager.destroyLoadedModel(model);
    try std.testing.expect(probe.closed);
    try std.testing.expect(observer.frees != 0);
    try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
}

test "model manager teardown raw session retains monitor and offline driver IO after manager" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    var alive = true;
    defer if (alive) manager.deinit();
    var probe = TeardownTestProbe{};
    var managed = ManagedSession{ .session = try probe.session(&manager) };
    const escaped = managed.disownSession();
    managed.deinit();
    const domain = manager.teardown_domain.?;
    const driver = domain.driver_io.?;
    try std.testing.expectEqual(@as(usize, 2), driver.refs.load(.acquire));
    manager.deinit();
    alive = false;
    try std.testing.expectEqual(@as(usize, 1), domain.refs.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), driver.refs.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), probe.ticket.?.deadline_ns.load(.acquire));
    escaped.close();
    try std.testing.expect(probe.closed);
}

test "model manager teardown rejects missing isolation and failed ticket before backend entry" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native}, .process_termination_available = false });
    defer manager.deinit();
    try std.testing.expectError(error.ProcessIsolationRequired, manager.prepareSessionClose(.{ .backend = .metal }));
    try std.testing.expect(manager.teardown_domain == null and manager.owned_load_runtime == null);
    try std.testing.expect(try manager.prepareSessionClose(.{ .backend = .native }) == null);

    manager.session_manager.process_termination_available = true;
    _ = try manager.loadCoordinationIoLocked();
    const domain = try TeardownDomain.create(allocator, manager.owned_load_io_owner);
    manager.teardown_domain = domain;
    for (0..2) |fail_index| {
        // Failure 0 is ticket allocation; failure 1 is watchdog entry arm.
        var failure = std.testing.FailingAllocator.init(allocator, .{ .fail_index = fail_index });
        domain.allocator = failure.allocator();
        domain.watchdog.allocator = failure.allocator();
        var probe = TeardownTestProbe{};
        const result = probe.session(&manager);
        domain.allocator = allocator;
        domain.watchdog.allocator = allocator;
        try std.testing.expectError(error.OutOfMemory, result);
        try std.testing.expect(probe.ticket == null and !probe.closed);
        try std.testing.expectEqual(@as(usize, 0), probe.constructor_entries);
        try std.testing.expectEqual(@as(usize, 0), domain.watchdog.entries.items.len);
        try std.testing.expectEqual(@as(usize, 1), domain.refs.load(.acquire));
    }
}

// The process-level runner selects only this test, once per fresh child. Its
// deadline is private test data, never a production option or environment knob.
test "model manager teardown supervised child fixture" {
    if (!builtin.is_test) unreachable;
    const mode = if (std.c.getenv("ANTFLY_TEST_MANAGER_TEARDOWN_CHILD")) |raw| std.mem.span(raw) else return error.SkipZigTest;
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    var manager_alive = true;
    defer if (manager_alive) manager.deinit();
    var probe = TeardownTestProbe{ .block = true };
    const stderr_close = std.mem.eql(u8, mode, "stderr-close");
    const stderr_return = std.mem.eql(u8, mode, "stderr-return");
    if (stderr_close or stderr_return) {
        manager.configureAdmissionLimits(.{ .host_limit_bytes = 64 });
        try manager.ensureResourceOwnerReady();
        var lease: ?runtime.tier.memory.AdmissionLease = try manager.acquireAmountsWithEviction(.cpu, .{ .host_limit_bytes = 64 }, .{ .host_weight_bytes = 64 });
        defer if (lease) |*owned| owned.release();
        probe.controller = manager.admissionController();
        probe.block = false;
        var managed = ManagedSession{ .session = try probe.session(&manager), .resource_lease = lease };
        lease = null;
        defer managed.deinit();
        probe.ticket.?.close_timeout_ns = 100 * std.time.ns_per_ms;
        var blocker = TeardownStderrBlocker{};
        defer blocker.deinit();
        std.debug.print("teardown-fixture operation-start:{s}\n", .{mode});
        if (stderr_return) {
            // Isolate the final-release deadline check: the polling watcher
            // cannot run until this exact check exits. Its mutex is test-only
            // coordination, not a production clock or watchdog override.
            const watchdog = manager.teardown_domain.?.watchdog;
            spinLock(&watchdog.mutex);
            defer watchdog.mutex.unlock();
            std.debug.print("teardown-fixture final-check-isolated\n", .{});
            try blocker.start();
            probe.stderr_locked = true;
            probe.wait_until_deadline = true;
            managed.deinit();
        } else {
            try blocker.start();
            probe.stderr_locked = true;
            probe.block = true;
            managed.deinit();
        }
        try std.Io.File.stdout().writeStreamingAll(std.testing.io, "teardown-fixture lease-released\n");
        return error.ExpectedTeardownWatchdogExit;
    } else if (std.mem.eql(u8, mode, "escaped")) {
        var managed = ManagedSession{ .session = try probe.session(&manager) };
        probe.ticket.?.close_timeout_ns = 100 * std.time.ns_per_ms;
        const session = managed.disownSession();
        managed.deinit();
        manager.deinit();
        manager_alive = false;
        std.debug.print("teardown-fixture operation-start:{s}\n", .{mode});
        session.close();
    } else {
        manager.configureAdmissionLimits(.{ .host_limit_bytes = 64 });
        try manager.ensureResourceOwnerReady();
        manager.configureModelCache(1, 1);
        const model = try teardownTestModel(&manager, &probe);
        probe.ticket.?.close_timeout_ns = 100 * std.time.ns_per_ms;
        if (std.mem.eql(u8, mode, "cache")) {
            // Exercise the existing compiled-executor destructor before any
            // Session.close call; neither session nor test arms an outer scope.
            probe.block = false;
            var optional = TeardownTestProbe{};
            model.vision_session = try optional.session(&manager);
            optional.ticket.?.close_timeout_ns = 100 * std.time.ns_per_ms;
            var cached = TeardownCacheProbe{ .primary = &probe, .optional = &optional, .block = true };
            try cached.attach(model);
            var handle = try manager.publishLoadedModel(model, true, null, null, 0);
            handle.release();
            std.debug.print("teardown-fixture operation-start:{s}\n", .{mode});
            manager.evictExpiredAt(model.last_used_ns + 2 * std.time.ns_per_ms);
        } else if (std.mem.eql(u8, mode, "rollback")) {
            std.debug.print("teardown-fixture operation-start:{s}\n", .{mode});
            manager.destroyLoadedModel(model);
        } else {
            var handle = try manager.publishLoadedModel(model, true, null, null, 0);
            std.debug.print("teardown-fixture operation-start:{s}\n", .{mode});
            if (std.mem.eql(u8, mode, "retired")) {
                manager.retireLoadedModel(model);
                handle.release();
            } else {
                handle.release();
                if (std.mem.eql(u8, mode, "ttl")) {
                    manager.evictExpiredAt(model.last_used_ns + 2 * std.time.ns_per_ms);
                } else if (std.mem.eql(u8, mode, "admission")) {
                    var unexpected = manager.acquireAmountsWithEviction(.cpu, .{ .host_limit_bytes = 64 }, .{ .host_weight_bytes = 64 }) catch |err| {
                        std.debug.print("teardown-fixture operation-error:{s}\n", .{@errorName(err)});
                        return err;
                    };
                    unexpected.release();
                } else if (std.mem.eql(u8, mode, "shutdown")) {
                    manager.deinit();
                    manager_alive = false;
                } else return error.InvalidTeardownFixtureMode;
            }
        }
    }
    std.debug.print("teardown-fixture cleanup-returned\n", .{});
    return error.ExpectedTeardownWatchdogExit;
}

test "GGUF Unigram tokenizer consumes its embedded normalization map" {
    const allocator = std.testing.allocator;
    const bytes = try buildTestGgufWithT5Tokenizer(allocator);
    defer allocator.free(bytes);
    var parsed = try gguf_format.parse(allocator, bytes);
    defer parsed.deinit(allocator);
    // Minimal Darts map H -> h; the fixture's vocabulary contains 'hello'.
    var map: [4 + 256 * 4 + 2]u8 = @splat(0);
    std.mem.writeInt(u32, map[0..4], 256 * 4, .little);
    std.mem.writeInt(u32, map[4..8], 1 << 10, .little);
    std.mem.writeInt(u32, map[4 + 73 * 4 ..][0..4], 'H' | (1 << 8) | ((73 ^ 2) << 10), .little);
    std.mem.writeInt(u32, map[4 + 2 * 4 ..][0..4], 0x80000000, .little);
    map[4 + 256 * 4] = 'h';
    const map_values = try allocator.alloc(gguf_format.MetadataValue, map.len);
    defer allocator.free(map_values);
    for (map, map_values) |byte, *value| value.* = .{ .u8 = byte };
    const original = parsed.metadata;
    const metadata = try allocator.alloc(gguf_format.MetadataEntry, original.len + 1);
    defer allocator.free(metadata);
    @memcpy(metadata[0..original.len], original);
    metadata[original.len] = .{ .key = "tokenizer.ggml.precompiled_charsmap", .value = .{ .array = .{ .element_type = .u8, .values = map_values } } };
    parsed.metadata = metadata;
    defer parsed.metadata = original;
    const tokenizer_json = try unigramTokenizerJsonFromGguf(allocator, &parsed);
    defer allocator.free(tokenizer_json);
    const tok = try hf_tokenizer.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
    defer tok.deinitSelf();
    const ids = try tok.tokenizer().encode(allocator, "Hello world");
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i32, &.{ 4, 5 }, ids);
}

test "ONNX WordPiece loading and admission use export artifacts with root fallback" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |export_vocab| {
        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();
        try dir.dir.createDirPath(io, "onnx");
        const paths = [_][]const u8{ "onnx/model.onnx", "onnx/config.json", if (export_vocab) "onnx/vocab.txt" else "vocab.txt", "tokenizer_config.json" };
        const bodies = [_][]const u8{ "onnx", "{\"model_type\":\"bert\",\"hidden_size\":8}", "[PAD]\n[UNK]\n[CLS]\n[SEP]\n[MASK]\nhello\n", "{\"do_lower_case\":true}" };
        var artifacts: [paths.len]managed_receipt.ArtifactReceipt = undefined;
        for (paths, bodies, 0..) |path, body, i| {
            try dir.dir.writeFile(io, .{ .sub_path = path, .data = body });
            artifacts[i] = .{ .path = path, .size = body.len };
        }
        // An unreceipted JSON file must not shadow the selected vocabulary.
        try dir.dir.writeFile(io, .{ .sub_path = "onnx/tokenizer.json", .data = "not a tokenizer" });
        const receipt = try std.json.Stringify.valueAlloc(a, managed_receipt.DownloadReceipt{
            .version = 2,
            .source = .{ .owner = "owner", .name = "model", .variant = "onnx", .selected_format = "onnx" },
            .artifacts = &artifacts,
        }, .{});
        defer a.free(receipt);
        try dir.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
        const root = try dir.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        var man = try manifest_mod.loadFromDir(a, root);
        defer man.deinit();
        try std.testing.expectEqual(.huggingface, man.tokenizer_type);
        try std.testing.expect(man.tokenizer_json_path == null);
        try std.testing.expect(std.mem.endsWith(u8, man.vocab_txt_path.?, paths[2]));
        const expected_plan = try tokenizerAdmissionPlan(bodies[2].len + bodies[3].len, .wordpiece);
        const actual_plan = try tokenizerLoadAdmissionPlan(a, root, &man, .huggingface);
        try std.testing.expectEqualDeep(expected_plan, actual_plan);
        const tok = try loadHuggingFaceTokenizerFromManifest(a, &man);
        defer tok.deinitSelf();
        var encoded = try tok.tokenizer().encodeForModel(a, "HELLO", 3);
        defer encoded.deinit();
        try std.testing.expectEqualSlices(i32, &.{ 2, 5, 3 }, encoded.ids);
    }
}

test "ONNX export tokenizer format takes precedence over root JSON" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |export_json| {
        var dir = std.testing.tmpDir(.{});
        defer dir.cleanup();
        try dir.dir.createDirPath(io, "onnx");
        const paths = [_][]const u8{ "onnx/model.onnx", "onnx/config.json", "onnx/vocab.txt", "tokenizer_config.json", "tokenizer.json" };
        const bodies = [_][]const u8{ "onnx", "{\"model_type\":\"bert\",\"hidden_size\":8}", "[PAD]\n[UNK]\n[CLS]\n[SEP]\n[MASK]\nhello\n", "{\"do_lower_case\":true}", "{\"model\":{\"type\":\"WordPiece\",\"unk_token\":\"[UNK]\",\"vocab\":{\"[UNK]\":0,\"hello\":1}}}" };
        var artifacts: [paths.len + 1]managed_receipt.ArtifactReceipt = undefined;
        for (paths, bodies, 0..) |path, body, i| {
            try dir.dir.writeFile(io, .{ .sub_path = path, .data = body });
            artifacts[i] = .{ .path = path, .size = body.len };
        }
        const local_json = "{\"model\":{\"type\":\"WordPiece\",\"unk_token\":\"[UNK]\",\"vocab\":{\"[UNK]\":0,\"hello\":7}}}";
        // An unreceipted JSON file must not shadow the export vocabulary.
        try dir.dir.writeFile(io, .{ .sub_path = "onnx/tokenizer.json", .data = if (export_json) local_json else "not a tokenizer" });
        artifacts[paths.len] = .{ .path = "onnx/tokenizer.json", .size = local_json.len };
        const receipt = try std.json.Stringify.valueAlloc(a, managed_receipt.DownloadReceipt{
            .version = 2,
            .source = .{ .owner = "owner", .name = "model", .variant = "onnx", .selected_format = "onnx" },
            .artifacts = artifacts[0 .. paths.len + @intFromBool(export_json)],
        }, .{});
        defer a.free(receipt);
        try dir.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
        const root = try dir.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        var man = try manifest_mod.loadFromDir(a, root);
        defer man.deinit();
        try std.testing.expectEqual(.huggingface, man.tokenizer_type);
        var listing = try manifest_mod.loadListingFromDir(a, root);
        defer listing.deinit();
        try std.testing.expectEqual(export_json, man.tokenizer_json_path != null);
        try std.testing.expectEqual(export_json, listing.tokenizer_json_path != null);
        const expected_plan = if (export_json)
            try tokenizerAdmissionPlan(local_json.len, .huggingface)
        else
            try tokenizerAdmissionPlan(bodies[2].len + bodies[3].len, .wordpiece);
        try std.testing.expectEqualDeep(expected_plan, try tokenizerLoadAdmissionPlan(a, root, &man, .huggingface));
        const tok = try loadHuggingFaceTokenizerFromManifest(a, &man);
        defer tok.deinitSelf();
        const ids = try tok.tokenizer().encode(a, "hello");
        defer a.free(ids);
        try std.testing.expectEqualSlices(i32, if (export_json) &.{7} else &.{5}, ids);
    }
}

test "embeddinggemma2 preflight shares host capacity before model load and preserves denied leases" {
    const a = std.testing.allocator;
    var manager = ModelManager.init(a, backends.SessionManager.init(a));
    defer manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 128, .scratch_limit_bytes = 128 });
    var first = try manager.admitHostPreflightChecked(128, false);
    defer first.release();
    try std.testing.expectEqual(@as(usize, 128), manager.resource_domain.?.admission.snapshot().host_scratch_bytes);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, manager.admitHostPreflightChecked(1, false));
    try std.testing.expectEqual(@as(usize, 128), manager.resource_domain.?.admission.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), manager.loaded.count());
    first.release();
    var retry = try manager.admitHostPreflightChecked(128, false);
    retry.release();
    try std.testing.expectEqual(@as(usize, 0), manager.resource_domain.?.admission.snapshot().host_scratch_bytes);
}

test "capi resolver model identity uses receipt digests before load and survives copying" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var first = std.testing.tmpDir(.{});
    defer first.cleanup();
    var second = std.testing.tmpDir(.{});
    defer second.cleanup();
    const body = "identical fake model weights";
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    const receipt = try std.json.Stringify.valueAlloc(a, managed_receipt.DownloadReceipt{ .version = 2, .source = .{ .owner = "test", .name = "identity", .variant = "safetensors", .selected_format = "safetensors" }, .artifacts = &.{.{ .path = "model.safetensors", .size = body.len, .sha256 = &hex }} }, .{});
    defer a.free(receipt);
    for ([_]std.Io.Dir{ first.dir, second.dir }) |dir| {
        try dir.writeFile(io, .{ .sub_path = "model.safetensors", .data = body });
        try dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
    }
    const first_path = try first.dir.realPathFileAlloc(io, ".", a);
    defer a.free(first_path);
    const second_path = try second.dir.realPathFileAlloc(io, ".", a);
    defer a.free(second_path);
    const manifest: manifest_mod.ModelManifest = .{ .allocator = a };
    const original = try modelIdentity(a, io, first_path, manifest);
    const copied = try modelIdentity(a, io, second_path, manifest);
    try std.testing.expectEqualSlices(u8, &original, &copied);
    LegacyArtifactDigestCache.entries = @splat(null); // a reopened owner/process
    LegacyArtifactDigestCache.forbid_hashing_for_test = true;
    defer LegacyArtifactDigestCache.forbid_hashing_for_test = false;
    try std.testing.expectEqualSlices(u8, &original, &try modelIdentity(a, io, second_path, manifest));
    LegacyArtifactDigestCache.forbid_hashing_for_test = false;
    const legacy = try std.json.Stringify.valueAlloc(a, managed_receipt.DownloadReceipt{ .artifacts = &.{.{ .path = "model.safetensors", .size = body.len }} }, .{});
    defer a.free(legacy);
    try second.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = legacy });
    const legacy_identity = try modelIdentity(a, io, second_path, manifest);
    try std.testing.expectEqualSlices(u8, &original, &legacy_identity);
    try std.testing.expectEqualSlices(u8, &legacy_identity, &try modelIdentity(a, io, second_path, manifest));
    // Invalid weight bytes deliberately prove identity lookup does not load a model.
    const changed = try std.mem.replaceOwned(u8, a, receipt, &hex, "0000000000000000000000000000000000000000000000000000000000000000");
    defer a.free(changed);
    try second.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = changed });
    try std.testing.expectError(error.ChecksumMismatch, modelIdentity(a, io, second_path, manifest));
    // A stale pinned receipt must also reject in-place weight replacement.
    try second.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
    try second.dir.writeFile(io, .{ .sub_path = "model.safetensors", .data = "different fake model weights" });
    try std.testing.expectError(error.ChecksumMismatch, modelIdentity(a, io, second_path, manifest));
}

test "embeddinggemma2 generation metadata clones survive retirement and release pins on OOM" {
    const a = std.testing.allocator;
    var manager = ModelManager.init(a, .{ .allocator = a, .preferred_backends = &.{.metal} });
    defer manager.deinit();
    try manager.ensureResourceOwnerReady();
    var probe = TeardownTestProbe{};
    const model = try teardownTestModel(&manager, &probe);
    model.manifest.embedding_style = .embedding_gemma2;
    model.manifest.hidden_size = 512;
    model.manifest.embedding_profile.query.prefix = try a.dupe(u8, "task: search result | query: ");
    var inference = try manager.publishLoadedModel(model, true, null, null, 0);
    defer inference.release();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, manager.copyCachedEmbeddingGemma2Manifest(failing.allocator(), "teardown-fixture"));
    try std.testing.expectEqual(@as(usize, 1), model.active_handles);
    var copy = (try manager.copyCachedEmbeddingGemma2Manifest(a, "teardown-fixture")).?;
    defer copy.deinit();
    try std.testing.expectEqual(@as(usize, 1), model.active_handles);
    inference.retire();
    try std.testing.expect(probe.closed);
    try std.testing.expectEqual(@as(u32, 512), copy.hidden_size);
    try std.testing.expectEqualStrings("task: search result | query: ", copy.queryPrefix());
    try std.testing.expect((try manager.copyCachedEmbeddingGemma2Manifest(a, "teardown-fixture")) == null);
}

test "memory admission diagnostics retain pressure and recover after release" {
    const memory = runtime.tier.memory;
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();
    const limits = memory.Limits{ .host_limit_bytes = 100 };
    var held = try manager.acquireAmountsWithEviction(.cpu, limits, .{ .host_scratch_bytes = 80 });
    defer held.release();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, manager.acquireAmountsWithEviction(.cpu, limits, .{ .host_scratch_bytes = 30 }));
    const denied = manager.memoryAdmissionDiagnostic();
    try std.testing.expectEqualStrings("domain_host", denied.pressure);
    try std.testing.expectEqual(@as(u64, 1), denied.denied_count);
    try std.testing.expectEqual(@as(u64, 30), denied.requested_host_bytes);
    try std.testing.expectEqual(@as(u64, 80), denied.admitted_host_bytes);
    try std.testing.expectEqual(@as(u64, 100), denied.host_limit_bytes);
    for (0..64) |_| {
        try std.testing.expectError(error.ResourceTemporarilyUnavailable, manager.acquireAmountsWithEviction(.cpu, limits, .{ .host_scratch_bytes = 30 }));
    }
    try std.testing.expectEqual(@as(u64, 65), manager.memoryAdmissionDiagnostic().denied_count);
    held.release();
    var recovered = try manager.acquireAmountsWithEviction(.cpu, limits, .{ .host_scratch_bytes = 30 });
    recovered.release();
    try std.testing.expectEqual(@as(usize, 0), manager.admissionController().snapshot().hostTotalBytes());
    // The last denial remains observable after recovery; its timestamp/count
    // distinguish it from the current request and survive DB snapshot refresh.
    try std.testing.expectEqual(@as(u64, 65), manager.memoryAdmissionDiagnostic().denied_count);
}

/// Planning does not construct a device. Execution validates its actual identity
/// after selection, including when a preferred accelerator falls back to CPU.
const embeddingPlanningBackend = loaded_runtime.embeddingPlanningBackend;

test "embeddinggemma2 planning precision follows required backend before preferences" {
    var manager = backends.SessionManager.init(std.testing.allocator);
    manager.preferred_backends = &.{ .cuda, .native };
    try std.testing.expectEqual(if (backends.BackendType.cuda.available()) backends.BackendType.cuda else backends.BackendType.native, embeddingPlanningBackend(manager));
    manager.required_backend = .metal;
    try std.testing.expectEqual(backends.BackendType.metal, embeddingPlanningBackend(manager));
    manager.required_backend = .native;
    try std.testing.expectEqual(backends.BackendType.native, embeddingPlanningBackend(manager));
}

test "chunk planning survives metadata-only re-pulls and rejects changed tokenizer bytes" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = "original" });
    const root = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", &tmp.sub_path });
    defer a.free(root);
    const path = try std.fs.path.join(a, &.{ root, "tokenizer.json" });
    defer a.free(path);
    const manifest = manifest_mod.ModelManifest{ .allocator = a, .tokenizer_json_path = path };
    const snapshot = try chunkPlanningSnapshot(a, io, manifest, @splat(0));
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
    defer manager.deinit();
    // Only planning metadata is exercised; no tokenizer/backend is loaded.
    var model: LoadedModel = undefined;
    model.managed_publication = null;
    model.allocator = a;
    model.model_dir = root;
    model.manifest = manifest;
    model.executor_io = io;
    model.chunk_planning = snapshot;
    model.resolution_revision = @splat(0);
    model.embedding_identity_mutex = .unlocked;
    model.chunk_planning_lane = .{};
    model.host = manager.loadedRuntimeHost();
    model.retired = false;
    try std.Io.Dir.hardLink(tmp.dir, "tokenizer.json", tmp.dir, "linked.json", io, .{});
    try std.testing.expectEqual(snapshot.digest, try model.chunkPlanningIdentity());
    try tmp.dir.deleteFile(io, "linked.json");
    try std.testing.expectEqual(snapshot.digest, try model.chunkPlanningIdentity());
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = "changed!" });
    // Some Linux filesystems give successive same-size writes identical
    // mtime/ctime values within one clock tick. Exercise content revalidation
    // with an observable generation change without sleeping or making warm
    // planning reread tokenizer/model contents on every request.
    var changed = try tmp.dir.openFile(io, "tokenizer.json", .{ .mode = .read_write });
    defer changed.close(io);
    const stat = try changed.stat(io);
    try changed.setTimestamps(io, .{ .modify_timestamp = .{ .new = .fromNanoseconds(stat.mtime.toNanoseconds() + std.time.ns_per_s) } });
    try std.testing.expectError(error.ModelArtifactsChanging, model.chunkPlanningIdentity());
}

test "embedding identity validation without asset signatures remains cancellable and performs no discovery" {
    var model: LoadedModel = undefined;
    model.managed_publication = null;
    model.embedding_identity_mutex = .unlocked;
    model.embedding_validation = null;
    // Other fields deliberately have no valid allocator, paths or session:
    // models without an asset signature must stay on the metadata-free path.
    try model.verifyEmbeddingIdentityWithControl(.{});
    try std.testing.expectError(error.Timeout, model.verifyEmbeddingIdentityWithControl(.{ .deadline_ns = 0 }));
    try model.verifyEmbeddingIdentityWithControl(.{});
}

test "GGUF chunk planning verifies embedded tokenizer bytes while preserving unchanged identities" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // Planning checks only identity; it must not construct a GGUF backend.
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = "original" });
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = "{}" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const path = try std.fs.path.join(a, &.{ root, "model.gguf" });
    defer a.free(path);
    const config = try std.fs.path.join(a, &.{ root, "config.json" });
    defer a.free(config);
    const manifest = manifest_mod.ModelManifest{ .allocator = a, .gguf_path = path, .config_path = config };
    const snapshot = try chunkPlanningSnapshot(a, io, manifest, @splat(9));
    try std.testing.expect(snapshot.embedded_tokenizer_digest != null);
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
    defer manager.deinit();
    var model: LoadedModel = undefined;
    model.managed_publication = null;
    model.allocator = a;
    model.model_dir = root;
    model.manifest = manifest;
    model.executor_io = io;
    model.chunk_planning = snapshot;
    model.resolution_revision = @splat(9);
    model.embedding_identity_mutex = .unlocked;
    model.chunk_planning_lane = .{};
    model.host = manager.loadedRuntimeHost();
    model.retired = false;

    try std.Io.Dir.hardLink(tmp.dir, "model.gguf", tmp.dir, "linked.gguf", io, .{});
    try std.testing.expectEqual(snapshot.digest, try model.chunkPlanningIdentity());
    try tmp.dir.deleteFile(io, "linked.gguf");
    try std.testing.expectEqual(snapshot.digest, try model.chunkPlanningIdentity());
    // Neither warm calls nor a sidecar-only metadata change rehash weights.
    LegacyArtifactDigestCache.forbid_hashing_for_test = true;
    defer LegacyArtifactDigestCache.forbid_hashing_for_test = false;
    try std.testing.expectEqual(snapshot.digest, try model.chunkPlanningIdentity());
    try std.Io.Dir.hardLink(tmp.dir, "config.json", tmp.dir, "linked.json", io, .{});
    try std.testing.expectEqual(snapshot.digest, try model.chunkPlanningIdentity());
    LegacyArtifactDigestCache.forbid_hashing_for_test = false;
    // Atomic replacement with identical bytes preserves the old public key.
    try tmp.dir.writeFile(io, .{ .sub_path = "replacement.gguf", .data = "original" });
    try std.Io.Dir.rename(tmp.dir, "replacement.gguf", tmp.dir, "model.gguf", io);
    try std.testing.expectEqual(snapshot.digest, try model.chunkPlanningIdentity());
    const validated = model.chunk_planning.?;
    // Missing publication is transient and cannot advance validation evidence.
    try std.Io.Dir.rename(tmp.dir, "model.gguf", tmp.dir, "missing.gguf", io);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, model.chunkPlanningIdentity());
    try std.testing.expectEqualDeep(validated, model.chunk_planning.?);
    try std.Io.Dir.rename(tmp.dir, "missing.gguf", tmp.dir, "model.gguf", io);
    try std.testing.expectEqual(snapshot.digest, try model.chunkPlanningIdentity());
    const before_change = model.chunk_planning.?;
    // A same-size replacement must fail even though the old revision is fixed.
    try tmp.dir.writeFile(io, .{ .sub_path = "replacement.gguf", .data = "changed!" });
    try std.Io.Dir.rename(tmp.dir, "replacement.gguf", tmp.dir, "model.gguf", io);
    try std.testing.expectError(error.ModelArtifactsChanging, model.chunkPlanningIdentity());
    try std.testing.expectEqualDeep(before_change, model.chunk_planning.?);
    // The same guarantee applies to observable in-place generations. Coarse
    // filesystem timestamps can otherwise collapse both same-size writes into
    // one metadata signature. Advance from a fixed base, without sleeping or
    // requiring warm planning calls to hash the entire model.
    var changed = try tmp.dir.openFile(io, "model.gguf", .{ .mode = .read_write });
    defer changed.close(io);
    const base_mtime = (try changed.stat(io)).mtime.toNanoseconds();
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = "original" });
    try changed.setTimestamps(io, .{ .modify_timestamp = .{ .new = .fromNanoseconds(base_mtime + std.time.ns_per_s) } });
    try std.testing.expectEqual(snapshot.digest, try model.chunkPlanningIdentity());
    const before_write = model.chunk_planning.?;
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = "changed!" });
    try changed.setTimestamps(io, .{ .modify_timestamp = .{ .new = .fromNanoseconds(base_mtime + 2 * std.time.ns_per_s) } });
    try std.testing.expectError(error.ModelArtifactsChanging, model.chunkPlanningIdentity());
    try std.testing.expectEqualDeep(before_write, model.chunk_planning.?);
}

test "GGUF chunk planning cancellation leaves revalidation retryable" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const payload = try a.alloc(u8, 256 * 1024);
    defer a.free(payload);
    @memset(payload, 1);
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = payload });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const path = try std.fs.path.join(a, &.{ root, "model.gguf" });
    defer a.free(path);
    const manifest = manifest_mod.ModelManifest{ .allocator = a, .gguf_path = path };
    const snapshot = try chunkPlanningSnapshot(a, io, manifest, @splat(5));
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
    defer manager.deinit();
    var model: LoadedModel = undefined;
    model.managed_publication = null;
    model.allocator = a;
    model.model_dir = root;
    model.manifest = manifest;
    model.executor_io = io;
    model.chunk_planning = snapshot;
    model.resolution_revision = @splat(5);
    model.embedding_identity_mutex = .unlocked;
    model.chunk_planning_lane = .{};
    model.host = manager.loadedRuntimeHost();
    model.retired = false;
    try std.Io.Dir.hardLink(tmp.dir, "model.gguf", tmp.dir, "linked.gguf", io, .{});
    const Probe = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks >= 9) return error.Cancelled;
        }
    };
    var probe = Probe{};
    try std.testing.expectError(error.Cancelled, model.chunkPlanningIdentityWithControl(.{ .io = io, .ptr = &probe, .check_fn = Probe.check }));
    try std.testing.expectEqualDeep(snapshot, model.chunk_planning.?);
    try std.testing.expectError(error.Timeout, model.chunkPlanningIdentityWithControl(.{ .deadline_ns = 0 }));
    try std.testing.expectEqual(snapshot.digest, try model.chunkPlanningIdentityWithControl(.{ .io = io }));
}

test "warm generator admission metadata releases pins on OOM and survives retirement" {
    const a = std.testing.allocator;
    var manager = ModelManager.init(a, .{ .allocator = a, .preferred_backends = &.{.metal} });
    defer manager.deinit();
    try manager.ensureResourceOwnerReady();
    try std.testing.expect((try manager.copyCachedGenerationManifest(a, "not-loaded")) == null);
    var probe = TeardownTestProbe{};
    const model = try teardownTestModel(&manager, &probe);
    model.manifest.model_type = .generator;
    model.manifest.inputs = try a.alloc([]const u8, 1);
    model.manifest.inputs[0] = try a.dupe(u8, "text");
    model.manifest.capabilities = try a.alloc([]const u8, 1);
    model.manifest.capabilities[0] = try a.dupe(u8, "inference.max_input_tokens_per_item=4096");
    var inference = try manager.publishLoadedModel(model, true, null, null, 0);
    defer inference.release();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, manager.copyCachedGenerationManifest(failing.allocator(), "teardown-fixture"));
    try std.testing.expectEqual(@as(usize, 1), model.active_handles);
    var copy = (try manager.copyCachedGenerationManifest(a, "teardown-fixture")).?;
    defer copy.deinit();
    try std.testing.expectEqual(@as(usize, 1), model.active_handles);
    inference.retire();
    try std.testing.expect(probe.closed);
    try std.testing.expectEqualStrings("text", copy.inputs[0]);
    try std.testing.expectEqualStrings("inference.max_input_tokens_per_item=4096", copy.capabilities[0]);
    try std.testing.expect((try manager.copyCachedGenerationManifest(a, "teardown-fixture")) == null);
    var replacement_probe = TeardownTestProbe{};
    const replacement_model = try teardownTestModel(&manager, &replacement_probe);
    replacement_model.manifest.model_type = .generator;
    replacement_model.manifest.hidden_size = 2048;
    var replacement = try manager.publishLoadedModel(replacement_model, true, null, null, 0);
    defer replacement.release();
    var fresh = (try manager.copyCachedGenerationManifest(a, "teardown-fixture")).?;
    defer fresh.deinit();
    try std.testing.expectEqual(@as(u32, 2048), fresh.hidden_size);
    try std.testing.expectEqual(@as(usize, 0), fresh.capabilities.len);
    try std.testing.expectEqual(@as(usize, 1), replacement_model.active_handles);
}

test "model aware media host index cache pressure releases idle leases before preflight or model eviction" {
    const a = std.testing.allocator;
    var manager = ModelManager.init(a, backends.SessionManager.init(a));
    defer manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 128, .scratch_limit_bytes = 128 });
    var held = try manager.admitHostPreflightChecked(128, false);
    defer held.release();
    const Cache = struct {
        lease: *runtime.tier.memory.AdmissionLease,
        pinned: bool = true,
        calls: usize = 0,
        fn reclaim(raw: *anyopaque, _: usize) usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.pinned) return 0;
            const freed = self.lease.amounts.host_scratch_bytes;
            self.lease.release();
            return freed;
        }
    };
    var cache: Cache = .{ .lease = &held };
    manager.setHostCacheReclaimer(.{ .context = &cache, .reclaim = Cache.reclaim });
    defer manager.setHostCacheReclaimer(null);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, manager.admitHostPreflightChecked(1, false));
    try std.testing.expectEqual(@as(usize, 128), manager.admissionController().snapshot().host_scratch_bytes);
    cache.pinned = false;
    var next = try manager.admitHostPreflightChecked(16, false);
    next.release();
    try std.testing.expectEqual(@as(usize, 2), cache.calls);
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 1024, .scratch_limit_bytes = 128 });
    held = try manager.admitHostPreflightChecked(100, false);
    var work = try manager.acquireAmountsWithEviction(.cpu, .{ .host_limit_bytes = 1024, .scratch_limit_bytes = 128 }, .{ .host_scratch_bytes = 64 });
    work.release();
    try std.testing.expectEqual(@as(usize, 3), cache.calls);
    try std.testing.expectEqual(@as(usize, 0), manager.loaded.count());
    try std.testing.expectEqual(@as(usize, 0), manager.admissionController().snapshot().hostTotalBytes());
}

fn unifiedDeviceAdmissionLimit(process_limit: usize, combined_override: usize) usize {
    const physical = runtime.tier.memory.defaultSharedAdmissionLimitsWithProcessLimit(process_limit).host_limit_bytes;
    return if (combined_override == 0) physical else @min(physical, combined_override);
}

test "integrated CUDA combined override constrains simultaneous CPU and GPU leases" {
    const limit = unifiedDeviceAdmissionLimit(16 * 1024 * 1024 * 1024, 8 * 1024 * 1024 * 1024);
    try std.testing.expect(limit <= 8 * 1024 * 1024 * 1024);
    var controller: runtime.tier.memory.AdmissionController = .{};
    controller.tightenUnifiedLimit(limit);
    var cpu = try controller.tryAcquire(.cpu, .{}, .{ .host_weight_bytes = limit / 2 + 1 }, false);
    defer cpu.release();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, controller.tryAcquire(.gpu, .{}, .{ .backend_weight_bytes = limit / 2 + 1 }, false));
}

test "startup load policy survives LRU eviction and explicit acquisitions stay separate" {
    const allocator = std.testing.allocator;
    var first = std.testing.tmpDir(.{});
    defer first.cleanup();
    var second = std.testing.tmpDir(.{});
    defer second.cleanup();
    try writeCancellationTestModel(first.dir, allocator);
    try writeCancellationTestModel(second.dir, allocator);
    const root = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", first.sub_path[0..] });
    defer allocator.free(root);
    const peer = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", second.sub_path[0..] });
    defer allocator.free(peer);
    // A policy-free reload cannot use the global preference on this CPU fixture.
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.cuda} });
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    manager.configureModelCache(0, 1);
    try manager.setStartupLoadPolicy(root, .{ .backend = .native });
    var loaded = try manager.acquireFromDir(root);
    const first_id = loaded.get().runtime_id;
    try std.testing.expect(!loaded.get().pinned);
    try std.testing.expectEqual(backends.BackendType.native, loaded.get().session.backend());
    loaded.release();
    var other = try manager.acquireFromDirWithPreferredBackends(peer, &.{.native}, false);
    other.release();
    var reloaded = try manager.acquireFromDirWithControl(root, .{});
    defer reloaded.release();
    try std.testing.expect(first_id != reloaded.get().runtime_id);
    try std.testing.expectEqual(backends.BackendType.native, reloaded.get().session.backend());
    var metadata = (try manager.acquireCachedFromDir(root)).?;
    defer metadata.release();
    try std.testing.expectEqual(reloaded.get().runtime_id, metadata.get().runtime_id);
    // An explicitly different backend cannot consume the configured alias.
    manager.lockLoadedModels();
    const explicit_cuda = try manager.lookupLoadedModelLocked(root, &.{.cuda}, true, inheritedA4bCachePolicy(null, true));
    manager.unlockLoadedModels();
    try std.testing.expect(explicit_cuda == null);
    try std.testing.expectEqualSlices(backends.BackendType, &.{.native}, manager.defaultModelBackends(root));
}

test "startup load policy owns directory keys and preserves precision and A4B selection" {
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();
    var dir = [_]u8{ 'm', 'o', 'd', 'e', 'l' };
    try manager.setStartupLoadPolicy(&dir, .{ .backend = .cuda, .gliner_cuda_precision = .bf16 });
    dir[0] = 'X';
    const policy = manager.startupLoadPolicy("model");
    try std.testing.expectEqual(backends.BackendType.cuda, policy.backend.?);
    try std.testing.expectEqual(session_factory.GlinerCudaPrecision.bf16, policy.gliner_cuda_precision.?);
    const default_precision = manager.defaultModelLoadPolicy("model");
    try std.testing.expectEqualSlices(backends.BackendType, &.{.cuda}, default_precision.preferred_backends);
    try std.testing.expectEqual(session_factory.GlinerCudaPrecision.bf16, default_precision.cache_policy.gliner_cuda_precision.?);
    try std.testing.expect(!default_precision.cache_policy.accept_default_alias);
    // Updating policy never aliases the caller's buffer or drops key ownership.
    try manager.setStartupLoadPolicy("model", .{ .backend = .native, .a4b_request = .{ .residency_mode = .streamed, .memory_budget_mb = 4096 } });
    const updated = manager.startupLoadPolicy("model");
    try std.testing.expect(updated.gliner_cuda_precision == null);
    try std.testing.expectEqual(backend_contracts.A4bResidencyMode.streamed, updated.a4b_request.?.residency_mode);
    try std.testing.expectEqual(@as(u32, 4096), updated.a4b_request.?.memory_budget_mb);
    try std.testing.expectEqualDeep(updated.a4b_request, manager.defaultModelLoadPolicy("model").cache_policy.a4b_request);
}

test "model manager teardown scopes leave retained sessions dormant between physical closes" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    var probe = TeardownTestProbe{};
    const session = try probe.session(&manager);
    defer session.close();
    const ticket = probe.ticket.?;
    var outer = session.beginClose();
    const deadline = ticket.deadline_ns.load(.acquire);
    try std.testing.expect(deadline != 0);
    var inner = session.beginClose();
    try std.testing.expectEqual(@as(usize, 2), ticket.active_scopes);
    try std.testing.expectEqual(deadline, ticket.deadline_ns.load(.acquire));
    inner.deinit();
    try std.testing.expectEqual(deadline, ticket.deadline_ns.load(.acquire));
    outer.deinit();
    try std.testing.expectEqual(@as(usize, 0), ticket.active_scopes);
    try std.testing.expectEqual(@as(u64, 0), ticket.deadline_ns.load(.acquire));
    try TeardownTicket.check(ticket);
    try std.testing.expectEqual(@as(usize, 1), ticket.refs.load(.acquire));
    // The retained ticket is still watched, but its next real cleanup gets a
    // fresh bounded scope rather than inheriting time spent holding an output.
    var resumed = session.beginClose();
    try std.testing.expect(ticket.deadline_ns.load(.acquire) >= deadline);
    resumed.deinit();
    try std.testing.expectEqual(@as(u64, 0), ticket.deadline_ns.load(.acquire));
}

test "model manager retained session pins admission and offline IO past manager shutdown" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    var manager_alive = true;
    defer if (manager_alive) manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 64 });
    try manager.ensureResourceOwnerReady();
    var probe = TeardownTestProbe{ .controller = manager.admissionController() };
    var managed = ManagedSession{ .session = try probe.session(&manager) };
    var managed_alive = true;
    defer if (managed_alive) managed.deinit();
    try manager.attachSessionResourcePin(&managed.session);
    managed.resource_lease = try manager.acquireAmountsWithEviction(.cpu, .{ .host_limit_bytes = 64 }, .{ .host_weight_bytes = 64 });
    const resource_domain = manager.resource_domain.?;
    const offline_io = manager.owned_load_io_owner.?;
    // Stand in for an escaped resident storage reference. The ordinary owning
    // Session close must quiesce, while final physical destruction is delayed.
    const held = managed.session.model_lifetime.?.retain();
    var held_alive = true;
    defer if (held_alive) held.release();
    managed.deinit();
    managed_alive = false;
    try std.testing.expect(!probe.closed);
    try std.testing.expectEqual(@as(u64, 64), resource_domain.admission.snapshot().host_weight_bytes);
    try std.testing.expectEqual(@as(u64, 0), probe.ticket.?.deadline_ns.load(.acquire));
    manager.deinit();
    manager_alive = false;
    try std.testing.expect(resource_domain.closing.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), resource_domain.references.load(.acquire));
    try std.testing.expect(offline_io.refs.load(.acquire) > 0);
    try std.testing.expectEqual(@as(u64, 64), resource_domain.admission.snapshot().host_weight_bytes);
    held.release();
    held_alive = false;
    try std.testing.expect(probe.closed);
}

test "model manager escaped host outputs retain admission without retaining model weights" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    var manager_alive = true;
    defer if (manager_alive) manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 4096 });
    try manager.ensureResourceOwnerReady();
    const HostOutputs = struct {
        fn run(_: *anyopaque, _: []const backends.Tensor, alloc: std.mem.Allocator) ![]backends.Tensor {
            const outputs = try alloc.alloc(backends.Tensor, 1);
            errdefer alloc.free(outputs);
            outputs[0] = try backends.Tensor.initFloat32(alloc, "hidden", &.{ 1, 2 }, &.{ 3, 4 });
            return outputs;
        }
    };
    var probe = TeardownTestProbe{};
    var session = try probe.session(&manager);
    var session_alive = true;
    defer if (session_alive) session.close();
    var vtable = TeardownTestProbe.vtable;
    vtable.run = HostOutputs.run;
    session.vtable = &vtable;
    session.run_admission = .{
        .controller = manager.admissionController(),
        .backend_class = .cpu,
        .limits = .{ .host_limit_bytes = 4096 },
        .static_workspace_bytes = 1024,
        .check_live_memory = false,
    };
    try manager.attachSessionResourcePin(&session);
    const domain = manager.resource_domain.?;
    const outputs = try session.run(&.{}, allocator);
    defer allocator.free(outputs);
    var outputs_alive = true;
    defer if (outputs_alive) for (outputs) |*output| output.deinit();
    try std.testing.expect(domain.admission.snapshot().host_scratch_bytes > 0);
    session.close();
    session_alive = false;
    try std.testing.expect(probe.closed);
    manager.deinit();
    manager_alive = false;
    try std.testing.expect(domain.closing.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), domain.references.load(.acquire));
    try std.testing.expect(domain.admission.snapshot().host_scratch_bytes > 0);
    try std.testing.expectEqualSlices(f32, &.{ 3, 4 }, outputs[0].asFloat32());
    outputs[0].deinit();
    outputs_alive = false;
}

test "model manager aggregate retains session admission through dependent shared cleanup" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    defer manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 64 });
    try manager.ensureResourceOwnerReady();
    var probe = TeardownTestProbe{};
    const model = try teardownTestModel(&manager, &probe);
    var model_alive = true;
    defer if (model_alive) manager.destroyLoadedModel(model);
    try manager.attachSessionResourcePin(&model.session);
    const Observer = struct {
        probe: *TeardownTestProbe,
        controller: *runtime.tier.memory.AdmissionController,
        frees: usize = 0,
        fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            return std.testing.allocator.rawAlloc(len, alignment, ret);
        }
        fn resize(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) bool {
            return std.testing.allocator.rawResize(memory, alignment, len, ret);
        }
        fn remap(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) ?[*]u8 {
            return std.testing.allocator.rawRemap(memory, alignment, len, ret);
        }
        fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.probe.closed or self.controller.snapshot().host_weight_bytes != 64)
                @panic("shared model state outlived its retained session admission");
            self.frees += 1;
            std.testing.allocator.rawFree(memory, alignment, ret);
        }
        const vtable = std.mem.Allocator.VTable{ .alloc = alloc, .resize = resize, .remap = remap, .free = free };
    };
    var observer = Observer{ .probe = &probe, .controller = manager.admissionController() };
    const state = try allocator.create(runtime.tier.shared.SharedPrefetchState);
    state.* = runtime.tier.shared.SharedPrefetchState.init(.{ .ptr = &observer, .vtable = &Observer.vtable });
    model.shared_prefetch = state;
    _ = try state.noteRequest("weight", 1);
    manager.destroyLoadedModel(model);
    model_alive = false;
    try std.testing.expect(observer.frees > 0);
    try std.testing.expect(probe.closed);
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.admissionController().snapshot());
}

test "model manager escaped run permit retains admission past model and manager shutdown" {
    const allocator = std.testing.allocator;
    var manager = ModelManager.init(allocator, .{ .allocator = allocator, .preferred_backends = &.{.native} });
    var manager_alive = true;
    defer if (manager_alive) manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 4096 });
    try manager.ensureResourceOwnerReady();
    var probe = TeardownTestProbe{};
    var session = try probe.session(&manager);
    var session_alive = true;
    defer if (session_alive) session.close();
    session.run_admission = .{
        .controller = manager.admissionController(),
        .backend_class = .cpu,
        .limits = .{ .host_limit_bytes = 4096 },
        .static_workspace_bytes = 0,
        .check_live_memory = false,
    };
    try manager.attachSessionResourcePin(&session);
    const domain = manager.resource_domain.?;
    var permit = try session.admitHostPreprocess(1024);
    defer permit.deinit();
    try std.testing.expectEqual(@as(usize, 1024), domain.admission.snapshot().host_scratch_bytes);
    session.close();
    session_alive = false;
    try std.testing.expect(probe.closed);
    manager.deinit();
    manager_alive = false;
    try std.testing.expect(domain.closing.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), domain.references.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1024), domain.admission.snapshot().host_scratch_bytes);
    // A retired owner cannot execute again, but its already-issued reservation
    // must still release through a live controller when its borrower retires.
    permit.deinit();
}

fn loadPolicySelectionTestModel(runtime_id: RuntimeId) LoadedModel {
    const Stub = struct {
        var probe: TeardownTestProbe = .{};
        fn backend(_: *anyopaque) backends.BackendType {
            return .cuda;
        }
        const vtable = blk: {
            var table = TeardownTestProbe.vtable;
            table.backend = backend;
            break :blk table;
        };
    };
    var model = admissionEvictionTestModel(0);
    model.session = .{ .ptr = &Stub.probe, .vtable = &Stub.vtable };
    model.runtime_id = runtime_id;
    return model;
}

fn deinitLoadPolicySelectionTestManager(manager: *ModelManager) void {
    // These are borrowed metadata-only models. No driver/session is entered.
    var loaded = manager.loaded.keyIterator();
    while (loaded.next()) |key| manager.allocator.free(key.*);
    manager.loaded.deinit(manager.allocator);
    var aliases = manager.loaded_aliases.keyIterator();
    while (aliases.next()) |key| manager.allocator.free(key.*);
    manager.loaded_aliases.deinit(manager.allocator);
    var policies = manager.startup_load_policies.keyIterator();
    while (policies.next()) |key| manager.allocator.free(key.*);
    manager.startup_load_policies.deinit(manager.allocator);
    manager.in_flight_loads.deinit(manager.allocator);
}

fn expectLoadPolicySelection(handle: ModelHandle, expected: RuntimeId) !void {
    var owned = handle;
    defer owned.release();
    try std.testing.expectEqual(expected, owned.get().runtime_id);
}

test "explicit load policy placement preserves configured A4B variants with and without aliases" {
    const a = std.testing.allocator;
    var manager = ModelManager.init(a, .{ .allocator = a, .preferred_backends = &.{.cuda} });
    defer deinitLoadPolicySelectionTestManager(&manager);
    const configured_request = backend_contracts.A4bInferenceRequest{ .residency_mode = .resident, .memory_budget_mb = 20480, .load_workers = 8, .load_staging_mb = 512 };
    const override_request = backend_contracts.A4bInferenceRequest{ .residency_mode = .resident, .memory_budget_mb = 24576, .load_workers = 4 };
    try manager.setStartupLoadPolicy("model", .{ .backend = .cuda, .a4b_request = configured_request });
    var configured = loadPolicySelectionTestModel(1);
    var unqualified = loadPolicySelectionTestModel(2);
    var overridden = loadPolicySelectionTestModel(3);
    try manager.loaded.put(a, try backendVariantCacheKey(a, "model", .cuda, configured_request), &configured);
    try manager.loaded.put(a, try backendVariantCacheKey(a, "model", .cuda, null), &unqualified);
    try manager.loaded.put(a, try backendVariantCacheKey(a, "model", .cuda, override_request), &overridden);
    try manager.loaded_aliases.put(a, try a.dupe(u8, "model"), &configured);
    for (0..2) |phase| {
        if (phase == 1) {
            const alias = manager.loaded_aliases.fetchRemove("model").?;
            a.free(alias.key);
        }
        try expectLoadPolicySelection(try manager.acquireFromDir("model"), 1);
        try expectLoadPolicySelection(try manager.acquireFromDirWithControl("model", .{}), 1);
        try expectLoadPolicySelection(try manager.acquireFromDirWithPreferredBackends("model", &.{.cuda}, false), 1);
        try expectLoadPolicySelection(try manager.acquireFromDirWithPreferredBackendsAndControl("model", &.{.cuda}, false, .{}), 1);
        try expectLoadPolicySelection((try manager.acquireCachedFromDir("model")).?, 1);
        // A real policy override remains exact, regardless of the default alias.
        try expectLoadPolicySelection(try manager.acquireFromDirWithA4bRequest("model", override_request), 3);
        try expectLoadPolicySelection(try manager.acquireFromDirWithA4bRequestAndControl("model", override_request, .{}), 3);
        try expectLoadPolicySelection(try manager.acquireFromDirWithPreferredBackendsAndA4bRequest("model", &.{.cuda}, false, override_request), 3);
        try expectLoadPolicySelection(try manager.acquireFromDirWithPreferredBackendsAndA4bRequestAndControl("model", &.{.cuda}, false, override_request, .{}), 3);
    }
    const automatic = manager.resolveModelLoadPolicy("model", .{});
    const explicit = manager.resolveModelLoadPolicy("model", .{ .preferred_backends = &.{.cuda} });
    try std.testing.expectEqualDeep(automatic.cache_policy, explicit.cache_policy);
    const auto_flight = try manager.loadFlightKey("model", automatic.preferred_backends, false, automatic.cache_policy);
    defer a.free(auto_flight);
    const explicit_flight = try manager.loadFlightKey("model", explicit.preferred_backends, false, explicit.cache_policy);
    defer a.free(explicit_flight);
    try std.testing.expectEqualStrings(auto_flight, explicit_flight);
}

test "explicit load policy placement retains configured GLiNER precision and honors an explicit precision override" {
    const a = std.testing.allocator;
    var manager = ModelManager.init(a, .{ .allocator = a, .preferred_backends = &.{.cuda} });
    defer deinitLoadPolicySelectionTestManager(&manager);
    try manager.setStartupLoadPolicy("model", .{ .backend = .cuda, .gliner_cuda_precision = .bf16 });
    var configured = loadPolicySelectionTestModel(1);
    var unqualified = loadPolicySelectionTestModel(2);
    var overridden = loadPolicySelectionTestModel(3);
    try manager.loaded.put(a, try backendVariantCacheKeyWithPrecision(a, "model", .cuda, null, .bf16), &configured);
    try manager.loaded.put(a, try backendVariantCacheKeyWithPrecision(a, "model", .cuda, null, null), &unqualified);
    try manager.loaded.put(a, try backendVariantCacheKeyWithPrecision(a, "model", .cuda, null, .fp32), &overridden);
    try expectLoadPolicySelection(try manager.acquireFromDirWithPreferredBackends("model", &.{.cuda}, false), 1);
    try expectLoadPolicySelection(try manager.acquireFromDirWithPreferredBackendsAndControl("model", &.{.cuda}, false, .{}), 1);
    try expectLoadPolicySelection(try manager.acquireFromDirWithGlinerCudaPrecision("model", .fp32), 3);
}

test "explicit load policy resolves global model and call precedence before any model is resident" {
    var manager = ModelManager.init(std.testing.allocator, .{ .allocator = std.testing.allocator, .preferred_backends = &.{ .native, .cuda }, .a4b_inference_request = .{ .memory_budget_mb = 16384 } });
    defer manager.deinit();
    try manager.setStartupLoadPolicy("model", .{ .backend = .cuda, .a4b_request = .{ .memory_budget_mb = 20480 } });
    try std.testing.expectEqual(@as(u32, 16384), manager.resolveModelLoadPolicy("other", .{}).cache_policy.a4b_request.?.memory_budget_mb);
    const explicit = manager.resolveModelLoadPolicy("model", .{ .preferred_backends = &.{.metal} });
    try std.testing.expectEqualSlices(backends.BackendType, &.{.metal}, explicit.preferred_backends);
    try std.testing.expectEqual(@as(u32, 20480), explicit.cache_policy.a4b_request.?.memory_budget_mb);
    const override = manager.resolveModelLoadPolicy("model", .{ .a4b_request = .{ .memory_budget_mb = 24576 } });
    try std.testing.expectEqual(@as(u32, 24576), override.cache_policy.a4b_request.?.memory_budget_mb);
}

test "model manager resident child views retain actual Native source and quota after manager shutdown" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    const ops = @import("../ops/ops.zig");
    const Resident = @import("../backends/session.zig").ResidentOutputs;
    const Tensor = backends.Tensor;
    var manager = ModelManager.init(a, .{ .allocator = a, .preferred_backends = &.{.native} });
    var manager_open = true;
    defer if (manager_open) manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 1024 * 1024 });
    try manager.ensureResourceOwnerReady();
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer weights.deinitOwned();
    var compute = native.NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const Probe = struct {
        close: TeardownTestProbe = .{},
        cb: ops.ComputeBackend,
        charged_at_cleanup: bool = false,
        controller: *runtime.tier.memory.AdmissionController,
        fn kind(_: *anyopaque) backends.BackendType {
            return .native;
        }
        fn info(_: *anyopaque) []const backends.TensorInfo {
            return &.{.{ .name = "values", .dtype = .f32, .shape = &.{ -1, 2 } }};
        }
        fn forward(raw: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) !?Resident {
            const close: *TeardownTestProbe = @ptrCast(@alignCast(raw));
            const self: *@This() = @fieldParentPtr("close", close);
            const source = try self.cb.fromFloat32Shape(inputs[0].asFloat32(), &.{ 2, 2 });
            errdefer self.cb.free(source);
            const vector = try allocator.alloc(ops.CT, 1);
            vector[0] = source;
            return .{ .outputs = vector, .backend = &self.cb, .allocator = allocator, .backend_owner = self, .deinit_backend_owner = cleanup };
        }
        fn cleanup(raw: *anyopaque, _: std.mem.Allocator) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.charged_at_cleanup = self.controller.snapshot().host_scratch_bytes > 0;
            std.debug.assert(self.close.ticket.?.deadline_ns.load(.acquire) != 0);
        }
    };
    var probe = Probe{ .cb = compute.computeBackend(), .controller = manager.admissionController() };
    var session = try probe.close.session(&manager);
    var session_open = true;
    defer if (session_open) session.close();
    var table = TeardownTestProbe.vtable;
    table.runResident = Probe.forward;
    table.inputInfo = Probe.info;
    table.outputInfo = Probe.info;
    table.backend = Probe.kind;
    session.vtable = &table;
    session.run_admission = .{ .controller = manager.admissionController(), .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1024 * 1024 }, .static_workspace_bytes = 1, .check_live_memory = false };
    session.installBorrowedCloseProtection(session.close_protection);
    try manager.attachSessionResourcePin(&session);
    const domain = manager.resource_domain.?;
    var input = try Tensor.initFloat32(a, "values", &.{ 2, 2 }, &.{ 1, 2, 3, 4 });
    defer input.deinit();
    var output = (try session.runResident(&.{input}, a)).?;
    var output_open = true;
    defer if (output_open) output.deinit();
    const first_vector = try a.alloc(ops.CT, 1);
    var first_vector_open = true;
    defer if (first_vector_open) a.free(first_vector);
    first_vector[0] = try probe.cb.sliceRows2D(a, output.outputs[0], 0, 1, 2);
    var first_ct_open = true;
    defer if (first_ct_open) probe.cb.free(first_vector[0]);
    var first = try output.ownViewsOf(a, first_vector);
    first_vector_open = false;
    first_ct_open = false;
    var first_open = true;
    defer if (first_open) first.deinit();
    const second_vector = try a.alloc(ops.CT, 1);
    var second_vector_open = true;
    defer if (second_vector_open) a.free(second_vector);
    second_vector[0] = try probe.cb.sliceRows2D(a, output.outputs[0], 1, 1, 2);
    var second_ct_open = true;
    defer if (second_ct_open) probe.cb.free(second_vector[0]);
    var second = try output.ownViewsOf(a, second_vector);
    second_vector_open = false;
    second_ct_open = false;
    var second_open = true;
    defer if (second_open) second.deinit();
    output.deinit();
    output_open = false;
    session.close();
    session_open = false;
    manager.deinit();
    manager_open = false;
    try std.testing.expect(domain.closing.load(.acquire));
    try std.testing.expect(!probe.close.closed);
    try std.testing.expect(domain.admission.snapshot().host_scratch_bytes > 0);
    first.deinit();
    first_open = false;
    try std.testing.expect(!probe.close.closed);
    const values = try second.backend.toFloat32(second.outputs[0], a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 3, 4 }, values);
    second.deinit();
    second_open = false;
    try std.testing.expect(probe.close.closed and probe.charged_at_cleanup);
}

test "pure admission pin preserves external partition token after actual session manager IO and arena teardown" {
    const a = std.testing.allocator;
    const Recorder = struct { retained: usize = 0, contexts_released: usize = 0, contexts_destroyed: usize = 0, reserves: usize = 0, shrinks: usize = 0, releases: usize = 0 };
    const Budget = struct {
        allocator: std.mem.Allocator,
        recorder: *Recorder,
        references: std.atomic.Value(usize) = .init(1),
        amounts: runtime.tier.memory.AdmissionAmounts = .{},
        fn capability(self: *@This()) runtime.tier.memory.AdmissionResourceBudget {
            return .{ .context = self, .retain_context = retainContext, .release_context = releaseContext, .try_reserve = reserve, .retain = shrink, .release = release };
        }
        fn retainContext(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const old = self.references.fetchAdd(1, .monotonic);
            std.debug.assert(old > 0);
            self.recorder.retained += 1;
            return true;
        }
        fn releaseContext(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.recorder.contexts_released += 1;
            const old = self.references.fetchSub(1, .acq_rel);
            std.debug.assert(old > 0);
            if (old == 1) {
                self.recorder.contexts_destroyed += 1;
                const allocator = self.allocator;
                allocator.destroy(self);
            }
        }
        fn reserve(raw: *anyopaque, amounts: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.recorder.reserves != 0) return error.ResourceLimitExceeded;
            self.recorder.reserves += 1;
            self.amounts = amounts;
            return 0x1166;
        }
        fn shrink(raw: *anyopaque, token: usize, amounts: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (token != 0x1166 or amounts.host_scratch_bytes > self.amounts.host_scratch_bytes) return error.ResourceLimitExceeded;
            self.recorder.shrinks += 1;
            self.amounts = amounts;
        }
        fn release(raw: *anyopaque, token: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(token == 0x1166);
            self.recorder.releases += 1;
            self.amounts = .{};
        }
    };
    var arena = std.heap.ArenaAllocator.init(a);
    var arena_live = true;
    defer if (arena_live) arena.deinit();
    var synchronized = @import("executor_microbatch.zig").SynchronizedAllocator{ .child = arena.allocator() };
    const alloc = synchronized.allocator();
    var manager = ModelManager.init(alloc, .{ .allocator = alloc, .preferred_backends = &.{.native} });
    var manager_live = true;
    defer if (manager_live) manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 64 });
    try manager.ensureResourceOwnerReady();
    {
        spinLock(&manager.load_lock);
        defer manager.load_lock.unlock();
        _ = try manager.loadCoordinationIoLocked();
    }
    try std.testing.expect(manager.owned_load_io_owner != null);
    var probe = TeardownTestProbe{};
    var session = try probe.session(&manager);
    var session_live = true;
    defer if (session_live) session.close();
    try manager.attachSessionResourcePin(&session);
    const controller = manager.admissionController();
    const pin = session.model_lifetime.?.retainAdmissionDomain(controller) orelse return error.MissingAdmissionDomain;
    var pin_live = true;
    defer if (pin_live) pin.release();
    var unrelated = runtime.tier.memory.AdmissionController{};
    defer unrelated.deinit();
    try std.testing.expect(session.model_lifetime.?.retainAdmissionDomain(&unrelated) == null);
    var recorder = Recorder{};
    const external = try a.create(Budget);
    external.* = .{ .allocator = a, .recorder = &recorder };
    var external_live = true;
    defer if (external_live) Budget.releaseContext(external);
    try controller.configureResourceBudget(external.capability());
    var lease = try controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64 }, .{ .host_scratch_bytes = 32 }, false);
    defer lease.release();
    // The requested metadata allocator deliberately belongs to the Arena.
    // Domain controllers must place the shared token in their stable owner.
    var child = try lease.partition(alloc, .{ .host_scratch_bytes = 16 });
    defer child.release();
    lease.release();
    Budget.releaseContext(external);
    external_live = false;
    try std.testing.expectEqual(@as(usize, 2), recorder.retained);
    try std.testing.expectEqual(@as(usize, 1), recorder.shrinks);
    session.close();
    session_live = false;
    try std.testing.expect(probe.closed);
    manager.deinit();
    manager_live = false;
    arena.deinit();
    arena_live = false;
    try std.testing.expectEqual(@as(usize, 16), pin.controller().snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), recorder.contexts_destroyed);
    try std.testing.expectEqual(@as(usize, 0), recorder.releases);
    child.release();
    try std.testing.expectEqual(runtime.tier.memory.AdmissionAmounts{}, pin.controller().snapshot());
    try std.testing.expectEqual(@as(usize, 1), recorder.reserves);
    try std.testing.expectEqual(@as(usize, 1), recorder.releases);
    try std.testing.expectEqual(@as(usize, 2), recorder.contexts_released);
    pin.release();
    pin_live = false;
    try std.testing.expectEqual(@as(usize, 3), recorder.contexts_released);
    try std.testing.expectEqual(@as(usize, 1), recorder.contexts_destroyed);
}

test "managed cold diagnostics policy observes constructor ownership and rejects allocation failure cleanly" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try writeCancellationTestModel(dir.dir, a);
    const root = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer a.free(root);
    var sessions = backends.SessionManager{ .allocator = a, .preferred_backends = &.{.native}, .execution_diagnostics = .{ .enabled = true } };
    var manager = ModelManager.init(a, sessions);
    defer manager.deinit();
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    var loaded = try loadSessionForPreferredBackends(&manager, &.{.native}, root, man, &manager.session_manager, null);
    defer loaded.deinit();
    const binding = loaded.session.invocation_diagnostics orelse return error.MissingManagedDiagnostics;
    try std.testing.expect(binding.diagnostics.isEnabled());
    try std.testing.expect(binding.diagnostics.export_owner != 0);
    const observer = @import("../execution/allocation_observer.zig").State.forAllocator(binding.model_allocator orelse return error.MissingModelAllocator) orelse return error.MissingColdModelObserver;
    try std.testing.expectEqual(.from_owner_creation, observer.coverage);
    // No numerical event occurred: exporting cannot reuse another request's
    // previous success or manufacture a model-construction success snapshot.
    try std.testing.expect(loaded.session.executionDiagnostics() == null);
    var fail = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    sessions.allocator = fail.allocator();
    try std.testing.expectError(error.OutOfMemory, sessions.loadModel(root));
    const quiet = backends.SessionManager{ .allocator = a, .preferred_backends = &.{.native} };
    try std.testing.expect(!quiet.execution_diagnostics.enabled);
    try std.testing.expectEqual(@intFromPtr(a.ptr), @intFromPtr(quiet.allocator.ptr));
}

test "permanent memory admission failures retain estimates and release reservations" {
    const memory = runtime.tier.memory;
    var manager = ModelManager.init(std.testing.allocator, backends.SessionManager.init(std.testing.allocator));
    defer manager.deinit();
    const limits = memory.Limits{ .host_limit_bytes = 100, .scratch_limit_bytes = 20 };
    try std.testing.expectError(error.ResourceLimitExceeded, manager.acquireAmountsWithEviction(.cpu, limits, .{ .host_scratch_bytes = 30 }));
    const denied = manager.memoryAdmissionDiagnostic();
    try std.testing.expectEqualStrings("ResourceLimitExceeded", denied.error_code);
    try std.testing.expect(!denied.retryable);
    try std.testing.expectEqual(@as(u64, 30), denied.requested_scratch_bytes);
    try std.testing.expectEqual(@as(u64, 20), denied.scratch_limit_bytes);
    try std.testing.expectEqual(@as(u64, 1), denied.denied_count);
    try std.testing.expectEqual(@as(usize, 0), manager.admissionController().snapshot().hostTotalBytes());
    try std.testing.expectError(error.ResourceLimitExceeded, manager.acquireRequestsWithEviction(&.{.{ .backend_class = .cpu, .limits = limits, .amounts = .{ .host_scratch_bytes = 30 } }}));
    try std.testing.expectEqual(@as(u64, 2), manager.memoryAdmissionDiagnostic().denied_count);
}

test "Native active cache refund and reentrant final Model release occur outside both Manager locks" {
    const a = std.testing.allocator;
    var manager = ModelManager.init(a, backends.SessionManager.init(a));
    defer manager.deinit();
    try manager.ensureResourceOwnerReady();
    const domain = try runtime.tier.admission_domain.Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    var cache = runtime.tier.cache.SharedCache.init(.{ .host_limit_bytes = 32 });
    defer cache.deinitAdmission();
    try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .cpu, .{ .host_limit_bytes = 32 }, .{});
    try cache.installRetirementDomainOutsideExecution(&pin);
    const Fixture = struct {
        manager: *ModelManager,
        cache: *runtime.tier.cache.SharedCache,
        lifetime: ?*backends.Session.ModelLifetime = null,
        drop_base: bool = false,
        violation: bool = false,
        closed: bool = false,
        releases: usize = 0,
        fn inspect(self: *@This()) void {
            if (self.manager.load_lock.tryLock()) self.manager.load_lock.unlock() else self.violation = true;
            if (self.manager.eviction_lock.tryLock()) self.manager.eviction_lock.unlock() else self.violation = true;
        }
        fn reserve(raw: *anyopaque, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
            return 44;
        }
        fn retain(raw: *anyopaque, _: usize, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
            self.releases += 1;
            if (self.drop_base) {
                self.drop_base = false;
                self.lifetime.?.release(); // callback retires original identity
                if (self.closed) self.violation = true; // helper's exact pin lives
            }
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
            self.cache.deinitAdmission();
            self.closed = true;
        }
        fn settle(session: backends.Session) !bool {
            const self: *@This() = @ptrCast(@alignCast(session.ptr));
            const before = self.cache.admissionAmounts();
            try self.cache.flushReleasedOutsideExecution();
            const after = self.cache.admissionAmounts();
            return after.host_weight_bytes < before.host_weight_bytes;
        }
    };
    var fixture = Fixture{ .manager = &manager, .cache = &cache };
    var model = backends.Session.ModelLifetime{ .owner = &fixture, .destroy_fn = Fixture.close };
    fixture.lifetime = &model;
    try pin.controller().configureResourceBudget(.{ .context = &fixture, .try_reserve = Fixture.reserve, .retain = Fixture.retain, .release = Fixture.release });
    try cache.reserve(.host, 32);
    cache.deferPhysicalReleases();
    manager.lockLoadedModels();
    spinLock(&manager.eviction_lock);
    cache.noteRelease(.host, 32); // actual backing already died under residency guard
    try std.testing.expectEqual(@as(usize, 0), fixture.releases);
    const selected = backends.Session{ .ptr = &fixture, .vtable = &TeardownTestProbe.vtable, .model_lifetime = &model };
    const actual_pin = model.retain(); // real selected owner BEFORE lock release
    fixture.drop_base = true;
    try std.testing.expect(manager.finishReclaimedHostCacheOutsideLocks(selected, actual_pin, Fixture.settle));
    // Same outer acquisition lock contract is restored after all callbacks.
    try std.testing.expect(!manager.eviction_lock.tryLock());
    manager.eviction_lock.unlock();
    try std.testing.expect(manager.load_lock.tryLock());
    manager.load_lock.unlock();
    try std.testing.expect(fixture.closed and !fixture.violation);
    try std.testing.expectEqual(@as(usize, 1), fixture.releases);
    try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().host_weight_bytes);
    try std.testing.expect(cache.admission == null);
}

// The entire loader, admission Domain and cold service are production owners.
// Only analytical model data and metadata-allocation failures are test inputs.
fn checkManagedPackedProjectionHead(comptime count: usize, comptime rows: usize) !void {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const metal = @import("../backends/metal_runtime.zig");
    if (!metal.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const imported = @import("../backends/imported_onnx_session.zig");
    const Graph = ml.graph.Graph;
    const Shape = ml.graph.Shape;
    const Tensor = @import("../backends/tensor.zig").Tensor;
    const width = if (rows >= 128) 128 else 64;
    var graph = Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const x = try builder.parameter("x", Shape.init(.f32, &.{ rows, width }));
    var matrices: [count][width * width]f32 = @splat(@splat(0));
    const names = [_][]const u8{ "w0", "w1", "w2" };
    var initializers: [count]onnx_graph.ParameterInitializer = undefined;
    for (0..count) |index| {
        for (0..width) |column| matrices[index][column * width + column] = @floatFromInt(index + 1);
        const weight = try builder.parameter(names[index], Shape.init(.f32, &.{ width, width }));
        try graph.markOutput(try builder.linearNoBias(x, weight, rows, width, width));
        initializers[index] = .{ .name = names[index], .shape = Shape.init(.f32, &.{ width, width }), .data = .{ .f32 = &matrices[index] } };
    }
    const bytes = try onnx_graph.exportGraph(a, &graph, .{ .parameter_initializers = &initializers });
    defer a.free(bytes);
    var dir = std.testing.tmpDir(.{});
    defer dir.cleanup();
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "model.onnx", .data = bytes });
    const config = try std.fmt.allocPrint(a, "{{\"model_type\":\"bert\",\"hidden_size\":{d},\"num_hidden_layers\":0,\"num_attention_heads\":1,\"intermediate_size\":{d},\"vocab_size\":2,\"max_position_embeddings\":128}}", .{ width, width });
    defer a.free(config);
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = config });
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "tokenizer.json", .data = "{\"version\":\"1.0\",\"model\":{\"type\":\"BPE\",\"vocab\":{\"a\":0,\"b\":1},\"merges\":[]}}" });
    const root = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", dir.sub_path[0..] });
    defer a.free(root);
    const path = try std.fs.path.join(a, &.{ root, "model.onnx" });
    defer a.free(path);
    var manager = ModelManager.init(a, .{ .allocator = a, .preferred_backends = &.{.metal}, .required_backend = .metal, .graph_runtime_strategy = .partitioned, .execution_diagnostics = .{ .enabled = true } });
    var manager_open = true;
    defer if (manager_open) manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    var handle = try manager.acquireFromDir(root);
    var handle_open = true;
    defer if (handle_open) handle.release();
    const session = handle.get().session;
    try std.testing.expectEqual(backends.BackendType.metal, session.backend());
    const impl: *imported.ImportedOnnxSession = @ptrCast(@alignCast(session.ptr));
    const admission = session.run_admission orelse return error.MissingRunAdmission;
    const pin = session.model_lifetime.?.retainAdmissionDomain(admission.controller) orelse return error.MissingCacheRetirementDomain;
    defer pin.release();
    const controller = pin.controller();
    const cache = (try impl.shared_backend_ctx.managedMetalCache()) orelse return error.MissingImportedManagedCache;
    try std.testing.expect(cache.admission != null and cache.admission.?.controller == controller);
    const head = @import("../ops/kernel_descriptor.zig").projectionHead(impl.executable.graph) orelse return error.MissingSemanticProjectionHead;
    try std.testing.expectEqual(@as(u8, count), head.count);
    try std.testing.expect(try @import("../execution/prepared_invocation.zig").needsBeforeExecution(session));
    var values: [rows * width]f32 = undefined;
    for (&values, 0..) |*value, index| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(index % 17)) - 8)) / 32;
    var input = try Tensor.initFloat32(a, "x", &.{ rows, width }, &values);
    defer input.deinit();
    const compute = impl.shared_backend_ctx.ctx.metal_hosted.compute;
    const before = controller.snapshot();
    const sdk_before = @import("../backends/metal_tensor.zig").memoryStatsSnapshot();
    // Exercise real public input-wrapper allocation failure before the first
    // ordinary call. Upload may already create a device tensor; rollback must
    // retire it and preserve admission, rather than assert no SDK allocation.
    for (0..2) |fail_index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
        const previous_allocator = compute.allocator;
        compute.allocator = failing.allocator();
        const outcome = session.runResident(&.{input}, a);
        compute.allocator = previous_allocator;
        const failure = if (outcome) |optional| blk: {
            if (optional) |value| {
                var unexpected = value;
                unexpected.deinit();
            }
            break :blk error.ExpectedColdMetadataAllocationFailure;
        } else |err| err;
        try std.testing.expectEqual(@as(anyerror, error.OutOfMemory), failure);
        try std.testing.expectEqualDeep(before, controller.snapshot());
        const sdk_after = @import("../backends/metal_tensor.zig").memoryStatsSnapshot();
        try std.testing.expectEqual(sdk_before.device_owned_live_bytes, sdk_after.device_owned_live_bytes);
        try std.testing.expectEqual(sdk_after.device_owned_buffers_created - sdk_before.device_owned_buffers_created, sdk_after.device_owned_buffers_released - sdk_before.device_owned_buffers_released);
        try std.testing.expectEqual(sdk_after.device_owned_bytes_created - sdk_before.device_owned_bytes_created, sdk_after.device_owned_bytes_released - sdk_before.device_owned_bytes_released);
    }
    // Real constructor preparation can already make the first Source eligible.
    // Preserve its original selection; do not force a compatibility warmup.
    {
        var original = (try session.runResident(&.{input}, a)) orelse return error.MissingResidentResult;
        defer original.deinit();
        const first_stats = session.executionDiagnostics().?.execution.?;
        try std.testing.expect(first_stats.runtime_region_plan_dispatches <= 1);
        // The existing grouped compiler may cover four QKV nodes on a cold
        // call; the assigned semantic head below must cover all six instead.
        try std.testing.expect(first_stats.runtime_region_plan_covered_nodes <= (if (count == 2) @as(u64, 2) else 6));
        if (first_stats.runtime_region_plan_dispatches == 0) try std.testing.expectEqual(@as(u64, 0), first_stats.runtime_region_plan_covered_nodes);
        for (original.outputs, 0..) |output, index| {
            const actual = try original.backend.toFloat32(output, a);
            defer a.free(actual);
            for (actual, &values) |got, value| try std.testing.expectApproxEqAbs(value * @as(f32, @floatFromInt(index + 1)), got, 0.0001);
        }
    }
    // This fixture exports fixed shapes. The optimized fused semantic head
    // must retain a region-capable physical plan on this actual root.
    const target = impl;
    try std.testing.expectEqual(@import("../execution/executable.zig").runtime_module.Strategy.partitioned, target.requested_strategy);
    try std.testing.expect(target.source_path == null);
    try std.testing.expect(target.executable.runtime.physical_pool != null);
    const request = blk: {
        var metadata = try @import("../execution/backend_operation.zig").enter(target.cb, target.executable.graph, null, .metadata);
        defer metadata.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        const pool = target.executable.runtime.physical_pool orelse return error.MissingConcretePhysicalPool;
        break :blk (try @import("../ops/metal_compute.zig").MetalCompute.collectProjectionPackRequest(&target.cb, target.executable.graph, pool)) orelse return error.MissingWarmProjectionPackRequest;
    };
    // This invocation, with no manual slots or fixture-only cache attachment,
    // uses the actual pre-profile Source service and Frame.executeRegion.
    var held = (try session.runResident(&.{input}, a)) orelse return error.MissingResidentResult;
    var held_open = true;
    defer if (held_open) held.deinit();
    const stats = session.executionDiagnostics().?.execution.?;
    try std.testing.expectEqual(@as(u64, 1), stats.runtime_region_plan_dispatches);
    try std.testing.expectEqual(if (count == 2) @as(u64, 2) else 6, stats.runtime_region_plan_covered_nodes);
    try std.testing.expect(compute.packed_projection_entries != null);
    try std.testing.expect(!metal.hasActiveFrame(compute.provider_impl.raw_decode_runtime) and !metal.hasSubmittedFrame(compute.provider_impl.raw_decode_runtime));
    const after = controller.snapshot();
    try std.testing.expect(after.backend_weight_bytes > before.backend_weight_bytes);
    try std.testing.expect(after.host_weight_bytes > before.host_weight_bytes);
    for (held.outputs, 0..) |output, index| {
        const actual = try held.backend.toFloat32(output, a);
        defer a.free(actual);
        for (actual, &values) |got, value| try std.testing.expectApproxEqAbs(value * @as(f32, @floatFromInt(index + 1)), got, 0.0001);
    }
    // Independent CPU evaluation uses the original same serialized artifact.
    var native_session = try imported.createSessionWithOptions(a, path, .native, .{ .graph_runtime_strategy = .partitioned });
    defer native_session.close();
    const cpu = try native_session.run(&.{input}, a);
    defer {
        for (cpu) |*output| output.deinit();
        a.free(cpu);
    }
    for (cpu, held.outputs) |expected, output| {
        const actual = try held.backend.toFloat32(output, a);
        defer a.free(actual);
        for (actual, expected.asFloat32()) |got, want| try std.testing.expectApproxEqAbs(want, got, 0.0001);
    }
    // A second call proves reuse without another packed root allocation while
    // the prior independently owned physical outputs remain live and correct.
    const warm_cache = cache.admissionAmounts();
    {
        var next = (try session.runResident(&.{input}, a)) orelse return error.MissingResidentResult;
        defer next.deinit();
        try std.testing.expectEqual(@as(u64, 1), session.executionDiagnostics().?.execution.?.runtime_region_plan_dispatches);
        for (held.outputs, next.outputs) |old, current| {
            const previous = try held.backend.toFloat32(old, a);
            defer a.free(previous);
            const actual = try next.backend.toFloat32(current, a);
            defer a.free(actual);
            try std.testing.expectEqualSlices(f32, previous, actual);
        }
    }
    // Public host publication and already-resident inputs must use the same
    // Source preparation owner; neither may silently bypass its packed claim.
    {
        const outputs = try session.run(&.{input}, a);
        defer {
            for (outputs) |*output| output.deinit();
            a.free(outputs);
        }
        try std.testing.expectEqual(@as(u64, 1), session.executionDiagnostics().?.execution.?.runtime_region_plan_dispatches);
        try std.testing.expectEqual(@as(u64, head.covered_count), session.executionDiagnostics().?.execution.?.runtime_region_plan_covered_nodes);
        for (outputs, 0..) |output, index| for (output.asFloat32(), &values) |got, value| {
            try std.testing.expectApproxEqAbs(value * @as(f32, @floatFromInt(index + 1)), got, 0.0001);
        };
    }
    {
        const resident = try target.cb.ensureDeviceResidentOwned(try target.cb.fromFloat32Shape(&values, &.{ rows, width }));
        defer target.cb.free(resident);
        var outputs = (try session.runResidentInputs(&.{.{ .value = resident, .backend = &target.cb, .estimated_bytes = @sizeOf(@TypeOf(values)), .shape = &.{ rows, width } }}, a)) orelse return error.MissingResidentResult;
        defer outputs.deinit();
        try std.testing.expectEqual(@as(u64, 1), session.executionDiagnostics().?.execution.?.runtime_region_plan_dispatches);
        try std.testing.expectEqual(@as(u64, head.covered_count), session.executionDiagnostics().?.execution.?.runtime_region_plan_covered_nodes);
        for (outputs.outputs, 0..) |output, index| {
            const actual = try outputs.backend.toFloat32(output, a);
            defer a.free(actual);
            for (actual, &values) |got, value| try std.testing.expectApproxEqAbs(value * @as(f32, @floatFromInt(index + 1)), got, 0.0001);
        }
    }
    try std.testing.expectEqual(warm_cache.backend_weight_bytes, cache.admissionAmounts().backend_weight_bytes);
    try std.testing.expectEqual(warm_cache.host_weight_bytes, cache.admissionAmounts().host_weight_bytes);
    // Compatibility and assigned calls consume exactly the same physical
    // packed payload. The ordinary callback retains its original selector.
    {
        const cached = compute.packed_projection_entries orelse return error.MissingPackedProjectionEntry;
        const current_runtime = compute.provider_impl.raw_decode_runtime;
        try std.testing.expectEqual(@as(c_int, 3), metal.antfly_metal_packed_projection_is_cached(current_runtime, cached.owner.entry));
        const resident = try target.cb.ensureDeviceResidentOwned(try target.cb.fromFloat32Shape(&values, &.{ rows, width }));
        defer target.cb.free(resident);
        const pool = target.executable.runtime.physical_pool.?;
        const immutable = pool.immutable_bindings.?;
        var weights: [3]@import("../ops/ops.zig").CT = undefined;
        for (head.weights[0..head.count], 0..) |node, index| weights[index] = (immutable.originalTensor(node) orelse return error.MissingProjectionWeight).value;
        var sink = try session.model_lifetime.?.retainRetirementSink();
        defer sink.release();
        var guard = try sink.enter();
        defer guard.finish();
        const owner_references = cached.owner.references;
        var operation = try @import("../execution/backend_operation.zig").enter(target.cb, target.executable.graph, null, .managed);
        defer operation.deinitChecked() catch platform.inference_process_supervisor.restartWorker();
        try std.testing.expect(try operation.bindCleanupEpoch(&guard.epoch));
        var generic_qkv: ?@import("../ops/ops.zig").LinearNoBiasTripleResult = null;
        defer if (generic_qkv) |triple| {
            target.cb.free(triple.first);
            target.cb.free(triple.second);
            target.cb.free(triple.third);
        };
        var frame = try metal.AssignedDenseFrame.begin(current_runtime);
        defer frame.drain() catch platform.inference_process_supervisor.restartWorker();
        var outputs: [3]@import("../ops/ops.zig").CT = undefined;
        if (comptime count == 2) {
            const pair = try target.cb.linearNoBiasPair(resident, weights[0], weights[1], rows, width, width);
            outputs[0] = pair.first;
            outputs[1] = pair.second;
        } else {
            // Metal intentionally leaves this generic hook absent: its public
            // fallback is Q + a KV pair, which does NOT read the triple pack.
            try std.testing.expect(target.cb.vtable.linearNoBiasQkv == null);
            generic_qkv = try target.cb.linearNoBiasQkv(resident, weights[0], weights[1], weights[2], rows, width, width, width);
            try std.testing.expectEqual(owner_references, cached.owner.references);
            // This is the actual grouped-QKV compatibility consumer used by
            // the production Metal partition executor, with its real slots.
            const triple = (try target.cb.decoderRuntimeApplyLinearQkv(&.{
                .q_slot = request.slots[0],
                .k_slot = request.slots[1],
                .v_slot = request.slots[2],
                .input = resident,
                .in_dim = request.width,
                .q_out_dim = request.widths[0],
                .kv_out_dim = request.widths[1],
            })) orelse return error.MissingGroupedCompatibilityQkv;
            outputs[0] = triple.first;
            outputs[1] = triple.second;
            outputs[2] = triple.third;
        }
        defer for (outputs[0..count]) |output| target.cb.free(output);
        try frame.finish();
        try operation.finish();
        try std.testing.expectEqual(owner_references + 1, cached.owner.references);
        guard.finish();
        try std.testing.expectEqual(owner_references, cached.owner.references);
        try std.testing.expectEqual(@as(c_int, 3), metal.antfly_metal_packed_projection_is_cached(current_runtime, cached.owner.entry));
        if (generic_qkv) |triple| for ([_]@import("../ops/ops.zig").CT{ triple.first, triple.second, triple.third }, 0..) |output, index| {
            const actual = try target.cb.toFloat32(output, a);
            defer a.free(actual);
            for (actual, &values) |got, value| try std.testing.expectApproxEqAbs(value * @as(f32, @floatFromInt(index + 1)), got, 0.0001);
        };
        for (outputs[0..count], 0..) |output, index| {
            const actual = try target.cb.toFloat32(output, a);
            defer a.free(actual);
            for (actual, &values) |got, value| try std.testing.expectApproxEqAbs(value * @as(f32, @floatFromInt(index + 1)), got, 0.0001);
        }
    }
    // An unused pre-entry Claim owns only exact metadata/pins. Wrong graphs
    // and copied backend tables refuse before opaque/SDK access; cancellation
    // drains only its own newly opened empty frame.
    {
        const compute_mod = @import("../ops/metal_compute.zig");
        var copied_table = target.cb.vtable.*;
        std.mem.doNotOptimizeAway(&copied_table);
        try std.testing.expect(@intFromPtr(&copied_table) != @intFromPtr(target.cb.vtable));
        const foreign = @import("../ops/ops.zig").ComputeBackend{ .ptr = @ptrFromInt(16), .vtable = &copied_table };
        try std.testing.expectError(error.ForeignPreparedProjectionBackend, compute_mod.MetalCompute.claimPreparedProjectionProfile(&foreign, target.executable.graph, request));
        var sink = try session.model_lifetime.?.retainRetirementSink();
        defer sink.release();
        var guard = try sink.enter();
        defer guard.finish();
        var claim = (try session.prepareProjectionPackBeforeExecution(&target.cb, target.executable.graph, request, .{ .boundary = &guard.epoch }, null)) orelse return error.MissingPreparedProjectionProfile;
        defer claim.deinitChecked() catch platform.inference_process_supervisor.restartWorker();
        const profile_request = @import("../execution/region_execution_profile.zig").Request{ .operation = .graph_execution, .implementation = .{ .context = target.cb.ptr, .table = target.cb.vtable }, .program = target.executable.graph };
        var other_graph_request = profile_request;
        other_graph_request.program = &graph;
        try std.testing.expectError(error.RegionExecutionProfileChanged, claim.validate(other_graph_request));
        try std.testing.expect(compute.provider_impl.linear_slot_index_mutex.tryLock());
        const contended_profile = claim.validate(profile_request);
        compute.provider_impl.linear_slot_index_mutex.unlock();
        try std.testing.expectError(error.RegionExecutionProfileChanged, contended_profile);
        try claim.validate(profile_request);
        try std.testing.expect(try claim.ensureScope(profile_request));
        try claim.bindOwnedCleanupEpoch(&guard.epoch);
        try claim.deinitChecked();
        try std.testing.expect(!metal.hasActiveFrame(compute.provider_impl.raw_decode_runtime) and !metal.hasSubmittedFrame(compute.provider_impl.raw_decode_runtime));
    }
    // A second genuine component loader shares this manager's Domain and
    // actual compute context. The same binding is reported by both Models;
    // conservation counts that exact independently charged ledger once.
    for (0..count) |index| for (0..width) |column| {
        matrices[index][column * width + column] = @floatFromInt(index + 2);
    };
    const replacement_bytes = try onnx_graph.exportGraph(a, &graph, .{ .parameter_initializers = &initializers });
    defer a.free(replacement_bytes);
    try dir.dir.writeFile(std.testing.io, .{ .sub_path = "replacement.onnx", .data = replacement_bytes });
    const replacement_path = try std.fs.path.join(a, &.{ root, "replacement.onnx" });
    defer a.free(replacement_path);
    var second = try manager.loadManagedSessionWithAdmission(replacement_path, &.{.metal}, impl.shared_backend_ctx, null, .{ .path = root, .publication = try managed_receipt.publicationGeneration(a, std.testing.io, root) }, null);
    var second_open = true;
    defer if (second_open) second.deinit();
    const second_admission = second.session.run_admission orelse return error.MissingRunAdmission;
    const second_pin = second.session.model_lifetime.?.retainAdmissionDomain(second_admission.controller) orelse return error.MissingCacheRetirementDomain;
    defer second_pin.release();
    try std.testing.expect(second_pin.controller() == controller);
    const first_snapshot = @import("../execution/architecture_session.zig").sharedCacheAdmissionSnapshot(session) orelse return error.MissingImportedManagedCache;
    const second_snapshot = @import("../execution/architecture_session.zig").sharedCacheAdmissionSnapshot(second.session) orelse return error.MissingImportedManagedCache;
    try std.testing.expectEqual(first_snapshot.identity, second_snapshot.identity);
    try std.testing.expectEqualDeep(first_snapshot.amounts, second_snapshot.amounts);
    const first_runtime = handle.get().runtime_id;
    handle.release();
    handle_open = false;
    try std.testing.expectEqual(UnloadRuntimeResult.unloaded, manager.unloadRuntime(first_runtime));
    try std.testing.expect(controller.snapshot().backend_weight_bytes > 0);
    for (held.outputs, 0..) |output, index| {
        const actual = try held.backend.toFloat32(output, a);
        defer a.free(actual);
        for (actual, &values) |got, value| try std.testing.expectApproxEqAbs(value * @as(f32, @floatFromInt(index + 1)), got, 0.0001);
    }
    held.deinit();
    held_open = false;
    _ = @import("../execution/resource_retirement.zig").poll();
    // The original public model and every output are gone, but its exact
    // packed charge belongs to the still-live shared context and Domain.
    try std.testing.expect(controller.snapshot().backend_weight_bytes > 0);
    // A distinct Model can need one compatibility call for its ordinary source
    // identities, then its actual Source service must use the bounded pack.
    for (0..2) |iteration| {
        var next = (try second.session.runResident(&.{input}, a)) orelse return error.MissingResidentResult;
        defer next.deinit();
        if (iteration == 1) try std.testing.expectEqual(@as(u64, 1), second.session.executionDiagnostics().?.execution.?.runtime_region_plan_dispatches);
        for (next.outputs, 0..) |output, index| {
            const actual = try next.backend.toFloat32(output, a);
            defer a.free(actual);
            for (actual, &values) |got, value| try std.testing.expectApproxEqAbs(value * @as(f32, @floatFromInt(index + 2)), got, 0.0001);
        }
    }
    const surviving_snapshot = @import("../execution/architecture_session.zig").sharedCacheAdmissionSnapshot(second.session) orelse return error.MissingImportedManagedCache;
    try std.testing.expectEqual(first_snapshot.identity, surviving_snapshot.identity);
    try std.testing.expect(surviving_snapshot.amounts.backend_weight_bytes > 0);
    second.deinit();
    second_open = false;
    manager.deinit();
    manager_open = false;
    _ = @import("../execution/resource_retirement.zig").poll();
    try std.testing.expectEqual(@import("../runtime/tier/memory.zig").AdmissionAmounts{}, controller.snapshot());
    try std.testing.expectEqual(@import("../runtime/tier/memory.zig").AdmissionAmounts{}, second_pin.controller().snapshot());
}

test "managed Imported Metal packed pair cold service owns direct reduce roots outputs and Model close" {
    try checkManagedPackedProjectionHead(2, 3);
}
test "managed Imported Metal packed QKV cold service owns direct tiled roots outputs and Model close" {
    try checkManagedPackedProjectionHead(3, 128);
}

test "direct Imported component floors use file classification and preserve explicit hard caps" {
    const a = std.testing.allocator;
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var manager = ModelManager.init(failing.allocator(), .{ .allocator = failing.allocator(), .preferred_backends = &.{.metal} });
    defer manager.deinit();
    const memory = runtime.tier.memory;
    const backend = backends.BackendRuntime{ .backend = .metal };
    const floor = memory.Limits{ .host_limit_bytes = 4096, .backend_limit_bytes = 8192, .combined_limit_bytes = 12288 };
    manager.admission_limit_overrides = .{ .host_limit_bytes = 1024, .backend_limit_bytes = 2048, .combined_limit_bytes = 3072 };
    const expected = backends.clampPhysicalMemoryLimits(backend.backend, memory.applyLimitOverrides(memory.maxCompositeLimits(backends.defaultMemoryLimits(backend.backend, manager.process_memory_limit_bytes), floor), manager.admission_limit_overrides), manager.process_memory_limit_bytes);
    // The artifact estimator validates/maps the real ONNX before this policy
    // callback. Policy must neither reopen a file as a directory nor allocate
    // an architecture manifest; the genuine public component case is above.
    const actual = try manager.admissionLimitsForModelSource(.{ .path = "direct-component.onnx", .kind = .onnx }, backend, floor);
    try std.testing.expectEqualDeep(expected, actual);
    try std.testing.expectEqual(@as(usize, 1024), actual.host_limit_bytes);
    try std.testing.expectEqual(@as(usize, 2048), actual.backend_limit_bytes);
    try std.testing.expectEqual(@as(usize, 3072), actual.combined_limit_bytes);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expect(backends.isOnnxFilePath("direct-component.onnx"));
    try std.testing.expect(!backends.isOnnxFilePath("native-model"));
}

test "model selection resident sessions and load flights fence receipt generations" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/model.gguf", .data = "weights" });
    const directory = try tmp.dir.realPathFileAlloc(io, "model", a);
    defer a.free(directory);
    try tmp.dir.writeFile(io, .{ .sub_path = "model/.antfly-download-complete.json", .data = "{\"version\":2,\"source\":{\"owner\":\"acme\",\"name\":\"model\",\"variant\":\"gguf:Q4_0\",\"resolved_revision\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"},\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7}]}" });
    var manager = ModelManager.init(a, .{ .allocator = a, .io = io, .preferred_backends = &.{.metal} });
    defer manager.deinit();
    manager.configureAdmissionLimits(.{ .host_limit_bytes = 256 });
    try manager.ensureResourceOwnerReady();
    const first_publication = try managed_receipt.publicationGeneration(a, io, directory);
    var old_probe = TeardownTestProbe{};
    const old = try teardownTestModelWithHostLimit(&manager, &old_probe, 256);
    old_probe.controller = null; // Several protected fixture sessions coexist.
    a.free(old.model_dir);
    old.model_dir = try a.dupe(u8, directory);
    old.managed_publication = first_publication;
    var original = try manager.publishLoadedModel(old, true, null, null, 0);
    defer original.release();
    const old_runtime = original.get().runtime_id;
    var reused = try manager.acquireFromDirWithControl(directory, .{});
    reused.release();
    try tmp.dir.writeFile(io, .{ .sub_path = "model/.antfly-download-complete.json", .data = "{\"version\":2,\"source\":{\"owner\":\"acme\",\"name\":\"model\",\"variant\":\"gguf:Q4_0\",\"resolved_revision\":\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"},\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7}]}" });
    const next_publication = try managed_receipt.publicationGeneration(a, io, directory);
    try std.testing.expectError(error.ModelArtifactsChanging, manager.acquireFromDirWithControl(directory, .{}));
    try std.testing.expect(original.get().retired);
    const first_flight = try manager.loadFlightKey(directory, &.{.metal}, true, .{ .managed_publication = first_publication });
    defer a.free(first_flight);
    const next_flight = try manager.loadFlightKey(directory, &.{.metal}, true, .{ .managed_publication = next_publication });
    defer a.free(next_flight);
    try std.testing.expect(!std.mem.eql(u8, first_flight, next_flight));
    var new_probe = TeardownTestProbe{};
    const replacement = try teardownTestModelWithHostLimit(&manager, &new_probe, 256);
    new_probe.controller = null; // Several protected fixture sessions coexist.
    a.free(replacement.model_dir);
    replacement.model_dir = try a.dupe(u8, directory);
    replacement.managed_publication = next_publication;
    var fresh = try manager.publishLoadedModel(replacement, true, null, null, 0);
    defer fresh.release();
    try std.testing.expect(fresh.get().runtime_id != old_runtime);
    var cached = (try manager.acquireCachedFromDir(directory)).?;
    defer cached.release();
    try std.testing.expectEqual(fresh.get().runtime_id, cached.get().runtime_id);
    var late_probe = TeardownTestProbe{};
    const late = try teardownTestModelWithHostLimit(&manager, &late_probe, 256);
    late_probe.controller = null; // Several protected fixture sessions coexist.
    a.free(late.model_dir);
    late.model_dir = try a.dupe(u8, directory);
    late.managed_publication = first_publication;
    try std.testing.expectError(error.ModelArtifactsChanging, manager.publishLoadedModel(late, true, null, null, 0));
    try tmp.dir.writeFile(io, .{ .sub_path = "model/.antfly-download-complete.json", .data = "{\"version\":2,\"source\":{\"owner\":\"acme\",\"name\":\"model\",\"variant\":\"gguf:Q4_0\",\"resolved_revision\":\"cccccccccccccccccccccccccccccccccccccccc\"},\"artifacts\":[{\"path\":\"model.gguf\",\"size\":7}]}" });
    try std.testing.expect(manager.acquireLoadedModel(directory) == null);
    try std.testing.expect(fresh.get().retired);
}

test "model selection runtime snapshots carry composite asset and flight publications" {
    const a = std.testing.allocator;
    var manager = ModelManager.init(a, backends.SessionManager.init(a));
    defer manager.deinit();
    const asset_publication: [32]u8 = @splat(0x41);
    const flight_publication: [32]u8 = @splat(0x42);
    var assets = CompositeAssets{ .runtime_id = 17, .model_dir = @constCast("/models/transcribers/loaded"), .managed_publication = asset_publication, .managed_tokenizer = undefined, .prompt_cache = null, .decoder_config = .{}, .generation = @splat(1) };
    const key: ComponentPlanKey = @splat(1);
    try manager.composite_assets.put(a, key, &assets);
    defer _ = manager.composite_assets.remove(key);
    var flight = CompositeAssetsLoadFlight{ .runtime_id = 18, .model_dir = @constCast("/models/transcribers/loading"), .managed_publication = flight_publication, .io = std.testing.io, .load_state = .{ .io = std.testing.io } };
    const flight_key: ComponentPlanKey = @splat(2);
    try manager.in_flight_composite_assets.put(a, flight_key, &flight);
    defer _ = manager.in_flight_composite_assets.remove(flight_key);
    var snapshot = try manager.runtimeSnapshot(a, null);
    defer snapshot.deinit();
    try std.testing.expectEqual(@as(usize, 2), snapshot.entries.len);
    for (snapshot.entries) |entry| {
        try std.testing.expectEqual(RuntimeKind.composite, entry.runtime_kind);
        const expected = if (entry.runtime_id == 17) asset_publication else flight_publication;
        try std.testing.expectEqual(expected, entry.managed_publication.?);
    }
}

test "model selection lazy optional component retains primary publication" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const gguf = try std.fs.path.join(a, &.{ root, "model.gguf" });
    defer a.free(gguf);
    const size = (try tmp.dir.statFile(io, "model.gguf", .{})).size;
    const receipt_a = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .source = .{ .owner = "acme", .name = "model", .variant = "gguf", .resolved_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, .artifacts = .{.{ .path = "model.gguf", .size = size }} }, .{});
    defer a.free(receipt_a);
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt_a });
    var original_manifest = try manifest_mod.loadFromDir(a, root);
    defer original_manifest.deinit();
    const publication_a = try managed_receipt.publicationGeneration(a, io, root);
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    var primary = try manager.loadManagedSessionWithAdmission(root, &.{.native}, null, null, .{ .path = root, .publication = try managed_receipt.publicationGeneration(a, std.testing.io, root) }, null);
    defer primary.deinit();
    var model: LoadedModel = undefined;
    model.managed_publication = publication_a;
    model.model_dir = root;
    model.allocator = a;
    model.session = primary.session;
    model.session_manager = &manager.session_manager;
    model.host = manager.loadedRuntimeHost();
    model.kernel_jit_profile_bundle = null;
    model.manifest = .{ .allocator = a, .gguf_projector_path = gguf };
    model.projector_store_mutex = .unlocked;
    model.projector_store = null;
    const old_selection = @import("../registry/model_selection.zig").ResolvedModel{ .path = root, .publication = publication_a };
    const control = old_selection.bind(InferenceExecutionControl{ .io = io, .native_call_policy = .in_process });
    var existing: ?backends.Session = null;
    var existing_lease: ?runtime.tier.memory.AdmissionLease = null;
    defer if (existing_lease) |*held| held.release();
    defer if (existing) |session| session.close();
    try std.testing.expect(try model.ensureOptionalSessionWithControl(.text_projection, &existing, &existing_lease, root, control));
    const previous_session = existing.?.ptr;
    const bytes = try std.Io.Dir.cwd().readFileAlloc(io, gguf, a, .limited(1024 * 1024));
    defer a.free(bytes);
    std.mem.writeInt(u32, bytes[bytes.len - 4 ..][0..4], @bitCast(@as(f32, 1.0)), .little);
    try @import("../registry/download.zig").writeFileAtomically(a, io, gguf, bytes);
    const receipt_b = try a.dupe(u8, receipt_a);
    defer a.free(receipt_b);
    const pin = std.mem.indexOf(u8, receipt_b, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").?;
    @memset(receipt_b[pin..][0..40], 'b');
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt_b });
    try std.testing.expect(!std.meta.eql(publication_a, try managed_receipt.publicationGeneration(a, io, root)));
    if (manager.loadArtifactSession(root, original_manifest)) |loaded| {
        var unexpected = loaded;
        unexpected.deinit();
        return error.TestExpectedError;
    } else |err| try std.testing.expectEqual(error.ModelArtifactsChanging, err);
    if (manager.loadGenerationTokenizerAssets(root, &original_manifest)) |loaded| {
        var unexpected = loaded;
        unexpected.deinit();
        return error.TestExpectedError;
    } else |err| try std.testing.expectEqual(error.ModelArtifactsChanging, err);
    // Already materialized A remains safe for the request that owns A.
    try std.testing.expect(!try model.ensureOptionalSessionWithControl(.text_projection, &existing, &existing_lease, root, control));
    try std.testing.expect(existing.?.ptr == previous_session);
    try std.testing.expectError(error.ModelArtifactsChanging, model.ensureProjectorStoreWithControl(control));
    var slot: ?backends.Session = null;
    var lease: ?runtime.tier.memory.AdmissionLease = null;
    defer if (lease) |*held| held.release();
    defer if (slot) |session| session.close();
    try std.testing.expectError(error.ModelArtifactsChanging, model.ensureOptionalSessionWithControl(.text_projection, &slot, &lease, root, control));
}

test "shared model load admission combined cap spans CPU and GPU on every platform" {
    const a = std.testing.allocator;
    const mib: usize = 1024 * 1024;
    const limits = runtime.tier.memory.Limits{ .host_limit_bytes = 8 * mib, .backend_limit_bytes = 8 * mib, .combined_limit_bytes = 8 * mib };
    var resources = try ModelManager.SharedResources.init(a, limits);
    defer resources.deinit();
    var gpu = try resources.domain.admission.tryAcquire(.gpu, limits, .{ .backend_weight_bytes = 5 * mib }, false);
    defer gpu.release();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, resources.domain.admission.tryAcquire(.cpu, limits, .{ .host_weight_bytes = 5 * mib }, false));
    gpu.release();
    var cpu = try resources.domain.admission.tryAcquire(.cpu, limits, .{ .host_weight_bytes = 5 * mib }, false);
    cpu.release();
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, resources.domain.admission.snapshot());
}

test "shared model load admission artifact sessions do not construct a second tokenizer" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    // Session-only preparation must work with no tokenizer file and a cap
    // smaller than the tokenizer construction envelope.
    try tmp.dir.deleteFile(std.testing.io, "tokenizer.json");
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    const limits = runtime.tier.memory.Limits{ .host_limit_bytes = 3 * 1024 * 1024, .combined_limit_bytes = 3 * 1024 * 1024 };
    var resources = try ModelManager.SharedResources.init(a, limits);
    defer resources.deinit();
    var manager = ModelManager.init(a, backends.SessionManager.init(a));
    defer manager.deinit();
    try (ModelLoadPolicy{ .limits = limits, .resources = &resources }).configure(&manager);
    {
        var session = try manager.loadArtifactSession(root, man);
        defer session.deinit();
        try std.testing.expectEqual(backends.BackendType.native, session.session.backend());
        try std.testing.expect(manager.currentAdmissionAmounts().hostTotalBytes() > 0);
        try std.testing.expectEqual(@as(usize, 0), manager.loaded.count());
    }
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
}

test "model selection composite ignores unlisted merged decoders and preserves listed optimization" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const left = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const bias = try builder.tensorConst(&.{ 1, 1, 1, 1 }, ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try builder.add(left, bias));
    const ordinary = try onnx_graph.exportGraph(a, &graph, .{});
    defer a.free(ordinary);
    for ([_][]const u8{ "encoder_model.onnx", "decoder_model.onnx" }) |name| try tmp.dir.writeFile(io, .{ .sub_path = name, .data = ordinary });
    var merged = ml.graph.Graph.init(a);
    defer merged.deinit();
    var b = ml.graph.Builder.init(&merged);
    const ids = try b.parameter("input_ids", ml.graph.Shape.init(.i64, &.{ 1, -1 }));
    const encoder = try b.parameter("encoder_hidden_states", ml.graph.Shape.init(.f32, &.{ 1, -1, 4 }));
    const branch = try b.parameter("use_cache_branch", ml.graph.Shape.init(.bool_, &.{1}));
    const past = try b.parameter("past_key_values.0.decoder.key", ml.graph.Shape.init(.f32, &.{ 1, 1, -1, 4 }));
    const ids_sum = try b.reduceSum(try b.convertDtype(ids, .f32), &.{ 0, 1 });
    const branch_sum = try b.reduceSum(try b.convertDtype(branch, .f32), &.{0});
    const logits = try b.add(encoder, try b.add(ids_sum, branch_sum));
    const present = try b.add(past, past);
    try merged.markOutput(logits);
    try merged.markOutput(present);
    const merged_bytes = try onnx_graph.exportGraph(a, &merged, .{ .node_name_overrides = &.{ .{ .node_id = logits, .name = "logits" }, .{ .node_id = present, .name = "present.0.decoder.key" } } });
    defer a.free(merged_bytes);
    try tmp.dir.writeFile(io, .{ .sub_path = "decoder_model_merged.onnx", .data = merged_bytes });
    const config = "{\"model_type\":\"t5\",\"vocab_size\":4,\"d_model\":4,\"decoder_start_token_id\":0}";
    const tokenizer = "{\"version\":\"1.0\",\"added_tokens\":[{\"id\":0,\"content\":\"<unk>\"}],\"model\":{\"type\":\"BPE\",\"vocab\":{\"<unk>\":0},\"merges\":[]}}";
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = config });
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = tokenizer });
    const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .source = .{ .owner = "acme", .name = "seq2seq", .variant = "onnx", .resolved_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, .artifacts = .{ .{ .path = "encoder_model.onnx", .size = ordinary.len }, .{ .path = "decoder_model.onnx", .size = ordinary.len }, .{ .path = "config.json", .size = config.len }, .{ .path = "tokenizer.json", .size = tokenizer.len } } }, .{});
    defer a.free(receipt);
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var artifacts = try @import("../registry/published_artifacts.zig").PublishedArtifacts.init(a, io, root, null);
    defer artifacts.deinit();
    try std.testing.expect((try artifacts.resolve("decoder_model_merged.onnx")) == null);
    try std.testing.expect((try encoder_decoder.findMergedDecoder(a, root)) == null);
    const raw = try tmp.dir.realPathFileAlloc(io, "decoder_model_merged.onnx", a);
    defer a.free(raw);
    var signature_arena = std.heap.ArenaAllocator.init(a);
    defer signature_arena.deinit();
    const signature = try backends.imported_onnx_session.inspectSignature(signature_arena.allocator(), raw);
    try std.testing.expect(@import("../pipelines/seq2seq_decode.zig").qualifiedSignature(signature.inputs, signature.outputs));
    const paths = try encoder_decoder.findEncoderDecoderPaths(a, root);
    defer a.free(paths.encoder);
    defer a.free(paths.decoder);
    var sessions = backends.SessionManager.initWithIo(a, io);
    sessions.preferred_backends = &.{.native};
    sessions.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sessions);
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    const selected = @import("../registry/model_selection.zig").ResolvedModel{ .path = root, .publication = artifacts.publication };
    var handle = try manager.acquireCompositeRuntime(root, &.{ paths.encoder, paths.decoder }, .seq2seq, selected.bind(InferenceExecutionControl{ .io = io, .request_owned = false, .native_call_policy = .in_process }));
    defer handle.release();
    try std.testing.expectEqual(.full_prefix, handle.get().decoder_execution);
    try std.testing.expect(!handle.get().considered_merged_decoder);
    try manager.validateCompositeAssetsCurrent(&handle, root, &.{ paths.encoder, paths.decoder });
    // Publishing a receipt that includes the optional graph enables it.
    var merged_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(merged_bytes, &merged_digest, .{});
    const expanded = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .source = .{ .owner = "acme", .name = "seq2seq", .variant = "onnx", .resolved_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, .artifacts = .{ .{ .path = "encoder_model.onnx", .size = ordinary.len }, .{ .path = "decoder_model.onnx", .size = ordinary.len }, .{ .path = "decoder_model_merged.onnx", .size = merged_bytes.len, .sha256 = std.fmt.bytesToHex(merged_digest, .lower) }, .{ .path = "config.json", .size = config.len }, .{ .path = "tokenizer.json", .size = tokenizer.len } } }, .{});
    defer a.free(expanded);
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = expanded });
    const included = @import("../registry/model_selection.zig").ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
    var optimized = try manager.acquireCompositeRuntime(root, &.{ paths.encoder, paths.decoder }, .seq2seq, included.bind(InferenceExecutionControl{ .io = io, .request_owned = false, .native_call_policy = .in_process }));
    defer optimized.release();
    try std.testing.expectEqual(.merged_kv, optimized.get().decoder_execution);
    try manager.validateCompositeAssetsCurrent(&optimized, root, &.{ paths.encoder, paths.decoder });
    // A listed but corrupt optional graph must fail closed, not silently fall
    // back to an ordinary decoder under the same receipt.
    const corrupt = try a.dupe(u8, merged_bytes);
    defer a.free(corrupt);
    corrupt[0] ^= 1;
    try @import("../registry/download.zig").writeFileAtomically(a, io, raw, corrupt);
    try std.testing.expectError(error.ChecksumMismatch, manager.acquireCompositeRuntime(root, &.{ paths.encoder, paths.decoder }, .seq2seq, included.bind(InferenceExecutionControl{ .io = io, .request_owned = false, .native_call_policy = .in_process })));
}

test "host admission reclaims idle VAD residency but preserves active owners" {
    const a = std.testing.allocator;
    var node = try @import("server.zig").Node.init(a, .{});
    defer node.deinit();
    const silero = @import("../pipelines/silero_vad.zig");
    const owner = try a.create(silero.SharedWeights);
    owner.* = .{ .allocator = a, .weights = .{ .allocator = a, .stft_basis = &.{}, .encoder = @splat(.{ .weight = &.{}, .bias = &.{}, .in_channels = 0, .out_channels = 0, .stride = 1 }), .lstm_w = &.{}, .lstm_r = &.{}, .lstm_b = &.{}, .decoder_weight = &.{}, .decoder_bias = 0 } };
    const resident = silero.residentWeightBytes(&owner.weights);
    const limit = resident * 2;
    node.model_manager.configureAdmissionLimits(.{ .host_limit_bytes = limit });
    try node.model_manager.ensureResourceOwnerReady();
    owner.admission = try node.model_manager.admissionController().tryAcquire(.cpu, node.model_manager.resource_domain.?.admission_limits, .{ .host_weight_bytes = resident }, false);
    try node.host_caches.silero_weights.entries.append(a, .{ .path = try a.dupe(u8, "model"), .publication = null, .owner = owner, .used = 0 });
    try std.testing.expectEqual(@as(usize, 1), owner.refs.load(.acquire));
    // An active request/streaming session prevents reclamation.
    owner.retain();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, node.model_manager.admitHostPreflightChecked(limit, false));
    owner.release();
    // Admission callbacks must not wait on a concurrent cache user.
    try std.testing.expect(node.host_caches.silero_weights_lock.tryLock());
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, node.model_manager.admitHostPreflightChecked(limit, false));
    node.host_caches.silero_weights_lock.unlock();
    var retry = try node.model_manager.admitHostPreflightChecked(limit, false);
    retry.release();
    try std.testing.expectEqual(@as(usize, 0), node.host_caches.silero_weights.entries.items.len);
}

test "model selection unchanged preparation permits warm execution metadata" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model");
    var dir = try tmp.dir.openDir(io, "model", .{ .iterate = true });
    defer dir.close(io);
    try writeCancellationTestModel(dir, a);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const Artifact = struct { path: []const u8, size: u64 };
    var files: std.ArrayListUnmanaged(Artifact) = .empty;
    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        try files.append(alloc, .{ .path = try alloc.dupe(u8, entry.name), .size = (try dir.statFile(io, entry.name, .{})).size });
    }
    const receipt = try std.json.Stringify.valueAlloc(alloc, .{ .version = @as(u32, 2), .source = .{ .owner = "acme", .name = "model", .variant = "gguf", .resolved_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, .artifacts = files.items }, .{});
    try dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const path = try tmp.dir.realPathFileAlloc(io, "model", a);
    defer a.free(path);
    var node = try @import("server.zig").Node.init(a, .{ .models_dir = root });
    defer node.deinit();
    node.model_manager.session_manager.preferred_backends = &.{.native};
    const selected = @import("../registry/model_selection.zig").ResolvedModel{ .path = path, .publication = try managed_receipt.publicationGeneration(a, io, path) };
    const control = selected.bind(InferenceExecutionControl{ .io = io, .request_owned = false, .native_call_policy = .in_process });
    var model = try node.model_manager.acquireFromDirWithControl(path, control);
    defer model.release();
    var before = try node.manifestForSelection(a, selected);
    before.deinit();
    var publication = try @import("../registry/catalog_publication.zig").Publication.prepare(a, io, root, "model");
    defer publication.deinit();
    try std.testing.expect(!publication.started);
    _ = try @import("../registry/catalog_publication.zig").read(a, io, root);
    var warm = (try node.model_manager.acquireLoadedModelWithControl(path, control)).?;
    defer warm.release();
    try std.testing.expect(warm.get() == model.get());
    var during = try node.manifestForSelection(a, selected);
    defer during.deinit();
    try std.testing.expectEqual(selected.publication, during.managed_publication);
}

test "model selection primary ONNX requires receipted external weights and verifies their checksum" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const weight = try builder.parameter("weight", ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try builder.add(input, weight));
    const values = [_]f32{ 1, 2, 3, 4 };
    var exported = try onnx_graph.exportGraphWithExternalData(a, &graph, .{ .parameter_initializers = &.{.{ .name = "weight", .shape = ml.graph.Shape.init(.f32, &.{4}), .data = .{ .raw_bytes = std.mem.asBytes(&values) } }} }, "weights.data");
    defer exported.deinit(a);
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = exported.model_bytes });
    try tmp.dir.writeFile(io, .{ .sub_path = "weights.data", .data = exported.external_data.?.bytes });
    const config = "{\"model_type\":\"bert\",\"hidden_size\":4,\"num_attention_heads\":1,\"vocab_size\":4}";
    const tokenizer = "{\"version\":\"1.0\",\"added_tokens\":[{\"id\":0,\"content\":\"<unk>\"}],\"model\":{\"type\":\"BPE\",\"vocab\":{\"<unk>\":0},\"merges\":[]}}";
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = config });
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = tokenizer });
    const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .source = .{ .owner = "acme", .name = "model", .variant = "onnx", .resolved_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, .artifacts = .{ .{ .path = "model.onnx", .size = exported.model_bytes.len }, .{ .path = "config.json", .size = config.len }, .{ .path = "tokenizer.json", .size = tokenizer.len } } }, .{});
    defer a.free(receipt);
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var artifacts = try @import("../registry/published_artifacts.zig").PublishedArtifacts.init(a, io, root, null);
    defer artifacts.deinit();
    try std.testing.expect((try artifacts.resolve("weights.data")) == null);
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    const selected = @import("../registry/model_selection.zig").ResolvedModel{ .path = root, .publication = artifacts.publication };
    const control = selected.bind(InferenceExecutionControl{ .io = io, .request_owned = false, .native_call_policy = .in_process });
    try std.testing.expectError(error.InvalidManagedDownload, manager.acquireFromDirWithControl(root, control));
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(exported.external_data.?.bytes, &digest, .{});
    const expanded = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .source = .{ .owner = "acme", .name = "model", .variant = "onnx", .resolved_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, .artifacts = .{ .{ .path = "model.onnx", .size = exported.model_bytes.len }, .{ .path = "weights.data", .size = exported.external_data.?.bytes.len, .sha256 = std.fmt.bytesToHex(digest, .lower) }, .{ .path = "config.json", .size = config.len }, .{ .path = "tokenizer.json", .size = tokenizer.len } } }, .{});
    defer a.free(expanded);
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = expanded });
    const included = @import("../registry/model_selection.zig").ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
    const included_control = included.bind(InferenceExecutionControl{ .io = io, .request_owned = false, .native_call_policy = .in_process });
    // A fresh manager ensures the success path performs cold construction.
    var verified_manager = ModelManager.init(a, sm);
    defer verified_manager.deinit();
    verified_manager.configureServingPolicy(.{ .allow_unknown = true });
    var handle = try verified_manager.acquireFromDirWithControl(root, included_control);
    defer handle.release();
    var tensor = try backends.Tensor.initFloat32(a, "input", &.{4}, &.{ 0, 0, 0, 0 });
    defer tensor.deinit();
    const outputs = try handle.get().session.runWithControl(&.{tensor}, a, included_control);
    defer {
        for (outputs) |*output| output.deinit();
        a.free(outputs);
    }
    try std.testing.expectEqualSlices(f32, &values, outputs[0].asFloat32());
    const corrupt = try a.dupe(u8, exported.external_data.?.bytes);
    defer a.free(corrupt);
    corrupt[0] ^= 1;
    try tmp.dir.writeFile(io, .{ .sub_path = "weights.data", .data = corrupt });
    var corrupt_manager = ModelManager.init(a, sm);
    defer corrupt_manager.deinit();
    corrupt_manager.configureServingPolicy(.{ .allow_unknown = true });
    try std.testing.expectError(error.ChecksumMismatch, corrupt_manager.acquireFromDirWithControl(root, included_control));
}

test "model selection lazy ONNX sessions enforce graph and external tensor receipts" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const input = try b.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const weight = try b.parameter("weight", ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try b.add(input, weight));
    const values = [_]f32{ 2, 4, 6, 8 };
    var exported = try onnx_graph.exportGraphWithExternalData(a, &graph, .{ .parameter_initializers = &.{.{ .name = "weight", .shape = ml.graph.Shape.init(.f32, &.{4}), .data = .{ .raw_bytes = std.mem.asBytes(&values) } }} }, "unlisted.data");
    defer exported.deinit(a);
    try tmp.dir.writeFile(io, .{ .sub_path = "optional.onnx", .data = exported.model_bytes });
    try tmp.dir.writeFile(io, .{ .sub_path = "unlisted.data", .data = exported.external_data.?.bytes });
    const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .source = .{ .owner = "acme", .name = "model", .variant = "gguf", .resolved_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, .artifacts = .{ .{ .path = "model.gguf", .size = (try tmp.dir.statFile(io, "model.gguf", .{})).size }, .{ .path = "optional.onnx", .size = exported.model_bytes.len } } }, .{});
    defer a.free(receipt);
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const optional = try tmp.dir.realPathFileAlloc(io, "optional.onnx", a);
    defer a.free(optional);
    const selected = @import("../registry/model_selection.zig").ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
    const control = selected.bind(InferenceExecutionControl{ .io = io, .native_call_policy = .in_process });
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    var primary = try manager.loadManagedSessionWithAdmission(root, &.{.native}, null, control, selected, null);
    defer primary.deinit();
    var model: LoadedModel = undefined;
    model.managed_publication = selected.publication;
    model.model_dir = root;
    model.allocator = a;
    model.session = primary.session;
    model.session_manager = &manager.session_manager;
    model.host = manager.loadedRuntimeHost();
    model.kernel_jit_profile_bundle = null;
    var slot: ?backends.Session = null;
    var lease: ?runtime.tier.memory.AdmissionLease = null;
    defer if (lease) |*held| held.release();
    defer if (slot) |session| session.close();
    const before = manager.currentAdmissionAmounts();
    for ([_]DeclaredOptionalSessionKind{ .vision, .audio, .text_projection, .visual_projection, .audio_projection }) |kind| {
        try std.testing.expectError(error.InvalidManagedDownload, model.ensureOptionalSessionWithControl(kind, &slot, &lease, optional, control));
        try std.testing.expect(slot == null and lease == null);
        try std.testing.expectEqualDeep(before, manager.currentAdmissionAmounts());
    }
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(exported.external_data.?.bytes, &digest, .{});
    const complete = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .source = .{ .owner = "acme", .name = "model", .variant = "gguf", .resolved_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, .artifacts = .{ .{ .path = "model.gguf", .size = (try tmp.dir.statFile(io, "model.gguf", .{})).size }, .{ .path = "optional.onnx", .size = exported.model_bytes.len }, .{ .path = "unlisted.data", .size = exported.external_data.?.bytes.len, .sha256 = std.fmt.bytesToHex(digest, .lower) } } }, .{});
    defer a.free(complete);
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = complete });
    try std.testing.expectError(error.ModelArtifactsChanging, model.ensureOptionalSessionWithControl(.text_projection, &slot, &lease, optional, control));
    const current = ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
    const current_control = current.bind(InferenceExecutionControl{ .io = io, .native_call_policy = .in_process });
    var current_primary = try manager.loadManagedSessionWithAdmission(root, &.{.native}, null, current_control, current, null);
    defer current_primary.deinit();
    model.managed_publication = current.publication;
    model.session = current_primary.session;
    try std.testing.expect(try model.ensureOptionalSessionWithControl(.text_projection, &slot, &lease, optional, current_control));
    var tensor = try backends.Tensor.initFloat32(a, "input", &.{4}, &.{ 0, 0, 0, 0 });
    defer tensor.deinit();
    const outputs = try slot.?.runWithControl(&.{tensor}, a, current_control);
    defer {
        for (outputs) |*output| output.deinit();
        a.free(outputs);
    }
    try std.testing.expectEqualSlices(f32, &values, outputs[0].asFloat32());
    // Bundle loaders reuse checksum proofs across component construction.
    const Opens = struct {
        var count: usize = 0;
        var original: @FieldType(std.Io.VTable, "dirOpenFile") = undefined;
        fn open(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
            if (std.mem.endsWith(u8, path, "unlisted.data")) count += 1;
            return original(raw, dir, path, options);
        }
    };
    var table = io.vtable.*;
    Opens.original = table.dirOpenFile;
    table.dirOpenFile = Opens.open;
    const counted = std.Io{ .userdata = io.userdata, .vtable = &table };
    {
        var published = try PublishedArtifacts.init(a, counted, root, .{ .publication = current.publication });
        defer published.deinit();
        const plan = (try manager.componentLoaderForPaths(root, &.{.native}, &.{optional})).withArtifacts(&published);
        Opens.count = 0;
        for (0..2) |_| {
            var loaded = try plan.loadWithControl(optional, current_control);
            loaded.deinit();
        }
        try std.testing.expectEqual(@as(usize, 1), Opens.count);
    }
    // A lazy pool owns the path/proof and cannot retain a transient resolver.
    var pool_loader = blk: {
        const transient_root = try a.dupe(u8, root);
        defer a.free(transient_root);
        var published = try PublishedArtifacts.init(a, io, root, .{ .publication = current.publication });
        defer published.deinit();
        const plan = (try manager.componentLoaderForPaths(transient_root, &.{.native}, &.{optional})).withArtifacts(&published);
        break :blk try plan.sessionPoolLoader();
    };
    defer pool_loader.deinit();
    var pooled = try pool_loader.load(optional);
    defer pooled.deinit();
    try tmp.dir.writeFile(io, .{ .sub_path = "unlisted.onnx", .data = exported.model_bytes });
    const unlisted = try tmp.dir.realPathFileAlloc(io, "unlisted.onnx", a);
    defer a.free(unlisted);
    var absent: ?backends.Session = null;
    var absent_lease: ?runtime.tier.memory.AdmissionLease = null;
    try std.testing.expectError(error.InvalidManagedDownload, model.ensureOptionalSessionWithControl(.vision, &absent, &absent_lease, unlisted, current_control));
    try std.testing.expect(absent == null and absent_lease == null);
    const corrupt = try a.dupe(u8, exported.external_data.?.bytes);
    defer a.free(corrupt);
    corrupt[0] ^= 1;
    const external_path = try tmp.dir.realPathFileAlloc(io, "unlisted.data", a);
    defer a.free(external_path);
    try @import("../registry/download.zig").writeFileAtomically(a, io, external_path, corrupt);
    // The already materialized generation stays usable; a new cold component
    // cannot adopt bytes that disagree with that generation's receipt.
    try std.testing.expect(!try model.ensureOptionalSessionWithControl(.text_projection, &slot, &lease, optional, current_control));
    var fresh: ?backends.Session = null;
    var fresh_lease: ?runtime.tier.memory.AdmissionLease = null;
    try std.testing.expectError(error.ChecksumMismatch, model.ensureOptionalSessionWithControl(.vision, &fresh, &fresh_lease, optional, current_control));
    try std.testing.expect(fresh == null and fresh_lease == null);
    const replacement = try a.dupe(u8, complete);
    defer a.free(replacement);
    const pin = std.mem.indexOf(u8, replacement, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").?;
    @memset(replacement[pin..][0..40], 'b');
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = replacement });
    try std.testing.expectError(error.ModelArtifactsChanging, pool_loader.load(optional));
}

test "model selection warm merged membership has constant filesystem work and tracks receipts" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Probe = struct {
        var count: usize = 0;
        var original: @FieldType(std.Io.VTable, "dirStatFile") = undefined;
        fn stat(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.StatFileOptions) std.Io.Dir.StatFileError!std.Io.File.Stat {
            count += 1;
            return original(raw, dir, path, options);
        }
    };
    var small_count: ?usize = null;
    for ([_]usize{ 8, 512 }) |count| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const alloc = arena.allocator();
        const Artifact = struct { path: []const u8, size: u64 = 4 };
        const entries = try alloc.alloc(Artifact, count);
        for (entries, 0..) |*entry, i| {
            const name = if (i == 0) "encoder_model.onnx" else try std.fmt.allocPrint(alloc, "aux-{d}.bin", .{i});
            entry.* = .{ .path = name };
            try tmp.dir.writeFile(io, .{ .sub_path = name, .data = "data" });
        }
        const receipt = try std.json.Stringify.valueAlloc(alloc, .{ .version = @as(u32, 2), .artifacts = entries }, .{});
        try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
        const root = try tmp.dir.realPathFileAlloc(io, ".", alloc);
        var table = io.vtable.*;
        Probe.original = table.dirStatFile;
        table.dirStatFile = Probe.stat;
        const counted = std.Io{ .userdata = io.userdata, .vtable = &table };
        var sm = backends.SessionManager.initWithIo(a, counted);
        sm.preferred_backends = &.{.native};
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        try std.testing.expect((try manager.findMergedDecoderCached(root, null)) == null);
        Probe.count = 0;
        for (0..10) |_| try std.testing.expect((try manager.findMergedDecoderCached(root, null)) == null);
        if (small_count) |expected| {
            try std.testing.expectEqual(expected, Probe.count);
        } else small_count = Probe.count;
        std.debug.print("warm merged membership artifacts={d} stat_calls={d}\n", .{ count, Probe.count });
        // Adding an unlisted file must not change a cached negative answer.
        try tmp.dir.writeFile(io, .{ .sub_path = "decoder_model_merged.onnx", .data = "data" });
        try std.testing.expect((try manager.findMergedDecoderCached(root, null)) == null);
        const expanded = try alloc.alloc(Artifact, entries.len + 1);
        @memcpy(expanded[0..entries.len], entries);
        expanded[entries.len] = .{ .path = "decoder_model_merged.onnx" };
        const complete = try std.json.Stringify.valueAlloc(alloc, .{ .version = @as(u32, 2), .artifacts = expanded }, .{});
        try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = complete });
        const path = (try manager.findMergedDecoderCached(root, null)).?;
        defer a.free(path);
        try std.testing.expect(std.mem.endsWith(u8, path, "decoder_model_merged.onnx"));
        Probe.count = 0;
        const warm = (try manager.findMergedDecoderCached(root, null)).?;
        a.free(warm);
        try std.testing.expectEqual(small_count.? / 10, Probe.count);
        try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
        try std.testing.expect((try manager.findMergedDecoderCached(root, null)) == null);
    }
}

test "model selection merged membership remains bounded and legacy misses remain discoverable" {
    const a = std.testing.allocator;
    var cache: MergedDecoderMembershipCache = .{};
    defer cache.deinit(a);
    for (0..MergedDecoderMembershipCache.capacity * 2) |i| {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(std.mem.asBytes(&i));
        try cache.put(a, hash.finalResult(), if (i % 2 == 0) .root else .absent);
    }
    try std.testing.expectEqual(@as(usize, MergedDecoderMembershipCache.capacity), cache.entries.items.len);
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
    defer manager.deinit();
    try std.testing.expect((try manager.findMergedDecoderCached(root, null)) == null);
    try tmp.dir.writeFile(io, .{ .sub_path = "decoder_model_merged.onnx", .data = "data" });
    const found = (try manager.findMergedDecoderCached(root, null)).?;
    defer a.free(found);
    try std.testing.expectEqual(@as(usize, 0), manager.merged_decoder_membership.entries.items.len);
}

test "model selection merged membership cancellation releases its cold builder gate" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "encoder_model.onnx", .data = "data" });
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data =
        \\{"version":2,"artifacts":[{"path":"encoder_model.onnx","size":4}]}
    });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
    defer manager.deinit();
    const selected = ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
    const key = MergedDecoderMembershipCache.keyFor(selected);
    const builder = &manager.merged_decoder_membership.builders[key[0] % manager.merged_decoder_membership.builders.len];
    try std.testing.expect(builder.tryLock());
    const Cancel = struct {
        var calls: usize = 0;
        fn check(_: ?*anyopaque) !void {
            calls += 1;
            if (calls > 1) return error.Canceled;
        }
    };
    Cancel.calls = 0;
    try std.testing.expectError(error.Canceled, manager.findMergedDecoderCached(root, selected.bind(InferenceExecutionControl{ .io = io, .check_fn = Cancel.check })));
    builder.unlock();
    try std.testing.expect((try manager.findMergedDecoderCached(root, null)) == null);
    try std.testing.expect(builder.tryLock());
    builder.unlock();
}
test "model selection conflicting canonical aliases cannot substitute external tensor checksums" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const input = try b.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const weight = try b.parameter("weight", ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try b.add(input, weight));
    const intended = [_]f32{ 1, 1, 1, 1 };
    const other = [_]f32{ 2, 2, 2, 2 };
    var exported = try onnx_graph.exportGraphWithExternalData(a, &graph, .{ .parameter_initializers = &.{.{ .name = "weight", .shape = ml.graph.Shape.init(.f32, &.{4}), .data = .{ .raw_bytes = std.mem.asBytes(&intended) } }} }, "weights.data");
    defer exported.deinit(a);
    try std.testing.expectEqual(exported.external_data.?.bytes.len, std.mem.asBytes(&other).len);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = exported.model_bytes });
    try tmp.dir.writeFile(io, .{ .sub_path = "weights.data", .data = exported.external_data.?.bytes });
    try tmp.dir.writeFile(io, .{ .sub_path = "alternate.data", .data = std.mem.asBytes(&other) });
    const config = "{\"model_type\":\"bert\",\"hidden_size\":4,\"num_attention_heads\":1,\"vocab_size\":4}";
    const tokenizer = "{\"version\":\"1.0\",\"added_tokens\":[{\"id\":0,\"content\":\"<unk>\"}],\"model\":{\"type\":\"BPE\",\"vocab\":{\"<unk>\":0},\"merges\":[]}}";
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = config });
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = tokenizer });
    var expected: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(exported.external_data.?.bytes, &expected, .{});
    var alternate: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(std.mem.asBytes(&other), &alternate, .{});
    const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .source = .{ .owner = "acme", .name = "model", .variant = "onnx", .resolved_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, .artifacts = .{ .{ .path = "model.onnx", .size = exported.model_bytes.len }, .{ .path = "weights.data", .size = exported.external_data.?.bytes.len, .sha256 = std.fmt.bytesToHex(expected, .lower) }, .{ .path = "alternate.data", .size = std.mem.asBytes(&other).len, .sha256 = std.fmt.bytesToHex(alternate, .lower) }, .{ .path = "config.json", .size = config.len }, .{ .path = "tokenizer.json", .size = tokenizer.len } } }, .{});
    defer a.free(receipt);
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const selected = ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
    try tmp.dir.deleteFile(io, "weights.data");
    try tmp.dir.symLink(io, "alternate.data", "weights.data", .{});
    const requested = try std.fs.path.join(a, &.{ root, "model.onnx" });
    defer a.free(requested);
    try std.testing.expectError(error.InvalidManagedDownload, PublishedArtifacts.init(a, io, root, .{ .publication = selected.publication }));
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    try std.testing.expectError(error.InvalidManagedDownload, manager.componentLoaderForPaths(root, &.{.native}, &.{requested}));
}

test "model selection merged membership follows identical-receipt installation aliases" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const input = try b.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const bias = try b.tensorConst(&.{ 1, 1, 1, 1 }, ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try b.add(input, bias));
    const bytes = try onnx_graph.exportGraph(a, &graph, .{});
    defer a.free(bytes);
    const config = "{\"model_type\":\"t5\",\"vocab_size\":4,\"d_model\":4,\"decoder_start_token_id\":0}";
    const tokenizer = "{\"version\":\"1.0\",\"added_tokens\":[{\"id\":0,\"content\":\"<unk>\"}],\"model\":{\"type\":\"BPE\",\"vocab\":{\"<unk>\":0},\"merges\":[]}}";
    const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .source = .{ .owner = "acme", .name = "model", .variant = "onnx", .resolved_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, .artifacts = .{ .{ .path = "encoder_model.onnx", .size = bytes.len }, .{ .path = "decoder_model.onnx", .size = bytes.len }, .{ .path = "decoder_model_merged.onnx", .size = bytes.len }, .{ .path = "config.json", .size = config.len }, .{ .path = "tokenizer.json", .size = tokenizer.len } } }, .{});
    defer a.free(receipt);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][]const u8{ "installed-a", "installed-b" }) |directory| {
        try tmp.dir.createDirPath(io, directory);
        var dir = try tmp.dir.openDir(io, directory, .{});
        defer dir.close(io);
        for ([_][]const u8{ "encoder_model.onnx", "decoder_model.onnx", "decoder_model_merged.onnx" }) |name| try dir.writeFile(io, .{ .sub_path = name, .data = bytes });
        try dir.writeFile(io, .{ .sub_path = "config.json", .data = config });
        try dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = tokenizer });
        try dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
    }
    try tmp.dir.symLink(io, "installed-a", "current", .{ .is_directory = true });
    const parent = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(parent);
    const root = try std.fs.path.join(a, &.{ parent, "current" });
    defer a.free(root);
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    const selected = ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
    const control = selected.bind(InferenceExecutionControl{ .io = io, .native_call_policy = .in_process, .request_owned = false });
    const before = try encoder_decoder.findEncoderDecoderPaths(a, root);
    defer a.free(before.encoder);
    defer a.free(before.decoder);
    var first = try manager.acquireCompositeRuntime(root, &.{ before.encoder, before.decoder }, .seq2seq, control);
    first.release();
    try tmp.dir.deleteFile(io, "current");
    try tmp.dir.symLink(io, "installed-b", "current", .{ .is_directory = true });
    try selected.verify(a, io);
    const current = try encoder_decoder.findEncoderDecoderPaths(a, root);
    defer a.free(current.encoder);
    defer a.free(current.decoder);
    try std.testing.expect(!std.mem.eql(u8, before.encoder, current.encoder));
    var retry = try manager.acquireCompositeRuntime(root, &.{ current.encoder, current.decoder }, .seq2seq, control);
    defer retry.release();
}

test "model selection composite metadata rejects declared checksum mismatches" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const input = try b.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const bias = try b.tensorConst(&.{ 1, 1, 1, 1 }, ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try b.add(input, bias));
    const bytes = try onnx_graph.exportGraph(a, &graph, .{});
    defer a.free(bytes);
    const config = "{\"model_type\":\"t5\",\"vocab_size\":4,\"d_model\":4,\"decoder_start_token_id\":0}";
    const tokenizer = "{\"version\":\"1.0\",\"added_tokens\":[{\"id\":0,\"content\":\"<unk>\"}],\"model\":{\"type\":\"BPE\",\"vocab\":{\"<unk>\":0},\"merges\":[]}}";
    const generation_json = "{\"eos_token_id\":0}";
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(config, &digest, .{});
    var tokenizer_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(tokenizer, &tokenizer_digest, .{});
    var generation_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(generation_json, &generation_digest, .{});
    const receipt = try std.json.Stringify.valueAlloc(a, .{ .version = @as(u32, 2), .source = .{ .owner = "acme", .name = "model", .variant = "onnx", .resolved_revision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }, .artifacts = .{ .{ .path = "encoder_model.onnx", .size = bytes.len }, .{ .path = "decoder_model.onnx", .size = bytes.len }, .{ .path = "config.json", .size = config.len, .sha256 = std.fmt.bytesToHex(digest, .lower) }, .{ .path = "tokenizer.json", .size = tokenizer.len, .sha256 = std.fmt.bytesToHex(tokenizer_digest, .lower) }, .{ .path = "generation_config.json", .size = generation_json.len, .sha256 = std.fmt.bytesToHex(generation_digest, .lower) } } }, .{});
    defer a.free(receipt);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][]const u8{ "encoder_model.onnx", "decoder_model.onnx" }) |name| try tmp.dir.writeFile(io, .{ .sub_path = name, .data = bytes });
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = config });
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = tokenizer });
    try tmp.dir.writeFile(io, .{ .sub_path = "generation_config.json", .data = generation_json });
    try tmp.dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const selected = ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
    const replacement = try a.dupe(u8, config);
    defer a.free(replacement);
    const tag = "\"decoder_start_token_id\":0";
    const at = std.mem.indexOf(u8, replacement, tag).?;
    replacement[at + tag.len - 1] = '1';
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = replacement });
    var artifacts = try PublishedArtifacts.init(a, io, root, .{ .publication = selected.publication });
    defer artifacts.deinit();
    try std.testing.expectError(error.ChecksumMismatch, artifacts.verifyFile("config.json"));
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    manager.configureServingPolicy(.{ .allow_unknown = true });
    const paths = try encoder_decoder.findEncoderDecoderPaths(a, root);
    defer a.free(paths.encoder);
    defer a.free(paths.decoder);
    const control = selected.bind(InferenceExecutionControl{ .io = io, .native_call_policy = .in_process, .request_owned = false });
    try std.testing.expectError(error.ChecksumMismatch, manager.acquireCompositeRuntime(root, &.{ paths.encoder, paths.decoder }, .seq2seq, control));
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = config });
    var valid = try manager.acquireCompositeRuntime(root, &.{ paths.encoder, paths.decoder }, .seq2seq, control);
    defer valid.release();
    try std.testing.expectEqual(@as(i32, 0), valid.get().decoder_config.decoder_start_token_id);
    try std.testing.expectEqual(selected.publication, valid.get().managed_publication);
    const bad_tokenizer = try a.dupe(u8, tokenizer);
    defer a.free(bad_tokenizer);
    const token_at = std.mem.indexOf(u8, bad_tokenizer, "<unk>").?;
    bad_tokenizer[token_at + 1] = 'x';
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = bad_tokenizer });
    try std.testing.expectError(error.ChecksumMismatch, manager.acquireCompositeRuntime(root, &.{ paths.encoder, paths.decoder }, .seq2seq, control));
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = tokenizer });
    const bad_generation = try a.dupe(u8, generation_json);
    defer a.free(bad_generation);
    bad_generation[bad_generation.len - 2] = '1';
    try tmp.dir.writeFile(io, .{ .sub_path = "generation_config.json", .data = bad_generation });
    try std.testing.expectError(error.ChecksumMismatch, manager.acquireCompositeRuntime(root, &.{}, .whisper_metadata, control));
}

fn writeArtifactIdentityTestReceipt(dir: std.Io.Dir, names: []const []const u8) !void {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const files = try alloc.alloc(managed_receipt.ArtifactReceipt, names.len);
    for (names, files) |name, *file| {
        const bytes = try dir.readFileAlloc(io, name, alloc, .limited(1024 * 1024));
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        file.* = .{ .path = name, .size = bytes.len, .sha256 = try alloc.dupe(u8, &std.fmt.bytesToHex(digest, .lower)) };
    }
    const receipt = try std.json.Stringify.valueAlloc(alloc, managed_receipt.DownloadReceipt{ .version = 2, .artifacts = files }, .{});
    try dir.writeFile(io, .{ .sub_path = managed_receipt.complete_filename, .data = receipt });
}

test "model selection native directory constructors reject corrupt GGUF and retain valid loading" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "aaaa" });
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "config.json", "tokenizer.json", "model.onnx" });
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = "bbbb" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const original = try tmp.dir.readFileAlloc(io, "model.gguf", a, .limited(1024 * 1024));
    defer a.free(original);
    const changed = try a.dupe(u8, original);
    defer a.free(changed);
    std.mem.writeInt(u32, changed[changed.len - 4 ..][0..4], @bitCast(@as(f32, 7.0)), .little);
    const selected = ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    const control = selected.bind(InferenceExecutionControl{ .io = io, .native_call_policy = .in_process, .request_owned = false });
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = changed });
    try std.testing.expectError(error.ChecksumMismatch, manager.loadManagedSessionWithAdmission(root, &.{.native}, null, control, selected, null));
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = original });
    var loaded = try manager.loadManagedSessionWithAdmission(root, &.{.native}, null, control, selected, null);
    defer loaded.deinit();
    try std.testing.expectEqual(backends.BackendType.native, loaded.session.backend());
}

test "model selection native directory proof authenticates single and sharded safetensors" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |sharded| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const payload = if (sharded) "shard.safetensors" else "model.safetensors";
        try tmp.dir.writeFile(io, .{ .sub_path = payload, .data = "aaaa" });
        try tmp.dir.writeFile(io, .{ .sub_path = "model.safetensors.index.json", .data = "{\"weight_map\":{\"w\":\"shard.safetensors\"}}" });
        try writeArtifactIdentityTestReceipt(tmp.dir, if (sharded) &.{ payload, "model.safetensors.index.json" } else &.{payload});
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        const selected = ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
        var proof = try ComponentConstruction.init(a, io, selected, null, null);
        defer proof.deinit();
        try proof.verifyDirectory(root, .native);
        try proof.finish();
        try tmp.dir.writeFile(io, .{ .sub_path = payload, .data = "bbbb" });
        try std.testing.expectError(error.ChecksumMismatch, proof.verifyDirectory(root, .native));
        if (sharded) {
            try tmp.dir.writeFile(io, .{ .sub_path = payload, .data = "aaaa" });
            try tmp.dir.writeFile(io, .{ .sub_path = "unused.onnx", .data = "data" });
            try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.safetensors.index.json", "unused.onnx" });
            var incomplete = try ComponentConstruction.init(a, io, .{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) }, null, null);
            defer incomplete.deinit();
            try std.testing.expectError(error.InvalidManagedDownload, incomplete.verifyDirectory(root, .native));
        }
    }
}

test "model selection tokenizer-only preparation authenticates consumed JSON" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    const tokenizer = try tmp.dir.readFileAlloc(io, "tokenizer.json", a, .limited(1024 * 1024));
    defer a.free(tokenizer);
    const changed = try a.dupe(u8, tokenizer);
    defer a.free(changed);
    const at = std.mem.indexOf(u8, changed, "\"a\":0").?;
    changed[at + 1] = 'x';
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = changed });
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
    defer manager.deinit();
    try std.testing.expectError(error.ChecksumMismatch, manager.loadGenerationTokenizerAssets(root, &man));
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = tokenizer });
    var assets = try manager.loadGenerationTokenizerAssets(root, &man);
    defer assets.deinit();
    try std.testing.expect(assets.hf_tok != null);
}

test "model selection WordPiece preparation verifies vocabulary and metadata" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const names = [_][]const u8{ "model.onnx", "vocab.txt", "tokenizer_config.json", "special_tokens_map.json" };
    const contents = [_][]const u8{ "data", "a\n[UNK]\n", "{\"do_lower_case\":true}", "{\"unk_token\":\"[UNK]\"}" };
    for (names, contents) |name, bytes| try tmp.dir.writeFile(io, .{ .sub_path = name, .data = bytes });
    try writeArtifactIdentityTestReceipt(tmp.dir, &names);
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
    defer manager.deinit();
    {
        var valid = try manager.loadGenerationTokenizerAssets(root, &man);
        defer valid.deinit();
        try std.testing.expect(valid.hf_tok != null);
    }
    for (names[1..], contents[1..]) |name, original| {
        const changed = try a.dupe(u8, original);
        defer a.free(changed);
        changed[1] ^= 1;
        try tmp.dir.writeFile(io, .{ .sub_path = name, .data = changed });
        try std.testing.expectError(error.ChecksumMismatch, manager.loadGenerationTokenizerAssets(root, &man));
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
        try tmp.dir.writeFile(io, .{ .sub_path = name, .data = original });
    }
}

test "model selection unlisted SentencePiece override cannot replace an admitted tokenizer" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "gemma");
    var dir = try tmp.dir.openDir(io, "gemma", .{});
    defer dir.close(io);
    try writeCancellationTestModel(dir, a);
    try writeArtifactIdentityTestReceipt(dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
    // Both a stale override and a large stale sidecar must be absent from
    // selection and from its admission envelope.
    try dir.writeFile(io, .{ .sub_path = "tokenizer.model", .data = "not a tokenizer" });
    try dir.writeFile(io, .{ .sub_path = "added_tokens.json", .data = "not metadata" });
    const root = try tmp.dir.realPathFileAlloc(io, "gemma", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    var assets = try manager.loadGenerationTokenizerAssets(root, &man);
    defer assets.deinit();
    try std.testing.expect(assets.hf_tok != null and assets.sp_tok == null);
    const selected = ResolvedModel{ .path = root, .publication = man.managed_publication };
    var primary = try manager.acquireFromDirWithControl(root, selected.bind(InferenceExecutionControl{ .io = io, .native_call_policy = .in_process, .request_owned = false }));
    defer primary.release();
    try std.testing.expect(primary.get().hf_tok != null);
}

test "model selection embedded SentencePiece accounts and verifies independent overlays" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const weights = try buildTestGgufWithT5Tokenizer(a);
    defer a.free(weights);
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = weights });
    const overlay = "{\"added_tokens\":[{\"content\":\"<extra>\",\"id\":10}]}";
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = overlay });
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "tokenizer.json" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    man.tokenizer_type = .sentencepiece;
    const Probe = struct {
        var opens: usize = 0;
        var original: @FieldType(std.Io.VTable, "dirOpenFile") = undefined;
        fn open(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
            if (std.mem.endsWith(u8, path, "model.gguf")) opens += 1;
            return original(raw, dir, path, options);
        }
    };
    var table = io.vtable.*;
    Probe.original = table.dirOpenFile;
    table.dirOpenFile = Probe.open;
    const counted_io = std.Io{ .userdata = io.userdata, .vtable = &table };
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, counted_io));
    defer manager.deinit();
    var artifacts = try PublishedArtifacts.init(a, io, root, null);
    defer artifacts.deinit();
    const plan = try tokenizerLoadAdmissionPlanWithArtifacts(a, root, &man, .sentencepiece, &artifacts);
    const encoded = try gguf_format.encodedMetadataBytesWithPrefix(weights, "tokenizer.");
    const expected = try tokenizerAdmissionPlan(encoded + overlay.len, .sentencepiece);
    try std.testing.expectEqualDeep(expected.resident, plan.resident);
    {
        Probe.opens = 0;
        var valid = try manager.loadGenerationTokenizerAssets(root, &man);
        defer valid.deinit();
        try std.testing.expectEqual(@as(?i32, 10), valid.sp_tok.?.extra_reserved_map.get("<extra>"));
        try std.testing.expectEqual(@as(usize, 1), Probe.opens);
    }
    {
        var shared = try PublishedArtifacts.init(a, counted_io, root, null);
        defer shared.deinit();
        Probe.opens = 0;
        _ = try modelIdentityWithArtifacts(a, counted_io, root, man, &shared);
        _ = try tokenizerLoadAdmissionPlanWithArtifacts(a, root, &man, .sentencepiece, &shared);
        const tokenizer = try loadSentencePieceTokenizerWithArtifacts(a, &man, &shared);
        defer {
            tokenizer.deinit();
            a.destroy(tokenizer);
        }
        try shared.verify();
        try std.testing.expectEqual(@as(usize, 1), Probe.opens);
    }
    const changed = try a.dupe(u8, overlay);
    defer a.free(changed);
    changed[std.mem.indexOf(u8, changed, "extra").?] = 'x';
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = changed });
    try std.testing.expectError(error.ChecksumMismatch, manager.loadGenerationTokenizerAssets(root, &man));
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = overlay });
    weights[std.mem.indexOf(u8, weights, "hello").?] = 'j';
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = weights });
    try std.testing.expectError(error.ChecksumMismatch, manager.loadGenerationTokenizerAssets(root, &man));
}

test "model selection managed and standalone direct GGUF tokenizer paths preserve publication" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const weights = try buildTestGgufWithGpt2Tokenizer(a);
    defer a.free(weights);
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = weights });
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{"model.gguf"});
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const gguf = try tmp.dir.realPathFileAlloc(io, "model.gguf", a);
    defer a.free(gguf);
    var man = try manifest_mod.loadFromDir(a, gguf);
    defer man.deinit();
    try std.testing.expect(man.managed_publication != null);
    try std.testing.expectEqual(man.managed_publication, try managed_receipt.publicationGeneration(a, io, gguf));
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
    defer manager.deinit();
    var direct = try manager.loadGenerationTokenizerAssets(gguf, &man);
    defer direct.deinit();
    try std.testing.expect(direct.hf_tok != null);
    var directory_man = try manifest_mod.loadFromDir(a, root);
    defer directory_man.deinit();
    var valid = try manager.loadGenerationTokenizerAssets(root, &directory_man);
    defer valid.deinit();
    try std.testing.expect(valid.hf_tok != null);
    try tmp.dir.deleteFile(io, managed_receipt.complete_filename);
    var standalone_man = try manifest_mod.loadFromDir(a, gguf);
    defer standalone_man.deinit();
    try std.testing.expect(standalone_man.managed_publication == null);
    var standalone = try manager.loadGenerationTokenizerAssets(gguf, &standalone_man);
    defer standalone.deinit();
    try std.testing.expect(standalone.hf_tok != null);
}

test "model selection HF tokenizer admission does not inspect unrelated config" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    const Probe = struct {
        var config_reads: usize = 0;
        var original: @FieldType(std.Io.VTable, "dirOpenFile") = undefined;
        fn open(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
            if (std.mem.endsWith(u8, path, "/config.json")) config_reads += 1;
            return original(raw, dir, path, options);
        }
    };
    var table = io.vtable.*;
    Probe.original = table.dirOpenFile;
    table.dirOpenFile = Probe.open;
    const counted = std.Io{ .userdata = io.userdata, .vtable = &table };
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, counted));
    defer manager.deinit();
    try (ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 1, .combined_limit_bytes = 1 } }).configure(&manager);
    const result = manager.loadGenerationTokenizerAssets(root, &man);
    if (result) |loaded| {
        var unexpected = loaded;
        unexpected.deinit();
        return error.TestExpectedError;
    } else |err| try std.testing.expect(err == error.ResourceTemporarilyUnavailable or err == error.ResourceLimitExceeded);
    try std.testing.expectEqual(@as(usize, 0), Probe.config_reads);
}

test "model selection tokenizer quota rejects before configuration allocation" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    var failing = std.testing.FailingAllocator.init(a, .{});
    const Probe = struct {
        var failing_state: *std.testing.FailingAllocator = undefined;
        var original: @FieldType(std.Io.VTable, "dirOpenFile") = undefined;
        fn open(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
            const file = try original(raw, dir, path, options);
            if (std.mem.endsWith(u8, path, "/config.json")) failing_state.fail_index = failing_state.alloc_index;
            return file;
        }
    };
    var table = io.vtable.*;
    Probe.original = table.dirOpenFile;
    Probe.failing_state = &failing;
    table.dirOpenFile = Probe.open;
    const counted = std.Io{ .userdata = io.userdata, .vtable = &table };
    var manager = ModelManager.init(failing.allocator(), backends.SessionManager.initWithIo(failing.allocator(), counted));
    defer manager.deinit();
    try (ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 1, .combined_limit_bytes = 1 } }).configure(&manager);
    if (manager.loadGenerationTokenizerAssets(root, &man)) |value| {
        var unexpected = value;
        unexpected.deinit();
        return error.TestExpectedError;
    } else |err| try std.testing.expect(err == error.ResourceTemporarilyUnavailable or err == error.ResourceLimitExceeded);
    try std.testing.expect(!failing.has_induced_failure);
    failing.fail_index = std.math.maxInt(usize);
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
}

test "model selection repeated chunk planning reuses stamped checkpoint proofs" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    _ = try modelIdentity(a, io, root, man);
    const Probe = struct {
        var weight_opens: usize = 0;
        var original: @FieldType(std.Io.VTable, "dirOpenFile") = undefined;
        fn open(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
            if (std.mem.endsWith(u8, path, "/model.gguf")) weight_opens += 1;
            return original(raw, dir, path, options);
        }
    };
    var table = io.vtable.*;
    Probe.original = table.dirOpenFile;
    table.dirOpenFile = Probe.open;
    const counted = std.Io{ .userdata = io.userdata, .vtable = &table };
    _ = try modelIdentity(a, counted, root, man);
    try std.testing.expectEqual(@as(usize, 0), Probe.weight_opens);
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, counted));
    defer manager.deinit();
    for (0..2) |_| {
        var planning = try manager.loadChunkPlanningAssets(root, counted, .{ .io = counted });
        defer planning.deinit();
        try planning.verify();
    }
    // The first acquisition authenticates the bytes; later acquisitions only
    // revalidate stamps, including when the legacy identity cache was warm.
    try std.testing.expectEqual(@as(usize, 1), Probe.weight_opens);
    const original = try tmp.dir.readFileAlloc(io, "model.gguf", a, .limited(1024 * 1024));
    defer a.free(original);
    const changed = try a.dupe(u8, original);
    defer a.free(changed);
    changed[changed.len - 1] ^= 1;
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = changed });
    try std.testing.expectError(error.ChecksumMismatch, manager.loadChunkPlanningAssets(root, counted, .{ .io = counted }));
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = original });
    var restored = try manager.loadChunkPlanningAssets(root, counted, .{ .io = counted });
    defer restored.deinit();
    try restored.verify();
    try std.testing.expectEqual(@as(usize, 3), Probe.weight_opens);
}

test "model selection native Whisper consumes only authenticated prompt sidecars" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(tmp.dir, a, "model.safetensors");
    const cfg = "{\"model_type\":\"whisper\",\"d_model\":4,\"encoder_layers\":1,\"decoder_layers\":1,\"encoder_attention_heads\":1,\"decoder_attention_heads\":1,\"encoder_ffn_dim\":8,\"decoder_ffn_dim\":8,\"vocab_size\":14,\"num_mel_bins\":4}";
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = cfg });
    const tok = "{\"version\":\"1.0\",\"added_tokens\":[{\"id\":0,\"content\":\"<unk>\"},{\"id\":10,\"content\":\"<|en|>\"},{\"id\":12,\"content\":\"<|transcribe|>\"},{\"id\":13,\"content\":\"<|notimestamps|>\"}],\"model\":{\"type\":\"BPE\",\"vocab\":{\"<unk>\":0,\"<|en|>\":10,\"<|transcribe|>\":12,\"<|notimestamps|>\":13},\"merges\":[]}}";
    try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = tok });
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.safetensors", "config.json", "tokenizer.json" });
    try tmp.dir.writeFile(io, .{ .sub_path = "generation_config.json", .data = "{\"eos_token_id\":7}" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var published = try PublishedArtifacts.init(a, io, root, null);
    defer published.deinit();
    try std.testing.expect((try published.resolve("generation_config.json")) == null);
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    try (ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 128 * 1024 * 1024, .combined_limit_bytes = 128 * 1024 * 1024 } }).configure(&manager);
    var loaded = try manager.acquireFromDirWithControl(root, .{ .io = io, .request_owned = false, .native_call_policy = .in_process });
    defer loaded.release();
    try std.testing.expectEqual(@as(i32, 50257), loaded.get().whisper_prompt_cache.?.decode.eot_id);
    try std.testing.expect(loaded.get().whisper_prompt_admission != null);
    const first_runtime = loaded.get().runtime_id;
    loaded.release();
    try std.testing.expectEqual(UnloadRuntimeResult.unloaded, manager.unloadRuntime(first_runtime));
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.safetensors", "config.json", "tokenizer.json", "generation_config.json" });
    var listed = try manager.acquireFromDirWithControl(root, .{ .io = io, .request_owned = false, .native_call_policy = .in_process });
    defer listed.release();
    try std.testing.expectEqual(@as(i32, 7), listed.get().whisper_prompt_cache.?.decode.eot_id);
    const listed_runtime = listed.get().runtime_id;
    listed.release();
    try std.testing.expectEqual(UnloadRuntimeResult.unloaded, manager.unloadRuntime(listed_runtime));
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    try tmp.dir.writeFile(io, .{ .sub_path = "generation_config.json", .data = "{\"eos_token_id\":8}" });
    try std.testing.expectError(error.ChecksumMismatch, manager.acquireFromDirWithControl(root, .{ .io = io, .request_owned = false, .native_call_policy = .in_process }));
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    // Authorized but malformed settings fail parsing and refund the temporary
    // prompt envelope as well as the already constructed tokenizer/session.
    try tmp.dir.writeFile(io, .{ .sub_path = "generation_config.json", .data = "{" });
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.safetensors", "config.json", "tokenizer.json", "generation_config.json" });
    try std.testing.expectError(error.InvalidWhisperDecoderConfig, manager.acquireFromDirWithControl(root, .{ .io = io, .request_owned = false, .native_call_policy = .in_process }));
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    var metadata = try manifest_mod.loadFromDir(a, root);
    defer metadata.deinit();
    var prompt_artifacts = try PublishedArtifacts.init(a, io, root, null);
    defer prompt_artifacts.deinit();
    const prompt_tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(a, tok);
    defer prompt_tokenizer.deinitSelf();
    var constrained = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
    defer constrained.deinit();
    try (ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 1, .combined_limit_bytes = 1 } }).configure(&constrained);
    if (constrained.loadNativeWhisperPrompt(&metadata, &prompt_artifacts, prompt_tokenizer.tokenizer())) |value| {
        var unexpected = value;
        unexpected.deinit();
        return error.TestExpectedError;
    } else |err| try std.testing.expect(err == error.ResourceTemporarilyUnavailable or err == error.ResourceLimitExceeded);
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, constrained.currentAdmissionAmounts());
}

test "model selection legacy Whisper metadata growth cannot exceed its admission" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = "{}" });
    const original = "{\"eos_token_id\":7}";
    try tmp.dir.writeFile(io, .{ .sub_path = "generation_config.json", .data = original });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const grown = try a.alloc(u8, 2 * 1024 * 1024);
    defer a.free(grown);
    @memset(grown, ' ');
    @memcpy(grown[0..original.len], original);
    const Probe = struct {
        var original_open: @FieldType(std.Io.VTable, "dirOpenFile") = undefined;
        var fixture_dir: std.Io.Dir = undefined;
        var replacement: []const u8 = undefined;
        var grow = true;
        fn open(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
            if (grow and std.mem.endsWith(u8, path, "/generation_config.json")) {
                grow = false;
                fixture_dir.writeFile(std.testing.io, .{ .sub_path = "generation_config.json", .data = replacement }) catch return error.Unexpected;
            }
            return original_open(raw, dir, path, options);
        }
    };
    var table = io.vtable.*;
    Probe.original_open = table.dirOpenFile;
    Probe.fixture_dir = tmp.dir;
    Probe.replacement = grown;
    Probe.grow = true;
    table.dirOpenFile = Probe.open;
    const growing_io = std.Io{ .userdata = io.userdata, .vtable = &table };
    var artifacts = try PublishedArtifacts.init(a, growing_io, root, null);
    defer artifacts.deinit();
    const tokenizer = try hf_tokenizer.HfTokenizer.loadFromBytes(a,
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<|transcribe|>"},{"id":1,"content":"<|notimestamps|>"}],"model":{"type":"BPE","vocab":{"<|transcribe|>":0,"<|notimestamps|>":1},"merges":[]}}
    );
    defer tokenizer.deinitSelf();
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, growing_io));
    defer manager.deinit();
    try (ModelLoadPolicy{ .limits = .{ .host_limit_bytes = 128 * 1024 * 1024, .combined_limit_bytes = 128 * 1024 * 1024 } }).configure(&manager);
    const man = manifest_mod.ModelManifest{ .allocator = a };
    try std.testing.expectError(error.FileTooLarge, manager.loadNativeWhisperPrompt(&man, &artifacts, tokenizer.tokenizer()));
    try std.testing.expect(!Probe.grow);
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
}

test "component and primary construction reject same-receipt namespace retargets" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for (0..3) |loader| {
        for ([_]bool{ false, true }) |direct_file| {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            for ([_][]const u8{ "first", "second" }) |name| {
                try tmp.dir.createDirPath(io, name);
                var dir = try tmp.dir.openDir(io, name, .{});
                defer dir.close(io);
                try writeCancellationTestModel(dir, a);
            }
            var first_dir = try tmp.dir.openDir(io, "first", .{});
            defer first_dir.close(io);
            try writeArtifactIdentityTestReceipt(first_dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
            const receipt = try tmp.dir.readFileAlloc(io, "first/.antfly-download-complete.json", a, .limited(1024 * 1024));
            defer a.free(receipt);
            try tmp.dir.writeFile(io, .{ .sub_path = "second/.antfly-download-complete.json", .data = receipt });
            const altered = try tmp.dir.readFileAlloc(io, "second/model.gguf", a, .limited(1024 * 1024));
            defer a.free(altered);
            std.mem.writeInt(u32, altered[altered.len - 4 ..][0..4], @bitCast(@as(f32, 7.0)), .little);
            try tmp.dir.writeFile(io, .{ .sub_path = "second/model.gguf", .data = altered });
            const alias_name = if (direct_file) "current.gguf" else "current";
            try tmp.dir.symLink(io, if (direct_file) "first/model.gguf" else "first", alias_name, .{ .is_directory = !direct_file });
            const root = try tmp.dir.realPathFileAlloc(io, ".", a);
            defer a.free(root);
            const alias = try std.fs.path.join(a, &.{ root, alias_name });
            defer a.free(alias);
            const Probe = struct {
                dir: std.Io.Dir,
                direct: bool,
                hashed_first: bool = false,
                switched: bool = false,
                var current: *@This() = undefined;
                var original: @FieldType(std.Io.VTable, "dirOpenFile") = undefined;
                fn open(raw: ?*anyopaque, dir: std.Io.Dir, path: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
                    if (std.mem.endsWith(u8, path, "/first/model.gguf")) current.hashed_first = true;
                    return original(raw, dir, path, options);
                }
                fn check(raw: ?*anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw.?));
                    if (self.hashed_first and !self.switched) {
                        self.switched = true;
                        const name = if (self.direct) "current.gguf" else "current";
                        try self.dir.deleteFile(std.testing.io, name);
                        try self.dir.symLink(std.testing.io, if (self.direct) "second/model.gguf" else "second", name, .{ .is_directory = !self.direct });
                    }
                }
            };
            var probe = Probe{ .dir = tmp.dir, .direct = direct_file };
            var table = io.vtable.*;
            Probe.current = &probe;
            Probe.original = table.dirOpenFile;
            table.dirOpenFile = Probe.open;
            const counted = std.Io{ .userdata = io.userdata, .vtable = &table };
            var sm = backends.SessionManager.initWithIo(a, counted);
            sm.preferred_backends = &.{.native};
            sm.native_call_policy = .in_process;
            var manager = ModelManager.init(a, sm);
            defer manager.deinit();
            const selected = ResolvedModel{ .path = alias, .publication = try managed_receipt.publicationGeneration(a, io, alias) };
            const control = selected.bind(InferenceExecutionControl{ .io = counted, .request_owned = false, .native_call_policy = .in_process, .ptr = &probe, .check_fn = Probe.check });
            if (loader != 0) {
                var man = try manifest_mod.loadFromDir(a, alias);
                defer man.deinit();
                if (loader == 2) {
                    var tracking = std.testing.FailingAllocator.init(a, .{});
                    {
                        var artifacts = try PublishedArtifacts.init(tracking.allocator(), counted, alias, null);
                        defer artifacts.deinit();
                        artifacts.verification_cache = &manager.verified_artifacts;
                        try std.testing.expectError(error.ModelArtifactsChanging, loadSessionForPreferredBackendsWithArtifacts(&manager, &.{.native}, alias, man, &manager.session_manager, control, &artifacts));
                        try std.testing.expect(manager.verified_artifacts.entries.count() > 0);
                    }
                    try std.testing.expectEqual(tracking.allocated_bytes, tracking.freed_bytes);
                } else try std.testing.expectError(error.ModelArtifactsChanging, loadSessionForPreferredBackends(&manager, &.{.native}, alias, man, &manager.session_manager, control));
            } else {
                try std.testing.expectError(error.ModelArtifactsChanging, manager.loadManagedSessionWithAdmission(alias, &.{.native}, null, control, selected, null));
            }
            try std.testing.expect(probe.switched);
            try std.testing.expectEqual(selected.publication, try managed_receipt.publicationGeneration(a, io, alias));
            const second = try tmp.dir.realPathFileAlloc(io, "second", a);
            defer a.free(second);
            var actual = try PublishedArtifacts.init(a, io, second, null);
            defer actual.deinit();
            try std.testing.expectError(error.ChecksumMismatch, actual.verifyFile("model.gguf"));
        }
    }
}

test "Whisper prompt cache propagates tokenizer resource failures" {
    const a = std.testing.allocator;
    const tok = try hf_tokenizer.HfTokenizer.loadFromBytes(a,
        \\{"version":"1.0","added_tokens":[{"id":0,"content":"<unk>"},{"id":10,"content":"<|en|>"},{"id":12,"content":"<|transcribe|>"},{"id":13,"content":"<|notimestamps|>"}],"model":{"type":"BPE","vocab":{"<unk>":0,"<|en|>":10,"<|transcribe|>":12,"<|notimestamps|>":13},"merges":[]}}
    );
    defer tok.deinitSelf();
    const Wrapper = struct {
        base: tokenizer_mod.Tokenizer,
        fail_text: ?[]const u8 = null,
        failure: anyerror = error.OutOfMemory,
        fn encode(raw: *anyopaque, alloc: std.mem.Allocator, text: []const u8) ![]i32 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail_text) |fail_text| if (std.mem.eql(u8, text, fail_text)) return self.failure;
            return self.base.encode(alloc, text);
        }
        fn specials(raw: *anyopaque) tokenizer_mod.SpecialTokens {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.base.specialTokens();
        }
        const vtable = tokenizer_mod.Tokenizer.VTable{ .encode = encode, .decode = undefined, .encodeInto = undefined, .encodeForModel = undefined, .encodeGeneration = undefined, .specialTokens = specials, .vocabSize = undefined, .deinit = undefined };
    };
    var wrapper = Wrapper{ .base = tok.tokenizer() };
    const wrapped = tokenizer_mod.Tokenizer{ .ptr = &wrapper, .vtable = &Wrapper.vtable };
    for ([_]anyerror{ error.OutOfMemory, error.Cancelled, error.ResourceLimitExceeded }) |failure| {
        wrapper.failure = failure;
        for ([_][]const u8{ "<|en|>", "<|transcribe|>", "<|notimestamps|>", "<|startofprev|>", "<|endoftext|>", "<|0.00|>", " -", "#", " #", "♩", " ♩", " " }) |fail_text| {
            wrapper.fail_text = fail_text;
            try std.testing.expectError(failure, whisper_prompt.PromptCache.initFromBytes(a, null, null, wrapped));
        }
    }
    wrapper.fail_text = null;
    var scratch: [3]whisper_prompt.ForcedDecoderId = undefined;
    var valid = try whisper_prompt.PromptCache.initFromBytes(a, null, null, wrapped);
    defer valid.deinit();
    try std.testing.expectEqual(@as(usize, 1), valid.language_tokens.len);
    const complete = try valid.resolve(&scratch, "en");
    try std.testing.expectEqual(@as(usize, 3), complete.len);
    try std.testing.expectEqual(@as(?i32, 10), complete[0].token_id);
}

fn whisperPromptAllocationScenario(a: std.mem.Allocator, generation_data: ?[]const u8, config: ?[]const u8) !void {
    const Fixture = struct {
        fn encode(_: *anyopaque, allocator: std.mem.Allocator, text: []const u8) ![]i32 {
            const id: i32 = if (std.mem.eql(u8, text, "<|en|>")) 10 else if (std.mem.eql(u8, text, "<|transcribe|>")) 12 else if (std.mem.eql(u8, text, "<|notimestamps|>")) 13 else 0;
            return allocator.dupe(i32, &.{id});
        }
        fn specials(_: *anyopaque) tokenizer_mod.SpecialTokens {
            return .{ .unk_id = 0 };
        }
        const vtable = tokenizer_mod.Tokenizer.VTable{ .encode = encode, .decode = undefined, .encodeInto = undefined, .encodeForModel = undefined, .encodeGeneration = undefined, .specialTokens = specials, .vocabSize = undefined, .deinit = undefined };
    };
    var context: u8 = 0;
    const tokenizer = tokenizer_mod.Tokenizer{ .ptr = &context, .vtable = &Fixture.vtable };
    var cache = try whisper_prompt.PromptCache.initFromBytes(a, generation_data, config, tokenizer);
    defer cache.deinit();
    try std.testing.expectEqual(@as(usize, 1), cache.language_tokens.len);
    var scratch: [3]whisper_prompt.ForcedDecoderId = undefined;
    const prompt = try cache.resolve(&scratch, "en");
    try std.testing.expectEqual(@as(usize, 3), prompt.len);
    try std.testing.expectEqual(@as(?i32, 10), prompt[0].token_id);
}

test "Whisper prompt preparation unwinds every allocation failure" {
    const settings =
        \\{"forced_decoder_ids":[[1,null],[2,12],[3,13]],"suppress_tokens":[1,2],"begin_suppress_tokens":[3,4]}
    ;
    for ([_]?[]const u8{ null, "{}", settings }) |generation_data| {
        try platform.allocator.checkAllAllocationFailures(std.testing.allocator, whisperPromptAllocationScenario, .{ generation_data, settings });
    }
}

fn expectBoundSourceOutput(session: backends.Session, gguf: bool) !void {
    const a = std.testing.allocator;
    if (session.run_admission != null) {
        const owner = session.model_lifetime orelse return error.MissingAdmittedModelOwner;
        try std.testing.expect(session.execution_gate != null);
        try std.testing.expectEqual(&owner.execution_gate, session.execution_gate.?);
    }
    if (gguf) {
        var ids = try backends.Tensor.initInt64(a, "input_ids", &.{ 1, 2 }, &.{ 1, 2 });
        defer ids.deinit();
        var mask = try backends.Tensor.initInt64(a, "attention_mask", &.{ 1, 2 }, &.{ 1, 1 });
        defer mask.deinit();
        const outputs = try session.run(&.{ ids, mask }, a);
        defer {
            for (outputs) |*tensor| tensor.deinit();
            a.free(outputs);
        }
        try std.testing.expect(outputs.len > 0);
        for (outputs[0].asFloat32()) |value| try std.testing.expectEqual(@as(f32, 0), value);
    } else {
        var input = try backends.Tensor.initFloat32(a, "input", &.{4}, &.{ 0, 0, 0, 0 });
        defer input.deinit();
        const outputs = try session.run(&.{input}, a);
        defer {
            for (outputs) |*tensor| tensor.deinit();
            a.free(outputs);
        }
        try std.testing.expectEqual(@as(usize, 1), outputs.len);
        try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, outputs[0].asFloat32());
    }
}

fn exerciseBoundSourceLoaders(root: []const u8, input: []const u8, gguf: bool) !void {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    manager.admission_enabled = true;
    manager.configureServingPolicy(.{ .allow_unknown = true });
    const selected = ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
    {
        var loaded = try manager.loadManagedSessionWithAdmission(input, &.{.native}, null, null, selected, null);
        defer loaded.deinit();
        try expectBoundSourceOutput(loaded.session, gguf);
    }
    var manifest = try manifest_mod.loadFromDir(a, root);
    defer manifest.deinit();
    var allowed_scratch: [7]backends.BackendType = undefined;
    const allowed = try policyAllowedBackends(a, &allowed_scratch, root, &manifest, &.{.native}, .{ .allow_unknown = true });
    try std.testing.expectEqualSlices(backends.BackendType, &.{.native}, allowed);
    if (!gguf) {
        var artifacts = try backends.imported_onnx_session.inspectArtifactSet(a, input);
        defer artifacts.deinit();
        try std.testing.expectEqual(artifacts.encoded_bytes, try estimateModelArtifactBytes(manifest));
        const access = try @import("../models/tensor_access.zig").openFromManifest(a, manifest);
        defer access.deinit();
        var weight = try access.getRecord(a, "weight");
        defer weight.deinit();
        try std.testing.expectEqualSlices(u8, std.mem.asBytes(&[_]f32{ 1, 2, 3, 4 }), weight.raw_bytes);
    }
    {
        var loaded = try loadSessionForPreferredBackends(&manager, &.{.native}, root, manifest, &manager.session_manager, null);
        defer loaded.deinit();
        try expectBoundSourceOutput(loaded.session, gguf);
    }
    {
        var loaded = try manager.loadArtifactSession(root, manifest);
        defer loaded.deinit();
        try expectBoundSourceOutput(loaded.session, gguf);
    }
    {
        const component = try manager.componentLoaderForPaths(root, &.{.native}, &.{input});
        var loader = try component.sessionPoolLoader();
        defer loader.deinit();
        var loaded = try loader.load(input);
        defer loaded.deinit();
        try expectBoundSourceOutput(loaded.session, gguf);
    }
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
}

fn admittedLoaderOwnerScenario(a: std.mem.Allocator, path: []const u8, component: bool, fail_after_attach: bool) !void {
    const Probe = struct {
        manager: ModelManager,
        fail_after_attach: bool,
        attachments: usize = 0,
        fn attach(raw: *anyopaque, session: *backends.Session, backend_runtime: backends.BackendRuntime, resident: runtime.tier.memory.AdmissionAmounts, man: ?*const manifest_mod.ModelManifest) !void {
            const manager: *ModelManager = @ptrCast(@alignCast(raw));
            const self: *@This() = @fieldParentPtr("manager", manager);
            const owner = session.model_lifetime orelse return error.MissingAdmittedModelOwner;
            try std.testing.expect(session.execution_gate != null);
            try std.testing.expectEqual(&owner.execution_gate, session.execution_gate.?);
            try std.testing.expect(session.cohort_scheduler != null);
            try std.testing.expectEqual(&owner.cohort_scheduler, session.cohort_scheduler.?);
            // Exercise actual cold final-resource allocation before policy and
            // the failure edge AFTER it attached real admission and a Model pin.
            try std.testing.expect(owner.prepared_final_resources != null);
            self.attachments += 1;
            try SessionLoadHost.attachAdmission(raw, session, backend_runtime, resident, man);
            if (self.fail_after_attach) return error.InjectedLateModelAdmission;
        }
    };
    var sm = backends.SessionManager.initWithIo(a, std.testing.io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var probe = Probe{ .manager = ModelManager.init(a, sm), .fail_after_attach = fail_after_attach };
    defer probe.manager.deinit();
    probe.manager.admission_enabled = true;
    probe.manager.configureServingPolicy(.{ .allow_unknown = true });
    var host = SessionLoadHost.services(&probe.manager);
    host.attach_admission_fn = Probe.attach;
    var manifest = try manifest_mod.loadFromDir(a, path);
    defer manifest.deinit();
    const result = if (component)
        session_loader.loadComponent(a, host, path, &.{.native}, null, &probe.manager.session_manager, null, .{ .path = path, .publication = try managed_receipt.publicationGeneration(a, std.testing.io, path) }, null)
    else
        session_loader.loadPreferred(a, host, &.{.native}, path, manifest, &probe.manager.session_manager, null);
    if (result) |value| {
        var loaded = value;
        loaded.deinit();
        if (fail_after_attach) return error.ExpectedLateAdmissionFailure;
    } else |err| {
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, probe.manager.currentAdmissionAmounts());
        if (err != error.InjectedLateModelAdmission) return err;
        if (!fail_after_attach) return err;
    }
    try std.testing.expectEqual(@as(usize, 1), probe.attachments);
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, probe.manager.currentAdmissionAmounts());
}

test "admitted preferred and component loaders publish the actual execution owner and unwind late policy failure" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    const path = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(path);
    for ([_]bool{ false, true }) |component| for ([_]bool{ false, true }) |fail_after_attach| {
        try platform.allocator.checkAllAllocationFailures(a, admittedLoaderOwnerScenario, .{ path, component, fail_after_attach });
    };
}

test "model selection bound GGUF sources preserve format independently of target suffix" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |managed_model| for ([_][]const u8{ "payload", "payload.onnx" }) |target| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try writeCancellationTestModel(tmp.dir, a);
        try tmp.dir.rename("model.gguf", tmp.dir, target, io);
        try tmp.dir.symLink(io, target, "model.gguf", .{});
        if (managed_model) try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        const alias = try std.fs.path.join(a, &.{ root, "model.gguf" });
        defer a.free(alias);
        try exerciseBoundSourceLoaders(alias, alias, true);
    };
}

test "model selection bound ONNX sources preserve external tensor context through every loader" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const weight = try builder.parameter("weight", ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try builder.add(input, weight));
    const values = [_]f32{ 1, 2, 3, 4 };
    var exported = try onnx_graph.exportGraphWithExternalData(a, &graph, .{ .parameter_initializers = &.{.{ .name = "weight", .shape = ml.graph.Shape.init(.f32, &.{4}), .data = .{ .raw_bytes = std.mem.asBytes(&values) } }} }, "weights.data");
    defer exported.deinit(a);
    for ([_]bool{ false, true }) |managed_model| for ([_][]const u8{ "payload", "payload.gguf", "payload.onnx" }) |target| {
        if (!managed_model and std.mem.endsWith(u8, target, ".gguf")) continue; // Unmanaged native-file discovery takes precedence.
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.createDirPath(io, "blobs");
        const physical = try std.fs.path.join(a, &.{ "blobs", target });
        defer a.free(physical);
        try tmp.dir.writeFile(io, .{ .sub_path = physical, .data = exported.model_bytes });
        try tmp.dir.symLink(io, physical, "model.onnx", .{});
        // Both directories have valid, different tensor payloads. Loading the
        // graph's physical directory would silently execute a different model.
        const other = [_]f32{ 10, 20, 30, 40 };
        try tmp.dir.writeFile(io, .{ .sub_path = "weights.data", .data = exported.external_data.?.bytes });
        try tmp.dir.writeFile(io, .{ .sub_path = "blobs/weights.data", .data = std.mem.asBytes(&other) });
        if (managed_model) try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.onnx", "weights.data", "blobs/weights.data" });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        const alias = try std.fs.path.join(a, &.{ root, "model.onnx" });
        defer a.free(alias);
        try exerciseBoundSourceLoaders(root, alias, false);
        try tmp.dir.deleteFile(io, "blobs/weights.data");
        if (managed_model) try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.onnx", "weights.data" });
        try exerciseBoundSourceLoaders(root, alias, false);
        if (managed_model) {
            // Explicit graph format cannot bypass external-file membership.
            try writeArtifactIdentityTestReceipt(tmp.dir, &.{"model.onnx"});
            var sm = backends.SessionManager.initWithIo(a, io);
            sm.preferred_backends = &.{.native};
            var manager = ModelManager.init(a, sm);
            defer manager.deinit();
            const selected = ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
            try std.testing.expectError(error.InvalidManagedDownload, manager.loadManagedSessionWithAdmission(alias, &.{.native}, null, null, selected, null));
        }
    };
}

fn boundSourceAllocationScenario(a: std.mem.Allocator, owner: []const u8, requested: []const u8, kind: ModelSource.Kind, manifest: ?*const manifest_mod.ModelManifest) !void {
    var proof = try ComponentConstruction.init(a, std.testing.io, .{ .path = owner, .publication = try managed_receipt.publicationGeneration(a, std.testing.io, owner) }, null, null);
    defer proof.deinit();
    var source = try BoundModelSource.init(&proof, requested, kind, manifest, null);
    defer source.deinit();
    try std.testing.expectEqual(kind, source.view().kind);
    if (try source.forBackend(&proof, .native)) |first| {
        const again = (try source.forBackend(&proof, .native)).?;
        try std.testing.expectEqual(first.manifest, again.manifest);
    }
    if (kind == .directory) {
        if (try source.forBackend(&proof, .wasm)) |first| {
            const again = (try source.forBackend(&proof, .wasm)).?;
            try std.testing.expectEqual(first.path.ptr, again.path.ptr);
        }
    }
    if (kind == .onnx) try std.testing.expect(source.external_base_dir != null) else try std.testing.expect(source.view().manifest != null);
    if (manifest) |provided| if (source.view().manifest) |bound| {
        try std.testing.expectEqual(provided.model_type, bound.model_type);
        try std.testing.expectEqualStrings(provided.config_model_arch, bound.config_model_arch);
        if (provided.audio_model_path) |path| {
            if (ModelSource.kindForPath(path) != .directory) try std.testing.expectEqual(ModelSource.kindForPath(path), ModelSource.kindForPath(bound.audio_model_path.?));
        }
    };
}

test "model selection bound sources unwind owned and borrowed metadata allocation failures" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    try tmp.dir.rename("model.gguf", tmp.dir, "payload", io);
    try tmp.dir.symLink(io, "payload", "model.gguf", .{});
    try tmp.dir.writeFile(io, .{ .sub_path = "graph", .data = "data" });
    try tmp.dir.symLink(io, "graph", "model.onnx", .{});
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "model.onnx" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const alias = try std.fs.path.join(a, &.{ root, "model.gguf" });
    defer a.free(alias);
    var manifest = try manifest_mod.loadFromDir(a, alias);
    defer manifest.deinit();
    try platform.allocator.checkAllAllocationFailures(a, boundSourceAllocationScenario, .{ alias, alias, ModelSource.Kind.gguf, &manifest });
    try platform.allocator.checkAllAllocationFailures(a, boundSourceAllocationScenario, .{ alias, alias, ModelSource.Kind.gguf, null });
    const onnx = try std.fs.path.join(a, &.{ root, "model.onnx" });
    defer a.free(onnx);
    try platform.allocator.checkAllAllocationFailures(a, boundSourceAllocationScenario, .{ root, onnx, ModelSource.Kind.onnx, null });
    const legacy = manifest_mod.ModelManifest{ .allocator = a, .model_type = .reranker, .gguf_path = alias, .onnx_path = onnx, .visual_model_path = onnx, .audio_model_path = alias };
    try platform.allocator.checkAllAllocationFailures(a, boundSourceAllocationScenario, .{ root, root, ModelSource.Kind.directory, &legacy });
    try std.testing.expect(legacy.source_root == null);
    try std.testing.expectEqualStrings(alias, legacy.gguf_path.?);
    // A direct GGUF descriptor cannot silently construct a preferred safetensors model.
    var proof = try ComponentConstruction.init(a, io, .{ .path = alias, .publication = manifest.managed_publication }, null, null);
    defer proof.deinit();
    const conflicting = manifest_mod.ModelManifest{ .allocator = a, .gguf_path = manifest.gguf_path, .safetensors_path = manifest.gguf_path };
    try std.testing.expectError(error.ModelArtifactsChanging, BoundModelSource.init(&proof, alias, .gguf, &conflicting, null));
}

test "model selection rejects reused unmanaged manifests after directory alias switches" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][]const u8{ "first", "second" }) |name| {
        try tmp.dir.createDirPath(io, name);
        var dir = try tmp.dir.openDir(io, name, .{});
        defer dir.close(io);
        try writeTinyHeadSafetensorsForModelManagerTest(dir, a, "model.safetensors");
    }
    try tmp.dir.symLink(io, "first", "current", .{ .is_directory = true });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const alias = try std.fs.path.join(a, &.{ root, "current" });
    defer a.free(alias);
    var manifest = try manifest_mod.loadFromDir(a, alias);
    defer manifest.deinit();
    var copy = try manifest.clone(a);
    defer copy.deinit();
    try std.testing.expectEqualStrings(manifest.source_root.?, copy.source_root.?);
    var proof = try ComponentConstruction.init(a, io, .{ .path = alias, .publication = null }, null, null);
    defer proof.deinit();
    var bound = try BoundModelSource.init(&proof, alias, .directory, &manifest, null);
    defer bound.deinit();
    try tmp.dir.deleteFile(io, "current");
    try tmp.dir.symLink(io, "second", "current", .{ .is_directory = true });
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    manager.admission_enabled = true;
    try std.testing.expectError(error.ModelArtifactsChanging, manager.loadArtifactSession(alias, manifest));
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    var fresh = try manifest_mod.loadFromDir(a, alias);
    defer fresh.deinit();
    try boundSourceAllocationScenario(a, alias, alias, .directory, &fresh);
    // A replacement at the same canonical pathname is a new installation too.
    try tmp.dir.deleteFile(io, "current");
    try tmp.dir.symLink(io, "first", "current", .{ .is_directory = true });
    try tmp.dir.rename("first", tmp.dir, "old-first", io);
    try tmp.dir.rename("second", tmp.dir, "first", io);
    try std.testing.expectError(error.ModelArtifactsChanging, bound.finish(&proof));
    try std.testing.expectError(error.ModelArtifactsChanging, manager.loadArtifactSession(alias, manifest));
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
}

test "model selection companion ONNX sources preserve tensor context" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const names = [_][]const u8{ "vision_model.onnx", "audio_model.onnx", "text_projection.onnx", "visual_projection.onnx", "audio_projection.onnx" };
    const prefixes = [_][]const u8{ "vision_model.weight", "audio_model.weight", "text_projection.weight", "visual_projection.weight", "audio_projection.weight" };
    for ([_]bool{ false, true }) |managed_model| for (names, prefixes, 0..) |name, prefix, component_index| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var builder = ml.graph.Builder.init(&graph);
        const input = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
        const weight = try builder.parameter("weight", ml.graph.Shape.init(.f32, &.{4}));
        try graph.markOutput(try builder.add(input, weight));
        const values = [_]f32{ 1, 2, 3, 4 };
        var exported = try onnx_graph.exportGraphWithExternalData(a, &graph, .{ .parameter_initializers = &.{.{ .name = "weight", .shape = ml.graph.Shape.init(.f32, &.{4}), .data = .{ .raw_bytes = std.mem.asBytes(&values) } }} }, "weights.data");
        defer exported.deinit(a);
        try tmp.dir.createDirPath(io, "blobs");
        try tmp.dir.writeFile(io, .{ .sub_path = "blobs/graph", .data = exported.model_bytes });
        try tmp.dir.symLink(io, "blobs/graph", name, .{});
        try tmp.dir.writeFile(io, .{ .sub_path = "weights.data", .data = exported.external_data.?.bytes });
        // A wrong base must not silently select valid but different weights.
        try tmp.dir.writeFile(io, .{ .sub_path = "blobs/weights.data", .data = std.mem.asBytes(&[_]f32{ 10, 20, 30, 40 }) });
        if (managed_model) try writeArtifactIdentityTestReceipt(tmp.dir, &.{ name, "weights.data", "blobs/weights.data" });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        const alias = try std.fs.path.join(a, &.{ root, name });
        defer a.free(alias);
        var manifest = try manifest_mod.loadFromDir(a, root);
        defer manifest.deinit();
        try std.testing.expect(manifest.onnx_path == null);
        const component_path = ([_]?[]const u8{ manifest.visual_model_path, manifest.audio_model_path, manifest.text_projection_path, manifest.visual_projection_path, manifest.audio_projection_path })[component_index].?;
        var listing = try manifest_mod.loadListingFromDir(a, root);
        defer listing.deinit();
        const listing_path = ([_]?[]const u8{ listing.visual_model_path, listing.audio_model_path, listing.text_projection_path, listing.visual_projection_path, listing.audio_projection_path })[component_index].?;
        try std.testing.expectEqualStrings(component_path, listing_path);
        const access = try @import("../models/tensor_access.zig").openFromManifest(a, manifest);
        defer access.deinit();
        var record = try access.getRecord(a, prefix);
        defer record.deinit();
        try std.testing.expectEqualSlices(u8, std.mem.asBytes(&values), record.raw_bytes);
        var sm = backends.SessionManager.initWithIo(a, io);
        sm.preferred_backends = &.{.native};
        sm.native_call_policy = .in_process;
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        manager.admission_enabled = true;
        const selected = ResolvedModel{ .path = root, .publication = manifest.managed_publication };
        var good = try manager.loadManagedSessionWithAdmission(alias, &.{.native}, null, null, selected, null);
        defer good.deinit();
        try expectBoundSourceOutput(good.session, false);
        var component = try manager.loadManagedSessionWithAdmission(component_path, &.{.native}, null, null, selected, null);
        defer component.deinit();
        try expectBoundSourceOutput(component.session, false);
    };
}

test "model selection native directory binding preserves logical task hints" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |managed_model| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.createDirPath(io, "blob");
        try tmp.dir.createDirPath(io, "rerankers");
        var dir = try tmp.dir.openDir(io, "blob", .{});
        defer dir.close(io);
        try writeCancellationTestModel(dir, a);
        if (managed_model) try writeArtifactIdentityTestReceipt(dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
        try tmp.dir.symLink(io, "../blob", "rerankers/model", .{ .is_directory = true });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        const alias = try std.fs.path.join(a, &.{ root, "rerankers/model" });
        defer a.free(alias);
        var direct = try manifest_mod.loadFromDir(a, alias);
        defer direct.deinit();
        try std.testing.expectEqual(manifest_mod.ModelType.reranker, direct.model_type);
        var proof = try ComponentConstruction.init(a, io, .{ .path = alias, .publication = direct.managed_publication }, null, null);
        defer proof.deinit();
        var bound = try BoundModelSource.init(&proof, alias, .directory, null, null);
        defer bound.deinit();
        try std.testing.expectEqual(manifest_mod.ModelType.reranker, bound.view().manifest.?.model_type);
        var sm = backends.SessionManager.initWithIo(a, io);
        sm.preferred_backends = &.{.native};
        sm.native_call_policy = .in_process;
        const original = try sm.loadModel(alias);
        defer original.close();
        try std.testing.expectEqualStrings("logits", original.outputInfo()[0].name);
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        var loaded = try manager.loadManagedSessionWithAdmission(alias, &.{.native}, null, null, .{ .path = alias, .publication = direct.managed_publication }, null);
        defer loaded.deinit();
        try std.testing.expectEqualStrings("logits", loaded.session.outputInfo()[0].name);
    }
}

test "model selection explicit legacy graph selection never rediscovers defaults" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |managed_model| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var builder = ml.graph.Builder.init(&graph);
        const input_node = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
        try graph.markOutput(input_node);
        const bytes = try onnx_graph.exportGraph(a, &graph, .{});
        defer a.free(bytes);
        try tmp.dir.createDirPath(io, "blobs");
        try tmp.dir.writeFile(io, .{ .sub_path = "blobs/graph", .data = bytes });
        try tmp.dir.symLink(io, "blobs/graph", "custom.onnx", .{});
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        const path = try std.fs.path.join(a, &.{ root, "custom.onnx" });
        defer a.free(path);
        var sm = backends.SessionManager.initWithIo(a, io);
        sm.preferred_backends = &.{.native};
        sm.native_call_policy = .in_process;
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        manager.admission_enabled = true;
        var supplied = manifest_mod.ModelManifest{ .allocator = a, .onnx_path = path };
        for ([_]bool{ false, true }) |with_default| {
            if (with_default) {
                var other = ml.graph.Graph.init(a);
                defer other.deinit();
                var other_builder = ml.graph.Builder.init(&other);
                const x = try other_builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
                try other.markOutput(try other_builder.add(x, x));
                const other_bytes = try onnx_graph.exportGraph(a, &other, .{});
                defer a.free(other_bytes);
                try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = other_bytes });
            }
            if (managed_model) try writeArtifactIdentityTestReceipt(tmp.dir, if (with_default) &.{ "custom.onnx", "model.onnx" } else &.{"custom.onnx"});
            supplied.managed_publication = try managed_receipt.publicationGeneration(a, io, root);
            var loaded = try manager.loadArtifactSession(root, supplied);
            defer loaded.deinit();
            var input = try backends.Tensor.initFloat32(a, "input", &.{4}, &.{ 1, 2, 3, 4 });
            defer input.deinit();
            const output = try loaded.session.run(&.{input}, a);
            defer {
                for (output) |*tensor| tensor.deinit();
                a.free(output);
            }
            try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, output[0].asFloat32());
            try std.testing.expect(supplied.source_root == null);
            try std.testing.expectEqualStrings(path, supplied.onnx_path.?);
        }
        if (managed_model) {
            // An authenticated default cannot replace an unpublished selection.
            try writeArtifactIdentityTestReceipt(tmp.dir, &.{"model.onnx"});
            supplied.managed_publication = try managed_receipt.publicationGeneration(a, io, root);
            try std.testing.expectError(error.InvalidManagedDownload, manager.loadArtifactSession(root, supplied));
        }
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    }
}

test "model selection catalog GGUF companion aliases preserve source kind" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |managed_model| for ([_][]const u8{ "blobs/audio", "blobs/audio.onnx", "blobs/audio.gguf" }) |target| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try writeCancellationTestModel(tmp.dir, a);
        try tmp.dir.rename("model.gguf", tmp.dir, "clip.gguf", io);
        try tmp.dir.createDirPath(io, "blobs");
        try writeTinyDebertaEncoderGgufForModelManagerTest(tmp.dir, a, target);
        try tmp.dir.symLink(io, target, "clap.gguf", .{});
        try tmp.dir.writeFile(io, .{ .sub_path = "antfly_inference_bundle.json", .data = "{\"family\":\"clipclap_gguf_bundle/v1\",\"clip\":\"clip.gguf\",\"clap\":\"clap.gguf\"}" });
        if (managed_model) try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "clip.gguf", "clap.gguf", "config.json", "tokenizer.json", "antfly_inference_bundle.json" });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        var manifest = try manifest_mod.loadFromDir(a, root);
        defer manifest.deinit();
        try std.testing.expect(manifest.isClipclapGgufBundle());
        try std.testing.expectEqual(ModelSource.Kind.gguf, ModelSource.kindForPath(manifest.audio_model_path.?));
        const logical = try std.fs.path.join(a, &.{ root, "clap.gguf" });
        defer a.free(logical);
        var sm = backends.SessionManager.initWithIo(a, io);
        sm.preferred_backends = &.{.native};
        sm.native_call_policy = .in_process;
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        const selected = ResolvedModel{ .path = root, .publication = manifest.managed_publication };
        var good = try manager.loadManagedSessionWithAdmission(logical, &.{.native}, null, null, selected, null);
        defer good.deinit();
        try expectBoundSourceOutput(good.session, true);
        var discovered = try manager.loadManagedSessionWithAdmission(manifest.audio_model_path.?, &.{.native}, null, null, selected, null);
        defer discovered.deinit();
        try expectBoundSourceOutput(discovered.session, true);
        var listing = try manifest_mod.loadListingFromDir(a, root);
        defer listing.deinit();
        try std.testing.expectEqualStrings(manifest.audio_model_path.?, listing.audio_model_path.?);
    };
}

test "model selection binds optional exports only for their consuming backend" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    try graph.markOutput(try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4})));
    const bytes = try onnx_graph.exportGraph(a, &graph, .{});
    defer a.free(bytes);
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const gguf = try std.fs.path.join(a, &.{ root, "model.gguf" });
    defer a.free(gguf);
    const onnx = try std.fs.path.join(a, &.{ root, "unused.onnx" });
    defer a.free(onnx);
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    manager.admission_enabled = true;
    for (0..3) |state| {
        // Unlisted, absent, then published: native must ignore all alternatives;
        // selecting ONNX must still enforce its own membership and dependency set.
        if (state == 1) try tmp.dir.deleteFile(io, "unused.onnx") else try tmp.dir.writeFile(io, .{ .sub_path = "unused.onnx", .data = bytes });
        try writeArtifactIdentityTestReceipt(tmp.dir, if (state == 2) &.{ "model.gguf", "config.json", "tokenizer.json", "unused.onnx" } else &.{ "model.gguf", "config.json", "tokenizer.json" });
        const publication = try managed_receipt.publicationGeneration(a, io, root);
        for ([_]bool{ false, true }) |provenance| {
            const supplied = manifest_mod.ModelManifest{ .allocator = a, .managed_publication = publication, .gguf_path = gguf, .onnx_path = onnx, .source_root = if (provenance) root else null, .source_root_inode = if (provenance) (try std.Io.Dir.cwd().statFile(io, root, .{})).inode else null };
            var good = try manager.loadArtifactSession(root, supplied);
            defer good.deinit();
            try expectBoundSourceOutput(good.session, true);
            var proof = try ComponentConstruction.init(a, io, .{ .path = root, .publication = publication }, null, null);
            defer proof.deinit();
            var bound = try BoundModelSource.init(&proof, root, .directory, &supplied, null);
            defer bound.deinit();
            const native = (try bound.forBackend(&proof, .native)).?;
            try std.testing.expectEqual(ModelSource.Kind.directory, native.kind);
            try bound.verify(&proof, .native);
            if (state == 2) {
                const selected_graph = (try bound.forBackend(&proof, .wasm)).?;
                try std.testing.expectEqual(ModelSource.Kind.onnx, selected_graph.kind);
                try std.testing.expect(!manifestHasNativeAssets(selected_graph.manifest.?.*));
                try bound.verify(&proof, .wasm);
                var estimate = try ComponentArtifactEstimate.init(a, selected_graph);
                defer estimate.deinit();
                try std.testing.expectEqual(bytes.len, try estimate.encodedBytes());
                const plan = try estimateModelLoadAdmission(selected_graph.path, selected_graph.manifest.?.*, .{ .backend = .wasm }, null, false);
                try std.testing.expectEqual(bytes.len, plan.resident.host_weight_bytes);
                try std.testing.expectEqual(2 * bytes.len, plan.peak.host_weight_bytes);
                // Source preparation and validation do not change the native selection.
                try std.testing.expectEqual(native.manifest, (try bound.forBackend(&proof, .native)).?.manifest);
                // The reverse direction also ignores unused native artifacts.
                const missing = try std.fs.path.join(a, &.{ root, "missing.gguf" });
                defer a.free(missing);
                var alternate = supplied;
                alternate.gguf_path = missing;
                var graph_only = try BoundModelSource.init(&proof, root, .directory, &alternate, null);
                defer graph_only.deinit();
                const graph_source = (try graph_only.forBackend(&proof, .wasm)).?;
                try graph_only.verify(&proof, .wasm);
                try std.testing.expectEqual(ArtifactCandidateKind.onnx, artifactCandidateForBackend(alternate, .wasm).?);
                try std.testing.expectError(error.FileNotFound, graph_only.forBackend(&proof, .native));
                try std.testing.expectEqual(graph_source.path.ptr, (try graph_only.forBackend(&proof, .wasm)).?.path.ptr);
                const session = try sm.loadModelSource(graph_source, null);
                defer session.close();
                var input = try backends.Tensor.initFloat32(a, "input", &.{4}, &.{ 1, 2, 3, 4 });
                defer input.deinit();
                const output = try session.run(&.{input}, a);
                defer {
                    for (output) |*tensor| tensor.deinit();
                    a.free(output);
                }
                try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, output[0].asFloat32());
            } else try std.testing.expectError(if (state == 0) error.InvalidManagedDownload else error.FileNotFound, bound.forBackend(&proof, .wasm));
            try bound.finish(&proof);
        }
    }
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
}

test "model selection native architecture config is bound and authenticated" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    const original = try tmp.dir.readFileAlloc(io, "config.json", a, .limited(65536));
    defer a.free(original);
    const changed = try a.dupe(u8, original);
    defer a.free(changed);
    changed[changed.len - 2] = '7'; // Valid JSON, same size, changed position_buckets.
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = changed });
    try std.testing.expectError(error.ChecksumMismatch, manager.loadArtifactSession(root, man));
    try std.testing.expectError(error.ChecksumMismatch, manager.loadArtifactSession(man.gguf_path.?, man));
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = original });
    var good = try manager.loadArtifactSession(root, man);
    defer good.deinit();
    try expectBoundSourceOutput(good.session, true);
    var direct = try manager.loadArtifactSession(man.gguf_path.?, man);
    defer direct.deinit();
    try expectBoundSourceOutput(direct.session, true);
    // Unpublished root config must not influence a manifest selected from GGUF.
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"bert\",\"hidden_size\":4,\"num_hidden_layers\":1,\"num_attention_heads\":2,\"intermediate_size\":8,\"vocab_size\":16,\"max_position_embeddings\":16}" });
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{"model.gguf"});
    const legacy = manifest_mod.ModelManifest{ .allocator = a, .managed_publication = try managed_receipt.publicationGeneration(a, io, root), .gguf_path = man.gguf_path };
    var embedded = try manager.loadArtifactSession(root, legacy);
    defer embedded.deinit();
    try expectBoundSourceOutput(embedded.session, true);
    // Catalog absence must also ignore an unlisted default that points outside
    // the model. It grants no authority and must not be opened speculatively.
    var foreign = std.testing.tmpDir(.{});
    defer foreign.cleanup();
    try foreign.dir.writeFile(io, .{ .sub_path = "config.json", .data = "{}" });
    const foreign_config = try foreign.dir.realPathFileAlloc(io, "config.json", a);
    defer a.free(foreign_config);
    try tmp.dir.deleteFile(io, "config.json");
    try tmp.dir.symLink(io, foreign_config, "config.json", .{});
    var ignored = try manager.loadArtifactSession(root, legacy);
    defer ignored.deinit();
    try expectBoundSourceOutput(ignored.session, true);
}

test "model selection native binding ignores unconsumed companion exports" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |managed_model| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try writeCancellationTestModel(tmp.dir, a);
        if (managed_model) try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        var man = try manifest_mod.loadFromDir(a, root);
        defer man.deinit();
        man.gliner_head_gguf_path = try std.fs.path.join(a, &.{ root, "unused-head.gguf" });
        man.gliner_head_safetensors_path = try std.fs.path.join(a, &.{ root, "unused-head.safetensors" });
        var sm = backends.SessionManager.initWithIo(a, io);
        sm.preferred_backends = &.{.native};
        sm.native_call_policy = .in_process;
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        manager.admission_enabled = true;
        var loaded = try manager.loadArtifactSession(root, man);
        defer loaded.deinit();
        try expectBoundSourceOutput(loaded.session, true);
    }
}

test "model selection native companion proof and admission select only the GGUF head" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    try writeTinyDebertaEncoderGgufForModelManagerTest(tmp.dir, a, "gliner_head.gguf");
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "config.json", "gliner_head.gguf" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    man.gliner_model_type = try a.dupe(u8, "gliner");
    man.gliner_head_safetensors_path = try std.fs.path.join(a, &.{ root, "missing-alternate.safetensors" });
    man.gguf_projector_path = try std.fs.path.join(a, &.{ root, "missing-projector.gguf" });
    var proof = try ComponentConstruction.init(a, io, .{ .path = root, .publication = man.managed_publication }, null, null);
    defer proof.deinit();
    var bound = try BoundModelSource.init(&proof, root, .directory, &man, null);
    defer bound.deinit();
    const native = (try bound.forBackend(&proof, .native)).?;
    try bound.verify(&proof, .native);
    const mf = native.manifest.?;
    const expected = (try c_file.fileSize(a, mf.gguf_path.?)) + (try c_file.fileSize(a, mf.gliner_head_gguf_path.?));
    try std.testing.expectEqual(expected, try session_factory.estimateNativeWeightBytes(a, mf.*));
    const store = try @import("../models/tensor_store.zig").openFromManifest(a, mf.*);
    defer store.deinit();
    try bound.finish(&proof);
    try tmp.dir.writeFile(io, .{ .sub_path = "gliner_head.gguf", .data = "corrupt selected head" });
    try std.testing.expectError(error.InvalidManagedDownload, bound.verify(&proof, .native));
}

test "model selection GGUF inspection uses explicit selection despite corrupt default" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    try tmp.dir.rename("model.gguf", tmp.dir, "selected.gguf", io);
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    try tmp.dir.writeFile(io, .{ .sub_path = "model.gguf", .data = "invalid unselected export" });
    var report = try session_factory.inspectGgufModelForListing(a, root, man);
    defer if (report) |*value| value.deinit();
    try std.testing.expect(report != null);
    const session = try session_factory.createSessionFromManifest(a, root, &man, .native, .{}, .dynamic, null, null, false);
    defer session.close();
    try expectBoundSourceOutput(session, true);
}

test "native compatibility ignores head exports outside split GLiNER routes" {
    const summary = try validateNativeCompanionsForBackend(
        std.testing.allocator,
        &.{ .allocator = std.testing.allocator, .gliner_head_safetensors_path = "/missing/unused-head.safetensors" },
        .gguf,
        .native,
        .{ .family = .unsupported },
    );
    try std.testing.expect(summary == null);
}

test "model selection direct GGUF accepts catalog aliases and directory snapshots" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |managed_model| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try writeCancellationTestModel(tmp.dir, a);
        try tmp.dir.createDirPath(io, "blobs");
        try tmp.dir.rename("model.gguf", tmp.dir, "blobs/clip", io);
        try tmp.dir.symLink(io, "blobs/clip", "clip.gguf", .{});
        try tmp.dir.symLink(io, "blobs/clip", "clip-alias.gguf", .{});
        try writeTinyDebertaEncoderGgufForModelManagerTest(tmp.dir, a, "clap.gguf");
        try tmp.dir.writeFile(io, .{ .sub_path = "antfly_inference_bundle.json", .data = "{\"family\":\"clipclap_gguf_bundle/v1\",\"clip\":\"clip.gguf\",\"clap\":\"clap.gguf\"}" });
        if (managed_model) try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "clip.gguf", "clip-alias.gguf", "clap.gguf", "config.json", "tokenizer.json", "antfly_inference_bundle.json" });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        var man = try manifest_mod.loadFromDir(a, root);
        defer man.deinit();
        const canonical = try tmp.dir.realPathFileAlloc(io, "clip.gguf", a);
        defer a.free(canonical);
        try std.testing.expect(!std.mem.eql(u8, man.gguf_path.?, canonical));
        var sm = backends.SessionManager.initWithIo(a, io);
        sm.preferred_backends = &.{.native};
        sm.native_call_policy = .in_process;
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        // Directory construction succeeds using this same selected metadata.
        var directory = try manager.loadArtifactSession(root, man);
        defer directory.deinit();
        try expectBoundSourceOutput(directory.session, true);
        var direct = try manager.loadArtifactSession(man.gguf_path.?, man);
        defer direct.deinit();
        try expectBoundSourceOutput(direct.session, true);
        const alternate = try std.fs.path.join(a, &.{ root, "clip-alias.gguf" });
        defer a.free(alternate);
        var same_file = try manager.loadArtifactSession(alternate, man);
        defer same_file.deinit();
        try expectBoundSourceOutput(same_file.session, true);
    }
}

test "model selection unmanaged direct GGUF retains its directory configuration" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    var directory = try manager.loadArtifactSession(root, man);
    defer directory.deinit();
    try expectBoundSourceOutput(directory.session, true);
    var direct = try manager.loadArtifactSession(man.gguf_path.?, man);
    defer direct.deinit();
    try expectBoundSourceOutput(direct.session, true);
}

test "model selection lazy projector authenticates before opening and reuses its store" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeTinyHeadSafetensorsForModelManagerTest(tmp.dir, a, "model.safetensors");
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"bert\",\"hidden_size\":4,\"num_hidden_layers\":1,\"num_attention_heads\":2,\"intermediate_size\":8,\"vocab_size\":16}" });
    try writeTinyProjectorGgufForModelManagerTest(tmp.dir, a, "mmproj.gguf", .gemma4_image);
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.safetensors", "config.json", "mmproj.gguf" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    try std.testing.expect(man.gguf_projector_path != null);
    const payload = try tmp.dir.readFileAlloc(io, "mmproj.gguf", a, .limited(1024 * 1024));
    defer a.free(payload);
    const original_byte = payload[payload.len - 1];
    payload[payload.len - 1] = 1;
    try tmp.dir.writeFile(io, .{ .sub_path = "mmproj.gguf", .data = payload });
    const selected = ResolvedModel{ .path = root, .publication = man.managed_publication };
    var proof = try ComponentConstruction.init(a, io, selected, null, null);
    defer proof.deinit();
    var bound = try BoundModelSource.init(&proof, root, .directory, &man, null);
    defer bound.deinit();
    _ = (try bound.forBackend(&proof, .native)).?;
    try bound.verify(&proof, .native);
    try bound.finish(&proof);
    var sm = backends.SessionManager.initWithIo(a, io);
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    var model: LoadedModel = undefined;
    model.allocator = a;
    model.model_dir = root;
    model.managed_publication = man.managed_publication;
    model.session_manager = &sm;
    model.host = manager.loadedRuntimeHost();
    model.manifest = man;
    model.projector_store_mutex = .unlocked;
    model.projector_store = null;
    const control = selected.bind(InferenceExecutionControl{ .io = io });
    try std.testing.expectError(error.ChecksumMismatch, model.ensureProjectorStoreWithControl(control));
    try std.testing.expect(model.projector_store == null);
    payload[payload.len - 1] = original_byte;
    try tmp.dir.writeFile(io, .{ .sub_path = "mmproj.gguf", .data = payload });
    const store = (try model.ensureProjectorStoreWithControl(control)).?;
    defer store.close();
    // A resident projector must not allocate or rehash its weights on warm use.
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    model.allocator = failing.allocator();
    try std.testing.expect(store == (try model.ensureProjectorStoreWithControl(control)).?);
    model.allocator = a;
}

fn writeJinaAdapterTestModel(dir: std.Io.Dir, a: std.mem.Allocator) !void {
    try writeTinyHeadSafetensorsForModelManagerTest(dir, a, "model.safetensors");
    try dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"jina_embeddings_v5\",\"hidden_size\":4,\"num_hidden_layers\":1,\"num_attention_heads\":2,\"num_key_value_heads\":2,\"intermediate_size\":8,\"vocab_size\":16}" });
    try dir.createDirPath(std.testing.io, "adapters/retrieval");
    try dir.writeFile(std.testing.io, .{ .sub_path = "adapters/retrieval/adapter_config.json", .data = "{\"r\":1,\"lora_alpha\":1}" });
    try writeTinyHeadSafetensorsForModelManagerTest(dir, a, "adapters/retrieval/adapter_model.safetensors");
}

test "model selection Jina adapter absence is captured and partial publication is rejected" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJinaAdapterTestModel(tmp.dir, a);
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.safetensors", "config.json" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    try std.testing.expect(man.jina_adapter_selection_captured);
    try std.testing.expect((try man.jinaRetrievalAdapter()) == null);
    // Even an invalid unlisted default cannot alter the captured construction.
    try tmp.dir.writeFile(io, .{ .sub_path = "adapters/retrieval/adapter_config.json", .data = "[]" });
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    for ([_]bool{ false, true }) |legacy| {
        var supplied = man;
        if (legacy) supplied.jina_adapter_selection_captured = false;
        var loaded = try manager.loadArtifactSession(root, supplied);
        defer loaded.deinit();
    }
    var listing = try manifest_mod.loadListingFromDir(a, root);
    defer listing.deinit();
    try std.testing.expect((try listing.jinaRetrievalAdapter()) == null);
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.safetensors", "config.json", "adapters/retrieval/adapter_config.json" });
    var partial = try manifest_mod.loadFromDir(a, root);
    defer partial.deinit();
    try std.testing.expectError(error.IncompleteJinaV5Adapter, manager.loadArtifactSession(root, partial));
}

test "model selection Jina adapters authenticate both files and account backend lifetime" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeJinaAdapterTestModel(tmp.dir, a);
    try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.safetensors", "config.json", "adapters/retrieval/adapter_config.json", "adapters/retrieval/adapter_model.safetensors" });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    const adapter = (try man.jinaRetrievalAdapter()).?;
    var proof = try ComponentConstruction.init(a, io, .{ .path = root, .publication = man.managed_publication }, null, null);
    defer proof.deinit();
    var bound = try BoundModelSource.init(&proof, root, .directory, &man, null);
    defer bound.deinit();
    const native = (try bound.forBackend(&proof, .native)).?;
    try bound.verify(&proof, .native);
    try std.testing.expectEqual(native.manifest, (try bound.forBackend(&proof, .native)).?.manifest);
    try std.testing.expectEqual(native.manifest, (try bound.forBackend(&proof, .metal)).?.manifest);
    const empty = ModelLoadAdmissionPlan{ .peak = .{}, .resident = .{} };
    const cpu = try withJinaAdapterAdmission(native.manifest.?.*, .native, empty);
    const gpu = try withJinaAdapterAdmission(native.manifest.?.*, .metal, empty);
    const encoded = try c_file.fileSize(a, adapter.weights);
    try std.testing.expect(cpu.peak.host_weight_bytes > encoded);
    try std.testing.expectEqual(@as(usize, 0), cpu.resident.host_weight_bytes);
    try std.testing.expectEqual(cpu.peak.host_weight_bytes, gpu.peak.host_weight_bytes);
    try std.testing.expectEqual(encoded, gpu.resident.host_weight_bytes);
    const session = try session_factory.createSessionFromManifest(a, native.path, native.manifest.?, .native, .{}, .dynamic, null, null, false);
    defer session.close();
    try bound.finish(&proof);
    inline for (.{ "adapters/retrieval/adapter_config.json", "adapters/retrieval/adapter_model.safetensors" }) |name| {
        const original = try tmp.dir.readFileAlloc(io, name, a, .limited(1024 * 1024));
        defer a.free(original);
        const changed = try a.dupe(u8, original);
        defer a.free(changed);
        changed[changed.len - 2] +%= 1;
        try tmp.dir.writeFile(io, .{ .sub_path = name, .data = changed });
        try std.testing.expectError(error.ChecksumMismatch, bound.verify(&proof, .native));
        try tmp.dir.writeFile(io, .{ .sub_path = name, .data = original });
    }
    try bound.verify(&proof, .native);
    try bound.finish(&proof);
}

test "model selection Jina GGUF CPU ignores adapters reserved for Metal" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try writeCancellationTestModel(tmp.dir, a);
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    var man = try manifest_mod.loadFromDir(a, root);
    defer man.deinit();
    var supplied = man;
    supplied.config_model_arch = "jina_embeddings_v5";
    supplied.jina_adapter_config_path = "/missing/adapter-config.json";
    supplied.jina_adapter_weights_path = "/missing/adapter.safetensors";
    var proof = try ComponentConstruction.init(a, io, .{ .path = root, .publication = null }, null, null);
    defer proof.deinit();
    var bound = try BoundModelSource.init(&proof, root, .directory, &supplied, null);
    defer bound.deinit();
    const native = (try bound.forBackend(&proof, .native)).?;
    try bound.verify(&proof, .native);
    const session = try session_factory.createSessionFromManifest(a, native.path, native.manifest.?, .native, .{}, .dynamic, null, null, false);
    defer session.close();
    try expectBoundSourceOutput(session, true);
    try std.testing.expectError(error.FileNotFound, bound.forBackend(&proof, .metal));
    try std.testing.expectEqual(native.manifest, (try bound.forBackend(&proof, .native)).?.manifest);
}

test "model selection direct tokenizer preparation shares its directory snapshot with the session" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |managed_model| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try writeCancellationTestModel(tmp.dir, a);
        if (managed_model) try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        var man = try manifest_mod.loadFromDir(a, root);
        defer man.deinit();
        var sm = backends.SessionManager.initWithIo(a, io);
        sm.preferred_backends = &.{.native};
        sm.native_call_policy = .in_process;
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        manager.admission_enabled = true;
        {
            var session = try manager.loadArtifactSession(man.gguf_path.?, man);
            defer session.deinit();
            try expectBoundSourceOutput(session.session, true);
            var directory = try manager.loadGenerationTokenizerAssets(root, &man);
            defer directory.deinit();
            var direct = try manager.loadGenerationTokenizerAssets(man.gguf_path.?, &man);
            defer direct.deinit();
            const expected = try directory.tokenizer().encode(a, "a");
            defer a.free(expected);
            const actual = try direct.tokenizer().encode(a, "a");
            defer a.free(actual);
            try std.testing.expect(expected.len != 0);
            try std.testing.expectEqualSlices(i32, expected, actual);
        }
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    }
}

test "model selection tokenizer preparation rejects stale directory provenance before parsing" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][]const u8{ "first", "second" }) |name| {
        try tmp.dir.createDirPath(io, name);
        var dir = try tmp.dir.openDir(io, name, .{});
        defer dir.close(io);
        try writeCancellationTestModel(dir, a);
    }
    try tmp.dir.symLink(io, "first", "current", .{ .is_directory = true });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const alias = try std.fs.path.join(a, &.{ root, "current" });
    defer a.free(alias);
    var man = try manifest_mod.loadFromDir(a, alias);
    defer man.deinit();
    try tmp.dir.deleteFile(io, "current");
    try tmp.dir.symLink(io, "second", "current", .{ .is_directory = true });
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
    defer manager.deinit();
    try std.testing.expectError(error.ModelArtifactsChanging, manager.loadGenerationTokenizerAssets(alias, &man));
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
}

const tokenizerLoadAdmissionPlanWithArtifacts = load_plan.tokenizerLoadAdmissionPlanWithArtifacts;

const estimateModelLoadAdmission = load_plan.estimateModelLoadAdmission;

const withJinaAdapterAdmission = load_plan.withJinaAdapterAdmission;

fn loadSessionForPreferredBackendsWithArtifacts(manager: *ModelManager, preferred_backends: []const backends.BackendType, model_dir: []const u8, man: manifest_mod.ModelManifest, source_session_manager: *const backends.SessionManager, control: ?InferenceExecutionControl, borrowed: ?*PublishedArtifacts) !ManagedSession {
    return session_loader.loadPreferredWithArtifacts(manager.allocator, SessionLoadHost.services(manager), preferred_backends, model_dir, man, source_session_manager, control, borrowed);
}

const modelIdentityWithArtifacts = loaded_runtime.modelIdentityWithArtifacts;

const loadSentencePieceTokenizerWithArtifacts = tokenizer_loader.loadSentencePieceTokenizerWithArtifacts;
test "model selection direct ONNX artifacts share directory data and tokenizer context" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const input = try builder.parameter("input", ml.graph.Shape.init(.f32, &.{4}));
    const weight = try builder.parameter("weight", ml.graph.Shape.init(.f32, &.{4}));
    try graph.markOutput(try builder.add(input, weight));
    const values = [_]f32{ 1, 2, 3, 4 };
    var exported = try onnx_graph.exportGraphWithExternalData(a, &graph, .{ .parameter_initializers = &.{.{ .name = "weight", .shape = ml.graph.Shape.init(.f32, &.{4}), .data = .{ .raw_bytes = std.mem.asBytes(&values) } }} }, "weights.data");
    defer exported.deinit(a);
    for ([_]bool{ false, true }) |managed_model| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.createDirPath(io, "blobs");
        try tmp.dir.writeFile(io, .{ .sub_path = "blobs/graph.onnx", .data = exported.model_bytes });
        try tmp.dir.symLink(io, "blobs/graph.onnx", "model.onnx", .{});
        try tmp.dir.writeFile(io, .{ .sub_path = "weights.data", .data = exported.external_data.?.bytes });
        const other = [_]f32{ 10, 20, 30, 40 };
        try tmp.dir.writeFile(io, .{ .sub_path = "blobs/weights.data", .data = std.mem.asBytes(&other) });
        try tmp.dir.writeFile(io, .{ .sub_path = "tokenizer.json", .data = "{\"version\":\"1.0\",\"model\":{\"type\":\"BPE\",\"vocab\":{\"a\":0,\"b\":1},\"merges\":[]}}" });
        if (managed_model) try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.onnx", "blobs/graph.onnx", "weights.data", "blobs/weights.data", "tokenizer.json" });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        const direct = try std.fs.path.join(a, &.{ root, "model.onnx" });
        defer a.free(direct);
        var man = try manifest_mod.loadFromDir(a, root);
        defer man.deinit();
        try platform.allocator.checkAllAllocationFailures(a, boundSourceAllocationScenario, .{ direct, direct, ModelSource.Kind.onnx, &man });
        try std.testing.expectEqual(man.managed_publication, try managed_receipt.publicationGeneration(a, io, direct));
        var sm = backends.SessionManager.initWithIo(a, io);
        sm.preferred_backends = &.{.native};
        sm.native_call_policy = .in_process;
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        manager.admission_enabled = true;
        {
            var directory_session = try manager.loadArtifactSession(root, man);
            defer directory_session.deinit();
            var graph_selection = man;
            graph_selection.gguf_path = "missing-unused.gguf";
            var direct_session = try manager.loadArtifactSession(direct, graph_selection);
            defer direct_session.deinit();
            try expectBoundSourceOutput(directory_session.session, false);
            try expectBoundSourceOutput(direct_session.session, false);
            var physical_session = try manager.loadArtifactSession(man.onnx_path.?, graph_selection);
            defer physical_session.deinit();
            try expectBoundSourceOutput(physical_session.session, false);
            var legacy = graph_selection;
            legacy.source_root = null;
            legacy.source_root_inode = null;
            legacy.onnx_path = direct;
            legacy.onnx_logical_path = null;
            var legacy_session = try manager.loadArtifactSession(direct, legacy);
            defer legacy_session.deinit();
            try expectBoundSourceOutput(legacy_session.session, false);
            var directory_tokenizer = try manager.loadGenerationTokenizerAssets(root, &man);
            defer directory_tokenizer.deinit();
            var direct_tokenizer = try manager.loadGenerationTokenizerAssets(direct, &man);
            defer direct_tokenizer.deinit();
            const expected = try directory_tokenizer.tokenizer().encode(a, "a");
            defer a.free(expected);
            const actual = try direct_tokenizer.tokenizer().encode(a, "a");
            defer a.free(actual);
            try std.testing.expect(expected.len != 0);
            try std.testing.expectEqualSlices(i32, expected, actual);
        }
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
        {
            var proof = try ComponentConstruction.init(a, io, .{ .path = root, .publication = man.managed_publication }, null, null);
            defer proof.deinit();
            const foreign_base = try std.fs.path.join(a, &.{ root, "blobs" });
            defer a.free(foreign_base);
            // Both candidate data files can be valid receipt members. Changing
            // the selected snapshot's data directory still changes the model.
            try std.testing.expectError(error.ModelArtifactsChanging, BoundModelSource.init(&proof, man.onnx_path.?, .onnx, &man, foreign_base));
            var bound = try BoundModelSource.init(&proof, man.onnx_path.?, .onnx, &man, root);
            defer bound.deinit();
            try std.testing.expectEqualStrings(root, bound.view().external_base_dir.?);
            try std.testing.expectEqualStrings(man.onnx_logical_path.?, bound.view().manifest.?.onnx_logical_path.?);
            try bound.finish(&proof);
        }
        if (managed_model) {
            try tmp.dir.writeFile(io, .{ .sub_path = "weights.data", .data = std.mem.asBytes(&other) });
            try std.testing.expectError(error.ChecksumMismatch, manager.loadArtifactSession(direct, man));
            try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.onnx", "tokenizer.json" });
            var incomplete = try manifest_mod.loadFromDir(a, root);
            defer incomplete.deinit();
            try std.testing.expectError(error.InvalidManagedDownload, manager.loadArtifactSession(direct, incomplete));
            try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
        }
    }
}

test "model selection chunk planning rejects root replacement during construction" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |managed_model| for ([_]?usize{ 2, 3, null }) |replacement_check| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        for ([_][]const u8{ "model", "replacement" }) |name| {
            try tmp.dir.createDirPath(io, name);
            var dir = try tmp.dir.openDir(io, name, .{});
            defer dir.close(io);
            try writeCancellationTestModel(dir, a);
            if (managed_model) try writeArtifactIdentityTestReceipt(dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
        }
        const root = try tmp.dir.realPathFileAlloc(io, "model", a);
        defer a.free(root);
        const Probe = struct {
            dir: std.Io.Dir,
            checks: usize = 0,
            switch_at: ?usize = null,
            fn check(raw: ?*anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                self.checks += 1;
                if (self.switch_at == self.checks) {
                    try self.dir.rename("model", self.dir, "old", std.testing.io);
                    try self.dir.rename("replacement", self.dir, "model", std.testing.io);
                }
            }
        };
        var probe = Probe{ .dir = tmp.dir };
        var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
        defer manager.deinit();
        manager.admission_enabled = true;
        // Trace the final callback on a successful load, then replace at that
        // same checkpoint after parsing and retaining tokenizer admission.
        var baseline = try manager.loadChunkPlanningAssets(root, io, .{ .ptr = &probe, .check_fn = Probe.check });
        baseline.deinit();
        probe.switch_at = replacement_check orelse probe.checks;
        probe.checks = 0;
        if (manager.loadChunkPlanningAssets(root, io, .{ .ptr = &probe, .check_fn = Probe.check })) |returned| {
            var unexpected = returned;
            unexpected.deinit();
            return error.TestExpectedError;
        } else |err| try std.testing.expectEqual(error.ModelArtifactsChanging, err);
        try std.testing.expect(probe.checks >= probe.switch_at.?);
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    };
}

test "model selection planning reuse rejects directory replacement with identical file identities" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model");
    try tmp.dir.createDirPath(io, "replacement");
    var dir = try tmp.dir.openDir(io, "model", .{});
    defer dir.close(io);
    try writeCancellationTestModel(dir, a);
    // Prepare hard links before discovery: replacing the parent will preserve
    // every file's inode, timestamps and bytes, so file signatures cannot help.
    for ([_][]const u8{ "model.gguf", "config.json", "tokenizer.json" }) |name| {
        const replacement = try std.fs.path.join(a, &.{ "replacement", name });
        defer a.free(replacement);
        try std.Io.Dir.hardLink(dir, name, tmp.dir, replacement, io, .{});
    }
    const root = try tmp.dir.realPathFileAlloc(io, "model", a);
    defer a.free(root);
    var manager = ModelManager.init(a, backends.SessionManager.initWithIo(a, io));
    defer manager.deinit();
    var assets = try manager.loadChunkPlanningAssets(root, io, .{});
    defer assets.deinit();
    try assets.verify();
    try tmp.dir.rename("model", tmp.dir, "old", io);
    try tmp.dir.rename("replacement", tmp.dir, "model", io);
    var paths: [9][]const u8 = undefined;
    try std.testing.expectEqual(assets.planning.signature, try componentDependencySignature(io, chunkPlanningPaths(assets.manifest, &paths)));
    try std.testing.expectError(error.ModelArtifactsChanging, assets.verify());
}

test "model selection resident planning revalidates identical repulls and retires changed content" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |managed_model| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        for ([_][]const u8{ "model", "replacement" }) |name| {
            try tmp.dir.createDirPath(io, name);
            var dir = try tmp.dir.openDir(io, name, .{});
            defer dir.close(io);
            try writeCancellationTestModel(dir, a);
            if (managed_model) try writeArtifactIdentityTestReceipt(dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
        }
        const root = try tmp.dir.realPathFileAlloc(io, "model", a);
        defer a.free(root);
        var sm = backends.SessionManager.initWithIo(a, io);
        sm.preferred_backends = &.{.native};
        sm.native_call_policy = .in_process;
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        manager.admission_enabled = true;
        var original = try manager.acquireFromDir(root);
        defer original.release();
        const runtime_id = original.get().runtime_id;
        const digest = try original.get().chunkPlanningIdentity();
        try tmp.dir.rename("model", tmp.dir, "old", io);
        try tmp.dir.rename("replacement", tmp.dir, "model", io);
        try std.testing.expectEqual(original.get().managed_publication, try managed_receipt.publicationGeneration(a, io, root));
        // A replacement with the same content needs one revalidation, then
        // retains the refreshed stamp signature and existing resident runtime.
        try std.testing.expectEqual(digest, try original.get().chunkPlanningIdentity());
        try std.testing.expectEqual(digest, try original.get().chunkPlanningIdentity());
        try std.testing.expect(!original.get().retired);
        var same = try manager.acquireFromDir(root);
        try std.testing.expectEqual(runtime_id, same.get().runtime_id);
        same.release();
        try tmp.dir.writeFile(io, .{ .sub_path = "model/tokenizer.json", .data = "{\"version\":\"1.0\",\"model\":{\"type\":\"BPE\",\"vocab\":{\"a\":1,\"b\":0},\"merges\":[]}}\n" });
        if (managed_model) {
            var dir = try tmp.dir.openDir(io, "model", .{});
            defer dir.close(io);
            try writeArtifactIdentityTestReceipt(dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
        }
        try std.testing.expectError(error.ModelArtifactsChanging, original.get().chunkPlanningIdentity());
        try std.testing.expect(original.get().retired);
        var fresh = try manager.acquireFromDir(root);
        defer fresh.release();
        try std.testing.expect(fresh.get().runtime_id != runtime_id);
        const ids = try fresh.get().getTokenizer().encode(a, "a");
        defer a.free(ids);
        try std.testing.expectEqualSlices(i32, &.{1}, ids);
        try std.testing.expect(!std.mem.eql(u8, &digest, &try fresh.get().chunkPlanningIdentity()));
    }
}

test "model selection cold primary retains its root proof through final adoption" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for ([_]bool{ false, true }) |managed_model| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        for ([_][]const u8{ "model", "replacement" }) |name| {
            try tmp.dir.createDirPath(io, name);
            var dir = try tmp.dir.openDir(io, name, .{});
            defer dir.close(io);
            try writeCancellationTestModel(dir, a);
            if (managed_model) try writeArtifactIdentityTestReceipt(dir, &.{ "model.gguf", "config.json", "tokenizer.json" });
        }
        const root = try tmp.dir.realPathFileAlloc(io, "model", a);
        defer a.free(root);
        const Probe = struct {
            dir: std.Io.Dir,
            checks: usize = 0,
            switch_at: ?usize = null,
            fn check(raw: ?*anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                self.checks += 1;
                if (self.switch_at == self.checks) {
                    try self.dir.rename("model", self.dir, "old", std.testing.io);
                    try self.dir.rename("replacement", self.dir, "model", std.testing.io);
                }
            }
        };
        var probe = Probe{ .dir = tmp.dir };
        const control = InferenceExecutionControl{ .ptr = &probe, .check_fn = Probe.check };
        var sm = backends.SessionManager.initWithIo(a, io);
        sm.preferred_backends = &.{.native};
        sm.native_call_policy = .in_process;
        {
            var baseline = ModelManager.init(a, sm);
            defer baseline.deinit();
            baseline.admission_enabled = true;
            var handle = try baseline.loadFromDirUncached(root, &baseline.session_manager, true, null, 0, control);
            defer handle.release();
        }
        probe.switch_at = probe.checks;
        probe.checks = 0;
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        manager.admission_enabled = true;
        try std.testing.expectError(error.ModelArtifactsChanging, manager.loadFromDirUncached(root, &manager.session_manager, true, null, 0, control));
        try std.testing.expectEqual(probe.switch_at.?, probe.checks);
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    }
}

test "model selection retained ONNX handles defeat external leaf and parent ABA switches" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    for (0..3) |shape_case| for ([_]bool{ false, true }) |parent_switch| {
        const dims: []const i64 = if (shape_case == 0) &.{4} else if (shape_case == 1) &.{ 4, 8 } else &.{32};
        const count: usize = if (shape_case == 0) 4 else 32;
        var values: [32]f32 = undefined;
        var other: [32]f32 = undefined;
        for (&values, &other, 0..) |*value, *replacement, i| {
            value.* = @floatFromInt(i + 1);
            replacement.* = value.* * 10;
        }
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var builder = ml.graph.Builder.init(&graph);
        const input = try builder.parameter("input", ml.graph.Shape.init(.f32, dims));
        const weight = try builder.parameter("weight", ml.graph.Shape.init(.f32, dims));
        try graph.markOutput(try builder.add(input, weight));
        var exported = try onnx_graph.exportGraphWithExternalData(a, &graph, .{ .parameter_initializers = &.{.{ .name = "weight", .shape = ml.graph.Shape.init(.f32, dims), .data = .{ .raw_bytes = std.mem.sliceAsBytes(values[0..count]) } }} }, "weights.data");
        defer exported.deinit(a);
        var replacement_graph = ml.graph.Graph.init(a);
        defer replacement_graph.deinit();
        var replacement_builder = ml.graph.Builder.init(&replacement_graph);
        const replacement_input = try replacement_builder.parameter("input", ml.graph.Shape.init(.f32, dims));
        try replacement_graph.markOutput(try replacement_builder.add(replacement_input, replacement_input));
        const replacement_bytes = try onnx_graph.exportGraph(a, &replacement_graph, .{});
        defer a.free(replacement_bytes);
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.createDirPath(io, "files");
        try tmp.dir.createDirPath(io, "foreign");
        try tmp.dir.writeFile(io, .{ .sub_path = "files/graph.onnx", .data = exported.model_bytes });
        try tmp.dir.writeFile(io, .{ .sub_path = "foreign/graph.onnx", .data = replacement_bytes });
        try tmp.dir.writeFile(io, .{ .sub_path = "files/first.data", .data = exported.external_data.?.bytes });
        try tmp.dir.writeFile(io, .{ .sub_path = "foreign/first.data", .data = std.mem.sliceAsBytes(other[0..count]) });
        try tmp.dir.symLink(io, "files/graph.onnx", "model.onnx", .{});
        try tmp.dir.symLink(io, "files/first.data", "weights.data", .{});
        try writeArtifactIdentityTestReceipt(tmp.dir, &.{ "model.onnx", "weights.data" });
        const root = try tmp.dir.realPathFileAlloc(io, ".", a);
        defer a.free(root);
        const path = try std.fs.path.join(a, &.{ root, "model.onnx" });
        defer a.free(path);
        const Probe = struct {
            dir: std.Io.Dir,
            parent: bool,
            authenticated: bool = false,
            checks: usize = 0,
            var current: *@This() = undefined;
            var original: @FieldType(std.Io.VTable, "dirOpenFile") = undefined;
            fn open(raw: ?*anyopaque, dir: std.Io.Dir, name: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
                if (std.mem.endsWith(u8, name, "/files/first.data")) current.authenticated = true;
                return original(raw, dir, name, options);
            }
            fn check(raw: ?*anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw.?));
                if (!self.authenticated) return;
                self.checks += 1;
                if (self.checks != 1 and self.checks != 3) return;
                if (self.parent) {
                    if (self.checks == 1) {
                        try self.dir.rename("files", self.dir, "saved", std.testing.io);
                        try self.dir.symLink(std.testing.io, "foreign", "files", .{ .is_directory = true });
                    } else {
                        try self.dir.deleteFile(std.testing.io, "files");
                        try self.dir.rename("saved", self.dir, "files", std.testing.io);
                    }
                } else {
                    try self.dir.deleteFile(std.testing.io, "weights.data");
                    try self.dir.symLink(std.testing.io, if (self.checks == 1) "foreign/first.data" else "files/first.data", "weights.data", .{});
                }
            }
        };
        var probe = Probe{ .dir = tmp.dir, .parent = parent_switch };
        var table = io.vtable.*;
        Probe.current = &probe;
        Probe.original = table.dirOpenFile;
        table.dirOpenFile = Probe.open;
        const counted = std.Io{ .userdata = io.userdata, .vtable = &table };
        var sm = backends.SessionManager.initWithIo(a, counted);
        sm.preferred_backends = &.{.native};
        sm.native_call_policy = .in_process;
        var manager = ModelManager.init(a, sm);
        defer manager.deinit();
        manager.admission_enabled = true;
        const selected = ResolvedModel{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) };
        {
            var loaded = try manager.loadManagedSessionWithAdmission(path, &.{.native}, null, .{ .io = counted, .ptr = &probe, .check_fn = Probe.check }, selected, null);
            defer loaded.deinit();
            try std.testing.expectEqual(@as(usize, 3), probe.checks);
            // Construction's publication fence and all retained descriptors are
            // gone here. Execution owns decoded tensors, not borrowed readers.
            const zeros: [32]f32 = @splat(0);
            var tensor = try backends.Tensor.initFloat32(a, "input", dims, zeros[0..count]);
            defer tensor.deinit();
            const outputs = try loaded.session.run(&.{tensor}, a);
            defer {
                for (outputs) |*output| output.deinit();
                a.free(outputs);
            }
            try std.testing.expectEqualSlices(f32, values[0..count], outputs[0].asFloat32());
        }
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, manager.currentAdmissionAmounts());
    };
}

test "model selection many-file ONNX model loads within a bounded descriptor budget" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const count = 96;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const proto = onnx_graph.proto;
    const initializers = try scratch.alloc(proto.TensorProto, count);
    const nodes = try scratch.alloc(proto.NodeProto, count);
    const members = try scratch.alloc([]const u8, count + 1);
    members[0] = "model.onnx";
    var dims = [_]i64{1};
    const value: f32 = 1;
    var previous: []const u8 = "input";
    for (initializers, nodes, 0..) |*tensor, *node, i| {
        const name = try std.fmt.allocPrint(scratch, "w{d}", .{i});
        const filename = try std.fmt.allocPrint(scratch, "w{d}.data", .{i});
        const output = try std.fmt.allocPrint(scratch, "o{d}", .{i});
        const ext = try scratch.alloc(proto.ExternalDataEntry, 2);
        ext[0] = .{ .key = "location", .value = filename };
        ext[1] = .{ .key = "length", .value = "4" };
        tensor.* = .{ .name = name, .dims = &dims, .data_type = .float32, .data_location = .external, .external_data = ext };
        const ni = try scratch.alloc([]const u8, 2);
        ni[0] = previous;
        ni[1] = name;
        const no = try scratch.alloc([]const u8, 1);
        no[0] = output;
        node.* = .{ .op_type = "Add", .inputs = ni, .outputs = no };
        previous = output;
        members[i + 1] = filename;
        try tmp.dir.writeFile(io, .{ .sub_path = filename, .data = std.mem.asBytes(&value) });
    }
    var shape = [_]proto.TensorShapeProto.Dimension{.{ .dim_value = 1 }};
    var inputs = [_]proto.ValueInfoProto{.{ .name = "input", .type_proto = .{ .tensor_type = .{ .elem_type = .float32, .shape = .{ .dims = &shape } } } }};
    var outputs = [_]proto.ValueInfoProto{.{ .name = previous, .type_proto = inputs[0].type_proto }};
    var opsets = [_]proto.OpsetImport{.{ .domain = "", .version = 14 }};
    const bytes = try onnx_graph.serializeModel(a, &.{ .ir_version = 7, .opset_import = &opsets, .graph = .{ .name = "many-files", .nodes = nodes, .initializers = initializers, .inputs = &inputs, .outputs = &outputs } });
    defer a.free(bytes);
    try tmp.dir.writeFile(io, .{ .sub_path = "model.onnx", .data = bytes });
    try writeArtifactIdentityTestReceipt(tmp.dir, members);
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const path = try std.fs.path.join(a, &.{ root, "model.onnx" });
    defer a.free(path);
    var sm = backends.SessionManager.initWithIo(a, io);
    sm.preferred_backends = &.{.native};
    sm.native_call_policy = .in_process;
    var manager = ModelManager.init(a, sm);
    defer manager.deinit();
    var loaded = try manager.loadManagedSessionWithAdmission(path, &.{.native}, null, null, .{ .path = root, .publication = try managed_receipt.publicationGeneration(a, io, root) }, null);
    defer loaded.deinit();
    var input = try backends.Tensor.initFloat32(a, "input", &.{1}, &.{0});
    defer input.deinit();
    const actual = try loaded.session.run(&.{input}, a);
    defer {
        for (actual) |*tensor| tensor.deinit();
        a.free(actual);
    }
    try std.testing.expectEqualSlices(f32, &.{96}, actual[0].asFloat32());
}
