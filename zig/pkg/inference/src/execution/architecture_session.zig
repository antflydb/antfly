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

//! Architecture session execution and publication of prepared model owners.
//! Artifact loading and residency preparation belong to models; backend
//! construction belongs to backends.compute_factory. This owner binds their
//! stable storage to architecture execution and the public session contract.

const std = @import("std");
const backend_compatibility = @import("../models/backend_compatibility.zig");
const cuda_weights = @import("../models/cuda_weights.zig");
const device_weights = @import("../models/device_weights.zig");
const host_weights = @import("../models/host_weights.zig");
const build_options = @import("build_options");
const platform = @import("antfly_platform");

const Session = @import("../backends/session.zig").Session;
const ResidentOutputs = @import("../backends/session.zig").ResidentOutputs;
const ResidentTextEmbeddingRequest = @import("../backends/session.zig").ResidentTextEmbeddingRequest;
const Tensor = @import("../backends/tensor.zig").Tensor;
const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
const BackendType = @import("../backends/backends.zig").BackendType;
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;
const incremental_observation = @import("invocation_observation.zig");
const bert = @import("../models/bert.zig");
const t5_mod = @import("../models/t5.zig");
const gpt_mod = @import("../models/gpt.zig");
const qwen35_execution = @import("../architectures/qwen35_execution.zig");
const embedding_gemma2_model = @import("../models/embedding_gemma2.zig");

const whisper_mod = @import("../models/whisper.zig");
const florence_mod = @import("../models/florence.zig");
const clip_mod = @import("../models/clip.zig");
const clap_mod = @import("../models/clap.zig");
const deberta_mod = @import("../models/deberta.zig");
const boundary_bundle = @import("../models/gliner_boundary_bundle.zig");
const boundary_resident = @import("../ops/gliner_boundary_resident.zig");
const bert_arch = @import("../architectures/bert.zig");
const modern_bert_arch = @import("../architectures/modern_bert.zig");
const nomic_bert_arch = @import("../architectures/nomic_bert.zig");
const embedding_gemma2_reference = @import("../architectures/embedding_gemma2_reference.zig");
const layoutlmv3_arch = @import("../architectures/layoutlmv3.zig");
const t5_arch = @import("../architectures/t5.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const embedding_gemma2_arch = @import("../architectures/embedding_gemma2.zig");
const deepseek_v4_arch = @import("../architectures/deepseek_v4.zig");
const whisper_arch = @import("../architectures/whisper.zig");
const clip_arch = @import("../architectures/clip.zig");
const clip_graph = @import("../architectures/clip_graph.zig");
const clap_arch = @import("../architectures/clap.zig");
const florence_arch = @import("../architectures/florence.zig");
const deberta_arch = @import("../architectures/deberta.zig");
const gliner_head = @import("../architectures/gliner_head.zig");
const gliner_decision_head = @import("../architectures/gliner_decision_head.zig");
const gliner_encoder = @import("../models/gliner_encoder.zig");
const gliner_boundary_model = @import("../models/gliner_boundary.zig");
const gliner_decide_qualification = @import("../models/gliner_decide_qualification.zig");
const gliner_specialty = @import("../models/gliner_specialty_qualification.zig");
const gliner_head_graph = @import("../architectures/gliner_head_graph.zig");
const kernel_jit = @import("../graph/kernel_jit.zig");
const graph_runtime = @import("../graph/runtime.zig");
const manifest_mod = @import("../models/manifest.zig");
const ops = @import("../ops/ops.zig");
const NativeCompute = @import("../ops/native_compute.zig").NativeCompute;
const metal_compute_mod = @import("../ops/metal_compute.zig");
const MetalCompute = metal_compute_mod.MetalCompute;
const weight_source_mod = @import("../models/weight_source.zig");
const SafetensorsSource = @import("../models/weight_source.zig").SafetensorsSource;
const LoadedWeight = @import("../models/weight_source.zig").LoadedWeight;
const tensor_store_mod = @import("../models/tensor_store.zig");
const gguf_mod = @import("../gguf/root.zig");
const c_file = @import("../util/c_file.zig");
const runtime = @import("../runtime/root.zig");
const backend_contracts = @import("../graph/backend_contracts.zig");

const cuda_compute_mod = if (build_options.enable_cuda) @import("../ops/cuda/cuda_compute.zig") else struct {};
const a4b_prepared_pack_mod = @import("../ops/cuda/a4b_prepared_pack.zig");
pub const CudaRuntimeStats = if (build_options.enable_cuda) cuda_compute_mod.RuntimeStats else void;
const CudaCapabilityProfile = cuda_weights.CudaCapabilityProfile;

const metal_runtime = if (build_options.enable_metal) @import("../backends/metal_runtime.zig") else struct {
    fn metalDeviceAvailable() bool {
        return false;
    }

    fn validateMetalJitLoadContext(
        config: kernel_jit.Config,
        load_context: kernel_jit.LoadContext,
    ) !void {
        if (!load_context.allowsQualification() and
            (config.mode.failClosed() or config.qualified_profile_path != null or config.profile_capture_only))
        {
            return error.KernelJitRequiredDynamicLoad;
        }
    }
};
const MetalJitRouteScope = if (build_options.enable_metal) metal_runtime.MetalJitRouteScope else void;

const pjrt_lib = if (build_options.enable_pjrt) @import("pjrt") else struct {};

const directQuantEnabled = host_weights.directQuantEnabled;

const gpuHostedQuantExecutionMode = device_weights.gpuHostedQuantExecutionMode;

fn parseMetalGemmaKvDTypeOverride(value: []const u8) ?runtime.kv.pool.KvDType {
    if (std.ascii.eqlIgnoreCase(value, "f16")) return .f16;
    if (std.ascii.eqlIgnoreCase(value, "f32")) return .f32;
    return null;
}

fn metalGemmaKvDTypeOverride() ?runtime.kv.pool.KvDType {
    const value = platform.env.getenv("TERMITE_METAL_GEMMA_KV_DTYPE") orelse
        platform.env.getenv("ANTFLY_INFERENCE_METAL_GEMMA_KV_DTYPE") orelse
        return null;
    return parseMetalGemmaKvDTypeOverride(value);
}

fn graphRuntimeStrategyEnabled(strategy: ?graph_runtime.Strategy) bool {
    return if (strategy) |s| s != .interpreter else false;
}

pub const estimateNativeWeightBytes = device_weights.estimateNativeWeightBytes;

/// Keep backend-specific materialization policy behind the session factory.
/// ModelManager owns admission orchestration, while each backend owns the
/// physical representation it will create.
pub const estimateBackendWeightResidencyBytes = cuda_weights.estimateBackendWeightResidencyBytes;

const glinerBaseWeightKey = host_weights.glinerBaseWeightKey;

const validateNativeBoundaryWeights = host_weights.validateNativeBoundaryWeights;

const captureBoundaryIdentity = host_weights.captureBoundaryIdentity;

const captureGlinerDecisionIdentity = host_weights.captureGlinerDecisionIdentity;

/// Supported model architecture families.
const model_definition = @import("../architectures/model_definition.zig");
const ArchType = model_definition.ArchType;
const ArchConfig = model_definition.ArchConfig;
const legacy_gliner_encoder_config_max_bytes = model_definition.legacy_gliner_encoder_config_max_bytes;
pub const refineGptConfigFromGgufTensorInfo = model_definition.refineGptConfigFromGgufTensorInfo;
const makeBertConfig = model_definition.makeBertConfig;
const detectModelType = model_definition.detectModelType;
const verifyGlinerDecisionSnapshot = model_definition.verifyGlinerDecisionSnapshot;
const sameDigest = model_definition.sameDigest;
const overlayGptStructuralConfig = model_definition.overlayGptStructuralConfig;
const detectArchitectureFromGguf = model_definition.detectArchitectureFromGguf;
const loadLegacyGlinerEncoderConfig = model_definition.loadLegacyGlinerEncoderConfig;
const detectArchitectureWithGgufFile = model_definition.detectArchitectureWithGgufFile;
const detectArchitecture = model_definition.detectArchitecture;

const SessionTask = @import("../backends/session.zig").ModelContract.Task;
pub const TaskOverride = SessionTask;

pub const GenericEncoderArchConfig = union(enum) {
    bert: bert.Config,
    deberta: deberta_mod.Config,
};

const sessionTaskForModelType = device_weights.sessionTaskForModelType;

const deberta_reranker_weight_mirror_min_rows: usize = 128;

fn debertaRerankerPrefersWeightMirrors(enabled: bool, batch: usize, seq_len: usize) bool {
    if (!enabled) return false;
    const rows = std.math.mul(usize, batch, seq_len) catch return false;
    return rows >= deberta_reranker_weight_mirror_min_rows;
}

const sessionEnablesDebertaRerankerWeightMirrors = device_weights.sessionEnablesDebertaRerankerWeightMirrors;

const sessionEnablesImmutableF32WeightBorrow = device_weights.sessionEnablesImmutableF32WeightBorrow;

const sessionPrefersImmutableGlinerF32Mps = device_weights.sessionPrefersImmutableGlinerF32Mps;

/// Metal mirror/cache and graph-plan scratch amounts that ModelManager must
/// reserve for native DeBERTa and GLiNER sessions. An absent reranker
/// architecture hint is treated conservatively because GGUF metadata can still
/// identify DeBERTa at load.
pub fn metalDebertaFastPathAdmissionAmounts(mf: manifest_mod.ModelManifest) runtime.tier.memory.AdmissionAmounts {
    const gliner_bundle_hint = mf.gliner_head_gguf_path != null or
        mf.gliner_head_safetensors_path != null or
        mf.gliner_model_type.len != 0 or
        std.mem.startsWith(u8, mf.inference_bundle_family, "gliner2");
    // A parsed nested encoder family is authoritative. Unknown preserves the
    // legacy split-bundle contract, whose wrapper predates this declaration.
    const gliner_deberta = switch (mf.gliner_span_encoder_family) {
        .deberta => true,
        .modern_bert => false,
        .unknown => gliner_bundle_hint,
    };
    const direct_deberta = mf.config_model_arch.len > 0 and deberta_mod.isDebertaModel(mf.config_model_arch);
    const unknown_reranker = mf.model_type == .reranker and mf.config_model_arch.len == 0;
    if (!gliner_deberta and !direct_deberta and !unknown_reranker) return .{};

    const reservation = deberta_arch.metalFastPathReservationBytes(.{
        .hidden_size = mf.hidden_size,
        .num_hidden_layers = mf.num_hidden_layers,
        .num_attention_heads = mf.num_attention_heads,
        .intermediate_size = mf.intermediate_size,
        .vocab_size = mf.bert_vocab_size,
        .max_position_embeddings = mf.max_position_embeddings,
        .num_labels = mf.num_labels,
        .metal_require_f32_weight_mirrors = mf.gliner_specialty_profile == .pii,
    }, gliner_deberta or mf.model_type == .reranker);
    return .{
        .backend_weight_bytes = reservation.persistent_bytes,
        .backend_scratch_bytes = reservation.scratch_bytes,
    };
}

pub const GlinerBoundaryResidentLoadAmounts = struct {
    peak: runtime.tier.memory.AdmissionAmounts,
    resident: runtime.tier.memory.AdmissionAmounts,
};

/// Exact new FP32 ownership replaces the generic mirrored-DeBERTa reservation.
/// The opened source mapping and separate device allocations coexist. Keep the
/// existing import allowance in the construction peak; workspace retention is
/// admitted separately and is never folded into immutable model bytes.
pub fn glinerBoundaryResidentLoadAmounts(mf: manifest_mod.ModelManifest, source_bytes: usize) !?GlinerBoundaryResidentLoadAmounts {
    if (mf.gliner_architecture != .boundary) return null;
    const config = mf.gliner_boundary_config orelse return error.InvalidGlinerBoundaryConfig;
    if (mf.gliner_boundary_bundle) |receipt| {
        if (receipt.value.precision != .fp32) return null;
    } else if (mf.usesGgufWeights()) return null;
    const admitted = try boundary_resident.estimate(config.backbone);
    if (source_bytes < admitted.weight_bytes) return error.InvalidGlinerBoundaryTensorByteLength;
    const resident = runtime.tier.memory.AdmissionAmounts{
        .host_weight_bytes = try std.math.add(usize, source_bytes, admitted.host_metadata_bytes),
        .backend_weight_bytes = admitted.model_device_bytes,
    };
    var peak = resident;
    peak.host_weight_bytes = try std.math.add(usize, peak.host_weight_bytes, source_bytes / 4);
    peak.host_scratch_bytes = admitted.upload_staging_bytes;
    peak.backend_scratch_bytes = admitted.derived_preparation_device_bytes;
    return .{ .peak = peak, .resident = resident };
}

/// Tree-packed Laya models (models/laya/LAYA.md) run the generic masked
/// encoder and never the resident Metal kernels. Unreadable configs are not
/// packed; session creation reports their real error.
pub fn isPackedLayaModel(allocator: std.mem.Allocator, model_path: []const u8) bool {
    const bytes = @import("../util/c_file.zig").readFileFromDir(allocator, model_path, "config.json") catch return false;
    defer allocator.free(bytes);
    const config = modern_bert_arch.parseConfig(allocator, bytes) catch return false;
    return if (config.laya) |laya| laya.packing.enabled() else false;
}

/// Laya keeps native projection storage plus F32 embedding/norm constants.
/// Two encoded copies bound even an all-F16 artifact; staging and host cache
/// coexist only during preparation. No request workspace is retained here.
pub fn layaResidentLoadAmounts(mf: manifest_mod.ModelManifest, source_bytes: usize) !?GlinerBoundaryResidentLoadAmounts {
    if (comptime !build_options.enable_metal) return null;
    if (!@import("../ops/laya_metal.zig").enabled() or !mf.hasCapability("typed_decisions")) return null;
    if (!modern_bert_arch.isModernBertModel(mf.config_model_arch)) return error.UnsupportedLayaArtifact;
    const metadata: usize = 8 * 1024 * 1024;
    const resident = runtime.tier.memory.AdmissionAmounts{
        .host_weight_bytes = try std.math.add(usize, source_bytes, metadata),
        .backend_weight_bytes = try std.math.add(usize, try std.math.mul(usize, source_bytes, 2), metadata),
    };
    var peak = resident;
    peak.host_weight_bytes = try std.math.add(usize, peak.host_weight_bytes, source_bytes);
    peak.host_scratch_bytes = 4 * 1024 * 1024;
    return .{ .peak = peak, .resident = resident };
}

pub fn prepareLayaResident(session: Session, control: ?InferenceExecutionControl) !void {
    return prepareLayaResidentImpl(session, session, control);
}

fn prepareLayaResidentImpl(session: Session, published: Session, control: ?InferenceExecutionControl) !void {
    if (comptime !build_options.enable_metal) return;
    if (session.vtable != &arch_vtable or !@import("../ops/laya_metal.zig").enabled()) return;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    // Packed rows use the generic masked encoder, not the fused resident kernels.
    if (self.backend_type != .metal or self.arch_config != .modern_bert or self.arch_config.modern_bert.laya == null or isPackedLaya(self)) return;
    if (!@import("../ops/laya_metal.zig").enabledFor(self.arch_config.modern_bert.laya.?)) return;
    try @import("../models/resident_preparation.zig").prepareLaya(.{ .ptr = self, .session = session, .create = createPreparationBackend }, published, self.allocator, self.arch_config.modern_bert, control);
}

pub const UnsupportedTensorTypeCount = host_weights.UnsupportedTensorTypeCount;

pub const GgufTensorSample = host_weights.GgufTensorSample;

pub const GgufInspectionReport = host_weights.GgufInspectionReport;

const qualifiedA4bArtifact = device_weights.qualifiedA4bArtifact;

const requiresModernBertSpanClassifier = host_weights.requiresModernBertSpanClassifier;

/// Admit Q8_0 encoder storage with F32 norms and heads. Other packed formats
/// fail before device allocation. Exact split-artifact identity is checked
/// from the open store before constructing resident weights.
const ensureCudaGlinerModernBertGgufEligible = cuda_weights.ensureCudaGlinerModernBertGgufEligible;

pub fn resolveA4bInferenceConfigForModelListing(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    manifest: manifest_mod.ModelManifest,
    request: ?backend_contracts.A4bInferenceRequest,
) !?backend_contracts.A4bInferenceConfig {
    var report_opt = try inspectGgufModelForListing(allocator, model_path, manifest);
    defer if (report_opt) |*report| report.deinit();
    const report = report_opt orelse {
        if (request != null) return error.A4bUnsupportedArtifact;
        return null;
    };
    const config = report.gpt_config orelse {
        if (request != null) return error.A4bUnsupportedGeometry;
        return null;
    };
    return resolveA4bGptInferenceConfig(config, request, qualifiedA4bArtifact(report));
}

/// Resolve the qualified CUDA policy once for every caller that must agree on
/// its resident envelope (resource admission, CLI preflight, and construction).
pub const resolveCudaA4bInferenceConfigForModelListing = cuda_weights.resolveCudaA4bInferenceConfigForModelListing;

pub fn inspectGgufModel(allocator: std.mem.Allocator, model_path: []const u8) !?GgufInspectionReport {
    var mf = try manifest_mod.loadFromDir(allocator, model_path);
    defer mf.deinit();
    if (!mf.usesGgufWeights()) return null;

    const arch_config = try detectArchitecture(allocator, model_path, mf);
    var store = try tensor_store_mod.openFromManifest(allocator, mf);
    defer store.deinit();
    return try buildGgufInspectionReport(allocator, arch_config, store, requiresModernBertSpanClassifier(mf));
}

/// Cold-listing inspection reads only GGUF metadata and tensor headers. In
/// particular, tokenizer vocabulary arrays are skipped rather than allocated.
pub const inspectGgufModelForListing = host_weights.inspectGgufModelForListing;

/// Whether a Laya checkpoint serves its linear weights as Q8_0 (LAYA.md 1d).
const layaQuantizesWeights = host_weights.layaQuantizesWeights;

/// Create a native CPU session from a model directory.
pub fn createNativeSession(allocator: std.mem.Allocator, model_path: []const u8) !Session {
    return createNativeSessionWithTaskOverride(allocator, model_path, null);
}

pub fn createNativeSessionWithTaskOverride(allocator: std.mem.Allocator, model_path: []const u8, override: ?TaskOverride) !Session {
    return openHostSession(allocator, model_path, override, .{});
}

pub const NativeSessionOptions = struct { observe_allocations: bool = false };
/// Observe allocator-owned model storage from construction, including lazy
/// stores/prepared caches. Mapped file extents remain a separate memory scope.
pub fn createNativeSessionWithOptions(allocator: std.mem.Allocator, model_path: []const u8, override: ?TaskOverride, options: NativeSessionOptions) !Session {
    if (!options.observe_allocations) return createNativeSessionWithTaskOverride(allocator, model_path, override);
    if (@import("allocation_observer.zig").State.forAllocator(allocator) != null) return openHostSession(allocator, model_path, override, .{});
    const observer = try @import("allocation_observer.zig").State.create(allocator, .from_owner_creation);
    defer observer.release();
    return openHostSession(observer.allocator(), model_path, override, .{});
}

/// Host preparation selects the final owner before installing prefetch guards.
/// CUDA upload retains packed encoder storage and materializes weights that
/// would otherwise be lazy; ordinary execution retains its CPU policy.
const HostPreparation = host_weights.HostPreparation;

fn openHostSession(allocator: std.mem.Allocator, model_path: []const u8, override: ?TaskOverride, preparation: HostPreparation) !Session {
    var loaded_model = try model_definition.LoadedModel.open(allocator, model_path);
    defer loaded_model.deinit();
    return prepareLoadedHostSession(allocator, model_path, &loaded_model, override, preparation);
}

/// Consumes the loaded source's weights, retaining its metadata until return.
/// Both host backends use the same normalization, residency and cleanup path.
fn prepareLoadedHostSession(allocator: std.mem.Allocator, model_path: []const u8, loaded_model: *model_definition.LoadedModel, override: ?TaskOverride, preparation: HostPreparation) !Session {
    var prepared = try host_weights.prepare(allocator, model_path, loaded_model, preparation);
    defer prepared.deinit();
    const mf = loaded_model.manifest;
    const arch_config = prepared.architecture;
    const boundary_identity = prepared.boundary_identity;
    const gliner_decision_identity = prepared.decision_identity;
    const task = sessionTaskForModelType(mf.model_type, override);
    const impl = try allocator.create(ArchSession);
    impl.* = .{
        .allocator = allocator,
        .arch_config = arch_config,
        .task = task,
        .gliner_span_encoder_family = mf.gliner_span_encoder_family,
        .deberta_reranker_weight_mirrors = sessionEnablesDebertaRerankerWeightMirrors(
            mf.model_type,
            std.meta.activeTag(arch_config),
            task,
        ),
        .backend_type = switch (preparation.backend) {
            .native => .native,
            .pjrt => .pjrt,
        },
        .boundary_identity = boundary_identity,
        .gliner_decision_identity = gliner_decision_identity,
        .backend_data = switch (preparation.backend) {
            .native => .{ .native = prepared.takeWeights() },
            .pjrt => .{ .pjrt = .{ .native = prepared.takeWeights() } },
        },
    };
    errdefer archClose(impl);
    try impl.model_lifetime.prepareFinalResources();
    // The final union member is in place before queues borrow its address.
    // Client ownership transfers directly to the session; all subsequent
    // failures close it together with the native weight inventory.
    if (build_options.enable_pjrt and preparation.backend == .pjrt) {
        impl.backend_data.pjrt.client = @import("../backends/compute_factory.zig").createPjrtClient(allocator);
    }
    const host_data = switch (preparation.backend) {
        .native => &impl.backend_data.native,
        .pjrt => &impl.backend_data.pjrt.native,
    };
    try @import("../backends/compute_factory.zig").initializeNativeWeightPrefetch(host_data, allocator);
    return archSessionHandle(impl);
}

/// Create a PJRT-backed session from a model directory.
///
/// Weights are loaded via the native CPU host backend. A PJRT client is
/// initialized via `Client.initFromEnv` which searches for the plugin in:
///   1. `PJRT_PLUGIN_PATH` env var
///   2. `~/Library/Application Support/go-xla/pjrt_c_api_cpu_plugin.dylib`
///   3. `~/.antfly/inference/pjrt/darwin-arm64/pjrt_c_api_cpu_plugin.dylib`
///
/// If the plugin is not found, the session is created anyway but without a
/// PJRT client (falls back to pure native CPU execution — no XLA-compiled
/// partitions). Callers can check `getPjrtClientPtr` to see whether a client
/// is actually available.
///
/// For graph-mode (generation) workloads the caller should pass the client
/// pointer to `NativeGenerationPipeline.pjrt_client` so that compiled HLO
/// partitions are dispatched through `attachPjrtExecutors`.
pub fn createPjrtSession(allocator: std.mem.Allocator, model_path: []const u8) !Session {
    return createPjrtSessionWithTaskOverride(allocator, model_path, null);
}

pub fn createPjrtSessionWithTaskOverride(allocator: std.mem.Allocator, model_path: []const u8, override: ?TaskOverride) !Session {
    return openHostSession(allocator, model_path, override, .{ .backend = .pjrt });
}

test "host preparation shares native and PJRT ownership and unwinds allocation failures" {
    if (!build_options.enable_native or build_options.enable_pjrt) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"bert\",\"hidden_size\":4,\"num_hidden_layers\":1,\"num_attention_heads\":1,\"intermediate_size\":8,\"vocab_size\":2}" });
    const header = "{\"bert.embeddings.word_embeddings.weight\":{\"dtype\":\"F32\",\"shape\":[2,4],\"data_offsets\":[0,32]}}";
    const padded_len = comptime std.mem.alignForward(usize, header.len, 8);
    var bytes: [8 + padded_len + 32]u8 = undefined;
    @memset(&bytes, ' ');
    std.mem.writeInt(u64, bytes[0..8], padded_len, .little);
    @memcpy(bytes[8..][0..header.len], header);
    const values = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    @memcpy(bytes[8 + padded_len ..], std.mem.asBytes(&values));
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.safetensors", .data = &bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(path);
    const Check = struct {
        fn run(a: std.mem.Allocator, model_path: []const u8, preparation: HostPreparation, gpt2: bool) !void {
            var loaded = try model_definition.LoadedModel.open(a, model_path);
            defer loaded.deinit();
            var session = try prepareLoadedHostSession(a, model_path, &loaded, .classifier, preparation);
            defer session.close();
            try std.testing.expect(loaded.weights == null);
            const impl: *ArchSession = @ptrCast(@alignCast(session.ptr));
            try std.testing.expectEqual(SessionTask.classifier, impl.task);
            const data = switch (preparation.backend) {
                .native => &impl.backend_data.native,
                .pjrt => &impl.backend_data.pjrt.native,
            };
            try std.testing.expect(data.prefetch_initialized);
            try std.testing.expectEqual(@as(u32, 1), data.resident_weights.count());
            const weight = data.resident_weights.get(if (gpt2) "h.0.attn.c_attn.weight" else "embeddings.word_embeddings.weight").?;
            try std.testing.expectEqualSlices(i64, if (gpt2) &.{ 4, 2 } else &.{ 2, 4 }, weight.tensor.shape);
            try std.testing.expectEqualSlices(u8, if (gpt2) std.mem.asBytes(&[_]f32{ 1, 5, 2, 6, 3, 7, 4, 8 }) else std.mem.asBytes(&[_]f32{ 1, 2, 3, 4, 5, 6, 7, 8 }), weight.tensor.data);
        }
    };
    for ([_]HostPreparation{ .{}, .{ .backend = .pjrt }, .{ .intent = .device_upload } }) |preparation|
        try platform.allocator.checkAllAllocationFailures(allocator, Check.run, .{ path, preparation, false });
    // GPT-2 safetensors Conv1D storage needs the same transpose for PJRT's
    // native fallback as for direct CPU execution.
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"gpt2\",\"n_embd\":2,\"n_layer\":1,\"n_head\":1,\"n_inner\":4,\"vocab_size\":2}" });
    const gpt_header = "{\"h.0.attn.c_attn.weight\":{\"dtype\":\"F32\",\"shape\":[2,4],\"data_offsets\":[0,32]}}";
    @memset(bytes[8 .. 8 + padded_len], ' ');
    @memcpy(bytes[8..][0..gpt_header.len], gpt_header);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.safetensors", .data = &bytes });
    for ([_]HostPreparation{ .{}, .{ .backend = .pjrt } }) |preparation|
        try platform.allocator.checkAllAllocationFailures(allocator, Check.run, .{ path, preparation, true });
}

/// Return the PJRT client as a type-erased `*anyopaque` pointer, or null if
/// this is not a PJRT session or the client was not initialized.
///
/// Intended for callers that need to populate
/// `NativeGenerationPipeline.pjrt_client` so that compiled HLO partitions
/// are dispatched through PJRT during graph-mode generation:
///
///   pipeline.pjrt_client = session_factory.getPjrtClientPtr(session);
pub fn getPjrtClientPtr(session: Session) ?*anyopaque {
    if (!build_options.enable_pjrt) return null;
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .pjrt) return null;
    // client is ?pjrt_lib.pjrt.Client only when enable_pjrt is true (void otherwise).
    // The enable_pjrt check above ensures we only reach here with the real type.
    if (build_options.enable_pjrt) {
        if (self.backend_data.pjrt.client) |*client| {
            return @ptrCast(client);
        }
    }
    return null;
}

pub fn createMetalSession(allocator: std.mem.Allocator, model_path: []const u8) !Session {
    return createMetalSessionWithKernelJit(allocator, model_path, .{});
}

pub fn createMetalSessionWithTaskOverride(allocator: std.mem.Allocator, model_path: []const u8, override: ?TaskOverride) !Session {
    return createMetalSessionWithTaskOverrideAndKernelJit(allocator, model_path, override, .{});
}

pub fn createMetalSessionWithKernelJit(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    config: kernel_jit.Config,
) !Session {
    return createMetalSessionWithKernelJitAndLoadContext(allocator, model_path, config, .dynamic);
}

pub fn createMetalSessionWithKernelJitAndLoadContext(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
) !Session {
    return createMetalSessionWithKernelJitAndLoadContextAndA4bRequest(
        allocator,
        model_path,
        config,
        load_context,
        null,
    );
}

pub fn createMetalSessionWithKernelJitAndLoadContextAndA4bRequest(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
    a4b_request: ?backend_contracts.A4bInferenceRequest,
) !Session {
    return createGpuHostedSessionWithTaskOverride(
        allocator,
        model_path,
        null,
        .metal,
        config,
        load_context,
        a4b_request,
    );
}

pub fn createMetalSessionWithTaskOverrideAndKernelJit(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    config: kernel_jit.Config,
) !Session {
    return createMetalSessionWithTaskOverrideAndKernelJitAndLoadContext(allocator, model_path, override, config, .dynamic);
}

pub fn createMetalSessionWithTaskOverrideAndKernelJitAndLoadContext(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
) !Session {
    return createGpuHostedSessionWithTaskOverride(allocator, model_path, override, .metal, config, load_context, null);
}

pub fn createCudaSession(allocator: std.mem.Allocator, model_path: []const u8) !Session {
    return createCudaSessionWithKernelJit(allocator, model_path, .{});
}

pub const GlinerCudaPrecision = cuda_weights.GlinerCudaPrecision;

/// Explicit precision for qualification and model owners. Automatic selection
/// remains FP32 until a measured artifact/profile is added to release policy.
pub fn createGlinerCudaSessionWithPrecision(allocator: std.mem.Allocator, model_path: []const u8, precision: GlinerCudaPrecision) !Session {
    return createCudaSessionWithRequiredProfile(allocator, model_path, null, .{}, .dynamic, null, null, precision);
}

/// Managed loads retain the release gate even when an explicit precision is
/// requested. The diagnostic constructor above measures candidates without
/// promoting them to serving policy.
pub fn createCudaSessionWithGlinerPrecision(allocator: std.mem.Allocator, model_path: []const u8, config: kernel_jit.Config, load_context: kernel_jit.LoadContext, precision: GlinerCudaPrecision) !Session {
    if (precision == .fp16 or precision == .bf16) return error.UnqualifiedGlinerCudaPrecision;
    return createGlinerCudaSessionWithLoadPolicy(allocator, model_path, config, load_context, precision);
}

/// Qualification tests must exercise the managed admission/load path with the
/// candidate precision. This entry point cannot be compiled into a server.
pub fn createCudaSessionWithGlinerPrecisionForTest(allocator: std.mem.Allocator, model_path: []const u8, config: kernel_jit.Config, load_context: kernel_jit.LoadContext, precision: GlinerCudaPrecision) !Session {
    if (!@import("builtin").is_test) @compileError("GLiNER precision qualification override is test-only");
    return createGlinerCudaSessionWithLoadPolicy(allocator, model_path, config, load_context, precision);
}

fn createGlinerCudaSessionWithLoadPolicy(allocator: std.mem.Allocator, model_path: []const u8, config: kernel_jit.Config, load_context: kernel_jit.LoadContext, precision: GlinerCudaPrecision) !Session {
    var manifest = try manifest_mod.loadListingFromDir(allocator, model_path);
    defer manifest.deinit();
    if (manifest.gliner_architecture != .boundary and manifest.gliner_classification_head != .label_marker_mlp) return error.UnsupportedGlinerCudaPrecision;
    return createCudaSessionWithRequiredProfile(allocator, model_path, null, config, load_context, null, null, precision);
}

test "GLiNER CUDA managed reduced precision remains release gated" {
    for ([_]GlinerCudaPrecision{ .fp16, .bf16 }) |precision| {
        try std.testing.expectError(error.UnqualifiedGlinerCudaPrecision, createCudaSessionWithGlinerPrecision(std.testing.allocator, "/nonexistent-gliner-model", .{}, .dynamic, precision));
    }
}

pub fn createCudaSessionWithTaskOverride(allocator: std.mem.Allocator, model_path: []const u8, override: ?TaskOverride) !Session {
    return createCudaSessionWithTaskOverrideAndKernelJit(allocator, model_path, override, .{});
}

pub fn createCudaSessionWithKernelJit(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    config: kernel_jit.Config,
) !Session {
    return createCudaSessionWithKernelJitAndLoadContext(allocator, model_path, config, .dynamic);
}

pub fn createCudaSessionWithKernelJitAndLoadContext(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
) !Session {
    return createCudaSessionWithKernelJitAndLoadContextAndA4bRequest(
        allocator,
        model_path,
        config,
        load_context,
        null,
    );
}

pub fn createCudaSessionWithKernelJitAndLoadContextAndA4bRequest(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
    a4b_request: ?backend_contracts.A4bInferenceRequest,
) !Session {
    return createCudaSessionWithTaskOverrideAndKernelJitAndLoadContext(
        allocator,
        model_path,
        null,
        config,
        load_context,
        a4b_request,
    );
}

pub fn createCudaSessionWithTaskOverrideAndKernelJit(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    config: kernel_jit.Config,
) !Session {
    return createCudaSessionWithTaskOverrideAndKernelJitAndLoadContext(
        allocator,
        model_path,
        override,
        config,
        .dynamic,
        null,
    );
}

pub fn createCudaSessionWithTaskOverrideAndKernelJitAndLoadContext(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
    a4b_request: ?backend_contracts.A4bInferenceRequest,
) !Session {
    return createCudaSessionWithRequiredProfile(
        allocator,
        model_path,
        override,
        config,
        load_context,
        a4b_request,
        null,
        .fp32,
    );
}

/// Create a Gemma 4 CUDA session with the stronger compiled-training kernel
/// contract.  This is intentionally text-only and dense today: quantized MoE
/// policy training remains fail-closed until its routed-expert VJP is present.
pub fn createGemma4CudaTrainingSession(
    allocator: std.mem.Allocator,
    model_path: []const u8,
) !Session {
    return createCudaSessionWithRequiredProfile(
        allocator,
        model_path,
        null,
        .{},
        .dynamic,
        null,
        .gemma4_training,
        .fp32,
    );
}

fn createCudaSessionWithRequiredProfile(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    config: kernel_jit.Config,
    load_context: kernel_jit.LoadContext,
    a4b_request: ?backend_contracts.A4bInferenceRequest,
    required_profile_override: ?CudaCapabilityProfile,
    gliner_precision: GlinerCudaPrecision,
) !Session {
    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
    var prepared = try cuda_weights.prepare(allocator, model_path, override, config, load_context, a4b_request, required_profile_override, gliner_precision);
    defer prepared.deinit();
    const impl = try allocator.create(ArchSession);
    impl.* = .{
        .allocator = allocator,
        .arch_config = prepared.architecture,
        .recurrent_execution = prepared.recurrent_execution,
        .task = prepared.task,
        .gliner_span_encoder_family = prepared.span_encoder_family,
        .gliner_decision_identity = prepared.decision_identity,
        .gliner_decision_identity_sealed = false,
        .backend_type = .cuda,
        .kernel_jit_config = config,
        .backend_data = .{ .cuda = .{ .compute = prepared.takeCompute() } },
        .boundary_identity = prepared.boundary_identity,
        .budget_floor = prepared.budget_floor,
    };
    errdefer archClose(impl);
    try impl.model_lifetime.prepareFinalResources();
    return archSessionHandle(impl);
}

/// Materialize the immutable, pre-sharded expert payload consumed by the A4B
/// CUDA admission fast path. Shared host-weight preparation preserves catalog
/// normalization and packed-source validation without a temporary CPU session
/// or prefetch worker. It does not require a CUDA device.
pub fn writeCudaA4bPreparedPack(
    allocator: std.mem.Allocator,
    io: std.Io,
    model_path: []const u8,
    output_path: []const u8,
    shard_count: u8,
) !a4b_prepared_pack_mod.WriteReport {
    if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
    var model_manifest = try manifest_mod.loadFromDir(allocator, model_path);
    var manifest_owned = true;
    defer if (manifest_owned) model_manifest.deinit();
    const source_artifact_path = model_manifest.gguf_path orelse return error.A4bCudaPackedStoreUnavailable;
    const config = (try resolveA4bInferenceConfigForModelListing(
        allocator,
        model_path,
        model_manifest,
        .{
            .residency_mode = .resident,
            .memory_budget_mb = backend_contracts.qualified_cuda_a4b_memory_budget_mb,
            .prepared_pack = .off,
        },
    )) orelse return error.A4bUnsupportedGeometry;
    manifest_owned = false; // The loaded source consumes it on all paths.
    var loaded = try model_definition.LoadedModel.fromOwnedManifest(allocator, model_path, model_manifest);
    defer loaded.deinit();
    var prepared = try host_weights.prepare(allocator, model_path, &loaded, .{});
    defer prepared.deinit();
    return cuda_compute_mod.CudaCompute.writeA4bPreparedPack(
        allocator,
        io,
        &prepared.weights,
        config,
        source_artifact_path,
        output_path,
        shard_count,
    );
}

pub fn verifyCudaA4bPreparedPack(
    allocator: std.mem.Allocator,
    model_path: []const u8,
) !a4b_prepared_pack_mod.VerifyReport {
    var model_manifest = try manifest_mod.loadFromDir(allocator, model_path);
    defer model_manifest.deinit();
    const source_artifact_path = model_manifest.gguf_path orelse return error.A4bCudaPackedStoreUnavailable;
    const config = (try resolveA4bInferenceConfigForModelListing(
        allocator,
        model_path,
        model_manifest,
        .{
            .residency_mode = .resident,
            .memory_budget_mb = backend_contracts.qualified_cuda_a4b_memory_budget_mb,
            .prepared_pack = .off,
        },
    )) orelse return error.A4bUnsupportedGeometry;
    return a4b_prepared_pack_mod.verify(
        allocator,
        model_path,
        source_artifact_path,
        .{
            .moe_layer_count = config.geometry.moe_layer_count,
            .expert_count = config.geometry.expert_count,
            .top_k = config.geometry.top_k,
            .hidden_size = config.geometry.hidden_size,
            .expert_intermediate_size = config.geometry.expert_intermediate_size,
            .encoded_expert_bytes = config.geometry.encoded_expert_bytes,
        },
    );
}

const cudaSupportsArch = cuda_weights.cudaSupportsArch;

const cudaProfileForArch = cuda_weights.cudaProfileForArch;

test "cuda support gate admits only supported model roles" {
    const generic_manifest = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    const qwen3_embedder = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .embedder,
        .pooling = .last,
        .embedding_style = .qwen3_embedding,
    };
    const qwen3_vl_generator = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .generator,
        .inference_bundle_family = manifest_mod.qwen3_vl_safetensors_bundle_family,
    };
    try std.testing.expect(cudaSupportsArch(.{ .gpt = .{ .family = .gemma } }, &generic_manifest));
    try std.testing.expect(!cudaSupportsArch(.{ .gpt = .{ .family = .qwen2 } }, &generic_manifest));
    try std.testing.expect(cudaSupportsArch(.{ .clip = .{} }, &generic_manifest));
    try std.testing.expect(cudaSupportsArch(.{ .clap = .{} }, &generic_manifest));
    try std.testing.expect(cudaSupportsArch(.{ .bert = .{} }, &generic_manifest));
    try std.testing.expect(!cudaSupportsArch(.{ .modern_bert = .{} }, &generic_manifest));
    try std.testing.expect(cudaSupportsArch(.{ .modern_bert = .{ .laya = .{} } }, &generic_manifest));
    try std.testing.expect(!cudaSupportsArch(.{ .modern_bert = .{ .laya = .{ .max_len = 1024 } } }, &generic_manifest));
    // Tree-packed rows use the generic masked encoder (models/laya/LAYA.md).
    try std.testing.expect(!cudaSupportsArch(.{ .modern_bert = .{ .laya = .{ .packing = .{ .mode = .question, .max_packed_len = 2048 } } } }, &generic_manifest));
    try std.testing.expect(cudaSupportsArch(.{ .deberta = .{} }, &generic_manifest));
    try std.testing.expect(cudaSupportsArch(.{ .gliner = .{} }, &generic_manifest));
    try std.testing.expect(cudaSupportsArch(.{ .florence = .{} }, &generic_manifest));
    const span_modern_manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .extractor,
        .gliner_architecture = .span,
        .gliner_span_encoder_family = .modern_bert,
    };
    const decide_1b = ArchConfig{ .modern_bert = .{ .hidden_size = 1792, .num_attention_heads = 28, .num_hidden_layers = 28, .intermediate_size = 3840 } };
    try std.testing.expectEqual(CudaCapabilityProfile.gliner25_modern_bert, cudaProfileForArch(decide_1b, .extractor, &span_modern_manifest).?);
    var oversized_decide = decide_1b;
    oversized_decide.modern_bert.max_position_embeddings = 8193;
    try std.testing.expect(cudaProfileForArch(oversized_decide, .extractor, &span_modern_manifest) == null);
    try std.testing.expect(cudaProfileForArch(decide_1b, .generic, &span_modern_manifest) == null);
    if (comptime build_options.enable_cuda) {
        try std.testing.expectEqual(CudaCapabilityProfile.clipclap, cudaProfileForArch(.{ .clip = .{} }, .generic, &generic_manifest).?);
        try std.testing.expectEqual(CudaCapabilityProfile.bert_encoder, cudaProfileForArch(.{ .bert = .{} }, .generic, &generic_manifest).?);
        try std.testing.expectEqual(CudaCapabilityProfile.deberta_reranker, cudaProfileForArch(.{ .deberta = .{} }, .classifier, &generic_manifest).?);
        try std.testing.expectEqual(CudaCapabilityProfile.gliner2, cudaProfileForArch(.{ .gliner = .{} }, .extractor, &generic_manifest).?);
        try std.testing.expectEqual(CudaCapabilityProfile.florence2, cudaProfileForArch(.{ .florence = .{} }, .generic, &generic_manifest).?);
        try std.testing.expectEqual(CudaCapabilityProfile.gemma4, cudaProfileForArch(.{ .gpt = .{ .family = .gemma } }, .generic, &generic_manifest).?);
        try std.testing.expectEqual(CudaCapabilityProfile.qwen3_embedding, cudaProfileForArch(.{ .gpt = .{ .family = .qwen3 } }, .generic, &qwen3_embedder).?);
        try std.testing.expect(cudaProfileForArch(.{ .gpt = .{ .family = .qwen3 } }, .generic, &generic_manifest) == null);
        try std.testing.expectEqual(CudaCapabilityProfile.qwen3_vl_generation, cudaProfileForArch(.{ .gpt = .{ .family = .qwen3_vl } }, .generic, &qwen3_vl_generator).?);
        try std.testing.expect(cudaProfileForArch(.{ .gpt = .{ .family = .qwen3_vl } }, .generic, &generic_manifest) == null);
        try std.testing.expect(cudaProfileForArch(.{ .gpt = .{ .family = .qwen3_vl } }, .classifier, &qwen3_vl_generator) == null);
    }
}

test "GLiNER Decide CUDA GGUF eligibility admits Q8 and rejects other packed formats" {
    const modern_span = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .extractor,
        .gliner_architecture = .span,
        .gliner_span_encoder_family = .modern_bert,
    };
    var dense_types = [_]UnsupportedTensorTypeCount{
        .{ .tensor_type = .{ .known = .F32 }, .count = 199 },
    };
    var quantized_types = [_]UnsupportedTensorTypeCount{
        .{ .tensor_type = .{ .known = .F32 }, .count = 31 },
        .{ .tensor_type = .{ .known = .Q8_0 }, .count = 168 },
    };
    const dense_report = GgufInspectionReport{
        .allocator = std.testing.allocator,
        .architecture = "modernbert",
        .tensor_count = 199,
        .metadata_count = 1,
        .all_tensor_types = &dense_types,
    };
    const quantized_report = GgufInspectionReport{
        .allocator = std.testing.allocator,
        .architecture = "modernbert",
        .tensor_count = 199,
        .metadata_count = 1,
        .all_tensor_types = &quantized_types,
    };

    try ensureCudaGlinerModernBertGgufEligible(modern_span, dense_report);
    dense_types[0].tensor_type = .{ .known = .F16 };
    try ensureCudaGlinerModernBertGgufEligible(modern_span, dense_report);
    try ensureCudaGlinerModernBertGgufEligible(modern_span, quantized_report);
    quantized_types[1].tensor_type = .{ .known = .Q4_0 };
    try std.testing.expectError(error.UnsupportedCudaQuantizedModernBertSpan, ensureCudaGlinerModernBertGgufEligible(modern_span, quantized_report));

    var deberta_span = modern_span;
    deberta_span.gliner_span_encoder_family = .deberta;
    try ensureCudaGlinerModernBertGgufEligible(deberta_span, quantized_report);
}

test "CUDA runtime JIT required dynamic session rejects before model access" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    try std.testing.expectError(
        error.KernelJitRequiredDynamicLoad,
        createCudaSessionWithKernelJitAndLoadContext(
            std.testing.allocator,
            "/private/tmp/antfly-runtime-jit-intentionally-missing-model",
            .{ .mode = .required },
            .dynamic,
        ),
    );
}

test "Metal JIT load policy is backend independent" {
    try metal_runtime.validateMetalJitLoadContext(.{ .mode = .on }, .dynamic);
    try std.testing.expectError(
        error.KernelJitRequiredDynamicLoad,
        metal_runtime.validateMetalJitLoadContext(.{ .mode = .required }, .dynamic),
    );
    try std.testing.expectError(
        error.KernelJitRequiredDynamicLoad,
        metal_runtime.validateMetalJitLoadContext(.{
            .mode = .on,
            .qualified_profile_path = "/tmp/qualified-profile.json",
        }, .dynamic),
    );
    try std.testing.expectError(
        error.KernelJitRequiredDynamicLoad,
        metal_runtime.validateMetalJitLoadContext(.{
            .mode = .shadow,
            .profile_capture_only = true,
        }, .dynamic),
    );
    try metal_runtime.validateMetalJitLoadContext(.{
        .mode = .on,
        .qualified_profile_path = "/tmp/qualified-profile.json",
    }, .startup_preload);
}

fn createGpuHostedSessionWithTaskOverride(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    override: ?TaskOverride,
    backend_type: BackendType,
    kernel_jit_config: kernel_jit.Config,
    kernel_jit_load_context: kernel_jit.LoadContext,
    a4b_request: ?backend_contracts.A4bInferenceRequest,
) !Session {
    var prepared = try device_weights.prepare(allocator, model_path, override, backend_type, kernel_jit_config, kernel_jit_load_context, a4b_request);
    defer prepared.deinit();
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    const impl = try allocator.create(ArchSession);
    impl.* = .{
        .allocator = allocator,
        .arch_config = prepared.architecture,
        .task = prepared.task,
        .gliner_span_encoder_family = prepared.span_family,
        .boundary_identity = prepared.boundary_identity,
        .gliner_decision_identity = prepared.decision_identity,
        .deberta_reranker_weight_mirrors = prepared.deberta_weight_mirrors,
        .backend_type = backend_type,
        .kernel_jit_config = kernel_jit_config,
        .kernel_jit_load_context = .dynamic,
        .metal_jit_scope = prepared.jit_scope,
        .budget_floor = prepared.budget_floor,
        .shared_cache_budget_floor = prepared.cache_floor,
        .backend_data = .{ .metal = prepared.takeWeights() },
    };
    errdefer archClose(impl);
    try impl.model_lifetime.prepareFinalResources();
    try device_weights.finalizeStableStorage(allocator, gpuBackendData(impl), prepared.boundary_identity, .{
        .config = kernel_jit_config,
        .scope = prepared.jit_scope,
        .load_context = kernel_jit_load_context,
    });
    return archSessionHandle(impl);
}

test "gliner boundary cannot fall through to legacy session architecture" {
    const allocator = std.testing.allocator;
    const manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .gliner_architecture = .boundary,
    };
    try std.testing.expectError(error.InvalidGlinerBoundaryConfig, detectArchitecture(
        allocator,
        "/private/tmp/antfly-gliner25-intentionally-missing",
        manifest,
    ));
}

/// Detect the model architecture from config.json.
const buildGgufInspectionReport = host_weights.buildGgufInspectionReport;

const buildGgufInspectionReportFromFile = host_weights.buildGgufInspectionReportFromFile;

const collectNormalizedGgufNames = host_weights.collectNormalizedGgufNames;

const collectMissingRequiredGptWeights = host_weights.collectMissingRequiredGptWeights;

const collectMissingRequiredDebertaWeights = host_weights.collectMissingRequiredDebertaWeights;

const collectMissingRequiredModernBertWeights = host_weights.collectMissingRequiredModernBertWeights;

const collectMissingRequiredGlinerModernBertClassifier = host_weights.collectMissingRequiredGlinerModernBertClassifier;

const tensorTypeSupported = host_weights.tensorTypeSupported;

/// Whether a backend can consume a tensor encoding after the generic GGUF
/// loader has decoded or retained it. Companion-artifact preflight uses this
/// same gate as whole-model inspection so admission and runtime cannot diverge.
pub const ggufTensorTypeSupportsBackend = backend_compatibility.ggufTensorTypeSupportsBackend;

const ensureGgufInspectionCompatible = host_weights.ensureGgufInspectionCompatible;

const metalGgufIncompatibleTensorType = backend_compatibility.metalGgufIncompatibleTensorType;

pub const ggufInspectionSupportsBackend = backend_compatibility.ggufInspectionSupportsBackend;

const normalizeWeightKey = host_weights.normalizeWeightKey;

test "GLiNER ModernBERT composite heads and Laya decision heads keep their own namespaces" {
    const span = ArchConfig{ .modern_bert = .{} };
    const laya = ArchConfig{ .modern_bert = .{ .laya = .{} } };
    var buf: [256]u8 = undefined;
    for ([_][]const u8{ "classifier.0.weight", "span_rep.weight", "count_embed.weight" }) |key|
        try std.testing.expectEqualStrings(key, try normalizeWeightKey(.safetensors, span, key, &buf));
    for ([_][]const u8{ "type_emb.weight", "head.layers.0.self_attn.in_proj_weight", "scorer.1.weight", "act_head.0.weight" }) |key| {
        var expected: [256]u8 = undefined;
        try std.testing.expectEqualStrings(try std.fmt.bufPrint(&expected, "model.{s}", .{key}), try normalizeWeightKey(.safetensors, laya, key, &buf));
    }
    for ([_]ArchConfig{ span, laya }) |config| {
        try std.testing.expectEqualStrings("model.embeddings.tok_embeddings.weight", try normalizeWeightKey(.safetensors, config, "encoder.embeddings.tok_embeddings.weight", &buf));
        try std.testing.expectEqualStrings("model.layers.0.attn.Wqkv.weight", try normalizeWeightKey(.safetensors, config, "layers.0.attn.Wqkv.weight", &buf));
        try std.testing.expectEqualStrings("model.scorer.1.weight", try normalizeWeightKey(.safetensors, config, "model.scorer.1.weight", &buf));
    }
}

const maybeInferGptAttentionLayoutFromStore = host_weights.maybeInferGptAttentionLayoutFromStore;

const findTensorNameByPriority = host_weights.findTensorNameByPriority;

const normalizeGgufGptWeightKey = host_weights.normalizeGgufGptWeightKey;

const appendPackedMoeLazyWeights = host_weights.appendPackedMoeLazyWeights;

const parsePackedMoeTensor = host_weights.parsePackedMoeTensor;

const refineGptConfigFromStore = device_weights.refineGptConfigFromStore;

const shouldLazyLoadWeight = host_weights.shouldLazyLoadWeight;

const isMetalJitLinearWeightKey = device_weights.isMetalJitLinearWeightKey;

const metalJitUsesExactProfileScope = device_weights.metalJitUsesExactProfileScope;

/// Derive Metal JIT slots from quantized matrices that the loaded WeightStore
/// can actually feed to linear dispatch. Global GGUF type presence is too
/// broad: embedding tables and dequant-only tensors must not become required
/// matmul routes.
const includeMetalJitTensorShape = device_weights.includeMetalJitTensorShape;

const includeMetalJitLinearWeightFormats = device_weights.includeMetalJitLinearWeightFormats;

const isMetalJitOutputHeadKey = device_weights.isMetalJitOutputHeadKey;

test "Metal JIT scope records GGUF linear dimensions in logical order" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    var scope = metal_runtime.MetalJitRouteScope.none();
    includeMetalJitTensorShape(&scope, .Q2_K, &.{ 512, 256 }, false, false);
    const shapes = scope.observedShapes(.q2_k);
    try std.testing.expectEqual(@as(usize, 1), shapes.len);
    try std.testing.expectEqual([2]usize{ 256, 512 }, shapes[0]);
    try std.testing.expectEqual(@as(usize, 1), metal_runtime.metalJitProductionRouteCount(scope));

    var fused_scope = metal_runtime.MetalJitRouteScope.none();
    includeMetalJitTensorShape(&fused_scope, .Q2_K, &.{ 512, 512, 8 }, true, false);
    try std.testing.expectEqual([2]usize{ 256, 512 }, fused_scope.observedShapes(.q2_k)[0]);
    includeMetalJitTensorShape(&fused_scope, .Q2_K, &.{ 512, 511, 8 }, true, false);
    try std.testing.expect(!fused_scope.conformance_complete);

    // LM-head projections can execute with multiple rows, so they must remain
    // in scope. A synthetic fixture that exceeds the bounded preload footprint
    // stays on the bundled route instead of being silently treated as covered.
    try std.testing.expect(isMetalJitLinearWeightKey("lm_head.weight", 0, false));
    try std.testing.expect(isMetalJitOutputHeadKey("lm_head.weight"));
    try std.testing.expect(isMetalJitOutputHeadKey("model.language_model.lm_head.weight"));
    try std.testing.expect(isMetalJitOutputHeadKey("output.weight"));
    try std.testing.expect(!isMetalJitOutputHeadKey("model.embed_tokens.weight"));
    try std.testing.expect(!isMetalJitLinearWeightKey("model.embed_tokens.weight", 0, false));
    try std.testing.expect(isMetalJitLinearWeightKey("model.embed_tokens.weight", 0, true));
    var large_head_scope = metal_runtime.MetalJitRouteScope.none();
    // A supported body matrix remains eligible even when the same format's
    // tied vocabulary projection cannot fit the bounded live fixture. The
    // exact shape gate keeps only the body on the generated pipeline.
    includeMetalJitTensorShape(&large_head_scope, .Q8_K, &.{ 4096, 4096 }, false, false);
    includeMetalJitTensorShape(&large_head_scope, .Q8_K, &.{ 4096, 262144 }, false, false);
    try std.testing.expect(!large_head_scope.conformance_complete);
    try std.testing.expectEqual(metal_runtime.MetalJitScopeInvalidReason.fixture_resource_limit, large_head_scope.invalid_reason);
    try std.testing.expectEqual(@as(usize, 1), large_head_scope.observedShapes(.q8_k).len);
    try std.testing.expectEqual(@as(usize, 1), metal_runtime.metalJitProductionRouteCount(large_head_scope));
}

test "Metal exact profile scope ignores unrelated oversized formats and keeps tuned formats fail closed" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;

    try std.testing.expect(metalJitUsesExactProfileScope(.{ .mode = .shadow, .profile_capture_only = true }));
    try std.testing.expect(metalJitUsesExactProfileScope(.{ .mode = .required, .qualified_profile_path = "profile.json" }));
    try std.testing.expect(!metalJitUsesExactProfileScope(.{ .mode = .required }));

    var profile_scope = metal_runtime.MetalJitRouteScope.none();
    includeMetalJitTensorShape(&profile_scope, .Q4_0, &.{ 10240, 2560 }, false, true);
    includeMetalJitTensorShape(&profile_scope, .Q6_K, &.{ 4096, 1024 }, false, true);
    includeMetalJitTensorShape(&profile_scope, .Q5_K, &.{ 10752, 262144 }, false, true);
    try std.testing.expect(profile_scope.conformance_complete);
    try std.testing.expectEqualSlices([2]usize, &.{.{ 2560, 10240 }}, profile_scope.observedShapes(.q4_0));
    try std.testing.expectEqualSlices([2]usize, &.{.{ 1024, 4096 }}, profile_scope.observedShapes(.q6_k));

    var relevant_oversized_scope = metal_runtime.MetalJitRouteScope.none();
    includeMetalJitTensorShape(&relevant_oversized_scope, .Q4_0, &.{ 10752, 262144 }, false, true);
    try std.testing.expect(!relevant_oversized_scope.conformance_complete);
    try std.testing.expectEqual(
        metal_runtime.MetalJitScopeInvalidReason.fixture_resource_limit,
        relevant_oversized_scope.invalid_reason,
    );
}

test "Metal JIT scope discovers quantized weights in a secondary GGUF store" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const allocator = std.testing.allocator;

    const SecondaryGgufStore = struct {
        allocator: std.mem.Allocator,
        encoder_file: gguf_mod.format.File,
        load_calls: usize = 0,

        const vtable = tensor_store_mod.TensorStore.VTable{
            .kind = @ptrCast(&kindImpl),
            .weightSource = @ptrCast(&weightSourceImpl),
            .describeTensor = @ptrCast(&describeTensorImpl),
            .describeTensorRange = @ptrCast(&describeTensorRangeImpl),
            .loadTensorRef = @ptrCast(&loadTensorRefImpl),
            .loadQuantizedStorageRef = @ptrCast(&loadQuantizedStorageRefImpl),
            .discardTensorFileCache = @ptrCast(&discardTensorFileCacheImpl),
            .preserveFileCacheOnDeinit = @ptrCast(&preserveFileCacheOnDeinitImpl),
            .ggufFile = @ptrCast(&ggufFileImpl),
            .deinit = @ptrCast(&deinitSelf),
        };

        fn tensorStore(self: *@This()) tensor_store_mod.TensorStore {
            return .{ .ptr = self, .vtable = &vtable };
        }

        fn kindImpl(_: *@This()) tensor_store_mod.StoreKind {
            return .gguf;
        }

        fn weightSourceImpl(_: *@This()) !?weight_source_mod.WeightSource {
            return null;
        }

        fn describeTensorImpl(_: *@This(), _: std.mem.Allocator, _: []const u8) !tensor_store_mod.LazyTensorRef {
            return error.UnsupportedOperation;
        }

        fn describeTensorRangeImpl(_: *@This(), _: std.mem.Allocator, _: []const u8) !?tensor_store_mod.TensorRangeRef {
            return null;
        }

        fn loadTensorRefImpl(_: *@This(), _: *const tensor_store_mod.LazyTensorRef) !weight_source_mod.LoadedWeight {
            return error.UnsupportedOperation;
        }

        fn loadQuantizedStorageRefImpl(
            self: *@This(),
            tensor_ref: *const tensor_store_mod.LazyTensorRef,
        ) !?weight_source_mod.QuantizedStorage {
            const source_name = tensor_ref.source_name orelse tensor_ref.name;
            if (!std.mem.eql(u8, source_name, "span_rep.secondary.weight")) return error.TensorNotFound;
            self.load_calls += 1;
            const shape = try self.allocator.dupe(i64, &.{ 3072, 768 });
            errdefer self.allocator.free(shape);
            const owned_source_name = try self.allocator.dupe(u8, source_name);
            errdefer self.allocator.free(owned_source_name);
            return .{
                .tensor_type = .{ .known = .Q4_K },
                .raw_bytes = &.{},
                .shape = shape,
                .source_name = owned_source_name,
                .raw_owned = false,
                .allocator = self.allocator,
            };
        }

        fn discardTensorFileCacheImpl(_: *@This(), _: []const u8) void {}

        fn preserveFileCacheOnDeinitImpl(_: *@This()) void {}

        fn ggufFileImpl(self: *@This()) ?*const gguf_mod.format.File {
            return &self.encoder_file;
        }

        fn deinitSelf(_: *@This()) void {}
    };

    var metadata: [0]gguf_mod.format.MetadataEntry = .{};
    var tensors: [0]gguf_mod.format.TensorInfo = .{};
    var secondary = SecondaryGgufStore{
        .allocator = allocator,
        .encoder_file = .{
            .header = .{ .version = 3, .tensor_count = 0, .metadata_count = 0 },
            .metadata = metadata[0..],
            .tensors = tensors[0..],
            .alignment = gguf_mod.format.default_alignment,
            .data_region_offset = 0,
        },
    };

    var lazy_weights = std.StringHashMapUnmanaged(gpu_hosted_store_mod.LazyWeightEntry){};
    defer {
        var iterator = lazy_weights.iterator();
        while (iterator.next()) |entry| {
            entry.value_ptr.tensor_ref.deinit(allocator);
            allocator.free(entry.key_ptr.*);
        }
        lazy_weights.deinit(allocator);
    }
    const key = try allocator.dupe(u8, "span_rep.secondary.weight");
    var key_owned = true;
    defer if (key_owned) allocator.free(key);
    const ref_name = try allocator.dupe(u8, key);
    var ref_name_owned = true;
    defer if (ref_name_owned) allocator.free(ref_name);
    const ref_source_name = try allocator.dupe(u8, key);
    var ref_source_name_owned = true;
    defer if (ref_source_name_owned) allocator.free(ref_source_name);
    const tensor_ref = tensor_store_mod.LazyTensorRef{
        .name = ref_name,
        .source_name = ref_source_name,
        .quantized = true,
    };
    try lazy_weights.put(allocator, key, .{ .tensor_ref = tensor_ref });
    key_owned = false;
    ref_name_owned = false;
    ref_source_name_owned = false;

    var scope = metal_runtime.MetalJitRouteScope.none();
    includeMetalJitLinearWeightFormats(
        &scope,
        &lazy_weights,
        secondary.tensorStore(),
        true,
        .{ .gliner = .{} },
        false,
    );

    try std.testing.expect(scope.conformance_complete);
    try std.testing.expectEqual(@as(usize, 1), secondary.load_calls);
    try std.testing.expectEqual([2]usize{ 3072, 768 }, scope.observedShapes(.q4_k)[0]);
    try std.testing.expectEqual(@as(usize, 1), metal_runtime.metalJitProductionRouteCount(scope));
}

const shouldKeepResidentGptEmbeddingQuantizedOnly = host_weights.shouldKeepResidentGptEmbeddingQuantizedOnly;

const shouldKeepResidentGptWeightQuantizedOnly = host_weights.shouldKeepResidentGptWeightQuantizedOnly;

const defaultResidentExpertsPerLayer = host_weights.defaultResidentExpertsPerLayer;

const resolveA4bGptInferenceConfig = device_weights.resolveA4bGptInferenceConfig;

const resolveCudaA4bGptInferenceConfig = cuda_weights.resolveCudaA4bGptInferenceConfig;

test "CUDA A4B resolver applies the resident envelope default before validating residency" {
    const gpt_config = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2816,
        .num_hidden_layers = 30,
        .num_local_experts = 128,
        .num_experts_per_tok = 8,
        .num_shared_experts = 1,
        .expert_intermediate_size = 704,
    };
    const request = backend_contracts.A4bInferenceRequest{ .residency_mode = .resident };
    const resolved = (try resolveCudaA4bGptInferenceConfig(gpt_config, request, true)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(backend_contracts.A4bResidencyMode.resident, resolved.residency_mode);
    try std.testing.expectEqual(
        @as(u64, backend_contracts.qualified_cuda_a4b_memory_budget_mb) * 1024 * 1024,
        resolved.memory_budget_bytes,
    );
}

const shouldRetainTensorStore = host_weights.shouldRetainTensorStore;

const defaultPlanContextForBackend = host_weights.defaultPlanContextForBackend;

const recommendedGpuHostedLazyQuantBudgetFloor = device_weights.recommendedGpuHostedLazyQuantBudgetFloor;

const recommendedGpuHostedLargeMultimodalGemmaBudgetFloor = device_weights.recommendedGpuHostedLargeMultimodalGemmaBudgetFloor;

/// Large dense safetensors models have two independently real residency
/// domains in GPU-hosted execution: the lazily faulted source mapping and the
/// prepared backend weights. The generic GPU cache is intentionally too small
/// for a complete 2B BF16 model, so qualified bundle families need an explicit
/// floor sized from their immutable weight artifact rather than failing midway
/// through the decoder after a partial publication.
const recommendedGpuHostedLargeDenseSafetensorsBudgetFloor = device_weights.recommendedGpuHostedLargeDenseSafetensorsBudgetFloor;

/// Ettin's FP32 checkpoint and full-context batches exceed the generic GPU
/// workload envelope. Match the existing conservative CUDA load reservation
/// and the bounded decision batch workspace; explicit node caps still win.
const glinerCudaBudgetFloor = cuda_weights.glinerCudaBudgetFloor;

test "GLiNER Ettin CUDA budget admits checkpoint and full decision batch workspace" {
    const cfg = modern_bert_arch.Config{ .hidden_size = 1792, .intermediate_size = 3840, .num_hidden_layers = 28, .num_attention_heads = 28, .max_position_embeddings = 7999 };
    const weights = 4755208228;
    const floor = try glinerCudaBudgetFloor(.{ .gliner_modern_bert = cfg }, weights);
    try std.testing.expectEqualDeep(floor, try glinerCudaBudgetFloor(.{ .modern_bert = cfg }, weights));
    try std.testing.expect(floor.host_limit_bytes > weights);
    const workspace = try layaCudaWorkspace(8, 2048, 3, cfg.hidden_size, cfg.intermediate_size);
    try std.testing.expect(floor.scratch_limit_bytes >= workspace);
    try std.testing.expect(floor.backend_limit_bytes > try estimateBackendWeightResidencyBytes(.cuda, weights) + workspace);
    try std.testing.expectEqual(floor.host_limit_bytes + floor.backend_limit_bytes, floor.combined_limit_bytes);
    try std.testing.expectEqual(@as(usize, 0), (try glinerCudaBudgetFloor(.{ .gliner = .{} }, weights)).host_limit_bytes);
    const capped = runtime.tier.memory.applyLimitOverrides(floor, .{ .backend_limit_bytes = gib(2) });
    try std.testing.expectEqual(gib(2), capped.backend_limit_bytes);
}

const isBgeM3DenseEncoder = device_weights.isBgeM3DenseEncoder;

/// Unquantized GLiNER2 span checkpoints on a DeBERTa-v3-large backbone
/// (e.g. fastino/GLiNER2.5-Decide, ~1.9 GB F32) exceed the generic 1.5 GiB
/// host cache on 16 GiB machines, like dense BGE-M3.
const isLargeGlinerSpanEncoder = device_weights.isLargeGlinerSpanEncoder;

test "large GLiNER span encoder takes the dense F32 encoder budget" {
    var manifest = manifest_mod.ModelManifest{ .allocator = std.testing.allocator };
    try std.testing.expect(isLargeGlinerSpanEncoder(manifest, .{ .gliner = .{ .hidden_size = 1024, .num_hidden_layers = 24, .num_attention_heads = 16 } }));
    try std.testing.expect(!isLargeGlinerSpanEncoder(manifest, .{ .gliner = .{} }));
    manifest.gliner_architecture = .span;
    manifest.gliner_span_encoder_family = .modern_bert;
    try std.testing.expect(isLargeGlinerSpanEncoder(manifest, .{ .modern_bert = .{ .hidden_size = 1792, .num_hidden_layers = 28, .num_attention_heads = 28 } }));
    manifest.safetensors_index_path = "model.safetensors.index.json";
    const artifact_bytes = 4_755_186_772;
    const modern = ArchConfig{ .modern_bert = .{ .hidden_size = 1792, .num_hidden_layers = 28, .num_attention_heads = 28 } };
    try std.testing.expect(shouldUseLargeDenseSafetensorsBudgets(artifact_bytes, manifest, modern));
    try std.testing.expect(!sessionDirectQuantEnabled(true, manifest, modern));
    const floor = recommendedGpuHostedLargeDenseSafetensorsBudgetFloor(artifact_bytes);
    try std.testing.expect(floor.host_limit_bytes >= artifact_bytes);
    try std.testing.expect(floor.backend_limit_bytes >= artifact_bytes);
    try std.testing.expect(floor.combined_limit_bytes >= floor.host_limit_bytes + floor.backend_limit_bytes);
}

const sessionDirectQuantEnabled = device_weights.sessionDirectQuantEnabled;

const recommendedGpuHostedBgeM3BudgetFloor = device_weights.recommendedGpuHostedBgeM3BudgetFloor;

const recommendedGpuHostedBgeM3SharedCacheBudget = device_weights.recommendedGpuHostedBgeM3SharedCacheBudget;

test "BGE-M3 dense encoder budget is exact-geometry qualified" {
    const manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .config_model_arch = "xlm-roberta",
    };
    const config = bert_arch.Config{
        .model_type = .roberta,
        .vocab_size = 250002,
        .hidden_size = 1024,
        .num_hidden_layers = 24,
        .num_attention_heads = 16,
        .intermediate_size = 4096,
        .max_position_embeddings = 8194,
        .type_vocab_size = 1,
        .hidden_act = .gelu_exact,
        .layer_norm_eps = 1e-5,
        .pad_token_id = 1,
        .position_id_mode = .roberta_padding,
    };
    try std.testing.expect(isBgeM3DenseEncoder(manifest, .{ .bert = config }));
    try std.testing.expect(!sessionDirectQuantEnabled(true, manifest, .{ .bert = config }));
    try std.testing.expect(!sessionDirectQuantEnabled(false, manifest, .{ .bert = config }));

    var wrong_geometry = config;
    wrong_geometry.hidden_size = 768;
    try std.testing.expect(!isBgeM3DenseEncoder(manifest, .{ .bert = wrong_geometry }));
    try std.testing.expect(sessionDirectQuantEnabled(true, manifest, .{ .bert = wrong_geometry }));

    const artifact_bytes = gib(2) + mib(160);
    const floor = recommendedGpuHostedBgeM3BudgetFloor(artifact_bytes);
    const cache = recommendedGpuHostedBgeM3SharedCacheBudget(artifact_bytes);
    try std.testing.expect(floor.host_limit_bytes >= artifact_bytes);
    try std.testing.expectEqual(floor.host_limit_bytes, cache.host_limit_bytes);
    try std.testing.expect(floor.combined_limit_bytes >= floor.host_limit_bytes + floor.backend_limit_bytes);
}

const ensureMetalHostedSessionAvailable = device_weights.ensureMetalHostedSessionAvailable;

test "gpu-hosted Metal disabled constructors reject before model allocation" {
    if (comptime build_options.enable_metal) return error.SkipZigTest;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const allocator = failing.allocator();
    const missing_model = "/private/tmp/antfly-metal-disabled-intentionally-missing";
    try std.testing.expectError(error.MetalNotEnabled, createMetalSession(allocator, missing_model));
    try std.testing.expectError(error.MetalNotEnabled, createMetalSessionWithTaskOverride(allocator, missing_model, .classifier));
    try std.testing.expect(!failing.has_induced_failure);
}

test "gpu-hosted Metal availability gate agrees with linked runtime probe" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (metal_runtime.termite_metal_device_available() == 0) return error.SkipZigTest;
    try ensureMetalHostedSessionAvailable();
}

test "A4B artifact qualification requires the complete Q4_0 expert set" {
    var report = GgufInspectionReport{
        .allocator = std.testing.allocator,
        .architecture = "gemma4",
        .tensor_count = 90,
        .metadata_count = 0,
        .gpt_config = .{
            .family = .gemma,
            .hidden_size = 2816,
            .num_hidden_layers = 30,
            .num_local_experts = 128,
            .num_experts_per_tok = 8,
            .num_shared_experts = 1,
            .expert_intermediate_size = 704,
        },
        .packed_moe_expert_tensor_count = 60,
        .packed_moe_q4_0_tensor_count = 60,
        .a4b_packed_expert_layout_qualified = true,
    };
    try std.testing.expect(qualifiedA4bArtifact(report));
    report.packed_moe_q4_0_tensor_count -= 1;
    try std.testing.expect(!qualifiedA4bArtifact(report));
}

test "A4B geometry does not opt an unqualified artifact into A4B inference" {
    const matching_geometry = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2816,
        .num_hidden_layers = 30,
        .num_local_experts = 128,
        .num_experts_per_tok = 8,
        .num_shared_experts = 1,
        .expert_intermediate_size = 704,
    };

    try std.testing.expect((try resolveA4bGptInferenceConfig(
        matching_geometry,
        null,
        false,
    )) == null);
    try std.testing.expectError(error.A4bUnsupportedArtifact, resolveA4bGptInferenceConfig(
        matching_geometry,
        .{},
        false,
    ));
    try std.testing.expect((try resolveA4bGptInferenceConfig(
        matching_geometry,
        null,
        true,
    )) != null);

    const cuda_default = (try resolveCudaA4bGptInferenceConfig(
        matching_geometry,
        null,
        true,
    )).?;
    try std.testing.expectEqual(backend_contracts.A4bResidencyMode.resident, cuda_default.residency_mode);
    try std.testing.expectEqual(
        @as(u64, backend_contracts.qualified_cuda_a4b_memory_budget_mb) * 1024 * 1024,
        cuda_default.memory_budget_bytes,
    );
}

test "A4B configured GGUF passes metadata-only production qualification" {
    const model_path = platform.env.getenv("ANTFLY_INFERENCE_GEMMA4_A4B_TEST_MODEL") orelse
        return error.SkipZigTest;
    var manifest = try manifest_mod.loadListingFromDir(std.testing.allocator, model_path);
    defer manifest.deinit();

    const config = try resolveA4bInferenceConfigForModelListing(
        std.testing.allocator,
        model_path,
        manifest,
        .{
            .residency_mode = .streamed,
            .memory_budget_mb = 2048,
        },
    );
    const qualified = config orelse return error.A4bUnsupportedArtifact;
    try std.testing.expectEqual(backend_contracts.A4bResidencyMode.streamed, qualified.residency_mode);
    try std.testing.expectEqual(@as(u64, 2 * 1024 * 1024 * 1024), qualified.memory_budget_bytes);
    try std.testing.expectEqual(@as(u8, 8), qualified.expert_cache_slots);
}

const a4bGpuHostedBudgetPolicy = device_weights.a4bGpuHostedBudgetPolicy;

const gpuHostedBudgetPolicy = device_weights.gpuHostedBudgetPolicy;

const lightOnOcr4bBudgetPolicy = device_weights.lightOnOcr4bBudgetPolicy;

pub fn widenBudgetLimitsForModelPath(
    allocator: std.mem.Allocator,
    model_path: []const u8,
    limits: runtime.tier.memory.Limits,
    backend_type: BackendType,
) !runtime.tier.memory.Limits {
    if (!backend_type.usesGpuHostedSession()) return limits;
    switch (backend_type) {
        .metal => if (!build_options.enable_metal) return limits,
        .cuda => if (!build_options.enable_cuda) return limits,
        else => return limits,
    }

    const direct_quant_enabled = directQuantEnabled();
    const quant_mode = gpuHostedQuantExecutionMode(direct_quant_enabled);

    var mf = try manifest_mod.loadFromDir(allocator, model_path);
    defer mf.deinit();

    const model_weight_bytes = estimateNativeWeightBytes(allocator, mf) catch 0;

    const arch_config = try detectArchitecture(allocator, model_path, mf);
    const policy = gpuHostedBudgetPolicy(backend_type, model_weight_bytes, mf, arch_config, quant_mode);
    const floor = if (backend_type == .cuda)
        widenLimits(policy.budget_floor, try glinerCudaBudgetFloor(arch_config, model_weight_bytes))
    else
        policy.budget_floor;
    return widenLimits(limits, floor);
}

const shouldUseLargeDenseSafetensorsBudgets = device_weights.shouldUseLargeDenseSafetensorsBudgets;

const shouldUseQwen3VlRerankerGgufBudgets = device_weights.shouldUseQwen3VlRerankerGgufBudgets;

/// A Qwen3-VL reranker keeps its decoder resident, then maps the external
/// Q8 projector for image requests. The host bucket must fit both artifacts
/// plus bounded image preprocessing. This is a limit floor rather than a
/// permanent reservation; per-run admission still charges the projector and
/// scratch exactly, and an explicit operator limit remains authoritative.
const recommendedGpuHostedQwen3VlRerankerGgufBudgetFloor = device_weights.recommendedGpuHostedQwen3VlRerankerGgufBudgetFloor;

fn widenGpuHostedTierCache(self: *ArchSession, budget: *runtime.tier.memory.RunBudget) void {
    if (gpuBackendData(self).tier_cache) |*tier_cache| {
        tier_cache.widenToAtLeast(.{
            .host_limit_bytes = @max(budget.limits.host_limit_bytes, self.shared_cache_budget_floor.host_limit_bytes),
            .backend_limit_bytes = @max(budget.limits.backend_limit_bytes, self.shared_cache_budget_floor.backend_limit_bytes),
        });
    }
}

fn makeGpuHostedComputeBackend(
    self: *ArchSession,
    allocator: std.mem.Allocator,
    run_budget: ?*runtime.tier.memory.RunBudget,
) !ops.ComputeBackend {
    return switch (self.backend_type) {
        .metal => makeMetalHostedComputeBackend(self, allocator, run_budget),
        else => unreachable,
    };
}

fn makeMetalHostedComputeBackend(self: *ArchSession, allocator: std.mem.Allocator, run_budget: ?*runtime.tier.memory.RunBudget) !ops.ComputeBackend {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    const deferred_attention = switch (self.arch_config) {
        .gpt => |cfg| cfg.hasQwen35VisionArchitectureContract() and
            ((cfg.hidden_size == 1024 and cfg.intermediate_size == 3584 and cfg.num_hidden_layers == 24) or
                (cfg.hidden_size == 2560 and cfg.intermediate_size == 9216 and cfg.num_hidden_layers == 32)),
        else => false,
    };
    return @import("../backends/compute_factory.zig").createMetal(allocator, gpuBackendData(self), run_budget, self.io, .{
        .config = self.kernel_jit_config,
        .scope = self.metal_jit_scope,
        .load_context = self.kernel_jit_load_context,
    }, .{
        .require_f32_linears = self.arch_config == .embedding_gemma2,
        .encoder_rms_norm = self.arch_config == .embedding_gemma2,
        .deferred_attention = deferred_attention,
        .projection_fusions = deferred_attention,
    });
}

const widenLimits = device_weights.widenLimits;

const mib = device_weights.mib;

const gib = device_weights.gib;

const parseMoeExpertCoord = host_weights.parseMoeExpertCoord;

const projectionMaskForWeightKey = host_weights.projectionMaskForWeightKey;

const parseJinaLoraConfig = host_weights.parseJinaLoraConfig;

const jinaRetrievalAdapterPaths = host_weights.jinaRetrievalAdapterPaths;

test "shouldRetainTensorStore keeps safetensors-backed resident weights alive" {
    try std.testing.expect(shouldRetainTensorStore(.safetensors, 0));
    try std.testing.expect(shouldRetainTensorStore(.gguf, 0));
    try std.testing.expect(shouldRetainTensorStore(.gguf, 2));
    try std.testing.expect(shouldRetainTensorStore(.safetensors, 3));
}

test "overlay gpt structural config carries gguf vocab size" {
    var target = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2560,
        .num_hidden_layers = 28,
        .num_attention_heads = 8,
        .num_key_value_heads = 4,
        .intermediate_size = 10240,
        .vocab_size = 50257,
    };
    const source = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2560,
        .num_hidden_layers = 28,
        .num_attention_heads = 8,
        .num_key_value_heads = 4,
        .intermediate_size = 10240,
        .vocab_size = 262208,
    };

    overlayGptStructuralConfig(&target, source);

    try std.testing.expectEqual(@as(u32, 262208), target.vocab_size);
}

test "detectModelType reads top-level model_type instead of nested model_type" {
    const json =
        \\{
        \\  "audio_config": { "model_type": "clap_audio_model" },
        \\  "text_config": { "model_type": "clap_text_model" },
        \\  "model_type": "clap"
        \\}
    ;
    const detected = try detectModelType(std.testing.allocator, json);
    defer if (detected) |value| std.testing.allocator.free(value);
    try std.testing.expect(detected != null);
    try std.testing.expectEqualStrings("clap", detected.?);
}

test "makeBertConfig carries num_labels from manifest" {
    const allocator = std.testing.allocator;
    var mf = manifest_mod.ModelManifest{
        .allocator = allocator,
        .bert_model_type = .roberta,
        .hidden_size = 384,
        .num_hidden_layers = 6,
        .num_attention_heads = 12,
        .intermediate_size = 1536,
        .max_position_embeddings = 256,
        .bert_vocab_size = 250002,
        .bert_type_vocab_size = 1,
        .bert_layer_norm_eps = 0.00001,
        .num_labels = 3,
        .bert_pad_token_id = 1,
    };
    defer mf.deinit();

    const cfg = makeBertConfig(mf);
    try std.testing.expectEqual(@as(bert.ModelType, .roberta), cfg.model_type);
    try std.testing.expectEqual(@as(u32, 250002), cfg.vocab_size);
    try std.testing.expectEqual(@as(u32, 1), cfg.type_vocab_size);
    try std.testing.expectApproxEqAbs(@as(f32, 0.00001), cfg.layer_norm_eps, 0.0000001);
    try std.testing.expectEqual(@as(u32, 3), cfg.num_labels);
    try std.testing.expectEqual(@as(i64, 1), cfg.pad_token_id);
    try std.testing.expectEqual(@as(bert.PositionIdMode, .roberta_padding), cfg.position_id_mode);
}

test "sessionTaskForModelType maps classifier and extractor tasks" {
    try std.testing.expectEqual(@as(SessionTask, .classifier), sessionTaskForModelType(.classifier, null));
    try std.testing.expectEqual(@as(SessionTask, .classifier), sessionTaskForModelType(.reranker, null));
    try std.testing.expectEqual(@as(SessionTask, .extractor), sessionTaskForModelType(.extractor, null));
    try std.testing.expectEqual(@as(SessionTask, .generic), sessionTaskForModelType(.embedder, null));
    try std.testing.expectEqual(@as(SessionTask, .generic), sessionTaskForModelType(.reranker, .generic));
}

test "GLiNER immutable F32 borrow policy includes declared ModernBERT span inference only" {
    try std.testing.expect(sessionEnablesImmutableF32WeightBorrow(.metal, .gliner, sessionTaskForModelType(.extractor, null), .unknown));
    try std.testing.expect(sessionEnablesImmutableF32WeightBorrow(.metal, .gliner, sessionTaskForModelType(.extractor, .extractor), .unknown));
    try std.testing.expect(!sessionEnablesImmutableF32WeightBorrow(.metal, .gliner, sessionTaskForModelType(.extractor, .generic), .unknown));
    try std.testing.expect(sessionEnablesImmutableF32WeightBorrow(.metal, .modern_bert, .extractor, .modern_bert));
    try std.testing.expect(!sessionEnablesImmutableF32WeightBorrow(.metal, .modern_bert, .extractor, .unknown));
    try std.testing.expect(!sessionEnablesImmutableF32WeightBorrow(.metal, .modern_bert, .generic, .modern_bert));
    try std.testing.expect(!sessionEnablesImmutableF32WeightBorrow(.native, .modern_bert, .extractor, .modern_bert));
    try std.testing.expect(sessionPrefersImmutableGlinerF32Mps(.metal, .gliner, .extractor));
    try std.testing.expect(!sessionPrefersImmutableGlinerF32Mps(.metal, .modern_bert, .extractor));
    inline for (std.meta.tags(BackendType)) |backend_type| {
        inline for (std.meta.tags(ArchType)) |arch_type| {
            inline for (std.meta.tags(SessionTask)) |task| {
                const expected = backend_type == .metal and arch_type == .gliner and task == .extractor;
                try std.testing.expectEqual(expected, sessionEnablesImmutableF32WeightBorrow(backend_type, arch_type, task, .unknown));
            }
        }
    }
}

test "DeBERTa fast-path admission covers direct classifiers and reranker mirrors" {
    try std.testing.expect(!debertaRerankerPrefersWeightMirrors(false, 1, 512));
    try std.testing.expect(!debertaRerankerPrefersWeightMirrors(true, 1, 127));
    try std.testing.expect(debertaRerankerPrefersWeightMirrors(true, 1, 128));
    try std.testing.expect(debertaRerankerPrefersWeightMirrors(true, 8, 16));
    try std.testing.expect(!debertaRerankerPrefersWeightMirrors(true, std.math.maxInt(usize), 2));

    try std.testing.expect(sessionEnablesDebertaRerankerWeightMirrors(.reranker, .deberta, .classifier));
    try std.testing.expect(!sessionEnablesDebertaRerankerWeightMirrors(.classifier, .deberta, .classifier));
    try std.testing.expect(!sessionEnablesDebertaRerankerWeightMirrors(.reranker, .bert, .classifier));
    try std.testing.expect(!sessionEnablesDebertaRerankerWeightMirrors(.reranker, .deberta, .generic));

    const classifier_manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .classifier,
        .config_model_arch = "deberta-v2",
    };
    const classifier_admission = metalDebertaFastPathAdmissionAmounts(classifier_manifest);
    try std.testing.expect(classifier_admission.backend_weight_bytes > 0);
    try std.testing.expectEqual(
        deberta_arch.metalDebertaMpsAttentionScratchMaxBytes(),
        classifier_admission.backend_scratch_bytes,
    );
    const bert_reranker_manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .reranker,
        .config_model_arch = "bert",
    };
    try std.testing.expectEqual(
        runtime.tier.memory.AdmissionAmounts{},
        metalDebertaFastPathAdmissionAmounts(bert_reranker_manifest),
    );
    const gliner_manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .extractor,
        .inference_bundle_family = "gliner2_split_bundle/v1",
    };
    const gliner_admission = metalDebertaFastPathAdmissionAmounts(gliner_manifest);
    try std.testing.expect(gliner_admission.backend_weight_bytes > 0);
    try std.testing.expectEqual(
        deberta_arch.metalDebertaMpsAttentionScratchMaxBytes(),
        gliner_admission.backend_scratch_bytes,
    );
    const declared_deberta_manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .extractor,
        .gliner_model_type = "gliner2",
        .gliner_span_encoder_family = .deberta,
    };
    const declared_deberta_admission = metalDebertaFastPathAdmissionAmounts(declared_deberta_manifest);
    try std.testing.expect(declared_deberta_admission.backend_weight_bytes > 0);
    try std.testing.expectEqual(
        deberta_arch.metalDebertaMpsAttentionScratchMaxBytes(),
        declared_deberta_admission.backend_scratch_bytes,
    );
    const modern_bert_1b_manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .extractor,
        .gliner_model_type = "gliner2",
        .gliner_span_encoder_family = .modern_bert,
        .hidden_size = 1792,
        .intermediate_size = 3840,
        .max_position_embeddings = 7999,
        .num_hidden_layers = 28,
        .num_attention_heads = 28,
        .bert_vocab_size = 50378,
    };
    try std.testing.expectEqual(
        runtime.tier.memory.AdmissionAmounts{},
        metalDebertaFastPathAdmissionAmounts(modern_bert_1b_manifest),
    );
}

test "detectArchitecture recognizes generic deberta classifier configs" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"deberta-v3","hidden_size":768,"num_hidden_layers":12,"num_attention_heads":12,"intermediate_size":3072,"num_labels":3}
        ,
    });

    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);

    var mf = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .classifier,
    };
    defer mf.deinit();

    const arch = try detectArchitecture(allocator, model_dir, mf);
    switch (arch) {
        .deberta => |cfg| try std.testing.expectEqual(@as(u32, 3), cfg.num_labels),
        else => return error.TestUnexpectedResult,
    }
}

test "legacy GLiNER architecture reads local encoder sidecar and preserves label overrides" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"extractor\",\"encoder_name\":\"ignored/remote-model\"}" });
    try tmp.dir.createDir(io, "encoder_config", .default_dir);
    try tmp.dir.writeFile(io, .{
        .sub_path = "encoder_config/config.json",
        .data = "{\"model_type\":\"deberta-v2\",\"hidden_size\":64,\"num_hidden_layers\":2,\"num_attention_heads\":4,\"intermediate_size\":128,\"vocab_size\":99,\"max_position_embeddings\":32,\"position_buckets\":16,\"layer_norm_eps\":0.000001,\"hidden_act\":\"gelu\"}",
    });
    try tmp.dir.writeFile(io, .{ .sub_path = "added_tokens.json", .data = "{\"[C]\":88,\"[E]\":89,\"[R]\":90}" });
    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);
    const mf = manifest_mod.ModelManifest{ .allocator = allocator, .model_type = .extractor, .gliner_token_e = 87 };
    const arch = try detectArchitecture(allocator, model_dir, mf);
    try std.testing.expect(arch == .gliner);
    const cfg = arch.gliner;
    try std.testing.expectEqual(@as(u32, 64), cfg.hidden_size);
    try std.testing.expectEqual(@as(u32, 2), cfg.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 4), cfg.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 128), cfg.intermediate_size);
    try std.testing.expectEqual(@as(u32, 99), cfg.vocab_size);
    try std.testing.expectEqual(@as(u32, 32), cfg.max_position_embeddings);
    try std.testing.expectEqual(@as(u32, 16), cfg.position_buckets);
    try std.testing.expectEqual(@as(f32, 0.000001), cfg.layer_norm_eps);
    try std.testing.expect(cfg.use_exact_gelu);
    try std.testing.expectEqual(@as(i64, 88), cfg.classification_token_id);
    try std.testing.expectEqual(@as(i64, 89), cfg.entity_token_id);
    try std.testing.expectEqual(@as(i64, 90), cfg.relation_token_id);
    var wrapped_manifest = mf;
    wrapped_manifest.gliner_model_type = "gliner2";
    try std.testing.expectEqual(cfg, (try detectArchitecture(allocator, model_dir, wrapped_manifest)).gliner);
}

test "legacy GLiNER absent encoder sidecar keeps defaults while malformed present config rejects" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"extractor\"}" });
    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);
    const mf = manifest_mod.ModelManifest{ .allocator = allocator, .model_type = .extractor };
    try std.testing.expectEqual(deberta_mod.Config{}, (try detectArchitecture(allocator, model_dir, mf)).gliner);
    try tmp.dir.createDir(io, "encoder_config", .default_dir);
    try std.testing.expectEqual(deberta_mod.Config{}, (try detectArchitecture(allocator, model_dir, mf)).gliner);

    try tmp.dir.writeFile(io, .{ .sub_path = "encoder_config/config.json", .data = "{" });
    try std.testing.expectError(error.UnexpectedEndOfInput, detectArchitecture(allocator, model_dir, mf));
    try tmp.dir.writeFile(io, .{ .sub_path = "encoder_config/config.json", .data = "[]" });
    try std.testing.expectError(error.InvalidDebertaConfig, detectArchitecture(allocator, model_dir, mf));
    try tmp.dir.writeFile(io, .{ .sub_path = "encoder_config/config.json", .data = "{\"hidden_act\":\"relu\"}" });
    try std.testing.expectError(error.UnsupportedDebertaActivation, detectArchitecture(allocator, model_dir, mf));
    try tmp.dir.writeFile(io, .{ .sub_path = "encoder_config/config.json", .data = "{\"hidden_act\":\"gelu_new\"}" });
    try std.testing.expect(!(try detectArchitecture(allocator, model_dir, mf)).gliner.use_exact_gelu);
}

test "GLiNER span ModernBERT loads nested Decide 1B geometry" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const wrapper_config =
        \\{"model_type":"extractor","architecture":"span","model_name":"jhu-clsp/ettin-enc-from-dec-1b","counting_layer":"count_lstm","token_pooling":"first","use_moe":false,"span_head":{"span_mode":"markerV0"}}
    ;
    const encoder_config =
        \\{"model_type":"modernbert","vocab_size":50378,"hidden_size":1792,"intermediate_size":3840,"num_hidden_layers":28,"num_attention_heads":28,"max_position_embeddings":7999,"local_attention":128,"global_attn_every_n_layers":3,"rope_parameters":{"full_attention":{"rope_theta":160000},"sliding_attention":{"rope_theta":160000}}}
    ;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "config.json", .data = wrapper_config });
    try tmp.dir.createDir(io, "encoder_config", .default_dir);
    try tmp.dir.writeFile(io, .{ .sub_path = "encoder_config/config.json", .data = encoder_config });
    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);
    const mf = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .extractor,
        .gliner_architecture = .span,
        .gliner_span_declared = true,
        .gliner_span_encoder_family = .modern_bert,
        .gliner_span_config_digest = gliner_decide_qualification.Digest.of(wrapper_config),
        .gliner_span_encoder_digest = gliner_decide_qualification.Digest.of(encoder_config),
    };
    const arch = try detectArchitecture(allocator, model_dir, mf);
    try std.testing.expect(arch == .modern_bert);
    try std.testing.expectEqual(@as(u32, 1792), arch.modern_bert.hidden_size);
    try std.testing.expectEqual(@as(u32, 28), arch.modern_bert.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 64), arch.modern_bert.hidden_size / arch.modern_bert.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 7999), arch.modern_bert.max_position_embeddings);
}

test "legacy GLiNER encoder sidecar bounds regular input and recovers after allocation failure" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDir(io, "encoder_config", .default_dir);
    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);
    {
        const oversized = try tmp.dir.createFile(io, "encoder_config/config.json", .{});
        defer oversized.close(io);
        try oversized.setLength(io, legacy_gliner_encoder_config_max_bytes + 1);
    }
    try std.testing.expectError(error.SnapshotLimitExceeded, loadLegacyGlinerEncoderConfig(allocator, model_dir, .{}));
    try tmp.dir.deleteFile(io, "encoder_config/config.json");
    try tmp.dir.createDir(io, "encoder_config/config.json", .default_dir);
    try std.testing.expectError(error.InvalidModelArtifactKind, loadLegacyGlinerEncoderConfig(allocator, model_dir, .{}));
    try tmp.dir.deleteDir(io, "encoder_config/config.json");
    try tmp.dir.writeFile(io, .{ .sub_path = "encoder_config/config.json", .data = "{\"hidden_size\":64,\"hidden_act\":\"gelu\"}" });
    const Check = struct {
        fn run(a: std.mem.Allocator, path: []const u8) !void {
            const cfg = try loadLegacyGlinerEncoderConfig(a, path, .{});
            try std.testing.expectEqual(@as(u32, 64), cfg.hidden_size);
            try std.testing.expect(cfg.use_exact_gelu);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(allocator, Check.run, .{model_dir});
    try Check.run(allocator, model_dir);
}

test "legacy GLiNER encoder sidecar obeys managed inventory and root containment" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const managed_receipt = @import("../registry/managed_receipt.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model/encoder_config");
    try tmp.dir.writeFile(io, .{ .sub_path = "outside.json", .data = "{\"hidden_act\":\"gelu\"}" });
    try tmp.dir.symLink(io, "../../outside.json", "model/encoder_config/config.json", .{});
    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model", .{tmp.sub_path});
    defer allocator.free(model_dir);
    try std.testing.expectError(error.ModelArtifactOutsideRoot, loadLegacyGlinerEncoderConfig(allocator, model_dir, .{}));
    try tmp.dir.deleteFile(io, "model/encoder_config/config.json");
    try tmp.dir.writeFile(io, .{ .sub_path = "model/encoder.json", .data = "{\"hidden_act\":\"gelu\"}" });
    try tmp.dir.symLink(io, "../encoder.json", "model/encoder_config/config.json", .{});
    try std.testing.expect((try loadLegacyGlinerEncoderConfig(allocator, model_dir, .{})).use_exact_gelu);

    // A complete managed publication may omit optional encoder metadata. An
    // unrelated local sidecar must not silently override that admitted set.
    try tmp.dir.writeFile(io, .{ .sub_path = "model/model.safetensors", .data = "payload" });
    const receipt_path = "model/" ++ managed_receipt.complete_filename;
    try tmp.dir.writeFile(io, .{
        .sub_path = receipt_path,
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.safetensors\",\"size\":7}]}",
    });
    try std.testing.expectEqual(deberta_mod.Config{}, try loadLegacyGlinerEncoderConfig(allocator, model_dir, .{}));
    try tmp.dir.writeFile(io, .{
        .sub_path = receipt_path,
        .data = "{\"version\":1,\"artifacts\":[{\"path\":\"model.safetensors\",\"size\":7},{\"path\":\"encoder_config/config.json\",\"size\":21}]}",
    });
    try std.testing.expect((try loadLegacyGlinerEncoderConfig(allocator, model_dir, .{})).use_exact_gelu);
}

test "detectArchitecture preserves exact GELU for BGE-M3 XLM-R config" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"xlm-roberta","vocab_size":250002,"hidden_size":1024,"num_hidden_layers":24,"num_attention_heads":16,"intermediate_size":4096,"max_position_embeddings":8194,"type_vocab_size":1,"pad_token_id":1,"layer_norm_eps":1e-5,"hidden_act":"gelu"}
        ,
    });
    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);

    var mf = manifest_mod.ModelManifest{ .allocator = allocator };
    defer mf.deinit();
    const arch = try detectArchitecture(allocator, model_dir, mf);
    switch (arch) {
        .bert => |cfg| {
            try std.testing.expectEqual(bert.ModelType.roberta, cfg.model_type);
            try std.testing.expectEqual(bert.HiddenActivation.gelu_exact, cfg.hidden_act);
            try std.testing.expectEqual(bert.PositionIdMode.roberta_padding, cfg.position_id_mode);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "detectArchitecture and weight normalization recognize HuggingFace ModernBERT embeddings" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"architectures":["ModernBertModel"],"model_type":"modernbert","hidden_size":768,"num_hidden_layers":22,"num_attention_heads":12,"intermediate_size":1152,"vocab_size":50368,"max_position_embeddings":8192,"local_attention":128,"global_attn_every_n_layers":3}
        ,
    });
    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);

    var mf = manifest_mod.ModelManifest{ .allocator = allocator };
    defer mf.deinit();
    const arch = try detectArchitecture(allocator, model_dir, mf);
    switch (arch) {
        .modern_bert => |cfg| {
            try std.testing.expectEqual(modern_bert_arch.CheckpointLayout.huggingface_fused_qkv_no_bias, cfg.checkpoint_layout);
            try std.testing.expectEqual(@as(u32, 22), cfg.num_hidden_layers);
            try std.testing.expectEqual(@as(u32, 128), cfg.local_attention_window);
        },
        else => return error.TestUnexpectedResult,
    }

    var key_buf: [256]u8 = undefined;
    try std.testing.expectEqualStrings(
        "model.layers.0.attn.Wqkv.weight",
        try normalizeWeightKey(.safetensors, arch, "layers.0.attn.Wqkv.weight", &key_buf),
    );
    try std.testing.expectEqualStrings(
        "model.embeddings.tok_embeddings.weight",
        try normalizeWeightKey(.safetensors, arch, "embeddings.tok_embeddings.weight", &key_buf),
    );
    try std.testing.expectEqualStrings(
        "model.layers.0.attn.Wqkv.weight",
        try normalizeWeightKey(.safetensors, arch, "encoder.layers.0.attn.Wqkv.weight", &key_buf),
    );
    try std.testing.expectEqualStrings(
        "model.final_norm.weight",
        try normalizeWeightKey(.safetensors, arch, "encoder.final_norm.weight", &key_buf),
    );
    try std.testing.expectEqualStrings(
        "classifier.0.weight",
        try normalizeWeightKey(.safetensors, arch, "classifier.0.weight", &key_buf),
    );
}

test "ModernBERT Laya head weights share the encoder runtime namespace" {
    const arch: ArchConfig = .{ .modern_bert = .{ .laya = .{} } };
    var key_buf: [256]u8 = undefined;
    const keys = [_]struct { source: []const u8, runtime: []const u8 }{
        .{ .source = "encoder.layers.0.attn.Wqkv.weight", .runtime = "model.layers.0.attn.Wqkv.weight" },
        .{ .source = "type_emb.weight", .runtime = "model.type_emb.weight" },
        .{ .source = "head.layers.0.attn.Wqkv.weight", .runtime = "model.head.layers.0.attn.Wqkv.weight" },
        .{ .source = "model.type_emb.weight", .runtime = "model.type_emb.weight" },
    };
    for (keys) |key| {
        try std.testing.expectEqualStrings(key.runtime, try normalizeWeightKey(.safetensors, arch, key.source, &key_buf));
    }
}

test "detectArchitecture recognizes Nomic Embed Text NomicBERT config" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"architectures":["NomicBertModel"],"model_type":"nomic_bert","n_embd":768,"n_layer":12,"n_head":12,"n_inner":3072,"n_positions":8192,"vocab_size":30528,"type_vocab_size":2,"layer_norm_eps":1e-12,"rotary_emb_base":1000}
        ,
    });
    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);

    var mf = manifest_mod.ModelManifest{ .allocator = allocator };
    defer mf.deinit();
    const arch = try detectArchitecture(allocator, model_dir, mf);
    switch (arch) {
        .nomic_bert => |cfg| {
            try std.testing.expectEqual(@as(u32, 12), cfg.num_hidden_layers);
            try std.testing.expectEqual(@as(u32, 8192), cfg.max_position_embeddings);
            try std.testing.expectEqual(@as(f32, 1000.0), cfg.rope_theta);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "detectArchitecture treats split gliner bundle encoder config as gliner" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "config.json",
        .data =
        \\{"model_type":"deberta-v2","hidden_size":768,"num_hidden_layers":12,"num_attention_heads":12,"intermediate_size":3072,"vocab_size":128011,"position_buckets":256}
        ,
    });
    try tmp.dir.writeFile(std.testing.io, .{
        .sub_path = "added_tokens.json",
        .data = "{\"[C]\":51,\"[E]\":52,\"[R]\":53}",
    });

    const model_dir = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer allocator.free(model_dir);

    var mf = manifest_mod.ModelManifest{
        .allocator = allocator,
        .model_type = .extractor,
        .gliner_model_type = try allocator.dupe(u8, "gliner2"),
    };
    defer mf.deinit();

    const arch = try detectArchitecture(allocator, model_dir, mf);
    switch (arch) {
        .gliner => |cfg| {
            try std.testing.expectEqual(@as(u32, 128011), cfg.vocab_size);
            try std.testing.expectEqual(@as(i64, 51), cfg.classification_token_id);
            try std.testing.expectEqual(@as(i64, 52), cfg.entity_token_id);
            try std.testing.expectEqual(@as(i64, 53), cfg.relation_token_id);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "detectArchitectureFromGguf recognizes deberta metadata" {
    const allocator = std.testing.allocator;
    var metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "deberta" } },
        .{ .key = "deberta.vocab_size", .value = .{ .u32 = 32000 } },
        .{ .key = "deberta.embedding_length", .value = .{ .u32 = 384 } },
        .{ .key = "deberta.block_count", .value = .{ .u32 = 6 } },
        .{ .key = "deberta.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "deberta.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "deberta.context_length", .value = .{ .u32 = 1024 } },
        .{ .key = "deberta.position_buckets", .value = .{ .u32 = 128 } },
        .{ .key = "deberta.label_count", .value = .{ .u32 = 7 } },
    };
    const tensors = [_]gguf_mod.writer.TensorSpec{};
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const arch = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (arch) {
        .deberta => |cfg| {
            try std.testing.expectEqual(@as(u32, 384), cfg.hidden_size);
            try std.testing.expectEqual(@as(u32, 7), cfg.num_labels);
        },
        else => return error.TestUnexpectedResult,
    }
}

test "detectArchitectureFromGguf recognizes bert metadata" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "bert" } },
        .{ .key = "bert.family", .value = .{ .string = "roberta" } },
        .{ .key = "bert.vocab_size", .value = .{ .u32 = 32000 } },
        .{ .key = "bert.embedding_length", .value = .{ .u32 = 384 } },
        .{ .key = "bert.block_count", .value = .{ .u32 = 6 } },
        .{ .key = "bert.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "bert.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "bert.context_length", .value = .{ .u32 = 512 } },
        .{ .key = "bert.token_type_count", .value = .{ .u32 = 2 } },
        .{ .key = "bert.label_count", .value = .{ .u32 = 7 } },
        .{ .key = "bert.hidden_act", .value = .{ .string = "gelu" } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .bert => |cfg| {
            try std.testing.expectEqual(bert.ModelType.roberta, cfg.model_type);
            try std.testing.expectEqualStrings("", cfg.effectivePrefix());
            try std.testing.expectEqual(@as(u32, 384), cfg.hidden_size);
            try std.testing.expectEqual(@as(u32, 7), cfg.num_labels);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes t5 metadata" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "t5" } },
        .{ .key = "t5.family", .value = .{ .string = "mt5" } },
        .{ .key = "t5.embedding_length", .value = .{ .u32 = 768 } },
        .{ .key = "t5.attention.key_value_length", .value = .{ .u32 = 64 } },
        .{ .key = "t5.feed_forward_length", .value = .{ .u32 = 2048 } },
        .{ .key = "t5.attention.head_count", .value = .{ .u32 = 12 } },
        .{ .key = "t5.encoder.block_count", .value = .{ .u32 = 8 } },
        .{ .key = "t5.decoder.block_count", .value = .{ .u32 = 10 } },
        .{ .key = "t5.attention.relative_buckets", .value = .{ .u32 = 32 } },
        .{ .key = "t5.attention.relative_max_distance", .value = .{ .u32 = 128 } },
        .{ .key = "t5.vocab_size", .value = .{ .u32 = 250112 } },
        .{ .key = "t5.decoder_start_token_id", .value = .{ .i64 = 0 } },
        .{ .key = "t5.eos_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "t5.pad_token_id", .value = .{ .i64 = 0 } },
        .{ .key = "t5.is_gated_act", .value = .{ .bool_ = true } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .t5 => |cfg| {
            try std.testing.expectEqual(t5_mod.ModelType.mt5, cfg.model_type);
            try std.testing.expectEqual(@as(u32, 768), cfg.d_model);
            try std.testing.expectEqual(@as(u32, 10), cfg.effectiveDecoderLayers());
            try std.testing.expect(cfg.is_gated_act);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes whisper metadata" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "whisper" } },
        .{ .key = "whisper.embedding_length", .value = .{ .u32 = 384 } },
        .{ .key = "whisper.encoder.block_count", .value = .{ .u32 = 4 } },
        .{ .key = "whisper.decoder.block_count", .value = .{ .u32 = 4 } },
        .{ .key = "whisper.encoder.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "whisper.decoder.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "whisper.encoder.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "whisper.decoder.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "whisper.num_mel_bins", .value = .{ .u32 = 80 } },
        .{ .key = "whisper.vocab_size", .value = .{ .u32 = 51865 } },
        .{ .key = "whisper.encoder.context_length", .value = .{ .u32 = 1500 } },
        .{ .key = "whisper.decoder.context_length", .value = .{ .u32 = 448 } },
        .{ .key = "whisper.scale_embedding", .value = .{ .bool_ = false } },
        .{ .key = "whisper.bos_token_id", .value = .{ .i64 = 50257 } },
        .{ .key = "whisper.eos_token_id", .value = .{ .i64 = 50257 } },
        .{ .key = "whisper.pad_token_id", .value = .{ .i64 = 50257 } },
        .{ .key = "whisper.decoder_start_token_id", .value = .{ .i64 = 50258 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .whisper => |cfg| {
            try std.testing.expectEqual(@as(u32, 384), cfg.d_model);
            try std.testing.expectEqual(@as(u32, 80), cfg.num_mel_bins);
            try std.testing.expectEqual(@as(i32, 50258), cfg.decoder_start_token_id);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes layoutlmv3 metadata" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "layoutlmv3" } },
        .{ .key = "layoutlmv3.vocab_size", .value = .{ .u32 = 30522 } },
        .{ .key = "layoutlmv3.embedding_length", .value = .{ .u32 = 384 } },
        .{ .key = "layoutlmv3.block_count", .value = .{ .u32 = 6 } },
        .{ .key = "layoutlmv3.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "layoutlmv3.feed_forward_length", .value = .{ .u32 = 1536 } },
        .{ .key = "layoutlmv3.context_length", .value = .{ .u32 = 512 } },
        .{ .key = "layoutlmv3.token_type_count", .value = .{ .u32 = 2 } },
        .{ .key = "layoutlmv3.max_2d_position_embeddings", .value = .{ .u32 = 1024 } },
        .{ .key = "layoutlmv3.coordinate_size", .value = .{ .u32 = 128 } },
        .{ .key = "layoutlmv3.shape_size", .value = .{ .u32 = 128 } },
        .{ .key = "layoutlmv3.input_size", .value = .{ .u32 = 224 } },
        .{ .key = "layoutlmv3.patch_size", .value = .{ .u32 = 16 } },
        .{ .key = "layoutlmv3.num_channels", .value = .{ .u32 = 3 } },
        .{ .key = "layoutlmv3.label_count", .value = .{ .u32 = 7 } },
        .{ .key = "layoutlmv3.pad_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "layoutlmv3.layer_norm_epsilon", .value = .{ .f32 = 1e-5 } },
        .{ .key = "layoutlmv3.has_relative_attention_bias", .value = .{ .bool_ = true } },
        .{ .key = "layoutlmv3.has_spatial_attention_bias", .value = .{ .bool_ = true } },
        .{ .key = "layoutlmv3.rel_pos_bins", .value = .{ .u32 = 32 } },
        .{ .key = "layoutlmv3.max_rel_pos", .value = .{ .u32 = 128 } },
        .{ .key = "layoutlmv3.rel_2d_pos_bins", .value = .{ .u32 = 64 } },
        .{ .key = "layoutlmv3.max_rel_2d_pos", .value = .{ .u32 = 256 } },
        .{ .key = "layoutlmv3.visual_bbox_max_len", .value = .{ .i64 = 1000 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const gguf_path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(gguf_path);

    const detected = (try detectArchitectureFromGguf(allocator, gguf_path)).?;
    switch (detected) {
        .layoutlmv3 => |cfg| {
            try std.testing.expectEqualStrings("", cfg.weight_prefix);
            try std.testing.expectEqual(@as(u32, 384), cfg.hidden_size);
            try std.testing.expectEqual(@as(u32, 7), cfg.num_labels);
            try std.testing.expect(cfg.has_relative_attention_bias);
            try std.testing.expectEqual(@as(i64, 1000), cfg.visual_bbox_max_len);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes clip metadata" {
    const allocator = std.testing.allocator;
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clip" } },
        .{ .key = "clip.family", .value = .{ .string = "clip" } },
        .{ .key = "clip.text.embedding_length", .value = .{ .u32 = 64 } },
        .{ .key = "clip.text.block_count", .value = .{ .u32 = 2 } },
        .{ .key = "clip.text.attention.head_count", .value = .{ .u32 = 4 } },
        .{ .key = "clip.text.feed_forward_length", .value = .{ .u32 = 128 } },
        .{ .key = "clip.text.context_length", .value = .{ .u32 = 16 } },
        .{ .key = "clip.text.vocab_size", .value = .{ .u32 = 32 } },
        .{ .key = "clip.vision.embedding_length", .value = .{ .u32 = 96 } },
        .{ .key = "clip.vision.block_count", .value = .{ .u32 = 3 } },
        .{ .key = "clip.vision.attention.head_count", .value = .{ .u32 = 6 } },
        .{ .key = "clip.vision.feed_forward_length", .value = .{ .u32 = 192 } },
        .{ .key = "clip.vision.image_size", .value = .{ .u32 = 32 } },
        .{ .key = "clip.vision.patch_size", .value = .{ .u32 = 16 } },
        .{ .key = "clip.projection_dim", .value = .{ .u32 = 48 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .clip => |cfg| {
            try std.testing.expectEqual(clip_mod.ModelFamily.clip, cfg.family);
            try std.testing.expectEqual(@as(u32, 64), cfg.text_hidden_size);
            try std.testing.expectEqual(@as(u32, 96), cfg.vision_hidden_size);
            try std.testing.expectEqual(@as(u32, 48), cfg.projection_dim);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes clap metadata" {
    const allocator = std.testing.allocator;
    var patch_stride = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 4 }, .{ .u32 = 4 } };
    var depths = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 2 }, .{ .u32 = 2 }, .{ .u32 = 6 }, .{ .u32 = 2 } };
    var heads = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 4 }, .{ .u32 = 8 }, .{ .u32 = 16 }, .{ .u32 = 32 } };
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "clap" } },
        .{ .key = "clap.projection_dim", .value = .{ .u32 = 64 } },
        .{ .key = "clap.projection_hidden_act", .value = .{ .string = "relu" } },
        .{ .key = "clap.logit_scale_init_value", .value = .{ .f32 = 14.0 } },
        .{ .key = "clap.text.vocab_size", .value = .{ .u32 = 32 } },
        .{ .key = "clap.text.embedding_length", .value = .{ .u32 = 48 } },
        .{ .key = "clap.text.block_count", .value = .{ .u32 = 2 } },
        .{ .key = "clap.text.attention.head_count", .value = .{ .u32 = 4 } },
        .{ .key = "clap.text.feed_forward_length", .value = .{ .u32 = 96 } },
        .{ .key = "clap.text.context_length", .value = .{ .u32 = 16 } },
        .{ .key = "clap.text.token_type_count", .value = .{ .u32 = 1 } },
        .{ .key = "clap.text.pad_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "clap.audio.embedding_length", .value = .{ .u32 = 128 } },
        .{ .key = "clap.audio.patch_embeds_hidden_size", .value = .{ .u32 = 16 } },
        .{ .key = "clap.audio.patch_embed_input_channels", .value = .{ .u32 = 1 } },
        .{ .key = "clap.audio.patch_size", .value = .{ .u32 = 4 } },
        .{ .key = "clap.audio.patch_stride", .value = .{ .array = .{ .element_type = .u32, .values = &patch_stride } } },
        .{ .key = "clap.audio.num_mel_bins", .value = .{ .u32 = 64 } },
        .{ .key = "clap.audio.spec_size", .value = .{ .u32 = 64 } },
        .{ .key = "clap.audio.window_size", .value = .{ .u32 = 8 } },
        .{ .key = "clap.audio.depths", .value = .{ .array = .{ .element_type = .u32, .values = &depths } } },
        .{ .key = "clap.audio.attention_head_counts", .value = .{ .array = .{ .element_type = .u32, .values = &heads } } },
        .{ .key = "clap.audio.mlp_ratio", .value = .{ .f32 = 4.0 } },
        .{ .key = "clap.audio.layer_norm_epsilon", .value = .{ .f32 = 1e-5 } },
        .{ .key = "clap.audio.hidden_act", .value = .{ .string = "gelu" } },
        .{ .key = "clap.audio.qkv_bias", .value = .{ .bool_ = true } },
        .{ .key = "clap.audio.enable_fusion", .value = .{ .bool_ = false } },
        .{ .key = "clap.audio.enable_patch_fusion", .value = .{ .bool_ = false } },
        .{ .key = "clap.audio.enable_patch_layer_norm", .value = .{ .bool_ = true } },
        .{ .key = "clap.enable_fusion", .value = .{ .bool_ = false } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .clap => |cfg| {
            try std.testing.expectEqual(@as(u32, 64), cfg.projection_dim);
            try std.testing.expectEqual(@as(u32, 48), cfg.text_config.hidden_size);
            try std.testing.expectEqual(@as(u32, 16), cfg.audio_config.patch_embeds_hidden_size);
            try std.testing.expectEqual(@as(u32, 6), cfg.audio_config.depths[2]);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "detectArchitectureFromGguf recognizes florence metadata" {
    const allocator = std.testing.allocator;
    var patch_size = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 7 }, .{ .u32 = 3 }, .{ .u32 = 3 }, .{ .u32 = 3 } };
    var patch_stride = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 4 }, .{ .u32 = 2 }, .{ .u32 = 2 }, .{ .u32 = 2 } };
    var patch_padding = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 3 }, .{ .u32 = 1 }, .{ .u32 = 1 }, .{ .u32 = 1 } };
    var patch_prenorm = [_]gguf_mod.format.MetadataValue{ .{ .bool_ = false }, .{ .bool_ = true }, .{ .bool_ = true }, .{ .bool_ = true } };
    var dim_embed = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 8 }, .{ .u32 = 16 }, .{ .u32 = 24 }, .{ .u32 = 32 } };
    var num_heads = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 1 }, .{ .u32 = 2 }, .{ .u32 = 3 }, .{ .u32 = 4 } };
    var num_groups = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 1 }, .{ .u32 = 2 }, .{ .u32 = 3 }, .{ .u32 = 4 } };
    var depths = [_]gguf_mod.format.MetadataValue{ .{ .u32 = 1 }, .{ .u32 = 1 }, .{ .u32 = 2 }, .{ .u32 = 1 } };
    var image_feature_source = [_]gguf_mod.format.MetadataValue{ .{ .string = "spatial_avg_pool" }, .{ .string = "last_frame" } };
    const metadata = [_]gguf_mod.format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "florence" } },
        .{ .key = "florence.text.d_model", .value = .{ .u32 = 8 } },
        .{ .key = "florence.text.encoder_layers", .value = .{ .u32 = 1 } },
        .{ .key = "florence.text.decoder_layers", .value = .{ .u32 = 1 } },
        .{ .key = "florence.text.encoder_attention_heads", .value = .{ .u32 = 2 } },
        .{ .key = "florence.text.decoder_attention_heads", .value = .{ .u32 = 2 } },
        .{ .key = "florence.text.encoder_ffn_dim", .value = .{ .u32 = 16 } },
        .{ .key = "florence.text.decoder_ffn_dim", .value = .{ .u32 = 16 } },
        .{ .key = "florence.text.vocab_size", .value = .{ .u32 = 32 } },
        .{ .key = "florence.text.max_position_embeddings", .value = .{ .u32 = 16 } },
        .{ .key = "florence.vision.image_size", .value = .{ .u32 = 32 } },
        .{ .key = "florence.vision.hidden_size", .value = .{ .u32 = 8 } },
        .{ .key = "florence.vision.patch_size", .value = .{ .array = .{ .element_type = .u32, .values = &patch_size } } },
        .{ .key = "florence.vision.patch_stride", .value = .{ .array = .{ .element_type = .u32, .values = &patch_stride } } },
        .{ .key = "florence.vision.patch_padding", .value = .{ .array = .{ .element_type = .u32, .values = &patch_padding } } },
        .{ .key = "florence.vision.patch_prenorm", .value = .{ .array = .{ .element_type = .bool_, .values = &patch_prenorm } } },
        .{ .key = "florence.vision.dim_embed", .value = .{ .array = .{ .element_type = .u32, .values = &dim_embed } } },
        .{ .key = "florence.vision.num_heads", .value = .{ .array = .{ .element_type = .u32, .values = &num_heads } } },
        .{ .key = "florence.vision.num_groups", .value = .{ .array = .{ .element_type = .u32, .values = &num_groups } } },
        .{ .key = "florence.vision.depths", .value = .{ .array = .{ .element_type = .u32, .values = &depths } } },
        .{ .key = "florence.vision.window_size", .value = .{ .u32 = 12 } },
        .{ .key = "florence.vision.image_pos_embed_max_pos", .value = .{ .u32 = 50 } },
        .{ .key = "florence.vision.visual_temporal_max_embeddings", .value = .{ .u32 = 100 } },
        .{ .key = "florence.vision.image_feature_source", .value = .{ .array = .{ .element_type = .string, .values = &image_feature_source } } },
        .{ .key = "florence.projection_dim", .value = .{ .u32 = 8 } },
        .{ .key = "florence.image_token_id", .value = .{ .i64 = 31 } },
        .{ .key = "florence.bos_token_id", .value = .{ .i64 = 2 } },
        .{ .key = "florence.eos_token_id", .value = .{ .i64 = 3 } },
        .{ .key = "florence.pad_token_id", .value = .{ .i64 = 1 } },
        .{ .key = "florence.decoder_start_token_id", .value = .{ .i64 = 2 } },
    };
    var layout = try gguf_mod.writer.buildLayout(allocator, &metadata, &.{});
    defer layout.deinit(allocator);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.gguf", .data = layout.header_bytes });
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/model.gguf", .{tmp.sub_path});
    defer allocator.free(path);

    const detected = (try detectArchitectureFromGguf(allocator, path)).?;
    switch (detected) {
        .florence => |cfg| {
            try std.testing.expectEqual(@as(u32, 8), cfg.d_model);
            try std.testing.expectEqual(@as(u32, 32), cfg.image_size);
            try std.testing.expectEqual(@as(u32, 8), cfg.projection_dim);
            try std.testing.expectEqual(@as(u32, 2), cfg.image_feature_source_count);
            try std.testing.expectEqual(florence_mod.ImageFeatureSource.last_frame, cfg.image_feature_sources[1]);
        },
        else => return error.WrongArchitectureDetected,
    }
}

test "deberta required tensors match exported names" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        names.deinit(allocator);
    }
    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    const required = [_][]const u8{
        "embeddings.word_embeddings.weight",
        "embeddings.LayerNorm.weight",
        "embeddings.LayerNorm.bias",
        "encoder.rel_embeddings.weight",
        "encoder.LayerNorm.weight",
        "encoder.LayerNorm.bias",
        "encoder.layer.0.attention.self.query_proj.weight",
        "encoder.layer.0.attention.self.query_proj.bias",
        "encoder.layer.0.attention.self.key_proj.weight",
        "encoder.layer.0.attention.self.key_proj.bias",
        "encoder.layer.0.attention.self.value_proj.weight",
        "encoder.layer.0.attention.self.value_proj.bias",
        "encoder.layer.0.attention.output.dense.weight",
        "encoder.layer.0.attention.output.dense.bias",
        "encoder.layer.0.attention.output.LayerNorm.weight",
        "encoder.layer.0.attention.output.LayerNorm.bias",
        "encoder.layer.0.intermediate.dense.weight",
        "encoder.layer.0.intermediate.dense.bias",
        "encoder.layer.0.output.dense.weight",
        "encoder.layer.0.output.dense.bias",
        "encoder.layer.0.output.LayerNorm.weight",
        "encoder.layer.0.output.LayerNorm.bias",
    };
    for (required) |name| {
        try names.put(allocator, try allocator.dupe(u8, name), {});
    }

    try collectMissingRequiredDebertaWeights(allocator, .{
        .hidden_size = 4,
        .num_hidden_layers = 1,
        .num_attention_heads = 2,
        .intermediate_size = 8,
    }, &names, &missing);
    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}

test "ModernBERT GGUF inventory requires the fused encoder graph" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        names.deinit(allocator);
    }
    const required = [_][]const u8{
        "model.embeddings.tok_embeddings.weight",
        "model.embeddings.norm.weight",
        "model.final_norm.weight",
        "model.layers.0.attn.Wqkv.weight",
        "model.layers.0.attn.Wo.weight",
        "model.layers.0.mlp_norm.weight",
        "model.layers.0.mlp.Wi.weight",
        "model.layers.0.mlp.Wo.weight",
    };
    for (required) |name| try names.put(allocator, try allocator.dupe(u8, name), {});

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }
    const config = modern_bert_arch.Config{
        .num_hidden_layers = 1,
        .checkpoint_layout = .huggingface_fused_qkv_no_bias,
    };
    try collectMissingRequiredModernBertWeights(allocator, config, &names, &missing);
    try std.testing.expectEqual(@as(usize, 0), missing.items.len);

    const removed = names.fetchRemove("model.layers.0.attn.Wqkv.weight") orelse return error.TestUnexpectedResult;
    allocator.free(removed.key);
    try collectMissingRequiredModernBertWeights(allocator, config, &names, &missing);
    try std.testing.expectEqual(@as(usize, 1), missing.items.len);
    try std.testing.expectEqualStrings("model.layers.0.attn.Wqkv.weight", missing.items[0]);
}

test "declared GLiNER ModernBERT GGUF inventory also requires its classifier" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.iterator();
        while (it.next()) |entry| allocator.free(entry.key_ptr.*);
        names.deinit(allocator);
    }
    inline for (&.{
        "classifier.0.weight",
        "classifier.0.bias",
        "classifier.2.weight",
        "classifier.2.bias",
    }) |name| try names.put(allocator, try allocator.dupe(u8, name), {});

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    // Generic ModernBERT only asks for encoder tensors. The composite GLiNER
    // wrapper opts into this additional inventory explicitly.
    try collectMissingRequiredGlinerModernBertClassifier(allocator, &names, &missing);
    try std.testing.expectEqual(@as(usize, 0), missing.items.len);

    const removed = names.fetchRemove("classifier.2.bias") orelse return error.TestUnexpectedResult;
    allocator.free(removed.key);
    try collectMissingRequiredGlinerModernBertClassifier(allocator, &names, &missing);
    try std.testing.expectEqual(@as(usize, 1), missing.items.len);
    try std.testing.expectEqualStrings("classifier.2.bias", missing.items[0]);
}

test "gliner base weight key preserves exported gguf deberta names" {
    try std.testing.expectEqualStrings("embeddings.word_embeddings.weight", glinerBaseWeightKey("encoder.embeddings.word_embeddings.weight"));
    try std.testing.expectEqualStrings("encoder.rel_embeddings.weight", glinerBaseWeightKey("encoder.rel_embeddings.weight"));
    try std.testing.expectEqualStrings("encoder.rel_embeddings.weight", glinerBaseWeightKey("encoder.encoder.rel_embeddings.weight"));
    try std.testing.expectEqualStrings("span_rep.weight", glinerBaseWeightKey("span_rep.weight"));
}

// --- Unified Session implementation ---

const native_mod = @import("../ops/native_compute.zig");
const gpu_hosted_store_mod = @import("../ops/gpu_hosted_store.zig");
const NativeData = native_mod.WeightStore;
const LazyWeightEntry = native_mod.LazyWeightEntry;

const GpuHostedData = if (false or build_options.enable_metal) gpu_hosted_store_mod.WeightStore else void;

/// PJRT backend data: weights are stored and served via the native backend,
/// while the PJRT client is used for compiled partition execution.
const PjrtData = struct {
    /// Native weight store — PJRT uses the native backend for data
    /// conversion (host CTs ↔ PJRT buffers).
    native: NativeData,
    /// Initialized PJRT client. null when the plugin was not found and we
    /// fell back to pure native CPU execution.
    client: if (build_options.enable_pjrt) ?pjrt_lib.pjrt.Client else void =
        if (build_options.enable_pjrt) null else {},
};

const CudaData = struct {
    compute: if (build_options.enable_cuda) *cuda_compute_mod.CudaCompute else void,
};

const BackendData = union {
    native: NativeData,
    metal: if (build_options.enable_metal) GpuHostedData else void,
    cuda: if (build_options.enable_cuda) CudaData else void,
    pjrt: PjrtData,
};

const ArchSession = struct {
    diagnostics: @import("diagnostics.zig").Diagnostics = .{},
    model_lifetime: Session.ModelLifetime = .{ .owner = undefined, .destroy_fn = archClose, .quiesce_fn = archQuiesce },
    // Independent of the model execution lane: preparation can overlap model
    // work. Session close follows worker shutdown, like the borrowed device.
    video_coefficient_mutex: std.Io.Mutex = .init,
    video_coefficients: ?@import("antfly_video").preparation.CoefficientCache = null,
    video_coefficient_permit: ?@import("../backends/session.zig").RunPermit = null,
    video_coefficient_device_permit: ?@import("../backends/session.zig").RunPermit = null,
    video_coefficient_suspended: bool = false,
    allocator: std.mem.Allocator,
    arch_config: ArchConfig,
    recurrent_execution: ?qwen35_execution.Decision = null,
    boundary_identity: ?boundary_bundle.Identity = null,
    gliner_decision_identity: ?gliner_decide_qualification.Identity = null,
    gliner_decision_identity_sealed: bool = false,
    task: SessionTask = .generic,
    /// A ModernBERT extractor is a GLiNER span session only when its manifest
    /// declared the SpanExtractor wrapper and nested ModernBERT encoder.
    gliner_span_encoder_family: manifest_mod.GlinerSpanEncoderFamily = .unknown,
    /// Only real DeBERTa reranker sessions may trade persistent mirror memory
    /// for the resident fused-layer path. Generic classifiers stay unchanged.
    deberta_reranker_weight_mirrors: bool = false,
    backend_type: BackendType,
    kernel_jit_config: kernel_jit.Config = .{},
    kernel_jit_load_context: kernel_jit.LoadContext = .dynamic,
    metal_jit_scope: MetalJitRouteScope = if (build_options.enable_metal)
        metal_runtime.MetalJitRouteScope.none()
    else {},
    budget_floor: runtime.tier.memory.Limits = .{},
    generation_workspace: ?runtime.tier.memory.ReservedGenerationWorkspace = null,
    shared_cache_budget_floor: runtime.tier.cache.Budget = .{},
    backend_data: BackendData,
    /// Optional Io for parallel GEMM dispatch via lib/linalg's Io variants.
    /// Set by `attachIo` (called from SessionManager when its `io` field is
    /// non-null).  Threaded through `makeComputeBackend` to NativeCompute
    /// /GpuHostedCompute/MetalCompute via their `initWithIo` constructors.
    io: ?std.Io = null,
    /// Optional graph-runtime execution strategy.  Set by
    /// `attachGraphRuntimeStrategy` (called from SessionManager when its
    /// `graph_runtime_strategy` field is non-null).  The eager `runArch`
    /// path consults this on a per-architecture basis: today only the
    /// gliner branch routes through `gliner_head_graph.forwardViaGraph`
    /// when this is set; other architectures fall through to their
    /// existing eager path.
    graph_runtime_strategy: ?graph_runtime.Strategy = null,
    /// Tree-packed Laya trunk keys and values, created on first packed run.
    laya_trunk_cache: ?*@import("../architectures/laya_trunk_cache.zig").Cache = null,
    laya_trunk_cache_lock: std.atomic.Mutex = .unlocked,
};

const GlinerDecisionIdentityHandoff = struct {
    identity: ?gliner_decide_qualification.Identity,
    sealed: bool,
};

/// Backend conversion preserves the exact artifact identity captured by the
/// source loader, but the published session must be sealed independently only
/// after ModelManager parses and verifies its tokenizer snapshot. An already
/// sealed source therefore never transfers sealing authority to a new owner.
fn unsealedGlinerDecisionIdentityHandoff(identity: ?gliner_decide_qualification.Identity) GlinerDecisionIdentityHandoff {
    return .{ .identity = identity, .sealed = false };
}

fn layaTrunkCache(self: *ArchSession) ?*@import("../architectures/laya_trunk_cache.zig").Cache {
    platform.sync.lockYielding(&self.laya_trunk_cache_lock);
    defer self.laya_trunk_cache_lock.unlock();
    if (self.laya_trunk_cache == null) {
        const cache = self.allocator.create(@import("../architectures/laya_trunk_cache.zig").Cache) catch return null;
        cache.* = @import("../architectures/laya_trunk_cache.zig").Cache.fromEnvironment(self.allocator);
        self.laya_trunk_cache = cache;
    }
    return self.laya_trunk_cache;
}

/// Hit/miss counters of a packed Laya session's trunk cache.
pub fn layaTrunkCacheStats(session: Session) ?@import("../architectures/laya_trunk_cache.zig").Stats {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    const cache = layaTrunkCache(self) orelse return null;
    return cache.snapshot();
}

/// Choose f16 (default) or exact f32 entries for new trunk cache entries.
pub fn setLayaTrunkCachePrecision(session: Session, precision: @import("../architectures/laya_trunk_cache.zig").Precision) void {
    if (session.vtable != &arch_vtable) return;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    const cache = layaTrunkCache(self) orelse return;
    platform.sync.lockYielding(&cache.mutex);
    defer cache.mutex.unlock();
    cache.precision = precision;
}

/// Replace a session's trunk cache budget (0 disables it) and minimum
/// cached trunk length.
pub fn setLayaTrunkCacheLimit(session: Session, limit_bytes: usize, min_tokens: usize) void {
    if (session.vtable != &arch_vtable) return;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    const cache = layaTrunkCache(self) orelse return;
    platform.sync.lockYielding(&cache.mutex);
    defer cache.mutex.unlock();
    cache.limit_bytes = limit_bytes;
    cache.min_tokens = min_tokens;
}

/// Attach a runtime Io to a Session created by this factory so its
/// compute backend dispatches matmul work through the caller's thread
/// pool.  Safe to call on any Session; no-op when the Session wasn't
/// produced by `createNativeSession` / `createMetalSession`.
pub fn attachIo(session: Session, io: std.Io) void {
    if (session.vtable != &arch_vtable) return;
    const arch_session: *ArchSession = @ptrCast(@alignCast(session.ptr));
    arch_session.io = io;
}

/// Attach a graph-runtime execution strategy to a Session created by this
/// factory.  Mirrors `attachIo`'s lifecycle: SessionManager calls it after
/// `loadFromDir` if its own `graph_runtime_strategy` is non-null.  Today
/// only the gliner branch consults the field (routing through the
/// CT-resident graph path when non-null); other architectures fall
/// through to their existing eager forward.  No-op when called on a
/// Session this factory didn't produce.
pub fn attachGraphRuntimeStrategy(session: Session, strategy: graph_runtime.Strategy) void {
    if (session.vtable != &arch_vtable) return;
    const arch_session: *ArchSession = @ptrCast(@alignCast(session.ptr));
    arch_session.graph_runtime_strategy = strategy;
}

pub const MetalWorkloadProfileExport = if (build_options.enable_metal) metal_runtime.WorkloadProfileExport else void;

fn qwenWorkspaceProvider(session: Session) !*@import("../backends/metal_native_provider.zig").MetalNativeProvider {
    if (comptime !build_options.enable_metal) return error.UnsupportedBackend;
    if (session.vtable != &arch_vtable) return error.UnsupportedBackend;
    const arch: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (arch.backend_type != .metal) return error.UnsupportedBackend;
    return gpuBackendData(arch).shared_metal_native_provider orelse error.MissingMetalNativeProvider;
}

/// All workspace methods require the caller's model execution gate. The
/// managed backend is retired before inspecting or reclaiming physical buffers.
pub fn qwenMetalWorkspaceCredit(session: Session, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !usize {
    if (comptime !build_options.enable_metal) return 0;
    var compute = try getComputeBackendWithControl(session, allocator, control);
    compute.deinit();
    const provider = try qwenWorkspaceProvider(session);
    // A denied larger-shape probe must not discard reusable buffers. The
    // profile includes replacement overlap, and the native guard limits every
    // fresh allocation to the incremental permit after retained credit.
    if (provider.embedding_workspace_lease_count == provider.embedding_workspace_leases.len or
        (provider.embedding_workspace_bytes == 0 and metal_runtime.termite_metal_decode_runtime_embedding_workspace_bytes(provider.raw_decode_runtime) != 0))
        try provider.trimEmbeddingWorkspace();
    return provider.embedding_workspace_bytes;
}

pub fn trimQwenMetalWorkspace(session: Session) !void {
    if (comptime !build_options.enable_metal) return;
    try (try qwenWorkspaceProvider(session)).trimEmbeddingWorkspace();
}

pub fn beginQwenMetalWorkspace(session: Session, bytes: usize) !void {
    if (comptime !build_options.enable_metal) return;
    const provider = try qwenWorkspaceProvider(session);
    if (metal_runtime.termite_metal_decode_runtime_begin_embedding_workspace(provider.raw_decode_runtime, bytes) != 0)
        return error.MetalWorkspaceBusy;
}

pub fn finishQwenMetalWorkspace(session: Session, lease: *?runtime.tier.memory.AdmissionLease) !void {
    if (comptime !build_options.enable_metal) return;
    const provider = try qwenWorkspaceProvider(session);
    errdefer provider.trimEmbeddingWorkspace() catch platform.inference_process_supervisor.restartWorker();
    const result = metal_runtime.termite_metal_decode_runtime_end_embedding_workspace(provider.raw_decode_runtime);
    if (result < 0) return error.MetalWorkspaceBusy;
    if (result > 0) return error.ResourceLimitExceeded;
    try provider.retainEmbeddingWorkspace(lease);
}

/// Starts an explicit pre-serving/benchmark census on an architecture session.
/// Unsupported or non-Metal sessions return false; there is no backend-pointer
/// downcast outside this factory.
pub fn beginMetalWorkloadProfile(session: Session, regime: ops.WorkloadRegime) !bool {
    if (comptime !build_options.enable_metal) return false;
    if (session.vtable != &arch_vtable) return false;
    const arch_session: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (arch_session.backend_type != .metal) return false;
    const provider = gpuBackendData(arch_session).shared_metal_native_provider orelse
        return error.MetalWorkloadProfileUnavailable;
    try provider.workloadProfileBegin(@fromBackingInt(@intCast(@backingInt(regime))));
    return true;
}

/// Stops capture at a command-buffer boundary, optionally calibrates every
/// eligible first-slice signature, and returns an owned backend-neutral export.
pub fn endMetalWorkloadProfile(
    session: Session,
    allocator: std.mem.Allocator,
    calibrate_first_slice: bool,
) !?MetalWorkloadProfileExport {
    if (comptime !build_options.enable_metal) return null;
    if (session.vtable != &arch_vtable) return null;
    const arch_session: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (arch_session.backend_type != .metal) return null;
    const provider = gpuBackendData(arch_session).shared_metal_native_provider orelse
        return error.MetalWorkloadProfileUnavailable;
    try provider.workloadProfileEnd();
    var snapshot = try provider.workloadProfileSnapshot();
    var tuning: metal_runtime.MetalWorkloadTuningSummary = .{};
    if (calibrate_first_slice and snapshot.eligibleForSelection()) {
        if (!metal_runtime.metalJitHasPersistentQualificationCache(provider)) {
            return error.MetalJitQualificationCacheUnavailable;
        }
        try metal_runtime.calibrateFirstMetalTuningSlice(allocator, provider.raw_decode_runtime, &snapshot);
        tuning = try metal_runtime.tuneFirstMetalWorkloadSlice(provider, &snapshot);
    }
    return try metal_runtime.workloadProfileExportWithTuningAlloc(allocator, snapshot, tuning, &provider.jit_scope);
}

test "attachIo reaches native compute backend" {
    const allocator = std.testing.allocator;
    var arch_session = ArchSession{
        .allocator = allocator,
        .arch_config = .{ .gpt = .{
            .hidden_size = 4,
            .num_hidden_layers = 1,
            .num_attention_heads = 1,
            .intermediate_size = 8,
            .vocab_size = 16,
        } },
        .backend_type = .native,
        .backend_data = .{ .native = .{
            .allocator = allocator,
            .resident_weights = .{},
            .lazy_weights = .{},
        } },
    };
    const session = Session{
        .ptr = &arch_session,
        .vtable = &arch_vtable,
    };

    attachIo(session, std.testing.io);
    var cb = try makeComputeBackend(&arch_session, allocator, null);
    defer cb.deinit();
    try std.testing.expect(cb.getIo() != null);
}

fn gpuBackendData(self: *ArchSession) *GpuHostedData {
    return switch (self.backend_type) {
        .metal => if (comptime build_options.enable_metal) &self.backend_data.metal else unreachable,
        else => unreachable,
    };
}

const arch_vtable = Session.VTable{
    .hasLayaDecisions = archHasLayaDecisions,
    .runLayaDecisions = archRunLayaDecisions,
    .run = &archRun,
    .runWithControl = &archRunWithControl,
    .runResident = &archRunResident,
    .runResidentTextEmbedding = &archRunResidentTextEmbedding,
    .runResidentTextEmbeddingWithControl = &archRunResidentTextEmbeddingWithControl,
    .runResidentWithControl = &archRunResidentWithControl,
    .inputInfo = &archInputInfo,
    .outputInfo = &archOutputInfo,
    .independentBatchRows = &archIndependentBatchRows,
    .runGeometry = &archRunGeometry,
    .backend = &archBackend,
    .close = &archClose,
};

fn archRunResidentWithControl(
    ptr: *anyopaque,
    inputs: []const Tensor,
    allocator: std.mem.Allocator,
    control: InferenceExecutionControl,
) !?ResidentOutputs {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    if (self.task == .classifier or self.task == .extractor) return null;
    if (self.arch_config == .clip or self.arch_config == .clap) return archRunMediaResident(self, inputs, allocator, control);
    if (self.arch_config == .whisper) return archRunIncrementalEncoderResident(self, inputs, allocator, control);
    const cfg = switch (self.arch_config) {
        .bert => |cfg| cfg,
        // Returning null forces the caller through runWithControl. Never
        // advertise a controlled resident fast path by delegating to an
        // implementation that cannot observe the request lifetime.
        else => return null,
    };
    const bert_inputs = try parseBertRunInputs(inputs);
    const cb = try allocator.create(ops.ComputeBackend);
    errdefer allocator.destroy(cb);
    cb.* = try makeComputeBackend(self, allocator, null);
    cb.execution_control = control;
    errdefer cb.deinit();
    const hidden = try bert_arch.forwardCtWithControl(
        cb,
        allocator,
        cfg,
        bert_inputs.input_ids,
        bert_inputs.attention_mask,
        bert_inputs.token_type_ids,
        bert_inputs.batch,
        bert_inputs.seq_len,
        control,
    );
    errdefer cb.free(hidden);
    const outputs = try allocator.alloc(ops.CT, 1);
    errdefer allocator.free(outputs);
    outputs[0] = hidden;
    return .{
        .outputs = outputs,
        .backend = cb,
        .allocator = allocator,
        .backend_owner = cb,
        .deinit_backend_owner = &deinitResidentComputeBackend,
        .backend_owner_cleanup_services = &resident_compute_cleanup_services,
    };
}

/// Actual architecture-owned projected CT entry. Explicit compiled/graph
/// strategies retain their chosen runtime and decline this eager resident seam.
/// Host wrappers remain responsible for their explicit final host outputs.
fn archRunMediaResident(self: *ArchSession, inputs: []const Tensor, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !?ResidentOutputs {
    if (self.arch_config != .clip and self.arch_config != .clap) return null;
    if (graphRuntimeStrategyEnabled(self.graph_runtime_strategy)) return null;
    if (inputs.len == 0) return error.MissingInputs;
    const first = inputs[0];
    if (first.shape.len == 0 or first.shape[0] <= 0) return error.InvalidInputShape;
    if (self.backend_type != .native and self.backend_type != .metal and self.backend_type != .cuda) return null;
    // These resident frontends intentionally retain the existing GPU policy.
    // Decline immutable unsupported profiles before constructing any backend.
    if (self.backend_type == .native and first.dtype == .f32 and
        (std.mem.eql(u8, first.name, "pixel_values") or std.mem.eql(u8, first.name, "input_features"))) return null;
    const cb = try allocator.create(ops.ComputeBackend);
    var backend_live = false;
    var transferred = false;
    defer if (!transferred) {
        if (backend_live) cb.deinit();
        allocator.destroy(cb);
    };
    cb.* = try makeComputeBackend(self, allocator, null);
    backend_live = true;
    cb.execution_control = control;
    if (control) |active| try active.check();
    const output: ops.CT = switch (self.arch_config) {
        .clip => |cfg| media: {
            if (first.dtype == .f32 and std.mem.eql(u8, first.name, "pixel_values")) {
                const dims = try mediaFloatInput(first, 4);
                if (first.shape[1] != 3 or first.shape[2] != cfg.image_size or first.shape[3] != cfg.image_size) return error.InvalidInputShape;
                break :media (try clip_arch.visionEncoderForwardResident(cb, allocator, cfg, first.asFloat32(), dims[0])) orelse return null;
            }
            if (cb.vtable.takeRows == null) return null;
            const text = try parseBertRunInputs(inputs);
            break :media try clip_arch.textEncoderForwardTensor(cb, allocator, cfg, text.input_ids, text.batch, text.seq_len);
        },
        .clap => |cfg| media: {
            if (first.dtype == .f32 and std.mem.eql(u8, first.name, "input_features")) {
                const dims = try mediaFloatInput(first, 4);
                const longer = if (inputs.len > 1 and inputs[1].dtype == .bool_) inputs[1].data else &.{};
                break :media (try clap_arch.audioEncoderForwardResident(cb, allocator, cfg, first.asFloat32(), dims[0], dims[1], dims[2], dims[3], longer)) orelse return null;
            }
            if (cb.vtable.takeRows == null) return null;
            const text = try parseBertRunInputs(inputs);
            break :media try clap_arch.textEncoderForwardTensor(cb, allocator, cfg, text.input_ids, text.attention_mask, text.token_type_ids, text.batch, text.seq_len);
        },
        else => unreachable,
    };
    errdefer cb.free(output);
    const expected_width = switch (self.arch_config) {
        .clip => |cfg| cfg.projection_dim,
        .clap => |cfg| cfg.projection_dim,
        else => unreachable,
    };
    const shape = try cb.tensorShape(output, allocator);
    defer allocator.free(shape);
    if (try cb.tensorDType(output) != .f32 or shape.len != 2 or shape[0] != first.shape[0] or shape[1] != expected_width) return error.UnexpectedOutputShape;
    if (control) |active| try active.check();
    const outputs = try allocator.alloc(ops.CT, 1);
    outputs[0] = output;
    transferred = true;
    return .{ .outputs = outputs, .backend = cb, .allocator = allocator, .backend_owner = cb, .deinit_backend_owner = deinitResidentComputeBackend, .backend_owner_cleanup_services = &resident_compute_cleanup_services };
}

fn mediaFloatInput(input: Tensor, rank: usize) ![4]usize {
    if (input.dtype != .f32 or input.shape.len != rank or rank > 4 or !input.isAlignedFor(f32)) return error.InvalidInputShape;
    var shape: [4]usize = @splat(1);
    var elements: usize = 1;
    for (input.shape, 0..) |dimension, axis| {
        if (dimension <= 0) return error.InvalidInputShape;
        shape[axis] = std.math.cast(usize, dimension) orelse return error.InvalidInputShape;
        elements = std.math.mul(usize, elements, shape[axis]) catch return error.InvalidInputShape;
    }
    if (input.data.len != (std.math.mul(usize, elements, @sizeOf(f32)) catch return error.InvalidInputShape)) return error.InvalidInputShape;
    return shape;
}

fn archRunIncrementalEncoderResident(self: *ArchSession, inputs: []const Tensor, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !?ResidentOutputs {
    if (self.arch_config != .whisper or inputs.len != 1) return null;
    const first = inputs[0];
    if (first.dtype != .f32 or !std.mem.eql(u8, first.name, "input_features")) return null;
    const config = self.arch_config.whisper;
    if (first.shape.len != 3 or first.shape[0] <= 0 or first.shape[1] != config.num_mel_bins or first.shape[2] <= 0 or !first.isAlignedFor(f32)) return error.InvalidInputShape;
    const batch = std.math.cast(usize, first.shape[0]) orelse return error.ResourceLimitExceeded;
    const frames = std.math.cast(usize, first.shape[2]) orelse return error.ResourceLimitExceeded;
    const row_elements = std.math.mul(usize, config.num_mel_bins, frames) catch return error.ResourceLimitExceeded;
    const elements = std.math.mul(usize, batch, row_elements) catch return error.ResourceLimitExceeded;
    const bytes = std.math.mul(usize, elements, @sizeOf(f32)) catch return error.ResourceLimitExceeded;
    if (first.data.len != bytes) return error.InvalidInputShape;
    const mel_shape = [_]i32{ std.math.cast(i32, batch) orelse return error.ResourceLimitExceeded, std.math.cast(i32, config.num_mel_bins) orelse return error.ResourceLimitExceeded, std.math.cast(i32, frames) orelse return error.ResourceLimitExceeded };
    const rows = (std.math.add(usize, frames, 1) catch return error.ResourceLimitExceeded) / 2;
    const cb = try allocator.create(ops.ComputeBackend);
    errdefer allocator.destroy(cb);
    cb.* = try makeComputeBackend(self, allocator, null);
    cb.execution_control = control;
    errdefer cb.deinit();
    if (control) |active| try active.check();
    const mel = try cb.fromFloat32Shape(first.asFloat32(), &mel_shape);
    defer cb.free(mel);
    const encoded = try whisper_arch.encoderForwardTensor(cb, allocator, config, mel, batch, frames);
    defer cb.free(encoded);
    const hidden = try cb.primReshape(encoded, &.{ first.shape[0], std.math.cast(i64, rows) orelse return error.ResourceLimitExceeded, config.d_model });
    errdefer cb.free(hidden);
    const outputs = try allocator.alloc(ops.CT, 1);
    outputs[0] = hidden;
    return .{ .outputs = outputs, .backend = cb, .allocator = allocator, .backend_owner = cb, .deinit_backend_owner = deinitResidentComputeBackend, .backend_owner_cleanup_services = &resident_compute_cleanup_services };
}

fn acceptsResidentComputeCleanup(outputs: *const ResidentOutputs) bool {
    return outputs.deinit_backend_owner == deinitResidentComputeBackend;
}
const resident_compute_cleanup_services = ResidentOutputs.BackendOwnerCleanupServices{ .accepts = acceptsResidentComputeCleanup, .deinit = deinitResidentComputeBackendWithCleanup, .detach_dense_f32 = detachResidentDenseF32Payload };
fn detachResidentDenseF32Payload(outputs: *ResidentOutputs, current: @import("cleanup_context.zig").Context) !bool {
    const model = outputs.model_retention orelse return false;
    if (outputs.backend_owner_cleanup_services != &resident_compute_cleanup_services or
        outputs.deinit_backend_owner != deinitResidentComputeBackend or
        outputs.backend_owner != @as(*const anyopaque, @ptrCast(outputs.backend))) return false;
    // Static exact model services must veto wrappers before an owner cast.
    if (model.destroy_fn != archClose or model.quiesce_fn != archQuiesce) return false;
    const owner: *ArchSession = @ptrCast(@alignCast(model.owner));
    if (owner.backend_type != .metal) return false;
    if (comptime !build_options.enable_metal) return false;
    if (&owner.model_lifetime != model) return false;
    // Current B and its exact live numerical operation are both required.
    // Keep supervision through producer completion AND original Engine teardown.
    _ = try current.operationFor(outputs.backend);
    const control = outputs.backend.execution_control orelse return error.ExecutionControlRequired;
    var sdk_guard = try control.enterUninterruptible(.uninterruptible);
    defer sdk_guard.deinit();
    const payload = (try outputs.backend.detachDenseF32PhysicalPayload(outputs.outputs, @ptrCast(gpuBackendData(owner)), current)) orelse return false;
    var replacement = outputs.*;
    replacement.outputs = payload.outputs;
    replacement.backend = payload.backend;
    replacement.allocator = payload.allocator;
    replacement.backend_owner = payload.backend;
    replacement.deinit_backend_owner = deinitResidentComputeBackend;
    replacement.backend_owner_cleanup_services = &resident_compute_cleanup_services;
    replacement.model_retention = model.retain();
    // Exact move of the existing request quota and cold shell; old SDK/header
    // destruction cannot refund the still-live physical payload.
    outputs.resource_lease = null;
    outputs.retirement_shell = null;
    outputs.deinitWithCleanup(current);
    outputs.* = replacement;
    return true;
}

fn deinitResidentComputeBackendWithCleanup(owner: *anyopaque, allocator: std.mem.Allocator, current: @import("../execution/cleanup_context.zig").Context) void {
    const cb: *ops.ComputeBackend = @ptrCast(@alignCast(owner));
    cb.deinitWithCleanup(current);
    allocator.destroy(cb);
}

fn deinitResidentComputeBackend(owner: *anyopaque, allocator: std.mem.Allocator) void {
    const cb: *ops.ComputeBackend = @ptrCast(@alignCast(owner));
    cb.deinit();
    allocator.destroy(cb);
}

const BertRunInputs = struct {
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
};

fn validateI64Matrix(tensor: Tensor, expected_shape: ?[2]usize) !struct {
    values: []const i64,
    shape: [2]usize,
} {
    if (tensor.dtype != .i64) return error.InvalidInputType;
    if (tensor.shape.len != 2 or tensor.shape[0] <= 0 or tensor.shape[1] <= 0) return error.InvalidInputShape;
    const shape = [2]usize{
        std.math.cast(usize, tensor.shape[0]) orelse return error.InvalidInputShape,
        std.math.cast(usize, tensor.shape[1]) orelse return error.InvalidInputShape,
    };
    if (expected_shape) |expected| {
        if (!std.mem.eql(usize, &shape, &expected)) return error.InvalidInputShape;
    }
    const element_count = std.math.mul(usize, shape[0], shape[1]) catch return error.InvalidInputShape;
    const expected_bytes = std.math.mul(usize, element_count, @sizeOf(i64)) catch return error.InvalidInputShape;
    if (tensor.data.len != expected_bytes or !tensor.isAlignedFor(i64)) return error.InvalidInputShape;
    return .{ .values = tensor.asInt64(), .shape = shape };
}

fn parseBertRunInputs(inputs: []const Tensor) !BertRunInputs {
    if (inputs.len < 2) return error.MissingInputs;
    const ids = try validateI64Matrix(inputs[0], null);
    const mask = try validateI64Matrix(inputs[1], ids.shape);
    const token_type_ids = if (inputs.len > 2)
        (try validateI64Matrix(inputs[2], ids.shape)).values
    else
        null;
    return .{
        .input_ids = ids.values,
        .attention_mask = mask.values,
        .token_type_ids = token_type_ids,
        .batch = ids.shape[0],
        .seq_len = ids.shape[1],
    };
}

fn parseEmbeddingGemma2Inputs(inputs: []const Tensor, cfg: embedding_gemma2_model.Config) !embedding_gemma2_reference.Request {
    var ids: ?[]const i64 = null;
    var embeddings: ?[]const f32 = null;
    var mask: ?[]const i64 = null;
    var shape: ?[2]usize = null;
    for (inputs) |tensor| {
        if (std.mem.eql(u8, tensor.name, "input_ids")) {
            const matrix = try validateI64Matrix(tensor, shape);
            ids = matrix.values;
            shape = matrix.shape;
        } else if (std.mem.eql(u8, tensor.name, "attention_mask")) {
            const matrix = try validateI64Matrix(tensor, shape);
            mask = matrix.values;
            shape = matrix.shape;
        } else if (std.mem.eql(u8, tensor.name, "inputs_embeds")) {
            if (tensor.dtype != .f32 or tensor.shape.len != 3 or tensor.shape[0] <= 0 or tensor.shape[1] <= 0 or tensor.shape[2] != cfg.hidden_size or !tensor.isAlignedFor(f32))
                return error.InvalidInputShape;
            const matrix_shape = [2]usize{ @intCast(tensor.shape[0]), @intCast(tensor.shape[1]) };
            if (shape) |expected| if (!std.mem.eql(usize, &matrix_shape, &expected)) return error.InvalidInputShape;
            const tokens = std.math.mul(usize, matrix_shape[0], matrix_shape[1]) catch return error.InvalidInputShape;
            const count = std.math.mul(usize, tokens, cfg.hidden_size) catch return error.InvalidInputShape;
            const byte_count = std.math.mul(usize, count, @sizeOf(f32)) catch return error.InvalidInputShape;
            if (tensor.data.len != byte_count) return error.InvalidInputShape;
            embeddings = tensor.asFloat32();
            shape = matrix_shape;
        }
    }
    const resolved_shape = shape orelse return error.MissingInputs;
    const attention_mask = mask orelse return error.MissingInputs;
    const input: embedding_gemma2_reference.Input = if (embeddings) |values|
        .{ .input_embeddings = values }
    else if (ids) |values|
        .{ .token_ids = values }
    else
        return error.MissingInputs;
    const request = embedding_gemma2_reference.Request{
        .input = input,
        .attention_mask = attention_mask,
        .batch = resolved_shape[0],
        .sequence = resolved_shape[1],
    };
    try request.validate(cfg);
    return request;
}

fn validateEmbeddingGemma2Output(values: []const f32, batch: usize, dimension: usize) !void {
    if (batch == 0 or dimension == 0) return error.InvalidEmbeddingOutput;
    const expected = std.math.mul(usize, batch, dimension) catch return error.InvalidEmbeddingOutput;
    if (values.len != expected) return error.InvalidEmbeddingOutput;
    for (0..batch) |row| {
        var norm_squared: f32 = 0;
        for (values[row * dimension ..][0..dimension]) |value| {
            if (!std.math.isFinite(value)) return error.InvalidEmbeddingOutput;
            norm_squared += value * value;
        }
        if (!std.math.isFinite(norm_squared) or norm_squared == 0) return error.InvalidEmbeddingOutput;
    }
}

test "EmbeddingGemma2 output validation is batch exact and finite" {
    try validateEmbeddingGemma2Output(&.{ 0.6, 0.8, 0.8, 0.6 }, 2, 2);
    try std.testing.expectError(error.InvalidEmbeddingOutput, validateEmbeddingGemma2Output(&.{ 0.6, 0.8 }, 2, 2));
    try std.testing.expectError(error.InvalidEmbeddingOutput, validateEmbeddingGemma2Output(&.{ 0.6, 0.8, 0, 0 }, 2, 2));
    try std.testing.expectError(error.InvalidEmbeddingOutput, validateEmbeddingGemma2Output(&.{ 0.6, std.math.nan(f32) }, 1, 2));
    try std.testing.expectError(error.InvalidEmbeddingOutput, validateEmbeddingGemma2Output(&.{}, std.math.maxInt(usize), 2));
}

test "BERT session inputs require matching aligned i64 matrices" {
    const allocator = std.testing.allocator;
    const shape = [_]i64{ 1, 2 };
    var ids = try Tensor.initInt64(allocator, "input_ids", &shape, &.{ 7, 8 });
    defer ids.deinit();
    var mask = try Tensor.initInt64(allocator, "attention_mask", &shape, &.{ 1, 1 });
    defer mask.deinit();
    const parsed = try parseBertRunInputs(&.{ ids, mask });
    try std.testing.expectEqual(@as(usize, 1), parsed.batch);
    try std.testing.expectEqual(@as(usize, 2), parsed.seq_len);

    var wrong_type = try Tensor.initFloat32(allocator, "attention_mask", &shape, &.{ 1, 1 });
    defer wrong_type.deinit();
    try std.testing.expectError(error.InvalidInputType, parseBertRunInputs(&.{ ids, wrong_type }));

    const wrong_shape = [_]i64{ 2, 1 };
    var mismatched = try Tensor.initInt64(allocator, "attention_mask", &wrong_shape, &.{ 1, 1 });
    defer mismatched.deinit();
    try std.testing.expectError(error.InvalidInputShape, parseBertRunInputs(&.{ ids, mismatched }));
}

test "BERT architecture regression declarations compile" {
    std.testing.refAllDecls(bert_arch);
}

test "GLiNER decision head declarations compile" {
    std.testing.refAllDecls(gliner_decision_head);
}

fn archRunResident(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) !?ResidentOutputs {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    if (self.task == .classifier or self.task == .extractor) return null;
    if (self.arch_config == .clip or self.arch_config == .clap) return archRunMediaResident(self, inputs, allocator, null);
    if (self.arch_config == .whisper) return archRunIncrementalEncoderResident(self, inputs, allocator, null);
    switch (self.arch_config) {
        .bert, .modern_bert, .nomic_bert, .embedding_gemma2 => {},
        else => return null,
    }
    if (self.arch_config == .embedding_gemma2 and self.backend_type == .cuda) return null;
    const bert_inputs = try parseBertRunInputs(inputs);

    const cb = try allocator.create(ops.ComputeBackend);
    errdefer allocator.destroy(cb);
    cb.* = try makeComputeBackend(self, allocator, null);
    errdefer cb.deinit();

    const hidden = switch (self.arch_config) {
        .bert => |cfg| try bert_arch.forwardCt(
            cb,
            allocator,
            cfg,
            bert_inputs.input_ids,
            bert_inputs.attention_mask,
            bert_inputs.token_type_ids,
            bert_inputs.batch,
            bert_inputs.seq_len,
        ),
        .modern_bert => |cfg| try modern_bert_arch.forwardCT(
            cb,
            allocator,
            cfg,
            bert_inputs.input_ids,
            bert_inputs.attention_mask,
            bert_inputs.batch,
            bert_inputs.seq_len,
        ),
        .embedding_gemma2 => |cfg| try embedding_gemma2_arch.forwardCT(cb, allocator, cfg, bert_inputs.input_ids, bert_inputs.attention_mask, bert_inputs.batch, bert_inputs.seq_len),
        .nomic_bert => |cfg| try nomic_bert_arch.forwardCT(
            cb,
            allocator,
            cfg,
            bert_inputs.input_ids,
            bert_inputs.attention_mask,
            bert_inputs.token_type_ids,
            bert_inputs.batch,
            bert_inputs.seq_len,
        ),
        else => unreachable,
    };
    errdefer cb.free(hidden);
    const outputs = try allocator.alloc(ops.CT, 1);
    errdefer allocator.free(outputs);
    outputs[0] = hidden;
    return .{
        .outputs = outputs,
        .backend = cb,
        .allocator = allocator,
        .backend_owner = cb,
        .deinit_backend_owner = &deinitResidentComputeBackend,
        .backend_owner_cleanup_services = &resident_compute_cleanup_services,
    };
}

fn archRunResidentTextEmbedding(
    ptr: *anyopaque,
    inputs: []const Tensor,
    request: ResidentTextEmbeddingRequest,
    allocator: std.mem.Allocator,
) !?ResidentOutputs {
    return archRunResidentTextEmbeddingImpl(ptr, inputs, request, allocator, null);
}

fn archRunResidentTextEmbeddingWithControl(
    ptr: *anyopaque,
    inputs: []const Tensor,
    request: ResidentTextEmbeddingRequest,
    allocator: std.mem.Allocator,
    control: InferenceExecutionControl,
) !?ResidentOutputs {
    return archRunResidentTextEmbeddingImpl(ptr, inputs, request, allocator, control);
}

fn archRunResidentTextEmbeddingImpl(
    ptr: *anyopaque,
    inputs: []const Tensor,
    request: ResidentTextEmbeddingRequest,
    allocator: std.mem.Allocator,
    control: ?InferenceExecutionControl,
) !?ResidentOutputs {
    if (control) |active| try active.check();
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    if (self.task == .classifier or self.task == .extractor) return null;
    if (request.pooling != .mean) return null;
    if (self.arch_config == .embedding_gemma2 and self.backend_type == .cuda) {
        const cfg = self.arch_config.embedding_gemma2.resident orelse return error.UnsupportedEmbeddingGemma2Config;
        const encoder_request = try parseEmbeddingGemma2Inputs(inputs, cfg);
        const embedding = switch (self.backend_type) {
            .cuda => if (comptime build_options.enable_cuda) try self.backend_data.cuda.compute.runEmbeddingGemma2(
                allocator,
                cfg,
                switch (encoder_request.input) {
                    .token_ids => |values| .{ .token_ids = values },
                    .input_embeddings => |values| .{ .input_embeddings = values },
                },
                encoder_request.attention_mask,
                encoder_request.batch,
                encoder_request.sequence,
                cfg.outputDim(),
                control,
            ) else return error.CudaNotEnabled,
            .native => blk: {
                var reference_cb = try makeComputeBackend(self, allocator, null);
                defer reference_cb.deinit();
                reference_cb.execution_control = control;
                break :blk try embedding_gemma2_reference.forwardReference(&reference_cb, allocator, cfg, encoder_request, cfg.outputDim());
            },
            else => return null,
        };
        defer allocator.free(embedding);
        try validateEmbeddingGemma2Output(embedding, encoder_request.batch, cfg.outputDim());
        const cb = try allocator.create(ops.ComputeBackend);
        errdefer allocator.destroy(cb);
        cb.* = try makeComputeBackend(self, allocator, null);
        errdefer cb.deinit();
        const output_shape = [_]i32{ @intCast(encoder_request.batch), @intCast(cfg.outputDim()) };
        const output = try cb.fromFloat32Shape(embedding, &output_shape);
        errdefer cb.free(output);
        const outputs = try allocator.alloc(ops.CT, 1);
        errdefer allocator.free(outputs);
        outputs[0] = output;
        return .{
            .outputs = outputs,
            .backend = cb,
            .allocator = allocator,
            .backend_owner = cb,
            .deinit_backend_owner = &deinitResidentComputeBackend,
            .backend_owner_cleanup_services = &resident_compute_cleanup_services,
        };
    }
    if (self.backend_type != .metal) return null;
    const cfg = switch (self.arch_config) {
        .nomic_bert => |cfg| cfg,
        else => return null,
    };
    const bert_inputs = try parseBertRunInputs(inputs);

    const cb = try allocator.create(ops.ComputeBackend);
    errdefer allocator.destroy(cb);
    cb.* = try makeComputeBackend(self, allocator, null);
    errdefer cb.deinit();

    const embedding = (try nomic_bert_arch.forwardEmbeddingCT(
        cb,
        allocator,
        cfg,
        bert_inputs.input_ids,
        bert_inputs.attention_mask,
        bert_inputs.token_type_ids,
        bert_inputs.batch,
        bert_inputs.seq_len,
        request.normalize,
    )) orelse {
        cb.deinit();
        allocator.destroy(cb);
        return null;
    };
    errdefer cb.free(embedding);
    const outputs = try allocator.alloc(ops.CT, 1);
    errdefer allocator.free(outputs);
    outputs[0] = embedding;
    return .{
        .outputs = outputs,
        .backend = cb,
        .allocator = allocator,
        .backend_owner = cb,
        .deinit_backend_owner = &deinitResidentComputeBackend,
        .backend_owner_cleanup_services = &resident_compute_cleanup_services,
    };
}

/// Create a ComputeBackend from an ArchSession. Used internally and by generation pipeline.
fn makeComputeBackend(
    self: *ArchSession,
    allocator: std.mem.Allocator,
    run_budget: ?*runtime.tier.memory.RunBudget,
) !ops.ComputeBackend {
    if (run_budget) |budget| {
        switch (self.backend_type) {
            .native => if (self.backend_data.native.tier_cache) |*tier_cache| {
                tier_cache.widenToAtLeast(.{
                    .host_limit_bytes = @max(budget.limits.host_limit_bytes, self.shared_cache_budget_floor.host_limit_bytes),
                    .backend_limit_bytes = @max(budget.limits.backend_limit_bytes, self.shared_cache_budget_floor.backend_limit_bytes),
                });
            },
            .metal => if (build_options.enable_metal) widenGpuHostedTierCache(self, budget),
            // PJRT: widen the native CPU host-backend tier cache if present.
            .pjrt => if (self.backend_data.pjrt.native.tier_cache) |*tier_cache| {
                tier_cache.widenToAtLeast(.{
                    .host_limit_bytes = @max(budget.limits.host_limit_bytes, self.shared_cache_budget_floor.host_limit_bytes),
                    .backend_limit_bytes = @max(budget.limits.backend_limit_bytes, self.shared_cache_budget_floor.backend_limit_bytes),
                });
            },
            .cuda => {},
            .wasm => {},
        }
    }
    return switch (self.backend_type) {
        .native => try @import("../backends/compute_factory.zig").createNative(allocator, &self.backend_data.native, run_budget, self.io, .{
            .observe_allocations = self.diagnostics.enabled.load(.acquire),
            .quantized_activations = if (self.arch_config == .gliner_boundary or self.arch_config == .bert) .strict_f32 else .automatic,
            .borrow_bf16_linear_weights = self.arch_config == .gpt and self.arch_config.gpt.family == .qwen3,
            .cache_bf16_weight_mirrors = self.arch_config == .embedding_gemma2,
        }),
        .metal => try makeGpuHostedComputeBackend(self, allocator, run_budget),
        // PJRT does not have a generic ComputeBackend VTable of its own.
        // Non-partitioned ops (weight loads, sampling, etc.) run on native CPU.
        // Compiled partitions are executed via PjrtExecutor through the
        // multi_executor pipeline; see generation.zig attachPjrtExecutors.
        .pjrt => try @import("../backends/compute_factory.zig").createNative(allocator, &self.backend_data.pjrt.native, run_budget, self.io, .{ .observe_allocations = self.diagnostics.enabled.load(.acquire) }),
        .cuda => if (comptime build_options.enable_cuda)
            try @import("../backends/compute_factory.zig").acquireCuda(self.backend_data.cuda.compute, run_budget)
        else
            return error.CudaNotEnabled,
        .wasm => return error.WasmNotSupportedHere,
    };
}

/// The placement resolved at model load; read-only diagnostics never re-resolve
/// mutable environment flags or alter an existing recurrent history.
pub fn getRecurrentExecution(session: Session) ?qwen35_execution.Decision {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return self.recurrent_execution;
}

/// Get GPT config from a session, if it's a GPT model. Returns null for non-GPT architectures.
pub fn getGptConfig(session: Session) ?gpt_mod.Config {
    if (session.model_contract.decoder_config) |config| return config.*;
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .gpt => |cfg| cfg,
        else => null,
    };
}

pub fn getEmbeddingGemma2ResidentConfig(session: Session) ?embedding_gemma2_model.Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .embedding_gemma2 => |cfg| if (self.backend_type == .cuda) cfg.resident else null,
        else => null,
    };
}

/// Load GPT architecture metadata without opening a runtime session or weight
/// backend. This is for artifact-backed whole-model runtimes that still need
/// tokenizer/config shape information but must not keep a duplicate native
/// model resident beside the compiled backend.
pub fn loadGptConfigFromModelDir(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    mf: manifest_mod.ModelManifest,
) !gpt_mod.Config {
    return switch (try detectArchitecture(allocator, model_dir, mf)) {
        .gpt => |parsed| blk: {
            var config = parsed;
            try refineGptConfigFromManifestTensorMetadata(allocator, mf, &config);
            break :blk config;
        },
        else => error.InvalidModelForGeneration,
    };
}

/// Load GPT architecture metadata without retaining a runtime session or
/// reading tensor payloads. Gemma 4 checkpoints omit some structural
/// dimensions from config.json, so SafeTensors/GGUF headers are still used to
/// refine the graph config before a strict training session is admitted.
pub fn loadGptConfigMetadataFromModelDir(
    allocator: std.mem.Allocator,
    model_dir: []const u8,
) !gpt_mod.Config {
    var mf = try manifest_mod.loadListingFromDir(allocator, model_dir);
    defer mf.deinit();

    const arch_config = if (mf.usesGgufWeights()) blk: {
        const gguf_path = mf.gguf_path orelse return error.MissingModelWeights;
        var mapped = try c_file.MmapRegion.init(allocator, gguf_path);
        defer mapped.deinit();

        var file = try gguf_mod.format.parseStructure(allocator, mapped.data);
        defer file.deinit(allocator);
        try gguf_mod.format.validateTensorDataRanges(&file, mapped.data.len);
        const parsed_prefix_len = std.math.cast(usize, file.data_region_offset) orelse mapped.data.len;
        mapped.adviseSequentialPrefix(@min(parsed_prefix_len, mapped.data.len));
        break :blk try detectArchitectureWithGgufFile(allocator, model_dir, mf, &file);
    } else try detectArchitecture(allocator, model_dir, mf);

    return switch (arch_config) {
        .gpt => |parsed| blk: {
            var config = parsed;
            try refineGptConfigFromManifestTensorMetadata(allocator, mf, &config);
            break :blk config;
        },
        else => error.UnsupportedModelArchitecture,
    };
}

fn refineGptConfigFromManifestTensorMetadata(
    allocator: std.mem.Allocator,
    mf: manifest_mod.ModelManifest,
    config: *gpt_mod.Config,
) !void {
    if (mf.usesGgufWeights()) return;
    if (mf.safetensors_path == null and mf.safetensors_index_path == null) return;

    var store = try tensor_store_mod.openFromManifest(allocator, mf);
    defer store.deinit();
    const source = (try store.weightSource()) orelse return error.NoDenseWeightSource;
    const all_names = try source.listNames(allocator);
    defer allocator.free(all_names);
    try refineGptConfigFromStore(allocator, store, all_names, config);
}

/// Arithmetic policy is frozen with the session identity, never re-read by queues.
pub fn getEmbeddingGemma2CudaPolicy(session: Session) ?@import("../ops/cuda/embedding_gemma2_policy.zig").Policy {
    if (comptime !build_options.enable_cuda) return null;
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .cuda or self.arch_config != .embedding_gemma2) return null;
    return self.backend_data.cuda.compute.embedding_gemma2_policy;
}

pub fn getCudaRuntimeStats(session: Session) ?CudaRuntimeStats {
    if (comptime !build_options.enable_cuda) return null;
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.backend_type) {
        .cuda => self.backend_data.cuda.compute.snapshotStats(),
        else => null,
    };
}

pub const CudaAllocationStats = struct {
    limit: usize,
    live: usize,
    peak: usize,
};

/// Context-owned physical buffers; caller holds the session execution lock.
/// Driver/library-private allocations are outside this lifetime peak meter.
pub fn getCudaAllocationStats(session: Session) ?CudaAllocationStats {
    if (comptime !build_options.enable_cuda) return null;
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .cuda) return null;
    const value = self.backend_data.cuda.compute.allocationStats();
    return .{ .limit = value.limit, .live = value.live, .peak = value.peak };
}

/// Drain any buffered non-blocking CUDA op-profile events into the compute's
/// stat counters. Call this after a request finishes (before reading stats) so
/// the profiler's single host sync happens off the hot path. No-op when the
/// session is not CUDA-backed or profiling is disabled.
pub fn drainCudaProfile(session: Session) void {
    if (comptime !build_options.enable_cuda) return;
    if (session.vtable != &arch_vtable) return;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    switch (self.backend_type) {
        .cuda => cuda_compute_mod.drainCudaProfile(self.backend_data.cuda.compute),
        else => {},
    }
}

/// Shared formatter for the `cuda_prefill_profile_us:` telemetry line so the CLI
/// and the HTTP server emit byte-identical output. Formats into `buf` (768 bytes
/// is ample) and returns the written slice, WITHOUT a trailing newline.
pub fn formatCudaPrefillProfileLine(buf: []u8, stats: CudaRuntimeStats) []const u8 {
    if (comptime build_options.enable_cuda) {
        return std.fmt.bufPrint(
            buf,
            "cuda_prefill_profile_us: events={d} q4_linear={d} q4_qkv={d} q4_pair={d} q4_gated_down={d} bf16_linear={d} f16_linear={d} bf16_qkv={d} bf16_pair={d} attention={d} ple_dense={d} staging={d} norm={d} rope={d} kv_write={d} elementwise={d} embedding={d}",
            .{
                stats.prefill_profile_events,
                stats.prefill_profile_q4_linear_us,
                stats.prefill_profile_q4_qkv_us,
                stats.prefill_profile_q4_pair_us,
                stats.prefill_profile_q4_gated_down_us,
                stats.prefill_profile_bf16_linear_us,
                stats.prefill_profile_f16_linear_us,
                stats.prefill_profile_bf16_qkv_us,
                stats.prefill_profile_bf16_pair_us,
                stats.prefill_profile_attention_us,
                stats.prefill_profile_ple_dense_us,
                stats.prefill_profile_staging_us,
                stats.prefill_profile_norm_us,
                stats.prefill_profile_rope_us,
                stats.prefill_profile_kv_write_us,
                stats.prefill_profile_elementwise_us,
                stats.prefill_profile_embedding_us,
            },
        ) catch buf[0..0];
    }
    return buf[0..0];
}

/// True when either CUDA op-profile mode is enabled via env, i.e. there is a
/// profile line worth draining and emitting.
pub fn cudaOpProfileLoggingEnabled() bool {
    if (comptime !build_options.enable_cuda) return false;
    return cuda_compute_mod.cudaPrefillOpProfileEnabled() or cuda_compute_mod.cudaDecodeProfileEnabled();
}

/// Saturating per-field delta of two CUDA runtime-stat snapshots. Shared by the
/// CLI and the server so a request's profile line reflects that request alone.
pub fn cudaStatsDelta(after: CudaRuntimeStats, before: CudaRuntimeStats) CudaRuntimeStats {
    if (comptime !build_options.enable_cuda) return after;
    var delta = after;
    const info = @typeInfo(CudaRuntimeStats).@"struct";
    inline for (info.field_names, info.field_types) |reflected_name, Field| {
        switch (@typeInfo(Field)) {
            .int => @field(delta, reflected_name) = @field(after, reflected_name) -| @field(before, reflected_name),
            else => {},
        }
    }
    return delta;
}

/// Shared formatter for the `cuda_decode_profile_us:` telemetry line (see
/// formatCudaPrefillProfileLine). Returns the written slice, no trailing newline.
pub fn formatCudaDecodeProfileLine(buf: []u8, stats: CudaRuntimeStats) []const u8 {
    if (comptime build_options.enable_cuda) {
        return std.fmt.bufPrint(
            buf,
            "cuda_decode_profile_us: events={d} qkv={d} gqa_attention={d} attention_output={d} attention_norm_residual={d} ffn_gate_up={d} ffn_gated_down={d} ffn_post_norm={d} lm_head_argmax={d} graph_replay={d}",
            .{
                stats.decode_profile_events,
                stats.decode_profile_qkv_us,
                stats.decode_profile_gqa_attention_us,
                stats.decode_profile_attention_output_us,
                stats.decode_profile_attention_norm_residual_us,
                stats.decode_profile_ffn_gate_up_us,
                stats.decode_profile_ffn_gated_down_us,
                stats.decode_profile_ffn_post_norm_us,
                stats.decode_profile_lm_head_argmax_us,
                stats.decode_profile_graph_replay_us,
            },
        ) catch buf[0..0];
    }
    return buf[0..0];
}

pub const MetalExactJitDispatchStats = struct {
    q4_0_hits: u64 = 0,
    q4_k_hits: u64 = 0,

    pub fn add(self: *MetalExactJitDispatchStats, other: MetalExactJitDispatchStats) void {
        self.q4_0_hits +|= other.q4_0_hits;
        self.q4_k_hits +|= other.q4_k_hits;
    }
};

pub fn getMetalExactJitDispatchStats(session: Session) ?MetalExactJitDispatchStats {
    if (comptime !build_options.enable_metal) return null;
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .metal) return null;
    const provider = gpuBackendData(self).shared_metal_native_provider orelse return null;
    const stats = metal_runtime.exactJitDispatchStatsSnapshot(provider.raw_decode_runtime) catch return null;
    return .{
        .q4_0_hits = stats.q4_0_hits,
        .q4_k_hits = stats.q4_k_hits,
    };
}

pub fn recommendedKvDTypeForGptConfig(config: gpt_mod.Config, backend_kind: runtime.kv.pool.BackendKind) runtime.kv.pool.KvDType {
    return switch (backend_kind) {
        .native => .f32,
        .cuda => if (config.family == .gemma and !isGemma4KvArchitecture(config)) .f32 else .f16,
        .metal => if (config.family == .gemma) metalGemmaKvDTypeOverride() orelse .f16 else .f16,
    };
}

fn isGemma4KvArchitecture(config: gpt_mod.Config) bool {
    if (config.family != .gemma) return false;
    return config.num_kv_shared_layers > 0 or
        config.global_head_dim > 0 or
        config.ple_hidden_size > 0 or
        config.gemma4_mtp_assistant or
        config.attention_k_eq_v or
        config.rope_dim_override > 0;
}

pub fn recommendedKvDTypeForSession(session: Session, backend_kind: runtime.kv.pool.BackendKind) runtime.kv.pool.KvDType {
    if (getGptConfig(session)) |cfg| return recommendedKvDTypeForGptConfig(cfg, backend_kind);
    return switch (backend_kind) {
        .native => .f32,
        .cuda, .metal => .f16,
    };
}

test "parseMetalGemmaKvDTypeOverride only accepts staged Gemma Metal dtypes" {
    try std.testing.expectEqual(runtime.kv.pool.KvDType.f16, parseMetalGemmaKvDTypeOverride("f16").?);
    try std.testing.expectEqual(runtime.kv.pool.KvDType.f32, parseMetalGemmaKvDTypeOverride("F32").?);
    try std.testing.expect(parseMetalGemmaKvDTypeOverride("int8") == null);
}

test "recommendedKvDTypeForGptConfig keeps backend defaults without a session" {
    const gemma = gpt_mod.Config{ .family = .gemma };
    const gemma4 = gpt_mod.Config{ .family = .gemma, .num_kv_shared_layers = 20 };
    const llama = gpt_mod.Config{ .family = .llama };
    try std.testing.expectEqual(runtime.kv.pool.KvDType.f32, recommendedKvDTypeForGptConfig(gemma, .native));
    try std.testing.expectEqual(runtime.kv.pool.KvDType.f32, recommendedKvDTypeForGptConfig(gemma, .cuda));
    try std.testing.expectEqual(runtime.kv.pool.KvDType.f16, recommendedKvDTypeForGptConfig(gemma4, .cuda));
    try std.testing.expectEqual(runtime.kv.pool.KvDType.f16, recommendedKvDTypeForGptConfig(llama, .metal));
}

pub fn getClipConfig(session: Session) ?clip_mod.Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .clip => |cfg| cfg,
        else => null,
    };
}

pub fn getClapConfig(session: Session) ?clap_mod.Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .clap => |cfg| cfg,
        else => null,
    };
}

pub fn getWhisperConfig(session: Session) ?whisper_mod.Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .whisper => |cfg| cfg,
        else => null,
    };
}

pub fn getFlorenceConfig(session: Session) ?florence_mod.Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .florence => |cfg| cfg,
        else => null,
    };
}

pub fn runFlorenceEncoderResident(
    session: Session,
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    pixel_values: []const f32,
    batch: usize,
    prompt_input_ids: []const i64,
    prompt_seq_len: usize,
) !?florence_arch.EncoderForwardTensorResult {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .florence => |cfg| try florence_arch.encoderForwardTensor(
            cb,
            allocator,
            cfg,
            pixel_values,
            batch,
            prompt_input_ids,
            prompt_seq_len,
        ),
        else => null,
    };
}

pub fn runFlorenceDecoderResident(
    session: Session,
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    decoder_input_ids: []const i64,
    encoder_hidden: ops.CT,
    encoder_mask: []const i64,
    batch: usize,
    dec_seq: usize,
    enc_seq: usize,
) !?ops.CT {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .florence => |cfg| try florence_arch.decoderForwardTensor(
            cb,
            allocator,
            cfg,
            decoder_input_ids,
            encoder_hidden,
            encoder_mask,
            batch,
            dec_seq,
            enc_seq,
        ),
        else => null,
    };
}

/// Get a ComputeBackend from an architecture session.
pub fn getComputeBackend(session: Session, allocator: std.mem.Allocator) !ops.ComputeBackend {
    if (session.compute_backend_provider != null) return session.acquireComputeBackend(allocator, .{});
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    var cb = try makeComputeBackend(self, allocator, null);
    errdefer cb.deinit();
    try @import("../backends/compute_factory.zig").beginRequest(session, &cb);
    return cb;
}

pub fn getEmbeddingGemma2Config(session: Session) ?embedding_gemma2_arch.Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return if (self.arch_config == .embedding_gemma2) self.arch_config.embedding_gemma2 else null;
}

/// Called only after a relevant admission denial. The model/session remains
/// retained, and native destruction has the same interruption protection as
/// compute construction. Busy providers keep their weights and return zero.
pub fn reclaimEmbeddingGemma2VisionWeights(session: Session, control: InferenceExecutionControl) !usize {
    const coefficients = try reclaimVideoPreparationCoefficients(session, control);
    return coefficients + try reclaimEmbeddingGemma2Weights(session, control, .vision);
}

pub fn reclaimVideoPreparationCoefficients(session: Session, control: InferenceExecutionControl) !usize {
    if (session.vtable != &arch_vtable) return 0;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    const io = self.io orelse return 0;
    try control.check();
    self.video_coefficient_mutex.lockUncancelable(io);
    defer self.video_coefficient_mutex.unlock(io);
    if (self.video_coefficients) |*cache| {
        if (self.video_coefficient_suspended) return 0;
        // The native gate prevents racing producers from repopulating storage
        // after its device lease is returned. Request pins remain admitted.
        cache.disable();
        if (self.video_coefficient_permit) |*permit| try permit.retainHostBytes(@import("antfly_video").preparation.CoefficientCache.host_budget);
        if (self.video_coefficient_device_permit) |*permit| permit.deinit();
        self.video_coefficient_device_permit = null;
        self.video_coefficient_suspended = true;
        return @import("antfly_video").preparation.CoefficientCache.device_budget;
    }
    return 0;
}

pub fn reclaimEmbeddingGemma2AudioWeights(session: Session, control: InferenceExecutionControl) !usize {
    return reclaimEmbeddingGemma2Weights(session, control, .audio);
}

pub fn reclaimEmbeddingGemma2LanguageWeights(session: Session, control: InferenceExecutionControl) !usize {
    return reclaimEmbeddingGemma2Weights(session, control, .language);
}

fn reclaimEmbeddingGemma2Weights(session: Session, control: InferenceExecutionControl, cache_class: @import("../models/embedding_gemma2_weight_cache.zig").MetalClass) !usize {
    if (session.vtable != &arch_vtable) return 0;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.arch_config != .embedding_gemma2) return 0;
    try control.check();
    if (self.backend_type == .native) return native_mod.reclaimEmbeddingGemma2Weights(&self.backend_data.native, cache_class);
    if (comptime !build_options.enable_metal) return 0;
    if (self.backend_type != .metal) return 0;
    try control.check();
    var guard = try control.enterUninterruptible(session.interruption());
    defer guard.deinit();
    return @import("../ops/metal_compute.zig").reclaimEmbeddingGemma2Weights(gpuBackendData(self), self.io, cache_class);
}

/// Preparation owns its own queue and never borrows mutable provider execution
/// state. The loaded session handle must remain alive through producer cleanup;
/// callers guard native driver work before querying/retaining this device.
pub fn videoPreparationContext(session: Session, prepare: bool) !struct { io: ?std.Io, device: ?*anyopaque, coefficients: ?*const @import("antfly_video").preparation.CoefficientCache = null } {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .metal) return .{ .io = self.io, .device = null };
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    const device = metal_runtime.termite_metal_video_preparation_device() orelse return error.MetalRuntimeUnavailable;
    if (self.io) |io| {
        self.video_coefficient_mutex.lockUncancelable(io);
        defer self.video_coefficient_mutex.unlock(io);
        if (self.video_coefficients == null and prepare) {
            const Cache = @import("antfly_video").preparation.CoefficientCache;
            var permit = session.admitRetainedMedia(Cache.host_budget, Cache.device_budget) catch return .{ .io = self.io, .device = device };
            const cache = Cache.init(device, Cache.device_budget) catch {
                permit.deinit();
                return .{ .io = self.io, .device = device };
            };
            self.video_coefficients = cache;
            self.video_coefficient_permit = permit;
        }
        if (self.video_coefficients != null and self.video_coefficient_suspended and prepare) {
            const Cache = @import("antfly_video").preparation.CoefficientCache;
            var permit = session.admitRetainedMedia(0, Cache.device_budget) catch return .{ .io = self.io, .device = device };
            self.video_coefficients.?.enable(Cache.device_budget) catch {
                permit.deinit();
                return .{ .io = self.io, .device = device };
            };
            self.video_coefficient_device_permit = permit;
            self.video_coefficient_suspended = false;
        }
        return .{ .io = self.io, .device = device, .coefficients = if (self.video_coefficients) |*cache| cache else null };
    }
    return .{ .io = self.io, .device = device };
}

/// Incremental native Whisper decoder bound to one session for the duration
/// of one transcription. Holds the compute backend (and on shared GPU
/// backends its execution lease) and the session's execution gate, so it
/// serializes with other users of the model exactly like `Session.run`.
pub const WhisperNativeDecoder = struct {
    allocator: std.mem.Allocator,
    cb: ManagedComputeBackend,
    config: whisper_mod.Config,
    encoder_hidden: ops.CT,
    cache: whisper_arch.DecodeCache,
    gate: ?*std.atomic.Mutex,
    encoder_owned: bool = true,
    source_resources: ?Session.ModelLifetime.ResourcePin = null,
    receiver_cleanup: ?ReceiverCleanup = null,
    destination_model: ?*Session.ModelLifetime = null,
    base_permit: ?@import("../backends/session.zig").RunPermit = null,
    cache_permit: ?@import("../backends/session.zig").RunPermit = null,
    diagnostics_binding: ?incremental_observation.Binding = null,
    pending_observation: @import("pending_observation.zig").Span = .{},
    fn ensurePendingObservation(self: *WhisperNativeDecoder) void {
        if (self.pending_observation.source != null) return;
        const binding = self.diagnostics_binding orelse return;
        if (!binding.diagnostics.isEnabled()) return;
        const model = self.destination_model orelse return;
        self.pending_observation = @import("pending_observation.zig").Span.begin(.{ .publisher = binding.diagnostics, .model = model, .model_allocator = binding.model_allocator });
    }
    fn classifyPipelinedStep(result: whisper_arch.StepResult) incremental_observation.ResultClassification {
        return .{ .unsupported = result != .encoded };
    }
    fn discardPending(backend: *const ops.ComputeBackend) !bool {
        if (!backend.decoderRuntimeHasActiveFrame()) return false;
        try backend.decoderRuntimeCancelFrame();
        return true;
    }

    fn invocationBinding(self: *WhisperNativeDecoder) ?incremental_observation.Binding {
        var binding = self.diagnostics_binding;
        if (binding) |*value| value.* = value.forBackend(self.cb.backend);
        return binding;
    }

    /// Logits (`[vocab_size]` f32) for the last of `tokens`, which must be
    /// the tokens not yet decoded (`tokens.len >= 1`).
    pub fn step(self: *WhisperNativeDecoder, tokens: []const i64) ![]f32 {
        const operation: @import("diagnostics.zig").Operation = if (self.cache.positions == 0 or tokens.len > 1) .prefill else .decode_step;
        return incremental_observation.callOperation(self.invocationBinding(), operation, stepImpl, .{ self, tokens });
    }

    fn stepImpl(self: *WhisperNativeDecoder, tokens: []const i64) ![]f32 {
        if (self.cb.backend.execution_control) |control| try control.check();
        return whisper_arch.decoderStepCachedLogits(&self.cb.backend, self.allocator, self.config, tokens, &self.cache);
    }

    /// Like `step`, choosing what the last token yields: the logits row, or
    /// the device-side token statistics (which fall back to logits when the
    /// backend cannot produce them).
    pub fn stepWith(self: *WhisperNativeDecoder, tokens: []const i64, output: whisper_arch.StepOutput) !whisper_arch.StepResult {
        const operation: @import("diagnostics.zig").Operation = if (self.cache.positions == 0 or tokens.len > 1) .prefill else .decode_step;
        if (output == .pipelined) {
            self.ensurePendingObservation();
            return self.pending_observation.callOperationClassified(&self.cb.backend, operation, .encode_next, .unavailable, classifyPipelinedStep, stepWithImpl, .{ self, tokens, output });
        }
        return incremental_observation.callOperation(self.invocationBinding(), operation, stepWithImpl, .{ self, tokens, output });
    }

    fn stepWithImpl(self: *WhisperNativeDecoder, tokens: []const i64, output: whisper_arch.StepOutput) !whisper_arch.StepResult {
        if (self.cb.backend.execution_control) |control| try control.check();
        return whisper_arch.decoderStepCached(&self.cb.backend, self.allocator, self.config, tokens, &self.cache, output);
    }

    /// Enter or leave pipelined decoding (`.pipelined` steps); false when
    /// the backend cannot keep a step in flight.
    pub fn setPipelined(self: *WhisperNativeDecoder, enabled: bool) bool {
        return whisper_arch.setPipelinedDecode(&self.cb.backend, &self.cache, enabled);
    }

    /// Seed the device timestamp-grammar state before pipelined steps.
    pub fn seedGrammar(self: *WhisperNativeDecoder, state: *const ops.WhisperGrammarState) bool {
        return self.cb.backend.whisperGrammarWrite(state);
    }

    /// Submit the step the last `.pipelined` call encoded.
    pub fn submit(self: *WhisperNativeDecoder) !void {
        self.ensurePendingObservation();
        return self.pending_observation.call(&self.cb.backend, .submit, .unavailable, whisper_arch.decoderStepSubmit, .{&self.cb.backend});
    }

    /// Drop the step the last `.pipelined` call encoded without running it.
    pub fn discard(self: *WhisperNativeDecoder) void {
        self.ensurePendingObservation();
        _ = self.pending_observation.call(&self.cb.backend, .cancel, .unavailable, discardPending, .{&self.cb.backend}) catch {};
    }

    /// Collect the statistics of the step in flight from `slot`.
    pub fn awaitStats(self: *WhisperNativeDecoder, slot: usize) !?ops.WhisperLogitsStatsRaw {
        self.ensurePendingObservation();
        return self.pending_observation.call(&self.cb.backend, .await_readback, .readback_returned, whisper_arch.decoderStepAwait, .{ &self.cb.backend, slot });
    }

    /// Wait for any step still in flight and drop any step still encoded;
    /// safe to call when there is neither.
    fn retirePending(backend: *const ops.ComputeBackend) !void {
        try backend.decoderRuntimeWaitSubmittedFrame();
        if (backend.decoderRuntimeHasActiveFrame()) try backend.decoderRuntimeCancelFrame();
    }

    pub fn drain(self: *WhisperNativeDecoder) void {
        // A failed fence cannot authorize freeing cache/provider addresses.
        // Preserve process fail-closed policy instead of unlocking/replaying.
        self.pending_observation.call(&self.cb.backend, .drain, .unavailable, retirePending, .{&self.cb.backend}) catch platform.inference_process_supervisor.restartWorker();
    }

    pub fn positions(self: *const WhisperNativeDecoder) usize {
        return self.cache.positions;
    }

    pub fn deinit(self: *WhisperNativeDecoder) void {
        var cleanup = self.receiver_cleanup;
        self.receiver_cleanup = null;
        // Published constructors always own a cold B. A manual raw decoder
        // may use the historical raw destructor only with no tracked gate or
        // retained source whose cleanup could run under another owner's lane.
        if (cleanup == null and (self.gate != null or self.source_resources != null))
            platform.inference_process_supervisor.restartWorker();
        defer if (cleanup) |*owner| owner.finish();
        const boundary = if (cleanup) |*owner| owner.boundary() else null;
        const destination_model = self.destination_model;
        const source = self.source_resources;
        self.source_resources = null;
        var scope = if (destination_model) |model| model.beginClose() else Session.CloseScope{};
        defer scope.deinit();
        self.drain();
        self.cache.deinit(&self.cb.backend);
        if (self.encoder_owned) self.cb.backend.freeWithBoundary(self.encoder_hidden, boundary);
        if (cleanup != null) self.cb.deinitWithCleanup(.{ .boundary = boundary }) else self.cb.deinit();
        self.pending_observation.deinit();
        // Source SDK destruction remains protected while destination owns its
        // gate; every source quota continuation receives this exact current B.
        if (source) |resources| resources.releaseWithBoundary(boundary);
        if (self.gate) |gate| gate.unlock();
        self.gate = null;
        var cache_permit = self.cache_permit;
        var base_permit = self.base_permit;
        self.cache_permit = null;
        self.base_permit = null;
        scope.deinit();
        self.* = undefined;
        // Producers see all concrete SDK/provider/model and close scopes ended.
        if (cache_permit) |*permit| permit.deinitWithCleanup(.{ .boundary = boundary });
        if (base_permit) |*permit| permit.deinitWithCleanup(.{ .boundary = boundary });
        if (destination_model) |model| model.releaseWithBoundary(boundary);
    }
};

/// Compile-time native incremental implementation. Concrete config/weight
/// semantics stay here; staged runtimes consume the operation contract only.
pub const IncrementalDecoderAdapter = struct {
    pub const State = WhisperNativeDecoder;
    pub const StatsRequest = whisper_arch.StatsRequest;
    pub const Source = RetainedEncoderSource;
    pub const Receiver = struct {
        source_ptr: *anyopaque,
        source_vtable: *const Session.VTable,
        destination_ptr: *anyopaque,
        destination_vtable: *const Session.VTable,
        backend: BackendType,
        width: usize,
        same_model_owner: bool,
        /// Only the ModelLifetime-owned lane can outlive a model handle. The
        /// complete RetainedEncoderSource.resources pins this exact lifetime.
        source_owned_lane: ?*std.atomic.Mutex = null,
    };

    /// No driver/provider construction, cache allocation, or model submission.
    pub fn qualify(source: Session, destination: Session) ?Receiver {
        if (source.vtable != &arch_vtable or destination.vtable != &arch_vtable) return null;
        const src: *ArchSession = @ptrCast(@alignCast(source.ptr));
        const dst: *ArchSession = @ptrCast(@alignCast(destination.ptr));
        if (src.arch_config != .whisper or dst.arch_config != .whisper or src.backend_type != dst.backend_type) return null;
        if (src.task == .classifier or src.task == .extractor or dst.task == .classifier or dst.task == .extractor) return null;
        if (src.arch_config.whisper.d_model != dst.arch_config.whisper.d_model) return null;
        switch (src.backend_type) {
            .native => {},
            .metal => if (comptime build_options.enable_metal) {
                // Native Metal architecture providers use the one system device.
                // Distinct stores may not acquire the same already-leased provider.
                const src_provider = gpuBackendData(src).shared_metal_native_provider;
                const dst_provider = gpuBackendData(dst).shared_metal_native_provider;
                if (src != dst and src_provider != null and src_provider == dst_provider) return null;
            } else return null,
            .cuda => if (comptime build_options.enable_cuda) {
                const source_context = &src.backend_data.cuda.compute.ctx;
                const target_context = &dst.backend_data.cuda.compute.ctx;
                if (source_context.ctx == null or source_context.ctx != target_context.ctx or source_context.device != target_context.device) return null;
            } else return null,
            else => return null,
        }
        var source_owned_lane: ?*std.atomic.Mutex = null;
        if (src != dst and src.backend_type == .cuda) if (source.execution_gate) |gate| {
            const lifetime = source.model_lifetime orelse return null;
            if (gate != &lifetime.execution_gate) return null;
            source_owned_lane = gate;
        };
        return .{ .source_ptr = source.ptr, .source_vtable = source.vtable, .destination_ptr = destination.ptr, .destination_vtable = destination.vtable, .backend = src.backend_type, .width = dst.arch_config.whisper.d_model, .same_model_owner = src == dst, .source_owned_lane = source_owned_lane };
    }

    pub fn fromHost(session: Session, allocator: std.mem.Allocator, control: ?InferenceExecutionControl, hidden: []const f32, rows: usize) !?State {
        return whisperNativeDecoder(session, allocator, control, hidden, rows);
    }

    /// Consumes the independent source capability on success and failure.
    pub fn fromResident(session: Session, allocator: std.mem.Allocator, control: ?InferenceExecutionControl, source: Source) !State {
        const held = if (session.model_lifetime) |model| model.retain() else null;
        defer if (held) |model| model.release();
        return createResidentIncrementalDecoder(session, allocator, control, source);
    }
};

pub const RetainedEncoderSource = struct {
    receiver: IncrementalDecoderAdapter.Receiver,
    value: ops.CT,
    backend: *const ops.ComputeBackend,
    shape: [3]i64,
    resources: Session.ModelLifetime.ResourcePin,
    cleanup_root_qualified: bool = false,
    cleanup_root_deadline_ns: ?u64 = null,
    cleanup_root_background_supervised: bool = false,
    pub fn payloadCharge(self: RetainedEncoderSource) ?@import("payload_charge.zig").Capability {
        if (!self.cleanup_root_qualified) return null;
        var capability = self.resources.payloadCharge() orelse return null;
        capability.final_cleanup_qualified = true;
        capability.root_cleanup_deadline_ns = self.cleanup_root_deadline_ns;
        capability.background_supervised = self.cleanup_root_background_supervised;
        return capability;
    }

    pub fn retained(self: RetainedEncoderSource) !RetainedEncoderSource {
        var result = self;
        result.resources = self.resources.retain() orelse return error.UnretainableResidentResources;
        return result;
    }
    pub fn deinit(self: *RetainedEncoderSource) void {
        // Historical raw destructor declares outside-execution cleanup.
        // A caller under another lane must use the explicit boundary method.
        self.resources.release();
        self.* = undefined;
    }
    pub fn deinitWithBoundary(self: *RetainedEncoderSource, boundary: ?*const @import("retirement_sink.zig").Epoch) void {
        self.resources.releaseWithBoundary(boundary);
        self.* = undefined;
    }
};

fn incrementalBasePermit(session: Session, config: whisper_mod.Config, geometry: whisper_arch.CacheGeometry, resident_input_held: bool, independent_device_copy: bool) !@import("../backends/session.zig").RunPermit {
    const hidden_bytes = std.math.mul(usize, geometry.encoder_rows, std.math.mul(usize, geometry.width, @sizeOf(f32)) catch return error.ResourceLimitExceeded) catch return error.ResourceLimitExceeded;
    const output_bytes = std.math.mul(usize, config.vocab_size, @sizeOf(f32)) catch return error.ResourceLimitExceeded;
    // CUDA row statistics retain no device workspace after encode. Bound the
    // actual supported maximum ids/partials/result before the model gate;
    // existing CPU/Metal budgets and schedules remain unchanged.
    const selection_workspace: usize = if (session.backend() == .cuda)
        (try @import("../ops/row_selection_stats.zig").Geometry.init(config.vocab_size, config.vocab_size)).workspace_bytes
    else
        0;
    const workspace = std.math.add(usize, try geometry.workspaceBytes(config), selection_workspace) catch return error.ResourceLimitExceeded;
    return session.admit(.{
        .batch = 1,
        .sequence = 1,
        .input_bytes = if (resident_input_held) 0 else hidden_bytes,
        .backend_input_bytes = if (resident_input_held and !independent_device_copy) 0 else hidden_bytes,
        .host_preprocess_bytes = if (resident_input_held) 0 else hidden_bytes,
        .output_bytes = output_bytes,
        .workspace_bytes = workspace,
        // Preserve static/profile/source-route floors. Cache geometry does not
        // pretend to prove opaque quantized or cached-panel staging coverage.
        .workspace_planned = false,
    });
}

fn incrementalCachePermit(session: Session, base: *const @import("../backends/session.zig").RunPermit, geometry: whisper_arch.CacheGeometry, cb: *const ops.ComputeBackend) !?@import("../backends/session.zig").RunPermit {
    const lease = base.lease orelse return null;
    const target = try lease.amounts.merge(try geometry.cacheAmounts(cb));
    return try session.admitSupplement(base, target);
}

fn createResidentIncrementalDecoder(session: Session, allocator: std.mem.Allocator, control: ?InferenceExecutionControl, source: RetainedEncoderSource) !WhisperNativeDecoder {
    var cleanup: ?ReceiverCleanup = null;
    var transferred = false;
    defer if (!transferred) if (cleanup) |*owner| owner.finish();
    var destination_model: ?*Session.ModelLifetime = null;
    defer if (!transferred) {
        if (cleanup) |*owner| source.resources.releaseWithBoundary(owner.boundary()) else source.resources.release();
        if (destination_model) |model| if (cleanup) |*owner| model.releaseWithBoundary(owner.boundary()) else model.release();
    };
    if (session.vtable != &arch_vtable or session.ptr != source.receiver.destination_ptr or session.vtable != source.receiver.destination_vtable) return error.InvalidIncrementalReceiver;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.arch_config != .whisper) return error.InvalidIncrementalReceiver;
    const config = self.arch_config.whisper;
    const expected_backend: ops.BackendKind = switch (self.backend_type) {
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        else => return error.InvalidIncrementalReceiverStorage,
    };
    if (source.backend.kind() != expected_backend or source.receiver.backend != self.backend_type or source.receiver.width != config.d_model) return error.InvalidIncrementalReceiverStorage;
    const same_owner = source.receiver.source_ptr == session.ptr and source.receiver.source_vtable == session.vtable;
    if (source.receiver.same_model_owner != same_owner) return error.InvalidIncrementalReceiver;
    if (source.shape[0] != 1 or source.shape[1] <= 0 or source.shape[2] != config.d_model) return error.InvalidInputShape;
    const rows = std.math.cast(usize, source.shape[1]) orelse return error.ResourceLimitExceeded;
    destination_model = if (session.model_lifetime) |model| model.retain() else null;
    const geometry = try whisper_arch.CacheGeometry.init(config, rows);
    const foreign_cuda = source.receiver.backend == .cuda and !source.receiver.same_model_owner;
    const foreign_async_eligible = foreign_cuda and ReceiverCleanup.permitsAsyncSource(session, source.payloadCharge());
    cleanup = try ReceiverCleanup.beginForOwnedGate(session);
    var base = try incrementalBasePermit(session, config, geometry, true, foreign_cuda);
    errdefer base.deinit();
    // Source authority precedes BOTH the destination lane and its older
    // compute/provider acquisition path. Raw sessions with no published lane
    // preserve their existing caller-guaranteed exclusive-use contract.
    var funding: ?@import("payload_charge.zig").ReceiverFunding = null;
    defer if (funding) |*owned| owned.abort();
    if (foreign_async_eligible) {
        const lease = if (base.lease) |*value| value else null;
        const domain = if (destination_model) |model| if (lease) |owned| if (owned.controller) |controller| model.retainAdmissionDomain(controller) else null else null else null;
        const bytes = std.math.mul(usize, geometry.encoder_rows, std.math.mul(usize, geometry.width, 4) catch return error.ResourceLimitExceeded) catch return error.ResourceLimitExceeded;
        funding = try @import("payload_charge.zig").ReceiverFunding.prepare(source.payloadCharge(), source.value, lease, domain, bytes);
    }
    const ForeignCopy = if (build_options.enable_cuda) cuda_compute_mod.PreparedForeignTensorCopy else void;
    var foreign_copy: ?ForeignCopy = null;
    defer if (comptime build_options.enable_cuda) if (foreign_copy) |*copy| copy.deinitWithBoundary(if (cleanup) |*owner| owner.boundary() else null);
    if (foreign_cuda) {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        const target_backend = self.backend_data.cuda.compute.computeBackend();
        const target_identity = cuda_compute_mod.foreignTensorCopyTarget(&target_backend) orelse return error.InvalidIncrementalReceiverStorage;
        const source_identity = cuda_compute_mod.foreignTensorCopyTarget(source.backend) orelse return error.InvalidIncrementalReceiverStorage;
        if (!source_identity.matches(target_identity)) return error.InvalidIncrementalReceiverStorage;
        var source_lane_guard = @import("source_lane.zig").Guard{};
        if (source.receiver.source_owned_lane) |lane| {
            try @import("source_lane.zig").Guard.acquireInto(lane, .{ .context = source.backend.ptr, .table = source.backend.vtable }, control, &source_lane_guard);
        }
        defer source_lane_guard.finish();
        var source_sdk_guard: @import("../execution_control.zig").UninterruptibleGuard = if (control) |active| try active.enterUninterruptible(.uninterruptible) else .{};
        defer source_sdk_guard.deinit();
        var source_scope = @import("backend_operation.zig").Scope{};
        try @import("backend_operation.zig").enterInto(source.backend, null, source.backend.operationToken(), .legacy_unknown_frame, &source_scope);
        defer source_scope.deinitChecked() catch platform.inference_process_supervisor.restartWorker();
        if (control) |active| try active.check();
        const cleanup_deadline = if (control) |active| if (active.deadline_ns) |deadline| deadline +| (active.cancellation_grace_ns orelse 0) else null else null;
        const source_lane_proof = if (source.receiver.source_owned_lane != null) try source_lane_guard.borrow() else null;
        if (source_lane_proof) |proof| try proof.validate(.{ .context = source.backend.ptr, .table = source.backend.vtable });
        foreign_copy = (if (foreign_async_eligible and source_lane_proof != null and (if (control) |active| active.native_call_policy == .require_supervision and cleanup_deadline != null else false)) try cuda_compute_mod.prepareAsyncForeignTensorCopy(source.backend, source.value, target_identity, if (funding) |*owned| owned else null, if (cleanup) |*owner| owner.boundary() else null, cleanup_deadline) else null) orelse try cuda_compute_mod.prepareForeignTensorCopy(source.backend, source.value);
        try source_scope.finish();
    }
    // Context-retain/release callbacks are metadata work, not source SDK work.
    // Never abort an unused token plan under the destination's later gate.
    if (funding) |*owned| owned.abort();
    const gate = session.execution_gate;
    if (gate) |mutex| {
        if (control) |active| try active.lock(mutex) else while (!mutex.tryLock()) std.atomic.spinLoopHint();
    }
    errdefer if (gate) |mutex| mutex.unlock();
    const binding = session.invocation_diagnostics;
    const decoder = try incremental_observation.callOperation(binding, .decoder_init, finishResidentIncrementalDecoder, .{ session, allocator, control, source, config, rows, geometry, base, gate, destination_model, if (foreign_copy) |*copy| copy else null, if (cleanup) |*owner| owner else null });
    transferred = true;
    return decoder;
}

fn acquireIncrementalSourceProvider(session: Session, allocator: std.mem.Allocator, options: @import("../backends/session.zig").ComputeBackendOptions) !ops.ComputeBackend {
    if (options.run_budget != null) return error.UnsupportedComputeProviderMode;
    if (session.backend() == .cuda) {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        const owner: *ArchSession = @ptrCast(@alignCast(session.ptr));
        // The source already began this request. A second CUDA beginRequest
        // resets capture/pinned-temp state while the encoder output is live.
        return @import("../backends/compute_factory.zig").acquireCuda(owner.backend_data.cuda.compute, null);
    }
    return getComputeBackendBorrowingSharedProvider(session, allocator);
}

fn finishResidentIncrementalDecoder(session: Session, allocator: std.mem.Allocator, control: ?InferenceExecutionControl, source: RetainedEncoderSource, config: whisper_mod.Config, rows: usize, geometry: whisper_arch.CacheGeometry, base: @import("../backends/session.zig").RunPermit, gate: ?*std.atomic.Mutex, destination_model: ?*Session.ModelLifetime, foreign_copy: ?*const (if (build_options.enable_cuda) cuda_compute_mod.PreparedForeignTensorCopy else void), cleanup: ?*const ReceiverCleanup) !WhisperNativeDecoder {
    // Own per-attempt compute caches while the complete source retains the
    // outer provider lease. Native gets private compute; Metal borrows its
    // already-leased provider without taking/releasing that lease twice.
    // Establish/borrow the ACTUAL provider operation before even the private
    // backend constructor reads SDK metadata. Raw source callers get a new
    // managed scope; engine sources retain their original explicit outer one.
    var source_operation = @import("backend_operation.zig").Scope{};
    if (source.receiver.same_model_owner and source.backend.kind() == .metal) {
        try @import("backend_operation.zig").enterInto(source.backend, null, source.backend.operationToken(), .managed, &source_operation);
    }
    defer source_operation.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    var cb = if (source.receiver.same_model_owner)
        try @import("../backends/compute_provider.zig").acquireWith(session, allocator, null, control, acquireIncrementalSourceProvider)
    else
        try getComputeBackendWithControl(session, allocator, control);
    errdefer if (cleanup) |owner| cb.deinitWithCleanup(.{ .boundary = owner.boundary() }) else cb.deinit();
    if (source.receiver.same_model_owner and source.backend.kind() == .metal) {
        if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
        // Verify original numerical identity and actual shared provider before
        // adopting authority. This does not wrap or replace the backend table.
        const token = source_operation.token() orelse return error.BackendOperationUnavailable;
        try token.validate(.{ .implementation = .{ .context = cb.backend.ptr, .table = cb.backend.vtable }, .program = null, .mode = .managed });
        const target: *@import("../ops/metal_compute.zig").MetalCompute = @ptrCast(@alignCast(cb.backend.ptr));
        if (source_operation.takeOwned()) |owned| {
            target.adoptEngineOperation(owned) catch |err| {
                var retired = owned;
                retired.deinitChecked() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
                return err;
            };
        } else try target.adoptEngineBorrowedOperation(token);
    }
    // B is a resource/source cleanup fence, not invented numerical authority.
    // A same-provider Metal borrower must keep its source's original operation
    // epoch. Only its actual owned Engine can bind a new numerical epoch.
    if (cleanup) |owner| {
        if (!(source.receiver.same_model_owner and cb.backend.kind() == .metal))
            try bindOwnedDecoderCleanup(&cb.backend, owner.boundary());
    }
    var cache_permit = try incrementalCachePermit(session, &base, geometry, &cb.backend);
    errdefer if (cache_permit) |*permit| permit.deinit();
    var owns_encoder = false;
    const encoder_ct = if (source.receiver.backend == .cuda and !source.receiver.same_model_owner) blk: {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        const ready = foreign_copy orelse return error.InvalidIncrementalReceiverStorage;
        const copied = try cuda_compute_mod.importPreparedForeignTensorCopy(&cb.backend, ready);
        owns_encoder = true;
        break :blk copied;
    } else source.value;
    errdefer if (owns_encoder) cb.backend.freeWithBoundary(encoder_ct, if (cleanup) |owner| owner.boundary() else null);
    const cache = try whisper_arch.DecodeCache.init(&cb.backend, allocator, config, encoder_ct, rows);
    return .{ .allocator = allocator, .cb = cb, .config = config, .encoder_hidden = encoder_ct, .cache = cache, .gate = gate, .encoder_owned = owns_encoder, .source_resources = source.resources, .receiver_cleanup = if (cleanup) |owner| owner.* else null, .destination_model = destination_model, .base_permit = base, .cache_permit = cache_permit, .diagnostics_binding = session.invocation_diagnostics };
}

/// Open an incremental decoder over `encoder_hidden` (host f32,
/// `[enc_seq, d_model]`). Returns null for sessions that are not native
/// Whisper (ONNX bundles keep their own incremental path).
pub fn whisperNativeDecoder(
    session: Session,
    allocator: std.mem.Allocator,
    control: ?InferenceExecutionControl,
    encoder_hidden: []const f32,
    enc_seq: usize,
) !?WhisperNativeDecoder {
    const held = if (session.model_lifetime) |model| model.retain() else null;
    defer if (held) |model| model.release();
    return createHostIncrementalDecoder(session, allocator, control, encoder_hidden, enc_seq);
}

fn createHostIncrementalDecoder(
    session: Session,
    allocator: std.mem.Allocator,
    control: ?InferenceExecutionControl,
    encoder_hidden: []const f32,
    enc_seq: usize,
) !?WhisperNativeDecoder {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    const cfg = switch (self.arch_config) {
        .whisper => |cfg| cfg,
        else => return null,
    };
    const hidden_elements = std.math.mul(usize, enc_seq, cfg.d_model) catch return error.ResourceLimitExceeded;
    if (encoder_hidden.len != hidden_elements) return error.InvalidInputShape;
    var cleanup = try ReceiverCleanup.beginForOwnedGate(session);
    var transferred = false;
    defer if (!transferred) cleanup.finish();
    const destination_model = if (session.model_lifetime) |model| model.retain() else null;
    errdefer if (destination_model) |model| model.releaseWithBoundary(cleanup.boundary());
    const geometry = try whisper_arch.CacheGeometry.init(cfg, enc_seq);
    var base_permit = try incrementalBasePermit(session, cfg, geometry, false, false);
    errdefer base_permit.deinit();
    const gate = session.execution_gate;
    if (gate) |mutex| {
        if (control) |active| try active.lock(mutex) else while (!mutex.tryLock()) std.atomic.spinLoopHint();
    }
    errdefer if (gate) |mutex| mutex.unlock();
    const result = try incremental_observation.callOperation(session.invocation_diagnostics, .decoder_init, finishHostIncrementalDecoder, .{ session, allocator, control, encoder_hidden, enc_seq, cfg, geometry, base_permit, gate, destination_model, &cleanup });
    transferred = true;
    return result;
}

fn finishHostIncrementalDecoder(session: Session, allocator: std.mem.Allocator, control: ?InferenceExecutionControl, encoder_hidden: []const f32, enc_seq: usize, cfg: whisper_mod.Config, geometry: whisper_arch.CacheGeometry, base_permit: @import("../backends/session.zig").RunPermit, gate: ?*std.atomic.Mutex, destination_model: ?*Session.ModelLifetime, cleanup: *const ReceiverCleanup) !WhisperNativeDecoder {
    var cb = try getComputeBackendWithControl(session, allocator, control);
    errdefer cb.deinitWithCleanup(.{ .boundary = cleanup.boundary() });
    try bindOwnedDecoderCleanup(&cb.backend, cleanup.boundary());
    const shape = [_]i32{ 1, @intCast(enc_seq), @intCast(cfg.d_model) };
    // Upload once so the cross-attention projections run on the device and
    // their outputs are born resident. The host handle remains owned if the
    // provider rejects/fails the upload before a resident handle exists.
    const encoder_ct = blk: {
        const host = try cb.backend.fromFloat32Shape(encoder_hidden, &shape);
        errdefer cb.backend.free(host);
        if (try cb.backend.ensureDeviceResident(host)) |device| {
            cb.backend.free(host);
            break :blk device;
        }
        break :blk host;
    };
    errdefer cb.backend.free(encoder_ct);
    var cache_permit = try incrementalCachePermit(session, &base_permit, geometry, &cb.backend);
    errdefer if (cache_permit) |*permit| permit.deinit();
    const cache = try whisper_arch.DecodeCache.init(&cb.backend, allocator, cfg, encoder_ct, enc_seq);
    return .{
        .allocator = allocator,
        .cb = cb,
        .config = cfg,
        .encoder_hidden = encoder_ct,
        .cache = cache,
        .gate = gate,
        .destination_model = destination_model,
        .receiver_cleanup = cleanup.*,
        .base_permit = base_permit,
        .cache_permit = cache_permit,
        .diagnostics_binding = session.invocation_diagnostics,
    };
}

/// Compute backend for a long-lived runtime cached on the loaded model (the
/// Metal whole-model executor). On Metal it borrows the store's shared
/// provider instead of taking the per-request execution lease, because the
/// runtime is only driven by requests that already hold that lease; see
/// `MetalCompute.initBorrowingSharedProvider`. Other backends behave as
/// `getComputeBackend`.
pub fn getComputeBackendBorrowingSharedProvider(session: Session, allocator: std.mem.Allocator) !ops.ComputeBackend {
    if (session.compute_backend_provider != null) return session.acquireComputeBackend(allocator, .{ .borrow_shared_provider = true });
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .metal) return getComputeBackend(session, allocator);
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    return @import("../backends/compute_factory.zig").borrowMetalProvider(allocator, gpuBackendData(self), self.io, .{
        .config = self.kernel_jit_config,
        .scope = self.metal_jit_scope,
        .load_context = self.kernel_jit_load_context,
    }, self.arch_config == .embedding_gemma2);
}

/// Compatibility names for the backend-owned request lifetime protocol.
pub const ManagedComputeBackend = @import("../backends/compute_provider.zig").ManagedComputeBackend;

pub fn getComputeBackendWithControl(session: Session, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !ManagedComputeBackend {
    return getManagedComputeBackend(session, allocator, null, control);
}

pub fn getManagedComputeBackend(session: Session, allocator: std.mem.Allocator, run_budget: ?*runtime.tier.memory.RunBudget, control: ?InferenceExecutionControl) !ManagedComputeBackend {
    return @import("../backends/compute_provider.zig").acquireWith(session, allocator, run_budget, control, acquireCompatibleComputeBackend);
}

fn acquireCompatibleComputeBackend(session: Session, allocator: std.mem.Allocator, options: @import("../backends/session.zig").ComputeBackendOptions) !ops.ComputeBackend {
    if (session.compute_backend_provider != null) return session.acquireComputeBackend(allocator, options);
    if (options.run_budget) |budget| return getComputeBackendWithBudget(session, allocator, budget);
    return getComputeBackend(session, allocator);
}

/// Prepare immutable FP32 state before publishing a model or reporting an
/// offline worker ready. The caller owns load admission and model serialization.
/// This hook independently preserves process protection through construction,
/// synchronous device preparation, CB cleanup, and failed partial-table cleanup.
/// CPU and reduced artifacts retain their existing storage and execution path.
pub fn prepareGlinerBoundaryResident(session: Session, control: ?InferenceExecutionControl) !void {
    return prepareGlinerBoundaryResidentImpl(session, session, control);
}

fn prepareGlinerBoundaryResidentImpl(session: Session, published: Session, control: ?InferenceExecutionControl) !void {
    if (comptime !build_options.enable_metal) return;
    if (session.vtable != &arch_vtable) return;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .metal or self.arch_config != .gliner_boundary) return;
    const data = gpuBackendData(self);
    const owner = data.boundary_resident orelse return;
    try @import("../models/resident_preparation.zig").prepareBoundary(.{ .ptr = self, .session = session, .create = createPreparationBackend, .runtime_identity = preparationRuntimeIdentity }, published, self.allocator, owner, data.tensor_store, &self.arch_config.gliner_boundary, control);
}

/// Read only while holding the session/model lifetime and serialization lease.
pub fn isGlinerBoundaryResidentReady(session: Session) bool {
    if (comptime !build_options.enable_metal) return false;
    if (session.vtable != &arch_vtable) return false;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .metal or self.arch_config != .gliner_boundary) return false;
    const owner = gpuBackendData(self).boundary_resident orelse return false;
    return owner.isReady();
}

pub const GlinerBoundaryResidentStats = struct {
    model_live_bytes: usize = 0,
    workspace_live_bytes: usize = 0,
    workspace_capacity_bytes: usize = 0,
    workspace_generation: u64 = 0,
    generation: u64 = 0,
    weight_upload_bytes: u64 = 0,
    weight_upload_calls: u64 = 0,
    ready: bool = false,
};

/// Snapshot only; never creates a CB, uploads, extends model TTL, or performs
/// device synchronization. Do not call it after releasing the model/session.
pub fn glinerBoundaryResidentStats(session: Session) !GlinerBoundaryResidentStats {
    if (comptime !build_options.enable_metal) return .{};
    if (session.vtable != &arch_vtable) return .{};
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .metal or self.arch_config != .gliner_boundary) return .{};
    const owner = gpuBackendData(self).boundary_resident orelse return .{};
    const stats = owner.stats();
    const workspace = owner.workspace.stats();
    return .{ .model_live_bytes = stats.resident_model_live_bytes, .workspace_live_bytes = workspace.live_bytes, .workspace_capacity_bytes = workspace.capacity_bytes, .workspace_generation = workspace.generation, .generation = stats.generation, .weight_upload_bytes = stats.weight_upload_bytes, .weight_upload_calls = stats.weight_upload_calls, .ready = owner.isReady() };
}

/// No lock, CB, allocation or ownership token. The model manager may call this
/// only after excluding active handles and in-flight loads under its cache lock;
/// a normal caller must instead hold the session's execution/lifetime lease.
pub fn glinerBoundaryWorkspaceAdmissionAmounts(session: Session) runtime.tier.memory.AdmissionAmounts {
    if (comptime !build_options.enable_metal) return .{};
    if (session.vtable != &arch_vtable) return .{};
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .metal or self.arch_config != .gliner_boundary) return .{};
    const owner = gpuBackendData(self).boundary_resident orelse return .{};
    return owner.workspace.admittedAmounts();
}

test "gliner boundary workspace admission reader leaves foreign native and cold sessions untouched" {
    const empty = runtime.tier.memory.AdmissionAmounts{};
    var unrelated: u8 = 0;
    var foreign = arch_vtable;
    foreign.independentBatchRows = null;
    try std.testing.expectEqual(empty, glinerBoundaryWorkspaceAdmissionAmounts(.{ .ptr = &unrelated, .vtable = &foreign }));
    var self = ArchSession{
        .allocator = std.testing.allocator,
        .arch_config = .{ .gliner_boundary = std.mem.zeroes(gliner_boundary_model.Config) },
        .backend_type = .native,
        .backend_data = .{ .native = .{ .allocator = std.testing.allocator, .resident_weights = .empty, .lazy_weights = .empty } },
    };
    const session = Session{ .ptr = &self, .vtable = &arch_vtable };
    try std.testing.expectEqual(empty, glinerBoundaryWorkspaceAdmissionAmounts(session));
    if (comptime build_options.enable_metal) {
        self.backend_type = .metal;
        self.backend_data = .{ .metal = .{ .allocator = std.testing.allocator, .prefix = "", .lazy_weights = .empty } };
        try std.testing.expectEqual(empty, glinerBoundaryWorkspaceAdmissionAmounts(session));
        try std.testing.expect(self.backend_data.metal.boundary_resident == null);
        try std.testing.expect(self.backend_data.metal.shared_metal_native_provider == null);
    }
}

/// Exact serial encoder geometry and the observed model-owned workspace slot.
/// Snapshot under the model lock, release that lock before acquiring admission,
/// then pass this value and its separate permit to the managed constructor.
pub const GlinerBoundaryWorkspacePlan = struct {
    batch: usize,
    sequence: usize,
    model_generation: u64,
    observed_generation: u64,
    observed_capacity_bytes: usize,
    required_capacity_bytes: usize,
    effective_capacity_bytes: usize,
    max_product_elements: usize,
    replacement_amounts: ?runtime.tier.memory.AdmissionAmounts,

    fn init(model_generation: u64, workspace: boundary_resident.WorkspaceStats, batch: usize, sequence: usize, hidden: usize, intermediate: usize) !GlinerBoundaryWorkspacePlan {
        if (workspace.active) return error.GlinerBoundaryWorkspaceBusy;
        const geometry = try ops.gliner_boundary_device.EncoderWorkspacePlan.init(batch, sequence, hidden, intermediate);
        return .{
            .batch = batch,
            .sequence = sequence,
            .model_generation = model_generation,
            .observed_generation = workspace.generation,
            .observed_capacity_bytes = workspace.capacity_bytes,
            .required_capacity_bytes = geometry.capacity_bytes,
            .effective_capacity_bytes = @max(workspace.capacity_bytes, geometry.capacity_bytes),
            .max_product_elements = geometry.max_product_elements,
            .replacement_amounts = if (workspace.capacity_bytes < geometry.capacity_bytes)
                try boundary_resident.Workspace.replacementAmounts(geometry.capacity_bytes)
            else
                null,
        };
    }

    /// The existing encoder ceiling still covers workspace plus request-local
    /// allocations. The workspace lease is retained separately, so subtract it
    /// from both the execution owner ceiling and the transient admission amount.
    pub fn requestEncoderLimit(self: GlinerBoundaryWorkspacePlan, encoder_limit_bytes: usize) !usize {
        if (self.effective_capacity_bytes == 0 or self.effective_capacity_bytes >= encoder_limit_bytes)
            return error.ResourceLimitExceeded;
        return encoder_limit_bytes - self.effective_capacity_bytes;
    }
};

fn readyGlinerBoundaryResident(session: Session) !*boundary_resident.Owner {
    if (comptime !build_options.enable_metal) return error.UnsupportedGlinerBoundaryDevice;
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .metal or self.arch_config != .gliner_boundary) return error.UnsupportedGlinerBoundaryDevice;
    const owner = gpuBackendData(self).boundary_resident orelse return error.UnsupportedGlinerBoundaryPrecision;
    if (!owner.isReady()) return error.GlinerBoundaryResidentNotPrepared;
    return owner;
}

/// Pure scalar planning: no CB creation, allocation, admission or device work.
/// The caller supplies actual padded token geometry, never a configured maximum.
pub fn planGlinerBoundaryWorkspace(session: Session, batch: usize, sequence: usize) !GlinerBoundaryWorkspacePlan {
    const owner = try readyGlinerBoundaryResident(session);
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    const config = self.arch_config.gliner_boundary.encoder;
    return GlinerBoundaryWorkspacePlan.init(owner.generation, owner.workspace.stats(), batch, sequence, config.hidden_size, config.intermediate_size);
}

/// Uses only a previously acquired exact replacement permit. This function
/// runs under the caller's model lock and the CB's provider lease; it must never
/// call admission or eviction. A racing growth is rejected before creating a
/// backend or consuming the caller's permit, allowing an outside-lock retry.
pub fn getManagedGlinerBoundaryComputeBackend(
    session: Session,
    allocator: std.mem.Allocator,
    run_budget: ?*runtime.tier.memory.RunBudget,
    control: ?InferenceExecutionControl,
    plan: GlinerBoundaryWorkspacePlan,
    replacement_permit: ?*runtime.tier.memory.AdmissionLease,
) !ManagedComputeBackend {
    if (comptime !build_options.enable_metal) return error.UnsupportedGlinerBoundaryDevice;
    const effective = control orelse InferenceExecutionControl{};
    try effective.check();
    const current = try planGlinerBoundaryWorkspace(session, plan.batch, plan.sequence);
    if (!std.meta.eql(current, plan)) return error.GlinerBoundaryWorkspacePlanChanged;
    const owner = try readyGlinerBoundaryResident(session);
    var managed = try getManagedComputeBackend(session, allocator, run_budget, control);
    errdefer managed.deinit();
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    const provider = gpuBackendData(self).shared_metal_native_provider orelse return error.UnsupportedGlinerBoundaryDevice;
    const runtime_identity: *anyopaque = @ptrCast(provider.raw_decode_runtime orelse return error.UnsupportedGlinerBoundaryDevice);
    try owner.checkRuntime(runtime_identity);
    if (plan.replacement_amounts != null) {
        const permit = replacement_permit orelse return error.InvalidGlinerBoundaryWorkspaceAdmission;
        var pending = try owner.workspace.prepareReplacement(runtime_identity, owner.generation, plan.required_capacity_bytes, permit, control);
        defer pending.deinit();
        try owner.workspace.install(&pending);
    }
    try effective.check();
    var borrow = try owner.workspace.begin(runtime_identity, owner.generation);
    managed.completion = .{
        .ptr = borrow.workspace.?,
        .generation = borrow.generation,
        .epoch = borrow.epoch,
        .finish_fn = finishBoundaryWorkspace,
    };
    // Only this architecture's Metal constructor supplied this concrete CB.
    // The backend copies the borrow's generation/epoch and never finishes it.
    const compute: *MetalCompute = @ptrCast(@alignCast(managed.backend.ptr));
    try compute.bindBoundaryWorkspace(&borrow, plan.max_product_elements);
    try effective.check();
    return managed;
}

fn finishBoundaryWorkspace(ptr: *anyopaque, generation: u64, epoch: u64) !void {
    var borrow = boundary_resident.Workspace.Borrow{
        .workspace = @ptrCast(@alignCast(ptr)),
        .generation = generation,
        .epoch = epoch,
    };
    try borrow.finish();
}

test "gliner boundary workspace plan separates persistent capacity from request ceiling" {
    const first = try GlinerBoundaryWorkspacePlan.init(7, .{ .live_bytes = 0, .capacity_bytes = 0, .generation = 0, .epoch = 0, .active = false }, 1, 17, 384, 1536);
    try std.testing.expectEqual(@as(usize, 2 * 17 * (5 * 384 + 1536) * 4), first.required_capacity_bytes);
    try std.testing.expectEqual(@as(usize, 17 * 1536), first.max_product_elements);
    try std.testing.expectEqual(first.required_capacity_bytes, first.replacement_amounts.?.backend_scratch_bytes);
    const ceiling: usize = 2 * 1024 * 1024 * 1024;
    try std.testing.expectEqual(ceiling, first.effective_capacity_bytes + try first.requestEncoderLimit(ceiling));
    try std.testing.expectError(error.ResourceLimitExceeded, first.requestEncoderLimit(first.effective_capacity_bytes));
    const cached = try GlinerBoundaryWorkspacePlan.init(7, .{ .live_bytes = first.effective_capacity_bytes, .capacity_bytes = first.effective_capacity_bytes, .generation = 1, .epoch = 3, .active = false }, 1, 1, 384, 1536);
    try std.testing.expect(cached.replacement_amounts == null);
    try std.testing.expectEqual(first.effective_capacity_bytes, cached.effective_capacity_bytes);
    try std.testing.expectEqual(ceiling, cached.effective_capacity_bytes + try cached.requestEncoderLimit(ceiling));
    const grown = try GlinerBoundaryWorkspacePlan.init(7, .{ .live_bytes = first.effective_capacity_bytes, .capacity_bytes = first.effective_capacity_bytes, .generation = 1, .epoch = 3, .active = false }, 2, 17, 384, 1536);
    try std.testing.expectEqual(2 * first.effective_capacity_bytes, grown.replacement_amounts.?.backend_scratch_bytes);
    try std.testing.expectError(error.GlinerBoundaryWorkspaceBusy, GlinerBoundaryWorkspacePlan.init(7, .{ .live_bytes = 32, .capacity_bytes = 32, .generation = 1, .epoch = 3, .active = true }, 1, 1, 384, 1536));
}

pub fn replaceBlasResidentWeight(session: Session, name: []const u8, weight: LoadedWeight) !void {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .native) return error.NotNativeSession;

    if (self.backend_data.native.resident_weights.getPtr(name)) |slot| {
        var old = slot.*;
        old.deinit();
        slot.* = weight;
        return;
    }

    try self.backend_data.native.resident_weights.put(self.allocator, try self.allocator.dupe(u8, name), weight);
}

test "managed direct compute guards construction and cleanup" {
    const Probe = struct {
        armed: bool = false,
        disarms: usize = 0,
        closes: usize = 0,
        fn backend(_: *anyopaque) BackendType {
            return .metal;
        }
        fn arm(raw: *anyopaque, _: @import("../execution_control.zig").MonitorControl) !u64 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.armed);
            self.armed = true;
            return 1;
        }
        fn disarm(raw: *anyopaque, _: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.armed);
            self.armed = false;
            self.disarms += 1;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.armed);
            self.closes += 1;
        }
    };
    var probe = Probe{};
    var vtable: Session.VTable = undefined;
    vtable.backend = Probe.backend;
    vtable.interruption = null;
    const session = Session{ .ptr = &probe, .vtable = &vtable };
    const allocator = std.testing.allocator;
    try std.testing.expectError(error.Timeout, getComputeBackendWithControl(session, allocator, .{ .deadline_ns = 0 }));
    try std.testing.expectError(error.ProcessIsolationRequired, getComputeBackendWithControl(session, allocator, .{}));
    try std.testing.expectError(error.ProcessIsolationRequired, getManagedComputeBackend(session, allocator, null, null));
    const control = InferenceExecutionControl{
        .hard_cancellation = .{ .ptr = &probe, .arm_fn = Probe.arm, .disarm_fn = Probe.disarm },
    };
    // A non-architecture session fails construction only after arming, then
    // unwinds the guard without retaining the borrowed control.
    try std.testing.expectError(error.NotArchSession, getComputeBackendWithControl(session, allocator, control));
    try std.testing.expectEqual(@as(usize, 1), probe.disarms);
    try std.testing.expect(!probe.armed);

    var compute_vtable: ops.ComputeBackend.VTable = undefined;
    compute_vtable.deinitBackend = Probe.close;
    var managed = ManagedComputeBackend{
        .backend = .{ .ptr = &probe, .vtable = &compute_vtable },
        .guard = try control.enterUninterruptible(.uninterruptible),
    };
    managed.deinit();
    managed.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.closes);
    try std.testing.expectEqual(@as(usize, 2), probe.disarms);
    try std.testing.expect(!probe.armed);
}

pub fn getComputeBackendWithBudget(
    session: Session,
    allocator: std.mem.Allocator,
    run_budget: *runtime.tier.memory.RunBudget,
) !ops.ComputeBackend {
    if (session.compute_backend_provider != null) return session.acquireComputeBackend(allocator, .{ .run_budget = run_budget });
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    var cb = try makeComputeBackend(self, allocator, run_budget);
    errdefer cb.deinit();
    // The CUDA implementation is shared by the loaded session. Establish an
    // unconditional request boundary here, before prompt-cache selection can
    // bypass KV-hook provisioning, so no graph or pinned-address ABI survives
    // from the preceding request.
    try @import("../backends/compute_factory.zig").beginRequest(session, &cb);
    return cb;
}

pub fn getGenericEncoderArchConfig(session: Session) !GenericEncoderArchConfig {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .bert => |cfg| .{ .bert = cfg },
        .deberta => |cfg| .{ .deberta = cfg },
        .gliner => |cfg| .{ .deberta = cfg },
        else => error.UnsupportedArchitecture,
    };
}

/// Schema-aware extraction owns boundary task execution. Generic Session.run
/// must not synthesize a legacy span request for this architecture.
pub fn getGlinerBoundaryConfig(session: Session) !gliner_boundary_model.Config {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .gliner_boundary => |config| config,
        else => error.NotGlinerBoundarySession,
    };
}

pub const GlinerSpanConfig = union(enum) {
    deberta: deberta_mod.Config,
    modern_bert: modern_bert_arch.Config,
};

/// Encoder geometry selected by a span (SpanExtractor) GLiNER2 session.
pub fn getGlinerSpanConfig(session: Session) !GlinerSpanConfig {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.arch_config) {
        .gliner => |config| .{ .deberta = config },
        .gliner_modern_bert => |config| .{ .modern_bert = config },
        .modern_bert => |config| if (self.task == .extractor and self.gliner_span_encoder_family == .modern_bert) .{ .modern_bert = config } else error.NotGlinerSpanSession,
        else => error.NotGlinerSpanSession,
    };
}

test "ModernBERT extractor requires the declared GLiNER span wrapper" {
    var arch = ArchSession{
        .allocator = std.testing.allocator,
        .arch_config = .{ .modern_bert = .{} },
        .task = .extractor,
        .backend_type = .native,
        .backend_data = .{ .native = .{ .allocator = std.testing.allocator, .resident_weights = .empty, .lazy_weights = .empty } },
    };
    const session = Session{ .ptr = &arch, .vtable = &arch_vtable };
    try std.testing.expectError(error.NotGlinerSpanSession, getGlinerSpanConfig(session));
    arch.gliner_span_encoder_family = .modern_bert;
    try std.testing.expect((try getGlinerSpanConfig(session)) == .modern_bert);
}

test "declared ModernBERT GLiNER session runs the marker decision head" {
    const a = std.testing.allocator;
    const Fixture = struct {
        fn put(store: *NativeData, name: []const u8, shape: []const i64, values: []const f32) !void {
            const key = try store.allocator.dupe(u8, name);
            errdefer store.allocator.free(key);
            var value = try Tensor.initFloat32(store.allocator, key, shape, values);
            errdefer value.deinit();
            try store.resident_weights.put(store.allocator, key, .{ .tensor = value });
        }
    };
    var store = NativeData{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    try Fixture.put(&store, "model.embeddings.tok_embeddings.weight", &.{ 4, 4 }, &.{
        1, 0, 0, 0,
        0, 1, 0, 0,
        0, 0, 1, 0,
        0, 0, 0, 1,
    });
    try Fixture.put(&store, "model.embeddings.norm.weight", &.{4}, &.{ 1, 1, 1, 1 });
    try Fixture.put(&store, "model.final_norm.weight", &.{4}, &.{ 1, 1, 1, 1 });
    try Fixture.put(&store, "model.layers.0.attn.Wqkv.weight", &.{ 12, 4 }, &(@as([48]f32, @splat(0))));
    try Fixture.put(&store, "model.layers.0.attn.Wo.weight", &.{ 4, 4 }, &(@as([16]f32, @splat(0))));
    try Fixture.put(&store, "model.layers.0.mlp_norm.weight", &.{4}, &.{ 1, 1, 1, 1 });
    try Fixture.put(&store, "model.layers.0.mlp.Wi.weight", &.{ 8, 4 }, &(@as([32]f32, @splat(0))));
    try Fixture.put(&store, "model.layers.0.mlp.Wo.weight", &.{ 4, 4 }, &(@as([16]f32, @splat(0))));
    try Fixture.put(&store, "classifier.0.weight", &.{ 8, 4 }, &(@as([32]f32, @splat(0))));
    try Fixture.put(&store, "classifier.0.bias", &.{8}, &.{ 1, 2, 3, 4, 5, 6, 7, 8 });
    try Fixture.put(&store, "classifier.2.weight", &.{ 1, 8 }, &.{ 1, 1, 1, 1, 1, 1, 1, 1 });
    try Fixture.put(&store, "classifier.2.bias", &.{1}, &.{0.5});

    var arch = ArchSession{
        .allocator = a,
        .arch_config = .{ .modern_bert = .{
            .vocab_size = 4,
            .hidden_size = 4,
            .num_hidden_layers = 1,
            .num_attention_heads = 2,
            .intermediate_size = 4,
            .max_position_embeddings = 8,
            .checkpoint_layout = .huggingface_fused_qkv_no_bias,
        } },
        .task = .extractor,
        .gliner_span_encoder_family = .modern_bert,
        .backend_type = .native,
        .backend_data = .{ .native = store },
    };
    // Ownership moved into the stack session for the duration of this run.
    store.resident_weights = .empty;
    defer arch.backend_data.native.deinitOwned();
    var inputs = [_]Tensor{
        try Tensor.initInt64(a, "input_ids", &.{ 1, 2 }, &.{ 0, 1 }),
        try Tensor.initInt64(a, "attention_mask", &.{ 1, 2 }, &.{ 1, 1 }),
        try Tensor.initInt64(a, "decision_marker_positions", &.{ 1, 1 }, &.{1}),
        try Tensor.initInt64(a, "decision_marker_mask", &.{ 1, 1 }, &.{1}),
    };
    defer for (&inputs) |*input| input.deinit();
    const outputs = try archRunImpl(&arch, &inputs, a, null);
    defer {
        for (outputs) |*output| output.deinit();
        a.free(outputs);
    }
    try std.testing.expectEqual(@as(usize, 1), outputs.len);
    try std.testing.expectEqualSlices(f32, &.{36.5}, outputs[0].asFloat32());

    var padded_inputs = [_]Tensor{
        try Tensor.initInt64(a, "input_ids", &.{ 1, 2 }, &.{ 0, 1 }),
        try Tensor.initInt64(a, "attention_mask", &.{ 1, 2 }, &.{ 1, 0 }),
        try Tensor.initInt64(a, "decision_marker_positions", &.{ 1, 1 }, &.{1}),
        try Tensor.initInt64(a, "decision_marker_mask", &.{ 1, 1 }, &.{1}),
    };
    defer for (&padded_inputs) |*input| input.deinit();
    try std.testing.expectError(error.InvalidGlinerDecisionMarkerPosition, archRunImpl(&arch, &padded_inputs, a, null));
}

pub fn getGlinerBoundaryIdentity(session: Session) !boundary_bundle.Identity {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.arch_config != .gliner_boundary) return error.NotGlinerBoundarySession;
    return self.boundary_identity orelse error.MissingGlinerBoundaryIdentity;
}

/// Seal the tokenizer snapshot only after ModelManager parses these exact
/// bytes. Offline sessions remain usable, while public serving cannot observe
/// a complete identity assembled from an unrelated metadata read.
pub fn sealGlinerDecisionTokenizerDigest(session: Session, tokenizer: gliner_decide_qualification.Digest) !void {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    const identity = self.gliner_decision_identity orelse return;
    if (!sameDigest(identity.sidecars.tokenizer, tokenizer)) return error.GlinerDecisionArtifactMismatch;
    self.gliner_decision_identity.?.sidecars.tokenizer = tokenizer;
    self.gliner_decision_identity_sealed = true;
}

pub fn getGlinerDecisionIdentity(session: Session) !gliner_decide_qualification.Identity {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (!self.gliner_decision_identity_sealed) return error.MissingGlinerDecisionIdentity;
    return self.gliner_decision_identity orelse error.MissingGlinerDecisionIdentity;
}

test "GLiNER decision identity seals only the exact tokenizer snapshot" {
    const digest = gliner_decide_qualification.Digest.of("tokenizer");
    const other = gliner_decide_qualification.Digest.of("other");
    var arch = ArchSession{
        .allocator = std.testing.allocator,
        .arch_config = .{ .modern_bert = .{} },
        .task = .extractor,
        .gliner_span_encoder_family = .modern_bert,
        .gliner_decision_identity = .{
            .encoder_family = .modern_bert,
            .geometry = .{ .hidden_size = 1, .intermediate_size = 1, .num_hidden_layers = 1, .num_attention_heads = 1, .vocab_size = 1, .max_position_embeddings = 1 },
            .markers = .{ .p = 1, .c = 2, .e = 3, .r = 4, .l = 5, .sep_struct = 6, .sep_text = 7 },
            .inventory = .{ .count = 1, .all_f32 = true },
            .weight = other,
            .sidecars = .{ .config = other, .encoder_config = other, .tokenizer = digest, .tokenizer_config = other, .special_tokens_map = null },
        },
        .backend_type = .native,
        .backend_data = .{ .native = .{ .allocator = std.testing.allocator, .resident_weights = .empty, .lazy_weights = .empty } },
    };
    const session = Session{ .ptr = &arch, .vtable = &arch_vtable };
    try std.testing.expectError(error.MissingGlinerDecisionIdentity, getGlinerDecisionIdentity(session));
    try std.testing.expectError(error.GlinerDecisionArtifactMismatch, sealGlinerDecisionTokenizerDigest(session, other));
    try std.testing.expectError(error.MissingGlinerDecisionIdentity, getGlinerDecisionIdentity(session));
    try sealGlinerDecisionTokenizerDigest(session, digest);
    try std.testing.expect(sameDigest((try getGlinerDecisionIdentity(session)).sidecars.tokenizer, digest));

    try verifyGlinerDecisionSnapshot("config", gliner_decide_qualification.Digest.of("config"));
    try std.testing.expectError(error.GlinerDecisionArtifactMismatch, verifyGlinerDecisionSnapshot("changed", gliner_decide_qualification.Digest.of("config")));
    try std.testing.expectError(error.MissingGlinerDecisionIdentity, verifyGlinerDecisionSnapshot("config", null));
}

test "CUDA backend identity handoff preserves bytes but requires fresh tokenizer sealing" {
    const tokenizer = gliner_decide_qualification.Digest.of("tokenizer");
    const other = gliner_decide_qualification.Digest.of("other");
    const identity = gliner_decide_qualification.Identity{
        .encoder_family = .modern_bert,
        .geometry = .{ .hidden_size = 1, .intermediate_size = 1, .num_hidden_layers = 1, .num_attention_heads = 1, .vocab_size = 1, .max_position_embeddings = 1 },
        .markers = .{ .p = 1, .c = 2, .e = 3, .r = 4, .l = 5, .sep_struct = 6, .sep_text = 7 },
        .inventory = .{ .count = 1, .all_f32 = true },
        .weight = other,
        .sidecars = .{ .config = other, .encoder_config = other, .tokenizer = tokenizer, .tokenizer_config = other, .special_tokens_map = null },
    };
    const source = ArchSession{
        .allocator = std.testing.allocator,
        .arch_config = .{ .modern_bert = .{} },
        .gliner_decision_identity = identity,
        // Even a previously published source cannot confer tokenizer sealing
        // authority on a newly constructed backend session.
        .gliner_decision_identity_sealed = true,
        .backend_type = .native,
        .backend_data = .{ .native = .{ .allocator = std.testing.allocator, .resident_weights = .empty, .lazy_weights = .empty } },
    };
    const handoff = unsealedGlinerDecisionIdentityHandoff(source.gliner_decision_identity);
    try std.testing.expect(handoff.identity != null);
    try std.testing.expect(std.meta.eql(identity, handoff.identity.?));
    try std.testing.expect(!handoff.sealed);

    var converted = ArchSession{
        .allocator = std.testing.allocator,
        .arch_config = source.arch_config,
        .gliner_decision_identity = handoff.identity,
        .gliner_decision_identity_sealed = handoff.sealed,
        .backend_type = .native,
        .backend_data = .{ .native = .{ .allocator = std.testing.allocator, .resident_weights = .empty, .lazy_weights = .empty } },
    };
    const session = Session{ .ptr = &converted, .vtable = &arch_vtable };
    try std.testing.expectError(error.MissingGlinerDecisionIdentity, getGlinerDecisionIdentity(session));
    try std.testing.expectError(error.GlinerDecisionArtifactMismatch, sealGlinerDecisionTokenizerDigest(session, other));
    try sealGlinerDecisionTokenizerDigest(session, tokenizer);
    try std.testing.expect(std.meta.eql(identity, try getGlinerDecisionIdentity(session)));
}

/// Test-only observation of the actual loaded owner. A caller must retain the
/// model (or hold its cache owner's load lock) and its execution mutex until
/// this function returns. The returned addresses are identity scalars, never
/// borrowed handles to use after the model lifetime ends.
///
/// This namespace has no callable surface in production. It does not create a
/// compute backend, reopen files, upload weights, synchronize or allocate.
pub const TestGlinerBoundaryMetalOwner = if (@import("builtin").is_test) struct {
    pub const Identity = struct {
        store_address: usize,
        reader_address: usize,
        mapping_address: usize,
        mapping_bytes: usize,
        source_bytes: usize,
        source_tensor_count: usize,
        lazy_tensor_count: usize,
        provider_address: usize,
        runtime_address: usize,
    };

    pub const RuntimeState = struct {
        reported_buffer_count: u64,
        reported_buffer_bytes: u64,
        mapped_model_logical_bytes: u64,
        mapped_model_allocated_bytes: u64,
        scratch_in_use_slots: u64,
        scratch_pending_slots: u64,
        frame_retained_bytes: u64,
        graph_plan_active: u64,
        active_frame: bool,
        submitted_frame: bool,
    };

    pub const Snapshot = struct {
        identity: Identity,
        state: RuntimeState,
    };

    pub fn snapshot(session: Session, io: std.Io) !Snapshot {
        if (session.vtable != &arch_vtable) return error.NotArchSession;
        const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
        if (self.arch_config != .gliner_boundary) return error.NotGlinerBoundarySession;
        if (self.backend_type != .metal) return error.NotMetalSession;
        if (comptime !build_options.enable_metal) return error.MetalUnavailable;
        const data = gpuBackendData(self);
        // Same final lock as MetalCompute: cache load -> model execution ->
        // shared provider. An observer must not wait for or construct a user.
        if (!data.shared_metal_native_provider_lock.tryLock()) return error.QueueFull;
        defer data.shared_metal_native_provider_lock.unlock(io);
        const provider = data.shared_metal_native_provider orelse return error.MissingMetalNativeProvider;
        const device_runtime = provider.raw_decode_runtime orelse return error.MissingMetalRuntime;
        const store = data.tensor_store orelse return error.MissingTensorStore;
        const reader = store.singleSafetensorsReader() orelse return error.NotSingleSafetensorsStore;
        const mapping = reader.mmap_region orelse return error.ModelWeightsNotMapped;
        if (mapping.data.ptr != reader.file_bytes.ptr or mapping.data.len != reader.file_bytes.len)
            return error.ModelMappingMismatch;
        const state = metal_runtime.runtimeMemorySnapshot(device_runtime);
        return .{
            .identity = .{
                .store_address = @intFromPtr(store.ptr),
                .reader_address = @intFromPtr(reader),
                .mapping_address = @intFromPtr(mapping.data.ptr),
                .mapping_bytes = mapping.data.len,
                .source_bytes = reader.file_bytes.len,
                .source_tensor_count = reader.header.tensors.count(),
                .lazy_tensor_count = data.lazy_weights.count(),
                .provider_address = @intFromPtr(provider),
                .runtime_address = @intFromPtr(device_runtime),
            },
            .state = .{
                .reported_buffer_count = state.buffer_count,
                .reported_buffer_bytes = state.total_bytes,
                .mapped_model_logical_bytes = state.mapped_model_logical_bytes,
                .mapped_model_allocated_bytes = state.mapped_model_allocated_bytes,
                .scratch_in_use_slots = state.scratch_pool_in_use_slots,
                .scratch_pending_slots = state.scratch_pool_pending_slots,
                .frame_retained_bytes = state.frame_retained_bytes,
                .graph_plan_active = state.graph_plan_active,
                .active_frame = metal_runtime.hasActiveFrame(device_runtime),
                .submitted_frame = metal_runtime.hasSubmittedFrame(device_runtime),
            },
        };
    }
} else struct {};

test "gliner boundary persistent Metal owner observation rejects foreign cold and busy sessions without construction" {
    var unrelated: u8 = 0;
    var other_vtable = arch_vtable;
    other_vtable.independentBatchRows = null;
    try std.testing.expectError(error.NotArchSession, TestGlinerBoundaryMetalOwner.snapshot(.{
        .ptr = &unrelated,
        .vtable = &other_vtable,
    }, std.testing.io));
    var self = ArchSession{
        .allocator = std.testing.allocator,
        // Only the discriminator is observed; this is not a loaded model.
        .arch_config = .{ .gliner_boundary = std.mem.zeroes(gliner_boundary_model.Config) },
        .backend_type = .native,
        .backend_data = .{ .native = .{
            .allocator = std.testing.allocator,
            .resident_weights = .empty,
            .lazy_weights = .empty,
        } },
    };
    const session = Session{ .ptr = &self, .vtable = &arch_vtable };
    try std.testing.expectError(error.NotMetalSession, TestGlinerBoundaryMetalOwner.snapshot(session, std.testing.io));
    self.arch_config = .{ .gpt = .{
        .hidden_size = 4,
        .num_hidden_layers = 1,
        .num_attention_heads = 1,
        .intermediate_size = 8,
        .vocab_size = 16,
    } };
    try std.testing.expectError(error.NotGlinerBoundarySession, TestGlinerBoundaryMetalOwner.snapshot(session, std.testing.io));
    if (comptime build_options.enable_metal) {
        self.arch_config = .{ .gliner_boundary = std.mem.zeroes(gliner_boundary_model.Config) };
        self.backend_type = .metal;
        self.backend_data = .{ .metal = .{ .allocator = std.testing.allocator, .prefix = "", .lazy_weights = .empty } };
        const data = gpuBackendData(&self);
        try std.testing.expectError(error.MissingMetalNativeProvider, TestGlinerBoundaryMetalOwner.snapshot(session, std.testing.io));
        try std.testing.expect(data.shared_metal_native_provider == null);
        try std.testing.expect(data.shared_metal_native_provider_lock.tryLock());
        defer data.shared_metal_native_provider_lock.unlock(std.testing.io);
        try std.testing.expectError(error.QueueFull, TestGlinerBoundaryMetalOwner.snapshot(session, std.testing.io));
        try std.testing.expect(data.shared_metal_native_provider == null);
    }
}

pub fn getLayaConfig(session: Session) ?@import("../models/laya.zig").Config {
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return if (self.arch_config == .modern_bert) self.arch_config.modern_bert.laya else null;
}

fn acquireArchComputeBackend(ctx: *anyopaque, session: Session, allocator: std.mem.Allocator, options: @import("../backends/session.zig").ComputeBackendOptions) !ops.ComputeBackend {
    // Adapt the published owner to existing target construction while keeping
    // wrapper admission/interruptibility contracts on the acquisition request.
    var owner_session = session;
    owner_session.ptr = ctx;
    owner_session.vtable = &arch_vtable;
    owner_session.compute_backend_provider = null;
    if (options.borrow_shared_provider) {
        if (options.run_budget != null) return error.UnsupportedComputeProviderMode;
        return getComputeBackendBorrowingSharedProvider(owner_session, allocator);
    }
    if (options.run_budget) |budget| return getComputeBackendWithBudget(owner_session, allocator, budget);
    return getComputeBackend(owner_session, allocator);
}

const archComputeServices = @import("../backends/session.zig").ComputeBackendProvider.VTable{
    .acquire = acquireArchComputeBackend,
    .cold_target_services = &.{ .accepts = acceptsArchComputeServices, .snapshot = coldArchComputeTarget },
};
fn acceptsArchComputeServices(actual: *const @import("../backends/session.zig").ComputeBackendProvider.VTable) bool {
    return actual == &archComputeServices;
}
fn coldArchComputeTarget(raw: *anyopaque, _: Session, control: ?InferenceExecutionControl) !?@import("../backends/cold_compute_target.zig").Target {
    if (control) |active| try active.check();
    const self: *ArchSession = @ptrCast(@alignCast(raw));
    switch (self.backend_type) {
        .cuda => {
            if (comptime !build_options.enable_cuda) return null;
            const compute = self.backend_data.cuda.compute;
            const primary = compute.ctx.ctx orelse return null;
            const backend = compute.computeBackend();
            return .{ .cuda = .{ .implementation_context = backend.ptr, .implementation_table = backend.vtable, .primary_context = @ptrCast(primary), .device = compute.ctx.device } };
        },
        .metal => {
            if (comptime !build_options.enable_metal) return null;
            const io = self.io orelse return null;
            const data = gpuBackendData(self);
            if (!data.shared_metal_native_provider_lock.tryLock()) return error.QueueFull;
            defer data.shared_metal_native_provider_lock.unlock(io);
            const provider = data.shared_metal_native_provider orelse return null;
            const raw_runtime = provider.raw_decode_runtime orelse return null;
            return .{ .metal = .{ .runtime = @ptrCast(raw_runtime), .exclusion = &provider.operations.gate } };
        },
        else => return null,
    }
}

fn preparationRuntimeIdentity(raw: *anyopaque) !*anyopaque {
    if (comptime !build_options.enable_metal) return error.MetalNotEnabled;
    const self: *ArchSession = @ptrCast(@alignCast(raw));
    const provider = gpuBackendData(self).shared_metal_native_provider orelse return error.UnsupportedGlinerBoundaryDevice;
    return @ptrCast(provider.raw_decode_runtime orelse return error.UnsupportedGlinerBoundaryDevice);
}

fn createPreparationBackend(raw: *anyopaque, session: Session, allocator: std.mem.Allocator, begin_request: bool) !ops.ComputeBackend {
    if (begin_request) return getComputeBackend(session, allocator);
    const self: *ArchSession = @ptrCast(@alignCast(raw));
    return makeComputeBackend(self, allocator, null);
}

fn prepareArchResident(ctx: *anyopaque, published: Session, control: ?InferenceExecutionControl) !void {
    // Preserve the published admission/interruption contract while addressing
    // the actual model owner, rather than a transient wrapper implementation.
    var owner_session = published;
    owner_session.ptr = ctx;
    owner_session.vtable = &arch_vtable;
    try prepareGlinerBoundaryResidentImpl(owner_session, published, control);
    try prepareLayaResidentImpl(owner_session, published, control);
}

/// Publish architecture metadata once. Serving can combine this with format
/// metadata without depending on ArchSession's private implementation identity.
fn archSessionHandle(impl: *ArchSession) Session {
    impl.model_lifetime.owner = impl;
    const encoder = switch (impl.arch_config) {
        .bert, .modern_bert, .nomic_bert, .embedding_gemma2 => true,
        else => false,
    };
    return .{
        .model_lifetime = &impl.model_lifetime,
        .invocation_diagnostics = .{
            .diagnostics = &impl.diagnostics,
            .capture_programs = true,
            .model_allocator = switch (impl.backend_type) {
                .native => impl.backend_data.native.allocator,
                .pjrt => impl.backend_data.pjrt.native.allocator,
                else => null,
            },
            .stable_backend = if (comptime build_options.enable_cuda) if (impl.backend_type == .cuda) impl.backend_data.cuda.compute.computeBackend() else null else null,
        },
        .ptr = impl,
        .vtable = &arch_vtable,
        .compute_backend_provider = .{ .ptr = impl, .services = &archComputeServices },
        .resident_preparation = .{ .ptr = impl, .prepare = prepareArchResident },
        .model_contract = .{
            .task = impl.task,
            .decoder_config = if (impl.arch_config == .gpt) &impl.arch_config.gpt else null,
            .padding = if (encoder or impl.arch_config == .deberta or impl.arch_config == .gliner) .batch_max else .declared,
            .resident_text_embeddings = (encoder and (impl.backend_type == .metal or impl.backend_type == .cuda)) or
                (impl.arch_config == .embedding_gemma2 and impl.backend_type == .native),
        },
    };
}

pub fn widenBudgetLimitsForSession(
    session: Session,
    limits: runtime.tier.memory.Limits,
) runtime.tier.memory.Limits {
    if (session.generation_workspace) |workspace| return workspace.widenLimits(limits);
    if (session.vtable != &arch_vtable) return limits;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return widenLimits(limits, self.budget_floor);
}

/// Publish the load plan's serving policy only after its resident lease is
/// retained. The model owner keeps the session and reservation alive together.
pub fn configureReservedGenerationWorkspace(
    session: *Session,
    floor: runtime.tier.memory.Limits,
    resident: runtime.tier.memory.AdmissionAmounts,
) void {
    std.debug.assert(session.vtable == &arch_vtable);
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    std.debug.assert(self.backend_type == .cuda);
    std.debug.assert(self.generation_workspace == null);
    self.generation_workspace = .{
        .floor = floor,
        .capacity = .{
            .backend_kv_bytes = resident.backend_kv_bytes,
            .backend_scratch_bytes = resident.backend_scratch_bytes,
        },
    };
    session.generation_workspace = &self.generation_workspace.?;
}

/// Bind a session's lazy residency cache to the serving owner's hard limits
/// before the session is published. Cache defaults are derived from host/node
/// capacity during construction and can be much larger than a container or an
/// explicit serving policy; leaving those defaults in place lets lazy weight
/// promotion bypass the ModelManager/ResourceManager envelope.
pub fn configureSharedCacheAdmissionForSession(
    session: Session,
    allocator: std.mem.Allocator,
    controller: *runtime.tier.memory.AdmissionController,
    backend_class: runtime.tier.memory.BackendClass,
    limits: runtime.tier.memory.Limits,
    resident: runtime.tier.memory.AdmissionAmounts,
) !void {
    if (session.vtable != &arch_vtable) return;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    // Packed Laya trunk entries are charged as KV memory on the device that
    // holds them (models/laya/LAYA.md, "State cache").
    if (isPackedLaya(self)) if (layaTrunkCache(self)) |trunk_cache| trunk_cache.configureAdmission(.{
        .controller = controller,
        .backend_class = backend_class,
        .limits = limits,
        .device = self.backend_type == .metal,
    });
    const hard_budget = runtime.tier.cache.Budget{
        .host_limit_bytes = limits.host_limit_bytes,
        .backend_limit_bytes = limits.backend_limit_bytes,
    };
    const tier_cache: ?*runtime.tier.cache.SharedCache = switch (self.backend_type) {
        .native => if (self.backend_data.native.tier_cache) |*cache| cache else null,
        .metal => if (build_options.enable_metal)
            if (gpuBackendData(self).tier_cache) |*cache| cache else null
        else
            null,
        .pjrt => if (self.backend_data.pjrt.native.tier_cache) |*cache| cache else null,
        .cuda, .wasm => null,
    };
    const cache = tier_cache orelse return;
    cache.configureHardBudget(hard_budget);
    try cache.configureAdmission(
        allocator,
        controller,
        backend_class,
        limits,
        .{
            .host_limit_bytes = resident.host_weight_bytes,
            .backend_limit_bytes = resident.backend_weight_bytes,
        },
    );
}

/// Exact admission ownership held by a session's tier cache above the model
/// load lease baseline. This remains separate from the LoadedModel lease so a
/// cache can grow and evict entries without rewriting model ownership.
pub fn sharedCacheAdmissionAmounts(session: Session) runtime.tier.memory.AdmissionAmounts {
    if (session.vtable != &arch_vtable) return .{};
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    const tier_cache: ?*runtime.tier.cache.SharedCache = switch (self.backend_type) {
        .native => if (self.backend_data.native.tier_cache) |*cache| cache else null,
        .metal => if (build_options.enable_metal)
            if (gpuBackendData(self).tier_cache) |*cache| cache else null
        else
            null,
        .pjrt => if (self.backend_data.pjrt.native.tier_cache) |*cache| cache else null,
        .cuda, .wasm => null,
    };
    return if (tier_cache) |cache| cache.admissionAmounts() else .{};
}

/// Release one backend-owned, unpinned host-cache entry without unloading the
/// model identity. ModelManager uses this after an authoritative aggregate or
/// live-host denial, then re-probes admission before choosing another victim.
/// Unsupported backends return zero and retain their existing whole-model
/// eviction behavior.
pub fn reclaimOneHostCacheEntryForSession(session: Session) usize {
    if (session.vtable != &arch_vtable) return 0;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.backend_type) {
        .native => native_mod.reclaimOneHostCacheEntry(&self.backend_data.native),
        .pjrt => native_mod.reclaimOneHostCacheEntry(&self.backend_data.pjrt.native),
        .metal, .cuda, .wasm => 0,
    };
}

pub fn memoryBudgetExceededDetail(
    session: Session,
    run_budget: *const runtime.tier.memory.RunBudget,
    buf: []u8,
) ![]const u8 {
    if (run_budget.hasLastDenial()) return run_budget.lastDenialString(buf);
    if (session.vtable != &arch_vtable) {
        return std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{});
    }
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    return switch (self.backend_type) {
        .native => if (self.backend_data.native.tier_cache) |*tier_cache|
            tier_cache.lastDenialString(buf)
        else
            std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{}),
        .metal => if (build_options.enable_metal) {
            if (gpuBackendData(self).tier_cache) |*tier_cache| {
                return tier_cache.lastDenialString(buf);
            }
            return std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{});
        } else std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{}),
        .pjrt => if (self.backend_data.pjrt.native.tier_cache) |*tier_cache|
            tier_cache.lastDenialString(buf)
        else
            std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{}),
        .cuda => std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{}),
        .wasm => std.fmt.bufPrint(buf, "request exceeds native generation memory budget", .{}),
    };
}

pub fn attachSharedPrefetchState(session: Session, shared_prefetch: *runtime.tier.shared.SharedPrefetchState) !void {
    if (session.vtable != &arch_vtable) return error.NotArchSession;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    switch (self.backend_type) {
        .native => self.backend_data.native.shared_prefetch = shared_prefetch,
        .metal => if (build_options.enable_metal) {
            gpuBackendData(self).shared_prefetch = shared_prefetch;
        } else {
            return error.MetalNotEnabled;
        },
        .pjrt => self.backend_data.pjrt.native.shared_prefetch = shared_prefetch,
        .cuda => {},
        .wasm => {},
    }
}

fn isQwen3GenerativeRerankerFamily(family: gpt_arch.ModelFamily) bool {
    return family == .qwen3 or family == .qwen3_vl;
}

fn archHasLayaDecisions(ptr: *anyopaque) bool {
    if (comptime !build_options.enable_metal) return false;
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    return self.backend_type == .metal and self.arch_config == .modern_bert and self.arch_config.modern_bert.laya != null and !isPackedLaya(self) and @import("../ops/laya_metal.zig").enabledFor(self.arch_config.modern_bert.laya.?);
}

fn archRunLayaDecisions(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator, control: ?InferenceExecutionControl) !?[]Tensor {
    if (comptime !build_options.enable_metal) return null;
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    if (self.backend_type != .metal or self.arch_config != .modern_bert or self.arch_config.modern_bert.laya == null or isPackedLaya(self) or !@import("../ops/laya_metal.zig").enabledFor(self.arch_config.modern_bert.laya.?)) return null;
    if (inputs.len != 4) return error.InvalidLayaInputs;
    const bi = try parseBertRunInputs(inputs[0..2]);
    const kinds = try validateI64Matrix(inputs[2], .{ bi.batch, 1 });
    const markers = try validateI64Matrix(inputs[3], null);
    if (markers.shape[0] != bi.batch) return error.InvalidLayaInputs;
    var cb = try makeComputeBackend(self, allocator, null);
    cb.execution_control = control;
    defer cb.deinit();
    const compute: *MetalCompute = @ptrCast(@alignCast(cb.ptr));
    return try @import("../ops/laya_metal.zig").run(compute, allocator, self.arch_config.modern_bert, bi.input_ids, bi.attention_mask, kinds.values, markers.values, bi.batch, bi.seq_len, markers.shape[1], control, true);
}

/// Snapshot only while holding the session lifetime and exclusive execution access.
pub fn layaResidentStats(session: Session) ?@import("../ops/laya_metal.zig").Stats {
    if (comptime !build_options.enable_metal) return null;
    if (session.vtable != &arch_vtable) return null;
    const self: *ArchSession = @ptrCast(@alignCast(session.ptr));
    if (self.backend_type != .metal) return null;
    const owner = gpuBackendData(self).laya_resident orelse return null;
    return owner.snapshot();
}

fn runGlinerDecision(cb: *ops.ComputeBackend, allocator: std.mem.Allocator, encoder: gliner_encoder.Config, inputs: []const Tensor) ![]Tensor {
    var decision_positions_tensor: ?Tensor = null;
    var decision_mask_tensor: ?Tensor = null;
    for (inputs) |input| {
        if (std.mem.eql(u8, input.name, "decision_marker_positions")) {
            if (decision_positions_tensor != null) return error.DuplicateInputs;
            decision_positions_tensor = input;
        }
        if (std.mem.eql(u8, input.name, "decision_marker_mask")) {
            if (decision_mask_tensor != null) return error.DuplicateInputs;
            decision_mask_tensor = input;
        }
    }
    if ((decision_positions_tensor == null) != (decision_mask_tensor == null))
        return error.MissingInputs;
    if (decision_positions_tensor) |positions_tensor| {
        const marker_mask_tensor = decision_mask_tensor.?;
        var input_ids_tensor: ?Tensor = null;
        var attention_mask_tensor: ?Tensor = null;
        for (inputs) |input| {
            if (std.mem.eql(u8, input.name, "input_ids")) {
                if (input_ids_tensor != null) return error.DuplicateInputs;
                input_ids_tensor = input;
            }
            if (std.mem.eql(u8, input.name, "attention_mask")) {
                if (attention_mask_tensor != null) return error.DuplicateInputs;
                attention_mask_tensor = input;
            }
        }
        const ids_tensor = input_ids_tensor orelse return error.MissingInputs;
        const mask_tensor = attention_mask_tensor orelse return error.MissingInputs;
        if (ids_tensor.dtype != .i64 or mask_tensor.dtype != .i64 or
            positions_tensor.dtype != .i64 or marker_mask_tensor.dtype != .i64 or
            ids_tensor.shape.len != 2 or mask_tensor.shape.len != 2 or
            positions_tensor.shape.len != 2 or marker_mask_tensor.shape.len != 2 or
            !std.mem.eql(i64, ids_tensor.shape, mask_tensor.shape) or
            !std.mem.eql(i64, positions_tensor.shape, marker_mask_tensor.shape) or
            ids_tensor.shape[0] <= 0 or ids_tensor.shape[1] <= 0 or positions_tensor.shape[1] <= 0 or
            positions_tensor.shape[0] != ids_tensor.shape[0])
            return error.InvalidInputShape;
        const batch: usize = @intCast(ids_tensor.shape[0]);
        const seq_len: usize = @intCast(ids_tensor.shape[1]);
        const labels: usize = @intCast(positions_tensor.shape[1]);
        if (seq_len > @as(usize, encoder.geometry().max_position_embeddings)) return error.InvalidInputShape;
        const token_count = std.math.mul(usize, batch, seq_len) catch return error.InvalidInputShape;
        const marker_count = std.math.mul(usize, batch, labels) catch return error.InvalidInputShape;
        const token_bytes = std.math.mul(usize, token_count, @sizeOf(i64)) catch return error.InvalidInputShape;
        const marker_bytes = std.math.mul(usize, marker_count, @sizeOf(i64)) catch return error.InvalidInputShape;
        if (ids_tensor.data.len != token_bytes or mask_tensor.data.len != token_bytes or
            positions_tensor.data.len != marker_bytes or marker_mask_tensor.data.len != marker_bytes or
            !ids_tensor.isAlignedFor(i64) or !mask_tensor.isAlignedFor(i64) or
            !positions_tensor.isAlignedFor(i64) or !marker_mask_tensor.isAlignedFor(i64))
            return error.InvalidInputShape;
        const input_ids = ids_tensor.asInt64();
        const attention_mask = mask_tensor.asInt64();
        const marker_positions = positions_tensor.asInt64();
        const marker_mask = marker_mask_tensor.asInt64();
        for (marker_positions, marker_mask) |position, valid| {
            if (valid != 0 and valid != 1) return error.InvalidGlinerDecisionMarkerMask;
            if (valid == 1 and (position < 0 or position >= @as(i64, @intCast(seq_len)))) return error.InvalidGlinerDecisionMarkerPosition;
        }

        cb.preferEagerQuantMirrors(true);
        const hidden = switch (encoder) {
            .deberta => |cfg| try deberta_arch.forwardCt(cb, allocator, cfg, input_ids, attention_mask, batch, seq_len, true),
            .modern_bert => |cfg| try modern_bert_arch.forwardCT(cb, allocator, cfg, input_ids, attention_mask, batch, seq_len),
        };
        defer cb.free(hidden);
        const decision = try gliner_decision_head.forwardCt(
            cb,
            allocator,
            hidden,
            marker_positions,
            marker_mask,
            batch,
            seq_len,
            labels,
            encoder.geometry().hidden_size,
        );
        defer cb.free(decision.logits);
        const logits = try cb.toFloat32(decision.logits, allocator);
        defer allocator.free(logits);
        for (marker_mask, logits) |valid, *logit| if (valid == 0) {
            logit.* = -1.0e4;
        };
        const output_shape = [_]i64{ @intCast(batch), @intCast(labels) };
        var output = try Tensor.initFloat32(allocator, "logits", &output_shape, logits);
        errdefer output.deinit();
        const result = try allocator.alloc(Tensor, 1);
        result[0] = output;
        return result;
    }
    return error.MissingInputs;
}

fn archRun(ptr: *anyopaque, inputs: []const Tensor, allocator: std.mem.Allocator) ![]Tensor {
    return archRunImpl(ptr, inputs, allocator, null);
}

fn archRunWithControl(
    ptr: *anyopaque,
    inputs: []const Tensor,
    allocator: std.mem.Allocator,
    control: InferenceExecutionControl,
) ![]Tensor {
    return archRunImpl(ptr, inputs, allocator, control);
}

fn validateGlinerRightPaddedAttentionMask(mask: []const i64, batch: usize, seq_len: usize) !void {
    const total = std.math.mul(usize, batch, seq_len) catch return error.InvalidInputShape;
    if (batch == 0 or seq_len == 0 or mask.len != total) return error.InvalidInputShape;
    for (0..batch) |row| {
        var seen_padding = false;
        var real_tokens: usize = 0;
        for (mask[row * seq_len ..][0..seq_len]) |value| switch (value) {
            1 => {
                if (seen_padding) return error.InvalidGlinerAttentionMask;
                real_tokens += 1;
            },
            0 => seen_padding = true,
            else => return error.InvalidGlinerAttentionMask,
        };
        if (real_tokens == 0) return error.InvalidGlinerAttentionMask;
    }
}

test "GLiNER ModernBERT attention masks must be nonempty and right padded" {
    try validateGlinerRightPaddedAttentionMask(&.{ 1, 1, 0, 1, 0, 0 }, 2, 3);
    try std.testing.expectError(error.InvalidGlinerAttentionMask, validateGlinerRightPaddedAttentionMask(&.{ 1, 0, 1 }, 1, 3));
    try std.testing.expectError(error.InvalidGlinerAttentionMask, validateGlinerRightPaddedAttentionMask(&.{ 0, 0 }, 1, 2));
    try std.testing.expectError(error.InvalidGlinerAttentionMask, validateGlinerRightPaddedAttentionMask(&.{ 1, 2 }, 1, 2));
}

fn runModernBertGlinerDecision(
    cb: *ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: modern_bert_arch.Config,
    inputs: []const Tensor,
) ![]Tensor {
    var input_ids_tensor: ?Tensor = null;
    var attention_mask_tensor: ?Tensor = null;
    var positions_tensor: ?Tensor = null;
    var marker_mask_tensor: ?Tensor = null;
    for (inputs) |input| {
        if (std.mem.eql(u8, input.name, "input_ids")) {
            if (input_ids_tensor != null) return error.DuplicateInputs;
            input_ids_tensor = input;
        } else if (std.mem.eql(u8, input.name, "attention_mask")) {
            if (attention_mask_tensor != null) return error.DuplicateInputs;
            attention_mask_tensor = input;
        } else if (std.mem.eql(u8, input.name, "decision_marker_positions")) {
            if (positions_tensor != null) return error.DuplicateInputs;
            positions_tensor = input;
        } else if (std.mem.eql(u8, input.name, "decision_marker_mask")) {
            if (marker_mask_tensor != null) return error.DuplicateInputs;
            marker_mask_tensor = input;
        }
    }
    const ids_tensor = input_ids_tensor orelse return error.GlinerSpanExtractionRequiresSchema;
    const mask_tensor = attention_mask_tensor orelse return error.GlinerSpanExtractionRequiresSchema;
    const marker_positions_tensor = positions_tensor orelse return error.GlinerSpanExtractionRequiresSchema;
    const markers_valid_tensor = marker_mask_tensor orelse return error.GlinerSpanExtractionRequiresSchema;
    if (ids_tensor.dtype != .i64 or mask_tensor.dtype != .i64 or
        marker_positions_tensor.dtype != .i64 or markers_valid_tensor.dtype != .i64 or
        ids_tensor.shape.len != 2 or mask_tensor.shape.len != 2 or
        marker_positions_tensor.shape.len != 2 or markers_valid_tensor.shape.len != 2 or
        !std.mem.eql(i64, ids_tensor.shape, mask_tensor.shape) or
        !std.mem.eql(i64, marker_positions_tensor.shape, markers_valid_tensor.shape) or
        ids_tensor.shape[0] <= 0 or ids_tensor.shape[1] <= 0 or marker_positions_tensor.shape[1] <= 0 or
        marker_positions_tensor.shape[0] != ids_tensor.shape[0])
        return error.InvalidInputShape;
    const batch: usize = @intCast(ids_tensor.shape[0]);
    const seq_len: usize = @intCast(ids_tensor.shape[1]);
    const labels: usize = @intCast(marker_positions_tensor.shape[1]);
    if (seq_len > @as(usize, cfg.max_position_embeddings)) return error.InvalidInputShape;
    if (cb.kind() == .cuda and seq_len > 8192) return error.UnsupportedModernBertCudaSequenceLength;
    const token_count = std.math.mul(usize, batch, seq_len) catch return error.InvalidInputShape;
    const marker_count = std.math.mul(usize, batch, labels) catch return error.InvalidInputShape;
    const token_bytes = std.math.mul(usize, token_count, @sizeOf(i64)) catch return error.InvalidInputShape;
    const marker_bytes = std.math.mul(usize, marker_count, @sizeOf(i64)) catch return error.InvalidInputShape;
    if (ids_tensor.data.len != token_bytes or mask_tensor.data.len != token_bytes or
        marker_positions_tensor.data.len != marker_bytes or markers_valid_tensor.data.len != marker_bytes or
        !ids_tensor.isAlignedFor(i64) or !mask_tensor.isAlignedFor(i64) or
        !marker_positions_tensor.isAlignedFor(i64) or !markers_valid_tensor.isAlignedFor(i64))
        return error.InvalidInputShape;
    const input_ids = ids_tensor.asInt64();
    const attention_mask = mask_tensor.asInt64();
    const marker_positions = marker_positions_tensor.asInt64();
    const marker_mask = markers_valid_tensor.asInt64();
    try validateGlinerRightPaddedAttentionMask(attention_mask, batch, seq_len);
    for (marker_positions, marker_mask, 0..) |position, valid, index| {
        if (valid != 0 and valid != 1) return error.InvalidGlinerDecisionMarkerMask;
        if (valid == 1) {
            if (position < 0 or position >= @as(i64, @intCast(seq_len)))
                return error.InvalidGlinerDecisionMarkerPosition;
            const row = index / labels;
            const token = std.math.add(usize, std.math.mul(usize, row, seq_len) catch return error.InvalidInputShape, @intCast(position)) catch return error.InvalidInputShape;
            if (attention_mask[token] != 1) return error.InvalidGlinerDecisionMarkerPosition;
        }
    }

    cb.preferEagerQuantMirrors(true);
    const hidden = try modern_bert_arch.forwardCT(cb, allocator, cfg, input_ids, attention_mask, batch, seq_len);
    defer cb.free(hidden);
    const decision = try gliner_decision_head.forwardCt(cb, allocator, hidden, marker_positions, marker_mask, batch, seq_len, labels, cfg.hidden_size);
    defer cb.free(decision.logits);
    const logits = try cb.toFloat32(decision.logits, allocator);
    defer allocator.free(logits);
    for (marker_mask, logits) |valid, *logit| if (valid == 0) {
        logit.* = -1.0e4;
    };
    const output_shape = [_]i64{ @intCast(batch), @intCast(labels) };
    var output = try Tensor.initFloat32(allocator, "logits", &output_shape, logits);
    errdefer output.deinit();
    const result = try allocator.alloc(Tensor, 1);
    result[0] = output;
    return result;
}

fn archRunImpl(
    ptr: *anyopaque,
    inputs: []const Tensor,
    allocator: std.mem.Allocator,
    control: ?InferenceExecutionControl,
) ![]Tensor {
    if (control) |active| try active.check();
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    const debug_cuda_session = platform.env.getenvBool("ANTFLY_INFERENCE_DEBUG_CUDA_SESSION");
    if (debug_cuda_session) std.log.info("arch-run: start backend={s}", .{@tagName(self.backend_type)});

    // Create the appropriate ComputeBackend
    var cb = try makeComputeBackend(self, allocator, null);
    cb.execution_control = control;
    if (debug_cuda_session) std.log.info("arch-run: compute backend made kind={s}", .{@tagName(cb.kind())});
    defer cb.deinit();

    // Dispatch based on architecture
    switch (self.arch_config) {
        .bert => |cfg| {
            const bert_inputs = try parseBertRunInputs(inputs);
            const batch = bert_inputs.batch;
            const seq_len = bert_inputs.seq_len;
            const hidden = try bert_arch.forwardWithControl(
                &cb,
                allocator,
                cfg,
                bert_inputs.input_ids,
                bert_inputs.attention_mask,
                bert_inputs.token_type_ids,
                batch,
                seq_len,
                control,
            );
            defer allocator.free(hidden);

            if (self.task == .classifier) {
                const logits = try runBertSequenceClassifier(&cb, allocator, cfg, hidden, batch, seq_len);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }
            if (self.task == .extractor) {
                const logits = try runTokenClassifier(&cb, allocator, hidden, batch, seq_len, cfg.hidden_size, cfg.num_labels);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            const H = cfg.hidden_size;
            const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(H) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .embedding_gemma2 => |cfg| {
            if (self.task != .generic) return error.UnsupportedArchitectureTask;
            if (self.backend_type == .cuda) {
                const resident = cfg.resident orelse return error.UnsupportedEmbeddingGemma2Config;
                const request = try parseEmbeddingGemma2Inputs(inputs, resident);
                const values = if (comptime build_options.enable_cuda) try self.backend_data.cuda.compute.runEmbeddingGemma2(
                    allocator,
                    resident,
                    switch (request.input) {
                        .token_ids => |ids| .{ .token_ids = ids },
                        .input_embeddings => |embeddings| .{ .input_embeddings = embeddings },
                    },
                    request.attention_mask,
                    request.batch,
                    request.sequence,
                    resident.outputDim(),
                    control,
                ) else return error.CudaNotEnabled;
                defer allocator.free(values);
                try validateEmbeddingGemma2Output(values, request.batch, resident.outputDim());
                var tensor = try Tensor.initFloat32(allocator, "sentence_embedding", &.{ @intCast(request.batch), @intCast(resident.outputDim()) }, values);
                errdefer tensor.deinit();
                const outputs = try allocator.alloc(Tensor, 1);
                outputs[0] = tensor;
                return outputs;
            }
            const bi = try parseBertRunInputs(inputs);
            const result_ct = try embedding_gemma2_arch.forwardCT(&cb, allocator, cfg, bi.input_ids, bi.attention_mask, bi.batch, bi.seq_len);
            defer cb.free(result_ct);
            const values = try cb.toFloat32(result_ct, allocator);
            defer allocator.free(values);
            var tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &.{ @intCast(bi.batch), @intCast(bi.seq_len), @intCast(cfg.embedding_dim) }, values);
            errdefer tensor.deinit();
            const outputs = try allocator.alloc(Tensor, 1);
            outputs[0] = tensor;
            return outputs;
        },
        .modern_bert => |cfg| {
            if (cfg.laya) |laya| {
                if (laya.packing.enabled()) return @import("../architectures/laya_packed.zig").run(&cb, allocator, cfg, inputs, layaTrunkCache(self));
                if (inputs.len != 4) return error.InvalidLayaInputs;
                const bi = try parseBertRunInputs(inputs[0..2]);
                const kinds = try validateI64Matrix(inputs[2], .{ bi.batch, 1 });
                const markers = try validateI64Matrix(inputs[3], null);
                if (markers.shape[0] != bi.batch) return error.InvalidLayaInputs;
                if (comptime build_options.enable_metal) {
                    if (self.backend_type == .metal and @import("../ops/laya_metal.zig").enabledFor(cfg.laya.?)) {
                        const compute: *MetalCompute = @ptrCast(@alignCast(cb.ptr));
                        return @import("../ops/laya_metal.zig").run(compute, allocator, cfg, bi.input_ids, bi.attention_mask, kinds.values, markers.values, bi.batch, bi.seq_len, markers.shape[1], control, false);
                    }
                }
                const hidden = try modern_bert_arch.forwardCT(&cb, allocator, cfg, bi.input_ids, bi.attention_mask, bi.batch, bi.seq_len);
                defer cb.free(hidden);
                return @import("../architectures/laya_head.zig").forward(&cb, allocator, laya, hidden, bi.attention_mask, kinds.values, markers.values, bi.batch, bi.seq_len, markers.shape[1], cfg.hidden_size);
            }
            if (self.task == .extractor and self.gliner_span_encoder_family == .modern_bert)
                return runModernBertGlinerDecision(&cb, allocator, cfg, inputs);
            if (self.task != .generic) return error.UnsupportedArchitectureTask;
            const bert_inputs = try parseBertRunInputs(inputs);
            const hidden = try modern_bert_arch.forward(
                &cb,
                allocator,
                cfg,
                bert_inputs.input_ids,
                bert_inputs.attention_mask,
                bert_inputs.batch,
                bert_inputs.seq_len,
            );
            defer allocator.free(hidden);

            const shape = [_]i64{ @intCast(bert_inputs.batch), @intCast(bert_inputs.seq_len), @intCast(cfg.hidden_size) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();
            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .nomic_bert => |cfg| {
            if (self.task != .generic) return error.UnsupportedArchitectureTask;
            const bert_inputs = try parseBertRunInputs(inputs);
            const hidden = try nomic_bert_arch.forward(
                &cb,
                allocator,
                cfg,
                bert_inputs.input_ids,
                bert_inputs.attention_mask,
                bert_inputs.token_type_ids,
                bert_inputs.batch,
                bert_inputs.seq_len,
            );
            defer allocator.free(hidden);

            const shape = [_]i64{ @intCast(bert_inputs.batch), @intCast(bert_inputs.seq_len), @intCast(cfg.hidden_size) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();
            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .layoutlmv3 => |cfg| {
            if (inputs.len < 3) return error.MissingInputs;
            const input_ids_tensor = inputs[0];
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();
            const attention_mask = inputs[1].asInt64();
            const bbox = inputs[2].asInt64();
            const token_type_ids: ?[]const i64 = if (inputs.len > 3 and std.mem.eql(u8, inputs[3].name, "token_type_ids"))
                inputs[3].asInt64()
            else
                null;
            const pixel_values: ?[]const f32 = blk: {
                for (inputs[3..]) |tensor| {
                    if (tensor.dtype == .f32 and std.mem.eql(u8, tensor.name, "pixel_values")) break :blk tensor.asFloat32();
                }
                break :blk null;
            };
            const forward_out = try layoutlmv3_arch.forward(&cb, allocator, cfg, input_ids, attention_mask, token_type_ids, bbox, pixel_values, batch, seq_len);
            defer allocator.free(forward_out.hidden);
            const total_seq_len = forward_out.seq_len;

            if (self.task == .classifier) {
                const cls_embeddings = try extractClsEmbeddings(allocator, forward_out.hidden, batch, total_seq_len, cfg.hidden_size);
                defer allocator.free(cls_embeddings);
                const bert_cfg = bert.Config{
                    .model_type = .roberta,
                    .weight_prefix = "",
                    .vocab_size = cfg.vocab_size,
                    .hidden_size = cfg.hidden_size,
                    .num_hidden_layers = cfg.num_hidden_layers,
                    .num_attention_heads = cfg.num_attention_heads,
                    .intermediate_size = cfg.intermediate_size,
                    .max_position_embeddings = cfg.max_position_embeddings,
                    .type_vocab_size = cfg.type_vocab_size,
                    .num_labels = cfg.num_labels,
                };
                const logits = try runRobertaClassifier(&cb, allocator, bert_cfg, cls_embeddings, batch, cfg.hidden_size);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }
            if (self.task == .extractor) {
                const logits = try runTokenClassifier(&cb, allocator, forward_out.hidden, batch, total_seq_len, cfg.hidden_size, cfg.num_labels);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(total_seq_len), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            const H = cfg.hidden_size;
            const shape = [_]i64{ @intCast(batch), @intCast(total_seq_len), @intCast(H) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, forward_out.hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .deberta => |cfg| {
            if (inputs.len < 2) return error.MissingInputs;
            const input_ids_tensor = inputs[0];
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();
            const attention_mask = inputs[1].asInt64();

            if (self.task == .classifier) {
                // Long-sequence DeBERTa rerankers need the same bounded F16
                // mirrors as GLiNER to stay on the resident fused-layer path.
                // Non-Metal backends ignore this preference, and the existing
                // TERMITE_METAL_DISABLE_DEBERTA_WEIGHT_MIRRORS switch opts out.
                const prefer_weight_mirrors = debertaRerankerPrefersWeightMirrors(
                    self.deberta_reranker_weight_mirrors,
                    batch,
                    seq_len,
                );
                const hidden_ct = try deberta_arch.forwardCt(&cb, allocator, cfg, input_ids, attention_mask, batch, seq_len, prefer_weight_mirrors);
                defer cb.free(hidden_ct);

                const logits = try runDebertaSequenceClassifierCt(&cb, allocator, cfg, hidden_ct, batch, seq_len);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            const hidden = try deberta_arch.forward(&cb, allocator, cfg, input_ids, attention_mask, batch, seq_len);
            defer allocator.free(hidden);

            if (self.task == .extractor) {
                const logits = try runTokenClassifier(&cb, allocator, hidden, batch, seq_len, cfg.hidden_size, cfg.num_labels);
                defer allocator.free(logits);

                const logits_shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(cfg.num_labels) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &logits_shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            const H = cfg.hidden_size;
            const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(H) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .t5 => |cfg| {
            if (inputs.len < 2) return error.MissingInputs;
            const input_ids_tensor = inputs[0];
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();
            const attention_mask = inputs[1].asInt64();
            // For T5, the session factory creates separate encoder and decoder sessions.
            // When called as a single session, run the encoder.
            const hidden = try t5_arch.encoderForward(&cb, allocator, cfg, input_ids, attention_mask, batch, seq_len);
            defer allocator.free(hidden);

            const d_model = cfg.d_model;
            const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(d_model) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .gpt => |cfg| {
            if (inputs.len < 1) return error.MissingInputs;
            const input_ids_tensor = inputs[0];
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();

            if (self.task == .classifier and isQwen3GenerativeRerankerFamily(cfg.family)) {
                if (inputs.len < 2 or !std.mem.eql(u8, inputs[1].name, "attention_mask")) {
                    return error.MissingInputs;
                }
                const attention_mask = inputs[1].asInt64();
                const logits = try gpt_arch.qwen3RerankerLogits(
                    &cb,
                    allocator,
                    cfg,
                    input_ids,
                    attention_mask,
                    batch,
                    seq_len,
                );
                defer allocator.free(logits);
                const shape = [_]i64{ @intCast(batch), 1 };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &shape, logits);
                errdefer output_tensor.deinit();
                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            // Return hidden states (not logits) — used for embedding extraction.
            const hidden = try gpt_arch.hiddenForward(&cb, allocator, cfg, input_ids, batch, seq_len, null);
            defer allocator.free(hidden);

            const H = cfg.hidden_size;
            const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(H) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .whisper => |cfg| {
            if (inputs.len < 1) return error.MissingInputs;
            const first = inputs[0];

            if (first.dtype == .f32 and std.mem.eql(u8, first.name, "input_features")) {
                if (first.shape.len != 3) return error.InvalidInputShape;
                const batch: usize = @intCast(first.shape[0]);
                const n_mels: usize = @intCast(first.shape[1]);
                const time_steps: usize = @intCast(first.shape[2]);
                if (n_mels != cfg.num_mel_bins) return error.InvalidInputShape;

                const mel_shape = [_]i32{
                    @intCast(batch),
                    @intCast(n_mels),
                    @intCast(time_steps),
                };
                const mel_ct = try cb.fromFloat32Shape(first.asFloat32(), &mel_shape);
                defer cb.free(mel_ct);

                const hidden = try whisper_arch.encoderForward(&cb, allocator, cfg, mel_ct, batch, time_steps);
                defer allocator.free(hidden);

                const enc_seq = (time_steps + 2 * 1 - 3) / 2 + 1;
                const shape = [_]i64{ @intCast(batch), @intCast(enc_seq), @intCast(cfg.d_model) };
                var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            if (inputs.len < 2) return error.MissingInputs;
            const input_ids_tensor = first;
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const encoder_hidden_tensor = inputs[1];
            if (encoder_hidden_tensor.dtype != .f32 or encoder_hidden_tensor.shape.len != 3) return error.InvalidInputShape;

            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const dec_seq: usize = @intCast(input_ids_tensor.shape[1]);
            const enc_batch: usize = @intCast(encoder_hidden_tensor.shape[0]);
            const enc_seq: usize = @intCast(encoder_hidden_tensor.shape[1]);
            const enc_hidden_size: usize = @intCast(encoder_hidden_tensor.shape[2]);
            if (enc_batch != batch or enc_hidden_size != cfg.d_model) return error.InvalidInputShape;

            const encoder_hidden_shape = [_]i32{
                @intCast(batch),
                @intCast(enc_seq),
                @intCast(enc_hidden_size),
            };
            const encoder_hidden = try cb.fromFloat32Shape(encoder_hidden_tensor.asFloat32(), &encoder_hidden_shape);
            defer cb.free(encoder_hidden);

            const encoder_mask = try allocator.alloc(i64, batch * enc_seq);
            defer allocator.free(encoder_mask);
            @memset(encoder_mask, 1);

            const logits = try whisper_arch.decoderForward(
                &cb,
                allocator,
                cfg,
                input_ids_tensor.asInt64(),
                encoder_hidden,
                encoder_mask,
                batch,
                dec_seq,
                enc_seq,
            );
            defer allocator.free(logits);

            const shape = [_]i64{ @intCast(batch), @intCast(dec_seq), @intCast(cfg.vocab_size) };
            var output_tensor = try Tensor.initFloat32(allocator, "logits", &shape, logits);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .florence => |cfg| {
            if (debug_cuda_session) std.log.info("arch-run: florence branch inputs={d}", .{inputs.len});
            if (inputs.len < 1) return error.MissingInputs;
            const first = inputs[0];

            if (first.dtype == .f32 and std.mem.eql(u8, first.name, "pixel_values")) {
                if (first.shape.len != 4) return error.InvalidInputShape;
                const batch: usize = @intCast(first.shape[0]);
                const channels: usize = @intCast(first.shape[1]);
                const height: usize = @intCast(first.shape[2]);
                const width: usize = @intCast(first.shape[3]);
                if (channels != 3 or height != cfg.image_size or width != cfg.image_size) return error.InvalidInputShape;

                var prompt_ids: []const i64 = &.{};
                var prompt_seq_len: usize = 0;
                if (inputs.len > 1) {
                    const prompt_tensor = inputs[1];
                    if (prompt_tensor.dtype != .i64 or prompt_tensor.shape.len != 2) return error.InvalidInputShape;
                    const prompt_batch: usize = @intCast(prompt_tensor.shape[0]);
                    if (prompt_batch != batch) return error.InvalidInputShape;
                    prompt_seq_len = @intCast(prompt_tensor.shape[1]);
                    prompt_ids = prompt_tensor.asInt64();
                }

                if (debug_cuda_session) std.log.info("arch-run: florence encoder start batch={d} prompt_seq={d}", .{ batch, prompt_seq_len });
                const encoder = try florence_arch.encoderForward(
                    &cb,
                    allocator,
                    cfg,
                    first.asFloat32(),
                    batch,
                    prompt_ids,
                    prompt_seq_len,
                );
                if (debug_cuda_session) std.log.info("arch-run: florence encoder done seq={d}", .{encoder.seq_len});
                defer allocator.free(encoder.hidden);

                const shape = [_]i64{ @intCast(batch), @intCast(encoder.seq_len), @intCast(cfg.d_model) };
                var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, encoder.hidden);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            const input_ids_tensor = first;
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();

            var encoder_hidden_tensor: ?Tensor = null;
            var encoder_mask_tensor: ?Tensor = null;
            for (inputs[1..]) |tensor| {
                if (tensor.dtype == .f32 and tensor.shape.len == 3 and
                    (std.mem.eql(u8, tensor.name, "encoder_hidden_states") or encoder_hidden_tensor == null))
                {
                    encoder_hidden_tensor = tensor;
                } else if (tensor.dtype == .i64 and tensor.shape.len == 2 and
                    (std.mem.eql(u8, tensor.name, "encoder_attention_mask") or encoder_mask_tensor == null))
                {
                    encoder_mask_tensor = tensor;
                }
            }

            if (encoder_hidden_tensor) |enc_tensor| {
                const enc_batch: usize = @intCast(enc_tensor.shape[0]);
                const enc_seq: usize = @intCast(enc_tensor.shape[1]);
                const enc_hidden_size: usize = @intCast(enc_tensor.shape[2]);
                if (enc_batch != batch or enc_hidden_size != cfg.d_model) return error.InvalidInputShape;

                const encoder_hidden_shape = [_]i32{
                    @intCast(batch),
                    @intCast(enc_seq),
                    @intCast(enc_hidden_size),
                };
                const encoder_hidden = try cb.fromFloat32Shape(enc_tensor.asFloat32(), &encoder_hidden_shape);
                defer cb.free(encoder_hidden);

                const encoder_mask = try if (encoder_mask_tensor) |mask_tensor|
                    allocator.dupe(i64, mask_tensor.asInt64())
                else blk: {
                    const mask = try allocator.alloc(i64, batch * enc_seq);
                    @memset(mask, 1);
                    break :blk mask;
                };
                defer allocator.free(encoder_mask);

                const logits = try florence_arch.decoderForward(
                    &cb,
                    allocator,
                    cfg,
                    input_ids,
                    encoder_hidden,
                    encoder_mask,
                    batch,
                    seq_len,
                    enc_seq,
                );
                defer allocator.free(logits);

                const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(cfg.vocab_size) };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &shape, logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            // When called as a text-only session, run the BART decoder in
            // hidden-state mode for embedding-style use.
            const hidden = try florence_arch.decoderHiddenForward(&cb, allocator, cfg, input_ids, batch, seq_len);
            defer allocator.free(hidden);

            const d_model = cfg.d_model;
            const shape = [_]i64{ @intCast(batch), @intCast(seq_len), @intCast(d_model) };
            var output_tensor = try Tensor.initFloat32(allocator, "last_hidden_state", &shape, hidden);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .clip => |cfg| {
            if (inputs.len < 1) return error.MissingInputs;
            const first = inputs[0];

            if (first.dtype == .f32 and std.mem.eql(u8, first.name, "pixel_values")) {
                if (first.shape.len != 4) return error.InvalidInputShape;
                const batch: usize = @intCast(first.shape[0]);
                const use_graph_runtime = graphRuntimeStrategyEnabled(self.graph_runtime_strategy);
                const embeddings = if (use_graph_runtime)
                    try clip_graph.runVisionGraph(&cb, allocator, cfg, first.asFloat32(), batch, self.graph_runtime_strategy.?)
                else
                    try clip_arch.visionEncoderForward(&cb, allocator, cfg, first.asFloat32(), batch);
                defer allocator.free(embeddings);

                const proj_dim = cfg.projection_dim;
                const shape = [_]i64{ @intCast(batch), @intCast(proj_dim) };
                var output_tensor = try Tensor.initFloat32(allocator, "image_embeds", &shape, embeddings);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            if (inputs.len < 2) return error.MissingInputs;
            const input_ids_tensor = first;
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();

            const use_graph_runtime = graphRuntimeStrategyEnabled(self.graph_runtime_strategy);
            const embeddings = if (use_graph_runtime)
                try clip_graph.runTextGraph(&cb, allocator, cfg, input_ids, batch, seq_len, self.graph_runtime_strategy.?)
            else
                try clip_arch.textEncoderForward(&cb, allocator, cfg, input_ids, batch, seq_len);
            defer allocator.free(embeddings);

            const proj_dim = cfg.projection_dim;
            const shape = [_]i64{ @intCast(batch), @intCast(proj_dim) };
            var output_tensor = try Tensor.initFloat32(allocator, "text_embeds", &shape, embeddings);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .clap => |cfg| {
            if (inputs.len < 1) return error.MissingInputs;
            const first = inputs[0];
            if (first.dtype == .f32 and std.mem.eql(u8, first.name, "input_features")) {
                if (first.shape.len != 4) return error.InvalidInputShape;
                const batch: usize = @intCast(first.shape[0]);
                const channels: usize = @intCast(first.shape[1]);
                const time_frames: usize = @intCast(first.shape[2]);
                const mel_bins: usize = @intCast(first.shape[3]);
                const is_longer = if (inputs.len > 1 and inputs[1].dtype == .bool_) inputs[1].data else &[_]u8{};

                const use_graph_runtime = graphRuntimeStrategyEnabled(self.graph_runtime_strategy);
                const embeddings = if (use_graph_runtime)
                    try clap_arch.audioEncoderForwardGraphTail(&cb, allocator, cfg, first.asFloat32(), batch, channels, time_frames, mel_bins, is_longer, self.graph_runtime_strategy.?)
                else
                    try clap_arch.audioEncoderForward(&cb, allocator, cfg, first.asFloat32(), batch, channels, time_frames, mel_bins, is_longer);
                defer allocator.free(embeddings);

                const shape = [_]i64{ @intCast(batch), @intCast(cfg.projection_dim) };
                var output_tensor = try Tensor.initFloat32(allocator, "audio_embeds", &shape, embeddings);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }
            if (inputs.len < 2) return error.MissingInputs;
            if (first.shape.len != 2) return error.InvalidInputShape;

            const batch: usize = @intCast(first.shape[0]);
            const seq_len: usize = @intCast(first.shape[1]);
            const input_ids = first.asInt64();
            const attention_mask = inputs[1].asInt64();
            const token_type_ids: ?[]const i64 = if (inputs.len > 2) inputs[2].asInt64() else null;

            const embeddings = try clap_arch.textEncoderForward(&cb, allocator, cfg, input_ids, attention_mask, token_type_ids, batch, seq_len);
            defer allocator.free(embeddings);

            const shape = [_]i64{ @intCast(batch), @intCast(cfg.projection_dim) };
            var output_tensor = try Tensor.initFloat32(allocator, "text_embeds", &shape, embeddings);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
        .gliner_boundary => return error.BoundaryExtractionRequiresSchema,
        .gliner_modern_bert => |cfg| return runGlinerDecision(&cb, allocator, .{ .modern_bert = cfg }, inputs),
        .gliner => |cfg| {
            var decision_positions_tensor: ?Tensor = null;
            var decision_mask_tensor: ?Tensor = null;
            for (inputs) |input| {
                if (std.mem.eql(u8, input.name, "decision_marker_positions")) {
                    if (decision_positions_tensor != null) return error.DuplicateInputs;
                    decision_positions_tensor = input;
                }
                if (std.mem.eql(u8, input.name, "decision_marker_mask")) {
                    if (decision_mask_tensor != null) return error.DuplicateInputs;
                    decision_mask_tensor = input;
                }
            }
            if ((decision_positions_tensor == null) != (decision_mask_tensor == null))
                return error.MissingInputs;
            if (decision_positions_tensor != null) {
                if (!cfg.label_marker_decision_head) return error.UnsupportedGlinerDecisionHead;
                return runGlinerDecision(&cb, allocator, .{ .deberta = cfg }, inputs);
            }
            // GLiNER2: DeBERTa encoder + span classification head
            // Inputs: input_ids, attention_mask, words_mask, span_idx
            if (inputs.len < 4) return error.MissingInputs;
            const input_ids_tensor = inputs[0];
            if (input_ids_tensor.shape.len != 2) return error.InvalidInputShape;
            const batch: usize = @intCast(input_ids_tensor.shape[0]);
            const seq_len: usize = @intCast(input_ids_tensor.shape[1]);
            const input_ids = input_ids_tensor.asInt64();
            const attention_mask = inputs[1].asInt64();
            const words_mask = inputs[2].asInt64();
            const span_idx = inputs[3].asInt64();

            // The graph head implements only CountLSTMv2; v1 checkpoints run eagerly.
            const use_graph_runtime = graphRuntimeStrategyEnabled(self.graph_runtime_strategy) and
                cfg.gliner_count_layer == .count_lstm_v2;
            if (use_graph_runtime) {
                const head_cfg = gliner_head_graph.Config{
                    .hidden_size = cfg.hidden_size,
                    .classification_token_id = cfg.classification_token_id,
                    .entity_token_id = cfg.entity_token_id,
                    .relation_token_id = cfg.relation_token_id,
                };
                const graph_result = try gliner_head_graph.runFullGraph(
                    &cb,
                    allocator,
                    cfg,
                    head_cfg,
                    input_ids,
                    attention_mask,
                    words_mask,
                    span_idx,
                    batch,
                    seq_len,
                    self.graph_runtime_strategy.?,
                );
                defer allocator.free(graph_result.logits);

                // Output shape mirrors the eager path: [batch, num_words,
                // max_width, num_labels].  num_words / num_labels come from
                // the graph result; max_width is derived the same way the
                // eager `getSpanInfo` does (num_spans / num_words, default 8
                // when num_words == 0).
                const max_width: usize = if (graph_result.num_words == 0)
                    8
                else
                    @as(usize, @intCast(graph_result.num_spans)) / @as(usize, @intCast(graph_result.num_words));
                const shape = [_]i64{
                    @intCast(batch),
                    @intCast(graph_result.num_words),
                    @intCast(max_width),
                    @intCast(graph_result.num_labels),
                };
                var output_tensor = try Tensor.initFloat32(allocator, "logits", &shape, graph_result.logits);
                errdefer output_tensor.deinit();

                const result = try allocator.alloc(Tensor, 1);
                result[0] = output_tensor;
                return result;
            }

            // The GLiNER span head and DeBERTa encoder run encoder-shaped
            // quantized GEMMs. Bounded f16 mirrors avoid per-row quantized
            // setup while leaving other model sessions unchanged.
            cb.preferEagerQuantMirrors(true);
            const hidden = try deberta_arch.forwardCt(&cb, allocator, cfg, input_ids, attention_mask, batch, seq_len, deberta_mod.glinerPrefersWeightMirrors(cfg));
            defer cb.free(hidden);

            // Eager head path -- keeps the encoder/head boundary on the
            // backend (CT) so we skip the toFloat32 + fromFloat32Shape
            // round-trip the legacy []f32-typed APIs do. One Metal frame
            // amortizes command-buffer commits across the resident head;
            // host-reading ops such as the label GRU drain and reopen it.
            var head_frame_active = false;
            if (cb.kind() == .metal and !cb.decoderRuntimeHasActiveFrame()) {
                head_frame_active = cb.decoderRuntimeBeginFrame() catch false;
            }
            errdefer if (head_frame_active) cb.decoderRuntimeCancelFrame() catch {};
            const head_result = try gliner_head.forwardCtWithLabelMarkers(&cb, allocator, hidden, input_ids, words_mask, span_idx, batch, seq_len, cfg.hidden_size, .{
                .classification = cfg.classification_token_id,
                .entity = cfg.entity_token_id,
                .relation = cfg.relation_token_id,
                .count_layer = switch (cfg.gliner_count_layer) {
                    .count_lstm => .count_lstm,
                    .count_lstm_v2 => .count_lstm_v2,
                },
            });
            defer cb.free(head_result.logits);
            if (head_frame_active) {
                try cb.decoderRuntimeSubmitAndWaitFrame();
                head_frame_active = false;
            }

            const logits_f32 = if (head_result.num_labels == 0)
                try allocator.alloc(f32, 0)
            else
                try cb.toFloat32(head_result.logits, allocator);

            const shape = [_]i64{
                @intCast(batch),
                @intCast(head_result.num_words),
                @intCast(head_result.max_width),
                @intCast(head_result.num_labels),
            };
            var output_tensor = try Tensor.initFloat32(allocator, "logits", &shape, logits_f32);
            allocator.free(logits_f32);
            errdefer output_tensor.deinit();

            const result = try allocator.alloc(Tensor, 1);
            result[0] = output_tensor;
            return result;
        },
    }
}

fn runBertSequenceClassifier(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: bert.Config,
    hidden: []const f32,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const H: usize = @intCast(cfg.hidden_size);
    const cls_embeddings = try extractClsEmbeddings(allocator, hidden, batch, seq_len, H);
    defer allocator.free(cls_embeddings);

    return switch (cfg.model_type) {
        .distilbert => runDistilBertClassifier(cb, allocator, cfg, cls_embeddings, batch, H),
        .roberta => runRobertaClassifier(cb, allocator, cfg, cls_embeddings, batch, H),
        .bert => runBertClassifier(cb, allocator, cfg, cls_embeddings, batch, H),
    };
}

fn extractClsEmbeddings(
    allocator: std.mem.Allocator,
    hidden: []const f32,
    batch: usize,
    seq_len: usize,
    hidden_size: usize,
) ![]f32 {
    const out = try allocator.alloc(f32, batch * hidden_size);
    for (0..batch) |b| {
        const src_start = b * seq_len * hidden_size;
        const dst_start = b * hidden_size;
        @memcpy(out[dst_start .. dst_start + hidden_size], hidden[src_start .. src_start + hidden_size]);
    }
    return out;
}

fn runBertClassifier(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: bert.Config,
    cls_embeddings: []const f32,
    batch: usize,
    hidden_size: usize,
) ![]f32 {
    const pooled = try maybeApplyPooler(cb, allocator, cls_embeddings, batch, hidden_size, "pooler.dense.weight", "pooler.dense.bias");
    defer allocator.free(pooled);
    return runLinearHead(allocator, cb, pooled, batch, hidden_size, cfg.num_labels, "classifier.weight", "classifier.bias");
}

fn runDebertaSequenceClassifierCt(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: deberta_mod.Config,
    hidden: ops.CT,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    const H: usize = @intCast(cfg.hidden_size);
    const cls_embeddings = try extractClsEmbeddingsCt(cb, allocator, hidden, batch, seq_len, H);
    defer cb.free(cls_embeddings);

    const pooled = try maybeApplyPoolerCt(cb, allocator, cls_embeddings, batch, H, "pooler.dense.weight", "pooler.dense.bias");
    defer cb.free(pooled);

    const logits = try runLinearHeadCt(cb, pooled, batch, H, cfg.num_labels, "classifier.weight", "classifier.bias");
    defer cb.free(logits);
    return try cb.toFloat32(logits, allocator);
}

fn extractClsEmbeddingsCt(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: ops.CT,
    batch: usize,
    seq_len: usize,
    hidden_size: usize,
) !ops.CT {
    const row_ids = try allocator.alloc(u32, batch);
    defer allocator.free(row_ids);
    for (0..batch) |b| row_ids[b] = @intCast(b * seq_len);

    if (try cb.takeRows(hidden, row_ids, batch, hidden_size)) |cls| {
        return cls;
    }

    const hidden_f32 = try cb.toFloat32(hidden, allocator);
    defer allocator.free(hidden_f32);
    const cls_f32 = try extractClsEmbeddings(allocator, hidden_f32, batch, seq_len, hidden_size);
    defer allocator.free(cls_f32);
    const shape = [_]i32{ @intCast(batch), @intCast(hidden_size) };
    return try cb.fromFloat32Shape(cls_f32, &shape);
}

fn maybeApplyPoolerCt(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cls_embeddings: ops.CT,
    batch: usize,
    hidden_size: usize,
    weight_name: []const u8,
    bias_name: []const u8,
) !ops.CT {
    const pool_w = cb.getWeight(weight_name) catch |err| switch (err) {
        error.WeightNotFound => {
            const shape = [_]i32{ @intCast(batch), @intCast(hidden_size) };
            if (try cb.cloneTensorShape(cls_embeddings, &shape)) |clone| return clone;
            const cls_f32 = try cb.toFloat32(cls_embeddings, allocator);
            defer allocator.free(cls_f32);
            return try cb.fromFloat32Shape(cls_f32, &shape);
        },
        else => return err,
    };
    defer cb.free(pool_w);
    const pool_b = try cb.getWeight(bias_name);
    defer cb.free(pool_b);

    const pooled_ct = try cb.linear(cls_embeddings, pool_w, pool_b, batch, hidden_size, hidden_size);
    defer cb.free(pooled_ct);

    return try cb.tanh_act(pooled_ct);
}

fn runLinearHeadCt(
    cb: *const ops.ComputeBackend,
    input: ops.CT,
    batch: usize,
    input_dim: usize,
    output_dim_u32: u32,
    weight_name: []const u8,
    bias_name: []const u8,
) !ops.CT {
    const output_dim: usize = @intCast(output_dim_u32);
    const weight = try cb.getWeight(weight_name);
    defer cb.free(weight);
    const bias = try cb.getWeight(bias_name);
    defer cb.free(bias);
    return try cb.linear(input, weight, bias, batch, input_dim, output_dim);
}

fn runTokenClassifier(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: []const f32,
    batch: usize,
    seq_len: usize,
    hidden_size_u32: u32,
    num_labels_u32: u32,
) ![]f32 {
    const hidden_size: usize = @intCast(hidden_size_u32);
    const rows = batch * seq_len;
    return runLinearHead(allocator, cb, hidden, rows, hidden_size, num_labels_u32, "classifier.weight", "classifier.bias");
}

fn runRobertaClassifier(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: bert.Config,
    cls_embeddings: []const f32,
    batch: usize,
    hidden_size: usize,
) ![]f32 {
    if (runTwoLayerHead(
        allocator,
        cb,
        cls_embeddings,
        batch,
        hidden_size,
        hidden_size,
        cfg.num_labels,
        "classifier.dense.weight",
        "classifier.dense.bias",
        "classifier.out_proj.weight",
        "classifier.out_proj.bias",
        .tanh,
    )) |logits| {
        return logits;
    } else |err| switch (err) {
        error.MissingHead => return runLinearHead(allocator, cb, cls_embeddings, batch, hidden_size, cfg.num_labels, "classifier.weight", "classifier.bias"),
        else => return err,
    }
}

fn runDistilBertClassifier(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cfg: bert.Config,
    cls_embeddings: []const f32,
    batch: usize,
    hidden_size: usize,
) ![]f32 {
    return runTwoLayerHead(
        allocator,
        cb,
        cls_embeddings,
        batch,
        hidden_size,
        hidden_size,
        cfg.num_labels,
        "pre_classifier.weight",
        "pre_classifier.bias",
        "classifier.weight",
        "classifier.bias",
        .relu,
    );
}

const HeadActivation = enum {
    relu,
    tanh,
};

fn runTwoLayerHead(
    allocator: std.mem.Allocator,
    cb: *const ops.ComputeBackend,
    input: []const f32,
    batch: usize,
    input_dim: usize,
    hidden_dim: usize,
    output_dim_u32: u32,
    first_weight: []const u8,
    first_bias: []const u8,
    second_weight: []const u8,
    second_bias: []const u8,
    activation: HeadActivation,
) ![]f32 {
    const first_w = cb.getWeight(first_weight) catch |err| switch (err) {
        error.WeightNotFound => return error.MissingHead,
        else => return err,
    };
    defer cb.free(first_w);
    const first_b = cb.getWeight(first_bias) catch |err| switch (err) {
        error.WeightNotFound => return error.MissingHead,
        else => return err,
    };
    defer cb.free(first_b);
    const output_dim: usize = @intCast(output_dim_u32);
    const input_shape = [_]i32{ @intCast(batch), @intCast(input_dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);

    const first_ct = try cb.linear(input_ct, first_w, first_b, batch, input_dim, hidden_dim);
    defer cb.free(first_ct);

    const activated_ct = switch (activation) {
        .relu => try cb.relu(first_ct),
        .tanh => try cb.tanh_act(first_ct),
    };
    defer cb.free(activated_ct);

    const second_w = try cb.getWeight(second_weight);
    defer cb.free(second_w);
    const second_b = try cb.getWeight(second_bias);
    defer cb.free(second_b);
    const logits_ct = try cb.linear(activated_ct, second_w, second_b, batch, hidden_dim, output_dim);
    defer cb.free(logits_ct);
    return try cb.toFloat32(logits_ct, allocator);
}

fn runLinearHead(
    allocator: std.mem.Allocator,
    cb: *const ops.ComputeBackend,
    input: []const f32,
    batch: usize,
    input_dim: usize,
    output_dim_u32: u32,
    weight_name: []const u8,
    bias_name: []const u8,
) ![]f32 {
    const output_dim: usize = @intCast(output_dim_u32);
    const input_shape = [_]i32{ @intCast(batch), @intCast(input_dim) };
    const input_ct = try cb.fromFloat32Shape(input, &input_shape);
    defer cb.free(input_ct);

    const weight = try cb.getWeight(weight_name);
    defer cb.free(weight);
    const bias = try cb.getWeight(bias_name);
    defer cb.free(bias);
    const logits_ct = try cb.linear(input_ct, weight, bias, batch, input_dim, output_dim);
    defer cb.free(logits_ct);
    return try cb.toFloat32(logits_ct, allocator);
}

fn maybeApplyPooler(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    cls_embeddings: []const f32,
    batch: usize,
    hidden_size: usize,
    weight_name: []const u8,
    bias_name: []const u8,
) ![]f32 {
    const pool_w = cb.getWeight(weight_name) catch |err| switch (err) {
        error.WeightNotFound => return try allocator.dupe(f32, cls_embeddings),
        else => return err,
    };
    defer cb.free(pool_w);
    const pool_b = try cb.getWeight(bias_name);
    defer cb.free(pool_b);
    const input_shape = [_]i32{ @intCast(batch), @intCast(hidden_size) };
    const cls_ct = try cb.fromFloat32Shape(cls_embeddings, &input_shape);
    defer cb.free(cls_ct);

    const pooled_ct = try cb.linear(cls_ct, pool_w, pool_b, batch, hidden_size, hidden_size);
    defer cb.free(pooled_ct);

    const activated_ct = try cb.tanh_act(pooled_ct);
    defer cb.free(activated_ct);
    return try cb.toFloat32(activated_ct, allocator);
}

fn archRunGeometry(ptr: *anyopaque, inputs: @import("../backends/session.zig").ShapeInputs, batch: usize) !?@import("../backends/session.zig").RunGeometry {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    if (inputs.len() == 0) return null;
    const first = inputs.get(0);
    if (first.shape.len < 2 or first.shape[1] <= 0) return null;
    const input_seq: usize = @intCast(first.shape[1]);
    var sequence = input_seq;
    var output_seq = input_seq;
    var workspace_bytes: usize = 0;
    const width: usize = switch (self.arch_config) {
        .bert => |cfg| blk: {
            if (self.task == .classifier) output_seq = 1;
            break :blk if (self.task == .classifier or self.task == .extractor) cfg.num_labels else cfg.hidden_size;
        },
        .deberta => |cfg| blk: {
            if (self.task == .classifier) output_seq = 1;
            break :blk if (self.task == .classifier or self.task == .extractor) cfg.num_labels else cfg.hidden_size;
        },
        .modern_bert => |cfg| blk: {
            if (self.task == .extractor and self.gliner_span_encoder_family == .modern_bert) {
                const markers = inputs.named("decision_marker_positions") orelse return error.GlinerSpanExtractionRequiresSchema;
                const marker_mask = inputs.named("decision_marker_mask") orelse return error.GlinerSpanExtractionRequiresSchema;
                if (markers.shape.len != 2 or marker_mask.shape.len != 2 or
                    !std.mem.eql(i64, markers.shape, marker_mask.shape) or
                    markers.shape[0] != first.shape[0] or markers.shape[1] <= 0)
                    return error.InvalidInputShape;
                output_seq = @intCast(markers.shape[1]);
                workspace_bytes = try modernBertGlinerWorkspace(batch, input_seq, cfg.hidden_size, cfg.intermediate_size);
                break :blk 1;
            }
            if (cfg.laya) |laya| {
                if (laya.packing.enabled()) {
                    const packed_rows = @import("../architectures/laya_packed.zig");
                    if (inputs.len() != packed_rows.input_count) return error.InvalidLayaInputs;
                    const markers = inputs.get(5);
                    if (first.shape[0] != 1 or input_seq > laya.packing.max_packed_len or markers.shape.len != 2 or markers.shape[0] <= 0 or markers.shape[1] < 2 or markers.shape[1] > laya.maxOptions()) return error.InvalidLayaInputs;
                    // One row of `Q` decisions: logits plus action logits per question.
                    output_seq = @intCast(markers.shape[0]);
                    workspace_bytes = try packed_rows.workspaceBytes(cfg, input_seq);
                    break :blk @as(usize, @intCast(markers.shape[1])) + laya.n_act;
                }
                if (inputs.len() != 4) return error.InvalidLayaInputs;
                const markers = inputs.get(3);
                if (markers.shape.len != 2 or markers.shape[1] < 2 or markers.shape[1] > 20 or input_seq > laya.max_len) return error.InvalidLayaInputs;
                const count: usize = @intCast(markers.shape[1]);
                output_seq = 1;
                workspace_bytes = if (self.backend_type == .cuda)
                    try layaCudaWorkspace(batch, input_seq, count, cfg.hidden_size, cfg.intermediate_size)
                else
                    try std.math.mul(usize, 2, try whisperStageWorkspace(batch, input_seq, input_seq, cfg.hidden_size, @max(cfg.num_attention_heads, cfg.hidden_size / 64), cfg.hidden_size * 4));
                if (comptime build_options.enable_metal) if (self.backend_type == .metal and @import("../ops/laya_metal.zig").enabledFor(cfg.laya.?)) {
                    workspace_bytes = try @import("../ops/laya_metal.zig").workspaceBound(cfg, batch, input_seq, count);
                };
                break :blk count + @max(laya.n_act, 6);
            }
            break :blk cfg.hidden_size;
        },
        .gliner_modern_bert => |cfg| blk: {
            const markers = inputs.named("decision_marker_positions") orelse return error.MissingInputs;
            if (inputs.len() != 4 or markers.shape.len != 2 or markers.shape[0] != first.shape[0] or
                markers.shape[1] <= 0 or input_seq > cfg.max_position_embeddings) return error.InvalidInputShape;
            const count: usize = @intCast(markers.shape[1]);
            output_seq = 1;
            workspace_bytes = if (self.backend_type == .cuda)
                try layaCudaWorkspace(batch, input_seq, count, cfg.hidden_size, cfg.intermediate_size)
            else
                try whisperStageWorkspace(batch, input_seq, input_seq, cfg.hidden_size, cfg.num_attention_heads, cfg.intermediate_size * 2);
            break :blk count;
        },
        .embedding_gemma2 => |cfg| blk: {
            if (input_seq > embedding_gemma2_arch.max_tokens) return error.InvalidEmbeddingInputLength;
            if (self.backend_type == .cuda) output_seq = 1;
            // Live activations, expanded GQA, streamed PLE, and host/output
            // copies. Attention scores use fixed tiles, never tokens squared.
            workspace_bytes = try std.math.mul(usize, try std.math.mul(usize, batch, input_seq), 64 * 1024);
            break :blk cfg.embedding_dim;
        },
        // Both media/text towers already include their architecture head.
        // Protect the real final projected row even when legacy outputInfo
        // retains symbolic dimensions. No opaque/static workspace is removed.
        .clip => |cfg| blk: {
            output_seq = 1;
            break :blk cfg.projection_dim;
        },
        .clap => |cfg| blk: {
            output_seq = 1;
            break :blk cfg.projection_dim;
        },
        .nomic_bert => |cfg| cfg.hidden_size,
        .t5 => |cfg| cfg.d_model,
        .gpt => |cfg| blk: {
            if (self.task == .classifier and (cfg.family == .qwen3 or cfg.family == .qwen3_vl)) {
                output_seq = 1;
                break :blk 1;
            }
            break :blk cfg.hidden_size;
        },
        .whisper => |cfg| blk: {
            if (first.dtype == .f32 and std.mem.eql(u8, first.name, "input_features")) {
                if (first.shape.len != 3 or first.shape[2] <= 0) return error.InvalidInputShape;
                sequence = @intCast(first.shape[2]);
                output_seq = (std.math.add(usize, sequence, 1) catch return error.ResourceLimitExceeded) / 2;
                workspace_bytes = try whisperStageWorkspace(batch, output_seq, output_seq, cfg.d_model, cfg.encoder_attention_heads, cfg.encoder_ffn_dim);
                break :blk cfg.d_model;
            }
            const hidden = inputs.named("encoder_hidden_states") orelse return error.InvalidInputShape;
            if (hidden.shape.len != 3 or hidden.shape[1] <= 0) return error.InvalidInputShape;
            sequence = @max(input_seq, @as(usize, @intCast(hidden.shape[1])));
            workspace_bytes = try whisperStageWorkspace(batch, input_seq, sequence, cfg.d_model, cfg.decoder_attention_heads, cfg.decoder_ffn_dim);
            break :blk cfg.vocab_size;
        },
        else => return null,
    };
    const elements = std.math.mul(usize, batch, std.math.mul(usize, output_seq, width) catch return error.ResourceLimitExceeded) catch return error.ResourceLimitExceeded;
    const bytes = std.math.mul(usize, elements, @sizeOf(f32)) catch return error.ResourceLimitExceeded;
    const shape_elements: usize = if (self.arch_config == .modern_bert and self.arch_config.modern_bert.laya != null) 4 else 3;
    return .{ .sequence = sequence, .output_bytes = std.math.add(usize, bytes, shape_elements * @sizeOf(i64)) catch return error.ResourceLimitExceeded, .workspace_bytes = workspace_bytes };
}

fn modernBertGlinerWorkspace(batch: usize, sequence: usize, hidden: usize, intermediate: usize) !usize {
    const tokens = try std.math.mul(usize, batch, sequence);
    const activation_width = try std.math.add(usize, try std.math.mul(usize, 10, hidden), try std.math.mul(usize, 2, intermediate));
    // Mirrors gliner_span_v2_executor.deviceScratchUpperBound: bounded encoder
    // activations and FFN intermediates, doubled for transient backend copies.
    return std.math.mul(usize, try std.math.mul(usize, tokens, activation_width), 2 * @sizeOf(f32));
}

/// Conservative simultaneous-live-tensor bound for the eager CUDA encoder/head.
/// Attention scores live in block shared memory; no B*heads*S*S device tensor.
const layaCudaWorkspace = cuda_weights.layaCudaWorkspace;

fn whisperStageWorkspace(batch: usize, queries: usize, keys: usize, hidden: usize, heads: usize, ffn: usize) !usize {
    // Attention score/probability storage, FFN intermediates, and K/V/frontend
    // storage are sequential peaks, not one copy per transformer layer.
    const mul = std.math.mul;
    const add = std.math.add;
    const scores = try mul(usize, try mul(usize, try mul(usize, queries, keys), heads), 3);
    const activations = try mul(usize, queries, try add(usize, try mul(usize, hidden, 6), try mul(usize, ffn, 3)));
    const context = try mul(usize, try mul(usize, keys, hidden), 6);
    return mul(usize, try mul(usize, batch, @max(scores, @max(activations, context))), @sizeOf(f32)) catch error.ResourceLimitExceeded;
}

test "native stage geometry resolves Whisper encoder and decoder output residency" {
    var arch: ArchSession = undefined;
    arch.arch_config = .{ .whisper = .{} };
    arch.task = .generic;
    const input = Tensor{ .data = &.{}, .dtype = .f32, .shape = &.{ 1, 80, 3000 }, .name = "input_features", .allocator = std.testing.allocator, .owns_data = false, .owns_shape = false };
    const encoder = (try archRunGeometry(&arch, .{ .tensors = &.{input} }, 8)).?;
    try std.testing.expectEqual(@as(usize, 3000), encoder.sequence);
    try std.testing.expectEqual(@as(usize, 8 * 1500 * 384 * 4 + 24), encoder.output_bytes);
    var ids = input;
    ids.dtype = .i64;
    ids.shape = &.{ 1, 3 };
    ids.name = "input_ids";
    var hidden = input;
    hidden.shape = &.{ 1, 1500, 384 };
    hidden.name = "encoder_hidden_states";
    const decoder = (try archRunGeometry(&arch, .{ .tensors = &.{ ids, hidden } }, 8)).?;
    try std.testing.expectEqual(@as(usize, 1500), decoder.sequence);
    try std.testing.expectEqual(@as(usize, 8 * 3 * 51865 * 4 + 24), decoder.output_bytes);
}

fn archIndependentBatchRows(ptr: *anyopaque, inputs: []const Tensor) bool {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    // Only stateless forward stages are qualified here. Native generation
    // caches and resident multimodal stages use their own scheduler contracts.
    return switch (self.arch_config) {
        // Packed Laya rows are one tree; marker rows are questions, not batch rows.
        .modern_bert => !isPackedLaya(self) and inputs.len >= 2 and
            inputs[0].dtype == .i64 and inputs[0].shape.len == 2 and
            inputs[1].dtype == .i64 and inputs[1].shape.len == 2,
        .bert, .deberta, .nomic_bert => inputs.len >= 2 and
            inputs[0].dtype == .i64 and inputs[0].shape.len == 2 and
            inputs[1].dtype == .i64 and inputs[1].shape.len == 2,
        .whisper => if (inputs.len == 1)
            inputs[0].dtype == .f32 and inputs[0].shape.len == 3 and std.mem.eql(u8, inputs[0].name, "input_features")
        else
            inputs.len == 2 and inputs[0].dtype == .i64 and inputs[0].shape.len == 2 and
                inputs[1].dtype == .f32 and inputs[1].shape.len == 3,
        .t5 => inputs.len == 2 and inputs[0].dtype == .i64 and inputs[0].shape.len == 2 and
            inputs[1].dtype == .i64 and inputs[1].shape.len == 2,
        .gpt => self.task == .classifier and (self.arch_config.gpt.family == .qwen3 or self.arch_config.gpt.family == .qwen3_vl),
        else => false,
    };
}

fn isPackedLaya(self: *const ArchSession) bool {
    return self.arch_config == .modern_bert and self.arch_config.modern_bert.laya != null and self.arch_config.modern_bert.laya.?.packing.enabled();
}

fn archInputInfo(ptr: *anyopaque) []const TensorInfo {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    if (isPackedLaya(self)) return &@import("../architectures/laya_packed.zig").inputs_info;
    if (self.arch_config == .modern_bert and self.arch_config.modern_bert.laya != null) return &.{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
        .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
        .{ .name = "qtype", .dtype = .i64, .shape = &.{ -1, 1 } },
        .{ .name = "marker_pos", .dtype = .i64, .shape = &.{ -1, -1 } },
    };
    return switch (self.arch_config) {
        .clip => &.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "pixel_values", .dtype = .f32, .shape = &.{ -1, 3, -1, -1 } },
        },
        .clap => &.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "input_features", .dtype = .f32, .shape = &.{ -1, -1, -1, -1 } },
            .{ .name = "is_longer", .dtype = .bool_, .shape = &.{ -1, 1 } },
        },
        .layoutlmv3 => &.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "bbox", .dtype = .i64, .shape = &.{ -1, -1, 4 } },
            .{ .name = "token_type_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "pixel_values", .dtype = .f32, .shape = &.{ -1, 3, -1, -1 } },
        },
        else => &.{
            .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
            .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
        },
    };
}

fn archOutputInfo(ptr: *anyopaque) []const TensorInfo {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    if (self.arch_config == .modern_bert and self.arch_config.modern_bert.laya != null) return &.{
        .{ .name = "logits", .dtype = .f32, .shape = &.{ -1, -1 } },
        .{ .name = "action_logits", .dtype = .f32, .shape = &.{ -1, -1 } },
    };
    if (self.task == .classifier and self.arch_config == .gpt and
        isQwen3GenerativeRerankerFamily(self.arch_config.gpt.family))
    {
        return &.{
            .{ .name = "logits", .dtype = .f32, .shape = &.{ -1, 1 } },
        };
    }
    if (self.task == .classifier and (self.arch_config == .bert or self.arch_config == .deberta or self.arch_config == .layoutlmv3)) {
        return &.{
            .{ .name = "logits", .dtype = .f32, .shape = &.{ -1, -1 } },
        };
    }
    if (self.task == .extractor and (self.arch_config == .bert or self.arch_config == .deberta or self.arch_config == .layoutlmv3)) {
        return &.{
            .{ .name = "logits", .dtype = .f32, .shape = &.{ -1, -1, -1 } },
        };
    }
    return switch (self.arch_config) {
        .embedding_gemma2 => if (self.backend_type == .cuda) &.{
            .{ .name = "sentence_embedding", .dtype = .f32, .shape = &.{ -1, 768 } },
        } else &.{
            .{ .name = "last_hidden_state", .dtype = .f32, .shape = &.{ -1, -1, 768 } },
        },
        .clap => &.{
            .{ .name = "text_embeds", .dtype = .f32, .shape = &.{ -1, -1 } },
        },
        else => &.{
            .{ .name = "last_hidden_state", .dtype = .f32, .shape = &.{ -1, -1, -1 } },
        },
    };
}

fn archBackend(ptr: *anyopaque) BackendType {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    return self.backend_type;
}

/// Stop workers and detach borrowed runtime state at owner close, before the
/// aggregate runtime may retire that state. Weights/providers stay owned until
/// the last resident result retires its compute context and model retention.
fn archQuiesce(ptr: *anyopaque) void {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    switch (self.backend_type) {
        .native => {
            native_mod.stopPrefetchWorker(&self.backend_data.native);
            self.backend_data.native.shared_prefetch = null;
        },
        .pjrt => {
            native_mod.stopPrefetchWorker(&self.backend_data.pjrt.native);
            self.backend_data.pjrt.native.shared_prefetch = null;
        },
        .metal => if (comptime build_options.enable_metal) {
            const data = gpuBackendData(self);
            metal_compute_mod.stopPrefetchWorker(data);
            data.shared_prefetch = null;
        },
        .cuda, .wasm => {},
    }
}

fn archClose(ptr: *anyopaque) void {
    const self: *ArchSession = @ptrCast(@alignCast(ptr));
    self.model_lifetime.discardUnpublishedFinalResources();
    if (self.video_coefficients) |*cache| cache.deinit();
    if (self.video_coefficient_permit) |*permit| permit.deinit();
    if (self.video_coefficient_device_permit) |*permit| permit.deinit();
    if (self.generation_workspace) |*workspace| workspace.borrowed.deinit();
    if (self.laya_trunk_cache) |cache| {
        cache.deinit();
        self.allocator.destroy(cache);
        self.laya_trunk_cache = null;
    }
    switch (self.backend_type) {
        .native => self.backend_data.native.deinitOwned(),
        .metal => {
            if (comptime build_options.enable_metal) {
                const gpu_data = gpuBackendData(self);
                @import("../backends/compute_factory.zig").closeMetalWeights(self.allocator, gpu_data);
            }
        },
        .pjrt => {
            // Deinit the PJRT client (if one was successfully initialized).
            if (build_options.enable_pjrt) {
                if (self.backend_data.pjrt.client) |*client| {
                    client.deinit();
                }
            }
            self.backend_data.pjrt.native.deinitOwned();
        },
        .cuda => {
            if (comptime build_options.enable_cuda) {
                @import("../backends/compute_factory.zig").closeCuda(self.allocator, self.backend_data.cuda.compute);
            }
        },
        .wasm => {},
    }
    self.allocator.destroy(self);
}

test "architecture close retires prefetch before destroying weight maps" {
    const allocator = std.testing.allocator;
    inline for (.{ BackendType.native, BackendType.pjrt, BackendType.metal }) |backend_type| {
        if (comptime backend_type == .metal and !build_options.enable_metal) continue;
        const self = try allocator.create(ArchSession);
        self.* = .{
            .allocator = allocator,
            .arch_config = .{ .gpt = .{
                .hidden_size = 4,
                .num_hidden_layers = 1,
                .num_attention_heads = 1,
                .intermediate_size = 8,
                .vocab_size = 16,
            } },
            .backend_type = backend_type,
            .backend_data = switch (backend_type) {
                .native => .{ .native = .{ .allocator = allocator, .resident_weights = .empty, .lazy_weights = .empty } },
                .pjrt => .{ .pjrt = .{ .native = .{ .allocator = allocator, .resident_weights = .empty, .lazy_weights = .empty } } },
                .metal => .{ .metal = .{ .allocator = allocator, .prefix = "", .lazy_weights = .empty } },
                else => unreachable,
            },
        };
        // An initialized queue must be retired before even an empty map is
        // destroyed: deinit poisons the map that queue cleanup still iterates.
        // No model files, GPU device, or PJRT client are needed for teardown.
        switch (backend_type) {
            .native => native_mod.initPrefetchQueue(&self.backend_data.native, allocator),
            .pjrt => native_mod.initPrefetchQueue(&self.backend_data.pjrt.native, allocator),
            .metal => metal_compute_mod.initPrefetchQueue(&self.backend_data.metal, allocator),
            else => unreachable,
        }
        defer archClose(self);
        switch (backend_type) {
            .native => try self.backend_data.native.lazy_weights.ensureTotalCapacity(allocator, 1),
            .pjrt => try self.backend_data.pjrt.native.lazy_weights.ensureTotalCapacity(allocator, 1),
            .metal => try self.backend_data.metal.lazy_weights.ensureTotalCapacity(allocator, 1),
            else => unreachable,
        }
    }
}

test "gemma gguf ffn norm maps to pre-feedforward layernorm" {
    var buf: [256]u8 = undefined;
    const mapped = normalizeGgufGptWeightKey(.{
        .family = .gemma,
    }, "blk.0.ffn_norm.weight", &buf).?;
    try std.testing.expectEqualStrings("model.layers.0.pre_feedforward_layernorm.weight", mapped);
}

test "large GPU-hosted lazy quant budget floor widens host and backend limits" {
    const floor = recommendedGpuHostedLazyQuantBudgetFloor(3 * 1024 * 1024 * 1024, .prefer_backend_dense);
    try std.testing.expect(floor.host_limit_bytes >= 3 * 1024 * 1024 * 1024);
    try std.testing.expect(floor.backend_limit_bytes >= 6 * 1024 * 1024 * 1024);
    try std.testing.expect(floor.combined_limit_bytes >= floor.host_limit_bytes + floor.backend_limit_bytes);
}

test "device-native GPU-hosted lazy quant budget covers E2B Q8 host cache" {
    const e2b_q8_bytes = 4700 * 1024 * 1024;
    const floor = recommendedGpuHostedLazyQuantBudgetFloor(e2b_q8_bytes, .device_native);
    try std.testing.expect(floor.host_limit_bytes >= e2b_q8_bytes + 256 * 1024 * 1024);
    try std.testing.expect(floor.host_limit_bytes <= gib(6));
    try std.testing.expect(floor.combined_limit_bytes >= floor.host_limit_bytes + floor.backend_limit_bytes);
}

test "large multimodal gemma gpu_hosted budget floor widens dense limits" {
    const floor = recommendedGpuHostedLargeMultimodalGemmaBudgetFloor(8 * 1024 * 1024 * 1024, true);
    try std.testing.expect(floor.host_limit_bytes >= 2 * 1024 * 1024 * 1024);
    try std.testing.expect(floor.backend_limit_bytes >= 6 * 1024 * 1024 * 1024);
    try std.testing.expect(floor.combined_limit_bytes >= floor.backend_limit_bytes);
}

test "CUDA A4B serving policy is attached to the returned session" {
    var owner = ArchSession{
        .allocator = std.testing.allocator,
        .arch_config = .{ .gpt = .{ .family = .gemma } },
        .backend_type = .cuda,
        .backend_data = undefined,
    };
    var session = Session{ .ptr = &owner, .vtable = &arch_vtable };
    const resident = runtime.tier.memory.AdmissionAmounts{ .backend_weight_bytes = 80, .backend_kv_bytes = 16, .backend_scratch_bytes = 4 };
    configureReservedGenerationWorkspace(&session, .{ .backend_limit_bytes = 100, .combined_limit_bytes = 100, .kv_limit_bytes = 16, .scratch_limit_bytes = 4 }, resident);
    defer owner.generation_workspace.?.borrowed.deinit();
    const limits = widenBudgetLimitsForSession(session, .{ .host_limit_bytes = 20, .backend_limit_bytes = 50, .combined_limit_bytes = 70 });
    try std.testing.expectEqual(@as(usize, 100), limits.backend_limit_bytes);
    try std.testing.expectEqual(@as(usize, 120), limits.combined_limit_bytes);
    var amounts = runtime.tier.memory.AdmissionAmounts{ .backend_kv_bytes = 16, .backend_scratch_bytes = 4 };
    var lease = try session.generation_workspace.?.acquire(&amounts);
    defer lease.release();
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, amounts);
    // Value copies of Session borrow from the same bounded owner.
    const copy = session;
    var another = runtime.tier.memory.AdmissionAmounts{ .backend_scratch_bytes = 1 };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, copy.generation_workspace.?.acquire(&another));
}

test "CUDA A4B hosted policy keeps construction budget independent of staging" {
    const config = try backend_contracts.buildCudaA4bInferenceConfig(
        null,
        backend_contracts.qualified_a4b_geometries[0],
    );
    const floor = a4bGpuHostedBudgetPolicy(config).budget_floor;
    const backend_budget = @as(usize, @intCast(config.memory_budget_bytes));
    try std.testing.expectEqual(backend_budget, floor.backend_limit_bytes);
    try std.testing.expectEqual(backend_budget, floor.combined_limit_bytes);
}

test "Qwen3-VL reranker BF16 budget covers mapped and backend weight domains" {
    const weight_bytes = gib(4);
    const floor = recommendedGpuHostedLargeDenseSafetensorsBudgetFloor(weight_bytes);
    try std.testing.expectEqual(weight_bytes + mib(256), floor.host_limit_bytes);
    try std.testing.expectEqual(weight_bytes + mib(512), floor.backend_limit_bytes);
    try std.testing.expectEqual(weight_bytes * 2 + gib(1) + mib(768), floor.combined_limit_bytes);

    const manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .inference_bundle_family = manifest_mod.qwen3_vl_reranker_safetensors_bundle_family,
    };
    try std.testing.expect(shouldUseLargeDenseSafetensorsBudgets(
        weight_bytes,
        manifest,
        .{ .gpt = .{ .family = .qwen3_vl } },
    ));
    try std.testing.expect(!shouldUseLargeDenseSafetensorsBudgets(
        weight_bytes,
        manifest,
        .{ .gpt = .{ .family = .qwen2 } },
    ));
}

test "Qwen3 text and vision families share generative reranker session output" {
    try std.testing.expect(isQwen3GenerativeRerankerFamily(.qwen3));
    try std.testing.expect(isQwen3GenerativeRerankerFamily(.qwen3_vl));
    try std.testing.expect(!isQwen3GenerativeRerankerFamily(.qwen2));
}

test "Qwen3-VL reranker GGUF budget reserves image projector host envelope" {
    const decoder_bytes = @as(u64, 1_834_438_720);
    const floor = recommendedGpuHostedQwen3VlRerankerGgufBudgetFloor(decoder_bytes);
    try std.testing.expectEqual(gib(3), floor.host_limit_bytes);

    const manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .inference_bundle_family = manifest_mod.qwen3_vl_reranker_gguf_bundle_family,
    };
    try std.testing.expect(shouldUseQwen3VlRerankerGgufBudgets(
        decoder_bytes,
        manifest,
        .{ .gpt = .{ .family = .qwen3_vl } },
    ));
    try std.testing.expect(!shouldUseQwen3VlRerankerGgufBudgets(
        decoder_bytes,
        manifest,
        .{ .gpt = .{ .family = .qwen3 } },
    ));
}

test "session budget widening preserves higher explicit limits" {
    const widened = widenLimits(.{
        .host_limit_bytes = gib(1),
        .backend_limit_bytes = gib(4),
        .combined_limit_bytes = gib(6),
        .kv_limit_bytes = mib(512),
        .scratch_limit_bytes = mib(256),
    }, .{
        .host_limit_bytes = gib(4),
        .backend_limit_bytes = gib(8),
        .combined_limit_bytes = gib(12),
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    });
    try std.testing.expectEqual(gib(4), widened.host_limit_bytes);
    try std.testing.expectEqual(gib(8), widened.backend_limit_bytes);
    try std.testing.expectEqual(gib(12), widened.combined_limit_bytes);
    try std.testing.expectEqual(mib(512), widened.kv_limit_bytes);
    try std.testing.expectEqual(mib(256), widened.scratch_limit_bytes);
}

test "gemma gguf norm aliases stay distinct" {
    const cfg: gpt_mod.Config = .{ .family = .gemma };

    var buf0: [256]u8 = undefined;
    var buf1: [256]u8 = undefined;
    var buf2: [256]u8 = undefined;
    var buf3: [256]u8 = undefined;
    var buf4: [256]u8 = undefined;
    var buf5: [256]u8 = undefined;

    const attn = normalizeGgufGptWeightKey(cfg, "blk.0.attn_norm.weight", &buf0).?;
    const q = normalizeGgufGptWeightKey(cfg, "blk.0.attn_q_norm.weight", &buf1).?;
    const k = normalizeGgufGptWeightKey(cfg, "blk.0.attn_k_norm.weight", &buf2).?;
    const ffn = normalizeGgufGptWeightKey(cfg, "blk.0.ffn_norm.weight", &buf3).?;
    const post_attn = normalizeGgufGptWeightKey(cfg, "blk.0.post_attention_norm.weight", &buf4).?;
    const post_ffn = normalizeGgufGptWeightKey(cfg, "blk.0.post_ffw_norm.weight", &buf5).?;

    try std.testing.expectEqualStrings("model.layers.0.input_layernorm.weight", attn);
    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_norm.weight", q);
    try std.testing.expectEqualStrings("model.layers.0.self_attn.k_norm.weight", k);
    try std.testing.expectEqualStrings("model.layers.0.pre_feedforward_layernorm.weight", ffn);
    try std.testing.expectEqualStrings("model.layers.0.post_attention_layernorm.weight", post_attn);
    try std.testing.expectEqualStrings("model.layers.0.post_feedforward_layernorm.weight", post_ffn);
}

test "overlay gpt structural config keeps gguf gemma norm offset" {
    var target: gpt_mod.Config = .{
        .family = .gemma,
        .norm_weight_offset = 1.0,
        .rope_theta = 1_000_000.0,
    };
    const source: gpt_mod.Config = .{
        .family = .gemma,
        .norm_weight_offset = 0.0,
        .rope_theta = 1_000_000.0,
    };
    overlayGptStructuralConfig(&target, source);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), target.norm_weight_offset, 1e-6);
}

test "overlay gpt structural config takes weight tying from gguf" {
    // Gemma 3 config.json omits tie_word_embeddings, so the HF-derived config says false
    // while the GGUF (which has no output.weight tensor) says true. Honoring config.json
    // sent the runtime looking for a lm_head.weight that is not in the file.
    var target: gpt_mod.Config = .{ .family = .gemma, .weight_tying = false };
    const source: gpt_mod.Config = .{ .family = .gemma, .weight_tying = true };

    overlayGptStructuralConfig(&target, source);

    try std.testing.expect(target.weight_tying);
}

test "overlay gpt structural config takes rope partial factor from gguf" {
    // unsloth's Gemma 4 config.json claims partial_rotary_factor 0.25 while its GGUF
    // declares rope.dimension_count equal to the head dim (full rotation). Following
    // config.json produced empty completions from an otherwise-identical artifact.
    var target: gpt_mod.Config = .{ .family = .gemma, .rope_partial_factor = 0.25 };
    const source: gpt_mod.Config = .{ .family = .gemma, .rope_partial_factor = 1.0 };

    overlayGptStructuralConfig(&target, source);

    try std.testing.expectApproxEqAbs(@as(f32, 1.0), target.rope_partial_factor, 1e-6);
}

test "overlay gpt structural config clears sidecar-only shared tail ffn size" {
    var target: gpt_mod.Config = .{
        .family = .gemma,
        .intermediate_size = 10240,
        .shared_layer_intermediate_size = 20480,
    };
    const source: gpt_mod.Config = .{
        .family = .gemma,
        .intermediate_size = 10240,
        .shared_layer_intermediate_size = 0,
    };

    overlayGptStructuralConfig(&target, source);

    try std.testing.expectEqual(@as(u32, 0), target.shared_layer_intermediate_size);
    try std.testing.expectEqual(@as(u32, 10240), target.intermediateSize(41));
}

test "metal gguf preflight rejects release-unsafe iq4 xs tensors" {
    var tensor_types = [_]UnsupportedTensorTypeCount{
        .{ .tensor_type = .{ .known = .IQ4_XS }, .count = 15 },
    };
    const report = GgufInspectionReport{
        .allocator = std.testing.allocator,
        .architecture = "gemma4",
        .tensor_count = 15,
        .metadata_count = 0,
        .all_tensor_types = tensor_types[0..],
    };

    if (comptime build_options.enable_metal) {
        const incompatible = metalGgufIncompatibleTensorType(report) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(gguf_mod.tensor_types.KnownTensorType.IQ4_XS, incompatible.tensor_type.known);
        try std.testing.expectEqual(@as(usize, 15), incompatible.count);
    } else try std.testing.expect(metalGgufIncompatibleTensorType(report) == null);
}

test "overlay gpt structural config keeps config softcapping when gguf omits it" {
    var target: gpt_mod.Config = .{ .family = .gemma, .final_logit_softcapping = 30.0 };
    const source: gpt_mod.Config = .{ .family = .gemma, .final_logit_softcapping = 0.0 };

    overlayGptStructuralConfig(&target, source);

    try std.testing.expectApproxEqAbs(@as(f32, 30.0), target.final_logit_softcapping, 1e-6);
}

test "qwen3 gguf weights normalize onto hf names" {
    var buf: [256]u8 = undefined;
    const config = gpt_mod.Config{ .family = .qwen3 };

    try std.testing.expectEqualStrings(
        "model.layers.7.self_attn.q_norm.weight",
        normalizeGgufGptWeightKey(config, "blk.7.attn_q_norm.weight", &buf).?,
    );
    try std.testing.expectEqualStrings(
        "model.layers.7.self_attn.k_norm.weight",
        normalizeGgufGptWeightKey(config, "blk.7.attn_k_norm.weight", &buf).?,
    );
    try std.testing.expectEqualStrings(
        "model.layers.0.self_attn.q_proj.weight",
        normalizeGgufGptWeightKey(config, "blk.0.attn_q.weight", &buf).?,
    );
    // Qwen3 has no qkv bias, so ffn_norm must land on post_attention_layernorm rather
    // than the Gemma-style pre_feedforward_layernorm.
    try std.testing.expectEqualStrings(
        "model.layers.0.post_attention_layernorm.weight",
        normalizeGgufGptWeightKey(config, "blk.0.ffn_norm.weight", &buf).?,
    );
    try std.testing.expectEqualStrings(
        "model.embed_tokens.weight",
        normalizeGgufGptWeightKey(config, "token_embd.weight", &buf).?,
    );
}

test "qwen3_5 gguf weights are not claimed as mappable" {
    // qwen3_5 carries ssm_*/attn_qkv/attn_gate tensors with no mapping to the
    // linear-attention runtime. Claiming support would fail late with a wall of
    // missing tensors instead of a clear unsupported-architecture error.
    var buf: [256]u8 = undefined;
    try std.testing.expect(!gpt_mod.ggufWeightMappingSupported(.qwen3_5));
    try std.testing.expect(normalizeGgufGptWeightKey(
        .{ .family = .qwen3_5 },
        "blk.0.attn_q.weight",
        &buf,
    ) == null);
}

test "mistral gguf ffn norm maps to post-attention layernorm" {
    var buf: [256]u8 = undefined;
    const mapped = normalizeGgufGptWeightKey(.{
        .family = .mistral,
    }, "blk.0.ffn_norm.weight", &buf).?;
    try std.testing.expectEqualStrings("model.layers.0.post_attention_layernorm.weight", mapped);
}

test "qwen2 gguf attention bias tensors map to architecture weights" {
    const cfg: gpt_mod.Config = .{ .family = .qwen2 };

    var buf0: [256]u8 = undefined;
    const q_bias = normalizeGgufGptWeightKey(cfg, "blk.0.attn_q.bias", &buf0).?;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_proj.bias", q_bias);

    var buf1: [256]u8 = undefined;
    const k_bias = normalizeGgufGptWeightKey(cfg, "blk.0.attn_k.bias", &buf1).?;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.k_proj.bias", k_bias);

    var buf2: [256]u8 = undefined;
    const v_bias = normalizeGgufGptWeightKey(cfg, "blk.0.attn_v.bias", &buf2).?;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.v_proj.bias", v_bias);
}

test "gpt neo required tensors match native gguf names" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    const present = [_][]const u8{
        "wte.weight",
        "wpe.weight",
        "ln_f.weight",
        "ln_f.bias",
        "lm_head.weight",
        "h.0.ln_1.weight",
        "h.0.ln_1.bias",
        "h.0.attn.attention.q_proj.weight",
        "h.0.attn.attention.k_proj.weight",
        "h.0.attn.attention.v_proj.weight",
        "h.0.attn.attention.out_proj.weight",
        "h.0.ln_2.weight",
        "h.0.ln_2.bias",
        "h.0.mlp.c_fc.weight",
        "h.0.mlp.c_fc.bias",
        "h.0.mlp.c_proj.weight",
        "h.0.mlp.c_proj.bias",
    };
    for (present) |name| {
        try names.put(allocator, try allocator.dupe(u8, name), {});
    }

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    try collectMissingRequiredGptWeights(allocator, .{
        .family = .gpt_neo,
        .num_hidden_layers = 1,
        .position_encoding = .absolute,
        .weight_tying = false,
    }, &names, &missing, false);

    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}

test "gptj required tensors match generic exported names" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    const present = [_][]const u8{
        "model.embed_tokens.weight",
        "wpe.weight",
        "model.norm.weight",
        "model.norm.bias",
        "lm_head.weight",
        "model.layers.0.input_layernorm.weight",
        "model.layers.0.input_layernorm.bias",
        "model.layers.0.self_attn.q_proj.weight",
        "model.layers.0.self_attn.k_proj.weight",
        "model.layers.0.self_attn.v_proj.weight",
        "model.layers.0.self_attn.o_proj.weight",
        "model.layers.0.post_attention_layernorm.weight",
        "model.layers.0.post_attention_layernorm.bias",
        "model.layers.0.mlp.fc1_proj.weight",
        "model.layers.0.mlp.fc1_proj.bias",
        "model.layers.0.mlp.fc2_proj.weight",
        "model.layers.0.mlp.fc2_proj.bias",
    };
    for (present) |name| {
        try names.put(allocator, try allocator.dupe(u8, name), {});
    }

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    try collectMissingRequiredGptWeights(allocator, .{
        .family = .gptj,
        .num_hidden_layers = 1,
        .position_encoding = .absolute,
        .weight_tying = false,
    }, &names, &missing, false);

    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}

test "gpt neox required tensors match generic exported names" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    const present = [_][]const u8{
        "model.embed_tokens.weight",
        "model.norm.weight",
        "model.norm.bias",
        "lm_head.weight",
        "model.layers.0.input_layernorm.weight",
        "model.layers.0.input_layernorm.bias",
        "model.layers.0.self_attn.q_proj.weight",
        "model.layers.0.self_attn.q_proj.bias",
        "model.layers.0.self_attn.k_proj.weight",
        "model.layers.0.self_attn.k_proj.bias",
        "model.layers.0.self_attn.v_proj.weight",
        "model.layers.0.self_attn.v_proj.bias",
        "model.layers.0.self_attn.o_proj.weight",
        "model.layers.0.post_attention_layernorm.weight",
        "model.layers.0.post_attention_layernorm.bias",
        "model.layers.0.mlp.fc1_proj.weight",
        "model.layers.0.mlp.fc1_proj.bias",
        "model.layers.0.mlp.fc2_proj.weight",
        "model.layers.0.mlp.fc2_proj.bias",
    };
    for (present) |name| {
        try names.put(allocator, try allocator.dupe(u8, name), {});
    }

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    try collectMissingRequiredGptWeights(allocator, .{
        .family = .gpt_neox,
        .num_hidden_layers = 1,
        .position_encoding = .rope,
        .weight_tying = false,
    }, &names, &missing, false);

    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}

test "gemma4 shared kv tail does not require per-layer k/v tensors" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    const present = [_][]const u8{
        "model.embed_tokens.weight",
        "model.norm.weight",
        "model.layers.0.input_layernorm.weight",
        "model.layers.0.self_attn.q_proj.weight",
        "model.layers.0.self_attn.k_proj.weight",
        "model.layers.0.self_attn.v_proj.weight",
        "model.layers.0.self_attn.o_proj.weight",
        "model.layers.0.post_attention_layernorm.weight",
        "model.layers.0.mlp.gate_proj.weight",
        "model.layers.0.mlp.up_proj.weight",
        "model.layers.0.mlp.down_proj.weight",
        "model.layers.1.input_layernorm.weight",
        "model.layers.1.self_attn.q_proj.weight",
        "model.layers.1.self_attn.o_proj.weight",
        "model.layers.1.post_attention_layernorm.weight",
        "model.layers.1.mlp.gate_proj.weight",
        "model.layers.1.mlp.up_proj.weight",
        "model.layers.1.mlp.down_proj.weight",
    };
    for (present) |name| {
        try names.put(allocator, try allocator.dupe(u8, name), {});
    }

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    try collectMissingRequiredGptWeights(allocator, .{
        .family = .gemma,
        .num_hidden_layers = 2,
        .num_kv_shared_layers = 1,
        .position_encoding = .rope,
        .weight_tying = true,
    }, &names, &missing, false);

    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}

test "deepseek v4 required tensors use canonical hf names" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    var attention_schedule = @as([gpt_mod.deepseek_v4_max_layers]gpt_mod.DeepseekV4AttentionKind, @splat(.sliding_attention));
    attention_schedule[1] = .compressed_sparse_attention;
    attention_schedule[2] = .heavily_compressed_attention;

    var mlp_schedule = @as([gpt_mod.deepseek_v4_max_layers]gpt_mod.DeepseekV4MlpKind, @splat(.moe));
    mlp_schedule[0] = .hash_moe;

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    try collectMissingRequiredGptWeights(allocator, .{
        .family = .deepseek_v4,
        .num_hidden_layers = 3,
        .deepseek_v4_attention_schedule_len = 3,
        .deepseek_v4_attention_schedule = attention_schedule,
        .deepseek_v4_mlp_schedule_len = 3,
        .deepseek_v4_mlp_schedule = mlp_schedule,
    }, &names, &missing, false);

    try std.testing.expectEqual(@as(usize, 91), missing.items.len);
    try std.testing.expect(missingContains(missing.items, "model.embed_tokens.weight"));
    try std.testing.expect(missingContains(missing.items, "model.norm.weight"));
    try std.testing.expect(missingContains(missing.items, "model.hc_head.hc_fn"));
    try std.testing.expect(missingContains(missing.items, "model.hc_head.hc_base"));
    try std.testing.expect(missingContains(missing.items, "model.hc_head.hc_scale"));

    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.q_a_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.q_a_norm.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.q_b_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.kv_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.kv_norm.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.o_a_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.o_b_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.self_attn.sinks"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.attn_hc.fn"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.attn_hc.base"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.attn_hc.scale"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.ffn_hc.fn"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.ffn_hc.base"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.ffn_hc.scale"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.mlp.gate.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.0.mlp.gate.tid2eid"));
    try std.testing.expect(!missingContains(missing.items, "model.layers.0.mlp.gate.e_score_correction_bias"));

    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.kv_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.gate_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.position_bias"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.kv_norm.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.kv_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.gate_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.position_bias"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.kv_norm.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.q_b_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.self_attn.compressor.indexer.weights_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.1.mlp.gate.e_score_correction_bias"));
    try std.testing.expect(!missingContains(missing.items, "model.layers.1.mlp.gate.tid2eid"));

    try std.testing.expect(missingContains(missing.items, "model.layers.2.self_attn.compressor.kv_proj.weight"));
    try std.testing.expect(!missingContains(missing.items, "model.layers.2.self_attn.compressor.indexer.kv_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.experts.gate_proj"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.experts.up_proj"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.experts.down_proj"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.shared_experts.gate_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.shared_experts.up_proj.weight"));
    try std.testing.expect(missingContains(missing.items, "model.layers.2.mlp.shared_experts.down_proj.weight"));

    try std.testing.expect(!missingContains(missing.items, "model.layers.0.self_attn.q_proj.weight"));
    try std.testing.expect(!missingContains(missing.items, "model.layers.0.mlp.down_proj.weight"));
}

test "deepseek v4 gguf tensor names map to canonical runtime names" {
    var buf0: [256]u8 = undefined;
    var buf1: [256]u8 = undefined;
    var buf2: [256]u8 = undefined;
    var buf3: [256]u8 = undefined;
    var buf4: [256]u8 = undefined;
    var buf5: [256]u8 = undefined;
    const cfg = gpt_mod.Config{ .family = .deepseek_v4 };

    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_a_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.attn_q_a.weight", &buf0).?);
    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_a_norm.weight", normalizeGgufGptWeightKey(cfg, "blk.0.attn_q_a_norm.weight", &buf1).?);
    try std.testing.expectEqualStrings("model.layers.0.self_attn.kv_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.attn_kv_a_mqa.weight", &buf2).?);
    try std.testing.expectEqualStrings("model.layers.0.self_attn.o_b_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.attn_o_b.weight", &buf3).?);
    try std.testing.expectEqualStrings("model.layers.0.mlp.gate.weight", normalizeGgufGptWeightKey(cfg, "blk.0.ffn_gate_inp.weight", &buf4).?);
    try std.testing.expectEqualStrings("model.layers.0.mlp.shared_experts.up_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.ffn_up_shexp.weight", &buf5).?);
}

test "deepseek v4 flash gguf tensor dump aliases map to canonical runtime names" {
    const cfg = gpt_mod.Config{ .family = .deepseek_v4 };
    var buf0: [256]u8 = undefined;
    var buf1: [256]u8 = undefined;
    var buf2: [256]u8 = undefined;
    var buf3: [256]u8 = undefined;
    var buf4: [256]u8 = undefined;
    var buf5: [256]u8 = undefined;
    var buf6: [256]u8 = undefined;
    var buf7: [256]u8 = undefined;
    var buf8: [256]u8 = undefined;
    var buf9: [256]u8 = undefined;
    var buf10: [256]u8 = undefined;
    var buf11: [256]u8 = undefined;
    var buf12: [256]u8 = undefined;
    var buf13: [256]u8 = undefined;
    var buf14: [256]u8 = undefined;
    var buf15: [256]u8 = undefined;
    var buf16: [256]u8 = undefined;
    var buf17: [256]u8 = undefined;
    var buf18: [256]u8 = undefined;

    try std.testing.expectEqualStrings("model.layers.2.self_attn.kv_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_kv_latent.weight", &buf0).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.o_a_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_output_a.weight", &buf1).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.o_b_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_output_b.weight", &buf2).?);
    try std.testing.expectEqualStrings("model.layers.2.mlp.gate.tid2eid", normalizeGgufGptWeightKey(cfg, "blk.2.ffn_gate_tid2eid", &buf3).?);
    try std.testing.expectEqualStrings("model.layers.2.mlp.gate.e_score_correction_bias", normalizeGgufGptWeightKey(cfg, "blk.2.exp_probs_b", &buf4).?);
    try std.testing.expectEqualStrings("model.layers.2.attn_hc.fn", normalizeGgufGptWeightKey(cfg, "blk.2.hc_attn_fn", &buf5).?);
    try std.testing.expectEqualStrings("model.layers.2.attn_hc.base", normalizeGgufGptWeightKey(cfg, "blk.2.hc_attn_base", &buf6).?);
    try std.testing.expectEqualStrings("model.layers.2.attn_hc.scale", normalizeGgufGptWeightKey(cfg, "blk.2.hc_attn_scale", &buf7).?);
    try std.testing.expectEqualStrings("model.layers.2.ffn_hc.fn", normalizeGgufGptWeightKey(cfg, "blk.2.hc_ffn_fn", &buf8).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.kv_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_compress_kv.weight", &buf9).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.gate_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_compress_gate.weight", &buf10).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.position_bias", normalizeGgufGptWeightKey(cfg, "blk.2.attn_compress_ape", &buf11).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.kv_norm.weight", normalizeGgufGptWeightKey(cfg, "blk.2.attn_compress_norm.weight", &buf12).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.q_b_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.attn_q_b.weight", &buf13).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.weights_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.proj.weight", &buf14).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.position_bias", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.compress_ape", &buf15).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.kv_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.compress_kv.weight", &buf16).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.gate_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.compress_gate.weight", &buf17).?);
    try std.testing.expectEqualStrings("model.layers.2.self_attn.compressor.indexer.kv_norm.weight", normalizeGgufGptWeightKey(cfg, "blk.2.indexer.compress_norm.weight", &buf18).?);
}

test "deepseek v4 gguf packed moe names map to routed expert lazy keys" {
    const gate = parsePackedMoeTensor("blk.2.ffn_gate_exps.weight").?;
    try std.testing.expectEqual(@as(usize, 2), gate.layer);
    try std.testing.expectEqualStrings("gate_proj", deepseek_v4_arch.moeProjectionName(gate.proj).?);
    try std.testing.expect(gate.proj2 == null);
    try std.testing.expect(!gate.fused_gate_up);

    const up = parsePackedMoeTensor("blk.2.ffn_up_exps.weight").?;
    try std.testing.expectEqualStrings("up_proj", deepseek_v4_arch.moeProjectionName(up.proj).?);
    const down = parsePackedMoeTensor("blk.2.ffn_down_exps.weight").?;
    try std.testing.expectEqualStrings("down_proj", deepseek_v4_arch.moeProjectionName(down.proj).?);

    const fused = parsePackedMoeTensor("blk.2.ffn_gate_up_exps.weight").?;
    try std.testing.expect(fused.fused_gate_up);
    try std.testing.expectEqualStrings("w1", fused.proj);
    try std.testing.expectEqualStrings("w3", fused.proj2.?);

    const coord = parseMoeExpertCoord("model.layers.2.mlp.experts.7.gate_proj").?;
    try std.testing.expectEqual(@as(usize, 2), coord.layer_index);
    try std.testing.expectEqual(@as(u32, 7), coord.expert_index);
    try std.testing.expectEqual(@as(u8, 0x1), projectionMaskForWeightKey("model.layers.2.mlp.experts.7.gate_proj"));
    try std.testing.expectEqual(@as(u8, 0x2), projectionMaskForWeightKey("model.layers.2.mlp.experts.7.down_proj"));
    try std.testing.expectEqual(@as(u8, 0x4), projectionMaskForWeightKey("model.layers.2.mlp.experts.7.up_proj"));
    try std.testing.expectEqual(@as(u8, 0x5), projectionMaskForWeightKey("model.layers.2.mlp.experts.7.gate_up_proj"));
    try std.testing.expect(shouldLazyLoadWeight(.gguf, .{ .gpt = .{ .family = .deepseek_v4, .num_local_experts = 8, .num_experts_per_tok = 2 } }, "model.layers.2.mlp.experts.7.gate_proj"));
}

fn missingContains(missing: []const []const u8, expected: []const u8) bool {
    for (missing) |name| {
        if (std.mem.eql(u8, name, expected)) return true;
    }
    return false;
}

test "phi gguf tensors map to architecture weights" {
    const cfg: gpt_mod.Config = .{ .family = .phi };

    var buf0: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.norm.bias", normalizeGgufGptWeightKey(cfg, "output_norm.bias", &buf0).?);

    var buf1: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.layers.0.input_layernorm.bias", normalizeGgufGptWeightKey(cfg, "blk.0.attn_norm.bias", &buf1).?);

    var buf2: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.layers.0.post_attention_layernorm.bias", normalizeGgufGptWeightKey(cfg, "blk.0.ffn_norm.bias", &buf2).?);

    var buf3: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_proj.bias", normalizeGgufGptWeightKey(cfg, "blk.0.attn_q.bias", &buf3).?);

    var buf4: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.layers.0.mlp.fc1_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.ffn_up.weight", &buf4).?);

    var buf5: [256]u8 = undefined;
    try std.testing.expectEqualStrings("model.layers.0.mlp.fc2_proj.weight", normalizeGgufGptWeightKey(cfg, "blk.0.ffn_down.weight", &buf5).?);
}

test "bitnet gguf tensor names map to architecture weights" {
    const cfg: gpt_mod.Config = .{ .family = .bitnet };

    var buf0: [256]u8 = undefined;
    const token = normalizeGgufGptWeightKey(cfg, "token_embd.weight", &buf0).?;
    try std.testing.expectEqualStrings("model.embed_tokens.weight", token);

    var buf1: [256]u8 = undefined;
    const attn_norm = normalizeGgufGptWeightKey(cfg, "blk.0.attn_norm.weight", &buf1).?;
    try std.testing.expectEqualStrings("model.layers.0.input_layernorm.weight", attn_norm);

    var buf2: [256]u8 = undefined;
    const ffn_norm = normalizeGgufGptWeightKey(cfg, "blk.0.ffn_norm.weight", &buf2).?;
    try std.testing.expectEqualStrings("model.layers.0.post_attention_layernorm.weight", ffn_norm);

    var buf3: [256]u8 = undefined;
    const attn_sub_norm = normalizeGgufGptWeightKey(cfg, "blk.0.attn_sub_norm.weight", &buf3).?;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.attn_sub_norm.weight", attn_sub_norm);

    var buf4: [256]u8 = undefined;
    const ffn_sub_norm = normalizeGgufGptWeightKey(cfg, "blk.0.ffn_sub_norm.weight", &buf4).?;
    try std.testing.expectEqualStrings("model.layers.0.mlp.ffn_sub_norm.weight", ffn_sub_norm);

    var buf5: [256]u8 = undefined;
    const q_proj = normalizeGgufGptWeightKey(cfg, "blk.0.attn_q.weight", &buf5).?;
    try std.testing.expectEqualStrings("model.layers.0.self_attn.q_proj.weight", q_proj);

    var buf6: [256]u8 = undefined;
    const down_proj = normalizeGgufGptWeightKey(cfg, "blk.0.ffn_down.weight", &buf6).?;
    try std.testing.expectEqualStrings("model.layers.0.mlp.down_proj.weight", down_proj);
}

test "bitnet i2_s tensor type is supported for gguf inspection" {
    try std.testing.expect(tensorTypeSupported(.{ .known = .I2_S }));
}

test "gemma4 gguf shared expert weight names map correctly" {
    var buf: [256]u8 = undefined;

    const gate = normalizeGgufGptWeightKey(.{ .family = .gemma }, "blk.5.ffn_gate_shexp.weight", &buf).?;
    try std.testing.expectEqualStrings("model.layers.5.block_sparse_moe.shared_expert.gate_proj.weight", gate);

    var buf2: [256]u8 = undefined;
    const down = normalizeGgufGptWeightKey(.{ .family = .gemma }, "blk.0.ffn_down_shexp.weight", &buf2).?;
    try std.testing.expectEqualStrings("model.layers.0.block_sparse_moe.shared_expert.down_proj.weight", down);

    var buf3: [256]u8 = undefined;
    const up = normalizeGgufGptWeightKey(.{ .family = .gemma }, "blk.12.ffn_up_shexp.weight", &buf3).?;
    try std.testing.expectEqualStrings("model.layers.12.block_sparse_moe.shared_expert.up_proj.weight", up);
}

test "gemma4 packed moe tensors are tracked but not reported as unmapped" {
    const allocator = std.testing.allocator;
    var tensors = [_]gguf_mod.format.TensorInfo{
        .{
            .name = "blk.0.ffn_down_exps.weight",
            .dimensions = &.{},
            .tensor_type = .{ .known = .Q5_1 },
            .offset = 0,
            .data_offset = 0,
        },
    };
    const file = gguf_mod.format.File{
        .header = .{ .version = 3, .tensor_count = tensors.len, .metadata_count = 0 },
        .metadata = &.{},
        .tensors = tensors[0..],
        .alignment = gguf_mod.format.default_alignment,
        .data_region_offset = 0,
    };

    var names = std.StringHashMapUnmanaged(void).empty;
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }
    var unmapped = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (unmapped.items) |name| allocator.free(name);
        unmapped.deinit(allocator);
    }
    var packed_moe = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (packed_moe.items) |name| allocator.free(name);
        packed_moe.deinit(allocator);
    }

    try collectNormalizedGgufNames(allocator, .{ .gpt = .{ .family = .gemma } }, &file, &names, &unmapped, &packed_moe);

    try std.testing.expectEqual(@as(usize, 1), packed_moe.items.len);
    try std.testing.expectEqual(@as(usize, 0), unmapped.items.len);
}

test "defaultResidentExpertsPerLayer returns 24 for 128-expert top_k=8 model" {
    const cfg: gpt_mod.Config = .{
        .num_local_experts = 128,
        .num_experts_per_tok = 8,
    };
    const resident = defaultResidentExpertsPerLayer(.{ .gpt = cfg });
    // 128 experts, top_k=8, multiplier=3 (>=64 experts) → min(128, max(4, 24)) = 24
    try std.testing.expectEqual(@as(usize, 24), resident);
}

test "defaultResidentExpertsPerLayer returns 4 for 8-expert top_k=2 model" {
    const cfg: gpt_mod.Config = .{
        .num_local_experts = 8,
        .num_experts_per_tok = 2,
    };
    const resident = defaultResidentExpertsPerLayer(.{ .gpt = cfg });
    // 8 experts, top_k=2, multiplier=2 (<64 experts) → min(8, max(4, 4)) = 4
    try std.testing.expectEqual(@as(usize, 4), resident);
}

test "defaultResidentExpertsPerLayer returns 0 for non-MoE model" {
    const cfg: gpt_mod.Config = .{};
    const resident = defaultResidentExpertsPerLayer(.{ .gpt = cfg });
    try std.testing.expectEqual(@as(usize, 0), resident);
}

test "Gemma resident embeddings retain CUDA-supported quantized formats" {
    const gemma_cfg: gpt_mod.Config = .{ .family = .gemma };
    try std.testing.expect(shouldKeepResidentGptEmbeddingQuantizedOnly(gemma_cfg, .{ .known = .Q8_0 }));
    try std.testing.expect(shouldKeepResidentGptEmbeddingQuantizedOnly(gemma_cfg, .{ .known = .Q4_0 }));
    try std.testing.expect(shouldKeepResidentGptEmbeddingQuantizedOnly(gemma_cfg, .{ .known = .Q4_K }));
    try std.testing.expect(shouldKeepResidentGptEmbeddingQuantizedOnly(gemma_cfg, .{ .known = .Q6_K }));

    const llama_cfg: gpt_mod.Config = .{ .family = .llama };
    try std.testing.expect(!shouldKeepResidentGptEmbeddingQuantizedOnly(llama_cfg, .{ .known = .Q6_K }));
}

test "Qwen3 resident quantized-only policy covers Q8 embedding projections and reranker head" {
    const config: gpt_mod.Config = .{ .family = .qwen3 };
    const q8: gguf_mod.tensor_types.TensorType = .{ .known = .Q8_0 };
    try std.testing.expect(shouldKeepResidentGptEmbeddingQuantizedOnly(config, q8));
    try std.testing.expect(!shouldKeepResidentGptEmbeddingQuantizedOnly(config, .{ .known = .Q4_K }));
    for ([_][]const u8{
        "model.embed_tokens.weight",
        "lm_head.weight",
        "cls.output.weight",
        "model.layers.0.self_attn.q_proj.weight",
        "model.layers.0.self_attn.k_proj.weight",
        "model.layers.0.self_attn.v_proj.weight",
        "model.layers.27.self_attn.o_proj.weight",
        "model.layers.27.mlp.gate_proj.weight",
        "model.layers.27.mlp.up_proj.weight",
        "model.layers.27.mlp.down_proj.weight",
    }) |key| {
        try std.testing.expect(shouldKeepResidentGptWeightQuantizedOnly(config, key, q8));
        try std.testing.expect(!shouldKeepResidentGptWeightQuantizedOnly(config, key, .{ .known = .Q4_K }));
        try std.testing.expect(!shouldKeepResidentGptWeightQuantizedOnly(config, key, .{ .known = .BF16 }));
        try std.testing.expect(!shouldKeepResidentGptWeightQuantizedOnly(config, key, null));
    }
    for ([_][]const u8{
        "model.layers.0.input_layernorm.weight",
        "model.layers.0.self_attn.q_norm.weight",
        "model.layers.0.self_attn.q_proj.bias",
        "model.layers.0.block_sparse_moe.experts.0.w1.weight",
        "model.layers.0.block_sparse_moe.gate.weight",
        "model.per_layer_input.per_layer_token_embd.weight",
        "visual.blocks.0.self_attn.q_proj.weight",
    }) |key| try std.testing.expect(!shouldKeepResidentGptWeightQuantizedOnly(config, key, q8));
}

test "serving policy does not disable existing gguf weight mappings" {
    var buf: [256]u8 = undefined;
    try std.testing.expectEqualStrings(
        "model.layers.3.self_attn.q_proj.weight",
        normalizeGgufGptWeightKey(.{ .family = .mistral }, "blk.3.attn_q.weight", &buf).?,
    );
    try std.testing.expect(normalizeGgufGptWeightKey(
        .{ .family = .falcon },
        "blk.3.attn_q.weight",
        &buf,
    ) == null);
}

test "deepseek v4 gguf architecture name is recognized" {
    // llama.cpp writes "deepseek4", not any of the HF config.json spellings. Without
    // this the whole deepseek_v4 implementation is unreachable from GGUF.
    try std.testing.expectEqual(gpt_mod.ModelFamily.deepseek_v4, gpt_mod.detectFamily("deepseek4"));
    try std.testing.expect(gpt_mod.isGenerativeModel("deepseek4"));
}

test "i32 index tables pass gguf tensor type inspection" {
    // DeepSeek V4 stores hash-MoE routing tables as I32. They are indices, not weights,
    // and quant_codec materializes them; inspection must not reject the file.
    try std.testing.expect(tensorTypeSupported(.{ .known = .I32 }));
}

test "unsupported families keep their gguf weight mapping" {
    // The mapping predicate is independent of the support tier. deepseek_v4 is blocked
    // for generation but its tensor names must still resolve, both because its mapping
    // tests depend on them and because blocking a family should not silently change what
    // the loader can parse.
    var buf: [256]u8 = undefined;
    try std.testing.expect(gpt_mod.ggufWeightMappingSupported(.deepseek_v4));
    try std.testing.expectEqualStrings(
        "model.layers.0.self_attn.q_a_proj.weight",
        normalizeGgufGptWeightKey(.{ .family = .deepseek_v4 }, "blk.0.attn_q_a.weight", &buf).?,
    );
}

test "LightOnOCR3 4B budget covers both weight domains and preserves explicit limits" {
    const cfg = try gpt_mod.parseConfig(std.testing.allocator, @embedFile("../models/testdata/lightonocr3/config-4b.json"));
    var capabilities = [_][]const u8{"lightonocr3"};
    var tasks = [_][]const u8{ "read", "generate" };
    const manifest = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .model_type = .generator,
        .config_model_arch = "qwen3_5",
        .capabilities = &capabilities,
        .tasks = &tasks,
    };
    const weight_bytes = 9_078_531_072;
    const policy = lightOnOcr4bBudgetPolicy(weight_bytes, manifest, .{ .gpt = cfg }).?;
    try std.testing.expect(policy.budget_floor.host_limit_bytes > weight_bytes);
    try std.testing.expect(policy.budget_floor.backend_limit_bytes > weight_bytes);
    try std.testing.expect(policy.budget_floor.combined_limit_bytes > policy.budget_floor.host_limit_bytes + policy.budget_floor.backend_limit_bytes);
    try std.testing.expectEqual(policy.budget_floor.host_limit_bytes, policy.shared_cache_floor.host_limit_bytes);
    try std.testing.expectEqual(policy.budget_floor.backend_limit_bytes, policy.shared_cache_floor.backend_limit_bytes);
    var unqualified = manifest;
    unqualified.capabilities = &.{};
    try std.testing.expect(lightOnOcr4bBudgetPolicy(weight_bytes, unqualified, .{ .gpt = cfg }) == null);
    const small = try gpt_mod.parseConfig(std.testing.allocator, @embedFile("../models/testdata/lightonocr3/config.json"));
    try std.testing.expect(lightOnOcr4bBudgetPolicy(weight_bytes, manifest, .{ .gpt = small }) == null);
    const overridden = runtime.tier.memory.applyLimitOverrides(policy.budget_floor, .{ .backend_limit_bytes = 1 });
    try std.testing.expectEqual(@as(usize, 1), overridden.backend_limit_bytes);
}

test "embeddinggemma2 coefficient cache relinquishes device credit before recovery and readmits before publication" {
    if (!build_options.enable_metal) return error.SkipZigTest;
    const Cache = @import("antfly_video").preparation.CoefficientCache;
    const device = metal_runtime.termite_metal_video_preparation_device() orelse return error.SkipZigTest;
    var controller = runtime.tier.memory.AdmissionController{};
    defer controller.deinit();
    // This probe exercises only the independently owned preparation context;
    // model weights and execution-provider fields are deliberately unused.
    var self: ArchSession = undefined;
    self.backend_type = .metal;
    self.io = std.testing.io;
    self.video_coefficient_mutex = .init;
    self.video_coefficient_permit = null;
    self.video_coefficient_device_permit = null;
    self.video_coefficient_suspended = false;
    self.video_coefficients = try Cache.init(device, Cache.device_budget);
    defer if (self.video_coefficient_permit) |*permit| permit.deinit();
    defer if (self.video_coefficient_device_permit) |*permit| permit.deinit();
    defer self.video_coefficients.?.deinit();
    const session = Session{ .ptr = &self, .vtable = &arch_vtable, .run_admission = .{ .controller = &controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 128 * 1024 * 1024, .backend_limit_bytes = 128 * 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    self.video_coefficient_permit = try session.admitRetainedMedia(Cache.host_budget, Cache.device_budget);
    try std.testing.expectEqual(@as(usize, Cache.device_budget), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, Cache.device_budget), try reclaimVideoPreparationCoefficients(session, .{}));
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, Cache.host_budget), controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), try reclaimVideoPreparationCoefficients(session, .{}));
    var peer = try session.admitRetainedMedia(0, 120 * 1024 * 1024);
    defer peer.deinit();
    try std.testing.expect((try videoPreparationContext(session, true)).coefficients == null);
    try std.testing.expect(self.video_coefficient_suspended);
    try std.testing.expectEqual(@as(usize, 120 * 1024 * 1024), controller.snapshot().backend_scratch_bytes);
    peer.deinit();
    try std.testing.expect((try videoPreparationContext(session, true)).coefficients != null);
    try std.testing.expect(!self.video_coefficient_suspended);
    try std.testing.expectEqual(@as(usize, Cache.device_budget), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, Cache.device_budget), try reclaimVideoPreparationCoefficients(session, .{}));
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
}

test "resolved decoder contract survives wrappers and common CPU executor preparation" {
    const a = std.testing.allocator;
    var owner = ArchSession{
        .allocator = a,
        .arch_config = .{ .gpt = .{ .hidden_size = 8, .num_hidden_layers = 1, .num_attention_heads = 2, .intermediate_size = 16, .vocab_size = 32 } },
        .backend_type = .native,
        .backend_data = .{ .native = .{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty } },
    };
    var wrapped = archSessionHandle(&owner);
    var wrapper_vtable = arch_vtable;
    wrapped.vtable = &wrapper_vtable;
    try std.testing.expectEqual(@as(usize, 8), getGptConfig(wrapped).?.hidden_size);
    try std.testing.expect(wrapped.model_contract.decoder_config.? == &owner.arch_config.gpt);
    var executor = (try @import("../execution/model_candidate.zig").createSessionExecutor(a, wrapped, .{})).?;
    defer executor.deinit();
    var runtime_instance = try executor.createRuntime(a);
    defer runtime_instance.deinit();
    // Reading resolved semantics never dereferences a wrapped implementation.
    wrapped.ptr = @ptrFromInt(8);
    try std.testing.expectEqual(@as(usize, 32), getGptConfig(wrapped).?.vocab_size);
}

test "LightOnOCR3 CUDA loader profile requires the reader role and supported recurrent geometry" {
    const a = std.testing.allocator;
    const cfg = try gpt_mod.parseConfig(a, @embedFile("../models/testdata/lightonocr3/config.json"));
    var capabilities = [_][]const u8{"lightonocr3"};
    var manifest = manifest_mod.ModelManifest{
        .allocator = a,
        .model_type = .generator,
        .config_model_arch = "qwen3_5",
        .capabilities = &capabilities,
    };
    try std.testing.expectEqual(CudaCapabilityProfile.lightonocr3, cudaProfileForArch(.{ .gpt = cfg }, .generic, &manifest).?);
    try std.testing.expect(cudaProfileForArch(.{ .gpt = cfg }, .classifier, &manifest) == null);
    try std.testing.expect(cudaProfileForArch(.{ .gpt = cfg }, .extractor, &manifest) == null);
    var unsupported = cfg;
    unsupported.qwen35_linear_key_head_dim = 64;
    try std.testing.expect(cudaProfileForArch(.{ .gpt = unsupported }, .generic, &manifest) == null);
    unsupported = cfg;
    unsupported.vision_temporal_patch_size = 1;
    try std.testing.expect(cudaProfileForArch(.{ .gpt = unsupported }, .generic, &manifest) == null);
    manifest.capabilities = &.{};
    try std.testing.expect(cudaProfileForArch(.{ .gpt = cfg }, .generic, &manifest) == null);
}

test "device weight preparation and final session ownership unwind every allocation failure" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"bert\",\"hidden_size\":4,\"num_hidden_layers\":1,\"num_attention_heads\":1,\"intermediate_size\":8,\"vocab_size\":2}" });
    const header = "{\"bert.embeddings.word_embeddings.weight\":{\"dtype\":\"F32\",\"shape\":[2,4],\"data_offsets\":[0,32]}}";
    const padded_len = comptime std.mem.alignForward(usize, header.len, 8);
    var bytes: [8 + padded_len + 32]u8 = @splat(' ');
    std.mem.writeInt(u64, bytes[0..8], padded_len, .little);
    @memcpy(bytes[8..][0..header.len], header);
    @memcpy(bytes[8 + padded_len ..], std.mem.asBytes(&[_]f32{ 1, 2, 3, 4, 5, 6, 7, 8 }));
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "model.safetensors", .data = &bytes });
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(path);
    const Case = struct {
        fn run(allocator: std.mem.Allocator, model_path: []const u8) !void {
            var session = try createMetalSession(allocator, model_path);
            defer session.close();
            const owner: *ArchSession = @ptrCast(@alignCast(session.ptr));
            const weights = gpuBackendData(owner);
            try std.testing.expect(weights.prefetch_initialized);
            try std.testing.expectEqual(@as(u32, 1), weights.lazy_weights.count());
            const entry = weights.lazy_weights.get("embeddings.word_embeddings.weight") orelse return error.MissingWeight;
            try std.testing.expectEqual(@as(usize, 32), entry.tensor_ref.byte_len);
        }
    };
    // Includes failure to allocate the final session after source preparation,
    // and failures after queue installation. Both must retire the same owner.
    try platform.allocator.checkAllAllocationFailures(a, Case.run, .{path});
}

const IncrementalFixture = struct {
    fn session(a: std.mem.Allocator) !Session {
        var store = NativeData{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
        errdefer store.deinitOwned();
        try @import("testing/incremental_encoder_decoder.zig").populate(&store);
        const owner = try a.create(ArchSession);
        owner.* = .{ .allocator = a, .arch_config = .{ .whisper = @import("testing/incremental_encoder_decoder.zig").config }, .backend_type = .native, .backend_data = .{ .native = store } };
        owner.model_lifetime.prepareFinalResources() catch |err| {
            a.destroy(owner);
            return err;
        };
        return archSessionHandle(owner);
    }

    fn features(a: std.mem.Allocator) !Tensor {
        return Tensor.initFloat32(a, "input_features", &.{ 1, 2, 4 }, &.{ 0.2, 0.5, -0.1, 0.4, 0.3, -0.2, 0.7, 0.1 });
    }

    fn source(outputs: *@import("../backends/session.zig").ResidentOutputs, a: std.mem.Allocator, receiver: IncrementalDecoderAdapter.Receiver) !RetainedEncoderSource {
        const shape = try outputs.backend.tensorShape(outputs.outputs[0], a);
        defer a.free(shape);
        if (shape.len != 3) return error.InvalidInputShape;
        return .{ .receiver = receiver, .value = outputs.outputs[0], .backend = outputs.backend, .shape = .{ shape[0], shape[1], shape[2] }, .resources = try outputs.retainResources(a) };
    }
};

test "native incremental resident source matches actual host encoder and KV steps after source and destination close" {
    const a = std.testing.allocator;
    const encoder = try IncrementalFixture.session(a);
    var encoder_open = true;
    defer if (encoder_open) encoder.close();
    const destination = try IncrementalFixture.session(a);
    var destination_open = true;
    defer if (destination_open) destination.close();
    var features = [_]Tensor{try IncrementalFixture.features(a)};
    defer features[0].deinit();
    const host = try encoder.run(&features, a);
    defer {
        for (host) |*value| value.deinit();
        a.free(host);
    }
    var reference = (try whisperNativeDecoder(destination, a, null, host[0].asFloat32(), 2)).?;
    var reference_open = true;
    defer if (reference_open) reference.deinit();
    const first = try reference.step(&.{ 0, 1 });
    defer a.free(first);
    const second = try reference.step(&.{2});
    defer a.free(second);
    reference.deinit();
    reference_open = false;
    var outputs = (try encoder.runResident(&features, a)).?;
    var outputs_open = true;
    defer if (outputs_open) outputs.deinit();
    const resident_values = try outputs.backend.toFloat32(outputs.outputs[0], a);
    defer a.free(resident_values);
    try std.testing.expectEqualSlices(f32, host[0].asFloat32(), resident_values);
    const receiver = IncrementalDecoderAdapter.qualify(encoder, destination).?;
    try std.testing.expect(!receiver.same_model_owner);
    var decoder = try IncrementalDecoderAdapter.fromResident(destination, a, null, try IncrementalFixture.source(&outputs, a, receiver));
    defer decoder.deinit();
    outputs.deinit();
    outputs_open = false;
    encoder.close();
    encoder_open = false;
    destination.close();
    destination_open = false;
    const actual_first = try decoder.step(&.{ 0, 1 });
    defer a.free(actual_first);
    const actual_second = try decoder.step(&.{2});
    defer a.free(actual_second);
    try std.testing.expectEqualSlices(f32, first, actual_first);
    try std.testing.expectEqualSlices(f32, second, actual_second);
    try std.testing.expectEqual(@as(usize, 3), decoder.positions());
    try std.testing.expect(decoder.cache.layers[0].k_cross != decoder.encoder_hidden);
}

test "native incremental same-session receiver owns private compute and keeps zero-copy source alive" {
    const a = std.testing.allocator;
    const session = try IncrementalFixture.session(a);
    var open = true;
    defer if (open) session.close();
    var features = [_]Tensor{try IncrementalFixture.features(a)};
    defer features[0].deinit();
    var outputs = (try session.runResident(&features, a)).?;
    var outputs_open = true;
    defer if (outputs_open) outputs.deinit();
    const original_backend = outputs.backend.*;
    const receiver = IncrementalDecoderAdapter.qualify(session, session).?;
    try std.testing.expect(receiver.same_model_owner);
    var decoder = try IncrementalDecoderAdapter.fromResident(session, a, null, try IncrementalFixture.source(&outputs, a, receiver));
    defer decoder.deinit();
    try std.testing.expect(original_backend.ptr != decoder.cb.backend.ptr);
    try std.testing.expectEqual(original_backend.kind(), decoder.cb.backend.kind());
    try std.testing.expect(decoder.cb.owns_backend);
    try std.testing.expectEqual(outputs.outputs[0], decoder.encoder_hidden);
    try std.testing.expect(!decoder.encoder_owned);
    outputs.deinit();
    outputs_open = false;
    session.close();
    open = false;
    const logits = try decoder.step(&.{0});
    defer a.free(logits);
    try std.testing.expectEqual(@as(usize, 4), logits.len);
}

test "native incremental host receiver retains model through actual KV steps after Session close" {
    const a = std.testing.allocator;
    const session = try IncrementalFixture.session(a);
    var open = true;
    defer if (open) session.close();
    const hidden = [_]f32{ 0.2, 0.4, -0.1, 0.7, 0.5, 0.6, -0.2, 0.3 };
    var decoder = (try whisperNativeDecoder(session, a, null, &hidden, 2)).?;
    defer decoder.deinit();
    session.close();
    open = false;
    const logits = try decoder.step(&.{ 0, 1 });
    defer a.free(logits);
    try std.testing.expectEqual(@as(usize, 4), logits.len);
    try std.testing.expectEqual(@as(usize, 2), decoder.positions());
}

test "native incremental constructor unwinds every allocation failure preserving complete resident source" {
    const Probe = struct {
        fn run(failing: std.mem.Allocator) !void {
            const a = std.testing.allocator;
            const session = try IncrementalFixture.session(a);
            defer session.close();
            const destination = try IncrementalFixture.session(a);
            defer destination.close();
            var features = [_]Tensor{try IncrementalFixture.features(a)};
            defer features[0].deinit();
            var outputs = (try session.runResident(&features, a)).?;
            defer outputs.deinit();
            const receiver = IncrementalDecoderAdapter.qualify(session, destination).?;
            const source = try IncrementalFixture.source(&outputs, a, receiver);
            var decoder = try IncrementalDecoderAdapter.fromResident(destination, failing, null, source);
            defer decoder.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "native incremental actual cache tensors match canonical geometry and remain admitted through decoder destruction" {
    const a = std.testing.allocator;
    var controller = runtime.tier.memory.AdmissionController{};
    defer controller.deinit();
    var session = try IncrementalFixture.session(a);
    var open = true;
    defer if (open) session.close();
    session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 16 * 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false };
    const hidden = [_]f32{ 0.2, 0.4, -0.1, 0.7, 0.5, 0.6, -0.2, 0.3 };
    var decoder = (try whisperNativeDecoder(session, a, null, &hidden, 2)).?;
    var decoder_open = true;
    defer if (decoder_open) decoder.deinit();
    const geometry = try whisper_arch.CacheGeometry.init(decoder.config, 2);
    const cache_amounts = try geometry.cacheAmounts(&decoder.cb.backend);
    try std.testing.expectEqual(cache_amounts.host_kv_bytes, controller.snapshot().host_kv_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_kv_bytes);
    try std.testing.expectEqual(geometry.capacity, decoder.cache.capacity);
    const cross = try decoder.cb.backend.toFloat32(decoder.cache.layers[0].k_cross, a);
    defer a.free(cross);
    const slab = try decoder.cb.backend.toFloat32(decoder.cache.layers[0].k_self.?, a);
    defer a.free(slab);
    try std.testing.expectEqual(geometry.cross_tensor_bytes, cross.len * @sizeOf(f32));
    try std.testing.expectEqual(geometry.self_tensor_bytes, slab.len * @sizeOf(f32));
    session.close();
    open = false;
    try std.testing.expectEqual(cache_amounts.host_kv_bytes, controller.snapshot().host_kv_bytes);
    decoder.deinit();
    decoder_open = false;
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_kv_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "native incremental private cache is denied before actual cache weight lookup and releases base request" {
    const a = std.testing.allocator;
    var controller = runtime.tier.memory.AdmissionController{};
    defer controller.deinit();
    const owner = try a.create(ArchSession);
    owner.* = .{ .allocator = a, .arch_config = .{ .whisper = .{ .d_model = 4, .encoder_layers = 0, .decoder_layers = 1, .decoder_attention_heads = 2, .decoder_ffn_dim = 8, .vocab_size = 4, .max_target_positions = 8 } }, .backend_type = .native, .backend_data = .{ .native = .{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty } } };
    var session = archSessionHandle(owner);
    defer session.close();
    session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false };
    const geometry = try whisper_arch.CacheGeometry.init(owner.arch_config.whisper, 2);
    var base = try incrementalBasePermit(session, owner.arch_config.whisper, geometry, false, false);
    const base_amounts = base.lease.?.amounts;
    base.deinit();
    session.run_admission.?.limits.host_limit_bytes = base_amounts.hostTotalBytes();
    var sizing_compute = native_mod.NativeCompute.init(a, &owner.backend_data.native, null);
    defer sizing_compute.deinit();
    const sizing_backend = sizing_compute.computeBackend();
    const extra = try geometry.cacheAmounts(&sizing_backend);
    // The incremental request fits alone. The live base reservation occupies
    // the cap when the constructor requests its supplement, so ordinary
    // admission classifies this as temporary capacity pressure.
    try std.testing.expect(extra.hostTotalBytes() <= base_amounts.hostTotalBytes());
    try std.testing.expect(extra.hostTotalBytes() > 0);

    const hidden = [_]f32{ 0.2, 0.4, -0.1, 0.7, 0.5, 0.6, -0.2, 0.3 };
    // This model deliberately has no decoder weights. Reaching Cache.init
    // would produce MissingWeight rather than the expected admission failure.
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, whisperNativeDecoder(session, a, null, &hidden, 2));
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_kv_bytes);
}

test "native incremental stage receiver qualifies explicitly and keeps real source through task encoder retirement" {
    const a = std.testing.allocator;
    const stages = @import("encoder_decoder_runtime.zig");
    const decoder_stage = @import("decoder_stage.zig");
    const encoder = try IncrementalFixture.session(a);
    defer encoder.close();
    const destination = try IncrementalFixture.session(a);
    defer destination.close();
    var features = [_]Tensor{try IncrementalFixture.features(a)};
    defer features[0].deinit();
    var stateless = try stages.encodeWithPermit(a, encoder, destination, &features, null, null, null);
    defer stateless.deinit(a);
    try std.testing.expect(stateless == .host);
    var reference = try decoder_stage.Decoder.init(a, destination, &stateless, 2, &.{ 1, 1 }, 4, null);
    var reference_open = true;
    defer if (reference_open) reference.deinit();
    var first = try reference.step(&.{ 0, 1 }, null, null);
    defer first.deinit();
    var second = try reference.step(&.{ 0, 1, 2 }, null, null);
    defer second.deinit();
    reference.deinit();
    reference_open = false;
    var encoded = try stages.encodeForIncrementalWithPermit(a, encoder, destination, &features, null, null, null);
    var encoded_open = true;
    defer if (encoded_open) encoded.deinit(a);
    try std.testing.expect(encoded == .resident);
    var source = (try decoder_stage.retainIncrementalSource(a, &encoded, destination)).?;
    var source_open = true;
    defer if (source_open) source.deinit();
    var runtime_decoder = try decoder_stage.Decoder.initWithSource(a, destination, &encoded, 2, &.{ 1, 1 }, 4, null, .{}, &source);
    defer runtime_decoder.deinit();
    source.deinit();
    source_open = false;
    encoded.deinit(a);
    encoded_open = false;
    var actual_first = try runtime_decoder.step(&.{ 0, 1 }, null, null);
    defer actual_first.deinit();
    var actual_second = try runtime_decoder.step(&.{ 0, 1, 2 }, null, null);
    defer actual_second.deinit();
    try std.testing.expectEqualSlices(f32, first.logits, actual_first.logits);
    try std.testing.expectEqualSlices(f32, second.logits, actual_second.logits);
}

test "native incremental host constructor unwinds actual backend and cache allocation failures" {
    const Probe = struct {
        fn run(failing: std.mem.Allocator) !void {
            const session = try IncrementalFixture.session(std.testing.allocator);
            defer session.close();
            const hidden = [_]f32{ 0.2, 0.4, -0.1, 0.7, 0.5, 0.6, -0.2, 0.3 };
            var decoder = (try whisperNativeDecoder(session, failing, null, &hidden, 2)).?;
            defer decoder.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "native incremental cancellation at actual destination gate preserves source owner and gate ownership" {
    const a = std.testing.allocator;
    const encoder = try IncrementalFixture.session(a);
    defer encoder.close();
    var destination = try IncrementalFixture.session(a);
    defer destination.close();
    var gate: std.atomic.Mutex = .unlocked;
    try std.testing.expect(gate.tryLock());
    var gate_locked = true;
    defer if (gate_locked) gate.unlock();
    destination.execution_gate = &gate;
    var features = [_]Tensor{try IncrementalFixture.features(a)};
    defer features[0].deinit();
    var outputs = (try encoder.runResident(&features, a)).?;
    defer outputs.deinit();
    const receiver = IncrementalDecoderAdapter.qualify(encoder, destination).?;
    var source = try IncrementalFixture.source(&outputs, a, receiver);
    defer source.deinit();
    const before = destination.model_lifetime.?.references.load(.acquire);
    try std.testing.expectError(error.Timeout, IncrementalDecoderAdapter.fromResident(destination, a, .{ .deadline_ns = 0 }, try source.retained()));
    try std.testing.expectEqual(before, destination.model_lifetime.?.references.load(.acquire));
    try std.testing.expect(!gate.tryLock());
    const values = try outputs.backend.toFloat32(outputs.outputs[0], a);
    defer a.free(values);
    try std.testing.expectEqual(@as(usize, 8), values.len);
    gate.unlock();
    gate_locked = false;
    var decoder = try IncrementalDecoderAdapter.fromResident(destination, a, null, try source.retained());
    defer decoder.deinit();
    const logits = try decoder.step(&.{0});
    defer a.free(logits);
    try std.testing.expectEqual(@as(usize, 4), logits.len);
}

test "native incremental borrowed provider retires pending command callbacks before actual encoder and KV storage" {
    const a = std.testing.allocator;
    var cleanup = try ReceiverCleanup.beginStandalone();
    var cleanup_moved = false;
    defer if (!cleanup_moved) cleanup.finish();
    const Probe = struct {
        compute: native_mod.NativeCompute,
        submitted: bool = false,
        encoded: bool = false,
        fail_wait: bool = false,
        fail_cancel: bool = false,
        frees: usize = 0,
        waits: usize = 0,
        cancels: usize = 0,
        fn from(raw: *anyopaque) *@This() {
            const compute: *native_mod.NativeCompute = @ptrCast(@alignCast(raw));
            return @fieldParentPtr("compute", compute);
        }
        fn wait(raw: *anyopaque) !void {
            const self = from(raw);
            self.waits += 1;
            if (self.fail_wait) return error.InjectedPendingRetirementFailure;
            self.submitted = false;
        }
        fn active(raw: *anyopaque) bool {
            return from(raw).encoded;
        }
        fn cancel(raw: *anyopaque) !void {
            const self = from(raw);
            std.debug.assert(!self.submitted);
            self.cancels += 1;
            if (self.fail_cancel) return error.InjectedPendingCancellationFailure;
            self.encoded = false;
        }
        fn free(raw: *anyopaque, value: ops.CT) void {
            const self = from(raw);
            std.debug.assert(!self.submitted and !self.encoded);
            self.frees += 1;
            native_mod.vtable_impl.freeTensor(raw, value);
        }
    };
    var weights = NativeData{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer weights.deinitOwned();
    try @import("testing/incremental_encoder_decoder.zig").populate(&weights);
    var probe = Probe{ .compute = native_mod.NativeCompute.init(a, &weights, null) };
    defer probe.compute.deinit();
    var table = native_mod.vtable_impl;
    table.regions.commands.waitSubmittedFrame = Probe.wait;
    table.regions.commands.hasActiveFrame = Probe.active;
    table.regions.commands.cancelFrame = Probe.cancel;
    table.freeTensor = Probe.free;
    const cb = ops.ComputeBackend{ .ptr = &probe.compute, .vtable = &table };
    const config = @import("testing/incremental_encoder_decoder.zig").config;
    const mel = try cb.fromFloat32Shape(&.{ 0.2, 0.5, -0.1, 0.4, 0.3, -0.2, 0.7, 0.1 }, &.{ 1, 2, 4 });
    defer cb.free(mel);
    const encoded = try whisper_arch.encoderForwardTensor(&cb, a, config, mel, 1, 4);
    const values = try a.alloc(ops.CT, 1);
    values[0] = encoded;
    var outputs = ResidentOutputs{ .outputs = values, .backend = &cb, .allocator = a };
    var outputs_open = true;
    defer if (outputs_open) outputs.deinit();
    const source = try outputs.retainResources(a);
    var source_open = true;
    defer if (source_open) source.release();
    const cache = try whisper_arch.DecodeCache.init(&cb, a, config, encoded, 2);
    var decoder = WhisperNativeDecoder{ .allocator = a, .cb = .{ .backend = cb, .guard = .{}, .owns_backend = false }, .config = config, .encoder_hidden = encoded, .cache = cache, .gate = null, .encoder_owned = false, .source_resources = source, .receiver_cleanup = cleanup };
    cleanup_moved = true;
    source_open = false;
    var decoder_open = true;
    defer if (decoder_open) decoder.deinit();
    outputs.deinit();
    outputs_open = false;
    const logits = try decoder.step(&.{0});
    defer a.free(logits);
    const before_frees = probe.frees;
    probe.submitted = true;
    probe.encoded = true;
    probe.fail_wait = true;
    try std.testing.expectError(error.InjectedPendingRetirementFailure, WhisperNativeDecoder.retirePending(&cb));
    try std.testing.expect(probe.submitted and probe.encoded);
    try std.testing.expectEqual(before_frees, probe.frees);
    probe.fail_wait = false;
    probe.fail_cancel = true;
    try std.testing.expectError(error.InjectedPendingCancellationFailure, WhisperNativeDecoder.retirePending(&cb));
    try std.testing.expect(!probe.submitted and probe.encoded);
    try std.testing.expectEqual(before_frees, probe.frees);
    probe.fail_cancel = false;
    decoder.deinit();
    decoder_open = false;
    try std.testing.expect(!probe.submitted and !probe.encoded);
    try std.testing.expectEqual(@as(usize, 3), probe.waits);
    try std.testing.expectEqual(@as(usize, 2), probe.cancels);
    try std.testing.expect(probe.frees > before_frees);
}

fn incrementalMetalFixtureSession(a: std.mem.Allocator) !Session {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    var native_weights = NativeData{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer native_weights.deinitOwned();
    try @import("testing/incremental_encoder_decoder.zig").populate(&native_weights);
    var hosted = GpuHostedData{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    errdefer @import("../backends/compute_factory.zig").closeMetalWeights(a, &hosted);
    var iterator = native_weights.resident_weights.iterator();
    while (iterator.next()) |entry| {
        const key = try a.dupe(u8, entry.key_ptr.*);
        errdefer a.free(key);
        const reference_name = try a.dupe(u8, entry.key_ptr.*);
        errdefer a.free(reference_name);
        var value = try Tensor.initFloat32(a, key, entry.value_ptr.tensor.shape, entry.value_ptr.tensor.asFloat32());
        errdefer value.deinit();
        try hosted.lazy_weights.put(a, key, .{ .tensor_ref = .{ .name = reference_name }, .host_loaded = .{ .tensor = value } });
    }
    try @import("../backends/compute_factory.zig").initializeMetalWeightPrefetch(a, &hosted, false);
    const owner = try a.create(ArchSession);
    owner.* = .{ .allocator = a, .arch_config = .{ .whisper = @import("testing/incremental_encoder_decoder.zig").config }, .backend_type = .metal, .backend_data = .{ .metal = hosted } };
    owner.model_lifetime.prepareFinalResources() catch |err| {
        a.destroy(owner);
        return err;
    };
    return archSessionHandle(owner);
}

test "native incremental actual Metal owned provider preserves source lease and two KV steps across retries" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    const session = try incrementalMetalFixtureSession(a);
    defer session.close();
    const control = InferenceExecutionControl{ .native_call_policy = .in_process };
    var features = [_]Tensor{try IncrementalFixture.features(a)};
    defer features[0].deinit();
    const host = try session.runWithControl(&features, a, control);
    defer {
        for (host) |*value| value.deinit();
        a.free(host);
    }
    var reference = (try whisperNativeDecoder(session, a, control, host[0].asFloat32(), 2)).?;
    var reference_open = true;
    defer if (reference_open) reference.deinit();
    const first = try reference.step(&.{ 0, 1 });
    defer a.free(first);
    const second = try reference.step(&.{2});
    defer a.free(second);
    reference.deinit();
    reference_open = false;
    var encoder_permit = try session.admitInputs(&features, a, control);
    defer encoder_permit.deinit();
    try encoder_permit.prepareResidentOutputRetirement();
    var outputs = (try encoder_permit.runResidentWithControl(&features, a, control)).?;
    if (outputs.resource_lease == null) {
        outputs.resource_lease = encoder_permit.lease;
        encoder_permit.lease = null;
    }
    var outputs_open = true;
    defer if (outputs_open) outputs.deinit();
    const source_cb = outputs.backend.*;
    const source_compute: *metal_compute_mod.MetalCompute = @ptrCast(@alignCast(source_cb.ptr));
    const receiver = IncrementalDecoderAdapter.qualify(session, session).?;
    try std.testing.expect(receiver.same_model_owner);
    var source = try IncrementalFixture.source(&outputs, a, receiver);
    defer source.deinit();
    outputs.deinit();
    outputs_open = false;
    for (0..2) |_| {
        const before = @import("../ops/media_residency_test.zig").Transfers.snapshot(source_cb);
        var decoder = try IncrementalDecoderAdapter.fromResident(session, a, control, try source.retained());
        var decoder_open = true;
        defer if (decoder_open) decoder.deinit();
        const compute: *metal_compute_mod.MetalCompute = @ptrCast(@alignCast(decoder.cb.backend.ptr));
        try std.testing.expect(source_cb.ptr != decoder.cb.backend.ptr);
        try std.testing.expectEqual(source_compute.provider_impl, compute.provider_impl);
        try std.testing.expect(compute.shared_provider_lease_io == null);
        try std.testing.expect(source_compute.shared_provider_lease_io != null);
        try std.testing.expect(decoder.cb.owns_backend and !decoder.encoder_owned);
        try std.testing.expectEqual(source.value, decoder.encoder_hidden);
        try before.expectUnchanged(source_cb);
        const actual_first = try decoder.step(&.{ 0, 1 });
        defer a.free(actual_first);
        const actual_second = try decoder.step(&.{2});
        defer a.free(actual_second);
        for (first, actual_first) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.0001);
        for (second, actual_second) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.0001);
        try std.testing.expectEqual(@as(usize, 3), decoder.positions());
        decoder.deinit();
        decoder_open = false;
        // Nested compute retirement must leave the real source lease held;
        // acquiring an unrelated numerical owner still fails immediately.
        const owner: *ArchSession = @ptrCast(@alignCast(session.ptr));
        try std.testing.expectError(error.QueueFull, metal_compute_mod.MetalCompute.init(a, gpuBackendData(owner), null));
        try std.testing.expect(source_compute.shared_provider_lease_io != null);
    }
}

const IncrementalAggregateGateFixture = struct {
    const tokenizer_json = "{\"version\":\"1.0\",\"added_tokens\":[{\"id\":0,\"content\":\"<unk>\"},{\"id\":10,\"content\":\"<|en|>\"},{\"id\":11,\"content\":\"<|es|>\"},{\"id\":12,\"content\":\"<|transcribe|>\"},{\"id\":13,\"content\":\"<|notimestamps|>\"}],\"model\":{\"type\":\"BPE\",\"vocab\":{\"<unk>\":0,\"<|en|>\":10,\"<|es|>\":11,\"<|transcribe|>\":12,\"<|notimestamps|>\":13},\"merges\":[]}}";
    fn write(dir: std.Io.Dir) !void {
        try dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"t5\",\"d_model\":4,\"num_heads\":2,\"num_layers\":1,\"vocab_size\":4}" });
        try dir.writeFile(std.testing.io, .{ .sub_path = "tokenizer.json", .data = tokenizer_json });
    }
    fn inside(gate: ?*std.atomic.Mutex, aggregate: *anyopaque, size: usize) bool {
        const address = @intFromPtr(gate orelse return false);
        const start = @intFromPtr(aggregate);
        return address >= start and address - start < size;
    }
    fn tokenizer(_: *anyopaque, _: []const u8) !@import("../models/managed_tokenizer.zig").Managed {
        return .{ .tokenizer = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(std.testing.allocator, tokenizer_json) };
    }
    fn session(_: *anyopaque, _: []const u8, _: []const BackendType, _: ?*@import("../backends/imported_onnx_session.zig").SharedBackendContext, control: ?InferenceExecutionControl) !@import("../models/session_loader.zig").ManagedSession {
        if (control) |active| try active.check();
        return .{ .session = try IncrementalFixture.session(std.testing.allocator) };
    }
    fn plan(raw: *anyopaque, _: []const u8, _: []const BackendType, paths: []const []const u8) !@import("../models/component_loader.zig").ComponentLoader {
        var loader = @import("../models/component_loader.zig").ComponentLoader{ .services = .{ .allocator = std.testing.allocator, .context = raw, .load_session_fn = session, .load_tokenizer_fn = tokenizer } };
        loader.allowed_backends[0] = .native;
        loader.allowed_backend_count = 1;
        for (paths) |path| try loader.addComponentPath(path);
        return loader;
    }
    fn identity(_: *anyopaque, _: @import("../models/composite_runtime.zig").Request) ![32]u8 {
        return @splat(7);
    }
    fn loaded(path: []const u8, sessions: *@import("../backends/backends.zig").SessionManager) !*@import("../models/loaded_runtime.zig").Runtime {
        const a = std.testing.allocator;
        const owner = @import("../models/loaded_runtime.zig");
        var manifest = try manifest_mod.loadFromDir(a, path);
        var manifest_open = true;
        defer if (manifest_open) manifest.deinit();
        const native_session = try IncrementalFixture.session(a);
        var session_open = true;
        defer if (session_open) native_session.close();
        const tok = try @import("inference_hf_tokenizer").HfTokenizer.loadFromBytes(a, tokenizer_json);
        var tokenizer_open = true;
        defer if (tokenizer_open) tok.deinitSelf();
        const revision = try owner.resolutionModelRevision(a, std.testing.io, manifest);
        const assets = owner.Assets{ .manifest = manifest, .hf_tok = tok, .sp_tok = null, .session = .{ .session = native_session }, .session_manager = sessions, .host = owner.Host.unmanaged(), .model_dir = path, .preparation_io = std.testing.io, .executor_io = std.testing.io, .resolution_revision = revision, .resolution_file_revision = revision, .native_kv_asset_generation = null, .tokenizer_type = .huggingface, .embedding_signature = null, .tokenizer_resource_lease = null, .kernel_jit_profile_bundle = null };
        manifest_open = false;
        session_open = false;
        tokenizer_open = false;
        return owner.Runtime.fromOwnedAssets(a, assets, null);
    }
};

test "native incremental real LoadedModel destruction cannot retire retained decoder execution gate" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    try IncrementalAggregateGateFixture.write(temporary.dir);
    const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temporary.sub_path[0..] });
    defer a.free(path);
    var sessions = @import("../backends/backends.zig").SessionManager.init(a);
    const model = try IncrementalAggregateGateFixture.loaded(path, &sessions);
    var model_open = true;
    defer if (model_open) {
        model.deinit();
        a.destroy(model);
    };
    const hidden = [_]f32{ 0.2, 0.4, -0.1, 0.7, 0.5, 0.6, -0.2, 0.3 };
    var decoder = (try whisperNativeDecoder(model.session, a, null, &hidden, 2)).?;
    var decoder_open = true;
    defer if (decoder_open) decoder.deinit();
    try std.testing.expect(decoder.gate != null);
    try std.testing.expectEqual(model.session.execution_gate, decoder.gate);
    const gate_in_aggregate = IncrementalAggregateGateFixture.inside(decoder.gate, model, @sizeOf(@import("../models/loaded_runtime.zig").Runtime));
    // Bounded old-route canary: record the invalid lifetime before destroying
    // the real aggregate. Detach only that already-proven stale cleanup pointer
    // so the failure is an assertion instead of touching freed mutex storage.
    if (gate_in_aggregate) decoder.gate = null;
    model.deinit();
    a.destroy(model);
    model_open = false;
    const logits = try decoder.step(&.{0});
    defer a.free(logits);
    try std.testing.expectEqual(@as(usize, 4), logits.len);
    decoder.deinit();
    decoder_open = false;
    try std.testing.expect(!gate_in_aggregate);
}

test "native incremental real CompositeAssets destruction cannot retire retained decoder execution gate" {
    const a = std.testing.allocator;
    const composites = @import("../models/composite_runtime.zig");
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    try IncrementalAggregateGateFixture.write(temporary.dir);
    const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temporary.sub_path[0..] });
    defer a.free(path);
    var token: u8 = 0;
    const assets = try composites.load(a, .{ .context = &token, .io = std.testing.io, .load_tokenizer_fn = IncrementalAggregateGateFixture.tokenizer, .plan_components_fn = IncrementalAggregateGateFixture.plan, .identity_fn = IncrementalAggregateGateFixture.identity }, .{ .model_dir = path, .component_paths = &.{ "encoder.onnx", "decoder.onnx" }, .preferred_backends = &.{.native}, .asset_generation = @splat(7), .runtime_id = 19, .model_type = .rewriter, .kind = .seq2seq }, null);
    var assets_open = true;
    defer if (assets_open) {
        assets.deinit();
        a.destroy(assets);
    };
    const hidden = [_]f32{ 0.2, 0.4, -0.1, 0.7, 0.5, 0.6, -0.2, 0.3 };
    var decoder = (try whisperNativeDecoder(assets.decoder.?.session, a, null, &hidden, 2)).?;
    var decoder_open = true;
    defer if (decoder_open) decoder.deinit();
    try std.testing.expect(decoder.gate != null);
    try std.testing.expectEqual(assets.decoder.?.session.execution_gate, decoder.gate);
    const gate_in_aggregate = IncrementalAggregateGateFixture.inside(decoder.gate, assets, @sizeOf(composites.CompositeAssets));
    if (gate_in_aggregate) decoder.gate = null;
    assets.deinit();
    a.destroy(assets);
    assets_open = false;
    const logits = try decoder.step(&.{0});
    defer a.free(logits);
    try std.testing.expectEqual(@as(usize, 4), logits.len);
    decoder.deinit();
    decoder_open = false;
    try std.testing.expect(!gate_in_aggregate);
}

fn retainedDecoderFixtureSession(allocator: std.mem.Allocator) !Session {
    var store = NativeData{ .allocator = allocator, .resident_weights = .empty, .lazy_weights = .empty };
    errdefer store.deinitOwned();
    try @import("testing/decoder.zig").populate(&store);
    const owner = try allocator.create(ArchSession);
    owner.* = .{ .allocator = allocator, .arch_config = .{ .gpt = @import("testing/decoder.zig").config() }, .backend_type = .native, .backend_data = .{ .native = store } };
    owner.model_lifetime.prepareFinalResources() catch |err| {
        allocator.destroy(owner);
        return err;
    };
    return archSessionHandle(owner);
}

fn retainedDecoderFixtureModel(path: []const u8, sessions: *@import("../backends/backends.zig").SessionManager, source: Session) !*@import("../models/loaded_runtime.zig").Runtime {
    const a = std.testing.allocator;
    const owner = @import("../models/loaded_runtime.zig");
    var source_open = true;
    defer if (source_open) source.close();
    var manifest = try manifest_mod.loadFromDir(a, path);
    var manifest_open = true;
    defer if (manifest_open) manifest.deinit();
    const revision = try owner.resolutionModelRevision(a, std.testing.io, manifest);
    const assets = owner.Assets{ .manifest = manifest, .hf_tok = null, .sp_tok = null, .session = .{ .session = source }, .session_manager = sessions, .host = owner.Host.unmanaged(), .model_dir = path, .preparation_io = std.testing.io, .executor_io = std.testing.io, .resolution_revision = revision, .resolution_file_revision = revision, .native_kv_asset_generation = null, .tokenizer_type = .huggingface, .embedding_signature = null, .tokenizer_resource_lease = null, .kernel_jit_profile_bundle = null };
    source_open = false;
    manifest_open = false;
    return owner.Runtime.fromOwnedAssets(a, assets, null);
}

fn writeRetainedDecoderConfig(dir: std.Io.Dir) !void {
    try dir.writeFile(std.testing.io, .{ .sub_path = "config.json", .data = "{\"model_type\":\"llama\",\"hidden_size\":64,\"num_hidden_layers\":1,\"num_attention_heads\":1,\"intermediate_size\":128,\"vocab_size\":2,\"max_position_embeddings\":64,\"tie_word_embeddings\":true}" });
}

test "owned decoder runtime retains actual Native model across executor and LoadedModel destruction" {
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    try writeRetainedDecoderConfig(temporary.dir);
    const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temporary.sub_path[0..] });
    defer a.free(path);
    var sessions = @import("../backends/backends.zig").SessionManager.init(a);
    const model = try retainedDecoderFixtureModel(path, &sessions, try retainedDecoderFixtureSession(a));
    var model_open = true;
    defer if (model_open) {
        model.deinit();
        a.destroy(model);
    };
    const lifetime = model.session.model_lifetime.?;
    const before = lifetime.references.load(.acquire);
    var executor = (try @import("model_candidate.zig").createSessionExecutor(a, model.session, .{ .kv_dtype = .f32 })).?;
    var executor_open = true;
    defer if (executor_open) executor.deinit();
    var physical = try executor.createRuntime(a);
    var physical_open = true;
    defer if (physical_open) physical.deinit();
    executor.deinit();
    executor_open = false;
    const model_retained = lifetime.references.load(.acquire) > before;
    // Real aggregate destruction follows below. The old implementation has no
    // pin: hold one explicit bounded safety reference solely so the canary can
    // report the missing owner without dereferencing destroyed weight storage.
    const safety = lifetime.retain();
    var safety_open = true;
    defer {
        if (physical_open) {
            physical.deinit();
            physical_open = false;
        }
        if (safety_open) safety.release();
    }
    model.deinit();
    a.destroy(model);
    model_open = false;
    if (model_retained) {
        safety.release();
        safety_open = false;
    }
    var output = try physical.prefill(a, .{ .input_ids = &.{ 0, 1 }, .seq_len = 2, .query_seq_len = 2, .attention_mode = .full_recompute });
    var output_open = true;
    defer if (output_open) output.deinit(a);
    const logits = try output.takeHostLogits(a);
    defer a.free(logits);
    try std.testing.expectEqual(@as(usize, 2), logits.len);
    output.deinit(a);
    output_open = false;
    physical.deinit();
    physical_open = false;
    if (safety_open) {
        safety.release();
        safety_open = false;
    }
    try std.testing.expect(model_retained);
}

fn retainedDecoderMetalFixtureSession(a: std.mem.Allocator) !Session {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    var native_weights = NativeData{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer native_weights.deinitOwned();
    try @import("testing/decoder.zig").populate(&native_weights);
    var hosted = GpuHostedData{ .allocator = a, .prefix = "", .lazy_weights = .empty };
    errdefer @import("../backends/compute_factory.zig").closeMetalWeights(a, &hosted);
    var iterator = native_weights.resident_weights.iterator();
    while (iterator.next()) |entry| {
        const key = try a.dupe(u8, entry.key_ptr.*);
        errdefer a.free(key);
        const reference_name = try a.dupe(u8, entry.key_ptr.*);
        errdefer a.free(reference_name);
        var value = try Tensor.initFloat32(a, key, entry.value_ptr.tensor.shape, entry.value_ptr.tensor.asFloat32());
        errdefer value.deinit();
        try hosted.lazy_weights.put(a, key, .{ .tensor_ref = .{ .name = reference_name }, .host_loaded = .{ .tensor = value } });
    }
    try @import("../backends/compute_factory.zig").initializeMetalWeightPrefetch(a, &hosted, false);
    const owner = try a.create(ArchSession);
    owner.* = .{ .allocator = a, .arch_config = .{ .gpt = @import("testing/decoder.zig").config() }, .backend_type = .metal, .backend_data = .{ .metal = hosted } };
    owner.model_lifetime.prepareFinalResources() catch |err| {
        a.destroy(owner);
        return err;
    };
    return archSessionHandle(owner);
}

test "owned decoder ModelOutput retains actual Metal model after runtime and LoadedModel destruction" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    try writeRetainedDecoderConfig(temporary.dir);
    const path = try std.fs.path.join(a, &.{ ".zig-cache", "tmp", temporary.sub_path[0..] });
    defer a.free(path);
    var sessions = @import("../backends/backends.zig").SessionManager.init(a);
    const model = try retainedDecoderFixtureModel(path, &sessions, try retainedDecoderMetalFixtureSession(a));
    var model_open = true;
    defer if (model_open) {
        model.deinit();
        a.destroy(model);
    };
    const lifetime = model.session.model_lifetime.?;
    var source_compute = try getComputeBackendWithControl(model.session, a, .{ .native_call_policy = .in_process });
    var source_compute_open = true;
    defer if (source_compute_open) source_compute.deinit();
    const before = lifetime.references.load(.acquire);
    var executor = (try @import("model_candidate.zig").createSessionExecutor(a, model.session, .{ .kv_dtype = .f16 })).?;
    var executor_open = true;
    defer if (executor_open) executor.deinit();
    var physical = try executor.createRuntime(a);
    var physical_open = true;
    defer if (physical_open) physical.deinit();
    executor.deinit();
    executor_open = false;
    const safety = lifetime.retain();
    var safety_open = true;
    defer {
        if (physical_open) {
            physical.deinit();
            physical_open = false;
        }
        if (safety_open) safety.release();
    }
    // The private cached runtime deliberately stores no request token. This
    // fixture owns the actual Engine, so route its live capability explicitly
    // through the request rather than trying to reacquire its provider gate.
    var output = try physical.prefill(a, .{ .execution_control = .{ .native_call_policy = .in_process }, .backend_operation = source_compute.backend.operationToken(), .input_ids = &.{1}, .seq_len = 1, .query_seq_len = 1, .attention_mode = .paged_prefill });
    var output_open = true;
    defer if (output_open) output.deinit(a);
    // Only a published resident output can prove the output-owner contract.
    // A host fallback cannot substitute for retained device storage here.
    if (output.device_logits == null and output.prepared_tail == null) return error.MetalRetainedOutputUnexpectedHostFallback;
    if (output.owner == null) return error.MetalRetainedOutputMissingRuntimeOwner;
    const retained_model = lifetime.references.load(.acquire) > before + 1;
    physical.deinit();
    physical_open = false;
    source_compute.deinit();
    source_compute_open = false;
    model.deinit();
    a.destroy(model);
    model_open = false;
    if (retained_model) {
        safety.release();
        safety_open = false;
    }
    const owner_before_failed_materialization = output.owner.?.ptr;
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, output.hostLogits(failing.allocator()));
    if (output.owner == null) return error.MetalRetainedOutputLostOwnerOnFailedMaterialization;
    if (output.owner.?.ptr != owner_before_failed_materialization) return error.MetalRetainedOutputChangedOwnerOnFailedMaterialization;
    const logits = try output.takeHostLogits(a);
    defer a.free(logits);
    try std.testing.expectEqual(@as(usize, 2), logits.len);
    for (logits) |value| try std.testing.expect(std.math.isFinite(value));
    output.deinit(a);
    output_open = false;
    if (safety_open) {
        safety.release();
        safety_open = false;
    }
    if (!retained_model) return error.MetalRetainedOutputMissingModelPin;
}

const WhisperPendingObservationFixture = struct {
    const Probe = struct {
        observed: bool,
        active: bool = false,
        submitted: bool = false,
        fail_submit: bool = false,
        fail_wait: bool = false,
        fail_cancel: bool = false,
        destroyed: bool = false,
        submits: usize = 0,
        waits: usize = 0,
        reads: usize = 0,
        cancels: usize = 0,
        fn get(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn check(self: *@This()) !void {
            if (self.destroyed) return error.WhisperPendingOwnerRetired;
            if (@import("allocation_registry.zig").enabled() != self.observed) return error.WhisperPendingDisabledPathChanged;
        }
        fn kind(_: *anyopaque) ops.BackendKind {
            return .native;
        }
        fn submit(raw: *anyopaque) !void {
            const self = get(raw);
            try self.check();
            self.submits += 1;
            if (self.fail_submit) return error.WhisperSubmitFailure;
            self.submitted = true;
        }
        fn wait(raw: *anyopaque) !void {
            const self = get(raw);
            try self.check();
            self.waits += 1;
            if (self.fail_wait) return error.WhisperAwaitFailure;
            self.submitted = false;
        }
        fn activeFrame(raw: *anyopaque) bool {
            return get(raw).active;
        }
        fn cancel(raw: *anyopaque) !void {
            const self = get(raw);
            try self.check();
            self.cancels += 1;
            if (self.fail_cancel) return error.WhisperCancelFailure;
            self.active = false;
        }
        fn stats(raw: *anyopaque, slot: usize, values: *ops.WhisperLogitsStatsRaw) bool {
            const self = get(raw);
            std.debug.assert(!self.destroyed and slot == 0 and !self.submitted);
            self.reads += 1;
            values.* = @splat(0);
            return true;
        }
        fn destroy(raw: *anyopaque) void {
            get(raw).destroyed = true;
        }
    };
};

test "Whisper pending owner observes submit await cancel failures and disabled parity without claiming interval GPU completion" {
    const F = WhisperPendingObservationFixture;
    for (0..5) |mode| {
        const enabled = mode != 0;
        var probe = F.Probe{ .observed = enabled, .fail_submit = mode == 2, .fail_wait = mode == 3, .fail_cancel = mode == 4 };
        var lifetime = Session.ModelLifetime{ .owner = &probe, .destroy_fn = F.Probe.destroy };
        var original_model = true;
        defer if (original_model) lifetime.release();
        var diagnostics = @import("diagnostics.zig").Diagnostics{};
        diagnostics.configure(enabled);
        var table = @import("../ops/native_compute.zig").vtable_impl;
        table.backendKind = F.Probe.kind;
        table.registerRuntimeObservation = null;
        table.debugTimingSnapshot = null;
        table.regions.commands.submitFrame = F.Probe.submit;
        table.regions.commands.waitSubmittedFrame = F.Probe.wait;
        table.regions.commands.hasActiveFrame = F.Probe.activeFrame;
        table.regions.commands.cancelFrame = F.Probe.cancel;
        table.regions.token_selection.readLogitsStats = F.Probe.stats;
        const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = &table };
        var decoder = WhisperNativeDecoder{ .allocator = std.testing.allocator, .cb = .{ .backend = backend, .guard = undefined, .owns_backend = false }, .config = undefined, .encoder_hidden = undefined, .cache = undefined, .gate = null, .destination_model = &lifetime, .diagnostics_binding = .{ .diagnostics = &diagnostics } };
        // This phase fixture has no numerical cache/encoder allocation. Its
        // exact backend and publication owner remain live until every callback.
        defer decoder.pending_observation.deinit();
        if (mode == 2) {
            try std.testing.expectError(error.WhisperSubmitFailure, decoder.submit());
        } else try decoder.submit();
        var identity: u64 = 0;
        if (enabled) {
            const submitted = diagnostics.snapshot().?;
            identity = submitted.pending_interval.?.identity;
            try std.testing.expect(submitted.pending_interval.?.phase == .submit and submitted.pending_interval.?.completion == .unavailable);
            try std.testing.expectEqual(mode == 2, submitted.outcome == .failed);
            lifetime.release();
            original_model = false;
            try std.testing.expect(!probe.destroyed);
        } else try std.testing.expect(diagnostics.snapshot() == null);
        if (mode != 2) {
            if (mode == 3) {
                try std.testing.expectError(error.WhisperAwaitFailure, decoder.awaitStats(0));
                const failed = diagnostics.snapshot().?;
                try std.testing.expect(failed.outcome == .failed and failed.pending_interval.?.completion == .unavailable);
            } else {
                try std.testing.expect((try decoder.awaitStats(0)) != null);
                if (enabled) try std.testing.expect(diagnostics.snapshot().?.pending_interval.?.completion == .readback_returned);
            }
            if (mode != 3) {
                probe.active = true;
                decoder.discard();
                if (enabled) {
                    const cancelled = diagnostics.snapshot().?;
                    try std.testing.expect(cancelled.pending_interval.?.phase == .cancel and cancelled.pending_interval.?.completion == .unavailable);
                    try std.testing.expectEqual(mode == 4, cancelled.outcome == .failed);
                }
            }
        }
        probe.fail_submit = false;
        probe.fail_wait = false;
        probe.fail_cancel = false;
        if (mode <= 1) {
            diagnostics.configure(false);
            probe.observed = false;
            probe.active = true;
            decoder.discard();
        }
        decoder.drain();
        if (mode <= 1) {
            try std.testing.expect(diagnostics.snapshot() == null);
            try std.testing.expectEqual(@as(usize, 1), probe.submits);
            try std.testing.expectEqual(@as(usize, 2), probe.waits);
            try std.testing.expectEqual(@as(usize, 1), probe.reads);
            try std.testing.expectEqual(@as(usize, 2), probe.cancels);
        } else {
            const drained = diagnostics.snapshot().?;
            try std.testing.expectEqual(identity, drained.pending_interval.?.identity);
            try std.testing.expect(drained.pending_interval.?.phase == .drain and drained.pending_interval.?.completion == .unavailable);
            const failure: []const u8 = if (mode == 2) "WhisperSubmitFailure" else if (mode == 3) "WhisperAwaitFailure" else "WhisperCancelFailure";
            try std.testing.expectEqualStrings(failure, drained.pending_interval.?.prior_failure.?.view());
            try std.testing.expect(drained.pending_interval.?.whole_interval_counters == .unavailable);
        }
        decoder.pending_observation.deinit();
        try std.testing.expectEqual(enabled, probe.destroyed);
        try std.testing.expect(!@import("allocation_registry.zig").enabled());
    }
}

test "actual Native Whisper numerical decoder publishes rejected pending operations through its retained model owner" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const session = try IncrementalFixture.session(a);
    var session_open = true;
    defer if (session_open) session.close();
    try std.testing.expect(session.configureExecutionDiagnostics(true));
    const diagnostics = session.invocation_diagnostics.?.diagnostics;
    var features = [_]Tensor{try IncrementalFixture.features(a)};
    defer features[0].deinit();
    const encoded = try session.run(&features, a);
    defer {
        for (encoded) |*value| value.deinit();
        a.free(encoded);
    }
    var decoder = (try whisperNativeDecoder(session, a, null, encoded[0].asFloat32(), 2)).?;
    defer decoder.deinit();
    const logits = try decoder.step(&.{ 0, 1 });
    defer a.free(logits);
    try std.testing.expectEqual(@as(usize, @import("testing/incremental_encoder_decoder.zig").config.vocab_size), logits.len);
    try std.testing.expect(diagnostics.snapshot().?.outcome == .success);
    session.close();
    session_open = false;
    try std.testing.expectError(error.UnsupportedOperation, decoder.submit());
    const rejected = diagnostics.snapshot().?;
    try std.testing.expect(rejected.outcome == .failed and rejected.pending_interval.?.phase == .submit);
    try std.testing.expectEqualStrings("UnsupportedOperation", rejected.error_name.?.view());
    try std.testing.expect((try decoder.awaitStats(0)) == null);
    const unavailable = diagnostics.snapshot().?;
    try std.testing.expect(unavailable.outcome == .unsupported and unavailable.pending_interval.?.completion == .unavailable);
    try std.testing.expectEqual(@as(usize, 2), decoder.positions());
}

test "actual opt-in Metal Whisper pipelined numerical step preserves encoded submit await observation phases" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const session = try incrementalMetalFixtureSession(a);
    defer session.close();
    try std.testing.expect(session.configureExecutionDiagnostics(true));
    const diagnostics = session.invocation_diagnostics.?.diagnostics;
    const control = InferenceExecutionControl{ .native_call_policy = .in_process };
    var features = [_]Tensor{try IncrementalFixture.features(a)};
    defer features[0].deinit();
    const encoded = try session.runWithControl(&features, a, control);
    defer {
        for (encoded) |*value| value.deinit();
        a.free(encoded);
    }
    var decoder = (try whisperNativeDecoder(session, a, control, encoded[0].asFloat32(), 2)).?;
    defer decoder.deinit();
    const prefix = try decoder.step(&.{ 0, 1 });
    defer a.free(prefix);
    // The numerical profile is explicitly opt-in. Absence of support is not
    // replaced by an eager replay or a fabricated completion observation.
    if (!decoder.setPipelined(true)) return error.SkipZigTest;
    defer {
        decoder.drain();
        _ = decoder.setPipelined(false);
    }
    const params = ops.WhisperLogitsParams{ .out_dim = 4, .suppress_count = 0, .ts_begin = 4, .text_allowed = 1, .ts_min = 4, .ts_max = 4, .eot = 3, .probe_id = 4, .mode = ops.whisper_logits_mode_choose };
    const result = decoder.stepWith(&.{2}, .{ .pipelined = .{ .params = params, .suppress = &.{} } }) catch |err| switch (err) {
        error.UnsupportedOperation => return error.SkipZigTest,
        else => return err,
    };
    try std.testing.expect(result == .encoded);
    const encoded_phase = diagnostics.snapshot().?;
    try std.testing.expect(encoded_phase.pending_interval.?.phase == .encode_next and encoded_phase.pending_interval.?.completion == .unavailable);
    try decoder.submit();
    const submitted = diagnostics.snapshot().?;
    try std.testing.expectEqual(encoded_phase.pending_interval.?.identity, submitted.pending_interval.?.identity);
    try std.testing.expect(submitted.pending_interval.?.phase == .submit and submitted.pending_interval.?.completion == .unavailable);
    try std.testing.expect((try decoder.awaitStats(0)) != null);
    const ready = diagnostics.snapshot().?;
    try std.testing.expect(ready.pending_interval.?.phase == .await_readback and ready.pending_interval.?.completion == .readback_returned);
    try std.testing.expect(ready.pending_interval.?.whole_interval_counters == .unavailable);
    try std.testing.expectEqual(@as(usize, 3), decoder.positions());
}

test "CUDA incremental receiver qualification requires exact live primary context and device before submission" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    var source_compute: cuda_compute_mod.CudaCompute = undefined;
    var target_compute: cuda_compute_mod.CudaCompute = undefined;
    source_compute.ctx.ctx = @ptrFromInt(8);
    source_compute.ctx.device = 0;
    source_compute.ctx.info.selected_device = 0;
    target_compute.ctx.ctx = @ptrFromInt(8);
    target_compute.ctx.device = 0;
    target_compute.ctx.info.selected_device = 0;
    var source_owner = ArchSession{ .allocator = std.testing.allocator, .arch_config = .{ .whisper = .{ .d_model = 4 } }, .backend_type = .cuda, .backend_data = .{ .cuda = .{ .compute = &source_compute } } };
    var target_owner = ArchSession{ .allocator = std.testing.allocator, .arch_config = .{ .whisper = .{ .d_model = 4 } }, .backend_type = .cuda, .backend_data = .{ .cuda = .{ .compute = &target_compute } } };
    const source = Session{ .ptr = &source_owner, .vtable = &arch_vtable };
    const target = Session{ .ptr = &target_owner, .vtable = &arch_vtable };
    // Everything except the immutable metadata above remains undefined: any
    // driver, allocator, budget, or queue access would make this test invalid.
    try std.testing.expect(IncrementalDecoderAdapter.qualify(source, target) != null);
    target_compute.ctx.ctx = @ptrFromInt(16);
    try std.testing.expect(IncrementalDecoderAdapter.qualify(source, target) == null);
    target_compute.ctx.ctx = source_compute.ctx.ctx;
    target_compute.ctx.device = 1;
    try std.testing.expect(IncrementalDecoderAdapter.qualify(source, target) == null);
    target_compute.ctx.device = 0;
    source_compute.ctx.ctx = null;
    target_compute.ctx.ctx = null;
    try std.testing.expect(IncrementalDecoderAdapter.qualify(source, target) == null);
}

/// Lives in the actual incremental decoder owner, never in a borrowed request
/// pointer. The guard starts before source preparation/destination acquisition;
/// finish runs only after destination gate, provider and model CloseScopes end.
fn bindOwnedDecoderCleanup(backend: *const ops.ComputeBackend, boundary: *const @import("retirement_sink.zig").Epoch) !void {
    if (try backend.bindCleanupEpoch(boundary)) return;
    const services = backend.vtable.operation_services orelse return error.IncrementalCleanupAuthorityUnavailable;
    if (!services.accepts(backend.vtable)) return error.IncrementalCleanupAuthorityUnavailable;
    switch (backend.kind()) {
        .native, .cuda => {}, // Metadata fence only; no numerical capability is created.
        else => return error.IncrementalCleanupAuthorityUnavailable,
    }
}

const ReceiverCleanup = struct {
    sink: @import("retirement_sink.zig").Ref,
    guard: @import("retirement_sink.zig").Guard,
    epoch: @import("retirement_sink.zig").Epoch,

    fn permitsAsyncSource(destination: Session, capability: ?@import("payload_charge.zig").Capability) bool {
        const source = capability orelse return false;
        if (!source.final_cleanup_qualified or !source.background_supervised or source.root_cleanup_deadline_ns == null) return false;
        const model = destination.model_lifetime orelse return false;
        return destination.execution_gate == &model.execution_gate;
    }
    // Compatibility qualification query for the separately certified foreign
    // async CUDA route. This does not decide whether an owned gate needs B.
    fn begin(destination: Session, capability: ?@import("payload_charge.zig").Capability) !?ReceiverCleanup {
        if (!permitsAsyncSource(destination, capability)) return null;
        return beginForOwnedGate(destination) catch |err| switch (err) {
            error.RetirementEpochCapacityExceeded, error.ResourceTemporarilyUnavailable => null,
            else => return err,
        };
    }
    fn beginForOwnedGate(destination: Session) !ReceiverCleanup {
        const stable = if (destination.model_lifetime != null)
            (try destination.retainRetirementSink()) orelse return error.IncrementalCleanupAuthorityUnavailable
        else
            try @import("retirement_sink.zig").Ref.create();
        return beginWithOwnedSink(stable);
    }
    /// Explicit raw owner reserves its pure cleanup lifetime before any lane,
    /// SDK construction, or source-root transfer. No fake Session is required.
    fn beginStandalone() !ReceiverCleanup {
        return beginWithOwnedSink(try @import("retirement_sink.zig").Ref.create());
    }
    fn beginWithOwnedSink(owned: @import("retirement_sink.zig").Ref) !ReceiverCleanup {
        var stable = owned;
        errdefer stable.release();
        var guard = stable.enter() catch |err| switch (err) {
            error.RetirementEpochCapacityExceeded => return error.ResourceTemporarilyUnavailable,
            else => return err,
        };
        errdefer guard.finish();
        const epoch = try guard.retainEpoch();
        return .{ .sink = stable, .guard = guard, .epoch = epoch };
    }
    fn boundary(self: *const ReceiverCleanup) *const @import("retirement_sink.zig").Epoch {
        return &self.epoch;
    }
    fn finish(self: *ReceiverCleanup) void {
        var guard = self.guard;
        var epoch = self.epoch;
        var stable = self.sink;
        self.* = undefined;
        guard.finish();
        epoch.release();
        stable.release();
    }
};

test "owned incremental input keeps both real encoder roots across cancelled and repeated decoder attempts" {
    const a = std.testing.allocator;
    const Root = @import("owned_incremental_input.zig");
    const stages = @import("encoder_decoder_runtime.zig");
    const decoder_stage = @import("decoder_stage.zig");
    const encoder = try IncrementalFixture.session(a);
    defer encoder.close();
    const destination = try IncrementalFixture.session(a);
    defer destination.close();
    var features = [_]Tensor{try IncrementalFixture.features(a)};
    defer features[0].deinit();
    var host = try stages.encodeWithPermit(a, encoder, destination, &features, null, null, null);
    defer host.deinit(a);
    var reference = try decoder_stage.Decoder.init(a, destination, &host, 2, &.{ 1, 1 }, 4, null);
    var reference_open = true;
    defer if (reference_open) reference.deinit();
    var expected = try reference.step(&.{ 0, 1 }, null, null);
    defer expected.deinit();
    reference.deinit();
    reference_open = false;
    var cleanup = try Root.Cleanup.beginTaskWithoutEnclosingLanes(null);
    defer cleanup.finish();
    var root_epoch = (try cleanup.borrowedEpoch()).?;
    defer root_epoch.release();
    var encoded = try stages.encodeForIncrementalWithPermit(a, encoder, destination, &features, null, null, null);
    var encoded_open = true;
    defer if (encoded_open) encoded.deinit(a);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Root.OwnedIncrementalInput.take(failing.allocator(), &encoded, destination, &cleanup));
    try std.testing.expect(encoded == .resident);
    try std.testing.expectEqual(@as(usize, 1), encoded.resident.values.outputs.len);
    var owned = try Root.OwnedIncrementalInput.take(a, &encoded, destination, &cleanup);
    encoded_open = false;
    var owned_open = true;
    defer if (owned_open) owned.deinit();
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.InferenceCancelled;
        }
    };
    try std.testing.expectError(error.InferenceCancelled, decoder_stage.Decoder.initWithSource(a, destination, &owned.encoder, 2, &.{ 1, 1 }, 4, .{ .check_fn = Cancel.check }, .{}, &owned.source.?));
    try std.testing.expect(!(try root_epoch.isSealed()));
    for (0..3) |_| {
        var actual = try decoder_stage.Decoder.initWithSource(a, destination, &owned.encoder, 2, &.{ 1, 1 }, 4, null, .{}, &owned.source.?);
        defer actual.deinit();
        var row = try actual.step(&.{ 0, 1 }, null, null);
        defer row.deinit();
        try std.testing.expectEqualSlices(f32, expected.logits, row.logits);
        try std.testing.expect(!(try root_epoch.isSealed()));
    }
    owned.deinit();
    owned_open = false;
    try std.testing.expect(try root_epoch.isSealed());
}

test "optional receiver cleanup capacity declines before work and releases its retained model sink" {
    const Sink = @import("retirement_sink.zig");
    const Probe = struct {
        destroyed: bool = false,
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.destroyed = true;
        }
        fn reserve(_: *anyopaque, _: usize, _: ?u64) !bool {
            return error.UnexpectedPayloadReservation;
        }
        fn take(_: *anyopaque) @import("payload_charge.zig").Owned {
            unreachable;
        }
        fn noop(_: *anyopaque) void {}
    };
    var probe = Probe{};
    var model = Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.destroy };
    defer model.release();
    const destination = Session{ .ptr = &probe, .vtable = undefined, .model_lifetime = &model, .execution_gate = &model.execution_gate };
    var sink = try model.retainRetirementSink();
    defer sink.release();
    var guards: [64]Sink.Guard = undefined;
    var initialized: usize = 0;
    defer while (initialized != 0) {
        initialized -= 1;
        guards[initialized].finish();
    };
    for (&guards) |*guard| {
        guard.* = try sink.enter();
        initialized += 1;
    }
    const source = @import("payload_charge.zig").Capability{ .context = &probe, .prepare_fn = Probe.reserve, .commit_fn = Probe.noop, .abort_fn = Probe.noop, .take_fn = Probe.take, .final_cleanup_qualified = true, .background_supervised = true, .root_cleanup_deadline_ns = @import("antfly_platform").time.monotonicNs() + 30 * std.time.ns_per_s };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, ReceiverCleanup.beginForOwnedGate(destination));
    try std.testing.expect((try ReceiverCleanup.begin(destination, source)) == null);
    initialized -= 1;
    guards[initialized].finish();
    var admitted = (try ReceiverCleanup.begin(destination, source)).?;
    admitted.finish();
    try std.testing.expect(!probe.destroyed);
}

test "decoder owned current B retires real foreign Metal source before two provider and model reentry" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var encoder = try incrementalMetalFixtureSession(a);
    defer encoder.close();
    var destination = try incrementalMetalFixtureSession(a);
    defer destination.close();
    encoder.execution_gate = &encoder.model_lifetime.?.execution_gate;
    destination.execution_gate = &destination.model_lifetime.?.execution_gate;
    var features = [_]Tensor{try IncrementalFixture.features(a)};
    defer features[0].deinit();
    const source_gate = encoder.execution_gate.?;
    try std.testing.expect(source_gate.tryLock());
    var source_locked = true;
    defer if (source_locked) source_gate.unlock();
    var outputs = (try encoder.runResident(&features, a)) orelse return error.MissingResidentEncoder;
    var outputs_open = true;
    defer if (outputs_open) outputs.deinit();
    const receiver = IncrementalDecoderAdapter.qualify(encoder, destination) orelse return error.MissingIncrementalReceiver;
    const retained_source = try IncrementalFixture.source(&outputs, a, receiver);
    source_gate.unlock();
    source_locked = false;
    var decoder = try IncrementalDecoderAdapter.fromResident(destination, a, .{ .native_call_policy = .in_process }, retained_source);
    var decoder_open = true;
    defer if (decoder_open) decoder.deinit();
    outputs.deinit();
    outputs_open = false;
    var expected = try decoder.receiver_cleanup.?.epoch.retain();
    defer expected.release();
    const Pin = Session.ModelLifetime.ResourcePin;
    const Probe = struct {
        source: Session,
        destination: Session,
        original: ?Pin,
        expected: *const @import("retirement_sink.zig").Epoch,
        node: @import("resource_retirement.zig").Node = undefined,
        queued: usize = 0,
        calls: usize = 0,
        failure: ?anyerror = null,
        fn rawRelease(_: *anyopaque) void {
            @panic("decoder selected unqualified raw source release under its owned gate");
        }
        fn releaseWithBoundary(raw: *anyopaque, boundary: ?*const @import("retirement_sink.zig").Epoch) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const current = boundary orelse @panic("decoder omitted current B");
            std.debug.assert(current.record == self.expected.record and current.nonce == self.expected.nonce);
            self.node = .{ .context = self, .release_fn = retire };
            const deferred = current.boundary() catch @panic("invalid decoder B");
            deferred.register(deferred.context, deferred.nonce, &self.node) catch @panic("decoder B registration failed");
            self.queued += 1;
            const original = self.original.?;
            self.original = null;
            // Complete actual source SDK/backend destruction while destination
            // still owns its lane. The producer callback must remain queued.
            original.releaseWithBoundary(current);
            std.debug.assert(self.calls == 0);
        }
        fn retire(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.check() catch |err| {
                self.failure = err;
            };
        }
        fn check(self: *@This()) !void {
            const source_model_gate = self.source.execution_gate.?;
            const destination_model_gate = self.destination.execution_gate.?;
            if (!source_model_gate.tryLock()) return error.SourceModelGateStillHeld;
            defer source_model_gate.unlock();
            if (!destination_model_gate.tryLock()) return error.DestinationModelGateStillHeld;
            defer destination_model_gate.unlock();
            // These concrete model constructors acquire the actual distinct
            // shared-provider leases and operation gates; QueueFull is a
            // meaningful failure rather than a synthetic mutex substitute.
            var source_compute = try getComputeBackendWithControl(self.source, std.testing.allocator, .{ .native_call_policy = .in_process });
            defer source_compute.deinit();
            var destination_compute = try getComputeBackendWithControl(self.destination, std.testing.allocator, .{ .native_call_policy = .in_process });
            defer destination_compute.deinit();
            for ([_]*const ops.ComputeBackend{ &source_compute.backend, &destination_compute.backend }) |backend| {
                const token = backend.operationToken() orelse return error.MissingActualProviderOperation;
                try token.validate(.{ .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .program = null, .mode = .managed });
                const input = try backend.fromFloat32(&.{ 2, 5 });
                defer backend.free(input);
                const result = try backend.add(input, input);
                defer backend.free(result);
                const values = try backend.toFloat32(result, std.testing.allocator);
                defer std.testing.allocator.free(values);
                try std.testing.expectEqualSlices(f32, &.{ 4, 10 }, values);
            }
        }
    };
    var probe = Probe{ .source = encoder, .destination = destination, .original = decoder.source_resources, .expected = &expected };
    decoder.source_resources = .{ .ptr = &probe, .release_fn = Probe.rawRelease, .release_with_boundary_fn = Probe.releaseWithBoundary };
    decoder.deinit();
    decoder_open = false;
    try std.testing.expectEqual(@as(usize, 1), probe.queued);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expect(probe.original == null);
    try std.testing.expect(try expected.isSealed());
    if (probe.failure) |err| return err;
}

test "PII specialty Metal admission reserves required DeBERTa F32 mirrors" {
    const mf = manifest_mod.ModelManifest{
        .allocator = std.testing.allocator,
        .gliner_model_type = "gliner2",
        .gliner_span_encoder_family = .deberta,
        .gliner_specialty_profile = .pii,
        .hidden_size = 768,
        .num_hidden_layers = 12,
        .num_attention_heads = 12,
        .intermediate_size = 3072,
        .bert_vocab_size = 250112,
        .max_position_embeddings = 512,
    };
    const actual = metalDebertaFastPathAdmissionAmounts(mf);
    const expected = deberta_arch.metalFastPathReservationBytes(.{
        .hidden_size = mf.hidden_size,
        .num_hidden_layers = mf.num_hidden_layers,
        .num_attention_heads = mf.num_attention_heads,
        .intermediate_size = mf.intermediate_size,
        .vocab_size = mf.bert_vocab_size,
        .max_position_embeddings = mf.max_position_embeddings,
        .num_labels = mf.num_labels,
        .metal_require_f32_weight_mirrors = true,
    }, true);
    try std.testing.expectEqual(expected.persistent_bytes, actual.backend_weight_bytes);
    try std.testing.expectEqual(expected.scratch_bytes, actual.backend_scratch_bytes);
}

pub fn getGlinerSpecialtyIdentity(session: Session) !gliner_specialty.Identity {
    const identity = try getGlinerDecisionIdentity(session);
    const profile = try gliner_specialty.require(identity);
    try gliner_specialty.requireBackend(profile, switch (session.backend()) {
        .native => .native,
        .metal => .metal,
        .cuda => .cuda,
        else => .other,
    });
    return identity;
}

test "GLiNER specialty sealed identity accepts native Metal and CUDA sessions" {
    inline for (.{ gliner_specialty.Profile.pii, gliner_specialty.Profile.gliguard }) |profile| {
        var arch = ArchSession{
            .allocator = std.testing.allocator,
            .arch_config = .{ .deberta = .{} },
            .task = .extractor,
            .gliner_decision_identity = gliner_specialty.pinnedIdentity(profile),
            .gliner_decision_identity_sealed = true,
            .backend_type = .native,
            // Identity queries inspect metadata only, without creating a device.
            .backend_data = .{ .native = .{ .allocator = std.testing.allocator, .resident_weights = .empty, .lazy_weights = .empty } },
        };
        const session = Session{ .ptr = &arch, .vtable = &arch_vtable };
        inline for (.{ BackendType.native, BackendType.metal, BackendType.cuda }) |backend| {
            arch.backend_type = backend;
            try std.testing.expectEqual(profile, try gliner_specialty.require(try getGlinerSpecialtyIdentity(session)));
        }
        arch.gliner_decision_identity_sealed = false;
        try std.testing.expectError(error.MissingGlinerDecisionIdentity, getGlinerSpecialtyIdentity(session));
    }
}

const DetachedDenseBackingProbe = struct {
    identity: ?@import("storage_ownership.zig").Identity = null,
    count: usize = 0,
    fn visit(raw: *anyopaque, report: @import("storage_ownership.zig").Report) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (report == .backing and report.backing.domain == .backend) {
            self.count += 1;
            self.identity = report.backing.identity;
        }
    }
    fn read(backend: *const ops.ComputeBackend, value: ops.CT) !@import("storage_ownership.zig").Identity {
        const kernels = backend.vtable.kernels orelse return error.MissingStorageOwnership;
        const query = kernels.storageOwnership orelse return error.MissingStorageOwnership;
        var probe = @This(){};
        try std.testing.expect(try query(backend.ptr, value, .{ .context = &probe, .visit = visit }) == .complete);
        try std.testing.expectEqual(@as(usize, 1), probe.count);
        return probe.identity orelse error.MissingStorageIdentity;
    }
};

test "dense stage promotion preserves actual Metal buffer through original Engine and arena close with two provider quota reentry" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const Sink = @import("retirement_sink.zig");
    const operation = @import("backend_operation.zig");
    const transfer = @import("transfer_audit.zig");
    const retirement = @import("resource_retirement.zig");
    var source = try incrementalMetalFixtureSession(a);
    var source_open = true;
    defer if (source_open) source.close();
    var destination = try incrementalMetalFixtureSession(a);
    defer destination.close();
    try source.installOwnedExecutionGate(a);
    try destination.installOwnedExecutionGate(a);
    const source_model = source.model_lifetime.?;
    const source_pin = source_model.retain();
    defer source_pin.release();
    const Probe = struct {
        sessions: [2]Session,
        calls: usize = 0,
        failure: ?anyerror = null,
        fn reserve(_: *anyopaque, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: runtime.tier.memory.AdmissionAmounts) runtime.tier.memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.reenter() catch |err| {
                self.failure = err;
            };
        }
        fn reenter(self: *@This()) !void {
            for (self.sessions) |session| {
                const model = session.model_lifetime.?;
                if (!model.execution_gate.tryLock()) return error.ModelGateStillOwned;
                model.execution_gate.unlock();
                var compute = try getComputeBackendWithControl(session, std.testing.allocator, .{ .native_call_policy = .in_process });
                defer compute.deinit();
                const input = try compute.backend.fromFloat32(&.{ 2, 5 });
                defer compute.backend.free(input);
                const result = try compute.backend.add(input, input);
                defer compute.backend.free(result);
                const values = try compute.backend.toFloat32(result, std.testing.allocator);
                defer std.testing.allocator.free(values);
                try std.testing.expectEqualSlices(f32, &.{ 4, 10 }, values);
            }
        }
    };
    const domain = try Domain.create();
    defer domain.release();
    try std.testing.expect(source_model.admission_domain == null);
    source_model.admission_domain = domain.pin();
    var probe = Probe{ .sessions = .{ source, destination } };
    try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var lease: ?runtime.tier.memory.AdmissionLease = try domain.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 16, .host_scratch_bytes = 16 }, false);
    defer if (lease) |*value| value.release();
    var arena = std.heap.ArenaAllocator.init(a);
    var arena_open = true;
    defer if (arena_open) arena.deinit();
    const old_header = try arena.allocator().create(ops.ComputeBackend);
    const impl: *ArchSession = @ptrCast(@alignCast(source.ptr));
    old_header.* = try makeComputeBackend(impl, arena.allocator(), null);
    old_header.execution_control = .{ .native_call_policy = .in_process };
    var backend_owned = true;
    defer if (backend_owned) old_header.deinit();
    const compute: *metal_compute_mod.MetalCompute = @ptrCast(@alignCast(old_header.ptr));
    const value = try compute.importResidentF32(&.{ 1, 2, 3, 4 }, &.{4});
    var value_owned = true;
    defer if (value_owned) old_header.free(value);
    const vector = try arena.allocator().alloc(ops.CT, 1);
    vector[0] = value;
    var outputs = ResidentOutputs{ .outputs = vector, .backend = old_header, .allocator = arena.allocator(), .backend_owner = old_header, .deinit_backend_owner = deinitResidentComputeBackend, .backend_owner_cleanup_services = &resident_compute_cleanup_services, .model_retention = source_model.retain(), .resource_lease = lease };
    backend_owned = false;
    value_owned = false;
    lease = null;
    var outputs_open = true;
    defer if (outputs_open) outputs.deinit();
    try outputs.prepareRetirement();
    const original_shell = outputs.retirement_shell.?.owner;
    const original_identity = try DetachedDenseBackingProbe.read(outputs.backend, outputs.outputs[0]);
    var owner = try Sink.Ref.create();
    defer owner.release();
    var guard = try owner.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    @import("antfly_platform").sync.lockYielding(source.execution_gate.?);
    var source_locked = true;
    defer if (source_locked) source.execution_gate.?.unlock();
    var audit = transfer.Audit{ .forbid_device_host_access = true };
    try transfer.begin(&audit);
    var auditing = true;
    defer if (auditing) transfer.end();
    try std.testing.expect(try old_header.bindCleanupEpoch(&epoch));
    const source_operation = old_header.operationToken() orelse return error.BackendOperationUnavailable;
    try std.testing.expectError(error.BackendCleanupAuthorityRequired, outputs.detachDenseF32PhysicalPayload(.{ .boundary = &epoch }));
    old_header.execution_control = null;
    try std.testing.expectError(error.ExecutionControlRequired, outputs.detachDenseF32PhysicalPayload(.{ .boundary = &epoch, .operation = source_operation }));
    old_header.execution_control = .{ .native_call_policy = .in_process };
    try std.testing.expect(try outputs.detachDenseF32PhysicalPayload(.{ .boundary = &epoch, .operation = source_operation }));
    transfer.end();
    auditing = false;
    try std.testing.expect(outputs.backend != old_header);
    try std.testing.expect(outputs.retirement_shell.?.owner == original_shell and outputs.resource_lease != null);
    const moved_identity = try DetachedDenseBackingProbe.read(outputs.backend, outputs.outputs[0]);
    try std.testing.expectEqual(original_identity.base, moved_identity.base);
    try std.testing.expect(original_identity.context == moved_identity.context and moved_identity.kind == .metal);
    try std.testing.expectEqual(@as(u64, 0), audit.device_copy_calls);
    try std.testing.expectEqual(@as(u64, 0), audit.download_calls);
    try std.testing.expectEqual(@as(u64, 0), audit.upload_calls);
    try std.testing.expectEqual(@as(u64, 0), audit.host_accesses);
    source.execution_gate.?.unlock();
    source_locked = false;
    guard.finish();
    source.close();
    source_open = false;
    arena.deinit();
    arena_open = false;
    try probe.reenter(); // Original Engine/provider exclusion has actually ended.
    {
        var scope = try operation.enter(outputs.backend.*, null, null, .managed);
        defer scope.deinitChecked() catch @panic("dense promoted read cleanup failed");
        // The exact accepted producer Engine and its arena are gone. An
        // immediate consumer may still decline the actual one-dimensional
        // output shape; export the SAME output without consulting old Session
        // headers, metadata slices or numerical callbacks.
        try std.testing.expectError(error.UnexpectedOutputShape, @import("sequence_pool.zig").poolOutput(a, outputs.backend, outputs.outputs[0], &.{1}, 1, 1, .mean));
        const host = try outputs.materializeFloat32(a, &.{}, scope.token());
        defer {
            for (host) |*tensor| tensor.deinit();
            a.free(host);
        }
        try std.testing.expectEqual(@as(usize, 1), host.len);
        try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, host[0].asFloat32());
        try std.testing.expect(domain.controller.snapshot().host_scratch_bytes >= host[0].data.len);
        try scope.finish();
    }
    var final_guard = try owner.enter();
    defer final_guard.finish();
    var final_epoch = try final_guard.retainEpoch();
    defer final_epoch.release();
    @import("antfly_platform").sync.lockYielding(source.execution_gate.?);
    source_locked = true;
    outputs.deinitWithBoundary(&final_epoch);
    outputs_open = false;
    try std.testing.expectEqual(@as(usize, 16), domain.controller.snapshot().backendTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    source.execution_gate.?.unlock();
    source_locked = false;
    final_guard.finish();
    _ = retirement.pollRawController(&domain.controller);
    try std.testing.expectEqual(@as(usize, 0), domain.controller.snapshot().backendTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), domain.controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    if (probe.failure) |err| return err;
}

test "foreign Metal public Whisper encoder final norm promotes before receiver admission without transport" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    const encoder = try incrementalMetalFixtureSession(a);
    defer encoder.close();
    const destination = try incrementalMetalFixtureSession(a);
    defer destination.close();
    const control = InferenceExecutionControl{ .native_call_policy = .in_process };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const request_allocator = arena.allocator();
    var features = [_]Tensor{try IncrementalFixture.features(a)};
    defer features[0].deinit();
    const Root = @import("owned_incremental_input.zig");
    const stages = @import("encoder_decoder_runtime.zig");
    var cleanup = try Root.Cleanup.beginTaskWithoutEnclosingLanes(control);
    defer cleanup.finish();
    var permit = try encoder.admitInputs(&features, request_allocator, control);
    defer permit.deinit();
    try stages.prepareEncoderPermit(encoder, destination, null, control, true, &permit);
    var encoded = try stages.encodeForIncrementalWithPermit(request_allocator, encoder, destination, &features, null, control, &permit);
    var encoded_open = true;
    defer if (encoded_open) encoded.deinit(request_allocator);
    permit.deinit();
    const before_backend = encoded.resident.values.backend;
    const before_identity = try DetachedDenseBackingProbe.read(before_backend, encoded.resident.values.outputs[0]);
    var audit = @import("transfer_audit.zig").Audit{ .forbid_device_host_access = true };
    try @import("transfer_audit.zig").begin(&audit);
    var auditing = true;
    defer if (auditing) @import("transfer_audit.zig").end();
    var owned = try Root.OwnedIncrementalInput.take(request_allocator, &encoded, destination, &cleanup);
    encoded_open = false;
    defer owned.deinit();
    @import("transfer_audit.zig").end();
    auditing = false;
    try std.testing.expect(owned.encoder.resident.values.backend != before_backend);
    const after_identity = try DetachedDenseBackingProbe.read(owned.source.?.backend, owned.source.?.value);
    try std.testing.expectEqual(before_identity.base, after_identity.base);
    try std.testing.expect(before_identity.context == after_identity.context and after_identity.kind == .metal);
    try std.testing.expectEqual(@as(u64, 0), audit.device_copy_calls);
    try std.testing.expectEqual(@as(u64, 0), audit.upload_calls);
    try std.testing.expectEqual(@as(u64, 0), audit.download_calls);
    try std.testing.expectEqual(@as(u64, 0), audit.host_accesses);
    const owner: *ArchSession = @ptrCast(@alignCast(encoder.ptr));
    const store = gpuBackendData(owner);
    // The ACTUAL source shared-provider lease is no longer held by outputs.
    try std.testing.expect(store.shared_metal_native_provider_lock.tryLock());
    store.shared_metal_native_provider_lock.unlock(std.testing.io);
    var decoder = try @import("decoder_stage.zig").Decoder.initWithSource(request_allocator, destination, &owned.encoder, 2, &.{ 1, 1 }, @import("testing/incremental_encoder_decoder.zig").config.vocab_size, control, .{}, &owned.source.?);
    defer decoder.deinit();
    var first = try decoder.step(&.{ 0, 1 }, null, null);
    defer first.deinit();
    try std.testing.expect(first.logits.len != 0);
}

fn mediaTextEntryFixtureSession(a: std.mem.Allocator, clap: bool) !Session {
    const fixture = @import("../ops/media_residency_test.zig");
    var weights = NativeData{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    errdefer weights.deinitOwned();
    if (clap) {
        try fixture.putWeight(&weights, "text_model.embeddings.word_embeddings.weight", &.{ 8, 4 }, null);
        try fixture.putWeight(&weights, "text_model.embeddings.position_embeddings.weight", &.{ 8, 4 }, null);
        try fixture.putWeight(&weights, "text_model.embeddings.token_type_embeddings.weight", &.{ 1, 4 }, null);
        try fixture.putWeight(&weights, "text_model.embeddings.LayerNorm.weight", &.{4}, 1);
        try fixture.putWeight(&weights, "text_model.embeddings.LayerNorm.bias", &.{4}, 0);
        try fixture.putWeight(&weights, "text_model.pooler.dense.weight", &.{ 4, 4 }, null);
        try fixture.putWeight(&weights, "text_model.pooler.dense.bias", &.{4}, null);
        try fixture.putWeight(&weights, "text_projection.linear1.weight", &.{ 3, 4 }, null);
        try fixture.putWeight(&weights, "text_projection.linear1.bias", &.{3}, null);
        try fixture.putWeight(&weights, "text_projection.linear2.weight", &.{ 3, 3 }, null);
        try fixture.putWeight(&weights, "text_projection.linear2.bias", &.{3}, null);
    } else {
        try fixture.putWeight(&weights, "text_model.embeddings.token_embedding.weight", &.{ 8, 4 }, null);
        try fixture.putWeight(&weights, "text_model.embeddings.position_embedding.weight", &.{ 8, 4 }, null);
        try fixture.putWeight(&weights, "text_model.final_layer_norm.weight", &.{4}, 1);
        try fixture.putWeight(&weights, "text_model.final_layer_norm.bias", &.{4}, 0);
        try fixture.putWeight(&weights, "text_projection.weight", &.{ 3, 4 }, null);
    }
    const owner = try a.create(ArchSession);
    errdefer a.destroy(owner);
    owner.* = .{ .allocator = a, .arch_config = if (clap) .{ .clap = .{ .text_config = .{ .model_type = .roberta, .hidden_size = 4, .num_hidden_layers = 0, .num_attention_heads = 2, .intermediate_size = 8, .max_position_embeddings = 8, .type_vocab_size = 1 }, .projection_dim = 3 } } else .{ .clip = .{ .text_hidden_size = 4, .text_num_layers = 0, .text_num_heads = 2, .text_intermediate_size = 8, .text_max_position_embeddings = 8, .projection_dim = 3 } }, .backend_type = .native, .backend_data = .{ .native = weights } };
    // This constructor retains the one actual architecture/model owner.
    try owner.model_lifetime.prepareFinalResources();
    weights.resident_weights = .empty;
    weights.lazy_weights = .empty;
    return archSessionHandle(owner);
}

test "actual Native architecture CLIP and CLAP resident entries preserve current B quota lifetime cancellation and graph decline" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const cleanup_mod = @import("cleanup_owner.zig");
    for ([_]bool{ false, true }) |clap| {
        var session = try mediaTextEntryFixtureSession(a, clap);
        var session_open = true;
        defer if (session_open) session.close();
        try session.installOwnedExecutionGate(a);
        const impl: *ArchSession = @ptrCast(@alignCast(session.ptr));
        var controller = runtime.tier.memory.AdmissionController{};
        defer controller.deinit();
        session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1024 * 1024 }, .static_workspace_bytes = 4096, .check_live_memory = false };
        var ids = try Tensor.initInt64(a, "input_ids", &.{ 1, 4 }, &.{ 0, 7, 7, 1 });
        defer ids.deinit();
        var mask = try Tensor.initInt64(a, "attention_mask", &.{ 1, 4 }, &.{ 1, 1, 1, 0 });
        defer mask.deinit();
        const inputs = [_]Tensor{ ids, mask };
        const control = InferenceExecutionControl{ .native_call_policy = .in_process };
        for (0..2) |_| {
            {
                var cleanup = try cleanup_mod.Owner.beginResident(session);
                defer cleanup.finishAfterUnlock();
                var permit = try session.admitInputsBeforeExecution(&inputs, a, control);
                defer permit.deinitWithCleanup(.{ .boundary = cleanup.epoch() });
                try permit.prepareResidentOutputRetirement();
                try permit.bindCurrentCleanupEpoch(cleanup.epoch().?);
                @import("antfly_platform").sync.lockYielding(session.execution_gate.?);
                defer session.execution_gate.?.unlock();
                var outputs = (try permit.runResidentWithControl(&inputs, a, control)) orelse return error.MissingActualMediaResidentEntry;
                try permit.attachResidentOutputs(&outputs);
                defer outputs.deinitWithBoundary(cleanup.epoch());
                try std.testing.expect(outputs.model_retention == session.model_lifetime);
                try std.testing.expectEqual(@as(usize, 1), outputs.outputs.len);
                const shape = try outputs.backend.tensorShape(outputs.outputs[0], a);
                defer a.free(shape);
                try std.testing.expectEqualSlices(i64, &.{ 1, 3 }, shape);
                try std.testing.expect(outputs.resource_lease != null and controller.snapshot().host_scratch_bytes > 0);
                const materialized = try outputs.materializeFloat32(a, &.{}, outputs.backend.operationToken());
                defer {
                    for (materialized) |*value| value.deinit();
                    a.free(materialized);
                }
                for (materialized[0].asFloat32()) |value| try std.testing.expect(std.math.isFinite(value));
            }
            try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, controller.snapshot());
        }
        // Explicit strategy decline happens before backend allocation.
        impl.graph_runtime_strategy = .partitioned;
        var fail = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
        try std.testing.expect((try archRunMediaResident(impl, &inputs, fail.allocator(), control)) == null);
        try std.testing.expectEqual(@as(usize, 0), fail.alloc_index);
        impl.graph_runtime_strategy = null;
        const Cancel = struct {
            fn check(_: ?*anyopaque) !void {
                return error.Cancelled;
            }
        };
        var cancelled_permit = try session.admitInputsBeforeExecution(&inputs, a, control);
        defer cancelled_permit.deinit();
        try cancelled_permit.prepareResidentOutputRetirement();
        try std.testing.expectError(error.Cancelled, cancelled_permit.runResidentWithControl(&inputs, a, .{ .check_fn = Cancel.check, .native_call_policy = .in_process }));
        cancelled_permit.deinit();
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, controller.snapshot());
        session.close();
        session_open = false;
        try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, controller.snapshot());
    }
}

test "native media geometry protects actual projected rows for both text and image audio inputs" {
    var owner: ArchSession = undefined;
    owner.task = .generic;
    const info = [_]TensorInfo{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 2, 4 } },
        .{ .name = "pixel_values", .dtype = .f32, .shape = &.{ 2, 3, 16, 16 } },
        .{ .name = "input_features", .dtype = .f32, .shape = &.{ 2, 1, 64, 64 } },
    };
    for ([_]bool{ false, true }) |clap| {
        owner.arch_config = if (clap) .{ .clap = .{ .projection_dim = 7 } } else .{ .clip = .{ .projection_dim = 7 } };
        for (info) |input| {
            const resolved = (try archRunGeometry(&owner, .{ .shapes = &.{input} }, 2)).?;
            try std.testing.expectEqual(@as(usize, 2 * 7 * @sizeOf(f32) + 3 * @sizeOf(i64)), resolved.output_bytes);
            try std.testing.expectEqual(@as(usize, @intCast(input.shape[1])), resolved.sequence);
        }
    }
}

/// One stable compute wrapper, but only ready GPU work owns the model/provider.
/// Suspending retires all request-local caches; outputs crossing stages must be
/// copied into independent buffers. CPU/CUDA retain established lease policy.
pub const StagedExecution = struct {
    // WORKING owner fragment. This scope must reach its final caller address
    // before initInto; borrowed Workload remains live through close. It is not
    // a persistent backend/lease owner and stores no creation operation/epoch.
    const PreparedCache = if (build_options.enable_cuda) @import("../ops/cuda/prepared_cache.zig") else struct {};
    pub const CachePhase = if (build_options.enable_cuda) struct {
        stage: *StagedExecution,
        work: PreparedCache.Workload,
        // Official supported topology uses49 encoder requests; vision uses33.
        // Inline values keep warm phases allocator-free and names owned.
        request_storage: [128]PreparedCache.Request = undefined,
        warm: PreparedCache.Funding = PreparedCache.Funding.empty(),
        cold: ?*PreparedCache.Funding = null,
        bound: bool = false,
        live: bool = false,

        fn initInto(self: *@This(), stage: *StagedExecution, work: PreparedCache.Workload) !void {
            if (stage.cache_scope != null or !stage.cold_prepared or !stage.active) return error.InvalidPreparedCacheState;
            self.* = .{ .stage = stage, .work = work };
            try self.prepare();
            stage.cache_scope = self;
            self.live = true;
        }
        pub fn initEncoderInto(self: *@This(), stage: *StagedExecution, config: embedding_gemma2_model.Config, batch: usize, sequence: usize, mask: []const i64) !void {
            if (stage.cache_scope != null or !stage.cold_prepared or !stage.active) return error.InvalidPreparedCacheState;
            self.* = .{ .stage = stage, .work = .{ .requests = &.{} } };
            const compute = try stage.cudaStageCompute();
            var builder = PreparedCache.Builder{ .storage = &self.request_storage };
            try compute.appendEncoderCacheWorkload(config, batch, sequence, mask, &builder);
            self.work = builder.workload();
            try self.prepare();
            stage.cache_scope = self;
            self.live = true;
        }
        fn binding(self: *@This()) *PreparedCache.Funding {
            return self.cold orelse &self.warm;
        }
        fn unbindLocked(self: *@This()) void {
            if (!self.bound) return;
            const compute = self.stage.cudaStageCompute() catch platform.inference_process_supervisor.restartWorker();
            cuda_compute_mod.CudaCompute.unbindCacheFunding(self.stage.cb, compute, self.binding());
            self.bound = false;
            if (self.cold) |cold| {
                if (self.stage.cache_pending != null) platform.inference_process_supervisor.restartWorker();
                self.stage.cache_pending = cold;
                self.cold = null;
            }
            self.warm = PreparedCache.Funding.empty();
        }
        /// All producer TAKEN tokens have published/failed before their call
        /// returns. READY leftovers own no SDK roots. Unbind is metadata only;
        /// the Stage's existing real yield supplies its normal checked fence.
        pub fn close(self: *@This()) void {
            if (!self.live) return;
            if (self.stage.cache_scope != self or (self.bound and !self.stage.active)) platform.inference_process_supervisor.restartWorker();
            self.unbindLocked();
            self.stage.cache_scope = null;
            self.live = false;
        }
        fn prepare(self: *@This()) anyerror!void {
            const stage = self.stage;
            errdefer stage.restoreAfterQuotaForCleanup();
            for (0..8) |_| {
                try stage.control.check();
                var compute = try stage.cudaStageCompute();
                if (!try cuda_compute_mod.CudaCompute.cacheWorkloadNeedsFunding(stage.cb, compute, self.work)) {
                    try cuda_compute_mod.CudaCompute.bindCacheFunding(stage.cb, compute, self.work, &self.warm, stage.epoch() orelse return error.MissingCurrentCleanupEpoch);
                    self.bound = true;
                    return;
                }
                // Preserve the existing Stage fence/suspend. Cold metadata and
                // supplier callbacks are created only after that real unlock.
                _ = try stage.suspendForQuota();
                const funding = try PreparedCache.Funding.create(self.work);
                var owned = true;
                defer if (owned) {
                    if (stage.active) stage.leaveCudaModel();
                    funding.finishOutsideExecution();
                };
                try stage.enter(stage.control, false);
                try stage.establishCudaPolicy(stage.cuda_policy);
                compute = try stage.cudaStageCompute();
                const victims = try cuda_compute_mod.CudaCompute.cacheWorkloadVictims(stage.cb, compute, self.work);
                switch (victims) {
                    .all => return error.InvalidPreparedCacheState,
                    .geometry => |geometry| if (geometry.resize_plans != 0 or geometry.resize_sources != 0 or geometry.execution_arenas != 0) {
                        stage.retireCudaCachesSelected(victims) catch |err| {
                            if (!retirementSnapshotChanged(err)) return err;
                            if (stage.active) stage.leaveCudaModel();
                            funding.finishOutsideExecution();
                            owned = false;
                            try stage.enter(stage.control, false);
                            try stage.establishCudaPolicy(stage.cuda_policy);
                            continue;
                        };
                        try stage.enter(stage.control, false);
                        try stage.establishCudaPolicy(stage.cuda_policy);
                        compute = try stage.cudaStageCompute();
                    },
                }
                cuda_compute_mod.CudaCompute.snapshotCacheFunding(stage.cb, compute, funding) catch |err| {
                    if (err != error.CacheVictimsRequireRetirement) return err;
                    stage.leaveCudaModel();
                    funding.finishOutsideExecution();
                    owned = false;
                    try stage.enter(stage.control, false);
                    try stage.establishCudaPolicy(stage.cuda_policy);
                    continue;
                };
                stage.leaveCudaModel();
                const model = stage.model_retention orelse return error.MissingModelLifetime;
                var pin = if (funding.admission) |admission| model.retainAdmissionDomain(admission.controller) else null;
                defer if (pin) |held| held.release();
                funding.fundOutsideExecution(if (pin) |*held| held else null) catch |err| {
                    if (err != error.ResourceLimitExceeded and err != error.ResourceTemporarilyUnavailable) return err;
                    // These are optional cache roots. Keep the fully admitted
                    // numerical run/floors and use the existing eager/uncached
                    // body when only the separate cache reservation declines.
                    // Chosen obsolete geometry may already have retired, just
                    // as the original discard/recreate cache policy required.
                    funding.finishOutsideExecution();
                    owned = false;
                    try stage.enter(stage.control, false);
                    try stage.establishCudaPolicy(stage.cuda_policy);
                    compute = try stage.cudaStageCompute();
                    self.warm = PreparedCache.Funding.compatibility();
                    try cuda_compute_mod.CudaCompute.bindCacheFunding(stage.cb, compute, self.work, &self.warm, stage.epoch() orelse return error.MissingCurrentCleanupEpoch);
                    self.bound = true;
                    return;
                };
                try stage.enter(stage.control, false);
                try stage.establishCudaPolicy(stage.cuda_policy);
                compute = try stage.cudaStageCompute();
                cuda_compute_mod.CudaCompute.bindCacheFunding(stage.cb, compute, self.work, funding, stage.epoch() orelse return error.MissingCurrentCleanupEpoch) catch |err| {
                    if (err != error.PreparedCachePlanChanged) return err;
                    stage.leaveCudaModel();
                    funding.finishOutsideExecution();
                    owned = false;
                    try stage.enter(stage.control, false);
                    try stage.establishCudaPolicy(stage.cuda_policy);
                    continue;
                };
                self.cold = funding;
                owned = false;
                self.bound = true;
                return;
            }
            return error.ResourceTemporarilyUnavailable;
        }
    } else struct {};

    fn unbindCachePhase(self: *StagedExecution) void {
        if (comptime build_options.enable_cuda) if (self.cache_scope) |scope| scope.unbindLocked();
    }
    fn finishCacheFundingOutsideExecution(self: *StagedExecution) void {
        if (comptime build_options.enable_cuda) if (self.cache_pending) |funding| {
            if (self.active) platform.inference_process_supervisor.restartWorker();
            self.cache_pending = null;
            funding.finishOutsideExecution();
        };
    }

    const RunPermit = @import("../backends/session.zig").RunPermit;
    const CudaPolicy = struct {
        present: bool = false,
        controller: ?*runtime.tier.memory.AdmissionController = null,
        limits: runtime.tier.memory.Limits = .{},
        check_live_memory: bool = true,
        fn from(value: ?@import("../ops/cuda/video_cache_budget.zig").Admission) CudaPolicy {
            const admission = value orelse return .{};
            return .{ .present = true, .controller = admission.controller, .limits = admission.limits, .check_live_memory = admission.check_live_memory };
        }
        fn toAdmission(self: CudaPolicy) ?@import("../ops/cuda/video_cache_budget.zig").Admission {
            if (!self.present) return null;
            return .{ .controller = self.controller.?, .limits = self.limits, .check_live_memory = self.check_live_memory };
        }
        fn same(a: CudaPolicy, b: CudaPolicy) bool {
            if (a.present != b.present) return false;
            if (!a.present) return true;
            return a.controller == b.controller and a.check_live_memory == b.check_live_memory and
                a.limits.host_limit_bytes == b.limits.host_limit_bytes and
                a.limits.backend_limit_bytes == b.limits.backend_limit_bytes and
                a.limits.combined_limit_bytes == b.limits.combined_limit_bytes and
                a.limits.kv_limit_bytes == b.limits.kv_limit_bytes and
                a.limits.scratch_limit_bytes == b.limits.scratch_limit_bytes;
        }
    };

    const Control = InferenceExecutionControl;
    const ComputeBackend = ops.ComputeBackend;
    const Scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler;
    pub const Counters = struct {
        phase_reclaim_attempts: usize = 0,
        stage_retire_ns: u64 = 0,
        stage_rebind_ns: u64 = 0,
        stage_copy_ns: u64 = 0,
        stage_copy_bytes: usize = 0,
        stage_yields: usize = 0,
        stage_acquisitions: usize = 0,
    };
    const MediaTimings = Counters;
    fn mediaNow() u64 {
        return platform.time.monotonicNs();
    }
    fn enterModel(session: Session, gate: ?*std.atomic.Mutex, control: Control, waiter: *Scheduler.Waiter) !void {
        try session.enterExecutionCohort(gate, control, waiter);
    }
    fn leaveModel(gate: ?*std.atomic.Mutex, waiter: *Scheduler.Waiter) void {
        const held = gate orelse return;
        if (waiter.owner != null) Scheduler.leave(waiter) else held.unlock();
    }
    cache_scope: if (build_options.enable_cuda) ?*CachePhase else void = if (build_options.enable_cuda) null else {},
    cache_pending: if (build_options.enable_cuda) ?*PreparedCache.Funding else void = if (build_options.enable_cuda) null else {},
    cuda_policy: CudaPolicy = .{},
    cuda_policy_set: bool = false,
    cleanup_owner: @import("cleanup_owner.zig").Owner = .{},
    model_retention: ?*Session.ModelLifetime = null,
    cold_prepared: bool = false,
    managed: ?@import("../backends/compute_provider.zig").ManagedComputeBackend = null,
    // Actual authored phase scopes have depth at most two: the image attempt
    // and its fallback, or one ready/audio phase. The next fenced admission
    // flushes ended tickets before acquiring its replacement. No heap holder.
    pending_phases: [2]?RunPermit = @splat(null),
    replace_extra_at_admission: bool = false,
    session: Session,
    gate: ?*std.atomic.Mutex,
    waiter: *Scheduler.Waiter,
    cb: *ComputeBackend,
    control: Control,
    active: bool = true,
    scheduling_enabled: bool = true,
    permit: *RunPermit,
    owns_initial_permit: bool = false,
    timings: ?*MediaTimings = null,
    idle_backend: usize,
    extra: ?RunPermit = null,
    video_phases: bool = false,
    phase_provider_suspended: bool = false,
    owns_unstaged_provider: bool = false,
    composition_request: ?@import("../backends/session.zig").RunRequest = null,
    hook: @import("../ops/ops.zig").EmbeddingStageControl = undefined,
    pub const Phase = struct {
        owner: ?*StagedExecution,
        owned: ?RunPermit,
        pub fn deinit(self: *Phase) void {
            var owned = self.owned orelse return;
            self.owned = null;
            // Only pure supplemental admission may enter this ticket. Never
            // retain a prepared plan, physical bank, B or numerical operation.
            if (owned.prepared != null or owned.physical_storage != null or owned.dispatch_cleanup_epoch != null or owned.output_retirement_shell != null) platform.inference_process_supervisor.restartWorker();
            if (owned.lease == null) {
                // Session.retainPermit does not retain ResourcePin for an
                // empty supplement; this path has no owned resource to drop.
                if (owned.resource_pin != null) platform.inference_process_supervisor.restartWorker();
                return;
            }
            if (self.owner) |owner| {
                for (&owner.pending_phases) |*slot| if (slot.* == null) {
                    slot.* = owned;
                    return;
                };
                platform.inference_process_supervisor.restartWorker();
            } else owned.deinit();
        }
    };
    pub fn emptyPhase(self: *StagedExecution) Phase {
        return .{ .owner = null, .owned = .{ .session = self.session, .lease = null } };
    }
    pub fn admitPhaseScoped(self: *StagedExecution, target: @import("../runtime/tier/memory.zig").AdmissionAmounts, other: ?*const RunPermit) !Phase {
        return .{ .owner = if (self.cold_prepared) self else null, .owned = try self.admitPhase(target, other) };
    }
    fn flushEndedPhases(self: *StagedExecution) void {
        for (&self.pending_phases) |*slot| {
            if (slot.*) |*permit| permit.deinitWithCleanup(.{ .boundary = self.epoch() });
            slot.* = null;
        }
    }

    pub const ReadyAdmission = struct { count: usize, permit: Phase };
    pub fn admitReady(lane: *StagedExecution, maximum: usize, context: anytype, comptime request_fn: anytype) !ReadyAdmission {
        std.debug.assert(maximum > 0 and maximum <= 4);
        var count = maximum;
        while (true) {
            try lane.control.check();
            const request = try request_fn(context, count);
            if (count > 1 and !try lane.session.fitsRun(request)) {
                count -= 1;
                continue;
            }
            const admission = lane.session.run_admission orelse return .{ .count = count, .permit = lane.emptyPhase() };
            const target = try admission.estimateRequest(request, lane.session.outputInfo());
            const permit = lane.admitPhaseScoped(target, lane.credit()) catch |err| {
                if (count == 1 or !(err == error.MemoryBudgetExceeded or err == error.ResourceLimitExceeded or err == error.ResourceTemporarilyUnavailable)) return err;
                count -= 1;
                continue;
            };
            return .{ .count = count, .permit = permit };
        }
    }
    pub fn phaseAdmissionEnabled(backend_kind: BackendType, gate: ?*std.atomic.Mutex) bool {
        return (backend_kind == .metal or backend_kind == .cuda) and gate != null and !platform.env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_STAGE_SCHEDULING");
    }
    pub fn prepareBeforeExecution(self: *StagedExecution) !void {
        if (self.cold_prepared) return error.EmbeddingStageAlreadyPrepared;
        if (self.waiter.owner != null or self.waiter.acquired or self.waiter.gate != null) return error.InvalidStageCohort;
        if (self.session.execution_gate) |published| {
            if (self.gate != null and self.gate != published) return error.ExecutionCohortGateChanged;
            self.gate = published;
        }
        if (self.permit.session.ptr != self.session.ptr or self.permit.session.vtable != self.session.vtable or self.permit.execution_yielded or self.permit.prepared != null or self.permit.physical_storage != null or self.permit.dispatch_cleanup_epoch != null) return error.InvalidStageAdmissionOwner;
        self.cleanup_owner = if ((self.session.backend() == .metal or self.session.backend() == .cuda)) try @import("cleanup_owner.zig").Owner.beginResident(self.session) else .{};
        self.model_retention = if (self.session.model_lifetime) |model| model.retain() else null;
        self.cold_prepared = true;
    }
    pub fn epoch(self: *const StagedExecution) ?*const @import("retirement_sink.zig").Epoch {
        return self.cleanup_owner.epoch();
    }
    /// The numerical/provider header stays at this owner's final address.
    /// No task callback acquires or releases a provider/model gate.
    fn acquireCudaRetirementOnly(session: Session, allocator: std.mem.Allocator, options: @import("../backends/session.zig").ComputeBackendOptions) !ops.ComputeBackend {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        if (session.vtable != &arch_vtable or options.run_budget != null or options.borrow_shared_provider) return error.UnsupportedEmbeddingStage;
        const actual: *ArchSession = @ptrCast(@alignCast(session.ptr));
        if (actual.backend_type != .cuda or actual.arch_config != .embedding_gemma2) return error.UnsupportedEmbeddingStage;
        // Cache retirement must not run request-boundary policy replacement.
        return makeComputeBackend(actual, allocator, null);
    }
    fn leaveCudaModel(self: *StagedExecution) void {
        std.debug.assert(self.active);
        self.unbindCachePhase();
        leaveModel(self.gate, self.waiter);
        self.active = false;
        self.finishCacheFundingOutsideExecution();
    }
    fn enterCudaRetirementOnly(self: *StagedExecution, allocator: std.mem.Allocator) !void {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        if (!self.cold_prepared or self.managed != null or self.active or self.session.vtable != &arch_vtable) return error.InvalidStageCohort;
        try enterModel(self.session, self.gate, self.control, self.waiter);
        self.active = true;
        errdefer if (self.active) self.leaveCudaModel();
        self.managed = try @import("../backends/compute_provider.zig").acquireWith(self.session, allocator, null, self.control, acquireCudaRetirementOnly);
        errdefer {
            self.managed.?.deinitWithCleanup(.{ .boundary = self.epoch() });
            self.managed = null;
        }
        self.cb = &self.managed.?.backend;
        _ = try self.cudaStageCompute();
    }
    fn retirementSnapshotChanged(err: anyerror) bool {
        return switch (err) {
            error.CacheRetirementChanged, error.CacheRetirementCapacityChanged, error.QueueFull => true,
            else => false,
        };
    }
    /// Entered under this owner's actual model cohort. Every SDK free finishes
    /// outside that cohort; pending clears before quota callbacks can reenter.
    /// Returns with the model lane released, including every error path.
    fn retireCudaCaches(self: *StagedExecution) !void {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        return self.retireCudaCachesSelected(.all);
    }
    fn retireCudaCachesSelected(self: *StagedExecution, selection: if (build_options.enable_cuda) cuda_compute_mod.CudaCompute.CacheRetirementSelection else void) !void {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        errdefer if (self.active) self.leaveCudaModel();
        const model = self.model_retention orelse return error.MissingModelLifetime;
        for (0..8) |_| {
            try self.control.check();
            const compute = try self.cudaStageCompute();
            const required = try cuda_compute_mod.CudaCompute.cacheRetirementRequirements(self.cb, compute, selection);
            self.leaveCudaModel();
            var old_domain = if (required.old_controller) |controller| model.retainAdmissionDomain(controller) else null;
            defer if (old_domain) |pin| pin.release();
            const carrier = try cuda_compute_mod.CudaCompute.CacheRetirement.prepare(required, model, if (old_domain) |*pin| pin else null);
            if (old_domain) |pin| pin.release();
            old_domain = null;
            var consumed = false;
            defer if (!consumed) carrier.deinitUnused();
            try self.control.check();
            try enterModel(self.session, self.gate, self.control, self.waiter);
            self.active = true;
            carrier.detachFenced(self.cb, compute) catch |err| {
                self.leaveCudaModel();
                if (!retirementSnapshotChanged(err)) return err;
                // A mask is derived from one actual cache snapshot. Its
                // caller must recompute victims after a competing mutation.
                switch (selection) {
                    .geometry => return err,
                    .all => {},
                }
                carrier.deinitUnused();
                consumed = true;
                try self.control.check();
                try enterModel(self.session, self.gate, self.control, self.waiter);
                self.active = true;
                continue;
            };
            self.leaveCudaModel();
            // Ownership moved. Cancellation cannot abandon a detached cache or
            // replay numerical work after uncertain SDK completion.
            carrier.retireOutsideExecution(self.epoch()) catch platform.inference_process_supervisor.restartWorker();
            consumed = true;
            carrier.destroy();
            return;
        }
        return error.ResourceTemporarilyUnavailable;
    }
    fn establishCudaPolicy(self: *StagedExecution, desired: CudaPolicy) !void {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        for (0..8) |_| {
            try self.control.check();
            const compute = try self.cudaStageCompute();
            if (CudaPolicy.same(desired, CudaPolicy.from(compute.ctx.video_cache_budget.admission))) return;
            try self.retireCudaCaches();
            try self.control.check();
            try enterModel(self.session, self.gate, self.control, self.waiter);
            self.active = true;
            cuda_compute_mod.CudaCompute.installEmptyEmbeddingGemma2Policy(self.cb, compute, desired.toAdmission()) catch |err| {
                if (err == error.CachePolicyNotEmpty) continue;
                return err;
            };
            return;
        }
        return error.ResourceTemporarilyUnavailable;
    }
    /// Called while the actual source owns its model lane. Preserve ordinary
    /// owned inputs; snapshot only borrowed/pinned representations byte-for-byte.
    pub fn prepareCudaInputHandoff(self: *StagedExecution, value: *ops.CT) !void {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        const compute = try self.cudaStageCompute();
        if (try cuda_compute_mod.CudaCompute.keepEmbeddingStageInput(self.cb, compute, value.*)) return;
        const snapshot_value = try cuda_compute_mod.CudaCompute.escapeEmbeddingStage(self.cb, value.*, null);
        const original = value.*;
        value.* = snapshot_value;
        self.cb.freeWithCleanup(original, .{ .boundary = self.epoch() });
    }

    pub fn admitInitialCuda(self: *StagedExecution, allocator: std.mem.Allocator, request: @import("../backends/session.zig").RunRequest, supplied_pressure: ?*?runtime.tier.memory.AdmissionPressure) !void {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        if (!self.cold_prepared or self.active or self.managed != null or self.permit.lease != null or self.permit.resource_pin != null or self.session.backend() != .cuda) return error.InvalidStageCohort;
        var own_pressure: ?runtime.tier.memory.AdmissionPressure = null;
        const pressure = supplied_pressure orelse &own_pressure;
        try self.control.check();
        self.permit.* = self.session.admitWithPressure(request, pressure) catch |err| retry: {
            if (err != error.ResourceTemporarilyUnavailable and err != error.ResourceLimitExceeded) return err;
            try self.enterCudaRetirementOnly(allocator);
            defer {
                if (self.active) self.leaveCudaModel();
                self.managed.?.deinitWithCleanup(.{ .boundary = self.epoch() });
                self.managed = null;
                self.cb = undefined;
            }
            try self.retireCudaCaches();
            try self.control.check();
            break :retry try self.session.admitWithPressure(request, pressure);
        };
        // This entry created the actual primary lease. The SAME owner returns
        // it only after actual lane unlock, while whole-task B is still live.
        self.owns_initial_permit = true;
    }

    pub fn enterInitial(self: *StagedExecution, allocator: std.mem.Allocator, control: ?Control) !void {
        if (!self.cold_prepared or self.managed != null) return error.EmbeddingStageNotPrepared;
        try enterModel(self.session, self.gate, self.control, self.waiter);
        self.active = true;
        errdefer if (self.active) self.leaveCudaModel();
        self.managed = if (self.session.backend() == .cuda)
            try @import("../backends/compute_provider.zig").acquireWith(self.session, allocator, null, control, acquireCudaRetirementOnly)
        else
            try @import("../backends/compute_provider.zig").acquire(self.session, allocator, null, control);
        errdefer {
            self.managed.?.deinitWithCleanup(.{ .boundary = self.epoch() });
            self.managed = null;
        }
        self.cb = &self.managed.?.backend;
        self.active = true;
        if (self.session.backend() == .cuda) {
            if (comptime build_options.enable_cuda) {
                const desired = CudaPolicy.from(if (self.session.run_admission) |admission| .{ .controller = admission.controller, .limits = admission.limits, .check_live_memory = admission.check_live_memory } else null);
                try self.establishCudaPolicy(desired);
                try @import("../backends/compute_factory.zig").beginRequest(self.session, self.cb);
            } else return error.CudaNotEnabled;
        }
        try self.attach();
    }
    pub fn backend(self: *StagedExecution) *ComputeBackend {
        std.debug.assert(self.managed != null and self.active);
        return self.cb;
    }
    fn finishAfterUnlock(self: *StagedExecution) void {
        self.flushEndedPhases();
        if (self.extra) |*extra| extra.deinitWithCleanup(.{ .boundary = self.cleanup_owner.epoch() });
        self.extra = null;
        if (self.owns_initial_permit) self.permit.deinitWithCleanup(.{ .boundary = self.epoch() });
        self.owns_initial_permit = false;
        if (self.model_retention) |model| model.releaseWithBoundary(self.cleanup_owner.epoch());
        self.model_retention = null;
        self.cleanup_owner.finishAfterUnlock();
        self.cold_prepared = false;
    }
    fn current(self: *StagedExecution) @import("cleanup_context.zig").Context {
        return .{ .boundary = self.cleanup_owner.epoch(), .operation = if (self.cold_prepared and self.cb.kind() == .metal) self.cb.operationToken() else null };
    }
    fn cudaStageCompute(self: *StagedExecution) !(if (build_options.enable_cuda) *cuda_compute_mod.CudaCompute else void) {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        if (!self.cold_prepared or !self.active or self.managed == null or self.gate == null or self.session.vtable != &arch_vtable) return error.UnsupportedEmbeddingStage;
        const model = self.model_retention orelse return error.MissingModelLifetime;
        if (model.destroy_fn != archClose or model.quiesce_fn != archQuiesce) return error.UnsupportedEmbeddingStage;
        const actual: *ArchSession = @ptrCast(@alignCast(model.owner));
        if (self.session.ptr != @as(*anyopaque, @ptrCast(actual)) or &actual.model_lifetime != model or actual.backend_type != .cuda or actual.arch_config != .embedding_gemma2) return error.UnsupportedEmbeddingStage;
        const compute = actual.backend_data.cuda.compute;
        try cuda_compute_mod.CudaCompute.validateEmbeddingStage(self.cb, compute);
        return compute;
    }
    fn requireCudaPolicy(self: *StagedExecution) !void {
        if (self.session.backend() != .cuda) return;
        if (comptime build_options.enable_cuda) {
            const compute = try self.cudaStageCompute();
            if (!self.cuda_policy_set or !CudaPolicy.same(self.cuda_policy, CudaPolicy.from(compute.ctx.video_cache_budget.admission))) return error.BackendAdmissionPolicyChanged;
        } else return error.CudaNotEnabled;
    }
    fn attach(self: *StagedExecution) !void {
        if (self.cb.kind() == .cuda) {
            if (comptime build_options.enable_cuda) {
                const compute = try self.cudaStageCompute();
                self.cuda_policy = CudaPolicy.from(compute.ctx.video_cache_budget.admission);
                self.cuda_policy_set = true;
            } else return error.CudaNotEnabled;
        } else {
            if (self.cb.kind() != .metal) return;
            if (!self.cold_prepared) return error.EmbeddingStageNotPrepared;
            if (!@import("../ops/metal_compute.zig").MetalCompute.acceptsEmbeddingStage(self.cb)) return error.UnsupportedEmbeddingStage;
            const b = self.cleanup_owner.epoch() orelse return error.MissingCurrentCleanupEpoch;
            if (!try self.cb.bindCleanupEpoch(b)) return error.BackendOperationCleanupUnavailable;
            _ = try self.current().operationFor(self.cb);
        }
        if (!self.scheduling_enabled or self.gate == null or platform.env.getenvBool("ANTFLY_EMBEDDINGGEMMA2_DISABLE_STAGE_SCHEDULING")) return;
        self.hook = .{ .ptr = self, .pause_fn = pause, .resume_fn = acquire, .yield_ready_fn = yieldReady, .cleanup_fn = cleanup, .workspace_fn = if (self.video_phases) workspace else null, .copy_fn = if (self.timings != null) copied else null };
        self.cb.embedding_stage = &self.hook;
    }
    pub fn yieldReady(raw: *anyopaque) !void {
        const self: *StagedExecution = @ptrCast(@alignCast(raw));
        try self.control.check();
        if (!self.active or !Scheduler.hasPeer(self.waiter)) return;
        try pause(raw);
        try acquire(raw);
    }
    fn pause(raw: *anyopaque) !void {
        const self: *StagedExecution = @ptrCast(@alignCast(raw));
        if (!self.active) return;
        const retired_at = if (self.timings != null) mediaNow() else 0;
        if (self.cb.kind() == .cuda) {
            if (comptime build_options.enable_cuda) {
                const compute = try self.cudaStageCompute();
                try cuda_compute_mod.CudaCompute.quiesceEmbeddingStage(self.cb, compute);
            } else return error.CudaNotEnabled;
        } else try @import("../ops/metal_compute.zig").MetalCompute.embeddingStageLeaseWithCleanup(self.cb, false, self.current());
        if (self.timings) |t| t.stage_retire_ns += mediaNow() - retired_at;
        self.unbindCachePhase();
        leaveModel(self.gate, self.waiter);
        self.active = false;
        self.finishCacheFundingOutsideExecution();
        if (self.extra) |*extra| extra.deinitWithCleanup(.{ .boundary = self.cleanup_owner.epoch() });
        self.extra = null;
        if (self.permit.lease) |*lease| {
            var idle = lease.amounts;
            idle.backend_scratch_bytes = @min(idle.backend_scratch_bytes, self.idle_backend);
            // Retain is a reduction; if a process owner rejects it, keep the
            // conservative reservation and still hand the model gate onward.
            lease.retain(idle) catch {};
        }
        if (self.timings) |t| t.stage_yields += 1;
    }
    fn enter(self: *StagedExecution, control: Control, execution: bool) !void {
        if (self.active) return;
        if (execution) try self.reserve(if (self.video_phases) 0 else null);
        errdefer {
            if (self.extra) |*extra| extra.deinit();
            self.extra = null;
        }
        try enterModel(self.session, self.gate, control, self.waiter);
        self.active = true;
        errdefer if (self.active) self.leaveCudaModel();
        const acquired_at = if (self.timings != null) mediaNow() else 0;
        if (self.cb.kind() == .cuda) {
            if (comptime build_options.enable_cuda) {
                _ = try self.cudaStageCompute();
                if (execution) {
                    try self.establishCudaPolicy(self.cuda_policy);
                    try self.requireCudaPolicy();
                    if (self.cache_scope) |scope| if (!scope.bound) try scope.prepare();
                }
            } else return error.CudaNotEnabled;
        } else try @import("../ops/metal_compute.zig").MetalCompute.embeddingStageLeaseWithCleanup(self.cb, true, .{ .boundary = self.cleanup_owner.epoch() });
        if (self.timings) |t| t.stage_rebind_ns += mediaNow() - acquired_at;
        self.active = true;
        if (self.timings) |t| t.stage_acquisitions += 1;
    }
    pub fn reserve(self: *StagedExecution, bytes: ?usize) !void {
        // Every caller has fenced its previous GPU phase. Releasing an old
        // supplement before acquiring the replacement never borrows stale credit.
        self.replace_extra_at_admission = true;
        var request = self.composition_request orelse self.permit.request orelse return error.InvalidAdmissionLeaseReduction;
        if (bytes) |work| {
            request.workspace_bytes = try std.math.add(usize, self.idle_backend, work);
            request.backend_input_bytes = 0;
            request.batch = 1;
            request.sequence = 1;
        }
        if (self.session.run_admission) |admission| self.extra = try self.admitPhase(try admission.estimateRequest(request, self.session.outputInfo()), null);
    }
    fn prepareReclaim(self: *StagedExecution, _: Control) !void {
        if (self.timings) |t| t.phase_reclaim_attempts += 1;
        // Keep the FIFO turn, but expose an idle provider to cache reclamation.
        // Inputs at these boundaries own independent buffers. No active frame
        // or request-local weight binding may survive retirement.
        if (!self.active or (self.gate == null and !self.owns_unstaged_provider)) return;
        const started = if (self.timings != null) mediaNow() else 0;
        if (self.cb.kind() == .cuda) {
            if (comptime build_options.enable_cuda) {
                const compute = try self.cudaStageCompute();
                try cuda_compute_mod.CudaCompute.quiesceEmbeddingStage(self.cb, compute);
            } else return error.CudaNotEnabled;
        } else try @import("../ops/metal_compute.zig").MetalCompute.embeddingStageLeaseWithCleanup(self.cb, false, self.current());
        self.phase_provider_suspended = true;
        if (self.timings) |t| t.stage_retire_ns += mediaNow() - started;
    }
    fn reclaimMedia(self: *StagedExecution, control: Control) !usize {
        return StagedAdmission.reclaimMediaWeights(self.session, control);
    }
    fn reclaimLanguage(self: *StagedExecution, control: Control) !usize {
        return reclaimEmbeddingGemma2LanguageWeights(self.session, control);
    }
    fn suspendForQuota(self: *StagedExecution) !bool {
        if (!self.cold_prepared or !self.active) return false;
        switch (self.cb.kind()) {
            .native => {
                // Actual request-private Native callbacks join before return.
                // An explicitly shared/foreign operation is never ambient
                // authority to release or borrow that provider's gate.
                if (self.cb.vtable != &native_mod.vtable_impl) return error.UnsupportedStageProvider;
                const compute: *NativeCompute = @ptrCast(@alignCast(self.cb.ptr));
                if (compute.shared_operation_owner != null) return error.BackendCleanupAuthorityRequired;
            },
            .metal => {},
            .cuda => {
                if (comptime build_options.enable_cuda) {
                    const compute = try self.cudaStageCompute();
                    try cuda_compute_mod.CudaCompute.quiesceEmbeddingStage(self.cb, compute);
                } else return error.CudaNotEnabled;
            },
            else => return false,
        }
        if (self.cb.kind() == .metal) {
            const started = if (self.timings != null) mediaNow() else 0;
            try @import("../ops/metal_compute.zig").MetalCompute.embeddingStageLeaseWithCleanup(self.cb, false, self.current());
            if (self.timings) |counters| counters.stage_retire_ns += mediaNow() - started;
            self.phase_provider_suspended = true;
        }
        self.unbindCachePhase();
        leaveModel(self.gate, self.waiter);
        self.active = false;
        self.finishCacheFundingOutsideExecution();
        return true;
    }
    fn restoreAfterQuotaForCleanup(self: *StagedExecution) void {
        while (true) {
            self.enter(.{}, false) catch {
                platform.time.yieldNow();
                continue;
            };
            break;
        }
        self.phase_provider_suspended = false;
    }
    fn restoreAfterQuota(self: *StagedExecution) !void {
        self.restoreAfterQuotaForCleanup();
        // Cleanup may reacquire solely to destroy owned values under the real
        // model lane. A successful numerical continuation needs its own policy.
        if (self.session.backend() == .cuda) {
            if (comptime build_options.enable_cuda) try self.establishCudaPolicy(self.cuda_policy) else return error.CudaNotEnabled;
        }
        try self.requireCudaPolicy();
        if (comptime build_options.enable_cuda) if (self.cache_scope) |scope| if (!scope.bound) try scope.prepare();
    }
    fn admitPhase(self: *StagedExecution, target: @import("../runtime/tier/memory.zig").AdmissionAmounts, other: ?*const RunPermit) !RunPermit {
        if (self.session.backend() == .cuda) try self.control.check();
        const deficit = try self.session.supplementPairDeficit(self.permit, other, target);
        var refund = false;
        for (self.pending_phases) |slot| if (slot != null) {
            refund = true;
        };
        if (self.replace_extra_at_admission) if (self.extra) |extra| {
            if (extra.lease != null) refund = true;
        };
        if (!refund and std.meta.eql(deficit, @import("../runtime/tier/memory.zig").AdmissionAmounts{})) {
            if (self.replace_extra_at_admission) {
                if (self.extra) |*extra| extra.deinit();
                self.extra = null;
                self.replace_extra_at_admission = false;
            }
            return self.session.admitSupplementPair(self.permit, other, target);
        }
        const restore = try self.suspendForQuota();
        errdefer if (restore) self.restoreAfterQuotaForCleanup();
        defer if (!restore and self.active and self.phase_provider_suspended) {
            const started = if (self.timings != null) mediaNow() else 0;
            // Ownership restoration is cleanup, including after cancellation or
            // denial. It needs no compute supplement and cannot abandon tensors.
            while (true) {
                @import("../ops/metal_compute.zig").MetalCompute.embeddingStageLeaseWithCleanup(self.cb, true, .{ .boundary = self.cleanup_owner.epoch() }) catch {
                    platform.time.yieldNow();
                    continue;
                };
                break;
            }
            self.phase_provider_suspended = false;
            if (self.timings) |t| t.stage_rebind_ns += mediaNow() - started;
        };
        if (self.cold_prepared and !self.active) self.flushEndedPhases();
        if (self.replace_extra_at_admission) {
            if (self.extra) |*extra| extra.deinitWithCleanup(.{ .boundary = self.cleanup_owner.epoch() });
            self.extra = null;
            self.replace_extra_at_admission = false;
        }
        const result = try StagedAdmission.admitSupplementReclaimingCaches(self.session, self.permit, other, target, self.control, self, prepareReclaim, reclaimMedia, reclaimLanguage);
        if (restore) self.restoreAfterQuota() catch |err| {
            // Restoration owns the lane for cleanup only. The newly admitted
            // pure supplement must refund after actual unlock, not here.
            var rejected = Phase{ .owner = self, .owned = result };
            rejected.deinit();
            return err;
        };
        return result;
    }
    fn copied(raw: *anyopaque, bytes: usize, nanos: u64) void {
        const self: *StagedExecution = @ptrCast(@alignCast(raw));
        if (self.timings) |t| {
            t.stage_copy_ns += nanos;
            t.stage_copy_bytes += bytes;
        }
    }
    fn workspace(raw: *anyopaque, bytes: ?usize) !void {
        const self: *StagedExecution = @ptrCast(@alignCast(raw));
        if (!self.active or self.cb.decoderRuntimeHasActiveFrame() or self.cb.decoderRuntimeHasSubmittedFrame()) return error.EmbeddingStageBusy;
        try self.control.check();
        try self.reserve(bytes);
    }
    fn acquire(raw: *anyopaque) !void {
        const self: *StagedExecution = @ptrCast(@alignCast(raw));
        self.enter(self.control, true) catch |err| {
            // A failed numerical resume still returns the actual cleanup lane
            // to the source owner before its caller destroys live input CTs.
            cleanup(self);
            return err;
        };
    }
    fn cleanup(raw: *anyopaque) void {
        const self: *StagedExecution = @ptrCast(@alignCast(raw));
        while (!self.active) self.enter(.{}, false) catch {
            platform.time.yieldNow();
        };
    }
    pub fn deinit(self: *StagedExecution) void {
        if (self.cold_prepared) {
            if (self.managed) |*managed| {
                if (!self.active) cleanup(self);
                if (self.cb.embedding_stage == &self.hook) self.cb.embedding_stage = null;
                if (self.cb.kind() == .cuda) {
                    if (comptime build_options.enable_cuda) {
                        const compute = self.cudaStageCompute() catch platform.inference_process_supervisor.restartWorker();
                        cuda_compute_mod.CudaCompute.quiesceEmbeddingStage(self.cb, compute) catch platform.inference_process_supervisor.restartWorker();
                    } else platform.inference_process_supervisor.restartWorker();
                }
                self.unbindCachePhase();
                if (comptime build_options.enable_cuda) if (self.cache_scope) |scope| {
                    scope.live = false;
                    self.cache_scope = null;
                };
                managed.deinitWithCleanup(.{ .boundary = self.epoch() });
                self.managed = null;
                leaveModel(self.gate, self.waiter);
                self.active = false;
                self.finishCacheFundingOutsideExecution();
            }
            self.finishAfterUnlock();
            return;
        }
        // An empty/failed cold constructor has no backend or lane to touch.
    }
    pub fn credit(self: *StagedExecution) ?*const RunPermit {
        return if (self.extra) |*extra| extra else null;
    }
    /// Execute the already-admitted resident source on this actual private
    /// backend. Task callers supply ordering/masks/requested dimensions; the
    /// runtime owns capability/policy selection and the concrete CUDA cast.
    pub fn runResidentEmbeddingEncoder(self: *StagedExecution, allocator: std.mem.Allocator, config: embedding_gemma2_model.Config, input: ops.CT, mask: []const i64, batch: usize, sequence: usize, dimensions: usize, record_media_batch: bool) ![]f32 {
        if (comptime !build_options.enable_cuda) return error.CudaNotEnabled;
        if (!self.cold_prepared or !self.active) return error.EmbeddingStageNotPrepared;
        try self.requireCudaPolicy();
        const original_compute = try self.cudaStageCompute();
        if (!try cuda_compute_mod.CudaCompute.keepEmbeddingStageInput(self.cb, original_compute, input)) return error.CudaStageInputRequiresHandoff;
        if (record_media_batch and batch > 1) (try self.cudaStageCompute()).embedding_gemma2_media_batches += 1;
        var caches: CachePhase = undefined;
        try caches.initEncoderInto(self, config, batch, sequence, mask);
        defer caches.close();
        const compute = try self.cudaStageCompute();
        return compute.runEmbeddingGemma2(allocator, config, .{ .resident_embeddings = input }, mask, batch, sequence, dimensions, self.control);
    }

    pub fn retain(self: *StagedExecution, request: @import("../backends/session.zig").RunRequest) !void {
        const restore = try self.suspendForQuota();
        errdefer if (restore) self.restoreAfterQuotaForCleanup();
        if (self.session.backend() == .metal) {
            // Every Metal composition path reaches a fenced boundary, including
            // preparation/residency opt-outs. The actual backbone can exceed
            // its original conservative tower reservation; admit that deficit
            // before reducing either independently owned lease.
            self.replace_extra_at_admission = true;
            if (self.session.run_admission) |admission| self.extra = try self.admitPhase(try admission.estimateRequest(request, self.session.outputInfo()), null);
        }
        if (self.extra) |*extra| {
            try self.permit.retainForRunWithSupplement(extra, request);
        } else try self.permit.retainForRun(request);
        self.composition_request = null;
        if (restore) try self.restoreAfterQuota();
    }
    /// Snapshot escape borrows only this actual private Engine's published
    /// operation and its already-bound B, never another active provider scope.
    /// No stage callback context is cast or treated as gate-release authority.
    pub fn escapeCurrent(cb: *const ops.ComputeBackend, tensor: ops.CT) !ops.CT {
        if (cb.embedding_stage == null) return error.UnsupportedEmbeddingStage;
        if (cb.kind() == .cuda) {
            if (comptime build_options.enable_cuda) return cuda_compute_mod.CudaCompute.escapeEmbeddingStage(cb, tensor, null);
            return error.CudaNotEnabled;
        }
        const token = cb.operationToken() orelse return error.BackendCleanupAuthorityRequired;
        var bound_b = (try token.retainCleanupEpoch()) orelse return error.MissingCurrentCleanupEpoch;
        defer bound_b.release();
        return @import("../ops/metal_compute.zig").MetalCompute.escapeEmbeddingStageWithCleanup(cb, tensor, null, .{ .boundary = &bound_b, .operation = token }, null);
    }
    pub fn releasePreparation(self: *StagedExecution, host: *RunPermit, request: @import("../backends/session.zig").RunRequest, retained_host: usize) !void {
        if (host.session.ptr != self.session.ptr or host.session.vtable != self.session.vtable) return error.EmbeddingStageChanged;
        const restore = try self.suspendForQuota();
        errdefer if (restore) self.restoreAfterQuotaForCleanup();
        try self.permit.retainForRun(request);
        try host.retainHostBytes(retained_host);
        if (restore) try self.restoreAfterQuota();
    }
    /// This actual projector returns one fresh destination filled completely
    /// before publication; it retains no future writer after returning. Other
    /// stage escapes preserve their established snapshot-copy semantics.
    pub fn retainProjectedOutput(self: *StagedExecution, projected: *@import("../architectures/gemma4_projector.zig").ResidentProjectedImages) !void {
        if (self.cb.kind() != .metal and self.cb.kind() != .cuda) return;
        if (projected.backend != self.cb) return error.EmbeddingStageChanged;
        try self.control.check();
        const replacement = try self.escapeImpl(projected.embeddings, projected.output_ownership == .completed_fresh_destination);
        errdefer self.cb.free(replacement);
        try self.control.check();
        self.cb.free(projected.embeddings);
        projected.embeddings = replacement;
    }
    pub fn escape(self: *StagedExecution, tensor: ops.CT) !ops.CT {
        return self.escapeImpl(tensor, false);
    }
    fn escapeImpl(self: *StagedExecution, tensor: ops.CT, completed_projection: bool) !ops.CT {
        if (!self.active or !self.cold_prepared) return error.EmbeddingStageBusy;
        if (self.cb.kind() == .cuda) {
            if (comptime build_options.enable_cuda) {
                _ = try self.cudaStageCompute();
                var copy = cuda_compute_mod.CudaCompute.StageCopyStats{};
                const result = try cuda_compute_mod.CudaCompute.escapeEmbeddingStage(self.cb, tensor, if (self.timings != null) &copy else null);
                if (self.timings) |counters| {
                    counters.stage_copy_bytes += copy.bytes;
                    counters.stage_copy_ns += copy.nanos;
                }
                return result;
            } else return error.CudaNotEnabled;
        }
        _ = try self.current().operationFor(self.cb);
        var expected: ?*const anyopaque = null;
        if (completed_projection and self.session.vtable == &arch_vtable) if (self.model_retention) |model| {
            if (model.destroy_fn == archClose and model.quiesce_fn == archQuiesce) {
                const actual: *ArchSession = @ptrCast(@alignCast(model.owner));
                if (self.session.ptr == @as(*anyopaque, @ptrCast(actual)) and &actual.model_lifetime == model and actual.backend_type == .metal)
                    expected = @as(*const anyopaque, @ptrCast(gpuBackendData(actual)));
            }
        };
        const Metal = @import("../ops/metal_compute.zig").MetalCompute;
        var copy = Metal.StageCopyStats{};
        const observe = self.timings != null and self.cb.embedding_stage == null;
        const result = try Metal.escapeEmbeddingStageWithCleanup(self.cb, tensor, expected, self.current(), if (observe) &copy else null);
        if (observe) if (self.timings) |counters| {
            counters.stage_copy_bytes += copy.bytes;
            counters.stage_copy_ns += copy.nanos;
        };
        return result;
    }
};

pub const StagedAdmission = struct {
    const RunPermit = @import("../backends/session.zig").RunPermit;
    const Control = InferenceExecutionControl;
    /// Called at a fenced boundary. A relevant denial can retire provider bindings
    /// once, reclaim optional classes in order, and retry the exact supplemental
    /// deficit. Live retained leases remain independently owned throughout.
    pub fn admitSupplementReclaimingCaches(session: Session, base: *const RunPermit, other: ?*const RunPermit, target: @import("../runtime/tier/memory.zig").AdmissionAmounts, control: Control, context: anytype, comptime prepare: anytype, comptime reclaim_media: anytype, comptime reclaim_language: anytype) !RunPermit {
        try control.check();
        var pressure: ?@import("../runtime/tier/memory.zig").AdmissionPressure = null;
        return session.admitSupplementPairWithPressure(base, other, target, &pressure) catch |err| blk: {
            var denied: anyerror = err;
            var prepared = false;
            inline for (.{ reclaim_media, reclaim_language }) |reclaim| {
                const reason = pressure orelse return denied;
                if (!visionCacheCanRelieve(reason)) return denied;
                try control.check();
                if (!prepared) {
                    try prepare(context, control);
                    prepared = true;
                    try control.check();
                    // Retirement itself can return admitted native bindings. Retry
                    // before evicting model caches, and recheck the denial class.
                    pressure = null;
                    if (session.admitSupplementPairWithPressure(base, other, target, &pressure)) |permit| break :blk permit else |failure| denied = failure;
                    if (!visionCacheCanRelieve(pressure orelse return denied)) return denied;
                }
                const bytes = try reclaim(context, control);
                try control.check();
                if (bytes != 0) {
                    pressure = null;
                    if (session.admitSupplementPairWithPressure(base, other, target, &pressure)) |permit| break :blk permit else |failure| denied = failure;
                }
            }
            return denied;
        };
    }

    pub fn visionCacheCanRelieve(pressure: @import("../runtime/tier/memory.zig").AdmissionPressure) bool {
        return switch (pressure) {
            .shared_unified, .external_budget, .live_host => true,
            .domain_backend, .domain_combined => |backend| backend == .gpu,
            else => false,
        };
    }

    fn reclaimMediaWeights(session: Session, control: Control) !usize {
        const vision = try reclaimEmbeddingGemma2VisionWeights(session, control);
        return vision + try reclaimEmbeddingGemma2AudioWeights(session, control);
    }

    pub fn admitWithVisionCacheReclaim(session: Session, request: @import("../backends/session.zig").RunRequest, control: Control) !@import("../backends/session.zig").RunPermit {
        return admitReclaimingVisionCache(session, request, control, session, reclaimMediaWeights, reclaimEmbeddingGemma2LanguageWeights);
    }

    /// Optional caches cannot starve an unchanged request reservation. Prefer
    /// vision reclamation; yield language constants only if pressure persists.
    /// Each class is attempted once, with no waiting or native locks across admit.
    pub fn admitReclaimingVisionCache(session: Session, request: @import("../backends/session.zig").RunRequest, control: Control, context: anytype, comptime reclaim_vision: anytype, comptime reclaim_language: anytype) !RunPermit {
        return admitReclaimingResources(false, session, request, control, context, reclaim_vision, reclaim_language);
    }
    pub fn admitHostPreprocess(session: Session, bytes: usize, control: Control) !RunPermit {
        return admitReclaimingResources(true, session, bytes, control, session, reclaimMediaWeights, reclaimEmbeddingGemma2LanguageWeights);
    }
    pub fn admitReclaimingResources(comptime host_only: bool, session: Session, request: if (host_only) usize else @import("../backends/session.zig").RunRequest, control: Control, context: anytype, comptime reclaim_vision: anytype, comptime reclaim_language: anytype) !RunPermit {
        try control.check();
        var pressure: ?@import("../runtime/tier/memory.zig").AdmissionPressure = null;
        return (if (host_only) session.admitHostPreprocessWithPressure(request, &pressure) else session.admitWithPressure(request, &pressure)) catch |err| blk: {
            var denied: anyerror = err;
            inline for (.{ reclaim_vision, reclaim_language }) |reclaim| {
                const reason = pressure orelse return denied;
                const native_pressure = session.backend() == .native and switch (reason) {
                    .shared_host => true,
                    .domain_host, .domain_combined => |backend| backend == .cpu,
                    else => false,
                };
                if (!visionCacheCanRelieve(reason) and !native_pressure) return denied;
                try control.check();
                const bytes = try reclaim(context, control);
                try control.check();
                if (bytes != 0) {
                    pressure = null;
                    if (if (host_only) session.admitHostPreprocessWithPressure(request, &pressure) else session.admitWithPressure(request, &pressure)) |permit| {
                        break :blk permit;
                    } else |failure| denied = failure;
                }
            }
            return denied;
        };
    }
};

const StagedNativeOwnerFixture = struct {
    weights: native_mod.WeightStore,
    creates: usize = 0,
    cancelled: bool = false,
    fn acquire(raw: *anyopaque, _: Session, allocator: std.mem.Allocator, options: @import("../backends/session.zig").ComputeBackendOptions) !ops.ComputeBackend {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.creates += 1;
        return @import("../backends/compute_factory.zig").createNative(allocator, &self.weights, options.run_budget, null, .{});
    }
    fn host(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
        return error.UnexpectedTaskHostForward;
    }
    fn info(_: *anyopaque) []const TensorInfo {
        return &.{};
    }
    fn kind(_: *anyopaque) BackendType {
        return .native;
    }
    fn close(_: *anyopaque) void {}
    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.cancelled) return error.Cancelled;
    }
    const table = Session.VTable{ .backend = kind, .run = host, .inputInfo = info, .outputInfo = info, .close = close };
    const provider = @import("../backends/session.zig").ComputeBackendProvider.VTable{ .acquire = acquire };
};

fn stagedNativeOwnerAllocationCase(injected: std.mem.Allocator) !void {
    var fixture = StagedNativeOwnerFixture{ .weights = .{ .allocator = std.testing.allocator, .resident_weights = .{}, .lazy_weights = .{} } };
    defer fixture.weights.deinitOwned();
    var gate: std.atomic.Mutex = .unlocked;
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    const session = Session{ .ptr = &fixture, .vtable = &StagedNativeOwnerFixture.table, .compute_backend_provider = .{ .ptr = &fixture, .services = &StagedNativeOwnerFixture.provider }, .execution_gate = &gate, .cohort_scheduler = &scheduler };
    var permit = try session.admit(.{});
    defer permit.deinit();
    {
        var foreign_gate: std.atomic.Mutex = .unlocked;
        var wrong_waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
        var wrong = StagedExecution{ .session = session, .gate = &foreign_gate, .waiter = &wrong_waiter, .cb = undefined, .control = .{}, .permit = &permit, .idle_backend = 0 };
        defer wrong.deinit();
        try std.testing.expectError(error.ExecutionCohortGateChanged, wrong.prepareBeforeExecution());
        try std.testing.expectEqual(@as(usize, 0), fixture.creates);
    }
    {
        var foreign: u8 = 0;
        var wrong_permit = @import("../backends/session.zig").RunPermit{ .session = session, .lease = null };
        wrong_permit.session.ptr = &foreign;
        var wrong_waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
        var wrong = StagedExecution{ .session = session, .gate = &gate, .waiter = &wrong_waiter, .cb = undefined, .control = .{}, .permit = &wrong_permit, .idle_backend = 0 };
        defer wrong.deinit();
        try std.testing.expectError(error.InvalidStageAdmissionOwner, wrong.prepareBeforeExecution());
        try std.testing.expectEqual(@as(usize, 0), fixture.creates);
    }
    var waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
    var stage = StagedExecution{ .session = session, .gate = null, .waiter = &waiter, .cb = undefined, .control = .{ .ptr = &fixture, .check_fn = StagedNativeOwnerFixture.check, .native_call_policy = .in_process }, .permit = &permit, .idle_backend = 0 };
    defer {
        stage.deinit();
        std.testing.expect(scheduler.head == null) catch @panic("stage FIFO leak");
        std.testing.expect(gate.tryLock()) catch @panic("stage model gate leak");
        gate.unlock();
    }
    try stage.prepareBeforeExecution();
    try std.testing.expect(stage.gate == &gate and stage.epoch() == null);
    fixture.cancelled = true;
    try std.testing.expectError(error.Cancelled, stage.enterInitial(injected, stage.control));
    try std.testing.expectEqual(@as(usize, 0), fixture.creates);
    fixture.cancelled = false;
    try stage.enterInitial(injected, stage.control);
    try std.testing.expect(waiter.acquired and scheduler.head == &waiter);
    const backend = stage.backend();
    const input = try backend.fromFloat32Shape(&.{ 1, -2, 3, 4 }, &.{ 2, 2 });
    defer backend.free(input);
    const result = try backend.add(input, input);
    defer backend.free(result);
    const host = try backend.toFloat32(result, injected);
    defer injected.free(host);
    try std.testing.expectEqualSlices(f32, &.{ 2, -4, 6, 8 }, host);
    try std.testing.expectEqual(@as(usize, 1), fixture.creates);
}

test "staged runtime owns actual Native provider numerical lifetime cancellation and every allocation failure" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, stagedNativeOwnerAllocationCase, .{});
}

test "ended stage phase tickets move exact leases without callbacks until an outside-lane drain" {
    // Pure admission protocol fixture; actual Metal/provider completion is
    // covered by the controlled provider/public model qualification separately.
    const admission = @import("../runtime/tier/memory.zig");
    const Probe = struct {
        gate: *std.atomic.Mutex,
        reserves: usize = 0,
        releases: usize = 0,
        failure: bool = false,
        fn observe(self: *@This()) void {
            if (!self.gate.tryLock()) self.failure = true else self.gate.unlock();
        }
        fn reserve(raw: *anyopaque, _: admission.AdmissionAmounts) admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.reserves += 1;
            return self.reserves;
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.releases += 1;
        }
        fn retain(raw: *anyopaque, _: usize, _: admission.AdmissionAmounts) admission.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn host(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
            return error.UnexpectedNumericalWork;
        }
        fn info(_: *anyopaque) []const TensorInfo {
            return &.{};
        }
        fn kind(_: *anyopaque) BackendType {
            return .native;
        }
        fn close(_: *anyopaque) void {}
        const table = Session.VTable{ .backend = kind, .run = host, .inputInfo = info, .outputInfo = info, .close = close };
    };
    var gate: std.atomic.Mutex = .unlocked;
    var probe = Probe{ .gate = &gate };
    var controller = admission.AdmissionController{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    const session = Session{ .ptr = &probe, .vtable = &Probe.table, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1024, .backend_limit_bytes = 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var base = try session.admitHostPreprocess(16);
    defer base.deinit();
    var stage = StagedExecution{ .session = session, .gate = null, .waiter = undefined, .cb = undefined, .control = .{}, .permit = &base, .idle_backend = 0, .cold_prepared = true };
    defer stage.deinit();
    var first = StagedExecution.Phase{ .owner = &stage, .owned = try session.admitSupplement(&base, .{ .host_scratch_bytes = 48 }) };
    defer first.deinit();
    var second = StagedExecution.Phase{ .owner = &stage, .owned = try session.admitSupplement(&base, .{ .host_scratch_bytes = 80 }) };
    defer second.deinit();
    try std.testing.expectEqual(@as(usize, 112), controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 3), probe.reserves);
    var held = false;
    defer if (held) gate.unlock();
    try std.testing.expect(gate.tryLock());
    held = true;
    first.deinit();
    second.deinit();
    first.deinit();
    try std.testing.expectEqual(@as(usize, 0), probe.releases);
    try std.testing.expectEqual(@as(usize, 112), controller.snapshot().host_scratch_bytes);
    try std.testing.expect(stage.pending_phases[0] != null and stage.pending_phases[1] != null);
    gate.unlock();
    held = false;
    stage.deinit();
    try std.testing.expectEqual(@as(usize, 2), probe.releases);
    try std.testing.expectEqual(@as(usize, 16), controller.snapshot().host_scratch_bytes);
    base.deinit();
    try std.testing.expectEqual(@as(usize, 3), probe.releases);
    try std.testing.expect(!probe.failure);
    try std.testing.expectEqualDeep(admission.AdmissionAmounts{}, controller.snapshot());
}

test "actual private Native stage quota callbacks can reenter the same model while original values stay owned" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const admission = @import("../runtime/tier/memory.zig");
    const a = std.testing.allocator;
    var fixture = StagedNativeOwnerFixture{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer fixture.weights.deinitOwned();
    var gate: std.atomic.Mutex = .unlocked;
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    var session = Session{ .ptr = &fixture, .vtable = &StagedNativeOwnerFixture.table, .compute_backend_provider = .{ .ptr = &fixture, .services = &StagedNativeOwnerFixture.provider }, .execution_gate = &gate, .cohort_scheduler = &scheduler };
    const Probe = struct {
        session: *Session,
        gate: *std.atomic.Mutex,
        next: usize = 1,
        live: usize = 0,
        reentries: usize = 0,
        failure: ?anyerror = null,
        fn reenter(self: *@This()) !void {
            if (!self.gate.tryLock()) return error.StageSupplierUnderActualModelGate;
            self.gate.unlock();
            var waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
            try self.session.enterExecutionCohort(self.gate, .{}, &waiter);
            defer self.session.leaveExecutionCohort(self.gate, &waiter);
            var backend = try self.session.acquireComputeBackend(std.testing.allocator, .{});
            defer backend.deinit();
            const input = try backend.fromFloat32(&.{ 2, 5 });
            defer backend.free(input);
            const doubled = try backend.add(input, input);
            defer backend.free(doubled);
            const values = try backend.toFloat32(doubled, std.testing.allocator);
            defer std.testing.allocator.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 4, 10 }, values);
            self.reentries += 1;
        }
        fn observe(self: *@This()) void {
            self.reenter() catch |err| {
                self.failure = err;
            };
        }
        fn reserve(raw: *anyopaque, _: admission.AdmissionAmounts) admission.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            const token = self.next;
            self.next += 1;
            self.live += 1;
            return token;
        }
        fn retain(raw: *anyopaque, _: usize, _: admission.AdmissionAmounts) admission.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.live -= 1;
        }
    };
    var probe = Probe{ .session = &session, .gate = &gate };
    var controller = admission.AdmissionController{};
    defer controller.deinit();
    session.run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1024, .backend_limit_bytes = 1024 }, .static_workspace_bytes = 0, .check_live_memory = false };
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var base = try session.admitHostPreprocess(16);
    defer base.deinit();
    var waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
    var stage = StagedExecution{ .session = session, .gate = &gate, .waiter = &waiter, .cb = undefined, .control = .{ .native_call_policy = .in_process }, .permit = &base, .idle_backend = 0 };
    defer stage.deinit();
    try stage.prepareBeforeExecution();
    try stage.enterInitial(a, stage.control);
    {
        const backend = stage.backend();
        const original = backend.vtable;
        const copied = original.*;
        backend.vtable = &copied;
        defer backend.vtable = original;
        try std.testing.expectError(error.UnsupportedStageProvider, stage.admitPhaseScoped(.{ .host_scratch_bytes = 48 }, null));
        try std.testing.expect(waiter.acquired and scheduler.head == &waiter);
        try std.testing.expectEqual(@as(usize, 1), probe.live);
    }
    {
        const backend = stage.backend();
        const compute: *NativeCompute = @ptrCast(@alignCast(backend.ptr));
        var shared = @import("operation_owner.zig").Owner{};
        compute.shared_operation_owner = &shared;
        defer compute.shared_operation_owner = null;
        var scope: @import("backend_operation.zig").Scope = undefined;
        try @import("backend_operation.zig").enterInto(backend, null, null, .managed, &scope);
        defer scope.deinitChecked() catch @panic("native stage held scope cleanup");
        try std.testing.expect(scope.token() != null);
        try std.testing.expectError(error.BackendCleanupAuthorityRequired, stage.admitPhaseScoped(.{ .host_scratch_bytes = 48 }, null));
        try std.testing.expect(waiter.acquired and scheduler.head == &waiter);
        try std.testing.expectEqual(@as(usize, 1), probe.live);
        try scope.finish();
    }
    const input = try stage.backend().fromFloat32Shape(&.{ 1, -2 }, &.{ 1, 2 });
    var input_live = true;
    defer if (input_live) stage.backend().free(input);
    var first = try stage.admitPhaseScoped(.{ .host_scratch_bytes = 48 }, null);
    defer first.deinit();
    first.deinit();
    try std.testing.expectEqual(@as(usize, 48), controller.snapshot().host_scratch_bytes);
    var second = try stage.admitPhaseScoped(.{ .host_scratch_bytes = 80 }, null);
    defer second.deinit();
    const values = try stage.backend().toFloat32(input, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 1, -2 }, values);
    stage.backend().free(input);
    input_live = false;
    second.deinit();
    stage.deinit();
    base.deinit();
    if (probe.failure) |err| return err;
    try std.testing.expect(probe.reentries >= 6);
    try std.testing.expectEqual(@as(usize, 0), probe.live);
    try std.testing.expectEqualDeep(admission.AdmissionAmounts{}, controller.snapshot());
    try std.testing.expect(scheduler.head == null);
}

test "actual Metal staged fresh output keeps physical identity and quota through provider handoff" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    var session = try incrementalMetalFixtureSession(a);
    defer session.close();
    try session.installOwnedExecutionGate(a);
    const domain = try Domain.create();
    defer domain.release();
    session.model_lifetime.?.admission_domain = domain.pin();
    session.run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = 64 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false };
    var base = try session.admit(.{ .workspace_bytes = 8192, .workspace_planned = true, .backend_input_bytes = 0, .output_bytes = 4096 });
    var base_open = true;
    defer if (base_open) base.deinit();
    var waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
    var counters = StagedExecution.Counters{};
    var stage = StagedExecution{ .session = session, .gate = session.execution_gate, .waiter = &waiter, .cb = undefined, .control = .{ .native_call_policy = .in_process }, .permit = &base, .idle_backend = 8192, .timings = &counters };
    var stage_open = true;
    defer if (stage_open) stage.deinit();
    try stage.prepareBeforeExecution();
    try stage.enterInitial(a, .{ .native_call_policy = .in_process });
    const cb = stage.backend();
    const data: [1024]f32 = @splat(0.25);
    const source = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&data, &.{ 16, 64 }));
    var source_open = true;
    defer if (source_open) cb.free(source);
    const fresh = (try cb.allocUninitF32Shape(&.{ 16, 64 })) orelse return error.MissingFreshProjectionDestination;
    var fresh_open = true;
    defer if (fresh_open) cb.free(fresh);
    try std.testing.expect(try cb.copyRows2D(a, fresh, 0, source, 0, 16, 64));
    // This is the exact fresh-destination producer contract: one allocation,
    // every output row filled, no future writer retained after publication.
    var projected = @import("../architectures/gemma4_projector.zig").ResidentProjectedImages{
        .allocator = a,
        .backend = cb,
        .embeddings = fresh,
        .tokens_per_image = try a.dupe(usize, &.{16}),
        .hidden_size = 64,
        .batches_executed = 1,
        .max_batch_executed = 1,
        .output_ownership = .completed_fresh_destination,
    };
    fresh_open = false;
    var projected_open = true;
    defer if (projected_open) projected.deinit();
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    const before_cancel = projected.embeddings;
    stage.control.check_fn = Cancel.check;
    try std.testing.expectError(error.Cancelled, stage.retainProjectedOutput(&projected));
    stage.control.check_fn = null;
    try std.testing.expect(projected.embeddings == before_cancel);
    const copied = try stage.escape(source); // Unknown snapshot stays a copy.
    var copied_open = true;
    defer if (copied_open) cb.free(copied);
    const source_identity = try DetachedDenseBackingProbe.read(cb, source);
    const copied_identity = try DetachedDenseBackingProbe.read(cb, copied);
    try std.testing.expect(source_identity.base != copied_identity.base);
    const copied_bytes = counters.stage_copy_bytes;
    try std.testing.expectEqual(@as(usize, 4096), copied_bytes);
    const original = try DetachedDenseBackingProbe.read(cb, projected.embeddings);
    var audit = @import("transfer_audit.zig").Audit{ .forbid_device_host_access = true };
    try @import("transfer_audit.zig").begin(&audit);
    var audit_open = true;
    defer if (audit_open) @import("transfer_audit.zig").end();
    try stage.retainProjectedOutput(&projected);
    const retained = try DetachedDenseBackingProbe.read(cb, projected.embeddings);
    @import("transfer_audit.zig").end();
    audit_open = false;
    try std.testing.expectEqual(original.base, retained.base);
    try std.testing.expect(original.context == retained.context);
    try std.testing.expectEqual(@as(u64, 0), audit.device_copy_calls);
    try std.testing.expectEqual(@as(u64, 0), audit.download_calls);
    try std.testing.expectEqual(copied_bytes, counters.stage_copy_bytes);
    const funded = domain.controller.snapshot().backendTotalBytes();
    try std.testing.expect(funded >= 4096 and funded <= 64 * 1024);
    try stage.reserve(4096); // Actual fenced provider retirement and resume.
    const after = try DetachedDenseBackingProbe.read(cb, projected.embeddings);
    try std.testing.expectEqual(original.base, after.base);
    const result = try cb.add(projected.embeddings, projected.embeddings);
    var result_open = true;
    defer if (result_open) cb.free(result);
    const host = try cb.toFloat32(result, a);
    defer a.free(host);
    for (host) |value| try std.testing.expectEqual(@as(f32, 0.5), value);
    try std.testing.expect(domain.controller.snapshot().backendTotalBytes() >= 4096);
    projected.deinit();
    projected_open = false;
    cb.free(result);
    result_open = false;
    cb.free(source);
    source_open = false;
    cb.free(copied);
    copied_open = false;
    stage.deinit();
    stage_open = false;
    base.deinit();
    base_open = false;
    _ = @import("resource_retirement.zig").pollRawController(&domain.controller);
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "actual CUDA staged quota boundaries let same model reenter while owned snapshots survive" {
    if (comptime !build_options.enable_cuda) return error.SkipZigTest;
    const Case = struct {
        fn run(scheduling: bool) !void {
            const a = std.testing.allocator;
            const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
            const memory = runtime.tier.memory;
            const compute = try a.create(cuda_compute_mod.CudaCompute);
            var compute_open = false;
            defer if (compute_open) {
                compute.deinit();
                a.destroy(compute);
            };
            compute.* = cuda_compute_mod.CudaCompute.initWithKernelJitForProfile(a, .{}, .embedding_gemma2) catch |err| {
                a.destroy(compute);
                switch (err) {
                    error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
                    else => return err,
                }
            };
            compute_open = true;
            // This is an ownership fixture, not a model numerical oracle: no encoder
            // uses these zero geometry facts. Numerical callbacks are the real CUDA
            // elementwise programs on the exact model-owned context and table.
            const owner = try a.create(ArchSession);
            owner.* = .{ .allocator = a, .arch_config = .{ .embedding_gemma2 = std.mem.zeroes(embedding_gemma2_arch.Config) }, .backend_type = .cuda, .backend_data = .{ .cuda = .{ .compute = compute } } };
            owner.model_lifetime.prepareFinalResources() catch |err| {
                a.destroy(owner);
                return err;
            };
            var session = archSessionHandle(owner);
            compute_open = false;
            defer session.close();
            try session.installOwnedExecutionGate(a);
            var cache_scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
            session.cohort_scheduler = &cache_scheduler;
            const domain = try Domain.create();
            defer domain.release();
            session.model_lifetime.?.admission_domain = domain.pin();
            session.run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = 64 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false };
            const Probe = struct {
                session: Session,
                calls: usize = 0,
                failure: ?anyerror = null,
                change_policy: bool = false,
                deny_first: bool = false,
                reserve_calls: usize = 0,
                changing_policy: bool = false,
                failure_during_policy: bool = false,
                failure_callback: ?Callback = null,
                const Callback = enum { reserve, retain, release };
                fn check(self: *@This(), callback: Callback) void {
                    self.calls += 1;
                    self.reenter() catch |err| {
                        if (self.failure == null) {
                            self.failure = err;
                            self.failure_during_policy = self.changing_policy;
                            self.failure_callback = callback;
                        }
                    };
                }
                fn reenter(self: *@This()) !void {
                    const gate = self.session.execution_gate.?;
                    if (!gate.tryLock()) return error.SourceModelStillOwnedDuringSupplier;
                    defer gate.unlock();
                    var managed = try getComputeBackendWithControl(self.session, std.testing.allocator, .{ .native_call_policy = .in_process });
                    defer managed.deinit();
                    const cb = &managed.backend;
                    if (self.change_policy) {
                        self.change_policy = false;
                        var peer = self.session;
                        peer.run_admission.?.check_live_memory = !peer.run_admission.?.check_live_memory;
                        // Actual published request entry replaces the context policy;
                        // no borrowed test callback or raw field mutation fakes it.
                        self.changing_policy = true;
                        defer self.changing_policy = false;
                        try @import("../backends/compute_factory.zig").beginRequest(peer, cb);
                    }
                    const input = try cb.fromFloat32(&.{ 2, 5 });
                    defer cb.free(input);
                    const result = try cb.add(input, input);
                    defer cb.free(result);
                    const host = try cb.toFloat32(result, std.testing.allocator);
                    defer std.testing.allocator.free(host);
                    try std.testing.expectEqualSlices(f32, &.{ 4, 10 }, host);
                }
                fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.reserve_calls += 1;
                    self.check(.reserve);
                    if (self.deny_first) {
                        self.deny_first = false;
                        return error.ResourceTemporarilyUnavailable;
                    }
                    return 1;
                }
                fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.check(.retain);
                }
                fn release(raw: *anyopaque, _: usize) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.check(.release);
                }
            };
            var probe = Probe{ .session = session };
            try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
            var base = @import("../backends/session.zig").RunPermit{ .session = session, .lease = null };
            var base_open = true;
            defer if (base_open) base.deinit();
            var waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
            var stage = StagedExecution{ .session = session, .gate = session.execution_gate, .waiter = &waiter, .cb = undefined, .control = .{ .native_call_policy = .in_process }, .permit = &base, .idle_backend = 8192, .active = false, .scheduling_enabled = scheduling };
            var stage_open = true;
            defer if (stage_open) stage.deinit();
            try stage.prepareBeforeExecution();
            const seed_backend = compute.computeBackend();
            var held_constant: ?ops.CT = null;
            defer if (held_constant) |value| seed_backend.freeWithCleanup(value, .{ .boundary = stage.epoch() });
            // Real cache construction is cold, supervised, and outside BOTH
            // actual lanes. Supplier callbacks reenter the same loaded owner.
            {
                var guard = try stage.control.enterUninterruptible(session.interruption());
                defer guard.deinit();
                compute.temp_slot_config = .{};
                try compute.configureEmbeddingGemma2Admission(.{ .controller = &domain.controller, .limits = session.run_admission.?.limits, .check_live_memory = false });
                _ = try compute.embedding_gemma2_resize.plan(a, &compute.ctx, .{ .width = 2, .height = 2, .target_width = 2, .target_height = 2, .resident = false });
                const prepared_rows = (try compute.embedding_gemma2_execution.allocatePacked(a, &compute.ctx, "stage-packed", 256)) orelse return error.MissingActualPackedCache;
                const bytes: [32]u8 = @splat(7);
                try compute.embedding_gemma2_resize.uploadSource(&compute.ctx, prepared_rows, &bytes);
                _ = (try compute.embedding_gemma2_execution.arena(&compute.ctx, 1, 1, 0, .{ 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 })) orelse return error.MissingActualExecutionArena;
                const source_value = try seed_backend.fromFloat32(&.{ 1, 2, 3, 4 });
                defer seed_backend.free(source_value);
                held_constant = (try seed_backend.modelConstant("audio_tower.prepared.stage-ownership-fixture", source_value)) orelse return error.MissingActualModelConstant;
            }
            const cache_usage = domain.controller.snapshot();
            try std.testing.expect(cache_usage.backend_scratch_bytes > 16 and cache_usage.host_scratch_bytes > 0);
            const attempts = probe.reserve_calls;
            probe.deny_first = true;
            try stage.admitInitialCuda(a, .{ .workspace_bytes = 8192, .workspace_planned = true, .backend_input_bytes = 0, .output_bytes = 16 }, null);
            try std.testing.expectEqual(attempts + 2, probe.reserve_calls);
            // Ordinary old payloads really refunded BEFORE retry. Only the
            // held physical constant retains its exact old16-byte charge.
            const expected_after_retry = try base.lease.?.amounts.merge(.{ .backend_scratch_bytes = 16 });
            try std.testing.expectEqualDeep(expected_after_retry, domain.controller.snapshot());
            try std.testing.expectEqual(@as(usize, 0), compute.embedding_gemma2_constants.count());
            try std.testing.expectEqual(@as(usize, 0), compute.embedding_gemma2_execution.weights.count());
            try std.testing.expect(stage.managed == null and !stage.active);
            try stage.enterInitial(a, .{ .native_call_policy = .in_process });
            const cb = stage.backend();
            const Cache = @import("../ops/cuda/prepared_cache.zig");
            const cache_requests = [_]Cache.Request{try Cache.Request.packedSpan("stage.prepared.scope", 64)};
            const cache_work: Cache.Workload = .{ .requests = &cache_requests };
            var cold_packed_ptr: u64 = 0;
            {
                var cache_phase: StagedExecution.CachePhase = undefined;
                try cache_phase.initInto(&stage, cache_work);
                defer cache_phase.close();
                try std.testing.expect(cache_phase.cold != null and compute.ctx.prepared_cache_funding == cache_phase.binding());
                const calls = probe.calls;
                const packed_span = (try compute.embedding_gemma2_execution.allocatePacked(a, &compute.ctx, "stage.prepared.scope", 64)) orelse return error.MissingActualPackedCache;
                try std.testing.expect(packed_span.ptr != 0 and probe.calls == calls);
                cold_packed_ptr = packed_span.ptr;
                cache_phase.close();
                try std.testing.expect(compute.ctx.prepared_cache_funding == null and stage.cache_pending != null);
                // Metadata close cannot invoke a supplier under this real Model.
                try std.testing.expectEqual(calls, probe.calls);
            }
            try StagedExecution.pause(&stage);
            try std.testing.expect(stage.cache_pending == null and !stage.active);
            try StagedExecution.acquire(&stage);
            const warm_allocations = compute.ctx.device_allocations.snapshot();
            {
                var warm: StagedExecution.CachePhase = undefined;
                try warm.initInto(&stage, cache_work);
                defer warm.close();
                try std.testing.expect(warm.cold == null and compute.ctx.prepared_cache_funding == &warm.warm);
                const calls = probe.calls;
                // Match the actual numerical producer: reuse an existing key
                // before invoking the cold-only allocating admission path.
                const packed_span = compute.embedding_gemma2_execution.findPacked("stage.prepared.scope") orelse
                    (try compute.embedding_gemma2_execution.allocatePacked(a, &compute.ctx, "stage.prepared.scope", 64)) orelse return error.MissingActualPackedCache;
                try std.testing.expectEqual(cold_packed_ptr, packed_span.ptr);
                try std.testing.expectEqual(calls, probe.calls);
                warm.close();
                try std.testing.expectEqualDeep(warm_allocations, compute.ctx.device_allocations.snapshot());
            }
            // Only optional cache quota denial declines to the existing
            // numerical compatibility route. No cold SDK allocation follows.
            const declined_requests = [_]Cache.Request{try Cache.Request.packedSpan("stage.optional.declined", 64)};
            probe.deny_first = true;
            {
                var declined: StagedExecution.CachePhase = undefined;
                try declined.initInto(&stage, .{ .requests = &declined_requests });
                defer declined.close();
                try std.testing.expect(declined.cold == null and declined.warm.deny_cold_misses);
                const before_cache = compute.ctx.device_allocations.snapshot();
                try std.testing.expect((try compute.embedding_gemma2_execution.allocatePacked(a, &compute.ctx, "stage.optional.declined", 64)) == null);
                try std.testing.expectEqualDeep(before_cache, compute.ctx.device_allocations.snapshot());
            }
            const source = try cb.fromFloat32(&.{ 1, -2, 3, 4 });
            var source_open = true;
            defer if (source_open) cb.free(source);
            const snapshot = try stage.escape(source);
            var snapshot_open = true;
            defer if (snapshot_open) cb.free(snapshot);
            // Only the actual independent snapshot crosses the peer boundary.
            cb.free(source);
            source_open = false;
            const before = probe.calls;
            var phase = try stage.admitPhaseScoped(.{ .backend_scratch_bytes = base.lease.?.amounts.backend_scratch_bytes + 4096 }, null);
            defer phase.deinit();
            try std.testing.expect(probe.calls > before);
            try std.testing.expect(stage.active);
            try std.testing.expect((cb.embedding_stage != null) == scheduling);
            const held = probe.calls;
            phase.deinit();
            try std.testing.expectEqual(held, probe.calls); // Ticket cannot refund in lane.
            try stage.reserve(0); // Real checked stream handoff, then outside refund.
            try std.testing.expect(probe.calls > held);
            const host = try cb.toFloat32(snapshot, a);
            defer a.free(host);
            try std.testing.expectEqualSlices(f32, &.{ 1, -2, 3, 4 }, host);
            const Cancel = struct {
                fn check(_: ?*anyopaque) !void {
                    return error.Cancelled;
                }
            };
            stage.control.check_fn = Cancel.check;
            try std.testing.expectError(error.Cancelled, stage.admitPhaseScoped(.{ .backend_scratch_bytes = 16384 }, null));
            stage.control.check_fn = null;
            try std.testing.expect(stage.active);
            probe.change_policy = true;
            var restored = try stage.admitPhaseScoped(.{ .backend_scratch_bytes = 16384 }, null);
            defer restored.deinit();
            try std.testing.expect(stage.active);
            try std.testing.expect(StagedExecution.CudaPolicy.same(stage.cuda_policy, StagedExecution.CudaPolicy.from(compute.ctx.video_cache_budget.admission)));
            const doubled = try cb.add(snapshot, snapshot);
            var doubled_live = true;
            defer if (doubled_live) cb.free(doubled);
            const resumed = try cb.toFloat32(doubled, a);
            defer a.free(resumed);
            try std.testing.expectEqualSlices(f32, &.{ 2, -4, 6, 8 }, resumed);
            cb.free(doubled);
            doubled_live = false;
            restored.deinit();
            if (!scheduling) {
                try StagedExecution.pause(&stage);
                stage.control.check_fn = Cancel.check;
                try std.testing.expectError(error.Cancelled, StagedExecution.acquire(&stage));
                stage.control.check_fn = null;
                try std.testing.expect(stage.active and stage.waiter.acquired);
                try std.testing.expect(cb.embedding_stage == null);
                // A REAL detached carrier owns the pending SDK interval. Failed
                // reentry must release the actual model turn, not just a flag.
                const required = try cuda_compute_mod.CudaCompute.cacheRetirementRequirements(cb, compute, .all);
                try StagedExecution.pause(&stage);
                var pin = if (required.old_controller) |controller| session.model_lifetime.?.retainAdmissionDomain(controller) else null;
                defer if (pin) |value| value.release();
                const carrier = try cuda_compute_mod.CudaCompute.CacheRetirement.prepare(required, session.model_lifetime.?, if (pin) |*value| value else null);
                var detached = false;
                var finished = false;
                defer if (!finished) {
                    if (stage.active) stage.leaveCudaModel();
                    if (detached) {
                        carrier.retireOutsideExecution(stage.epoch()) catch platform.inference_process_supervisor.restartWorker();
                        carrier.destroy();
                    } else carrier.deinitUnused();
                    StagedExecution.cleanup(&stage);
                };
                try stage.enter(stage.control, false);
                try carrier.detachFenced(cb, compute);
                detached = true;
                stage.leaveCudaModel();
                const before_pending = compute.ctx.device_allocations.snapshot();
                try std.testing.expectError(error.QueueFull, stage.enter(stage.control, false));
                try std.testing.expect(!stage.active and !stage.waiter.acquired);
                try std.testing.expect(session.execution_gate.?.tryLock());
                session.execution_gate.?.unlock();
                try std.testing.expectError(error.QueueFull, @import("../backends/compute_factory.zig").acquireCuda(compute, null));
                try std.testing.expectError(error.QueueFull, @import("../backends/compute_factory.zig").beginRequest(session, cb));
                try std.testing.expectEqualDeep(before_pending, compute.ctx.device_allocations.snapshot());
                carrier.retireOutsideExecution(stage.epoch()) catch platform.inference_process_supervisor.restartWorker();
                carrier.destroy();
                finished = true;
                StagedExecution.cleanup(&stage);
                try std.testing.expect(stage.active);
            }
            cb.free(snapshot);
            snapshot_open = false;
            cb.freeWithCleanup(held_constant.?, .{ .boundary = stage.epoch() });
            held_constant = null;
            // Final SDK death has occurred; exact quota remains funded until
            // THIS final consumer B seals, rather than any creation epoch.
            try std.testing.expect(domain.controller.snapshot().backend_scratch_bytes >= 16);
            stage.deinit();
            stage_open = false;
            base.deinit();
            base_open = false;
            _ = @import("resource_retirement.zig").pollRawController(&domain.controller);
            if (probe.failure) |err| {
                std.debug.print("CUDA_STAGE_SUPPLIER_FAILURE callback={s} peer_policy_change={any} scheduling={any} error={s}\n", .{ @tagName(probe.failure_callback.?), probe.failure_during_policy, scheduling, @errorName(err) });
                return err;
            }
            try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
        }
    };
    try Case.run(true);
    try Case.run(false);
}

test "CUDA staged policy equality uses quota identity and semantic limits not descriptor addresses" {
    const P = StagedExecution.CudaPolicy;
    const controller: *runtime.tier.memory.AdmissionController = @ptrFromInt(16);
    const original = P.from(.{ .controller = controller, .limits = .{ .host_limit_bytes = 1, .backend_limit_bytes = 2, .combined_limit_bytes = 3, .kv_limit_bytes = 4, .scratch_limit_bytes = 5 }, .check_live_memory = false });
    const same_value = P.from(.{ .controller = controller, .limits = .{ .host_limit_bytes = 1, .backend_limit_bytes = 2, .combined_limit_bytes = 3, .kv_limit_bytes = 4, .scratch_limit_bytes = 5 }, .check_live_memory = false });
    try std.testing.expect(P.same(original, same_value));
    try std.testing.expect(P.same(P.from(null), P.from(null)));
    try std.testing.expect(!P.same(original, P.from(null)));
    var changed = original;
    changed.controller = @ptrFromInt(32);
    try std.testing.expect(!P.same(original, changed));
    inline for (.{ "host_limit_bytes", "backend_limit_bytes", "combined_limit_bytes", "kv_limit_bytes", "scratch_limit_bytes" }) |field| {
        changed = original;
        @field(changed.limits, field) += 1;
        try std.testing.expect(!P.same(original, changed));
    }
    changed = original;
    changed.check_live_memory = true;
    try std.testing.expect(!P.same(original, changed));
}

test "actual Metal nested stage retain preserves model before provider ordering for queued peer" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const memory = runtime.tier.memory;
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    var session = try incrementalMetalFixtureSession(a);
    defer session.close();
    const stage_owner: *ArchSession = @ptrCast(@alignCast(session.ptr));
    stage_owner.arch_config = .{ .embedding_gemma2 = std.mem.zeroes(@import("../architectures/embedding_gemma2_config.zig").Config) };
    try session.installOwnedExecutionGate(a);
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    session.cohort_scheduler = &scheduler;
    const domain = try Domain.create();
    defer domain.release();
    session.model_lifetime.?.admission_domain = domain.pin();
    session.run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = 64 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false };
    const control = InferenceExecutionControl{ .deadline_ns = platform.time.monotonicNs() + 10 * std.time.ns_per_s, .native_call_policy = .in_process };
    const Probe = struct {
        session: Session,
        control: InferenceExecutionControl,
        stage: ?*StagedExecution = null,
        armed: bool = false,
        next: usize = 1,
        failure: ?anyerror = null,
        peer_error: ?anyerror = null,
        peer_held: std.atomic.Value(bool) = .init(false),
        peer_finished: std.atomic.Value(bool) = .init(false),
        release_peer: std.atomic.Value(bool) = .init(false),
        fn peer(self: *@This()) void {
            defer self.peer_finished.store(true, .release);
            self.peerBody() catch |err| {
                self.peer_error = err;
            };
        }
        fn peerBody(self: *@This()) !void {
            var waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
            try self.session.selectReadyCohort(self.session.execution_gate, self.control, &waiter);
            defer self.session.leaveSelectedCohort(&waiter);
            try self.session.acquireSelectedCohort(self.control, &waiter);
            self.peer_held.store(true, .release);
            while (!self.release_peer.load(.acquire)) {
                try self.control.check();
                platform.time.yieldNow();
            }
            // A legitimate peer owns model before acquiring the actual provider.
            var compute = try getComputeBackendWithControl(self.session, std.testing.allocator, self.control);
            defer compute.deinit();
            const input = try compute.backend.fromFloat32(&.{ 2, 5 });
            defer compute.backend.free(input);
            const result = try compute.backend.add(input, input);
            defer compute.backend.free(result);
            const values = try compute.backend.toFloat32(result, std.testing.allocator);
            defer std.testing.allocator.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 4, 10 }, values);
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const token = self.next;
            self.next += 1;
            if (self.armed) while (!self.peer_held.load(.acquire) and !self.peer_finished.load(.acquire)) {
                self.control.check() catch |err| {
                    self.failure = err;
                    self.release_peer.store(true, .release);
                    break;
                };
                platform.time.yieldNow();
            };
            return token;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.armed) return;
            self.armed = false;
            defer self.release_peer.store(true, .release);
            if (self.stage.?.active) {
                self.failure = error.ActualStageActiveDuringRetain;
                return;
            }
            if (self.stage.?.waiter.acquired) {
                self.failure = error.ActualStageModelHeldDuringRetain;
                return;
            }
            // A retained lease can invoke its callback before a new reserve.
            // Establish the intended finite peer interleaving after proving
            // the source released its actual model lane.
            while (!self.peer_held.load(.acquire) and !self.peer_finished.load(.acquire)) {
                self.control.check() catch |err| {
                    self.failure = err;
                    return;
                };
                platform.time.yieldNow();
            }
            if (!self.peer_held.load(.acquire)) {
                self.failure = error.ActualStagePeerNotHeldDuringRetain;
                return;
            }
            const impl: *ArchSession = @ptrCast(@alignCast(self.session.ptr));
            const store = gpuBackendData(impl);
            if (!store.shared_metal_native_provider_lock.tryLock()) {
                self.failure = error.ProviderReacquiredBeforeModelDuringSupplier;
                return;
            }
            store.shared_metal_native_provider_lock.unlock(std.testing.io);
        }
        fn release(_: *anyopaque, _: usize) void {}
    };
    var probe = Probe{ .session = session, .control = control };
    try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var base = try session.admit(.{ .workspace_bytes = 8192, .workspace_planned = true, .backend_input_bytes = 0, .output_bytes = 16 });
    var base_open = true;
    defer if (base_open) base.deinit();
    var waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
    var stage = StagedExecution{ .session = session, .gate = session.execution_gate, .waiter = &waiter, .cb = undefined, .control = control, .permit = &base, .idle_backend = 8192 };
    var stage_open = true;
    defer if (stage_open) stage.deinit();
    try stage.prepareBeforeExecution();
    try stage.enterInitial(a, control);
    probe.stage = &stage;
    const thread = try std.Thread.spawn(.{}, Probe.peer, .{&probe});
    var joined = false;
    defer if (!joined) {
        probe.release_peer.store(true, .release);
        thread.join();
    };
    while (!@import("../runtime/embedding_scheduler.zig").Scheduler.hasPeer(&waiter)) {
        try control.check();
        platform.time.yieldNow();
    }
    probe.armed = true;
    try stage.retain(.{ .workspace_bytes = 16384, .workspace_planned = true, .backend_input_bytes = 0, .output_bytes = 16 });
    probe.release_peer.store(true, .release);
    thread.join();
    joined = true;
    if (probe.peer_error) |err| return err;
    if (probe.failure) |err| return err;
    try std.testing.expect(!probe.armed and stage.active);
    stage.deinit();
    stage_open = false;
    base.deinit();
    base_open = false;
    _ = @import("resource_retirement.zig").pollRawController(&domain.controller);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "Gemma 4 CUDA KV defaults preserve full precision across architecture variants" {
    const configs = [_]gpt_mod.Config{
        .{ .family = .gemma, .num_kv_shared_layers = 20 },
        .{ .family = .gemma, .global_head_dim = 512 },
        .{ .family = .gemma, .ple_hidden_size = 256 },
        .{ .family = .gemma, .gemma4_mtp_assistant = true },
        .{ .family = .gemma, .attention_k_eq_v = true },
        .{ .family = .gemma, .rope_dim_override = 128 },
    };
    for (configs) |config| {
        try std.testing.expectEqual(runtime.kv.pool.KvDType.f16, recommendedKvDTypeForGptConfig(config, .cuda));
        try std.testing.expectEqual(runtime.kv.pool.KvDType.f32, recommendedKvDTypeForGptConfig(config, .native));
    }
}

test "actual Native media source prepaid lease funds synchronous consumers then surviving host arrays" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const source_mod = @import("../backends/session.zig");
    const Admitted = @import("../backends/admitted_allocator.zig").AdmittedAllocator;
    const memory = runtime.tier.memory;
    const a = std.testing.allocator;
    const Probe = struct {
        gate: *std.atomic.Mutex,
        consumer_phase: bool = false,
        consumer_reserves: usize = 0,
        live: usize = 0,
        callbacks: usize = 0,
        failure: bool = false,
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callbacks += 1;
            if (!self.gate.tryLock()) self.failure = true else self.gate.unlock();
            if (self.consumer_phase) self.consumer_reserves += 1;
            self.live += 1;
            return self.live;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callbacks += 1;
            if (!self.gate.tryLock()) self.failure = true else self.gate.unlock();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.callbacks += 1;
            if (!self.gate.tryLock()) self.failure = true else self.gate.unlock();
            self.live -= 1;
        }
        fn consume(backend: *const ops.ComputeBackend, value: ops.CT, allocator: std.mem.Allocator) ![]f32 {
            const doubled = try backend.add(value, value);
            defer backend.free(doubled);
            return backend.toFloat32(doubled, allocator);
        }
    };
    for ([_]bool{ false, true }) |clap| {
        var session = try mediaTextEntryFixtureSession(a, clap);
        defer session.close();
        try session.installOwnedExecutionGate(a);
        var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
        session.cohort_scheduler = &scheduler;
        var probe = Probe{ .gate = session.execution_gate.? };
        const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
        defer domain.release();
        session.model_lifetime.?.admission_domain = domain.pin();
        const controller = &domain.controller;
        try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
        session.run_admission = .{ .controller = controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 8 * 1024 * 1024 }, .static_workspace_bytes = 4096, .check_live_memory = false };
        var workspace = Admitted{ .backing = a, .session = session };
        defer workspace.deinit();
        const admitted = workspace.allocator();
        var ids = try Tensor.initInt64(a, "input_ids", &.{ 1, 4 }, &.{ 0, 7, 7, 1 });
        defer ids.deinit();
        var mask = try Tensor.initInt64(a, "attention_mask", &.{ 1, 4 }, &.{ 1, 1, 1, 0 });
        defer mask.deinit();
        const inputs = [_]Tensor{ ids, mask };
        const foreign_session = try mediaTextEntryFixtureSession(a, !clap);
        defer foreign_session.close();
        var source: source_mod.PreparedSource = .{};
        defer source.deinit();
        try source.initShapesBeforeExecutionInto(session, session, session.execution_gate, .{ .tensors = &inputs }, .{ .batch = 1, .sequence = 4, .input_bytes = 64, .output_bytes = 12, .workspace_bytes = 512 * 1024 }, &workspace, .resident_consumer, admitted, .{ .native_call_policy = .in_process });
        try std.testing.expectEqual(@as(usize, 12 + 3 * @sizeOf(i64)), source.shape_scope.?.permit.request.?.output_bytes.?);
        try std.testing.expect(!source.shape_scope.?.permit.execution_yielded);
        try source.bindInputsBeforeExecution(.{ .host = &inputs }, .resident_consumer);
        try source.enter();
        const permit = try source.beginExecutionDispatch();
        try std.testing.expectError(error.SourceInvocationAlreadyExecuted, source.beginExecutionDispatch());
        // A changed managed declaration refuses BEFORE numerical dispatch or
        // any supplier/refund. The real cold geometry and B remain owned.
        const planned = permit.request.?;
        const amounts_before = permit.lease.?.amounts;
        const controller_before = controller.snapshot();
        const callbacks_before = probe.callbacks;
        const epoch_before = permit.dispatch_cleanup_epoch.?.record;
        permit.request.?.output_bytes = 0;
        const late_growth = permit.runResidentWithControl(&inputs, admitted, .{ .native_call_policy = .in_process });
        permit.request = planned;
        if (late_growth) |unexpected| {
            if (unexpected) |value| {
                var owned = value;
                owned.deinitWithBoundary(source.epoch());
            }
            return error.UnexpectedManagedGeometryGrowth;
        } else |err| try std.testing.expectEqual(error.PreparedAdmissionRequiresReadyTurn, err);
        try std.testing.expectEqualDeep(amounts_before, permit.lease.?.amounts);
        try std.testing.expectEqualDeep(controller_before, controller.snapshot());
        try std.testing.expectEqual(callbacks_before, probe.callbacks);
        try std.testing.expect(!permit.execution_yielded and permit.dispatch_cleanup_epoch.?.record == epoch_before);
        try std.testing.expect(source.waiter.acquired and scheduler.head == &source.waiter);
        var pending = try permit.runResidentWithControl(&inputs, admitted, .{ .native_call_policy = .in_process });
        errdefer if (pending) |*outputs| outputs.deinitWithBoundary(source.epoch());
        // Actual canonical Native is necessary but not authority to borrow
        // another model's quota. Both declines preserve the SAME pending value.
        const original_model = source.source_model.?;
        try std.testing.expect(pending.?.model_retention == original_model);
        const original_outputs = pending.?.outputs.ptr;
        const original_amounts = permit.lease.?.amounts;
        source.source_model = null;
        const missing_model = source.adoptSynchronousResident(&pending);
        source.source_model = original_model;
        if (missing_model) |unexpected| {
            var owned = unexpected;
            owned.close(.{ .boundary = source.epoch() });
            return error.UnexpectedSourceModelAcceptance;
        } else |err| try std.testing.expectEqual(error.MissingSourceModelLifetime, err);
        try std.testing.expect(pending != null and pending.?.outputs.ptr == original_outputs);
        try std.testing.expectEqualDeep(original_amounts, permit.lease.?.amounts);
        source.source_model = foreign_session.model_lifetime.?;
        const foreign_model = source.adoptSynchronousResident(&pending);
        source.source_model = original_model;
        if (foreign_model) |unexpected| {
            var owned = unexpected;
            owned.close(.{ .boundary = source.epoch() });
            return error.UnexpectedSourceModelAcceptance;
        } else |err| try std.testing.expectEqual(error.ResidentOutputsOwnerChanged, err);
        try std.testing.expect(pending != null and pending.?.outputs.ptr == original_outputs and pending.?.resource_lease == null);
        try std.testing.expectEqualDeep(original_amounts, permit.lease.?.amounts);
        try std.testing.expectEqual(@as(usize, 0), source.synchronous_consumers);
        var consumer = try source.adoptSynchronousResident(&pending);
        defer consumer.close(.{ .boundary = source.epoch() });
        try std.testing.expect(pending == null and consumer.outputs.resource_lease == null);
        try std.testing.expect(permit.lease != null and workspace.prepaid_host_bytes > 0);
        try std.testing.expectEqual(@as(usize, 1), source.synchronous_consumers);
        const host = try consumer.outputs.materializeFloat32(admitted, &.{}, null);
        var host_live = true;
        defer if (host_live) {
            for (host) |*value| value.deinit();
            admitted.free(host);
        };
        try std.testing.expect(try consumer.outputs.prepareResidentConsumer(.{ .boundary = source.epoch() }));
        try source.releaseForConsumer();
        try std.testing.expect(session.execution_gate.?.tryLock());
        session.execution_gate.?.unlock();
        probe.consumer_phase = true;
        var destination = try session.admitHostPreprocess(16);
        defer destination.deinit();
        try std.testing.expect(probe.consumer_reserves > 0 and controller.snapshot().host_scratch_bytes >= workspace.prepaid_host_bytes);
        const actual = try @import("backend_operation.zig").callOpaque(consumer.outputs.backend.*, null, Probe.consume, .{ consumer.outputs.backend, consumer.outputs.outputs[0], admitted });
        var actual_live = true;
        defer if (actual_live) admitted.free(actual);
        for (actual, host[0].asFloat32()) |got, expected| try std.testing.expectApproxEqAbs(expected * 2, got, 1e-6);
        for (host) |*value| value.deinit();
        admitted.free(host);
        host_live = false;
        consumer.close(.{ .boundary = source.epoch() });
        try std.testing.expectEqual(@as(usize, 0), source.synchronous_consumers);
        try std.testing.expect(permit.lease != null);
        source.deinit();
        try std.testing.expectEqual(@as(usize, 0), workspace.prepaid_host_bytes);
        try std.testing.expect(workspace.live_bytes > 0 and controller.snapshot().host_scratch_bytes >= workspace.live_bytes);
        for (actual) |value| try std.testing.expect(std.math.isFinite(value));
        admitted.free(actual);
        actual_live = false;
        destination.deinit();
        try workspace.trim();
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
        try std.testing.expect(probe.live == 0 and !probe.failure);
    }
}

test "resident encoder source refuses unprepared stage before inspecting backend or payload" {
    var permit = @import("../backends/session.zig").RunPermit{ .session = undefined, .lease = null };
    var waiter = @import("../runtime/embedding_scheduler.zig").Scheduler.Waiter{};
    var stage = StagedExecution{ .session = undefined, .gate = null, .waiter = &waiter, .cb = undefined, .control = .{}, .active = false, .permit = &permit, .idle_backend = 0 };
    try std.testing.expectError(if (build_options.enable_cuda) error.EmbeddingStageNotPrepared else error.CudaNotEnabled, stage.runResidentEmbeddingEncoder(std.testing.allocator, undefined, undefined, &.{}, 1, 1, 768, false));
}

test "actual Native source geometry preserves declared packed rows and refuses unfunded full padded execution" {
    if (comptime !build_options.enable_native) return error.SkipZigTest;
    const source_mod = @import("../backends/session.zig");
    const Admitted = @import("../backends/admitted_allocator.zig").AdmittedAllocator;
    const a = std.testing.allocator;
    var session = try mediaTextEntryFixtureSession(a, false);
    defer session.close();
    // Metadata-only ownership fixture: actual canonical architecture geometry,
    // no claim that the tiny CLIP weights are a numerical Gemma checkpoint.
    const actual: *ArchSession = @ptrCast(@alignCast(session.ptr));
    actual.arch_config = .{ .embedding_gemma2 = .{} };
    try session.installOwnedExecutionGate(a);
    var scheduler = @import("../runtime/embedding_scheduler.zig").Scheduler{};
    session.cohort_scheduler = &scheduler;
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    session.model_lifetime.?.admission_domain = domain.pin();
    const controller = &domain.controller;
    session.run_admission = .{ .controller = controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 341 * 1024 * 1024, .scratch_limit_bytes = 341 * 1024 * 1024 }, .static_workspace_bytes = 128 * 1024 * 1024, .check_live_memory = false };
    var workspace = Admitted{ .backing = a, .session = session };
    defer workspace.deinit();
    const declared: source_mod.RunRequest = .{ .batch = 1, .sequence = 2, .input_bytes = 8192 * 2 * @sizeOf(i64), .output_bytes = 2 * 768 * @sizeOf(f32), .workspace_bytes = try @import("../pipelines/embedding_gemma2_admission.zig").textEncoderWorkspace(2) };
    const full_info = [_]TensorInfo{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, 8192 } },
        .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ 1, 8192 } },
    };
    {
        var full: source_mod.PreparedSource = .{};
        defer full.deinit();
        try std.testing.expectError(error.ResourceLimitExceeded, full.initShapesBeforeExecutionInto(session, session, session.execution_gate, .{ .shapes = &full_info }, declared, &workspace, .host, a, .{ .native_call_policy = .in_process }));
    }
    try std.testing.expect(scheduler.head == null);
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, controller.snapshot());
    const packed_info = [_]TensorInfo{
        .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, 2 } },
        .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ 1, 2 } },
    };
    {
        var packed_source: source_mod.PreparedSource = .{};
        defer packed_source.deinit();
        try packed_source.initShapesBeforeExecutionInto(session, session, session.execution_gate, .{ .shapes = &packed_info }, declared, &workspace, .host, a, .{ .native_call_policy = .in_process });
        const request = packed_source.shape_scope.?.permit.request.?;
        try std.testing.expectEqual(@as(usize, 2), request.sequence);
        try std.testing.expectEqual(declared.input_bytes, request.input_bytes);
        try std.testing.expectEqual(declared.workspace_bytes, request.workspace_bytes);
        try std.testing.expectEqual(@as(usize, 2 * 768 * @sizeOf(f32) + 3 * @sizeOf(i64)), request.output_bytes.?);
        try std.testing.expect(session.execution_gate.?.tryLock());
        session.execution_gate.?.unlock();
    }
    try std.testing.expect(scheduler.head == null);
    try std.testing.expectEqualDeep(runtime.tier.memory.AdmissionAmounts{}, controller.snapshot());
}
