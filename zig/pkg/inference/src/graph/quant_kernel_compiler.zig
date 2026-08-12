// Copyright 2026 Antfly, Inc.
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

//! Domain-specific quant matmul compiler scaffolding.
//!
//! This is intentionally tiny: packed GGUF block decode, matmul schedules, and
//! common epilogues only. Checked-in AOT promotion remains a separate path;
//! runtime JIT dispatch may use a generated candidate only after exact,
//! pre-publication qualification against the bundled production kernel.

const std = @import("std");
const platform = @import("antfly_platform");
const backend_contracts = @import("backend_contracts.zig");
const quant_matmul = @import("quant_matmul.zig");
const quant_codec = @import("../gguf/quant_codec.zig");
const tensor_types = @import("../gguf/tensor_types.zig");
const quant_kernel_op = @import("quant_kernel_op.zig");
const kernel_jit = @import("kernel_jit.zig");
const cuda_renderer = @import("quant_kernel_cuda_renderer.zig");
const metal_renderer = @import("quant_kernel_metal_renderer.zig");

pub const metal_promotion_min_speedup: f64 = 1.02;
pub const metal_promotion_speedup_tolerance: f64 = 0.001;
pub const metal_promotion_repeat_runs: usize = 5;
pub const metal_promotion_warmup_repeat_runs: u32 = 2;
pub const metal_promotion_measure_iters: u32 = 500;
const metal_promotion_repeat_runs_text = "5";
const metal_promotion_measure_iters_text = "500";
const metal_promotion_args = " --repeat-runs " ++ metal_promotion_repeat_runs_text ++ " --measure-iters " ++ metal_promotion_measure_iters_text ++ " --attest-provenance --promotion-ready-kernel ";
pub const metal_blocker_none = "none";
pub const metal_blocker_speedup_gate_missing = "speedup_gate_missing";
pub const metal_blocker_unstable_benchmark_timing = "unstable_benchmark_timing";
pub const metal_blocker_runtime_route_only = "runtime_route_only";
pub const metal_blocker_missing_generated_route = "missing_metal_generated_route_evidence";
pub const metal_blocker_missing_provider_route = "missing_metal_provider_route_evidence";
pub const metal_blocker_unsupported_handwritten = "unsupported_handwritten_baseline";
pub const metal_blocker_dev_only_candidate = "dev_only_candidate";

pub const Backend = enum(u8) {
    cuda,
    metal,
};

pub const TargetResourceLimits = struct {
    max_threads_per_block: u16 = 1024,
    max_shared_memory_bytes: u32 = 0,
    max_registers_per_thread: u16 = 255,
    max_registers_per_block: u32 = 0,

    pub fn validate(self: TargetResourceLimits) !void {
        if (self.max_threads_per_block == 0 or self.max_threads_per_block > 1024) {
            return error.InvalidTargetThreadLimit;
        }
        if (self.max_registers_per_thread == 0) return error.InvalidTargetRegisterLimit;
    }
};

/// Exact AOT compilation target. `architecture` is a canonical backend-owned
/// name such as `sm_89` or `apple_family_9`; model names never participate in
/// target selection.
pub const Target = struct {
    backend: Backend,
    architecture: []const u8,
    required_features: u64 = 0,
    limits: TargetResourceLimits = .{},

    pub fn validate(self: Target) !void {
        if (!isCanonicalCatalogToken(self.architecture)) return error.InvalidTargetArchitecture;
        try self.limits.validate();
    }

    pub fn matches(self: Target, available: Target) bool {
        return self.backend == available.backend and
            std.mem.eql(u8, self.architecture, available.architecture) and
            (available.required_features & self.required_features) == self.required_features;
    }

    pub fn overlaps(a: Target, b: Target) bool {
        // Feature requirements are monotone. A target supporting the union of
        // both masks would match both entries, so masks cannot disambiguate.
        return a.backend == b.backend and std.mem.eql(u8, a.architecture, b.architecture);
    }
};

/// Machine-readable target identity for the checked-in CUDA promotion
/// evidence. Catalogs must reuse this target rather than independently naming
/// an architecture that the evidence did not measure.
pub const cuda_sm89_promotion_target = Target{
    .backend = .cuda,
    .architecture = "sm_89",
    .limits = .{
        .max_threads_per_block = 1024,
        .max_shared_memory_bytes = 99 * 1024,
        .max_registers_per_thread = 255,
        .max_registers_per_block = 64 * 1024,
    },
};

pub const cuda_sm89_promotion_target_fingerprint = targetFingerprint(cuda_sm89_promotion_target);

pub const ScheduleResources = struct {
    static_shared_memory_bytes: u32 = 0,
    dynamic_shared_memory_bytes: u32 = 0,
    registers_per_thread: u16 = 0,
};

/// Backend-neutral AOT schedule identity. Backend render plans may carry more
/// lowering detail; these fields are the dispatch/resource contract required
/// for deterministic selection and catalog validation.
pub const Schedule = struct {
    family: []const u8,
    threads_per_block: u16,
    rows_per_block: u16 = 1,
    cols_per_block: u16 = 1,
    vector_width: u8 = 1,
    split_count: u16 = 1,
    resources: ScheduleResources = .{},

    pub fn validateForTarget(self: Schedule, target: Target) !void {
        if (!isCanonicalCatalogToken(self.family)) return error.InvalidScheduleFamily;
        if (self.threads_per_block == 0 or self.threads_per_block > target.limits.max_threads_per_block) {
            return error.ScheduleThreadsExceedTarget;
        }
        if (self.rows_per_block == 0 or self.cols_per_block == 0 or self.vector_width == 0 or self.split_count == 0) {
            return error.InvalidScheduleTile;
        }

        const shared_bytes = @as(u64, self.resources.static_shared_memory_bytes) +
            @as(u64, self.resources.dynamic_shared_memory_bytes);
        if (target.limits.max_shared_memory_bytes != 0 and shared_bytes > target.limits.max_shared_memory_bytes) {
            return error.ScheduleSharedMemoryExceedsTarget;
        }
        if (self.resources.registers_per_thread != 0) {
            if (self.resources.registers_per_thread > target.limits.max_registers_per_thread) {
                return error.ScheduleRegistersExceedTarget;
            }
            const block_registers = @as(u64, self.resources.registers_per_thread) * self.threads_per_block;
            if (target.limits.max_registers_per_block != 0 and block_registers > target.limits.max_registers_per_block) {
                return error.ScheduleRegisterFileExceedsTarget;
            }
        }
    }
};

pub const CatalogEntry = struct {
    signature: quant_kernel_op.SpecializationSignature,
    runtime: quant_kernel_op.RuntimeConstraints,
    target: Target,
    schedule: Schedule,
    kernel_id: []const u8,
    source_fingerprint: u64 = 0,
    production_enabled: bool = false,

    pub fn validate(self: CatalogEntry) !void {
        try self.signature.validate();
        try self.runtime.validateExactFor(self.signature);
        try self.target.validate();
        try self.schedule.validateForTarget(self.target);
        if (!isCanonicalCatalogToken(self.kernel_id)) return error.InvalidCatalogKernelId;
    }

    pub fn fingerprint(self: CatalogEntry) u64 {
        return catalogEntryFingerprint(self);
    }

    pub fn id(self: CatalogEntry, allocator: std.mem.Allocator) ![]u8 {
        return catalogEntryId(allocator, self);
    }
};

pub const CatalogQuery = struct {
    signature: quant_kernel_op.SpecializationSignature,
    runtime: quant_kernel_op.RuntimeShape,
    target: Target,
};

pub const Catalog = struct {
    entries: []const CatalogEntry,

    pub fn validate(self: Catalog) !void {
        for (self.entries, 0..) |entry, index| {
            try entry.validate();
            for (self.entries[index + 1 ..]) |other| {
                if (!entry.signature.eql(other.signature)) continue;
                if (!Target.overlaps(entry.target, other.target)) continue;
                if (!quant_kernel_op.RuntimeConstraints.overlaps(entry.runtime, other.runtime)) continue;
                return error.AmbiguousCatalogEntry;
            }
        }
    }

    pub fn resolve(self: Catalog, query: CatalogQuery) !?CatalogEntry {
        var match: ?CatalogEntry = null;
        for (self.entries) |entry| {
            if (!entry.signature.eql(query.signature) or
                !entry.target.matches(query.target) or
                !entry.runtime.matches(query.runtime))
            {
                continue;
            }
            if (match != null) return error.AmbiguousCatalogEntry;
            match = entry;
        }
        return match;
    }
};

pub const AotTarget = Target;
pub const AotSchedule = Schedule;
pub const AotCatalogEntry = CatalogEntry;
pub const AotCatalog = Catalog;

const catalog_fingerprint_seed: u64 = 0x414e_5446_4c59_414f; // "ANTFLYAO"

pub fn targetFingerprint(target: Target) u64 {
    var hasher = std.hash.Wyhash.init(catalog_fingerprint_seed ^ 0x5441_5247_4554_0001);
    catalogHashU8(&hasher, 1);
    catalogHashU8(&hasher, @intFromEnum(target.backend));
    catalogHashBytes(&hasher, target.architecture);
    catalogHashU64(&hasher, target.required_features);
    return hasher.final();
}

pub fn scheduleFingerprint(schedule: Schedule) u64 {
    var hasher = std.hash.Wyhash.init(catalog_fingerprint_seed ^ 0x5343_4845_4455_4c45);
    catalogHashU8(&hasher, 1);
    catalogHashBytes(&hasher, schedule.family);
    catalogHashU16(&hasher, schedule.threads_per_block);
    catalogHashU16(&hasher, schedule.rows_per_block);
    catalogHashU16(&hasher, schedule.cols_per_block);
    catalogHashU8(&hasher, schedule.vector_width);
    catalogHashU16(&hasher, schedule.split_count);
    catalogHashU32(&hasher, schedule.resources.static_shared_memory_bytes);
    catalogHashU32(&hasher, schedule.resources.dynamic_shared_memory_bytes);
    catalogHashU16(&hasher, schedule.resources.registers_per_thread);
    return hasher.final();
}

/// Stable specialization fingerprint. Source bytes and promotion state are
/// intentionally excluded: recompiling the same semantic schedule must retain
/// its identity, while source provenance remains separately attestable.
pub fn catalogEntryFingerprint(entry: CatalogEntry) u64 {
    var hasher = std.hash.Wyhash.init(catalog_fingerprint_seed);
    catalogHashU8(&hasher, 1);
    catalogHashU64(&hasher, quant_kernel_op.specializationSignatureFingerprint(entry.signature));
    catalogHashU64(&hasher, quant_kernel_op.runtimeConstraintsFingerprint(entry.runtime));
    catalogHashU64(&hasher, targetFingerprint(entry.target));
    catalogHashU64(&hasher, scheduleFingerprint(entry.schedule));
    return hasher.final();
}

pub fn catalogEntryId(allocator: std.mem.Allocator, entry: CatalogEntry) ![]u8 {
    return std.fmt.allocPrint(
        allocator,
        "antfly.kernel.aot.v1/{s}/{s}/{s}/{x:0>16}",
        .{
            @tagName(entry.target.backend),
            entry.target.architecture,
            @tagName(entry.signature.opKind()),
            catalogEntryFingerprint(entry),
        },
    );
}

pub const specializationFingerprint = catalogEntryFingerprint;
pub const specializationId = catalogEntryId;

fn isCanonicalCatalogToken(value: []const u8) bool {
    if (value.len == 0) return false;
    for (value) |char| switch (char) {
        'a'...'z', '0'...'9', '_', '-', '.' => {},
        else => return false,
    };
    return true;
}

fn catalogHashBytes(hasher: *std.hash.Wyhash, bytes: []const u8) void {
    catalogHashU32(hasher, @intCast(bytes.len));
    hasher.update(bytes);
}

fn catalogHashU8(hasher: *std.hash.Wyhash, value: u8) void {
    hasher.update(&.{value});
}

fn catalogHashU16(hasher: *std.hash.Wyhash, value: u16) void {
    var bytes: [2]u8 = undefined;
    std.mem.writeInt(u16, &bytes, value, .little);
    hasher.update(&bytes);
}

fn catalogHashU32(hasher: *std.hash.Wyhash, value: u32) void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .little);
    hasher.update(&bytes);
}

fn catalogHashU64(hasher: *std.hash.Wyhash, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hasher.update(&bytes);
}

fn testAotCatalogEntry(output_dim: u32) CatalogEntry {
    return .{
        .signature = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .dispatch = .mmv,
            .epilogue = .none,
            .activation = .q8_1,
        } },
        .runtime = .exactMatmul(1, 1536, output_dim),
        .target = .{
            .backend = .cuda,
            .architecture = "sm_89",
            .required_features = 0b0011,
            .limits = .{
                .max_threads_per_block = 1024,
                .max_shared_memory_bytes = 64 * 1024,
                .max_registers_per_thread = 255,
                .max_registers_per_block = 64 * 1024,
            },
        },
        .schedule = .{
            .family = "mmvq",
            .threads_per_block = 128,
            .cols_per_block = 1,
            .vector_width = 2,
            .resources = .{
                .static_shared_memory_bytes = 512,
                .registers_per_thread = 32,
            },
        },
        .kernel_id = if (output_dim == 6144) "antfly_q4_0_q8_1_1536x6144_sm89" else "antfly_q4_0_q8_1_1536x12288_sm89",
    };
}

test "quant kernel compiler AOT catalog identities are semantic and stable" {
    try std.testing.expectEqualStrings("sm_89", cuda_sm89_promotion_target.architecture);
    try std.testing.expectEqual(@as(u64, 0xa9ce_514a_184e_e15d), cuda_sm89_promotion_target_fingerprint);

    const entry = testAotCatalogEntry(6144);
    try entry.validate();
    try std.testing.expectEqual(@as(u64, 0x860a_6d1e_5aa3_40b2), entry.fingerprint());

    var provenance_changed = entry;
    provenance_changed.source_fingerprint = 0xfeed_beef;
    provenance_changed.production_enabled = true;
    try std.testing.expectEqual(entry.fingerprint(), provenance_changed.fingerprint());

    var limit_changed = entry;
    limit_changed.target.limits.max_shared_memory_bytes = 96 * 1024;
    try std.testing.expectEqual(entry.fingerprint(), limit_changed.fingerprint());

    const id = try entry.id(std.testing.allocator);
    defer std.testing.allocator.free(id);
    try std.testing.expect(std.mem.startsWith(
        u8,
        id,
        "antfly.kernel.aot.v1/cuda/sm_89/small_batch_matmul/",
    ));
}

test "quant kernel compiler AOT catalog requires exact compute shapes" {
    var entry = testAotCatalogEntry(6144);
    entry.runtime.input_dim = .{ .min = 1024, .max = 2048, .multiple_of = 32 };
    try std.testing.expectError(error.InexactMatmulRuntimeConstraint, entry.validate());

    entry = testAotCatalogEntry(6144);
    entry.runtime.input_dim = .{ .min = 1536, .max = 1536 };
    try entry.validate();
}

test "quant kernel compiler AOT catalog rejects ambiguous runtime entries" {
    const first = testAotCatalogEntry(6144);
    var ambiguous = first;
    ambiguous.kernel_id = "antfly_q4_0_q8_1_1536x6144_sm89_alt";
    ambiguous.schedule.threads_per_block = 256;
    const ambiguous_entries = [_]CatalogEntry{ first, ambiguous };
    try std.testing.expectError(error.AmbiguousCatalogEntry, (Catalog{ .entries = &ambiguous_entries }).validate());

    const second = testAotCatalogEntry(12288);
    const entries = [_]CatalogEntry{ first, second };
    const catalog = Catalog{ .entries = &entries };
    try catalog.validate();

    const resolved = (try catalog.resolve(.{
        .signature = first.signature,
        .runtime = .{ .rows = 1, .input_dim = 1536, .output_dim = 12288 },
        .target = .{
            .backend = .cuda,
            .architecture = "sm_89",
            .required_features = 0b1111,
        },
    })) orelse return error.MissingAotCatalogEntry;
    try std.testing.expectEqualStrings(second.kernel_id, resolved.kernel_id);

    try std.testing.expect((try catalog.resolve(.{
        .signature = first.signature,
        .runtime = .{ .rows = 2, .input_dim = 1536, .output_dim = 12288 },
        .target = .{ .backend = .cuda, .architecture = "sm_89", .required_features = 0b1111 },
    })) == null);
}

test "quant kernel compiler AOT catalog validates target resources" {
    var entry = testAotCatalogEntry(6144);
    entry.schedule.threads_per_block = 1024;
    entry.target.limits.max_threads_per_block = 512;
    try std.testing.expectError(error.ScheduleThreadsExceedTarget, entry.validate());

    entry = testAotCatalogEntry(6144);
    entry.schedule.resources.dynamic_shared_memory_bytes = 64 * 1024;
    try std.testing.expectError(error.ScheduleSharedMemoryExceedsTarget, entry.validate());

    entry = testAotCatalogEntry(6144);
    entry.schedule.resources.registers_per_thread = 129;
    entry.schedule.threads_per_block = 512;
    try std.testing.expectError(error.ScheduleRegisterFileExceedsTarget, entry.validate());
}

/// The kind of kernel a generated artifact represents. The renderer, evidence,
/// and manifest machinery are keyed by `kernel_id` and are op-agnostic; `op_kind`
/// is the routing dimension that selects which skeleton/spec a route uses. Today
/// every generated route is `small_batch_matmul`; `attention` and `microkernel`
/// are the extension points (a family of narrowly-routed attention kernels and
/// fused attention-adjacent microkernels — RMSNorm/rope/KV read-write).
pub const OpKind = quant_kernel_op.OpKind;
pub const MicrokernelKind = quant_kernel_op.MicrokernelKind;
pub const AttentionKind = quant_kernel_op.AttentionKind;
pub const Epilogue = quant_kernel_op.Epilogue;
pub const ActivationEncoding = quant_kernel_op.ActivationEncoding;
pub const ActivationFunction = quant_kernel_op.ActivationFunction;
pub const OutputEncoding = quant_kernel_op.OutputEncoding;
pub const SpecializationSignature = quant_kernel_op.SpecializationSignature;
pub const RuntimeDimensionConstraint = quant_kernel_op.RuntimeDimensionConstraint;
pub const RuntimeShape = quant_kernel_op.RuntimeShape;
pub const RuntimeConstraints = quant_kernel_op.RuntimeConstraints;

pub const DType = enum(u8) {
    f32,
    f16,
    bf16,
};

pub const DecodeOpKind = enum(u8) {
    load_packed_bytes,
    extract_scale,
    extract_min,
    extract_quant_lane,
    dequant_lane,
};

pub const DecodeOp = struct {
    kind: DecodeOpKind,
    expression: []const u8,
};

pub const BlockField = struct {
    name: []const u8,
    offset: usize,
    bytes: usize,
};

pub const QuantKernelSpec = struct {
    format: quant_matmul.Format,
    block_values: usize,
    block_bytes: usize,
    block_fields: []const BlockField,
    decode_ops: []const DecodeOp,
    supported_schedules: []const quant_matmul.DispatchKind,
    supported_epilogues: []const Epilogue,
    accumulator_dtype: DType,
    output_dtype: DType,
    supported_backends: []const Backend,

    pub fn supportsSchedule(self: QuantKernelSpec, schedule: quant_matmul.DispatchKind) bool {
        return contains(quant_matmul.DispatchKind, self.supported_schedules, schedule);
    }

    pub fn supportsEpilogue(self: QuantKernelSpec, epilogue: Epilogue) bool {
        return contains(Epilogue, self.supported_epilogues, epilogue);
    }

    pub fn supportsBackend(self: QuantKernelSpec, backend: Backend) bool {
        return contains(Backend, self.supported_backends, backend);
    }
};

pub const IROp = enum(u8) {
    load_input_row,
    load_quant_block,
    decode_quant_lane,
    multiply_accumulate,
    reduce_accumulator,
    apply_bias,
    apply_gelu,
    write_output,
    write_argmax_pair,
};

pub const QuantKernelIR = struct {
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    dispatch: quant_matmul.DispatchKind,
    epilogue: Epilogue,
    ops: []const IROp,
};

pub const QuantKernelSchedule = struct {
    dispatch: quant_matmul.DispatchKind,
    row_bucket: quant_matmul.RowBucket,
    tile_rows: usize,
    tile_cols: usize,
    vector_width: usize,
    threads_per_block: usize,
    shared_memory_bytes: usize,
    register_pressure_hint: u8,
    tensor_core_eligible: bool,
};

pub const QuantKernelPlanId = struct {
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    dispatch: quant_matmul.DispatchKind,
};

/// How a small-batch quant kernel reduces the per-thread partial sums into the
/// final per-column result.
pub const ReductionKind = enum(u8) {
    /// Single simdgroup (32 threads); reduce with `simd_sum` only.
    simd_sum,
    /// Multiple simdgroups; reduce a `partial[threads]` array with a
    /// threadgroup-barrier tree.
    threadgroup_tree,
    /// Multiple simdgroups; `simd_sum` per simdgroup into `partial[32]`, then a
    /// final `simd_sum` (needs `lane_id`/`simdgroup_id`).
    hybrid_simd,
    /// Independent simdgroups each produce a register tile. There is no
    /// cross-simdgroup reduction; `cols_per_threadgroup` is the sum of the
    /// columns produced by all simdgroups in the threadgroup.
    simdgroup_tiled,
    /// Five or eight simdgroups cooperatively stage a 40x32x64 or 64x32x64
    /// half tile and accumulate it with Metal simdgroup matrix operations.
    simdgroup_matrix,
};

/// The launch/loop schedule of one generated Metal small-batch quant kernel.
/// This is the single source of truth for the dispatch grid (threads/cols),
/// replacing the launch-shape switch in metal_kernels.m and the ad-hoc
/// `metalGeneratedThreadsPerThreadgroup`/`Cols` helpers.
pub const KernelSchedule = struct {
    threads_per_threadgroup: u16,
    cols_per_threadgroup: u8,
    reduction: ReductionKind,
    rows_per_threadgroup: u8 = 1,

    /// Attention-only schedule knobs (`op_kind == .attention`). Defaulted so
    /// every matmul/microkernel `KernelSchedule` literal (which omits them) stays
    /// byte-identical and the matmul renderer/validate/fingerprint paths ignore
    /// them entirely. Read only by the flash-prefill attention body:
    ///  - `key_chunk`: KV tokens staged + scored per flash chunk (32 or 64). The
    ///    shmem layout and the Q·K^T/score/P·V tile counts derive from this.
    ///  - `skip_rescale`: guard the online-softmax O-accumulator rescale behind a
    ///    "did any row's running max change this chunk" flag (an identity multiply
    ///    otherwise). ALU-only; layout-neutral.
    key_chunk: u16 = 32,
    skip_rescale: bool = false,
    attention_serial_threads_per_threadgroup: u16 = 0,
    attention_stage2_threads_per_threadgroup: u16 = 0,
    attention_tiled64_threads_per_threadgroup: u16 = 0,
    attention_kv_splits: u8 = 1,
    attention_query_heads_per_kv_head: u8 = 1,
    attention_split_kv_min_tokens: u16 = 0,
    attention_max_kv_tokens: u16 = 0,
    attention_tiled64_max_kv_tokens: u16 = 0,
    attention_query_tile: u16 = 0,
    attention_key_tile: u16 = 0,
    attention_page_size_tokens: u16 = 0,
    attention_dynamic_shared_memory_bytes: u32 = 0,
    attention_required_compute_major: u8 = 0,
    attention_required_compute_minor: u8 = 0,
    attention_query_length_policy: ?cuda_renderer.FlashPrefillQueryLengthPolicy = null,
    attention_storage: AttentionStorage = .f32,
    attention_key_storage: AttentionStorage = .f32,
    attention_value_storage: AttentionStorage = .f32,

    /// A block's lanes are processed strided across threads when the block has
    /// more values than the threadgroup has threads.
    pub fn strided(self: KernelSchedule, block_values: usize) bool {
        return block_values > self.threads_per_threadgroup;
    }

    pub fn validate(self: KernelSchedule, block_values: usize) !void {
        if (self.threads_per_threadgroup % 32 != 0 or self.threads_per_threadgroup == 0 or self.threads_per_threadgroup > 1024) {
            return error.InvalidThreadCount;
        }
        switch (self.reduction) {
            .simd_sum => {
                if (self.rows_per_threadgroup != 1) return error.InvalidRowCount;
                if (self.cols_per_threadgroup != 1 and self.cols_per_threadgroup != 2) return error.InvalidColCount;
                if (self.threads_per_threadgroup != 32) return error.SimdSumNeeds32Threads;
            },
            .threadgroup_tree => {
                if (self.rows_per_threadgroup != 1) return error.InvalidRowCount;
                if (self.cols_per_threadgroup != 1 and self.cols_per_threadgroup != 2) return error.InvalidColCount;
                if (self.threads_per_threadgroup < 64) return error.MultiSimdgroupNeeds64Threads;
                if (!std.math.isPowerOfTwo(self.threads_per_threadgroup)) return error.TreeReductionNeedsPowerOfTwoThreads;
            },
            .hybrid_simd => {
                if (self.rows_per_threadgroup != 1) return error.InvalidRowCount;
                if (self.cols_per_threadgroup != 1 and self.cols_per_threadgroup != 2) return error.InvalidColCount;
                if (self.threads_per_threadgroup < 64) return error.MultiSimdgroupNeeds64Threads;
            },
            .simdgroup_tiled => {
                if (block_values != 32 and block_values != 256) return error.TiledReductionRequiresQuantBlock;
                if (self.rows_per_threadgroup != 2 and
                    (block_values != 256 or (self.rows_per_threadgroup != 4 and self.rows_per_threadgroup != 8)))
                {
                    return error.TiledReductionRequiresSupportedRows;
                }
                if (self.threads_per_threadgroup > 256) return error.TiledReductionTooManyThreads;
                const simdgroups: u8 = @intCast(self.threads_per_threadgroup / 32);
                if (self.cols_per_threadgroup % simdgroups != 0) return error.InvalidColCount;
                const cols_per_simdgroup = self.cols_per_threadgroup / simdgroups;
                if (cols_per_simdgroup != 2 and cols_per_simdgroup != 4 and cols_per_simdgroup != 8) {
                    return error.InvalidColCount;
                }
            },
            .simdgroup_matrix => {
                if (block_values != 256) return error.MatrixReductionRequiresKBlock;
                const tile_40 = self.threads_per_threadgroup == 160 and self.rows_per_threadgroup == 40;
                const tile_64 = self.threads_per_threadgroup == 256 and self.rows_per_threadgroup == 64;
                if (self.cols_per_threadgroup != 64 or (!tile_40 and !tile_64)) {
                    return error.InvalidMatrixTile;
                }
            },
        }
        if (block_values == 0 or (block_values & (block_values - 1)) != 0) {
            return error.BlockValuesNotPowerOfTwo;
        }
    }
};

pub const AttentionStorage = enum {
    f32,
    f16,
    bf16,
    paged_f16,
    paged_f16_or_polar4,
    paged_f16_or_f32,
};

test "quant kernel compiler rejects unsafe Metal reduction thread counts" {
    const non_power_of_two_tree = KernelSchedule{ .threads_per_threadgroup = 96, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree };
    try std.testing.expectError(error.TreeReductionNeedsPowerOfTwoThreads, non_power_of_two_tree.validate(256));

    const oversized_hybrid = KernelSchedule{ .threads_per_threadgroup = 1056, .cols_per_threadgroup = 1, .reduction = .hybrid_simd };
    try std.testing.expectError(error.InvalidThreadCount, oversized_hybrid.validate(256));
}

/// One (format, row_bucket, epilogue) -> schedule mapping. The
/// `metal_production_schedules` table below enumerates every currently
/// generated Metal small-batch route.
pub const MetalRouteSchedule = struct {
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    schedule: KernelSchedule,
};

/// The dispatch schedule of every generated Metal small-batch quant route.
/// Threads/cols here MUST match the dispatch grid used at runtime; this table
/// generates both the runtime launch-shape lookup (metal_kernels.m) and the
/// benchmark CheckCase shapes. Reduction records the body's reduction strategy
/// (consumed by the renderer; the launch table needs only threads/cols).
///
/// Transcribed from the v1 kernel bodies + the launch-shape switch. Note:
/// `metalGeneratedColsPerThreadgroup` historically reported cols=1 for
/// q8_0/bias_gelu while the kernel and dispatch use cols=2 — this table carries
/// the correct cols=2, fixing that latent under-count.
pub const metal_production_schedules = [_]MetalRouteSchedule{
    // 32-value blocks, single simdgroup.
    .{ .format = .q4_0, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum } },
    .{ .format = .q4_1, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 2, .reduction = .simd_sum } },
    .{ .format = .q5_0, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum } },
    .{ .format = .q5_1, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 2, .reduction = .simd_sum } },
    .{ .format = .q8_0, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 2, .reduction = .simd_sum } },
    .{ .format = .q8_0, .row_bucket = .rows_2_8, .epilogue = .bias, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum } },
    .{ .format = .q8_0, .row_bucket = .rows_2_8, .epilogue = .bias_gelu, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 2, .reduction = .simd_sum } },
    .{ .format = .q8_0, .row_bucket = .rows_2_8, .epilogue = .relu, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum } },
    .{ .format = .q8_1, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum } },
    // 256-value blocks reduced on a single simdgroup (strided lanes).
    .{ .format = .q8_k, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 64, .cols_per_threadgroup = 1, .reduction = .hybrid_simd } },
    .{ .format = .q2_k, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 1, .reduction = .hybrid_simd } },
    .{ .format = .q2_k, .row_bucket = .rows_2_8, .epilogue = .bias, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum } },
    .{ .format = .q2_k, .row_bucket = .rows_2_8, .epilogue = .bias_gelu, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum } },
    .{ .format = .q3_k, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum } },
    .{ .format = .q3_k, .row_bucket = .rows_2_8, .epilogue = .bias, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum } },
    .{ .format = .q3_k, .row_bucket = .rows_2_8, .epilogue = .bias_gelu, .schedule = .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum } },
    // Q4_K shares weights across two rows and activations across sixteen columns.
    .{ .format = .q4_k, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled } },
    .{ .format = .q4_k, .row_bucket = .rows_2_8, .epilogue = .bias, .schedule = .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled } },
    .{ .format = .q4_k, .row_bucket = .rows_2_8, .epilogue = .bias_gelu, .schedule = .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled } },
    .{ .format = .q5_k, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 1, .reduction = .hybrid_simd } },
    .{ .format = .q5_k, .row_bucket = .rows_2_8, .epilogue = .bias, .schedule = .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 1, .reduction = .hybrid_simd } },
    .{ .format = .q5_k, .row_bucket = .rows_2_8, .epilogue = .bias_gelu, .schedule = .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 1, .reduction = .hybrid_simd } },
    // Q6_K uses the same two-row by sixteen-column register tile as Q4_K.
    .{ .format = .q6_k, .row_bucket = .rows_2_8, .epilogue = .none, .schedule = .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled } },
    .{ .format = .q6_k, .row_bucket = .rows_2_8, .epilogue = .bias, .schedule = .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled } },
    .{ .format = .q6_k, .row_bucket = .rows_2_8, .epilogue = .bias_gelu, .schedule = .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled } },
};

/// Look up a route's schedule in `metal_production_schedules`. Returns null for
/// routes without a generated Metal small-batch kernel.
pub fn metalRouteScheduleFor(
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
) ?KernelSchedule {
    for (metal_production_schedules) |entry| {
        if (entry.format == format and entry.row_bucket == row_bucket and entry.epilogue == epilogue) {
            return entry.schedule;
        }
    }
    return null;
}

pub const metal_schedule_candidate_capacity = kernel_jit.maximum_candidates;

/// Deterministic runtime/offline tuning catalog for a Metal matmul route.
/// The current schedule grammar has two honest variants for 32-value blocks
/// and eight for 256-value blocks; do not pad the catalog with duplicates.
pub fn metalScheduleCandidates(
    format: quant_matmul.Format,
    epilogue: Epilogue,
    out: *[metal_schedule_candidate_capacity]KernelSchedule,
) usize {
    switch (epilogue) {
        .none, .bias, .bias_gelu, .relu => {},
        else => return 0,
    }
    const block_values = format.valuesPerBlock() orelse return 0;
    var count: usize = 0;
    if ((format == .q4_k or format == .q6_k) and
        (epilogue == .none or epilogue == .bias or epilogue == .bias_gelu))
    {
        for ([_]u16{ 32, 64, 128, 256 }) |threads| {
            const simdgroups: u8 = @intCast(threads / 32);
            for ([_]u8{ 2, 4 }) |cols_per_simdgroup| {
                out[count] = .{
                    .threads_per_threadgroup = threads,
                    .cols_per_threadgroup = simdgroups * cols_per_simdgroup,
                    .rows_per_threadgroup = 2,
                    .reduction = .simdgroup_tiled,
                };
                count += 1;
            }
        }
        return count;
    }
    for ([_]u16{ 32, 64, 128, 256 }) |threads| {
        if (threads > block_values) continue;
        if (threads == 32) {
            for ([_]u8{ 1, 2 }) |cols| {
                const schedule = KernelSchedule{
                    .threads_per_threadgroup = threads,
                    .cols_per_threadgroup = cols,
                    .reduction = .simd_sum,
                };
                schedule.validate(block_values) catch continue;
                if (count == out.len) return count;
                out[count] = schedule;
                count += 1;
            }
        } else {
            for ([_]ReductionKind{ .threadgroup_tree, .hybrid_simd }) |reduction| {
                const schedule = KernelSchedule{
                    .threads_per_threadgroup = threads,
                    .cols_per_threadgroup = 1,
                    .reduction = reduction,
                };
                schedule.validate(block_values) catch continue;
                if (count == out.len) return count;
                out[count] = schedule;
                count += 1;
            }
        }
    }
    return count;
}

/// Shape-aware schedule catalog used by exact-signature Metal tuning. Every
/// valid two-row Q4_0 projection gets a bounded packed register-tile grid;
/// Q4_K/Q6_K encoder projections get bounded register and row-tiled matrix
/// candidates; every other shape keeps the generic catalog unchanged.
pub fn metalScheduleCandidatesForExactShape(
    format: quant_matmul.Format,
    epilogue: Epilogue,
    rows: u64,
    in_dim: u64,
    out_dim: u64,
    out: *[metal_schedule_candidate_capacity]KernelSchedule,
) usize {
    if ((format == .q4_k or format == .q6_k) and
        (epilogue == .none or epilogue == .bias or epilogue == .bias_gelu) and
        rows >= 9 and
        in_dim != 0 and in_dim % 256 == 0 and out_dim != 0)
    {
        const row_tiles = if (rows <= 32) [_]u8{ 2, 4 } else [_]u8{ 4, 8 };
        var count: usize = 0;
        for ([_]u16{ 64, 128 }) |threads| {
            const simdgroups: u8 = @intCast(threads / 32);
            for ([_]u8{ 2, 4 }) |cols_per_simdgroup| {
                for (row_tiles) |rows_per_threadgroup| {
                    out[count] = .{
                        .threads_per_threadgroup = threads,
                        .cols_per_threadgroup = simdgroups * cols_per_simdgroup,
                        .rows_per_threadgroup = rows_per_threadgroup,
                        .reduction = .simdgroup_tiled,
                    };
                    count += 1;
                }
            }
        }
        if (epilogue == .none and rows >= 40) {
            out[count - 1] = .{
                .threads_per_threadgroup = 160,
                .cols_per_threadgroup = 64,
                .rows_per_threadgroup = 40,
                .reduction = .simdgroup_matrix,
            };
            if (rows >= 64) {
                // Compare both matrix tiles without growing the bounded
                // candidate set or displacing the strongest register shapes.
                out[count - 3] = out[count - 1];
                out[count - 1] = .{
                    .threads_per_threadgroup = 256,
                    .cols_per_threadgroup = 64,
                    .rows_per_threadgroup = 64,
                    .reduction = .simdgroup_matrix,
                };
            }
        }
        return count;
    }

    if (format != .q4_0 or
        epilogue != .none or
        rows != 2 or
        in_dim == 0 or
        in_dim % 32 != 0 or
        out_dim == 0)
    {
        return metalScheduleCandidates(format, epilogue, out);
    }

    var count: usize = 0;
    for ([_]u16{ 32, 64, 128, 256 }) |threads| {
        const simdgroups: u8 = @intCast(threads / 32);
        for ([_]u8{ 4, 8 }) |default_cols_per_simdgroup| {
            // At small output widths, the wider 32-thread tile consistently
            // underfills the device. Keep the catalog bounded by replacing
            // only that losing candidate with a two-column tile.
            const cols_per_simdgroup: u8 = if (out_dim <= 512 and
                threads == 32 and
                default_cols_per_simdgroup == 8)
                2
            else
                default_cols_per_simdgroup;
            out[count] = .{
                .threads_per_threadgroup = threads,
                .cols_per_threadgroup = simdgroups * cols_per_simdgroup,
                .rows_per_threadgroup = 2,
                .reduction = .simdgroup_tiled,
            };
            count += 1;
        }
    }
    return count;
}

pub const LoweringRoute = enum(u8) {
    generated_production,
    generated_dev_candidate,
    handwritten_production,
    unsupported,
};

pub const FallbackReason = enum(u8) {
    none,
    unsupported_format,
    unsupported_shape,
    unsupported_epilogue,
    unsupported_backend,
    generated_artifact_missing,
    generated_runtime_not_wired,
    tensor_core_repack_required,
};

pub const QuantKernelLowering = struct {
    plan_id: QuantKernelPlanId,
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    schedule: QuantKernelSchedule,
    production_route: LoweringRoute,
    candidate_route: LoweringRoute,
    production_kernel_id: []const u8,
    fallback_reason: FallbackReason,
    kernel_id: []const u8,
    candidate_source_path: []const u8,
};

pub const PlanCounters = struct {
    quant_kernel_planned_ops: usize = 0,
    quant_kernel_handwritten_production: usize = 0,
    quant_kernel_generated_production: usize = 0,
    quant_kernel_unsupported_routes: usize = 0,
    quant_kernel_generated_candidates: usize = 0,
    quant_kernel_fallback_generated_artifact_missing: usize = 0,
    quant_kernel_fallback_generated_runtime_not_wired: usize = 0,
    quant_kernel_fallback_unsupported_format: usize = 0,
    quant_kernel_fallback_unsupported_shape: usize = 0,
    quant_kernel_fallback_unsupported_epilogue: usize = 0,
    quant_kernel_fallback_unsupported_backend: usize = 0,
    quant_kernel_fallback_tensor_core_repack_required: usize = 0,
    quant_kernel_fallback_unsupported: usize = 0,
};

pub const QuantKernelRegistry = struct {
    entries: []const QuantKernelLowering,

    pub fn lookup(
        self: QuantKernelRegistry,
        backend: Backend,
        format: quant_matmul.Format,
        row_bucket: quant_matmul.RowBucket,
        epilogue: Epilogue,
        dispatch: quant_matmul.DispatchKind,
    ) ?QuantKernelLowering {
        for (self.entries) |entry| {
            if (entry.backend == backend and
                entry.format == format and
                entry.row_bucket == row_bucket and
                entry.epilogue == epilogue and
                entry.schedule.dispatch == dispatch)
            {
                return entry;
            }
        }
        return null;
    }
};

pub const CoverageCase = struct {
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
};

pub const ConformanceCase = struct {
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    dispatch: quant_matmul.DispatchKind,
    reference_supported: bool,
    reference_tensor_type: tensor_types.TensorType,
    tolerance_abs: f32,
    cuda_route: LoweringRoute,
    cuda_candidate_route: LoweringRoute,
    cuda_fallback_reason: FallbackReason,
    metal_route: LoweringRoute,
    metal_candidate_route: LoweringRoute,
    metal_fallback_reason: FallbackReason,
};

pub const BenchmarkCase = struct {
    name: []const u8,
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    generated_kernel_id: []const u8,
    generated_source_path: []const u8,
    generated_source_fingerprint: u64,
    generated_ptx_path: []const u8,
    generated_ptx_command: []const u8,
    benchmark_command: []const u8,
    generated_ptx_arg: []const u8,
    handwritten_baseline: []const u8,
    correctness_tolerance_abs: f32,
    minimum_speedup: f64,
    correctness_evidence_path: []const u8,
    benchmark_evidence_path: []const u8,
    benchmark_mode: []const u8,
    target_fingerprint: u64,
    production_enabled: bool,
};

pub const MetalBenchmarkShape = enum {
    small,
    wide,
};

pub const MetalBenchmarkDims = struct {
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    tolerance_abs: f32,
};

pub const MetalProductionBenchmarkCase = struct {
    name: []const u8,
    kernel_id: []const u8,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    dispatch: quant_matmul.DispatchKind,
    shape: MetalBenchmarkShape,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    threads_per_threadgroup: usize,
    cols_per_threadgroup: usize,
    tolerance_abs: f32,
    generated_source_path: []const u8,
    generated_source_fingerprint: u64,
    check_command: []const u8,
    production_kernel_id: []const u8,
    benchmark_command: []const u8,
};

const BenchmarkEvidence = struct {
    kernel_id: []const u8,
    generated_source_path: []const u8,
    generated_source_fingerprint: u64,
    generated_ptx_path: []const u8,
    generated_ptx_command: []const u8,
    benchmark_command: []const u8,
    correctness_evidence_path: []const u8,
    benchmark_evidence_path: []const u8,
    benchmark_mode: []const u8,
    target_fingerprint: u64,
    repeat_runs: usize,
    correctness_passed: bool,
    benchmark_passed: bool,
    measured_speedup: f64,
};

const CudaQ4EvidenceShape = struct {
    label: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    baseline_ns: u64,
    generated_ns: u64,
    speedup: f64,
};

const CudaQ4EvidenceFile = struct {
    schema: []const u8,
    kernel_id: []const u8,
    generated_source_path: []const u8,
    generated_source_fingerprint: u64,
    generated_ptx_path: []const u8,
    generated_ptx_command: []const u8,
    benchmark_command: []const u8,
    correctness_evidence_path: []const u8,
    benchmark_evidence_path: []const u8,
    benchmark_mode: []const u8,
    repeat_runs: usize,
    runtime_min_in_dim: usize,
    production_enabled: bool,
    correctness_passed: bool,
    benchmark_passed: bool,
    promotion_ready: bool,
    measured_speedup: f64,
    worst_shape_speedup: f64,
    minimum_speedup: f64,
    correctness_tolerance_abs: f64,
    baseline_kernel: []const u8,
    warmup_iters: usize,
    measure_iters: usize,
    shapes: []const CudaQ4EvidenceShape,
};

const MetalRuntimeEvidence = struct {
    kernel_id: []const u8,
    source_path: []const u8,
    source_fingerprint: u64,
    check_command: []const u8,
    runtime_evidence_command: []const u8,
    promotion_check_command: []const u8,
    repeat_runs: usize,
    correctness_passed: bool,
    generated_route_checked: bool = false,
    provider_route_checked: bool = false,
    benchmark_passed: bool,
    measured_speedup: f64,
    // Minimum speedup across every promoted case in the evidence file.
    minimum_repeat_speedup: f64,
    production_enabled: bool,
    promotion_ready: bool,
    // Seven production-qualified routes predate reproducible provenance. Their
    // explicit exception preserves qualification history only; runtime rollout
    // stays default-off until attested_v1 evidence is checked in.
    provenance_status: []const u8 = metal_evidence_provenance_legacy_unattested,
    provenance_blocker: []const u8 = metal_blocker_missing_reproducible_provenance,
    legacy_production_exception: bool = false,
    source_commit: []const u8 = "",
    source_tree_clean: bool = false,
    source_status_sha256: []const u8 = "",
    host_os: []const u8 = "",
    host_arch: []const u8 = "",
    accelerator_name: []const u8 = "",
    metal_compiler_version: []const u8 = "",
    zig_version: []const u8 = "",
    recorded_at_utc: []const u8 = "",
};

pub const metal_evidence_provenance_legacy_unattested = "legacy_unattested";
pub const metal_evidence_provenance_attested_v1 = "attested_v1";
pub const metal_blocker_missing_reproducible_provenance = "missing_reproducible_provenance";
pub const metal_blocker_dirty_source_tree = "dirty_source_tree";
pub const metal_blocker_invalid_legacy_provenance_exception = "invalid_legacy_provenance_exception";
pub const metal_blocker_invalid_attested_provenance = "invalid_attested_provenance";

pub const MetalPromotionBlockerEvidence = struct {
    kernel_id: []const u8,
    blocker: []const u8,
    evidence_path: []const u8 = "",
    requires_production_regression_clear: bool = false,
};

const BenchmarkManifest = struct {
    schema: []const u8,
    benchmark_count: usize,
    evidence_count: usize,
    metal_evidence_count: usize,
    metal_legacy_unattested_evidence_count: usize,
    metal_legacy_unattested_evidence_is_release_blocker: bool,
    metal_future_promotions_require_attested_provenance: bool,
    metal_promotion_warmup_repeat_runs: u32,
    metal_production_regression_expected_kernel_count: usize,
    metal_production_regression_expected_case_count: usize,
    metal_production_regression_expected_route_ready_count: usize,
    metal_production_regression_case_fingerprint: u64,
    metal_production_regression_build_command: []const u8,
    metal_production_regression_evidence_command: []const u8,
    metal_production_regression_cases: []const MetalProductionBenchmarkManifestRecord,
    benchmarks: []const BenchmarkManifestRecord,
    evidence_records: []const BenchmarkEvidence,
    metal_evidence_records: []const MetalRuntimeEvidence,
};

const BenchmarkManifestRecord = struct {
    name: []const u8,
    backend: []const u8,
    format: []const u8,
    row_bucket: []const u8,
    epilogue: []const u8,
    dispatch: []const u8,
    generated_kernel_id: []const u8,
    generated_source_path: []const u8,
    generated_source_fingerprint: u64,
    generated_ptx_path: []const u8,
    generated_ptx_command: []const u8,
    benchmark_command: []const u8,
    generated_ptx_arg: []const u8,
    handwritten_baseline: []const u8,
    correctness_tolerance_abs: f32,
    minimum_speedup: f64,
    measured_speedup: f64,
    correctness_evidence_path: []const u8,
    benchmark_evidence_path: []const u8,
    benchmark_mode: []const u8,
    target_fingerprint: u64,
    production_enabled: bool,
    promotion_ready: bool,
    promotion_blocker: []const u8,
};

const MetalProductionBenchmarkManifestRecord = struct {
    name: []const u8,
    kernel_id: []const u8,
    format: []const u8,
    row_bucket: []const u8,
    epilogue: []const u8,
    dispatch: []const u8,
    shape: []const u8,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    threads_per_threadgroup: usize,
    cols_per_threadgroup: usize,
    tolerance_abs: f32,
    generated_source_path: []const u8,
    generated_source_fingerprint: u64,
    check_command: []const u8,
    production_kernel_id: []const u8,
    benchmark_command: []const u8,
};

const ArtifactManifest = struct {
    schema: []const u8,
    artifact_count: usize,
    registry_artifact_count: usize,
    checked_in_metal_evidence_count: usize,
    metal_legacy_unattested_evidence_count: usize,
    metal_legacy_unattested_evidence_is_release_blocker: bool,
    metal_future_promotions_require_attested_provenance: bool,
    metal_promotion_blocker_evidence_count: usize,
    metal_promotion_blocker_evidence_path_count: usize,
    metal_promotion_blocker_evidence_expected_case_count: usize,
    metal_promotion_blocker_evidence_expected_route_ready_count: usize,
    metal_promotion_blocker_check_command_count: usize,
    metal_promotion_blocker_skipped_no_path_count: usize,
    metal_promotion_blocker_cleared_requires_checked_in_evidence: bool,
    metal_promotion_blocker_speedup_gate_missing_count: usize,
    metal_promotion_blocker_unstable_benchmark_timing_count: usize,
    metal_promotion_blocker_runtime_route_only_count: usize,
    metal_promotion_blocker_unsupported_handwritten_count: usize,
    metal_unsupported_handwritten_baseline_blocks_promotion: bool,
    metal_unsupported_handwritten_baseline_uses_runtime_route_all_evidence: bool,
    metal_unsupported_handwritten_baseline_has_promotion_evidence_path: bool,
    metal_local_check_command: []const u8,
    metal_model_local_check_command: []const u8,
    metal_model_generated_route_check_command: []const u8,
    metal_model_generated_q8_0_small_batch_min: usize,
    metal_model_generated_q4_0_small_batch_min: usize,
    metal_industry_local_check_command: []const u8,
    metal_runtime_route_all_build_command: []const u8,
    metal_runtime_route_all_evidence_command: []const u8,
    metal_runtime_route_all_check_command: []const u8,
    metal_runtime_route_all_expected_case_count: usize,
    metal_runtime_route_all_expected_route_ready_count: usize,
    metal_runtime_route_all_expected_provider_route_count: usize,
    metal_production_regression_expected_kernel_count: usize,
    metal_production_regression_expected_case_count: usize,
    metal_production_regression_expected_route_ready_count: usize,
    metal_promotion_warmup_repeat_runs: u32,
    metal_production_regression_route_ready_is_hard_gate: bool,
    metal_production_regression_missing_provider_route_is_hard_gate: bool,
    metal_production_regression_speedup_gate_missing_is_hard_gate: bool,
    metal_production_regression_unstable_benchmark_timing_is_hard_gate: bool,
    metal_production_regression_build_command: []const u8,
    metal_production_regression_evidence_command: []const u8,
    metal_blocker_strict_check_command: []const u8,
    registry_artifacts: []const ArtifactRegistryManifestRecord,
    artifacts: []const ArtifactManifestRecord,
    metal_evidence_records: []const MetalRuntimeEvidence,
};

const ArtifactRegistryManifestRecord = struct {
    backend: []const u8,
    op_kind: []const u8,
    kernel_id: []const u8,
    source_path: []const u8,
    generated_source_fingerprint: u64,
    check_command: []const u8,
    production_enabled: bool,
    runtime_default_enabled: bool,
    runtime_min_in_dim: usize,
    cuda_kernel: ?[]const u8 = null,
    cuda_launch: ?cuda_renderer.LaunchMetadata = null,
    cuda_serial_kernel: ?[]const u8 = null,
    cuda_serial_launch: ?cuda_renderer.LaunchMetadata = null,
    cuda_reduction_kernel: ?[]const u8 = null,
    cuda_reduction_launch: ?cuda_renderer.LaunchMetadata = null,
    cuda_tiled64_kernel: ?[]const u8 = null,
    cuda_tiled64_launch: ?cuda_renderer.LaunchMetadata = null,
    cuda_tiled64_max_kv_tokens: ?u16 = null,
    cuda_attention_source_id: ?[]const u8 = null,
    cuda_attention_split_count: ?u8 = null,
    cuda_attention_workspace: ?cuda_renderer.AttentionWorkspaceLayout = null,
    cuda_splitk_online_workspace: ?cuda_renderer.SplitkOnlineDecodeWorkspaceLayout = null,
    matmul: ?MatmulArtifactManifestOp = null,
    microkernel: ?MicrokernelArtifactManifestOp = null,
    attention: ?AttentionArtifactManifestOp = null,
};

const MatmulArtifactManifestOp = struct {
    format: []const u8,
    row_bucket: []const u8,
    epilogue: []const u8,
};

const MicrokernelArtifactManifestOp = struct {
    kind: []const u8,
    schedule: KernelSchedule,
};

const AttentionArtifactManifestOp = struct {
    kind: []const u8,
    head_dim: u16,
    schedule: KernelSchedule,
};

const ArtifactManifestRecord = struct {
    backend: []const u8,
    format: []const u8,
    row_bucket: []const u8,
    epilogue: []const u8,
    kernel_id: []const u8,
    source_path: []const u8,
    generated_source_fingerprint: u64,
    check_command: []const u8,
    runtime_evidence_command: []const u8,
    runtime_route_evidence_command: []const u8,
    promotion_evidence_command: []const u8,
    promotion_check_command: []const u8,
    promotion_policy: []const u8,
    promotion_target_fingerprint: u64,
    production_enabled: bool,
    runtime_default_enabled: bool,
    runtime_wired: bool,
    runtime_gate_env: []const u8,
    runtime_min_in_dim: usize,
    cuda_kernel: ?[]const u8,
    cuda_launch: ?cuda_renderer.LaunchMetadata,
    production_regression_checked: bool,
    production_regression_command: []const u8,
    metal_promotion_min_speedup: f64,
    metal_promotion_repeat_runs: usize,
    metal_promotion_warmup_repeat_runs: u32,
    candidate_status: []const u8,
    promotion_ready: bool,
    promotion_blocker: []const u8,
    promotion_blocker_evidence_path: []const u8,
    promotion_blocker_check_command: []const u8,
    promotion_blocker_requires_production_regression_clear: bool,
};

const SpecManifest = struct {
    schema: []const u8,
    format_count: usize,
    specs: []const SpecManifestRecord,
};

const SpecManifestRecord = struct {
    format: []const u8,
    reference_tensor_type: []const u8,
    block_values: usize,
    block_bytes: usize,
    block_fields: []const BlockField,
    decode_ops: []const DecodeOp,
    supported_schedules: []const quant_matmul.DispatchKind,
    supported_epilogues: []const Epilogue,
    supported_backends: []const Backend,
    accumulator_dtype: []const u8,
    output_dtype: []const u8,
};

const ConformanceManifest = struct {
    schema: []const u8,
    case_count: usize,
    format_count: usize,
    row_bucket_count: usize,
    epilogue_count: usize,
    backend_count: usize,
    cuda_route_summary: PlanCounters,
    metal_route_summary: PlanCounters,
    cases: []const ConformanceManifestRecord,
};

const ConformanceManifestRecord = struct {
    format: []const u8,
    block_values: usize,
    block_bytes: usize,
    accumulator_dtype: []const u8,
    output_dtype: []const u8,
    row_bucket: []const u8,
    epilogue: []const u8,
    dispatch: []const u8,
    tile_rows: usize,
    tile_cols: usize,
    vector_width: usize,
    threads_per_block: usize,
    shared_memory_bytes: usize,
    register_pressure_hint: u8,
    tensor_core_eligible: bool,
    cuda_candidate_tile_rows: usize,
    cuda_candidate_tile_cols: usize,
    cuda_candidate_threads_per_block: usize,
    metal_candidate_tile_rows: usize,
    metal_candidate_tile_cols: usize,
    metal_candidate_threads_per_block: usize,
    reference_supported: bool,
    reference_tensor_type: []const u8,
    tolerance_abs: f32,
    cuda_route: []const u8,
    cuda_candidate_route: []const u8,
    cuda_production_kernel_id: []const u8,
    cuda_candidate_kernel_id: []const u8,
    cuda_candidate_source_path: []const u8,
    cuda_candidate_source_fingerprint: u64,
    cuda_fallback_reason: []const u8,
    metal_route: []const u8,
    metal_candidate_route: []const u8,
    metal_production_kernel_id: []const u8,
    metal_candidate_kernel_id: []const u8,
    metal_candidate_source_path: []const u8,
    metal_candidate_source_fingerprint: u64,
    metal_fallback_reason: []const u8,
};

pub const MatmulArtifactOp = struct {
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    activation: ActivationEncoding = .f32,
    /// `null` means the kernel accepts the activation function at runtime.
    function: ?ActivationFunction = null,
    output: OutputEncoding = .f32,
};

pub const MicrokernelArtifactOp = struct {
    kind: MicrokernelKind,
    schedule: KernelSchedule,
};

pub const AttentionArtifactOp = struct {
    kind: AttentionKind,
    head_dim: u16 = 0,
    schedule: KernelSchedule,
};

/// Backend-independent operation descriptor used by the artifact registry.
/// Only the active operation carries format- or schedule-specific metadata, so
/// non-matmul routes cannot acquire meaning through placeholder quant fields.
pub const GeneratedOp = union(OpKind) {
    small_batch_matmul: MatmulArtifactOp,
    attention: AttentionArtifactOp,
    microkernel: MicrokernelArtifactOp,
};

pub const GeneratedArtifact = struct {
    backend: Backend,
    kernel_id: []const u8,
    source_path: []const u8,
    check_command: []const u8,
    runtime_evidence_command: []const u8 = "",
    promotion_evidence_command: []const u8 = "",
    promotion_check_command: []const u8 = "",
    production_enabled: bool,
    /// Runtime rollout is independent from evidence qualification. New routes
    /// fail closed until their normal model path has its own release evidence.
    runtime_default_enabled: bool = false,
    runtime_shape: RuntimeShapeConstraint = .{},
    cuda_kernel: ?cuda_renderer.KernelKind = null,
    cuda_attention_kernel: ?cuda_renderer.AttentionKernelKind = null,
    cuda_flash_prefill_kernel: ?cuda_renderer.FlashPrefillKernelKind = null,
    cuda_splitk_online_decode_kernel: ?cuda_renderer.SplitkOnlineDecodeKernelKind = null,
    op: GeneratedOp,

    pub fn opKind(self: GeneratedArtifact) OpKind {
        return std.meta.activeTag(self.op);
    }

    pub fn matmulOp(self: GeneratedArtifact) ?MatmulArtifactOp {
        return switch (self.op) {
            .small_batch_matmul => |op| op,
            else => null,
        };
    }

    pub fn microkernelOp(self: GeneratedArtifact) ?MicrokernelArtifactOp {
        return switch (self.op) {
            .microkernel => |op| op,
            else => null,
        };
    }

    pub fn attentionOp(self: GeneratedArtifact) ?AttentionArtifactOp {
        return switch (self.op) {
            .attention => |op| op,
            else => null,
        };
    }
};

/// Matmul-only compatibility view for the promotion, benchmark, and routing
/// machinery. Values are derived from `first_generated_artifacts`; this is not
/// an independently authored registry.
pub const GeneratedMatmulArtifact = struct {
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    activation: ActivationEncoding = .f32,
    function: ?ActivationFunction = null,
    output: OutputEncoding = .f32,
    kernel_id: []const u8,
    source_path: []const u8,
    check_command: []const u8,
    runtime_evidence_command: []const u8 = "",
    promotion_evidence_command: []const u8 = "",
    promotion_check_command: []const u8 = "",
    production_enabled: bool,
    runtime_default_enabled: bool = false,
    runtime_shape: RuntimeShapeConstraint = .{},
    cuda_kernel: ?cuda_renderer.KernelKind = null,
    cuda_attention_kernel: ?cuda_renderer.AttentionKernelKind = null,
    cuda_flash_prefill_kernel: ?cuda_renderer.FlashPrefillKernelKind = null,
    cuda_splitk_online_decode_kernel: ?cuda_renderer.SplitkOnlineDecodeKernelKind = null,

    pub fn opKind(_: GeneratedMatmulArtifact) OpKind {
        return .small_batch_matmul;
    }

    pub fn matmulOp(self: GeneratedMatmulArtifact) ?MatmulArtifactOp {
        return .{
            .format = self.format,
            .row_bucket = self.row_bucket,
            .epilogue = self.epilogue,
            .activation = self.activation,
            .function = self.function,
            .output = self.output,
        };
    }

    pub fn attentionOp(_: GeneratedMatmulArtifact) ?AttentionArtifactOp {
        return null;
    }

    pub fn asGeneratedArtifact(self: GeneratedMatmulArtifact) GeneratedArtifact {
        return .{
            .backend = self.backend,
            .kernel_id = self.kernel_id,
            .source_path = self.source_path,
            .check_command = self.check_command,
            .runtime_evidence_command = self.runtime_evidence_command,
            .promotion_evidence_command = self.promotion_evidence_command,
            .promotion_check_command = self.promotion_check_command,
            .production_enabled = self.production_enabled,
            .runtime_default_enabled = self.runtime_default_enabled,
            .runtime_shape = self.runtime_shape,
            .cuda_kernel = self.cuda_kernel,
            .cuda_attention_kernel = self.cuda_attention_kernel,
            .cuda_flash_prefill_kernel = self.cuda_flash_prefill_kernel,
            .cuda_splitk_online_decode_kernel = self.cuda_splitk_online_decode_kernel,
            .op = .{ .small_batch_matmul = .{
                .format = self.format,
                .row_bucket = self.row_bucket,
                .epilogue = self.epilogue,
                .activation = self.activation,
                .function = self.function,
                .output = self.output,
            } },
        };
    }
};

pub const RuntimeShapeConstraint = struct {
    min_in_dim: usize = 0,

    pub fn matches(self: RuntimeShapeConstraint, plan: quant_matmul.Plan) bool {
        return plan.in_dim >= self.min_in_dim;
    }
};

pub const QuantKernelCompileRequest = struct {
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
};

pub const QuantKernelCompiledSource = struct {
    request: QuantKernelCompileRequest,
    spec: QuantKernelSpec,
    ir: QuantKernelIR,
    lowering: QuantKernelLowering,
    artifact: GeneratedMatmulArtifact,
    source: []const u8,
    source_path: []const u8,
    check_command: []const u8,
    runtime_gate_env: ?[*:0]const u8,
    production_enabled: bool,
    cuda_render_plan: ?cuda_renderer.RenderPlan = null,
};

pub const EmittedCompiledSource = struct {
    data: []const u8,
    owned: bool = false,

    pub fn deinit(self: EmittedCompiledSource, allocator: std.mem.Allocator) void {
        if (self.owned) allocator.free(self.data);
    }
};

/// Runtime-facing source view for every typed artifact in the unified
/// registry. CUDA renderers allocate complete translation units; checked-in
/// Metal source is immutable and can be borrowed directly.
pub const RuntimeArtifactSource = struct {
    artifact: GeneratedArtifact,
    data: []const u8,
    owned: bool = false,

    pub fn deinit(self: RuntimeArtifactSource, allocator: std.mem.Allocator) void {
        if (self.owned) allocator.free(self.data);
    }
};

const CodecFormatCoverage = struct {
    tensor_type: tensor_types.KnownTensorType,
    format: quant_matmul.Format,
};

const RouteExpectation = struct {
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    dispatch: quant_matmul.DispatchKind,
    production_route: LoweringRoute,
    candidate_route: LoweringRoute,
    fallback_reason: FallbackReason,
};

const no_backends = [_]Backend{};

const q1_0_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "qs", .offset = 2, .bytes = 16 },
};

const i2_s_block_fields = [_]BlockField{
    .{ .name = "qs", .offset = 0, .bytes = 32 },
};

const i8_s_block_fields = [_]BlockField{
    .{ .name = "q", .offset = 0, .bytes = 1 },
};

const iq4_nl_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "qs", .offset = 2, .bytes = 16 },
};

const iq4_xs_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "scales_h", .offset = 2, .bytes = 2 },
    .{ .name = "scales_l", .offset = 4, .bytes = 4 },
    .{ .name = "qs", .offset = 8, .bytes = 128 },
};

const mxfp4_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 1 },
    .{ .name = "qs", .offset = 1, .bytes = 16 },
};

const nvfp4_block_fields = [_]BlockField{
    .{ .name = "scales", .offset = 0, .bytes = 4 },
    .{ .name = "qs", .offset = 4, .bytes = 32 },
};

const tq1_0_block_fields = [_]BlockField{
    .{ .name = "qs", .offset = 0, .bytes = 48 },
    .{ .name = "qh", .offset = 48, .bytes = 4 },
    .{ .name = "d", .offset = 52, .bytes = 2 },
};

const tq2_0_block_fields = [_]BlockField{
    .{ .name = "qs", .offset = 0, .bytes = 64 },
    .{ .name = "d", .offset = 64, .bytes = 2 },
};

const iq2_xxs_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "qs", .offset = 2, .bytes = 64 },
};

const iq2_xs_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "qs", .offset = 2, .bytes = 64 },
    .{ .name = "scales", .offset = 66, .bytes = 8 },
};

const iq2_s_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "qs", .offset = 2, .bytes = 64 },
    .{ .name = "qh", .offset = 66, .bytes = 8 },
    .{ .name = "scales", .offset = 74, .bytes = 8 },
};

const iq3_xxs_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "qs", .offset = 2, .bytes = 96 },
};

const iq3_s_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "qs", .offset = 2, .bytes = 64 },
    .{ .name = "qh", .offset = 66, .bytes = 8 },
    .{ .name = "signs", .offset = 74, .bytes = 32 },
    .{ .name = "scales", .offset = 106, .bytes = 4 },
};

const iq1_s_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "qs", .offset = 2, .bytes = 32 },
    .{ .name = "qh", .offset = 34, .bytes = 16 },
};

const iq1_m_block_fields = [_]BlockField{
    .{ .name = "qs", .offset = 0, .bytes = 32 },
    .{ .name = "qh", .offset = 32, .bytes = 16 },
    .{ .name = "scales", .offset = 48, .bytes = 8 },
};

const q4_0_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "qs", .offset = 2, .bytes = 16 },
};

const q4_1_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "m", .offset = 2, .bytes = 2 },
    .{ .name = "qs", .offset = 4, .bytes = 16 },
};

const q5_0_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "qh", .offset = 2, .bytes = 4 },
    .{ .name = "qs", .offset = 6, .bytes = 16 },
};

const q5_1_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "m", .offset = 2, .bytes = 2 },
    .{ .name = "qh", .offset = 4, .bytes = 4 },
    .{ .name = "qs", .offset = 8, .bytes = 16 },
};

const q2_k_block_fields = [_]BlockField{
    .{ .name = "scales", .offset = 0, .bytes = 16 },
    .{ .name = "d", .offset = 16, .bytes = 2 },
    .{ .name = "dmin", .offset = 18, .bytes = 2 },
    .{ .name = "qs", .offset = 20, .bytes = 64 },
};

const q3_k_block_fields = [_]BlockField{
    .{ .name = "hmask", .offset = 0, .bytes = 32 },
    .{ .name = "qs", .offset = 32, .bytes = 64 },
    .{ .name = "scales", .offset = 96, .bytes = 12 },
    .{ .name = "d", .offset = 108, .bytes = 2 },
};

const q4_k_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "dmin", .offset = 2, .bytes = 2 },
    .{ .name = "scales", .offset = 4, .bytes = 12 },
    .{ .name = "qs", .offset = 16, .bytes = 128 },
};

const q5_k_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "dmin", .offset = 2, .bytes = 2 },
    .{ .name = "scales", .offset = 4, .bytes = 12 },
    .{ .name = "qh", .offset = 16, .bytes = 32 },
    .{ .name = "ql", .offset = 48, .bytes = 128 },
};

const q6_k_block_fields = [_]BlockField{
    .{ .name = "ql", .offset = 0, .bytes = 128 },
    .{ .name = "qh", .offset = 128, .bytes = 64 },
    .{ .name = "scales", .offset = 192, .bytes = 16 },
    .{ .name = "d", .offset = 208, .bytes = 2 },
};

const q8_0_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "qs", .offset = 2, .bytes = 32 },
};

const q8_1_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 2 },
    .{ .name = "sum", .offset = 2, .bytes = 2 },
    .{ .name = "qs", .offset = 4, .bytes = 32 },
};

const q8_k_block_fields = [_]BlockField{
    .{ .name = "d", .offset = 0, .bytes = 4 },
    .{ .name = "qs", .offset = 4, .bytes = 256 },
    .{ .name = "bsums", .offset = 260, .bytes = 32 },
};

const q4_0_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q4_0 {d,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d)" },
    .{ .kind = .extract_quant_lane, .expression = "q = low_or_high_nibble(qs[lane]) - 8" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const q4_1_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q4_1 {d,m,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d)" },
    .{ .kind = .extract_min, .expression = "min = fp16(m)" },
    .{ .kind = .extract_quant_lane, .expression = "q = low_or_high_nibble(qs[lane])" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q + min" },
};

const q5_0_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q5_0 {d,qh,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d)" },
    .{ .kind = .extract_quant_lane, .expression = "q = (low_or_high_nibble(qs[lane]) | high_bit(qh[lane])) - 16" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const q5_1_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q5_1 {d,m,qh,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d)" },
    .{ .kind = .extract_min, .expression = "min = fp16(m)" },
    .{ .kind = .extract_quant_lane, .expression = "q = low_or_high_nibble(qs[lane]) | high_bit(qh[lane])" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q + min" },
};

const q1_0_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q1_0 {d,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d)" },
    .{ .kind = .extract_quant_lane, .expression = "q = bit(qs[lane]) ? 1 : -1" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const i2_s_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_i2_s {qs}" },
    .{ .kind = .extract_scale, .expression = "scale = 1.0" },
    .{ .kind = .extract_quant_lane, .expression = "q = ternary(two_bit(qs[group]))" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const i8_s_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_i8_s {q}" },
    .{ .kind = .extract_scale, .expression = "scale = 1.0" },
    .{ .kind = .extract_quant_lane, .expression = "q = i8(q)" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const iq4_nl_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_iq4_nl {d,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d)" },
    .{ .kind = .extract_quant_lane, .expression = "q = iq4_nl_table[low_or_high_nibble(qs[lane])]" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const iq4_xs_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_iq4_xs {d,scales_h,scales_l,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * (packed_i6_scale[group] - 32)" },
    .{ .kind = .extract_quant_lane, .expression = "q = iq4_nl_table[low_or_high_nibble(qs[lane])]" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const mxfp4_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_mxfp4 {d,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = e8m0_half(d)" },
    .{ .kind = .extract_quant_lane, .expression = "q = e2m1_table[low_or_high_nibble(qs[lane])]" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const nvfp4_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_nvfp4 {scales,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = ue4m3_half(scales[subblock])" },
    .{ .kind = .extract_quant_lane, .expression = "q = e2m1_table[low_or_high_nibble(qs[lane])]" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const tq1_0_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_tq1_0 {qs,qh,d}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d)" },
    .{ .kind = .extract_quant_lane, .expression = "q = ternary_base3(qs,qh,lane) - 1" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const tq2_0_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_tq2_0 {qs,d}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d)" },
    .{ .kind = .extract_quant_lane, .expression = "q = two_bit(qs[lane]) - 1" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const iq2_xxs_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_iq2_xxs {d,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * packed_4bit_scale[group] * 0.25" },
    .{ .kind = .extract_quant_lane, .expression = "q = iq2_xxs_grid[packed_grid] * sign_mask" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const iq2_xs_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_iq2_xs {d,qs,scales}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * packed_nibble_scale[pair] * 0.25" },
    .{ .kind = .extract_quant_lane, .expression = "q = iq2_xs_grid[packed_grid] * sign_mask" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const iq2_s_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_iq2_s {d,qs,qh,scales}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * packed_nibble_scale[pair] * 0.25" },
    .{ .kind = .extract_quant_lane, .expression = "q = iq2_s_grid[grid_low|high_bits] * sign_bits" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const iq3_xxs_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_iq3_xxs {d,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * packed_4bit_scale[group] * 0.5" },
    .{ .kind = .extract_quant_lane, .expression = "q = iq3_xxs_grid[grid_index] * sign_mask" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const iq3_s_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_iq3_s {d,qs,qh,signs,scales}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * packed_nibble_scale[pair]" },
    .{ .kind = .extract_quant_lane, .expression = "q = iq3_s_grid[grid_low|high_bits] * sign_bits" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const iq1_s_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_iq1_s {d,qs,qh}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * packed_scale[group]" },
    .{ .kind = .extract_quant_lane, .expression = "q = iq1_s_grid[grid_low|high_bits] + delta" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const iq1_m_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_iq1_m {qs,qh,scales}" },
    .{ .kind = .extract_scale, .expression = "scale = packed_fp16(scales) * subscale[group]" },
    .{ .kind = .extract_quant_lane, .expression = "q = iq1_s_grid[grid_low|high_bits] + delta" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const q2_k_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q2_K {scales,d,dmin,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * packed_scale[subblock]" },
    .{ .kind = .extract_min, .expression = "min = fp16(dmin) * packed_min[subblock]" },
    .{ .kind = .extract_quant_lane, .expression = "q = two_bit(qs[lane])" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q - min" },
};

const q3_k_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q3_K {hmask,qs,scales,d}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * packed_i6_scale[subblock]" },
    .{ .kind = .extract_quant_lane, .expression = "q = two_bit(qs[lane]) | high_bit(hmask[lane])" },
    .{ .kind = .dequant_lane, .expression = "value = scale * (q - 4)" },
};

const q4_k_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q4_K {d,dmin,scales,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * packed_scale[subblock]" },
    .{ .kind = .extract_min, .expression = "min = fp16(dmin) * packed_min[subblock]" },
    .{ .kind = .extract_quant_lane, .expression = "q = low_or_high_nibble(qs[lane])" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q - min" },
};

const q5_k_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q5_K {d,dmin,scales,qh,ql}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * packed_scale[subblock]" },
    .{ .kind = .extract_min, .expression = "min = fp16(dmin) * packed_min[subblock]" },
    .{ .kind = .extract_quant_lane, .expression = "q = low_or_high_nibble(ql[lane]) | high_bit(qh[lane])" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q - min" },
};

const q6_k_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q6_K {ql,qh,scales,d}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d) * i8(scales[subblock])" },
    .{ .kind = .extract_quant_lane, .expression = "q = (low4(ql[lane]) | high2(qh[lane])) - 32" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const q8_0_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q8_0 {d,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d)" },
    .{ .kind = .extract_quant_lane, .expression = "q = i8(qs[lane])" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const q8_1_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q8_1 {d,sum,qs}" },
    .{ .kind = .extract_scale, .expression = "scale = fp16(d)" },
    .{ .kind = .extract_quant_lane, .expression = "q = i8(qs[lane])" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const q8_k_decode_ops = [_]DecodeOp{
    .{ .kind = .load_packed_bytes, .expression = "load block_q8_K {d,qs,bsums}" },
    .{ .kind = .extract_scale, .expression = "scale = f32(d)" },
    .{ .kind = .extract_quant_lane, .expression = "q = i8(qs[lane])" },
    .{ .kind = .dequant_lane, .expression = "value = scale * q" },
};

const codec_format_coverage = [_]CodecFormatCoverage{
    .{ .tensor_type = .Q1_0, .format = .q1_0 },
    .{ .tensor_type = .I2_S, .format = .i2_s },
    .{ .tensor_type = .I8_S, .format = .i8_s },
    .{ .tensor_type = .Q4_0, .format = .q4_0 },
    .{ .tensor_type = .Q4_1, .format = .q4_1 },
    .{ .tensor_type = .Q5_0, .format = .q5_0 },
    .{ .tensor_type = .Q5_1, .format = .q5_1 },
    .{ .tensor_type = .Q8_0, .format = .q8_0 },
    .{ .tensor_type = .Q8_1, .format = .q8_1 },
    .{ .tensor_type = .Q2_K, .format = .q2_k },
    .{ .tensor_type = .Q3_K, .format = .q3_k },
    .{ .tensor_type = .Q4_K, .format = .q4_k },
    .{ .tensor_type = .Q5_K, .format = .q5_k },
    .{ .tensor_type = .Q6_K, .format = .q6_k },
    .{ .tensor_type = .Q8_K, .format = .q8_k },
    .{ .tensor_type = .TQ1_0, .format = .tq1_0 },
    .{ .tensor_type = .TQ2_0, .format = .tq2_0 },
    .{ .tensor_type = .IQ2_XXS, .format = .iq2_xxs },
    .{ .tensor_type = .IQ2_XS, .format = .iq2_xs },
    .{ .tensor_type = .IQ2_S, .format = .iq2_s },
    .{ .tensor_type = .IQ3_XXS, .format = .iq3_xxs },
    .{ .tensor_type = .IQ3_S, .format = .iq3_s },
    .{ .tensor_type = .IQ1_S, .format = .iq1_s },
    .{ .tensor_type = .IQ1_M, .format = .iq1_m },
    .{ .tensor_type = .IQ4_NL, .format = .iq4_nl },
    .{ .tensor_type = .IQ4_XS, .format = .iq4_xs },
    .{ .tensor_type = .MXFP4, .format = .mxfp4 },
    .{ .tensor_type = .NVFP4, .format = .nvfp4 },
};
const descriptor_formats = buildDescriptorFormats();
const first_formats = descriptor_formats;
const first_row_buckets = [_]quant_matmul.RowBucket{ .rows_1, .rows_2_8, .rows_9_64, .rows_65_plus };
const first_schedules = [_]quant_matmul.DispatchKind{ .mmv, .small_batch, .mm };
const first_epilogues = [_]Epilogue{ .none, .bias, .bias_gelu, .pair };
const q8_0_epilogues = [_]Epilogue{ .none, .bias, .bias_gelu, .pair, .relu };
const coverage_epilogues = [_]Epilogue{ .none, .bias, .bias_gelu, .pair, .triple, .relu, .gelu, .add, .argmax, .pair_activation, .gated_down };
const no_bias_epilogues = [_]Epilogue{.none};
const q4_0_epilogues = [_]Epilogue{ .none, .pair, .argmax, .pair_activation, .gated_down };
const q2_k_epilogues = [_]Epilogue{ .none, .bias, .bias_gelu };
const q3_k_epilogues = [_]Epilogue{ .none, .bias, .bias_gelu };
const k_quant_epilogues = [_]Epilogue{ .none, .bias, .bias_gelu };
const q6_k_epilogues = [_]Epilogue{ .none, .bias, .bias_gelu, .argmax };
const first_backends = [_]Backend{ .cuda, .metal };
const metal_backends = [_]Backend{.metal};

const q4_0_spec = QuantKernelSpec{
    .format = .q4_0,
    .block_values = blockValuesForFormat(.q4_0),
    .block_bytes = blockBytesForFormat(.q4_0),
    .block_fields = &q4_0_block_fields,
    .decode_ops = &q4_0_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &q4_0_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &first_backends,
};

const q4_1_spec = QuantKernelSpec{
    .format = .q4_1,
    .block_values = blockValuesForFormat(.q4_1),
    .block_bytes = blockBytesForFormat(.q4_1),
    .block_fields = &q4_1_block_fields,
    .decode_ops = &q4_1_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &metal_backends,
};

const q5_0_spec = QuantKernelSpec{
    .format = .q5_0,
    .block_values = blockValuesForFormat(.q5_0),
    .block_bytes = blockBytesForFormat(.q5_0),
    .block_fields = &q5_0_block_fields,
    .decode_ops = &q5_0_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &metal_backends,
};

const q5_1_spec = QuantKernelSpec{
    .format = .q5_1,
    .block_values = blockValuesForFormat(.q5_1),
    .block_bytes = blockBytesForFormat(.q5_1),
    .block_fields = &q5_1_block_fields,
    .decode_ops = &q5_1_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &metal_backends,
};

const q1_0_spec = QuantKernelSpec{
    .format = .q1_0,
    .block_values = blockValuesForFormat(.q1_0),
    .block_bytes = blockBytesForFormat(.q1_0),
    .block_fields = &q1_0_block_fields,
    .decode_ops = &q1_0_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &metal_backends,
};

const i2_s_spec = QuantKernelSpec{
    .format = .i2_s,
    .block_values = blockValuesForFormat(.i2_s),
    .block_bytes = blockBytesForFormat(.i2_s),
    .block_fields = &i2_s_block_fields,
    .decode_ops = &i2_s_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const i8_s_spec = QuantKernelSpec{
    .format = .i8_s,
    .block_values = blockValuesForFormat(.i8_s),
    .block_bytes = blockBytesForFormat(.i8_s),
    .block_fields = &i8_s_block_fields,
    .decode_ops = &i8_s_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const iq4_nl_spec = QuantKernelSpec{
    .format = .iq4_nl,
    .block_values = blockValuesForFormat(.iq4_nl),
    .block_bytes = blockBytesForFormat(.iq4_nl),
    .block_fields = &iq4_nl_block_fields,
    .decode_ops = &iq4_nl_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const iq4_xs_spec = QuantKernelSpec{
    .format = .iq4_xs,
    .block_values = blockValuesForFormat(.iq4_xs),
    .block_bytes = blockBytesForFormat(.iq4_xs),
    .block_fields = &iq4_xs_block_fields,
    .decode_ops = &iq4_xs_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const mxfp4_spec = QuantKernelSpec{
    .format = .mxfp4,
    .block_values = blockValuesForFormat(.mxfp4),
    .block_bytes = blockBytesForFormat(.mxfp4),
    .block_fields = &mxfp4_block_fields,
    .decode_ops = &mxfp4_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const nvfp4_spec = QuantKernelSpec{
    .format = .nvfp4,
    .block_values = blockValuesForFormat(.nvfp4),
    .block_bytes = blockBytesForFormat(.nvfp4),
    .block_fields = &nvfp4_block_fields,
    .decode_ops = &nvfp4_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const tq1_0_spec = QuantKernelSpec{
    .format = .tq1_0,
    .block_values = blockValuesForFormat(.tq1_0),
    .block_bytes = blockBytesForFormat(.tq1_0),
    .block_fields = &tq1_0_block_fields,
    .decode_ops = &tq1_0_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const tq2_0_spec = QuantKernelSpec{
    .format = .tq2_0,
    .block_values = blockValuesForFormat(.tq2_0),
    .block_bytes = blockBytesForFormat(.tq2_0),
    .block_fields = &tq2_0_block_fields,
    .decode_ops = &tq2_0_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const iq2_xxs_spec = QuantKernelSpec{
    .format = .iq2_xxs,
    .block_values = blockValuesForFormat(.iq2_xxs),
    .block_bytes = blockBytesForFormat(.iq2_xxs),
    .block_fields = &iq2_xxs_block_fields,
    .decode_ops = &iq2_xxs_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const iq2_xs_spec = QuantKernelSpec{
    .format = .iq2_xs,
    .block_values = blockValuesForFormat(.iq2_xs),
    .block_bytes = blockBytesForFormat(.iq2_xs),
    .block_fields = &iq2_xs_block_fields,
    .decode_ops = &iq2_xs_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const iq2_s_spec = QuantKernelSpec{
    .format = .iq2_s,
    .block_values = blockValuesForFormat(.iq2_s),
    .block_bytes = blockBytesForFormat(.iq2_s),
    .block_fields = &iq2_s_block_fields,
    .decode_ops = &iq2_s_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const iq3_xxs_spec = QuantKernelSpec{
    .format = .iq3_xxs,
    .block_values = blockValuesForFormat(.iq3_xxs),
    .block_bytes = blockBytesForFormat(.iq3_xxs),
    .block_fields = &iq3_xxs_block_fields,
    .decode_ops = &iq3_xxs_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const iq3_s_spec = QuantKernelSpec{
    .format = .iq3_s,
    .block_values = blockValuesForFormat(.iq3_s),
    .block_bytes = blockBytesForFormat(.iq3_s),
    .block_fields = &iq3_s_block_fields,
    .decode_ops = &iq3_s_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const iq1_s_spec = QuantKernelSpec{
    .format = .iq1_s,
    .block_values = blockValuesForFormat(.iq1_s),
    .block_bytes = blockBytesForFormat(.iq1_s),
    .block_fields = &iq1_s_block_fields,
    .decode_ops = &iq1_s_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const iq1_m_spec = QuantKernelSpec{
    .format = .iq1_m,
    .block_values = blockValuesForFormat(.iq1_m),
    .block_bytes = blockBytesForFormat(.iq1_m),
    .block_fields = &iq1_m_block_fields,
    .decode_ops = &iq1_m_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &no_backends,
};

const q2_k_spec = QuantKernelSpec{
    .format = .q2_k,
    .block_values = blockValuesForFormat(.q2_k),
    .block_bytes = blockBytesForFormat(.q2_k),
    .block_fields = &q2_k_block_fields,
    .decode_ops = &q2_k_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &q2_k_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &metal_backends,
};

const q3_k_spec = QuantKernelSpec{
    .format = .q3_k,
    .block_values = blockValuesForFormat(.q3_k),
    .block_bytes = blockBytesForFormat(.q3_k),
    .block_fields = &q3_k_block_fields,
    .decode_ops = &q3_k_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &q3_k_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &metal_backends,
};

const q4_k_spec = QuantKernelSpec{
    .format = .q4_k,
    .block_values = blockValuesForFormat(.q4_k),
    .block_bytes = blockBytesForFormat(.q4_k),
    .block_fields = &q4_k_block_fields,
    .decode_ops = &q4_k_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &first_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &first_backends,
};

const q5_k_spec = QuantKernelSpec{
    .format = .q5_k,
    .block_values = blockValuesForFormat(.q5_k),
    .block_bytes = blockBytesForFormat(.q5_k),
    .block_fields = &q5_k_block_fields,
    .decode_ops = &q5_k_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &k_quant_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &metal_backends,
};

const q6_k_spec = QuantKernelSpec{
    .format = .q6_k,
    .block_values = blockValuesForFormat(.q6_k),
    .block_bytes = blockBytesForFormat(.q6_k),
    .block_fields = &q6_k_block_fields,
    .decode_ops = &q6_k_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &q6_k_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &first_backends,
};

const q8_0_spec = QuantKernelSpec{
    .format = .q8_0,
    .block_values = blockValuesForFormat(.q8_0),
    .block_bytes = blockBytesForFormat(.q8_0),
    .block_fields = &q8_0_block_fields,
    .decode_ops = &q8_0_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &q8_0_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &first_backends,
};

const q8_1_spec = QuantKernelSpec{
    .format = .q8_1,
    .block_values = blockValuesForFormat(.q8_1),
    .block_bytes = blockBytesForFormat(.q8_1),
    .block_fields = &q8_1_block_fields,
    .decode_ops = &q8_1_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &metal_backends,
};

const q8_k_spec = QuantKernelSpec{
    .format = .q8_k,
    .block_values = blockValuesForFormat(.q8_k),
    .block_bytes = blockBytesForFormat(.q8_k),
    .block_fields = &q8_k_block_fields,
    .decode_ops = &q8_k_decode_ops,
    .supported_schedules = &first_schedules,
    .supported_epilogues = &no_bias_epilogues,
    .accumulator_dtype = .f32,
    .output_dtype = .f32,
    .supported_backends = &metal_backends,
};

const ir_ops_basic = [_]IROp{
    .load_input_row,
    .load_quant_block,
    .decode_quant_lane,
    .multiply_accumulate,
    .reduce_accumulator,
    .write_output,
};

const ir_ops_bias = [_]IROp{
    .load_input_row,
    .load_quant_block,
    .decode_quant_lane,
    .multiply_accumulate,
    .reduce_accumulator,
    .apply_bias,
    .write_output,
};

const ir_ops_bias_gelu = [_]IROp{
    .load_input_row,
    .load_quant_block,
    .decode_quant_lane,
    .multiply_accumulate,
    .reduce_accumulator,
    .apply_bias,
    .apply_gelu,
    .write_output,
};

const ir_ops_argmax = [_]IROp{
    .load_input_row,
    .load_quant_block,
    .decode_quant_lane,
    .multiply_accumulate,
    .reduce_accumulator,
    .write_argmax_pair,
};

pub const first_lazy_benchmark_evidence_path = "src/ops/cuda/generated/evidence/q4_k_small_batch_bias_gelu_benchmark.json";
pub const first_lazy_cuda_source_fingerprint = sourceFingerprint(first_lazy_cuda_source);

pub const first_general_cuda_q4_k_mmv_kernel_id = "antfly_q4_k_mmv_f32_v1";
pub const first_general_cuda_q4_k_mmv_source_path = "src/ops/cuda/generated/quant_kernel_q4_k_mmv.cu";
pub const first_general_cuda_q4_k_mmv_ptx_path = "/tmp/antfly_q4_k_mmv_f32_v1.fatbin";
pub const first_general_cuda_q4_k_mmv_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_general_cuda_q4_k_mmv_source_path ++ " -o " ++ first_general_cuda_q4_k_mmv_ptx_path;
pub const first_general_cuda_q4_k_mmv_source_fingerprint = sourceFingerprint(first_general_cuda_q4_k_mmv_source);

pub const first_lazy_benchmark = BenchmarkCase{
    .name = "q4_k_small_batch_bias_gelu",
    .backend = .cuda,
    .format = .q4_k,
    .row_bucket = .rows_2_8,
    .epilogue = .bias_gelu,
    .generated_kernel_id = "antfly_q4_k_small_batch_bias_gelu_f32_v1",
    .generated_source_path = "src/ops/cuda/generated/quant_kernel_q4_k_small_batch_bias_gelu.cu",
    .generated_source_fingerprint = first_lazy_cuda_source_fingerprint,
    .generated_ptx_path = "/tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.fatbin",
    .generated_ptx_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " src/ops/cuda/generated/quant_kernel_q4_k_small_batch_bias_gelu.cu -o /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.fatbin",
    .benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-lazy-target --quant-compiler-generated-ptx /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.fatbin --quant-compiler-repeat-runs 3 --quant-compiler-evidence-out " ++ first_lazy_benchmark_evidence_path,
    .generated_ptx_arg = "--quant-compiler-generated-ptx",
    .handwritten_baseline = "termite_linear_q4_k_bias_gelu_f32_tile4_r2",
    .correctness_tolerance_abs = 0.01,
    .minimum_speedup = 1.0,
    .correctness_evidence_path = "",
    .benchmark_evidence_path = "",
    .benchmark_mode = "",
    .target_fingerprint = cuda_sm89_promotion_target_fingerprint,
    .production_enabled = false,
};

pub const first_general_cuda_q4_0_mmv_source_fingerprint = sourceFingerprint(first_general_cuda_q4_0_mmv_source);
pub const first_general_cuda_q4_0_mm_source_fingerprint = sourceFingerprint(first_general_cuda_q4_0_mm_source);

pub const first_q4_0_mmv_benchmark = BenchmarkCase{
    .name = "q4_0_mmv",
    .backend = .cuda,
    .format = .q4_0,
    .row_bucket = .rows_1,
    .epilogue = .none,
    .generated_kernel_id = first_general_cuda_q4_0_mmv_kernel_id,
    .generated_source_path = first_general_cuda_q4_0_mmv_source_path,
    .generated_source_fingerprint = first_general_cuda_q4_0_mmv_source_fingerprint,
    .generated_ptx_path = first_general_cuda_q4_0_mmv_ptx_path,
    .generated_ptx_command = first_general_cuda_q4_0_mmv_check_command,
    .benchmark_command = first_general_cuda_q4_0_mmv_benchmark_command,
    .generated_ptx_arg = "--quant-compiler-q4-0-mmv-ptx",
    .handwritten_baseline = "termite_linear_q4_0_f32_tile4",
    .correctness_tolerance_abs = 0.01,
    .minimum_speedup = 1.0,
    .correctness_evidence_path = first_general_cuda_q4_0_mmv_evidence_path,
    .benchmark_evidence_path = first_general_cuda_q4_0_mmv_evidence_path,
    .benchmark_mode = "sequential",
    .target_fingerprint = cuda_sm89_promotion_target_fingerprint,
    .production_enabled = true,
};

pub const first_q4_0_mm_benchmark = BenchmarkCase{
    .name = "q4_0_mm",
    .backend = .cuda,
    .format = .q4_0,
    .row_bucket = .rows_9_64,
    .epilogue = .none,
    .generated_kernel_id = first_general_cuda_q4_0_mm_kernel_id,
    .generated_source_path = first_general_cuda_q4_0_mm_source_path,
    .generated_source_fingerprint = first_general_cuda_q4_0_mm_source_fingerprint,
    .generated_ptx_path = first_general_cuda_q4_0_mm_ptx_path,
    .generated_ptx_command = first_general_cuda_q4_0_mm_check_command,
    .benchmark_command = first_general_cuda_q4_0_mm_benchmark_command,
    .generated_ptx_arg = "--quant-compiler-q4-0-mm-ptx",
    .handwritten_baseline = "termite_linear_q4_0_f32",
    .correctness_tolerance_abs = 0.01,
    .minimum_speedup = 1.0,
    .correctness_evidence_path = first_general_cuda_q4_0_mm_evidence_path,
    .benchmark_evidence_path = first_general_cuda_q4_0_mm_evidence_path,
    .benchmark_mode = "sequential",
    .target_fingerprint = cuda_sm89_promotion_target_fingerprint,
    .production_enabled = true,
};

pub const first_general_cuda_q4_0_pair_source_fingerprint = sourceFingerprint(first_general_cuda_q4_0_pair_source);

pub const first_q4_0_pair_benchmark = BenchmarkCase{
    .name = "q4_0_pair_mmv",
    .backend = .cuda,
    .format = .q4_0,
    .row_bucket = .rows_1,
    .epilogue = .pair,
    .generated_kernel_id = first_general_cuda_q4_0_pair_kernel_id,
    .generated_source_path = first_general_cuda_q4_0_pair_source_path,
    .generated_source_fingerprint = first_general_cuda_q4_0_pair_source_fingerprint,
    .generated_ptx_path = first_general_cuda_q4_0_pair_ptx_path,
    .generated_ptx_command = first_general_cuda_q4_0_pair_check_command,
    .benchmark_command = first_general_cuda_q4_0_pair_benchmark_command,
    .generated_ptx_arg = "--quant-compiler-q4-0-pair-ptx",
    .handwritten_baseline = "termite_linear_q4_0_pair_nobias_f32_tile4_w4",
    .correctness_tolerance_abs = 0.01,
    .minimum_speedup = 1.0,
    .correctness_evidence_path = first_general_cuda_q4_0_pair_evidence_path,
    .benchmark_evidence_path = first_general_cuda_q4_0_pair_evidence_path,
    .benchmark_mode = "sequential",
    .target_fingerprint = cuda_sm89_promotion_target_fingerprint,
    .production_enabled = true,
};

pub const first_general_cuda_q4_0_pair_q8_source_fingerprint = sourceFingerprint(first_general_cuda_q4_0_pair_q8_source);
pub const first_general_cuda_q4_0_down_q8_source_fingerprint = sourceFingerprint(first_general_cuda_q4_0_down_q8_source);

pub const first_q4_0_pair_q8_benchmark = BenchmarkCase{
    .name = "q4_0_pair_activation_q8_1",
    .backend = .cuda,
    .format = .q4_0,
    .row_bucket = .rows_1,
    .epilogue = .pair_activation,
    .generated_kernel_id = first_general_cuda_q4_0_pair_q8_kernel_id,
    .generated_source_path = first_general_cuda_q4_0_pair_q8_source_path,
    .generated_source_fingerprint = first_general_cuda_q4_0_pair_q8_source_fingerprint,
    .generated_ptx_path = first_general_cuda_q4_0_pair_q8_ptx_path,
    .generated_ptx_command = first_general_cuda_q4_0_pair_q8_check_command,
    .benchmark_command = first_general_cuda_q4_0_pair_q8_benchmark_command,
    .generated_ptx_arg = "--quant-compiler-q4-0-pair-q8-ptx",
    .handwritten_baseline = "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn",
    .correctness_tolerance_abs = 0.01,
    .minimum_speedup = 1.0,
    .correctness_evidence_path = first_general_cuda_q4_0_pair_q8_evidence_path,
    .benchmark_evidence_path = first_general_cuda_q4_0_pair_q8_evidence_path,
    .benchmark_mode = "sequential",
    .target_fingerprint = cuda_sm89_promotion_target_fingerprint,
    .production_enabled = true,
};

pub const first_q4_0_down_q8_benchmark = BenchmarkCase{
    .name = "q4_0_down_q8_1",
    .backend = .cuda,
    .format = .q4_0,
    .row_bucket = .rows_1,
    .epilogue = .gated_down,
    .generated_kernel_id = first_general_cuda_q4_0_down_q8_kernel_id,
    .generated_source_path = first_general_cuda_q4_0_down_q8_source_path,
    .generated_source_fingerprint = first_general_cuda_q4_0_down_q8_source_fingerprint,
    .generated_ptx_path = first_general_cuda_q4_0_down_q8_ptx_path,
    .generated_ptx_command = first_general_cuda_q4_0_down_q8_check_command,
    .benchmark_command = first_general_cuda_q4_0_down_q8_benchmark_command,
    .generated_ptx_arg = "--quant-compiler-q4-0-down-q8-ptx",
    .handwritten_baseline = "termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down",
    .correctness_tolerance_abs = 0.01,
    .minimum_speedup = 1.0,
    .correctness_evidence_path = first_general_cuda_q4_0_down_q8_evidence_path,
    .benchmark_evidence_path = first_general_cuda_q4_0_down_q8_evidence_path,
    .benchmark_mode = "sequential",
    .target_fingerprint = cuda_sm89_promotion_target_fingerprint,
    .production_enabled = true,
};

pub const first_benchmarks = [_]BenchmarkCase{ first_lazy_benchmark, first_q4_0_mmv_benchmark, first_q4_0_mm_benchmark, first_q4_0_pair_benchmark, first_q4_0_pair_q8_benchmark, first_q4_0_down_q8_benchmark };
// Measured on NVIDIA L4 (driver 580.159.03, CUDA 13.2 nvcc) via the recorded
// benchmark_command; geomean sequential speedup across the four Gemma4 E2B QAT
// shapes in quant_compiler_q4_0_dims (see the checked-in evidence JSONs).
const first_benchmark_evidence = [_]BenchmarkEvidence{
    .{
        .kernel_id = first_general_cuda_q4_0_mmv_kernel_id,
        .generated_source_path = first_general_cuda_q4_0_mmv_source_path,
        .generated_source_fingerprint = first_general_cuda_q4_0_mmv_source_fingerprint,
        .generated_ptx_path = first_general_cuda_q4_0_mmv_ptx_path,
        .generated_ptx_command = first_general_cuda_q4_0_mmv_check_command,
        .benchmark_command = first_general_cuda_q4_0_mmv_benchmark_command,
        .correctness_evidence_path = first_general_cuda_q4_0_mmv_evidence_path,
        .benchmark_evidence_path = first_general_cuda_q4_0_mmv_evidence_path,
        .benchmark_mode = "sequential",
        .target_fingerprint = cuda_sm89_promotion_target_fingerprint,
        .repeat_runs = 3,
        .correctness_passed = true,
        .benchmark_passed = true,
        .measured_speedup = 1.184101,
    },
    .{
        .kernel_id = first_general_cuda_q4_0_mm_kernel_id,
        .generated_source_path = first_general_cuda_q4_0_mm_source_path,
        .generated_source_fingerprint = first_general_cuda_q4_0_mm_source_fingerprint,
        .generated_ptx_path = first_general_cuda_q4_0_mm_ptx_path,
        .generated_ptx_command = first_general_cuda_q4_0_mm_check_command,
        .benchmark_command = first_general_cuda_q4_0_mm_benchmark_command,
        .correctness_evidence_path = first_general_cuda_q4_0_mm_evidence_path,
        .benchmark_evidence_path = first_general_cuda_q4_0_mm_evidence_path,
        .benchmark_mode = "sequential",
        .target_fingerprint = cuda_sm89_promotion_target_fingerprint,
        .repeat_runs = 3,
        .correctness_passed = true,
        .benchmark_passed = true,
        .measured_speedup = 5.911449,
    },
    .{
        .kernel_id = first_general_cuda_q4_0_pair_kernel_id,
        .generated_source_path = first_general_cuda_q4_0_pair_source_path,
        .generated_source_fingerprint = first_general_cuda_q4_0_pair_source_fingerprint,
        .generated_ptx_path = first_general_cuda_q4_0_pair_ptx_path,
        .generated_ptx_command = first_general_cuda_q4_0_pair_check_command,
        .benchmark_command = first_general_cuda_q4_0_pair_benchmark_command,
        .correctness_evidence_path = first_general_cuda_q4_0_pair_evidence_path,
        .benchmark_evidence_path = first_general_cuda_q4_0_pair_evidence_path,
        .benchmark_mode = "sequential",
        .target_fingerprint = cuda_sm89_promotion_target_fingerprint,
        .repeat_runs = 3,
        .correctness_passed = true,
        .benchmark_passed = true,
        .measured_speedup = 1.290115,
    },
    .{
        .kernel_id = first_general_cuda_q4_0_pair_q8_kernel_id,
        .generated_source_path = first_general_cuda_q4_0_pair_q8_source_path,
        .generated_source_fingerprint = first_general_cuda_q4_0_pair_q8_source_fingerprint,
        .generated_ptx_path = first_general_cuda_q4_0_pair_q8_ptx_path,
        .generated_ptx_command = first_general_cuda_q4_0_pair_q8_check_command,
        .benchmark_command = first_general_cuda_q4_0_pair_q8_benchmark_command,
        .correctness_evidence_path = first_general_cuda_q4_0_pair_q8_evidence_path,
        .benchmark_evidence_path = first_general_cuda_q4_0_pair_q8_evidence_path,
        .benchmark_mode = "sequential",
        .target_fingerprint = cuda_sm89_promotion_target_fingerprint,
        .repeat_runs = 3,
        .correctness_passed = true,
        .benchmark_passed = true,
        .measured_speedup = 1.247815,
    },
    .{
        .kernel_id = first_general_cuda_q4_0_down_q8_kernel_id,
        .generated_source_path = first_general_cuda_q4_0_down_q8_source_path,
        .generated_source_fingerprint = first_general_cuda_q4_0_down_q8_source_fingerprint,
        .generated_ptx_path = first_general_cuda_q4_0_down_q8_ptx_path,
        .generated_ptx_command = first_general_cuda_q4_0_down_q8_check_command,
        .benchmark_command = first_general_cuda_q4_0_down_q8_benchmark_command,
        .correctness_evidence_path = first_general_cuda_q4_0_down_q8_evidence_path,
        .benchmark_evidence_path = first_general_cuda_q4_0_down_q8_evidence_path,
        .benchmark_mode = "sequential",
        .target_fingerprint = cuda_sm89_promotion_target_fingerprint,
        .repeat_runs = 3,
        .correctness_passed = true,
        .benchmark_passed = true,
        .measured_speedup = 1.089808,
    },
};

const first_cuda_q4_evidence_json = [_][]const u8{
    @embedFile("../ops/cuda/generated/evidence/q4_0_mmv_benchmark.json"),
    @embedFile("../ops/cuda/generated/evidence/q4_0_mm_benchmark.json"),
    @embedFile("../ops/cuda/generated/evidence/q4_0_pair_benchmark.json"),
    @embedFile("../ops/cuda/generated/evidence/q4_0_pair_q8_benchmark.json"),
    @embedFile("../ops/cuda/generated/evidence/q4_0_down_q8_benchmark.json"),
};
const first_metal_runtime_evidence = [_]MetalRuntimeEvidence{
    .{
        .kernel_id = first_general_metal_q2_kernel_id,
        .source_path = first_general_metal_q2_source_path,
        .source_fingerprint = sourceFingerprint(first_general_metal_q2_source),
        .check_command = first_general_metal_q2_check_command,
        .runtime_evidence_command = first_general_metal_q2_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q2_promotion_check_command,
        .repeat_runs = metal_promotion_repeat_runs,
        .correctness_passed = true,
        .generated_route_checked = true,
        .provider_route_checked = true,
        .benchmark_passed = true,
        .measured_speedup = 2.080139,
        .minimum_repeat_speedup = 1.458720,
        .production_enabled = true,
        .promotion_ready = true,
        .legacy_production_exception = true,
    },
    .{
        .kernel_id = first_general_metal_q3_kernel_id,
        .source_path = first_general_metal_q3_source_path,
        .source_fingerprint = sourceFingerprint(first_general_metal_q3_source),
        .check_command = first_general_metal_q3_check_command,
        .runtime_evidence_command = first_general_metal_q3_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q3_promotion_check_command,
        .repeat_runs = metal_promotion_repeat_runs,
        .correctness_passed = true,
        .generated_route_checked = true,
        .provider_route_checked = true,
        .benchmark_passed = true,
        .measured_speedup = 3.461401,
        .minimum_repeat_speedup = 2.642921,
        .production_enabled = true,
        .promotion_ready = true,
        .legacy_production_exception = true,
    },
    .{
        .kernel_id = first_general_metal_q6_kernel_id,
        .source_path = first_general_metal_q6_source_path,
        .source_fingerprint = sourceFingerprint(first_general_metal_q6_source),
        .check_command = first_general_metal_q6_check_command,
        .runtime_evidence_command = first_general_metal_q6_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q6_promotion_check_command,
        .repeat_runs = metal_promotion_repeat_runs,
        .correctness_passed = true,
        .generated_route_checked = true,
        .provider_route_checked = true,
        .benchmark_passed = true,
        .measured_speedup = 4.319982,
        .minimum_repeat_speedup = 3.881579,
        .production_enabled = true,
        .promotion_ready = true,
        .legacy_production_exception = true,
    },
    .{
        .kernel_id = first_general_metal_q4_kernel_id,
        .source_path = first_general_metal_q4_source_path,
        .source_fingerprint = sourceFingerprint(first_general_metal_q4_source),
        .check_command = first_general_metal_q4_check_command,
        .runtime_evidence_command = first_general_metal_q4_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q4_promotion_check_command,
        .repeat_runs = metal_promotion_repeat_runs,
        .correctness_passed = true,
        .generated_route_checked = true,
        .provider_route_checked = true,
        .benchmark_passed = true,
        .measured_speedup = 2.888,
        .minimum_repeat_speedup = 2.409,
        .production_enabled = true,
        .promotion_ready = true,
        .legacy_production_exception = true,
    },
    .{
        .kernel_id = first_general_metal_q5_kernel_id,
        .source_path = first_general_metal_q5_source_path,
        .source_fingerprint = sourceFingerprint(first_general_metal_q5_source),
        .check_command = first_general_metal_q5_check_command,
        .runtime_evidence_command = first_general_metal_q5_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q5_promotion_check_command,
        .repeat_runs = metal_promotion_repeat_runs,
        .correctness_passed = true,
        .generated_route_checked = true,
        .provider_route_checked = true,
        .benchmark_passed = true,
        .measured_speedup = 2.880,
        .minimum_repeat_speedup = 2.880,
        .production_enabled = true,
        .promotion_ready = true,
        .legacy_production_exception = true,
    },
    .{
        .kernel_id = first_general_metal_q8_k_kernel_id,
        .source_path = first_general_metal_q8_k_source_path,
        .source_fingerprint = sourceFingerprint(first_general_metal_q8_k_source),
        .check_command = first_general_metal_q8_k_check_command,
        .runtime_evidence_command = first_general_metal_q8_k_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q8_k_promotion_check_command,
        .repeat_runs = metal_promotion_repeat_runs,
        .correctness_passed = true,
        .generated_route_checked = true,
        .provider_route_checked = true,
        .benchmark_passed = true,
        .measured_speedup = 2.344,
        .minimum_repeat_speedup = 2.344,
        .production_enabled = true,
        .promotion_ready = true,
        .legacy_production_exception = true,
    },
    .{
        .kernel_id = first_general_metal_q8_kernel_id,
        .source_path = first_general_metal_q8_source_path,
        .source_fingerprint = sourceFingerprint(first_general_metal_q8_source),
        .check_command = first_general_metal_q8_check_command,
        .runtime_evidence_command = first_general_metal_q8_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q8_promotion_check_command,
        .repeat_runs = metal_promotion_repeat_runs,
        .correctness_passed = true,
        .generated_route_checked = true,
        .provider_route_checked = true,
        .benchmark_passed = true,
        .measured_speedup = 3.408203,
        .minimum_repeat_speedup = 2.411878,
        .production_enabled = true,
        .promotion_ready = true,
        .legacy_production_exception = true,
    },
};
pub const first_metal_runtime_evidence_count = first_metal_runtime_evidence.len;

fn metalLegacyUnattestedEvidenceCount() usize {
    var count: usize = 0;
    for (first_metal_runtime_evidence) |evidence| {
        if (std.mem.eql(u8, evidence.provenance_status, metal_evidence_provenance_legacy_unattested)) count += 1;
    }
    return count;
}

// A cleared blocker refresh is only a signal to investigate; promotion still
// requires checked-in runtime evidence plus a passing production-regression run.
pub const first_metal_promotion_blocker_evidence = [_]MetalPromotionBlockerEvidence{
    .{ .kernel_id = first_general_metal_q4_0_kernel_id, .blocker = "speedup_gate_missing", .evidence_path = first_general_metal_q4_0_promotion_evidence_path, .requires_production_regression_clear = true },
    .{ .kernel_id = first_general_metal_q4_1_kernel_id, .blocker = "unstable_benchmark_timing", .evidence_path = first_general_metal_q4_1_promotion_evidence_path, .requires_production_regression_clear = true },
    .{ .kernel_id = first_general_metal_q5_0_kernel_id, .blocker = "speedup_gate_missing", .evidence_path = first_general_metal_q5_0_promotion_evidence_path, .requires_production_regression_clear = true },
    .{ .kernel_id = first_general_metal_q5_1_kernel_id, .blocker = "speedup_gate_missing", .evidence_path = first_general_metal_q5_1_promotion_evidence_path, .requires_production_regression_clear = true },
    .{ .kernel_id = first_lazy_metal_kernel_id, .blocker = "unstable_benchmark_timing", .evidence_path = first_lazy_metal_promotion_evidence_path, .requires_production_regression_clear = true },
    .{ .kernel_id = first_general_metal_q5_bias_gelu_kernel_id, .blocker = "unstable_benchmark_timing", .evidence_path = first_general_metal_q5_bias_gelu_promotion_evidence_path, .requires_production_regression_clear = true },
    .{ .kernel_id = first_general_metal_q8_bias_gelu_kernel_id, .blocker = "speedup_gate_missing", .evidence_path = first_general_metal_q8_bias_gelu_promotion_evidence_path, .requires_production_regression_clear = true },
    .{ .kernel_id = first_general_metal_q6_bias_gelu_kernel_id, .blocker = "speedup_gate_missing", .evidence_path = first_general_metal_q6_bias_gelu_promotion_evidence_path, .requires_production_regression_clear = true },
    .{ .kernel_id = first_general_metal_q8_1_kernel_id, .blocker = "unstable_benchmark_timing", .evidence_path = first_general_metal_q8_1_promotion_evidence_path, .requires_production_regression_clear = true },
    .{ .kernel_id = first_general_metal_q4_bias_kernel_id, .blocker = metal_blocker_runtime_route_only },
    .{ .kernel_id = first_general_metal_q5_bias_kernel_id, .blocker = metal_blocker_runtime_route_only },
    .{ .kernel_id = first_general_metal_q6_bias_kernel_id, .blocker = metal_blocker_runtime_route_only },
    .{ .kernel_id = first_general_metal_q8_bias_kernel_id, .blocker = metal_blocker_runtime_route_only },
    .{ .kernel_id = first_general_metal_q2_bias_kernel_id, .blocker = "unsupported_handwritten_baseline" },
    .{ .kernel_id = first_general_metal_q2_bias_gelu_kernel_id, .blocker = "unsupported_handwritten_baseline" },
    .{ .kernel_id = first_general_metal_q3_bias_kernel_id, .blocker = "unsupported_handwritten_baseline" },
    .{ .kernel_id = first_general_metal_q3_bias_gelu_kernel_id, .blocker = "unsupported_handwritten_baseline" },
    .{ .kernel_id = first_general_metal_q8_relu_kernel_id, .blocker = "unsupported_handwritten_baseline" },
};
pub const first_metal_promotion_blocker_evidence_count = first_metal_promotion_blocker_evidence.len;
pub const first_metal_promotion_blocker_evidence_cases_per_kernel: usize = 2;
pub const first_metal_promotion_blocker_evidence_expected_case_count = metalPromotionBlockerEvidenceExpectedCaseCount();
pub const first_metal_promotion_blocker_evidence_expected_route_ready_count = first_metal_promotion_blocker_evidence_expected_case_count;
pub const first_metal_production_benchmark_cases = buildMetalProductionBenchmarkCases();
pub const first_metal_production_benchmark_case_count = first_metal_production_benchmark_cases.len;
pub const first_artifact_manifest_schema = "antfly.quant_kernel_artifacts.v6";
pub const first_benchmark_manifest_schema = "antfly.quant_kernel_benchmarks.v6";
pub const first_lazy_benchmark_check_command = "zig-out/bin/antfly-inference bench-cuda --quant-compiler-check-evidence " ++ first_lazy_benchmark_evidence_path ++ " --quant-compiler-require-promotion-ready";
pub const first_spec_manifest_path = "src/ops/cuda/generated/quant_kernel_specs.json";
pub const first_artifact_manifest_path = "src/ops/cuda/generated/quant_kernel_artifacts.json";
pub const first_benchmark_manifest_path = "src/ops/cuda/generated/quant_kernel_benchmarks.json";
pub const first_conformance_manifest_path = "src/ops/cuda/generated/quant_kernel_conformance.json";
pub const first_cuda_generated_fatbin_options = "-gencode=arch=compute_75,code=sm_75 -gencode=arch=compute_80,code=sm_80 -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_90,code=sm_90 -gencode=arch=compute_75,code=compute_75";

pub const first_general_cuda_q4_0_mmv_kernel_id = "antfly_q4_0_mmv_f32_v1";
pub const first_general_cuda_q4_0_mmv_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_mmv.cu";
pub const first_general_cuda_q4_0_mmv_ptx_path = "/tmp/antfly_q4_0_mmv_f32_v1.fatbin";
pub const first_general_cuda_q4_0_mmv_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_general_cuda_q4_0_mmv_source_path ++ " -o " ++ first_general_cuda_q4_0_mmv_ptx_path;
pub const first_general_cuda_q4_0_mmv_evidence_path = "src/ops/cuda/generated/evidence/q4_0_mmv_benchmark.json";
pub const first_general_cuda_q4_0_mmv_benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-q4-0-mmv-ptx " ++ first_general_cuda_q4_0_mmv_ptx_path ++ " --quant-compiler-repeat-runs 3 --quant-compiler-evidence-out " ++ first_general_cuda_q4_0_mmv_evidence_path;
pub const first_general_cuda_q4_0_mm_kernel_id = "antfly_q4_0_mm_f32_v1";
pub const first_general_cuda_q4_0_mm_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_mm.cu";
pub const first_general_cuda_q4_0_mm_ptx_path = "/tmp/antfly_q4_0_mm_f32_v1.fatbin";
pub const first_general_cuda_q4_0_mm_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_general_cuda_q4_0_mm_source_path ++ " -o " ++ first_general_cuda_q4_0_mm_ptx_path;
pub const first_general_cuda_q4_0_mm_evidence_path = "src/ops/cuda/generated/evidence/q4_0_mm_benchmark.json";
pub const first_general_cuda_q4_0_mm_benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-q4-0-mm-ptx " ++ first_general_cuda_q4_0_mm_ptx_path ++ " --quant-compiler-repeat-runs 3 --quant-compiler-evidence-out " ++ first_general_cuda_q4_0_mm_evidence_path;
pub const first_general_cuda_q4_0_pair_kernel_id = "antfly_q4_0_pair_mmv_f32_v1";
pub const first_general_cuda_q4_0_pair_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_pair_mmv.cu";
pub const first_general_cuda_q4_0_pair_ptx_path = "/tmp/antfly_q4_0_pair_mmv_f32_v1.fatbin";
pub const first_general_cuda_q4_0_pair_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_general_cuda_q4_0_pair_source_path ++ " -o " ++ first_general_cuda_q4_0_pair_ptx_path;
pub const first_general_cuda_q4_0_pair_evidence_path = "src/ops/cuda/generated/evidence/q4_0_pair_benchmark.json";
pub const first_general_cuda_q4_0_pair_benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-q4-0-pair-ptx " ++ first_general_cuda_q4_0_pair_ptx_path ++ " --quant-compiler-repeat-runs 3 --quant-compiler-evidence-out " ++ first_general_cuda_q4_0_pair_evidence_path;
pub const first_general_cuda_q4_0_pair_q8_kernel_id = "antfly_q4_0_pair_activation_q8_1_mmv_v1";
pub const first_general_cuda_q4_0_pair_q8_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_pair_activation_q8_1.cu";
pub const first_general_cuda_q4_0_pair_q8_ptx_path = "/tmp/antfly_q4_0_pair_activation_q8_1_mmv_v1.fatbin";
pub const first_general_cuda_q4_0_pair_q8_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_general_cuda_q4_0_pair_q8_source_path ++ " -o " ++ first_general_cuda_q4_0_pair_q8_ptx_path;
pub const first_general_cuda_q4_0_pair_q8_evidence_path = "src/ops/cuda/generated/evidence/q4_0_pair_q8_benchmark.json";
pub const first_general_cuda_q4_0_pair_q8_benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-q4-0-pair-q8-ptx " ++ first_general_cuda_q4_0_pair_q8_ptx_path ++ " --quant-compiler-repeat-runs 3 --quant-compiler-evidence-out " ++ first_general_cuda_q4_0_pair_q8_evidence_path;
pub const first_general_cuda_q4_0_down_q8_kernel_id = "antfly_q4_0_down_q8_1_mmv_v1";
pub const first_general_cuda_q4_0_down_q8_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_down_q8_1.cu";
pub const first_general_cuda_q4_0_down_q8_ptx_path = "/tmp/antfly_q4_0_down_q8_1_mmv_v1.fatbin";
pub const first_general_cuda_q4_0_down_q8_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_general_cuda_q4_0_down_q8_source_path ++ " -o " ++ first_general_cuda_q4_0_down_q8_ptx_path;
pub const first_general_cuda_q4_0_down_q8_evidence_path = "src/ops/cuda/generated/evidence/q4_0_down_q8_benchmark.json";
pub const first_general_cuda_q4_0_down_q8_benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-q4-0-down-q8-ptx " ++ first_general_cuda_q4_0_down_q8_ptx_path ++ " --quant-compiler-repeat-runs 3 --quant-compiler-evidence-out " ++ first_general_cuda_q4_0_down_q8_evidence_path;
pub const first_e2b_cuda_q4_0_pair_q8_6144_kernel_id = "antfly_q4_0_pair_activation_q8_1_e2b_6144_mmv_v1";
pub const first_e2b_cuda_q4_0_pair_q8_6144_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_pair_activation_q8_1_e2b_6144.cu";
pub const first_e2b_cuda_q4_0_pair_q8_6144_ptx_path = "/tmp/antfly_q4_0_pair_activation_q8_1_e2b_6144_mmv_v1.fatbin";
pub const first_e2b_cuda_q4_0_pair_q8_6144_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_pair_q8_6144_source_path ++ " -o " ++ first_e2b_cuda_q4_0_pair_q8_6144_ptx_path;
pub const first_e2b_cuda_q4_0_pair_q8_12288_kernel_id = "antfly_q4_0_pair_activation_q8_1_e2b_12288_mmv_v1";
pub const first_e2b_cuda_q4_0_pair_q8_12288_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_pair_activation_q8_1_e2b_12288.cu";
pub const first_e2b_cuda_q4_0_pair_q8_12288_ptx_path = "/tmp/antfly_q4_0_pair_activation_q8_1_e2b_12288_mmv_v1.fatbin";
pub const first_e2b_cuda_q4_0_pair_q8_12288_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_pair_q8_12288_source_path ++ " -o " ++ first_e2b_cuda_q4_0_pair_q8_12288_ptx_path;
pub const first_e2b_cuda_q4_0_down_q8_6144_kernel_id = "antfly_q4_0_down_q8_1_e2b_6144_mmv_v1";
pub const first_e2b_cuda_q4_0_down_q8_6144_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_down_q8_1_e2b_6144.cu";
pub const first_e2b_cuda_q4_0_down_q8_6144_ptx_path = "/tmp/antfly_q4_0_down_q8_1_e2b_6144_mmv_v1.fatbin";
pub const first_e2b_cuda_q4_0_down_q8_6144_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_down_q8_6144_source_path ++ " -o " ++ first_e2b_cuda_q4_0_down_q8_6144_ptx_path;
pub const first_e2b_cuda_q4_0_down_q8_12288_kernel_id = "antfly_q4_0_down_q8_1_e2b_12288_mmv_v1";
pub const first_e2b_cuda_q4_0_down_q8_12288_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_down_q8_1_e2b_12288.cu";
pub const first_e2b_cuda_q4_0_down_q8_12288_ptx_path = "/tmp/antfly_q4_0_down_q8_1_e2b_12288_mmv_v1.fatbin";
pub const first_e2b_cuda_q4_0_down_q8_12288_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_down_q8_12288_source_path ++ " -o " ++ first_e2b_cuda_q4_0_down_q8_12288_ptx_path;
pub const first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_kernel_id = "antfly_q4_0_pair_activation_ggml_q8_1_e2b_6144_mmv_v1";
pub const first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_pair_activation_ggml_q8_1_e2b_6144.cu";
pub const first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_ptx_path = "/tmp/antfly_q4_0_pair_activation_ggml_q8_1_e2b_6144_mmv_v1.fatbin";
pub const first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_source_path ++ " -o " ++ first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_ptx_path;
pub const first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_kernel_id = "antfly_q4_0_pair_activation_ggml_q8_1_e2b_12288_mmv_v1";
pub const first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_pair_activation_ggml_q8_1_e2b_12288.cu";
pub const first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_ptx_path = "/tmp/antfly_q4_0_pair_activation_ggml_q8_1_e2b_12288_mmv_v1.fatbin";
pub const first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_source_path ++ " -o " ++ first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_ptx_path;
pub const first_e2b_cuda_q4_0_down_ggml_q8_1_6144_kernel_id = "antfly_q4_0_down_ggml_q8_1_e2b_6144_mmv_v1";
pub const first_e2b_cuda_q4_0_down_ggml_q8_1_6144_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_down_ggml_q8_1_e2b_6144.cu";
pub const first_e2b_cuda_q4_0_down_ggml_q8_1_6144_ptx_path = "/tmp/antfly_q4_0_down_ggml_q8_1_e2b_6144_mmv_v1.fatbin";
pub const first_e2b_cuda_q4_0_down_ggml_q8_1_6144_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_down_ggml_q8_1_6144_source_path ++ " -o " ++ first_e2b_cuda_q4_0_down_ggml_q8_1_6144_ptx_path;
pub const first_e2b_cuda_q4_0_down_ggml_q8_1_12288_kernel_id = "antfly_q4_0_down_ggml_q8_1_e2b_12288_mmv_v1";
pub const first_e2b_cuda_q4_0_down_ggml_q8_1_12288_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_down_ggml_q8_1_e2b_12288.cu";
pub const first_e2b_cuda_q4_0_down_ggml_q8_1_12288_ptx_path = "/tmp/antfly_q4_0_down_ggml_q8_1_e2b_12288_mmv_v1.fatbin";
pub const first_e2b_cuda_q4_0_down_ggml_q8_1_12288_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_down_ggml_q8_1_12288_source_path ++ " -o " ++ first_e2b_cuda_q4_0_down_ggml_q8_1_12288_ptx_path;
pub const first_e2b_cuda_q4_0_pair_f32_6144_exact_kernel_id = "antfly_q4_0_pair_activation_f32_e2b_6144_exact_v1";
pub const first_e2b_cuda_q4_0_pair_f32_6144_exact_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_pair_activation_f32_e2b_6144_exact.cu";
pub const first_e2b_cuda_q4_0_pair_f32_6144_exact_ptx_path = "/tmp/antfly_q4_0_pair_activation_f32_e2b_6144_exact_v1.fatbin";
pub const first_e2b_cuda_q4_0_pair_f32_6144_exact_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_pair_f32_6144_exact_source_path ++ " -o " ++ first_e2b_cuda_q4_0_pair_f32_6144_exact_ptx_path;
pub const first_e2b_cuda_q4_0_pair_f32_12288_exact_kernel_id = "antfly_q4_0_pair_activation_f32_e2b_12288_exact_v1";
pub const first_e2b_cuda_q4_0_pair_f32_12288_exact_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_pair_activation_f32_e2b_12288_exact.cu";
pub const first_e2b_cuda_q4_0_pair_f32_12288_exact_ptx_path = "/tmp/antfly_q4_0_pair_activation_f32_e2b_12288_exact_v1.fatbin";
pub const first_e2b_cuda_q4_0_pair_f32_12288_exact_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_pair_f32_12288_exact_source_path ++ " -o " ++ first_e2b_cuda_q4_0_pair_f32_12288_exact_ptx_path;
pub const first_e2b_cuda_q4_0_down_f32_6144_exact_kernel_id = "antfly_q4_0_down_f32_e2b_6144_exact_v1";
pub const first_e2b_cuda_q4_0_down_f32_6144_exact_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_down_f32_e2b_6144_exact.cu";
pub const first_e2b_cuda_q4_0_down_f32_6144_exact_ptx_path = "/tmp/antfly_q4_0_down_f32_e2b_6144_exact_v1.fatbin";
pub const first_e2b_cuda_q4_0_down_f32_6144_exact_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_down_f32_6144_exact_source_path ++ " -o " ++ first_e2b_cuda_q4_0_down_f32_6144_exact_ptx_path;
pub const first_e2b_cuda_q4_0_down_f32_12288_exact_kernel_id = "antfly_q4_0_down_f32_e2b_12288_exact_v1";
pub const first_e2b_cuda_q4_0_down_f32_12288_exact_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_down_f32_e2b_12288_exact.cu";
pub const first_e2b_cuda_q4_0_down_f32_12288_exact_ptx_path = "/tmp/antfly_q4_0_down_f32_e2b_12288_exact_v1.fatbin";
pub const first_e2b_cuda_q4_0_down_f32_12288_exact_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_down_f32_12288_exact_source_path ++ " -o " ++ first_e2b_cuda_q4_0_down_f32_12288_exact_ptx_path;
pub const first_e2b_cuda_q4_0_ffn_benchmark_command = "scripts/benchmark_gemma4_cuda_e2b_ffn.sh";
pub const first_e2b_cuda_q4_0_q8_1_argmax_kernel_id = "antfly_q4_0_q8_1_argmax_rows_stage1_tile8_v1";
pub const first_e2b_cuda_q4_0_q8_1_argmax_source_path = "src/ops/cuda/generated/quant_kernel_q4_0_q8_1_argmax_e2b_tile8.cu";
pub const first_e2b_cuda_q4_0_q8_1_argmax_ptx_path = "/tmp/antfly_q4_0_q8_1_argmax_rows_stage1_tile8_v1.fatbin";
pub const first_e2b_cuda_q4_0_q8_1_argmax_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_e2b_cuda_q4_0_q8_1_argmax_source_path ++ " -o " ++ first_e2b_cuda_q4_0_q8_1_argmax_ptx_path;
pub const first_cuda_q6_k_q8_1_argmax_k2560_kernel_id = "antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1";
pub const first_cuda_q6_k_q8_1_argmax_k2560_source_path = "src/ops/cuda/generated/quant_kernel_q6_k_q8_1_argmax_k2560_tile8.cu";
pub const first_cuda_q6_k_q8_1_argmax_k2560_ptx_path = "/tmp/antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1.fatbin";
pub const first_cuda_q6_k_q8_1_argmax_k2560_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_cuda_q6_k_q8_1_argmax_k2560_source_path ++ " -o " ++ first_cuda_q6_k_q8_1_argmax_k2560_ptx_path;
pub const first_cuda_q6_k_q8_1_argmax_k2560_source_fingerprint = sourceFingerprint(first_cuda_q6_k_q8_1_argmax_k2560_source);
pub const first_cuda_q6_k_q8_1_argmax_k3840_kernel_id = "antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1";
pub const first_cuda_q6_k_q8_1_argmax_k3840_source_path = "src/ops/cuda/generated/quant_kernel_q6_k_q8_1_argmax_k3840_tile8.cu";
pub const first_cuda_q6_k_q8_1_argmax_k3840_ptx_path = "/tmp/antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1.fatbin";
pub const first_cuda_q6_k_q8_1_argmax_k3840_check_command = "nvcc -fatbin " ++ first_cuda_generated_fatbin_options ++ " " ++ first_cuda_q6_k_q8_1_argmax_k3840_source_path ++ " -o " ++ first_cuda_q6_k_q8_1_argmax_k3840_ptx_path;
pub const first_cuda_q6_k_q8_1_argmax_k3840_source_fingerprint = sourceFingerprint(first_cuda_q6_k_q8_1_argmax_k3840_source);
pub const first_lazy_metal_kernel_id = "antfly_q4_k_small_batch_bias_gelu_msl_v1";
pub const first_lazy_metal_source_path = "src/ops/metal/generated/quant_kernel_q4_k_small_batch_bias_gelu.metal";
pub const first_lazy_metal_air_path = "/tmp/antfly_q4_k_small_batch_bias_gelu_msl_v1.air";
pub const first_lazy_metal_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q4_k_small_batch_bias_gelu.metal -o /tmp/antfly_q4_k_small_batch_bias_gelu_msl_v1.air";

// ---- Microkernel (non-matmul fused op) artifacts -------------------------
// First non-matmul route brought under the compiler: RMSNorm. Uses op_kind
// `.microkernel` and the descriptor renderer's microkernel path. Dev-only
// candidate (opt-in kill switch) — the hand-written `termite_apply_rms_norm_rows`
// stays the production baseline until this clears its conformance gate.
pub const first_rms_norm_metal_kernel_id = "antfly_rms_norm_generated_msl_v1";
pub const first_rms_norm_metal_source_path = "src/ops/metal/generated/microkernel_rms_norm.metal";
pub const first_rms_norm_metal_air_path = "/tmp/antfly_rms_norm_generated_msl_v1.air";
pub const first_rms_norm_metal_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/microkernel_rms_norm.metal -o /tmp/antfly_rms_norm_generated_msl_v1.air";
/// Launch schedule for the RMSNorm microkernel: one threadgroup per row,
/// `threads_per_threadgroup` threads cooperatively reduce the `d` sum-of-squares
/// with a threadgroup tree. 256 threads covers d up to a few thousand well; the
/// strided lane loop scales to any d.
pub const first_rms_norm_metal_schedule = KernelSchedule{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree };

// ---- Attention (paged decode) artifacts ----------------------------------
// First `op_kind = .attention` route brought under the compiler: the scalar
// decode-1x paged-attention hot path (`termite_paged_attention_kv_1x`). Uses the
// descriptor renderer's self-contained attention path (its own params struct +
// paging helper) so the standalone `.metal` compiles alone AND the runtime
// region embeds byte-identical bytes. Dev-only candidate (opt-in kill switch
// TERMITE_METAL_ENABLE_ATTENTION_1X_GENERATED) — the hand-written
// `termite_paged_attention_kv_1x` stays the production baseline until the
// generated route clears its model-token acceptance gate.
pub const first_decode_attention_1x_metal_kernel_id = "antfly_paged_attention_1x_generated_msl_v1";
pub const first_decode_attention_1x_metal_source_path = "src/ops/metal/generated/attention_decode_1x.metal";
pub const first_decode_attention_1x_metal_air_path = "/tmp/antfly_paged_attention_1x_generated_msl_v1.air";
pub const first_decode_attention_1x_metal_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/attention_decode_1x.metal -o /tmp/antfly_paged_attention_1x_generated_msl_v1.air";
/// Launch schedule for the decode-1x paged-attention kernel: one threadgroup per
/// (query, head), NT threads split into NT/32 simdgroups. NT is pinned to 256 to
/// match the hand-written dispatch (threadgroup memory + grid), so the generated
/// route is byte-for-byte the hand-written one; the flash-prefill slice is where
/// NT/NSG become a `--sweep` knob.
pub const first_decode_attention_1x_metal_schedule = KernelSchedule{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree };

// CUDA split-KV decode candidates. Each generated source is one atomic artifact
// containing a serial short-context kernel, a KV-head-centric partial kernel,
// and a stable merge kernel. All three launches use the same device-scalar KV
// length so a captured graph can cross the split threshold without recapture.
//
// Split count is a first-class schedule choice. The legacy split-8 IDs remain
// runtime-owned for ABI compatibility; split-2 and split-4 are embedded
// dev-only candidates selected explicitly by the CUDA generated-attention gate.
pub const first_decode_attention_1x_cuda_kernel_id = cuda_renderer.generated_attention_hd256_stage1_kernel_id;
pub const first_decode_attention_1x_cuda_source_path = "src/ops/cuda/generated/attention_decode_scalars_hd256.cu";
pub const first_decode_attention_1x_cuda_fatbin_path = "/tmp/antfly_gqa_attention_decode_split_kv_hd256_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_check_command = "nvcc -fatbin -gencode=arch=compute_75,code=sm_75 -gencode=arch=compute_80,code=sm_80 -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_90,code=sm_90 -gencode=arch=compute_75,code=compute_75 src/ops/cuda/generated/attention_decode_scalars_hd256.cu -o /tmp/antfly_gqa_attention_decode_split_kv_hd256_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_schedule = cudaAttentionSchedule(256, .split8);
pub const first_decode_attention_1x_cuda_hd512_kernel_id = cuda_renderer.generated_attention_hd512_stage1_kernel_id;
pub const first_decode_attention_1x_cuda_hd512_source_path = "src/ops/cuda/generated/attention_decode_scalars_hd512.cu";
pub const first_decode_attention_1x_cuda_hd512_fatbin_path = "/tmp/antfly_gqa_attention_decode_split_kv_hd512_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_hd512_check_command = "nvcc -fatbin -gencode=arch=compute_75,code=sm_75 -gencode=arch=compute_80,code=sm_80 -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_90,code=sm_90 -gencode=arch=compute_75,code=compute_75 src/ops/cuda/generated/attention_decode_scalars_hd512.cu -o /tmp/antfly_gqa_attention_decode_split_kv_hd512_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_hd512_schedule = cudaAttentionSchedule(512, .split8);

pub const first_decode_attention_1x_cuda_split2_hd256_kernel_id = cuda_renderer.generated_attention_hd256_split2_stage1_kernel_id;
pub const first_decode_attention_1x_cuda_split2_hd256_source_path = "src/ops/cuda/generated/attention_decode_scalars_split2_hd256.cu";
pub const first_decode_attention_1x_cuda_split2_hd256_fatbin_path = "/tmp/antfly_gqa_attention_decode_split2_kv_hd256_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_split2_hd256_check_command = "nvcc -fatbin -gencode=arch=compute_75,code=sm_75 -gencode=arch=compute_80,code=sm_80 -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_90,code=sm_90 -gencode=arch=compute_75,code=compute_75 src/ops/cuda/generated/attention_decode_scalars_split2_hd256.cu -o /tmp/antfly_gqa_attention_decode_split2_kv_hd256_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_split2_hd256_schedule = cudaAttentionSchedule(256, .split2);

pub const first_decode_attention_1x_cuda_split2_hd512_kernel_id = cuda_renderer.generated_attention_hd512_split2_stage1_kernel_id;
pub const first_decode_attention_1x_cuda_split2_hd512_source_path = "src/ops/cuda/generated/attention_decode_scalars_split2_hd512.cu";
pub const first_decode_attention_1x_cuda_split2_hd512_fatbin_path = "/tmp/antfly_gqa_attention_decode_split2_kv_hd512_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_split2_hd512_check_command = "nvcc -fatbin -gencode=arch=compute_75,code=sm_75 -gencode=arch=compute_80,code=sm_80 -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_90,code=sm_90 -gencode=arch=compute_75,code=compute_75 src/ops/cuda/generated/attention_decode_scalars_split2_hd512.cu -o /tmp/antfly_gqa_attention_decode_split2_kv_hd512_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_split2_hd512_schedule = cudaAttentionSchedule(512, .split2);

pub const first_decode_attention_1x_cuda_split4_hd256_kernel_id = cuda_renderer.generated_attention_hd256_split4_stage1_kernel_id;
pub const first_decode_attention_1x_cuda_split4_hd256_source_path = "src/ops/cuda/generated/attention_decode_scalars_split4_hd256.cu";
pub const first_decode_attention_1x_cuda_split4_hd256_fatbin_path = "/tmp/antfly_gqa_attention_decode_split4_kv_hd256_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_split4_hd256_check_command = "nvcc -fatbin -gencode=arch=compute_75,code=sm_75 -gencode=arch=compute_80,code=sm_80 -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_90,code=sm_90 -gencode=arch=compute_75,code=compute_75 src/ops/cuda/generated/attention_decode_scalars_split4_hd256.cu -o /tmp/antfly_gqa_attention_decode_split4_kv_hd256_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_split4_hd256_schedule = cudaAttentionSchedule(256, .split4);

pub const first_decode_attention_1x_cuda_split4_hd512_kernel_id = cuda_renderer.generated_attention_hd512_split4_stage1_kernel_id;
pub const first_decode_attention_1x_cuda_split4_hd512_source_path = "src/ops/cuda/generated/attention_decode_scalars_split4_hd512.cu";
pub const first_decode_attention_1x_cuda_split4_hd512_fatbin_path = "/tmp/antfly_gqa_attention_decode_split4_kv_hd512_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_split4_hd512_check_command = "nvcc -fatbin -gencode=arch=compute_75,code=sm_75 -gencode=arch=compute_80,code=sm_80 -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_90,code=sm_90 -gencode=arch=compute_75,code=compute_75 src/ops/cuda/generated/attention_decode_scalars_split4_hd512.cu -o /tmp/antfly_gqa_attention_decode_split4_kv_hd512_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_split4_hd512_schedule = cudaAttentionSchedule(512, .split4);

// Paged exact score-prework decode composites. Each artifact bundles the score
// producer plus the serial and tiled64 consumers, and is promoted for the
// qualified SM89 Gemma 4 F16 geometry: the runtime automatic selector engages
// it by default at the 512-token KV crossover, with
// ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK=0 as the rollback.
pub const first_decode_attention_1x_cuda_score_prework_runtime_evidence_command = "zig build quant-kernel-cuda-paged-attention-diff -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 -Doptimize=ReleaseFast -- --head-dim all --kv-len 2003 --pattern all --key-format all --value-format all --page-order all --heads 8 --kv-heads 2 --iterations 100";
pub const first_decode_attention_1x_cuda_score_prework_promotion_evidence_command = "python3 scripts/validate_gemma4_cuda_candidate.py --kernel-id cuda.attention.gqa.decode.score_prework --qualification-profile screening --prompt-fixture scripts/fixtures/gemma4_long_context_v1.json --lengths 300 --prefill-chunk-size 512 --cache-dtype f16 --capture-kv-capacity 2432 --output-dir /tmp/antfly-score-prework-screening";
pub const first_decode_attention_1x_cuda_score_prework_hd256_kernel_id = cuda_renderer.generated_attention_hd256_score_prework_kernel_id;
pub const first_decode_attention_1x_cuda_score_prework_hd256_source_path = "src/ops/cuda/generated/attention_decode_score_prework_hd256.cu";
pub const first_decode_attention_1x_cuda_score_prework_hd256_check_command = "nvcc -fatbin -gencode=arch=compute_75,code=sm_75 -gencode=arch=compute_80,code=sm_80 -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_90,code=sm_90 -gencode=arch=compute_75,code=compute_75 src/ops/cuda/generated/attention_decode_score_prework_hd256.cu -o /tmp/antfly_gqa_attention_decode_score_prework_hd256_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_score_prework_hd256_schedule = cudaAttentionSchedule(256, .score_prework);

pub const first_decode_attention_1x_cuda_score_prework_hd512_kernel_id = cuda_renderer.generated_attention_hd512_score_prework_kernel_id;
pub const first_decode_attention_1x_cuda_score_prework_hd512_source_path = "src/ops/cuda/generated/attention_decode_score_prework_hd512.cu";
pub const first_decode_attention_1x_cuda_score_prework_hd512_check_command = "nvcc -fatbin -gencode=arch=compute_75,code=sm_75 -gencode=arch=compute_80,code=sm_80 -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_90,code=sm_90 -gencode=arch=compute_75,code=compute_75 src/ops/cuda/generated/attention_decode_score_prework_hd512.cu -o /tmp/antfly_gqa_attention_decode_score_prework_hd512_f32_v1.fatbin";
pub const first_decode_attention_1x_cuda_score_prework_hd512_schedule = cudaAttentionSchedule(512, .score_prework);

// Promoted SM89 Flash-prefill composites. The paged F16 prefill differential
// passed all guard/page-table/adversarial/determinism cases bitwise-identical,
// so the runtime automatic selector engages the flash route by default for the
// qualified SM89 Gemma 4 F16 geometry, with
// ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=off as the rollback.
pub const first_prefill_flash_cuda_runtime_evidence_command = "zig build quant-kernel-cuda-paged-prefill-diff -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 -Doptimize=ReleaseFast -- --json";
pub const first_prefill_flash_cuda_promotion_evidence_command = "python3 scripts/validate_gemma4_cuda_candidate.py --kernel-id cuda.attention.gqa.prefill.flash_f16_sm89 --qualification-profile screening --prompt-fixture scripts/fixtures/gemma4_long_context_v1.json --lengths 300 --prefill-chunk-size 512 --cache-dtype f16 --capture-kv-capacity 2432 --output-dir /tmp/antfly-flash-prefill-screening";
pub const first_prefill_flash_cuda_hd256_kernel_id = cuda_renderer.generated_flash_prefill_hd256_kernel_id;
pub const first_prefill_flash_cuda_hd256_source_path = "src/ops/cuda/generated/attention_prefill_flash_sm89_hd256.cu";
pub const first_prefill_flash_cuda_hd256_check_command = "nvcc -fatbin -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_89,code=compute_89 src/ops/cuda/generated/attention_prefill_flash_sm89_hd256.cu -o /tmp/antfly_gqa_attention_prefill_flash_sm89_hd256.fatbin";
pub const first_prefill_flash_cuda_hd256_schedule = cudaFlashPrefillSchedule(256);
pub const first_prefill_flash_cuda_hd512_kernel_id = cuda_renderer.generated_flash_prefill_hd512_kernel_id;
pub const first_prefill_flash_cuda_hd512_source_path = "src/ops/cuda/generated/attention_prefill_flash_sm89_hd512.cu";
pub const first_prefill_flash_cuda_hd512_check_command = "nvcc -fatbin -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_89,code=compute_89 src/ops/cuda/generated/attention_prefill_flash_sm89_hd512.cu -o /tmp/antfly_gqa_attention_prefill_flash_sm89_hd512.fatbin";
pub const first_prefill_flash_cuda_hd512_schedule = cudaFlashPrefillSchedule(512);

// Qualified one-launch SM89 split-K decode candidates. They are generated and
// runtime-wired for typed opt-in profiles, but remain production/default-off
// until end-to-end generated-token parity and release evidence promote them.
pub const first_decode_splitk_online_cuda_hd256_kernel_id = cuda_renderer.generated_splitk_online_decode_hd256_kernel_id;
pub const first_decode_splitk_online_cuda_hd256_source_path = "src/ops/cuda/generated/attention_decode_splitk_online_sm89_hd256.cu";
pub const first_decode_splitk_online_cuda_hd256_check_command = "nvcc -fatbin -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_89,code=compute_89 src/ops/cuda/generated/attention_decode_splitk_online_sm89_hd256.cu -o /tmp/antfly_gqa_attention_decode_splitk_online_sm89_hd256.fatbin";
pub const first_decode_splitk_online_cuda_hd256_schedule = cudaSplitkOnlineDecodeSchedule(256);
pub const first_decode_splitk_online_cuda_hd512_kernel_id = cuda_renderer.generated_splitk_online_decode_hd512_kernel_id;
pub const first_decode_splitk_online_cuda_hd512_source_path = "src/ops/cuda/generated/attention_decode_splitk_online_sm89_hd512.cu";
pub const first_decode_splitk_online_cuda_hd512_check_command = "nvcc -fatbin -gencode=arch=compute_89,code=sm_89 -gencode=arch=compute_89,code=compute_89 src/ops/cuda/generated/attention_decode_splitk_online_sm89_hd512.cu -o /tmp/antfly_gqa_attention_decode_splitk_online_sm89_hd512.fatbin";
pub const first_decode_splitk_online_cuda_hd512_schedule = cudaSplitkOnlineDecodeSchedule(512);

fn cudaAttentionSchedule(head_dim: u16, split_variant: cuda_renderer.AttentionSplitVariant) KernelSchedule {
    return .{
        .threads_per_threadgroup = head_dim,
        .cols_per_threadgroup = 1,
        .reduction = .threadgroup_tree,
        .attention_serial_threads_per_threadgroup = head_dim,
        .attention_stage2_threads_per_threadgroup = head_dim,
        .attention_tiled64_threads_per_threadgroup = if (split_variant == .score_prework)
            cuda_renderer.generated_attention_score_prework_tiled64_tile_size
        else
            0,
        .attention_kv_splits = split_variant.kvSplits(),
        .attention_query_heads_per_kv_head = cuda_renderer.generated_attention_query_heads_per_kv_head,
        .attention_split_kv_min_tokens = if (split_variant == .score_prework)
            0
        else
            cuda_renderer.generated_attention_split_kv_min_tokens_default,
        .attention_max_kv_tokens = if (split_variant == .score_prework)
            cuda_renderer.generated_attention_score_prework_max_kv_tokens
        else
            0,
        .attention_tiled64_max_kv_tokens = if (split_variant == .score_prework)
            cuda_renderer.generatedAttentionScorePreworkTiled64MaxKvTokens(head_dim).?
        else
            0,
        .attention_storage = .f32,
        .attention_key_storage = if (split_variant == .score_prework) .paged_f16_or_polar4 else .f32,
        .attention_value_storage = if (split_variant == .score_prework) .paged_f16_or_f32 else .f32,
    };
}

fn cudaFlashPrefillSchedule(head_dim: u16) KernelSchedule {
    return .{
        .threads_per_threadgroup = cuda_renderer.generated_flash_prefill_threads,
        .cols_per_threadgroup = 1,
        .reduction = .threadgroup_tree,
        .key_chunk = cuda_renderer.generated_flash_prefill_key_tile,
        .attention_query_heads_per_kv_head = 8,
        .attention_query_tile = cuda_renderer.generated_flash_prefill_query_tile,
        .attention_key_tile = cuda_renderer.generated_flash_prefill_key_tile,
        .attention_page_size_tokens = cuda_renderer.generated_flash_prefill_page_size_tokens,
        .attention_dynamic_shared_memory_bytes = cuda_renderer.generatedFlashPrefillDynamicSharedBytes(head_dim).?,
        .attention_required_compute_major = 8,
        .attention_required_compute_minor = 9,
        .attention_query_length_policy = .gemma4_q512_or_q3_v1,
        .attention_storage = .f32,
        .attention_key_storage = .paged_f16,
        .attention_value_storage = .paged_f16,
    };
}

fn cudaSplitkOnlineDecodeSchedule(head_dim: u16) KernelSchedule {
    return .{
        .threads_per_threadgroup = cuda_renderer.generated_splitk_online_decode_threads,
        .cols_per_threadgroup = 1,
        .reduction = .threadgroup_tree,
        .attention_kv_splits = cuda_renderer.generated_splitk_online_decode_splits,
        .attention_query_heads_per_kv_head = 8,
        .attention_max_kv_tokens = if (head_dim == 256)
            cuda_renderer.generated_splitk_online_decode_hd256_max_visible_tokens
        else
            cuda_renderer.generated_splitk_online_decode_hd512_max_visible_tokens,
        .attention_page_size_tokens = cuda_renderer.generated_splitk_online_decode_page_size_tokens,
        .attention_required_compute_major = 8,
        .attention_required_compute_minor = 9,
        .attention_storage = .f32,
        .attention_key_storage = .paged_f16,
        .attention_value_storage = .paged_f16,
    };
}

// Second `op_kind = .attention` route: the simdgroup-MMA flash prefill kernel
// (`termite_paged_attention_kv_prefill_sg`), brought under the compiler as a
// `--sweep`-tunable route. The checked-in schedule below is the baseline
// (`key_chunk=32, skip_rescale=false`), which preserves the hand-written
// kernel's MMA and online-softmax order while replacing the large K/V gather
// scratch with page-local loads. The sweep enumerates the
// `key_chunk ∈ {32,64}` × `skip_rescale ∈ {false,true}` variants in memory; only
// the baseline is checked in / runtime-embedded / model-gated. Production
// defaults to it only for the attested Gemma4 E4B geometry; other compatible
// geometries remain opt-in and retain the handwritten fallback.
pub const first_prefill_flash_metal_kernel_id = "antfly_paged_attention_prefill_flash_generated_msl_v1";
pub const first_prefill_flash_metal_source_path = "src/ops/metal/generated/attention_prefill_flash.metal";
pub const first_prefill_flash_metal_air_path = "/tmp/antfly_paged_attention_prefill_flash_generated_msl_v1.air";
pub const first_prefill_flash_metal_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/attention_prefill_flash.metal -o /tmp/antfly_paged_attention_prefill_flash_generated_msl_v1.air";
/// Baseline flash-prefill schedule: 128 threads / 4 simdgroups (structural),
/// `key_chunk=32` + `skip_rescale=false` (same arithmetic order as the
/// hand-written dispatch); grid ((q_len+7)/8, heads), threadgroup memory
/// 24*hd + 2016 bytes.
pub const first_prefill_flash_metal_schedule = KernelSchedule{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree, .key_chunk = 32, .skip_rescale = false };

// Gemma4 global attention: the same paged flash semantics with a fixed
// 256-thread / eight-simdgroup lowering that keeps head_dim=512 under portable
// Metal threadgroup-memory limits by loading page-local K/V tiles directly.
pub const first_prefill_flash_hd512_metal_kernel_id = "antfly_paged_attention_prefill_flash_hd512_generated_msl_v1";
pub const first_prefill_flash_hd512_metal_source_path = "src/ops/metal/generated/attention_prefill_flash_hd512.metal";
pub const first_prefill_flash_hd512_metal_air_path = "/tmp/antfly_paged_attention_prefill_flash_hd512_generated_msl_v1.air";
pub const first_prefill_flash_hd512_metal_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/attention_prefill_flash_hd512.metal -o /tmp/antfly_paged_attention_prefill_flash_hd512_generated_msl_v1.air";
pub const first_prefill_flash_hd512_metal_schedule = KernelSchedule{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 1, .reduction = .threadgroup_tree, .key_chunk = 64, .skip_rescale = false };

pub const first_general_metal_q4_0_kernel_id = "antfly_q4_0_small_batch_msl_v1";
pub const first_general_metal_q4_0_source_path = "src/ops/metal/generated/quant_kernel_q4_0_small_batch.metal";
pub const first_general_metal_q4_0_air_path = "/tmp/antfly_q4_0_small_batch_msl_v1.air";
pub const first_general_metal_q4_0_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q4_0_small_batch.metal -o /tmp/antfly_q4_0_small_batch_msl_v1.air";
pub const first_general_metal_q4_1_kernel_id = "antfly_q4_1_small_batch_msl_v1";
pub const first_general_metal_q4_1_source_path = "src/ops/metal/generated/quant_kernel_q4_1_small_batch.metal";
pub const first_general_metal_q4_1_air_path = "/tmp/antfly_q4_1_small_batch_msl_v1.air";
pub const first_general_metal_q4_1_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q4_1_small_batch.metal -o /tmp/antfly_q4_1_small_batch_msl_v1.air";
pub const first_general_metal_q5_0_kernel_id = "antfly_q5_0_small_batch_msl_v1";
pub const first_general_metal_q5_0_source_path = "src/ops/metal/generated/quant_kernel_q5_0_small_batch.metal";
pub const first_general_metal_q5_0_air_path = "/tmp/antfly_q5_0_small_batch_msl_v1.air";
pub const first_general_metal_q5_0_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q5_0_small_batch.metal -o /tmp/antfly_q5_0_small_batch_msl_v1.air";
pub const first_general_metal_q5_1_kernel_id = "antfly_q5_1_small_batch_msl_v1";
pub const first_general_metal_q5_1_source_path = "src/ops/metal/generated/quant_kernel_q5_1_small_batch.metal";
pub const first_general_metal_q5_1_air_path = "/tmp/antfly_q5_1_small_batch_msl_v1.air";
pub const first_general_metal_q5_1_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q5_1_small_batch.metal -o /tmp/antfly_q5_1_small_batch_msl_v1.air";
pub const first_general_metal_q2_kernel_id = "antfly_q2_k_small_batch_msl_v1";
pub const first_general_metal_q2_source_path = "src/ops/metal/generated/quant_kernel_q2_k_small_batch.metal";
pub const first_general_metal_q2_air_path = "/tmp/antfly_q2_k_small_batch_msl_v1.air";
pub const first_general_metal_q2_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q2_k_small_batch.metal -o /tmp/antfly_q2_k_small_batch_msl_v1.air";
pub const first_general_metal_q2_bias_kernel_id = "antfly_q2_k_small_batch_bias_msl_v1";
pub const first_general_metal_q2_bias_source_path = "src/ops/metal/generated/quant_kernel_q2_k_small_batch_bias.metal";
pub const first_general_metal_q2_bias_air_path = "/tmp/antfly_q2_k_small_batch_bias_msl_v1.air";
pub const first_general_metal_q2_bias_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q2_k_small_batch_bias.metal -o /tmp/antfly_q2_k_small_batch_bias_msl_v1.air";
pub const first_general_metal_q2_bias_gelu_kernel_id = "antfly_q2_k_small_batch_bias_gelu_msl_v1";
pub const first_general_metal_q2_bias_gelu_source_path = "src/ops/metal/generated/quant_kernel_q2_k_small_batch_bias_gelu.metal";
pub const first_general_metal_q2_bias_gelu_air_path = "/tmp/antfly_q2_k_small_batch_bias_gelu_msl_v1.air";
pub const first_general_metal_q2_bias_gelu_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q2_k_small_batch_bias_gelu.metal -o /tmp/antfly_q2_k_small_batch_bias_gelu_msl_v1.air";
pub const first_general_metal_q3_kernel_id = "antfly_q3_k_small_batch_msl_v1";
pub const first_general_metal_q3_source_path = "src/ops/metal/generated/quant_kernel_q3_k_small_batch.metal";
pub const first_general_metal_q3_air_path = "/tmp/antfly_q3_k_small_batch_msl_v1.air";
pub const first_general_metal_q3_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q3_k_small_batch.metal -o /tmp/antfly_q3_k_small_batch_msl_v1.air";
pub const first_general_metal_q3_bias_kernel_id = "antfly_q3_k_small_batch_bias_msl_v1";
pub const first_general_metal_q3_bias_source_path = "src/ops/metal/generated/quant_kernel_q3_k_small_batch_bias.metal";
pub const first_general_metal_q3_bias_air_path = "/tmp/antfly_q3_k_small_batch_bias_msl_v1.air";
pub const first_general_metal_q3_bias_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q3_k_small_batch_bias.metal -o /tmp/antfly_q3_k_small_batch_bias_msl_v1.air";
pub const first_general_metal_q3_bias_gelu_kernel_id = "antfly_q3_k_small_batch_bias_gelu_msl_v1";
pub const first_general_metal_q3_bias_gelu_source_path = "src/ops/metal/generated/quant_kernel_q3_k_small_batch_bias_gelu.metal";
pub const first_general_metal_q3_bias_gelu_air_path = "/tmp/antfly_q3_k_small_batch_bias_gelu_msl_v1.air";
pub const first_general_metal_q3_bias_gelu_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q3_k_small_batch_bias_gelu.metal -o /tmp/antfly_q3_k_small_batch_bias_gelu_msl_v1.air";
pub const first_general_metal_q4_bias_kernel_id = "antfly_q4_k_small_batch_bias_msl_v1";
pub const first_general_metal_q4_bias_source_path = "src/ops/metal/generated/quant_kernel_q4_k_small_batch_bias.metal";
pub const first_general_metal_q4_bias_air_path = "/tmp/antfly_q4_k_small_batch_bias_msl_v1.air";
pub const first_general_metal_q4_bias_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q4_k_small_batch_bias.metal -o /tmp/antfly_q4_k_small_batch_bias_msl_v1.air";
pub const first_general_metal_q4_kernel_id = "antfly_q4_k_small_batch_msl_v1";
pub const first_general_metal_q4_source_path = "src/ops/metal/generated/quant_kernel_q4_k_small_batch.metal";
pub const first_general_metal_q4_air_path = "/tmp/antfly_q4_k_small_batch_msl_v1.air";
pub const first_general_metal_q4_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q4_k_small_batch.metal -o /tmp/antfly_q4_k_small_batch_msl_v1.air";
pub const first_general_metal_q8_kernel_id = "antfly_q8_0_small_batch_msl_v1";
pub const first_general_metal_q8_source_path = "src/ops/metal/generated/quant_kernel_q8_0_small_batch.metal";
pub const first_general_metal_q8_air_path = "/tmp/antfly_q8_0_small_batch_msl_v1.air";
pub const first_general_metal_q8_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q8_0_small_batch.metal -o /tmp/antfly_q8_0_small_batch_msl_v1.air";
pub const first_general_metal_q8_bias_kernel_id = "antfly_q8_0_small_batch_bias_msl_v1";
pub const first_general_metal_q8_bias_source_path = "src/ops/metal/generated/quant_kernel_q8_0_small_batch_bias.metal";
pub const first_general_metal_q8_bias_air_path = "/tmp/antfly_q8_0_small_batch_bias_msl_v1.air";
pub const first_general_metal_q8_bias_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q8_0_small_batch_bias.metal -o /tmp/antfly_q8_0_small_batch_bias_msl_v1.air";
pub const first_general_metal_q8_bias_gelu_kernel_id = "antfly_q8_0_small_batch_bias_gelu_msl_v1";
pub const first_general_metal_q8_bias_gelu_source_path = "src/ops/metal/generated/quant_kernel_q8_0_small_batch_bias_gelu.metal";
pub const first_general_metal_q8_bias_gelu_air_path = "/tmp/antfly_q8_0_small_batch_bias_gelu_msl_v1.air";
pub const first_general_metal_q8_bias_gelu_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q8_0_small_batch_bias_gelu.metal -o /tmp/antfly_q8_0_small_batch_bias_gelu_msl_v1.air";
pub const first_general_metal_q8_bias_gelu_source_fingerprint = sourceFingerprint(first_general_metal_q8_bias_gelu_source);
pub const first_general_metal_q8_relu_kernel_id = "antfly_q8_0_small_batch_relu_msl_v1";
pub const first_general_metal_q8_relu_source_path = "src/ops/metal/generated/quant_kernel_q8_0_small_batch_relu.metal";
pub const first_general_metal_q8_relu_air_path = "/tmp/antfly_q8_0_small_batch_relu_msl_v1.air";
pub const first_general_metal_q8_relu_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q8_0_small_batch_relu.metal -o /tmp/antfly_q8_0_small_batch_relu_msl_v1.air";
pub const first_general_metal_q8_1_kernel_id = "antfly_q8_1_small_batch_msl_v1";
pub const first_general_metal_q8_1_source_path = "src/ops/metal/generated/quant_kernel_q8_1_small_batch.metal";
pub const first_general_metal_q8_1_air_path = "/tmp/antfly_q8_1_small_batch_msl_v1.air";
pub const first_general_metal_q8_1_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q8_1_small_batch.metal -o /tmp/antfly_q8_1_small_batch_msl_v1.air";
pub const first_general_metal_q8_k_kernel_id = "antfly_q8_k_small_batch_msl_v1";
pub const first_general_metal_q8_k_source_path = "src/ops/metal/generated/quant_kernel_q8_k_small_batch.metal";
pub const first_general_metal_q8_k_air_path = "/tmp/antfly_q8_k_small_batch_msl_v1.air";
pub const first_general_metal_q8_k_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q8_k_small_batch.metal -o /tmp/antfly_q8_k_small_batch_msl_v1.air";
pub const first_general_metal_q5_kernel_id = "antfly_q5_k_small_batch_msl_v1";
pub const first_general_metal_q5_source_path = "src/ops/metal/generated/quant_kernel_q5_k_small_batch.metal";
pub const first_general_metal_q5_air_path = "/tmp/antfly_q5_k_small_batch_msl_v1.air";
pub const first_general_metal_q5_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q5_k_small_batch.metal -o /tmp/antfly_q5_k_small_batch_msl_v1.air";
pub const first_general_metal_q5_bias_kernel_id = "antfly_q5_k_small_batch_bias_msl_v1";
pub const first_general_metal_q5_bias_source_path = "src/ops/metal/generated/quant_kernel_q5_k_small_batch_bias.metal";
pub const first_general_metal_q5_bias_air_path = "/tmp/antfly_q5_k_small_batch_bias_msl_v1.air";
pub const first_general_metal_q5_bias_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q5_k_small_batch_bias.metal -o /tmp/antfly_q5_k_small_batch_bias_msl_v1.air";
pub const first_general_metal_q5_bias_gelu_kernel_id = "antfly_q5_k_small_batch_bias_gelu_msl_v1";
pub const first_general_metal_q5_bias_gelu_source_path = "src/ops/metal/generated/quant_kernel_q5_k_small_batch_bias_gelu.metal";
pub const first_general_metal_q5_bias_gelu_air_path = "/tmp/antfly_q5_k_small_batch_bias_gelu_msl_v1.air";
pub const first_general_metal_q5_bias_gelu_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q5_k_small_batch_bias_gelu.metal -o /tmp/antfly_q5_k_small_batch_bias_gelu_msl_v1.air";
pub const first_general_metal_q6_kernel_id = "antfly_q6_k_small_batch_msl_v1";
pub const first_general_metal_q6_source_path = "src/ops/metal/generated/quant_kernel_q6_k_small_batch.metal";
pub const first_general_metal_q6_air_path = "/tmp/antfly_q6_k_small_batch_msl_v1.air";
pub const first_general_metal_q6_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q6_k_small_batch.metal -o /tmp/antfly_q6_k_small_batch_msl_v1.air";
pub const first_general_metal_q6_bias_kernel_id = "antfly_q6_k_small_batch_bias_msl_v1";
pub const first_general_metal_q6_bias_source_path = "src/ops/metal/generated/quant_kernel_q6_k_small_batch_bias.metal";
pub const first_general_metal_q6_bias_air_path = "/tmp/antfly_q6_k_small_batch_bias_msl_v1.air";
pub const first_general_metal_q6_bias_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q6_k_small_batch_bias.metal -o /tmp/antfly_q6_k_small_batch_bias_msl_v1.air";
pub const first_general_metal_q6_bias_gelu_kernel_id = "antfly_q6_k_small_batch_bias_gelu_msl_v1";
pub const first_general_metal_q6_bias_gelu_source_path = "src/ops/metal/generated/quant_kernel_q6_k_small_batch_bias_gelu.metal";
pub const first_general_metal_q6_bias_gelu_air_path = "/tmp/antfly_q6_k_small_batch_bias_gelu_msl_v1.air";
pub const first_general_metal_q6_bias_gelu_check_command = "xcrun --toolchain Metal metal -c src/ops/metal/generated/quant_kernel_q6_k_small_batch_bias_gelu.metal -o /tmp/antfly_q6_k_small_batch_bias_gelu_msl_v1.air";
pub const first_metal_runtime_evidence_path = "/private/tmp/antfly-quant-metal-evidence.json";
pub const first_metal_runtime_evidence_command = "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out " ++ first_metal_runtime_evidence_path ++ " --repeat-runs " ++ metal_promotion_repeat_runs_text;
pub const first_metal_local_check_command = "zig build quant-kernel-metal-local-check -Dmetal=true -Dcuda=false";
pub const first_metal_model_local_check_command = "zig build quant-kernel-metal-model-local-check -Dmetal=true -Dcuda=false";
pub const first_metal_model_generated_route_check_command = "zig build test-metal-gemma4-prefill-frame-e4b-generated-q8-q4-0 -Dmetal=true -Dcuda=false";
pub const first_metal_model_generated_q8_0_small_batch_min: usize = 1;
pub const first_metal_model_generated_q4_0_small_batch_min: usize = 1;
pub const first_metal_industry_local_check_command = "zig build quant-kernel-metal-industry-local-check -Dmetal=true -Dcuda=false";
pub const first_metal_runtime_route_all_evidence_path = "/private/tmp/antfly-quant-metal-runtime-route-all-evidence.json";
pub const first_metal_runtime_route_all_build_command = "zig build quant-kernel-metal-runtime-route-all -Dmetal=true -Dcuda=false";
pub const first_metal_runtime_route_all_evidence_command = "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out " ++ first_metal_runtime_route_all_evidence_path ++ " --runtime-route-all";
pub const first_metal_runtime_route_all_check_command = "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --check-evidence " ++ first_metal_runtime_route_all_evidence_path ++ " --require-runtime-route-all";
pub const first_metal_runtime_route_all_expected_case_count = metalRuntimeRouteAllExpectedCaseCount();
pub const first_metal_runtime_route_all_expected_route_ready_count = first_metal_runtime_route_all_expected_case_count;
pub const first_metal_runtime_route_all_expected_provider_route_count = metalRuntimeRouteAllExpectedProviderRouteCount();
pub const first_metal_production_regression_evidence_path = "/private/tmp/antfly-quant-metal-production-regression-evidence.json";
pub const first_metal_production_regression_build_command = "zig build quant-kernel-metal-production-regression-check -Dmetal=true -Dcuda=false";
pub const first_metal_production_regression_evidence_command = "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out " ++ first_metal_production_regression_evidence_path ++ " --repeat-runs " ++ metal_promotion_repeat_runs_text ++ " --measure-iters " ++ metal_promotion_measure_iters_text ++ " --production-regression-check";
pub const first_metal_production_regression_expected_kernel_count = metalProductionRegressionExpectedKernelCount();
pub const first_metal_production_regression_expected_case_count = metalProductionRegressionExpectedCaseCount();
pub const first_metal_production_regression_expected_route_ready_count = first_metal_production_regression_expected_case_count;
pub const first_metal_production_regression_route_ready_is_hard_gate = true;
pub const first_metal_production_regression_missing_provider_route_is_hard_gate = true;
pub const first_metal_production_regression_speedup_gate_missing_is_hard_gate = true;
pub const first_metal_production_regression_unstable_benchmark_timing_is_hard_gate = false;
pub const first_metal_unsupported_handwritten_baseline_blocks_promotion = true;
pub const first_metal_unsupported_handwritten_baseline_uses_runtime_route_all_evidence = true;
pub const first_metal_unsupported_handwritten_baseline_has_promotion_evidence_path = false;
pub const first_metal_blocker_strict_check_command = "zig build quant-kernel-metal-blocker-strict-check -Dmetal=true -Dcuda=false";
pub const first_metal_promotion_evidence_command = "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out ";
pub const first_metal_promotion_check_command = "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --check-evidence ";
pub const first_lazy_metal_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_lazy_metal_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q4_0_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q4_0_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q4_1_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q4_1_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q5_0_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q5_0_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q5_1_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q5_1_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q2_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q2_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q2_bias_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q2_bias_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q2_bias_gelu_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q2_bias_gelu_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q3_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q3_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q3_bias_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q3_bias_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q3_bias_gelu_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q3_bias_gelu_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q4_bias_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q4_bias_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q4_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q4_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q8_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q8_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q8_bias_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q8_bias_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q8_bias_gelu_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q8_bias_gelu_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q8_relu_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q8_relu_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q8_1_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q8_1_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q8_k_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q8_k_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q5_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q5_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q5_bias_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q5_bias_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q5_bias_gelu_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q5_bias_gelu_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q6_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q6_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q6_bias_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q6_bias_kernel_id ++ "-promotion-evidence.json";
pub const first_general_metal_q6_bias_gelu_promotion_evidence_path = "/private/tmp/antfly-quant-metal-" ++ first_general_metal_q6_bias_gelu_kernel_id ++ "-promotion-evidence.json";
pub const first_lazy_metal_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_lazy_metal_promotion_evidence_path ++ metal_promotion_args ++ first_lazy_metal_kernel_id;
pub const first_general_metal_q4_0_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q4_0_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q4_0_kernel_id;
pub const first_general_metal_q4_1_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q4_1_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q4_1_kernel_id;
pub const first_general_metal_q5_0_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q5_0_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q5_0_kernel_id;
pub const first_general_metal_q5_1_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q5_1_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q5_1_kernel_id;
pub const first_general_metal_q2_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q2_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q2_kernel_id;
pub const first_general_metal_q2_bias_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q2_bias_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q2_bias_kernel_id;
pub const first_general_metal_q2_bias_gelu_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q2_bias_gelu_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q2_bias_gelu_kernel_id;
pub const first_general_metal_q3_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q3_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q3_kernel_id;
pub const first_general_metal_q3_bias_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q3_bias_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q3_bias_kernel_id;
pub const first_general_metal_q3_bias_gelu_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q3_bias_gelu_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q3_bias_gelu_kernel_id;
pub const first_general_metal_q4_bias_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q4_bias_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q4_bias_kernel_id;
pub const first_general_metal_q4_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q4_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q4_kernel_id;
pub const first_general_metal_q8_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q8_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q8_kernel_id;
pub const first_general_metal_q8_bias_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q8_bias_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q8_bias_kernel_id;
pub const first_general_metal_q8_bias_gelu_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q8_bias_gelu_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q8_bias_gelu_kernel_id;
pub const first_general_metal_q8_relu_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q8_relu_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q8_relu_kernel_id;
pub const first_general_metal_q8_1_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q8_1_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q8_1_kernel_id;
pub const first_general_metal_q8_k_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q8_k_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q8_k_kernel_id;
pub const first_general_metal_q5_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q5_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q5_kernel_id;
pub const first_general_metal_q5_bias_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q5_bias_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q5_bias_kernel_id;
pub const first_general_metal_q5_bias_gelu_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q5_bias_gelu_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q5_bias_gelu_kernel_id;
pub const first_general_metal_q6_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q6_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q6_kernel_id;
pub const first_general_metal_q6_bias_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q6_bias_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q6_bias_kernel_id;
pub const first_general_metal_q6_bias_gelu_promotion_evidence_command = first_metal_promotion_evidence_command ++ first_general_metal_q6_bias_gelu_promotion_evidence_path ++ metal_promotion_args ++ first_general_metal_q6_bias_gelu_kernel_id;
pub const first_lazy_metal_promotion_check_command = first_metal_promotion_check_command ++ first_lazy_metal_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_lazy_metal_kernel_id;
pub const first_general_metal_q4_0_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q4_0_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q4_0_kernel_id;
pub const first_general_metal_q4_1_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q4_1_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q4_1_kernel_id;
pub const first_general_metal_q5_0_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q5_0_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q5_0_kernel_id;
pub const first_general_metal_q5_1_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q5_1_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q5_1_kernel_id;
pub const first_general_metal_q2_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q2_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q2_kernel_id;
pub const first_general_metal_q2_bias_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q2_bias_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q2_bias_kernel_id;
pub const first_general_metal_q2_bias_gelu_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q2_bias_gelu_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q2_bias_gelu_kernel_id;
pub const first_general_metal_q3_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q3_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q3_kernel_id;
pub const first_general_metal_q3_bias_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q3_bias_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q3_bias_kernel_id;
pub const first_general_metal_q3_bias_gelu_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q3_bias_gelu_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q3_bias_gelu_kernel_id;
pub const first_general_metal_q4_bias_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q4_bias_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q4_bias_kernel_id;
pub const first_general_metal_q4_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q4_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q4_kernel_id;
pub const first_general_metal_q8_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q8_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q8_kernel_id;
pub const first_general_metal_q8_bias_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q8_bias_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q8_bias_kernel_id;
pub const first_general_metal_q8_bias_gelu_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q8_bias_gelu_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q8_bias_gelu_kernel_id;
pub const first_general_metal_q8_relu_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q8_relu_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q8_relu_kernel_id;
pub const first_general_metal_q8_1_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q8_1_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q8_1_kernel_id;
pub const first_general_metal_q8_k_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q8_k_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q8_k_kernel_id;
pub const first_general_metal_q5_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q5_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q5_kernel_id;
pub const first_general_metal_q5_bias_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q5_bias_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q5_bias_kernel_id;
pub const first_general_metal_q5_bias_gelu_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q5_bias_gelu_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q5_bias_gelu_kernel_id;
pub const first_general_metal_q6_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q6_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q6_kernel_id;
pub const first_general_metal_q6_bias_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q6_bias_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q6_bias_kernel_id;
pub const first_general_metal_q6_bias_gelu_promotion_check_command = first_metal_promotion_check_command ++ first_general_metal_q6_bias_gelu_promotion_evidence_path ++ " --require-promotion-ready --require-kernel " ++ first_general_metal_q6_bias_gelu_kernel_id;

pub const first_generated_artifacts = [_]GeneratedArtifact{
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = first_lazy_benchmark.format,
            .row_bucket = first_lazy_benchmark.row_bucket,
            .epilogue = first_lazy_benchmark.epilogue,
        } },
        .kernel_id = first_lazy_benchmark.generated_kernel_id,
        .source_path = first_lazy_benchmark.generated_source_path,
        .check_command = first_lazy_benchmark.generated_ptx_command,
        .runtime_evidence_command = first_lazy_benchmark.benchmark_command,
        .promotion_check_command = first_lazy_benchmark_check_command,
        .production_enabled = first_lazy_benchmark.production_enabled,
        .cuda_kernel = .q4_k_small_batch_bias_gelu,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_k,
            .row_bucket = .rows_1,
            .epilogue = .none,
        } },
        .kernel_id = first_general_cuda_q4_k_mmv_kernel_id,
        .source_path = first_general_cuda_q4_k_mmv_source_path,
        .check_command = first_general_cuda_q4_k_mmv_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_k_mmv,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = first_lazy_benchmark.format,
            .row_bucket = first_lazy_benchmark.row_bucket,
            .epilogue = first_lazy_benchmark.epilogue,
        } },
        .kernel_id = first_lazy_metal_kernel_id,
        .source_path = first_lazy_metal_source_path,
        .check_command = first_lazy_metal_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_lazy_metal_promotion_evidence_command,
        .promotion_check_command = first_lazy_metal_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q4_0_kernel_id,
        .source_path = first_general_metal_q4_0_source_path,
        .check_command = first_general_metal_q4_0_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q4_0_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q4_0_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_1,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q4_1_kernel_id,
        .source_path = first_general_metal_q4_1_source_path,
        .check_command = first_general_metal_q4_1_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q4_1_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q4_1_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q5_0,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q5_0_kernel_id,
        .source_path = first_general_metal_q5_0_source_path,
        .check_command = first_general_metal_q5_0_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q5_0_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q5_0_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q5_1,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q5_1_kernel_id,
        .source_path = first_general_metal_q5_1_source_path,
        .check_command = first_general_metal_q5_1_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q5_1_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q5_1_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q2_k,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q2_kernel_id,
        .source_path = first_general_metal_q2_source_path,
        .check_command = first_general_metal_q2_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q2_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q2_promotion_check_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q2_k,
            .row_bucket = .rows_2_8,
            .epilogue = .bias,
        } },
        .kernel_id = first_general_metal_q2_bias_kernel_id,
        .source_path = first_general_metal_q2_bias_source_path,
        .check_command = first_general_metal_q2_bias_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q2_bias_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q2_bias_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q2_k,
            .row_bucket = .rows_2_8,
            .epilogue = .bias_gelu,
        } },
        .kernel_id = first_general_metal_q2_bias_gelu_kernel_id,
        .source_path = first_general_metal_q2_bias_gelu_source_path,
        .check_command = first_general_metal_q2_bias_gelu_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q2_bias_gelu_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q2_bias_gelu_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q3_k,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q3_kernel_id,
        .source_path = first_general_metal_q3_source_path,
        .check_command = first_general_metal_q3_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q3_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q3_promotion_check_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q3_k,
            .row_bucket = .rows_2_8,
            .epilogue = .bias,
        } },
        .kernel_id = first_general_metal_q3_bias_kernel_id,
        .source_path = first_general_metal_q3_bias_source_path,
        .check_command = first_general_metal_q3_bias_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q3_bias_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q3_bias_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q3_k,
            .row_bucket = .rows_2_8,
            .epilogue = .bias_gelu,
        } },
        .kernel_id = first_general_metal_q3_bias_gelu_kernel_id,
        .source_path = first_general_metal_q3_bias_gelu_source_path,
        .check_command = first_general_metal_q3_bias_gelu_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q3_bias_gelu_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q3_bias_gelu_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_k,
            .row_bucket = .rows_2_8,
            .epilogue = .bias,
        } },
        .kernel_id = first_general_metal_q4_bias_kernel_id,
        .source_path = first_general_metal_q4_bias_source_path,
        .check_command = first_general_metal_q4_bias_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q4_bias_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q4_bias_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_k,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q4_kernel_id,
        .source_path = first_general_metal_q4_source_path,
        .check_command = first_general_metal_q4_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q4_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q4_promotion_check_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q8_0,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q8_kernel_id,
        .source_path = first_general_metal_q8_source_path,
        .check_command = first_general_metal_q8_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q8_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q8_promotion_check_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q8_0,
            .row_bucket = .rows_2_8,
            .epilogue = .bias,
        } },
        .kernel_id = first_general_metal_q8_bias_kernel_id,
        .source_path = first_general_metal_q8_bias_source_path,
        .check_command = first_general_metal_q8_bias_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q8_bias_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q8_bias_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q8_0,
            .row_bucket = .rows_2_8,
            .epilogue = .bias_gelu,
        } },
        .kernel_id = first_general_metal_q8_bias_gelu_kernel_id,
        .source_path = first_general_metal_q8_bias_gelu_source_path,
        .check_command = first_general_metal_q8_bias_gelu_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q8_bias_gelu_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q8_bias_gelu_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q8_0,
            .row_bucket = .rows_2_8,
            .epilogue = .relu,
        } },
        .kernel_id = first_general_metal_q8_relu_kernel_id,
        .source_path = first_general_metal_q8_relu_source_path,
        .check_command = first_general_metal_q8_relu_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q8_relu_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q8_relu_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q8_1,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q8_1_kernel_id,
        .source_path = first_general_metal_q8_1_source_path,
        .check_command = first_general_metal_q8_1_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q8_1_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q8_1_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q8_k,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q8_k_kernel_id,
        .source_path = first_general_metal_q8_k_source_path,
        .check_command = first_general_metal_q8_k_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q8_k_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q8_k_promotion_check_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q5_k,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q5_kernel_id,
        .source_path = first_general_metal_q5_source_path,
        .check_command = first_general_metal_q5_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q5_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q5_promotion_check_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q5_k,
            .row_bucket = .rows_2_8,
            .epilogue = .bias,
        } },
        .kernel_id = first_general_metal_q5_bias_kernel_id,
        .source_path = first_general_metal_q5_bias_source_path,
        .check_command = first_general_metal_q5_bias_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q5_bias_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q5_bias_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q5_k,
            .row_bucket = .rows_2_8,
            .epilogue = .bias_gelu,
        } },
        .kernel_id = first_general_metal_q5_bias_gelu_kernel_id,
        .source_path = first_general_metal_q5_bias_gelu_source_path,
        .check_command = first_general_metal_q5_bias_gelu_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q5_bias_gelu_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q5_bias_gelu_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q6_k,
            .row_bucket = .rows_2_8,
            .epilogue = .none,
        } },
        .kernel_id = first_general_metal_q6_kernel_id,
        .source_path = first_general_metal_q6_source_path,
        .check_command = first_general_metal_q6_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q6_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q6_promotion_check_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q6_k,
            .row_bucket = .rows_2_8,
            .epilogue = .bias,
        } },
        .kernel_id = first_general_metal_q6_bias_kernel_id,
        .source_path = first_general_metal_q6_bias_source_path,
        .check_command = first_general_metal_q6_bias_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q6_bias_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q6_bias_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .small_batch_matmul = .{
            .format = .q6_k,
            .row_bucket = .rows_2_8,
            .epilogue = .bias_gelu,
        } },
        .kernel_id = first_general_metal_q6_bias_gelu_kernel_id,
        .source_path = first_general_metal_q6_bias_gelu_source_path,
        .check_command = first_general_metal_q6_bias_gelu_check_command,
        .runtime_evidence_command = first_metal_runtime_evidence_command,
        .promotion_evidence_command = first_general_metal_q6_bias_gelu_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q6_bias_gelu_promotion_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .none,
        } },
        .kernel_id = first_general_cuda_q4_0_mmv_kernel_id,
        .source_path = first_general_cuda_q4_0_mmv_source_path,
        .check_command = first_general_cuda_q4_0_mmv_check_command,
        .runtime_evidence_command = first_general_cuda_q4_0_mmv_benchmark_command,
        .promotion_evidence_command = first_general_cuda_q4_0_mmv_benchmark_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
        .cuda_kernel = .q4_0_mmv,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_9_64,
            .epilogue = .none,
        } },
        .kernel_id = first_general_cuda_q4_0_mm_kernel_id,
        .source_path = first_general_cuda_q4_0_mm_source_path,
        .check_command = first_general_cuda_q4_0_mm_check_command,
        .runtime_evidence_command = first_general_cuda_q4_0_mm_benchmark_command,
        .promotion_evidence_command = first_general_cuda_q4_0_mm_benchmark_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
        .runtime_shape = .{ .min_in_dim = 512 },
        .cuda_kernel = .q4_0_mm,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .pair,
        } },
        .kernel_id = first_general_cuda_q4_0_pair_kernel_id,
        .source_path = first_general_cuda_q4_0_pair_source_path,
        .check_command = first_general_cuda_q4_0_pair_check_command,
        .runtime_evidence_command = first_general_cuda_q4_0_pair_benchmark_command,
        .promotion_evidence_command = first_general_cuda_q4_0_pair_benchmark_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
        .cuda_kernel = .q4_0_pair_mmv,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .pair_activation,
            .activation = .q8_1,
            .output = .q8_1,
        } },
        .kernel_id = first_general_cuda_q4_0_pair_q8_kernel_id,
        .source_path = first_general_cuda_q4_0_pair_q8_source_path,
        .check_command = first_general_cuda_q4_0_pair_q8_check_command,
        .runtime_evidence_command = first_general_cuda_q4_0_pair_q8_benchmark_command,
        .promotion_evidence_command = first_general_cuda_q4_0_pair_q8_benchmark_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
        .cuda_kernel = .q4_0_pair_activation_q8_1,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .gated_down,
            .activation = .q8_1,
        } },
        .kernel_id = first_general_cuda_q4_0_down_q8_kernel_id,
        .source_path = first_general_cuda_q4_0_down_q8_source_path,
        .check_command = first_general_cuda_q4_0_down_q8_check_command,
        .runtime_evidence_command = first_general_cuda_q4_0_down_q8_benchmark_command,
        .promotion_evidence_command = first_general_cuda_q4_0_down_q8_benchmark_command,
        .production_enabled = true,
        .runtime_default_enabled = false,
        .cuda_kernel = .q4_0_down_q8_1,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .pair_activation,
            .activation = .q8_1,
            .output = .q8_1,
        } },
        .kernel_id = first_e2b_cuda_q4_0_pair_q8_6144_kernel_id,
        .source_path = first_e2b_cuda_q4_0_pair_q8_6144_source_path,
        .check_command = first_e2b_cuda_q4_0_pair_q8_6144_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_pair_activation_q8_1_e2b_6144,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .pair_activation,
            .activation = .q8_1,
            .output = .q8_1,
        } },
        .kernel_id = first_e2b_cuda_q4_0_pair_q8_12288_kernel_id,
        .source_path = first_e2b_cuda_q4_0_pair_q8_12288_source_path,
        .check_command = first_e2b_cuda_q4_0_pair_q8_12288_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_pair_activation_q8_1_e2b_12288,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .gated_down,
            .activation = .q8_1,
        } },
        .kernel_id = first_e2b_cuda_q4_0_down_q8_6144_kernel_id,
        .source_path = first_e2b_cuda_q4_0_down_q8_6144_source_path,
        .check_command = first_e2b_cuda_q4_0_down_q8_6144_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_down_q8_1_e2b_6144,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .gated_down,
            .activation = .q8_1,
        } },
        .kernel_id = first_e2b_cuda_q4_0_down_q8_12288_kernel_id,
        .source_path = first_e2b_cuda_q4_0_down_q8_12288_source_path,
        .check_command = first_e2b_cuda_q4_0_down_q8_12288_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_down_q8_1_e2b_12288,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .pair_activation,
            .activation = .q8_1,
            .output = .q8_1,
        } },
        .kernel_id = first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_kernel_id,
        .source_path = first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_source_path,
        .check_command = first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_pair_activation_ggml_q8_1_e2b_6144,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .pair_activation,
            .activation = .q8_1,
            .output = .q8_1,
        } },
        .kernel_id = first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_kernel_id,
        .source_path = first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_source_path,
        .check_command = first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_pair_activation_ggml_q8_1_e2b_12288,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .gated_down,
            .activation = .q8_1,
        } },
        .kernel_id = first_e2b_cuda_q4_0_down_ggml_q8_1_6144_kernel_id,
        .source_path = first_e2b_cuda_q4_0_down_ggml_q8_1_6144_source_path,
        .check_command = first_e2b_cuda_q4_0_down_ggml_q8_1_6144_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_down_ggml_q8_1_e2b_6144,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .gated_down,
            .activation = .q8_1,
        } },
        .kernel_id = first_e2b_cuda_q4_0_down_ggml_q8_1_12288_kernel_id,
        .source_path = first_e2b_cuda_q4_0_down_ggml_q8_1_12288_source_path,
        .check_command = first_e2b_cuda_q4_0_down_ggml_q8_1_12288_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_down_ggml_q8_1_e2b_12288,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .pair_activation,
            .activation = .f32,
            .output = .f32,
        } },
        .kernel_id = first_e2b_cuda_q4_0_pair_f32_6144_exact_kernel_id,
        .source_path = first_e2b_cuda_q4_0_pair_f32_6144_exact_source_path,
        .check_command = first_e2b_cuda_q4_0_pair_f32_6144_exact_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_pair_activation_f32_e2b_6144_exact,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .pair_activation,
            .activation = .f32,
            .output = .f32,
        } },
        .kernel_id = first_e2b_cuda_q4_0_pair_f32_12288_exact_kernel_id,
        .source_path = first_e2b_cuda_q4_0_pair_f32_12288_exact_source_path,
        .check_command = first_e2b_cuda_q4_0_pair_f32_12288_exact_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_pair_activation_f32_e2b_12288_exact,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .gated_down,
            .activation = .f32,
            .output = .f32,
        } },
        .kernel_id = first_e2b_cuda_q4_0_down_f32_6144_exact_kernel_id,
        .source_path = first_e2b_cuda_q4_0_down_f32_6144_exact_source_path,
        .check_command = first_e2b_cuda_q4_0_down_f32_6144_exact_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_down_f32_e2b_6144_exact,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .gated_down,
            .activation = .f32,
            .output = .f32,
        } },
        .kernel_id = first_e2b_cuda_q4_0_down_f32_12288_exact_kernel_id,
        .source_path = first_e2b_cuda_q4_0_down_f32_12288_exact_source_path,
        .check_command = first_e2b_cuda_q4_0_down_f32_12288_exact_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_down_f32_e2b_12288_exact,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q4_0,
            .row_bucket = .rows_1,
            .epilogue = .argmax,
            .activation = .q8_1,
            .output = .i32,
        } },
        .kernel_id = first_e2b_cuda_q4_0_q8_1_argmax_kernel_id,
        .source_path = first_e2b_cuda_q4_0_q8_1_argmax_source_path,
        .check_command = first_e2b_cuda_q4_0_q8_1_argmax_check_command,
        .production_enabled = false,
        .cuda_kernel = .q4_0_q8_1_argmax_e2b_tile8,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q6_k,
            .row_bucket = .rows_1,
            .epilogue = .argmax,
            .activation = .q8_1,
            .output = .i32,
        } },
        .kernel_id = first_cuda_q6_k_q8_1_argmax_k2560_kernel_id,
        .source_path = first_cuda_q6_k_q8_1_argmax_k2560_source_path,
        .check_command = first_cuda_q6_k_q8_1_argmax_k2560_check_command,
        .production_enabled = false,
        .cuda_kernel = .q6_k_q8_1_argmax_k2560_tile8,
    },
    .{
        .backend = .cuda,
        .op = .{ .small_batch_matmul = .{
            .format = .q6_k,
            .row_bucket = .rows_1,
            .epilogue = .argmax,
            .activation = .q8_1,
            .output = .i32,
        } },
        .kernel_id = first_cuda_q6_k_q8_1_argmax_k3840_kernel_id,
        .source_path = first_cuda_q6_k_q8_1_argmax_k3840_source_path,
        .check_command = first_cuda_q6_k_q8_1_argmax_k3840_check_command,
        .production_enabled = false,
        .cuda_kernel = .q6_k_q8_1_argmax_k3840_tile8,
    },
    .{
        .backend = .metal,
        .op = .{ .microkernel = .{
            .kind = .rms_norm,
            .schedule = first_rms_norm_metal_schedule,
        } },
        .kernel_id = first_rms_norm_metal_kernel_id,
        .source_path = first_rms_norm_metal_source_path,
        .check_command = first_rms_norm_metal_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .decode_1x,
            .head_dim = 256,
            .schedule = first_decode_attention_1x_cuda_schedule,
        } },
        .kernel_id = first_decode_attention_1x_cuda_kernel_id,
        .source_path = first_decode_attention_1x_cuda_source_path,
        .check_command = first_decode_attention_1x_cuda_check_command,
        .production_enabled = false,
        .cuda_attention_kernel = .gqa_decode_split_kv_hd256_f32,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .decode_1x,
            .head_dim = 512,
            .schedule = first_decode_attention_1x_cuda_hd512_schedule,
        } },
        .kernel_id = first_decode_attention_1x_cuda_hd512_kernel_id,
        .source_path = first_decode_attention_1x_cuda_hd512_source_path,
        .check_command = first_decode_attention_1x_cuda_hd512_check_command,
        .production_enabled = false,
        .cuda_attention_kernel = .gqa_decode_split_kv_hd512_f32,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .decode_1x,
            .head_dim = 256,
            .schedule = first_decode_attention_1x_cuda_split2_hd256_schedule,
        } },
        .kernel_id = first_decode_attention_1x_cuda_split2_hd256_kernel_id,
        .source_path = first_decode_attention_1x_cuda_split2_hd256_source_path,
        .check_command = first_decode_attention_1x_cuda_split2_hd256_check_command,
        .production_enabled = false,
        .cuda_attention_kernel = .gqa_decode_split2_kv_hd256_f32,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .decode_1x,
            .head_dim = 512,
            .schedule = first_decode_attention_1x_cuda_split2_hd512_schedule,
        } },
        .kernel_id = first_decode_attention_1x_cuda_split2_hd512_kernel_id,
        .source_path = first_decode_attention_1x_cuda_split2_hd512_source_path,
        .check_command = first_decode_attention_1x_cuda_split2_hd512_check_command,
        .production_enabled = false,
        .cuda_attention_kernel = .gqa_decode_split2_kv_hd512_f32,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .decode_1x,
            .head_dim = 256,
            .schedule = first_decode_attention_1x_cuda_split4_hd256_schedule,
        } },
        .kernel_id = first_decode_attention_1x_cuda_split4_hd256_kernel_id,
        .source_path = first_decode_attention_1x_cuda_split4_hd256_source_path,
        .check_command = first_decode_attention_1x_cuda_split4_hd256_check_command,
        .production_enabled = false,
        .cuda_attention_kernel = .gqa_decode_split4_kv_hd256_f32,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .decode_1x,
            .head_dim = 512,
            .schedule = first_decode_attention_1x_cuda_split4_hd512_schedule,
        } },
        .kernel_id = first_decode_attention_1x_cuda_split4_hd512_kernel_id,
        .source_path = first_decode_attention_1x_cuda_split4_hd512_source_path,
        .check_command = first_decode_attention_1x_cuda_split4_hd512_check_command,
        .production_enabled = false,
        .cuda_attention_kernel = .gqa_decode_split4_kv_hd512_f32,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .decode_1x,
            .head_dim = 256,
            .schedule = first_decode_attention_1x_cuda_score_prework_hd256_schedule,
        } },
        .kernel_id = first_decode_attention_1x_cuda_score_prework_hd256_kernel_id,
        .source_path = first_decode_attention_1x_cuda_score_prework_hd256_source_path,
        .check_command = first_decode_attention_1x_cuda_score_prework_hd256_check_command,
        .runtime_evidence_command = first_decode_attention_1x_cuda_score_prework_runtime_evidence_command,
        .promotion_evidence_command = first_decode_attention_1x_cuda_score_prework_promotion_evidence_command,
        .production_enabled = true,
        .runtime_default_enabled = true,
        .cuda_attention_kernel = .gqa_decode_score_prework_hd256_f32,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .decode_1x,
            .head_dim = 512,
            .schedule = first_decode_attention_1x_cuda_score_prework_hd512_schedule,
        } },
        .kernel_id = first_decode_attention_1x_cuda_score_prework_hd512_kernel_id,
        .source_path = first_decode_attention_1x_cuda_score_prework_hd512_source_path,
        .check_command = first_decode_attention_1x_cuda_score_prework_hd512_check_command,
        .runtime_evidence_command = first_decode_attention_1x_cuda_score_prework_runtime_evidence_command,
        .promotion_evidence_command = first_decode_attention_1x_cuda_score_prework_promotion_evidence_command,
        .production_enabled = true,
        .runtime_default_enabled = true,
        .cuda_attention_kernel = .gqa_decode_score_prework_hd512_f32,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .decode_1x,
            .head_dim = 256,
            .schedule = first_decode_splitk_online_cuda_hd256_schedule,
        } },
        .kernel_id = first_decode_splitk_online_cuda_hd256_kernel_id,
        .source_path = first_decode_splitk_online_cuda_hd256_source_path,
        .check_command = first_decode_splitk_online_cuda_hd256_check_command,
        .runtime_evidence_command = "scripts/build_cuda_gqa_decode_splitk_online_prototype.sh --output-dir /tmp/antfly-cuda-gqa-decode-splitk-online-evidence",
        .production_enabled = false,
        .runtime_default_enabled = false,
        .cuda_splitk_online_decode_kernel = .gqa_decode_splitk_online_sm89_hd256_swa512_f16_f32,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .decode_1x,
            .head_dim = 512,
            .schedule = first_decode_splitk_online_cuda_hd512_schedule,
        } },
        .kernel_id = first_decode_splitk_online_cuda_hd512_kernel_id,
        .source_path = first_decode_splitk_online_cuda_hd512_source_path,
        .check_command = first_decode_splitk_online_cuda_hd512_check_command,
        .runtime_evidence_command = "scripts/build_cuda_gqa_decode_splitk_online_prototype.sh --output-dir /tmp/antfly-cuda-gqa-decode-splitk-online-evidence",
        .production_enabled = false,
        .runtime_default_enabled = false,
        .cuda_splitk_online_decode_kernel = .gqa_decode_splitk_online_sm89_hd512_global_f16_f32,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .prefill_flash,
            .head_dim = 256,
            .schedule = first_prefill_flash_cuda_hd256_schedule,
        } },
        .kernel_id = first_prefill_flash_cuda_hd256_kernel_id,
        .source_path = first_prefill_flash_cuda_hd256_source_path,
        .check_command = first_prefill_flash_cuda_hd256_check_command,
        .runtime_evidence_command = first_prefill_flash_cuda_runtime_evidence_command,
        .promotion_evidence_command = first_prefill_flash_cuda_promotion_evidence_command,
        .production_enabled = true,
        .runtime_default_enabled = true,
        .cuda_flash_prefill_kernel = .gqa_prefill_flash_sm89_hd256_swa512_f32,
    },
    .{
        .backend = .cuda,
        .op = .{ .attention = .{
            .kind = .prefill_flash,
            .head_dim = 512,
            .schedule = first_prefill_flash_cuda_hd512_schedule,
        } },
        .kernel_id = first_prefill_flash_cuda_hd512_kernel_id,
        .source_path = first_prefill_flash_cuda_hd512_source_path,
        .check_command = first_prefill_flash_cuda_hd512_check_command,
        .runtime_evidence_command = first_prefill_flash_cuda_runtime_evidence_command,
        .promotion_evidence_command = first_prefill_flash_cuda_promotion_evidence_command,
        .production_enabled = true,
        .runtime_default_enabled = true,
        .cuda_flash_prefill_kernel = .gqa_prefill_flash_sm89_hd512_global_f32,
    },
    .{
        .backend = .metal,
        .op = .{ .attention = .{
            .kind = .decode_1x,
            .schedule = first_decode_attention_1x_metal_schedule,
        } },
        .kernel_id = first_decode_attention_1x_metal_kernel_id,
        .source_path = first_decode_attention_1x_metal_source_path,
        .check_command = first_decode_attention_1x_metal_check_command,
        .production_enabled = false,
    },
    .{
        .backend = .metal,
        .op = .{ .attention = .{
            .kind = .prefill_flash,
            .head_dim = 256,
            .schedule = first_prefill_flash_metal_schedule,
        } },
        .kernel_id = first_prefill_flash_metal_kernel_id,
        .source_path = first_prefill_flash_metal_source_path,
        .check_command = first_prefill_flash_metal_check_command,
        .production_enabled = true,
        .runtime_default_enabled = true,
    },
    .{
        .backend = .metal,
        .op = .{ .attention = .{
            .kind = .prefill_flash,
            .head_dim = 512,
            .schedule = first_prefill_flash_hd512_metal_schedule,
        } },
        .kernel_id = first_prefill_flash_hd512_metal_kernel_id,
        .source_path = first_prefill_flash_hd512_metal_source_path,
        .check_command = first_prefill_flash_hd512_metal_check_command,
        .production_enabled = true,
        .runtime_default_enabled = true,
    },
};

fn generatedArtifactCount(comptime kind: OpKind) usize {
    var count: usize = 0;
    for (first_generated_artifacts) |artifact| {
        if (artifact.opKind() == kind) count += 1;
    }
    return count;
}

pub fn matmulArtifactView(artifact: GeneratedArtifact) GeneratedMatmulArtifact {
    const op = artifact.matmulOp() orelse unreachable;
    return .{
        .backend = artifact.backend,
        .format = op.format,
        .row_bucket = op.row_bucket,
        .epilogue = op.epilogue,
        .activation = op.activation,
        .function = op.function,
        .output = op.output,
        .kernel_id = artifact.kernel_id,
        .source_path = artifact.source_path,
        .check_command = artifact.check_command,
        .runtime_evidence_command = artifact.runtime_evidence_command,
        .promotion_evidence_command = artifact.promotion_evidence_command,
        .promotion_check_command = artifact.promotion_check_command,
        .production_enabled = artifact.production_enabled,
        .runtime_default_enabled = artifact.runtime_default_enabled,
        .runtime_shape = artifact.runtime_shape,
        .cuda_kernel = artifact.cuda_kernel,
        .cuda_attention_kernel = artifact.cuda_attention_kernel,
        .cuda_flash_prefill_kernel = artifact.cuda_flash_prefill_kernel,
        .cuda_splitk_online_decode_kernel = artifact.cuda_splitk_online_decode_kernel,
    };
}

/// Matmul-only compatibility view for routing, benchmark, and promotion code.
/// The unified registry above remains the only authored artifact list.
pub const first_generated_matmul_artifacts = blk: {
    var artifacts: [generatedArtifactCount(.small_batch_matmul)]GeneratedMatmulArtifact = undefined;
    var index: usize = 0;
    for (first_generated_artifacts) |artifact| {
        if (artifact.opKind() != .small_batch_matmul) continue;
        artifacts[index] = matmulArtifactView(artifact);
        index += 1;
    }
    break :blk artifacts;
};

/// Compatibility filters used by the on-device operation-specific harnesses.
/// They are derived from the same authoritative registry as matmul artifacts.
pub const first_generated_microkernel_artifacts = blk: {
    var artifacts: [generatedArtifactCount(.microkernel)]GeneratedArtifact = undefined;
    var index: usize = 0;
    for (first_generated_artifacts) |artifact| {
        if (artifact.opKind() != .microkernel) continue;
        artifacts[index] = artifact;
        index += 1;
    }
    break :blk artifacts;
};

pub const first_generated_attention_artifacts = blk: {
    var artifacts: [generatedArtifactCount(.attention)]GeneratedArtifact = undefined;
    var index: usize = 0;
    for (first_generated_artifacts) |artifact| {
        if (artifact.opKind() != .attention) continue;
        artifacts[index] = artifact;
        index += 1;
    }
    break :blk artifacts;
};

fn metalRuntimeDefaultHasAttestedEvidence(
    artifact: GeneratedMatmulArtifact,
    evidence_records: []const MetalRuntimeEvidence,
) bool {
    if (artifact.backend != .metal or !artifact.runtime_default_enabled) return true;
    const evidence = metalRuntimeEvidenceFor(artifact, evidence_records) orelse return false;
    return !evidence.legacy_production_exception and
        std.mem.eql(u8, evidence.provenance_status, metal_evidence_provenance_attested_v1) and
        std.mem.eql(u8, metalRuntimeEvidenceProvenanceBlocker(evidence), metal_blocker_none);
}

pub fn validateGeneratedArtifactRegistry() !void {
    for (first_generated_artifacts, 0..) |artifact, index| {
        if (artifact.kernel_id.len == 0) return error.GeneratedArtifactKernelIdMissing;
        if (artifact.source_path.len == 0) return error.GeneratedArtifactSourcePathMissing;
        if (artifact.check_command.len == 0) return error.GeneratedArtifactCheckCommandMissing;
        if (artifact.runtime_default_enabled and !artifact.production_enabled) {
            return error.GeneratedArtifactRuntimeDefaultRequiresProduction;
        }
        if (artifact.backend == .metal and artifact.runtime_default_enabled) {
            if (artifact.matmulOp()) |matmul| {
                if (!metalRuntimeDefaultHasAttestedEvidence(matmulArtifactView(artifact), &first_metal_runtime_evidence)) {
                    return error.GeneratedArtifactMetalRuntimeDefaultRequiresAttestedEvidence;
                }
                _ = matmul;
            } else if (artifact.attentionOp() == null) {
                return error.GeneratedArtifactMetalRuntimeDefaultRequiresAttestedEvidence;
            }
        }
        if (artifact.backend == .cuda) {
            switch (artifact.opKind()) {
                .small_batch_matmul => {
                    if (artifact.cuda_kernel == null or artifact.cuda_attention_kernel != null or
                        artifact.cuda_flash_prefill_kernel != null or
                        artifact.cuda_splitk_online_decode_kernel != null)
                    {
                        return error.GeneratedArtifactCudaPlanMissing;
                    }
                    if (cudaRenderPlanForArtifact(artifact) == null) return error.GeneratedArtifactCudaPlanInvalid;
                },
                .attention => {
                    const plan_count = @intFromBool(artifact.cuda_attention_kernel != null) +
                        @intFromBool(artifact.cuda_flash_prefill_kernel != null) +
                        @intFromBool(artifact.cuda_splitk_online_decode_kernel != null);
                    if (artifact.cuda_kernel != null or plan_count != 1) {
                        return error.GeneratedArtifactCudaPlanMissing;
                    }
                    if (artifact.cuda_attention_kernel != null) {
                        if (cudaAttentionRenderPlanForArtifact(artifact) == null) return error.GeneratedArtifactCudaPlanInvalid;
                    } else if (artifact.cuda_flash_prefill_kernel != null) {
                        if (cudaFlashPrefillRenderPlanForArtifact(artifact) == null) return error.GeneratedArtifactCudaPlanInvalid;
                    } else if (cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact) == null) {
                        return error.GeneratedArtifactCudaPlanInvalid;
                    }
                },
                .microkernel => return error.GeneratedArtifactCudaPlanMissing,
            }
        } else if (artifact.cuda_kernel != null or artifact.cuda_attention_kernel != null or
            artifact.cuda_flash_prefill_kernel != null or
            artifact.cuda_splitk_online_decode_kernel != null)
        {
            return error.GeneratedArtifactCudaPlanOnWrongBackend;
        }
        if (generatedSourceForArtifact(artifact) == null) return error.GeneratedArtifactSourceMissing;

        switch (artifact.op) {
            .small_batch_matmul => |op| {
                if (specFor(op.format) == null) return error.GeneratedArtifactFormatUnsupported;
            },
            .microkernel => |op| {
                if (op.schedule.threads_per_threadgroup == 0) return error.GeneratedArtifactScheduleInvalid;
            },
            .attention => |op| {
                if (op.schedule.threads_per_threadgroup == 0 or op.schedule.key_chunk == 0) {
                    return error.GeneratedArtifactScheduleInvalid;
                }
                if (artifact.backend == .cuda) {
                    if (op.head_dim == 0 or op.schedule.attention_query_heads_per_kv_head == 0) {
                        return error.GeneratedArtifactScheduleInvalid;
                    }
                    if (artifact.cuda_splitk_online_decode_kernel != null) {
                        const max_tokens = if (op.head_dim == 256)
                            cuda_renderer.generated_splitk_online_decode_hd256_max_visible_tokens
                        else
                            cuda_renderer.generated_splitk_online_decode_hd512_max_visible_tokens;
                        if (op.kind != .decode_1x or
                            op.schedule.threads_per_threadgroup != cuda_renderer.generated_splitk_online_decode_threads or
                            op.schedule.attention_serial_threads_per_threadgroup != 0 or
                            op.schedule.attention_stage2_threads_per_threadgroup != 0 or
                            op.schedule.attention_tiled64_threads_per_threadgroup != 0 or
                            op.schedule.attention_kv_splits != cuda_renderer.generated_splitk_online_decode_splits or
                            op.schedule.attention_query_heads_per_kv_head != 8 or
                            op.schedule.attention_query_tile != 0 or
                            op.schedule.attention_key_tile != 0 or
                            op.schedule.attention_page_size_tokens != cuda_renderer.generated_splitk_online_decode_page_size_tokens or
                            op.schedule.attention_dynamic_shared_memory_bytes != 0 or
                            op.schedule.attention_required_compute_major != 8 or
                            op.schedule.attention_required_compute_minor != 9 or
                            op.schedule.attention_query_length_policy != null or
                            op.schedule.attention_storage != .f32 or
                            op.schedule.attention_key_storage != .paged_f16 or
                            op.schedule.attention_value_storage != .paged_f16 or
                            op.schedule.attention_split_kv_min_tokens != 0 or
                            op.schedule.attention_max_kv_tokens != max_tokens or
                            op.schedule.attention_tiled64_max_kv_tokens != 0)
                        {
                            return error.GeneratedArtifactScheduleInvalid;
                        }
                    } else if (op.kind == .prefill_flash) {
                        if (op.schedule.threads_per_threadgroup != cuda_renderer.generated_flash_prefill_threads or
                            op.schedule.attention_serial_threads_per_threadgroup != 0 or
                            op.schedule.attention_stage2_threads_per_threadgroup != 0 or
                            op.schedule.attention_tiled64_threads_per_threadgroup != 0 or
                            op.schedule.attention_kv_splits != 1 or
                            op.schedule.attention_query_heads_per_kv_head != 8 or
                            op.schedule.attention_query_tile != cuda_renderer.generated_flash_prefill_query_tile or
                            op.schedule.attention_key_tile != cuda_renderer.generated_flash_prefill_key_tile or
                            op.schedule.attention_page_size_tokens != cuda_renderer.generated_flash_prefill_page_size_tokens or
                            op.schedule.attention_dynamic_shared_memory_bytes != (cuda_renderer.generatedFlashPrefillDynamicSharedBytes(op.head_dim) orelse 0) or
                            op.schedule.attention_required_compute_major != 8 or
                            op.schedule.attention_required_compute_minor != 9 or
                            op.schedule.attention_query_length_policy != .gemma4_q512_or_q3_v1 or
                            op.schedule.attention_storage != .f32 or
                            op.schedule.attention_key_storage != .paged_f16 or
                            op.schedule.attention_value_storage != .paged_f16 or
                            op.schedule.attention_split_kv_min_tokens != 0 or
                            op.schedule.attention_max_kv_tokens != 0 or
                            op.schedule.attention_tiled64_max_kv_tokens != 0)
                        {
                            return error.GeneratedArtifactScheduleInvalid;
                        }
                    } else {
                        if (op.schedule.attention_serial_threads_per_threadgroup == 0 or
                            op.schedule.attention_stage2_threads_per_threadgroup == 0 or
                            op.schedule.attention_kv_splits == 0)
                        {
                            return error.GeneratedArtifactScheduleInvalid;
                        }
                        const score_prework = op.schedule.attention_key_storage == .paged_f16_or_polar4;
                        if (score_prework) {
                            if (op.schedule.attention_split_kv_min_tokens != 0 or
                                op.schedule.attention_max_kv_tokens == 0 or
                                op.schedule.attention_tiled64_threads_per_threadgroup != cuda_renderer.generated_attention_score_prework_tiled64_tile_size or
                                op.schedule.attention_tiled64_max_kv_tokens != (cuda_renderer.generatedAttentionScorePreworkTiled64MaxKvTokens(op.head_dim) orelse 0) or
                                op.schedule.attention_value_storage != .paged_f16_or_f32)
                            {
                                return error.GeneratedArtifactScheduleInvalid;
                            }
                        } else if (op.schedule.attention_split_kv_min_tokens == 0 or
                            op.schedule.attention_max_kv_tokens != 0 or
                            op.schedule.attention_tiled64_threads_per_threadgroup != 0 or
                            op.schedule.attention_tiled64_max_kv_tokens != 0 or
                            op.schedule.attention_value_storage != .f32)
                        {
                            return error.GeneratedArtifactScheduleInvalid;
                        }
                    }
                }
            },
        }

        for (first_generated_artifacts[index + 1 ..]) |other| {
            if (std.mem.eql(u8, artifact.kernel_id, other.kernel_id)) return error.GeneratedArtifactKernelIdDuplicate;
            if (std.mem.eql(u8, artifact.source_path, other.source_path)) return error.GeneratedArtifactSourcePathDuplicate;
            if (cudaAttentionRenderPlanForArtifact(artifact)) |plan| {
                if (cudaAttentionRenderPlanForArtifact(other)) |other_plan| {
                    if (std.mem.eql(u8, plan.source_id, other_plan.source_id)) {
                        return error.GeneratedArtifactCudaAttentionSourceIdDuplicate;
                    }
                }
            }
            if (cudaFlashPrefillRenderPlanForArtifact(artifact)) |plan| {
                if (cudaFlashPrefillRenderPlanForArtifact(other)) |other_plan| {
                    if (std.mem.eql(u8, plan.source_id, other_plan.source_id)) {
                        return error.GeneratedArtifactCudaAttentionSourceIdDuplicate;
                    }
                }
            }
            if (cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact)) |plan| {
                if (cudaSplitkOnlineDecodeRenderPlanForArtifact(other)) |other_plan| {
                    if (std.mem.eql(u8, plan.source_id, other_plan.source_id)) {
                        return error.GeneratedArtifactCudaAttentionSourceIdDuplicate;
                    }
                }
            }
        }
    }
}

const first_route_expectations = [_]RouteExpectation{
    .{
        .backend = .cuda,
        .format = .q4_0,
        .row_bucket = .rows_1,
        .epilogue = .none,
        .dispatch = .mmv,
        .production_route = .generated_production,
        .candidate_route = .unsupported,
        .fallback_reason = .none,
    },
    .{
        .backend = .cuda,
        .format = .q4_0,
        .row_bucket = .rows_9_64,
        .epilogue = .none,
        .dispatch = .mm,
        .production_route = .generated_production,
        .candidate_route = .unsupported,
        .fallback_reason = .none,
    },
    .{
        .backend = .cuda,
        .format = .q4_0,
        .row_bucket = .rows_1,
        .epilogue = .pair,
        .dispatch = .mmv,
        .production_route = .generated_production,
        .candidate_route = .unsupported,
        .fallback_reason = .none,
    },
    .{
        .backend = .cuda,
        .format = .q4_0,
        .row_bucket = .rows_1,
        .epilogue = .pair_activation,
        .dispatch = .mmv,
        .production_route = .generated_production,
        .candidate_route = .unsupported,
        .fallback_reason = .none,
    },
    .{
        .backend = .cuda,
        .format = .q4_0,
        .row_bucket = .rows_1,
        .epilogue = .gated_down,
        .dispatch = .mmv,
        .production_route = .generated_production,
        .candidate_route = .unsupported,
        .fallback_reason = .none,
    },
    .{
        .backend = .cuda,
        .format = .q4_0,
        .row_bucket = .rows_1,
        .epilogue = .argmax,
        .dispatch = .mmv,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_runtime_not_wired,
    },
    .{
        .backend = .cuda,
        .format = .q4_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias_gelu,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q4_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias_gelu,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .cuda,
        .format = .q8_0,
        .row_bucket = .rows_1,
        .epilogue = .none,
        .dispatch = .mmv,
        .production_route = .handwritten_production,
        .candidate_route = .unsupported,
        .fallback_reason = .none,
    },
    .{
        .backend = .cuda,
        .format = .q5_k,
        .row_bucket = .rows_2_8,
        .epilogue = .none,
        .dispatch = .small_batch,
        .production_route = .unsupported,
        .candidate_route = .unsupported,
        .fallback_reason = .unsupported_backend,
    },
    .{
        .backend = .cuda,
        .format = .q5_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias_gelu,
        .dispatch = .small_batch,
        .production_route = .unsupported,
        .candidate_route = .unsupported,
        .fallback_reason = .unsupported_backend,
    },
    .{
        .backend = .metal,
        .format = .q1_0,
        .row_bucket = .rows_2_8,
        .epilogue = .none,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .unsupported,
        .fallback_reason = .none,
    },
    .{
        .backend = .metal,
        .format = .q2_k,
        .row_bucket = .rows_2_8,
        .epilogue = .none,
        .dispatch = .small_batch,
        .production_route = .generated_production,
        .candidate_route = .unsupported,
        .fallback_reason = .none,
    },
    .{
        .backend = .metal,
        .format = .q2_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q2_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias_gelu,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q3_k,
        .row_bucket = .rows_2_8,
        .epilogue = .none,
        .dispatch = .small_batch,
        .production_route = .generated_production,
        .candidate_route = .unsupported,
        .fallback_reason = .none,
    },
    .{
        .backend = .metal,
        .format = .q3_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q3_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias_gelu,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q5_0,
        .row_bucket = .rows_2_8,
        .epilogue = .none,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q5_1,
        .row_bucket = .rows_2_8,
        .epilogue = .none,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q8_k,
        .row_bucket = .rows_2_8,
        .epilogue = .none,
        .dispatch = .small_batch,
        .production_route = .generated_production,
        .candidate_route = .unsupported,
        .fallback_reason = .none,
    },
    .{
        .backend = .metal,
        .format = .q8_1,
        .row_bucket = .rows_2_8,
        .epilogue = .none,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q8_0,
        .row_bucket = .rows_2_8,
        .epilogue = .bias,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q8_0,
        .row_bucket = .rows_2_8,
        .epilogue = .bias_gelu,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q5_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q5_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias_gelu,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q6_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .metal,
        .format = .q6_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias_gelu,
        .dispatch = .small_batch,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .fallback_reason = .generated_artifact_missing,
    },
    .{
        .backend = .cuda,
        .format = .q6_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias_gelu,
        .dispatch = .small_batch,
        .production_route = .unsupported,
        .candidate_route = .unsupported,
        .fallback_reason = .unsupported_epilogue,
    },
    .{
        .backend = .cuda,
        .format = .unknown,
        .row_bucket = .rows_2_8,
        .epilogue = .bias_gelu,
        .dispatch = .small_batch,
        .production_route = .unsupported,
        .candidate_route = .unsupported,
        .fallback_reason = .unsupported_format,
    },
    .{
        .backend = .cuda,
        .format = .q4_k,
        .row_bucket = .rows_2_8,
        .epilogue = .bias_gelu,
        .dispatch = .mm,
        .production_route = .unsupported,
        .candidate_route = .unsupported,
        .fallback_reason = .unsupported_shape,
    },
};

fn renderCudaKernelSource(comptime kind: cuda_renderer.KernelKind) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(50_000_000);
        var buf: [1 << 17]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        const rendered = cuda_renderer.renderKernel(fba.allocator(), cuda_renderer.planFor(kind)) catch
            @compileError("CUDA renderKernel failed for " ++ @tagName(kind));
        const source: [rendered.len]u8 = rendered[0..rendered.len].*;
        break :blk &source;
    };
}

fn renderCudaAttentionSource(comptime kind: cuda_renderer.AttentionKernelKind) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(50_000_000);
        var buf: [1 << 17]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        const rendered = cuda_renderer.renderAttentionKernel(fba.allocator(), cuda_renderer.attentionPlanFor(kind)) catch
            @compileError("CUDA renderAttentionKernel failed for " ++ @tagName(kind));
        const source: [rendered.len]u8 = rendered[0..rendered.len].*;
        break :blk &source;
    };
}

fn renderCudaFlashPrefillSource(comptime kind: cuda_renderer.FlashPrefillKernelKind) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(50_000_000);
        var buf: [1 << 17]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        const rendered = cuda_renderer.renderFlashPrefillKernel(fba.allocator(), cuda_renderer.flashPrefillPlanFor(kind)) catch
            @compileError("CUDA renderFlashPrefillKernel failed for " ++ @tagName(kind));
        const source: [rendered.len]u8 = rendered[0..rendered.len].*;
        break :blk &source;
    };
}

fn renderCudaSplitkOnlineDecodeSource(comptime kind: cuda_renderer.SplitkOnlineDecodeKernelKind) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(50_000_000);
        var buf: [1 << 17]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        const rendered = cuda_renderer.renderSplitkOnlineDecodeKernel(fba.allocator(), cuda_renderer.splitkOnlineDecodePlanFor(kind)) catch
            @compileError("CUDA renderSplitkOnlineDecodeKernel failed for " ++ @tagName(kind));
        const source: [rendered.len]u8 = rendered[0..rendered.len].*;
        break :blk &source;
    };
}

const first_lazy_cuda_source = renderCudaKernelSource(.q4_k_small_batch_bias_gelu);
const first_general_cuda_q4_k_mmv_source = renderCudaKernelSource(.q4_k_mmv);
const first_general_cuda_q4_0_mmv_source = renderCudaKernelSource(.q4_0_mmv);
const first_general_cuda_q4_0_mm_source = renderCudaKernelSource(.q4_0_mm);
const first_general_cuda_q4_0_pair_source = renderCudaKernelSource(.q4_0_pair_mmv);
const first_general_cuda_q4_0_pair_q8_source = renderCudaKernelSource(.q4_0_pair_activation_q8_1);
const first_general_cuda_q4_0_down_q8_source = renderCudaKernelSource(.q4_0_down_q8_1);
const first_e2b_cuda_q4_0_pair_q8_6144_source = renderCudaKernelSource(.q4_0_pair_activation_q8_1_e2b_6144);
const first_e2b_cuda_q4_0_pair_q8_12288_source = renderCudaKernelSource(.q4_0_pair_activation_q8_1_e2b_12288);
const first_e2b_cuda_q4_0_down_q8_6144_source = renderCudaKernelSource(.q4_0_down_q8_1_e2b_6144);
const first_e2b_cuda_q4_0_down_q8_12288_source = renderCudaKernelSource(.q4_0_down_q8_1_e2b_12288);
const first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_source = renderCudaKernelSource(.q4_0_pair_activation_ggml_q8_1_e2b_6144);
const first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_source = renderCudaKernelSource(.q4_0_pair_activation_ggml_q8_1_e2b_12288);
const first_e2b_cuda_q4_0_down_ggml_q8_1_6144_source = renderCudaKernelSource(.q4_0_down_ggml_q8_1_e2b_6144);
const first_e2b_cuda_q4_0_down_ggml_q8_1_12288_source = renderCudaKernelSource(.q4_0_down_ggml_q8_1_e2b_12288);
const first_e2b_cuda_q4_0_pair_f32_6144_exact_source = renderCudaKernelSource(.q4_0_pair_activation_f32_e2b_6144_exact);
const first_e2b_cuda_q4_0_pair_f32_12288_exact_source = renderCudaKernelSource(.q4_0_pair_activation_f32_e2b_12288_exact);
const first_e2b_cuda_q4_0_down_f32_6144_exact_source = renderCudaKernelSource(.q4_0_down_f32_e2b_6144_exact);
const first_e2b_cuda_q4_0_down_f32_12288_exact_source = renderCudaKernelSource(.q4_0_down_f32_e2b_12288_exact);
const first_e2b_cuda_q4_0_q8_1_argmax_source = renderCudaKernelSource(.q4_0_q8_1_argmax_e2b_tile8);
const first_cuda_q6_k_q8_1_argmax_k2560_source = renderCudaKernelSource(.q6_k_q8_1_argmax_k2560_tile8);
const first_cuda_q6_k_q8_1_argmax_k3840_source = renderCudaKernelSource(.q6_k_q8_1_argmax_k3840_tile8);
const first_decode_attention_1x_cuda_source = renderCudaAttentionSource(.gqa_decode_split_kv_hd256_f32);
const first_decode_attention_1x_cuda_hd512_source = renderCudaAttentionSource(.gqa_decode_split_kv_hd512_f32);
const first_decode_attention_1x_cuda_split2_hd256_source = renderCudaAttentionSource(.gqa_decode_split2_kv_hd256_f32);
const first_decode_attention_1x_cuda_split2_hd512_source = renderCudaAttentionSource(.gqa_decode_split2_kv_hd512_f32);
const first_decode_attention_1x_cuda_split4_hd256_source = renderCudaAttentionSource(.gqa_decode_split4_kv_hd256_f32);
const first_decode_attention_1x_cuda_split4_hd512_source = renderCudaAttentionSource(.gqa_decode_split4_kv_hd512_f32);
const first_decode_attention_1x_cuda_score_prework_hd256_source = renderCudaAttentionSource(.gqa_decode_score_prework_hd256_f32);
const first_decode_attention_1x_cuda_score_prework_hd512_source = renderCudaAttentionSource(.gqa_decode_score_prework_hd512_f32);
const first_prefill_flash_cuda_hd256_source = renderCudaFlashPrefillSource(.gqa_prefill_flash_sm89_hd256_swa512_f32);
const first_prefill_flash_cuda_hd512_source = renderCudaFlashPrefillSource(.gqa_prefill_flash_sm89_hd512_global_f32);
const first_decode_splitk_online_cuda_hd256_source = renderCudaSplitkOnlineDecodeSource(.gqa_decode_splitk_online_sm89_hd256_swa512_f16_f32);
const first_decode_splitk_online_cuda_hd512_source = renderCudaSplitkOnlineDecodeSource(.gqa_decode_splitk_online_sm89_hd512_global_f16_f32);

/// Source fingerprints cover the plan header, including the explicit split
/// schedule/source ID. They are independent for each candidate even where the
/// lowering body is otherwise structurally identical.
pub const first_decode_attention_1x_cuda_source_fingerprint = sourceFingerprint(first_decode_attention_1x_cuda_source);
pub const first_decode_attention_1x_cuda_hd512_source_fingerprint = sourceFingerprint(first_decode_attention_1x_cuda_hd512_source);
pub const first_decode_attention_1x_cuda_split2_hd256_source_fingerprint = sourceFingerprint(first_decode_attention_1x_cuda_split2_hd256_source);
pub const first_decode_attention_1x_cuda_split2_hd512_source_fingerprint = sourceFingerprint(first_decode_attention_1x_cuda_split2_hd512_source);
pub const first_decode_attention_1x_cuda_split4_hd256_source_fingerprint = sourceFingerprint(first_decode_attention_1x_cuda_split4_hd256_source);
pub const first_decode_attention_1x_cuda_split4_hd512_source_fingerprint = sourceFingerprint(first_decode_attention_1x_cuda_split4_hd512_source);
pub const first_prefill_flash_cuda_hd256_source_fingerprint = sourceFingerprint(first_prefill_flash_cuda_hd256_source);
pub const first_prefill_flash_cuda_hd512_source_fingerprint = sourceFingerprint(first_prefill_flash_cuda_hd512_source);
pub const first_decode_splitk_online_cuda_hd256_source_fingerprint = sourceFingerprint(first_decode_splitk_online_cuda_hd256_source);
pub const first_decode_splitk_online_cuda_hd512_source_fingerprint = sourceFingerprint(first_decode_splitk_online_cuda_hd512_source);
pub const first_decode_attention_1x_cuda_score_prework_hd256_source_fingerprint = sourceFingerprint(first_decode_attention_1x_cuda_score_prework_hd256_source);
pub const first_decode_attention_1x_cuda_score_prework_hd512_source_fingerprint = sourceFingerprint(first_decode_attention_1x_cuda_score_prework_hd512_source);

const metal_generated_source_license_header =
    \\// Copyright 2026 Antfly, Inc.
    \\//
    \\// Licensed under the Apache License, Version 2.0 (the "License");
    \\// you may not use this file except in compliance with the License.
    \\// You may obtain a copy of the License at
    \\//
    \\//     http://www.apache.org/licenses/LICENSE-2.0
    \\//
    \\// Unless required by applicable law or agreed to in writing, software
    \\// distributed under the License is distributed on an "AS IS" BASIS,
    \\// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    \\// See the License for the specific language governing permissions and
    \\// limitations under the License.
;

// Shared Metal helper defined in src/backends/metal_kernels.m OUTSIDE the
// codegen-owned quant kernel region (the termite q8_0 kernels own it there).
// The runtime-embedded antfly_q8_0_dequant_lane calls it, so standalone
// generated q8_0 sources duplicate the definition; the codegen tool verifies
// the metal_kernels.m copy still matches this text byte-for-byte.
const metal_rt_external_helper_termite_q8_0_block_scale =
    \\inline float termite_q8_0_block_scale(device const uchar *weight, uint off) { ushort bits = (ushort(weight[off + 1u]) << 8) | ushort(weight[off]); return float(as_type<half>(bits)); }
;

// Layout drift guard for the generated decode-attention kernel. The generated
// kernel emits its OWN `antfly_paged_attention_1x_params` struct (renderer helper
// fragment) and the dispatch reinterpret-casts the bytes it binds at buffer(6) —
// a `termite_metal_paged_attention_params` value — into it. Both structs share
// `paged_attention_params_field_body`, so this pins the hand-written MSL struct
// in metal_kernels.m (outside the region) to that same field body: if a field is
// added/reordered/retyped there without updating the shared body, the codegen
// `--check` fails, catching a would-be silent reinterpret-cast layout corruption.
// (This is a text drift guard, not a helper the runtime region references.)
const metal_rt_external_helper_paged_attention_params =
    "struct termite_metal_paged_attention_params { " ++ metal_renderer.paged_attention_params_field_body ++ " };";

// Text the codegen tool must find verbatim in metal_kernels.m outside the
// marker-delimited region: shared helpers duplicated into standalone sources,
// plus layout drift guards for structs the generated kernels reinterpret.
pub const metal_runtime_external_helpers = [_][]const u8{
    metal_rt_external_helper_termite_q8_0_block_scale,
    metal_rt_external_helper_paged_attention_params,
};

/// Returns whether a CUDA attention artifact is linked into the runtime module.
/// The score-prework decode composites are production and default-on for the
/// qualified SM89 Gemma 4 F16 automatic selector; every other entry remains
/// dev-only (`production_enabled = false`), meaning only the explicit
/// generated-attention selector can load its schedule for a benchmark. Keep
/// this exhaustive so a new schedule cannot be linked without an intentional
/// dispatch policy.
pub fn cudaAttentionArtifactRuntimeWired(artifact: GeneratedArtifact) bool {
    if (artifact.backend != .cuda or artifact.opKind() != .attention) return false;
    if (artifact.cuda_attention_kernel) |kind| {
        return switch (kind) {
            .gqa_decode_split_kv_hd256_f32,
            .gqa_decode_split_kv_hd512_f32,
            .gqa_decode_split2_kv_hd256_f32,
            .gqa_decode_split2_kv_hd512_f32,
            .gqa_decode_split4_kv_hd256_f32,
            .gqa_decode_split4_kv_hd512_f32,
            .gqa_decode_score_prework_hd256_f32,
            .gqa_decode_score_prework_hd512_f32,
            => true,
        };
    }
    return cudaFlashPrefillArtifactRuntimeWired(artifact) or
        cudaSplitkOnlineDecodeArtifactRuntimeWired(artifact);
}

pub fn cudaFlashPrefillArtifactRuntimeWired(artifact: GeneratedArtifact) bool {
    if (artifact.backend != .cuda or artifact.opKind() != .attention) return false;
    const kind = artifact.cuda_flash_prefill_kernel orelse return false;
    return switch (kind) {
        .gqa_prefill_flash_sm89_hd256_swa512_f32,
        .gqa_prefill_flash_sm89_hd512_global_f32,
        => true,
    };
}

pub fn cudaSplitkOnlineDecodeArtifactRuntimeWired(artifact: GeneratedArtifact) bool {
    if (artifact.backend != .cuda or artifact.opKind() != .attention) return false;
    const kind = artifact.cuda_splitk_online_decode_kernel orelse return false;
    return switch (kind) {
        .gqa_decode_splitk_online_sm89_hd256_swa512_f16_f32,
        .gqa_decode_splitk_online_sm89_hd512_global_f16_f32,
        => true,
    };
}

/// Renders the marker-delimited generated attention region embedded in
/// `inference_cuda_kernels.cu`. Registry order is authoritative and each entry
/// carries its plan metadata immediately before the renderer-owned kernel body.
pub fn renderCudaRuntimeAttentionRegion(allocator: std.mem.Allocator) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    var emitted: usize = 0;
    for (first_generated_attention_artifacts) |artifact| {
        if (!cudaAttentionArtifactRuntimeWired(artifact)) continue;
        if (emitted != 0) try out.append(allocator, '\n');
        try out.appendSlice(allocator, if (artifact.production_enabled)
            "// Production generated attention route from graph/quant_kernel_compiler.zig.\n"
        else
            "// Opt-in generated attention candidate from graph/quant_kernel_compiler.zig.\n");
        if (cudaFlashPrefillRenderPlanForArtifact(artifact)) |plan| {
            const plan_id = try cuda_renderer.flashPrefillPlanId(plan, allocator);
            defer allocator.free(plan_id);
            const body = try cuda_renderer.renderFlashPrefillBodyAlloc(allocator, plan);
            defer allocator.free(body);
            try appendFmt(allocator, &out, "// kernel_id={s} plan_id={s}\n", .{ plan.kernel_id, plan_id });
            try out.appendSlice(allocator, body);
        } else if (cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact)) |plan| {
            const plan_id = try cuda_renderer.splitkOnlineDecodePlanId(plan, allocator);
            defer allocator.free(plan_id);
            const body = try cuda_renderer.renderSplitkOnlineDecodeBodyAlloc(allocator, plan);
            defer allocator.free(body);
            try appendFmt(allocator, &out, "// kernel_id={s} plan_id={s}\n", .{ plan.kernel_id, plan_id });
            try out.appendSlice(allocator, body);
        } else {
            const plan = cudaAttentionRenderPlanForArtifact(artifact) orelse return error.MissingCudaAttentionRenderPlan;
            const plan_id = try cuda_renderer.attentionPlanId(plan, allocator);
            defer allocator.free(plan_id);
            const body = try cuda_renderer.renderAttentionBodyAlloc(allocator, plan);
            defer allocator.free(body);
            try appendFmt(allocator, &out, "// kernel_id={s} plan_id={s}\n", .{ plan.kernel_id, plan_id });
            try out.appendSlice(allocator, body);
        }
        emitted += 1;
    }
    if (emitted == 0) return error.MissingCudaAttentionRenderPlan;
    return out.toOwnedSlice(allocator);
}

/// The attention runtime region emits bodies only. Keep the block-reduction
/// helpers shared with the handwritten fast baseline byte-compatible and
/// positioned before the generated region.
pub fn validateCudaRuntimeAttentionExternalHelpers(prefix: []const u8) !void {
    for (cuda_renderer.attention_runtime_external_helpers) |helper| {
        if (std.mem.count(u8, prefix, helper.source) != 1) {
            return error.CudaRuntimeAttentionExternalHelperDrift;
        }
    }
    for (cuda_renderer.attention_score_prework_runtime_external_declarations) |declaration| {
        if (std.mem.count(u8, prefix, declaration.source) != 1) {
            return error.CudaRuntimeAttentionExternalHelperDrift;
        }
    }
}

/// Renders runtime-wired CUDA matmul candidates that remain dev-only. These
/// bodies live in their own bundle region after the shared Antfly Q4/Q8 helper
/// definitions, so renderer output can reference those helpers directly.
pub fn renderCudaRuntimeDevMatmulRegion(allocator: std.mem.Allocator) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator, "// Runtime-owned llama.cpp CUDA block_q8_1 support; generated, never hand-edited.\n");
    for (cuda_renderer.ggml_q8_1_runtime_owned_helpers) |helper| {
        try out.appendSlice(allocator, helper.source);
        if (helper.source.len == 0 or helper.source[helper.source.len - 1] != '\n') try out.append(allocator, '\n');
    }
    const quantize_body = try cuda_renderer.renderGgmlQ8_1QuantizeRowsBodyAlloc(allocator);
    defer allocator.free(quantize_body);
    try out.append(allocator, '\n');
    try out.appendSlice(allocator, quantize_body);
    if (quantize_body.len == 0 or quantize_body[quantize_body.len - 1] != '\n') try out.append(allocator, '\n');
    var emitted: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .cuda or artifact.production_enabled or !artifactRuntimeWired(artifact)) continue;
        const plan = cudaRenderPlanForArtifact(artifact) orelse return error.MissingCudaRenderPlan;
        const plan_id = try cuda_renderer.planId(plan, allocator);
        defer allocator.free(plan_id);
        const body = try cuda_renderer.renderBodyAlloc(allocator, plan);
        defer allocator.free(body);

        if (emitted != 0) try out.append(allocator, '\n');
        try out.appendSlice(allocator, "// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.\n");
        try appendFmt(allocator, &out, "// kernel_id={s} plan_id={s}\n", .{ plan.kernel_id, plan_id });
        try out.appendSlice(allocator, body);
        if (body.len == 0 or body[body.len - 1] != '\n') try out.append(allocator, '\n');
        emitted += 1;
    }
    if (emitted == 0) return error.MissingCudaRenderPlan;
    return out.toOwnedSlice(allocator);
}

/// Verifies that renderer helpers intentionally kept outside the owned dev
/// matmul region are defined exactly once before it. This prevents a fresh
/// region from compiling against stale or reordered helper implementations.
pub fn validateCudaRuntimeDevMatmulExternalHelpers(prefix: []const u8) !void {
    var checked_candidates: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .cuda or artifact.production_enabled or !artifactRuntimeWired(artifact)) continue;
        const plan = cudaRenderPlanForArtifact(artifact) orelse return error.MissingCudaRenderPlan;
        for (cuda_renderer.supportFor(plan.lowering).helpers) |helper| {
            if (std.mem.eql(u8, helper.name, cuda_renderer.helper_q4_0_ggml_q8_1_dot16.name)) continue;
            if (std.mem.count(u8, prefix, helper.source) != 1) return error.CudaRuntimeDevMatmulExternalHelperDrift;
        }
        checked_candidates += 1;
    }
    if (checked_candidates == 0) return error.MissingCudaRenderPlan;
}

// Renders the runtime-embedded quant kernel region of
// src/backends/metal_kernels.m as Objective-C string fragment lines. The
// codegen tool rewrites the marker-delimited region of that file with this
// output, so the runtime copy is assembled from the same bytes as the
// checked-in generated sources.
/// Stable kernel name for a small-batch route (matches the wired dispatch names).
pub fn metalRuntimeKernelId(allocator: std.mem.Allocator, format: quant_matmul.Format, epilogue: Epilogue) ![]u8 {
    const epi_suffix = switch (epilogue) {
        .none => "",
        .bias => "_bias",
        .bias_gelu => "_bias_gelu",
        .relu => "_relu",
        else => return error.UnsupportedEpilogueForSection,
    };
    return std.fmt.allocPrint(allocator, "antfly_{s}_small_batch{s}_msl_v1", .{ @tagName(format), epi_suffix });
}

/// Renders the runtime-embedded quant kernel region of metal_kernels.m as
/// Objective-C string fragment lines. Single-sourced: the MSL is produced by the
/// descriptor-driven renderer from `metal_production_schedules` (schedule +
/// FormatDecoder + epilogue), not from frozen body constants.
pub fn renderMetalRuntimeQuantRegion(allocator: std.mem.Allocator) ![]u8 {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var kernels = std.ArrayListUnmanaged(metal_renderer.RegionKernel).empty;
    for (first_generated_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        switch (artifact.op) {
            .small_batch_matmul => |op| {
                const decoder = metal_renderer.decoderFor(op.format) orelse return error.MissingDecoder;
                const schedule = metalRouteScheduleFor(op.format, op.row_bucket, op.epilogue) orelse
                    return error.MissingMetalRouteSchedule;
                try kernels.append(arena, .{
                    .kernel_id = artifact.kernel_id,
                    .decoder = decoder,
                    .schedule = schedule,
                    .epilogue = op.epilogue,
                });
            },
            .microkernel => |op| try kernels.append(arena, .{
                .kernel_id = artifact.kernel_id,
                .op_kind = .microkernel,
                .microkernel = op.kind,
                .schedule = op.schedule,
            }),
            .attention => |op| try kernels.append(arena, .{
                .kernel_id = artifact.kernel_id,
                .op_kind = .attention,
                .attention = op.kind,
                .schedule = op.schedule,
            }),
        }
    }
    const body = try metal_renderer.renderRuntimeRegion(arena, kernels.items);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    var lines = std.mem.splitScalar(u8, body, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        try out.appendSlice(allocator, "           \"");
        try out.appendSlice(allocator, line);
        try out.appendSlice(allocator, "\\n\"\n");
    }
    return out.toOwnedSlice(allocator);
}

/// Renders the C launch-shape table + lookup for metal_kernels.m from
/// `metal_production_schedules`. Single source of truth for the dispatch grid:
/// the generated `termite_metal_generated_quant_launch_shape_for` replaces the
/// hand-written switch. Indented 8 spaces to match the surrounding @autoreleasepool
/// scope style is unnecessary — this is file scope, no indentation.
pub fn renderMetalLaunchShapeRegion(allocator: std.mem.Allocator) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.appendSlice(allocator,
        \\typedef struct termite_metal_generated_quant_launch_entry {
        \\    uint32_t format;
        \\    termite_metal_generated_quant_epilogue epilogue;
        \\    NSUInteger threads_per_threadgroup;
        \\    NSUInteger cols_per_threadgroup;
        \\    NSUInteger rows_per_threadgroup;
        \\} termite_metal_generated_quant_launch_entry;
        \\
        \\static const termite_metal_generated_quant_launch_entry termite_metal_generated_quant_launch_table[] = {
        \\
    );
    for (metal_production_schedules) |entry| {
        const format_c = metalRuntimeQuantFormatConstant(entry.format) orelse return error.MissingMetalFormatConstant;
        const epilogue_c = metalRuntimeGeneratedEpilogueConstant(entry.epilogue) orelse return error.MissingMetalEpilogueConstant;
        try appendFmt(allocator, &out, "    {{ {s}, {s}, {d}u, {d}u, {d}u }},\n", .{
            format_c,
            epilogue_c,
            entry.schedule.threads_per_threadgroup,
            entry.schedule.cols_per_threadgroup,
            entry.schedule.rows_per_threadgroup,
        });
    }
    try out.appendSlice(allocator,
        \\};
        \\
        \\static bool termite_metal_generated_quant_launch_shape_for(
        \\    uint32_t format,
        \\    termite_metal_generated_quant_epilogue epilogue,
        \\    size_t rows,
        \\    termite_metal_generated_quant_launch_shape *shape
        \\) {
        \\    const bool extended_rows = format == TERMITE_METAL_QUANT_FORMAT_Q4_K || format == TERMITE_METAL_QUANT_FORMAT_Q6_K;
        \\    if (shape == NULL || rows < 2u || rows > (extended_rows ? 64u : 8u)) return false;
        \\    const size_t entry_count = sizeof(termite_metal_generated_quant_launch_table) / sizeof(termite_metal_generated_quant_launch_table[0]);
        \\    for (size_t i = 0; i < entry_count; ++i) {
        \\        const termite_metal_generated_quant_launch_entry *entry = &termite_metal_generated_quant_launch_table[i];
        \\        if (entry->format == format && entry->epilogue == epilogue) {
        \\            shape->threads_per_threadgroup = entry->threads_per_threadgroup;
        \\            shape->cols_per_threadgroup = entry->cols_per_threadgroup;
        \\            shape->rows_per_threadgroup = entry->rows_per_threadgroup;
        \\            return true;
        \\        }
        \\    }
        \\    return false;
        \\}
        \\
    );
    return out.toOwnedSlice(allocator);
}

const MetalSmallBatchHeader = struct {
    source_kind: []const u8,
    plan_id: []const u8,
    kernel_id: []const u8,
    production_baseline: []const u8 = "metal_handwritten_quant_matmul",
    production_enabled: bool,
    promotion_comment: []const u8,
};

// Renders a checked-in generated Metal small-batch source: the license header +
// plan/promotion metadata comment block, then the descriptor-driven kernel
// (shared vocabulary helpers + dequant fragment + body) produced by the same
// renderer that emits the runtime-embedded region. Single-sourced from the
// schedule table + FormatDecoder, so re-tuning a route updates this file and the
// runtime region together; only the header metadata is per-artifact. The
// existing runtime renderer runs at comptime here via a FixedBufferAllocator.
fn renderMetalSmallBatchSource(
    comptime header: MetalSmallBatchHeader,
    comptime format: quant_matmul.Format,
    comptime epilogue: Epilogue,
) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(50_000_000);
        const decoder = metal_renderer.decoderFor(format) orelse
            @compileError("missing Metal FormatDecoder for " ++ @tagName(format));
        const schedule = metalRouteScheduleFor(format, .rows_2_8, epilogue) orelse
            @compileError("missing production schedule for " ++ header.kernel_id);
        var buf: [1 << 17]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        const rendered = metal_renderer.renderKernel(fba.allocator(), header.kernel_id, decoder, schedule, epilogue) catch
            @compileError("renderKernel failed for " ++ header.kernel_id);
        const kernel: [rendered.len]u8 = rendered[0..rendered.len].*;
        break :blk metal_generated_source_license_header ++ "\n\n" ++
            "// " ++ header.source_kind ++ " from graph/quant_kernel_compiler.zig.\n" ++
            "// plan_id=" ++ header.plan_id ++ "\n" ++
            "// kernel_id=" ++ header.kernel_id ++ "\n" ++
            "// production_baseline=" ++ header.production_baseline ++ "\n" ++
            "// production_enabled=" ++ (if (header.production_enabled) "true" else "false") ++ "\n" ++
            header.promotion_comment ++ "\n" ++
            "\n" ++
            "#include <metal_stdlib>\n" ++
            "using namespace metal;\n" ++
            "\n" ++
            kernel;
    };
}

const MetalMicrokernelHeader = struct {
    source_kind: []const u8,
    plan_id: []const u8,
    kernel_id: []const u8,
    production_baseline: []const u8,
    production_enabled: bool,
    promotion_comment: []const u8,
};

// Renders a checked-in generated Metal microkernel source: the license header +
// plan/promotion metadata comment block, then the descriptor-driven microkernel
// body from the same renderer that emits the runtime-embedded region. Mirrors
// `renderMetalSmallBatchSource` so the microkernel `.metal` is single-sourced
// the same way (comptime render -> source constant), keeping the fingerprint /
// evidence machinery intact.
fn renderMetalMicrokernelSource(
    comptime header: MetalMicrokernelHeader,
    comptime kind: metal_renderer.MicrokernelKind,
    comptime schedule: KernelSchedule,
) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(50_000_000);
        var buf: [1 << 16]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        const rendered = metal_renderer.renderMicrokernel(fba.allocator(), header.kernel_id, kind, schedule) catch
            @compileError("renderMicrokernel failed for " ++ header.kernel_id);
        const kernel: [rendered.len]u8 = rendered[0..rendered.len].*;
        break :blk metal_generated_source_license_header ++ "\n\n" ++
            "// " ++ header.source_kind ++ " from graph/quant_kernel_compiler.zig.\n" ++
            "// plan_id=" ++ header.plan_id ++ "\n" ++
            "// kernel_id=" ++ header.kernel_id ++ "\n" ++
            "// production_baseline=" ++ header.production_baseline ++ "\n" ++
            "// production_enabled=" ++ (if (header.production_enabled) "true" else "false") ++ "\n" ++
            header.promotion_comment ++ "\n" ++
            "\n" ++
            "#include <metal_stdlib>\n" ++
            "using namespace metal;\n" ++
            "\n" ++
            kernel;
    };
}

const first_rms_norm_metal_source = renderMetalMicrokernelSource(
    .{
        .source_kind = "Generated Metal microkernel artifact",
        .plan_id = "metal/microkernel/rms_norm",
        .kernel_id = first_rms_norm_metal_kernel_id,
        .production_baseline = "termite_apply_rms_norm_rows",
        .production_enabled = false,
        .promotion_comment = "// Descriptor-driven RMSNorm microkernel (first non-matmul route)." ++ "\n" ++
            "// Production RMSNorm stays on the hand-written termite_apply_rms_norm_rows" ++ "\n" ++
            "// until this candidate clears its on-device conformance gate.",
    },
    .rms_norm,
    first_rms_norm_metal_schedule,
);

const MetalAttentionHeader = struct {
    source_kind: []const u8,
    plan_id: []const u8,
    kernel_id: []const u8,
    production_baseline: []const u8,
    production_enabled: bool,
    promotion_comment: []const u8,
};

// Renders a checked-in generated Metal attention source: the license header +
// plan/promotion metadata, then the self-contained attention kernel (its own
// params struct + paging helper + body) from the same renderer that emits the
// runtime-embedded region. Mirrors `renderMetalMicrokernelSource` so the
// attention `.metal` is single-sourced the same way (comptime render -> source
// constant). Unlike the microkernel path, the rendered kernel here carries
// helper fragments, so the standalone `.metal` compiles under `xcrun` with no
// external dependency on metal_kernels.m.
fn renderMetalAttentionSource(
    comptime header: MetalAttentionHeader,
    comptime kind: metal_renderer.AttentionKind,
    comptime schedule: KernelSchedule,
) []const u8 {
    return comptime blk: {
        @setEvalBranchQuota(50_000_000);
        var buf: [1 << 16]u8 = undefined;
        var fba = std.heap.FixedBufferAllocator.init(&buf);
        const rendered = metal_renderer.renderAttention(fba.allocator(), header.kernel_id, kind, schedule) catch
            @compileError("renderAttention failed for " ++ header.kernel_id);
        const kernel: [rendered.len]u8 = rendered[0..rendered.len].*;
        break :blk metal_generated_source_license_header ++ "\n\n" ++
            "// " ++ header.source_kind ++ " from graph/quant_kernel_compiler.zig.\n" ++
            "// plan_id=" ++ header.plan_id ++ "\n" ++
            "// kernel_id=" ++ header.kernel_id ++ "\n" ++
            "// production_baseline=" ++ header.production_baseline ++ "\n" ++
            "// production_enabled=" ++ (if (header.production_enabled) "true" else "false") ++ "\n" ++
            header.promotion_comment ++ "\n" ++
            "\n" ++
            "#include <metal_stdlib>\n" ++
            "using namespace metal;\n" ++
            "\n" ++
            kernel;
    };
}

const first_decode_attention_1x_metal_source = renderMetalAttentionSource(
    .{
        .source_kind = "Generated Metal attention artifact",
        .plan_id = "metal/attention/decode_1x",
        .kernel_id = first_decode_attention_1x_metal_kernel_id,
        .production_baseline = "termite_paged_attention_kv_1x",
        .production_enabled = false,
        .promotion_comment = "// Descriptor-driven paged decode attention (first op_kind=.attention route)." ++ "\n" ++
            "// Production decode attention stays on the hand-written termite_paged_attention_kv_1x" ++ "\n" ++
            "// until this candidate clears its bit-identical model-token acceptance gate.",
    },
    .decode_1x,
    first_decode_attention_1x_metal_schedule,
);

const first_prefill_flash_metal_source = renderMetalAttentionSource(
    .{
        .source_kind = "Generated Metal attention artifact",
        .plan_id = "metal/attention/prefill_flash",
        .kernel_id = first_prefill_flash_metal_kernel_id,
        .production_baseline = "termite_paged_attention_kv_prefill_sg",
        .production_enabled = true,
        .promotion_comment = "// Descriptor-driven low-memory simdgroup-MMA flash prefill attention." ++ "\n" ++
            "// The key_chunk=32/skip_rescale=false baseline preserves the hand-written" ++ "\n" ++
            "// arithmetic order while using page-local K/V loads and direct output stores." ++ "\n" ++
            "// Production defaults to this route only for Gemma4 E4B local attention;" ++ "\n" ++
            "// capability checks and the handwritten fallback remain authoritative.",
    },
    .prefill_flash,
    first_prefill_flash_metal_schedule,
);

const first_prefill_flash_hd512_metal_source = renderMetalAttentionSource(
    .{
        .source_kind = "Generated Metal attention artifact",
        .plan_id = "metal/attention/prefill_flash_hd512",
        .kernel_id = first_prefill_flash_hd512_metal_kernel_id,
        .production_baseline = "termite_paged_attention_kv",
        .production_enabled = true,
        .promotion_comment = "// Low-threadgroup-memory Gemma4 global-attention specialization." ++ "\n" ++
            "// Runtime routing is capability-checked and falls back to scalar paged attention.",
    },
    .prefill_flash,
    first_prefill_flash_hd512_metal_schedule,
);

const first_lazy_metal_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal candidate artifact",
        .plan_id = "metal/q4_k/rows_2_8/bias_gelu/small_batch",
        .kernel_id = "antfly_q4_k_small_batch_bias_gelu_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul epilogues." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q4_k,
    .bias_gelu,
);

const first_general_metal_q4_0_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal candidate artifact",
        .plan_id = "metal/q4_0/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q4_0_small_batch_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q4_0,
    .none,
);

const first_general_metal_q4_1_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal artifact source",
        .plan_id = "metal/q4_1/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q4_1_small_batch_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q4_1,
    .none,
);

const first_general_metal_q5_0_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal candidate",
        .plan_id = "metal/q5_0/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q5_0_small_batch_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q5_0,
    .none,
);

const first_general_metal_q5_1_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal candidate",
        .plan_id = "metal/q5_1/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q5_1_small_batch_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q5_1,
    .none,
);

const first_general_metal_q4_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal artifact source",
        .plan_id = "metal/q4_k/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q4_k_small_batch_msl_v1",
        .production_enabled = true,
        .promotion_comment = "// Promoted after the two-row by sixteen-column register tile cleared" ++ "\n" ++
            "// BGE-M3 model correctness and speed checks; runtime remains opt-in.",
    },
    .q4_k,
    .none,
);

const first_general_metal_q4_bias_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal shadow artifact",
        .plan_id = "metal/q4_k/rows_2_8/bias/small_batch",
        .kernel_id = "antfly_q4_k_small_batch_bias_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// Validated in the direct runtime harness; normal model execution does not" ++ "\n" ++
            "// call the fused-bias API, so production stays on the no-bias route plus bias op.",
    },
    .q4_k,
    .bias,
);

const first_general_metal_q8_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal artifact source",
        .plan_id = "metal/q8_0/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q8_0_small_batch_msl_v1",
        .production_enabled = true,
        .promotion_comment = "// Promoted after sequential Metal runtime evidence cleared correctness," ++ "\n" ++
            "// route, provider-route, and speedup gates.",
    },
    .q8_0,
    .none,
);

const first_general_metal_q8_bias_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal shadow artifact",
        .plan_id = "metal/q8_0/rows_2_8/bias/small_batch",
        .kernel_id = "antfly_q8_0_small_batch_bias_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// Validated in the direct runtime harness; normal model execution does not" ++ "\n" ++
            "// call the fused-bias API, so production stays on the no-bias route plus bias op.",
    },
    .q8_0,
    .bias,
);

const first_general_metal_q8_bias_gelu_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal artifact source",
        .plan_id = "metal/q8_0/rows_2_8/bias_gelu/small_batch",
        .kernel_id = "antfly_q8_0_small_batch_bias_gelu_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul epilogues." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q8_0,
    .bias_gelu,
);

const first_general_metal_q8_relu_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal candidate",
        .plan_id = "metal/q8_0/rows_2_8/relu/small_batch",
        .kernel_id = "antfly_q8_0_small_batch_relu_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul epilogues." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q8_0,
    .relu,
);

const first_general_metal_q2_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal candidate",
        .plan_id = "metal/q2_k/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q2_k_small_batch_msl_v1",
        .production_enabled = true,
        .promotion_comment = "// Promoted after sequential Metal runtime evidence cleared correctness," ++ "\n" ++
            "// route, and speedup gates.",
    },
    .q2_k,
    .none,
);

const first_general_metal_q2_bias_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal candidate",
        .plan_id = "metal/q2_k/rows_2_8/bias/small_batch",
        .kernel_id = "antfly_q2_k_small_batch_bias_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul epilogues." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q2_k,
    .bias,
);

const first_general_metal_q2_bias_gelu_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal candidate",
        .plan_id = "metal/q2_k/rows_2_8/bias_gelu/small_batch",
        .kernel_id = "antfly_q2_k_small_batch_bias_gelu_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul epilogues." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q2_k,
    .bias_gelu,
);

const first_general_metal_q3_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal candidate artifact",
        .plan_id = "metal/q3_k/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q3_k_small_batch_msl_v1",
        .production_enabled = true,
        .promotion_comment = "// Promoted after sequential Metal runtime evidence cleared correctness," ++ "\n" ++
            "// route, provider-route, and speedup gates.",
    },
    .q3_k,
    .none,
);

const first_general_metal_q3_bias_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal candidate",
        .plan_id = "metal/q3_k/rows_2_8/bias/small_batch",
        .kernel_id = "antfly_q3_k_small_batch_bias_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul epilogues." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q3_k,
    .bias,
);

const first_general_metal_q3_bias_gelu_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal candidate",
        .plan_id = "metal/q3_k/rows_2_8/bias_gelu/small_batch",
        .kernel_id = "antfly_q3_k_small_batch_bias_gelu_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul epilogues." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q3_k,
    .bias_gelu,
);

const first_general_metal_q8_1_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal candidate",
        .plan_id = "metal/q8_1/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q8_1_small_batch_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q8_1,
    .none,
);

const first_general_metal_q8_k_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal artifact source",
        .plan_id = "metal/q8_k/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q8_k_small_batch_msl_v1",
        .production_enabled = true,
        .promotion_comment = "// Promoted after the schedule sweep re-tuned this route to 64-thread" ++ "\n" ++
            "// hybrid-simd and the decode-runtime speedup gate cleared vs handwritten.",
    },
    .q8_k,
    .none,
);

const first_general_metal_q5_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal artifact source",
        .plan_id = "metal/q5_k/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q5_k_small_batch_msl_v1",
        .production_enabled = true,
        .promotion_comment = "// Promoted after the schedule sweep re-tuned this route to 256-thread" ++ "\n" ++
            "// hybrid-simd and the decode-runtime speedup gate cleared vs handwritten.",
    },
    .q5_k,
    .none,
);

const first_general_metal_q5_bias_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal shadow artifact",
        .plan_id = "metal/q5_k/rows_2_8/bias/small_batch",
        .kernel_id = "antfly_q5_k_small_batch_bias_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// Validated in the direct runtime harness; normal model execution does not" ++ "\n" ++
            "// call the fused-bias API, so production stays on the no-bias route plus bias op.",
    },
    .q5_k,
    .bias,
);

const first_general_metal_q5_bias_gelu_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal artifact source",
        .plan_id = "metal/q5_k/rows_2_8/bias_gelu/small_batch",
        .kernel_id = "antfly_q5_k_small_batch_bias_gelu_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul epilogues." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q5_k,
    .bias_gelu,
);

const first_general_metal_q6_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal candidate artifact",
        .plan_id = "metal/q6_k/rows_2_8/none/small_batch",
        .kernel_id = "antfly_q6_k_small_batch_msl_v1",
        .production_enabled = true,
        .promotion_comment = "// Promoted after the two-row by sixteen-column register tile cleared" ++ "\n" ++
            "// BGE-M3 model correctness and speed checks; runtime remains opt-in.",
    },
    .q6_k,
    .none,
);

const first_general_metal_q6_bias_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Dev-only generated Metal shadow artifact",
        .plan_id = "metal/q6_k/rows_2_8/bias/small_batch",
        .kernel_id = "antfly_q6_k_small_batch_bias_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// Validated in the direct runtime harness; normal model execution does not" ++ "\n" ++
            "// call the fused-bias API, so production stays on the no-bias route plus bias op.",
    },
    .q6_k,
    .bias,
);

const first_general_metal_q6_bias_gelu_source = renderMetalSmallBatchSource(
    .{
        .source_kind = "Generated Metal candidate artifact",
        .plan_id = "metal/q6_k/rows_2_8/bias_gelu/small_batch",
        .kernel_id = "antfly_q6_k_small_batch_bias_gelu_msl_v1",
        .production_enabled = false,
        .promotion_comment = "// General MSL lowering smoke for descriptor-driven quant matmul epilogues." ++ "\n" ++
            "// Production Metal dispatch stays on native handwritten MSL until this" ++ "\n" ++
            "// candidate clears correctness and benchmark gates.",
    },
    .q6_k,
    .bias_gelu,
);

pub const first_coverage = buildFirstCoverage();

const first_registry_entries = buildFirstRegistry();
pub const first_registry = QuantKernelRegistry{ .entries = &first_registry_entries };

pub const first_conformance = buildFirstConformance();

pub fn specFor(format: quant_matmul.Format) ?QuantKernelSpec {
    return switch (format) {
        .q1_0 => q1_0_spec,
        .i2_s => i2_s_spec,
        .i8_s => i8_s_spec,
        .q2_k => q2_k_spec,
        .q3_k => q3_k_spec,
        .q4_0 => q4_0_spec,
        .q4_1 => q4_1_spec,
        .q5_0 => q5_0_spec,
        .q5_1 => q5_1_spec,
        .q4_k => q4_k_spec,
        .q5_k => q5_k_spec,
        .q6_k => q6_k_spec,
        .q8_0 => q8_0_spec,
        .q8_1 => q8_1_spec,
        .q8_k => q8_k_spec,
        .tq1_0 => tq1_0_spec,
        .tq2_0 => tq2_0_spec,
        .iq2_xxs => iq2_xxs_spec,
        .iq2_xs => iq2_xs_spec,
        .iq2_s => iq2_s_spec,
        .iq3_xxs => iq3_xxs_spec,
        .iq3_s => iq3_s_spec,
        .iq1_s => iq1_s_spec,
        .iq1_m => iq1_m_spec,
        .iq4_nl => iq4_nl_spec,
        .iq4_xs => iq4_xs_spec,
        .mxfp4 => mxfp4_spec,
        .nvfp4 => nvfp4_spec,
        else => null,
    };
}

pub fn buildIr(format: quant_matmul.Format, row_bucket: quant_matmul.RowBucket, epilogue: Epilogue) ?QuantKernelIR {
    const spec = specFor(format) orelse return null;
    const dispatch = dispatchForRowBucket(row_bucket) orelse return null;
    if (!spec.supportsSchedule(dispatch) or !spec.supportsEpilogue(epilogue)) return null;
    return .{
        .format = format,
        .row_bucket = row_bucket,
        .dispatch = dispatch,
        .epilogue = epilogue,
        .ops = irOpsForEpilogue(epilogue),
    };
}

fn supportsEpilogueForBackend(spec: QuantKernelSpec, backend: Backend, epilogue: Epilogue) bool {
    if (!spec.supportsEpilogue(epilogue)) return false;
    if (backend == .cuda and spec.format == .q6_k and epilogue != .none and epilogue != .argmax) return false;
    if (backend == .metal and spec.format == .q4_0 and epilogue == .pair) return false;
    if (backend == .metal and (epilogue == .argmax or epilogue == .pair_activation or epilogue == .gated_down)) return false;
    return true;
}

pub fn emitFirstLazyCudaSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_lazy_cuda_source);
}

pub fn emitFirstLazyMetalSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_lazy_metal_source);
}

pub fn emitFirstGeneralMetalQ40Source(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q4_0_source);
}

pub fn emitFirstGeneralMetalQ41Source(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q4_1_source);
}

pub fn emitFirstGeneralMetalQ50Source(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q5_0_source);
}

pub fn emitFirstGeneralMetalQ51Source(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q5_1_source);
}

pub fn emitFirstGeneralMetalQ4Source(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q4_source);
}

pub fn emitFirstGeneralMetalQ4BiasSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q4_bias_source);
}

pub fn emitFirstGeneralMetalQ8Source(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q8_source);
}

pub fn emitFirstGeneralMetalQ8BiasSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q8_bias_source);
}

pub fn emitFirstGeneralMetalQ8BiasGeluSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q8_bias_gelu_source);
}

pub fn emitFirstGeneralMetalQ8ReluSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q8_relu_source);
}

pub fn emitFirstGeneralMetalQ2Source(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q2_source);
}

pub fn emitFirstGeneralMetalQ2BiasSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q2_bias_source);
}

pub fn emitFirstGeneralMetalQ2BiasGeluSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q2_bias_gelu_source);
}

pub fn emitFirstGeneralMetalQ3Source(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q3_source);
}

pub fn emitFirstGeneralMetalQ3BiasSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q3_bias_source);
}

pub fn emitFirstGeneralMetalQ3BiasGeluSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q3_bias_gelu_source);
}

pub fn emitFirstGeneralMetalQ81Source(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q8_1_source);
}

pub fn emitFirstGeneralMetalQ8KSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q8_k_source);
}

pub fn emitFirstGeneralMetalQ5Source(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q5_source);
}

pub fn emitFirstGeneralMetalQ5BiasSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q5_bias_source);
}

pub fn emitFirstGeneralMetalQ5BiasGeluSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q5_bias_gelu_source);
}

pub fn emitFirstGeneralMetalQ6Source(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q6_source);
}

pub fn emitFirstGeneralMetalQ6BiasSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q6_bias_source);
}

pub fn emitFirstGeneralMetalQ6BiasGeluSource(allocator: std.mem.Allocator) ![]u8 {
    return allocator.dupe(u8, first_general_metal_q6_bias_gelu_source);
}

pub fn firstLazyCudaSource() []const u8 {
    return first_lazy_cuda_source;
}

pub fn firstLazyMetalSource() []const u8 {
    return first_lazy_metal_source;
}

pub fn firstGeneralMetalQ40Source() []const u8 {
    return first_general_metal_q4_0_source;
}

pub fn firstGeneralMetalQ41Source() []const u8 {
    return first_general_metal_q4_1_source;
}

pub fn firstGeneralMetalQ50Source() []const u8 {
    return first_general_metal_q5_0_source;
}

pub fn firstGeneralMetalQ51Source() []const u8 {
    return first_general_metal_q5_1_source;
}

pub fn firstGeneralMetalQ4Source() []const u8 {
    return first_general_metal_q4_source;
}

pub fn firstGeneralMetalQ4BiasSource() []const u8 {
    return first_general_metal_q4_bias_source;
}

pub fn firstGeneralMetalQ8Source() []const u8 {
    return first_general_metal_q8_source;
}

pub fn firstGeneralMetalQ8BiasSource() []const u8 {
    return first_general_metal_q8_bias_source;
}

pub fn firstGeneralMetalQ8BiasGeluSource() []const u8 {
    return first_general_metal_q8_bias_gelu_source;
}

pub fn firstGeneralMetalQ8ReluSource() []const u8 {
    return first_general_metal_q8_relu_source;
}

pub fn firstGeneralMetalQ2Source() []const u8 {
    return first_general_metal_q2_source;
}

pub fn firstGeneralMetalQ2BiasSource() []const u8 {
    return first_general_metal_q2_bias_source;
}

pub fn firstGeneralMetalQ2BiasGeluSource() []const u8 {
    return first_general_metal_q2_bias_gelu_source;
}

pub fn firstGeneralMetalQ3Source() []const u8 {
    return first_general_metal_q3_source;
}

pub fn firstGeneralMetalQ3BiasSource() []const u8 {
    return first_general_metal_q3_bias_source;
}

pub fn firstGeneralMetalQ3BiasGeluSource() []const u8 {
    return first_general_metal_q3_bias_gelu_source;
}

pub fn firstGeneralMetalQ81Source() []const u8 {
    return first_general_metal_q8_1_source;
}

pub fn firstGeneralMetalQ8KSource() []const u8 {
    return first_general_metal_q8_k_source;
}

pub fn firstGeneralMetalQ5Source() []const u8 {
    return first_general_metal_q5_source;
}

pub fn firstGeneralMetalQ5BiasSource() []const u8 {
    return first_general_metal_q5_bias_source;
}

pub fn firstGeneralMetalQ5BiasGeluSource() []const u8 {
    return first_general_metal_q5_bias_gelu_source;
}

pub fn firstGeneralMetalQ6Source() []const u8 {
    return first_general_metal_q6_source;
}

pub fn firstGeneralMetalQ6BiasSource() []const u8 {
    return first_general_metal_q6_bias_source;
}

pub fn firstGeneralMetalQ6BiasGeluSource() []const u8 {
    return first_general_metal_q6_bias_gelu_source;
}

pub fn benchmarkManifestJson(allocator: std.mem.Allocator) ![]u8 {
    var records: [first_benchmarks.len]BenchmarkManifestRecord = undefined;
    for (first_benchmarks, 0..) |bench, index| {
        records[index] = benchmarkManifestRecord(bench);
    }
    var metal_production_records: [first_metal_production_benchmark_cases.len]MetalProductionBenchmarkManifestRecord = undefined;
    for (first_metal_production_benchmark_cases, 0..) |case, index| {
        metal_production_records[index] = metalProductionBenchmarkManifestRecord(case);
    }
    return std.json.Stringify.valueAlloc(allocator, BenchmarkManifest{
        .schema = first_benchmark_manifest_schema,
        .benchmark_count = first_benchmarks.len,
        .evidence_count = first_benchmark_evidence.len,
        .metal_evidence_count = first_metal_runtime_evidence.len,
        .metal_legacy_unattested_evidence_count = metalLegacyUnattestedEvidenceCount(),
        .metal_legacy_unattested_evidence_is_release_blocker = true,
        .metal_future_promotions_require_attested_provenance = true,
        .metal_promotion_warmup_repeat_runs = metal_promotion_warmup_repeat_runs,
        .metal_production_regression_expected_kernel_count = first_metal_production_regression_expected_kernel_count,
        .metal_production_regression_expected_case_count = first_metal_production_regression_expected_case_count,
        .metal_production_regression_expected_route_ready_count = first_metal_production_regression_expected_route_ready_count,
        .metal_production_regression_case_fingerprint = metalProductionBenchmarkCaseManifestFingerprint(),
        .metal_production_regression_build_command = first_metal_production_regression_build_command,
        .metal_production_regression_evidence_command = first_metal_production_regression_evidence_command,
        .metal_production_regression_cases = &metal_production_records,
        .benchmarks = &records,
        .evidence_records = &first_benchmark_evidence,
        .metal_evidence_records = &first_metal_runtime_evidence,
    }, .{ .whitespace = .indent_2 });
}

pub fn artifactManifestJson(allocator: std.mem.Allocator) ![]u8 {
    var registry_records: [first_generated_artifacts.len]ArtifactRegistryManifestRecord = undefined;
    for (first_generated_artifacts, 0..) |artifact, index| {
        registry_records[index] = artifactRegistryManifestRecord(artifact);
    }
    var records: [first_generated_matmul_artifacts.len]ArtifactManifestRecord = undefined;
    var owned_route_commands = [_][]const u8{""} ** first_generated_matmul_artifacts.len;
    var owned_blocker_check_commands = [_][]const u8{""} ** first_generated_matmul_artifacts.len;
    defer for (owned_route_commands) |command| {
        if (command.len != 0) allocator.free(command);
    };
    defer for (owned_blocker_check_commands) |command| {
        if (command.len != 0) allocator.free(command);
    };
    for (first_generated_matmul_artifacts, 0..) |artifact, index| {
        owned_route_commands[index] = try artifactRuntimeRouteEvidenceCommand(allocator, artifact);
        owned_blocker_check_commands[index] = try artifactPromotionBlockerCheckCommand(allocator, artifact);
        records[index] = artifactManifestRecord(artifact, owned_route_commands[index], owned_blocker_check_commands[index]);
    }
    return std.json.Stringify.valueAlloc(allocator, ArtifactManifest{
        .schema = first_artifact_manifest_schema,
        .artifact_count = first_generated_matmul_artifacts.len,
        .registry_artifact_count = first_generated_artifacts.len,
        .checked_in_metal_evidence_count = first_metal_runtime_evidence_count,
        .metal_legacy_unattested_evidence_count = metalLegacyUnattestedEvidenceCount(),
        .metal_legacy_unattested_evidence_is_release_blocker = true,
        .metal_future_promotions_require_attested_provenance = true,
        .metal_promotion_blocker_evidence_count = first_metal_promotion_blocker_evidence_count,
        .metal_promotion_blocker_evidence_path_count = metalPromotionBlockerEvidencePathCount(),
        .metal_promotion_blocker_evidence_expected_case_count = first_metal_promotion_blocker_evidence_expected_case_count,
        .metal_promotion_blocker_evidence_expected_route_ready_count = first_metal_promotion_blocker_evidence_expected_route_ready_count,
        .metal_promotion_blocker_check_command_count = metalPromotionBlockerEvidencePathCount(),
        .metal_promotion_blocker_skipped_no_path_count = metalPromotionBlockerSkippedNoPathCount(),
        .metal_promotion_blocker_cleared_requires_checked_in_evidence = true,
        .metal_promotion_blocker_speedup_gate_missing_count = metalPromotionBlockerEvidenceCount(metal_blocker_speedup_gate_missing),
        .metal_promotion_blocker_unstable_benchmark_timing_count = metalPromotionBlockerEvidenceCount(metal_blocker_unstable_benchmark_timing),
        .metal_promotion_blocker_runtime_route_only_count = metalPromotionBlockerEvidenceCount(metal_blocker_runtime_route_only),
        .metal_promotion_blocker_unsupported_handwritten_count = metalPromotionBlockerEvidenceCount(metal_blocker_unsupported_handwritten),
        .metal_unsupported_handwritten_baseline_blocks_promotion = first_metal_unsupported_handwritten_baseline_blocks_promotion,
        .metal_unsupported_handwritten_baseline_uses_runtime_route_all_evidence = first_metal_unsupported_handwritten_baseline_uses_runtime_route_all_evidence,
        .metal_unsupported_handwritten_baseline_has_promotion_evidence_path = first_metal_unsupported_handwritten_baseline_has_promotion_evidence_path,
        .metal_local_check_command = first_metal_local_check_command,
        .metal_model_local_check_command = first_metal_model_local_check_command,
        .metal_model_generated_route_check_command = first_metal_model_generated_route_check_command,
        .metal_model_generated_q8_0_small_batch_min = first_metal_model_generated_q8_0_small_batch_min,
        .metal_model_generated_q4_0_small_batch_min = first_metal_model_generated_q4_0_small_batch_min,
        .metal_industry_local_check_command = first_metal_industry_local_check_command,
        .metal_runtime_route_all_build_command = first_metal_runtime_route_all_build_command,
        .metal_runtime_route_all_evidence_command = first_metal_runtime_route_all_evidence_command,
        .metal_runtime_route_all_check_command = first_metal_runtime_route_all_check_command,
        .metal_runtime_route_all_expected_case_count = first_metal_runtime_route_all_expected_case_count,
        .metal_runtime_route_all_expected_route_ready_count = first_metal_runtime_route_all_expected_route_ready_count,
        .metal_runtime_route_all_expected_provider_route_count = first_metal_runtime_route_all_expected_provider_route_count,
        .metal_production_regression_expected_kernel_count = first_metal_production_regression_expected_kernel_count,
        .metal_production_regression_expected_case_count = first_metal_production_regression_expected_case_count,
        .metal_production_regression_expected_route_ready_count = first_metal_production_regression_expected_route_ready_count,
        .metal_promotion_warmup_repeat_runs = metal_promotion_warmup_repeat_runs,
        .metal_production_regression_route_ready_is_hard_gate = first_metal_production_regression_route_ready_is_hard_gate,
        .metal_production_regression_missing_provider_route_is_hard_gate = first_metal_production_regression_missing_provider_route_is_hard_gate,
        .metal_production_regression_speedup_gate_missing_is_hard_gate = first_metal_production_regression_speedup_gate_missing_is_hard_gate,
        .metal_production_regression_unstable_benchmark_timing_is_hard_gate = first_metal_production_regression_unstable_benchmark_timing_is_hard_gate,
        .metal_production_regression_build_command = first_metal_production_regression_build_command,
        .metal_production_regression_evidence_command = first_metal_production_regression_evidence_command,
        .metal_blocker_strict_check_command = first_metal_blocker_strict_check_command,
        .registry_artifacts = &registry_records,
        .artifacts = &records,
        .metal_evidence_records = &first_metal_runtime_evidence,
    }, .{ .whitespace = .indent_2 });
}

fn artifactRegistryManifestRecord(artifact: GeneratedArtifact) ArtifactRegistryManifestRecord {
    var record = ArtifactRegistryManifestRecord{
        .backend = @tagName(artifact.backend),
        .op_kind = @tagName(artifact.opKind()),
        .kernel_id = artifact.kernel_id,
        .source_path = artifact.source_path,
        .generated_source_fingerprint = artifactSourceFingerprint(artifact),
        .check_command = artifact.check_command,
        .production_enabled = artifact.production_enabled,
        .runtime_default_enabled = artifact.runtime_default_enabled,
        .runtime_min_in_dim = artifact.runtime_shape.min_in_dim,
    };
    if (cudaRenderPlanForArtifact(artifact)) |plan| {
        record.cuda_kernel = @tagName(plan.kind);
        record.cuda_launch = plan.launch;
    } else if (cudaAttentionRenderPlanForArtifact(artifact)) |plan| {
        record.cuda_kernel = @tagName(plan.kind);
        record.cuda_launch = plan.launch;
        record.cuda_serial_kernel = plan.serial_kernel_id;
        record.cuda_serial_launch = plan.serial_launch;
        record.cuda_reduction_kernel = plan.reduction_kernel_id;
        record.cuda_reduction_launch = plan.reduction_launch;
        record.cuda_tiled64_kernel = plan.tiled64_kernel_id;
        record.cuda_tiled64_launch = plan.tiled64_launch;
        record.cuda_tiled64_max_kv_tokens = if (plan.tiled64_kernel_id != null)
            cuda_renderer.generatedAttentionScorePreworkTiled64MaxKvTokens(plan.lowering.head_dim)
        else
            null;
        record.cuda_attention_source_id = plan.source_id;
        record.cuda_attention_split_count = plan.lowering.kv_splits;
        record.cuda_attention_workspace = cuda_renderer.generatedAttentionWorkspaceLayoutFor(plan.lowering.kv_splits) orelse unreachable;
    } else if (cudaFlashPrefillRenderPlanForArtifact(artifact)) |plan| {
        record.cuda_kernel = @tagName(plan.kind);
        record.cuda_launch = plan.launch;
        record.cuda_attention_source_id = plan.source_id;
    } else if (cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact)) |plan| {
        record.cuda_kernel = @tagName(plan.kind);
        record.cuda_launch = plan.launch;
        record.cuda_attention_source_id = plan.source_id;
        record.cuda_attention_split_count = @intCast(plan.lowering.kv_splits);
        record.cuda_splitk_online_workspace = plan.workspace;
    }
    switch (artifact.op) {
        .small_batch_matmul => |op| record.matmul = .{
            .format = @tagName(op.format),
            .row_bucket = @tagName(op.row_bucket),
            .epilogue = @tagName(op.epilogue),
        },
        .microkernel => |op| record.microkernel = .{
            .kind = @tagName(op.kind),
            .schedule = op.schedule,
        },
        .attention => |op| record.attention = .{
            .kind = @tagName(op.kind),
            .head_dim = op.head_dim,
            .schedule = op.schedule,
        },
    }
    return record;
}

pub fn specManifestJson(allocator: std.mem.Allocator) ![]u8 {
    var records: [first_formats.len]SpecManifestRecord = undefined;
    for (first_formats, 0..) |format, index| {
        records[index] = specManifestRecord(specFor(format).?);
    }
    return std.json.Stringify.valueAlloc(allocator, SpecManifest{
        .schema = "antfly.quant_kernel_specs.v1",
        .format_count = first_formats.len,
        .specs = &records,
    }, .{ .whitespace = .indent_2 });
}

pub fn conformanceManifestJson(allocator: std.mem.Allocator) ![]u8 {
    var records: [first_conformance.len]ConformanceManifestRecord = undefined;
    for (first_conformance, 0..) |case, index| {
        records[index] = conformanceManifestRecord(case);
    }
    return std.json.Stringify.valueAlloc(allocator, ConformanceManifest{
        .schema = "antfly.quant_kernel_conformance.v1",
        .case_count = first_conformance.len,
        .format_count = first_formats.len,
        .row_bucket_count = first_row_buckets.len,
        .epilogue_count = coverage_epilogues.len,
        .backend_count = first_backends.len,
        .cuda_route_summary = routeSummaryForBackend(.cuda),
        .metal_route_summary = routeSummaryForBackend(.metal),
        .cases = &records,
    }, .{ .whitespace = .indent_2 });
}

fn metalProductionBenchmarkCaseCount() comptime_int {
    @setEvalBranchQuota(10_000);
    var count: comptime_int = 0;
    inline for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend == .metal and artifactHasPromotionEvidence(artifact)) count += 2;
    }
    return count;
}

fn buildMetalProductionBenchmarkCases() [metalProductionBenchmarkCaseCount()]MetalProductionBenchmarkCase {
    @setEvalBranchQuota(10_000);
    var cases: [metalProductionBenchmarkCaseCount()]MetalProductionBenchmarkCase = undefined;
    var index: usize = 0;
    inline for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend == .metal and artifactHasPromotionEvidence(artifact)) {
            cases[index] = metalProductionBenchmarkCaseForArtifactShape(artifact, .small);
            index += 1;
            cases[index] = metalProductionBenchmarkCaseForArtifactShape(artifact, .wide);
            index += 1;
        }
    }
    return cases;
}

pub fn metalProductionBenchmarkCaseManifestFingerprint() u64 {
    var hasher = std.hash.Wyhash.init(0);
    std.hash.autoHash(&hasher, first_metal_production_benchmark_case_count);
    for (first_metal_production_benchmark_cases) |case| {
        hasher.update(case.name);
        hasher.update(case.kernel_id);
        std.hash.autoHash(&hasher, case.format);
        std.hash.autoHash(&hasher, case.row_bucket);
        std.hash.autoHash(&hasher, case.epilogue);
        std.hash.autoHash(&hasher, case.dispatch);
        std.hash.autoHash(&hasher, case.shape);
        std.hash.autoHash(&hasher, case.rows);
        std.hash.autoHash(&hasher, case.in_dim);
        std.hash.autoHash(&hasher, case.out_dim);
        std.hash.autoHash(&hasher, case.threads_per_threadgroup);
        std.hash.autoHash(&hasher, case.cols_per_threadgroup);
        std.hash.autoHash(&hasher, @as(u32, @bitCast(case.tolerance_abs)));
        hasher.update(case.generated_source_path);
        std.hash.autoHash(&hasher, case.generated_source_fingerprint);
        hasher.update(case.check_command);
        hasher.update(case.production_kernel_id);
        hasher.update(case.benchmark_command);
    }
    return hasher.final();
}

fn metalProductionBenchmarkCaseForArtifactShape(comptime artifact: GeneratedMatmulArtifact, comptime shape: MetalBenchmarkShape) MetalProductionBenchmarkCase {
    const dims = metalBenchmarkDimsForArtifact(artifact, shape);
    return .{
        .name = metalBenchmarkCaseName(artifact, shape),
        .kernel_id = artifact.kernel_id,
        .format = artifact.format,
        .row_bucket = artifact.row_bucket,
        .epilogue = artifact.epilogue,
        .dispatch = dispatchForRowBucket(artifact.row_bucket) orelse .scalar,
        .shape = shape,
        .rows = dims.rows,
        .in_dim = dims.in_dim,
        .out_dim = dims.out_dim,
        .threads_per_threadgroup = metalGeneratedThreadsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue),
        .cols_per_threadgroup = metalGeneratedColsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue),
        .tolerance_abs = dims.tolerance_abs,
        .generated_source_path = artifact.source_path,
        .generated_source_fingerprint = artifactSourceFingerprint(artifact),
        .check_command = artifact.check_command,
        .production_kernel_id = artifact.kernel_id,
        .benchmark_command = first_metal_production_regression_evidence_command,
    };
}

pub fn metalBenchmarkCaseName(comptime artifact: GeneratedMatmulArtifact, comptime shape: MetalBenchmarkShape) []const u8 {
    return switch (shape) {
        .small => std.fmt.comptimePrint("{s}_rows_2_8_{s}", .{ @tagName(artifact.format), @tagName(artifact.epilogue) }),
        .wide => std.fmt.comptimePrint("{s}_rows_8_cols_7_{s}", .{ @tagName(artifact.format), @tagName(artifact.epilogue) }),
    };
}

pub fn metalBenchmarkDimsForArtifact(comptime artifact: GeneratedMatmulArtifact, comptime shape: MetalBenchmarkShape) MetalBenchmarkDims {
    return switch (shape) {
        .small => .{
            .rows = if (artifact.format == .q4_k and artifact.epilogue == .bias_gelu) 4 else 3,
            .in_dim = 512,
            .out_dim = if (artifact.format == .q4_k and artifact.epilogue == .bias_gelu) 3 else 2,
            .tolerance_abs = metalBenchmarkTolerance(artifact.format, shape),
        },
        .wide => .{
            .rows = 8,
            .in_dim = 768,
            .out_dim = 7,
            .tolerance_abs = metalBenchmarkTolerance(artifact.format, shape),
        },
    };
}

pub fn metalBenchmarkTolerance(comptime format: quant_matmul.Format, comptime shape: MetalBenchmarkShape) f32 {
    const loose = format == .q2_k or format == .q3_k or format == .q8_k;
    return switch (shape) {
        .small => if (loose) 0.0005 else 0.0002,
        .wide => if (loose) 0.001 else 0.0005,
    };
}

fn buildDescriptorFormats() [codec_format_coverage.len]quant_matmul.Format {
    var formats: [codec_format_coverage.len]quant_matmul.Format = undefined;
    for (codec_format_coverage, 0..) |entry, index| {
        formats[index] = entry.format;
    }
    return formats;
}

fn buildFirstCoverage() [first_formats.len * first_row_buckets.len * coverage_epilogues.len]CoverageCase {
    @setEvalBranchQuota(4096);
    var cases: [first_formats.len * first_row_buckets.len * coverage_epilogues.len]CoverageCase = undefined;
    var index: usize = 0;
    for (first_formats) |format| {
        for (first_row_buckets) |row_bucket| {
            for (coverage_epilogues) |epilogue| {
                cases[index] = .{ .format = format, .row_bucket = row_bucket, .epilogue = epilogue };
                index += 1;
            }
        }
    }
    return cases;
}

fn buildFirstRegistry() [first_coverage.len * first_backends.len]QuantKernelLowering {
    @setEvalBranchQuota(131072);
    var entries: [first_coverage.len * first_backends.len]QuantKernelLowering = undefined;
    var index: usize = 0;
    for (first_coverage) |case| {
        for (first_backends) |backend| {
            entries[index] = loweringFor(backend, case.format, case.row_bucket, case.epilogue);
            index += 1;
        }
    }
    return entries;
}

fn routeSummaryForBackend(backend: Backend) PlanCounters {
    var total = PlanCounters{};
    for (first_registry.entries) |entry| {
        if (entry.backend != backend) continue;
        addCountersToStats(&total, countersForLowering(entry));
    }
    return total;
}

fn benchmarkManifestRecord(bench: BenchmarkCase) BenchmarkManifestRecord {
    return .{
        .name = bench.name,
        .backend = @tagName(bench.backend),
        .format = @tagName(bench.format),
        .row_bucket = @tagName(bench.row_bucket),
        .epilogue = @tagName(bench.epilogue),
        .dispatch = @tagName(dispatchForRowBucket(bench.row_bucket) orelse .scalar),
        .generated_kernel_id = bench.generated_kernel_id,
        .generated_source_path = bench.generated_source_path,
        .generated_source_fingerprint = bench.generated_source_fingerprint,
        .generated_ptx_path = bench.generated_ptx_path,
        .generated_ptx_command = bench.generated_ptx_command,
        .benchmark_command = bench.benchmark_command,
        .generated_ptx_arg = bench.generated_ptx_arg,
        .handwritten_baseline = bench.handwritten_baseline,
        .correctness_tolerance_abs = bench.correctness_tolerance_abs,
        .minimum_speedup = bench.minimum_speedup,
        .measured_speedup = benchmarkMeasuredSpeedup(bench),
        .correctness_evidence_path = bench.correctness_evidence_path,
        .benchmark_evidence_path = bench.benchmark_evidence_path,
        .benchmark_mode = bench.benchmark_mode,
        .target_fingerprint = bench.target_fingerprint,
        .production_enabled = bench.production_enabled,
        .promotion_ready = benchmarkHasPromotionEvidence(bench),
        .promotion_blocker = benchmarkPromotionBlocker(bench),
    };
}

fn metalProductionBenchmarkManifestRecord(case: MetalProductionBenchmarkCase) MetalProductionBenchmarkManifestRecord {
    return .{
        .name = case.name,
        .kernel_id = case.kernel_id,
        .format = @tagName(case.format),
        .row_bucket = @tagName(case.row_bucket),
        .epilogue = @tagName(case.epilogue),
        .dispatch = @tagName(case.dispatch),
        .shape = @tagName(case.shape),
        .rows = case.rows,
        .in_dim = case.in_dim,
        .out_dim = case.out_dim,
        .threads_per_threadgroup = case.threads_per_threadgroup,
        .cols_per_threadgroup = case.cols_per_threadgroup,
        .tolerance_abs = case.tolerance_abs,
        .generated_source_path = case.generated_source_path,
        .generated_source_fingerprint = case.generated_source_fingerprint,
        .check_command = case.check_command,
        .production_kernel_id = case.production_kernel_id,
        .benchmark_command = case.benchmark_command,
    };
}

fn artifactManifestRecord(artifact: GeneratedMatmulArtifact, runtime_route_evidence_command: []const u8, promotion_blocker_check_command: []const u8) ArtifactManifestRecord {
    const cuda_plan = cudaRenderPlanForArtifact(artifact);
    return .{
        .backend = @tagName(artifact.backend),
        .format = @tagName(artifact.format),
        .row_bucket = @tagName(artifact.row_bucket),
        .epilogue = @tagName(artifact.epilogue),
        .kernel_id = artifact.kernel_id,
        .source_path = artifact.source_path,
        .generated_source_fingerprint = artifactSourceFingerprint(artifact),
        .check_command = artifact.check_command,
        .runtime_evidence_command = artifact.runtime_evidence_command,
        .runtime_route_evidence_command = runtime_route_evidence_command,
        .promotion_evidence_command = artifact.promotion_evidence_command,
        .promotion_check_command = artifact.promotion_check_command,
        .promotion_policy = artifactPromotionPolicy(artifact),
        .promotion_target_fingerprint = artifactPromotionTargetFingerprint(artifact) orelse 0,
        .production_enabled = artifact.production_enabled,
        .runtime_default_enabled = artifact.runtime_default_enabled,
        .runtime_wired = artifactRuntimeWired(artifact),
        .runtime_gate_env = artifactRuntimeGateEnvText(artifact),
        .runtime_min_in_dim = artifact.runtime_shape.min_in_dim,
        .cuda_kernel = if (cuda_plan) |plan| @tagName(plan.kind) else null,
        .cuda_launch = if (cuda_plan) |plan| plan.launch else null,
        .production_regression_checked = artifactProductionRegressionChecked(artifact),
        .production_regression_command = artifactProductionRegressionCommand(artifact),
        .metal_promotion_min_speedup = if (artifact.backend == .metal) metal_promotion_min_speedup else 0.0,
        .metal_promotion_repeat_runs = if (artifact.backend == .metal) metal_promotion_repeat_runs else 0,
        .metal_promotion_warmup_repeat_runs = if (artifact.backend == .metal) metal_promotion_warmup_repeat_runs else 0,
        .candidate_status = artifactCandidateStatus(artifact),
        .promotion_ready = artifactHasPromotionEvidence(artifact),
        .promotion_blocker = artifactPromotionBlocker(artifact),
        .promotion_blocker_evidence_path = artifactPromotionBlockerEvidencePath(artifact),
        .promotion_blocker_check_command = promotion_blocker_check_command,
        .promotion_blocker_requires_production_regression_clear = artifactPromotionBlockerRequiresProductionRegressionClear(artifact),
    };
}

fn artifactRuntimeRouteEvidenceCommand(allocator: std.mem.Allocator, artifact: GeneratedMatmulArtifact) ![]const u8 {
    if (!artifactNeedsRuntimeRouteEvidence(artifact)) return "";
    return std.fmt.allocPrint(
        allocator,
        "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out /private/tmp/antfly-quant-metal-{s}-runtime-route-evidence.json --runtime-route-kernel {s}",
        .{ artifact.kernel_id, artifact.kernel_id },
    );
}

pub fn artifactNeedsRuntimeRouteEvidence(artifact: GeneratedMatmulArtifact) bool {
    return artifact.backend == .metal and artifactRuntimeWired(artifact) and !artifactHasPromotionEvidence(artifact);
}

fn artifactPromotionPolicy(artifact: GeneratedMatmulArtifact) []const u8 {
    if (artifact.backend == .metal and std.mem.eql(u8, artifactPromotionBlocker(artifact), metal_blocker_unsupported_handwritten)) {
        return "route_evidence_only_no_promotion";
    }
    if (artifact.backend == .metal and artifactHasPromotionEvidence(artifact)) {
        return "promoted_speedup_vs_handwritten";
    }
    if (artifact.backend == .metal and artifactRuntimeWired(artifact)) {
        return "speedup_vs_handwritten";
    }
    if (artifact.backend == .metal) {
        return "production_disabled";
    }
    if (artifact.backend == .cuda) {
        return "driver_artifact_policy";
    }
    return "unsupported";
}

fn artifactPromotionBlockerCheckCommand(allocator: std.mem.Allocator, artifact: GeneratedMatmulArtifact) ![]const u8 {
    const path = artifactPromotionBlockerEvidencePath(artifact);
    if (path.len == 0) return "";
    return std.fmt.allocPrint(
        allocator,
        "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --check-evidence {s} --require-evidence-kernel {s}",
        .{ path, artifact.kernel_id },
    );
}

pub fn artifactRuntimeWired(artifact: GeneratedMatmulArtifact) bool {
    if (artifact.backend == .cuda) {
        const kind = artifact.cuda_kernel orelse return false;
        return switch (kind) {
            .q4_0_mmv,
            .q4_0_mm,
            .q4_0_pair_mmv,
            .q4_0_pair_activation_q8_1,
            .q4_0_pair_activation_q8_1_e2b_6144,
            .q4_0_pair_activation_q8_1_e2b_12288,
            .q4_0_pair_activation_ggml_q8_1_e2b_6144,
            .q4_0_pair_activation_ggml_q8_1_e2b_12288,
            .q4_0_down_q8_1,
            .q4_0_down_q8_1_e2b_6144,
            .q4_0_down_q8_1_e2b_12288,
            .q4_0_down_ggml_q8_1_e2b_6144,
            .q4_0_down_ggml_q8_1_e2b_12288,
            .q4_0_pair_activation_f32_e2b_6144_exact,
            .q4_0_pair_activation_f32_e2b_12288_exact,
            .q4_0_down_f32_e2b_6144_exact,
            .q4_0_down_f32_e2b_12288_exact,
            .q4_0_q8_1_argmax_e2b_tile8,
            .q6_k_q8_1_argmax_k2560_tile8,
            .q6_k_q8_1_argmax_k3840_tile8,
            => true,
            .q4_k_small_batch_bias_gelu,
            .q4_k_mmv,
            => false,
        };
    }
    if (artifact.backend != .metal or artifact.row_bucket != .rows_2_8) return false;
    if (artifact.format == .q4_0 and artifact.epilogue == .none) return true;
    if (artifact.format == .q4_1 and artifact.epilogue == .none) return true;
    if ((artifact.format == .q5_0 or artifact.format == .q5_1) and artifact.epilogue == .none) return true;
    if ((artifact.format == .q8_1 or artifact.format == .q8_k) and artifact.epilogue == .none) return true;
    if (artifact.format == .q2_k and (artifact.epilogue == .bias or artifact.epilogue == .bias_gelu)) return true;
    if (artifact.format == .q3_k and (artifact.epilogue == .bias or artifact.epilogue == .bias_gelu)) return true;
    if (artifact.format == .q4_k and artifact.epilogue == .bias) return true;
    if (artifact.format == .q4_k and artifact.epilogue == .bias_gelu) return true;
    if (artifact.format == .q8_0 and (artifact.epilogue == .bias or artifact.epilogue == .bias_gelu or artifact.epilogue == .relu)) return true;
    if (artifact.format == .q5_k and (artifact.epilogue == .bias or artifact.epilogue == .bias_gelu)) return true;
    if (artifact.format == .q6_k and (artifact.epilogue == .bias or artifact.epilogue == .bias_gelu)) return true;
    if (artifact.epilogue != .none) return false;
    return switch (artifact.format) {
        .q8_0, .q2_k, .q3_k, .q4_k, .q5_k, .q6_k => true,
        else => false,
    };
}

fn metalRuntimeRouteAllExpectedCaseCount() usize {
    var count: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        if (artifactRuntimeWired(artifact)) count += 2;
    }
    return count;
}

fn metalRuntimeRouteAllExpectedProviderRouteCount() usize {
    var count: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifactHasMetalProviderRouteEvidence(artifact)) count += 2;
    }
    return count;
}

fn metalPromotionBlockerSkippedNoPathCount() usize {
    return first_metal_promotion_blocker_evidence_count - metalPromotionBlockerEvidencePathCount();
}

fn metalPromotionBlockerEvidenceExpectedCaseCount() usize {
    return metalPromotionBlockerEvidencePathCount() * first_metal_promotion_blocker_evidence_cases_per_kernel;
}

fn metalProductionRegressionExpectedKernelCount() usize {
    var count: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifactProductionRegressionChecked(artifact)) count += 1;
    }
    return count;
}

fn metalProductionRegressionExpectedCaseCount() usize {
    return metalProductionRegressionExpectedKernelCount() * 2;
}

fn artifactRuntimeGateEnvText(artifact: GeneratedMatmulArtifact) []const u8 {
    return if (artifactRuntimeGateEnv(artifact)) |env| std.mem.span(env) else "";
}

fn artifactCandidateOptInGateEnv(artifact: GeneratedMatmulArtifact) ?[*:0]const u8 {
    if (!artifactRuntimeWired(artifact)) return null;
    if (artifact.backend == .cuda) {
        const kind = artifact.cuda_kernel orelse return null;
        switch (kind) {
            .q4_0_pair_activation_q8_1_e2b_6144,
            .q4_0_pair_activation_q8_1_e2b_12288,
            .q4_0_down_q8_1_e2b_6144,
            .q4_0_down_q8_1_e2b_12288,
            => return "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN",
            .q4_0_pair_activation_ggml_q8_1_e2b_6144,
            .q4_0_pair_activation_ggml_q8_1_e2b_12288,
            .q4_0_down_ggml_q8_1_e2b_6144,
            .q4_0_down_ggml_q8_1_e2b_12288,
            => return "ANTFLY_INFERENCE_CUDA_SM89_Q4_0_Q8_1",
            .q4_0_pair_activation_f32_e2b_6144_exact,
            .q4_0_pair_activation_f32_e2b_12288_exact,
            .q4_0_down_f32_e2b_6144_exact,
            .q4_0_down_f32_e2b_12288_exact,
            => return "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT",
            .q6_k_q8_1_argmax_k2560_tile8,
            .q6_k_q8_1_argmax_k3840_tile8,
            => return "ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX",
            else => {},
        }
        return switch (artifact.row_bucket) {
            .rows_1 => switch (artifact.epilogue) {
                .pair => "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR",
                .argmax => "ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX",
                .pair_activation => "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR_Q8",
                .gated_down => "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_DOWN_Q8",
                else => "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MMV",
            },
            .rows_9_64 => "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MM",
            else => null,
        };
    }
    return switch (artifact.format) {
        .q8_0 => switch (artifact.epilogue) {
            .bias => "TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH_BIAS",
            .bias_gelu => "TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH_BIAS_GELU",
            .relu => "TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH_RELU",
            else => "TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH",
        },
        .q2_k => switch (artifact.epilogue) {
            .bias => "TERMITE_METAL_ENABLE_ANTFLY_Q2_K_SMALL_BATCH_BIAS",
            .bias_gelu => "TERMITE_METAL_ENABLE_ANTFLY_Q2_K_SMALL_BATCH_BIAS_GELU",
            else => "TERMITE_METAL_ENABLE_ANTFLY_Q2_K_SMALL_BATCH",
        },
        .q3_k => switch (artifact.epilogue) {
            .bias => "TERMITE_METAL_ENABLE_ANTFLY_Q3_K_SMALL_BATCH_BIAS",
            .bias_gelu => "TERMITE_METAL_ENABLE_ANTFLY_Q3_K_SMALL_BATCH_BIAS_GELU",
            else => "TERMITE_METAL_ENABLE_ANTFLY_Q3_K_SMALL_BATCH",
        },
        .q4_0 => "TERMITE_METAL_ENABLE_ANTFLY_Q4_0_SMALL_BATCH",
        .q4_1 => "TERMITE_METAL_ENABLE_ANTFLY_Q4_1_SMALL_BATCH",
        .q5_0 => "TERMITE_METAL_ENABLE_ANTFLY_Q5_0_SMALL_BATCH",
        .q5_1 => "TERMITE_METAL_ENABLE_ANTFLY_Q5_1_SMALL_BATCH",
        .q8_1 => "TERMITE_METAL_ENABLE_ANTFLY_Q8_1_SMALL_BATCH",
        .q8_k => "TERMITE_METAL_ENABLE_ANTFLY_Q8_K_SMALL_BATCH",
        .q4_k => switch (artifact.epilogue) {
            .bias => "TERMITE_METAL_ENABLE_ANTFLY_Q4_K_SMALL_BATCH_BIAS",
            .bias_gelu => "TERMITE_METAL_ENABLE_ANTFLY_Q4_K_SMALL_BATCH_BIAS_GELU",
            else => "TERMITE_METAL_ENABLE_ANTFLY_Q4_K_SMALL_BATCH",
        },
        .q5_k => switch (artifact.epilogue) {
            .bias => "TERMITE_METAL_ENABLE_ANTFLY_Q5_K_SMALL_BATCH_BIAS",
            .bias_gelu => "TERMITE_METAL_ENABLE_ANTFLY_Q5_K_SMALL_BATCH_BIAS_GELU",
            else => "TERMITE_METAL_ENABLE_ANTFLY_Q5_K_SMALL_BATCH",
        },
        .q6_k => switch (artifact.epilogue) {
            .bias => "TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH_BIAS",
            .bias_gelu => "TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH_BIAS_GELU",
            else => "TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH",
        },
        else => null,
    };
}

fn artifactProductionRegressionChecked(artifact: GeneratedMatmulArtifact) bool {
    return artifact.backend == .metal and artifactHasPromotionEvidence(artifact);
}

fn artifactProductionRegressionCommand(artifact: GeneratedMatmulArtifact) []const u8 {
    return if (artifactProductionRegressionChecked(artifact)) first_metal_production_regression_build_command else "";
}

pub fn artifactRuntimeGateEnv(artifact: GeneratedMatmulArtifact) ?[*:0]const u8 {
    if (!artifactRuntimeWired(artifact)) return null;
    if (artifact.backend == .cuda) {
        if (artifact.runtime_default_enabled) {
            return switch (artifact.row_bucket) {
                .rows_1 => switch (artifact.epilogue) {
                    .pair => "ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_PAIR",
                    .pair_activation => "ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_PAIR_Q8",
                    .gated_down => "ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_DOWN_Q8",
                    else => "ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_MMV",
                },
                .rows_9_64 => "ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_MM",
                else => null,
            };
        }
        return artifactCandidateOptInGateEnv(artifact);
    }
    const runtime_default_enabled = artifact.runtime_default_enabled;
    return switch (artifact.format) {
        .q8_0 => switch (artifact.epilogue) {
            .bias, .bias_gelu => if (runtime_default_enabled) null else artifactCandidateOptInGateEnv(artifact),
            .relu => artifactCandidateOptInGateEnv(artifact),
            else => if (runtime_default_enabled) "TERMITE_METAL_DISABLE_ANTFLY_Q8_0_SMALL_BATCH" else artifactCandidateOptInGateEnv(artifact),
        },
        .q2_k => switch (artifact.epilogue) {
            .bias, .bias_gelu => artifactCandidateOptInGateEnv(artifact),
            else => if (runtime_default_enabled) "TERMITE_METAL_DISABLE_ANTFLY_Q2_K_SMALL_BATCH" else artifactCandidateOptInGateEnv(artifact),
        },
        .q3_k => switch (artifact.epilogue) {
            .bias, .bias_gelu => artifactCandidateOptInGateEnv(artifact),
            else => if (runtime_default_enabled) "TERMITE_METAL_DISABLE_ANTFLY_Q3_K_SMALL_BATCH" else artifactCandidateOptInGateEnv(artifact),
        },
        .q4_0 => artifactCandidateOptInGateEnv(artifact),
        .q4_1 => if (runtime_default_enabled) "TERMITE_METAL_DISABLE_ANTFLY_Q4_1_SMALL_BATCH" else artifactCandidateOptInGateEnv(artifact),
        .q5_0 => if (runtime_default_enabled) "TERMITE_METAL_DISABLE_ANTFLY_Q5_0_SMALL_BATCH" else artifactCandidateOptInGateEnv(artifact),
        .q8_1 => if (runtime_default_enabled) "TERMITE_METAL_DISABLE_ANTFLY_Q8_1_SMALL_BATCH" else artifactCandidateOptInGateEnv(artifact),
        .q8_k => if (runtime_default_enabled) "TERMITE_METAL_DISABLE_ANTFLY_Q8_K_SMALL_BATCH" else artifactCandidateOptInGateEnv(artifact),
        .q5_1 => if (runtime_default_enabled) "TERMITE_METAL_DISABLE_ANTFLY_Q5_1_SMALL_BATCH" else artifactCandidateOptInGateEnv(artifact),
        .q4_k => switch (artifact.epilogue) {
            .bias, .bias_gelu => if (runtime_default_enabled) null else artifactCandidateOptInGateEnv(artifact),
            else => if (runtime_default_enabled) "TERMITE_METAL_DISABLE_ANTFLY_Q4_K_SMALL_BATCH" else artifactCandidateOptInGateEnv(artifact),
        },
        .q5_k => switch (artifact.epilogue) {
            .bias, .bias_gelu => if (runtime_default_enabled) null else artifactCandidateOptInGateEnv(artifact),
            else => if (runtime_default_enabled) "TERMITE_METAL_DISABLE_ANTFLY_Q5_K_SMALL_BATCH" else artifactCandidateOptInGateEnv(artifact),
        },
        .q6_k => switch (artifact.epilogue) {
            .bias, .bias_gelu => if (runtime_default_enabled) null else artifactCandidateOptInGateEnv(artifact),
            else => if (runtime_default_enabled) "TERMITE_METAL_DISABLE_ANTFLY_Q6_K_SMALL_BATCH" else artifactCandidateOptInGateEnv(artifact),
        },
        else => null,
    };
}

fn artifactCandidateStatus(artifact: GeneratedMatmulArtifact) []const u8 {
    if (artifactHasPromotionEvidence(artifact)) return "promoted";
    if (metalPromotionBlockerEvidenceFor(artifact) != null) return "blocked_by_evidence";
    if (artifact.production_enabled) return "promotion_blocked";
    return "dev_only_candidate";
}

fn cudaRuntimeWiredDevCandidate(artifact: GeneratedMatmulArtifact) bool {
    return artifact.backend == .cuda and
        !artifact.production_enabled and
        artifactRuntimeWired(artifact);
}

pub fn artifactSourceFingerprint(artifact: anytype) u64 {
    @setEvalBranchQuota(24_000_000);
    const source = generatedSourceForArtifact(artifact) orelse return 0;
    return std.hash.Wyhash.hash(0, source);
}

fn candidateSourceFingerprint(lowering: QuantKernelLowering) u64 {
    if (lowering.candidate_route != .generated_dev_candidate) return 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend == lowering.backend and
            artifact.format == lowering.format and
            artifact.row_bucket == lowering.row_bucket and
            artifact.epilogue == lowering.epilogue and
            std.mem.eql(u8, artifact.kernel_id, lowering.kernel_id) and
            std.mem.eql(u8, artifact.source_path, lowering.candidate_source_path))
        {
            return artifactSourceFingerprint(artifact);
        }
    }
    return 0;
}

fn specManifestRecord(spec: QuantKernelSpec) SpecManifestRecord {
    return .{
        .format = @tagName(spec.format),
        .reference_tensor_type = tensorTypeName(tensorTypeForFormat(spec.format).?),
        .block_values = spec.block_values,
        .block_bytes = spec.block_bytes,
        .block_fields = spec.block_fields,
        .decode_ops = spec.decode_ops,
        .supported_schedules = spec.supported_schedules,
        .supported_epilogues = spec.supported_epilogues,
        .supported_backends = spec.supported_backends,
        .accumulator_dtype = @tagName(spec.accumulator_dtype),
        .output_dtype = @tagName(spec.output_dtype),
    };
}

fn conformanceManifestRecord(case: ConformanceCase) ConformanceManifestRecord {
    const spec = specFor(case.format).?;
    const cuda = loweringFor(.cuda, case.format, case.row_bucket, case.epilogue);
    const metal = loweringFor(.metal, case.format, case.row_bucket, case.epilogue);
    const schedule = cuda.schedule;
    const cuda_candidate_schedule = candidateScheduleFor(cuda);
    const metal_candidate_schedule = candidateScheduleFor(metal);
    return .{
        .format = @tagName(case.format),
        .block_values = spec.block_values,
        .block_bytes = spec.block_bytes,
        .accumulator_dtype = @tagName(spec.accumulator_dtype),
        .output_dtype = @tagName(spec.output_dtype),
        .row_bucket = @tagName(case.row_bucket),
        .epilogue = @tagName(case.epilogue),
        .dispatch = @tagName(case.dispatch),
        .tile_rows = schedule.tile_rows,
        .tile_cols = schedule.tile_cols,
        .vector_width = schedule.vector_width,
        .threads_per_block = schedule.threads_per_block,
        .shared_memory_bytes = schedule.shared_memory_bytes,
        .register_pressure_hint = schedule.register_pressure_hint,
        .tensor_core_eligible = schedule.tensor_core_eligible,
        .cuda_candidate_tile_rows = cuda_candidate_schedule.tile_rows,
        .cuda_candidate_tile_cols = cuda_candidate_schedule.tile_cols,
        .cuda_candidate_threads_per_block = cuda_candidate_schedule.threads_per_block,
        .metal_candidate_tile_rows = metal_candidate_schedule.tile_rows,
        .metal_candidate_tile_cols = metal_candidate_schedule.tile_cols,
        .metal_candidate_threads_per_block = metal_candidate_schedule.threads_per_block,
        .reference_supported = case.reference_supported,
        .reference_tensor_type = tensorTypeName(case.reference_tensor_type),
        .tolerance_abs = case.tolerance_abs,
        .cuda_route = loweringRouteName(case.cuda_route),
        .cuda_candidate_route = loweringRouteName(case.cuda_candidate_route),
        .cuda_production_kernel_id = cuda.production_kernel_id,
        .cuda_candidate_kernel_id = cuda.kernel_id,
        .cuda_candidate_source_path = cuda.candidate_source_path,
        .cuda_candidate_source_fingerprint = candidateSourceFingerprint(cuda),
        .cuda_fallback_reason = fallbackReasonName(case.cuda_fallback_reason),
        .metal_route = loweringRouteName(case.metal_route),
        .metal_candidate_route = loweringRouteName(case.metal_candidate_route),
        .metal_production_kernel_id = metal.production_kernel_id,
        .metal_candidate_kernel_id = metal.kernel_id,
        .metal_candidate_source_path = metal.candidate_source_path,
        .metal_candidate_source_fingerprint = candidateSourceFingerprint(metal),
        .metal_fallback_reason = fallbackReasonName(case.metal_fallback_reason),
    };
}

fn buildFirstConformance() [first_coverage.len]ConformanceCase {
    @setEvalBranchQuota(131072);
    var cases: [first_coverage.len]ConformanceCase = undefined;
    for (first_coverage, 0..) |case, index| {
        const dispatch = dispatchForRowBucket(case.row_bucket) orelse .scalar;
        const cuda = loweringFor(.cuda, case.format, case.row_bucket, case.epilogue);
        const metal = loweringFor(.metal, case.format, case.row_bucket, case.epilogue);
        cases[index] = .{
            .format = case.format,
            .row_bucket = case.row_bucket,
            .epilogue = case.epilogue,
            .dispatch = dispatch,
            .reference_supported = referenceSupportedForEpilogue(case.epilogue),
            .reference_tensor_type = tensorTypeForFormat(case.format).?,
            .tolerance_abs = toleranceFor(case.format, case.epilogue),
            .cuda_route = cuda.production_route,
            .cuda_candidate_route = cuda.candidate_route,
            .cuda_fallback_reason = cuda.fallback_reason,
            .metal_route = metal.production_route,
            .metal_candidate_route = metal.candidate_route,
            .metal_fallback_reason = metal.fallback_reason,
        };
    }
    return cases;
}

pub fn generatedArtifactForCandidate(
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
) ?GeneratedMatmulArtifact {
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend == backend and
            artifact.format == format and
            artifact.row_bucket == row_bucket and
            artifact.epilogue == epilogue)
        {
            return artifact;
        }
    }
    return null;
}

pub fn generatedArtifactForKernel(backend: Backend, kernel_id: []const u8) ?GeneratedMatmulArtifact {
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend == backend and std.mem.eql(u8, artifact.kernel_id, kernel_id)) {
            return artifact;
        }
    }
    return null;
}

/// Typed registry lookup used by AOT catalogs. Unlike the matmul compatibility
/// view above, this preserves attention/microkernel operation metadata and the
/// semantic activation/output ABI of matmul artifacts.
pub fn generatedRegistryArtifactForKernel(backend: Backend, kernel_id: []const u8) ?GeneratedArtifact {
    for (first_generated_artifacts) |artifact| {
        if (artifact.backend == backend and std.mem.eql(u8, artifact.kernel_id, kernel_id)) {
            return artifact;
        }
    }
    return null;
}

pub fn loweringFor(
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
) QuantKernelLowering {
    const spec = specFor(format) orelse return unsupportedLowering(backend, format, row_bucket, epilogue, .scalar, .unsupported_format);

    const dispatch = dispatchForRowBucket(row_bucket) orelse return unsupportedLowering(backend, format, row_bucket, epilogue, .scalar, .unsupported_shape);

    if (!spec.supportsSchedule(dispatch)) return unsupportedLowering(backend, format, row_bucket, epilogue, dispatch, .unsupported_shape);

    if (!spec.supportsBackend(backend)) return .{
        .plan_id = planId(backend, format, row_bucket, epilogue, dispatch),
        .backend = backend,
        .format = format,
        .row_bucket = row_bucket,
        .epilogue = epilogue,
        .schedule = scheduleFor(row_bucket, dispatch),
        .production_route = .unsupported,
        .candidate_route = .unsupported,
        .production_kernel_id = "",
        .fallback_reason = .unsupported_backend,
        .kernel_id = "",
        .candidate_source_path = "",
    };

    if (!supportsEpilogueForBackend(spec, backend, epilogue)) return .{
        .plan_id = planId(backend, format, row_bucket, epilogue, dispatch),
        .backend = backend,
        .format = format,
        .row_bucket = row_bucket,
        .epilogue = epilogue,
        .schedule = scheduleFor(row_bucket, dispatch),
        .production_route = .unsupported,
        .candidate_route = .unsupported,
        .production_kernel_id = "",
        .fallback_reason = .unsupported_epilogue,
        .kernel_id = "",
        .candidate_source_path = "",
    };

    const candidate_artifact = generatedArtifactForCandidate(backend, format, row_bucket, epilogue);
    const generated_candidate = candidate_artifact != null;
    const kernel_id = if (candidate_artifact) |artifact| artifact.kernel_id else "";
    const source_path = if (candidate_artifact) |artifact| artifact.source_path else "";

    return promoteLoweringIfArtifactReady(.{
        .plan_id = planId(backend, format, row_bucket, epilogue, dispatch),
        .backend = backend,
        .format = format,
        .row_bucket = row_bucket,
        .epilogue = epilogue,
        .schedule = scheduleFor(row_bucket, dispatch),
        .production_route = .handwritten_production,
        .candidate_route = if (generated_candidate) .generated_dev_candidate else .unsupported,
        .production_kernel_id = productionKernelId(backend, format, row_bucket, epilogue),
        .fallback_reason = if (generated_candidate) generatedCandidateFallbackReason(backend, kernel_id) else .none,
        .kernel_id = if (generated_candidate) kernel_id else "",
        .candidate_source_path = if (generated_candidate) source_path else "",
    });
}

fn generatedCandidateFallbackReason(backend: Backend, kernel_id: []const u8) FallbackReason {
    if (backend == .metal and checkedInMetalEvidenceForKernel(kernel_id) != null) {
        return .generated_runtime_not_wired;
    }
    if (backend == .cuda) {
        const artifact = generatedArtifactForKernel(backend, kernel_id) orelse return .generated_artifact_missing;
        if (artifactRuntimeWired(artifact)) return .generated_runtime_not_wired;
    }
    return .generated_artifact_missing;
}

fn unsupportedLowering(
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    dispatch: quant_matmul.DispatchKind,
    reason: FallbackReason,
) QuantKernelLowering {
    return .{
        .plan_id = planId(backend, format, row_bucket, epilogue, dispatch),
        .backend = backend,
        .format = format,
        .row_bucket = row_bucket,
        .epilogue = epilogue,
        .schedule = unsupportedSchedule(row_bucket, dispatch),
        .production_route = .unsupported,
        .candidate_route = .unsupported,
        .production_kernel_id = "",
        .fallback_reason = reason,
        .kernel_id = "",
        .candidate_source_path = "",
    };
}

fn unsupportedSchedule(row_bucket: quant_matmul.RowBucket, dispatch: quant_matmul.DispatchKind) QuantKernelSchedule {
    return .{
        .dispatch = dispatch,
        .row_bucket = row_bucket,
        .tile_rows = 0,
        .tile_cols = 0,
        .vector_width = 0,
        .threads_per_block = 0,
        .shared_memory_bytes = 0,
        .register_pressure_hint = 0,
        .tensor_core_eligible = false,
    };
}

fn planId(
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    dispatch: quant_matmul.DispatchKind,
) QuantKernelPlanId {
    return .{
        .backend = backend,
        .format = format,
        .row_bucket = row_bucket,
        .epilogue = epilogue,
        .dispatch = dispatch,
    };
}

pub fn planIdName(allocator: std.mem.Allocator, id: QuantKernelPlanId) ![]u8 {
    return std.fmt.allocPrint(
        allocator,
        "{s}/{s}/{s}/{s}/{s}",
        .{
            @tagName(id.backend),
            @tagName(id.format),
            @tagName(id.row_bucket),
            @tagName(id.epilogue),
            @tagName(id.dispatch),
        },
    );
}

pub fn loweringRouteName(route: LoweringRoute) []const u8 {
    return switch (route) {
        .generated_production => "generated_production",
        .generated_dev_candidate => "generated_dev_candidate",
        .handwritten_production => "handwritten_production",
        .unsupported => "unsupported",
    };
}

pub fn fallbackReasonName(reason: FallbackReason) []const u8 {
    return switch (reason) {
        .none => "none",
        .unsupported_format => "unsupported_format",
        .unsupported_shape => "unsupported_shape",
        .unsupported_epilogue => "unsupported_epilogue",
        .unsupported_backend => "unsupported_backend",
        .generated_artifact_missing => "generated_artifact_missing",
        .generated_runtime_not_wired => "generated_runtime_not_wired",
        .tensor_core_repack_required => "tensor_core_repack_required",
    };
}

fn appendFmt(
    allocator: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const chunk = try std.fmt.allocPrint(allocator, fmt, args);
    defer allocator.free(chunk);
    try out.appendSlice(allocator, chunk);
}

pub fn metalGeneratedCounterNameForArtifact(artifact: GeneratedMatmulArtifact) ?[]const u8 {
    if (artifact.backend != .metal or !artifactRuntimeWired(artifact)) return null;
    return switch (artifact.format) {
        .q8_0 => switch (artifact.epilogue) {
            .none => "q8_0_small_batch",
            .bias => "q8_0_small_batch_bias",
            .bias_gelu => "q8_0_small_batch_bias_gelu",
            .relu => "q8_0_small_batch_relu",
            else => null,
        },
        .q8_1 => if (artifact.epilogue == .none) "q8_1_small_batch" else null,
        .q8_k => if (artifact.epilogue == .none) "q8_k_small_batch" else null,
        .q2_k => switch (artifact.epilogue) {
            .none => "q2_k_small_batch",
            .bias => "q2_k_small_batch_bias",
            .bias_gelu => "q2_k_small_batch_bias_gelu",
            else => null,
        },
        .q3_k => switch (artifact.epilogue) {
            .none => "q3_k_small_batch",
            .bias => "q3_k_small_batch_bias",
            .bias_gelu => "q3_k_small_batch_bias_gelu",
            else => null,
        },
        .q4_0 => if (artifact.epilogue == .none) "q4_0_small_batch" else null,
        .q4_1 => if (artifact.epilogue == .none) "q4_1_small_batch" else null,
        .q5_0 => if (artifact.epilogue == .none) "q5_0_small_batch" else null,
        .q5_1 => if (artifact.epilogue == .none) "q5_1_small_batch" else null,
        .q4_k => switch (artifact.epilogue) {
            .none => "q4_k_small_batch",
            .bias => "q4_k_small_batch_bias",
            .bias_gelu => "q4_k_small_batch_bias_gelu",
            else => null,
        },
        .q5_k => switch (artifact.epilogue) {
            .none => "q5_k_small_batch",
            .bias => "q5_k_small_batch_bias",
            .bias_gelu => "q5_k_small_batch_bias_gelu",
            else => null,
        },
        .q6_k => switch (artifact.epilogue) {
            .none => "q6_k_small_batch",
            .bias => "q6_k_small_batch_bias",
            .bias_gelu => "q6_k_small_batch_bias_gelu",
            else => null,
        },
        else => null,
    };
}

pub fn metalRuntimeRouteSummaryJson(allocator: std.mem.Allocator) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    try out.append(allocator, '{');
    var first = true;
    for (first_generated_matmul_artifacts) |artifact| {
        const counter_name = metalGeneratedCounterNameForArtifact(artifact) orelse continue;
        const lowering = loweringFor(.metal, artifact.format, artifact.row_bucket, artifact.epilogue);
        const plan_name = try planIdName(allocator, lowering.plan_id);
        defer allocator.free(plan_name);
        if (!first) try out.append(allocator, ',');
        first = false;
        const runtime_gate_env = if (artifactRuntimeGateEnv(artifact)) |env| std.mem.span(env) else "";
        const generated_source_path = artifact.source_path;
        try appendFmt(
            allocator,
            &out,
            \\{f}:{{"plan_id":{f},"format":{f},"row_bucket":{f},"epilogue":{f},"dispatch":{f},"kernel_id":{f},"production_kernel_id":{f},"production_route":{f},"candidate_route":{f},"fallback_reason":{f},"source_path":{f},"generated_source_path":{f},"runtime_gate_env":{f},"promotion_ready":{}}}
        ,
            .{
                std.json.fmt(counter_name, .{}),
                std.json.fmt(plan_name, .{}),
                std.json.fmt(@tagName(artifact.format), .{}),
                std.json.fmt(@tagName(artifact.row_bucket), .{}),
                std.json.fmt(@tagName(artifact.epilogue), .{}),
                std.json.fmt(@tagName(lowering.schedule.dispatch), .{}),
                std.json.fmt(artifact.kernel_id, .{}),
                std.json.fmt(lowering.production_kernel_id, .{}),
                std.json.fmt(loweringRouteName(lowering.production_route), .{}),
                std.json.fmt(loweringRouteName(lowering.candidate_route), .{}),
                std.json.fmt(fallbackReasonName(lowering.fallback_reason), .{}),
                std.json.fmt(artifact.source_path, .{}),
                std.json.fmt(generated_source_path, .{}),
                std.json.fmt(runtime_gate_env, .{}),
                artifactHasPromotionEvidence(artifact),
            },
        );
    }
    try out.append(allocator, '}');
    return try out.toOwnedSlice(allocator);
}

fn tensorTypeName(tensor_type: tensor_types.TensorType) []const u8 {
    return switch (tensor_type) {
        .known => |known| @tagName(known),
        .bitnet_tl2 => "bitnet_tl2",
        .unknown => "unknown",
    };
}

pub fn loweringDiagnostic(allocator: std.mem.Allocator, lowering: QuantKernelLowering) ![]u8 {
    const plan_name = try planIdName(allocator, lowering.plan_id);
    defer allocator.free(plan_name);
    return std.fmt.allocPrint(
        allocator,
        "plan={s} production={s} candidate={s} fallback={s} production_kernel={s} candidate_kernel={s} candidate_source={s}",
        .{
            plan_name,
            loweringRouteName(lowering.production_route),
            loweringRouteName(lowering.candidate_route),
            fallbackReasonName(lowering.fallback_reason),
            lowering.production_kernel_id,
            lowering.kernel_id,
            lowering.candidate_source_path,
        },
    );
}

pub fn registryLoweringFor(
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    dispatch: quant_matmul.DispatchKind,
) QuantKernelLowering {
    if (first_registry.lookup(backend, format, row_bucket, epilogue, dispatch)) |lowering| return lowering;
    if (specFor(format) == null) {
        return unsupportedLowering(backend, format, row_bucket, epilogue, dispatch, .unsupported_format);
    }
    const expected_dispatch = dispatchForRowBucket(row_bucket) orelse
        return unsupportedLowering(backend, format, row_bucket, epilogue, dispatch, .unsupported_shape);
    if (expected_dispatch != dispatch) {
        return unsupportedLowering(backend, format, row_bucket, epilogue, dispatch, .unsupported_shape);
    }
    return loweringFor(backend, format, row_bucket, epilogue);
}

pub fn registryLoweringForPlan(
    backend: Backend,
    plan: quant_matmul.Plan,
    epilogue: Epilogue,
) QuantKernelLowering {
    const lowering = registryLoweringFor(backend, plan.format, plan.row_bucket, epilogue, plan.dispatch);
    const artifact = promotedArtifactFor(lowering) orelse return lowering;
    if (artifact.runtime_shape.matches(plan)) return lowering;

    var handwritten = lowering;
    handwritten.production_route = .handwritten_production;
    handwritten.production_kernel_id = productionKernelId(backend, plan.format, plan.row_bucket, epilogue);
    return handwritten;
}

pub fn generatedArtifactSupportsPlan(
    backend: Backend,
    plan: quant_matmul.Plan,
    epilogue: Epilogue,
) bool {
    const artifact = generatedArtifactForCandidate(backend, plan.format, plan.row_bucket, epilogue) orelse return false;
    return artifact.runtime_shape.matches(plan);
}

// registryLoweringFor is a linear scan over the full route registry, and the
// partition executor records plan counters once per planned dispatch. A model
// run only ever touches a handful of distinct routes, so the counters for the
// routes seen so far are memoized here. Counters are a pure function of
// comptime registry data, so the memo never invalidates.
const PlannedCountersMemoEntry = struct {
    key: u64,
    counters: PlanCounters,
};
const planned_counters_memo_capacity = 64;
var planned_counters_memo: [planned_counters_memo_capacity]PlannedCountersMemoEntry = undefined;
var planned_counters_memo_len = std.atomic.Value(usize).init(0);
// Spin mutex (std.Thread.Mutex was removed in Zig 0.16). Held only for the
// memo insert, never across the registry scan.
var planned_counters_memo_mutex: std.atomic.Mutex = .unlocked;

const PlannedPlanCountersKey = struct {
    backend: Backend,
    dispatch: quant_matmul.DispatchKind,
    primitive: quant_matmul.Primitive,
    operator: quant_matmul.Operator,
    row_bucket: quant_matmul.RowBucket,
    format: quant_matmul.Format,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    epilogue: Epilogue,

    fn eql(a: @This(), b: @This()) bool {
        return std.meta.eql(a, b);
    }
};

const PlannedPlanCountersMemoEntry = struct {
    key: PlannedPlanCountersKey,
    counters: PlanCounters,
};
const planned_plan_counters_memo_capacity = 128;
var planned_plan_counters_memo: [planned_plan_counters_memo_capacity]PlannedPlanCountersMemoEntry = undefined;
var planned_plan_counters_memo_len = std.atomic.Value(usize).init(0);
var planned_plan_counters_memo_mutex: std.atomic.Mutex = .unlocked;

fn plannedCountersKey(
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    dispatch: quant_matmul.DispatchKind,
) u64 {
    var key: u64 = @intFromEnum(backend);
    // Format is enum(u16) with an explicit tag at 254, so give it 16 bits.
    key = (key << 16) | @intFromEnum(format);
    key = (key << 8) | @intFromEnum(row_bucket);
    key = (key << 8) | @intFromEnum(epilogue);
    key = (key << 8) | @intFromEnum(dispatch);
    return key;
}

pub fn plannedCountersFor(
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
    dispatch: quant_matmul.DispatchKind,
) PlanCounters {
    const key = plannedCountersKey(backend, format, row_bucket, epilogue, dispatch);
    // Entries [0..len) are fully written before len is release-stored, so the
    // lock-free read scan only ever sees complete entries.
    const len = planned_counters_memo_len.load(.acquire);
    for (planned_counters_memo[0..len]) |entry| {
        if (entry.key == key) return entry.counters;
    }
    const counters = countersForLowering(registryLoweringFor(backend, format, row_bucket, epilogue, dispatch));
    platform.sync.lockYielding(&planned_counters_memo_mutex);
    defer planned_counters_memo_mutex.unlock();
    const locked_len = planned_counters_memo_len.load(.acquire);
    for (planned_counters_memo[len..locked_len]) |entry| {
        if (entry.key == key) return counters;
    }
    if (locked_len < planned_counters_memo_capacity) {
        planned_counters_memo[locked_len] = .{ .key = key, .counters = counters };
        planned_counters_memo_len.store(locked_len + 1, .release);
    }
    return counters;
}

/// Shape-aware counterpart used by CUDA plan recording. Unlike the route-only
/// memo above, this retains the full runtime plan because promoted artifacts
/// may fall back below their minimum input dimension.
pub fn plannedCountersForPlan(
    backend: Backend,
    plan: quant_matmul.Plan,
    epilogue: Epilogue,
) PlanCounters {
    const key = PlannedPlanCountersKey{
        .backend = backend,
        .dispatch = plan.dispatch,
        .primitive = plan.primitive,
        .operator = plan.operator,
        .row_bucket = plan.row_bucket,
        .format = plan.format,
        .rows = plan.rows,
        .in_dim = plan.in_dim,
        .out_dim = plan.out_dim,
        .epilogue = epilogue,
    };
    const len = planned_plan_counters_memo_len.load(.acquire);
    for (planned_plan_counters_memo[0..len]) |entry| {
        if (entry.key.eql(key)) return entry.counters;
    }

    const counters = countersForLowering(registryLoweringForPlan(backend, plan, epilogue));
    platform.sync.lockYielding(&planned_plan_counters_memo_mutex);
    defer planned_plan_counters_memo_mutex.unlock();
    const locked_len = planned_plan_counters_memo_len.load(.acquire);
    for (planned_plan_counters_memo[len..locked_len]) |entry| {
        if (entry.key.eql(key)) return entry.counters;
    }
    if (locked_len < planned_plan_counters_memo_capacity) {
        planned_plan_counters_memo[locked_len] = .{ .key = key, .counters = counters };
        planned_plan_counters_memo_len.store(locked_len + 1, .release);
    }
    return counters;
}

pub fn countersForLowering(lowering: QuantKernelLowering) PlanCounters {
    var counters = PlanCounters{ .quant_kernel_planned_ops = 1 };
    switch (lowering.production_route) {
        .generated_production => counters.quant_kernel_generated_production = 1,
        .handwritten_production => counters.quant_kernel_handwritten_production = 1,
        .generated_dev_candidate => unreachable,
        .unsupported => counters.quant_kernel_unsupported_routes = 1,
    }
    if (lowering.candidate_route == .generated_dev_candidate) counters.quant_kernel_generated_candidates = 1;
    switch (lowering.fallback_reason) {
        .generated_artifact_missing => counters.quant_kernel_fallback_generated_artifact_missing = 1,
        .generated_runtime_not_wired => counters.quant_kernel_fallback_generated_runtime_not_wired = 1,
        .unsupported_format => {
            counters.quant_kernel_fallback_unsupported_format = 1;
            counters.quant_kernel_fallback_unsupported = 1;
        },
        .unsupported_shape => {
            counters.quant_kernel_fallback_unsupported_shape = 1;
            counters.quant_kernel_fallback_unsupported = 1;
        },
        .unsupported_epilogue => {
            counters.quant_kernel_fallback_unsupported_epilogue = 1;
            counters.quant_kernel_fallback_unsupported = 1;
        },
        .unsupported_backend => {
            counters.quant_kernel_fallback_unsupported_backend = 1;
            counters.quant_kernel_fallback_unsupported = 1;
        },
        .tensor_core_repack_required => counters.quant_kernel_fallback_tensor_core_repack_required = 1,
        .none => {},
    }
    return counters;
}

pub fn addCountersToStats(stats: anytype, counters: PlanCounters) void {
    inline for (@typeInfo(PlanCounters).@"struct".fields) |field| {
        @field(stats.*, field.name) += @intCast(@field(counters, field.name));
    }
}

fn promotedArtifactFor(lowering: QuantKernelLowering) ?GeneratedMatmulArtifact {
    if (lowering.production_route != .generated_production) return null;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend == lowering.backend and
            artifact.format == lowering.format and
            artifact.row_bucket == lowering.row_bucket and
            artifact.epilogue == lowering.epilogue and
            std.mem.eql(u8, artifact.kernel_id, lowering.production_kernel_id) and
            artifactHasPromotionEvidence(artifact))
        {
            return artifact;
        }
    }
    return null;
}

fn promoteLoweringIfArtifactReady(lowering: QuantKernelLowering) QuantKernelLowering {
    if (lowering.candidate_route != .generated_dev_candidate) return lowering;
    for (first_generated_matmul_artifacts) |artifact| {
        if (!artifactHasPromotionEvidence(artifact)) continue;
        if (promotedLoweringForArtifact(lowering, artifact)) |promoted| return promoted;
    }
    return lowering;
}

fn cudaScheduleForArtifact(artifact: GeneratedMatmulArtifact) ?QuantKernelSchedule {
    const plan = cudaRenderPlanForArtifact(artifact) orelse return null;
    return .{
        .dispatch = plan.route.dispatch,
        .row_bucket = plan.route.row_bucket,
        .tile_rows = plan.launch.output_rows_per_block,
        .tile_cols = plan.launch.output_cols_per_block,
        .vector_width = plan.launch.output_cols_per_block,
        .threads_per_block = plan.launch.threads_per_block,
        .shared_memory_bytes = plan.launch.static_shared_memory_bytes + plan.launch.dynamic_shared_memory_bytes,
        .register_pressure_hint = 8,
        .tensor_core_eligible = false,
    };
}

fn promotedLoweringForArtifact(lowering: QuantKernelLowering, artifact: GeneratedMatmulArtifact) ?QuantKernelLowering {
    if (lowering.candidate_route != .generated_dev_candidate) return null;
    if (artifact.backend != lowering.backend or
        artifact.format != lowering.format or
        artifact.row_bucket != lowering.row_bucket or
        artifact.epilogue != lowering.epilogue or
        !std.mem.eql(u8, artifact.kernel_id, lowering.kernel_id))
    {
        return null;
    }

    var promoted = lowering;
    promoted.production_route = .generated_production;
    promoted.candidate_route = .unsupported;
    promoted.production_kernel_id = artifact.kernel_id;
    promoted.fallback_reason = .none;
    promoted.kernel_id = "";
    promoted.candidate_source_path = "";
    if (artifact.backend == .cuda) {
        promoted.schedule = cudaScheduleForArtifact(artifact) orelse return null;
    }
    return promoted;
}

pub fn artifactHasPromotionEvidence(artifact: GeneratedMatmulArtifact) bool {
    return std.mem.eql(u8, artifactPromotionBlocker(artifact), metal_blocker_none);
}

/// Target attested by the benchmark record used to promote this artifact.
/// `null` means an exact-target catalog must keep the artifact disabled.
pub fn artifactPromotionTargetFingerprint(artifact: GeneratedMatmulArtifact) ?u64 {
    if (artifact.backend != .cuda) return null;
    const bench = benchmarkForArtifact(artifact) orelse return null;
    if (bench.target_fingerprint == 0) return null;
    return bench.target_fingerprint;
}

/// Target attested by the qualification evidence used to promote a generated
/// attention composite. Only the exact score-prework decode composites and the
/// SM89 flash-prefill composites carry promotion evidence today, and their
/// bitwise-parity and paired-throughput qualification (strict validator +
/// paged differential + L4 release gate) is scoped to SM89; any other
/// attention artifact must remain disabled in an exact-target catalog.
pub fn attentionArtifactPromotionTargetFingerprint(artifact: GeneratedArtifact) ?u64 {
    if (artifact.backend != .cuda or artifact.opKind() != .attention) return null;
    if (artifact.promotion_evidence_command.len == 0) return null;
    if (artifact.cuda_attention_kernel) |kind| {
        return switch (kind) {
            .gqa_decode_score_prework_hd256_f32,
            .gqa_decode_score_prework_hd512_f32,
            => cuda_sm89_promotion_target_fingerprint,
            else => null,
        };
    }
    if (artifact.cuda_flash_prefill_kernel) |kind| {
        return switch (kind) {
            .gqa_prefill_flash_sm89_hd256_swa512_f32,
            .gqa_prefill_flash_sm89_hd512_global_f32,
            => cuda_sm89_promotion_target_fingerprint,
        };
    }
    return null;
}

fn artifactPromotionBlocker(artifact: GeneratedMatmulArtifact) []const u8 {
    if (!artifact.production_enabled) return disabledArtifactPromotionBlocker(artifact);
    return switch (artifact.backend) {
        .cuda => blk: {
            const bench = benchmarkForArtifact(artifact) orelse return "missing_benchmark_record";
            break :blk benchmarkPromotionBlocker(bench);
        },
        .metal => metalArtifactPromotionBlocker(artifact),
    };
}

fn artifactPromotionBlockerEvidencePath(artifact: GeneratedMatmulArtifact) []const u8 {
    if (metalPromotionBlockerEvidenceFor(artifact)) |evidence| return evidence.evidence_path;
    return "";
}

fn artifactPromotionBlockerRequiresProductionRegressionClear(artifact: GeneratedMatmulArtifact) bool {
    if (metalPromotionBlockerEvidenceFor(artifact)) |evidence| return evidence.requires_production_regression_clear;
    return false;
}

fn disabledArtifactPromotionBlocker(artifact: GeneratedMatmulArtifact) []const u8 {
    if (metalPromotionBlockerEvidenceFor(artifact)) |evidence| return evidence.blocker;
    if (!artifactRuntimeWired(artifact)) return "production_disabled";
    return switch (artifact.backend) {
        .cuda => "awaiting_cuda_promotion_evidence",
        .metal => "awaiting_metal_promotion_evidence",
    };
}

fn metalPromotionBlockerEvidenceFor(artifact: GeneratedMatmulArtifact) ?MetalPromotionBlockerEvidence {
    @setEvalBranchQuota(10_000);
    if (artifact.backend != .metal) return null;
    for (first_metal_promotion_blocker_evidence) |evidence| {
        if (std.mem.eql(u8, artifact.kernel_id, evidence.kernel_id)) return evidence;
    }
    return null;
}

fn metalPromotionBlockerEvidenceCount(blocker: []const u8) usize {
    var count: usize = 0;
    for (first_metal_promotion_blocker_evidence) |evidence| {
        if (std.mem.eql(u8, evidence.blocker, blocker)) count += 1;
    }
    return count;
}

fn metalPromotionBlockerEvidencePathCount() usize {
    var count: usize = 0;
    for (first_metal_promotion_blocker_evidence) |evidence| {
        if (evidence.evidence_path.len != 0) count += 1;
    }
    return count;
}

fn metalArtifactPromotionBlocker(artifact: GeneratedMatmulArtifact) []const u8 {
    return metalArtifactPromotionBlockerWithEvidence(artifact, &first_metal_runtime_evidence);
}

fn metalArtifactPromotionBlockerWithEvidence(artifact: GeneratedMatmulArtifact, evidence_records: []const MetalRuntimeEvidence) []const u8 {
    if (artifact.backend != .metal) return "non_metal_artifact";
    if (!artifact.production_enabled) return disabledArtifactPromotionBlocker(artifact);
    if (!std.mem.endsWith(u8, artifact.source_path, ".metal")) return "missing_metal_source_path";
    if (artifactSourceFingerprint(artifact) == 0) return "missing_source_fingerprint";
    if (!commandFirstTokenEquals(artifact.check_command, "xcrun") or !commandHasToken(artifact.check_command, "metal")) return "missing_xcrun_metal_command";
    if (!commandHasArgValue(artifact.check_command, "-c", artifact.source_path)) return "metal_check_missing_source";
    if (!commandHasArgValue(artifact.check_command, "-o", metalAirPathForKernel(artifact.kernel_id) orelse return "missing_metal_air_path")) return "metal_check_missing_output";
    if (!commandHasToken(artifact.runtime_evidence_command, "quant-kernel-metal-runtime-check")) return "missing_metal_runtime_evidence_command";
    if (!commandHasToken(artifact.runtime_evidence_command, "--evidence-out")) return "missing_metal_evidence_out_arg";
    if (!commandHasArgValue(artifact.runtime_evidence_command, "--repeat-runs", metal_promotion_repeat_runs_text)) return "missing_metal_repeat_runs";
    if (!metalPromotionEvidenceCommandMatchesPolicy(artifact)) return "metal_promotion_evidence_command_mismatch";
    if (!commandHasToken(artifact.promotion_evidence_command, "quant-kernel-metal-runtime-check")) return "missing_metal_promotion_evidence_command";
    if (!commandHasToken(artifact.promotion_evidence_command, "--attest-provenance")) return "missing_metal_attested_provenance";
    if (!commandHasToken(artifact.promotion_evidence_command, "--promotion-ready-kernel")) return "missing_metal_promotion_ready_kernel";
    if (!commandHasArgValue(artifact.promotion_evidence_command, "--promotion-ready-kernel", artifact.kernel_id)) return "wrong_metal_promotion_ready_kernel";
    if (!commandHasToken(artifact.promotion_check_command, "quant-kernel-metal-runtime-check")) return "missing_metal_promotion_check_command";
    if (!commandHasToken(artifact.promotion_check_command, "--check-evidence")) return "missing_metal_check_evidence_arg";
    if (!commandHasToken(artifact.promotion_check_command, "--require-promotion-ready")) return "missing_metal_require_promotion_ready";
    if (!commandHasArgValue(artifact.promotion_check_command, "--require-kernel", artifact.kernel_id)) return "missing_metal_require_kernel";
    if (!metalPromotionCommandsUseSameEvidencePath(artifact)) return "metal_promotion_evidence_path_mismatch";
    const evidence = metalRuntimeEvidenceFor(artifact, evidence_records) orelse return "missing_metal_runtime_evidence";
    const provenance_blocker = metalRuntimeEvidenceProvenanceBlocker(evidence);
    if (!std.mem.eql(u8, provenance_blocker, metal_blocker_none)) return provenance_blocker;
    if (!evidence.production_enabled) return "production_disabled";
    if (!evidence.correctness_passed) return "correctness_evidence_failed";
    if (!evidence.generated_route_checked) return metal_blocker_missing_generated_route;
    if (metalProviderRouteRequiredForKernel(artifact.kernel_id) and !evidence.provider_route_checked) return metal_blocker_missing_provider_route;
    if (evidence.repeat_runs < metal_promotion_repeat_runs) return "insufficient_metal_repeats";
    const speedup_blocker = metalPromotionSpeedupBlocker(evidence.measured_speedup, evidence.minimum_repeat_speedup);
    if (!std.mem.eql(u8, speedup_blocker, metal_blocker_none)) return speedup_blocker;
    if (!evidence.benchmark_passed) return "benchmark_evidence_failed";
    if (!evidence.promotion_ready) return "metal_promotion_not_ready";
    return metal_blocker_none;
}

fn metalRuntimeEvidenceProvenanceBlocker(evidence: MetalRuntimeEvidence) []const u8 {
    if (evidence.legacy_production_exception) {
        if (!metalLegacyEvidenceExceptionAllowed(evidence.kernel_id) or
            !std.mem.eql(u8, evidence.provenance_status, metal_evidence_provenance_legacy_unattested) or
            !std.mem.eql(u8, evidence.provenance_blocker, metal_blocker_missing_reproducible_provenance))
        {
            return metal_blocker_invalid_legacy_provenance_exception;
        }
        return metal_blocker_none;
    }
    if (!std.mem.eql(u8, evidence.provenance_status, metal_evidence_provenance_attested_v1)) {
        return metal_blocker_missing_reproducible_provenance;
    }
    if (evidence.provenance_blocker.len != 0) return metal_blocker_invalid_attested_provenance;
    if (!evidence.source_tree_clean) return metal_blocker_dirty_source_tree;
    if ((!isHexDigest(evidence.source_commit, 40) and !isHexDigest(evidence.source_commit, 64)) or
        !std.mem.eql(u8, evidence.source_status_sha256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855") or
        evidence.host_os.len == 0 or
        evidence.host_arch.len == 0 or
        evidence.accelerator_name.len == 0 or
        evidence.metal_compiler_version.len == 0 or
        evidence.zig_version.len == 0 or
        evidence.recorded_at_utc.len == 0)
    {
        return metal_blocker_missing_reproducible_provenance;
    }
    return metal_blocker_none;
}

fn metalLegacyEvidenceExceptionAllowed(kernel_id: []const u8) bool {
    return std.mem.eql(u8, kernel_id, first_general_metal_q2_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q3_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q4_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q5_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q6_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q8_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q8_k_kernel_id);
}

fn isHexDigest(value: []const u8, expected_len: usize) bool {
    if (value.len != expected_len) return false;
    for (value) |byte| {
        if (!((byte >= '0' and byte <= '9') or
            (byte >= 'a' and byte <= 'f') or
            (byte >= 'A' and byte <= 'F')))
        {
            return false;
        }
    }
    return true;
}

pub fn metalPromotionSpeedupBlocker(measured_speedup: f64, minimum_repeat_speedup: f64) []const u8 {
    if (!metalPromotionSpeedupPasses(measured_speedup)) return metal_blocker_speedup_gate_missing;
    if (!metalPromotionSpeedupPasses(minimum_repeat_speedup)) return metal_blocker_unstable_benchmark_timing;
    return metal_blocker_none;
}

pub fn metalPromotionSpeedupPasses(speedup_value: f64) bool {
    if (!std.math.isFinite(speedup_value)) return false;
    return speedup_value >= metal_promotion_min_speedup;
}

pub fn metalProviderRouteRequiredForKernel(kernel_id: []const u8) bool {
    return std.mem.eql(u8, kernel_id, first_general_metal_q8_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q8_bias_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q8_bias_gelu_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q8_relu_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q8_1_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q8_k_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q2_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q2_bias_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q2_bias_gelu_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q3_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q3_bias_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q3_bias_gelu_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q4_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q4_bias_kernel_id) or
        std.mem.eql(u8, kernel_id, first_lazy_metal_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q5_0_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q5_1_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q5_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q5_bias_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q5_bias_gelu_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q6_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q6_bias_kernel_id) or
        std.mem.eql(u8, kernel_id, first_general_metal_q6_bias_gelu_kernel_id);
}

pub fn artifactHasMetalProviderRouteEvidence(artifact: GeneratedMatmulArtifact) bool {
    if (artifact.backend != .metal or !artifactRuntimeWired(artifact)) return false;
    return switch (artifact.format) {
        .q8_0 => artifact.epilogue == .none or artifact.epilogue == .bias or artifact.epilogue == .bias_gelu or artifact.epilogue == .relu,
        .q8_1, .q8_k => artifact.epilogue == .none,
        .q2_k => artifact.epilogue == .none or artifact.epilogue == .bias or artifact.epilogue == .bias_gelu,
        .q5_0, .q5_1 => artifact.epilogue == .none,
        .q3_k, .q4_k, .q5_k, .q6_k => artifact.epilogue == .none or artifact.epilogue == .bias or artifact.epilogue == .bias_gelu,
        else => false,
    };
}

fn metalPromotionEvidenceCommandMatchesPolicy(artifact: GeneratedMatmulArtifact) bool {
    const evidence_path = commandArgValue(artifact.promotion_evidence_command, "--evidence-out") orelse return false;
    return std.mem.startsWith(u8, artifact.promotion_evidence_command, first_metal_promotion_evidence_command) and
        std.mem.containsAtLeast(u8, evidence_path, 1, artifact.kernel_id) and
        commandHasArgValue(artifact.promotion_evidence_command, "--repeat-runs", metal_promotion_repeat_runs_text) and
        commandHasArgValue(artifact.promotion_evidence_command, "--measure-iters", metal_promotion_measure_iters_text) and
        commandHasToken(artifact.promotion_evidence_command, "--attest-provenance");
}

fn metalPromotionCommandsUseSameEvidencePath(artifact: GeneratedMatmulArtifact) bool {
    const evidence_path = commandArgValue(artifact.promotion_evidence_command, "--evidence-out") orelse return false;
    return commandHasArgValue(artifact.promotion_check_command, "--check-evidence", evidence_path);
}

fn benchmarkHasPromotionEvidence(bench: BenchmarkCase) bool {
    return std.mem.eql(u8, benchmarkPromotionBlocker(bench), "none");
}

fn benchmarkPromotionBlocker(bench: BenchmarkCase) []const u8 {
    return benchmarkPromotionBlockerWithEvidence(bench, &first_benchmark_evidence);
}

fn benchmarkPromotionBlockerWithEvidence(bench: BenchmarkCase, evidence_records: []const BenchmarkEvidence) []const u8 {
    if (!bench.production_enabled) return "production_disabled";
    if (bench.backend != .cuda) return "non_cuda_benchmark";
    if (bench.target_fingerprint == 0) return "missing_target_fingerprint";
    if (bench.generated_kernel_id.len == 0) return "missing_kernel_id";
    if (bench.generated_source_path.len == 0) return "missing_source_path";
    if (bench.generated_source_fingerprint == 0) return "missing_source_fingerprint";
    if (!benchmarkPtxArgRecognized(bench.generated_ptx_arg)) return "wrong_generated_ptx_arg";
    if (bench.handwritten_baseline.len == 0) return "missing_handwritten_baseline";
    if (!isCudaModulePath(bench.generated_ptx_path)) return "missing_generated_ptx_path";
    const builds_ptx = commandHasToken(bench.generated_ptx_command, "-ptx");
    const builds_fatbin = commandHasToken(bench.generated_ptx_command, "-fatbin");
    if (!commandHasToken(bench.generated_ptx_command, "nvcc") or (!builds_ptx and !builds_fatbin)) return "missing_nvcc_ptx_command";
    if (builds_ptx and !commandHasToken(bench.generated_ptx_command, "-arch=compute_75")) return "wrong_ptx_arch";
    if (builds_fatbin and !commandHasToken(bench.generated_ptx_command, "-gencode=arch=compute_89,code=sm_89")) return "missing_native_fatbin_arch";
    if (!commandHasToken(bench.generated_ptx_command, bench.generated_source_path)) return "ptx_command_missing_source";
    if (!commandHasArgValue(bench.generated_ptx_command, "-o", bench.generated_ptx_path)) return "ptx_command_missing_output";
    if (!commandFirstTokenEquals(bench.benchmark_command, "zig-out/bin/antfly-inference") or !commandHasToken(bench.benchmark_command, "bench-cuda")) return "missing_bench_cuda_command";
    if (!commandHasArgValue(bench.benchmark_command, "--warmup-iters", "5")) return "missing_warmup_iters";
    if (!commandHasArgValue(bench.benchmark_command, "--measure-iters", "50")) return "missing_measure_iters";
    if (std.mem.eql(u8, bench.generated_ptx_arg, "--quant-compiler-generated-ptx") and
        !commandHasToken(bench.benchmark_command, "--quant-compiler-lazy-target")) return "missing_lazy_target_flag";
    if (!commandHasToken(bench.benchmark_command, bench.generated_ptx_arg)) return "benchmark_missing_generated_ptx_arg";
    if (!commandHasArgValue(bench.benchmark_command, bench.generated_ptx_arg, bench.generated_ptx_path)) return "benchmark_missing_generated_ptx_path";
    if (!commandHasArgValue(bench.benchmark_command, "--quant-compiler-repeat-runs", "3")) return "missing_benchmark_repeat_runs";
    if (!commandHasToken(bench.benchmark_command, "--quant-compiler-evidence-out")) return "benchmark_missing_evidence_out_arg";
    if (!std.math.isFinite(bench.correctness_tolerance_abs) or bench.correctness_tolerance_abs <= 0.0 or bench.correctness_tolerance_abs > 0.01) return "correctness_tolerance_missing_or_loose";
    if (bench.correctness_evidence_path.len == 0) return "missing_correctness_evidence";
    if (bench.benchmark_evidence_path.len == 0) return "missing_benchmark_evidence";
    if (!std.mem.eql(u8, bench.correctness_evidence_path, bench.benchmark_evidence_path)) return "evidence_path_mismatch";
    if (!commandHasArgValue(bench.benchmark_command, "--quant-compiler-evidence-out", bench.benchmark_evidence_path)) return "benchmark_missing_evidence_out_path";
    if (!std.mem.eql(u8, bench.benchmark_mode, "sequential")) return "missing_sequential_benchmark_evidence";
    if (!std.math.isFinite(bench.minimum_speedup) or bench.minimum_speedup < 1.0) return "speedup_gate_missing";
    const evidence = benchmarkEvidenceFor(bench, evidence_records) orelse return "missing_matching_evidence_record";
    if (!evidence.correctness_passed) return "correctness_evidence_failed";
    if (!evidence.benchmark_passed) return "benchmark_evidence_failed";
    if (evidence.repeat_runs < 3) return "insufficient_benchmark_repeats";
    if (!std.math.isFinite(evidence.measured_speedup)) return "speedup_gate_missing";
    if (evidence.measured_speedup < bench.minimum_speedup) return "speedup_gate_missing";
    return "none";
}

fn commandFirstTokenEquals(command: []const u8, expected: []const u8) bool {
    var tokens = std.mem.tokenizeScalar(u8, command, ' ');
    const first = tokens.next() orelse return false;
    return std.mem.eql(u8, first, expected);
}

fn commandHasToken(command: []const u8, expected: []const u8) bool {
    var tokens = std.mem.tokenizeScalar(u8, command, ' ');
    while (tokens.next()) |token| {
        if (std.mem.eql(u8, token, expected)) return true;
    }
    return false;
}

fn commandHasArgValue(command: []const u8, arg: []const u8, value: []const u8) bool {
    const actual = commandArgValue(command, arg) orelse return false;
    return std.mem.eql(u8, actual, value);
}

fn commandArgValue(command: []const u8, arg: []const u8) ?[]const u8 {
    var tokens = std.mem.tokenizeScalar(u8, command, ' ');
    while (tokens.next()) |token| {
        if (std.mem.eql(u8, token, arg)) {
            return tokens.next();
        }
    }
    return null;
}

fn benchmarkMeasuredSpeedup(bench: BenchmarkCase) f64 {
    const evidence = benchmarkEvidenceFor(bench, &first_benchmark_evidence) orelse return 0.0;
    return evidence.measured_speedup;
}

fn normalizeCudaBundleTypes(allocator: std.mem.Allocator, source: []const u8) ![]u8 {
    const uint8_normalized = try std.mem.replaceOwned(u8, allocator, source, "uint8_t", "unsigned char");
    defer allocator.free(uint8_normalized);
    return std.mem.replaceOwned(u8, allocator, uint8_normalized, "uint16_t", "unsigned short");
}

test "quant kernel compiler promoted CUDA renderer fragments stay in sync with the production bundle" {
    const bundle = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "src/ops/cuda/artifacts/inference_cuda_kernels.cu", std.testing.allocator, .limited(4 * 1024 * 1024));
    defer std.testing.allocator.free(bundle);
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .cuda or !artifact.production_enabled) continue;
        const plan = cudaRenderPlanForArtifact(artifact) orelse return error.MissingCudaRenderPlan;
        const route_id = try cuda_renderer.planId(plan, std.testing.allocator);
        defer std.testing.allocator.free(route_id);
        const bundle_metadata = try std.fmt.allocPrint(std.testing.allocator, "kernel_id={s} plan_id={s}", .{ plan.kernel_id, route_id });
        defer std.testing.allocator.free(bundle_metadata);
        try std.testing.expect(std.mem.containsAtLeast(u8, bundle, 1, bundle_metadata));

        const support = cuda_renderer.supportFor(plan.lowering);
        for (support.helpers) |helper| {
            const normalized = try normalizeCudaBundleTypes(std.testing.allocator, helper.source);
            defer std.testing.allocator.free(normalized);
            try std.testing.expect(std.mem.containsAtLeast(u8, bundle, 1, normalized));
        }

        const body = try cuda_renderer.renderBodyAlloc(std.testing.allocator, plan);
        defer std.testing.allocator.free(body);
        const normalized_body = try normalizeCudaBundleTypes(std.testing.allocator, body);
        defer std.testing.allocator.free(normalized_body);
        try std.testing.expect(std.mem.containsAtLeast(u8, bundle, 1, normalized_body));
    }
}

test "quant kernel compiler paged CUDA address helpers stay in sync with the production bundle" {
    const bundle = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "src/ops/cuda/artifacts/inference_cuda_kernels.cu", std.testing.allocator, .limited(4 * 1024 * 1024));
    defer std.testing.allocator.free(bundle);
    for (cuda_renderer.attention_score_prework_runtime_external_helpers) |helper| {
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, bundle, helper.source));
    }
}

test "quant kernel compiler renders the runtime-wired CUDA attention region deterministically" {
    const first = try renderCudaRuntimeAttentionRegion(std.testing.allocator);
    defer std.testing.allocator.free(first);
    const second = try renderCudaRuntimeAttentionRegion(std.testing.allocator);
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualStrings(first, second);
    try std.testing.expectEqual(@as(usize, 15), first_generated_attention_artifacts.len);

    var runtime_wired_count: usize = 0;
    for (first_generated_attention_artifacts) |artifact| {
        if (!cudaAttentionArtifactRuntimeWired(artifact)) continue;
        runtime_wired_count += 1;
        if (cudaFlashPrefillRenderPlanForArtifact(artifact)) |plan| {
            const plan_id = try cuda_renderer.flashPrefillPlanId(plan, std.testing.allocator);
            defer std.testing.allocator.free(plan_id);
            const metadata = try std.fmt.allocPrint(std.testing.allocator, "kernel_id={s} plan_id={s}", .{ plan.kernel_id, plan_id });
            defer std.testing.allocator.free(metadata);
            try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, metadata));
            const exported_symbol = try std.fmt.allocPrint(std.testing.allocator, "#define ANTFLY_FLASH_KERNEL {s}\n", .{plan.kernel_id});
            defer std.testing.allocator.free(exported_symbol);
            try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, exported_symbol));
            try std.testing.expect(artifact.production_enabled);
            try std.testing.expect(artifact.runtime_default_enabled);
            const body = try cuda_renderer.renderFlashPrefillBodyAlloc(std.testing.allocator, plan);
            defer std.testing.allocator.free(body);
            try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, body));
        } else if (cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact)) |plan| {
            const plan_id = try cuda_renderer.splitkOnlineDecodePlanId(plan, std.testing.allocator);
            defer std.testing.allocator.free(plan_id);
            const metadata = try std.fmt.allocPrint(std.testing.allocator, "kernel_id={s} plan_id={s}", .{ plan.kernel_id, plan_id });
            defer std.testing.allocator.free(metadata);
            try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, metadata));
            const exported_symbol = try std.fmt.allocPrint(std.testing.allocator, "#define ANTFLY_SPLITK_ONLINE_KERNEL {s}\n", .{plan.kernel_id});
            defer std.testing.allocator.free(exported_symbol);
            try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, exported_symbol));
            try std.testing.expect(!artifact.production_enabled);
            try std.testing.expect(!artifact.runtime_default_enabled);
            const body = try cuda_renderer.renderSplitkOnlineDecodeBodyAlloc(std.testing.allocator, plan);
            defer std.testing.allocator.free(body);
            try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, body));
        } else {
            const plan = cudaAttentionRenderPlanForArtifact(artifact) orelse return error.MissingCudaAttentionRenderPlan;
            const plan_id = try cuda_renderer.attentionPlanId(plan, std.testing.allocator);
            defer std.testing.allocator.free(plan_id);
            const metadata = try std.fmt.allocPrint(std.testing.allocator, "kernel_id={s} plan_id={s}", .{ plan.kernel_id, plan_id });
            defer std.testing.allocator.free(metadata);
            try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, metadata));
            const body = try cuda_renderer.renderAttentionBodyAlloc(std.testing.allocator, plan);
            defer std.testing.allocator.free(body);
            try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, body));
        }
    }
    try std.testing.expectEqual(@as(usize, 12), runtime_wired_count);
    try std.testing.expect(!std.mem.containsAtLeast(u8, first, 1, first_decode_attention_1x_metal_kernel_id));
    for (cuda_renderer.attention_runtime_external_helpers) |helper| {
        try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, first, helper.source));
    }
}

test "quant kernel compiler renders runtime-wired dev CUDA matmul candidates deterministically" {
    const first = try renderCudaRuntimeDevMatmulRegion(std.testing.allocator);
    defer std.testing.allocator.free(first);
    const second = try renderCudaRuntimeDevMatmulRegion(std.testing.allocator);
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualStrings(first, second);

    var runtime_dev_count: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .cuda or artifact.production_enabled or !artifactRuntimeWired(artifact)) continue;
        runtime_dev_count += 1;
        const plan = cudaRenderPlanForArtifact(artifact) orelse return error.MissingCudaRenderPlan;
        const plan_id = try cuda_renderer.planId(plan, std.testing.allocator);
        defer std.testing.allocator.free(plan_id);
        const metadata = try std.fmt.allocPrint(std.testing.allocator, "kernel_id={s} plan_id={s}", .{ plan.kernel_id, plan_id });
        defer std.testing.allocator.free(metadata);
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, metadata));

        const body = try cuda_renderer.renderBodyAlloc(std.testing.allocator, plan);
        defer std.testing.allocator.free(body);
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, body));
        const declaration = try std.fmt.allocPrint(std.testing.allocator, "extern \"C\" __global__ void {s}(", .{plan.kernel_id});
        defer std.testing.allocator.free(declaration);
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, first, declaration));
    }
    try std.testing.expectEqual(@as(usize, 15), runtime_dev_count);
    try std.testing.expect(!std.mem.containsAtLeast(u8, first, 1, first_general_cuda_q4_0_mmv_kernel_id));
}

test "quant kernel compiler CUDA attention candidate stays in sync with the runtime bundle" {
    const bundle = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "src/ops/cuda/artifacts/inference_cuda_kernels.cu", std.testing.allocator, .limited(4 * 1024 * 1024));
    defer std.testing.allocator.free(bundle);
    const attention_marker = "// quant-kernel-codegen:begin generated CUDA attention kernels";
    const attention_region_begin = std.mem.indexOf(u8, bundle, attention_marker) orelse return error.MissingCudaAttentionRuntimeMarker;
    try validateCudaRuntimeAttentionExternalHelpers(bundle[0..attention_region_begin]);
    var runtime_cuda_attention_count: usize = 0;
    var standalone_cuda_attention_count: usize = 0;
    for (first_generated_attention_artifacts) |artifact| {
        if (artifact.backend != .cuda) continue;
        const kernel_id = if (cudaFlashPrefillRenderPlanForArtifact(artifact)) |plan|
            plan.kernel_id
        else if (cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact)) |plan|
            plan.kernel_id
        else if (cudaAttentionRenderPlanForArtifact(artifact)) |plan|
            plan.kernel_id
        else
            return error.MissingCudaAttentionRenderPlan;
        if (!cudaAttentionArtifactRuntimeWired(artifact)) {
            standalone_cuda_attention_count += 1;
            try std.testing.expect(!std.mem.containsAtLeast(u8, bundle, 1, kernel_id));
            continue;
        }
        runtime_cuda_attention_count += 1;
        if (cudaFlashPrefillRenderPlanForArtifact(artifact)) |plan| {
            const route_id = try cuda_renderer.flashPrefillPlanId(plan, std.testing.allocator);
            defer std.testing.allocator.free(route_id);
            const bundle_metadata = try std.fmt.allocPrint(std.testing.allocator, "kernel_id={s} plan_id={s}", .{ plan.kernel_id, route_id });
            defer std.testing.allocator.free(bundle_metadata);
            try std.testing.expect(std.mem.containsAtLeast(u8, bundle, 1, bundle_metadata));
            const body = try cuda_renderer.renderFlashPrefillBodyAlloc(std.testing.allocator, plan);
            defer std.testing.allocator.free(body);
            try std.testing.expect(std.mem.containsAtLeast(u8, bundle, 1, body));
        } else if (cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact)) |plan| {
            const route_id = try cuda_renderer.splitkOnlineDecodePlanId(plan, std.testing.allocator);
            defer std.testing.allocator.free(route_id);
            const bundle_metadata = try std.fmt.allocPrint(std.testing.allocator, "kernel_id={s} plan_id={s}", .{ plan.kernel_id, route_id });
            defer std.testing.allocator.free(bundle_metadata);
            try std.testing.expect(std.mem.containsAtLeast(u8, bundle, 1, bundle_metadata));
            const body = try cuda_renderer.renderSplitkOnlineDecodeBodyAlloc(std.testing.allocator, plan);
            defer std.testing.allocator.free(body);
            try std.testing.expect(std.mem.containsAtLeast(u8, bundle, 1, body));
        } else {
            const plan = cudaAttentionRenderPlanForArtifact(artifact) orelse return error.MissingCudaAttentionRenderPlan;
            const route_id = try cuda_renderer.attentionPlanId(plan, std.testing.allocator);
            defer std.testing.allocator.free(route_id);
            const bundle_metadata = try std.fmt.allocPrint(std.testing.allocator, "kernel_id={s} plan_id={s}", .{ plan.kernel_id, route_id });
            defer std.testing.allocator.free(bundle_metadata);
            try std.testing.expect(std.mem.containsAtLeast(u8, bundle, 1, bundle_metadata));
            const body = try cuda_renderer.renderAttentionBodyAlloc(std.testing.allocator, plan);
            defer std.testing.allocator.free(body);
            try std.testing.expect(std.mem.containsAtLeast(u8, bundle, 1, body));
        }
    }
    try std.testing.expectEqual(@as(usize, 12), runtime_cuda_attention_count);
    try std.testing.expectEqual(@as(usize, 0), standalone_cuda_attention_count);
}

fn benchmarkPtxArgRecognized(arg: []const u8) bool {
    const recognized = [_][]const u8{
        "--quant-compiler-generated-ptx",
        "--quant-compiler-q4-0-mmv-ptx",
        "--quant-compiler-q4-0-mm-ptx",
        "--quant-compiler-q4-0-pair-ptx",
        "--quant-compiler-q4-0-pair-q8-ptx",
        "--quant-compiler-q4-0-down-q8-ptx",
    };
    for (recognized) |candidate| {
        if (std.mem.eql(u8, arg, candidate)) return true;
    }
    return false;
}

fn benchmarkEvidenceFor(bench: BenchmarkCase, evidence_records: []const BenchmarkEvidence) ?BenchmarkEvidence {
    for (evidence_records) |evidence| {
        if (std.mem.eql(u8, evidence.kernel_id, bench.generated_kernel_id) and
            std.mem.eql(u8, evidence.generated_source_path, bench.generated_source_path) and
            evidence.generated_source_fingerprint == bench.generated_source_fingerprint and
            std.mem.eql(u8, evidence.generated_ptx_path, bench.generated_ptx_path) and
            std.mem.eql(u8, evidence.generated_ptx_command, bench.generated_ptx_command) and
            std.mem.eql(u8, evidence.benchmark_command, bench.benchmark_command) and
            std.mem.eql(u8, evidence.correctness_evidence_path, bench.correctness_evidence_path) and
            std.mem.eql(u8, evidence.benchmark_evidence_path, bench.benchmark_evidence_path) and
            std.mem.eql(u8, evidence.benchmark_mode, bench.benchmark_mode) and
            evidence.target_fingerprint == bench.target_fingerprint)
        {
            return evidence;
        }
    }
    return null;
}

fn metalRuntimeEvidenceFor(artifact: GeneratedMatmulArtifact, evidence_records: []const MetalRuntimeEvidence) ?MetalRuntimeEvidence {
    const expected_source_path = generatedMetalSourcePathForKernel(artifact.kernel_id) orelse return null;
    const expected_check_command = generatedMetalCheckCommandForKernel(artifact.kernel_id) orelse return null;
    for (evidence_records) |evidence| {
        if (std.mem.eql(u8, evidence.kernel_id, artifact.kernel_id) and
            std.mem.eql(u8, evidence.source_path, expected_source_path) and
            evidence.source_fingerprint == artifactSourceFingerprint(artifact) and
            std.mem.eql(u8, evidence.check_command, expected_check_command) and
            metalRuntimeEvidenceCommandMatchesArtifact(evidence, artifact) and
            std.mem.eql(u8, evidence.promotion_check_command, artifact.promotion_check_command))
        {
            return evidence;
        }
    }
    return null;
}

fn checkedInMetalEvidenceForKernel(kernel_id: []const u8) ?MetalRuntimeEvidence {
    for (first_metal_runtime_evidence) |evidence| {
        if (std.mem.eql(u8, evidence.kernel_id, kernel_id) and
            evidence.correctness_passed and
            evidence.source_fingerprint != 0)
        {
            return evidence;
        }
    }
    return null;
}

pub fn generatedMetalSourcePathForKernel(kernel_id: []const u8) ?[]const u8 {
    const artifact = generatedArtifactForKernel(.metal, kernel_id) orelse return null;
    return artifact.source_path;
}

pub fn generatedMetalCheckCommandForKernel(kernel_id: []const u8) ?[]const u8 {
    const artifact = generatedArtifactForKernel(.metal, kernel_id) orelse return null;
    return artifact.check_command;
}

fn metalRuntimeEvidenceCommandMatchesArtifact(evidence: MetalRuntimeEvidence, artifact: GeneratedMatmulArtifact) bool {
    if (evidence.production_enabled or evidence.promotion_ready) {
        return std.mem.eql(u8, evidence.runtime_evidence_command, artifact.promotion_evidence_command);
    }
    return std.mem.eql(u8, evidence.runtime_evidence_command, artifact.runtime_evidence_command);
}

fn benchmarkForArtifact(artifact: GeneratedMatmulArtifact) ?BenchmarkCase {
    for (first_benchmarks) |bench| {
        if (artifact.backend == bench.backend and
            artifact.format == bench.format and
            artifact.row_bucket == bench.row_bucket and
            artifact.epilogue == bench.epilogue and
            std.mem.eql(u8, artifact.source_path, bench.generated_source_path) and
            std.mem.eql(u8, artifact.kernel_id, bench.generated_kernel_id))
        {
            return bench;
        }
    }
    return null;
}

fn isCudaModulePath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, ".ptx") or std.mem.endsWith(u8, path, ".fatbin");
}

fn metalAirPathForKernel(kernel_id: []const u8) ?[]const u8 {
    const command = generatedMetalCheckCommandForKernel(kernel_id) orelse return null;
    return commandArgValue(command, "-o");
}

fn cudaEpilogueFor(epilogue: Epilogue) ?cuda_renderer.EpilogueKind {
    return switch (epilogue) {
        .none => .none,
        .bias => .bias,
        .bias_gelu => .bias_gelu,
        .pair => .pair,
        .triple => .triple,
        .relu => .relu,
        .gelu => .gelu,
        .add => .add,
        .argmax => .argmax,
        .pair_activation => .pair_activation,
        .gated_down => .gated_down,
    };
}

fn cudaProductionBaselineForArtifact(artifact: anytype, op: MatmulArtifactOp) []const u8 {
    const kind = artifact.cuda_kernel orelse
        return productionKernelId(.cuda, op.format, op.row_bucket, op.epilogue);
    return switch (kind) {
        .q4_0_pair_activation_f32_e2b_6144_exact,
        .q4_0_pair_activation_f32_e2b_12288_exact,
        => "termite_linear_q4_0_pair_activation_f32_tile4_w4",
        .q4_0_down_f32_e2b_6144_exact,
        .q4_0_down_f32_e2b_12288_exact,
        => "termite_linear_q4_0_f32_tile4",
        .q6_k_q8_1_argmax_k3840_tile8 => "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8+termite_argmax_reduce_rows_pairs_f32_w16",
        else => productionKernelId(.cuda, op.format, op.row_bucket, op.epilogue),
    };
}

/// Builds the canonical CUDA backend plan from the authoritative artifact
/// operation. The renderer descriptor is accepted only when every route and
/// promotion field agrees with the compiler registry.
pub fn cudaRenderPlanForArtifact(artifact: anytype) ?cuda_renderer.RenderPlan {
    if (artifact.backend != .cuda) return null;
    const kind = artifact.cuda_kernel orelse return null;
    const op = artifact.matmulOp() orelse return null;
    const epilogue = cudaEpilogueFor(op.epilogue) orelse return null;
    const dispatch = dispatchForRowBucket(op.row_bucket) orelse return null;
    const plan = cuda_renderer.planFor(kind);
    if (plan.route.format != op.format or
        plan.route.row_bucket != op.row_bucket or
        plan.route.epilogue != epilogue or
        plan.route.dispatch != dispatch or
        !std.mem.eql(u8, plan.kernel_id, artifact.kernel_id) or
        plan.production_enabled != artifact.production_enabled)
    {
        return null;
    }
    const baseline = cudaProductionBaselineForArtifact(artifact, op);
    if (!std.mem.eql(u8, plan.production_baseline, baseline)) return null;
    plan.validate() catch return null;
    return plan;
}

pub fn cudaAttentionRenderPlanForArtifact(artifact: anytype) ?cuda_renderer.AttentionRenderPlan {
    if (artifact.backend != .cuda) return null;
    const kind = artifact.cuda_attention_kernel orelse return null;
    const op = artifact.attentionOp() orelse return null;
    const plan = cuda_renderer.attentionPlanFor(kind);
    const storage_matches = switch (op.schedule.attention_storage) {
        .f32 => plan.lowering.query_storage == .f32,
        .f16 => plan.lowering.query_storage == .f16,
        .bf16 => plan.lowering.query_storage == .bf16,
        .paged_f16 => plan.lowering.query_storage == .paged_f16,
        .paged_f16_or_polar4 => plan.lowering.query_storage == .paged_f16_or_polar4,
        .paged_f16_or_f32 => plan.lowering.query_storage == .paged_f16_or_f32,
    };
    const key_storage_matches = switch (op.schedule.attention_key_storage) {
        .f32 => plan.lowering.key_storage == .f32,
        .f16 => plan.lowering.key_storage == .f16,
        .bf16 => plan.lowering.key_storage == .bf16,
        .paged_f16 => plan.lowering.key_storage == .paged_f16,
        .paged_f16_or_polar4 => plan.lowering.key_storage == .paged_f16_or_polar4,
        .paged_f16_or_f32 => plan.lowering.key_storage == .paged_f16_or_f32,
    };
    const value_storage_matches = switch (op.schedule.attention_value_storage) {
        .f32 => plan.lowering.value_storage == .f32,
        .f16 => plan.lowering.value_storage == .f16,
        .bf16 => plan.lowering.value_storage == .bf16,
        .paged_f16 => plan.lowering.value_storage == .paged_f16,
        .paged_f16_or_polar4 => plan.lowering.value_storage == .paged_f16_or_polar4,
        .paged_f16_or_f32 => plan.lowering.value_storage == .paged_f16_or_f32,
    };
    if (plan.lowering.kind != op.kind or
        plan.lowering.head_dim != op.head_dim or
        plan.serial_launch.threads_per_block != op.schedule.attention_serial_threads_per_threadgroup or
        plan.launch.threads_per_block != op.schedule.threads_per_threadgroup or
        plan.reduction_launch.threads_per_block != op.schedule.attention_stage2_threads_per_threadgroup or
        (if (plan.tiled64_launch) |launch| launch.threads_per_block else 0) != op.schedule.attention_tiled64_threads_per_threadgroup or
        plan.lowering.kv_splits != op.schedule.attention_kv_splits or
        plan.lowering.query_heads_per_kv_head != op.schedule.attention_query_heads_per_kv_head or
        plan.lowering.split_kv_min_tokens_default != op.schedule.attention_split_kv_min_tokens or
        plan.lowering.max_kv_tokens != op.schedule.attention_max_kv_tokens or
        (if (plan.tiled64_launch != null)
            (cuda_renderer.generatedAttentionScorePreworkTiled64MaxKvTokens(op.head_dim) orelse 0)
        else
            0) != op.schedule.attention_tiled64_max_kv_tokens or
        !storage_matches or !key_storage_matches or !value_storage_matches or
        !std.mem.eql(u8, plan.kernel_id, artifact.kernel_id) or
        plan.production_enabled != artifact.production_enabled)
    {
        return null;
    }
    plan.validate() catch return null;
    return plan;
}

pub fn cudaFlashPrefillRenderPlanForArtifact(artifact: anytype) ?cuda_renderer.FlashPrefillRenderPlan {
    if (artifact.backend != .cuda) return null;
    const kind = artifact.cuda_flash_prefill_kernel orelse return null;
    const op = artifact.attentionOp() orelse return null;
    const plan = cuda_renderer.flashPrefillPlanFor(kind);
    if (op.kind != .prefill_flash or
        op.head_dim != plan.lowering.head_dim or
        op.schedule.threads_per_threadgroup != plan.launch.threads_per_block or
        op.schedule.attention_query_heads_per_kv_head != plan.lowering.query_heads or
        plan.lowering.kv_heads != 1 or
        op.schedule.attention_query_tile != plan.lowering.query_tile or
        op.schedule.attention_key_tile != plan.lowering.key_tile or
        op.schedule.attention_page_size_tokens != plan.lowering.page_size_tokens or
        op.schedule.attention_dynamic_shared_memory_bytes != plan.launch.dynamic_shared_memory_bytes or
        op.schedule.attention_required_compute_major != plan.lowering.required_compute_major or
        op.schedule.attention_required_compute_minor != plan.lowering.required_compute_minor or
        op.schedule.attention_query_length_policy != plan.lowering.query_length_policy or
        op.schedule.attention_storage != .f32 or plan.lowering.query_storage != .f32 or
        op.schedule.attention_key_storage != .paged_f16 or plan.lowering.key_storage != .paged_f16 or
        op.schedule.attention_value_storage != .paged_f16 or plan.lowering.value_storage != .paged_f16 or
        plan.lowering.output_storage != .f32 or
        !std.mem.eql(u8, plan.kernel_id, artifact.kernel_id) or
        plan.production_enabled != artifact.production_enabled or
        plan.runtime_default_enabled != artifact.runtime_default_enabled)
    {
        return null;
    }
    plan.validate() catch return null;
    return plan;
}

pub fn cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact: anytype) ?cuda_renderer.SplitkOnlineDecodeRenderPlan {
    if (artifact.backend != .cuda) return null;
    const kind = artifact.cuda_splitk_online_decode_kernel orelse return null;
    const op = artifact.attentionOp() orelse return null;
    const plan = cuda_renderer.splitkOnlineDecodePlanFor(kind);
    if (op.kind != .decode_1x or
        op.head_dim != plan.lowering.head_dim or
        op.schedule.threads_per_threadgroup != plan.launch.threads_per_block or
        op.schedule.attention_kv_splits != plan.lowering.kv_splits or
        op.schedule.attention_query_heads_per_kv_head != plan.lowering.query_heads or
        op.schedule.attention_page_size_tokens != plan.lowering.page_size_tokens or
        op.schedule.attention_max_kv_tokens != plan.lowering.max_visible_tokens or
        op.schedule.attention_required_compute_major != plan.lowering.required_compute_major or
        op.schedule.attention_required_compute_minor != plan.lowering.required_compute_minor or
        op.schedule.attention_storage != .f32 or plan.lowering.query_storage != .f32 or
        op.schedule.attention_key_storage != .paged_f16 or plan.lowering.key_storage != .paged_f16 or
        op.schedule.attention_value_storage != .paged_f16 or plan.lowering.value_storage != .paged_f16 or
        plan.lowering.output_storage != .f32 or
        !std.mem.eql(u8, plan.kernel_id, artifact.kernel_id) or
        plan.production_enabled != artifact.production_enabled or
        plan.runtime_default_enabled != artifact.runtime_default_enabled)
    {
        return null;
    }
    plan.validate() catch return null;
    return plan;
}

fn cudaSourceForKind(kind: cuda_renderer.KernelKind) []const u8 {
    return switch (kind) {
        .q4_k_small_batch_bias_gelu => first_lazy_cuda_source,
        .q4_k_mmv => first_general_cuda_q4_k_mmv_source,
        .q4_0_mmv => first_general_cuda_q4_0_mmv_source,
        .q4_0_mm => first_general_cuda_q4_0_mm_source,
        .q4_0_pair_mmv => first_general_cuda_q4_0_pair_source,
        .q4_0_pair_activation_q8_1 => first_general_cuda_q4_0_pair_q8_source,
        .q4_0_pair_activation_q8_1_e2b_6144 => first_e2b_cuda_q4_0_pair_q8_6144_source,
        .q4_0_pair_activation_q8_1_e2b_12288 => first_e2b_cuda_q4_0_pair_q8_12288_source,
        .q4_0_pair_activation_ggml_q8_1_e2b_6144 => first_e2b_cuda_q4_0_pair_ggml_q8_1_6144_source,
        .q4_0_pair_activation_ggml_q8_1_e2b_12288 => first_e2b_cuda_q4_0_pair_ggml_q8_1_12288_source,
        .q4_0_down_q8_1 => first_general_cuda_q4_0_down_q8_source,
        .q4_0_down_q8_1_e2b_6144 => first_e2b_cuda_q4_0_down_q8_6144_source,
        .q4_0_down_q8_1_e2b_12288 => first_e2b_cuda_q4_0_down_q8_12288_source,
        .q4_0_down_ggml_q8_1_e2b_6144 => first_e2b_cuda_q4_0_down_ggml_q8_1_6144_source,
        .q4_0_down_ggml_q8_1_e2b_12288 => first_e2b_cuda_q4_0_down_ggml_q8_1_12288_source,
        .q4_0_pair_activation_f32_e2b_6144_exact => first_e2b_cuda_q4_0_pair_f32_6144_exact_source,
        .q4_0_pair_activation_f32_e2b_12288_exact => first_e2b_cuda_q4_0_pair_f32_12288_exact_source,
        .q4_0_down_f32_e2b_6144_exact => first_e2b_cuda_q4_0_down_f32_6144_exact_source,
        .q4_0_down_f32_e2b_12288_exact => first_e2b_cuda_q4_0_down_f32_12288_exact_source,
        .q4_0_q8_1_argmax_e2b_tile8 => first_e2b_cuda_q4_0_q8_1_argmax_source,
        .q6_k_q8_1_argmax_k2560_tile8 => first_cuda_q6_k_q8_1_argmax_k2560_source,
        .q6_k_q8_1_argmax_k3840_tile8 => first_cuda_q6_k_q8_1_argmax_k3840_source,
    };
}

fn cudaAttentionSourceForKind(kind: cuda_renderer.AttentionKernelKind) []const u8 {
    return switch (kind) {
        .gqa_decode_split_kv_hd256_f32 => first_decode_attention_1x_cuda_source,
        .gqa_decode_split_kv_hd512_f32 => first_decode_attention_1x_cuda_hd512_source,
        .gqa_decode_split2_kv_hd256_f32 => first_decode_attention_1x_cuda_split2_hd256_source,
        .gqa_decode_split2_kv_hd512_f32 => first_decode_attention_1x_cuda_split2_hd512_source,
        .gqa_decode_split4_kv_hd256_f32 => first_decode_attention_1x_cuda_split4_hd256_source,
        .gqa_decode_split4_kv_hd512_f32 => first_decode_attention_1x_cuda_split4_hd512_source,
        .gqa_decode_score_prework_hd256_f32 => first_decode_attention_1x_cuda_score_prework_hd256_source,
        .gqa_decode_score_prework_hd512_f32 => first_decode_attention_1x_cuda_score_prework_hd512_source,
    };
}

fn cudaFlashPrefillSourceForKind(kind: cuda_renderer.FlashPrefillKernelKind) []const u8 {
    return switch (kind) {
        .gqa_prefill_flash_sm89_hd256_swa512_f32 => first_prefill_flash_cuda_hd256_source,
        .gqa_prefill_flash_sm89_hd512_global_f32 => first_prefill_flash_cuda_hd512_source,
    };
}

fn cudaSplitkOnlineDecodeSourceForKind(kind: cuda_renderer.SplitkOnlineDecodeKernelKind) []const u8 {
    return switch (kind) {
        .gqa_decode_splitk_online_sm89_hd256_swa512_f16_f32 => first_decode_splitk_online_cuda_hd256_source,
        .gqa_decode_splitk_online_sm89_hd512_global_f16_f32 => first_decode_splitk_online_cuda_hd512_source,
    };
}

pub fn compileQuantKernelSource(request: QuantKernelCompileRequest) ?QuantKernelCompiledSource {
    const artifact = generatedArtifactForCandidate(request.backend, request.format, request.row_bucket, request.epilogue) orelse return null;
    return compileQuantKernelArtifactSource(artifact);
}

/// Compiles one registry artifact by identity. Multiple fixed-shape CUDA
/// candidates may share a logical matmul route while retaining distinct typed
/// render plans; route-based compilation continues to select the default plan.
pub fn compileQuantKernelArtifactSource(artifact: GeneratedMatmulArtifact) ?QuantKernelCompiledSource {
    const request = QuantKernelCompileRequest{
        .backend = artifact.backend,
        .format = artifact.format,
        .row_bucket = artifact.row_bucket,
        .epilogue = artifact.epilogue,
    };
    const spec = specFor(request.format) orelse return null;
    const ir = buildIr(request.format, request.row_bucket, request.epilogue) orelse return null;
    const source = generatedSourceForArtifact(artifact) orelse return null;
    const lowering = loweringForCompiledArtifact(artifact, ir.dispatch) orelse return null;
    const runtime_gate_env = artifactRuntimeGateEnv(artifact);
    const cuda_render_plan = if (request.backend == .cuda)
        cudaRenderPlanForArtifact(artifact)
    else
        null;
    if (request.backend == .cuda and cuda_render_plan == null) return null;
    const compiled = QuantKernelCompiledSource{
        .request = request,
        .spec = spec,
        .ir = ir,
        .lowering = lowering,
        .artifact = artifact,
        .source = source,
        .source_path = artifact.source_path,
        .check_command = artifact.check_command,
        .runtime_gate_env = runtime_gate_env,
        .production_enabled = artifact.production_enabled,
        .cuda_render_plan = cuda_render_plan,
    };
    if (!compiledSourceMatchesRoute(compiled)) return null;
    return compiled;
}

fn loweringForCompiledArtifact(
    artifact: GeneratedMatmulArtifact,
    dispatch: quant_matmul.DispatchKind,
) ?QuantKernelLowering {
    if (artifact.backend == .metal) {
        return registryLoweringFor(artifact.backend, artifact.format, artifact.row_bucket, artifact.epilogue, dispatch);
    }
    const schedule = cudaScheduleForArtifact(artifact) orelse return null;
    if (artifactHasPromotionEvidence(artifact)) {
        return .{
            .plan_id = planId(artifact.backend, artifact.format, artifact.row_bucket, artifact.epilogue, dispatch),
            .backend = artifact.backend,
            .format = artifact.format,
            .row_bucket = artifact.row_bucket,
            .epilogue = artifact.epilogue,
            .schedule = schedule,
            .production_route = .generated_production,
            .candidate_route = .unsupported,
            .production_kernel_id = artifact.kernel_id,
            .fallback_reason = .none,
            .kernel_id = "",
            .candidate_source_path = "",
        };
    }
    return .{
        .plan_id = planId(artifact.backend, artifact.format, artifact.row_bucket, artifact.epilogue, dispatch),
        .backend = artifact.backend,
        .format = artifact.format,
        .row_bucket = artifact.row_bucket,
        .epilogue = artifact.epilogue,
        .schedule = schedule,
        .production_route = .handwritten_production,
        .candidate_route = .generated_dev_candidate,
        .production_kernel_id = cudaProductionBaselineForArtifact(artifact, artifact.matmulOp().?),
        .fallback_reason = if (artifactRuntimeWired(artifact)) .generated_runtime_not_wired else .generated_artifact_missing,
        .kernel_id = artifact.kernel_id,
        .candidate_source_path = artifact.source_path,
    };
}

pub fn compiledSourceMatchesRoute(compiled: QuantKernelCompiledSource) bool {
    if (compiled.artifact.backend != compiled.request.backend or
        compiled.artifact.format != compiled.request.format or
        compiled.artifact.row_bucket != compiled.request.row_bucket or
        compiled.artifact.epilogue != compiled.request.epilogue)
    {
        return false;
    }
    const expected_source = generatedSourceForArtifact(compiled.artifact) orelse return false;
    if (!std.mem.eql(u8, compiled.source, expected_source) or
        !std.mem.eql(u8, compiled.source_path, compiled.artifact.source_path) or
        !std.mem.eql(u8, compiled.check_command, compiled.artifact.check_command) or
        compiled.production_enabled != compiled.artifact.production_enabled)
    {
        return false;
    }
    const expected_gate = artifactRuntimeGateEnv(compiled.artifact);
    if (!optionalCStringEquals(compiled.runtime_gate_env, expected_gate)) return false;
    if (compiled.request.backend == .metal) {
        if (compiled.cuda_render_plan != null) return false;
    } else {
        const expected_plan = cudaRenderPlanForArtifact(compiled.artifact) orelse return false;
        const actual_plan = compiled.cuda_render_plan orelse return false;
        if (!std.meta.eql(expected_plan, actual_plan)) return false;
    }
    if (compiled.spec.format != compiled.request.format or
        compiled.ir.format != compiled.request.format or
        compiled.ir.row_bucket != compiled.request.row_bucket or
        compiled.ir.epilogue != compiled.request.epilogue)
    {
        return false;
    }
    if (compiled.lowering.backend != compiled.request.backend or
        compiled.lowering.format != compiled.request.format or
        compiled.lowering.row_bucket != compiled.request.row_bucket or
        compiled.lowering.epilogue != compiled.request.epilogue or
        compiled.lowering.schedule.dispatch != compiled.ir.dispatch)
    {
        return false;
    }

    const promotion_ready = artifactHasPromotionEvidence(compiled.artifact);
    if (promotion_ready) {
        return compiled.lowering.production_route == .generated_production and
            compiled.lowering.candidate_route == .unsupported and
            compiled.lowering.fallback_reason == .none and
            std.mem.eql(u8, compiled.lowering.production_kernel_id, compiled.artifact.kernel_id) and
            compiled.lowering.kernel_id.len == 0 and
            compiled.lowering.candidate_source_path.len == 0;
    }

    return compiled.lowering.production_route == .handwritten_production and
        compiled.lowering.candidate_route == .generated_dev_candidate and
        std.mem.eql(u8, compiled.lowering.kernel_id, compiled.artifact.kernel_id) and
        std.mem.eql(u8, compiled.lowering.candidate_source_path, compiled.artifact.source_path);
}

fn optionalCStringEquals(a: ?[*:0]const u8, b: ?[*:0]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, std.mem.span(a.?), std.mem.span(b.?));
}

pub fn compileMetalKernelSource(
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
) ?QuantKernelCompiledSource {
    return compileQuantKernelSource(.{
        .backend = .metal,
        .format = format,
        .row_bucket = row_bucket,
        .epilogue = epilogue,
    });
}

pub fn emitCompiledSource(allocator: std.mem.Allocator, compiled: QuantKernelCompiledSource) !EmittedCompiledSource {
    if (compiled.cuda_render_plan) |plan| {
        return .{
            .data = try cuda_renderer.renderKernel(allocator, plan),
            .owned = true,
        };
    }
    return .{ .data = compiled.source };
}

pub fn emitRuntimeArtifactSource(
    allocator: std.mem.Allocator,
    artifact: GeneratedArtifact,
) !RuntimeArtifactSource {
    if (artifact.backend == .cuda) {
        const source = switch (artifact.opKind()) {
            .small_batch_matmul => blk: {
                const plan = cudaRenderPlanForArtifact(artifact) orelse
                    return error.MissingCudaRuntimeRenderPlan;
                break :blk try cuda_renderer.renderKernel(allocator, plan);
            },
            .attention => blk: {
                if (cudaFlashPrefillRenderPlanForArtifact(artifact)) |plan| {
                    break :blk try cuda_renderer.renderFlashPrefillKernel(allocator, plan);
                }
                if (cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact)) |plan| {
                    break :blk try cuda_renderer.renderSplitkOnlineDecodeKernel(allocator, plan);
                }
                const plan = cudaAttentionRenderPlanForArtifact(artifact) orelse
                    return error.MissingCudaRuntimeRenderPlan;
                break :blk try cuda_renderer.renderAttentionKernel(allocator, plan);
            },
            .microkernel => return error.UnsupportedCudaRuntimeMicrokernel,
        };
        return .{ .artifact = artifact, .data = source, .owned = true };
    }
    return .{
        .artifact = artifact,
        .data = generatedSourceForArtifact(artifact) orelse
            return error.MissingMetalRuntimeSource,
    };
}

/// Renders one bounded Metal schedule candidate for an existing typed matmul
/// artifact. Candidate source is deliberately ephemeral: checked-in artifacts
/// remain the production/AOT source of truth, while pre-serving autotuning can
/// compile several honest schedules without generating files or invoking an
/// external compiler toolchain.
pub fn emitMetalScheduleCandidateSource(
    allocator: std.mem.Allocator,
    artifact: GeneratedArtifact,
    schedule: KernelSchedule,
) !RuntimeArtifactSource {
    return emitMetalScheduleCandidateSourceKind(allocator, artifact, schedule, false);
}

/// Emits the homogeneous Q/K/V companion for an already-qualified exact
/// schedule. This is deliberately not a second tuning surface: it uses the
/// same decoder, shape, and launch tile as the qualified single projection.
pub fn emitMetalQkvScheduleCandidateSource(
    allocator: std.mem.Allocator,
    artifact: GeneratedArtifact,
    schedule: KernelSchedule,
) !RuntimeArtifactSource {
    return emitMetalScheduleCandidateSourceKind(allocator, artifact, schedule, true);
}

fn emitMetalScheduleCandidateSourceKind(
    allocator: std.mem.Allocator,
    artifact: GeneratedArtifact,
    schedule: KernelSchedule,
    qkv: bool,
) !RuntimeArtifactSource {
    if (artifact.backend != .metal) return error.UnsupportedMetalScheduleCandidate;
    const op = artifact.matmulOp() orelse return error.UnsupportedMetalScheduleCandidate;
    if (op.row_bucket != .rows_2_8) return error.UnsupportedMetalScheduleCandidate;
    if (qkv and (op.epilogue != .none or (op.format != .q4_k and op.format != .q6_k))) {
        return error.UnsupportedMetalQkvScheduleCandidate;
    }
    const decoder = metal_renderer.decoderFor(op.format) orelse
        return error.MissingMetalFormatDecoder;
    const kernel_id = if (qkv)
        try std.fmt.allocPrint(allocator, "{s}_qkv", .{artifact.kernel_id})
    else
        try allocator.dupe(u8, artifact.kernel_id);
    defer allocator.free(kernel_id);
    const kernel = if (qkv)
        try metal_renderer.renderQkvKernel(allocator, kernel_id, decoder, schedule)
    else
        try metal_renderer.renderKernel(allocator, kernel_id, decoder, schedule, op.epilogue);
    defer allocator.free(kernel);

    var source: std.ArrayListUnmanaged(u8) = .empty;
    errdefer source.deinit(allocator);
    try source.appendSlice(allocator, metal_generated_source_license_header);
    try source.appendSlice(allocator, "\n\n// Runtime Metal schedule candidate.\n");
    const metadata = try std.fmt.allocPrint(
        allocator,
        "// kernel_id={s}\n// production_baseline=metal_handwritten_quant_matmul\n// production_enabled=false\n// fusion={s}\n// schedule=threads:{d},rows:{d},cols:{d},reduction:{s}\n\n#include <metal_stdlib>\nusing namespace metal;\n\n",
        .{
            kernel_id,
            if (qkv) "qkv" else "none",
            schedule.threads_per_threadgroup,
            schedule.rows_per_threadgroup,
            schedule.cols_per_threadgroup,
            @tagName(schedule.reduction),
        },
    );
    defer allocator.free(metadata);
    try source.appendSlice(allocator, metadata);
    try source.appendSlice(allocator, kernel);
    return .{
        .artifact = artifact,
        .data = try source.toOwnedSlice(allocator),
        .owned = true,
    };
}

pub fn compiledSourceHeaderMatchesPlan(allocator: std.mem.Allocator, compiled: QuantKernelCompiledSource) !bool {
    return sourceHeaderMatchesCompiledPlan(allocator, compiled, compiled.source);
}

pub fn compiledSourceHeaderMatchesSource(
    allocator: std.mem.Allocator,
    compiled: QuantKernelCompiledSource,
    source: []const u8,
) !bool {
    return sourceHeaderMatchesCompiledPlan(allocator, compiled, source);
}

fn sourceHeaderMatchesCompiledPlan(
    allocator: std.mem.Allocator,
    compiled: QuantKernelCompiledSource,
    source: []const u8,
) !bool {
    const plan_name = try planIdName(allocator, compiled.lowering.plan_id);
    defer allocator.free(plan_name);

    const plan_metadata = try std.fmt.allocPrint(allocator, "plan_id={s}", .{plan_name});
    defer allocator.free(plan_metadata);
    if (!std.mem.containsAtLeast(u8, source, 1, plan_metadata)) return false;

    const kernel_metadata = try std.fmt.allocPrint(allocator, "kernel_id={s}", .{compiled.artifact.kernel_id});
    defer allocator.free(kernel_metadata);
    if (!std.mem.containsAtLeast(u8, source, 1, kernel_metadata)) return false;

    const baseline_id = if (compiled.artifact.backend == .cuda)
        cudaProductionBaselineForArtifact(compiled.artifact, compiled.artifact.matmulOp().?)
    else
        productionKernelId(
            compiled.artifact.backend,
            compiled.artifact.format,
            compiled.artifact.row_bucket,
            compiled.artifact.epilogue,
        );
    const baseline_metadata = try std.fmt.allocPrint(allocator, "production_baseline={s}", .{baseline_id});
    defer allocator.free(baseline_metadata);
    if (!std.mem.containsAtLeast(u8, source, 1, baseline_metadata)) return false;

    const enabled_metadata = if (compiled.artifact.production_enabled)
        "production_enabled=true"
    else
        "production_enabled=false";
    if (!std.mem.containsAtLeast(u8, source, 1, enabled_metadata)) return false;

    if (!std.mem.containsAtLeast(u8, source, 1, compiled.artifact.kernel_id)) return false;
    return true;
}

pub fn generatedSourceForArtifact(artifact: anytype) ?[]const u8 {
    if (artifact.backend == .cuda) {
        if (cudaFlashPrefillRenderPlanForArtifact(artifact)) |plan| {
            return cudaFlashPrefillSourceForKind(plan.kind);
        }
        if (cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact)) |plan| {
            return cudaSplitkOnlineDecodeSourceForKind(plan.kind);
        }
        if (cudaAttentionRenderPlanForArtifact(artifact)) |plan| {
            return cudaAttentionSourceForKind(plan.kind);
        }
        const plan = cudaRenderPlanForArtifact(artifact) orelse return null;
        return cudaSourceForKind(plan.kind);
    }
    if (artifact.backend == .metal and artifact.opKind() == .microkernel and std.mem.eql(u8, artifact.kernel_id, first_rms_norm_metal_kernel_id)) {
        return first_rms_norm_metal_source;
    }
    if (artifact.backend == .metal and artifact.opKind() == .attention and std.mem.eql(u8, artifact.kernel_id, first_decode_attention_1x_metal_kernel_id)) {
        return first_decode_attention_1x_metal_source;
    }
    if (artifact.backend == .metal and artifact.opKind() == .attention and std.mem.eql(u8, artifact.kernel_id, first_prefill_flash_metal_kernel_id)) {
        return first_prefill_flash_metal_source;
    }
    if (artifact.backend == .metal and artifact.opKind() == .attention and std.mem.eql(u8, artifact.kernel_id, first_prefill_flash_hd512_metal_kernel_id)) {
        return first_prefill_flash_hd512_metal_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_lazy_metal_kernel_id)) {
        return first_lazy_metal_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q4_0_kernel_id)) {
        return first_general_metal_q4_0_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q4_1_kernel_id)) {
        return first_general_metal_q4_1_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q5_0_kernel_id)) {
        return first_general_metal_q5_0_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q5_1_kernel_id)) {
        return first_general_metal_q5_1_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q4_kernel_id)) {
        return first_general_metal_q4_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q4_bias_kernel_id)) {
        return first_general_metal_q4_bias_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q8_kernel_id)) {
        return first_general_metal_q8_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q8_bias_kernel_id)) {
        return first_general_metal_q8_bias_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q8_bias_gelu_kernel_id)) {
        return first_general_metal_q8_bias_gelu_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q8_relu_kernel_id)) {
        return first_general_metal_q8_relu_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q2_kernel_id)) {
        return first_general_metal_q2_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q2_bias_kernel_id)) {
        return first_general_metal_q2_bias_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q2_bias_gelu_kernel_id)) {
        return first_general_metal_q2_bias_gelu_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q3_kernel_id)) {
        return first_general_metal_q3_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q3_bias_kernel_id)) {
        return first_general_metal_q3_bias_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q3_bias_gelu_kernel_id)) {
        return first_general_metal_q3_bias_gelu_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q8_1_kernel_id)) {
        return first_general_metal_q8_1_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q8_k_kernel_id)) {
        return first_general_metal_q8_k_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q5_kernel_id)) {
        return first_general_metal_q5_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q5_bias_kernel_id)) {
        return first_general_metal_q5_bias_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q5_bias_gelu_kernel_id)) {
        return first_general_metal_q5_bias_gelu_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q6_kernel_id)) {
        return first_general_metal_q6_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q6_bias_kernel_id)) {
        return first_general_metal_q6_bias_source;
    }
    if (artifact.backend == .metal and std.mem.eql(u8, artifact.kernel_id, first_general_metal_q6_bias_gelu_kernel_id)) {
        return first_general_metal_q6_bias_gelu_source;
    }
    return null;
}

fn productionKernelId(
    backend: Backend,
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
) []const u8 {
    if (backend == .cuda and format == first_lazy_benchmark.format and row_bucket == first_lazy_benchmark.row_bucket and epilogue == first_lazy_benchmark.epilogue) {
        return first_lazy_benchmark.handwritten_baseline;
    }
    if (backend == .cuda) {
        if (format == .q4_0 and epilogue == .none) {
            return switch (row_bucket) {
                .rows_1 => "termite_linear_q4_0_f32_tile4",
                .rows_9_64 => "termite_linear_q4_0_f32",
                else => "cuda_handwritten_quant_matmul",
            };
        }
        if (format == .q4_0 and epilogue == .pair) {
            return switch (row_bucket) {
                .rows_1 => "termite_linear_q4_0_pair_nobias_f32_tile4_w4",
                else => "cuda_handwritten_quant_matmul",
            };
        }
        if (format == .q4_0 and epilogue == .argmax) {
            return switch (row_bucket) {
                .rows_1 => "termite_linear_q4_0_q8_1_f32_tile4+termite_argmax_last_row_f32",
                else => "cuda_handwritten_quant_matmul",
            };
        }
        if (format == .q4_0 and epilogue == .pair_activation) {
            return switch (row_bucket) {
                .rows_1 => "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn",
                else => "cuda_handwritten_quant_matmul",
            };
        }
        if (format == .q4_0 and epilogue == .gated_down) {
            return switch (row_bucket) {
                .rows_1 => "termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down",
                else => "cuda_handwritten_quant_matmul",
            };
        }
        if (format == .q4_k and epilogue == .none) return "termite_linear_q4_k_f32_tile4";
        if (format == .q4_k and epilogue == .bias) {
            return switch (row_bucket) {
                .rows_1 => "termite_linear_q4_k_bias_f32_tile4",
                .rows_2_8 => "termite_linear_q4_k_bias_f32_tile4_r2",
                else => "cuda_handwritten_quant_matmul",
            };
        }
        if (format == .q6_k and epilogue == .none) return "termite_linear_q6_k_f32_tile4";
        if (format == .q6_k and epilogue == .argmax and row_bucket == .rows_1) {
            return "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b+termite_argmax_reduce_rows_pairs_f32_w16";
        }
    }
    return switch (backend) {
        .cuda => "cuda_handwritten_quant_matmul",
        .metal => "metal_handwritten_quant_matmul",
    };
}

pub fn referenceMatmulBiasGelu(
    allocator: std.mem.Allocator,
    format: quant_matmul.Format,
    raw_weight: []const u8,
    input: []const f32,
    bias: []const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output: []f32,
) !void {
    return referenceMatmulEpilogue(allocator, format, raw_weight, input, bias, rows, in_dim, out_dim, .bias_gelu, output);
}

pub fn referenceMatmulNoBias(
    allocator: std.mem.Allocator,
    format: quant_matmul.Format,
    raw_weight: []const u8,
    input: []const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output: []f32,
) !void {
    return referenceMatmulEpilogue(allocator, format, raw_weight, input, null, rows, in_dim, out_dim, .none, output);
}

pub fn referenceMatmulEpilogue(
    allocator: std.mem.Allocator,
    format: quant_matmul.Format,
    raw_weight: []const u8,
    input: []const f32,
    bias: ?[]const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    epilogue: Epilogue,
    output: []f32,
) !void {
    if (input.len != rows * in_dim or output.len != rows * out_dim) return error.InvalidShape;
    switch (epilogue) {
        .none, .bias, .bias_gelu, .relu, .gelu => {},
        else => return error.UnsupportedEpilogue,
    }
    if ((epilogue == .bias or epilogue == .bias_gelu) and (bias == null or bias.?.len != out_dim)) return error.InvalidShape;
    const tensor_type = tensorTypeForFormat(format) orelse return error.UnsupportedFormat;
    const dense_weight = try allocator.alloc(f32, in_dim * out_dim);
    defer allocator.free(dense_weight);
    try quant_codec.dequantizeToFloat32(tensor_type, raw_weight, dense_weight);

    for (0..rows) |r| {
        for (0..out_dim) |o| {
            var acc: f32 = 0.0;
            for (0..in_dim) |i| {
                acc += input[r * in_dim + i] * dense_weight[o * in_dim + i];
            }
            output[r * out_dim + o] = switch (epilogue) {
                .none => acc,
                .bias => acc + bias.?[o],
                .bias_gelu => gelu(acc + bias.?[o]),
                .relu => @max(acc, 0.0),
                .gelu => gelu(acc),
                else => return error.UnsupportedEpilogue,
            };
        }
    }
}

pub fn generatedMathMatmulBiasGelu(
    format: quant_matmul.Format,
    raw_weight: []const u8,
    input: []const f32,
    bias: []const f32,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    output: []f32,
) !void {
    if (format != .q4_k) return error.UnsupportedFormat;
    if (input.len != rows * in_dim or bias.len != out_dim or output.len != rows * out_dim) return error.InvalidShape;
    if (in_dim % q4_k_spec.block_values != 0) return error.InvalidShape;

    const block_count = in_dim / q4_k_spec.block_values;
    if (raw_weight.len != out_dim * block_count * q4_k_spec.block_bytes) return error.InvalidShape;

    for (0..rows) |r| {
        for (0..out_dim) |o| {
            var acc: f32 = bias[o];
            for (0..block_count) |block_idx| {
                const block_off = (o * block_count + block_idx) * q4_k_spec.block_bytes;
                const block = raw_weight[block_off .. block_off + q4_k_spec.block_bytes];
                const input_off = r * in_dim + block_idx * q4_k_spec.block_values;
                for (0..q4_k_spec.block_values) |lane| {
                    acc += input[input_off + lane] * generatedMathQ4KDequantLane(block, lane);
                }
            }
            output[r * out_dim + o] = gelu(acc);
        }
    }
}

const ScaleMin = struct {
    scale: f32,
    min: f32,
};

fn generatedMathQ4KDequantLane(block: []const u8, lane: usize) f32 {
    const d = quant_codec.decodeFp16Le(block[0], block[1]);
    const dmin = quant_codec.decodeFp16Le(block[2], block[3]);
    const scales = block[4..16];
    const qs = block[16..144];

    const sub = lane >> 5;
    const q_index = (sub >> 1) * 32 + (lane & 31);
    const packed_byte = qs[q_index];
    const q = if ((sub & 1) == 0) packed_byte & 0x0F else packed_byte >> 4;
    const scale_min = generatedMathQ4KScaleMin(scales, sub);
    return d * scale_min.scale * @as(f32, @floatFromInt(q)) - dmin * scale_min.min;
}

fn generatedMathQ4KScaleMin(scales: []const u8, sub: usize) ScaleMin {
    if (sub < 4) return .{
        .scale = @floatFromInt(scales[sub] & 63),
        .min = @floatFromInt(scales[sub + 4] & 63),
    };

    return .{
        .scale = @floatFromInt((scales[sub + 4] & 0x0F) | ((scales[sub - 4] >> 6) << 4)),
        .min = @floatFromInt((scales[sub + 4] >> 4) | ((scales[sub] >> 6) << 4)),
    };
}

fn scheduleFor(row_bucket: quant_matmul.RowBucket, dispatch: quant_matmul.DispatchKind) QuantKernelSchedule {
    return switch (row_bucket) {
        .rows_1 => .{
            .dispatch = dispatch,
            .row_bucket = row_bucket,
            .tile_rows = 1,
            .tile_cols = 4,
            .vector_width = 4,
            .threads_per_block = 256,
            .shared_memory_bytes = 0,
            .register_pressure_hint = 4,
            .tensor_core_eligible = false,
        },
        .rows_2_8 => .{
            .dispatch = dispatch,
            .row_bucket = row_bucket,
            .tile_rows = 2,
            .tile_cols = 4,
            .vector_width = 4,
            .threads_per_block = 256,
            .shared_memory_bytes = 0,
            .register_pressure_hint = 6,
            .tensor_core_eligible = false,
        },
        .rows_9_64, .rows_65_plus => .{
            .dispatch = dispatch,
            .row_bucket = row_bucket,
            .tile_rows = 8,
            .tile_cols = 4,
            .vector_width = 4,
            .threads_per_block = 256,
            .shared_memory_bytes = 0,
            .register_pressure_hint = 8,
            .tensor_core_eligible = false,
        },
        .rows_0 => .{
            .dispatch = .scalar,
            .row_bucket = row_bucket,
            .tile_rows = 0,
            .tile_cols = 0,
            .vector_width = 0,
            .threads_per_block = 0,
            .shared_memory_bytes = 0,
            .register_pressure_hint = 0,
            .tensor_core_eligible = false,
        },
    };
}

fn candidateScheduleFor(lowering: QuantKernelLowering) QuantKernelSchedule {
    if (lowering.candidate_route != .generated_dev_candidate) return emptyCandidateSchedule(lowering.row_bucket, lowering.schedule.dispatch);
    if (lowering.backend == .cuda) {
        const artifact = generatedArtifactForKernel(lowering.backend, lowering.kernel_id) orelse
            return emptyCandidateSchedule(lowering.row_bucket, lowering.schedule.dispatch);
        return cudaScheduleForArtifact(artifact) orelse emptyCandidateSchedule(lowering.row_bucket, lowering.schedule.dispatch);
    }
    const tile_cols: usize = if (lowering.backend == .metal)
        metalGeneratedColsPerThreadgroup(lowering.format, lowering.row_bucket, lowering.epilogue)
    else
        1;
    const threads_per_block: usize = if (lowering.backend == .metal)
        metalGeneratedThreadsPerThreadgroup(lowering.format, lowering.row_bucket, lowering.epilogue)
    else
        128;
    return .{
        .dispatch = lowering.schedule.dispatch,
        .row_bucket = lowering.row_bucket,
        .tile_rows = 1,
        .tile_cols = tile_cols,
        .vector_width = 1,
        .threads_per_block = threads_per_block,
        .shared_memory_bytes = 0,
        .register_pressure_hint = 6,
        .tensor_core_eligible = false,
    };
}

pub fn metalGeneratedColsPerThreadgroup(
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
) usize {
    const schedule = metalRouteScheduleFor(format, row_bucket, epilogue) orelse return 1;
    return schedule.cols_per_threadgroup;
}

pub fn metalGeneratedThreadsPerThreadgroup(
    format: quant_matmul.Format,
    row_bucket: quant_matmul.RowBucket,
    epilogue: Epilogue,
) usize {
    const schedule = metalRouteScheduleFor(format, row_bucket, epilogue) orelse return 128;
    return schedule.threads_per_threadgroup;
}

fn emptyCandidateSchedule(row_bucket: quant_matmul.RowBucket, dispatch: quant_matmul.DispatchKind) QuantKernelSchedule {
    return .{
        .dispatch = dispatch,
        .row_bucket = row_bucket,
        .tile_rows = 0,
        .tile_cols = 0,
        .vector_width = 0,
        .threads_per_block = 0,
        .shared_memory_bytes = 0,
        .register_pressure_hint = 0,
        .tensor_core_eligible = false,
    };
}

fn sourceFingerprint(comptime source: []const u8) u64 {
    @setEvalBranchQuota(131072);
    return std.hash.Wyhash.hash(0, source);
}

fn dispatchForRowBucket(row_bucket: quant_matmul.RowBucket) ?quant_matmul.DispatchKind {
    return switch (row_bucket) {
        .rows_0 => null,
        .rows_1 => .mmv,
        .rows_2_8 => .small_batch,
        .rows_9_64, .rows_65_plus => .mm,
    };
}

fn irOpsForEpilogue(epilogue: Epilogue) []const IROp {
    return switch (epilogue) {
        .none, .pair => &ir_ops_basic,
        .bias => &ir_ops_bias,
        .bias_gelu => &ir_ops_bias_gelu,
        .argmax => &ir_ops_argmax,
        else => &ir_ops_basic,
    };
}

fn tensorTypeForFormat(format: quant_matmul.Format) ?tensor_types.TensorType {
    return switch (format) {
        .q1_0 => .{ .known = .Q1_0 },
        .i2_s => .{ .known = .I2_S },
        .i8_s => .{ .known = .I8_S },
        .q2_k => .{ .known = .Q2_K },
        .q3_k => .{ .known = .Q3_K },
        .q4_0 => .{ .known = .Q4_0 },
        .q4_1 => .{ .known = .Q4_1 },
        .q5_0 => .{ .known = .Q5_0 },
        .q5_1 => .{ .known = .Q5_1 },
        .q4_k => .{ .known = .Q4_K },
        .q5_k => .{ .known = .Q5_K },
        .q6_k => .{ .known = .Q6_K },
        .q8_0 => .{ .known = .Q8_0 },
        .q8_1 => .{ .known = .Q8_1 },
        .q8_k => .{ .known = .Q8_K },
        .tq1_0 => .{ .known = .TQ1_0 },
        .tq2_0 => .{ .known = .TQ2_0 },
        .iq2_xxs => .{ .known = .IQ2_XXS },
        .iq2_xs => .{ .known = .IQ2_XS },
        .iq2_s => .{ .known = .IQ2_S },
        .iq3_xxs => .{ .known = .IQ3_XXS },
        .iq3_s => .{ .known = .IQ3_S },
        .iq1_s => .{ .known = .IQ1_S },
        .iq1_m => .{ .known = .IQ1_M },
        .iq4_nl => .{ .known = .IQ4_NL },
        .iq4_xs => .{ .known = .IQ4_XS },
        .mxfp4 => .{ .known = .MXFP4 },
        .nvfp4 => .{ .known = .NVFP4 },
        else => null,
    };
}

fn toleranceFor(format: quant_matmul.Format, epilogue: Epilogue) f32 {
    _ = format;
    _ = epilogue;
    return 0.0001;
}

fn referenceSupportedForEpilogue(epilogue: Epilogue) bool {
    return switch (epilogue) {
        .none, .bias, .bias_gelu, .relu, .gelu => true,
        else => false,
    };
}

fn blockValuesForFormat(comptime format: quant_matmul.Format) usize {
    const tensor_type = tensorTypeForFormat(format) orelse @compileError("missing GGUF tensor type for quant kernel format");
    return tensor_types.valuesPerBlock(tensor_type) orelse @compileError("missing GGUF block value count");
}

fn blockBytesForFormat(comptime format: quant_matmul.Format) usize {
    const tensor_type = tensorTypeForFormat(format) orelse @compileError("missing GGUF tensor type for quant kernel format");
    return tensor_types.bytesPerBlock(tensor_type) orelse @compileError("missing GGUF block byte count");
}

fn gelu(x: f32) f32 {
    if (!std.math.isFinite(x)) return 0.0;
    const inner = 0.7978845608028654 * (x + 0.044715 * x * x * x);
    if (inner > 10.0) return x;
    if (inner < -10.0) return 0.0;
    const y = 0.5 * x * (1.0 + std.math.tanh(inner));
    return if (std.math.isFinite(y)) y else 0.0;
}

test "quant kernel GELU saturates non-finite inputs" {
    try std.testing.expectEqual(@as(f32, 0.0), gelu(-std.math.inf(f32)));
    try std.testing.expectEqual(@as(f32, 0.0), gelu(std.math.nan(f32)));
    try std.testing.expectEqual(@as(f32, 1024.0), gelu(1024.0));
    try std.testing.expectEqual(@as(f32, 0.0), gelu(-1024.0));
}

fn contains(comptime T: type, haystack: []const T, needle: T) bool {
    for (haystack) |item| {
        if (item == needle) return true;
    }
    return false;
}

fn quantizeForFormat(allocator: std.mem.Allocator, format: quant_matmul.Format, dense: []const f32) ![]u8 {
    return switch (format) {
        .q1_0 => quant_codec.quantizeQ1_0FromF32(allocator, dense),
        .i2_s => quantizeI2SFromF32(allocator, dense),
        .i8_s => quantizeI8SFromF32(allocator, dense),
        .q2_k => quant_codec.quantizeQ2_KFromF32(allocator, dense),
        .q3_k => quant_codec.quantizeQ3_KFromF32(allocator, dense),
        .q4_0 => quant_codec.quantizeQ4_0FromF32(allocator, dense),
        .q4_1 => quant_codec.quantizeQ4_1FromF32(allocator, dense),
        .q5_0 => quant_codec.quantizeQ5_0FromF32(allocator, dense),
        .q5_1 => quant_codec.quantizeQ5_1FromF32(allocator, dense),
        .q4_k => quant_codec.quantizeQ4_KFromF32(allocator, dense),
        .q5_k => quant_codec.quantizeQ5_KFromF32(allocator, dense),
        .q6_k => quant_codec.quantizeQ6_KFromF32(allocator, dense),
        .q8_0 => quant_codec.quantizeQ8_0FromF32(allocator, dense),
        .q8_1 => quant_codec.quantizeQ8_1FromF32(allocator, dense),
        .q8_k => quant_codec.quantizeQ8_KFromF32(allocator, dense),
        .tq1_0,
        .tq2_0,
        .iq2_xxs,
        .iq2_xs,
        .iq2_s,
        .iq3_xxs,
        .iq3_s,
        .iq1_s,
        .iq1_m,
        => quantizeOpaqueValidBlocks(allocator, format, dense),
        .iq4_nl => quantizeIQ4NLFromF32(allocator, dense),
        .iq4_xs => quantizeIQ4XSFromF32(allocator, dense),
        .mxfp4 => quantizeMXFP4FromF32(allocator, dense),
        .nvfp4 => quantizeNVFP4FromF32(allocator, dense),
        else => error.UnsupportedFormat,
    };
}

fn quantizeI2SFromF32(allocator: std.mem.Allocator, dense: []const f32) ![]u8 {
    const values_per_block = 128;
    const bytes_per_block = 32;
    if (dense.len == 0 or dense.len % values_per_block != 0) return error.InvalidShape;
    const blocks = dense.len / values_per_block;
    const raw = try allocator.alloc(u8, blocks * bytes_per_block);
    errdefer allocator.free(raw);
    for (0..blocks) |block| {
        const base = block * values_per_block;
        for (0..32) |group| {
            var packed_byte: u8 = 0;
            for (0..4) |lane| {
                const value = dense[base + lane * 32 + group];
                const code: u8 = if (value > 0.5) 2 else if (value < -0.5) 0 else 1;
                const shift: u3 = @intCast((3 - lane) * 2);
                packed_byte |= code << shift;
            }
            raw[block * bytes_per_block + group] = packed_byte;
        }
    }
    return raw;
}

fn quantizeI8SFromF32(allocator: std.mem.Allocator, dense: []const f32) ![]u8 {
    if (dense.len == 0) return error.InvalidShape;
    const raw = try allocator.alloc(u8, dense.len);
    errdefer allocator.free(raw);
    for (dense, raw) |value, *dst| {
        const clamped = std.math.clamp(@round(value), -128.0, 127.0);
        const q: i8 = @intFromFloat(clamped);
        dst.* = @bitCast(q);
    }
    return raw;
}

fn quantizeOpaqueValidBlocks(allocator: std.mem.Allocator, format: quant_matmul.Format, dense: []const f32) ![]u8 {
    const tensor_type = tensorTypeForFormat(format) orelse return error.UnsupportedFormat;
    const values_per_block: usize = @intCast(tensor_types.valuesPerBlock(tensor_type) orelse return error.UnsupportedFormat);
    const bytes_per_block: usize = @intCast(tensor_types.bytesPerBlock(tensor_type) orelse return error.UnsupportedFormat);
    if (dense.len == 0 or dense.len % values_per_block != 0) return error.InvalidShape;
    const blocks = dense.len / values_per_block;
    const raw = try allocator.alloc(u8, blocks * bytes_per_block);
    errdefer allocator.free(raw);
    @memset(raw, 0);
    for (0..blocks) |block_index| {
        const block = raw[block_index * bytes_per_block ..][0..bytes_per_block];
        switch (format) {
            .tq1_0 => {
                block[52] = 0x00;
                block[53] = 0x3C;
            },
            .tq2_0 => {
                block[64] = 0x00;
                block[65] = 0x3C;
            },
            .iq2_xxs,
            .iq2_xs,
            .iq2_s,
            .iq3_xxs,
            .iq3_s,
            .iq1_s,
            => {
                block[0] = 0x00;
                block[1] = 0x3C;
            },
            .iq1_m => {},
            else => return error.UnsupportedFormat,
        }
    }
    return raw;
}

fn quantizeIQ4NLFromF32(allocator: std.mem.Allocator, dense: []const f32) ![]u8 {
    return quantizeNibbleTableFromF32(allocator, dense, 32, 18, 2, &quant_codec.iq4_nl_values, .iq4_nl);
}

fn quantizeMXFP4FromF32(allocator: std.mem.Allocator, dense: []const f32) ![]u8 {
    return quantizeNibbleTableFromF32(allocator, dense, 32, 17, 1, &quant_codec.mxfp4_values, .mxfp4);
}

fn quantizeIQ4XSFromF32(allocator: std.mem.Allocator, dense: []const f32) ![]u8 {
    const values_per_block = 256;
    const bytes_per_block = 136;
    if (dense.len == 0 or dense.len % values_per_block != 0) return error.InvalidShape;
    const blocks = dense.len / values_per_block;
    const raw = try allocator.alloc(u8, blocks * bytes_per_block);
    errdefer allocator.free(raw);
    for (0..blocks) |block| {
        const dst = raw[block * bytes_per_block ..][0..bytes_per_block];
        @memset(dst, 0);
        dst[0] = 0x00;
        dst[1] = 0x3C;
        std.mem.writeInt(u16, dst[2..4], 0xAAAA, .little);
        @memset(dst[4..8], 0x11);
        const src = dense[block * values_per_block ..][0..values_per_block];
        for (0..8) |group| {
            packNibbles(src[group * 32 ..][0..32], dst[8 + group * 16 ..][0..16], &quant_codec.iq4_nl_values);
        }
    }
    return raw;
}

fn quantizeNVFP4FromF32(allocator: std.mem.Allocator, dense: []const f32) ![]u8 {
    const values_per_block = 64;
    const bytes_per_block = 36;
    if (dense.len == 0 or dense.len % values_per_block != 0) return error.InvalidShape;
    const blocks = dense.len / values_per_block;
    const raw = try allocator.alloc(u8, blocks * bytes_per_block);
    errdefer allocator.free(raw);
    for (0..blocks) |block| {
        const dst = raw[block * bytes_per_block ..][0..bytes_per_block];
        @memset(dst[0..4], 0x40);
        const src = dense[block * values_per_block ..][0..values_per_block];
        for (0..4) |sub| {
            packNibbles(src[sub * 16 ..][0..16], dst[4 + sub * 8 ..][0..8], &quant_codec.mxfp4_values);
        }
    }
    return raw;
}

fn quantizeNibbleTableFromF32(
    allocator: std.mem.Allocator,
    dense: []const f32,
    values_per_block: usize,
    bytes_per_block: usize,
    qs_offset: usize,
    table: []const i8,
    format: quant_matmul.Format,
) ![]u8 {
    if (dense.len == 0 or dense.len % values_per_block != 0) return error.InvalidShape;
    const blocks = dense.len / values_per_block;
    const raw = try allocator.alloc(u8, blocks * bytes_per_block);
    errdefer allocator.free(raw);
    for (0..blocks) |block| {
        const dst = raw[block * bytes_per_block ..][0..bytes_per_block];
        @memset(dst, 0);
        switch (format) {
            .iq4_nl => {
                dst[0] = 0x00;
                dst[1] = 0x3C;
            },
            .mxfp4 => dst[0] = 128,
            else => return error.UnsupportedFormat,
        }
        packNibbles(dense[block * values_per_block ..][0..values_per_block], dst[qs_offset..][0 .. values_per_block / 2], table);
    }
    return raw;
}

fn packNibbles(values: []const f32, dst: []u8, table: []const i8) void {
    for (dst, 0..) |*byte, i| {
        const lo = nearestTableIndex(table, values[i]);
        const hi = nearestTableIndex(table, values[i + dst.len]);
        byte.* = lo | (hi << 4);
    }
}

fn nearestTableIndex(table: []const i8, value: f32) u8 {
    var best_index: u8 = 0;
    var best_diff = std.math.inf(f32);
    for (table, 0..) |candidate, i| {
        const diff = @abs(value - @as(f32, @floatFromInt(candidate)));
        if (diff < best_diff) {
            best_diff = diff;
            best_index = @intCast(i);
        }
    }
    return best_index;
}

test "quant kernel compiler descriptors mirror packed format metadata" {
    for (descriptor_formats) |format| {
        const spec = specFor(format).?;
        const desc = quant_matmul.packedFormatDescriptor(format);
        const tensor_type = tensorTypeForFormat(format).?;
        try std.testing.expectEqual(desc.values_per_block, spec.block_values);
        try std.testing.expectEqual(desc.bytes_per_block, spec.block_bytes);
        try std.testing.expectEqual(@as(usize, @intCast(tensor_types.valuesPerBlock(tensor_type).?)), spec.block_values);
        try std.testing.expectEqual(@as(usize, @intCast(tensor_types.bytesPerBlock(tensor_type).?)), spec.block_bytes);
        try std.testing.expectEqual(contains(Epilogue, spec.supported_epilogues, .bias_gelu), spec.supportsEpilogue(.bias_gelu));
        try std.testing.expectEqual(contains(Backend, spec.supported_backends, .cuda), spec.supportsBackend(.cuda));
        try std.testing.expectEqual(contains(Backend, spec.supported_backends, .metal), spec.supportsBackend(.metal));

        var offset: usize = 0;
        for (spec.block_fields) |field| {
            try std.testing.expectEqual(offset, field.offset);
            offset += field.bytes;
        }
        try std.testing.expectEqual(spec.block_bytes, offset);
    }

    try std.testing.expectEqualStrings("qs", specFor(.q1_0).?.block_fields[1].name);
    try std.testing.expectEqualStrings("qs", specFor(.i2_s).?.block_fields[0].name);
    try std.testing.expectEqualStrings("q", specFor(.i8_s).?.block_fields[0].name);
    try std.testing.expectEqualStrings("dmin", specFor(.q2_k).?.block_fields[2].name);
    try std.testing.expectEqualStrings("hmask", specFor(.q3_k).?.block_fields[0].name);
    try std.testing.expectEqualStrings("qs", specFor(.q4_0).?.block_fields[1].name);
    try std.testing.expectEqualStrings("m", specFor(.q4_1).?.block_fields[1].name);
    try std.testing.expectEqualStrings("qh", specFor(.q5_0).?.block_fields[1].name);
    try std.testing.expectEqualStrings("m", specFor(.q5_1).?.block_fields[1].name);
    try std.testing.expectEqualStrings("qs", specFor(.q4_k).?.block_fields[3].name);
    try std.testing.expectEqualStrings("qh", specFor(.q5_k).?.block_fields[3].name);
    try std.testing.expectEqualStrings("scales", specFor(.q6_k).?.block_fields[2].name);
    try std.testing.expectEqualStrings("d", specFor(.q8_0).?.block_fields[0].name);
    try std.testing.expectEqualStrings("sum", specFor(.q8_1).?.block_fields[1].name);
    try std.testing.expectEqualStrings("bsums", specFor(.q8_k).?.block_fields[2].name);
    try std.testing.expectEqualStrings("qh", specFor(.tq1_0).?.block_fields[1].name);
    try std.testing.expectEqualStrings("d", specFor(.tq2_0).?.block_fields[1].name);
    try std.testing.expectEqualStrings("qs", specFor(.iq2_xxs).?.block_fields[1].name);
    try std.testing.expectEqualStrings("scales", specFor(.iq2_xs).?.block_fields[2].name);
    try std.testing.expectEqualStrings("qh", specFor(.iq2_s).?.block_fields[2].name);
    try std.testing.expectEqualStrings("qs", specFor(.iq3_xxs).?.block_fields[1].name);
    try std.testing.expectEqualStrings("signs", specFor(.iq3_s).?.block_fields[3].name);
    try std.testing.expectEqualStrings("qh", specFor(.iq1_s).?.block_fields[2].name);
    try std.testing.expectEqualStrings("scales", specFor(.iq1_m).?.block_fields[2].name);
    try std.testing.expectEqualStrings("qs", specFor(.iq4_nl).?.block_fields[1].name);
    try std.testing.expectEqualStrings("scales_l", specFor(.iq4_xs).?.block_fields[2].name);
    try std.testing.expectEqualStrings("d", specFor(.mxfp4).?.block_fields[0].name);
    try std.testing.expectEqualStrings("scales", specFor(.nvfp4).?.block_fields[0].name);
}

test "quant kernel compiler classifies every codec quant format" {
    try std.testing.expectEqual(@as(usize, 28), codec_format_coverage.len);
    try std.testing.expectEqual(codec_format_coverage.len, descriptor_formats.len);

    for (codec_format_coverage, 0..) |entry, index| {
        const tensor_type: tensor_types.TensorType = .{ .known = entry.tensor_type };
        try std.testing.expectEqual(entry.format, descriptor_formats[index]);
        try std.testing.expectEqual(@as(?quant_matmul.Format, entry.format), backend_contracts.quantFormatFromGgufTensorType(tensor_type));
        try std.testing.expect(tensor_types.valuesPerBlock(tensor_type) != null);
        try std.testing.expect(tensor_types.bytesPerBlock(tensor_type) != null);

        for (codec_format_coverage[0..index]) |previous| {
            try std.testing.expect(previous.tensor_type != entry.tensor_type);
            try std.testing.expect(previous.format != entry.format);
        }

        try std.testing.expect(specFor(entry.format) != null);
    }
}

fn expectManifestArrayCount(manifest: []const u8, count_field: []const u8, array_field: []const u8, expected: usize) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, manifest, .{});
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidManifestJson;
    const count_value = parsed.value.object.get(count_field) orelse return error.InvalidManifestJson;
    const array_value = parsed.value.object.get(array_field) orelse return error.InvalidManifestJson;
    if (count_value != .integer or array_value != .array) return error.InvalidManifestJson;

    try std.testing.expectEqual(@as(i64, @intCast(expected)), count_value.integer);
    try std.testing.expectEqual(expected, array_value.array.items.len);
}

fn expectManifestNestedInteger(manifest: []const u8, object_field: []const u8, count_field: []const u8, expected: usize) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, manifest, .{});
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidManifestJson;
    const object_value = parsed.value.object.get(object_field) orelse return error.InvalidManifestJson;
    if (object_value != .object) return error.InvalidManifestJson;
    const count_value = object_value.object.get(count_field) orelse return error.InvalidManifestJson;
    if (count_value != .integer) return error.InvalidManifestJson;

    try std.testing.expectEqual(@as(i64, @intCast(expected)), count_value.integer);
}

test "quant kernel compiler spec manifest serializes descriptors" {
    const manifest = try specManifestJson(std.testing.allocator);
    defer std.testing.allocator.free(manifest);

    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "antfly.quant_kernel_specs.v1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"format_count\": 28"));
    try expectManifestArrayCount(manifest, "format_count", "specs", descriptor_formats.len);
    try std.testing.expectEqual(descriptor_formats.len, std.mem.count(u8, manifest, "\"format\": "));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"format\": \"q4_k\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"reference_tensor_type\": \"Q4_K\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"block_values\": 256"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"block_bytes\": 144"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"name\": \"qs\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"offset\": 16"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "load block_q4_K {d,dmin,scales,qs}"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"supported_epilogues\": ["));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"bias_gelu\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"supported_backends\": ["));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"cuda\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"accumulator_dtype\": \"f32\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"output_dtype\": \"f32\""));
}

test "quant kernel compiler artifact manifest serializes generated candidates" {
    const manifest = try artifactManifestJson(std.testing.allocator);
    defer std.testing.allocator.free(manifest);

    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_artifact_manifest_schema));
    const artifact_count = try std.fmt.allocPrint(std.testing.allocator, "\"artifact_count\": {d}", .{first_generated_matmul_artifacts.len});
    defer std.testing.allocator.free(artifact_count);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, artifact_count));
    const registry_artifact_count = try std.fmt.allocPrint(std.testing.allocator, "\"registry_artifact_count\": {d}", .{first_generated_artifacts.len});
    defer std.testing.allocator.free(registry_artifact_count);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, registry_artifact_count));
    try expectManifestArrayCount(manifest, "registry_artifact_count", "registry_artifacts", first_generated_artifacts.len);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"op_kind\": \"small_batch_matmul\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"op_kind\": \"microkernel\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"op_kind\": \"attention\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"kind\": \"rms_norm\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"kind\": \"decode_1x\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"kind\": \"prefill_flash\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"cuda_kernel\": \"q4_0_pair_activation_q8_1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"grid\": \"flattened_rows_by_output_blocks\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"threads_per_block\": 640"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"output_cols_per_block\": 32"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"static_shared_memory_bytes\": 768"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"dynamic_shared_memory_bytes\": 0"));
    const metal_evidence_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"checked_in_metal_evidence_count\": {d}", .{first_metal_runtime_evidence_count});
    defer std.testing.allocator.free(metal_evidence_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, metal_evidence_count_field));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_legacy_unattested_evidence_count\": 7"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_legacy_unattested_evidence_is_release_blocker\": true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_future_promotions_require_attested_provenance\": true"));
    try std.testing.expectEqual(first_metal_runtime_evidence_count, std.mem.count(u8, manifest, "\"provenance_status\": \"legacy_unattested\""));
    const blocker_evidence_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_promotion_blocker_evidence_count\": {d}", .{first_metal_promotion_blocker_evidence_count});
    defer std.testing.allocator.free(blocker_evidence_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, blocker_evidence_count_field));
    const blocker_path_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_promotion_blocker_evidence_path_count\": {d}", .{metalPromotionBlockerEvidencePathCount()});
    defer std.testing.allocator.free(blocker_path_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, blocker_path_count_field));
    const blocker_case_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_promotion_blocker_evidence_expected_case_count\": {d}", .{first_metal_promotion_blocker_evidence_expected_case_count});
    defer std.testing.allocator.free(blocker_case_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, blocker_case_count_field));
    const blocker_route_ready_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_promotion_blocker_evidence_expected_route_ready_count\": {d}", .{first_metal_promotion_blocker_evidence_expected_route_ready_count});
    defer std.testing.allocator.free(blocker_route_ready_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, blocker_route_ready_count_field));
    try std.testing.expectEqual(first_metal_promotion_blocker_evidence_expected_case_count, first_metal_promotion_blocker_evidence_expected_route_ready_count);
    const blocker_check_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_promotion_blocker_check_command_count\": {d}", .{metalPromotionBlockerEvidencePathCount()});
    defer std.testing.allocator.free(blocker_check_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, blocker_check_count_field));
    const blocker_skipped_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_promotion_blocker_skipped_no_path_count\": {d}", .{metalPromotionBlockerSkippedNoPathCount()});
    defer std.testing.allocator.free(blocker_skipped_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, blocker_skipped_count_field));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_promotion_blocker_cleared_requires_checked_in_evidence\": true"));
    try std.testing.expectEqual(
        metalPromotionBlockerSkippedNoPathCount(),
        metalPromotionBlockerEvidenceCount(metal_blocker_unsupported_handwritten) + metalPromotionBlockerEvidenceCount(metal_blocker_runtime_route_only),
    );
    const blocker_speedup_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_promotion_blocker_speedup_gate_missing_count\": {d}", .{metalPromotionBlockerEvidenceCount(metal_blocker_speedup_gate_missing)});
    defer std.testing.allocator.free(blocker_speedup_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, blocker_speedup_count_field));
    const blocker_unstable_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_promotion_blocker_unstable_benchmark_timing_count\": {d}", .{metalPromotionBlockerEvidenceCount(metal_blocker_unstable_benchmark_timing)});
    defer std.testing.allocator.free(blocker_unstable_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, blocker_unstable_count_field));
    const blocker_runtime_route_only_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_promotion_blocker_runtime_route_only_count\": {d}", .{metalPromotionBlockerEvidenceCount(metal_blocker_runtime_route_only)});
    defer std.testing.allocator.free(blocker_runtime_route_only_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, blocker_runtime_route_only_count_field));
    const blocker_unsupported_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_promotion_blocker_unsupported_handwritten_count\": {d}", .{metalPromotionBlockerEvidenceCount(metal_blocker_unsupported_handwritten)});
    defer std.testing.allocator.free(blocker_unsupported_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, blocker_unsupported_count_field));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_unsupported_handwritten_baseline_blocks_promotion\": true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_unsupported_handwritten_baseline_uses_runtime_route_all_evidence\": true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_unsupported_handwritten_baseline_has_promotion_evidence_path\": false"));
    const route_all_case_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_runtime_route_all_expected_case_count\": {d}", .{first_metal_runtime_route_all_expected_case_count});
    defer std.testing.allocator.free(route_all_case_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, route_all_case_count_field));
    const route_all_ready_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_runtime_route_all_expected_route_ready_count\": {d}", .{first_metal_runtime_route_all_expected_route_ready_count});
    defer std.testing.allocator.free(route_all_ready_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, route_all_ready_count_field));
    try std.testing.expectEqual(first_metal_runtime_route_all_expected_case_count, first_metal_runtime_route_all_expected_route_ready_count);
    const route_all_provider_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_runtime_route_all_expected_provider_route_count\": {d}", .{first_metal_runtime_route_all_expected_provider_route_count});
    defer std.testing.allocator.free(route_all_provider_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, route_all_provider_count_field));
    try std.testing.expect(first_metal_runtime_route_all_expected_case_count > first_metal_runtime_route_all_expected_provider_route_count);
    const production_regression_kernel_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_production_regression_expected_kernel_count\": {d}", .{metalProductionRegressionExpectedKernelCount()});
    defer std.testing.allocator.free(production_regression_kernel_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, production_regression_kernel_count_field));
    const production_regression_case_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_production_regression_expected_case_count\": {d}", .{metalProductionRegressionExpectedCaseCount()});
    defer std.testing.allocator.free(production_regression_case_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, production_regression_case_count_field));
    const production_regression_ready_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_production_regression_expected_route_ready_count\": {d}", .{first_metal_production_regression_expected_route_ready_count});
    defer std.testing.allocator.free(production_regression_ready_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, production_regression_ready_count_field));
    try std.testing.expectEqual(first_metal_production_regression_expected_case_count, first_metal_production_regression_expected_route_ready_count);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_production_regression_route_ready_is_hard_gate\": true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_production_regression_missing_provider_route_is_hard_gate\": true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_production_regression_speedup_gate_missing_is_hard_gate\": true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_production_regression_unstable_benchmark_timing_is_hard_gate\": false"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_promotion_warmup_repeat_runs\": 2"));
    try std.testing.expectEqual(first_metal_runtime_evidence_count, metalProductionRegressionExpectedKernelCount());
    try expectManifestArrayCount(manifest, "artifact_count", "artifacts", first_generated_matmul_artifacts.len);
    try expectManifestArrayCount(manifest, "checked_in_metal_evidence_count", "metal_evidence_records", first_metal_runtime_evidence.len);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_blocker_strict_check_command\": \"" ++ first_metal_blocker_strict_check_command ++ "\""));
    var runtime_evidence_count: usize = 0;
    var runtime_route_evidence_count: usize = 0;
    var promotion_evidence_count: usize = 0;
    var promotion_check_count: usize = 0;
    var metal_evidence_count: usize = 0;
    var route_only_no_promotion_policy_count: usize = 0;
    var speedup_policy_count: usize = 0;
    var promoted_speedup_policy_count: usize = 0;
    var production_disabled_policy_count: usize = 0;
    var cuda_policy_count: usize = 0;
    var dev_only_candidate_count: usize = 0;
    var promotion_ready_false_count: usize = 0;
    var promotion_blocker_none_count: usize = 0;
    var promotion_blocker_production_disabled_count: usize = 0;
    var promotion_blocker_awaiting_cuda_count: usize = 0;
    var runtime_default_enabled_count: usize = 0;
    var qualified_runtime_opt_in_count: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.runtime_evidence_command.len != 0) runtime_evidence_count += 1;
        if (artifact.backend == .metal and artifactRuntimeWired(artifact) and !artifactHasPromotionEvidence(artifact)) runtime_route_evidence_count += 1;
        if (artifact.promotion_evidence_command.len != 0) promotion_evidence_count += 1;
        if (artifact.promotion_check_command.len != 0) promotion_check_count += 1;
        if (artifact.backend == .metal and artifact.runtime_evidence_command.len != 0) metal_evidence_count += 1;
        const promotion_policy = artifactPromotionPolicy(artifact);
        if (std.mem.eql(u8, promotion_policy, "route_evidence_only_no_promotion")) route_only_no_promotion_policy_count += 1;
        if (std.mem.eql(u8, promotion_policy, "speedup_vs_handwritten")) speedup_policy_count += 1;
        if (std.mem.eql(u8, promotion_policy, "promoted_speedup_vs_handwritten")) promoted_speedup_policy_count += 1;
        if (std.mem.eql(u8, promotion_policy, "production_disabled")) production_disabled_policy_count += 1;
        if (std.mem.eql(u8, promotion_policy, "driver_artifact_policy")) cuda_policy_count += 1;
        if (std.mem.eql(u8, artifactCandidateStatus(artifact), "dev_only_candidate")) dev_only_candidate_count += 1;
        if (!artifactHasPromotionEvidence(artifact)) promotion_ready_false_count += 1;
        const promotion_blocker = artifactPromotionBlocker(artifact);
        if (std.mem.eql(u8, promotion_blocker, "none")) promotion_blocker_none_count += 1;
        if (std.mem.eql(u8, promotion_blocker, "production_disabled")) promotion_blocker_production_disabled_count += 1;
        if (std.mem.eql(u8, promotion_blocker, "awaiting_cuda_promotion_evidence")) promotion_blocker_awaiting_cuda_count += 1;
        if (artifact.runtime_default_enabled) runtime_default_enabled_count += 1;
        if (artifact.production_enabled and !artifact.runtime_default_enabled) qualified_runtime_opt_in_count += 1;
    }
    var registry_runtime_default_enabled_count: usize = 0;
    for (first_generated_artifacts) |artifact| {
        if (artifact.runtime_default_enabled) registry_runtime_default_enabled_count += 1;
    }
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"backend\": \"cuda\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"backend\": \"metal\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_lazy_benchmark.generated_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_lazy_benchmark.generated_ptx_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_lazy_benchmark.benchmark_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_lazy_benchmark_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, first_generated_matmul_artifacts.len, "\"generated_source_fingerprint\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_lazy_metal_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_lazy_metal_check_command));
    try std.testing.expectEqual(first_generated_artifacts.len + first_generated_matmul_artifacts.len + first_metal_runtime_evidence.len, std.mem.count(u8, manifest, "\"source_path\":"));
    try std.testing.expectEqual(@as(usize, 0), runtime_default_enabled_count);
    try std.testing.expectEqual(@as(usize, 12), qualified_runtime_opt_in_count);
    try std.testing.expectEqual(
        registry_runtime_default_enabled_count + runtime_default_enabled_count,
        std.mem.count(u8, manifest, "\"runtime_default_enabled\": true"),
    );
    try std.testing.expectEqual(
        first_generated_artifacts.len + first_generated_matmul_artifacts.len - registry_runtime_default_enabled_count - runtime_default_enabled_count,
        std.mem.count(u8, manifest, "\"runtime_default_enabled\": false"),
    );
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, manifest, "\"artifact_source_path\":"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, manifest, "\"generated_check_command\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"source_path\": \"" ++ first_general_metal_q5_1_source_path ++ "\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"check_command\": \"" ++ first_general_metal_q5_1_check_command ++ "\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"source_path\": \"" ++ first_lazy_benchmark.generated_source_path ++ "\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q4_0_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q4_0_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q4_1_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q4_1_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_0_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_0_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_1_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_1_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q4_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q4_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_bias_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_bias_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_bias_gelu_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_bias_gelu_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_relu_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_relu_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q2_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q2_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q2_bias_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q2_bias_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q2_bias_gelu_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q2_bias_gelu_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q3_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q3_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q3_bias_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q3_bias_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q3_bias_gelu_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q3_bias_gelu_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_1_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_1_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_k_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_k_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_bias_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_bias_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_bias_gelu_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_bias_gelu_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q6_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q6_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q6_bias_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q6_bias_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q6_bias_gelu_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q6_bias_gelu_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, runtime_evidence_count, "\"runtime_evidence_command\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, runtime_route_evidence_count, "\"runtime_route_evidence_command\": \"zig build quant-kernel-metal-runtime-check"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, runtime_route_evidence_count, "--runtime-route-kernel"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, promotion_evidence_count, "\"promotion_evidence_command\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, promotion_check_count, "\"promotion_check_command\":"));
    const blocked_unsupported_handwritten_count = metalPromotionBlockerEvidenceCount("unsupported_handwritten_baseline");
    try std.testing.expectEqual(first_generated_matmul_artifacts.len, std.mem.count(u8, manifest, "\"promotion_policy\":"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, route_only_no_promotion_policy_count, "\"promotion_policy\": \"route_evidence_only_no_promotion\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, speedup_policy_count, "\"promotion_policy\": \"speedup_vs_handwritten\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, promoted_speedup_policy_count, "\"promotion_policy\": \"promoted_speedup_vs_handwritten\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, production_disabled_policy_count, "\"promotion_policy\": \"production_disabled\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, cuda_policy_count, "\"promotion_policy\": \"driver_artifact_policy\""));
    try std.testing.expectEqual(blocked_unsupported_handwritten_count, route_only_no_promotion_policy_count);
    try std.testing.expectEqual(first_metal_runtime_evidence_count, promoted_speedup_policy_count);
    try std.testing.expect(route_only_no_promotion_policy_count > 0);
    try std.testing.expect(speedup_policy_count > 0);
    try std.testing.expect(promoted_speedup_policy_count > 0);
    try std.testing.expect(cuda_policy_count > 0);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, metal_evidence_count, first_metal_runtime_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_local_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_runtime_route_all_build_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_runtime_route_all_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_runtime_route_all_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "--require-runtime-route-all"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_production_regression_build_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_production_regression_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_production_regression_evidence_path));
    try std.testing.expectEqual(first_metal_runtime_evidence_count, std.mem.count(u8, manifest, "\"production_regression_checked\": true"));
    try std.testing.expectEqual(first_generated_matmul_artifacts.len - first_metal_runtime_evidence_count, std.mem.count(u8, manifest, "\"production_regression_checked\": false"));
    try std.testing.expectEqual(first_metal_runtime_evidence_count + 1, std.mem.count(u8, manifest, first_metal_production_regression_build_command));
    try std.testing.expectEqual(first_generated_matmul_artifacts.len - first_metal_runtime_evidence_count, std.mem.count(u8, manifest, "\"production_regression_command\": \"\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, metal_evidence_count, first_metal_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, metal_evidence_count, first_metal_promotion_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, metal_evidence_count, "--require-kernel"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_evidence_records\": ["));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q4_0_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q4_1_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_0_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_1_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_bias_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_bias_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_bias_gelu_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_bias_gelu_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_relu_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q2_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q2_bias_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q2_bias_gelu_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q3_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q3_bias_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q3_bias_gelu_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q4_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q4_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_1_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_k_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q8_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_bias_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q5_bias_gelu_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q6_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q6_bias_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q6_bias_gelu_promotion_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q6_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_general_metal_q6_bias_gelu_source_path));
    const blocked_metal_promotion_count = first_metal_promotion_blocker_evidence_count;
    try std.testing.expectEqual(dev_only_candidate_count, std.mem.count(u8, manifest, "\"candidate_status\": \"dev_only_candidate\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, first_metal_runtime_evidence_count, "\"candidate_status\": \"promoted\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, blocked_metal_promotion_count, "\"candidate_status\": \"blocked_by_evidence\""));
    try std.testing.expectEqual(promotion_ready_false_count, std.mem.count(u8, manifest, "\"promotion_ready\": false"));
    const runtime_wired_artifacts = first_metal_runtime_route_all_expected_case_count / 2;
    var cuda_runtime_wired_count: usize = 0;
    var cuda_runtime_wired_dev_count: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend == .cuda and artifactRuntimeWired(artifact)) {
            cuda_runtime_wired_count += 1;
            if (!artifact.production_enabled) cuda_runtime_wired_dev_count += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 20), cuda_runtime_wired_count);
    try std.testing.expectEqual(@as(usize, 15), cuda_runtime_wired_dev_count);
    const awaiting_metal_promotion_count = runtime_wired_artifacts - first_metal_runtime_evidence_count - blocked_metal_promotion_count;
    const blocked_speedup_count = metalPromotionBlockerEvidenceCount("speedup_gate_missing");
    const blocked_evidence_path_count = metalPromotionBlockerEvidencePathCount();
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, awaiting_metal_promotion_count, "\"promotion_blocker\": \"awaiting_metal_promotion_evidence\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, blocked_speedup_count, "\"promotion_blocker\": \"speedup_gate_missing\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, blocked_unsupported_handwritten_count, "\"promotion_blocker\": \"unsupported_handwritten_baseline\""));
    try std.testing.expectEqual(cuda_runtime_wired_dev_count, promotion_blocker_awaiting_cuda_count);
    try std.testing.expectEqual(promotion_blocker_awaiting_cuda_count, std.mem.count(u8, manifest, "\"promotion_blocker\": \"awaiting_cuda_promotion_evidence\""));
    try std.testing.expectEqual(promotion_blocker_none_count, std.mem.count(u8, manifest, "\"promotion_blocker\": \"none\""));
    try std.testing.expectEqual(promotion_blocker_production_disabled_count, std.mem.count(u8, manifest, "\"promotion_blocker\": \"production_disabled\""));
    try std.testing.expectEqual(first_generated_matmul_artifacts.len, std.mem.count(u8, manifest, "\"promotion_blocker_evidence_path\":"));
    try std.testing.expectEqual(blocked_evidence_path_count, std.mem.count(u8, manifest, "\"promotion_blocker_evidence_path\": \"/private/tmp/antfly-quant-metal-"));
    try std.testing.expectEqual(first_generated_matmul_artifacts.len - blocked_evidence_path_count, std.mem.count(u8, manifest, "\"promotion_blocker_evidence_path\": \"\""));
    try std.testing.expectEqual(first_generated_matmul_artifacts.len, std.mem.count(u8, manifest, "\"promotion_blocker_check_command\":"));
    try std.testing.expectEqual(blocked_evidence_path_count, std.mem.count(u8, manifest, "\"promotion_blocker_check_command\": \"zig build quant-kernel-metal-runtime-check"));
    try std.testing.expectEqual(blocked_evidence_path_count, std.mem.count(u8, manifest, "--require-evidence-kernel"));
    try std.testing.expectEqual(first_generated_matmul_artifacts.len - blocked_evidence_path_count, std.mem.count(u8, manifest, "\"promotion_blocker_check_command\": \"\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "--require-evidence-kernel " ++ first_general_metal_q4_0_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, runtime_wired_artifacts + cuda_runtime_wired_count, "\"runtime_wired\": true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, first_generated_matmul_artifacts.len - runtime_wired_artifacts - cuda_runtime_wired_count, "\"runtime_wired\": false"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, first_generated_matmul_artifacts.len - runtime_wired_artifacts - cuda_runtime_wired_count, "\"runtime_gate_env\": \"\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MMV\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MM\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR_Q8\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_DOWN_Q8\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0_"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 4, "\"runtime_gate_env\": \"ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 4, "\"runtime_gate_env\": \"ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 2, "\"runtime_gate_env\": \"ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_DISABLE_ANTFLY_Q8_0_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH_RELU\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q2_K_SMALL_BATCH\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_DISABLE_ANTFLY_Q2_K_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q2_K_SMALL_BATCH_BIAS\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q2_K_SMALL_BATCH_BIAS_GELU\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH_BIAS\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH_BIAS_GELU\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q3_K_SMALL_BATCH\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_DISABLE_ANTFLY_Q3_K_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q3_K_SMALL_BATCH_BIAS\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q3_K_SMALL_BATCH_BIAS_GELU\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q4_0_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q4_1_SMALL_BATCH\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_DISABLE_ANTFLY_Q4_1_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q5_0_SMALL_BATCH\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_DISABLE_ANTFLY_Q5_0_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q5_1_SMALL_BATCH\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_DISABLE_ANTFLY_Q5_1_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q8_1_SMALL_BATCH\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_DISABLE_ANTFLY_Q8_1_SMALL_BATCH\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_DISABLE_ANTFLY_Q8_K_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q8_K_SMALL_BATCH\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_DISABLE_ANTFLY_Q4_K_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q4_K_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q4_K_SMALL_BATCH_BIAS\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q4_K_SMALL_BATCH_BIAS_GELU\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_DISABLE_ANTFLY_Q5_K_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q5_K_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q5_K_SMALL_BATCH_BIAS\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q5_K_SMALL_BATCH_BIAS_GELU\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_DISABLE_ANTFLY_Q6_K_SMALL_BATCH\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH_BIAS\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"runtime_gate_env\": \"TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH_BIAS_GELU\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, metal_evidence_count, "\"metal_promotion_min_speedup\": 1.02"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, metal_evidence_count, "\"metal_promotion_repeat_runs\": 5"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, metal_evidence_count, "\"metal_promotion_warmup_repeat_runs\": 2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_model_local_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_model_generated_route_check_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_model_generated_q8_0_small_batch_min\": 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_model_generated_q4_0_small_batch_min\": 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_industry_local_check_command));
}

test "quant kernel compiler checked-in Metal evidence matches generated source" {
    try std.testing.expectEqual(@as(usize, 7), first_metal_runtime_evidence_count);
    for (first_metal_runtime_evidence) |evidence| {
        try std.testing.expectEqual(metal_promotion_repeat_runs, evidence.repeat_runs);
        try std.testing.expect(evidence.correctness_passed);
        try std.testing.expect(evidence.generated_route_checked);
        try std.testing.expectEqual(metalProviderRouteRequiredForKernel(evidence.kernel_id), evidence.provider_route_checked);
        try std.testing.expect(evidence.benchmark_passed);
        try std.testing.expect(evidence.production_enabled);
        try std.testing.expect(evidence.promotion_ready);
        try std.testing.expectEqualStrings(metal_evidence_provenance_legacy_unattested, evidence.provenance_status);
        try std.testing.expectEqualStrings(metal_blocker_missing_reproducible_provenance, evidence.provenance_blocker);
        try std.testing.expect(evidence.legacy_production_exception);
        try std.testing.expect(metalLegacyEvidenceExceptionAllowed(evidence.kernel_id));
        try std.testing.expectEqualStrings(metal_blocker_none, metalRuntimeEvidenceProvenanceBlocker(evidence));
        try std.testing.expectEqualStrings("", evidence.source_commit);
        try std.testing.expect(!evidence.source_tree_clean);

        const source_path = generatedMetalSourcePathForKernel(evidence.kernel_id) orelse return error.MissingMetalSourcePath;
        try std.testing.expectEqualStrings(source_path, evidence.source_path);

        var found = false;
        for (first_generated_matmul_artifacts) |artifact| {
            if (!std.mem.eql(u8, artifact.kernel_id, evidence.kernel_id)) continue;
            found = true;
            try std.testing.expectEqual(artifactSourceFingerprint(artifact), evidence.source_fingerprint);
            try std.testing.expect(metalRuntimeEvidenceFor(artifact, &first_metal_runtime_evidence) != null);
        }
        try std.testing.expect(found);
    }
}

test "quant kernel compiler checked-in Metal blocker evidence matches generated candidates" {
    try std.testing.expectEqual(@as(usize, 18), first_metal_promotion_blocker_evidence_count);
    try std.testing.expectEqual(@as(usize, 5), metalPromotionBlockerEvidenceCount("speedup_gate_missing"));
    try std.testing.expectEqual(@as(usize, 5), metalPromotionBlockerEvidenceCount("unsupported_handwritten_baseline"));
    try std.testing.expectEqual(@as(usize, 4), metalPromotionBlockerEvidenceCount("unstable_benchmark_timing"));
    try std.testing.expectEqual(@as(usize, 4), metalPromotionBlockerEvidenceCount(metal_blocker_runtime_route_only));
    try std.testing.expectEqual(@as(usize, 9), metalPromotionBlockerEvidencePathCount());
    try std.testing.expectEqual(@as(usize, 18), first_metal_promotion_blocker_evidence_expected_case_count);
    try std.testing.expectEqual(first_metal_promotion_blocker_evidence_expected_case_count, first_metal_promotion_blocker_evidence_expected_route_ready_count);
    for (first_metal_promotion_blocker_evidence) |evidence| {
        const artifact = generatedArtifactForKernel(.metal, evidence.kernel_id) orelse return error.MissingMetalBlockerArtifact;
        try std.testing.expectEqual(Backend.metal, artifact.backend);
        try std.testing.expect(artifactRuntimeWired(artifact));
        try std.testing.expect(!artifactHasPromotionEvidence(artifact));
        try std.testing.expectEqualStrings(evidence.blocker, artifactPromotionBlocker(artifact));
        try std.testing.expectEqualStrings(evidence.evidence_path, artifactPromotionBlockerEvidencePath(artifact));
        if (std.mem.eql(u8, evidence.blocker, metal_blocker_unsupported_handwritten) or
            std.mem.eql(u8, evidence.blocker, metal_blocker_runtime_route_only))
        {
            try std.testing.expectEqualStrings("", evidence.evidence_path);
            try std.testing.expect(artifactRuntimeWired(artifact));
        }
        if (evidence.evidence_path.len != 0) {
            try std.testing.expect(std.mem.startsWith(u8, evidence.evidence_path, "/private/tmp/antfly-quant-metal-"));
            try std.testing.expect(std.mem.containsAtLeast(u8, evidence.evidence_path, 1, evidence.kernel_id));
            try std.testing.expect(std.mem.endsWith(u8, evidence.evidence_path, "-promotion-evidence.json"));
        }
    }
}

test "quant kernel compiler Metal promotion requires route evidence where routed" {
    var artifact: GeneratedMatmulArtifact = undefined;
    var found_artifact = false;
    for (first_generated_matmul_artifacts) |candidate| {
        if (!std.mem.eql(u8, candidate.kernel_id, first_general_metal_q6_kernel_id)) continue;
        artifact = candidate;
        found_artifact = true;
        break;
    }
    try std.testing.expect(found_artifact);
    artifact.production_enabled = true;
    artifact.source_path = first_general_metal_q6_source_path;
    artifact.check_command = first_general_metal_q6_check_command;
    const evidence = MetalRuntimeEvidence{
        .kernel_id = first_general_metal_q6_kernel_id,
        .source_path = first_general_metal_q6_source_path,
        .source_fingerprint = artifactSourceFingerprint(artifact),
        .check_command = first_general_metal_q6_check_command,
        .runtime_evidence_command = first_general_metal_q6_promotion_evidence_command,
        .promotion_check_command = first_general_metal_q6_promotion_check_command,
        .repeat_runs = metal_promotion_repeat_runs,
        .correctness_passed = true,
        .generated_route_checked = true,
        .provider_route_checked = true,
        .benchmark_passed = true,
        .measured_speedup = metal_promotion_min_speedup + 0.1,
        .minimum_repeat_speedup = metal_promotion_min_speedup + 0.1,
        .production_enabled = true,
        .promotion_ready = true,
        .provenance_status = metal_evidence_provenance_attested_v1,
        .provenance_blocker = "",
        .source_commit = "0123456789abcdef0123456789abcdef01234567",
        .source_tree_clean = true,
        .source_status_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        .host_os = "test-os",
        .host_arch = "test-arch",
        .accelerator_name = "test-metal-device",
        .metal_compiler_version = "test-metal-compiler",
        .zig_version = "test-zig",
        .recorded_at_utc = "2026-07-13T00:00:00Z",
    };
    var missing_generated_evidence = [_]MetalRuntimeEvidence{evidence};
    missing_generated_evidence[0].generated_route_checked = false;
    try std.testing.expectEqualStrings(
        "missing_metal_generated_route_evidence",
        metalArtifactPromotionBlockerWithEvidence(artifact, &missing_generated_evidence),
    );
    var stale_evidence = [_]MetalRuntimeEvidence{evidence};
    stale_evidence[0].provider_route_checked = false;
    try std.testing.expectEqualStrings(
        "missing_metal_provider_route_evidence",
        metalArtifactPromotionBlockerWithEvidence(artifact, &stale_evidence),
    );

    var unattested_evidence = [_]MetalRuntimeEvidence{evidence};
    unattested_evidence[0].provenance_status = metal_evidence_provenance_legacy_unattested;
    unattested_evidence[0].provenance_blocker = metal_blocker_missing_reproducible_provenance;
    try std.testing.expectEqualStrings(
        metal_blocker_missing_reproducible_provenance,
        metalArtifactPromotionBlockerWithEvidence(artifact, &unattested_evidence),
    );
    var dirty_evidence = [_]MetalRuntimeEvidence{evidence};
    dirty_evidence[0].source_tree_clean = false;
    try std.testing.expectEqualStrings(
        metal_blocker_dirty_source_tree,
        metalArtifactPromotionBlockerWithEvidence(artifact, &dirty_evidence),
    );
    var incomplete_evidence = [_]MetalRuntimeEvidence{evidence};
    incomplete_evidence[0].accelerator_name = "";
    try std.testing.expectEqualStrings(
        metal_blocker_missing_reproducible_provenance,
        metalArtifactPromotionBlockerWithEvidence(artifact, &incomplete_evidence),
    );
    var invalid_legacy_exception = evidence;
    invalid_legacy_exception.kernel_id = "future_metal_kernel";
    invalid_legacy_exception.provenance_status = metal_evidence_provenance_legacy_unattested;
    invalid_legacy_exception.provenance_blocker = metal_blocker_missing_reproducible_provenance;
    invalid_legacy_exception.legacy_production_exception = true;
    try std.testing.expectEqualStrings(
        metal_blocker_invalid_legacy_provenance_exception,
        metalRuntimeEvidenceProvenanceBlocker(invalid_legacy_exception),
    );
}

test "quant kernel compiler route-all covers every generated Metal artifact" {
    var metal_artifacts: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        metal_artifacts += 1;
        try std.testing.expect(artifactRuntimeWired(artifact));
    }
    try std.testing.expect(metal_artifacts > first_metal_runtime_evidence_count);
    try std.testing.expectEqual(metal_artifacts * 2, first_metal_runtime_route_all_expected_case_count);
}

test "quant kernel compiler decode descriptors cover required stages" {
    for (descriptor_formats) |format| {
        const spec = specFor(format).?;
        try std.testing.expect(spec.decode_ops.len >= 4);
        try std.testing.expectEqual(DecodeOpKind.load_packed_bytes, spec.decode_ops[0].kind);
        try std.testing.expectEqual(DecodeOpKind.dequant_lane, spec.decode_ops[spec.decode_ops.len - 1].kind);

        for (spec.block_fields) |field| {
            try std.testing.expect(std.mem.containsAtLeast(u8, spec.decode_ops[0].expression, 1, field.name));
        }

        var has_scale = false;
        var has_min = false;
        var has_lane = false;
        for (spec.decode_ops) |op| {
            try std.testing.expect(op.expression.len != 0);
            has_scale = has_scale or op.kind == .extract_scale;
            has_min = has_min or op.kind == .extract_min;
            has_lane = has_lane or op.kind == .extract_quant_lane;
        }
        try std.testing.expect(has_scale);
        try std.testing.expect(has_lane);
        try std.testing.expectEqual(format == .q2_k or format == .q4_1 or format == .q5_1 or format == .q4_k or format == .q5_k, has_min);
    }
}

test "quant kernel compiler builds deterministic IR for the first lazy target" {
    const ir = buildIr(.q4_k, .rows_2_8, .bias_gelu).?;
    try std.testing.expectEqual(quant_matmul.DispatchKind.small_batch, ir.dispatch);
    try std.testing.expectEqual(Epilogue.bias_gelu, ir.epilogue);
    try std.testing.expectEqualSlices(IROp, &ir_ops_bias_gelu, ir.ops);

    const route = loweringFor(.cuda, .q4_k, .rows_2_8, .bias_gelu);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, route.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, route.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, route.fallback_reason);
    try std.testing.expectEqualStrings(first_lazy_benchmark.handwritten_baseline, route.production_kernel_id);
    try std.testing.expectEqualStrings(first_lazy_benchmark.generated_kernel_id, route.kernel_id);
}

test "quant kernel compiler IR covers descriptor-supported matrix" {
    for (descriptor_formats) |format| {
        const spec = specFor(format).?;
        for (first_row_buckets) |row_bucket| {
            for (first_epilogues) |epilogue| {
                const ir = buildIr(format, row_bucket, epilogue);
                if (!spec.supportsEpilogue(epilogue)) {
                    try std.testing.expect(ir == null);
                    continue;
                }

                const planned = ir orelse return error.MissingQuantKernelIR;
                try std.testing.expectEqual(format, planned.format);
                try std.testing.expectEqual(row_bucket, planned.row_bucket);
                try std.testing.expectEqual(dispatchForRowBucket(row_bucket).?, planned.dispatch);
                try std.testing.expectEqual(epilogue, planned.epilogue);
                try std.testing.expectEqualSlices(IROp, irOpsForEpilogue(epilogue), planned.ops);
            }
        }
    }

    try std.testing.expect(buildIr(.q4_k, .rows_0, .none) == null);
    try std.testing.expect(buildIr(.unknown, .rows_2_8, .none) == null);
}

test "quant kernel compiler exposes the first static route registry" {
    try std.testing.expectEqual(first_coverage.len * first_backends.len, first_registry.entries.len);

    const route = first_registry.lookup(.cuda, .q4_k, .rows_2_8, .bias_gelu, .small_batch).?;
    try std.testing.expectEqual(LoweringRoute.handwritten_production, route.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, route.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, route.fallback_reason);

    try std.testing.expect(first_registry.lookup(.cuda, .unknown, .rows_2_8, .bias_gelu, .small_batch) == null);
}

test "quant kernel compiler registry entries have unique route keys" {
    for (first_registry.entries, 0..) |a, i| {
        for (first_registry.entries[i + 1 ..]) |b| {
            try std.testing.expect(!(a.backend == b.backend and
                a.format == b.format and
                a.row_bucket == b.row_bucket and
                a.epilogue == b.epilogue and
                a.schedule.dispatch == b.schedule.dispatch));
        }
    }
}

test "quant kernel compiler plan ids match route keys" {
    for (first_registry.entries) |entry| {
        try std.testing.expectEqual(entry.backend, entry.plan_id.backend);
        try std.testing.expectEqual(entry.format, entry.plan_id.format);
        try std.testing.expectEqual(entry.row_bucket, entry.plan_id.row_bucket);
        try std.testing.expectEqual(entry.epilogue, entry.plan_id.epilogue);
        try std.testing.expectEqual(entry.schedule.dispatch, entry.plan_id.dispatch);
    }

    const unsupported_shape = loweringFor(.cuda, .q4_k, .rows_0, .bias_gelu);
    try std.testing.expectEqual(quant_matmul.DispatchKind.scalar, unsupported_shape.plan_id.dispatch);
    try std.testing.expectEqual(.rows_0, unsupported_shape.plan_id.row_bucket);
}

test "quant kernel compiler plan ids have stable diagnostic names" {
    const lazy = loweringFor(.cuda, .q4_k, .rows_2_8, .bias_gelu);
    const lazy_name = try planIdName(std.testing.allocator, lazy.plan_id);
    defer std.testing.allocator.free(lazy_name);
    try std.testing.expectEqualStrings("cuda/q4_k/rows_2_8/bias_gelu/small_batch", lazy_name);

    const unsupported = loweringFor(.metal, .q2_k, .rows_0, .argmax);
    const unsupported_name = try planIdName(std.testing.allocator, unsupported.plan_id);
    defer std.testing.allocator.free(unsupported_name);
    try std.testing.expectEqualStrings("metal/q2_k/rows_0/argmax/scalar", unsupported_name);
}

test "quant kernel compiler routes and fallback reasons have stable diagnostic names" {
    try std.testing.expectEqualStrings("generated_production", loweringRouteName(.generated_production));
    try std.testing.expectEqualStrings("generated_dev_candidate", loweringRouteName(.generated_dev_candidate));
    try std.testing.expectEqualStrings("handwritten_production", loweringRouteName(.handwritten_production));
    try std.testing.expectEqualStrings("unsupported", loweringRouteName(.unsupported));

    try std.testing.expectEqualStrings("none", fallbackReasonName(.none));
    try std.testing.expectEqualStrings("unsupported_format", fallbackReasonName(.unsupported_format));
    try std.testing.expectEqualStrings("unsupported_shape", fallbackReasonName(.unsupported_shape));
    try std.testing.expectEqualStrings("unsupported_epilogue", fallbackReasonName(.unsupported_epilogue));
    try std.testing.expectEqualStrings("unsupported_backend", fallbackReasonName(.unsupported_backend));
    try std.testing.expectEqualStrings("generated_artifact_missing", fallbackReasonName(.generated_artifact_missing));
    try std.testing.expectEqualStrings("generated_runtime_not_wired", fallbackReasonName(.generated_runtime_not_wired));
    try std.testing.expectEqualStrings("tensor_core_repack_required", fallbackReasonName(.tensor_core_repack_required));
}

test "quant kernel compiler lowering diagnostic includes route identity" {
    const lowering = registryLoweringFor(.cuda, .q4_k, .rows_2_8, .bias_gelu, .small_batch);
    const diagnostic = try loweringDiagnostic(std.testing.allocator, lowering);
    defer std.testing.allocator.free(diagnostic);

    try std.testing.expect(std.mem.containsAtLeast(u8, diagnostic, 1, "plan=cuda/q4_k/rows_2_8/bias_gelu/small_batch"));
    try std.testing.expect(std.mem.containsAtLeast(u8, diagnostic, 1, "production=handwritten_production"));
    try std.testing.expect(std.mem.containsAtLeast(u8, diagnostic, 1, "candidate=generated_dev_candidate"));
    try std.testing.expect(std.mem.containsAtLeast(u8, diagnostic, 1, "fallback=generated_artifact_missing"));
    try std.testing.expect(std.mem.containsAtLeast(u8, diagnostic, 1, first_lazy_benchmark.handwritten_baseline));
    try std.testing.expect(std.mem.containsAtLeast(u8, diagnostic, 1, first_lazy_benchmark.generated_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, diagnostic, 1, first_lazy_benchmark.generated_source_path));
}

test "quant kernel compiler Metal runtime route summary maps counters to route metadata" {
    const json = try metalRuntimeRouteSummaryJson(std.testing.allocator);
    defer std.testing.allocator.free(json);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, json, .{});
    defer parsed.deinit();

    const routes = parsed.value.object;
    const q8_bias = routes.get("q8_0_small_batch_bias").?.object;
    try std.testing.expectEqualStrings("metal/q8_0/rows_2_8/bias/small_batch", q8_bias.get("plan_id").?.string);
    try std.testing.expectEqualStrings(first_general_metal_q8_bias_kernel_id, q8_bias.get("kernel_id").?.string);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", q8_bias.get("production_kernel_id").?.string);
    try std.testing.expectEqualStrings("handwritten_production", q8_bias.get("production_route").?.string);
    try std.testing.expectEqualStrings("generated_dev_candidate", q8_bias.get("candidate_route").?.string);
    try std.testing.expectEqualStrings("generated_artifact_missing", q8_bias.get("fallback_reason").?.string);
    try std.testing.expectEqual(false, q8_bias.get("promotion_ready").?.bool);

    const q4_bias_gelu = routes.get("q4_k_small_batch_bias_gelu").?.object;
    try std.testing.expectEqualStrings(first_lazy_metal_kernel_id, q4_bias_gelu.get("kernel_id").?.string);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", q4_bias_gelu.get("production_kernel_id").?.string);
    try std.testing.expectEqualStrings("handwritten_production", q4_bias_gelu.get("production_route").?.string);
    try std.testing.expectEqualStrings("generated_dev_candidate", q4_bias_gelu.get("candidate_route").?.string);
    try std.testing.expectEqualStrings("generated_artifact_missing", q4_bias_gelu.get("fallback_reason").?.string);
    try std.testing.expectEqual(false, q4_bias_gelu.get("promotion_ready").?.bool);

    const q4_0 = routes.get("q4_0_small_batch").?.object;
    try std.testing.expectEqualStrings(first_general_metal_q4_0_source_path, q4_0.get("generated_source_path").?.string);
}

test "quant kernel compiler planned production routes are named" {
    for (first_registry.entries) |entry| {
        switch (entry.production_route) {
            .generated_production, .handwritten_production => try std.testing.expect(entry.production_kernel_id.len != 0),
            .generated_dev_candidate => try std.testing.expect(false),
            .unsupported => try std.testing.expect(entry.production_kernel_id.len == 0),
        }
    }
}

test "quant kernel compiler names checked-in CUDA production route symbols" {
    const cases = [_]struct {
        format: quant_matmul.Format,
        row_bucket: quant_matmul.RowBucket,
        epilogue: Epilogue,
        symbol: []const u8,
    }{
        .{ .format = .q4_k, .row_bucket = .rows_1, .epilogue = .none, .symbol = "termite_linear_q4_k_f32_tile4" },
        .{ .format = .q4_k, .row_bucket = .rows_2_8, .epilogue = .none, .symbol = "termite_linear_q4_k_f32_tile4" },
        .{ .format = .q4_k, .row_bucket = .rows_1, .epilogue = .bias, .symbol = "termite_linear_q4_k_bias_f32_tile4" },
        .{ .format = .q4_k, .row_bucket = .rows_2_8, .epilogue = .bias, .symbol = "termite_linear_q4_k_bias_f32_tile4_r2" },
        .{ .format = .q4_k, .row_bucket = .rows_2_8, .epilogue = .bias_gelu, .symbol = first_lazy_benchmark.handwritten_baseline },
        .{ .format = .q6_k, .row_bucket = .rows_2_8, .epilogue = .none, .symbol = "termite_linear_q6_k_f32_tile4" },
    };

    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "src/ops/cuda/artifacts/inference_cuda_kernels.cu", std.testing.allocator, .limited(1024 * 1024));
    defer std.testing.allocator.free(contents);
    for (cases) |case| {
        const route = loweringFor(.cuda, case.format, case.row_bucket, case.epilogue);
        try std.testing.expectEqual(LoweringRoute.handwritten_production, route.production_route);
        try std.testing.expectEqualStrings(case.symbol, route.production_kernel_id);
        try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, case.symbol));
    }
}

test "quant kernel compiler keeps shape-dependent CUDA routes generic" {
    try std.testing.expectEqualStrings("cuda_handwritten_quant_matmul", loweringFor(.cuda, .q8_0, .rows_2_8, .bias).production_kernel_id);
    try std.testing.expectEqualStrings("cuda_handwritten_quant_matmul", loweringFor(.cuda, .q4_k, .rows_9_64, .bias).production_kernel_id);
}

test "quant kernel compiler applies CUDA production shape constraints" {
    const artifact = generatedArtifactForKernel(.cuda, first_general_cuda_q4_0_mm_kernel_id).?;
    try std.testing.expectEqual(@as(usize, 512), artifact.runtime_shape.min_in_dim);

    const narrow = quant_matmul.plan(.{
        .rows = 22,
        .in_dim = 256,
        .out_dim = 1536,
        .format = .q4_0,
    });
    const narrow_lowering = registryLoweringForPlan(.cuda, narrow, .none);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, narrow_lowering.production_route);
    try std.testing.expectEqualStrings("termite_linear_q4_0_f32", narrow_lowering.production_kernel_id);
    try std.testing.expect(!generatedArtifactSupportsPlan(.cuda, narrow, .none));
    const narrow_counters = plannedCountersForPlan(.cuda, narrow, .none);
    try std.testing.expectEqual(@as(usize, 1), narrow_counters.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(usize, 0), narrow_counters.quant_kernel_generated_production);

    const wide = quant_matmul.plan(.{
        .rows = 22,
        .in_dim = 512,
        .out_dim = 1536,
        .format = .q4_0,
    });
    const wide_lowering = registryLoweringForPlan(.cuda, wide, .none);
    try std.testing.expectEqual(LoweringRoute.generated_production, wide_lowering.production_route);
    try std.testing.expectEqualStrings(first_general_cuda_q4_0_mm_kernel_id, wide_lowering.production_kernel_id);
    try std.testing.expect(generatedArtifactSupportsPlan(.cuda, wide, .none));
    const wide_counters = plannedCountersForPlan(.cuda, wide, .none);
    try std.testing.expectEqual(@as(usize, 0), wide_counters.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(usize, 1), wide_counters.quant_kernel_generated_production);
}

test "quant kernel compiler uses exact CUDA renderer launch schedules" {
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .cuda) continue;
        const expected = cudaScheduleForArtifact(artifact) orelse return error.MissingCudaRenderPlan;
        const compiled = compileQuantKernelArtifactSource(artifact) orelse return error.MissingCompiledQuantKernelSource;
        try std.testing.expectEqual(expected, compiled.lowering.schedule);
    }

    const fused = generatedArtifactForKernel(.cuda, first_general_cuda_q4_0_pair_q8_kernel_id) orelse return error.MissingGeneratedArtifact;
    const fused_schedule = cudaScheduleForArtifact(fused) orelse return error.MissingCudaRenderPlan;
    try std.testing.expectEqual(@as(usize, 640), fused_schedule.threads_per_block);
    try std.testing.expectEqual(@as(usize, 32), fused_schedule.tile_cols);
    try std.testing.expectEqual(@as(usize, 768), fused_schedule.shared_memory_bytes);
}

test "quant kernel compiler dev candidates never occupy production route slots" {
    for (first_registry.entries) |entry| {
        try std.testing.expect(entry.production_route != .generated_dev_candidate);
        try std.testing.expect(entry.candidate_route != .generated_production);
    }
}

test "quant kernel compiler generated candidates are named only when present" {
    for (first_registry.entries) |entry| {
        switch (entry.candidate_route) {
            .generated_dev_candidate => {
                try std.testing.expect(entry.kernel_id.len != 0);
                try std.testing.expect(entry.candidate_source_path.len != 0);
            },
            .generated_production, .handwritten_production, .unsupported => {
                try std.testing.expect(entry.kernel_id.len == 0);
                try std.testing.expect(entry.candidate_source_path.len == 0);
            },
        }
    }
}

test "quant kernel compiler routes honor descriptor schedules" {
    for (first_registry.entries) |entry| {
        const spec = specFor(entry.format) orelse {
            try std.testing.expectEqual(LoweringRoute.unsupported, entry.production_route);
            continue;
        };
        if (!spec.supportsSchedule(entry.schedule.dispatch)) {
            try std.testing.expectEqual(LoweringRoute.unsupported, entry.production_route);
            try std.testing.expectEqual(LoweringRoute.unsupported, entry.candidate_route);
            try std.testing.expectEqual(FallbackReason.unsupported_shape, entry.fallback_reason);
            continue;
        }
        if (entry.production_route != .unsupported or entry.candidate_route != .unsupported) {
            try std.testing.expect(spec.supportsSchedule(entry.schedule.dispatch));
        }
    }
}

test "quant kernel compiler coverage includes every non-empty row bucket" {
    const required = [_]quant_matmul.RowBucket{ .rows_1, .rows_2_8, .rows_9_64, .rows_65_plus };
    for (required) |row_bucket| {
        var found = false;
        for (first_coverage) |case| {
            if (case.row_bucket == row_bucket) {
                found = true;
                break;
            }
        }
        try std.testing.expect(found);
    }
}

test "quant kernel compiler row buckets follow quant matmul planner" {
    const samples = [_]struct {
        rows: usize,
        bucket: quant_matmul.RowBucket,
        dispatch: ?quant_matmul.DispatchKind,
    }{
        .{ .rows = 0, .bucket = .rows_0, .dispatch = null },
        .{ .rows = 1, .bucket = .rows_1, .dispatch = .mmv },
        .{ .rows = 2, .bucket = .rows_2_8, .dispatch = .small_batch },
        .{ .rows = 8, .bucket = .rows_2_8, .dispatch = .small_batch },
        .{ .rows = 9, .bucket = .rows_9_64, .dispatch = .mm },
        .{ .rows = 64, .bucket = .rows_9_64, .dispatch = .mm },
        .{ .rows = 65, .bucket = .rows_65_plus, .dispatch = .mm },
    };

    for (samples) |sample| {
        try std.testing.expectEqual(sample.bucket, quant_matmul.rowBucket(sample.rows));
        try std.testing.expectEqual(sample.dispatch, dispatchForRowBucket(sample.bucket));
        const planned = quant_matmul.plan(.{
            .rows = sample.rows,
            .in_dim = 256,
            .out_dim = 128,
            .format = .q4_k,
        });
        try std.testing.expectEqual(sample.bucket, planned.row_bucket);
        try std.testing.expectEqual(sample.dispatch orelse .scalar, planned.dispatch);
    }
}

test "quant kernel compiler conformance matrix pins route expectations" {
    try std.testing.expectEqual(first_coverage.len, first_conformance.len);
    for (first_conformance, first_coverage) |conf, cov| {
        try std.testing.expectEqual(cov.format, conf.format);
        try std.testing.expectEqual(cov.row_bucket, conf.row_bucket);
        try std.testing.expectEqual(cov.epilogue, conf.epilogue);
        try std.testing.expectEqual(dispatchForRowBucket(cov.row_bucket).?, conf.dispatch);
        try std.testing.expectEqual(tensorTypeForFormat(cov.format).?, conf.reference_tensor_type);
        try std.testing.expect(conf.tolerance_abs > 0.0 and conf.tolerance_abs <= 0.001);

        const cuda = loweringFor(.cuda, cov.format, cov.row_bucket, cov.epilogue);
        try std.testing.expectEqual(cuda.production_route, conf.cuda_route);
        try std.testing.expectEqual(cuda.candidate_route, conf.cuda_candidate_route);
        try std.testing.expectEqual(cuda.fallback_reason, conf.cuda_fallback_reason);

        const metal = loweringFor(.metal, cov.format, cov.row_bucket, cov.epilogue);
        try std.testing.expectEqual(metal.production_route, conf.metal_route);
        try std.testing.expectEqual(metal.candidate_route, conf.metal_candidate_route);
        try std.testing.expectEqual(metal.fallback_reason, conf.metal_fallback_reason);
    }
}

test "quant kernel compiler registry route summary is golden" {
    var by_backend = [_]PlanCounters{ .{}, .{} };
    for (first_registry.entries) |entry| {
        const counters = countersForLowering(entry);
        const index = @intFromEnum(entry.backend);
        addCountersToStats(&by_backend[index], counters);
    }

    const cuda = by_backend[@intFromEnum(@as(Backend, .cuda))];
    try std.testing.expectEqual(@as(usize, 1232), cuda.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(usize, 59), cuda.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(usize, 5), cuda.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(usize, 1168), cuda.quant_kernel_unsupported_routes);
    try std.testing.expectEqual(@as(usize, 4), cuda.quant_kernel_generated_candidates);
    try std.testing.expectEqual(@as(usize, 2), cuda.quant_kernel_fallback_generated_artifact_missing);
    try std.testing.expectEqual(@as(usize, 2), cuda.quant_kernel_fallback_generated_runtime_not_wired);
    try std.testing.expectEqual(@as(usize, 0), cuda.quant_kernel_fallback_unsupported_format);
    try std.testing.expectEqual(@as(usize, 0), cuda.quant_kernel_fallback_unsupported_shape);
    try std.testing.expectEqual(@as(usize, 112), cuda.quant_kernel_fallback_unsupported_epilogue);
    try std.testing.expectEqual(@as(usize, 1056), cuda.quant_kernel_fallback_unsupported_backend);
    try std.testing.expectEqual(@as(usize, 0), cuda.quant_kernel_fallback_tensor_core_repack_required);
    try std.testing.expectEqual(@as(usize, 1168), cuda.quant_kernel_fallback_unsupported);

    const metal = by_backend[@intFromEnum(@as(Backend, .metal))];
    try std.testing.expectEqual(@as(usize, 1232), metal.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(usize, 105), metal.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(usize, 7), metal.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(usize, 1120), metal.quant_kernel_unsupported_routes);
    try std.testing.expectEqual(@as(usize, 18), metal.quant_kernel_generated_candidates);
    try std.testing.expectEqual(@as(usize, 18), metal.quant_kernel_fallback_generated_artifact_missing);
    try std.testing.expectEqual(@as(usize, 0), metal.quant_kernel_fallback_generated_runtime_not_wired);
    try std.testing.expectEqual(@as(usize, 0), metal.quant_kernel_fallback_unsupported_format);
    try std.testing.expectEqual(@as(usize, 0), metal.quant_kernel_fallback_unsupported_shape);
    try std.testing.expectEqual(@as(usize, 460), metal.quant_kernel_fallback_unsupported_epilogue);
    try std.testing.expectEqual(@as(usize, 660), metal.quant_kernel_fallback_unsupported_backend);
    try std.testing.expectEqual(@as(usize, 0), metal.quant_kernel_fallback_tensor_core_repack_required);
    try std.testing.expectEqual(@as(usize, 1120), metal.quant_kernel_fallback_unsupported);
}

test "quant kernel compiler route expectations are hand pinned" {
    for (first_route_expectations) |want| {
        const got = registryLoweringFor(want.backend, want.format, want.row_bucket, want.epilogue, want.dispatch);
        try std.testing.expectEqual(want.production_route, got.production_route);
        try std.testing.expectEqual(want.candidate_route, got.candidate_route);
        try std.testing.expectEqual(want.fallback_reason, got.fallback_reason);
        try std.testing.expectEqual(want.dispatch, got.plan_id.dispatch);
        try std.testing.expectEqual(want.dispatch, got.schedule.dispatch);
    }
}

test "quant kernel compiler benchmark manifest maps to conformance rows" {
    const manifest = try benchmarkManifestJson(std.testing.allocator);
    defer std.testing.allocator.free(manifest);
    try expectManifestArrayCount(manifest, "benchmark_count", "benchmarks", first_benchmarks.len);
    try expectManifestArrayCount(manifest, "evidence_count", "evidence_records", first_benchmark_evidence.len);
    try expectManifestArrayCount(manifest, "metal_evidence_count", "metal_evidence_records", first_metal_runtime_evidence.len);
    try expectManifestArrayCount(manifest, "metal_production_regression_expected_case_count", "metal_production_regression_cases", first_metal_production_benchmark_case_count);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"schema\": \"" ++ first_benchmark_manifest_schema ++ "\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_legacy_unattested_evidence_count\": 7"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_legacy_unattested_evidence_is_release_blocker\": true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_future_promotions_require_attested_provenance\": true"));
    try std.testing.expectEqual(first_metal_runtime_evidence_count, std.mem.count(u8, manifest, "\"legacy_production_exception\": true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_promotion_warmup_repeat_runs\": 2"));
    const production_regression_case_fingerprint_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_production_regression_case_fingerprint\": {d}", .{metalProductionBenchmarkCaseManifestFingerprint()});
    defer std.testing.allocator.free(production_regression_case_fingerprint_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, production_regression_case_fingerprint_field));
    try std.testing.expectEqual(first_metal_runtime_evidence_count * 2, first_metal_production_benchmark_case_count);
    const production_regression_kernel_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_production_regression_expected_kernel_count\": {d}", .{first_metal_production_regression_expected_kernel_count});
    defer std.testing.allocator.free(production_regression_kernel_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, production_regression_kernel_count_field));
    const production_regression_case_count_field = try std.fmt.allocPrint(std.testing.allocator, "\"metal_production_regression_expected_case_count\": {d}", .{first_metal_production_regression_expected_case_count});
    defer std.testing.allocator.free(production_regression_case_count_field);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, production_regression_case_count_field));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_production_regression_build_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, first_metal_production_regression_evidence_command));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"kernel_id\": \"antfly_q6_k_small_batch_msl_v1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"name\": \"q6_k_rows_8_cols_7_none\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"shape\": \"wide\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"out_dim\": 7"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"production_kernel_id\": \"antfly_q6_k_small_batch_msl_v1\""));
    try std.testing.expect(!std.mem.containsAtLeast(u8, manifest, 1, "\"minimum_repeat_speedup\": 1.271992"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"generated_source_fingerprint\":"));

    for (first_benchmarks) |bench| {
        var matched = false;
        for (first_conformance) |conf| {
            if (conf.format == bench.format and conf.row_bucket == bench.row_bucket and conf.epilogue == bench.epilogue) {
                matched = true;
                try std.testing.expectEqual(Backend.cuda, bench.backend);
                try std.testing.expect(bench.generated_kernel_id.len != 0);
                try std.testing.expect(bench.generated_source_path.len != 0);
                try std.testing.expect(bench.generated_source_fingerprint != 0);
                try std.testing.expect(isCudaModulePath(bench.generated_ptx_path));
                try std.testing.expect(std.mem.containsAtLeast(u8, bench.generated_ptx_command, 1, "nvcc -fatbin"));
                try std.testing.expect(std.mem.containsAtLeast(u8, bench.generated_ptx_command, 1, bench.generated_source_path));
                try std.testing.expect(std.mem.containsAtLeast(u8, bench.generated_ptx_command, 1, bench.generated_ptx_path));
                try std.testing.expect(std.mem.containsAtLeast(u8, bench.benchmark_command, 1, "zig-out/bin/antfly-inference bench-cuda"));
                try std.testing.expect(std.mem.containsAtLeast(u8, bench.benchmark_command, 1, "--warmup-iters 5"));
                try std.testing.expect(std.mem.containsAtLeast(u8, bench.benchmark_command, 1, "--measure-iters 50"));
                try std.testing.expect(std.mem.containsAtLeast(u8, bench.benchmark_command, 1, bench.generated_ptx_arg));
                try std.testing.expect(std.mem.containsAtLeast(u8, bench.benchmark_command, 1, bench.generated_ptx_path));
                try std.testing.expect(std.mem.containsAtLeast(u8, bench.benchmark_command, 1, "--quant-compiler-evidence-out"));
                try std.testing.expect(bench.handwritten_baseline.len != 0);
                try std.testing.expect(bench.correctness_tolerance_abs > 0.0 and bench.correctness_tolerance_abs <= 0.01);
                try std.testing.expect(bench.minimum_speedup >= 1.0);
                if (bench.production_enabled) {
                    try std.testing.expectEqual(LoweringRoute.generated_production, conf.cuda_route);
                    try std.testing.expectEqual(LoweringRoute.unsupported, conf.cuda_candidate_route);
                    try std.testing.expectEqual(FallbackReason.none, conf.cuda_fallback_reason);
                    try std.testing.expect(std.mem.startsWith(u8, bench.generated_source_path, "src/ops/cuda/generated/"));
                    try std.testing.expect(bench.correctness_evidence_path.len != 0);
                    try std.testing.expectEqualStrings(bench.correctness_evidence_path, bench.benchmark_evidence_path);
                    try std.testing.expectEqualStrings("sequential", bench.benchmark_mode);
                    try std.testing.expect(benchmarkHasPromotionEvidence(bench));
                    try std.testing.expect(benchmarkMeasuredSpeedup(bench) >= bench.minimum_speedup);
                } else {
                    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, conf.cuda_candidate_route);
                    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, conf.cuda_fallback_reason);
                    try std.testing.expect(std.mem.containsAtLeast(u8, bench.benchmark_command, 1, "--quant-compiler-lazy-target"));
                    try std.testing.expectEqualStrings("--quant-compiler-generated-ptx", bench.generated_ptx_arg);
                    try std.testing.expectEqual(@as(f64, 0.0), benchmarkMeasuredSpeedup(bench));
                    try std.testing.expect(bench.correctness_evidence_path.len == 0);
                    try std.testing.expect(bench.benchmark_evidence_path.len == 0);
                    try std.testing.expect(bench.benchmark_mode.len == 0);
                    try std.testing.expect(!benchmarkHasPromotionEvidence(bench));
                }
                try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"promotion_ready\": false"));
                try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"promotion_blocker\": \"production_disabled\""));
                break;
            }
        }
        try std.testing.expect(matched);
    }
}

test "quant kernel compiler conformance manifest serializes the route matrix" {
    const manifest = try conformanceManifestJson(std.testing.allocator);
    defer std.testing.allocator.free(manifest);

    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "antfly.quant_kernel_conformance.v1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"case_count\": 1232"));
    try expectManifestArrayCount(manifest, "case_count", "cases", first_conformance.len);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"format_count\": 28"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"row_bucket_count\": 4"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"epilogue_count\": 11"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"backend_count\": 2"));
    try expectManifestNestedInteger(manifest, "cuda_route_summary", "quant_kernel_planned_ops", 1232);
    try expectManifestNestedInteger(manifest, "cuda_route_summary", "quant_kernel_generated_candidates", 4);
    try expectManifestNestedInteger(manifest, "cuda_route_summary", "quant_kernel_fallback_generated_artifact_missing", 2);
    try expectManifestNestedInteger(manifest, "cuda_route_summary", "quant_kernel_fallback_generated_runtime_not_wired", 2);
    try expectManifestNestedInteger(manifest, "cuda_route_summary", "quant_kernel_fallback_unsupported_epilogue", 112);
    try expectManifestNestedInteger(manifest, "metal_route_summary", "quant_kernel_planned_ops", 1232);
    try expectManifestNestedInteger(manifest, "metal_route_summary", "quant_kernel_generated_production", 7);
    try expectManifestNestedInteger(manifest, "metal_route_summary", "quant_kernel_generated_candidates", 18);
    try expectManifestNestedInteger(manifest, "metal_route_summary", "quant_kernel_fallback_generated_artifact_missing", 18);
    try expectManifestNestedInteger(manifest, "metal_route_summary", "quant_kernel_fallback_generated_runtime_not_wired", 0);
    try expectManifestNestedInteger(manifest, "metal_route_summary", "quant_kernel_fallback_unsupported_epilogue", 460);
    try std.testing.expectEqual(first_conformance.len, std.mem.count(u8, manifest, "\"format\": "));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"format\": \"q4_k\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"block_values\": 256"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"block_bytes\": 144"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"accumulator_dtype\": \"f32\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"output_dtype\": \"f32\""));
    try std.testing.expectEqual(first_formats.len * first_row_buckets.len * 5, std.mem.count(u8, manifest, "\"reference_supported\": true"));
    try std.testing.expectEqual(first_formats.len * first_row_buckets.len * (coverage_epilogues.len - 5), std.mem.count(u8, manifest, "\"reference_supported\": false"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"row_bucket\": \"rows_2_8\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"epilogue\": \"pair\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"epilogue\": \"bias_gelu\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"tile_rows\": 2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"tile_cols\": 4"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"vector_width\": 4"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"threads_per_block\": 256"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"cuda_candidate_tile_rows\": 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"cuda_candidate_tile_cols\": 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"cuda_candidate_threads_per_block\": 128"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_candidate_tile_rows\": 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_candidate_tile_cols\": 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_candidate_threads_per_block\": 32"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"shared_memory_bytes\": 0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"register_pressure_hint\": 6"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"tensor_core_eligible\": false"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"cuda_candidate_route\": \"generated_dev_candidate\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"cuda_production_kernel_id\": \"termite_linear_q4_k_bias_gelu_f32_tile4_r2\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"cuda_candidate_kernel_id\": \"antfly_q4_k_small_batch_bias_gelu_f32_v1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"cuda_candidate_source_path\": \"src/ops/cuda/generated/quant_kernel_q4_k_small_batch_bias_gelu.cu\""));
    const cuda_candidate_fingerprint = try std.fmt.allocPrint(std.testing.allocator, "\"cuda_candidate_source_fingerprint\": {d}", .{artifactSourceFingerprint(first_generated_matmul_artifacts[0])});
    defer std.testing.allocator.free(cuda_candidate_fingerprint);
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, cuda_candidate_fingerprint));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"cuda_fallback_reason\": \"generated_artifact_missing\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_candidate_route\": \"generated_dev_candidate\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_production_kernel_id\": \"metal_handwritten_quant_matmul\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_candidate_kernel_id\": \"antfly_q4_0_small_batch_msl_v1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_fallback_reason\": \"generated_artifact_missing\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_candidate_kernel_id\": \"antfly_q2_k_small_batch_bias_msl_v1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_candidate_kernel_id\": \"antfly_q2_k_small_batch_bias_gelu_msl_v1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_candidate_kernel_id\": \"antfly_q3_k_small_batch_bias_msl_v1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_candidate_kernel_id\": \"antfly_q3_k_small_batch_bias_gelu_msl_v1\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "\"metal_candidate_kernel_id\": \"antfly_q8_0_small_batch_relu_msl_v1\""));
}

test "quant kernel compiler conformance fingerprints match generated artifacts" {
    var cuda_candidates: usize = 0;
    var metal_candidates: usize = 0;
    for (first_conformance) |conf| {
        const record = conformanceManifestRecord(conf);
        const cuda = loweringFor(.cuda, conf.format, conf.row_bucket, conf.epilogue);
        const metal = loweringFor(.metal, conf.format, conf.row_bucket, conf.epilogue);

        try std.testing.expectEqual(candidateSourceFingerprint(cuda), record.cuda_candidate_source_fingerprint);
        try std.testing.expectEqual(candidateSourceFingerprint(metal), record.metal_candidate_source_fingerprint);

        if (cuda.candidate_route == .generated_dev_candidate) {
            cuda_candidates += 1;
            try std.testing.expect(record.cuda_candidate_source_fingerprint != 0);
        } else {
            try std.testing.expectEqual(@as(u64, 0), record.cuda_candidate_source_fingerprint);
        }

        if (metal.candidate_route == .generated_dev_candidate) {
            metal_candidates += 1;
            try std.testing.expect(record.metal_candidate_source_fingerprint != 0);
        } else {
            try std.testing.expectEqual(@as(u64, 0), record.metal_candidate_source_fingerprint);
        }
    }
    try std.testing.expectEqual(@as(usize, 4), cuda_candidates);
    try std.testing.expectEqual(@as(usize, 18), metal_candidates);
}

test "quant kernel compiler benchmark promotion evidence is complete" {
    var bench = first_lazy_benchmark;
    try std.testing.expect(!benchmarkHasPromotionEvidence(bench));
    try std.testing.expectEqualStrings("production_disabled", benchmarkPromotionBlocker(bench));

    bench.production_enabled = true;
    try std.testing.expect(!benchmarkHasPromotionEvidence(bench));
    try std.testing.expectEqualStrings("missing_correctness_evidence", benchmarkPromotionBlocker(bench));

    bench.generated_source_path = "src/ops/cuda/artifacts/inference_cuda_kernels.cu";
    try std.testing.expect(!benchmarkHasPromotionEvidence(bench));
    try std.testing.expectEqualStrings("ptx_command_missing_source", benchmarkPromotionBlocker(bench));

    bench.generated_ptx_command = "nvcc -fatbin -gencode=arch=compute_89,code=sm_89 src/ops/cuda/artifacts/inference_cuda_kernels.cu -o /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.fatbin";
    try std.testing.expect(!benchmarkHasPromotionEvidence(bench));
    try std.testing.expectEqualStrings("missing_correctness_evidence", benchmarkPromotionBlocker(bench));

    bench.correctness_evidence_path = first_lazy_benchmark_evidence_path;
    try std.testing.expect(!benchmarkHasPromotionEvidence(bench));
    try std.testing.expectEqualStrings("missing_benchmark_evidence", benchmarkPromotionBlocker(bench));

    var mismatched_evidence_path = bench;
    mismatched_evidence_path.benchmark_evidence_path = "src/ops/cuda/generated/evidence/q4_k_small_batch_bias_gelu_other.json";
    try std.testing.expect(!benchmarkHasPromotionEvidence(mismatched_evidence_path));
    try std.testing.expectEqualStrings("evidence_path_mismatch", benchmarkPromotionBlocker(mismatched_evidence_path));

    bench.benchmark_evidence_path = first_lazy_benchmark_evidence_path;
    try std.testing.expect(!benchmarkHasPromotionEvidence(bench));
    try std.testing.expectEqualStrings("missing_sequential_benchmark_evidence", benchmarkPromotionBlocker(bench));

    bench.benchmark_mode = "sequential";
    try std.testing.expect(!benchmarkHasPromotionEvidence(bench));
    try std.testing.expectEqualStrings("missing_matching_evidence_record", benchmarkPromotionBlocker(bench));

    const passing_evidence = [_]BenchmarkEvidence{.{
        .kernel_id = bench.generated_kernel_id,
        .generated_source_path = bench.generated_source_path,
        .generated_source_fingerprint = bench.generated_source_fingerprint,
        .generated_ptx_path = bench.generated_ptx_path,
        .generated_ptx_command = bench.generated_ptx_command,
        .benchmark_command = bench.benchmark_command,
        .correctness_evidence_path = bench.correctness_evidence_path,
        .benchmark_evidence_path = bench.benchmark_evidence_path,
        .benchmark_mode = bench.benchmark_mode,
        .target_fingerprint = bench.target_fingerprint,
        .repeat_runs = metal_promotion_repeat_runs,
        .correctness_passed = true,
        .benchmark_passed = true,
        .measured_speedup = 1.0,
    }};
    try std.testing.expect(std.mem.eql(u8, benchmarkPromotionBlockerWithEvidence(bench, &passing_evidence), "none"));

    var wrong_evidence_source = passing_evidence;
    wrong_evidence_source[0].generated_source_path = "src/ops/cuda/artifacts/other.cu";
    try std.testing.expectEqualStrings("missing_matching_evidence_record", benchmarkPromotionBlockerWithEvidence(bench, &wrong_evidence_source));

    var wrong_evidence_source_fingerprint = passing_evidence;
    wrong_evidence_source_fingerprint[0].generated_source_fingerprint = bench.generated_source_fingerprint +% 1;
    try std.testing.expectEqualStrings("missing_matching_evidence_record", benchmarkPromotionBlockerWithEvidence(bench, &wrong_evidence_source_fingerprint));

    var wrong_evidence_target = passing_evidence;
    wrong_evidence_target[0].target_fingerprint +%= 1;
    try std.testing.expectEqualStrings("missing_matching_evidence_record", benchmarkPromotionBlockerWithEvidence(bench, &wrong_evidence_target));

    var missing_target = bench;
    missing_target.target_fingerprint = 0;
    try std.testing.expectEqualStrings("missing_target_fingerprint", benchmarkPromotionBlockerWithEvidence(missing_target, &passing_evidence));

    var wrong_evidence_ptx = passing_evidence;
    wrong_evidence_ptx[0].generated_ptx_path = "/tmp/other.ptx";
    try std.testing.expectEqualStrings("missing_matching_evidence_record", benchmarkPromotionBlockerWithEvidence(bench, &wrong_evidence_ptx));

    var wrong_evidence_ptx_command = passing_evidence;
    wrong_evidence_ptx_command[0].generated_ptx_command = "nvcc -ptx -arch=compute_90 src/ops/cuda/generated/quant_kernel_q4_k_small_batch_bias_gelu.cu -o /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.ptx";
    try std.testing.expectEqualStrings("missing_matching_evidence_record", benchmarkPromotionBlockerWithEvidence(bench, &wrong_evidence_ptx_command));

    var wrong_evidence_benchmark_command = passing_evidence;
    wrong_evidence_benchmark_command[0].benchmark_command = "zig-out/bin/antfly-inference bench-cuda --quant-compiler-lazy-target --quant-compiler-generated-ptx /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.ptx";
    try std.testing.expectEqualStrings("missing_matching_evidence_record", benchmarkPromotionBlockerWithEvidence(bench, &wrong_evidence_benchmark_command));

    var single_repeat_evidence = passing_evidence;
    single_repeat_evidence[0].repeat_runs = 1;
    try std.testing.expectEqualStrings("insufficient_benchmark_repeats", benchmarkPromotionBlockerWithEvidence(bench, &single_repeat_evidence));

    var wrong_backend = bench;
    wrong_backend.backend = .metal;
    try std.testing.expect(!benchmarkHasPromotionEvidence(wrong_backend));
    try std.testing.expectEqualStrings("non_cuda_benchmark", benchmarkPromotionBlocker(wrong_backend));

    var missing_baseline = bench;
    missing_baseline.handwritten_baseline = "";
    try std.testing.expect(!benchmarkHasPromotionEvidence(missing_baseline));
    try std.testing.expectEqualStrings("missing_handwritten_baseline", benchmarkPromotionBlocker(missing_baseline));

    var missing_source_fingerprint = bench;
    missing_source_fingerprint.generated_source_fingerprint = 0;
    try std.testing.expect(!benchmarkHasPromotionEvidence(missing_source_fingerprint));
    try std.testing.expectEqualStrings("missing_source_fingerprint", benchmarkPromotionBlocker(missing_source_fingerprint));

    var empty_ptx_arg = bench;
    empty_ptx_arg.generated_ptx_arg = "";
    try std.testing.expect(!benchmarkHasPromotionEvidence(empty_ptx_arg));
    try std.testing.expectEqualStrings("wrong_generated_ptx_arg", benchmarkPromotionBlocker(empty_ptx_arg));

    var wrong_ptx_arg = bench;
    wrong_ptx_arg.generated_ptx_arg = "--ptx";
    wrong_ptx_arg.benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-lazy-target --ptx /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.ptx";
    try std.testing.expect(!benchmarkHasPromotionEvidence(wrong_ptx_arg));
    try std.testing.expectEqualStrings("wrong_generated_ptx_arg", benchmarkPromotionBlocker(wrong_ptx_arg));

    var wrong_arch = bench;
    wrong_arch.generated_ptx_command = "nvcc -ptx -arch=compute_90 src/ops/cuda/artifacts/inference_cuda_kernels.cu -o /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.ptx";
    try std.testing.expect(!benchmarkHasPromotionEvidence(wrong_arch));
    try std.testing.expectEqualStrings("wrong_ptx_arch", benchmarkPromotionBlocker(wrong_arch));

    var wrong_ptx_extension = bench;
    wrong_ptx_extension.generated_ptx_path = "/tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.ptx.txt";
    try std.testing.expect(!benchmarkHasPromotionEvidence(wrong_ptx_extension));
    try std.testing.expectEqualStrings("missing_generated_ptx_path", benchmarkPromotionBlocker(wrong_ptx_extension));

    var missing_ptx_arg = bench;
    missing_ptx_arg.benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-lazy-target";
    try std.testing.expect(!benchmarkHasPromotionEvidence(missing_ptx_arg));
    try std.testing.expectEqualStrings("benchmark_missing_generated_ptx_arg", benchmarkPromotionBlocker(missing_ptx_arg));

    var missing_repeat = bench;
    missing_repeat.benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-lazy-target --quant-compiler-generated-ptx /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.fatbin --quant-compiler-evidence-out src/ops/cuda/generated/evidence/q4_k_small_batch_bias_gelu_benchmark.json";
    try std.testing.expect(!benchmarkHasPromotionEvidence(missing_repeat));
    try std.testing.expectEqualStrings("missing_benchmark_repeat_runs", benchmarkPromotionBlocker(missing_repeat));

    var wrong_ptx_path_value = bench;
    wrong_ptx_path_value.benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-lazy-target --quant-compiler-generated-ptx /tmp/wrong.fatbin /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.fatbin --quant-compiler-repeat-runs 3 --quant-compiler-evidence-out src/ops/cuda/generated/evidence/q4_k_small_batch_bias_gelu_benchmark.json";
    try std.testing.expect(!benchmarkHasPromotionEvidence(wrong_ptx_path_value));
    try std.testing.expectEqualStrings("benchmark_missing_generated_ptx_path", benchmarkPromotionBlocker(wrong_ptx_path_value));

    var missing_evidence_out = bench;
    missing_evidence_out.benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-lazy-target --quant-compiler-generated-ptx /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.fatbin --quant-compiler-repeat-runs 3";
    try std.testing.expect(!benchmarkHasPromotionEvidence(missing_evidence_out));
    try std.testing.expectEqualStrings("benchmark_missing_evidence_out_arg", benchmarkPromotionBlocker(missing_evidence_out));

    var wrong_evidence_path = bench;
    wrong_evidence_path.benchmark_command = "zig-out/bin/antfly-inference bench-cuda --warmup-iters 5 --measure-iters 50 --quant-compiler-lazy-target --quant-compiler-generated-ptx /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.fatbin --quant-compiler-repeat-runs 3 --quant-compiler-evidence-out /tmp/wrong-evidence.json";
    try std.testing.expect(!benchmarkHasPromotionEvidence(wrong_evidence_path));
    try std.testing.expectEqualStrings("benchmark_missing_evidence_out_path", benchmarkPromotionBlocker(wrong_evidence_path));

    var missing_iters = bench;
    missing_iters.benchmark_command = "zig-out/bin/antfly-inference bench-cuda --quant-compiler-lazy-target --quant-compiler-generated-ptx /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.fatbin";
    try std.testing.expect(!benchmarkHasPromotionEvidence(missing_iters));
    try std.testing.expectEqualStrings("missing_warmup_iters", benchmarkPromotionBlocker(missing_iters));

    var loose_correctness = bench;
    loose_correctness.correctness_tolerance_abs = 0.02;
    try std.testing.expect(!benchmarkHasPromotionEvidence(loose_correctness));
    try std.testing.expectEqualStrings("correctness_tolerance_missing_or_loose", benchmarkPromotionBlocker(loose_correctness));

    var nan_correctness = bench;
    nan_correctness.correctness_tolerance_abs = std.math.nan(f32);
    try std.testing.expect(!benchmarkHasPromotionEvidence(nan_correctness));
    try std.testing.expectEqualStrings("correctness_tolerance_missing_or_loose", benchmarkPromotionBlocker(nan_correctness));

    var no_speedup = bench;
    no_speedup.minimum_speedup = 0.99;
    try std.testing.expect(!benchmarkHasPromotionEvidence(no_speedup));
    try std.testing.expectEqualStrings("speedup_gate_missing", benchmarkPromotionBlocker(no_speedup));

    var nan_min_speedup = bench;
    nan_min_speedup.minimum_speedup = std.math.nan(f64);
    try std.testing.expect(!benchmarkHasPromotionEvidence(nan_min_speedup));
    try std.testing.expectEqualStrings("speedup_gate_missing", benchmarkPromotionBlocker(nan_min_speedup));

    const nan_speedup_evidence = [_]BenchmarkEvidence{.{
        .kernel_id = bench.generated_kernel_id,
        .generated_source_path = bench.generated_source_path,
        .generated_source_fingerprint = bench.generated_source_fingerprint,
        .generated_ptx_path = bench.generated_ptx_path,
        .generated_ptx_command = bench.generated_ptx_command,
        .benchmark_command = bench.benchmark_command,
        .correctness_evidence_path = bench.correctness_evidence_path,
        .benchmark_evidence_path = bench.benchmark_evidence_path,
        .benchmark_mode = bench.benchmark_mode,
        .target_fingerprint = bench.target_fingerprint,
        .repeat_runs = metal_promotion_repeat_runs,
        .correctness_passed = true,
        .benchmark_passed = true,
        .measured_speedup = std.math.nan(f64),
    }};
    try std.testing.expectEqualStrings("speedup_gate_missing", benchmarkPromotionBlockerWithEvidence(bench, &nan_speedup_evidence));
}

test "quant kernel compiler checked-in CUDA promotion evidence is fresh and route eligible" {
    const ExpectedShape = struct { label: []const u8, rows: usize, in_dim: usize, out_dim: usize };
    const mmv_shapes = [_]ExpectedShape{
        .{ .label = "E2B PLE proj", .rows = 1, .in_dim = 256, .out_dim = 1536 },
        .{ .label = "E2B attn out", .rows = 1, .in_dim = 2048, .out_dim = 1536 },
        .{ .label = "E2B FFN down", .rows = 1, .in_dim = 12288, .out_dim = 1536 },
        .{ .label = "E2B FFN up", .rows = 1, .in_dim = 1536, .out_dim = 8960 },
        .{ .label = "E2B LM head", .rows = 1, .in_dim = 1536, .out_dim = 262144 },
    };
    const mm_shapes = [_]ExpectedShape{
        .{ .label = "E2B PLE proj", .rows = 22, .in_dim = 256, .out_dim = 1536 },
        .{ .label = "E2B attn out", .rows = 22, .in_dim = 2048, .out_dim = 1536 },
        .{ .label = "E2B FFN down", .rows = 22, .in_dim = 12288, .out_dim = 1536 },
        .{ .label = "E2B FFN up", .rows = 22, .in_dim = 1536, .out_dim = 8960 },
    };
    const pair_shapes = [_]ExpectedShape{
        .{ .label = "E2B FFN gate+up", .rows = 1, .in_dim = 1536, .out_dim = 8960 },
        .{ .label = "E4B FFN gate+up", .rows = 1, .in_dim = 2560, .out_dim = 10240 },
    };
    const pair_q8_shapes = [_]ExpectedShape{.{ .label = "E4B FFN gate+up q8_1", .rows = 1, .in_dim = 2560, .out_dim = 10240 }};
    const down_q8_shapes = [_]ExpectedShape{.{ .label = "E4B FFN down q8_1", .rows = 1, .in_dim = 10240, .out_dim = 2560 }};
    const expected_shapes = [_][]const ExpectedShape{ &mmv_shapes, &mm_shapes, &pair_shapes, &pair_q8_shapes, &down_q8_shapes };

    try std.testing.expectEqual(first_benchmark_evidence.len, first_cuda_q4_evidence_json.len);
    try std.testing.expectEqual(first_benchmark_evidence.len, first_benchmarks.len - 1);

    for (first_cuda_q4_evidence_json, first_benchmarks[1..], first_benchmark_evidence, expected_shapes) |json, bench, record, expected| {
        var parsed = try std.json.parseFromSlice(CudaQ4EvidenceFile, std.testing.allocator, json, .{ .ignore_unknown_fields = true });
        defer parsed.deinit();
        const evidence = parsed.value;
        const artifact = generatedArtifactForKernel(.cuda, bench.generated_kernel_id) orelse return error.MissingGeneratedArtifact;

        try std.testing.expectEqual(cuda_sm89_promotion_target_fingerprint, bench.target_fingerprint);
        try std.testing.expectEqual(bench.target_fingerprint, record.target_fingerprint);
        try std.testing.expectEqualStrings("antfly.quant_kernel_q4_0_benchmark_evidence.v1", evidence.schema);
        try std.testing.expectEqualStrings(bench.generated_kernel_id, evidence.kernel_id);
        try std.testing.expectEqualStrings(bench.generated_source_path, evidence.generated_source_path);
        try std.testing.expectEqual(bench.generated_source_fingerprint, evidence.generated_source_fingerprint);
        try std.testing.expectEqualStrings(bench.generated_ptx_path, evidence.generated_ptx_path);
        try std.testing.expectEqualStrings(bench.generated_ptx_command, evidence.generated_ptx_command);
        try std.testing.expectEqualStrings(bench.benchmark_command, evidence.benchmark_command);
        try std.testing.expectEqualStrings(bench.correctness_evidence_path, evidence.correctness_evidence_path);
        try std.testing.expectEqualStrings(bench.benchmark_evidence_path, evidence.benchmark_evidence_path);
        try std.testing.expectEqualStrings(bench.benchmark_mode, evidence.benchmark_mode);
        try std.testing.expectEqualStrings(bench.handwritten_baseline, evidence.baseline_kernel);
        try std.testing.expectEqual(artifact.runtime_shape.min_in_dim, evidence.runtime_min_in_dim);
        try std.testing.expectEqual(record.repeat_runs, evidence.repeat_runs);
        try std.testing.expectEqual(bench.production_enabled, evidence.production_enabled);
        try std.testing.expect(evidence.correctness_passed);
        try std.testing.expect(evidence.benchmark_passed);
        try std.testing.expect(evidence.promotion_ready);
        try std.testing.expectEqual(@as(usize, 5), evidence.warmup_iters);
        try std.testing.expectEqual(@as(usize, 50), evidence.measure_iters);
        try std.testing.expectApproxEqAbs(bench.minimum_speedup, evidence.minimum_speedup, 0.000001);
        try std.testing.expectApproxEqAbs(@as(f64, @floatCast(bench.correctness_tolerance_abs)), evidence.correctness_tolerance_abs, 0.000001);
        try std.testing.expectEqual(expected.len, evidence.shapes.len);

        var eligible_count: usize = 0;
        var log_sum: f64 = 0.0;
        var worst = std.math.inf(f64);
        for (evidence.shapes, expected) |shape, expected_shape| {
            try std.testing.expectEqualStrings(expected_shape.label, shape.label);
            try std.testing.expectEqual(expected_shape.rows, shape.rows);
            try std.testing.expectEqual(expected_shape.in_dim, shape.in_dim);
            try std.testing.expectEqual(expected_shape.out_dim, shape.out_dim);
            try std.testing.expect(shape.baseline_ns != 0 and shape.generated_ns != 0);
            const measured = @as(f64, @floatFromInt(shape.baseline_ns)) / @as(f64, @floatFromInt(shape.generated_ns));
            try std.testing.expectApproxEqAbs(measured, shape.speedup, 0.000001);
            const plan = quant_matmul.plan(.{ .rows = shape.rows, .in_dim = shape.in_dim, .out_dim = shape.out_dim, .format = bench.format });
            if (!artifact.runtime_shape.matches(plan)) continue;
            eligible_count += 1;
            log_sum += @log(shape.speedup);
            worst = @min(worst, shape.speedup);
        }
        try std.testing.expect(eligible_count != 0);
        const geomean = @exp(log_sum / @as(f64, @floatFromInt(eligible_count)));
        try std.testing.expect(worst >= bench.minimum_speedup);
        try std.testing.expect(geomean >= bench.minimum_speedup);
        try std.testing.expectApproxEqAbs(geomean, evidence.measured_speedup, 0.00001);
        try std.testing.expectApproxEqAbs(worst, evidence.worst_shape_speedup, 0.000001);
        try std.testing.expectApproxEqAbs(record.measured_speedup, evidence.measured_speedup, 0.000001);
    }
}

test "quant kernel compiler production artifacts require promotion evidence" {
    for (first_generated_matmul_artifacts) |artifact| {
        try std.testing.expectEqual(artifact.production_enabled, artifactHasPromotionEvidence(artifact));
        if (artifact.runtime_default_enabled) {
            try std.testing.expect(artifact.production_enabled);
            try std.testing.expect(artifactHasPromotionEvidence(artifact));
        }
    }

    var cuda_artifact = first_generated_matmul_artifacts[0];
    try std.testing.expect(benchmarkForArtifact(cuda_artifact) != null);
    cuda_artifact.production_enabled = true;
    cuda_artifact.source_path = "src/ops/cuda/artifacts/inference_cuda_kernels.cu";
    try std.testing.expect(benchmarkForArtifact(cuda_artifact) == null);
    try std.testing.expect(!artifactHasPromotionEvidence(cuda_artifact));

    var unbenchmarked = cuda_artifact;
    unbenchmarked.kernel_id = "unbenchmarked_generated_kernel";
    try std.testing.expect(benchmarkForArtifact(unbenchmarked) == null);
    try std.testing.expect(!artifactHasPromotionEvidence(unbenchmarked));

    var unstable_general_metal = generatedArtifactForKernel(.metal, first_general_metal_q4_0_kernel_id).?;
    unstable_general_metal.production_enabled = true;
    try std.testing.expect(!artifactHasPromotionEvidence(unstable_general_metal));
    try std.testing.expectEqualStrings("TERMITE_METAL_ENABLE_ANTFLY_Q4_0_SMALL_BATCH", std.mem.span(artifactRuntimeGateEnv(unstable_general_metal).?));

    var unsupported_baseline_metal = generatedArtifactForKernel(.metal, first_general_metal_q8_relu_kernel_id).?;
    unsupported_baseline_metal.production_enabled = true;
    try std.testing.expect(!artifactHasPromotionEvidence(unsupported_baseline_metal));
    try std.testing.expectEqualStrings("TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH_RELU", std.mem.span(artifactRuntimeGateEnv(unsupported_baseline_metal).?));

    const metal_artifact = generatedArtifactForKernel(.metal, first_general_metal_q6_kernel_id).?;
    try std.testing.expect(benchmarkForArtifact(metal_artifact) == null);
    try std.testing.expect(artifactHasPromotionEvidence(metal_artifact));
    try std.testing.expectEqualStrings("TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH", std.mem.span(artifactRuntimeGateEnv(metal_artifact).?));
    try std.testing.expectEqualStrings("missing_metal_runtime_evidence", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &.{}));

    const metal_evidence = [_]MetalRuntimeEvidence{.{
        .kernel_id = metal_artifact.kernel_id,
        .source_path = first_general_metal_q6_source_path,
        .source_fingerprint = artifactSourceFingerprint(metal_artifact),
        .check_command = first_general_metal_q6_check_command,
        .runtime_evidence_command = metal_artifact.promotion_evidence_command,
        .promotion_check_command = metal_artifact.promotion_check_command,
        .repeat_runs = metal_promotion_repeat_runs,
        .correctness_passed = true,
        .generated_route_checked = true,
        .provider_route_checked = true,
        .benchmark_passed = true,
        .measured_speedup = metal_promotion_min_speedup + 0.01,
        .minimum_repeat_speedup = metal_promotion_min_speedup + 0.01,
        .production_enabled = true,
        .promotion_ready = true,
        .provenance_status = metal_evidence_provenance_attested_v1,
        .provenance_blocker = "",
        .source_commit = "0123456789abcdef0123456789abcdef01234567",
        .source_tree_clean = true,
        .source_status_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        .host_os = "test-os",
        .host_arch = "test-arch",
        .accelerator_name = "test-metal-device",
        .metal_compiler_version = "test-metal-compiler",
        .zig_version = "test-zig",
        .recorded_at_utc = "2026-07-13T00:00:00Z",
    }};
    try std.testing.expectEqualStrings("none", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &metal_evidence));

    var missing_route_evidence = metal_evidence;
    missing_route_evidence[0].generated_route_checked = false;
    try std.testing.expectEqualStrings("missing_metal_generated_route_evidence", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &missing_route_evidence));

    var stale_source_evidence = metal_evidence;
    stale_source_evidence[0].source_fingerprint = 0;
    try std.testing.expectEqualStrings("missing_metal_runtime_evidence", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &stale_source_evidence));

    var wrong_source_path_evidence = metal_evidence;
    wrong_source_path_evidence[0].source_path = first_lazy_metal_source_path;
    try std.testing.expectEqualStrings("missing_metal_runtime_evidence", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &wrong_source_path_evidence));

    var wrong_check_command_evidence = metal_evidence;
    wrong_check_command_evidence[0].check_command = first_lazy_metal_check_command;
    try std.testing.expectEqualStrings("missing_metal_runtime_evidence", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &wrong_check_command_evidence));

    const wrong_promotion_path_command = try std.fmt.allocPrint(
        std.testing.allocator,
        "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out /private/tmp/antfly-quant-metal-{s}-wrong-promotion-evidence.json" ++ metal_promotion_args ++ "{s}",
        .{ metal_artifact.kernel_id, metal_artifact.kernel_id },
    );
    defer std.testing.allocator.free(wrong_promotion_path_command);
    var mismatched_promotion_path = metal_artifact;
    mismatched_promotion_path.promotion_evidence_command = wrong_promotion_path_command;
    try std.testing.expectEqualStrings("metal_promotion_evidence_path_mismatch", metalArtifactPromotionBlockerWithEvidence(mismatched_promotion_path, &metal_evidence));

    const invalid_promotion_path_command = try std.fmt.allocPrint(
        std.testing.allocator,
        "zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false -- --evidence-out /tmp/wrong.json --repeat-runs " ++ metal_promotion_repeat_runs_text ++ " --promotion-ready-kernel {s}",
        .{metal_artifact.kernel_id},
    );
    defer std.testing.allocator.free(invalid_promotion_path_command);
    var mismatched_promotion_command = metal_artifact;
    mismatched_promotion_command.promotion_evidence_command = invalid_promotion_path_command;
    try std.testing.expectEqualStrings("metal_promotion_evidence_command_mismatch", metalArtifactPromotionBlockerWithEvidence(mismatched_promotion_command, &metal_evidence));

    var missing_promotion_command = metal_artifact;
    missing_promotion_command.promotion_evidence_command = "";
    try std.testing.expectEqualStrings("metal_promotion_evidence_command_mismatch", metalArtifactPromotionBlockerWithEvidence(missing_promotion_command, &metal_evidence));

    const wrong_promotion_kernel_command = first_metal_promotion_evidence_command ++ first_general_metal_q6_promotion_evidence_path ++ metal_promotion_args ++ "wrong_kernel";
    var wrong_promotion_kernel = metal_artifact;
    wrong_promotion_kernel.promotion_evidence_command = wrong_promotion_kernel_command;
    try std.testing.expectEqualStrings("wrong_metal_promotion_ready_kernel", metalArtifactPromotionBlockerWithEvidence(wrong_promotion_kernel, &metal_evidence));

    var dev_command_evidence = metal_evidence;
    dev_command_evidence[0].runtime_evidence_command = metal_artifact.runtime_evidence_command;
    try std.testing.expectEqualStrings("missing_metal_runtime_evidence", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &dev_command_evidence));

    const loose_repeat_command = try std.fmt.allocPrint(
        std.testing.allocator,
        "{s}0 --promotion-ready-kernel {s}",
        .{ metal_artifact.runtime_evidence_command, metal_artifact.kernel_id },
    );
    defer std.testing.allocator.free(loose_repeat_command);
    var loose_repeat_evidence = metal_evidence;
    loose_repeat_evidence[0].runtime_evidence_command = loose_repeat_command;
    try std.testing.expectEqualStrings("missing_metal_runtime_evidence", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &loose_repeat_evidence));

    var disabled_metal_artifact = metal_artifact;
    disabled_metal_artifact.production_enabled = false;
    try std.testing.expectEqualStrings("awaiting_metal_promotion_evidence", metalArtifactPromotionBlockerWithEvidence(disabled_metal_artifact, &metal_evidence));

    var single_repeat_evidence = metal_evidence;
    single_repeat_evidence[0].repeat_runs = 1;
    try std.testing.expectEqualStrings("insufficient_metal_repeats", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &single_repeat_evidence));

    var weak_average_evidence = metal_evidence;
    weak_average_evidence[0].measured_speedup = metal_promotion_min_speedup - 0.01;
    weak_average_evidence[0].minimum_repeat_speedup = metal_promotion_min_speedup - 0.02;
    try std.testing.expectEqualStrings("speedup_gate_missing", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &weak_average_evidence));

    var unstable_repeat_evidence = metal_evidence;
    unstable_repeat_evidence[0].measured_speedup = metal_promotion_min_speedup + 0.2;
    unstable_repeat_evidence[0].minimum_repeat_speedup = metal_promotion_min_speedup - 0.01;
    try std.testing.expectEqualStrings("unstable_benchmark_timing", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &unstable_repeat_evidence));

    var failed_benchmark_flag_evidence = metal_evidence;
    failed_benchmark_flag_evidence[0].benchmark_passed = false;
    try std.testing.expectEqualStrings("benchmark_evidence_failed", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &failed_benchmark_flag_evidence));

    var not_ready_evidence = metal_evidence;
    not_ready_evidence[0].promotion_ready = false;
    try std.testing.expectEqualStrings("metal_promotion_not_ready", metalArtifactPromotionBlockerWithEvidence(metal_artifact, &not_ready_evidence));

    var wrong_backend = cuda_artifact;
    wrong_backend.backend = .metal;
    try std.testing.expect(benchmarkForArtifact(wrong_backend) == null);
    try std.testing.expect(!artifactHasPromotionEvidence(wrong_backend));
}

test "quant kernel compiler separates qualification from runtime rollout" {
    const cases = [_]struct {
        kernel_id: []const u8,
        runtime_gate_env: []const u8,
    }{
        .{ .kernel_id = first_general_cuda_q4_0_mmv_kernel_id, .runtime_gate_env = "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MMV" },
        .{ .kernel_id = first_general_cuda_q4_0_mm_kernel_id, .runtime_gate_env = "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_MM" },
        .{ .kernel_id = first_general_cuda_q4_0_pair_kernel_id, .runtime_gate_env = "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR" },
        .{ .kernel_id = first_general_cuda_q4_0_pair_q8_kernel_id, .runtime_gate_env = "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_PAIR_Q8" },
        .{ .kernel_id = first_general_cuda_q4_0_down_q8_kernel_id, .runtime_gate_env = "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_DOWN_Q8" },
    };
    for (cases) |case| {
        const artifact = generatedArtifactForKernel(.cuda, case.kernel_id) orelse return error.MissingGeneratedArtifact;
        try std.testing.expect(artifact.production_enabled);
        try std.testing.expect(artifactHasPromotionEvidence(artifact));
        try std.testing.expect(!artifact.runtime_default_enabled);
        try std.testing.expectEqualStrings(case.runtime_gate_env, std.mem.span(artifactRuntimeGateEnv(artifact).?));

        const compiled = compileQuantKernelArtifactSource(artifact) orelse return error.GeneratedArtifactCompileFailed;
        try std.testing.expectEqualStrings(case.runtime_gate_env, std.mem.span(compiled.runtime_gate_env.?));
        try std.testing.expect(compiledSourceMatchesRoute(compiled));
    }

    const metal_qualified = generatedArtifactForKernel(.metal, first_general_metal_q6_kernel_id) orelse return error.MissingGeneratedArtifact;
    try std.testing.expect(metal_qualified.production_enabled);
    try std.testing.expect(artifactHasPromotionEvidence(metal_qualified));
    try std.testing.expect(!metal_qualified.runtime_default_enabled);
    try std.testing.expectEqualStrings("TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH", std.mem.span(artifactRuntimeGateEnv(metal_qualified).?));
}

test "quant kernel compiler legacy Metal evidence cannot enable runtime defaults" {
    var artifact = generatedArtifactForKernel(.metal, first_general_metal_q6_kernel_id) orelse return error.MissingGeneratedArtifact;
    try std.testing.expect(metalRuntimeDefaultHasAttestedEvidence(artifact, &first_metal_runtime_evidence));

    artifact.runtime_default_enabled = true;
    try std.testing.expect(!metalRuntimeDefaultHasAttestedEvidence(artifact, &first_metal_runtime_evidence));

    var evidence = checkedInMetalEvidenceForKernel(artifact.kernel_id) orelse return error.MissingMetalRuntimeEvidence;
    evidence.provenance_status = metal_evidence_provenance_attested_v1;
    evidence.provenance_blocker = "";
    evidence.legacy_production_exception = false;
    evidence.source_commit = "0123456789abcdef0123456789abcdef01234567";
    evidence.source_tree_clean = true;
    evidence.source_status_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    evidence.host_os = "test-os";
    evidence.host_arch = "test-arch";
    evidence.accelerator_name = "test-metal-device";
    evidence.metal_compiler_version = "test-metal-compiler";
    evidence.zig_version = "test-zig";
    evidence.recorded_at_utc = "2026-07-13T00:00:00Z";
    try std.testing.expect(metalRuntimeDefaultHasAttestedEvidence(artifact, &.{evidence}));
}

test "quant kernel compiler promotes the capability-gated Metal flash hd512 default" {
    const artifact = generatedRegistryArtifactForKernel(.metal, first_prefill_flash_hd512_metal_kernel_id) orelse return error.MissingGeneratedArtifact;
    try std.testing.expect(artifact.runtime_default_enabled);
    try std.testing.expect(artifact.production_enabled);
    const attention = artifact.attentionOp() orelse return error.MissingGeneratedArtifact;
    try std.testing.expectEqual(AttentionKind.prefill_flash, attention.kind);
    try std.testing.expectEqual(@as(u16, 512), attention.head_dim);
}

test "quant kernel compiler generated artifacts have unique ids and paths" {
    try validateGeneratedArtifactRegistry();
    for (first_generated_artifacts, 0..) |artifact, i| {
        try std.testing.expect(artifact.kernel_id.len != 0);
        try std.testing.expect(artifact.source_path.len != 0);
        try std.testing.expect(generatedSourceForArtifact(artifact) != null);
        for (first_generated_artifacts[i + 1 ..]) |other| {
            try std.testing.expect(!std.mem.eql(u8, artifact.source_path, other.source_path));
            try std.testing.expect(!std.mem.eql(u8, artifact.kernel_id, other.kernel_id));
        }
    }

    for (first_generated_matmul_artifacts, 0..) |artifact, i| {
        const lookup = generatedArtifactForCandidate(artifact.backend, artifact.format, artifact.row_bucket, artifact.epilogue) orelse return error.MissingGeneratedArtifactLookup;
        try std.testing.expectEqual(artifact.backend, lookup.backend);
        try std.testing.expectEqual(artifact.format, lookup.format);
        try std.testing.expectEqual(artifact.row_bucket, lookup.row_bucket);
        try std.testing.expectEqual(artifact.epilogue, lookup.epilogue);
        const kernel_lookup = generatedArtifactForKernel(artifact.backend, artifact.kernel_id) orelse return error.MissingGeneratedArtifactLookup;
        try std.testing.expectEqualStrings(artifact.source_path, kernel_lookup.source_path);
        for (first_generated_matmul_artifacts[i + 1 ..]) |other| {
            try std.testing.expect(!std.mem.eql(u8, artifact.source_path, other.source_path));
            try std.testing.expect(!std.mem.eql(u8, artifact.kernel_id, other.kernel_id));
            if (artifact.backend == other.backend and
                artifact.format == other.format and
                artifact.row_bucket == other.row_bucket and
                artifact.epilogue == other.epilogue and
                artifact.activation == other.activation and
                artifact.function == other.function and
                artifact.output == other.output)
            {
                const artifact_plan = cudaRenderPlanForArtifact(artifact) orelse return error.DuplicateRouteWithoutCudaPlan;
                const other_plan = cudaRenderPlanForArtifact(other) orelse return error.DuplicateRouteWithoutCudaPlan;
                // Dispatch determinism: same-semantics candidates must differ
                // either in exact shape or in their typed runtime opt-in gate.
                // The GGML-Q8_1 layout suite shares the legacy Q8_1 semantic
                // ABI at identical shapes but is reachable only through its
                // own gate (and its dedicated catalog resolver), so a distinct
                // gate is an equally unambiguous discriminator.
                const artifact_gate = artifactCandidateOptInGateEnv(artifact);
                const other_gate = artifactCandidateOptInGateEnv(other);
                const distinct_gates = artifact_gate != null and other_gate != null and
                    !std.mem.eql(u8, std.mem.span(artifact_gate.?), std.mem.span(other_gate.?));
                try std.testing.expect(distinct_gates or
                    artifact_plan.launch.input_dim.fixed != other_plan.launch.input_dim.fixed or
                    artifact_plan.launch.output_dim.fixed != other_plan.launch.output_dim.fixed);
            }
        }
    }
}

test "quant kernel compiler generated artifact manifest maps to route candidates" {
    for (first_generated_matmul_artifacts) |artifact| {
        try std.testing.expect(artifactSourceFingerprint(artifact) != 0);
        try std.testing.expect(artifact.check_command.len != 0);
        try std.testing.expect(std.mem.containsAtLeast(u8, artifact.check_command, 1, artifact.source_path));

        const compiled = compileQuantKernelArtifactSource(artifact) orelse return error.MissingCompiledQuantKernelSource;
        const route = compiled.lowering;
        if (artifact.production_enabled) {
            try std.testing.expectEqual(LoweringRoute.generated_production, route.production_route);
            try std.testing.expectEqual(LoweringRoute.unsupported, route.candidate_route);
            try std.testing.expectEqual(FallbackReason.none, route.fallback_reason);
            try std.testing.expectEqualStrings(artifact.kernel_id, route.production_kernel_id);
            try std.testing.expectEqualStrings("", route.kernel_id);
            try std.testing.expectEqualStrings("", route.candidate_source_path);
            const promoted = promotedArtifactFor(route) orelse return error.MissingPromotedArtifact;
            try std.testing.expectEqualStrings(artifact.source_path, promoted.source_path);
        } else {
            try std.testing.expectEqual(LoweringRoute.handwritten_production, route.production_route);
            try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, route.candidate_route);
            const expected_fallback: FallbackReason = if (artifact.backend == .metal and checkedInMetalEvidenceForKernel(artifact.kernel_id) != null)
                .generated_runtime_not_wired
            else if (artifact.backend == .cuda and artifactRuntimeWired(artifact))
                .generated_runtime_not_wired
            else
                .generated_artifact_missing;
            try std.testing.expectEqual(expected_fallback, route.fallback_reason);
            try std.testing.expectEqualStrings(artifact.kernel_id, route.kernel_id);
            try std.testing.expectEqualStrings(artifact.source_path, route.candidate_source_path);
            try std.testing.expectEqual(artifactSourceFingerprint(artifact), candidateSourceFingerprint(route));
            const candidate_schedule = candidateScheduleFor(route);
            const expected_tile_cols: usize = if (artifact.backend == .metal)
                metalGeneratedColsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue)
            else
                (cudaScheduleForArtifact(artifact) orelse return error.MissingCudaRenderPlan).tile_cols;
            const expected_threads_per_block: usize = if (artifact.backend == .metal)
                metalGeneratedThreadsPerThreadgroup(artifact.format, artifact.row_bucket, artifact.epilogue)
            else
                (cudaScheduleForArtifact(artifact) orelse return error.MissingCudaRenderPlan).threads_per_block;
            try std.testing.expectEqual(@as(usize, 1), candidate_schedule.tile_rows);
            try std.testing.expectEqual(expected_tile_cols, candidate_schedule.tile_cols);
            try std.testing.expectEqual(expected_threads_per_block, candidate_schedule.threads_per_block);
        }

        const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, artifact.source_path, std.testing.allocator, .limited(128 * 1024));
        defer std.testing.allocator.free(contents);
        const emitted = try emitCompiledSource(std.testing.allocator, compiled);
        defer emitted.deinit(std.testing.allocator);
        try std.testing.expectEqualStrings(emitted.data, contents);
        try std.testing.expectEqualStrings(artifact.kernel_id, compiled.artifact.kernel_id);
        try std.testing.expect(try compiledSourceHeaderMatchesSource(std.testing.allocator, compiled, emitted.data));
        switch (artifact.backend) {
            .cuda => {
                try std.testing.expect(std.mem.containsAtLeast(u8, artifact.check_command, 1, "nvcc -fatbin"));
                try std.testing.expect(std.mem.containsAtLeast(u8, artifact.check_command, 1, ".fatbin"));
                if (std.mem.eql(u8, artifact.kernel_id, first_lazy_benchmark.generated_kernel_id)) {
                    try std.testing.expectEqualStrings(first_lazy_benchmark.benchmark_command, artifact.runtime_evidence_command);
                    try std.testing.expectEqualStrings(first_lazy_benchmark_check_command, artifact.promotion_check_command);
                } else if (artifact.runtime_evidence_command.len != 0) {
                    try std.testing.expect(std.mem.containsAtLeast(u8, artifact.runtime_evidence_command, 1, "bench-cuda"));
                    try std.testing.expect(std.mem.containsAtLeast(u8, artifact.runtime_evidence_command, 1, artifact.kernel_id));
                } else {
                    try std.testing.expect(!artifact.production_enabled);
                    if (cudaRuntimeWiredDevCandidate(artifact)) {
                        try std.testing.expect(artifactRuntimeGateEnv(artifact) != null);
                        try std.testing.expectEqualStrings("awaiting_cuda_promotion_evidence", artifactPromotionBlocker(artifact));
                    } else {
                        try std.testing.expect(!artifactRuntimeWired(artifact));
                    }
                }
            },
            .metal => {
                try std.testing.expect(std.mem.containsAtLeast(u8, artifact.check_command, 1, "xcrun --toolchain Metal metal -c"));
                try std.testing.expect(std.mem.containsAtLeast(u8, artifact.check_command, 1, ".air"));
                try std.testing.expect(std.mem.containsAtLeast(u8, artifact.runtime_evidence_command, 1, "--repeat-runs " ++ metal_promotion_repeat_runs_text));
                try std.testing.expectEqualStrings(first_metal_runtime_evidence_command, artifact.runtime_evidence_command);
                try std.testing.expect(std.mem.containsAtLeast(u8, artifact.promotion_check_command, 1, first_metal_promotion_check_command));
                try std.testing.expect(commandHasArgValue(artifact.promotion_check_command, "--require-kernel", artifact.kernel_id));
                try std.testing.expect(commandHasArgValue(artifact.promotion_evidence_command, "--measure-iters", metal_promotion_measure_iters_text));
            },
        }
    }
}

test "quant kernel compiler generated production routes require promoted artifacts" {
    for (first_registry.entries) |entry| {
        if (entry.production_route != .generated_production) continue;
        try std.testing.expectEqual(LoweringRoute.unsupported, entry.candidate_route);
        try std.testing.expectEqual(FallbackReason.none, entry.fallback_reason);
        const artifact = promotedArtifactFor(entry) orelse return error.MissingPromotedArtifact;
        try std.testing.expect(artifactHasPromotionEvidence(artifact));
    }

    var unpromoted = loweringFor(.cuda, .q4_k, .rows_2_8, .bias_gelu);
    unpromoted.production_route = .generated_production;
    unpromoted.production_kernel_id = first_lazy_benchmark.generated_kernel_id;
    unpromoted.candidate_route = .unsupported;
    unpromoted.fallback_reason = .none;
    try std.testing.expect(promotedArtifactFor(unpromoted) == null);

    var dev_route = loweringFor(.metal, .q4_k, .rows_2_8, .bias_gelu);
    dev_route.production_route = .handwritten_production;
    dev_route.candidate_route = .generated_dev_candidate;
    dev_route.production_kernel_id = "metal_handwritten_quant_matmul";
    dev_route.fallback_reason = .generated_artifact_missing;
    dev_route.kernel_id = first_lazy_metal_kernel_id;
    dev_route.candidate_source_path = first_lazy_metal_source_path;
    const promoted_artifact = generatedArtifactForKernel(.metal, first_lazy_metal_kernel_id) orelse
        return error.MissingGeneratedArtifact;
    const promoted = promotedLoweringForArtifact(dev_route, promoted_artifact).?;
    try std.testing.expectEqual(LoweringRoute.generated_production, promoted.production_route);
    try std.testing.expectEqual(LoweringRoute.unsupported, promoted.candidate_route);
    try std.testing.expectEqual(FallbackReason.none, promoted.fallback_reason);
    try std.testing.expectEqualStrings(promoted_artifact.kernel_id, promoted.production_kernel_id);
    try std.testing.expectEqualStrings("", promoted.kernel_id);
    try std.testing.expectEqualStrings("", promoted.candidate_source_path);

    var mismatched_artifact = promoted_artifact;
    mismatched_artifact.kernel_id = "antfly_other_kernel";
    try std.testing.expect(promotedLoweringForArtifact(dev_route, mismatched_artifact) == null);
}

test "quant kernel compiler registry helper is the dispatch-facing route source" {
    const planned = registryLoweringFor(.cuda, .q4_k, .rows_2_8, .bias_gelu, .small_batch);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, planned.candidate_route);
    try std.testing.expectEqualStrings(first_lazy_benchmark.generated_kernel_id, planned.kernel_id);
    try std.testing.expectEqualStrings(first_lazy_benchmark.generated_source_path, planned.candidate_source_path);

    const metal_q4_0 = registryLoweringFor(.metal, .q4_0, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q4_0.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q4_0.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q4_0.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q4_0_kernel_id, metal_q4_0.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q4_0_source_path, metal_q4_0.candidate_source_path);

    const metal_q4_1 = registryLoweringFor(.metal, .q4_1, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q4_1.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q4_1.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q4_1.fallback_reason);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", metal_q4_1.production_kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q4_1_kernel_id, metal_q4_1.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q4_1_source_path, metal_q4_1.candidate_source_path);

    const metal_q5_0 = registryLoweringFor(.metal, .q5_0, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q5_0.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q5_0.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q5_0.fallback_reason);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", metal_q5_0.production_kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q5_0_kernel_id, metal_q5_0.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q5_0_source_path, metal_q5_0.candidate_source_path);

    const metal_q5_1 = registryLoweringFor(.metal, .q5_1, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q5_1.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q5_1.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q5_1.fallback_reason);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", metal_q5_1.production_kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q5_1_kernel_id, metal_q5_1.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q5_1_source_path, metal_q5_1.candidate_source_path);

    const metal_q4 = registryLoweringFor(.metal, .q4_k, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.generated_production, metal_q4.production_route);
    try std.testing.expectEqual(LoweringRoute.unsupported, metal_q4.candidate_route);
    try std.testing.expectEqual(FallbackReason.none, metal_q4.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q4_kernel_id, metal_q4.production_kernel_id);
    try std.testing.expectEqualStrings("", metal_q4.kernel_id);
    try std.testing.expectEqualStrings("", metal_q4.candidate_source_path);

    const metal_q4_bias = registryLoweringFor(.metal, .q4_k, .rows_2_8, .bias, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q4_bias.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q4_bias.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q4_bias.fallback_reason);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", metal_q4_bias.production_kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q4_bias_kernel_id, metal_q4_bias.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q4_bias_source_path, metal_q4_bias.candidate_source_path);

    const metal_q8 = registryLoweringFor(.metal, .q8_0, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.generated_production, metal_q8.production_route);
    try std.testing.expectEqual(LoweringRoute.unsupported, metal_q8.candidate_route);
    try std.testing.expectEqual(FallbackReason.none, metal_q8.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q8_kernel_id, metal_q8.production_kernel_id);
    try std.testing.expectEqualStrings("", metal_q8.kernel_id);
    try std.testing.expectEqualStrings("", metal_q8.candidate_source_path);

    const metal_q8_bias = registryLoweringFor(.metal, .q8_0, .rows_2_8, .bias, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q8_bias.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q8_bias.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q8_bias.fallback_reason);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", metal_q8_bias.production_kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q8_bias_kernel_id, metal_q8_bias.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q8_bias_source_path, metal_q8_bias.candidate_source_path);

    const metal_q8_bias_gelu = registryLoweringFor(.metal, .q8_0, .rows_2_8, .bias_gelu, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q8_bias_gelu.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q8_bias_gelu.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q8_bias_gelu.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q8_bias_gelu_kernel_id, metal_q8_bias_gelu.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q8_bias_gelu_source_path, metal_q8_bias_gelu.candidate_source_path);

    const metal_q8_relu = registryLoweringFor(.metal, .q8_0, .rows_2_8, .relu, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q8_relu.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q8_relu.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q8_relu.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q8_relu_kernel_id, metal_q8_relu.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q8_relu_source_path, metal_q8_relu.candidate_source_path);

    const metal_q2 = registryLoweringFor(.metal, .q2_k, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.generated_production, metal_q2.production_route);
    try std.testing.expectEqual(LoweringRoute.unsupported, metal_q2.candidate_route);
    try std.testing.expectEqual(FallbackReason.none, metal_q2.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q2_kernel_id, metal_q2.production_kernel_id);
    try std.testing.expectEqualStrings("", metal_q2.kernel_id);
    try std.testing.expectEqualStrings("", metal_q2.candidate_source_path);

    const metal_q2_bias = registryLoweringFor(.metal, .q2_k, .rows_2_8, .bias, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q2_bias.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q2_bias.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q2_bias.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q2_bias_kernel_id, metal_q2_bias.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q2_bias_source_path, metal_q2_bias.candidate_source_path);

    const metal_q2_bias_gelu = registryLoweringFor(.metal, .q2_k, .rows_2_8, .bias_gelu, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q2_bias_gelu.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q2_bias_gelu.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q2_bias_gelu.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q2_bias_gelu_kernel_id, metal_q2_bias_gelu.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q2_bias_gelu_source_path, metal_q2_bias_gelu.candidate_source_path);

    const metal_q3 = registryLoweringFor(.metal, .q3_k, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.generated_production, metal_q3.production_route);
    try std.testing.expectEqual(LoweringRoute.unsupported, metal_q3.candidate_route);
    try std.testing.expectEqual(FallbackReason.none, metal_q3.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q3_kernel_id, metal_q3.production_kernel_id);
    try std.testing.expectEqualStrings("", metal_q3.kernel_id);
    try std.testing.expectEqualStrings("", metal_q3.candidate_source_path);

    const metal_q3_bias = registryLoweringFor(.metal, .q3_k, .rows_2_8, .bias, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q3_bias.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q3_bias.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q3_bias.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q3_bias_kernel_id, metal_q3_bias.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q3_bias_source_path, metal_q3_bias.candidate_source_path);

    const metal_q3_bias_gelu = registryLoweringFor(.metal, .q3_k, .rows_2_8, .bias_gelu, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q3_bias_gelu.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q3_bias_gelu.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q3_bias_gelu.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q3_bias_gelu_kernel_id, metal_q3_bias_gelu.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q3_bias_gelu_source_path, metal_q3_bias_gelu.candidate_source_path);

    const metal_q8_1 = registryLoweringFor(.metal, .q8_1, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q8_1.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q8_1.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q8_1.fallback_reason);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", metal_q8_1.production_kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q8_1_kernel_id, metal_q8_1.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q8_1_source_path, metal_q8_1.candidate_source_path);

    const metal_q8_k = registryLoweringFor(.metal, .q8_k, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.generated_production, metal_q8_k.production_route);
    try std.testing.expectEqual(LoweringRoute.unsupported, metal_q8_k.candidate_route);
    try std.testing.expectEqual(FallbackReason.none, metal_q8_k.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q8_k_kernel_id, metal_q8_k.production_kernel_id);
    try std.testing.expectEqualStrings("", metal_q8_k.kernel_id);
    try std.testing.expectEqualStrings("", metal_q8_k.candidate_source_path);

    const metal_q5 = registryLoweringFor(.metal, .q5_k, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.generated_production, metal_q5.production_route);
    try std.testing.expectEqual(LoweringRoute.unsupported, metal_q5.candidate_route);
    try std.testing.expectEqual(FallbackReason.none, metal_q5.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q5_kernel_id, metal_q5.production_kernel_id);
    try std.testing.expectEqualStrings("", metal_q5.kernel_id);
    try std.testing.expectEqualStrings("", metal_q5.candidate_source_path);

    const metal_q5_bias = registryLoweringFor(.metal, .q5_k, .rows_2_8, .bias, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q5_bias.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q5_bias.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q5_bias.fallback_reason);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", metal_q5_bias.production_kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q5_bias_kernel_id, metal_q5_bias.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q5_bias_source_path, metal_q5_bias.candidate_source_path);
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q5_bias_source, 1, "threadgroup float partial[32];"));
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q5_bias_source, 1, "if (simdgroup_id == 0u && lane_id >= 4u) partial[lane_id] = 0.0f;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q5_bias_source, 1, "simdgroup_index_in_threadgroup"));

    const metal_q5_bias_gelu = registryLoweringFor(.metal, .q5_k, .rows_2_8, .bias_gelu, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q5_bias_gelu.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q5_bias_gelu.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q5_bias_gelu.fallback_reason);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", metal_q5_bias_gelu.production_kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q5_bias_gelu_kernel_id, metal_q5_bias_gelu.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q5_bias_gelu_source_path, metal_q5_bias_gelu.candidate_source_path);
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q5_bias_gelu_source, 1, "threadgroup float partial[32];"));
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q5_bias_gelu_source, 1, "if (simdgroup_id == 0u && lane_id >= 4u) partial[lane_id] = 0.0f;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q5_bias_gelu_source, 1, "simdgroup_index_in_threadgroup"));

    const metal_q6 = registryLoweringFor(.metal, .q6_k, .rows_2_8, .none, .small_batch);
    try std.testing.expectEqual(LoweringRoute.generated_production, metal_q6.production_route);
    try std.testing.expectEqual(LoweringRoute.unsupported, metal_q6.candidate_route);
    try std.testing.expectEqual(FallbackReason.none, metal_q6.fallback_reason);
    try std.testing.expectEqualStrings(first_general_metal_q6_kernel_id, metal_q6.production_kernel_id);
    try std.testing.expectEqualStrings("", metal_q6.kernel_id);
    try std.testing.expectEqualStrings("", metal_q6.candidate_source_path);

    const metal_q6_bias = registryLoweringFor(.metal, .q6_k, .rows_2_8, .bias, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q6_bias.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q6_bias.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q6_bias.fallback_reason);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", metal_q6_bias.production_kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q6_bias_kernel_id, metal_q6_bias.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q6_bias_source_path, metal_q6_bias.candidate_source_path);
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q6_bias_source, 1, "const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q6_bias_source, 1, "float acc[4][2]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q6_bias_source, 1, "simd_sum(acc[c][rr])"));

    const metal_q6_bias_gelu = registryLoweringFor(.metal, .q6_k, .rows_2_8, .bias_gelu, .small_batch);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, metal_q6_bias_gelu.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal_q6_bias_gelu.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, metal_q6_bias_gelu.fallback_reason);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", metal_q6_bias_gelu.production_kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q6_bias_gelu_kernel_id, metal_q6_bias_gelu.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q6_bias_gelu_source_path, metal_q6_bias_gelu.candidate_source_path);
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q6_bias_gelu_source, 1, "const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q6_bias_gelu_source, 1, "float acc[4][2]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, first_general_metal_q6_bias_gelu_source, 1, "antfly_qk_gelu(total + bias[col])"));

    const miss = registryLoweringFor(.cuda, .unknown, .rows_2_8, .bias_gelu, .small_batch);
    try std.testing.expectEqual(LoweringRoute.unsupported, miss.production_route);
    try std.testing.expectEqual(FallbackReason.unsupported_format, miss.fallback_reason);
    try std.testing.expectEqual(quant_matmul.DispatchKind.small_batch, miss.plan_id.dispatch);
    try std.testing.expectEqual(quant_matmul.DispatchKind.small_batch, miss.schedule.dispatch);

    const format_miss_with_bad_shape = registryLoweringFor(.cuda, .unknown, .rows_0, .bias_gelu, .small_batch);
    try std.testing.expectEqual(LoweringRoute.unsupported, format_miss_with_bad_shape.production_route);
    try std.testing.expectEqual(FallbackReason.unsupported_format, format_miss_with_bad_shape.fallback_reason);
    try std.testing.expectEqual(quant_matmul.DispatchKind.small_batch, format_miss_with_bad_shape.plan_id.dispatch);
}

test "quant kernel compiler registry rejects mismatched dispatch keys" {
    const miss = registryLoweringFor(.cuda, .q4_k, .rows_2_8, .bias_gelu, .mm);
    try std.testing.expectEqual(LoweringRoute.unsupported, miss.production_route);
    try std.testing.expectEqual(LoweringRoute.unsupported, miss.candidate_route);
    try std.testing.expectEqual(FallbackReason.unsupported_shape, miss.fallback_reason);
    try std.testing.expectEqual(quant_matmul.DispatchKind.mm, miss.plan_id.dispatch);
    try std.testing.expectEqual(quant_matmul.DispatchKind.mm, miss.schedule.dispatch);
    try std.testing.expectEqual(@as(usize, 0), miss.schedule.tile_rows);
    try std.testing.expectEqualStrings("", miss.production_kernel_id);
}

test "quant kernel compiler first CUDA candidate stays dev-only but checked in" {
    try std.testing.expect(!first_lazy_benchmark.production_enabled);
    try std.testing.expect(!std.mem.containsAtLeast(u8, first_lazy_benchmark.generated_source_path, 1, "/artifacts/"));

    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, first_lazy_benchmark.generated_source_path, std.testing.allocator, .limited(128 * 1024));
    defer std.testing.allocator.free(contents);
    const emitted = try emitFirstLazyCudaSource(std.testing.allocator);
    defer std.testing.allocator.free(emitted);

    try std.testing.expectEqualStrings(emitted, contents);

    const route = loweringFor(.cuda, .q4_k, .rows_2_8, .bias_gelu);
    try std.testing.expectEqualStrings(first_lazy_benchmark.generated_source_path, route.candidate_source_path);
    try std.testing.expectEqual(@as(usize, 2), route.schedule.tile_rows);
    try std.testing.expectEqual(@as(usize, 4), route.schedule.tile_cols);
    try std.testing.expectEqual(@as(usize, 256), route.schedule.threads_per_block);

    const candidate_schedule = candidateScheduleFor(route);
    try std.testing.expectEqual(@as(usize, 1), candidate_schedule.tile_rows);
    try std.testing.expectEqual(@as(usize, 1), candidate_schedule.tile_cols);
    try std.testing.expectEqual(@as(usize, 128), candidate_schedule.threads_per_block);
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "if (blockDim.x != 128) return;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "__shared__ float partial[128];"));
}

test "quant kernel compiler first Metal lazy target stays blocked by timing drift evidence" {
    try std.testing.expect(!std.mem.containsAtLeast(u8, first_lazy_metal_source_path, 1, "/artifacts/"));

    const route = loweringFor(.metal, .q4_k, .rows_2_8, .bias_gelu);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, route.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, route.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_artifact_missing, route.fallback_reason);
    try std.testing.expectEqualStrings("metal_handwritten_quant_matmul", route.production_kernel_id);
    try std.testing.expectEqualStrings(first_lazy_metal_kernel_id, route.kernel_id);
    try std.testing.expectEqualStrings(first_lazy_metal_source_path, route.candidate_source_path);

    const artifact = generatedArtifactForKernel(.metal, first_lazy_metal_kernel_id).?;
    try std.testing.expect(artifactRuntimeWired(artifact));
    try std.testing.expect(artifactHasMetalProviderRouteEvidence(artifact));
    try std.testing.expect(!artifactHasPromotionEvidence(artifact));
    try std.testing.expectEqualStrings("unstable_benchmark_timing", artifactPromotionBlocker(artifact));
    try std.testing.expectEqualStrings("TERMITE_METAL_ENABLE_ANTFLY_Q4_K_SMALL_BATCH_BIAS_GELU", std.mem.span(artifactRuntimeGateEnv(artifact).?));

    const counters = countersForLowering(route);
    try std.testing.expectEqual(@as(usize, 1), counters.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(usize, 1), counters.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(usize, 0), counters.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(usize, 1), counters.quant_kernel_generated_candidates);

    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, first_lazy_metal_source_path, std.testing.allocator, .limited(128 * 1024));
    defer std.testing.allocator.free(contents);
    const compiled_lazy = compileMetalKernelSource(.q4_k, .rows_2_8, .bias_gelu).?;
    const emitted = try emitCompiledSource(std.testing.allocator, compiled_lazy);
    defer emitted.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings(emitted.data, contents);
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "float acc[4][2]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "antfly_qk_gelu(total + bias[col])"));
}

test "quant kernel compiler compiles promoted Metal source from descriptor route" {
    const compiled = compileMetalKernelSource(.q6_k, .rows_2_8, .none).?;

    try std.testing.expectEqual(Backend.metal, compiled.request.backend);
    try std.testing.expectEqual(quant_matmul.Format.q6_k, compiled.spec.format);
    try std.testing.expectEqual(quant_matmul.Format.q6_k, compiled.ir.format);
    try std.testing.expectEqual(Epilogue.none, compiled.ir.epilogue);
    try std.testing.expectEqualSlices(IROp, &ir_ops_basic, compiled.ir.ops);
    try std.testing.expectEqualStrings(first_general_metal_q6_kernel_id, compiled.artifact.kernel_id);
    try std.testing.expectEqualStrings(first_general_metal_q6_source_path, compiled.source_path);
    try std.testing.expectEqualStrings(first_general_metal_q6_check_command, compiled.check_command);
    try std.testing.expectEqualStrings(first_general_metal_q6_source, compiled.source);
    try std.testing.expect(compiled.production_enabled);
    try std.testing.expectEqualStrings("TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH", std.mem.span(compiled.runtime_gate_env.?));
    try std.testing.expectEqual(LoweringRoute.generated_production, compiled.lowering.production_route);
    try std.testing.expectEqual(LoweringRoute.unsupported, compiled.lowering.candidate_route);
    try std.testing.expectEqual(FallbackReason.none, compiled.lowering.fallback_reason);
    try std.testing.expect(try compiledSourceHeaderMatchesPlan(std.testing.allocator, compiled));

    const q5_1 = compileMetalKernelSource(.q5_1, .rows_2_8, .none).?;
    try std.testing.expect(!q5_1.production_enabled);
    try std.testing.expectEqualStrings(first_general_metal_q5_1_kernel_id, q5_1.artifact.kernel_id);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, q5_1.lowering.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, q5_1.lowering.candidate_route);
    try std.testing.expect(q5_1.runtime_gate_env != null);
    try std.testing.expectEqualStrings("TERMITE_METAL_ENABLE_ANTFLY_Q5_1_SMALL_BATCH", std.mem.span(q5_1.runtime_gate_env.?));
    try std.testing.expect(std.mem.containsAtLeast(u8, q5_1.source, 1, "Dev-only generated Metal candidate"));
    try std.testing.expect(try compiledSourceHeaderMatchesPlan(std.testing.allocator, q5_1));

    const wrong_kernel_header = try std.mem.replaceOwned(
        u8,
        std.testing.allocator,
        compiled.source,
        first_general_metal_q6_kernel_id,
        first_general_metal_q5_kernel_id,
    );
    defer std.testing.allocator.free(wrong_kernel_header);
    try std.testing.expect(!try sourceHeaderMatchesCompiledPlan(std.testing.allocator, compiled, wrong_kernel_header));
}

test "quant kernel compiler derived matmul artifacts are operation homogeneous" {
    for (first_generated_matmul_artifacts) |artifact| {
        try std.testing.expectEqual(OpKind.small_batch_matmul, artifact.opKind());
    }
}

test "quant kernel compiler operation views partition the unified registry" {
    try std.testing.expectEqual(
        first_generated_artifacts.len,
        first_generated_matmul_artifacts.len + first_generated_microkernel_artifacts.len + first_generated_attention_artifacts.len,
    );

    for (first_generated_artifacts) |artifact| {
        var matches: usize = 0;
        switch (artifact.opKind()) {
            .small_batch_matmul => for (first_generated_matmul_artifacts) |candidate| {
                if (candidate.backend == artifact.backend and std.mem.eql(u8, candidate.kernel_id, artifact.kernel_id)) matches += 1;
            },
            .microkernel => for (first_generated_microkernel_artifacts) |candidate| {
                if (candidate.backend == artifact.backend and std.mem.eql(u8, candidate.kernel_id, artifact.kernel_id)) matches += 1;
            },
            .attention => for (first_generated_attention_artifacts) |candidate| {
                if (candidate.backend == artifact.backend and std.mem.eql(u8, candidate.kernel_id, artifact.kernel_id)) matches += 1;
            },
        }
        try std.testing.expectEqual(@as(usize, 1), matches);
    }
}

test "quant kernel compiler runtime source emitter covers every typed artifact" {
    for (first_generated_artifacts) |artifact| {
        const emitted = try emitRuntimeArtifactSource(std.testing.allocator, artifact);
        defer emitted.deinit(std.testing.allocator);
        try std.testing.expectEqual(artifact.backend, emitted.artifact.backend);
        try std.testing.expectEqualStrings(artifact.kernel_id, emitted.artifact.kernel_id);
        try std.testing.expect(emitted.data.len > 0);
        try std.testing.expect(emitted.data.len <= kernel_jit.maximum_source_bytes);
        try std.testing.expect(std.mem.containsAtLeast(u8, emitted.data, 1, artifact.kernel_id));
        try std.testing.expectEqual(artifact.backend == .cuda, emitted.owned);
    }
}

test "quant kernel compiler Metal runtime renderer covers the unified registry" {
    const region = try renderMetalRuntimeQuantRegion(std.testing.allocator);
    defer std.testing.allocator.free(region);
    for (first_generated_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        const declaration = try std.fmt.allocPrint(std.testing.allocator, "kernel void {s}(", .{artifact.kernel_id});
        defer std.testing.allocator.free(declaration);
        try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, region, declaration));
    }
}

test "quant kernel compiler microkernel artifacts carry typed render plans" {
    try std.testing.expect(first_generated_microkernel_artifacts.len > 0);
    for (first_generated_microkernel_artifacts) |artifact| {
        try std.testing.expectEqual(OpKind.microkernel, artifact.opKind());
        const op = artifact.microkernelOp() orelse return error.MissingMicrokernelArtifactOp;
        try std.testing.expectEqual(MicrokernelKind.rms_norm, op.kind);
        try std.testing.expect(op.schedule.threads_per_threadgroup > 0);
        try std.testing.expectEqual(Backend.metal, artifact.backend);
        try std.testing.expect(!artifact.production_enabled);
        const source = generatedSourceForArtifact(artifact) orelse return error.MissingGeneratedSource;
        // Header carries the artifact identity; body is the rendered kernel.
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, artifact.kernel_id));
        const kernel_decl = try std.fmt.allocPrint(std.testing.allocator, "kernel void {s}(", .{artifact.kernel_id});
        defer std.testing.allocator.free(kernel_decl);
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, kernel_decl));
    }
}

test "quant kernel compiler carries shape-specific E2B Q8 FFN candidates" {
    const expected = [_]struct {
        kernel_id: []const u8,
        kind: cuda_renderer.KernelKind,
        in_dim: u32,
        out_dim: u32,
        threads: u16,
    }{
        .{ .kernel_id = first_e2b_cuda_q4_0_pair_q8_6144_kernel_id, .kind = .q4_0_pair_activation_q8_1_e2b_6144, .in_dim = 1536, .out_dim = 6144, .threads = 384 },
        .{ .kernel_id = first_e2b_cuda_q4_0_pair_q8_12288_kernel_id, .kind = .q4_0_pair_activation_q8_1_e2b_12288, .in_dim = 1536, .out_dim = 12288, .threads = 384 },
        .{ .kernel_id = first_e2b_cuda_q4_0_down_q8_6144_kernel_id, .kind = .q4_0_down_q8_1_e2b_6144, .in_dim = 6144, .out_dim = 1536, .threads = 128 },
        .{ .kernel_id = first_e2b_cuda_q4_0_down_q8_12288_kernel_id, .kind = .q4_0_down_q8_1_e2b_12288, .in_dim = 12288, .out_dim = 1536, .threads = 256 },
    };
    for (expected) |item| {
        const artifact = generatedArtifactForKernel(.cuda, item.kernel_id) orelse return error.MissingGeneratedArtifact;
        try std.testing.expect(!artifact.production_enabled);
        try std.testing.expect(artifactRuntimeWired(artifact));
        try std.testing.expectEqualStrings(
            "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN",
            std.mem.span(artifactRuntimeGateEnv(artifact).?),
        );
        try std.testing.expect(artifact.runtime_evidence_command.len == 0);
        const plan = cudaRenderPlanForArtifact(artifact) orelse return error.MissingCudaRenderPlan;
        try std.testing.expectEqual(item.kind, plan.kind);
        try std.testing.expectEqual(item.in_dim, plan.launch.input_dim.fixed);
        try std.testing.expectEqual(item.out_dim, plan.launch.output_dim.fixed);
        try std.testing.expectEqual(item.threads, plan.launch.threads_per_block);
        const compiled = compileQuantKernelArtifactSource(artifact) orelse return error.MissingCompiledQuantKernelSource;
        try std.testing.expectEqualStrings(item.kernel_id, compiled.artifact.kernel_id);
        try std.testing.expectEqual(LoweringRoute.handwritten_production, compiled.lowering.production_route);
        try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, compiled.lowering.candidate_route);
        try std.testing.expectEqual(FallbackReason.generated_runtime_not_wired, compiled.lowering.fallback_reason);
        try std.testing.expectEqualStrings("awaiting_cuda_promotion_evidence", artifactPromotionBlocker(artifact));
        try std.testing.expect(std.mem.containsAtLeast(u8, compiled.source, 1, item.kernel_id));
    }

    const default_pair = compileQuantKernelSource(.{
        .backend = .cuda,
        .format = .q4_0,
        .row_bucket = .rows_1,
        .epilogue = .pair_activation,
    }) orelse return error.MissingCompiledQuantKernelSource;
    try std.testing.expectEqualStrings(first_general_cuda_q4_0_pair_q8_kernel_id, default_pair.artifact.kernel_id);
    const default_down = compileQuantKernelSource(.{
        .backend = .cuda,
        .format = .q4_0,
        .row_bucket = .rows_1,
        .epilogue = .gated_down,
    }) orelse return error.MissingCompiledQuantKernelSource;
    try std.testing.expectEqualStrings(first_general_cuda_q4_0_down_q8_kernel_id, default_down.artifact.kernel_id);
}

test "quant kernel compiler carries exact F32 E2B FFN candidates" {
    const expected = [_]struct {
        kernel_id: []const u8,
        kind: cuda_renderer.KernelKind,
        in_dim: u32,
        out_dim: u32,
        threads: u16,
        activation: ActivationEncoding,
        output: OutputEncoding,
    }{
        .{
            .kernel_id = first_e2b_cuda_q4_0_pair_f32_6144_exact_kernel_id,
            .kind = .q4_0_pair_activation_f32_e2b_6144_exact,
            .in_dim = 1536,
            .out_dim = 6144,
            .threads = 128,
            .activation = .f32,
            .output = .f32,
        },
        .{
            .kernel_id = first_e2b_cuda_q4_0_pair_f32_12288_exact_kernel_id,
            .kind = .q4_0_pair_activation_f32_e2b_12288_exact,
            .in_dim = 1536,
            .out_dim = 12_288,
            .threads = 128,
            .activation = .f32,
            .output = .f32,
        },
        .{
            .kernel_id = first_e2b_cuda_q4_0_down_f32_6144_exact_kernel_id,
            .kind = .q4_0_down_f32_e2b_6144_exact,
            .in_dim = 6144,
            .out_dim = 1536,
            .threads = 256,
            .activation = .f32,
            .output = .f32,
        },
        .{
            .kernel_id = first_e2b_cuda_q4_0_down_f32_12288_exact_kernel_id,
            .kind = .q4_0_down_f32_e2b_12288_exact,
            .in_dim = 12_288,
            .out_dim = 1536,
            .threads = 256,
            .activation = .f32,
            .output = .f32,
        },
    };
    for (expected) |item| {
        const artifact = generatedArtifactForKernel(.cuda, item.kernel_id) orelse return error.MissingGeneratedArtifact;
        try std.testing.expect(!artifact.production_enabled);
        try std.testing.expect(artifactRuntimeWired(artifact));
        try std.testing.expectEqual(item.activation, artifact.activation);
        try std.testing.expectEqual(item.output, artifact.output);
        try std.testing.expectEqualStrings(
            "ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_E2B_FFN_EXACT",
            std.mem.span(artifactRuntimeGateEnv(artifact).?),
        );
        const plan = cudaRenderPlanForArtifact(artifact) orelse return error.MissingCudaRenderPlan;
        try std.testing.expectEqual(item.kind, plan.kind);
        try std.testing.expectEqual(item.in_dim, plan.launch.input_dim.fixed);
        try std.testing.expectEqual(item.out_dim, plan.launch.output_dim.fixed);
        try std.testing.expectEqual(item.threads, plan.launch.threads_per_block);
        try std.testing.expectEqual(@as(u32, 128), plan.launch.static_shared_memory_bytes);
        const compiled = compileQuantKernelArtifactSource(artifact) orelse return error.MissingCompiledQuantKernelSource;
        try std.testing.expectEqualStrings(item.kernel_id, compiled.artifact.kernel_id);
        try std.testing.expectEqual(LoweringRoute.handwritten_production, compiled.lowering.production_route);
        try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, compiled.lowering.candidate_route);
        try std.testing.expectEqual(FallbackReason.generated_runtime_not_wired, compiled.lowering.fallback_reason);
        try std.testing.expectEqualStrings("awaiting_cuda_promotion_evidence", artifactPromotionBlocker(artifact));
        try std.testing.expect(std.mem.containsAtLeast(u8, compiled.source, 1, item.kernel_id));
    }
}

test "quant kernel compiler carries runtime-wired E2B Q8 LM argmax candidate" {
    const artifact = generatedArtifactForKernel(.cuda, first_e2b_cuda_q4_0_q8_1_argmax_kernel_id) orelse
        return error.MissingGeneratedArtifact;
    try std.testing.expect(!artifact.production_enabled);
    try std.testing.expect(artifactRuntimeWired(artifact));
    try std.testing.expectEqualStrings(
        "ANTFLY_INFERENCE_CUDA_Q4_0_LM_HEAD_Q8_1_ARGMAX",
        std.mem.span(artifactRuntimeGateEnv(artifact).?),
    );
    const plan = cudaRenderPlanForArtifact(artifact) orelse return error.MissingCudaRenderPlan;
    try std.testing.expectEqual(cuda_renderer.KernelKind.q4_0_q8_1_argmax_e2b_tile8, plan.kind);
    try std.testing.expectEqual(@as(u32, 1536), plan.launch.input_dim.fixed);
    try std.testing.expectEqual(@as(u32, 262144), plan.launch.output_dim.fixed);
    try std.testing.expectEqual(@as(u16, 96), plan.launch.threads_per_block);
    const compiled = compileQuantKernelArtifactSource(artifact) orelse return error.MissingCompiledQuantKernelSource;
    try std.testing.expectEqual(LoweringRoute.handwritten_production, compiled.lowering.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, compiled.lowering.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_runtime_not_wired, compiled.lowering.fallback_reason);
    try std.testing.expectEqualStrings("awaiting_cuda_promotion_evidence", artifactPromotionBlocker(artifact));
    const route = loweringFor(.cuda, .q4_0, .rows_1, .argmax);
    try std.testing.expectEqualStrings(first_e2b_cuda_q4_0_q8_1_argmax_kernel_id, route.kernel_id);
    try std.testing.expectEqual(FallbackReason.generated_runtime_not_wired, route.fallback_reason);
    try std.testing.expectEqual(IROp.write_argmax_pair, compiled.ir.ops[compiled.ir.ops.len - 1]);
    try std.testing.expect(std.mem.containsAtLeast(u8, compiled.source, 1, "value == best_value && col < best_index"));
}

test "quant kernel compiler carries standalone K2560 Q6_K Q8_1 argmax candidate" {
    const artifact = generatedArtifactForKernel(.cuda, first_cuda_q6_k_q8_1_argmax_k2560_kernel_id) orelse
        return error.MissingGeneratedArtifact;
    try std.testing.expect(!artifact.production_enabled);
    try std.testing.expect(artifactRuntimeWired(artifact));
    try std.testing.expectEqualStrings(
        "ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX",
        std.mem.span(artifactRuntimeGateEnv(artifact).?),
    );
    try std.testing.expectEqualStrings("awaiting_cuda_promotion_evidence", artifactPromotionBlocker(artifact));

    const plan = cudaRenderPlanForArtifact(artifact) orelse return error.MissingCudaRenderPlan;
    try std.testing.expectEqual(cuda_renderer.KernelKind.q6_k_q8_1_argmax_k2560_tile8, plan.kind);
    try std.testing.expectEqual(@as(u32, 2560), plan.launch.input_dim.fixed);
    try std.testing.expectEqual(@as(u32, 262144), plan.launch.output_dim.fixed);
    try std.testing.expectEqual(@as(u16, 160), plan.launch.threads_per_block);
    try std.testing.expectEqualStrings(
        "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b+termite_argmax_reduce_rows_pairs_f32_w16",
        plan.production_baseline,
    );

    const compiled = compileQuantKernelArtifactSource(artifact) orelse return error.MissingCompiledQuantKernelSource;
    try std.testing.expectEqual(LoweringRoute.handwritten_production, compiled.lowering.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, compiled.lowering.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_runtime_not_wired, compiled.lowering.fallback_reason);
    try std.testing.expectEqual(IROp.write_argmax_pair, compiled.ir.ops[compiled.ir.ops.len - 1]);
    try std.testing.expect(std.mem.containsAtLeast(u8, compiled.source, 1, first_cuda_q6_k_q8_1_argmax_k2560_kernel_id));
    try std.testing.expect(std.mem.containsAtLeast(u8, compiled.source, 1, "antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_q8_1_dot16_sub"));
    try std.testing.expect(std.mem.containsAtLeast(u8, compiled.source, 1, "sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, compiled.source, 1, "sumi = __dp4a(antfly_pack_q6_k_i8x4_sub"));
    try std.testing.expect(first_cuda_q6_k_q8_1_argmax_k2560_source_fingerprint != 0);
}

test "quant kernel compiler carries standalone K3840 Q6_K Q8_1 argmax candidate" {
    const artifact = generatedArtifactForKernel(.cuda, first_cuda_q6_k_q8_1_argmax_k3840_kernel_id) orelse
        return error.MissingGeneratedArtifact;
    try std.testing.expect(!artifact.production_enabled);
    try std.testing.expect(artifactRuntimeWired(artifact));
    try std.testing.expectEqualStrings(
        "ANTFLY_INFERENCE_CUDA_GENERATED_Q6_K_Q8_1_LM_HEAD_ARGMAX",
        std.mem.span(artifactRuntimeGateEnv(artifact).?),
    );
    try std.testing.expectEqual(ActivationEncoding.q8_1, artifact.activation);
    try std.testing.expectEqual(OutputEncoding.i32, artifact.output);

    const plan = cudaRenderPlanForArtifact(artifact) orelse return error.MissingCudaRenderPlan;
    try std.testing.expectEqual(cuda_renderer.KernelKind.q6_k_q8_1_argmax_k3840_tile8, plan.kind);
    try std.testing.expectEqual(@as(u32, 3840), plan.launch.input_dim.fixed);
    try std.testing.expectEqual(@as(u32, 262144), plan.launch.output_dim.fixed);
    try std.testing.expectEqual(@as(u16, 256), plan.launch.threads_per_block);
    try std.testing.expectEqual(@as(u32, 256), plan.launch.static_shared_memory_bytes);
    try std.testing.expectEqualStrings(
        "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8+termite_argmax_reduce_rows_pairs_f32_w16",
        plan.production_baseline,
    );

    const compiled = compileQuantKernelArtifactSource(artifact) orelse return error.MissingCompiledQuantKernelSource;
    try std.testing.expectEqual(LoweringRoute.handwritten_production, compiled.lowering.production_route);
    try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, compiled.lowering.candidate_route);
    try std.testing.expectEqual(FallbackReason.generated_runtime_not_wired, compiled.lowering.fallback_reason);
    try std.testing.expectEqualStrings(plan.production_baseline, compiled.lowering.production_kernel_id);
    try std.testing.expect(std.mem.containsAtLeast(u8, compiled.source, 1, "const unsigned int task_threads = 240u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, compiled.source, 1, "if (tid < task_threads)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, compiled.source, 1, "antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1_q6_k_q8_1_dot16_sub"));
    try std.testing.expect(std.mem.containsAtLeast(u8, compiled.source, 1, "sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1_q6_k_pack4"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, compiled.source, 1, "sumi = __dp4a(antfly_pack_q6_k_i8x4_sub"));
    try std.testing.expect(first_cuda_q6_k_q8_1_argmax_k3840_source_fingerprint != 0);
}

test "quant kernel compiler runtime region embeds the RMSNorm microkernel body" {
    const region = try renderMetalRuntimeQuantRegion(std.testing.allocator);
    defer std.testing.allocator.free(region);
    try std.testing.expect(std.mem.containsAtLeast(u8, region, 1, "kernel void antfly_rms_norm_generated_msl_v1("));
    // Single-sourced from the same renderer as the matmul routes.
    try std.testing.expect(std.mem.containsAtLeast(u8, region, 1, "antfly_q4_k_small_batch"));
}

test "quant kernel compiler attention artifacts carry typed render plans" {
    try std.testing.expect(first_generated_attention_artifacts.len > 0);
    var cuda_artifact_count: usize = 0;
    var runtime_cuda_artifact_count: usize = 0;
    var standalone_cuda_artifact_count: usize = 0;
    for (first_generated_attention_artifacts) |artifact| {
        try std.testing.expectEqual(OpKind.attention, artifact.opKind());
        const op = artifact.attentionOp() orelse return error.MissingAttentionArtifactOp;
        try std.testing.expect(op.schedule.threads_per_threadgroup > 0);
        const promoted_gemma4_flash = artifact.backend == .metal and
            (std.mem.eql(u8, artifact.kernel_id, first_prefill_flash_metal_kernel_id) or
                std.mem.eql(u8, artifact.kernel_id, first_prefill_flash_hd512_metal_kernel_id));
        const promoted_score_prework = artifact.backend == .cuda and
            (std.mem.eql(u8, artifact.kernel_id, first_decode_attention_1x_cuda_score_prework_hd256_kernel_id) or
                std.mem.eql(u8, artifact.kernel_id, first_decode_attention_1x_cuda_score_prework_hd512_kernel_id));
        const promoted_cuda_flash_prefill = artifact.backend == .cuda and
            (std.mem.eql(u8, artifact.kernel_id, first_prefill_flash_cuda_hd256_kernel_id) or
                std.mem.eql(u8, artifact.kernel_id, first_prefill_flash_cuda_hd512_kernel_id));
        // The Metal Gemma4 local and global flash routes cleared their
        // capability and model-token gates, the SM89 paged score-prework
        // decode composites cleared bitwise parity plus paired-throughput
        // qualification for the automatic selector, and the SM89 flash-prefill
        // composites cleared the paged-prefill bitwise differential for the
        // automatic prefill selector. Every other generated attention artifact
        // remains dev-only.
        const promoted = promoted_gemma4_flash or promoted_score_prework or promoted_cuda_flash_prefill;
        try std.testing.expectEqual(promoted, artifact.production_enabled);
        try std.testing.expectEqual(promoted, artifact.runtime_default_enabled);
        const source = generatedSourceForArtifact(artifact) orelse return error.MissingGeneratedSource;
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, artifact.kernel_id));
        // Flash and split-K online sources export their entry point through a
        // typed `#define` consumed by a macro-declared kernel; every other
        // route declares the kernel identifier directly.
        const kernel_decl = switch (artifact.backend) {
            .metal => try std.fmt.allocPrint(std.testing.allocator, "kernel void {s}(", .{artifact.kernel_id}),
            .cuda => if (artifact.cuda_flash_prefill_kernel != null)
                try std.fmt.allocPrint(std.testing.allocator, "#define ANTFLY_FLASH_KERNEL {s}\n", .{artifact.kernel_id})
            else if (artifact.cuda_splitk_online_decode_kernel != null)
                try std.fmt.allocPrint(std.testing.allocator, "#define ANTFLY_SPLITK_ONLINE_KERNEL {s}\n", .{artifact.kernel_id})
            else
                try std.fmt.allocPrint(std.testing.allocator, "extern \"C\" __global__ void {s}(", .{artifact.kernel_id}),
        };
        defer std.testing.allocator.free(kernel_decl);
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, kernel_decl));
        if (artifact.backend == .metal) {
            // Self-contained: the standalone `.metal` carries its own params struct +
            // paging helper (so `xcrun` compiles it with no metal_kernels.m dep).
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "struct antfly_paged_attention_1x_params {"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "inline uint antfly_paged_attention_1x_page_token("));
        } else {
            cuda_artifact_count += 1;
            try std.testing.expect(op.head_dim == 256 or op.head_dim == 512);
            if (cudaFlashPrefillRenderPlanForArtifact(artifact)) |plan| {
                try std.testing.expectEqual(AttentionKind.prefill_flash, op.kind);
                try std.testing.expectEqual(@as(u16, 256), op.schedule.threads_per_threadgroup);
                try std.testing.expectEqual(@as(u8, 8), op.schedule.attention_query_heads_per_kv_head);
                try std.testing.expectEqual(plan.lowering.query_tile, op.schedule.attention_query_tile);
                try std.testing.expectEqual(plan.lowering.key_tile, op.schedule.attention_key_tile);
                try std.testing.expectEqual(plan.lowering.page_size_tokens, op.schedule.attention_page_size_tokens);
                try std.testing.expectEqual(plan.launch.dynamic_shared_memory_bytes, op.schedule.attention_dynamic_shared_memory_bytes);
                try std.testing.expectEqual(AttentionStorage.f32, op.schedule.attention_storage);
                try std.testing.expectEqual(AttentionStorage.paged_f16, op.schedule.attention_key_storage);
                try std.testing.expectEqual(AttentionStorage.paged_f16, op.schedule.attention_value_storage);
                try std.testing.expect(artifact.production_enabled);
                try std.testing.expect(artifact.runtime_default_enabled);
                try std.testing.expect(plan.exact_token_parity_required_for_default);
                try std.testing.expect(plan.exact_token_parity_qualified);
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "wmma::mma_sync("));
            } else if (cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact)) |plan| {
                try std.testing.expectEqual(AttentionKind.decode_1x, op.kind);
                try std.testing.expectEqual(cuda_renderer.generated_splitk_online_decode_threads, op.schedule.threads_per_threadgroup);
                try std.testing.expectEqual(@as(u8, 8), op.schedule.attention_query_heads_per_kv_head);
                try std.testing.expectEqual(plan.lowering.kv_splits, op.schedule.attention_kv_splits);
                try std.testing.expectEqual(plan.lowering.max_visible_tokens, op.schedule.attention_max_kv_tokens);
                try std.testing.expectEqual(plan.lowering.page_size_tokens, op.schedule.attention_page_size_tokens);
                try std.testing.expectEqual(AttentionStorage.f32, op.schedule.attention_storage);
                try std.testing.expectEqual(AttentionStorage.paged_f16, op.schedule.attention_key_storage);
                try std.testing.expectEqual(AttentionStorage.paged_f16, op.schedule.attention_value_storage);
                try std.testing.expectEqual(
                    cuda_renderer.generatedSplitkOnlineDecodeWorkspaceLayout(),
                    plan.workspace,
                );
                try std.testing.expect(!artifact.production_enabled);
                try std.testing.expect(!artifact.runtime_default_enabled);
                try std.testing.expect(plan.exact_token_parity_required_for_default);
                try std.testing.expect(!plan.exact_token_parity_qualified);
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "unsigned* completion_counters"));
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const unsigned* decode_scalars"));
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "atomicExch(&completion_counters[head], 0u)"));
            } else {
                const plan = cudaAttentionRenderPlanForArtifact(artifact) orelse return error.MissingCudaAttentionRenderPlan;
                try std.testing.expectEqual(plan.lowering.kv_splits, op.schedule.attention_kv_splits);
                try std.testing.expectEqual(plan.lowering.split_variant.kvSplits(), op.schedule.attention_kv_splits);
                try std.testing.expectEqual(cuda_renderer.generated_attention_query_heads_per_kv_head, op.schedule.attention_query_heads_per_kv_head);
                try std.testing.expectEqual(plan.lowering.split_kv_min_tokens_default, op.schedule.attention_split_kv_min_tokens);
                try std.testing.expectEqual(plan.lowering.max_kv_tokens, op.schedule.attention_max_kv_tokens);
                try std.testing.expectEqual(AttentionStorage.f32, op.schedule.attention_storage);
                try std.testing.expectEqual(
                    if (plan.lowering.split_variant == .score_prework)
                        AttentionStorage.paged_f16_or_polar4
                    else
                        AttentionStorage.f32,
                    op.schedule.attention_key_storage,
                );
                try std.testing.expectEqual(
                    if (plan.lowering.split_variant == .score_prework)
                        AttentionStorage.paged_f16_or_f32
                    else
                        AttentionStorage.f32,
                    op.schedule.attention_value_storage,
                );
                try std.testing.expect(op.schedule.attention_serial_threads_per_threadgroup > 0);
                try std.testing.expect(op.schedule.attention_stage2_threads_per_threadgroup > 0);
                if (plan.lowering.split_variant == .score_prework) {
                    try std.testing.expectEqual(cuda_renderer.generated_attention_score_prework_tiled64_tile_size, op.schedule.attention_tiled64_threads_per_threadgroup);
                    try std.testing.expectEqual(
                        cuda_renderer.generatedAttentionScorePreworkTiled64MaxKvTokens(op.head_dim).?,
                        op.schedule.attention_tiled64_max_kv_tokens,
                    );
                } else {
                    try std.testing.expectEqual(@as(u16, 0), op.schedule.attention_tiled64_threads_per_threadgroup);
                    try std.testing.expectEqual(@as(u16, 0), op.schedule.attention_tiled64_max_kv_tokens);
                }
                inline for (.{ plan.serial_kernel_id, plan.kernel_id, plan.reduction_kernel_id }) |kernel_id| {
                    const cuda_decl = try std.fmt.allocPrint(std.testing.allocator, "extern \"C\" __global__ void {s}(", .{kernel_id});
                    defer std.testing.allocator.free(cuda_decl);
                    try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, cuda_decl));
                }
                if (plan.tiled64_kernel_id) |kernel_id| {
                    const cuda_decl = try std.fmt.allocPrint(std.testing.allocator, "extern \"C\" __global__ void {s}(", .{kernel_id});
                    defer std.testing.allocator.free(cuda_decl);
                    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, source, cuda_decl));
                }
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "const unsigned int* decode_scalars"));
                const source_id_header = try std.fmt.allocPrint(std.testing.allocator, "// source_id={s}", .{plan.source_id});
                defer std.testing.allocator.free(source_id_header);
                try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, source_id_header));
            }
            if (cudaAttentionArtifactRuntimeWired(artifact)) {
                runtime_cuda_artifact_count += 1;
            } else {
                standalone_cuda_artifact_count += 1;
                try std.testing.expect(!artifact.production_enabled);
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 12), cuda_artifact_count);
    try std.testing.expectEqual(@as(usize, 12), runtime_cuda_artifact_count);
    try std.testing.expectEqual(@as(usize, 0), standalone_cuda_artifact_count);
}

test "quant kernel compiler registers runtime-wired split-KV schedule variants" {
    const expected = [_]struct {
        kernel_id: []const u8,
        split_count: u8,
        source_fingerprint: u64,
    }{
        .{ .kernel_id = first_decode_attention_1x_cuda_split2_hd256_kernel_id, .split_count = 2, .source_fingerprint = first_decode_attention_1x_cuda_split2_hd256_source_fingerprint },
        .{ .kernel_id = first_decode_attention_1x_cuda_split2_hd512_kernel_id, .split_count = 2, .source_fingerprint = first_decode_attention_1x_cuda_split2_hd512_source_fingerprint },
        .{ .kernel_id = first_decode_attention_1x_cuda_split4_hd256_kernel_id, .split_count = 4, .source_fingerprint = first_decode_attention_1x_cuda_split4_hd256_source_fingerprint },
        .{ .kernel_id = first_decode_attention_1x_cuda_split4_hd512_kernel_id, .split_count = 4, .source_fingerprint = first_decode_attention_1x_cuda_split4_hd512_source_fingerprint },
    };
    for (expected) |candidate| {
        const artifact = generatedRegistryArtifactForKernel(.cuda, candidate.kernel_id) orelse return error.MissingSplitKvVariantArtifact;
        const plan = cudaAttentionRenderPlanForArtifact(artifact) orelse return error.MissingSplitKvVariantPlan;
        try std.testing.expectEqual(candidate.split_count, plan.lowering.kv_splits);
        try std.testing.expectEqual(candidate.source_fingerprint, artifactSourceFingerprint(artifact));
        try std.testing.expect(cudaAttentionArtifactRuntimeWired(artifact));
        try std.testing.expect(!artifact.production_enabled);
        const source = generatedSourceForArtifact(artifact) orelse return error.MissingSplitKvVariantSource;
        const split_text = try std.fmt.allocPrint(std.testing.allocator, "split{d}", .{candidate.split_count});
        defer std.testing.allocator.free(split_text);
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, split_text));
    }
}

test "quant kernel compiler registers promoted SM89 Flash prefill artifacts" {
    const expected = [_]struct {
        kernel_id: []const u8,
        head_dim: u16,
        sliding_window: u16,
        source_fingerprint: u64,
    }{
        .{
            .kernel_id = first_prefill_flash_cuda_hd256_kernel_id,
            .head_dim = 256,
            .sliding_window = 512,
            .source_fingerprint = first_prefill_flash_cuda_hd256_source_fingerprint,
        },
        .{
            .kernel_id = first_prefill_flash_cuda_hd512_kernel_id,
            .head_dim = 512,
            .sliding_window = 0,
            .source_fingerprint = first_prefill_flash_cuda_hd512_source_fingerprint,
        },
    };
    for (expected) |item| {
        const artifact = generatedRegistryArtifactForKernel(.cuda, item.kernel_id) orelse
            return error.MissingCudaFlashPrefillArtifact;
        const plan = cudaFlashPrefillRenderPlanForArtifact(artifact) orelse
            return error.MissingCudaFlashPrefillPlan;
        try std.testing.expectEqual(item.head_dim, plan.lowering.head_dim);
        try std.testing.expectEqual(item.sliding_window, plan.lowering.sliding_window);
        try std.testing.expectEqual(@as(u8, 8), plan.lowering.query_heads);
        try std.testing.expectEqual(@as(u8, 1), plan.lowering.kv_heads);
        try std.testing.expectEqual(cuda_renderer.AttentionStorage.f32, plan.lowering.query_storage);
        try std.testing.expectEqual(cuda_renderer.AttentionStorage.paged_f16, plan.lowering.key_storage);
        try std.testing.expectEqual(cuda_renderer.AttentionStorage.paged_f16, plan.lowering.value_storage);
        try std.testing.expectEqual(@as(u8, 8), plan.lowering.required_compute_major);
        try std.testing.expectEqual(@as(u8, 9), plan.lowering.required_compute_minor);
        try std.testing.expect(plan.lowering.query_length_policy.accepts(512, 1536));
        try std.testing.expect(plan.lowering.query_length_policy.accepts(3, 2048));
        try std.testing.expect(!plan.lowering.query_length_policy.accepts(1, 2050));
        try std.testing.expect(cudaFlashPrefillArtifactRuntimeWired(artifact));
        try std.testing.expect(cudaAttentionArtifactRuntimeWired(artifact));
        try std.testing.expect(artifact.production_enabled);
        try std.testing.expect(artifact.runtime_default_enabled);
        try std.testing.expectEqualStrings(
            first_prefill_flash_cuda_runtime_evidence_command,
            artifact.runtime_evidence_command,
        );
        try std.testing.expectEqualStrings(
            first_prefill_flash_cuda_promotion_evidence_command,
            artifact.promotion_evidence_command,
        );
        try std.testing.expectEqual(
            @as(?u64, cuda_sm89_promotion_target_fingerprint),
            attentionArtifactPromotionTargetFingerprint(artifact),
        );
        try std.testing.expect(item.source_fingerprint != 0);
        try std.testing.expectEqual(item.source_fingerprint, artifactSourceFingerprint(artifact));
        const source = generatedSourceForArtifact(artifact) orelse return error.MissingCudaFlashPrefillSource;
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, item.kernel_id));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "wmma::mma_sync("));
    }
    try std.testing.expect(first_prefill_flash_cuda_hd256_source_fingerprint != first_prefill_flash_cuda_hd512_source_fingerprint);
}

test "quant kernel compiler registers default-off SM89 split-K online decode artifacts" {
    const expected = [_]struct {
        kernel_id: []const u8,
        head_dim: u16,
        sliding_window: u16,
        max_visible_tokens: u16,
        source_fingerprint: u64,
    }{
        .{
            .kernel_id = first_decode_splitk_online_cuda_hd256_kernel_id,
            .head_dim = 256,
            .sliding_window = 512,
            .max_visible_tokens = 512,
            .source_fingerprint = first_decode_splitk_online_cuda_hd256_source_fingerprint,
        },
        .{
            .kernel_id = first_decode_splitk_online_cuda_hd512_kernel_id,
            .head_dim = 512,
            .sliding_window = 0,
            .max_visible_tokens = 4096,
            .source_fingerprint = first_decode_splitk_online_cuda_hd512_source_fingerprint,
        },
    };
    for (expected) |item| {
        const artifact = generatedRegistryArtifactForKernel(.cuda, item.kernel_id) orelse
            return error.MissingCudaSplitkOnlineDecodeArtifact;
        const plan = cudaSplitkOnlineDecodeRenderPlanForArtifact(artifact) orelse
            return error.MissingCudaSplitkOnlineDecodePlan;
        try std.testing.expectEqual(item.head_dim, plan.lowering.head_dim);
        try std.testing.expectEqual(item.sliding_window, plan.lowering.sliding_window);
        try std.testing.expectEqual(item.max_visible_tokens, plan.lowering.max_visible_tokens);
        try std.testing.expectEqual(@as(u16, 8), plan.lowering.query_heads);
        try std.testing.expectEqual(@as(u16, 1), plan.lowering.kv_heads);
        try std.testing.expectEqual(@as(u16, 64), plan.lowering.kv_splits);
        try std.testing.expectEqual(cuda_renderer.AttentionStorage.f32, plan.lowering.query_storage);
        try std.testing.expectEqual(cuda_renderer.AttentionStorage.paged_f16, plan.lowering.key_storage);
        try std.testing.expectEqual(cuda_renderer.AttentionStorage.paged_f16, plan.lowering.value_storage);
        try std.testing.expectEqual(@as(u8, 8), plan.lowering.required_compute_major);
        try std.testing.expectEqual(@as(u8, 9), plan.lowering.required_compute_minor);
        try std.testing.expect(cudaSplitkOnlineDecodeArtifactRuntimeWired(artifact));
        try std.testing.expect(cudaAttentionArtifactRuntimeWired(artifact));
        try std.testing.expect(!artifact.production_enabled);
        try std.testing.expect(!artifact.runtime_default_enabled);
        try std.testing.expect(item.source_fingerprint != 0);
        try std.testing.expectEqual(item.source_fingerprint, artifactSourceFingerprint(artifact));
        const source = generatedSourceForArtifact(artifact) orelse return error.MissingCudaSplitkOnlineDecodeSource;
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, item.kernel_id));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "unsigned* completion_counters"));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "atomicExch(&completion_counters[head], 0u)"));
    }
    try std.testing.expect(
        first_decode_splitk_online_cuda_hd256_source_fingerprint !=
            first_decode_splitk_online_cuda_hd512_source_fingerprint,
    );
}

test "quant kernel compiler manifests split-KV schedule/source identities" {
    const manifest = try artifactManifestJson(std.testing.allocator);
    defer std.testing.allocator.free(manifest);
    const kernel_ids = [_][]const u8{
        first_decode_attention_1x_cuda_split2_hd256_kernel_id,
        first_decode_attention_1x_cuda_split2_hd512_kernel_id,
        first_decode_attention_1x_cuda_split4_hd256_kernel_id,
        first_decode_attention_1x_cuda_split4_hd512_kernel_id,
    };
    for (kernel_ids) |kernel_id| {
        try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, kernel_id));
    }
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, manifest, "\"cuda_attention_split_count\": 2"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, manifest, "\"cuda_attention_split_count\": 4"));
    // Legacy split-8 entries keep their ABI-stable kernel IDs but record a
    // canonical split-aware source identity in the registry manifest.
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, manifest, "\"cuda_attention_split_count\": 8"));
    try std.testing.expect(std.mem.containsAtLeast(u8, manifest, 1, "antfly_gqa_attention_decode_split8_kv_hd256_f32_v1"));
}

test "quant kernel compiler runtime region embeds the decode-1x attention body" {
    const region = try renderMetalRuntimeQuantRegion(std.testing.allocator);
    defer std.testing.allocator.free(region);
    try std.testing.expect(std.mem.containsAtLeast(u8, region, 1, "kernel void antfly_paged_attention_1x_generated_msl_v1("));
    // The self-contained helpers ride into the region too.
    try std.testing.expect(std.mem.containsAtLeast(u8, region, 1, "struct antfly_paged_attention_1x_params {"));
    // Coexists with the matmul + microkernel routes in one region.
    try std.testing.expect(std.mem.containsAtLeast(u8, region, 1, "antfly_q4_k_small_batch"));
    try std.testing.expect(std.mem.containsAtLeast(u8, region, 1, "antfly_rms_norm_generated_msl_v1"));
}

test "quant kernel compiler runtime region embeds the flash prefill attention body" {
    const region = try renderMetalRuntimeQuantRegion(std.testing.allocator);
    defer std.testing.allocator.free(region);
    try std.testing.expect(std.mem.containsAtLeast(u8, region, 1, "kernel void antfly_paged_attention_prefill_flash_generated_msl_v1("));
    // Both attention kernels share the single deduped params struct + paging
    // helper (emitted exactly once across the whole region).
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, region, "struct antfly_paged_attention_1x_params {"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, region, "inline uint antfly_paged_attention_1x_page_token("));
    // The flash body's simdgroup-MMA markers are present.
    try std.testing.expect(std.mem.containsAtLeast(u8, region, 1, "simdgroup_multiply_accumulate(ms, mq, mk, ms)"));
}

test "quant kernel compiler pins the paged-attention params layout drift guard" {
    // The generated struct and the drift-guard entry share the field body, and
    // the drift-guard entry names the hand-written struct the dispatch binds.
    try std.testing.expect(std.mem.containsAtLeast(u8, metal_rt_external_helper_paged_attention_params, 1, "struct termite_metal_paged_attention_params { "));
    try std.testing.expect(std.mem.containsAtLeast(u8, metal_rt_external_helper_paged_attention_params, 1, metal_renderer.paged_attention_params_field_body));
    try std.testing.expect(std.mem.containsAtLeast(u8, metal_renderer.helper_paged_attention_1x_params.msl, 1, metal_renderer.paged_attention_params_field_body));
    // Every field the dispatch fills is present in the shared body.
    inline for (.{ "uint q_len;", "uint kv_tokens;", "uint num_heads;", "uint num_kv_heads;", "uint head_dim;", "uint key_row_bytes;", "uint base_key_row_bytes;", "uint query_position_offset;", "uint kv_position_offset;", "uint sliding_window;", "uint v_row_stride;", "uint page_size;", "uint block_count;", "uint contiguous_base_token;", "uint contiguous_blocks;", "uint format;", "uint v_element_bytes;", "uint has_sinks;", "float softcap;", "uint swa_scan_clamp;" }) |field| {
        try std.testing.expect(std.mem.containsAtLeast(u8, metal_renderer.paged_attention_params_field_body, 1, field));
    }

    const host_source = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "src/backends/metal_kernels.m", std.testing.allocator, .limited(8 * 1024 * 1024));
    defer std.testing.allocator.free(host_source);
    const field_count = std.mem.count(u8, metal_renderer.paged_attention_params_field_body, ";");
    const expected_size_assert = try std.fmt.allocPrint(
        std.testing.allocator,
        "_Static_assert(sizeof(termite_metal_paged_attention_params) == {d}",
        .{field_count * @sizeOf(u32)},
    );
    defer std.testing.allocator.free(expected_size_assert);
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, expected_size_assert));
    try std.testing.expectEqual(field_count, std.mem.count(u8, host_source, "_Static_assert(offsetof(termite_metal_paged_attention_params"));

    const decode_threads = try std.fmt.allocPrint(std.testing.allocator, "#define TERMITE_METAL_GENERATED_DECODE_THREADS {d}u", .{first_decode_attention_1x_metal_schedule.threads_per_threadgroup});
    defer std.testing.allocator.free(decode_threads);
    const flash_threads = try std.fmt.allocPrint(std.testing.allocator, "#define TERMITE_METAL_GENERATED_FLASH_THREADS {d}u", .{first_prefill_flash_metal_schedule.threads_per_threadgroup});
    defer std.testing.allocator.free(flash_threads);
    const flash_key_chunk = try std.fmt.allocPrint(std.testing.allocator, "#define TERMITE_METAL_GENERATED_FLASH_KEY_CHUNK {d}u", .{first_prefill_flash_metal_schedule.key_chunk});
    defer std.testing.allocator.free(flash_key_chunk);
    const rms_threads = try std.fmt.allocPrint(std.testing.allocator, "#define TERMITE_METAL_GENERATED_RMS_THREADS {d}u", .{first_rms_norm_metal_schedule.threads_per_threadgroup});
    defer std.testing.allocator.free(rms_threads);
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, decode_threads));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, flash_threads));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, flash_key_chunk));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, rms_threads));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, host_source, "termite_metal_threadgroup_memory_16(prefill_sg_memory_bytes)"));
    try std.testing.expectEqual(@as(usize, 3), std.mem.count(u8, host_source, "TERMITE_METAL_HANDWRITTEN_FLASH_MEMORY_BYTES(head_dim)"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, host_source, "? TERMITE_METAL_GENERATED_FLASH_MEMORY_BYTES(head_dim)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "BOOL missing_requested_generated_pipeline"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, host_source, "runtime->generated_attention_decode_1x_calls += 1"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, host_source, "runtime->generated_attention_flash_prefill_calls += 1"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, host_source, "runtime->generated_attention_flash_prefill_hd512_calls += 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "TERMITE_METAL_GENERATED_FLASH_HD512_MEMORY_BYTES 13888u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "TERMITE_METAL_DISABLE_PREFILL_FLASH_HD512"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "runtime->generated_rms_norm_calls += 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "return termite_metal_encode_rms_norm_generated("));
}

test "metal runtime source narrowly gates the small-row split GQA route" {
    const host_source = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "src/backends/metal_kernels.m", std.testing.allocator, .limited(8 * 1024 * 1024));
    defer std.testing.allocator.free(host_source);

    // Stage 1 maps query rows x KV heads x context splits and shares each KV
    // tile across the four E4B or eight E2B query heads in the group.
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "kernel void termite_paged_attention_kv_decode_gqa_split_stage("));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "kernel void termite_paged_attention_kv_decode_gqa_split_reduce("));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "const uint qi = tg.x; const uint kv_h = tg.y; const uint split = tg.z;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "const uint query_pos = p.query_position_offset + qi;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "(p.num_kv_heads != 1u && p.num_kv_heads != 2u)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "const uint heads_per_group = p.num_heads / p.num_kv_heads;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "for (uint kc = split * schedule.key_chunk; kc < p.kv_tokens; kc += split_count * schedule.key_chunk)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "sS[j] = sS[j] * corr + row_sum"));

    // Split is the compact innermost SoA dimension. Both partial O and M/S use
    // the selected active split count, so smaller schedules neither retain the
    // old fixed-32 stride nor read outside their compact scratch allocation.
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "partial_o[(qh * hd4 + d4) * split_count + split]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "partial_o[(qh * hd4 + d4) * split_count + uint(lane)]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "partial_stats[qh * (2u * split_count) + split]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "partial_stats[qh * (2u * split_count) + split_count + uint(lane)]"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "float merged_m = simd_max(M)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "float denom = simd_sum(S * weight)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "float4 merged = simd_sum(value * weight)"));

    // Both live encoders select policy independently but encode through one
    // shared stage/reduce helper. The dedicated 16-byte schedule ABI is bound
    // at stage buffer 7 and reducer buffer 3, and the helper is the sole owner
    // of the hazard transition and dispatch counters.
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "TERMITE_METAL_ENABLE_DECODE_GQA_SPLIT"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "TERMITE_METAL_DISABLE_DECODE_GQA_SPLIT"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "(head_dim == 512u && sliding_window == 0u)"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, host_source, 1, "TERMITE_METAL_ENABLE_DECODE_GQA_FLASH"));
    try std.testing.expectEqual(@as(usize, 3), std.mem.count(u8, host_source, "termite_metal_decode_gqa_split_eligible("));
    try std.testing.expectEqual(@as(usize, 3), std.mem.count(u8, host_source, "termite_metal_encode_decode_gqa_split_dispatch("));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "dispatchThreadgroups:MTLSizeMake(params->q_len, params->num_kv_heads, launch->params.split_count)"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "constant termite_metal_decode_gqa_split_params &schedule [[buffer(7)]]"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "constant termite_metal_decode_gqa_split_params &schedule [[buffer(3)]]"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "_Static_assert(sizeof(termite_metal_decode_gqa_split_params) == 16"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "[encoder setBytes:&launch->params length:sizeof(launch->params) atIndex:7]"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "[encoder setBytes:&launch->params length:sizeof(launch->params) atIndex:3]"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, host_source, "const bool use_decode_1x = !use_decode_gqa_split"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, host_source, "split_launch.scratch_bytes"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "launch->scratch_bytes"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "if (needs_explicit_split_barrier) [encoder memoryBarrierWithScope:MTLBarrierScopeBuffers];"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "runtime->decode_gqa_split_calls += 1"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "runtime->decode_gqa_split_schedule_calls[launch->shape][variant_index] += 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "TERMITE_METAL_DECODE_GQA_SPLIT_SWA_VARIANT"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "TERMITE_METAL_DECODE_GQA_SPLIT_GLOBAL_VARIANT"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "decode_gqa_split_explicitly_requested"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "decode_gqa_split_swa_override_value != NULL"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "decode_gqa_split_global_override_value != NULL"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, host_source, "bool decode_gqa_split_strict_failure = false;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "if (decode_gqa_split_strict_failure) return failure_code;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "if (decode_gqa_split_strict_failure) return -15;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "maxTotalThreadsPerThreadgroup >= TERMITE_METAL_DECODE_GQA_SPLIT_STAGE_THREADS"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "decode_gqa_split unavailable; using paged attention fallback"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "(runtime->decode_gqa_split_explicitly_requested &&"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "TERMITE_METAL_DECODE_GQA_SPLIT_SCRATCH_MAX_BYTES 1052672u"));
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, host_source, "newBufferWithLength:split_scratch_capacity"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "options:MTLResourceStorageModePrivate"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "scratch_runtime->submitted_frame_cb != nil"));

    // Gemma4 E2B/E4B's measured routes are exact 8Q with 1KV or 2KV f16 KV,
    // q_len 1-2, and HD256 SWA-512 or HD512 global attention.
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "num_heads != 8u || (num_kv_heads != 1u && num_kv_heads != 2u)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "q_len > 2u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "output_offset % (4u * sizeof(float)) != 0u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "head_dim == 256u && sliding_window == 512u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "head_dim == 512u && sliding_window == 0u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "TERMITE_METAL_DECODE_GQA_SPLIT_MIN_KV_TOKENS 512u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "kv_tokens < TERMITE_METAL_DECODE_GQA_SPLIT_MIN_KV_TOKENS"));

    // A ragged final KV tile must gather each physical V row and explicitly
    // zero masked lanes. Loading an entire private-buffer tile lets NaN page
    // padding contaminate the MMA even when its probability is zero.
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "if (kc + kk8 * 8u + 8u <= p.kv_tokens)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "sv[vi] = vphys != 0xffffffffu ? v_half[vphys * p.v_row_stride + kv_head_base + d8 + vc] : half(0.0f)"));
}

test "metal runtime source gates aligned and unrolled Q4 MM to measured shapes" {
    const host_source = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "src/backends/metal_kernels.m", std.testing.allocator, .limited(8 * 1024 * 1024));
    defer std.testing.allocator.free(host_source);

    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "kernel void termite_q4_0_linear_mm_sg_aligned("));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "kernel void termite_q4_0_linear_mm_sg_aligned_tail("));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "TERMITE_METAL_ENABLE_Q4_0_MM_SG_ALIGNED"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "TERMITE_METAL_DISABLE_Q4_0_MM_SG_ALIGNED"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "termite_dequantize_q4_0_4x4_half(x, il0, temp_a);"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "*(threadgroup half2x4 *)(sb + 64 * ib + 8 * ly) = (half2x4)(*((device const float2x4 *)y));"));

    // Measured E4B FFN and projection matrices default to aligned full/tail
    // routes. Only the measured 2048<->2560 full-tile projection may use the
    // unrolled route; explicit enable keeps the wider aligned experiment.
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "q4_0_mm_sg_aligned_e4b_ffn"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->in_dim == 2560u && descriptor->out_dim == 10240u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->in_dim == 10240u && descriptor->out_dim == 2560u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "q4_0_mm_sg_aligned_e4b_projection"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->in_dim == 2560u && descriptor->out_dim == 4096u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->in_dim == 4096u && descriptor->out_dim == 2560u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "q4_0_mm_sg_aligned_e4b_tail_projection"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->out_dim == 256u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->out_dim == 512u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->out_dim == 1024u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->out_dim == 2048u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->rows % 32u == 0u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "q4_0_mm_sg_unrolled_measured_projection"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->in_dim == 2048u && descriptor->out_dim == 2560u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->in_dim == 2560u && descriptor->out_dim == 2048u"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, host_source, 1, "descriptor->out_dim >= 1024u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "runtime->q4_0_mm_sg_aligned_dispatches += 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "runtime->q4_0_mm_sg_aligned_tail_dispatches += 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "runtime->q4_0_mm_sg_unrolled_dispatches += 1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->out_dim % 64u == 0u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->in_dim % 32u == 0u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->input_offset % 16u == 0u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "descriptor->rows % 32u != 0u"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "runtime->q4_0_mm_sg_aligned_tail_pipeline"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "const short nr1 = short(min(uint(NR1), p.rows - uint(r1)))"));
    try std.testing.expect(std.mem.containsAtLeast(u8, host_source, 1, "const short lr1 = short(min(int(tiitg) / int(NL1), int(nr1) - 1))"));
}

test "quant kernel compiler emits single-sourced backend source for every generated artifact" {
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        const compiled = compileQuantKernelSource(.{
            .backend = artifact.backend,
            .format = artifact.format,
            .row_bucket = artifact.row_bucket,
            .epilogue = artifact.epilogue,
        }) orelse return error.MissingGeneratedSource;
        const emitted = try emitCompiledSource(std.testing.allocator, compiled);
        defer emitted.deinit(std.testing.allocator);

        // Metal sources are borrowed from the assembled canonical constants;
        // nothing is re-derived at emit time.
        try std.testing.expect(!emitted.owned);
        try std.testing.expectEqualStrings(compiled.source, emitted.data);
        try std.testing.expect(try compiledSourceHeaderMatchesSource(std.testing.allocator, compiled, emitted.data));

        // The emitted .metal source ends with the descriptor-rendered kernel
        // (shared vocabulary helpers + body) for this route -- the same renderer
        // that produces the runtime-embedded region, so the two are single-sourced.
        const decoder = metal_renderer.decoderFor(artifact.format) orelse return error.MissingRuntimeQuantDecoder;
        const schedule = metalRouteScheduleFor(artifact.format, artifact.row_bucket, artifact.epilogue) orelse return error.MissingRuntimeQuantSchedule;
        const rendered_kernel = try metal_renderer.renderKernel(std.testing.allocator, artifact.kernel_id, decoder, schedule, artifact.epilogue);
        defer std.testing.allocator.free(rendered_kernel);
        try std.testing.expect(emitted.data.len > rendered_kernel.len + 1);
        try std.testing.expect(std.mem.endsWith(u8, emitted.data, rendered_kernel));
    }

    // Every CUDA source is rendered from the typed backend plan at emit time.
    // The checked-in files are full-byte goldens, so source fingerprints and
    // promotion evidence remain stable through the architecture migration.
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .cuda) continue;
        const compiled = compileQuantKernelArtifactSource(artifact) orelse return error.MissingGeneratedSource;
        const plan = compiled.cuda_render_plan orelse return error.MissingCudaRenderPlan;
        try plan.validate();

        const emitted = try emitCompiledSource(std.testing.allocator, compiled);
        defer emitted.deinit(std.testing.allocator);
        try std.testing.expect(emitted.owned);
        try std.testing.expectEqualStrings(compiled.source, emitted.data);

        const checked_in = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, artifact.source_path, std.testing.allocator, .limited(1 << 20));
        defer std.testing.allocator.free(checked_in);
        try std.testing.expectEqualStrings(checked_in, emitted.data);
    }
}

test "quant kernel compiler compile API requires a generated artifact" {
    try std.testing.expect(compileMetalKernelSource(.q6_k, .rows_9_64, .bias_gelu) == null);
    try std.testing.expect(compileMetalKernelSource(.q8_0, .rows_2_8, .argmax) == null);

    const lowering = loweringFor(.metal, .q6_k, .rows_9_64, .bias_gelu);
    try std.testing.expectEqual(LoweringRoute.handwritten_production, lowering.production_route);
    try std.testing.expectEqual(LoweringRoute.unsupported, lowering.candidate_route);
}

test "quant kernel compiler compile API rejects route metadata drift" {
    var checked: usize = 0;
    var promoted_checked: usize = 0;
    var candidate_checked: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        const compiled = compileQuantKernelArtifactSource(artifact) orelse return error.MissingCompiledQuantKernelSource;
        try std.testing.expect(compiledSourceMatchesRoute(compiled));
        checked += 1;
        if (artifactHasPromotionEvidence(artifact)) {
            promoted_checked += 1;
            try std.testing.expectEqual(LoweringRoute.generated_production, compiled.lowering.production_route);
            try std.testing.expectEqualStrings(artifact.kernel_id, compiled.lowering.production_kernel_id);
        } else {
            candidate_checked += 1;
            try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, compiled.lowering.candidate_route);
            try std.testing.expectEqualStrings(artifact.kernel_id, compiled.lowering.kernel_id);
        }
    }
    try std.testing.expectEqual(first_generated_matmul_artifacts.len, checked);
    try std.testing.expect(promoted_checked > 0);
    try std.testing.expect(candidate_checked > 0);

    var wrong_artifact = compileMetalKernelSource(.q6_k, .rows_2_8, .bias_gelu).?;
    wrong_artifact.artifact.kernel_id = first_general_metal_q5_bias_gelu_kernel_id;
    try std.testing.expect(!compiledSourceMatchesRoute(wrong_artifact));

    var wrong_candidate = compileMetalKernelSource(.q4_0, .rows_2_8, .none).?;
    wrong_candidate.lowering.kernel_id = first_general_metal_q6_kernel_id;
    try std.testing.expect(!compiledSourceMatchesRoute(wrong_candidate));

    var wrong_source_path = compileMetalKernelSource(.q6_k, .rows_2_8, .bias_gelu).?;
    wrong_source_path.source_path = first_general_metal_q5_bias_gelu_source_path;
    try std.testing.expect(!compiledSourceMatchesRoute(wrong_source_path));

    var wrong_check_command = compileMetalKernelSource(.q6_k, .rows_2_8, .bias_gelu).?;
    wrong_check_command.check_command = first_general_metal_q5_bias_gelu_check_command;
    try std.testing.expect(!compiledSourceMatchesRoute(wrong_check_command));

    var wrong_gate = compileMetalKernelSource(.q5_1, .rows_2_8, .none).?;
    wrong_gate.runtime_gate_env = null;
    try std.testing.expect(!compiledSourceMatchesRoute(wrong_gate));

    var wrong_production_state = compileMetalKernelSource(.q6_k, .rows_2_8, .none).?;
    wrong_production_state.production_enabled = false;
    try std.testing.expect(!compiledSourceMatchesRoute(wrong_production_state));
}

test "quant kernel compiler docs describe compile API guardrail" {
    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "QUANT_KERNEL_COMPILER.md", std.testing.allocator, .limited(64 * 1024));
    defer std.testing.allocator.free(contents);

    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "compileQuantKernelSource(...)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "is also a guardrail"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "descriptor, IR, route lowering, generated"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "production bit, and Metal runtime gate"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "Drift returns"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "compileMetalKernelSource(.q6_k, .rows_2_8, .bias_gelu)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "emitCompiledSource(allocator, compiled)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "compiled.source_path, check_command, runtime_gate_env"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "Repeated promotion and production-regression checks run two unrecorded warmup"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "`warmup_repeat_runs`"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "`metal_promotion_warmup_repeat_runs`"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "Route-all evidence is an observability check"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "The route-all evidence covers 50 generated cases"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "all 50 must be route-ready"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "46 must have provider-route evidence"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "all 18 candidate kernels are guarded. Nine have benchmark-evidence paths"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "`speedup_gate_missing` for Q4_0, Q5_0, Q5_1, Q6_K bias+GELU, and Q8_0\nbias+GELU"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "`unstable_benchmark_timing` for Q4_1, Q4_K bias+GELU, Q5_K\nbias+GELU, and Q8_1 none"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "four fused-bias shadows are blocked by\n`runtime_route_only`"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "and five are route-evidence-only"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "route-evidence-only"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "because their handwritten\nbaseline is unsupported"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "Timing drift from\n  an individual repeated run is reported as `production_regression_timing_drift`"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "does not hide the route/provider evidence"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "Promoted"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "generated-production routes report an empty `promotion_blocker`"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "`runtime_route_only`,"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "Unsupported-handwritten-baseline candidates are route-evidence-only"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "cannot be promoted"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "by the sequential speedup gate"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "benchmark_manifest=antfly.quant_kernel_benchmarks.v6:22:<fingerprint>"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "Runtime Observability"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "`fast_path_misses`: the sum of explicit, mutually exclusive fallback reasons"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "Actual generated Metal dispatch counters are not treated as handwritten\nfallbacks"));
}

fn metalRuntimeQuantFormatConstant(format: quant_matmul.Format) ?[]const u8 {
    return switch (format) {
        .q2_k => "TERMITE_METAL_QUANT_FORMAT_Q2_K",
        .q3_k => "TERMITE_METAL_QUANT_FORMAT_Q3_K",
        .q4_0 => "TERMITE_METAL_QUANT_FORMAT_Q4_0",
        .q4_1 => "TERMITE_METAL_QUANT_FORMAT_Q4_1",
        .q4_k => "TERMITE_METAL_QUANT_FORMAT_Q4_K",
        .q5_0 => "TERMITE_METAL_QUANT_FORMAT_Q5_0",
        .q5_1 => "TERMITE_METAL_QUANT_FORMAT_Q5_1",
        .q5_k => "TERMITE_METAL_QUANT_FORMAT_Q5_K",
        .q6_k => "TERMITE_METAL_QUANT_FORMAT_Q6_K",
        .q8_0 => "TERMITE_METAL_QUANT_FORMAT_Q8_0",
        .q8_1 => "TERMITE_METAL_QUANT_FORMAT_Q8_1",
        .q8_k => "TERMITE_METAL_QUANT_FORMAT_Q8_K",
        else => null,
    };
}

fn metalRuntimeGeneratedEpilogueConstant(epilogue: Epilogue) ?[]const u8 {
    return switch (epilogue) {
        .none => "TERMITE_METAL_GENERATED_QUANT_EPILOGUE_NONE",
        .bias => "TERMITE_METAL_GENERATED_QUANT_EPILOGUE_BIAS",
        .bias_gelu => "TERMITE_METAL_GENERATED_QUANT_EPILOGUE_BIAS_GELU",
        .relu => "TERMITE_METAL_GENERATED_QUANT_EPILOGUE_RELU",
        else => null,
    };
}

test "quant kernel compiler production Metal source includes only runtime-wired generated kernels" {
    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "src/backends/metal_kernels.m", std.testing.allocator, .limited(8 * 1024 * 1024));
    defer std.testing.allocator.free(contents);

    // Branch-added GPU fast paths stay positive opt-ins until their runtime
    // gates are model-scoped and their model-level release evidence is current.
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "TERMITE_METAL_ENABLE_PREFILL_SG_ATTENTION"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "TERMITE_METAL_ENABLE_PREFILL_SG_DIRECT_LOAD"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "TERMITE_METAL_ENABLE_Q6_K_R2_REDUCE"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "TERMITE_METAL_ENABLE_SMALL_ROWS_NORM_REDUCE"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "TERMITE_METAL_ENABLE_Q4_0_PAIR_ACTIVATION_SMALL_BATCH"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "Default-on: the flash-attention prefill kernel"));

    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "src/ops/metal/generated"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_lazy_metal_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_lazy_metal_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q4_0_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q4_0_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q4_1_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q4_1_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q5_0_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q5_0_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q5_1_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q5_1_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q4_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q4_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q4_bias_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q4_bias_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_bias_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_bias_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_bias_gelu_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_bias_gelu_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_relu_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_relu_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q2_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q2_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q2_bias_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q2_bias_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q2_bias_gelu_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q2_bias_gelu_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q3_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q3_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q3_bias_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q3_bias_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q3_bias_gelu_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q3_bias_gelu_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_1_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_1_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_k_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q8_k_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q5_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q5_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q5_bias_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q5_bias_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q5_bias_gelu_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q5_bias_gelu_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q6_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q6_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q6_bias_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q6_bias_kernel_id));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q6_bias_gelu_source_path));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, first_general_metal_q6_bias_gelu_kernel_id));
    try std.testing.expectEqual(@as(usize, 12), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "TERMITE_METAL_DISABLE_ANTFLY_Q8_0_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 8), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q2_K_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "TERMITE_METAL_DISABLE_ANTFLY_Q2_K_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 8), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q3_K_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "TERMITE_METAL_DISABLE_ANTFLY_Q3_K_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q4_0_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q4_1_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "TERMITE_METAL_DISABLE_ANTFLY_Q4_1_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q5_0_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "TERMITE_METAL_DISABLE_ANTFLY_Q5_0_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q5_1_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "TERMITE_METAL_DISABLE_ANTFLY_Q5_1_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q8_1_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "TERMITE_METAL_DISABLE_ANTFLY_Q8_1_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q8_K_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "TERMITE_METAL_DISABLE_ANTFLY_Q8_K_SMALL_BATCH"));
    // Eight generated wrapper/gate references plus the two fail-closed eager
    // provider checks that decide whether Q4_K may use the thin default path.
    try std.testing.expectEqual(@as(usize, 10), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q4_K_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "TERMITE_METAL_DISABLE_ANTFLY_Q4_K_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 8), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q5_K_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "TERMITE_METAL_DISABLE_ANTFLY_Q5_K_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 8), std.mem.count(u8, contents, "TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "TERMITE_METAL_DISABLE_ANTFLY_Q6_K_SMALL_BATCH"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "TERMITE_METAL_DISABLE_ANTFLY_GENERATED_QUANT"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "static BOOL termite_metal_runtime_generated_quant_disabled"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "if (termite_metal_runtime_generated_quant_disabled(runtime)) return NO;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "if (!termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q4_K_SMALL_BATCH_BIAS\", out_dim, in_dim)) return -2;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "typedef enum termite_metal_generated_quant_epilogue"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "TERMITE_METAL_GENERATED_QUANT_EPILOGUE_NONE"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "TERMITE_METAL_GENERATED_QUANT_EPILOGUE_BIAS"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "TERMITE_METAL_GENERATED_QUANT_EPILOGUE_BIAS_GELU"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "TERMITE_METAL_GENERATED_QUANT_EPILOGUE_RELU"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1,
        \\TERMITE_METAL_QUANT_FORMAT_Q8_0,
        \\        "TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH_BIAS",
        \\        runtime != NULL ? runtime->antfly_q8_0_small_batch_bias_pipeline : nil,
        \\        runtime != NULL ? &runtime->antfly_q8_0_small_batch_bias_dispatches : NULL,
        \\        TERMITE_METAL_GENERATED_QUANT_EPILOGUE_BIAS,
    ));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1,
        \\TERMITE_METAL_QUANT_FORMAT_Q8_0,
        \\        "TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH_BIAS_GELU",
        \\        runtime != NULL ? runtime->antfly_q8_0_small_batch_bias_gelu_pipeline : nil,
        \\        runtime != NULL ? &runtime->antfly_q8_0_small_batch_bias_gelu_dispatches : NULL,
        \\        TERMITE_METAL_GENERATED_QUANT_EPILOGUE_BIAS_GELU,
    ));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1,
        \\TERMITE_METAL_QUANT_FORMAT_Q5_K,
        \\        "TERMITE_METAL_ENABLE_ANTFLY_Q5_K_SMALL_BATCH_BIAS",
        \\        runtime != NULL ? runtime->antfly_q5_k_small_batch_bias_pipeline : nil,
        \\        runtime != NULL ? &runtime->antfly_q5_k_small_batch_bias_dispatches : NULL,
        \\        TERMITE_METAL_GENERATED_QUANT_EPILOGUE_BIAS,
    ));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1,
        \\TERMITE_METAL_QUANT_FORMAT_Q5_K,
        \\        "TERMITE_METAL_ENABLE_ANTFLY_Q5_K_SMALL_BATCH_BIAS_GELU",
        \\        runtime != NULL ? runtime->antfly_q5_k_small_batch_bias_gelu_pipeline : nil,
        \\        runtime != NULL ? &runtime->antfly_q5_k_small_batch_bias_gelu_dispatches : NULL,
        \\        TERMITE_METAL_GENERATED_QUANT_EPILOGUE_BIAS_GELU,
    ));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1,
        \\TERMITE_METAL_QUANT_FORMAT_Q6_K,
        \\        "TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH_BIAS",
        \\        runtime != NULL ? runtime->antfly_q6_k_small_batch_bias_pipeline : nil,
        \\        runtime != NULL ? &runtime->antfly_q6_k_small_batch_bias_dispatches : NULL,
        \\        TERMITE_METAL_GENERATED_QUANT_EPILOGUE_BIAS,
    ));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1,
        \\TERMITE_METAL_QUANT_FORMAT_Q6_K,
        \\        "TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH_BIAS_GELU",
        \\        runtime != NULL ? runtime->antfly_q6_k_small_batch_bias_gelu_pipeline : nil,
        \\        runtime != NULL ? &runtime->antfly_q6_k_small_batch_bias_gelu_dispatches : NULL,
        \\        TERMITE_METAL_GENERATED_QUANT_EPILOGUE_BIAS_GELU,
    ));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1,
        \\runtime != NULL ? &runtime->antfly_q2_k_small_batch_bias_dispatches : NULL,
        \\        128u,
        \\        1u,
    ));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1,
        \\runtime != NULL ? &runtime->antfly_q8_0_small_batch_bias_gelu_dispatches : NULL,
        \\        32u,
        \\        2u,
    ));
    const none_encoder = std.mem.indexOf(u8, contents, "static int termite_metal_encode_quant_matmul_generic_none_on_encoder") orelse return error.MissingMetalNoneEncoder;
    const q8_encoder = std.mem.indexOfPos(u8, contents, none_encoder, "static int termite_metal_encode_q8_0_linear(") orelse return error.MissingMetalQ8Encoder;
    const q8_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q8_0_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ8Enable;
    const q2_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q2_K_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ2Enable;
    const q4_0_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q4_0_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ4_0Enable;
    const q4_1_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q4_1_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ4_1Enable;
    const q5_0_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q5_0_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ5_0Enable;
    const q5_1_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q5_1_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ5_1Enable;
    const q8_1_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q8_1_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ8_1Enable;
    const q8_k_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q8_K_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ8_KEnable;
    const q3_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q3_K_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ3Enable;
    const q4_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q4_K_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ4Enable;
    const q5_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q5_K_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ5Enable;
    const q6_enable = std.mem.indexOf(u8, contents, "termite_metal_runtime_candidate_gate(runtime, \"TERMITE_METAL_ENABLE_ANTFLY_Q6_K_SMALL_BATCH\", descriptor->out_dim, descriptor->in_dim)") orelse return error.MissingMetalQ6Enable;
    try std.testing.expect(q8_enable > none_encoder and q8_enable < q8_encoder);
    try std.testing.expect(q2_enable > none_encoder and q2_enable < q8_encoder);
    try std.testing.expect(q3_enable > none_encoder and q3_enable < q8_encoder);
    try std.testing.expect(q4_0_enable > none_encoder and q4_0_enable < q8_encoder);
    try std.testing.expect(q4_1_enable > none_encoder and q4_1_enable < q8_encoder);
    try std.testing.expect(q5_0_enable > none_encoder and q5_0_enable < q8_encoder);
    try std.testing.expect(q5_1_enable > none_encoder and q5_1_enable < q8_encoder);
    try std.testing.expect(q8_1_enable > none_encoder and q8_1_enable < q8_encoder);
    try std.testing.expect(q8_k_enable > none_encoder and q8_k_enable < q8_encoder);
    try std.testing.expect(q4_enable > none_encoder and q4_enable < q8_encoder);
    try std.testing.expect(q5_enable > none_encoder and q5_enable < q8_encoder);
    try std.testing.expect(q6_enable > none_encoder and q6_enable < q8_encoder);
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "strcmp(kernel_name, \"antfly_q4_k_small_batch_msl_v1\") == 0"));
    const q5_1_kernel = std.mem.indexOf(u8, contents, "kernel void antfly_q5_1_small_batch_msl_v1") orelse return error.MissingMetalQ5_1Kernel;
    // Single-sourced region: helpers are emitted once up top, kernel bodies
    // together after. Bound the q5_1 body by the next generated kernel.
    const q5_1_kernel_end = std.mem.indexOfPos(u8, contents, q5_1_kernel + 1, "kernel void antfly_") orelse return error.MissingMetalQ5_1KernelEnd;
    const q5_1_kernel_body = contents[q5_1_kernel..q5_1_kernel_end];
    try std.testing.expect(std.mem.containsAtLeast(u8, q5_1_kernel_body, 1, "int col0 = int(group_pos.x << 1); int col1 = col0 + 1;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, q5_1_kernel_body, 1, "device const uchar *col1_weight = has_col1 ? weight_q5_1 + col1 * block_count * 24 : col0_weight;"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, q5_1_kernel_body, 1, "int col = int(group_pos.x); int row = int(group_pos.y);"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "uint32_t threads_per_threadgroup,\n    uint32_t cols_per_threadgroup,"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "const NSUInteger threads_per_threadgroup_size = (NSUInteger)threads_per_threadgroup;"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "const NSUInteger threads_per_threadgroup = (use_antfly_q2_k_small_batch || use_antfly_q3_k_small_batch"));
    // The launch-shape lookup is now a codegen-generated table (from
    // metal_production_schedules), not a hand-written switch. Validate the
    // generated region: the table + the table-scan lookup function.
    const launch_table = std.mem.indexOf(u8, contents, "termite_metal_generated_quant_launch_table[] = {") orelse return error.MissingMetalGeneratedLaunchTable;
    const launch_helper = std.mem.indexOf(u8, contents, "static bool termite_metal_generated_quant_launch_shape_for") orelse return error.MissingMetalGeneratedLaunchShapeHelper;
    const launch_helper_end = std.mem.indexOfPos(u8, contents, launch_helper, "static uint8_t termite_metal_quant_matmul_descriptor_planned_dispatch") orelse return error.MissingMetalGeneratedLaunchShapeHelperEnd;
    try std.testing.expect(launch_table < launch_helper);
    try std.testing.expect(launch_helper < none_encoder);
    const launch_region_body = contents[launch_table..launch_helper_end];
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .metal or artifact.row_bucket != .rows_2_8) continue;
        const format_constant = metalRuntimeQuantFormatConstant(artifact.format) orelse continue;
        const epilogue_constant = metalRuntimeGeneratedEpilogueConstant(artifact.epilogue) orelse continue;
        try std.testing.expect(std.mem.containsAtLeast(u8, launch_region_body, 1, format_constant));
        try std.testing.expect(std.mem.containsAtLeast(u8, launch_region_body, 1, epilogue_constant));
    }
    // The table encodes threads, columns, and rows per threadgroup, then scans
    // by (format, epilogue).
    try std.testing.expect(std.mem.containsAtLeast(u8, launch_region_body, 1, ", 32u, 2u, 1u },"));
    try std.testing.expect(std.mem.containsAtLeast(u8, launch_region_body, 1, "entry->format == format && entry->epilogue == epilogue"));
    try std.testing.expect(std.mem.containsAtLeast(u8, launch_region_body, 1, "shape->cols_per_threadgroup = entry->cols_per_threadgroup;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, launch_region_body, 1, "shape->rows_per_threadgroup = entry->rows_per_threadgroup;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 2, "termite_metal_generated_quant_launch_shape_for(descriptor->format, TERMITE_METAL_GENERATED_QUANT_EPILOGUE_NONE, descriptor->rows, &launch_shape)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 2, "launch_shape.threads_per_threadgroup"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 2, "launch_shape.cols_per_threadgroup"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "return termite_metal_dispatch_quant_matmul_none(provider, runtime, TERMITE_METAL_QUANT_FORMAT_Q4_K"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "return termite_metal_dispatch_quant_matmul_none(provider, NULL, TERMITE_METAL_QUANT_FORMAT_Q5_K"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "return termite_metal_dispatch_quant_matmul_none(provider, NULL, TERMITE_METAL_QUANT_FORMAT_Q6_K"));
}

fn metalCEnvArgForArtifact(allocator: std.mem.Allocator, artifact: GeneratedMatmulArtifact) ![]u8 {
    if (artifactRuntimeGateEnv(artifact)) |env| {
        return try std.fmt.allocPrint(allocator, "\"{s}\"", .{std.mem.span(env)});
    }
    return try allocator.dupe(u8, "NULL");
}

fn metalCBespokeRuntimeWrapperHasArtifact(
    allocator: std.mem.Allocator,
    contents: []const u8,
    artifact: GeneratedMatmulArtifact,
    counter_name: []const u8,
    format_constant: []const u8,
    epilogue_constant: []const u8,
) !bool {
    if (artifact.format != .q4_k or (artifact.epilogue != .bias and artifact.epilogue != .bias_gelu)) return false;

    const wrapper_suffix: []const u8 = switch (artifact.epilogue) {
        .bias => "q4_k_bias",
        .bias_gelu => "q4_k_bias_gelu",
        else => unreachable,
    };
    const function_name = try std.fmt.allocPrint(
        allocator,
        "int termite_metal_decode_runtime_apply_quantized_linear_{s}_slot_device(",
        .{wrapper_suffix},
    );
    defer allocator.free(function_name);
    const start = std.mem.indexOf(u8, contents, function_name) orelse return false;
    const end = std.mem.indexOfPos(u8, contents, start + function_name.len, "\n}\n\n") orelse return false;
    const body = contents[start..end];

    if (artifactRuntimeGateEnv(artifact)) |env| {
        const gate = try std.fmt.allocPrint(allocator, "termite_metal_runtime_candidate_gate(runtime, \"{s}\", out_dim, in_dim)", .{std.mem.span(env)});
        defer allocator.free(gate);
        if (!std.mem.containsAtLeast(u8, body, 1, gate)) return false;
    } else if (artifactCandidateOptInGateEnv(artifact)) |env| {
        const stale_gate = try std.fmt.allocPrint(allocator, "getenv(\"{s}\")", .{std.mem.span(env)});
        defer allocator.free(stale_gate);
        if (std.mem.containsAtLeast(u8, body, 1, stale_gate)) return false;
    }

    const pipeline = try std.fmt.allocPrint(allocator, "runtime->antfly_{s}_pipeline", .{counter_name});
    defer allocator.free(pipeline);
    const dispatch_counter = try std.fmt.allocPrint(allocator, "runtime->antfly_{s}_dispatches += 1;", .{counter_name});
    defer allocator.free(dispatch_counter);
    return std.mem.containsAtLeast(u8, body, 1, format_constant) and
        std.mem.containsAtLeast(u8, body, 1, epilogue_constant) and
        std.mem.containsAtLeast(u8, body, 1, pipeline) and
        std.mem.containsAtLeast(u8, body, 1, dispatch_counter);
}

test "quant kernel compiler generated Metal wrapper gates follow artifact metadata" {
    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "src/backends/metal_kernels.m", std.testing.allocator, .limited(8 * 1024 * 1024));
    defer std.testing.allocator.free(contents);

    var runtime_checked: usize = 0;
    var provider_checked: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .metal or !artifactRuntimeWired(artifact)) continue;
        const provider_helper: []const u8 = switch (artifact.epilogue) {
            .bias, .bias_gelu => "termite_metal_dispatch_generated_quant_bias",
            .relu => "termite_metal_dispatch_generated_quant_relu",
            else => continue,
        };
        const counter_name = metalGeneratedCounterNameForArtifact(artifact) orelse continue;
        const format_constant = metalRuntimeQuantFormatConstant(artifact.format) orelse continue;
        const epilogue_constant = metalRuntimeGeneratedEpilogueConstant(artifact.epilogue) orelse continue;
        const env_arg = try metalCEnvArgForArtifact(std.testing.allocator, artifact);
        defer std.testing.allocator.free(env_arg);

        const runtime_snippet = try std.fmt.allocPrint(
            std.testing.allocator,
            "{s},\n        {s},\n        runtime != NULL ? runtime->antfly_{s}_pipeline : nil,\n        runtime != NULL ? &runtime->antfly_{s}_dispatches : NULL,\n        {s},",
            .{ format_constant, env_arg, counter_name, counter_name, epilogue_constant },
        );
        defer std.testing.allocator.free(runtime_snippet);
        const runtime_found = std.mem.containsAtLeast(u8, contents, 1, runtime_snippet) or
            try metalCBespokeRuntimeWrapperHasArtifact(std.testing.allocator, contents, artifact, counter_name, format_constant, epilogue_constant);
        try std.testing.expect(runtime_found);
        runtime_checked += 1;

        if (artifactHasMetalProviderRouteEvidence(artifact)) {
            const provider_snippet = try std.fmt.allocPrint(
                std.testing.allocator,
                "return {s}(provider, {s}, provider->antfly_{s}_pipeline, {s}, {s},",
                .{ provider_helper, env_arg, counter_name, format_constant, epilogue_constant },
            );
            defer std.testing.allocator.free(provider_snippet);
            try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, provider_snippet));
            provider_checked += 1;
        }
    }
    try std.testing.expect(runtime_checked > 0);
    try std.testing.expect(provider_checked > 0);
}

test "quant kernel compiler embedded Metal source keeps generated q5 reduction and q6 tile" {
    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "src/backends/metal_kernels.m", std.testing.allocator, .limited(8 * 1024 * 1024));
    defer std.testing.allocator.free(contents);

    // The hybrid-simd reduction structure, independent of the simdgroup count
    // (`lane_id >= {threads/32}u`), which varies with the route's thread count.
    const optimized_reduction_head = "threadgroup float partial[32]; acc = simd_sum(acc); if (lane_id == 0u) partial[simdgroup_id] = acc; if (simdgroup_id == 0u && lane_id >= ";
    const optimized_reduction_tail = "u) partial[lane_id] = 0.0f; threadgroup_barrier(mem_flags::mem_threadgroup); float total = simd_sum(partial[lane_id]);";
    const old_reduction = "threadgroup float partial[32]; if (simdgroup_id == 0u) partial[lane_id] = 0.0f; acc = simd_sum(acc); threadgroup_barrier(mem_flags::mem_threadgroup); if (lane_id == 0u) partial[simdgroup_id] = acc; threadgroup_barrier(mem_flags::mem_threadgroup); float total = simd_sum(partial[lane_id]);";
    const q5_kernels = [_][]const u8{
        "antfly_q5_k_small_batch_bias_msl_v1",
        "antfly_q5_k_small_batch_bias_gelu_msl_v1",
    };

    for (q5_kernels) |kernel| {
        const start = std.mem.indexOf(u8, contents, kernel) orelse return error.MissingEmbeddedQ5Q6BiasKernel;
        const end = std.mem.indexOfPos(u8, contents, start, "\"}\\n\"") orelse return error.MissingEmbeddedQ5Q6BiasKernelEnd;
        const body = contents[start..end];
        try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, optimized_reduction_head));
        try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, optimized_reduction_tail));
        try std.testing.expect(!std.mem.containsAtLeast(u8, body, 1, old_reduction));
    }

    const q6_kernels = [_][]const u8{
        "antfly_q6_k_small_batch_bias_msl_v1",
        "antfly_q6_k_small_batch_bias_gelu_msl_v1",
    };
    for (q6_kernels) |kernel| {
        const start = std.mem.indexOf(u8, contents, kernel) orelse return error.MissingEmbeddedQ5Q6BiasKernel;
        const end = std.mem.indexOfPos(u8, contents, start, "\"}\\n\"") orelse return error.MissingEmbeddedQ5Q6BiasKernelEnd;
        const body = contents[start..end];
        try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "const uint NSG = 4u; const uint NC = 4u; const uint NR = 2u;"));
        try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "float acc[4][2]"));
        try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "simd_sum(acc[c][rr])"));
    }
}

test "quant kernel compiler generated q5 k and q6 k sources share the qk half helper" {
    const qk_sources = [_][]const u8{
        first_general_metal_q5_source,
        first_general_metal_q5_bias_source,
        first_general_metal_q5_bias_gelu_source,
        first_general_metal_q6_source,
        first_general_metal_q6_bias_source,
        first_general_metal_q6_bias_gelu_source,
    };

    for (qk_sources) |source| {
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, metal_renderer.helper_qk_half_le_to_float.msl));
        try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "antfly_qk_half_le_to_float(block"));
    }
}

test "quant kernel compiler generated Metal headers match production state" {
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        const compiled = compileQuantKernelSource(.{
            .backend = artifact.backend,
            .format = artifact.format,
            .row_bucket = artifact.row_bucket,
            .epilogue = artifact.epilogue,
        }) orelse return error.MissingGeneratedSource;
        const emitted = try emitCompiledSource(std.testing.allocator, compiled);
        defer emitted.deinit(std.testing.allocator);
        const source = emitted.data;
        if (artifact.production_enabled) {
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "production_enabled=true"));
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "Promoted after"));
            try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "stays on native handwritten MSL until this"));
        } else {
            try std.testing.expect(std.mem.containsAtLeast(u8, source, 1, "production_enabled=false"));
            try std.testing.expect(!std.mem.containsAtLeast(u8, source, 1, "Promoted after"));
        }
    }
}

test "quant kernel compiler Metal build check covers generated and promoted artifacts" {
    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "build.zig", std.testing.allocator, .limited(256 * 1024));
    defer std.testing.allocator.free(contents);
    const test_filter_contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "build/test_filters.zig", std.testing.allocator, .limited(16 * 1024));
    defer std.testing.allocator.free(test_filter_contents);

    const macos_gate = std.mem.indexOf(u8, contents, "if (target.result.os.tag == .macos) {\n        const quant_kernel_metal_artifact_check = b.addRunArtifact(quant_kernel_codegen_exe);") orelse return error.MissingMetalBuildMacosGate;
    const non_macos_fail_closed = std.mem.indexOf(u8, contents, "} else {\n        const quant_kernel_metal_unavailable = b.addFail(metal_unavailable_message);\n        quant_kernel_metal_unavailable_step = &quant_kernel_metal_unavailable.step;\n        quant_kernel_metal_check_step.dependOn(&quant_kernel_metal_unavailable.step);\n    }\n\n    const quant_kernel_metal_runtime_check_step") orelse return error.MissingMetalBuildNonMacosFailClosed;
    try std.testing.expect(macos_gate < non_macos_fail_closed);
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "return build_test_filters.select("));
    try std.testing.expect(std.mem.containsAtLeast(u8, test_filter_contents, 1, "else if (std.mem.startsWith(u8, arg, \"-\")) {"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "fn targetRunsOnBuildHost(b: *std.Build, target: std.Build.ResolvedTarget) bool"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, ".target = b.graph.host"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_artifact_check.addArg(\"--check-metal\")"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_artifact_check.step.dependOn(&quant_kernel_codegen_test_check.step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_artifact_check_step = &quant_kernel_metal_artifact_check.step"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "Metal quant kernel evidence targets require a macOS target with xcrun/Metal; no Metal runtime evidence was run."));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "const metal_unavailable_step = quant_kernel_metal_unavailable_step orelse unreachable;"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_production_regression_step.dependOn(metal_unavailable_step)"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "run_quant_kernel_metal_runtime_default_check"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_runtime_default_check_step"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant-kernel-metal-runtime-route-all"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "run_quant_kernel_metal_runtime_route_all.has_side_effects = true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "--runtime-route-all"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "--require-runtime-route-all"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant-kernel-metal-production-regression-check"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "\"500\",\n            \"--production-regression-check\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "--production-regression-check"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "run_quant_kernel_metal_production_regression.has_side_effects = true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "antfly-quant-metal-runtime-route-all-evidence-{x}.json"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "addOutputFileArg(route_all_evidence_name)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "antfly-quant-metal-production-regression-evidence-{x}.json"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "addOutputFileArg(production_regression_evidence_name)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant-kernel-metal-blocker-evidence-refresh"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "--refresh-blocker-evidence"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "refresh_quant_kernel_metal_blocker_evidence.has_side_effects = true"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "antfly-quant-metal-blocker-evidence-{x}"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "addOutputDirectoryArg(blocker_evidence_dir_name)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 2, "--blocker-evidence-dir"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 2, "addDirectoryArg(blocker_evidence_dir)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant-kernel-metal-blocker-strict-check"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "--confirm-cleared-blockers"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "--fail-on-cleared-blocker"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "refresh_quant_kernel_metal_blocker_evidence.step.dependOn(&check_quant_kernel_metal_runtime_route_all.step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "check_quant_kernel_metal_blocker_evidence.step.dependOn(&refresh_quant_kernel_metal_blocker_evidence.step)"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "run_quant_kernel_metal_runtime_route_all.step.dependOn(&run_quant_kernel_metal_production_regression.step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {\n        if (quant_kernel_metal_production_regression_run_step) |production_regression_step| {\n            production_regression_step.dependOn(&run_quant_kernel_metal_runtime_check_tests.step);\n        }\n        quant_kernel_metal_runtime_check_step.dependOn(&run_quant_kernel_metal_runtime_check_tests.step);\n    }"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant-kernel-metal-local-check"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_local_check_step.dependOn(quant_kernel_metal_runtime_default_check_step orelse quant_kernel_metal_runtime_check_step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {\n        quant_kernel_metal_local_check_step.dependOn(&run_quant_kernel_metal_runtime_check_tests.step);\n    }"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_local_check_step.dependOn(quant_kernel_metal_runtime_route_all_step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_local_check_step.dependOn(quant_kernel_metal_production_regression_step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_local_check_step.dependOn(&run_quant_kernel_compiler_tests.step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_local_check_step.dependOn(&quant_kernel_codegen_test_check.step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_local_check_step.dependOn(&cuda_artifact_source_policy_check.step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "if (enable_metal and target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {\n        quant_kernel_local_check_step.dependOn(quant_kernel_metal_local_check_step);\n    }"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_local_check_step.dependOn(quant_kernel_metal_runtime_route_all_step)"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_local_check_step.dependOn(quant_kernel_metal_production_regression_step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "if (targetRunsOnBuildHost(b, target)) {\n        quant_kernel_local_check_step.dependOn(&run_quant_kernel_compiler_tests.step);\n        quant_kernel_metal_local_check_step.dependOn(&run_quant_kernel_compiler_tests.step);\n    }"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "if (enable_cuda and targetRunsOnBuildHost(b, target)) {\n        const run_quant_kernel_cuda_microbench_tests = b.addRunArtifact(tests);"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "if (targetRunsOnBuildHost(b, target)) {\n        quant_kernel_local_check_step.dependOn(&run_quant_kernel_cuda_microbench_tests.step);\n    }"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "test-metal-gemma4-prefill-frame-e4b-generated-q8"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "\"--e4b-smoke\",\n        \"--generated-q8-smoke\""));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "test-metal-gemma4-prefill-frame-e4b-generated-q8-q4-0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "ANTFLY_INFERENCE_GEMMA4_MIN_GENERATED_Q4_0_SMALL_BATCH=1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "ANTFLY_INFERENCE_GEMMA4_MIN_GENERATED_FAMILY_COUNT=2"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "ANTFLY_INFERENCE_GEMMA4_EXPECTED_GENERATED_TOP_FAMILY=q4_0"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "ANTFLY_INFERENCE_GEMMA4_MIN_GENERATED_TOP_COUNT=1"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant-kernel-metal-model-local-check"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_model_local_check_step.dependOn(quant_kernel_metal_local_check_step)"));
    try std.testing.expect(!std.mem.containsAtLeast(u8, contents, 1, "metal_gemma4_prefill_frame_e4b_generated_q8_q4_0_test.step.dependOn(quant_kernel_metal_local_check_step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "if (target.result.os.tag == .macos and targetRunsOnBuildHost(b, target)) {\n        quant_kernel_metal_model_local_check_step.dependOn(&metal_gemma4_prefill_frame_e4b_generated_q8_q4_0_test.step);\n    }"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant-kernel-metal-industry-local-check"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_industry_local_check_step.dependOn(quant_kernel_metal_local_check_step)"));
    try std.testing.expect(std.mem.containsAtLeast(u8, contents, 1, "quant_kernel_metal_industry_local_check_step.dependOn(quant_kernel_metal_model_local_check_step)"));

    var metal_artifact_count: usize = 0;
    for (first_generated_matmul_artifacts) |artifact| {
        if (artifact.backend != .metal) continue;
        metal_artifact_count += 1;
        try std.testing.expect(std.mem.containsAtLeast(u8, artifact.check_command, 1, artifact.source_path));
    }
    try std.testing.expect(metal_artifact_count > 0);
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "src/ops/metal/generated/quant_kernel_"));
    try std.testing.expectEqual(@as(usize, 0), std.mem.count(u8, contents, "src/ops/metal/artifacts/quant_kernel_"));
}

test "quant kernel compiler coverage keeps CUDA and Metal route metadata aligned" {
    for (first_coverage) |case| {
        const cuda = loweringFor(.cuda, case.format, case.row_bucket, case.epilogue);
        const metal = loweringFor(.metal, case.format, case.row_bucket, case.epilogue);
        const spec = specFor(case.format).?;
        const cuda_dev_candidate = generatedArtifactForCandidate(.cuda, case.format, case.row_bucket, case.epilogue) != null;
        const metal_dev_candidate = generatedArtifactForCandidate(.metal, case.format, case.row_bucket, case.epilogue) != null;
        const metal_generated_production = metal.production_route == .generated_production;
        try std.testing.expectEqual(case.format, cuda.format);
        try std.testing.expectEqual(case.row_bucket, cuda.row_bucket);
        try std.testing.expectEqual(case.epilogue, cuda.epilogue);

        if (!spec.supportsBackend(.cuda)) {
            try std.testing.expectEqual(LoweringRoute.unsupported, cuda.production_route);
            try std.testing.expectEqual(FallbackReason.unsupported_backend, cuda.fallback_reason);
        } else if (!supportsEpilogueForBackend(spec, .cuda, case.epilogue)) {
            try std.testing.expectEqual(LoweringRoute.unsupported, cuda.production_route);
            try std.testing.expectEqual(FallbackReason.unsupported_epilogue, cuda.fallback_reason);
        } else if (cuda_dev_candidate) {
            const cuda_candidate = generatedArtifactForCandidate(.cuda, case.format, case.row_bucket, case.epilogue).?;
            if (artifactHasPromotionEvidence(cuda_candidate)) {
                try std.testing.expectEqual(LoweringRoute.generated_production, cuda.production_route);
                try std.testing.expectEqual(FallbackReason.none, cuda.fallback_reason);
                try std.testing.expectEqual(LoweringRoute.unsupported, cuda.candidate_route);
            } else {
                const expected_cuda_fallback: FallbackReason = if (artifactRuntimeWired(cuda_candidate))
                    .generated_runtime_not_wired
                else
                    .generated_artifact_missing;
                try std.testing.expectEqual(LoweringRoute.handwritten_production, cuda.production_route);
                try std.testing.expectEqual(expected_cuda_fallback, cuda.fallback_reason);
                try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, cuda.candidate_route);
            }
        } else {
            try std.testing.expectEqual(LoweringRoute.handwritten_production, cuda.production_route);
            try std.testing.expectEqual(FallbackReason.none, cuda.fallback_reason);
        }

        if (!spec.supportsBackend(.metal)) {
            try std.testing.expectEqual(LoweringRoute.unsupported, metal.production_route);
            try std.testing.expectEqual(FallbackReason.unsupported_backend, metal.fallback_reason);
        } else if (!supportsEpilogueForBackend(spec, .metal, case.epilogue)) {
            try std.testing.expectEqual(LoweringRoute.unsupported, metal.production_route);
            try std.testing.expectEqual(FallbackReason.unsupported_epilogue, metal.fallback_reason);
        } else if (metal_generated_production) {
            try std.testing.expectEqual(LoweringRoute.generated_production, metal.production_route);
            try std.testing.expectEqual(FallbackReason.none, metal.fallback_reason);
            try std.testing.expectEqual(LoweringRoute.unsupported, metal.candidate_route);
        } else if (metal_dev_candidate) {
            try std.testing.expectEqual(LoweringRoute.handwritten_production, metal.production_route);
            const artifact = generatedArtifactForKernel(.metal, metal.kernel_id) orelse return error.MissingMetalCandidateArtifact;
            const expected_fallback: FallbackReason = if (checkedInMetalEvidenceForKernel(artifact.kernel_id) != null)
                .generated_runtime_not_wired
            else
                .generated_artifact_missing;
            try std.testing.expectEqual(expected_fallback, metal.fallback_reason);
            try std.testing.expectEqual(LoweringRoute.generated_dev_candidate, metal.candidate_route);
        } else {
            try std.testing.expectEqual(LoweringRoute.handwritten_production, metal.production_route);
            try std.testing.expectEqual(FallbackReason.none, metal.fallback_reason);
        }

        try std.testing.expect(cuda.schedule.dispatch != .scalar);
        try std.testing.expect(metal.schedule.dispatch != .scalar);
    }
}

test "quant kernel compiler rejects unsupported shapes and epilogues explicitly" {
    const empty = loweringFor(.cuda, .q4_k, .rows_0, .bias_gelu);
    try std.testing.expectEqual(LoweringRoute.unsupported, empty.production_route);
    try std.testing.expectEqual(FallbackReason.unsupported_shape, empty.fallback_reason);

    const future_epilogues = [_]Epilogue{ .triple, .relu, .gelu, .add, .argmax };
    for (future_epilogues) |epilogue| {
        const unsupported_epilogue = loweringFor(.cuda, .q4_k, .rows_2_8, epilogue);
        try std.testing.expectEqual(LoweringRoute.unsupported, unsupported_epilogue.production_route);
        try std.testing.expectEqual(FallbackReason.unsupported_epilogue, unsupported_epilogue.fallback_reason);
    }
    const q6_bias_gelu = loweringFor(.cuda, .q6_k, .rows_2_8, .bias_gelu);
    try std.testing.expectEqual(LoweringRoute.unsupported, q6_bias_gelu.production_route);
    try std.testing.expectEqual(FallbackReason.unsupported_epilogue, q6_bias_gelu.fallback_reason);

    const q5_cuda = loweringFor(.cuda, .q5_k, .rows_2_8, .none);
    try std.testing.expectEqual(LoweringRoute.unsupported, q5_cuda.production_route);
    try std.testing.expectEqual(FallbackReason.unsupported_backend, q5_cuda.fallback_reason);

    const q5_cuda_bias_gelu = loweringFor(.cuda, .q5_k, .rows_2_8, .bias_gelu);
    try std.testing.expectEqual(LoweringRoute.unsupported, q5_cuda_bias_gelu.production_route);
    try std.testing.expectEqual(FallbackReason.unsupported_backend, q5_cuda_bias_gelu.fallback_reason);

    const unsupported_format = loweringFor(.cuda, .unknown, .rows_2_8, .bias_gelu);
    try std.testing.expectEqual(LoweringRoute.unsupported, unsupported_format.production_route);
    try std.testing.expectEqual(FallbackReason.unsupported_format, unsupported_format.fallback_reason);
}

test "quant kernel compiler counters classify route lowerings" {
    const lazy = countersForLowering(loweringFor(.cuda, .q4_k, .rows_2_8, .bias_gelu));
    try std.testing.expectEqual(@as(usize, 1), lazy.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(usize, 1), lazy.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(usize, 0), lazy.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(usize, 0), lazy.quant_kernel_unsupported_routes);
    try std.testing.expectEqual(@as(usize, 1), lazy.quant_kernel_generated_candidates);
    try std.testing.expectEqual(@as(usize, 1), lazy.quant_kernel_fallback_generated_artifact_missing);
    try std.testing.expectEqual(@as(usize, 0), lazy.quant_kernel_fallback_generated_runtime_not_wired);
    try std.testing.expectEqual(@as(usize, 0), lazy.quant_kernel_fallback_unsupported);

    const q6_ready = countersForLowering(loweringFor(.metal, .q6_k, .rows_2_8, .none));
    try std.testing.expectEqual(@as(usize, 1), q6_ready.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(usize, 0), q6_ready.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(usize, 1), q6_ready.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(usize, 0), q6_ready.quant_kernel_unsupported_routes);
    try std.testing.expectEqual(@as(usize, 0), q6_ready.quant_kernel_generated_candidates);
    try std.testing.expectEqual(@as(usize, 0), q6_ready.quant_kernel_fallback_generated_artifact_missing);
    try std.testing.expectEqual(@as(usize, 0), q6_ready.quant_kernel_fallback_generated_runtime_not_wired);
    try std.testing.expectEqual(@as(usize, 0), q6_ready.quant_kernel_fallback_unsupported);

    const handwritten_only = countersForLowering(loweringFor(.cuda, .q8_0, .rows_1, .none));
    try std.testing.expectEqual(@as(usize, 1), handwritten_only.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(usize, 1), handwritten_only.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(usize, 0), handwritten_only.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(usize, 0), handwritten_only.quant_kernel_unsupported_routes);
    try std.testing.expectEqual(@as(usize, 0), handwritten_only.quant_kernel_generated_candidates);
    try std.testing.expectEqual(@as(usize, 0), handwritten_only.quant_kernel_fallback_generated_artifact_missing);
    try std.testing.expectEqual(@as(usize, 0), handwritten_only.quant_kernel_fallback_generated_runtime_not_wired);
    try std.testing.expectEqual(@as(usize, 0), handwritten_only.quant_kernel_fallback_unsupported);

    const unsupported = countersForLowering(loweringFor(.cuda, .unknown, .rows_2_8, .bias_gelu));
    try std.testing.expectEqual(@as(usize, 1), unsupported.quant_kernel_planned_ops);
    try std.testing.expectEqual(@as(usize, 0), unsupported.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(usize, 0), unsupported.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(usize, 1), unsupported.quant_kernel_unsupported_routes);
    try std.testing.expectEqual(@as(usize, 0), unsupported.quant_kernel_generated_candidates);
    try std.testing.expectEqual(@as(usize, 0), unsupported.quant_kernel_fallback_generated_artifact_missing);
    try std.testing.expectEqual(@as(usize, 0), unsupported.quant_kernel_fallback_generated_runtime_not_wired);
    try std.testing.expectEqual(@as(usize, 1), unsupported.quant_kernel_fallback_unsupported_format);
    try std.testing.expectEqual(@as(usize, 0), unsupported.quant_kernel_fallback_unsupported_shape);
    try std.testing.expectEqual(@as(usize, 0), unsupported.quant_kernel_fallback_unsupported_epilogue);
    try std.testing.expectEqual(@as(usize, 0), unsupported.quant_kernel_fallback_unsupported_backend);
    try std.testing.expectEqual(@as(usize, 1), unsupported.quant_kernel_fallback_unsupported);

    const unsupported_backend = countersForLowering(loweringFor(.cuda, .q5_k, .rows_2_8, .none));
    try std.testing.expectEqual(@as(usize, 0), unsupported_backend.quant_kernel_fallback_unsupported_format);
    try std.testing.expectEqual(@as(usize, 1), unsupported_backend.quant_kernel_fallback_unsupported_backend);
    try std.testing.expectEqual(@as(usize, 1), unsupported_backend.quant_kernel_fallback_unsupported);

    var promoted = loweringFor(.cuda, .q4_k, .rows_2_8, .bias_gelu);
    promoted.production_route = .generated_production;
    promoted.candidate_route = .unsupported;
    promoted.fallback_reason = .none;
    const promoted_counters = countersForLowering(promoted);
    try std.testing.expectEqual(@as(usize, 0), promoted_counters.quant_kernel_handwritten_production);
    try std.testing.expectEqual(@as(usize, 1), promoted_counters.quant_kernel_generated_production);
    try std.testing.expectEqual(@as(usize, 0), promoted_counters.quant_kernel_generated_candidates);
}

test "quant kernel compiler adds every plan counter to runtime stats" {
    const Stats64 = struct {
        quant_kernel_planned_ops: u64 = 0,
        quant_kernel_handwritten_production: u64 = 0,
        quant_kernel_generated_production: u64 = 0,
        quant_kernel_unsupported_routes: u64 = 0,
        quant_kernel_generated_candidates: u64 = 0,
        quant_kernel_fallback_generated_artifact_missing: u64 = 0,
        quant_kernel_fallback_generated_runtime_not_wired: u64 = 0,
        quant_kernel_fallback_unsupported_format: u64 = 0,
        quant_kernel_fallback_unsupported_shape: u64 = 0,
        quant_kernel_fallback_unsupported_epilogue: u64 = 0,
        quant_kernel_fallback_unsupported_backend: u64 = 0,
        quant_kernel_fallback_tensor_core_repack_required: u64 = 0,
        quant_kernel_fallback_unsupported: u64 = 0,
    };
    const StatsUsize = struct {
        quant_kernel_planned_ops: usize = 0,
        quant_kernel_handwritten_production: usize = 0,
        quant_kernel_generated_production: usize = 0,
        quant_kernel_unsupported_routes: usize = 0,
        quant_kernel_generated_candidates: usize = 0,
        quant_kernel_fallback_generated_artifact_missing: usize = 0,
        quant_kernel_fallback_generated_runtime_not_wired: usize = 0,
        quant_kernel_fallback_unsupported_format: usize = 0,
        quant_kernel_fallback_unsupported_shape: usize = 0,
        quant_kernel_fallback_unsupported_epilogue: usize = 0,
        quant_kernel_fallback_unsupported_backend: usize = 0,
        quant_kernel_fallback_tensor_core_repack_required: usize = 0,
        quant_kernel_fallback_unsupported: usize = 0,
    };

    var counters = PlanCounters{};
    inline for (@typeInfo(PlanCounters).@"struct".fields, 1..) |field, value| {
        @field(counters, field.name) = value;
    }

    var stats64 = Stats64{};
    addCountersToStats(&stats64, counters);
    var stats_usize = StatsUsize{};
    addCountersToStats(&stats_usize, counters);

    inline for (@typeInfo(PlanCounters).@"struct".fields) |field| {
        try std.testing.expectEqual(@as(u64, @intCast(@field(counters, field.name))), @field(stats64, field.name));
        try std.testing.expectEqual(@field(counters, field.name), @field(stats_usize, field.name));
    }
}

test "quant kernel compiler fallback counters cover every reason" {
    const base = loweringFor(.cuda, .q4_k, .rows_2_8, .bias_gelu);
    inline for (std.meta.tags(FallbackReason)) |reason| {
        var lowering = base;
        lowering.production_route = .unsupported;
        lowering.candidate_route = .unsupported;
        lowering.fallback_reason = reason;
        const counters = countersForLowering(lowering);

        try std.testing.expectEqual(@as(usize, 1), counters.quant_kernel_planned_ops);
        try std.testing.expectEqual(@as(usize, 1), counters.quant_kernel_unsupported_routes);
        try std.testing.expectEqual(@as(usize, if (reason == .generated_artifact_missing) 1 else 0), counters.quant_kernel_fallback_generated_artifact_missing);
        try std.testing.expectEqual(@as(usize, if (reason == .generated_runtime_not_wired) 1 else 0), counters.quant_kernel_fallback_generated_runtime_not_wired);
        try std.testing.expectEqual(@as(usize, if (reason == .unsupported_format) 1 else 0), counters.quant_kernel_fallback_unsupported_format);
        try std.testing.expectEqual(@as(usize, if (reason == .unsupported_shape) 1 else 0), counters.quant_kernel_fallback_unsupported_shape);
        try std.testing.expectEqual(@as(usize, if (reason == .unsupported_epilogue) 1 else 0), counters.quant_kernel_fallback_unsupported_epilogue);
        try std.testing.expectEqual(@as(usize, if (reason == .unsupported_backend) 1 else 0), counters.quant_kernel_fallback_unsupported_backend);
        try std.testing.expectEqual(@as(usize, if (reason == .tensor_core_repack_required) 1 else 0), counters.quant_kernel_fallback_tensor_core_repack_required);
        try std.testing.expectEqual(@as(usize, switch (reason) {
            .unsupported_format, .unsupported_shape, .unsupported_epilogue, .unsupported_backend => 1,
            .none, .generated_artifact_missing, .generated_runtime_not_wired, .tensor_core_repack_required => 0,
        }), counters.quant_kernel_fallback_unsupported);
    }
}

test "quant kernel compiler reference checker covers Q4_K small-batch bias gelu" {
    const allocator = std.testing.allocator;
    const rows = 8;
    const in_dim = 512;
    const out_dim = 3;

    var dense_weight: [in_dim * out_dim]f32 = undefined;
    for (&dense_weight, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7) % 23)) - 11)) / 16.0;
    }
    const raw_weight = try quant_codec.quantizeQ4_KFromF32(allocator, &dense_weight);
    defer allocator.free(raw_weight);

    var input: [rows * in_dim]f32 = undefined;
    for (&input, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 11)) - 5)) / 32.0;
    }
    const bias = [_]f32{ 0.125, -0.25, 0.375 };
    var generated_actual: [rows * out_dim]f32 = undefined;
    try generatedMathMatmulBiasGelu(.q4_k, raw_weight, &input, &bias, rows, in_dim, out_dim, &generated_actual);

    var reference_actual: [rows * out_dim]f32 = undefined;
    try referenceMatmulBiasGelu(allocator, .q4_k, raw_weight, &input, &bias, rows, in_dim, out_dim, &reference_actual);

    var dequantized: [in_dim * out_dim]f32 = undefined;
    try quant_codec.dequantizeToFloat32(.{ .known = .Q4_K }, raw_weight, &dequantized);
    for (0..rows) |r| {
        for (0..out_dim) |o| {
            var expected = bias[o];
            for (0..in_dim) |i| expected += input[r * in_dim + i] * dequantized[o * in_dim + i];
            expected = gelu(expected);
            try std.testing.expectApproxEqAbs(expected, reference_actual[r * out_dim + o], 0.00001);
            try std.testing.expectApproxEqAbs(expected, generated_actual[r * out_dim + o], 0.00001);
        }
    }
}

test "quant kernel compiler CPU reference covers descriptor formats no-bias" {
    const allocator = std.testing.allocator;
    const rows = 3;
    const in_dim = 512;
    const out_dim = 2;

    var dense_weight: [in_dim * out_dim]f32 = undefined;
    for (&dense_weight, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 5 + 3) % 29)) - 14)) / 24.0;
    }

    var input: [rows * in_dim]f32 = undefined;
    for (&input, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 3) % 17)) - 8)) / 40.0;
    }

    for (descriptor_formats) |format| {
        const raw_weight = try quantizeForFormat(allocator, format, &dense_weight);
        defer allocator.free(raw_weight);

        var actual: [rows * out_dim]f32 = undefined;
        try referenceMatmulNoBias(allocator, format, raw_weight, &input, rows, in_dim, out_dim, &actual);

        var dequantized: [in_dim * out_dim]f32 = undefined;
        try quant_codec.dequantizeToFloat32(tensorTypeForFormat(format).?, raw_weight, &dequantized);
        for (0..rows) |r| {
            for (0..out_dim) |o| {
                var expected: f32 = 0.0;
                for (0..in_dim) |i| expected += input[r * in_dim + i] * dequantized[o * in_dim + i];
                try std.testing.expectApproxEqAbs(expected, actual[r * out_dim + o], toleranceFor(format, .none));
            }
        }
    }
}

test "quant kernel compiler CPU reference covers single-output epilogues" {
    const allocator = std.testing.allocator;
    const rows = 2;
    const in_dim = 512;
    const out_dim = 2;
    const formats = [_]quant_matmul.Format{ .q4_k, .q8_0 };
    const reference_epilogues = [_]Epilogue{ .none, .bias, .bias_gelu, .relu, .gelu };

    var dense_weight: [in_dim * out_dim]f32 = undefined;
    for (&dense_weight, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 11 + 1) % 31)) - 15)) / 20.0;
    }

    var input: [rows * in_dim]f32 = undefined;
    for (&input, 0..) |*value, i| {
        value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7) % 19)) - 9)) / 48.0;
    }
    const bias = [_]f32{ 0.2, -0.3 };

    for (formats) |format| {
        const raw_weight = try quantizeForFormat(allocator, format, &dense_weight);
        defer allocator.free(raw_weight);

        var dequantized: [in_dim * out_dim]f32 = undefined;
        try quant_codec.dequantizeToFloat32(tensorTypeForFormat(format).?, raw_weight, &dequantized);

        for (reference_epilogues) |epilogue| {
            var actual: [rows * out_dim]f32 = undefined;
            try referenceMatmulEpilogue(allocator, format, raw_weight, &input, &bias, rows, in_dim, out_dim, epilogue, &actual);

            for (0..rows) |r| {
                for (0..out_dim) |o| {
                    var acc: f32 = 0.0;
                    for (0..in_dim) |i| acc += input[r * in_dim + i] * dequantized[o * in_dim + i];
                    const expected = switch (epilogue) {
                        .none => acc,
                        .bias => acc + bias[o],
                        .bias_gelu => gelu(acc + bias[o]),
                        .relu => @max(acc, 0.0),
                        .gelu => gelu(acc),
                        else => unreachable,
                    };
                    try std.testing.expectApproxEqAbs(expected, actual[r * out_dim + o], toleranceFor(format, epilogue));
                }
            }
        }
    }

    var scratch: [rows * out_dim]f32 = undefined;
    try std.testing.expectError(error.UnsupportedEpilogue, referenceMatmulEpilogue(allocator, .q4_k, &.{}, &input, null, rows, in_dim, out_dim, .pair, &scratch));
}

test "quant kernel compiler metal_production_schedules reproduces the launch-shape switch" {
    // Authoritative dispatch threads/cols from the hand-written
    // termite_metal_generated_quant_launch_shape_for switch in metal_kernels.m
    // (rows 2..8). This locks the table against that behavior before the switch
    // is regenerated from it. cols=2 for q8_0/bias_gelu is intentional (matches
    // the kernel body and dispatch; the old Zig cols helper under-counted it).
    const Expected = struct { format: quant_matmul.Format, epilogue: Epilogue, threads: u16, cols: u8 };
    const expected = [_]Expected{
        .{ .format = .q4_0, .epilogue = .none, .threads = 32, .cols = 1 },
        .{ .format = .q4_1, .epilogue = .none, .threads = 32, .cols = 2 },
        .{ .format = .q5_0, .epilogue = .none, .threads = 32, .cols = 1 },
        .{ .format = .q5_1, .epilogue = .none, .threads = 32, .cols = 2 },
        .{ .format = .q8_0, .epilogue = .none, .threads = 32, .cols = 2 },
        .{ .format = .q8_0, .epilogue = .bias, .threads = 32, .cols = 1 },
        .{ .format = .q8_0, .epilogue = .bias_gelu, .threads = 32, .cols = 2 },
        .{ .format = .q8_0, .epilogue = .relu, .threads = 32, .cols = 1 },
        .{ .format = .q8_1, .epilogue = .none, .threads = 32, .cols = 1 },
        .{ .format = .q8_k, .epilogue = .none, .threads = 64, .cols = 1 },
        .{ .format = .q2_k, .epilogue = .none, .threads = 128, .cols = 1 },
        .{ .format = .q2_k, .epilogue = .bias, .threads = 32, .cols = 1 },
        .{ .format = .q2_k, .epilogue = .bias_gelu, .threads = 32, .cols = 1 },
        .{ .format = .q3_k, .epilogue = .none, .threads = 32, .cols = 1 },
        .{ .format = .q3_k, .epilogue = .bias, .threads = 32, .cols = 1 },
        .{ .format = .q3_k, .epilogue = .bias_gelu, .threads = 32, .cols = 1 },
        .{ .format = .q4_k, .epilogue = .none, .threads = 128, .cols = 16 },
        .{ .format = .q4_k, .epilogue = .bias, .threads = 128, .cols = 16 },
        .{ .format = .q4_k, .epilogue = .bias_gelu, .threads = 128, .cols = 16 },
        .{ .format = .q5_k, .epilogue = .none, .threads = 256, .cols = 1 },
        .{ .format = .q5_k, .epilogue = .bias, .threads = 128, .cols = 1 },
        .{ .format = .q5_k, .epilogue = .bias_gelu, .threads = 128, .cols = 1 },
        .{ .format = .q6_k, .epilogue = .none, .threads = 128, .cols = 16 },
        .{ .format = .q6_k, .epilogue = .bias, .threads = 128, .cols = 16 },
        .{ .format = .q6_k, .epilogue = .bias_gelu, .threads = 128, .cols = 16 },
    };
    try std.testing.expectEqual(expected.len, metal_production_schedules.len);
    for (expected) |want| {
        const schedule = metalRouteScheduleFor(want.format, .rows_2_8, want.epilogue) orelse {
            std.debug.print("missing schedule for {s}/{s}\n", .{ @tagName(want.format), @tagName(want.epilogue) });
            return error.MissingSchedule;
        };
        try std.testing.expectEqual(want.threads, schedule.threads_per_threadgroup);
        try std.testing.expectEqual(want.cols, schedule.cols_per_threadgroup);
        try schedule.validate(want.format.valuesPerBlock().?);
    }
    // Every scheduled route must be a valid, in-table entry.
    for (metal_production_schedules) |entry| {
        try std.testing.expectEqual(quant_matmul.RowBucket.rows_2_8, entry.row_bucket);
        try entry.schedule.validate(entry.format.valuesPerBlock().?);
    }
}

test "quant kernel compiler Metal schedule candidates are deterministic unique and include the baseline" {
    var first: [metal_schedule_candidate_capacity]KernelSchedule = undefined;
    var repeated: [metal_schedule_candidate_capacity]KernelSchedule = undefined;
    for (metal_production_schedules) |entry| {
        const count = metalScheduleCandidates(entry.format, entry.epilogue, &first);
        const repeated_count = metalScheduleCandidates(entry.format, entry.epilogue, &repeated);
        try std.testing.expect(count > 0 and count <= kernel_jit.maximum_candidates);
        try std.testing.expectEqual(count, repeated_count);
        try std.testing.expectEqualDeep(first[0..count], repeated[0..repeated_count]);

        var includes_baseline = false;
        for (first[0..count], 0..) |candidate, index| {
            try candidate.validate(entry.format.valuesPerBlock().?);
            includes_baseline = includes_baseline or std.meta.eql(candidate, entry.schedule);
            for (first[0..index]) |prior| try std.testing.expect(!std.meta.eql(candidate, prior));
        }
        try std.testing.expect(includes_baseline);
    }

    const expected_q4_k = [_]KernelSchedule{
        .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 2, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 4, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 64, .cols_per_threadgroup = 4, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 64, .cols_per_threadgroup = 8, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 8, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 32, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
    };
    const q4_k_count = metalScheduleCandidates(.q4_k, .none, &first);
    try std.testing.expectEqual(expected_q4_k.len, q4_k_count);
    try std.testing.expectEqualDeep(expected_q4_k[0..], first[0..q4_k_count]);

    const q4_k_bias_count = metalScheduleCandidates(.q4_k, .bias, &first);
    try std.testing.expectEqual(expected_q4_k.len, q4_k_bias_count);
    try std.testing.expectEqualDeep(expected_q4_k[0..], first[0..q4_k_bias_count]);

    const q4_k_bias_gelu_count = metalScheduleCandidates(.q4_k, .bias_gelu, &first);
    try std.testing.expectEqual(expected_q4_k.len, q4_k_bias_gelu_count);
    try std.testing.expectEqualDeep(expected_q4_k[0..], first[0..q4_k_bias_gelu_count]);

    const q6_k_count = metalScheduleCandidates(.q6_k, .none, &first);
    try std.testing.expectEqual(expected_q4_k.len, q6_k_count);
    try std.testing.expectEqualDeep(expected_q4_k[0..], first[0..q6_k_count]);

    const q4_0_count = metalScheduleCandidates(.q4_0, .none, &first);
    try std.testing.expectEqual(@as(usize, 2), q4_0_count);
    const expected_q4_0 = [_]KernelSchedule{
        .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum },
        .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 2, .reduction = .simd_sum },
    };
    try std.testing.expectEqualDeep(expected_q4_0[0..], first[0..q4_0_count]);
}

test "quant kernel compiler exact two-row Q4_0 catalog includes the bounded small-output tile" {
    var schedules: [metal_schedule_candidate_capacity]KernelSchedule = undefined;
    const expected = [_]KernelSchedule{
        .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 4, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 8, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 64, .cols_per_threadgroup = 8, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 64, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 16, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 128, .cols_per_threadgroup = 32, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 32, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
        .{ .threads_per_threadgroup = 256, .cols_per_threadgroup = 64, .rows_per_threadgroup = 2, .reduction = .simdgroup_tiled },
    };
    const covered_shapes = [_]struct { in_dim: u64, out_dim: u64 }{
        .{ .in_dim = 10240, .out_dim = 2560 },
        .{ .in_dim = 2048, .out_dim = 2560 },
        .{ .in_dim = 2560, .out_dim = 2048 },
        .{ .in_dim = 2560, .out_dim = 512 },
        .{ .in_dim = 544, .out_dim = 255 },
        .{ .in_dim = 32, .out_dim = 1 },
    };
    for (covered_shapes) |shape| {
        const count = metalScheduleCandidatesForExactShape(.q4_0, .none, 2, shape.in_dim, shape.out_dim, &schedules);
        try std.testing.expectEqual(expected.len, count);
        var expected_shape = expected;
        if (shape.out_dim <= 512) expected_shape[1].cols_per_threadgroup = 2;
        try std.testing.expectEqualDeep(expected_shape[0..], schedules[0..count]);
        for (schedules[0..count]) |schedule| try schedule.validate(32);
    }

    // The NR2 replacement is deliberately limited to small outputs. Keep the
    // boundary explicit so catalog changes cannot silently broaden its scope.
    var catalog_512: [metal_schedule_candidate_capacity]KernelSchedule = undefined;
    var catalog_513: [metal_schedule_candidate_capacity]KernelSchedule = undefined;
    const count_512 = metalScheduleCandidatesForExactShape(.q4_0, .none, 2, 2560, 512, &catalog_512);
    const count_513 = metalScheduleCandidatesForExactShape(.q4_0, .none, 2, 2560, 513, &catalog_513);
    try std.testing.expectEqual(@as(usize, 8), count_512);
    try std.testing.expectEqual(@as(usize, 8), count_513);
    try std.testing.expectEqual(@as(u8, 2), catalog_512[1].cols_per_threadgroup);
    try std.testing.expectEqual(@as(u8, 8), catalog_513[1].cols_per_threadgroup);
    for (catalog_512[0..count_512], catalog_513[0..count_513], 0..) |small, wide, index| {
        try std.testing.expectEqual(index == 1, !std.meta.eql(small, wide));
    }

    const invalid_nr1 = KernelSchedule{
        .threads_per_threadgroup = 32,
        .cols_per_threadgroup = 1,
        .rows_per_threadgroup = 2,
        .reduction = .simdgroup_tiled,
    };
    try std.testing.expectError(error.InvalidColCount, invalid_nr1.validate(32));

    var generic: [metal_schedule_candidate_capacity]KernelSchedule = undefined;
    const expected_generic = [_]KernelSchedule{
        .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 1, .reduction = .simd_sum },
        .{ .threads_per_threadgroup = 32, .cols_per_threadgroup = 2, .reduction = .simd_sum },
    };
    const generic_cases = [_]struct {
        epilogue: Epilogue,
        rows: u64,
        in_dim: u64,
        out_dim: u64,
    }{
        .{ .epilogue = .none, .rows = 3, .in_dim = 10240, .out_dim = 2560 },
        .{ .epilogue = .bias, .rows = 2, .in_dim = 10240, .out_dim = 2560 },
        .{ .epilogue = .none, .rows = 2, .in_dim = 0, .out_dim = 2560 },
        .{ .epilogue = .none, .rows = 2, .in_dim = 10241, .out_dim = 2560 },
        .{ .epilogue = .none, .rows = 2, .in_dim = 10240, .out_dim = 0 },
    };
    for (generic_cases) |case| {
        const count = metalScheduleCandidatesForExactShape(.q4_0, case.epilogue, case.rows, case.in_dim, case.out_dim, &generic);
        try std.testing.expectEqual(expected_generic.len, count);
        try std.testing.expectEqualDeep(expected_generic[0..], generic[0..count]);
    }
}

test "quant kernel compiler exact Q4_K and Q6_K encoder catalogs tile large rows" {
    var schedules: [metal_schedule_candidate_capacity]KernelSchedule = undefined;
    for ([_]struct {
        format: quant_matmul.Format,
        epilogue: Epilogue,
    }{
        .{ .format = .q4_k, .epilogue = .none },
        .{ .format = .q4_k, .epilogue = .bias },
        .{ .format = .q4_k, .epilogue = .bias_gelu },
        .{ .format = .q6_k, .epilogue = .none },
        .{ .format = .q6_k, .epilogue = .bias },
        .{ .format = .q6_k, .epilogue = .bias_gelu },
    }) |route| {
        const mid_count = metalScheduleCandidatesForExactShape(route.format, route.epilogue, 16, 1024, 1024, &schedules);
        try std.testing.expectEqual(@as(usize, 8), mid_count);
        for (schedules[0..mid_count]) |schedule| {
            try std.testing.expect(schedule.rows_per_threadgroup == 2 or schedule.rows_per_threadgroup == 4);
            try schedule.validate(256);
        }

        const wide_count = metalScheduleCandidatesForExactShape(route.format, route.epilogue, 48, 1024, 1024, &schedules);
        try std.testing.expectEqual(@as(usize, 8), wide_count);
        for (schedules[0..wide_count]) |schedule| {
            if (route.epilogue == .none and schedule.reduction == .simdgroup_matrix) {
                try std.testing.expectEqual(@as(u8, 40), schedule.rows_per_threadgroup);
            } else {
                try std.testing.expect(schedule.rows_per_threadgroup == 4 or schedule.rows_per_threadgroup == 8);
            }
            try schedule.validate(256);
        }

        const bge_count = metalScheduleCandidatesForExactShape(route.format, route.epilogue, 40, 1024, 1024, &schedules);
        try std.testing.expectEqual(@as(usize, 8), bge_count);
        if (route.epilogue == .none) {
            try std.testing.expectEqual(ReductionKind.simdgroup_matrix, schedules[7].reduction);
            try std.testing.expectEqual(@as(u16, 160), schedules[7].threads_per_threadgroup);
            try std.testing.expectEqual(@as(u8, 40), schedules[7].rows_per_threadgroup);
        }

        const large_count = metalScheduleCandidatesForExactShape(route.format, route.epilogue, 256, 1024, 4096, &schedules);
        try std.testing.expectEqual(@as(usize, 8), large_count);
        if (route.epilogue == .none) {
            try std.testing.expectEqual(ReductionKind.simdgroup_matrix, schedules[5].reduction);
            try std.testing.expectEqual(@as(u16, 160), schedules[5].threads_per_threadgroup);
            try std.testing.expectEqual(@as(u8, 40), schedules[5].rows_per_threadgroup);
            try std.testing.expectEqual(ReductionKind.simdgroup_matrix, schedules[7].reduction);
            try std.testing.expectEqual(@as(u16, 256), schedules[7].threads_per_threadgroup);
            try std.testing.expectEqual(@as(u8, 64), schedules[7].rows_per_threadgroup);
        } else {
            for (schedules[0..large_count]) |schedule| {
                try std.testing.expect(schedule.rows_per_threadgroup == 4 or schedule.rows_per_threadgroup == 8);
            }
        }
    }

    const small_count = metalScheduleCandidatesForExactShape(.q4_k, .none, 8, 1024, 1024, &schedules);
    try std.testing.expectEqual(@as(usize, 8), small_count);
    for (schedules[0..small_count]) |schedule| try std.testing.expectEqual(@as(u8, 2), schedule.rows_per_threadgroup);
}

test "quant kernel compiler renders distinct self-contained Metal schedule candidates" {
    const artifact = comptime blk: {
        for (first_generated_artifacts) |candidate| {
            const op = candidate.matmulOp() orelse continue;
            if (candidate.backend == .metal and op.format == .q4_k and op.epilogue == .none) {
                break :blk candidate;
            }
        }
        @compileError("missing Q4_K Metal artifact");
    };
    var schedules: [metal_schedule_candidate_capacity]KernelSchedule = undefined;
    const count = metalScheduleCandidates(.q4_k, .none, &schedules);
    try std.testing.expectEqual(@as(usize, 8), count);

    const first = try emitMetalScheduleCandidateSource(std.testing.allocator, artifact, schedules[0]);
    defer first.deinit(std.testing.allocator);
    const last = try emitMetalScheduleCandidateSource(std.testing.allocator, artifact, schedules[count - 1]);
    defer last.deinit(std.testing.allocator);
    try std.testing.expect(!std.mem.eql(u8, first.data, last.data));
    try std.testing.expect(std.mem.containsAtLeast(u8, first.data, 1, "production_baseline=metal_handwritten_quant_matmul"));
    try std.testing.expect(std.mem.containsAtLeast(u8, first.data, 1, "#include <metal_stdlib>"));
    try std.testing.expect(std.mem.containsAtLeast(u8, first.data, 1, artifact.kernel_id));
}

test "quant kernel compiler rendered bodies carry their schedule cols marker" {
    // cols==2 kernels compute two output columns per threadgroup via
    // `group_pos.x << 1`; cols==1 kernels do not. Renders each route through the
    // descriptor-driven renderer (the single source for these bodies) and locks
    // the table's cols column + reduction to the emitted body text.
    for (metal_production_schedules) |entry| {
        const decoder = metal_renderer.decoderFor(entry.format) orelse return error.MissingDecoder;
        const kernel_id = try metalRuntimeKernelId(std.testing.allocator, entry.format, entry.epilogue);
        defer std.testing.allocator.free(kernel_id);
        const body = try metal_renderer.renderKernel(std.testing.allocator, kernel_id, decoder, entry.schedule, entry.epilogue);
        defer std.testing.allocator.free(body);
        const has_two_col = std.mem.containsAtLeast(u8, body, 1, "group_pos.x << 1");
        try std.testing.expectEqual(entry.schedule.cols_per_threadgroup == 2, has_two_col);
        // Reduction primitive must be present in the body.
        switch (entry.schedule.reduction) {
            .simd_sum => try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "simd_sum")),
            .threadgroup_tree => try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "partial[")),
            .hybrid_simd => {
                try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "simd_sum"));
                try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "simdgroup_id"));
            },
            .simdgroup_tiled => {
                try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "const uint NSG"));
                try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "simd_sum"));
            },
            .simdgroup_matrix => try std.testing.expect(std.mem.containsAtLeast(u8, body, 1, "simdgroup_multiply_accumulate")),
        }
    }
}
