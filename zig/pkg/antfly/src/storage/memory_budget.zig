// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const process_memory = @import("antfly_platform").process_memory;
const lsm_backend = @import("lsm_backend/mod.zig");
const resource_manager_mod = @import("resource_manager.zig");

pub const MiB: u64 = 1024 * 1024;

pub const GiB: u64 = 1024 * MiB;

pub const MinSmartLsmCacheBytes: u64 = 64 * 1024 * 1024;

pub const MaxSmartLsmCacheBytes: u64 = 8 * GiB;

pub const MinSmartLsmCompactionBytes: u64 = 128 * 1024 * 1024;

pub const MaxSmartLsmCompactionBytes: u64 = 1024 * 1024 * 1024;

pub const MinSmartLsmTableBuilderBytes: u64 = 64 * 1024 * 1024;

pub const MaxSmartLsmTableBuilderBytes: u64 = 512 * 1024 * 1024;

pub const MinSmartLsmInMemoryStateBytes: u64 = 256 * 1024 * 1024;

pub const MaxSmartLsmInMemoryStateBytes: u64 = 768 * 1024 * 1024;

pub const MinSmartHbcCacheBytes: u64 = 128 * 1024 * 1024;

pub const MaxSmartHbcCacheBytes: u64 = 16 * GiB;

pub const MinSmartDenseApplyBytes: u64 = 64 * 1024 * 1024;

pub const MaxSmartDenseApplyBytes: u64 = 512 * 1024 * 1024;

pub const MinSmartReplayWindowBytes: u64 = 64 * 1024 * 1024;

pub const MaxSmartReplayWindowBytes: u64 = 256 * 1024 * 1024;

pub const MinSmartFullTextPendingBytes: u64 = 64 * 1024 * 1024;

pub const MaxSmartFullTextPendingBytes: u64 = 512 * 1024 * 1024;

pub const MinSmartFullTextBuildBytes: u64 = 128 * 1024 * 1024;

pub const MaxSmartFullTextBuildBytes: u64 = 1024 * 1024 * 1024;

pub const MinSmartFullTextResidencyBytes: u64 = 256 * 1024 * 1024;

pub const MaxSmartFullTextResidencyBytes: u64 = 2 * 1024 * 1024 * 1024;

pub const MinSmartDerivedBacklogBytes: u64 = 64 * 1024 * 1024;

pub const MaxSmartDerivedBacklogBytes: u64 = 512 * 1024 * 1024;

pub const MinSmartTextMergeBytes: u64 = 32 * 1024 * 1024;

pub const MaxSmartTextMergeBytes: u64 = 256 * 1024 * 1024;

pub const MinSmartAlgebraicTensorBytes: u64 = 32 * 1024 * 1024;

pub const MaxSmartAlgebraicTensorBytes: u64 = 256 * 1024 * 1024;

pub const MinSmartDenseRepairBytes: u64 = 64 * 1024 * 1024;

pub const MaxSmartDenseRepairBytes: u64 = 512 * 1024 * 1024;

pub const MinSmartShardTransitionBytes: u64 = 64 * 1024 * 1024;

pub const MaxSmartShardTransitionBytes: u64 = 512 * 1024 * 1024;

pub const MinSmartVectorBlockBuildBytes: u64 = 64 * 1024 * 1024;

pub const MaxSmartVectorBlockBuildBytes: u64 = 256 * 1024 * 1024;

pub const MemoryLimitSource = enum {
    explicit,
    cgroup_v2,
    cgroup_v1,
    host,
    unavailable,
};

pub const DetectedMemoryLimit = struct {
    bytes: u64,
    source: MemoryLimitSource,
};

pub fn resolveEffectiveMemoryLimit(
    explicit_bytes: ?u64,
    detected: ?DetectedMemoryLimit,
) ?DetectedMemoryLimit {
    if (detected) |detected_limit| {
        if (explicit_bytes) |explicit_limit| {
            if (explicit_limit <= detected_limit.bytes) {
                return .{ .bytes = explicit_limit, .source = .explicit };
            }
        }
        return detected_limit;
    }
    if (explicit_bytes) |explicit_limit| {
        return .{ .bytes = explicit_limit, .source = .explicit };
    }
    return null;
}

pub fn detectedMemoryLimit() ?DetectedMemoryLimit {
    const envelope = process_memory.systemEnvelope();
    if (envelope.limit_bytes == 0) return null;
    return .{
        .bytes = envelope.limit_bytes,
        .source = switch (envelope.source) {
            .cgroup_v2 => .cgroup_v2,
            .cgroup_v1 => .cgroup_v1,
            .host => .host,
            .unavailable => .unavailable,
        },
    };
}

pub fn adaptiveSliceHardLimit(total: u64, divisor: u64, min_bytes: u64, max_bytes: u64) u64 {
    const target = if (divisor == 0) total else total / divisor;
    if (total < min_bytes * 4) {
        return @min(@max(8 * MiB, target), max_bytes);
    }
    return std.math.clamp(target, min_bytes, max_bytes);
}

pub fn clampU64ToUsize(value: u64) usize {
    const usize_max: u64 = std.math.maxInt(usize);
    return @intCast(@min(value, usize_max));
}

pub fn resourceBudget(soft_numerator: u64, hard_limit_bytes: u64) resource_manager_mod.Budget {
    return .{
        .soft_limit_bytes = hard_limit_bytes * soft_numerator / 4,
        .hard_limit_bytes = hard_limit_bytes,
    };
}

pub fn elasticCacheBudget(hard_limit_bytes: u64) resource_manager_mod.Budget {
    return .{
        .soft_limit_bytes = hard_limit_bytes * 7 / 8,
        .hard_limit_bytes = hard_limit_bytes,
    };
}

pub const SmartResourceBudgets = struct {
    options: resource_manager_mod.Options,
    lsm_cache_budget_bytes: usize,
    effective_memory_limit_bytes: u64 = 0,
    memory_limit_source: MemoryLimitSource = .unavailable,
};

pub fn smartResourceBudgets(process_memory_limit_bytes: usize) SmartResourceBudgets {
    var options = resource_manager_mod.Options{};
    const explicit: ?u64 = if (process_memory_limit_bytes == 0) null else @intCast(process_memory_limit_bytes);
    const effective = resolveEffectiveMemoryLimit(explicit, detectedMemoryLimit()) orelse {
        const lsm_cache_budget = lsm_backend.DefaultCacheSizeBytes;
        options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)] = resourceBudget(3, @intCast(lsm_cache_budget));
        return .{
            .options = options,
            .lsm_cache_budget_bytes = lsm_cache_budget,
        };
    };

    var budgets = smartResourceBudgetsForTotal(effective.bytes);
    budgets.effective_memory_limit_bytes = effective.bytes;
    budgets.memory_limit_source = effective.source;
    return budgets;
}

pub fn smartResourceBudgetsResolved(
    process_memory_limit_bytes: usize,
    source: MemoryLimitSource,
) SmartResourceBudgets {
    if (process_memory_limit_bytes == 0) {
        const lsm_cache_budget = lsm_backend.DefaultCacheSizeBytes;
        var options = resource_manager_mod.Options{};
        options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)] = resourceBudget(3, @intCast(lsm_cache_budget));
        return .{
            .options = options,
            .lsm_cache_budget_bytes = lsm_cache_budget,
            .memory_limit_source = source,
        };
    }
    var budgets = smartResourceBudgetsForTotal(@intCast(process_memory_limit_bytes));
    budgets.effective_memory_limit_bytes = @intCast(process_memory_limit_bytes);
    budgets.memory_limit_source = source;
    return budgets;
}

pub fn safeManagedHostMemory(total: u64) u64 {
    const preferred_headroom = std.math.clamp(@max(total / 4, 6 * GiB), 4 * GiB, 24 * GiB);
    const headroom = @min(preferred_headroom, total / 2);
    return total - headroom;
}

pub fn smartResourceBudgetsForTotal(total: u64) SmartResourceBudgets {
    var options = resource_manager_mod.Options{};
    options.memory_budget = resourceBudget(3, safeManagedHostMemory(total));

    const lsm_hard = adaptiveSliceHardLimit(total, 4, MinSmartLsmCacheBytes, MaxSmartLsmCacheBytes);
    const lsm_compaction_hard = adaptiveSliceHardLimit(total, 16, MinSmartLsmCompactionBytes, MaxSmartLsmCompactionBytes);
    const lsm_table_builder_hard = adaptiveSliceHardLimit(total, 32, MinSmartLsmTableBuilderBytes, MaxSmartLsmTableBuilderBytes);
    const lsm_in_memory_state_hard = adaptiveSliceHardLimit(total, 8, MinSmartLsmInMemoryStateBytes, MaxSmartLsmInMemoryStateBytes);
    const lsm_wal_write_hard = adaptiveSliceHardLimit(total, 16, MinSmartLsmInMemoryStateBytes, MaxSmartLsmInMemoryStateBytes);
    const hbc_hard = adaptiveSliceHardLimit(total, 3, MinSmartHbcCacheBytes, MaxSmartHbcCacheBytes);
    const dense_search_hard = adaptiveSliceHardLimit(total, 24, MinSmartDenseApplyBytes, MaxSmartDenseApplyBytes);
    const dense_apply_hard = adaptiveSliceHardLimit(total, 24, MinSmartDenseApplyBytes, MaxSmartDenseApplyBytes);
    const replay_hard = adaptiveSliceHardLimit(total, 32, MinSmartReplayWindowBytes, MaxSmartReplayWindowBytes);
    const full_text_hard = adaptiveSliceHardLimit(total, 32, MinSmartFullTextPendingBytes, MaxSmartFullTextPendingBytes);
    const full_text_build_hard = adaptiveSliceHardLimit(total, 24, MinSmartFullTextBuildBytes, MaxSmartFullTextBuildBytes);
    const full_text_residency_hard = adaptiveSliceHardLimit(total, 8, MinSmartFullTextResidencyBytes, MaxSmartFullTextResidencyBytes);
    const derived_hard = adaptiveSliceHardLimit(total, 32, MinSmartDerivedBacklogBytes, MaxSmartDerivedBacklogBytes);
    const text_merge_hard = adaptiveSliceHardLimit(total, 64, MinSmartTextMergeBytes, MaxSmartTextMergeBytes);
    const algebraic_tensor_hard = adaptiveSliceHardLimit(total, 64, MinSmartAlgebraicTensorBytes, MaxSmartAlgebraicTensorBytes);
    const dense_repair_hard = adaptiveSliceHardLimit(total, 24, MinSmartDenseRepairBytes, MaxSmartDenseRepairBytes);
    const shard_transition_hard = adaptiveSliceHardLimit(total, 24, MinSmartShardTransitionBytes, MaxSmartShardTransitionBytes);
    const vector_block_build_hard = adaptiveSliceHardLimit(total, 16, MinSmartVectorBlockBuildBytes, MaxSmartVectorBlockBuildBytes);

    options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_block_table_cache)] = elasticCacheBudget(lsm_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_compaction_work)] = resourceBudget(3, lsm_compaction_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_table_builder_working_set)] = resourceBudget(3, lsm_table_builder_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_in_memory_state)] = resourceBudget(3, lsm_in_memory_state_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.lsm_wal_write_working_set)] = resourceBudget(3, lsm_wal_write_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.hbc_node_metadata_cache)] = elasticCacheBudget(hbc_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.dense_search_working_set)] = resourceBudget(3, dense_search_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.dense_apply_working_set)] = resourceBudget(3, dense_apply_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.dense_routing_working_set)] = resourceBudget(3, dense_apply_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.derived_replay_window)] = resourceBudget(3, replay_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.full_text_pending_segments)] = resourceBudget(3, full_text_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.full_text_build_working_set)] = resourceBudget(2, full_text_build_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.full_text_segment_residency)] = resourceBudget(3, full_text_residency_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.derived_backlog)] = resourceBudget(3, derived_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.text_merge_buffers)] = resourceBudget(3, text_merge_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.algebraic_tensor_accumulators)] = resourceBudget(3, algebraic_tensor_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.dense_repair_working_set)] = resourceBudget(3, dense_repair_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.shard_transition_working_set)] = resourceBudget(3, shard_transition_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.relational_preparation_working_set)] = resourceBudget(3, shard_transition_hard);
    options.budgets[@intFromEnum(resource_manager_mod.Slice.dense_vector_block_build_working_set)] = resourceBudget(3, vector_block_build_hard);
    // Inference slices are logical host-plus-accelerator metrics. Their host
    // component is enforced by the aggregate budget above; ModelManager and
    // BackendRuntime retain device-aware backend admission.

    return .{
        .options = options,
        .lsm_cache_budget_bytes = clampU64ToUsize(lsm_hard),
    };
}
