// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Request-scoped post-ingestion orchestration. Preparation owns immutable token
//! and eligibility snapshots, constructs the actual retention plan and admits
//! its complete KV peak before ingestion. The runner sees only a private hook.
//! Numerical validity does not authorize production: callers must select their
//! profile through the release registry at the loaded-model boundary.
const std = @import("std");
const fault_injection = @import("../context_fault_injection.zig");
const live_mod = @import("packed_context_session.zig");
const branch_mod = @import("model_replay_context.zig");
const storage = @import("packed_context_storage.zig");
const fitter = @import("context_compaction.zig");
const views = @import("../runtime/kv/context_view.zig");
const contract = @import("../runtime/kv/context_session.zig");
const payload = @import("../runtime/kv/payload.zig");
const reference = @import("../runtime/kv/reference_replay.zig");
const owner_alloc = @import("../runtime/kv/host_allocation_tracker.zig");
const query_protocol = @import("context_query_protocol.zig");
const query_campaign = @import("context_query_campaign_pool.zig");
const study_templates = @import("context_study_templates.zig");
pub const profiles = @import("../runtime/kv/compaction_profile.zig");
var next_campaign_identity = std.atomic.Value(u64).init(1);
const CampaignTimer = struct {
    started: u64,

    fn now() !u64 {
        var ts: std.posix.timespec = undefined;
        if (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts)) != .SUCCESS)
            return error.CampaignClockUnavailable;
        if (ts.sec < 0 or ts.nsec < 0) return error.CampaignClockUnavailable;
        return std.math.add(u64, try std.math.mul(u64, @intCast(ts.sec), std.time.ns_per_s), @intCast(ts.nsec));
    }

    fn start() !CampaignTimer {
        return .{ .started = try now() };
    }

    fn read(self: CampaignTimer) !u64 {
        return std.math.sub(u64, try now(), self.started);
    }
};

fn rounded(n: usize) !usize {
    return (try std.math.add(usize, n, std.heap.pageSize() - 1)) & ~(std.heap.pageSize() - 1);
}

pub fn sameLayout(a: @import("../runtime/context_policy.zig").Layout, b: @import("../runtime/context_policy.zig").Layout) bool {
    if (a.len == 0 or a.len > a.layers.len or a.len != b.len) return false;
    for (a.layers[0..a.len], b.layers[0..b.len]) |x, y| if (!std.meta.eql(x, y)) return false;
    return true;
}
pub const Request = struct {
    arena: *std.heap.ArenaAllocator,
    config: live_mod.Config,
    prompt: []const i64,
    eligible: payload.Eligible,
    replay: reference.Plan,
    parts: []const []const storage.Segment,
    retained_limit: u32,
    /// Conservative backend-visible peak. Driver/runtime control allocations
    /// remain backend charged; the request arena is accounted on the host.
    backend_peak_bytes: usize,
    host_peak_bytes: usize,
    /// Combined runtime allocation ceiling excluding immutable request-arena
    /// ownership. The live context APIs validate combined physical bytes.
    execution_peak_bytes: usize,
    peak_bytes: usize,
    request_bytes: usize,
    saved_bytes: usize,
    consumed: bool = false,
    publication_attempted: bool = false,
    published: bool = false,
    committed_before_bytes: usize = 0,
    committed_after_bytes: usize = 0,
    backend_kv_readback_before: u64 = 0,
    backend_kv_readback_after: u64 = 0,

    pub fn deinit(self: *Request) void {
        const parent = self.arena.child_allocator;
        self.arena.deinit();
        parent.destroy(self.arena);
        self.* = undefined;
    }

    /// strict=false is confined to test builds and only exercises replay and
    /// continuation with exact originals. It never counts as publication proof.
    pub fn run(self: *Request, live: *live_mod.Session, ctx: *anyopaque, runner: *const fn (*anyopaque, contract.Hook, usize, []const i64) anyerror!void, strict: bool) !void {
        // Legacy single-replay publication is diagnostic-only. Production
        // must enter through CampaignRequest and the five-source protocol.
        if (!@import("builtin").is_test) return error.UnqualifiedCompactionArtifact;
        if (self.consumed or live.logical_tokens != self.prompt.len or !std.meta.eql(live.config.plan, self.config.plan) or
            !sameLayout(live.config.layout, self.config.layout) or live.config.max_query_rows != self.config.max_query_rows or
            live.config.optimize_decode != self.config.optimize_decode or live.config.optimize_prefill != self.config.optimize_prefill)
            return error.InvalidContextTransaction;
        self.consumed = true;
        self.committed_before_bytes = live.metrics.live_bytes;
        self.committed_after_bytes = live.metrics.live_bytes;
        self.backend_kv_readback_before = live.metrics.kv_readback_bytes;
        self.backend_kv_readback_after = live.metrics.kv_readback_bytes;
        defer {
            self.committed_after_bytes = live.metrics.live_bytes;
            self.backend_kv_readback_after = live.metrics.kv_readback_bytes;
        }
        const budget = self.execution_peak_bytes;
        defer live.metrics.peak_bytes = @max(live.metrics.peak_bytes, self.execution_peak_bytes);
        var maintenance = try live.beginReplay(@intCast(self.replay.tokens.len), budget);
        defer maintenance.deinit();
        var cursor: usize = 0;
        while (cursor < self.replay.tokens.len) {
            const position = self.prompt.len + cursor;
            const rows = try maintenance.branch.hook().nextChunk(@intCast(position), @intCast(self.replay.tokens.len - cursor));
            try runner(ctx, maintenance.branch.hook(), position, self.replay.tokens[cursor..][0..rows]);
            cursor += rows;
        }
        try maintenance.seal();
        if (strict) {
            self.publication_attempted = true;
            try maintenance.publish(&self.eligible, self.parts, self.retained_limit, budget);
            self.published = true;
        }
    }
};

pub const Estimate = struct {
    chunk_rows: u32,
    backend_peak_bytes: usize,
    host_peak_bytes: usize,
    execution_peak_bytes: usize,
    combined_peak_bytes: usize,
    request_bytes: usize,
    retained_limit: u32,
    saved_bytes: usize,
};

/// Immutable request inputs. Chunk-dependent storage is estimated only after
/// ordinary admission has selected a candidate chunk, avoiding a circular
/// "prepare then shrink" contract.
pub const PreparedPlan = struct {
    arena: *std.heap.ArenaAllocator,
    config: live_mod.Config,
    prompt: []const i64,
    eligible: payload.Eligible,
    replay: reference.Plan,
    parts: []const []const storage.Segment,
    retained_limit: u32,
    staged_bytes: usize,
    staged_device: usize,
    staged_host: usize,
    old_device: usize,
    workspace: usize,
    minimum_saved_bytes: usize,
    transferred: bool = false,

    pub fn deinit(self: *PreparedPlan) void {
        if (!self.transferred) {
            const parent = self.arena.child_allocator;
            self.arena.deinit();
            parent.destroy(self.arena);
        }
        self.* = undefined;
    }

    fn configForChunk(self: *const PreparedPlan, chunk_rows: usize) !live_mod.Config {
        var config = self.config;
        config.max_query_rows = std.math.cast(u32, @max(chunk_rows, 1)) orelse return error.ResourceLimitExceeded;
        return config;
    }

    pub fn estimate(self: *const PreparedPlan, chunk_rows: usize) !Estimate {
        if (self.transferred) return error.InvalidContextTransaction;
        const config = try self.configForChunk(chunk_rows);
        const original = try live_mod.allocation(config);
        const branch = branch_mod.Config{
            .plan = try (@import("../runtime/context_policy.zig").Policy{}).admit(config.plan.context_limit, null, self.prompt.len, self.replay.tokens.len, 0),
            .layout = config.layout,
            .max_query_rows = config.max_query_rows,
            .physical_budget_bytes = 0,
        };
        const replay_size = try branch_mod.estimate(branch);
        if (self.staged_bytes >= original.global_bytes or self.staged_device >= self.old_device or
            original.global_bytes - self.staged_bytes < self.configuredProfileMinimum() or
            self.old_device - self.staged_device < self.configuredProfileMinimum()) return error.ContextAllocationNotReduced;

        const original_backend = try originalBackendBytes(config, original);
        if (original_backend > original.physical_bytes) return error.InvalidContextLayout;
        const original_host = original.physical_bytes - original_backend;
        const replay_tiers = try replayTierBytes(branch, replay_size);
        const request_host = try requestOwnerPreparationBytes(self.config.owner_allocation_tracker, self.arena);

        const replay_backend_peak = try add(original_backend, replay_tiers.peak_backend);
        const replay_host_peak = try add(try add(original_host, replay_tiers.peak_host), request_host);
        var publication_backend = try add(original_backend, replay_tiers.sealed_backend);
        publication_backend = try add(publication_backend, self.staged_device);
        publication_backend = try add(publication_backend, self.workspace);
        publication_backend = try add(publication_backend, std.heap.pageSize());
        var publication_host = try add(original_host, replay_size.owner_control_bytes);
        publication_host = try add(publication_host, replay_tiers.sealed_host);
        publication_host = try add(publication_host, self.staged_host);
        publication_host = try add(publication_host, try rounded(try mul(self.parts.len, @sizeOf(storage.Generation))));
        publication_host = try add(publication_host, request_host);
        const backend = @max(replay_backend_peak, publication_backend);
        const host = @max(replay_host_peak, publication_host);
        const replay_execution = try add(original.physical_bytes, replay_size.physical_bytes);
        var publication_execution = try add(original.physical_bytes, replay_size.owner_control_bytes);
        publication_execution = try add(publication_execution, replay_size.sealed_bytes);
        publication_execution = try add(publication_execution, self.staged_bytes);
        publication_execution = try add(publication_execution, self.workspace);
        publication_execution = try add(publication_execution, std.heap.pageSize());
        publication_execution = try add(publication_execution, try rounded(try mul(self.parts.len, @sizeOf(storage.Generation))));
        return .{
            .chunk_rows = config.max_query_rows,
            .backend_peak_bytes = backend,
            .host_peak_bytes = host,
            .execution_peak_bytes = @max(replay_execution, publication_execution),
            .combined_peak_bytes = try add(backend, host),
            .request_bytes = request_host,
            .retained_limit = self.retained_limit,
            .saved_bytes = @min(original.global_bytes - self.staged_bytes, self.old_device - self.staged_device),
        };
    }

    fn configuredProfileMinimum(self: *const PreparedPlan) usize {
        // The minimum is encoded in the already validated staging delta. Keep
        // it in the arena-owned plan without retaining a caller-owned Profile.
        return self.minimum_saved_bytes;
    }

    pub fn finalize(self: *PreparedPlan, chunk_rows: usize, backend_budget: usize, host_budget: usize) !Request {
        const sizes = try self.estimate(chunk_rows);
        if (sizes.backend_peak_bytes > backend_budget or sizes.host_peak_bytes > host_budget)
            return error.ContextStagingBudgetExceeded;
        const config = try self.configForChunk(chunk_rows);
        self.transferred = true;
        return .{
            .arena = self.arena,
            .config = config,
            .prompt = self.prompt,
            .eligible = self.eligible,
            .replay = self.replay,
            .parts = self.parts,
            .retained_limit = self.retained_limit,
            .backend_peak_bytes = sizes.backend_peak_bytes,
            .host_peak_bytes = sizes.host_peak_bytes,
            .execution_peak_bytes = sizes.execution_peak_bytes,
            .peak_bytes = sizes.combined_peak_bytes,
            .request_bytes = sizes.request_bytes,
            .saved_bytes = sizes.saved_bytes,
        };
    }
};

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b);
}

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b);
}

fn requestArenaBytes(arena: *const std.heap.ArenaAllocator) !usize {
    var bytes = try add(arena.queryCapacity(), @sizeOf(Request) + @sizeOf(std.heap.ArenaAllocator));
    for ([_]@TypeOf(arena.state.used_list){ arena.state.used_list, arena.state.free_list }) |head| {
        var node = head;
        while (node) |n| : (node = n.next) bytes = try add(bytes, @sizeOf(@TypeOf(n.*)) + std.heap.pageSize() - 1);
    }
    return rounded(bytes);
}

fn requestOwnerPreparationBytes(tracker: ?*@import("../runtime/kv/host_allocation_tracker.zig").HostAllocationTracker, arena: *const std.heap.ArenaAllocator) !usize {
    const owner = tracker orelse return requestArenaBytes(arena);
    const observed = owner.snapshot();
    // A production tier reservation must use physical allocator evidence. Do
    // not silently substitute logical bytes on targets without malloc-size
    // accounting: that would make the host gate incomparable to the actual
    // request owner used by serving.
    if (!observed.tracked_combined_physical_available) return error.ContextPhysicalAccountingUnavailable;
    return std.math.cast(usize, observed.tracked_combined_peak_bytes) orelse
        error.ResourceLimitExceeded;
}

test "context admission charges observed full-owner preparation peak" {
    const owner_mod = @import("../runtime/kv/host_allocation_tracker.zig");
    var owner = try owner_mod.RequestAllocationOwner.init(null);
    defer _ = owner.release();
    const a = owner.allocator();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const arena_alloc = arena.allocator();
    _ = try arena_alloc.alloc(u8, 257);
    const outside_arena = try a.alloc(u8, 65537);
    defer a.free(outside_arena);
    const snapshot = owner.tracker.snapshot();
    if (!snapshot.tracked_combined_physical_available) {
        try std.testing.expectError(error.ContextPhysicalAccountingUnavailable, requestOwnerPreparationBytes(owner.tracker, &arena));
        return;
    }
    const expected = snapshot.tracked_combined_peak_bytes;
    try std.testing.expect(expected > try requestArenaBytes(&arena));
    try std.testing.expectEqual(@as(usize, @intCast(expected)), try requestOwnerPreparationBytes(owner.tracker, &arena));
}

fn originalBackendBytes(config: live_mod.Config, sizes: live_mod.Allocation) !usize {
    // The packed allocation reports every persistent Metal control page,
    // descriptor arena, and optional split-decode scratch explicitly.
    var backend = sizes.backend_control_bytes;
    for (config.layout.layers[0..config.layout.len], 0..) |layer, i| {
        if (layer.owner != i) continue;
        if (layer.sliding_window == 0) {
            backend = try add(backend, (try storage.Generation.emptyAllocation(layer.head_dim, layer.kv_heads, @intCast(config.plan.logical_token_limit))).device_bytes);
        } else {
            const extended = try add(layer.sliding_window, config.max_query_rows - 1);
            const rows = @min(config.plan.logical_token_limit, extended);
            const elements = try mul(try mul(rows, layer.kv_heads), layer.head_dim);
            backend = try add(backend, try mul(2, try rounded(try mul(elements, 2))));
        }
    }
    return backend;
}

const ReplayTiers = struct {
    peak_backend: usize,
    peak_host: usize,
    sealed_backend: usize,
    sealed_host: usize,
};

fn replayFixedBackendBytes(sizes: branch_mod.Allocation) !usize {
    return add(try add(3 * std.heap.pageSize(), sizes.descriptor_arena_bytes), sizes.replay_split_scratch_bytes);
}

fn replayTierBytes(config: branch_mod.Config, sizes: branch_mod.Allocation) !ReplayTiers {
    var peak_backend: usize = try replayFixedBackendBytes(sizes);
    var sealed_backend: usize = try replayFixedBackendBytes(sizes);
    const tail_rows = config.plan.logical_token_limit - config.plan.prompt_tokens;
    for (config.layout.layers[0..config.layout.len], 0..) |layer, i| {
        if (layer.owner != i) continue;
        if (layer.sliding_window == 0) {
            const tail_elements = try mul(try mul(tail_rows, layer.kv_heads), layer.head_dim);
            peak_backend = try add(peak_backend, try mul(2, try rounded(try mul(tail_elements, 2))));
            peak_backend = try add(peak_backend, try rounded(try mul(layer.kv_heads, @sizeOf(views.Head))));
            peak_backend = try add(peak_backend, 2 * std.heap.pageSize());
            const q = try branch_mod.QueryShape.init(config, i);
            const query = try rounded(try mul(try mul(q.vectors, layer.head_dim), @sizeOf(f32)));
            peak_backend = try add(peak_backend, query);
            sealed_backend = try add(sealed_backend, query);
        } else {
            const seed = @min(config.plan.prompt_tokens, layer.sliding_window);
            const retained: [views.max_heads]u32 = @splat(@intCast(seed));
            const storage_sizes = try storage.allocation(.{
                .head_dim = layer.head_dim,
                .kv_heads = layer.kv_heads,
                .continuation_rows = @intCast(tail_rows),
                .max_retained_rows = @intCast(seed + tail_rows),
            }, &.{.{ .logical_count = @intCast(seed), .retained = retained[0..layer.kv_heads] }});
            peak_backend = try add(peak_backend, storage_sizes.device_bytes);
        }
    }
    peak_backend = try add(peak_backend, sizes.query_mapping_bytes);
    sealed_backend = try add(sealed_backend, sizes.query_mapping_bytes);
    if (peak_backend > sizes.physical_bytes or sealed_backend > sizes.sealed_bytes) return error.InvalidContextLayout;
    return .{
        .peak_backend = peak_backend,
        .peak_host = sizes.physical_bytes - peak_backend,
        .sealed_backend = sealed_backend,
        .sealed_host = sizes.sealed_bytes - sealed_backend,
    };
}

test "context replay tier charges qualification split scratch as exact backend residency" {
    const page = std.heap.pageSize();
    const raw = @as(usize, 8) * 8 * 16 * (512 + 2) * @sizeOf(f32);
    const scratch = try rounded(raw);
    const sizes = branch_mod.Allocation{ .descriptor_arena_bytes = 2 * page, .replay_split_scratch_bytes = scratch };
    try std.testing.expectEqual(3 * page + 2 * page + scratch, try replayFixedBackendBytes(sizes));
    try std.testing.expectEqual((raw + page - 1) & ~(page - 1), scratch);
    if (page == 16384) try std.testing.expectEqual(@as(usize, 2_113_536), scratch);
}

const RetentionFitGeometry = struct {
    training_rows: u32,
    validation_rows: u32,
    validation_group_rows: u32,
};

const RetentionInputs = struct {
    arena: *std.heap.ArenaAllocator,
    prompt: []const i64,
    eligible: payload.Eligible,
    parts: [][]const storage.Segment,
};

const RetentionSizing = struct {
    staged_bytes: usize = 0,
    staged_device: usize = 0,
    staged_host: usize = 0,
    old_device: usize = 0,
    workspace: usize = 0,
};

fn prepareRetentionInputs(parent: std.mem.Allocator, ids: []const i64, eligible: *const payload.Eligible, owner_count: usize) !RetentionInputs {
    if (ids.len == 0 or ids.len != eligible.labels.len or eligible.blocks.len == 0)
        return error.InvalidPayloadProvenance;
    for (ids) |id| if (id < 0) return error.InvalidPayloadProvenance;
    var end: u32 = 0;
    var block_index: usize = 0;
    for (eligible.ranges) |range| {
        if (range.start < end or range.start >= range.end or range.end > ids.len or
            !std.mem.allEqual(u32, eligible.labels[end..range.start], 0) or eligible.labels[range.start] == 0 or
            !std.mem.allEqual(u32, eligible.labels[range.start..range.end], eligible.labels[range.start]))
            return error.InvalidPayloadProvenance;
        var block = range.start;
        while (range.end - block >= 512) : (block += 512) {
            if (block_index >= eligible.blocks.len or eligible.blocks[block_index].start != block or eligible.blocks[block_index].end != block + 512)
                return error.InvalidPayloadProvenance;
            block_index += 1;
        }
        end = range.end;
    }
    if (block_index != eligible.blocks.len or !std.mem.allEqual(u32, eligible.labels[end..], 0))
        return error.InvalidPayloadProvenance;
    const arena = try parent.create(std.heap.ArenaAllocator);
    errdefer parent.destroy(arena);
    arena.* = std.heap.ArenaAllocator.init(parent);
    errdefer arena.deinit();
    const a = arena.allocator();
    return .{
        .arena = arena,
        .prompt = try a.dupe(i64, ids),
        .eligible = .{ .allocator = a, .labels = try a.dupe(u32, eligible.labels), .ranges = try a.dupe(reference.Range, eligible.ranges), .blocks = try a.dupe(reference.Range, eligible.blocks) },
        .parts = try a.alloc([]const storage.Segment, owner_count),
    };
}

/// The one retention segment/staging/fitter builder used by legacy diagnostics
/// and the production campaign. Query collection differs only in the already
/// validated geometry supplied for each owner.
fn buildRetention(
    config: live_mod.Config,
    profile: profiles.Profile,
    retained_limit: u32,
    output: u32,
    inputs: RetentionInputs,
    geometries: []const RetentionFitGeometry,
) !RetentionSizing {
    if (geometries.len != profile.owners.len or inputs.parts.len != profile.owners.len)
        return error.InvalidContextLayout;
    const a = inputs.arena.allocator();
    var sizing = RetentionSizing{};
    for (profile.owners, inputs.parts, geometries) |owner, *out, geometry| {
        const l = config.layout.layers[owner.layer];
        var segments: std.ArrayListUnmanaged(storage.Segment) = .empty;
        var position: u32 = 0;
        for (inputs.eligible.blocks) |block| {
            if (block.start < position or block.end > inputs.prompt.len or block.end -| block.start != 512 or block.end == inputs.prompt.len or
                inputs.eligible.labels[block.start] == 0 or !std.mem.allEqual(u32, inputs.eligible.labels[block.start..block.end], inputs.eligible.labels[block.start]))
                return error.InvalidPayloadProvenance;
            if (position < block.start) {
                const exact_rows = try a.alloc(u32, l.kv_heads);
                @memset(exact_rows, block.start - position);
                try segments.append(a, .{ .logical_count = block.start - position, .retained = exact_rows });
            }
            try segments.append(a, .{ .logical_count = 512, .retained = try a.dupe(u32, owner.retained) });
            position = block.end;
        }
        const final_rows = try a.alloc(u32, l.kv_heads);
        @memset(final_rows, @as(u32, @intCast(inputs.prompt.len)) - position);
        try segments.append(a, .{ .logical_count = @as(u32, @intCast(inputs.prompt.len)) - position, .retained = final_rows });
        out.* = try segments.toOwnedSlice(a);
        const next = try storage.allocation(.{ .head_dim = l.head_dim, .kv_heads = l.kv_heads, .continuation_rows = output, .max_retained_rows = retained_limit }, out.*);
        sizing.staged_bytes = try add(sizing.staged_bytes, next.physical_bytes);
        sizing.staged_device = try add(sizing.staged_device, next.device_bytes);
        sizing.staged_host = try add(sizing.staged_host, next.metadata_bytes);
        sizing.old_device = try add(sizing.old_device, (try storage.Generation.emptyAllocation(l.head_dim, l.kv_heads, @intCast(config.plan.logical_token_limit))).device_bytes);
        for (owner.retained) |count| if (count < 512) {
            sizing.workspace = @max(sizing.workspace, (try fitter.allocation(.{
                .source_rows = 512,
                .head_dim = l.head_dim,
                .kv_heads = 1,
                .kv_head = 0,
                .training_rows = geometry.training_rows,
                .validation_rows = geometry.validation_rows,
                .validation_group_rows = geometry.validation_group_rows,
                .scale = 1,
                .staging_budget_bytes = std.math.maxInt(usize),
                .options = .{ .retained_rows = count },
            })).physical_bytes);
        };
    }
    return sizing;
}

pub fn preparePlan(parent: std.mem.Allocator, config: live_mod.Config, ids: []const i64, eligible: *const payload.Eligible, instruction: []const i64, profile: profiles.Profile, actual: profiles.Identity, retained_limit: u32) !PreparedPlan {
    if (config.plan.prompt_tokens >= config.plan.logical_token_limit or config.plan.logical_token_limit > std.math.maxInt(u32) or
        ids.len != config.plan.prompt_tokens or ids.len != eligible.labels.len or eligible.blocks.len == 0) return error.InvalidPayloadProvenance;
    const output: u32 = @intCast(config.plan.logical_token_limit - config.plan.prompt_tokens);
    try profile.validate(actual, config.layout, ids.len, output);
    const inputs = try prepareRetentionInputs(parent, ids, eligible, profile.owners.len);
    errdefer {
        inputs.arena.deinit();
        parent.destroy(inputs.arena);
    }
    const replay = try reference.prepare(inputs.arena.allocator(), inputs.prompt, inputs.eligible.ranges, instruction, config.plan.context_limit);
    const branch = branch_mod.Config{ .plan = try (@import("../runtime/context_policy.zig").Policy{}).admit(config.plan.context_limit, null, inputs.prompt.len, replay.tokens.len, 0), .layout = config.layout, .max_query_rows = 1, .physical_budget_bytes = 0 };
    const geometries = try inputs.arena.allocator().alloc(RetentionFitGeometry, profile.owners.len);
    for (profile.owners, geometries) |owner, *geometry| {
        const q = try branch_mod.QueryShape.init(branch, owner.layer);
        geometry.* = .{ .training_rows = q.training_rows, .validation_rows = q.validation_rows, .validation_group_rows = q.held_per_stratum };
    }
    const sizing = try buildRetention(config, profile, retained_limit, output, inputs, geometries);
    return .{ .arena = inputs.arena, .config = config, .prompt = inputs.prompt, .eligible = inputs.eligible, .replay = replay, .parts = inputs.parts, .retained_limit = retained_limit, .staged_bytes = sizing.staged_bytes, .staged_device = sizing.staged_device, .staged_host = sizing.staged_host, .old_device = sizing.old_device, .workspace = sizing.workspace, .minimum_saved_bytes = profile.minimum_saved_bytes };
}

/// Compatibility wrapper for current test/native callers. New admission code
/// should retain a PreparedPlan, select a chunk from `estimate`, then finalize.
pub fn prepare(parent: std.mem.Allocator, config: live_mod.Config, ids: []const i64, eligible: *const payload.Eligible, instruction: []const i64, profile: profiles.Profile, actual: profiles.Identity, retained_limit: u32, peak_budget: usize) !Request {
    var plan = try preparePlan(parent, config, ids, eligible, instruction, profile, actual, retained_limit);
    defer plan.deinit();
    const sizing = try plan.estimate(config.max_query_rows);
    if (sizing.combined_peak_bytes > peak_budget) return error.ContextStagingBudgetExceeded;
    return plan.finalize(config.max_query_rows, peak_budget, peak_budget);
}

pub const PreparedQuestionSource = struct {
    ids: []const i64,
    source_start: u32,
    source_end: u32,
    prompt_tokens: u32,
    prompt_tokens_sha256: [32]u8,
};

pub const PreparedStudySource = struct {
    ids: []const i64,
    template_bytes: []const u8,
    generation_contract_bytes: []const u8,
    prompt_tokens: u32,
    prompt_tokens_sha256: [32]u8,
    response_tokens: u32,
};

/// All slices are copied into the request arena by prepareCampaignPlan. The
/// position limit is the loaded architecture limit, never the public serving
/// window. This permits an 8160+32 public request to run a bounded private
/// study when the loaded model itself supports the longer branch.
pub const PreparedQuerySources = struct {
    question: PreparedQuestionSource,
    fit: PreparedStudySource,
    gate: PreparedStudySource,
    loaded_private_position_limit: u32,
};

pub const SelectedCampaignAuthority = struct {
    load_generation: u64,
    loaded_position_limit: u32,
    identity: profiles.Identity,
    profile_artifact_sha256: [32]u8,
    profile_semantic_sha256: [32]u8,
};

pub const CampaignAuthority = union(enum) {
    production: struct {
        selected: SelectedCampaignAuthority,
        profile: profiles.Profile,
        retained_limit: u32,
    },
    /// Only test/capture binaries may construct this arm. It uses compiled
    /// query authorities and exact retention, and can never publish.
    capture: struct {
        load_generation: u64,
        loaded_position_limit: u32,
        identity: profiles.Identity,
        protocols: []const profiles.QueryProtocolAuthority,
    },
};

pub const CampaignStudyReceipt = struct {
    session_identity: u64,
    prefill_tokens: u32,
    generated_tokens: u32,
    generated_tokens_sha256: [32]u8,
    study_timing: ?StudyTiming = null,
    provider_delta: ?StudyProviderDelta = null,
    singleton_paged_decode_forwards: u32 = 0,
    singleton_paged_decode_expected_layer_dispatches: u64 = 0,
};

pub const StudyTiming = struct {
    prefill_hidden_ns: u64,
    initial_logits_ns: u64,
    generated_loop_ns: u64,
};

/// Selected cumulative backend counters sampled around one private study.
/// Every field is observational: failure to form a monotonic delta yields null
/// for the whole snapshot and never changes generation or branch ownership.
pub const StudyProviderDelta = struct {
    frame_begins: u64,
    frame_submits: u64,
    frame_wait_ns: u128,
    frame_gpu_ns: u128,
    compute_encoders: u64,
    blit_encoders: u64,
    precomputed_tail_blocks: u64,
    precomputed_tail_failures: u64,
    paged_attention_1x_calls: u64,
    context_session_paged_decode_framed_dispatches: u64,
    decode_gqa_split_calls: u64,
    generated_attention_decode_1x_calls: u64,
    generated_attention_flash_prefill_calls: u64,
    attention_prefill_direct_kv_calls: u64,
    attention_prefill_paged_kv_calls: u64,
    to_host_calls: u64,
    to_host_device_calls: u64,
    host_download_bytes: u64,
};

/// Absolute capture-only provider counters. It deliberately shares the delta
/// shape so every counter used by a study is also observable for question
/// replay. Production runners leave the callback null.
pub const StudyProviderSnapshot = StudyProviderDelta;

fn providerDelta(before: StudyProviderSnapshot, after: StudyProviderSnapshot) ?StudyProviderDelta {
    return .{
        .frame_begins = std.math.sub(u64, after.frame_begins, before.frame_begins) catch return null,
        .frame_submits = std.math.sub(u64, after.frame_submits, before.frame_submits) catch return null,
        .frame_wait_ns = std.math.sub(u128, after.frame_wait_ns, before.frame_wait_ns) catch return null,
        .frame_gpu_ns = std.math.sub(u128, after.frame_gpu_ns, before.frame_gpu_ns) catch return null,
        .compute_encoders = std.math.sub(u64, after.compute_encoders, before.compute_encoders) catch return null,
        .blit_encoders = std.math.sub(u64, after.blit_encoders, before.blit_encoders) catch return null,
        .precomputed_tail_blocks = std.math.sub(u64, after.precomputed_tail_blocks, before.precomputed_tail_blocks) catch return null,
        .precomputed_tail_failures = std.math.sub(u64, after.precomputed_tail_failures, before.precomputed_tail_failures) catch return null,
        .paged_attention_1x_calls = std.math.sub(u64, after.paged_attention_1x_calls, before.paged_attention_1x_calls) catch return null,
        .context_session_paged_decode_framed_dispatches = std.math.sub(u64, after.context_session_paged_decode_framed_dispatches, before.context_session_paged_decode_framed_dispatches) catch return null,
        .decode_gqa_split_calls = std.math.sub(u64, after.decode_gqa_split_calls, before.decode_gqa_split_calls) catch return null,
        .generated_attention_decode_1x_calls = std.math.sub(u64, after.generated_attention_decode_1x_calls, before.generated_attention_decode_1x_calls) catch return null,
        .generated_attention_flash_prefill_calls = std.math.sub(u64, after.generated_attention_flash_prefill_calls, before.generated_attention_flash_prefill_calls) catch return null,
        .attention_prefill_direct_kv_calls = std.math.sub(u64, after.attention_prefill_direct_kv_calls, before.attention_prefill_direct_kv_calls) catch return null,
        .attention_prefill_paged_kv_calls = std.math.sub(u64, after.attention_prefill_paged_kv_calls, before.attention_prefill_paged_kv_calls) catch return null,
        .to_host_calls = std.math.sub(u64, after.to_host_calls, before.to_host_calls) catch return null,
        .to_host_device_calls = std.math.sub(u64, after.to_host_device_calls, before.to_host_device_calls) catch return null,
        .host_download_bytes = std.math.sub(u64, after.host_download_bytes, before.host_download_bytes) catch return null,
    };
}

test "context campaign provider snapshot delta is monotonic" {
    const before = StudyProviderSnapshot{ .frame_begins = 1, .frame_submits = 2, .frame_wait_ns = 3, .frame_gpu_ns = 4, .compute_encoders = 5, .blit_encoders = 6, .precomputed_tail_blocks = 7, .precomputed_tail_failures = 8, .paged_attention_1x_calls = 9, .context_session_paged_decode_framed_dispatches = 10, .decode_gqa_split_calls = 11, .generated_attention_decode_1x_calls = 12, .generated_attention_flash_prefill_calls = 13, .attention_prefill_direct_kv_calls = 14, .attention_prefill_paged_kv_calls = 15, .to_host_calls = 16, .to_host_device_calls = 17, .host_download_bytes = 18 };
    var after = before;
    after.frame_begins += 2;
    after.context_session_paged_decode_framed_dispatches += 3;
    try std.testing.expectEqual(@as(u64, 2), providerDelta(before, after).?.frame_begins);
    try std.testing.expectEqual(@as(u64, 3), providerDelta(before, after).?.context_session_paged_decode_framed_dispatches);
    after.frame_wait_ns = 2;
    try std.testing.expect(providerDelta(before, after) == null);
}

pub const CampaignStudyExecutionOptions = struct {
    /// Test-binary-only request for q=1 study forwards to use the backend-owned
    /// paged-decode route. Multirow study prefix chunks remain paged-prefill.
    singleton_paged_decode: bool = false,
};

pub const CampaignRunner = struct {
    ctx: *anyopaque,
    replay_fn: *const fn (*anyopaque, contract.Hook, usize, []const i64) anyerror!void,
    /// `captured_tokens` is non-null only for a test-only observer run. The
    /// runner must fill every element with the actual on-policy token before
    /// returning; production never allocates this diagnostic buffer.
    study_fn: *const fn (*anyopaque, std.mem.Allocator, contract.Hook, []const i64, u32, CampaignStudyExecutionOptions, ?[]i64) anyerror!CampaignStudyReceipt,
    /// Test-capture-only passive counter source. It must not allocate, submit,
    /// wait, or mutate model/session state.
    provider_snapshot_fn: ?*const fn (*anyopaque) ?StudyProviderSnapshot = null,
    fault_controller: fault_injection.Slot = if (@import("builtin").is_test) null else {},
};

/// Test-binary-only replay controls for matched qualification A/B runs. Null
/// is the only production/default value and preserves serial global replay.
pub const CampaignCaptureReplayOptions = struct {
    global_attention_split: ?@import("replay_context.zig").SplitConfig = null,
    /// Explicit qualification-only A/B control. It never enters production
    /// CampaignRequest.run and is bound into the raw capture receipt.
    study_singleton_paged_decode: bool = false,
};

pub const CampaignBranchReceipt = struct {
    branch: query_campaign.Branch,
    session_identity: u64,
    prefix_ids: []const i64,
    generated_ids: []const i64,
    generated_tokens_sha256: [32]u8,
    elapsed_ns: u64,
    status_readback_bytes: u64,
    query_bytes: usize,
    physical_bytes: usize,
    peak_bytes: usize,
    replay_split_scratch_bytes: usize,
    serial_attention_dispatches: u64,
    split_attention_dispatches: u64,
    study_timing: ?StudyTiming = null,
    provider_delta: ?StudyProviderDelta = null,
    singleton_paged_decode_forwards: u32 = 0,
    singleton_paged_decode_expected_layer_dispatches: u64 = 0,
    physical_branch_id: u8,
    route: enum { question_hidden_replay, fixed_serial_private_study },
};

pub const CampaignObservation = struct {
    campaign: *const query_campaign.Campaign,
    question: CampaignBranchReceipt,
    fit: CampaignBranchReceipt,
    gate: CampaignBranchReceipt,
    load_generation: u64,
    live_epoch_before: u64,
    live_epoch_after: u64,
    live_bytes_before: usize,
    live_bytes_after: usize,
    live_session_identity_before: u64,
    live_session_identity_after: u64,
    live_fingerprint_before: ?@import("context_teacher_forced.zig").QualityFingerprint,
    live_fingerprint_after: ?@import("context_teacher_forced.zig").QualityFingerprint,
    publication_requested: bool,
    prompt_tokens: u32,
    prompt_tokens_sha256: [32]u8,
    question_source_start: u32,
    question_source_end: u32,
    campaign_query_bytes: usize,
    campaign_backend_bytes: usize,
    campaign_host_bytes: usize,
    campaign_physical_bytes: usize,
    query_export_bytes: usize,
    observer_output_admitted_bytes: usize,
    replay_options: CampaignCaptureReplayOptions,
};

pub const CampaignObserver = struct {
    ctx: *anyopaque,
    observe_fn: *const fn (*anyopaque, CampaignObservation) anyerror!void,
};

pub const StoredDiagnosticReservation = struct {
    reservation_identity: u64,
    algorithm_sha256: [32]u8,
    build_sha256: [32]u8,
    artifact_manifest_sha256: [32]u8,
    profile_sha256: [32]u8,
    coordinates: []const StoredDiagnosticCoordinate,
    source_descriptor_sha256: [32]u8,
    source_payload_sha256: [32]u8,
    loader_budget_bytes: usize,
    materialization_host_bytes: usize,
    artifact_count: u32,
    expected_baseline_context_bytes: u64,
    /// Global/profile owners; never total packed-session owners.
    expected_owner_count: u32,
    expected_total_owner_count: u32 = 0,
};

pub const StoredDiagnosticCoordinate = struct {
    owner_layer: u32,
    head: u32,
    span: u32,
    logical_start: u32,
    logical_count: u32,
    retained_rows: u32,
    head_dim: u32,
};

pub const StoredDiagnosticMaterialization = struct {
    artifacts: []const live_mod.StoredArtifact,
    /// Identity-derived manifest over the loaded artifact digests. This is
    /// distinct from the Phase-A raw artifact manifest frozen before held data.
    derived_manifest_sha256: [32]u8,
    /// Exact tracker-observed physical delta for provider-owned host buffers.
    /// The provider must allocate these through the supplied owner tracker.
    tracked_host_bytes: u64,
    ctx: *anyopaque,
    deinit_fn: *const fn (*anyopaque) anyerror!void,
};

/// Diagnostic driver owns opened immutable artifact/source descriptors and an
/// exclusive non-reuse ledger reservation. It receives identities only; it
/// never receives live Session, Maintenance, Batch, or destination handles.
pub const StoredDiagnosticProvider = struct {
    ctx: *anyopaque,
    reserve_fn: *const fn (*anyopaque) anyerror!StoredDiagnosticReservation,
    materialize_fn: *const fn (*anyopaque, live_mod.StoredCampaignIdentity, *owner_alloc.HostAllocationTracker) anyerror!StoredDiagnosticMaterialization,
    precommit_fn: *const fn (*anyopaque, live_mod.StoredCampaignIdentity, [32]u8) anyerror!void,
    commit_reservation_fn: *const fn (*anyopaque, u64, live_mod.StoredPublicationReceipt) anyerror!void,
    /// Durable failure closure: consumes the generator/corpus/row tuple as a
    /// failed attempt. It must never make held rows reusable.
    abort_reservation_fn: *const fn (*anyopaque, u64) anyerror!StoredReservationFailureReceipt,
};

pub const StoredReservationFailureReceipt = struct {
    reservation_identity: u64,
    durable: bool,
    nonreuse_committed: bool,
    evidence_sha256: [32]u8,
};

pub const StoredDiagnosticReceipt = struct {
    diagnostic_only: bool = true,
    serving_authority: bool = false,
    release_authority: bool = false,
    state: enum { prepared, failed_before_commit, ledger_failure, committed, committed_but_receipt_failed } = .prepared,
    reservation_identity: u64 = 0,
    algorithm_sha256: [32]u8 = @splat(0),
    build_sha256: [32]u8 = @splat(0),
    artifact_manifest_sha256: [32]u8 = @splat(0),
    profile_sha256: [32]u8 = @splat(0),
    failure: ?StoredReservationFailureReceipt = null,
    baseline_request_context_physical_bytes: u64 = 0,
    stored_request_context_physical_bytes: u64 = 0,
    baseline_owner_count: u32 = 0,
    baseline_global_owner_count: u32 = 0,
    baseline_total_owner_count: u32 = 0,
    stored_owner_count: u32 = 0,
    stored_global_owner_count: u32 = 0,
    stored_total_owner_count: u32 = 0,
    tracker_live_bytes_before: u64 = 0,
    tracker_live_bytes_after: u64 = 0,
    tracker_peak_bytes_before: u64 = 0,
    tracker_peak_bytes_after: u64 = 0,
    replay_control_bytes_before: u64 = 0,
    replay_control_bytes_after: u64 = 0,
    publication: ?live_mod.StoredPublicationReceipt = null,
};

const StoredDiagnosticRun = struct {
    provider: StoredDiagnosticProvider,
    reservation: StoredDiagnosticReservation,
    eligible: *const payload.Eligible,
    parts: []const []const storage.Segment,
    retained_limit: u32,
    publication: *?live_mod.StoredPublicationReceipt,
};

fn admitStoredProviderBeforeUpload(current_tracked: u64, materialization_host: usize, loader_budget: usize, native_peak_budget: usize) !void {
    var projected = current_tracked;
    projected = try std.math.add(u64, projected, @intCast(materialization_host));
    projected = try std.math.add(u64, projected, @intCast(loader_budget));
    if (projected > @as(u64, @intCast(native_peak_budget))) return error.ContextStagingBudgetExceeded;
}

test "stored provider host and loader are rejected before upload" {
    try admitStoredProviderBeforeUpload(700, 100, 200, 1000);
    try std.testing.expectError(error.ContextStagingBudgetExceeded, admitStoredProviderBeforeUpload(701, 100, 200, 1000));
}

fn validateStudyExecutionReceipt(receipt: CampaignStudyReceipt, response_tokens: u32, options: CampaignStudyExecutionOptions) !void {
    const forwards = if (options.singleton_paged_decode)
        try std.math.add(u32, response_tokens, 1)
    else
        0;
    if (receipt.singleton_paged_decode_forwards != forwards)
        return error.InvalidStudyGeneration;
    if (options.singleton_paged_decode) {
        if (receipt.singleton_paged_decode_expected_layer_dispatches == 0 or
            receipt.singleton_paged_decode_expected_layer_dispatches % @as(u64, forwards) != 0)
            return error.InvalidStudyGeneration;
    } else if (receipt.singleton_paged_decode_expected_layer_dispatches != 0) {
        return error.InvalidStudyGeneration;
    }
}

test "context campaign study execution receipt binds qualification route" {
    var receipt = CampaignStudyReceipt{
        .session_identity = 1,
        .prefill_tokens = 2,
        .generated_tokens = 3,
        .generated_tokens_sha256 = @splat(0),
    };
    try validateStudyExecutionReceipt(receipt, 3, .{});
    receipt.singleton_paged_decode_forwards = 4;
    receipt.singleton_paged_decode_expected_layer_dispatches = 140;
    try validateStudyExecutionReceipt(receipt, 3, .{ .singleton_paged_decode = true });
    receipt.singleton_paged_decode_forwards = 3;
    try std.testing.expectError(error.InvalidStudyGeneration, validateStudyExecutionReceipt(receipt, 3, .{ .singleton_paged_decode = true }));
    receipt.singleton_paged_decode_forwards = 4;
    receipt.singleton_paged_decode_expected_layer_dispatches = 139;
    try std.testing.expectError(error.InvalidStudyGeneration, validateStudyExecutionReceipt(receipt, 3, .{ .singleton_paged_decode = true }));
}

fn tokenDigest(ids: []const i64) ![32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for (ids) |id| {
        if (id < 0) return error.InvalidPayloadProvenance;
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &bytes, id, .little);
        hash.update(&bytes);
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn validateSourceBounds(prompt: []const i64, sources: PreparedQuerySources) !void {
    if (prompt.len == 0 or prompt.len > std.math.maxInt(u32) or
        sources.loaded_private_position_limit == 0 or sources.loaded_private_position_limit > 131072)
        return error.InvalidQueryProtocol;
    const prompt_count: u32 = @intCast(prompt.len);
    const digest = try tokenDigest(prompt);
    inline for (.{ sources.question.prompt_tokens, sources.fit.prompt_tokens, sources.gate.prompt_tokens }) |count|
        if (count != prompt_count) return error.InvalidPayloadProvenance;
    inline for (.{ sources.question.prompt_tokens_sha256, sources.fit.prompt_tokens_sha256, sources.gate.prompt_tokens_sha256 }) |actual|
        if (!std.mem.eql(u8, &actual, &digest)) return error.InvalidPayloadProvenance;
    if (sources.question.ids.len == 0 or sources.question.source_start >= sources.question.source_end or
        sources.question.source_end > prompt.len or
        sources.question.source_end - sources.question.source_start != sources.question.ids.len or
        !std.mem.eql(i64, prompt[sources.question.source_start..sources.question.source_end], sources.question.ids))
        return error.InvalidPayloadProvenance;
    inline for (.{ sources.fit, sources.gate }) |study| {
        if (study.ids.len == 0 or study.response_tokens == 0) return error.InvalidQueryProtocol;
        const end = try std.math.add(usize, prompt.len, try std.math.add(usize, study.ids.len, study.response_tokens));
        if (end > sources.loaded_private_position_limit) return error.ContextReplayBudgetExceeded;
        for (study.ids) |id| if (id < 0) return error.InvalidPayloadProvenance;
    }
    if (try std.math.add(usize, prompt.len, sources.question.ids.len) > sources.loaded_private_position_limit)
        return error.ContextReplayBudgetExceeded;
}

fn validatePreparedSources(
    prompt: []const i64,
    sources: PreparedQuerySources,
    protocols: []const profiles.QueryProtocolAuthority,
    layout: @import("../runtime/context_policy.zig").Layout,
) !void {
    try validateSourceBounds(prompt, sources);
    if (layout.len == 0 or layout.len > layout.layers.len) return error.InvalidContextLayout;
    if (protocols.len == 0) return error.CompactionQueryProtocolMismatch;
    var protocol_index: usize = 0;
    for (layout.layers[0..layout.len], 0..) |layer, layer_index| {
        if (layer.owner != layer_index or layer.sliding_window != 0) continue;
        if (protocol_index >= protocols.len or protocols[protocol_index].layer != layer_index)
            return error.CompactionQueryProtocolMismatch;
        try query_protocol.validateRuntimeAuthority(protocols[protocol_index], layout);
        protocol_index += 1;
    }
    if (protocol_index != protocols.len) return error.CompactionQueryProtocolMismatch;
    const first = protocols[0];
    try study_templates.validate(.fit, first.fit);
    try study_templates.validate(.gate, first.gate);
    try query_protocol.validateStudyBytes(first.fit, sources.fit.template_bytes, sources.fit.generation_contract_bytes);
    try query_protocol.validateStudyBytes(first.gate, sources.gate.template_bytes, sources.gate.generation_contract_bytes);
    if (sources.fit.response_tokens != first.fit.response_tokens or sources.gate.response_tokens != first.gate.response_tokens)
        return error.CompactionQueryProtocolMismatch;
    for (protocols[1..]) |authority| {
        if (!std.meta.eql(first.fit, authority.fit) or !std.meta.eql(first.gate, authority.gate))
            return error.CompactionQueryProtocolMismatch;
    }
}

fn campaignSources(sources: PreparedQuerySources) ![query_protocol.max_sources]query_protocol.Source {
    const prompt = sources.question.prompt_tokens;
    const question_end = try std.math.add(u32, prompt, @intCast(sources.question.ids.len));
    const fit_prefill_end = try std.math.add(u32, prompt, @intCast(sources.fit.ids.len));
    const fit_end = try std.math.add(u32, fit_prefill_end, sources.fit.response_tokens);
    const gate_prefill_end = try std.math.add(u32, prompt, @intCast(sources.gate.ids.len));
    const gate_end = try std.math.add(u32, gate_prefill_end, sources.gate.response_tokens);
    return .{
        .{ .kind = .request_question_replay, .partition = .training, .start = prompt, .end = question_end, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .training, .start = prompt, .end = fit_prefill_end, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .training, .start = fit_prefill_end, .end = fit_end, .active = false },
        .{ .kind = .fixed_study_prefill, .partition = .validation, .start = prompt, .end = gate_prefill_end, .active = false },
        .{ .kind = .fixed_study_generated_response, .partition = .validation, .start = gate_prefill_end, .end = gate_end, .active = false },
    };
}

fn sourcesForBranch(all: [query_protocol.max_sources]query_protocol.Source, branch: query_campaign.Branch) [query_protocol.max_sources]query_protocol.Source {
    var result = all;
    switch (branch) {
        .question => result[0].active = true,
        .fit => {
            result[1].active = true;
            result[2].active = true;
        },
        .gate => {
            result[3].active = true;
            result[4].active = true;
        },
    }
    return result;
}

fn replayQuestion(runner: CampaignRunner, hook: contract.Hook, prompt_tokens: usize, ids: []const i64) !void {
    var cursor: usize = 0;
    while (cursor < ids.len) {
        const position = try std.math.add(usize, prompt_tokens, cursor);
        const requested_rows: usize = if (comptime @import("builtin").is_test)
            if (runner.fault_controller) |fault|
                if (fault.kind == .cancel_replay) @min(ids.len - cursor, 1) else ids.len - cursor
            else
                ids.len - cursor
        else
            ids.len - cursor;
        const rows = try hook.nextChunk(@intCast(position), @intCast(requested_rows));
        if (rows == 0 or rows > ids.len - cursor) return error.InvalidStudyChunk;
        try runner.replay_fn(runner.ctx, hook, position, ids[cursor..][0..rows]);
        cursor += rows;
        if (comptime @import("builtin").is_test) if (runner.fault_controller) |fault|
            try fault.checkpoint(.replay, @intCast(cursor), @intCast(ids.len));
    }
}

/// The only branch orchestration routine. Production and test capture share
/// this exact order and device gather. Each earlier branch is destroyed after
/// its synchronous absorb; the gate branch stays alive only through the
/// publication transaction because Maintenance owns that transaction API.
const CampaignRunMode = enum { production, capture, stored_diagnostic };

fn validateCampaignRunMode(mode: CampaignRunMode, has_observer: bool, has_publication: bool, has_stored: bool, test_build: bool, replay_options: CampaignCaptureReplayOptions) !void {
    switch (mode) {
        .production => if (!has_publication or has_observer or has_stored or replay_options.global_attention_split != null or replay_options.study_singleton_paged_decode) return error.UnqualifiedCompactionArtifact,
        .capture => if (!test_build or has_publication or has_stored or !has_observer) return error.UnqualifiedCompactionArtifact,
        .stored_diagnostic => if (!test_build or has_publication or has_observer or !has_stored or replay_options.global_attention_split != null or replay_options.study_singleton_paged_decode) return error.UnqualifiedCompactionArtifact,
    }
}

test "context campaign run mode cannot cross publication and capture authority" {
    try validateCampaignRunMode(.production, false, true, false, false, .{});
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, validateCampaignRunMode(.production, true, true, false, true, .{}));
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, validateCampaignRunMode(.production, false, false, false, true, .{}));
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, validateCampaignRunMode(.production, false, true, false, true, .{ .global_attention_split = .{ .max_query_rows = 1, .split_count = 8 } }));
    try validateCampaignRunMode(.capture, true, false, false, true, .{});
    try validateCampaignRunMode(.capture, true, false, false, true, .{ .global_attention_split = .{ .max_query_rows = 1, .split_count = 8 } });
    try validateCampaignRunMode(.capture, true, false, false, true, .{ .study_singleton_paged_decode = true });
    try validateCampaignRunMode(.stored_diagnostic, false, false, true, true, .{});
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, validateCampaignRunMode(.stored_diagnostic, false, false, false, true, .{}));
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, validateCampaignRunMode(.production, false, true, false, true, .{ .study_singleton_paged_decode = true }));
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, validateCampaignRunMode(.capture, true, true, false, true, .{}));
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, validateCampaignRunMode(.capture, true, false, false, false, .{}));
}

fn runQueryCampaign(
    live: *live_mod.Session,
    sources: PreparedQuerySources,
    load_generation: u64,
    runner: CampaignRunner,
    observer: ?CampaignObserver,
    publication: ?struct {
        eligible: *const payload.Eligible,
        parts: []const []const storage.Segment,
        retained_limit: u32,
    },
    stored: ?StoredDiagnosticRun,
    peak_budget: usize,
    mode: CampaignRunMode,
    replay_options: CampaignCaptureReplayOptions,
) !void {
    const allocator = live.allocator;
    if (load_generation == 0 or live.config.execution_lease == null or live.config.execution_lease.?.generation != load_generation)
        return error.StaleContextView;
    try validateCampaignRunMode(mode, observer != null, publication != null, stored != null, @import("builtin").is_test, replay_options);
    const all_sources = try campaignSources(sources);
    const campaign_sizes = try query_campaign.Campaign.allocation(live.config.layout, &all_sources);
    const fit_output_admitted_bytes = if (mode == .capture) try rounded(try mul(sources.fit.response_tokens, @sizeOf(i64))) else 0;
    const gate_output_admitted_bytes = if (mode == .capture) try rounded(try mul(sources.gate.response_tokens, @sizeOf(i64))) else 0;
    const observer_output_admitted_bytes = try add(fit_output_admitted_bytes, gate_output_admitted_bytes);
    const fixed_campaign_bytes = try add(campaign_sizes.physical_bytes, observer_output_admitted_bytes);
    // Shared replay controls persist on the live generations after each
    // private branch closes. Materialize them before the strict campaign
    // baseline so they are admitted as session residency, never mistaken for
    // a branch leak or omitted from the peak.
    _ = try live.ensureReplayControls(peak_budget);
    const epoch_before = live.publication.epoch;
    const bytes_before = live.metrics.live_bytes;
    const live_identity_before = live.identity;
    // Deliberately outside every branch timer. This test-only readback hashes
    // initialized logical rows only and reports its bytes separately.
    const live_fingerprint_before = if (mode == .capture) try live.qualificationFingerprint() else null;
    if (try add(bytes_before, fixed_campaign_bytes) >= peak_budget)
        return error.ContextReplayBudgetExceeded;
    // Admit the largest complete private branch before allocating the campaign
    // pool. Replay controls are already resident and therefore subtracted;
    // the optional split scratch remains in the incremental branch bytes and
    // replayTierBytes classifies it as backend residency.
    var largest_incremental_branch: usize = 0;
    inline for (.{ query_campaign.Branch.question, query_campaign.Branch.fit, query_campaign.Branch.gate }) |branch_kind| {
        const branch_sources = sourcesForBranch(all_sources, branch_kind);
        var branch_config = try branchReplayConfig(live.config, sources, branch_kind);
        branch_config.query_sources = &branch_sources;
        branch_config.global_attention_split = replay_options.global_attention_split;
        const branch_sizes = try branch_mod.estimate(branch_config);
        _ = try replayTierBytes(branch_config, branch_sizes);
        largest_incremental_branch = @max(largest_incremental_branch, try std.math.sub(usize, branch_sizes.physical_bytes, branch_sizes.owner_control_bytes));
    }
    const admitted_peak = try add(try add(bytes_before, fixed_campaign_bytes), largest_incremental_branch);
    if (admitted_peak > peak_budget) return error.ContextReplayBudgetExceeded;
    const campaign_id = next_campaign_identity.fetchAdd(1, .monotonic);
    if (campaign_id == 0) return error.StaleContextView;
    var campaign = try query_campaign.Campaign.init(allocator, live.config.execution_lease.?, live.allocation_tracker, live.config.layout, &all_sources, campaign_sizes.physical_bytes, campaign_id, load_generation);
    defer campaign.deinit();
    const branch_budget = peak_budget - fixed_campaign_bytes;

    var question_receipt = CampaignBranchReceipt{ .branch = .question, .session_identity = 0, .prefix_ids = sources.question.ids, .generated_ids = &.{}, .generated_tokens_sha256 = @splat(0), .elapsed_ns = 0, .status_readback_bytes = 0, .query_bytes = 0, .physical_bytes = 0, .peak_bytes = 0, .replay_split_scratch_bytes = 0, .serial_attention_dispatches = 0, .split_attention_dispatches = 0, .study_timing = null, .provider_delta = null, .singleton_paged_decode_forwards = 0, .singleton_paged_decode_expected_layer_dispatches = 0, .physical_branch_id = 0, .route = .question_hidden_replay };
    {
        const timer = try CampaignTimer.start();
        const active = sourcesForBranch(all_sources, .question);
        var maintenance = try live.beginReplayWithOptions(@intCast(sources.question.ids.len), branch_budget, .{ .query_sources = &active, .private_position_limit = sources.loaded_private_position_limit, .qualification_global_attention_split = replay_options.global_attention_split });
        defer maintenance.deinit();
        const provider_before = if (mode == .capture) if (runner.provider_snapshot_fn) |snapshot| snapshot(runner.ctx) else null else null;
        try replayQuestion(runner, maintenance.branch.hook(), live.logical_tokens, sources.question.ids);
        const provider_after = if (mode == .capture) if (runner.provider_snapshot_fn) |snapshot| snapshot(runner.ctx) else null else null;
        question_receipt.provider_delta = if (provider_before != null and provider_after != null) providerDelta(provider_before.?, provider_after.?) else null;
        try maintenance.seal();
        question_receipt.session_identity = maintenance.branch.identity;
        question_receipt.status_readback_bytes = maintenance.branch.status_readback_bytes;
        question_receipt.query_bytes = maintenance.branch.sizes.query_bytes;
        question_receipt.physical_bytes = maintenance.incremental_physical_bytes;
        question_receipt.peak_bytes = try add(try add(bytes_before, campaign_sizes.physical_bytes), maintenance.incremental_physical_bytes);
        question_receipt.replay_split_scratch_bytes = maintenance.branch.sizes.replay_split_scratch_bytes;
        const dispatches = maintenance.branch.attentionDispatchCounts();
        question_receipt.serial_attention_dispatches = dispatches.serial;
        question_receipt.split_attention_dispatches = dispatches.split;
        question_receipt.elapsed_ns = try timer.read();
        try campaign.absorbReplay(.question, &maintenance.branch);
    }

    const capture = observer != null;
    const fit_output: []i64 = if (capture) try allocator.alloc(i64, sources.fit.response_tokens) else &.{};
    defer if (capture) allocator.free(fit_output);
    var fit_receipt: CampaignBranchReceipt = undefined;
    {
        const timer = try CampaignTimer.start();
        const active = sourcesForBranch(all_sources, .fit);
        const additional = try std.math.add(u32, @intCast(sources.fit.ids.len), sources.fit.response_tokens);
        var maintenance = try live.beginReplayWithOptions(additional, branch_budget, .{ .query_sources = &active, .private_position_limit = sources.loaded_private_position_limit, .qualification_global_attention_split = replay_options.global_attention_split });
        defer maintenance.deinit();
        const receipt = try runner.study_fn(runner.ctx, allocator, maintenance.branch.hook(), sources.fit.ids, sources.fit.response_tokens, .{ .singleton_paged_decode = replay_options.study_singleton_paged_decode }, if (capture) fit_output else null);
        try validateStudyExecutionReceipt(receipt, sources.fit.response_tokens, .{ .singleton_paged_decode = replay_options.study_singleton_paged_decode });
        if (receipt.session_identity != maintenance.branch.identity or receipt.prefill_tokens != sources.fit.ids.len or receipt.generated_tokens != sources.fit.response_tokens)
            return error.InvalidStudyGeneration;
        if (capture) {
            const actual_digest = try tokenDigest(fit_output);
            if (!std.mem.eql(u8, &actual_digest, &receipt.generated_tokens_sha256))
                return error.InvalidStudyGeneration;
        }
        try maintenance.seal();
        try campaign.absorbReplay(.fit, &maintenance.branch);
        const dispatches = maintenance.branch.attentionDispatchCounts();
        fit_receipt = .{ .branch = .fit, .session_identity = receipt.session_identity, .prefix_ids = sources.fit.ids, .generated_ids = fit_output, .generated_tokens_sha256 = receipt.generated_tokens_sha256, .elapsed_ns = try timer.read(), .status_readback_bytes = maintenance.branch.status_readback_bytes, .query_bytes = maintenance.branch.sizes.query_bytes, .physical_bytes = maintenance.incremental_physical_bytes, .peak_bytes = try add(try add(try add(bytes_before, campaign_sizes.physical_bytes), maintenance.incremental_physical_bytes), fit_output_admitted_bytes), .replay_split_scratch_bytes = maintenance.branch.sizes.replay_split_scratch_bytes, .serial_attention_dispatches = dispatches.serial, .split_attention_dispatches = dispatches.split, .study_timing = receipt.study_timing, .provider_delta = receipt.provider_delta, .singleton_paged_decode_forwards = receipt.singleton_paged_decode_forwards, .singleton_paged_decode_expected_layer_dispatches = receipt.singleton_paged_decode_expected_layer_dispatches, .physical_branch_id = 1, .route = .fixed_serial_private_study };
    }

    const gate_output: []i64 = if (capture) try allocator.alloc(i64, sources.gate.response_tokens) else &.{};
    defer if (capture) allocator.free(gate_output);
    const gate_timer = try CampaignTimer.start();
    const active = sourcesForBranch(all_sources, .gate);
    const additional = try std.math.add(u32, @intCast(sources.gate.ids.len), sources.gate.response_tokens);
    var maintenance = try live.beginReplayWithOptions(additional, branch_budget, .{ .query_sources = &active, .private_position_limit = sources.loaded_private_position_limit, .qualification_global_attention_split = replay_options.global_attention_split });
    var gate_active = true;
    defer if (gate_active) maintenance.deinit();
    const receipt = try runner.study_fn(runner.ctx, allocator, maintenance.branch.hook(), sources.gate.ids, sources.gate.response_tokens, .{ .singleton_paged_decode = replay_options.study_singleton_paged_decode }, if (capture) gate_output else null);
    try validateStudyExecutionReceipt(receipt, sources.gate.response_tokens, .{ .singleton_paged_decode = replay_options.study_singleton_paged_decode });
    if (receipt.session_identity != maintenance.branch.identity or receipt.prefill_tokens != sources.gate.ids.len or receipt.generated_tokens != sources.gate.response_tokens)
        return error.InvalidStudyGeneration;
    if (capture) {
        const actual_digest = try tokenDigest(gate_output);
        if (!std.mem.eql(u8, &actual_digest, &receipt.generated_tokens_sha256))
            return error.InvalidStudyGeneration;
    }
    try maintenance.seal();
    try campaign.absorbReplay(.gate, &maintenance.branch);
    try campaign.seal();
    const gate_dispatches = maintenance.branch.attentionDispatchCounts();
    const gate_receipt = CampaignBranchReceipt{ .branch = .gate, .session_identity = receipt.session_identity, .prefix_ids = sources.gate.ids, .generated_ids = gate_output, .generated_tokens_sha256 = receipt.generated_tokens_sha256, .elapsed_ns = try gate_timer.read(), .status_readback_bytes = maintenance.branch.status_readback_bytes, .query_bytes = maintenance.branch.sizes.query_bytes, .physical_bytes = maintenance.incremental_physical_bytes, .peak_bytes = try add(try add(try add(bytes_before, campaign_sizes.physical_bytes), maintenance.incremental_physical_bytes), observer_output_admitted_bytes), .replay_split_scratch_bytes = maintenance.branch.sizes.replay_split_scratch_bytes, .serial_attention_dispatches = gate_dispatches.serial, .split_attention_dispatches = gate_dispatches.split, .study_timing = receipt.study_timing, .provider_delta = receipt.provider_delta, .singleton_paged_decode_forwards = receipt.singleton_paged_decode_forwards, .singleton_paged_decode_expected_layer_dispatches = receipt.singleton_paged_decode_expected_layer_dispatches, .physical_branch_id = 2, .route = .fixed_serial_private_study };
    // Capture has no publication transaction, so release the final private
    // branch before invoking the exporter. Production keeps it until the
    // transactional publish returns, matching Maintenance ownership.
    if (mode == .capture) {
        maintenance.deinit();
        gate_active = false;
    }
    const query_export_bytes = if (mode == .capture) try campaign.captureQueryExportBytes() else 0;
    const live_fingerprint_after = if (mode == .capture) try live.qualificationFingerprint() else null;
    if (mode == .capture and query_export_bytes != campaign_sizes.query_bytes)
        return error.InvalidQueryCampaignSelection;
    if (observer) |sink| try sink.observe_fn(sink.ctx, .{
        .campaign = &campaign,
        .question = question_receipt,
        .fit = fit_receipt,
        .gate = gate_receipt,
        .load_generation = load_generation,
        .live_epoch_before = epoch_before,
        .live_epoch_after = live.publication.epoch,
        .live_bytes_before = bytes_before,
        .live_bytes_after = live.metrics.live_bytes,
        .live_session_identity_before = live_identity_before,
        .live_session_identity_after = live.identity,
        .live_fingerprint_before = live_fingerprint_before,
        .live_fingerprint_after = live_fingerprint_after,
        .publication_requested = publication != null,
        .prompt_tokens = sources.question.prompt_tokens,
        .prompt_tokens_sha256 = sources.question.prompt_tokens_sha256,
        .question_source_start = sources.question.source_start,
        .question_source_end = sources.question.source_end,
        .campaign_query_bytes = campaign_sizes.query_bytes,
        .campaign_backend_bytes = campaign_sizes.backend_bytes,
        .campaign_host_bytes = campaign_sizes.host_bytes,
        .campaign_physical_bytes = campaign_sizes.physical_bytes,
        .query_export_bytes = query_export_bytes,
        .observer_output_admitted_bytes = observer_output_admitted_bytes,
        .replay_options = replay_options,
    });
    if (stored) |stored_run| {
        if (stored_run.reservation.reservation_identity == 0 or
            stored_run.reservation.artifact_count != 36 or
            stored_run.reservation.loader_budget_bytes == 0 or
            std.mem.allEqual(u8, &stored_run.reservation.algorithm_sha256, 0) or
            std.mem.allEqual(u8, &stored_run.reservation.build_sha256, 0) or
            std.mem.allEqual(u8, &stored_run.reservation.artifact_manifest_sha256, 0))
            return error.InvalidPayloadProvenance;
        const campaign_fingerprint = try campaign.storedDiagnosticFingerprint();
        const query_payload = try campaign.storedDiagnosticPayloadDigest();
        const query_content = try campaign.storedDiagnosticContentDigest();
        const identity = live_mod.StoredCampaignIdentity{
            .load_generation = load_generation,
            .session_identity = live.identity,
            .publication_epoch = live.publication.epoch,
            .query_campaign_identity = campaign.campaign_identity,
            .query_campaign_sha256 = campaign_fingerprint,
            .query_payload_sha256 = query_payload.sha256,
            .query_content_sha256 = query_content.sha256,
            .query_payload_readback_bytes = query_payload.readback_bytes,
            .source_descriptor_sha256 = stored_run.reservation.source_descriptor_sha256,
            .source_payload_sha256 = stored_run.reservation.source_payload_sha256,
        };
        try maintenance.bindStoredIdentityDiagnostic(&campaign, identity);
        const before_provider = live.ownerAllocationSnapshot();
        if (!before_provider.allocation.tracked_combined_physical_available)
            return error.ContextPhysicalAccountingUnavailable;
        try admitStoredProviderBeforeUpload(
            before_provider.allocation.tracked_combined_live_bytes,
            stored_run.reservation.materialization_host_bytes,
            stored_run.reservation.loader_budget_bytes,
            peak_budget,
        );
        const provider_before_materialize = live.allocation_tracker.snapshot();
        const materialization = try stored_run.provider.materialize_fn(stored_run.provider.ctx, identity, live.allocation_tracker);
        var materialization_active = true;
        defer if (materialization_active) {
            _ = materialization.deinit_fn(materialization.ctx) catch {};
        };
        const provider_after_materialize = live.allocation_tracker.snapshot();
        if (!provider_before_materialize.tracked_combined_physical_available or
            !provider_after_materialize.tracked_combined_physical_available or
            provider_before_materialize.owner_identity != provider_after_materialize.owner_identity or
            provider_after_materialize.tracked_combined_live_bytes < provider_before_materialize.tracked_combined_live_bytes or
            provider_after_materialize.tracked_combined_live_bytes - provider_before_materialize.tracked_combined_live_bytes != materialization.tracked_host_bytes or
            materialization.tracked_host_bytes == 0 or
            materialization.tracked_host_bytes > @as(u64, @intCast(stored_run.reservation.materialization_host_bytes)))
            return error.ContextPhysicalAccountingUnavailable;
        if (provider_after_materialize.tracked_combined_peak_bytes > @as(u64, @intCast(peak_budget)))
            return error.ContextStagingBudgetExceeded;
        if (materialization.artifacts.len != 36 or materialization.artifacts.len != stored_run.reservation.artifact_count)
            return error.InvalidPayloadProvenance;
        for (materialization.artifacts, stored_run.reservation.coordinates) |artifact, coordinate|
            if (!artifactMatchesCoordinate(artifact, coordinate)) return error.InvalidPayloadProvenance;
        const exact_loader_budget = try live_mod.storedLoaderBudgetDiagnostic(materialization.artifacts);
        if (exact_loader_budget.physical_upper_bytes != stored_run.reservation.loader_budget_bytes)
            return error.InvalidPayloadProvenance;
        const lease = live.config.execution_lease orelse return error.ContextExecutionPoolNotWarm;
        var loader = try live_mod.StoredLoader.init(live.allocation_tracker, lease, identity, materialization.artifacts, stored_run.reservation.loader_budget_bytes);
        defer if (!loader.released) {
            _ = loader.deinitChecked() catch {};
        };
        if (!std.meta.eql(loader.manifest_sha256, materialization.derived_manifest_sha256))
            return error.InvalidPayloadProvenance;
        // Loader now owns immutable tracked GPU copies. Release provider host
        // bytes before Batch admission so publication peak arithmetic cannot
        // omit a live host overlap; transaction identity and same-FD evidence
        // remain owned by the provider through precommit/ledger commit.
        try materialization.deinit_fn(materialization.ctx);
        materialization_active = false;
        const after_provider_release = live.allocation_tracker.snapshot();
        if (!after_provider_release.tracked_combined_physical_available or
            after_provider_release.owner_identity != provider_before_materialize.owner_identity)
            return error.ContextPhysicalAccountingUnavailable;
        if (after_provider_release.tracked_combined_peak_bytes > @as(u64, @intCast(peak_budget)))
            return error.ContextStagingBudgetExceeded;
        const published = try maintenance.publishStoredCampaignDiagnostic(
            &campaign,
            &loader,
            stored_run.eligible,
            stored_run.parts,
            stored_run.retained_limit,
            peak_budget,
            .{ .ctx = stored_run.provider.ctx, .verify_fn = stored_run.provider.precommit_fn },
        );
        // Crossing this assignment means Batch.commit already linearized the
        // replacement. Later receipt/ledger failure must never be described as
        // rollback or retried with the same held campaign.
        stored_run.publication.* = published;
        if (!std.meta.eql(published.loader_manifest_sha256, materialization.derived_manifest_sha256) or
            published.artifact_count != 36 or
            !std.meta.eql(published.query_payload_sha256, query_payload.sha256))
            return error.InvalidPayloadProvenance;
        _ = try loader.deinitChecked();
        const after_loader_release = live.allocation_tracker.snapshot();
        if (!after_loader_release.tracked_combined_physical_available or
            after_loader_release.owner_identity != provider_before_materialize.owner_identity)
            return error.ContextPhysicalAccountingUnavailable;
        if (after_loader_release.tracked_combined_peak_bytes > @as(u64, @intCast(peak_budget)))
            return error.ContextStagingBudgetExceeded;
    } else if (publication) |publish| {
        try maintenance.publishCampaignWithFault(&campaign, publish.eligible, publish.parts, publish.retained_limit, peak_budget, runner.fault_controller);
    } else if (live.publication.epoch != epoch_before or live.metrics.live_bytes != bytes_before) {
        return error.InvalidContextTransaction;
    }
}

/// Preparation-time authority validation. Retention-plan construction remains
/// factored with preparePlan in the landing patch so existing Request users do
/// not change type; legacy reference replay is never constructed for this arm.
pub fn validateCampaignPreparation(
    prompt: []const i64,
    sources: PreparedQuerySources,
    authority: CampaignAuthority,
    actual: profiles.Identity,
    layout: @import("../runtime/context_policy.zig").Layout,
    output_tokens: u32,
) !void {
    const protocols = switch (authority) {
        .production => |production| blk: {
            const artifact_digest = try profiles.profileArtifactDigest(production.profile);
            const semantic_digest = production.profile.semanticDigest();
            if (!std.meta.eql(production.selected.identity, actual) or production.selected.load_generation == 0 or
                production.selected.loaded_position_limit != sources.loaded_private_position_limit or
                !std.mem.eql(u8, &production.selected.profile_artifact_sha256, &artifact_digest) or
                !std.mem.eql(u8, &production.selected.profile_semantic_sha256, &semantic_digest))
                return error.UnqualifiedCompactionArtifact;
            try production.profile.validate(actual, layout, prompt.len, output_tokens);
            break :blk production.profile.query_protocols;
        },
        .capture => |capture| blk: {
            if (!@import("builtin").is_test or capture.load_generation == 0 or
                capture.loaded_position_limit != sources.loaded_private_position_limit or
                !std.meta.eql(capture.identity, actual)) return error.UnqualifiedCompactionArtifact;
            break :blk capture.protocols;
        },
    };
    try validatePreparedSources(prompt, sources, protocols, layout);
}

fn copyPreparedSources(a: std.mem.Allocator, input: PreparedQuerySources) !PreparedQuerySources {
    return .{
        .question = .{
            .ids = try a.dupe(i64, input.question.ids),
            .source_start = input.question.source_start,
            .source_end = input.question.source_end,
            .prompt_tokens = input.question.prompt_tokens,
            .prompt_tokens_sha256 = input.question.prompt_tokens_sha256,
        },
        .fit = .{
            .ids = try a.dupe(i64, input.fit.ids),
            .template_bytes = try a.dupe(u8, input.fit.template_bytes),
            .generation_contract_bytes = try a.dupe(u8, input.fit.generation_contract_bytes),
            .prompt_tokens = input.fit.prompt_tokens,
            .prompt_tokens_sha256 = input.fit.prompt_tokens_sha256,
            .response_tokens = input.fit.response_tokens,
        },
        .gate = .{
            .ids = try a.dupe(i64, input.gate.ids),
            .template_bytes = try a.dupe(u8, input.gate.template_bytes),
            .generation_contract_bytes = try a.dupe(u8, input.gate.generation_contract_bytes),
            .prompt_tokens = input.gate.prompt_tokens,
            .prompt_tokens_sha256 = input.gate.prompt_tokens_sha256,
            .response_tokens = input.gate.response_tokens,
        },
        .loaded_private_position_limit = input.loaded_private_position_limit,
    };
}

fn branchReplayConfig(config: live_mod.Config, sources: PreparedQuerySources, branch: query_campaign.Branch) !branch_mod.Config {
    const all = try campaignSources(sources);
    const active = sourcesForBranch(all, branch);
    const additional: u32 = switch (branch) {
        .question => @intCast(sources.question.ids.len),
        .fit => try std.math.add(u32, @intCast(sources.fit.ids.len), sources.fit.response_tokens),
        .gate => try std.math.add(u32, @intCast(sources.gate.ids.len), sources.gate.response_tokens),
    };
    // `query_sources` is assigned by the caller from a stable arena copy. This
    // helper is used only for sizing; returning a pointer to `active` would be
    // invalid, so the branch loop constructs the final Config inline.
    _ = active;
    return .{
        .plan = try (@import("../runtime/context_policy.zig").Policy{}).admit(sources.loaded_private_position_limit, null, config.plan.prompt_tokens, additional, 0),
        .layout = config.layout,
        .max_query_rows = config.max_query_rows,
        .physical_budget_bytes = 0,
    };
}

pub const CampaignEstimate = struct {
    chunk_rows: u32,
    backend_peak_bytes: usize,
    host_peak_bytes: usize,
    execution_peak_bytes: usize,
    combined_peak_bytes: usize,
    request_bytes: usize,
    retained_limit: u32,
    saved_bytes: usize,
    campaign_bytes: usize,
    largest_private_branch_bytes: usize,
};

pub const PreparedCampaignPlan = struct {
    arena: *std.heap.ArenaAllocator,
    config: live_mod.Config,
    prompt: []const i64,
    eligible: payload.Eligible,
    sources: PreparedQuerySources,
    load_generation: u64,
    parts: []const []const storage.Segment,
    retained_limit: u32,
    staged_bytes: usize,
    staged_device: usize,
    staged_host: usize,
    old_device: usize,
    workspace: usize,
    minimum_saved_bytes: usize,
    transferred: bool = false,

    pub fn deinit(self: *PreparedCampaignPlan) void {
        if (!self.transferred) {
            const parent = self.arena.child_allocator;
            self.arena.deinit();
            parent.destroy(self.arena);
        }
        self.* = undefined;
    }

    fn configForChunk(self: *const PreparedCampaignPlan, chunk_rows: usize) !live_mod.Config {
        var config = self.config;
        config.max_query_rows = std.math.cast(u32, @max(chunk_rows, 1)) orelse return error.ResourceLimitExceeded;
        return config;
    }

    pub fn estimate(self: *const PreparedCampaignPlan, chunk_rows: usize) !CampaignEstimate {
        if (self.transferred) return error.InvalidContextTransaction;
        const config = try self.configForChunk(chunk_rows);
        const original = try live_mod.allocation(config);
        if (self.staged_bytes >= original.global_bytes or self.staged_device >= self.old_device or
            original.global_bytes - self.staged_bytes < self.minimum_saved_bytes or
            self.old_device - self.staged_device < self.minimum_saved_bytes)
            return error.ContextAllocationNotReduced;
        const all_sources = try campaignSources(self.sources);
        const campaign = try query_campaign.Campaign.allocation(config.layout, &all_sources);
        const original_backend = try originalBackendBytes(config, original);
        if (original_backend > original.physical_bytes) return error.InvalidContextLayout;
        const original_host = original.physical_bytes - original_backend;
        var replay_control_bytes: ?usize = null;
        var largest_branch: usize = 0;
        var replay_backend: usize = 0;
        var replay_host: usize = 0;
        var gate_sealed_backend: usize = 0;
        var gate_sealed_host: usize = 0;
        inline for (.{ query_campaign.Branch.question, query_campaign.Branch.fit, query_campaign.Branch.gate }) |branch| {
            const active = sourcesForBranch(all_sources, branch);
            var branch_config = try branchReplayConfig(config, self.sources, branch);
            branch_config.query_sources = &active;
            const branch_size = try branch_mod.estimate(branch_config);
            const tiers = try replayTierBytes(branch_config, branch_size);
            if (replay_control_bytes) |expected| {
                if (branch_size.owner_control_bytes != expected) return error.InvalidContextLayout;
            } else {
                replay_control_bytes = branch_size.owner_control_bytes;
            }
            if (tiers.peak_host < branch_size.owner_control_bytes) return error.InvalidContextLayout;
            const incremental_branch = try std.math.sub(usize, branch_size.physical_bytes, branch_size.owner_control_bytes);
            const incremental_host = try std.math.sub(usize, tiers.peak_host, branch_size.owner_control_bytes);
            largest_branch = @max(largest_branch, incremental_branch);
            replay_backend = @max(replay_backend, tiers.peak_backend);
            replay_host = @max(replay_host, incremental_host);
            if (branch == .gate) {
                gate_sealed_backend = tiers.sealed_backend;
                gate_sealed_host = tiers.sealed_host;
            }
        }
        const controls = replay_control_bytes orelse return error.InvalidContextLayout;
        const request_host = try requestOwnerPreparationBytes(config.owner_allocation_tracker, self.arena);
        const replay_peak_backend = try add(try add(original_backend, campaign.backend_bytes), replay_backend);
        var replay_peak_host = try add(original_host, controls);
        replay_peak_host = try add(try add(try add(replay_peak_host, campaign.host_bytes), replay_host), request_host);
        var publication_backend = try add(try add(original_backend, campaign.backend_bytes), gate_sealed_backend);
        publication_backend = try add(publication_backend, self.staged_device);
        publication_backend = try add(publication_backend, self.workspace);
        publication_backend = try add(publication_backend, std.heap.pageSize());
        var publication_host = try add(try add(try add(original_host, controls), campaign.host_bytes), gate_sealed_host);
        publication_host = try add(publication_host, self.staged_host);
        publication_host = try add(publication_host, try rounded(try mul(self.parts.len, @sizeOf(storage.Generation))));
        publication_host = try add(publication_host, request_host);
        const backend = @max(replay_peak_backend, publication_backend);
        const host = @max(replay_peak_host, publication_host);
        var publication_execution = try add(try add(original.physical_bytes, controls), campaign.physical_bytes);
        publication_execution = try add(publication_execution, try add(gate_sealed_backend, gate_sealed_host));
        publication_execution = try add(publication_execution, self.staged_bytes);
        publication_execution = try add(publication_execution, self.workspace);
        publication_execution = try add(publication_execution, std.heap.pageSize());
        publication_execution = try add(publication_execution, try rounded(try mul(self.parts.len, @sizeOf(storage.Generation))));
        return .{
            .chunk_rows = config.max_query_rows,
            .backend_peak_bytes = backend,
            .host_peak_bytes = host,
            .execution_peak_bytes = @max(try add(try add(try add(original.physical_bytes, controls), campaign.physical_bytes), largest_branch), publication_execution),
            .combined_peak_bytes = try add(backend, host),
            .request_bytes = request_host,
            .retained_limit = self.retained_limit,
            .saved_bytes = @min(original.global_bytes - self.staged_bytes, self.old_device - self.staged_device),
            .campaign_bytes = campaign.physical_bytes,
            .largest_private_branch_bytes = largest_branch,
        };
    }

    pub fn finalize(self: *PreparedCampaignPlan, chunk_rows: usize, backend_budget: usize, host_budget: usize) !CampaignRequest {
        const sizes = try self.estimate(chunk_rows);
        if (sizes.backend_peak_bytes > backend_budget or sizes.host_peak_bytes > host_budget)
            return error.ContextStagingBudgetExceeded;
        self.transferred = true;
        return .{
            .arena = self.arena,
            .config = try self.configForChunk(chunk_rows),
            .prompt = self.prompt,
            .eligible = self.eligible,
            .sources = self.sources,
            .load_generation = self.load_generation,
            .parts = self.parts,
            .retained_limit = self.retained_limit,
            .backend_peak_bytes = sizes.backend_peak_bytes,
            .host_peak_bytes = sizes.host_peak_bytes,
            .execution_peak_bytes = sizes.execution_peak_bytes,
            .peak_bytes = sizes.combined_peak_bytes,
            .request_bytes = sizes.request_bytes,
            .saved_bytes = sizes.saved_bytes,
        };
    }
};

/// Same observable sizing/config surface as Request, allowing the generation
/// integration to switch only its preparation/run dispatch. This type carries
/// no legacy reference.Plan and production has no non-campaign run method.
pub const CampaignRequest = struct {
    arena: *std.heap.ArenaAllocator,
    config: live_mod.Config,
    prompt: []const i64,
    eligible: payload.Eligible,
    sources: PreparedQuerySources,
    load_generation: u64,
    parts: []const []const storage.Segment,
    retained_limit: u32,
    backend_peak_bytes: usize,
    host_peak_bytes: usize,
    execution_peak_bytes: usize,
    peak_bytes: usize,
    request_bytes: usize,
    saved_bytes: usize,
    consumed: bool = false,
    publication_attempted: bool = false,
    published: bool = false,
    committed_before_bytes: usize = 0,
    committed_after_bytes: usize = 0,
    backend_kv_readback_before: u64 = 0,
    backend_kv_readback_after: u64 = 0,

    pub fn deinit(self: *CampaignRequest) void {
        const parent = self.arena.child_allocator;
        self.arena.deinit();
        parent.destroy(self.arena);
        self.* = undefined;
    }

    pub fn run(self: *CampaignRequest, live: *live_mod.Session, runner: CampaignRunner) !void {
        if (self.consumed or live.logical_tokens != self.prompt.len or !std.meta.eql(live.config.plan, self.config.plan) or
            !sameLayout(live.config.layout, self.config.layout) or live.config.max_query_rows != self.config.max_query_rows or
            live.config.optimize_decode != self.config.optimize_decode or live.config.optimize_prefill != self.config.optimize_prefill)
            return error.InvalidContextTransaction;
        self.consumed = true;
        self.publication_attempted = true;
        // Committed accounting includes the replay-control residency that the
        // campaign will retain on the live session after its branches close.
        _ = try live.ensureReplayControls(self.execution_peak_bytes);
        self.committed_before_bytes = live.metrics.live_bytes;
        self.committed_after_bytes = live.metrics.live_bytes;
        self.backend_kv_readback_before = live.metrics.kv_readback_bytes;
        self.backend_kv_readback_after = live.metrics.kv_readback_bytes;
        defer {
            self.committed_after_bytes = live.metrics.live_bytes;
            self.backend_kv_readback_after = live.metrics.kv_readback_bytes;
        }
        try runQueryCampaign(live, self.sources, self.load_generation, runner, null, .{
            .eligible = &self.eligible,
            .parts = self.parts,
            .retained_limit = self.retained_limit,
        }, null, self.execution_peak_bytes, .production, .{});
        self.published = true;
    }
};

/// Test-only raw-evidence entry point. Callers must first validate the capture
/// authority with validateCampaignPreparation(.capture); no profile, retention
/// decision, or publication object is accepted by this API.
pub fn runCampaignCapture(
    live: *live_mod.Session,
    prompt: []const i64,
    sources: PreparedQuerySources,
    authority: CampaignAuthority,
    actual: profiles.Identity,
    runner: CampaignRunner,
    observer: CampaignObserver,
    peak_budget: usize,
) !void {
    return runCampaignCaptureWithOptions(live, prompt, sources, authority, actual, runner, observer, peak_budget, .{});
}

/// Explicit test-only A/B entry point. No production request can carry these
/// controls, and no selected runtime policy is inferred from its measurements.
pub fn runCampaignCaptureWithOptions(
    live: *live_mod.Session,
    prompt: []const i64,
    sources: PreparedQuerySources,
    authority: CampaignAuthority,
    actual: profiles.Identity,
    runner: CampaignRunner,
    observer: CampaignObserver,
    peak_budget: usize,
    replay_options: CampaignCaptureReplayOptions,
) !void {
    if (!@import("builtin").is_test) return error.UnqualifiedCompactionArtifact;
    const capture = switch (authority) {
        .capture => |value| value,
        .production => return error.UnqualifiedCompactionArtifact,
    };
    if (prompt.len != live.logical_tokens) return error.InvalidContextTransaction;
    try validateCampaignPreparation(prompt, sources, authority, actual, live.config.layout, 0);
    return runQueryCampaign(live, sources, capture.load_generation, runner, observer, null, null, peak_budget, .capture, replay_options);
}

fn updateCoordinateDigest(hash: *std.crypto.hash.sha2.Sha256, coordinate: StoredDiagnosticCoordinate) void {
    inline for (.{ coordinate.owner_layer, coordinate.head, coordinate.span, coordinate.logical_start, coordinate.logical_count, coordinate.retained_rows, coordinate.head_dim }) |value| {
        var bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &bytes, value, .little);
        hash.update(&bytes);
    }
}

fn coordinateLess(a: StoredDiagnosticCoordinate, b: StoredDiagnosticCoordinate) bool {
    inline for (.{ "owner_layer", "head", "span", "logical_start" }) |field| {
        const av = @field(a, field);
        const bv = @field(b, field);
        if (av != bv) return av < bv;
    }
    return false;
}

pub fn storedDiagnosticProfileDigestDiagnostic(coordinates: []const StoredDiagnosticCoordinate) ![32]u8 {
    if (!@import("builtin").is_test) return error.UnqualifiedCompactionArtifact;
    if (coordinates.len != 36) return error.InvalidPayloadProvenance;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly.context.stored-profile.v1\x00");
    var owner_count: u32 = 0;
    var group_count: u32 = 0;
    var owner_retained: u32 = 0;
    var owner_dim: u32 = 0;
    for (coordinates, 0..) |coordinate, index| {
        if (coordinate.logical_count == 0 or coordinate.retained_rows == 0 or
            coordinate.retained_rows >= coordinate.logical_count or coordinate.head_dim == 0)
            return error.InvalidPayloadProvenance;
        if (index > 0 and !coordinateLess(coordinates[index - 1], coordinate))
            return error.InvalidPayloadProvenance;
        if (index == 0 or coordinate.owner_layer != coordinates[index - 1].owner_layer) {
            if (index > 0 and group_count != 12) return error.InvalidPayloadProvenance;
            owner_count += 1;
            group_count = 0;
            owner_retained = coordinate.retained_rows;
            owner_dim = coordinate.head_dim;
        } else if (coordinate.retained_rows != owner_retained or coordinate.head_dim != owner_dim) {
            // Current native Profile/Coordinator supports owner-level retained
            // rows only. Per-block fallback is deliberately rejected.
            return error.InvalidPayloadProvenance;
        }
        group_count += 1;
        updateCoordinateDigest(&hash, coordinate);
    }
    if (owner_count != 3 or group_count != 12) return error.InvalidPayloadProvenance;
    return hash.finalResult();
}

fn validateStoredDiagnosticProfile(coordinates: []const StoredDiagnosticCoordinate, expected: [32]u8) !void {
    if (!std.meta.eql(try storedDiagnosticProfileDigestDiagnostic(coordinates), expected))
        return error.InvalidPayloadProvenance;
}

fn artifactMatchesCoordinate(artifact: live_mod.StoredArtifact, coordinate: StoredDiagnosticCoordinate) bool {
    return artifact.owner_layer == coordinate.owner_layer and artifact.head == coordinate.head and
        artifact.span == coordinate.span and artifact.logical_start == coordinate.logical_start and
        artifact.logical_count == coordinate.logical_count and artifact.retained_rows == coordinate.retained_rows and
        artifact.head_dim == coordinate.head_dim;
}

fn requireTwentyFivePercentContextReduction(baseline: u64, stored: u64) !void {
    if (baseline == 0 or try std.math.mul(u64, stored, 4) > try std.math.mul(u64, baseline, 3))
        return error.ContextAllocationNotReduced;
}

fn closeFailedStoredReservation(provider: StoredDiagnosticProvider, reservation: StoredDiagnosticReservation, terminal: *StoredDiagnosticReceipt) !void {
    const failure = provider.abort_reservation_fn(provider.ctx, reservation.reservation_identity) catch {
        terminal.state = .ledger_failure;
        return error.StoredDiagnosticLedgerFailure;
    };
    if (failure.reservation_identity != reservation.reservation_identity or !failure.durable or
        !failure.nonreuse_committed or std.mem.allEqual(u8, &failure.evidence_sha256, 0))
    {
        terminal.state = .ledger_failure;
        return error.StoredDiagnosticLedgerFailure;
    }
    terminal.failure = failure;
    terminal.state = .failed_before_commit;
}

test "stored diagnostic physical target uses exact integer boundary and rejects overflow" {
    try requireTwentyFivePercentContextReduction(400, 300);
    try std.testing.expectError(error.ContextAllocationNotReduced, requireTwentyFivePercentContextReduction(400, 301));
    try std.testing.expectError(error.Overflow, requireTwentyFivePercentContextReduction(std.math.maxInt(u64), std.math.maxInt(u64)));
}

/// Test-only all-36 diagnostic coordinator. `native_peak_budget` is the
/// separately declared installed-model publication bound; it is not the 1 GiB
/// scientific fitting-process cap. Phase A must already have frozen recipe,
/// build and ordered artifacts before reserve_fn may generate fresh held data.
/// The provider's exclusive reservation rejects historical or concurrent
/// generator/corpus/row partitions; a new campaign nonce alone is insufficient.
pub fn runStoredCampaignDiagnostic(
    live: *live_mod.Session,
    prompt: []const i64,
    sources: PreparedQuerySources,
    authority: CampaignAuthority,
    actual: profiles.Identity,
    runner: CampaignRunner,
    provider: StoredDiagnosticProvider,
    eligible: *const payload.Eligible,
    parts: []const []const storage.Segment,
    retained_limit: u32,
    native_peak_budget: usize,
    terminal: *StoredDiagnosticReceipt,
) !void {
    if (!@import("builtin").is_test) return error.UnqualifiedCompactionArtifact;
    terminal.* = .{};
    errdefer {
        if (terminal.state == .prepared) terminal.state = .failed_before_commit;
    }
    const capture = switch (authority) {
        .capture => |value| value,
        .production => return error.UnqualifiedCompactionArtifact,
    };
    if (prompt.len != live.logical_tokens) return error.InvalidContextTransaction;
    try validateCampaignPreparation(prompt, sources, authority, actual, live.config.layout, 0);
    const baseline_storage = try live.committedContextStorageDiagnostic();
    if (baseline_storage.tracker_snapshot.tracked_combined_peak_bytes > @as(u64, @intCast(native_peak_budget)))
        return error.ContextStagingBudgetExceeded;
    const reservation = try provider.reserve_fn(provider.ctx);
    terminal.* = .{
        .reservation_identity = reservation.reservation_identity,
        .algorithm_sha256 = reservation.algorithm_sha256,
        .build_sha256 = reservation.build_sha256,
        .artifact_manifest_sha256 = reservation.artifact_manifest_sha256,
        .profile_sha256 = reservation.profile_sha256,
        .baseline_request_context_physical_bytes = baseline_storage.physical_bytes,
        .baseline_owner_count = baseline_storage.total_owner_count,
        .baseline_global_owner_count = baseline_storage.global_owner_count,
        .baseline_total_owner_count = baseline_storage.total_owner_count,
        .tracker_live_bytes_before = baseline_storage.tracker_snapshot.tracked_combined_live_bytes,
        .tracker_peak_bytes_before = baseline_storage.tracker_snapshot.tracked_combined_peak_bytes,
        .replay_control_bytes_before = baseline_storage.replay_control_bytes,
    };
    if (reservation.reservation_identity == 0 or reservation.artifact_count != 36 or
        reservation.expected_baseline_context_bytes != baseline_storage.physical_bytes or
        reservation.expected_owner_count != baseline_storage.global_owner_count or
        reservation.expected_total_owner_count != baseline_storage.total_owner_count or baseline_storage.replay_control_bytes != 0)
    {
        closeFailedStoredReservation(provider, reservation, terminal) catch return error.StoredDiagnosticLedgerFailure;
        return error.InvalidPayloadProvenance;
    }
    validateStoredDiagnosticProfile(reservation.coordinates, reservation.profile_sha256) catch |err| {
        closeFailedStoredReservation(provider, reservation, terminal) catch return error.StoredDiagnosticLedgerFailure;
        return err;
    };
    _ = live.ensureReplayControls(native_peak_budget) catch |err| {
        closeFailedStoredReservation(provider, reservation, terminal) catch return error.StoredDiagnosticLedgerFailure;
        return err;
    };
    var publication: ?live_mod.StoredPublicationReceipt = null;
    runQueryCampaign(live, sources, capture.load_generation, runner, null, null, .{
        .provider = provider,
        .reservation = reservation,
        .eligible = eligible,
        .parts = parts,
        .retained_limit = retained_limit,
        .publication = &publication,
    }, native_peak_budget, .stored_diagnostic, .{}) catch |err| {
        if (publication) |committed| {
            terminal.publication = committed;
            terminal.state = .committed_but_receipt_failed;
            // Batch.commit already linearized publication. Persist that
            // irreversible outcome even when a later in-run teardown proof
            // fails; it must never remain only a prepared ledger entry.
            provider.commit_reservation_fn(provider.ctx, reservation.reservation_identity, committed) catch
                return error.StoredDiagnosticLedgerFailure;
        } else {
            closeFailedStoredReservation(provider, reservation, terminal) catch return error.StoredDiagnosticLedgerFailure;
        }
        return err;
    };
    const committed = publication orelse return error.InvalidContextTransaction;
    // Batch publication and fresh-held consumption are irreversible even if
    // later physical-target or durable-ledger evidence fails.
    terminal.publication = committed;
    provider.commit_reservation_fn(provider.ctx, reservation.reservation_identity, committed) catch |err| {
        terminal.state = .committed_but_receipt_failed;
        return err;
    };
    const stored_storage = live.committedContextStorageDiagnostic() catch |err| {
        terminal.state = .committed_but_receipt_failed;
        return err;
    };
    terminal.stored_request_context_physical_bytes = stored_storage.physical_bytes;
    terminal.stored_owner_count = stored_storage.total_owner_count;
    terminal.stored_global_owner_count = stored_storage.global_owner_count;
    terminal.stored_total_owner_count = stored_storage.total_owner_count;
    terminal.tracker_live_bytes_after = stored_storage.tracker_snapshot.tracked_combined_live_bytes;
    terminal.tracker_peak_bytes_after = stored_storage.tracker_snapshot.tracked_combined_peak_bytes;
    terminal.replay_control_bytes_after = stored_storage.replay_control_bytes;
    if (stored_storage.global_owner_count != baseline_storage.global_owner_count or
        stored_storage.total_owner_count != baseline_storage.total_owner_count or stored_storage.replay_control_bytes != 0 or
        stored_storage.tracker_snapshot.tracked_combined_peak_bytes > @as(u64, @intCast(native_peak_budget)))
    {
        terminal.state = .committed_but_receipt_failed;
        return error.InvalidContextLayout;
    }
    requireTwentyFivePercentContextReduction(baseline_storage.physical_bytes, stored_storage.physical_bytes) catch |err| {
        terminal.state = .committed_but_receipt_failed;
        return err;
    };
    terminal.state = .committed;
}

test "stored all36 coordinator entrypoint rejects production authority" {
    var terminal: StoredDiagnosticReceipt = .{};
    try std.testing.expectError(error.UnqualifiedCompactionArtifact, runStoredCampaignDiagnostic(
        undefined,
        &.{},
        undefined,
        .{ .production = undefined },
        undefined,
        undefined,
        undefined,
        undefined,
        &.{},
        0,
        1,
        &terminal,
    ));
    try std.testing.expectEqual(.failed_before_commit, terminal.state);
    try std.testing.expectEqual(@as(u64, 0), terminal.reservation_identity);
    try std.testing.expect(terminal.publication == null and terminal.failure == null);
}

/// Production preparation entry point. The landing patch factors the shared
/// eligibility/segment builder with legacy preparePlan; this implementation
/// intentionally constructs no reference.Plan and therefore cannot silently
/// fall back to the old single-replay sampler.
pub fn prepareCampaignPlan(
    parent: std.mem.Allocator,
    config: live_mod.Config,
    ids: []const i64,
    eligible: *const payload.Eligible,
    query_sources: PreparedQuerySources,
    selected: SelectedCampaignAuthority,
    profile: profiles.Profile,
    actual: profiles.Identity,
    retained_limit: u32,
) !PreparedCampaignPlan {
    if (config.plan.prompt_tokens >= config.plan.logical_token_limit or config.plan.logical_token_limit > std.math.maxInt(u32))
        return error.InvalidPayloadProvenance;
    const output: u32 = std.math.cast(u32, config.plan.logical_token_limit - config.plan.prompt_tokens) orelse
        return error.UnsupportedCompactionProfile;
    try validateCampaignPreparation(ids, query_sources, .{ .production = .{ .selected = selected, .profile = profile, .retained_limit = retained_limit } }, actual, config.layout, output);
    if (ids.len != config.plan.prompt_tokens or ids.len != eligible.labels.len or eligible.blocks.len == 0)
        return error.InvalidPayloadProvenance;
    const inputs = try prepareRetentionInputs(parent, ids, eligible, profile.owners.len);
    errdefer {
        inputs.arena.deinit();
        parent.destroy(inputs.arena);
    }
    const a = inputs.arena.allocator();
    const copied_sources = try copyPreparedSources(a, query_sources);
    const all_sources = try campaignSources(copied_sources);
    const question_sources = sourcesForBranch(all_sources, .question);
    const fit_sources = sourcesForBranch(all_sources, .fit);
    const gate_sources = sourcesForBranch(all_sources, .gate);
    const geometries = try a.alloc(RetentionFitGeometry, profile.owners.len);
    for (profile.owners, profile.query_protocols, geometries) |owner, authority, *geometry| {
        const l = config.layout.layers[owner.layer];
        if (authority.strata.len % @as(usize, l.kv_heads) != 0) return error.CompactionQueryProtocolMismatch;
        const strata_per_head: u32 = @intCast(authority.strata.len / @as(usize, l.kv_heads));
        const question = try query_protocol.Plan.init(&question_sources, strata_per_head);
        const fit = try query_protocol.Plan.init(&fit_sources, strata_per_head);
        const gate = try query_protocol.Plan.init(&gate_sources, strata_per_head);
        if (question.training_per_source_stratum != fit.training_per_source_stratum or
            question.training_per_source_stratum != gate.training_per_source_stratum or
            question.validation_per_source_stratum != fit.validation_per_source_stratum or
            question.validation_per_source_stratum != gate.validation_per_source_stratum)
            return error.QueryProtocolQuotaDrift;
        geometry.* = .{
            .training_rows = try std.math.add(u32, question.training_rows, fit.training_rows),
            .validation_rows = gate.validation_rows,
            .validation_group_rows = try std.math.mul(u32, 2, gate.validation_per_source_stratum),
        };
    }
    const sizing = try buildRetention(config, profile, retained_limit, output, inputs, geometries);
    return .{
        .arena = inputs.arena,
        .config = config,
        .prompt = inputs.prompt,
        .eligible = inputs.eligible,
        .sources = copied_sources,
        .load_generation = selected.load_generation,
        .parts = inputs.parts,
        .retained_limit = retained_limit,
        .staged_bytes = sizing.staged_bytes,
        .staged_device = sizing.staged_device,
        .staged_host = sizing.staged_host,
        .old_device = sizing.old_device,
        .workspace = sizing.workspace,
        .minimum_saved_bytes = profile.minimum_saved_bytes,
    };
}

test "context campaign preparation binds prompt question studies and private position limit" {
    const prompt = [_]i64{ 2, 3, 5, 7 };
    const digest = try tokenDigest(&prompt);
    const sources = PreparedQuerySources{
        .question = .{ .ids = prompt[2..4], .source_start = 2, .source_end = 4, .prompt_tokens = 4, .prompt_tokens_sha256 = digest },
        .fit = .{ .ids = &.{ 11, 13 }, .template_bytes = "fit", .generation_contract_bytes = "contract", .prompt_tokens = 4, .prompt_tokens_sha256 = digest, .response_tokens = 3 },
        .gate = .{ .ids = &.{17}, .template_bytes = "gate", .generation_contract_bytes = "contract", .prompt_tokens = 4, .prompt_tokens_sha256 = digest, .response_tokens = 2 },
        .loaded_private_position_limit = 9,
    };
    const ranges = try campaignSources(sources);
    try std.testing.expectEqual(@as(u32, 4), ranges[0].start);
    try std.testing.expectEqual(@as(u32, 6), ranges[0].end);
    try std.testing.expectEqual(@as(u32, 6), ranges[1].end);
    try std.testing.expectEqual(@as(u32, 9), ranges[2].end);
    try std.testing.expectEqual(@as(u32, 5), ranges[3].end);
    try std.testing.expectEqual(@as(u32, 7), ranges[4].end);
    try validateSourceBounds(&prompt, sources);
    var bad_question = sources;
    bad_question.question.ids = prompt[1..3];
    try std.testing.expectError(error.InvalidPayloadProvenance, validateSourceBounds(&prompt, bad_question));
    var bad_digest = sources;
    bad_digest.fit.prompt_tokens_sha256[0] ^= 1;
    try std.testing.expectError(error.InvalidPayloadProvenance, validateSourceBounds(&prompt, bad_digest));
    var too_small = sources;
    too_small.loaded_private_position_limit = 6;
    try std.testing.expectError(error.ContextReplayBudgetExceeded, validateSourceBounds(&prompt, too_small));
    var zero_limit = sources;
    zero_limit.loaded_private_position_limit = 0;
    try std.testing.expectError(error.InvalidQueryProtocol, validateSourceBounds(&prompt, zero_limit));
    var excessive_limit = sources;
    excessive_limit.loaded_private_position_limit = 131073;
    try std.testing.expectError(error.InvalidQueryProtocol, validateSourceBounds(&prompt, excessive_limit));
    var invalid_range = sources;
    invalid_range.question.source_end = 5;
    try std.testing.expectError(error.InvalidPayloadProvenance, validateSourceBounds(&prompt, invalid_range));
}

test "context request rejects split decode mismatch before replay mutation" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var config: live_mod.Config = std.mem.zeroes(live_mod.Config);
    config.max_query_rows = 1;
    config.layout.len = 1;
    var request: Request = undefined;
    request.config = config;
    request.prompt = &.{};
    request.consumed = false;

    var live: live_mod.Session = undefined;
    live.config = config;
    live.config.optimize_decode = true;
    live.logical_tokens = 0;
    live.publication = .{ .epoch = 17, .issued_epoch = 17 };
    live.split_decode_dispatches = 9;

    const Runner = struct {
        fn run(ctx: *anyopaque, _: contract.Hook, _: usize, _: []const i64) !void {
            const calls: *usize = @ptrCast(@alignCast(ctx));
            calls.* += 1;
        }
    };
    var calls: usize = 0;
    try std.testing.expectError(error.InvalidContextTransaction, request.run(&live, &calls, Runner.run, false));
    try std.testing.expect(!request.consumed);
    try std.testing.expectEqual(@as(usize, 0), calls);
    try std.testing.expectEqual(@as(u64, 17), live.publication.epoch);
    try std.testing.expectEqual(@as(u64, 9), live.split_decode_dispatches);
}

test "context request rejects flash prefill mismatch before replay mutation" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var config: live_mod.Config = std.mem.zeroes(live_mod.Config);
    config.max_query_rows = 32;
    config.layout.len = 1;
    var request: Request = undefined;
    request.config = config;
    request.prompt = &.{};
    request.consumed = false;

    var live: live_mod.Session = undefined;
    live.config = config;
    live.config.optimize_prefill = true;
    live.logical_tokens = 0;
    live.publication = .{ .epoch = 23, .issued_epoch = 23 };
    live.split_decode_dispatches = 0;

    const Runner = struct {
        fn run(ctx: *anyopaque, _: contract.Hook, _: usize, _: []const i64) !void {
            const calls: *usize = @ptrCast(@alignCast(ctx));
            calls.* += 1;
        }
    };
    var calls: usize = 0;
    try std.testing.expectError(error.InvalidContextTransaction, request.run(&live, &calls, Runner.run, false));
    try std.testing.expect(!request.consumed);
    try std.testing.expectEqual(@as(usize, 0), calls);
    try std.testing.expectEqual(@as(u64, 23), live.publication.epoch);
}
