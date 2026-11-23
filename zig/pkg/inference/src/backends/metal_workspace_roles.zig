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

//! Copied role requirements from the ACTUAL selected command plan. This value
//! does not certify a dispatch or grant generic retained-workspace credit.
//! Concrete Metal capture supplies a size/alignment preflight for EVERY fresh
//! role and pins exact known Root metadata while still under the actual owner.
//! Supplier admission and destruction of this value occur outside all lanes.
const std = @import("std");
const commands = @import("../graph/metal_command_planner.zig");
const ownership = @import("metal_workspace_ownership.zig");
const backing = @import("../execution/backing_retirement.zig");

pub const geometry = @import("metal_workspace_geometry.zig");

pub const capacity = 32; // TERMITE_METAL_GRAPH_PLAN_SLOT_CAPACITY, checked at concrete capture.
pub const Requirements = struct {
    bytes: [capacity]usize = @splat(0),

    pub fn fromSelectedPlan(plan: commands.GraphCommandPlanView) !Requirements {
        var result: Requirements = .{};
        for (plan.scratch_slots) |slot| {
            if (slot.slot >= capacity) return error.UnsupportedWorkspaceRole;
            if (slot.bytes == 0) continue;
            if (slot.first_op > slot.last_op or slot.last_op >= plan.ops.len) return error.InvalidWorkspaceRoleLifetime;
            // SAME max-per-slot operation used by reserve_graph_plan_slot.
            result.bytes[slot.slot] = @max(result.bytes[slot.slot], slot.bytes);
        }
        return result;
    }
    /// The selected dense executor calls the existing fixed-slot C block,
    /// in addition to command-plan declarations. Max-union their SAME logical
    /// requests before capture/reserve, so a first frame never grows a smaller
    /// already-published slot and consumes a second unplanned allocation.
    pub fn fromSelectedDensePrefill(plan: commands.GraphCommandPlanView, rows: usize, hidden: usize, heads: usize, layers: anytype) !Requirements {
        var result = try fromSelectedPlan(plan);
        for (layers) |layer| {
            const scratch = try geometry.prefillScratch(.{ .rows = rows, .hidden = hidden, .heads = heads, .kv_heads = layer.kv_heads, .head_dim = layer.head_dim, .intermediate = layer.intermediate_size });
            for (scratch.slots, scratch.bytes) |slot, bytes| {
                if (slot >= capacity) return error.UnsupportedWorkspaceRole;
                result.bytes[slot] = @max(result.bytes[slot], bytes);
            }
        }
        return result;
    }
};

/// Borrowed only while the actual concrete runtime owner is held. The fresh
/// bound is the backend's existing standalone allocation size/alignment bound,
/// not logical bytes, a guessed page multiple, or a post-allocation reservation.
pub const CapturedRole = extern struct {
    buffer_identity: usize = 0,
    birth: u64 = 0,
    capacity_bytes: usize = 0,
    allocated_bytes: usize = 0,
    fresh_allocation_bound: usize = 0,
    root: ?*ownership.Root = null,
    operations: ?*const ownership.RootOperations = null,

    fn acceptedRoot(self: CapturedRole) !?*ownership.Root {
        const root = self.root orelse return null;
        // Root is opaque until the exact immutable concrete descriptor accepts
        // it. A copied/custom descriptor grants no authority to cast metadata.
        if (self.operations != &ownership.RootOperations.canonical)
            return error.UnsupportedWorkspaceRoot;
        return root;
    }
};
pub const Capture = [capacity]CapturedRole;

/// Concrete adapter calls this only after full backend-table acceptance and
/// current.operationFor(actual backend), while the actual runtime owner holds.
pub extern fn antfly_metal_graph_workspace_capture(runtime: *anyopaque, required: *const [capacity]usize, roles: *Capture, count: usize) c_int;

pub extern fn antfly_metal_workspace_allocation_bound(runtime: *anyopaque, bytes: usize, shared: c_int) usize;
extern fn antfly_metal_qwen_aux_workspace_capture(runtime: *anyopaque, scratch_bytes: usize, token_bytes: usize, host_ids_bytes: usize, attention: *const geometry.DenseAttention, roles: *[5]CapturedRole, staging_required: *usize, f16kv_required: *usize) c_int;

/// Exactly five non-graph persistent field roles, not an arbitrary extra
/// activation allowance. Metadata/physical bounds are captured with the same
/// actual provider exclusion as the full SDK census and validated pre-math.
pub const Auxiliary = struct {
    greedy: geometry.Greedy,
    host_ids_bytes: usize,
    attention: geometry.DenseAttention,
    plan: Plan,

    pub fn capture(runtime: *anyopaque, greedy: geometry.Greedy, host_ids_bytes: usize, attention: geometry.DenseAttention) !Auxiliary {
        var capture5: [5]CapturedRole = undefined;
        var required_staging: usize = 0;
        var required_f16kv: usize = 0;
        if (antfly_metal_qwen_aux_workspace_capture(runtime, greedy.scratch_bytes, greedy.token_bytes, host_ids_bytes, &attention, &capture5, &required_staging, &required_f16kv) != 0)
            return error.WorkspaceRoleCaptureUnavailable;
        var requirements: Requirements = .{};
        requirements.bytes[0] = greedy.scratch_bytes;
        requirements.bytes[1] = greedy.token_bytes;
        requirements.bytes[2] = required_staging;
        requirements.bytes[3] = required_f16kv;
        requirements.bytes[4] = required_f16kv;
        var roles: Capture = @splat(.{});
        @memcpy(roles[0..5], &capture5);
        return .{ .greedy = greedy, .host_ids_bytes = host_ids_bytes, .attention = attention, .plan = try Plan.capture(requirements, roles) };
    }
    pub fn validate(self: *const Auxiliary, runtime: *anyopaque) !void {
        var capture5: [5]CapturedRole = undefined;
        var required_staging: usize = 0;
        var required_f16kv: usize = 0;
        if (antfly_metal_qwen_aux_workspace_capture(runtime, self.greedy.scratch_bytes, self.greedy.token_bytes, self.host_ids_bytes, &self.attention, &capture5, &required_staging, &required_f16kv) != 0)
            return error.WorkspaceRoleCaptureUnavailable;
        if (required_staging != self.plan.requirements.bytes[2] or required_f16kv != self.plan.requirements.bytes[3] or required_f16kv != self.plan.requirements.bytes[4]) return error.WorkspaceRoleChanged;
        var current: Capture = @splat(.{});
        @memcpy(current[0..5], &capture5);
        try self.plan.validate(current);
    }
    pub fn deinitOutsideExecution(self: *Auxiliary) void {
        self.plan.deinitOutsideExecution();
    }
};

pub const Plan = struct {
    requirements: Requirements,
    captured: Capture,
    root_identities: [capacity]u64 = @splat(0),
    metadata: [capacity]?backing.BackingOwner.Metadata = @splat(null),
    fresh_slots: usize = 0,
    fresh_bytes: usize = 0,
    compatible_root_bytes: usize = 0,

    /// No SDK allocation, supplier callback, retained SDK object or per-role
    /// metadata allocation. Each unique compatible root receives one PURE pin.
    pub fn capture(requirements: Requirements, current: Capture) !Plan {
        var self: Plan = .{ .requirements = requirements, .captured = current };
        // Validate every declaration/overflow BEFORE retaining anything. Thus
        // invalid capture is failure-atomic even under numerical ownership.
        for (requirements.bytes, current, 0..) |required, role, i| {
            if (required == 0) continue;
            const reuses = role.buffer_identity != 0 and role.capacity_bytes >= required;
            if (!reuses) {
                if (role.fresh_allocation_bound < required) return error.MissingWorkspaceAllocationBound;
                self.fresh_slots += 1;
                self.fresh_bytes = try std.math.add(usize, self.fresh_bytes, role.fresh_allocation_bound);
            } else if (role.allocated_bytes < required) return error.InvalidWorkspaceBacking;
            const root = (try role.acceptedRoot()) orelse continue; // compatibility stays protected
            // A REPLACED old root earns zero credit, yet the reserve/replace
            // path can retire it during this invocation. Keep its exact pure
            // metadata in the same pre-math census for the actual-B use bank.
            if (role.buffer_identity == 0 or role.birth == 0 or root.logical_references.load(.acquire) == 0 or !root.creator_live.load(.acquire)) return error.ClosedWorkspaceRole;
            if (root.backing.admittedBytes() < role.allocated_bytes) return error.UncoveredWorkspaceBacking;
            self.root_identities[i] = root.identity;
            var already_credited = false;
            for (self.root_identities[0..i], 0..) |identity, j| if (identity == root.identity) {
                if (current[j].buffer_identity != role.buffer_identity or current[j].allocated_bytes != role.allocated_bytes or current[j].birth != role.birth) return error.InvalidWorkspaceRootAlias;
                if (current[j].capacity_bytes >= requirements.bytes[j]) already_credited = true;
            };
            if (reuses and !already_credited) self.compatible_root_bytes = try std.math.add(usize, self.compatible_root_bytes, role.allocated_bytes);
        }
        for (current, self.root_identities, 0..) |role, identity, i| {
            if (identity == 0) continue;
            var duplicate = false;
            for (self.root_identities[0..i]) |previous| if (previous == identity) {
                duplicate = true;
                break;
            };
            if (!duplicate) self.metadata[i] = role.root.?.retainSnapshot();
        }
        return self;
    }

    /// Accounting-class credit additionally requires the SAME controller and
    /// backend class. Physical existence alone cannot lend another budget's
    /// quota to this request. Replacement/unknown roots still earn zero here.
    pub fn compatibleBytesFor(self: *const Plan, identity: @import("../execution/storage_admission.zig").Identity) !usize {
        var bytes: usize = 0;
        for (self.requirements.bytes, self.captured, self.root_identities, 0..) |required, role, root_id, i| {
            if (required == 0 or root_id == 0 or role.capacity_bytes < required) continue;
            const root = role.root.?; // canonical descriptor accepted at capture
            if (!root.admission_identity.eql(identity)) continue;
            var duplicate = false;
            for (self.root_identities[0..i], 0..) |previous, j| if (previous == root_id and self.captured[j].capacity_bytes >= self.requirements.bytes[j]) {
                duplicate = true;
                break;
            };
            if (!duplicate) bytes = try std.math.add(usize, bytes, role.allocated_bytes);
        }
        return bytes;
    }
    /// Called after outside admission, under the SAME exact concrete owner,
    /// before reserve/commit or mathematical work. No numerical retry is allowed.
    /// Pure pins do not prevent physical/root replacement, so compare all roles.
    pub fn validate(self: *const Plan, current: Capture) !void {
        for (self.requirements.bytes, self.captured, current, self.root_identities) |required, before, now, identity| {
            if (required == 0) continue;
            if (before.buffer_identity != now.buffer_identity or before.birth != now.birth or
                before.capacity_bytes != now.capacity_bytes or before.allocated_bytes != now.allocated_bytes or
                before.fresh_allocation_bound != now.fresh_allocation_bound or before.root != now.root or before.operations != now.operations)
                return error.WorkspaceRoleChanged;
            if (identity != 0) {
                const root = (try now.acceptedRoot()) orelse return error.WorkspaceRoleChanged;
                if (root.identity != identity or root.logical_references.load(.acquire) == 0 or !root.creator_live.load(.acquire))
                    return error.WorkspaceRoleChanged;
            }
        }
    }

    /// Known outside-lane owner only. A snapshot never borrows its creator's B
    /// and never owns a physical SDK root or numerical operation.
    pub fn deinitOutsideExecution(self: *Plan) void {
        for (&self.metadata) |*item| if (item.*) |pin| {
            item.* = null;
            pin.release(pin.context);
        };
    }
};

test "workspace role plan bounds fresh cells by actual selected roles and retains SDK rounding" {
    var required: Requirements = .{};
    required.bytes[0] = 1;
    required.bytes[3] = 16385;
    var current: Capture = @splat(.{});
    current[0].fresh_allocation_bound = 16384;
    current[3].fresh_allocation_bound = 32768;
    var plan = try Plan.capture(required, current);
    defer plan.deinitOutsideExecution();
    try std.testing.expectEqual(@as(usize, 2), plan.fresh_slots);
    try std.testing.expectEqual(@as(usize, 49152), plan.fresh_bytes);
    try std.testing.expectEqual(@as(usize, 0), plan.compatible_root_bytes);
    try plan.validate(current);
    current[0].buffer_identity = 42;
    try std.testing.expectError(error.WorkspaceRoleChanged, plan.validate(current));
}

test "workspace role plan never credits replacement or unknown legacy backing" {
    var required: Requirements = .{};
    required.bytes[0] = 32768;
    required.bytes[1] = 16;
    var current: Capture = @splat(.{});
    current[0] = .{ .buffer_identity = 17, .birth = 9, .capacity_bytes = 16384, .allocated_bytes = 16384, .fresh_allocation_bound = 32768 };
    current[1] = .{ .buffer_identity = 18, .birth = 10, .capacity_bytes = 16384, .allocated_bytes = 16384 };
    var plan = try Plan.capture(required, current);
    defer plan.deinitOutsideExecution();
    try std.testing.expectEqual(@as(usize, 1), plan.fresh_slots);
    try std.testing.expectEqual(@as(usize, 32768), plan.fresh_bytes);
    try std.testing.expectEqual(@as(usize, 0), plan.compatible_root_bytes);
    current[0].fresh_allocation_bound = 32767;
    try std.testing.expectError(error.MissingWorkspaceAllocationBound, Plan.capture(required, current));
    current[0].fresh_allocation_bound = 0;
    try std.testing.expectError(error.MissingWorkspaceAllocationBound, Plan.capture(required, current));
}

test "workspace role declarations preserve actual max per slot and reject invalid schedule" {
    const command: commands.GraphCommandOp = .{ .kind = .unknown, .source = 0, .region = 0, .planned_op_index = 0, .scope_index = 0, .barrier_before = false, .resource_start = 0, .resource_count = 0 };
    const ops = [_]commands.GraphCommandOp{ command, command };
    var slots = [_]commands.ScratchSlotLifetime{
        .{ .slot = 3, .bytes = 5, .first_op = 0, .last_op = 0 },
        .{ .slot = 3, .bytes = 7, .first_op = 1, .last_op = 1 },
    };
    var view: commands.GraphCommandPlanView = .{ .ops = &ops, .resources = &.{}, .planned_ops = &.{}, .scopes = &.{}, .scratch_slots = &slots, .barrier_count = 0 };
    const requirements = try Requirements.fromSelectedPlan(view);
    try std.testing.expectEqual(@as(usize, 7), requirements.bytes[3]);
    slots[1].last_op = 2;
    try std.testing.expectError(error.InvalidWorkspaceRoleLifetime, Requirements.fromSelectedPlan(view));
    slots[1].last_op = 1;
    slots[1].slot = capacity;
    try std.testing.expectError(error.UnsupportedWorkspaceRole, Requirements.fromSelectedPlan(view));
    view.scratch_slots = &.{};
    try std.testing.expectEqualDeep(Requirements{}, try Requirements.fromSelectedPlan(view));
}

test "workspace role credit pins exact quota once and cannot keep a closed creator reusable" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const retirement = @import("../execution/resource_retirement.zig");
    const sink = @import("../execution/retirement_sink.zig");
    const Metadata = struct {
        deaths: usize = 0,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.deaths += 1;
        }
    };
    var metadata: Metadata = .{};
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    var lease: ?memory.AdmissionLease = try controller.tryAcquire(.gpu, .{ .backend_limit_bytes = 64 }, .{ .backend_scratch_bytes = 64 }, false);
    defer if (lease) |*owned| owned.release();
    var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    var physical: ?@import("../execution/storage_admission.zig").Reservation = .{
        .identity = .{ .controller = &controller, .backend_class = .gpu },
        .leases = .{ lease, null },
        .domain = .backend,
    };
    lease = null;
    const charge = try retirement.Charge.createReservation(&physical, &domain);
    var root: ownership.Root = undefined;
    root.init(charge, .{ .context = &metadata, .release = Metadata.release });
    var required: Requirements = .{};
    required.bytes[0] = 16;
    required.bytes[1] = 32;
    var current: Capture = @splat(.{});
    const role: CapturedRole = .{ .buffer_identity = 123, .birth = 7, .capacity_bytes = 64, .allocated_bytes = 64, .root = &root, .operations = &ownership.RootOperations.canonical };
    current[0] = role;
    current[1] = role;
    var plan = try Plan.capture(required, current);
    try std.testing.expectEqual(@as(usize, 64), plan.compatible_root_bytes);
    try std.testing.expectEqual(@as(usize, 64), try plan.compatibleBytesFor(.{ .controller = &controller, .backend_class = .gpu }));
    var other_controller: memory.AdmissionController = .{};
    defer other_controller.deinit();
    try std.testing.expectEqual(@as(usize, 0), try plan.compatibleBytesFor(.{ .controller = &other_controller, .backend_class = .gpu }));
    try std.testing.expectEqual(@as(usize, 0), try plan.compatibleBytesFor(.{ .controller = &controller, .backend_class = .cpu }));
    try std.testing.expectEqual(@as(usize, 0), plan.fresh_slots);
    try plan.validate(current);
    var replacement_requirements: Requirements = .{};
    replacement_requirements.bytes[0] = 128;
    var replacement_capture: Capture = @splat(.{});
    replacement_capture[0] = role;
    replacement_capture[0].fresh_allocation_bound = 128;
    var replacement = try Plan.capture(replacement_requirements, replacement_capture);
    try std.testing.expectEqual(@as(usize, 1), replacement.fresh_slots);
    try std.testing.expectEqual(@as(usize, 128), replacement.fresh_bytes);
    try std.testing.expectEqual(@as(usize, 0), replacement.compatible_root_bytes);
    try std.testing.expect(replacement.metadata[0] != null and replacement.root_identities[0] == root.identity);
    var reference = try sink.Ref.create();
    defer reference.release();
    var guard = try reference.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try root.releaseCreator(.{ .boundary = &epoch });
    try std.testing.expectError(error.WorkspaceRoleChanged, plan.validate(current));
    guard.finish();
    // Metadata pins retain quota, not permission to resurrect/reuse SDK roots.
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    try std.testing.expect(!root.retainPhysical());
    plan.deinitOutsideExecution();
    try std.testing.expectEqual(@as(usize, 0), metadata.deaths);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    replacement.deinitOutsideExecution();
    try std.testing.expectEqual(@as(usize, 1), metadata.deaths);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "workspace role capture rejects copied root descriptor before touching opaque context" {
    var required: Requirements = .{};
    required.bytes[0] = 16;
    var current: Capture = @splat(.{});
    var copied = ownership.RootOperations.canonical;
    current[0] = .{
        .buffer_identity = 17,
        .birth = 1,
        .capacity_bytes = 64,
        .allocated_bytes = 64,
        .root = @ptrFromInt(@alignOf(ownership.Root)),
        .operations = &copied,
    };
    try std.testing.expectError(error.UnsupportedWorkspaceRoot, Plan.capture(required, current));
    current[0].operations = null;
    try std.testing.expectError(error.UnsupportedWorkspaceRoot, Plan.capture(required, current));
}

/// Exact concrete C census ABI. No SDK ownership is transferred: caller holds
/// Retained cold target + its actual provider metadata Gate during capture.
/// Unknown/raw storage stays uncredited. Never cast root before operations veto.
pub const WorkspaceEntry = extern struct {
    buffer_identity: usize = 0,
    root: ?*ownership.Root = null,
    operations: ?*const ownership.RootOperations = null,
    unfunded_references: u64 = 0,
    bytes: usize = 0,
    birth: u64 = 0,
    tagged: c_int = 0,
    metadata_retained: c_int = 0,
};
pub extern fn antfly_metal_workspace_snapshot_capacity(runtime: *anyopaque) usize;
pub extern fn antfly_metal_workspace_snapshot(runtime: *anyopaque, entries: [*]WorkspaceEntry, entries_capacity: usize, out_count: *usize, generation: *u64, accepted_operations: *const ownership.RootOperations) c_int;
extern fn antfly_metal_workspace_snapshot_owned(runtime: *anyopaque, entries: [*]WorkspaceEntry, entries_capacity: usize, out_count: *usize, generation: *u64, accepted_operations: *const ownership.RootOperations) c_int;

/// A cold transaction owns ONLY copied identities + pure metadata pins. The
/// SDK/runtime/model target stays with the existing Retained caller. Capacity
/// is queried under its Gate, arrays allocated after releasing that Gate, then
/// actual capture rechecks capacity. No hot realloc or callback under exclusion.
/// This census covers existing/cache/parked-reported roots, NOT escaping views.
/// Escaping owners must supply their own independently preowned NOWB cells.
pub const Snapshot = struct {
    allocator: std.mem.Allocator,
    entries: []WorkspaceEntry,
    metadata: []?backing.BackingOwner.Metadata,
    count: usize = 0,
    generation: u64 = 0,
    captured: bool = false,

    pub fn requestedMetadataBytes(actual_capacity: usize) !usize {
        return std.math.mul(usize, actual_capacity, @sizeOf(WorkspaceEntry) + @sizeOf(?backing.BackingOwner.Metadata));
    }
    pub fn prepare(actual_capacity: usize) !Snapshot {
        const platform = @import("antfly_platform");
        return prepareWithAllocator(platform.allocator.processAllocator(platform.allocator.concurrentFallback()), actual_capacity);
    }
    fn prepareWithAllocator(a: std.mem.Allocator, actual_capacity: usize) !Snapshot {
        if (actual_capacity == 0) return error.WorkspaceCensusUnavailable;
        const entries = try a.alloc(WorkspaceEntry, actual_capacity);
        errdefer a.free(entries);
        const metadata = try a.alloc(?backing.BackingOwner.Metadata, actual_capacity);
        @memset(metadata, null);
        return .{ .allocator = a, .entries = entries, .metadata = metadata };
    }
    /// Actual concrete target's short metadata exclusion MUST be held. This
    /// method performs no SDK allocation, quota supplier or metadata allocation.
    pub fn capture(self: *Snapshot, runtime: *anyopaque) !void {
        return self.captureMode(runtime, false);
    }
    /// Final census for an existing-root-only receiving loan: new SDK legacy
    /// buffers grant ZERO storage credit and create no new modern Root.
    /// Capacity is the actual cold charged-root count, never a byte heuristic.
    pub fn captureOwned(self: *Snapshot, runtime: *anyopaque) !void {
        return self.captureMode(runtime, true);
    }
    fn captureMode(self: *Snapshot, runtime: *anyopaque, owned_only: bool) !void {
        if (self.captured or self.count != 0) return error.InvalidWorkspaceCensus;
        var count: usize = 0;
        var generation: u64 = 0;
        const status = if (owned_only)
            antfly_metal_workspace_snapshot_owned(runtime, self.entries.ptr, self.entries.len, &count, &generation, &ownership.RootOperations.canonical)
        else
            antfly_metal_workspace_snapshot(runtime, self.entries.ptr, self.entries.len, &count, &generation, &ownership.RootOperations.canonical);
        if (count > self.entries.len) @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.receivePins(count);
        if (status == -2) return error.WorkspaceCensusCapacityChanged;
        if (status != 0) return error.WorkspaceCensusUnavailable;
        try self.acceptCaptured(count, generation);
    }
    fn receivePins(self: *Snapshot, count: usize) void {
        // C transferred pure pins BEFORE releasing each actual SDK owner or
        // reuse-pool lock. Even a partial capacity failure owns these pins;
        // the enclosing outside caller destroys them AFTER releasing its Gate.
        self.count = count;
        for (self.entries[0..count], 0..) |entry, i| if (entry.metadata_retained != 0) {
            if (entry.operations != &ownership.RootOperations.canonical or entry.root == null)
                @import("antfly_platform").inference_process_supervisor.restartWorker();
            self.metadata[i] = entry.root.?.ownedSnapshotPin();
        };
    }
    fn acceptCaptured(self: *Snapshot, count: usize, generation: u64) !void {
        // C has transferred canonical pure pins while actual SDK owners were
        // held. Validate ALL opaque identities before any typed dereference;
        // every partial/vetoed transaction is destroyed outside the Gate.
        for (self.entries[0..count], 0..) |entry, i| {
            if (entry.buffer_identity == 0 or entry.bytes == 0) return error.InvalidWorkspaceBacking;
            const root = entry.root orelse continue;
            if (entry.operations != &ownership.RootOperations.canonical) return error.UnsupportedWorkspaceRoot;
            if (entry.metadata_retained != 1 or self.metadata[i] == null) return error.MissingWorkspaceSnapshotPin;
            if (entry.birth == 0 or root.logical_references.load(.acquire) == 0) return error.ClosedWorkspaceRole;
            if (root.backing.admittedBytes() < entry.bytes) return error.UncoveredWorkspaceBacking;
            for (self.entries[0..i]) |previous| {
                if (previous.buffer_identity == entry.buffer_identity or previous.root == root)
                    return error.InvalidWorkspaceRootAlias;
            }
        }
        self.count = count;
        self.generation = generation;
        self.captured = true;
    }
    /// Caller supplies storage sized from the ACTUAL completed census. These
    /// are every charged old root, including zero-credit replacement victims;
    /// the caller prepares separate actual-B cells OUTSIDE numerical gates.
    pub const CapturedCoverage = struct {
        witnessed_roots: usize = 0,
        witnessed_actual_bytes: usize = 0,
        witnessed_admitted_bytes: usize = 0,
        unwitnessed_roots: usize = 0,
        unwitnessed_actual_bytes: usize = 0,
    };
    /// Copied census facts for QA/diagnostics ONLY, never an admission-credit
    /// authority. Exact SDK identities were deduplicated at capture. Pure pins
    /// keep quota metadata alive, not the SDK creator/current selected route.
    pub fn capturedCoverage(self: *const Snapshot) !CapturedCoverage {
        if (!self.captured) return error.InvalidWorkspaceCensus;
        var result: CapturedCoverage = .{};
        for (self.entries[0..self.count]) |entry| {
            if (entry.root) |root| {
                result.witnessed_roots += 1;
                result.witnessed_actual_bytes = try std.math.add(usize, result.witnessed_actual_bytes, entry.bytes);
                result.witnessed_admitted_bytes = try std.math.add(usize, result.witnessed_admitted_bytes, root.backing.admittedBytes());
            } else {
                result.unwitnessed_roots += 1;
                result.unwitnessed_actual_bytes = try std.math.add(usize, result.unwitnessed_actual_bytes, entry.bytes);
            }
        }
        return result;
    }
    pub fn copyRoots(self: *const Snapshot, roots: []*ownership.Root) ![]*ownership.Root {
        if (!self.captured) return error.InvalidWorkspaceCensus;
        var needed: usize = 0;
        for (self.entries[0..self.count]) |entry| if (entry.root != null) {
            needed += 1;
        };
        if (roots.len < needed) return error.UnpreparedWorkspaceUses;
        var n: usize = 0;
        for (self.entries[0..self.count]) |entry| if (entry.root) |root| {
            roots[n] = root;
            n += 1;
        };
        return roots[0..n];
    }
    pub fn validate(self: *const Snapshot, current: *const Snapshot) !void {
        if (!self.captured or !current.captured) return error.InvalidWorkspaceCensus;
        if (self.count != current.count or self.generation != current.generation) return error.WorkspaceCensusChanged;
        for (self.entries[0..self.count], current.entries[0..current.count]) |before, now| {
            if (!std.meta.eql(before, now)) return error.WorkspaceCensusChanged;
            if (now.root) |root| if (root.logical_references.load(.acquire) == 0) return error.WorkspaceCensusChanged;
        }
    }
    pub fn deinitOutsideExecution(self: *Snapshot) void {
        for (self.metadata[0..self.count]) |maybe| if (maybe) |pin| pin.release(pin.context);
        self.allocator.free(self.entries);
        self.allocator.free(self.metadata);
        self.* = undefined;
    }
};

test "workspace actual census cold OOM and opaque descriptor veto publish no partial pins" {
    for (0..2) |failure| {
        var allocator = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = failure });
        try std.testing.expectError(error.OutOfMemory, Snapshot.prepareWithAllocator(allocator.allocator(), 3));
    }
    try std.testing.expectError(error.WorkspaceCensusUnavailable, Snapshot.prepareWithAllocator(std.testing.allocator, 0));
    var snapshot = try Snapshot.prepareWithAllocator(std.testing.allocator, 3);
    defer snapshot.deinitOutsideExecution();
    var copied = ownership.RootOperations.canonical;
    snapshot.entries[0] = .{ .buffer_identity = 17, .bytes = 64 }; // real compatibility shape, zero credit
    snapshot.entries[1] = .{ .buffer_identity = 18, .bytes = 64, .birth = 1, .root = @ptrFromInt(@alignOf(ownership.Root)), .operations = &copied };
    try std.testing.expectError(error.UnsupportedWorkspaceRoot, snapshot.acceptCaptured(2, 1));
    try std.testing.expect(!snapshot.captured and snapshot.count == 0);
    for (snapshot.metadata) |pin| try std.testing.expect(pin == null);
    try snapshot.acceptCaptured(1, 1);
    var roots: [1]*ownership.Root = undefined;
    try std.testing.expectEqual(@as(usize, 0), (try snapshot.copyRoots(&roots)).len);
}

test "workspace cold census transferred pure pin survives final source retirement and partial capture" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const retirement = @import("../execution/resource_retirement.zig");
    const sink = @import("../execution/retirement_sink.zig");
    const Gate = @import("../execution/operation_gate.zig").Gate;
    const Metadata = struct {
        gate: *Gate,
        deaths: usize = 0,
        failed: bool = false,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.deaths += 1;
            var ticket = self.gate.prepare() catch {
                self.failed = true;
                return;
            };
            defer ticket.release() catch unreachable;
            ticket.begin() catch {
                self.failed = true;
                return;
            };
            ticket.end() catch unreachable;
        }
    };
    var gate: Gate = .{};
    defer gate.assertIdle();
    var metadata: Metadata = .{ .gate = &gate };
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    var lease: ?memory.AdmissionLease = try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false);
    var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    var physical: ?@import("../execution/storage_admission.zig").Reservation = .{
        .identity = .{ .controller = &controller, .backend_class = .gpu },
        .leases = .{ lease, null },
        .domain = .backend,
    };
    lease = null;
    const charge = try retirement.Charge.createReservation(&physical, &domain);
    var root: ownership.Root = undefined;
    root.init(charge, .{ .context = &metadata, .release = Metadata.release });
    var snapshot = try Snapshot.prepareWithAllocator(std.testing.allocator, 2);
    var ticket = try gate.prepare();
    defer ticket.release() catch unreachable;
    try ticket.begin();
    // SAME canonical callback and exact transferred wire flag as production C.
    // No source SDK ownership is invented by this metadata-only test.
    ownership.RootOperations.canonical.retain_metadata(&root);
    snapshot.entries[0] = .{ .buffer_identity = 17, .root = &root, .operations = &ownership.RootOperations.canonical, .bytes = 64, .birth = 1, .tagged = 1, .metadata_retained = 1 };
    snapshot.receivePins(1); // partial capture can now fail, pins remain owned
    try ticket.end();
    var reference = try sink.Ref.create();
    defer reference.release();
    var guard = try reference.enter();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try root.releaseCreator(.{ .boundary = &epoch });
    guard.finish();
    try std.testing.expectEqual(@as(usize, 0), metadata.deaths);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    try std.testing.expect(!snapshot.captured); // partial-error destructor path
    snapshot.deinitOutsideExecution();
    try std.testing.expectEqual(@as(usize, 1), metadata.deaths);
    try std.testing.expect(!metadata.failed);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

// Explicit logical-core aggregation; these pure cold recipes allocate nothing.
test {
    _ = @import("metal_workspace_geometry.zig");
}

test "dense prefill cold declaration merges actual fixed hot banks before first SDK reserve" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const command: commands.GraphCommandOp = .{ .kind = .unknown, .source = 0, .region = 0, .planned_op_index = 0, .scope_index = 0, .barrier_before = false, .resource_start = 0, .resource_count = 0 };
    const ops = [_]commands.GraphCommandOp{command};
    const selected_slots = [_]commands.ScratchSlotLifetime{
        .{ .slot = 0, .bytes = 65536, .first_op = 0, .last_op = 0 },
        .{ .slot = 21, .bytes = 8192, .first_op = 0, .last_op = 0 },
        .{ .slot = 26, .bytes = 17, .first_op = 0, .last_op = 0 },
    };
    const selected: commands.GraphCommandPlanView = .{ .ops = &ops, .resources = &.{}, .planned_ops = &.{}, .scopes = &.{}, .scratch_slots = &selected_slots, .barrier_count = 0 };
    const Layer = struct { kv_heads: usize, head_dim: usize, intermediate_size: usize };
    const layers = [_]Layer{.{ .kv_heads = 1, .head_dim = 128, .intermediate_size = 256 }};
    const plain = try Requirements.fromSelectedPlan(selected);
    const merged = try Requirements.fromSelectedDensePrefill(selected, 32, 128, 1, &layers);
    try std.testing.expectEqual(@as(usize, 8192), plain.bytes[21]);
    for (merged.bytes[21..26]) |bytes| try std.testing.expectEqual(@as(usize, 32768), bytes);
    try std.testing.expectEqual(@as(usize, 65536), merged.bytes[0]); // larger selected role survives
    try std.testing.expectEqual(@as(usize, 17), merged.bytes[26]); // unrelated selected role survives
    try std.testing.expectEqual(@as(usize, 0), merged.bytes[11]); // no decoder logits tail invented
    const varied = [_]Layer{ layers[0], .{ .kv_heads = 1, .head_dim = 128, .intermediate_size = 512 } };
    const across_layers = try Requirements.fromSelectedDensePrefill(selected, 32, 128, 1, &varied);
    for (across_layers.bytes[21..26]) |bytes| try std.testing.expectEqual(@as(usize, 65536), bytes);
    try std.testing.expectError(error.WorkspaceGeometryOverflow, Requirements.fromSelectedDensePrefill(selected, std.math.maxInt(usize), 128, 1, &layers));
}

test "workspace dense F16 pair joint replacement reserves both roles without lending the surviving member" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const required = try geometry.denseF16Kv(.{ .sequence = 16, .heads = 2, .kv_heads = 1, .head_dim = 128 }, 15);
    for ([_][2]bool{ .{ true, true }, .{ true, false }, .{ false, true }, .{ false, false } }) |present| {
        const replace = geometry.denseF16KvReplaces(required, required, present[0], present[1]);
        var requirements: Requirements = .{};
        requirements.bytes[3] = required;
        requirements.bytes[4] = required;
        var captured: Capture = @splat(.{});
        for (present, 3..) |exists, index| {
            captured[index] = .{ .buffer_identity = if (exists) index + 1 else 0, .birth = if (exists) 1 else 0, .capacity_bytes = if (replace) 0 else required, .allocated_bytes = if (exists) required else 0, .fresh_allocation_bound = if (replace) 16384 else 0 };
        }
        var plan = try Plan.capture(requirements, captured);
        defer plan.deinitOutsideExecution();
        try std.testing.expectEqual(@as(usize, if (replace) 2 else 0), plan.fresh_slots);
        try std.testing.expectEqual(@as(usize, if (replace) 32768 else 0), plan.fresh_bytes);
        try std.testing.expectEqual(@as(usize, 0), plan.compatible_root_bytes); // raw existence is zero credit
    }
}

test "Metal dense F16 attention pair actual cold warm growth captures exact SDK members in full census" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const runtime = @import("metal_runtime.zig");
    if (!runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const raw = runtime.termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
    defer runtime.termite_metal_decode_runtime_destroy(raw);
    if (runtime.termite_metal_decode_runtime_ready(raw) == 0) return error.SkipZigTest;
    const provider = .{ .raw_decode_runtime = @as(?*runtime.RawMetalDecodeRuntime, raw) };
    const Produce = struct {
        fn run(actual: anytype, sequence: usize) !void {
            const a = std.testing.allocator;
            const count = sequence * 128;
            const values = try a.alloc(f32, count);
            defer a.free(values);
            for (values, 0..) |*value, index| value.* = @as(f32, @floatFromInt(index % 17)) / 64;
            var input = try runtime.MetalTensor.deviceAllocate(@ptrCast(actual.raw_decode_runtime.?), count * 4, .private, &.{ @intCast(sequence), 128 });
            defer input.deinit();
            try std.testing.expectEqual(@as(c_int, 0), runtime.termite_metal_buffer_upload(actual.raw_decode_runtime, input.deviceHandle(), 0, values.ptr, count * 4));
            var output = (try runtime.decoderRuntimeApplyBatchedAttentionF32DevicePreallocated(actual, .{ .q = input, .k = input, .v = input, .batch = @as(usize, 1), .q_len = sequence, .num_heads = @as(usize, 1), .num_kv_heads = @as(usize, 1), .head_dim = @as(usize, 128) })) orelse return error.ExpectedActualDenseAttention;
            defer output.deinit();
        }
    };
    const first: geometry.DenseAttention = .{ .sequence = 16, .heads = 1, .kv_heads = 1, .head_dim = 128 };
    var cold = try Auxiliary.capture(@ptrCast(raw), .{ .scratch_bytes = 0, .token_bytes = 0 }, 0, first);
    defer cold.deinitOutsideExecution();
    // An explicitly disabled/unavailable F16 pipeline selects another real
    // kernel; it must not receive invented unused staging admission.
    if (cold.plan.requirements.bytes[3] == 0) return error.SkipZigTest;
    try std.testing.expectEqual(@as(usize, 4096), cold.plan.requirements.bytes[3]);
    try std.testing.expectEqual(cold.plan.requirements.bytes[3], cold.plan.requirements.bytes[4]);
    try std.testing.expectEqual(@as(usize, 2), cold.plan.fresh_slots);
    try Produce.run(&provider, first.sequence);
    var warm = try Auxiliary.capture(@ptrCast(raw), .{ .scratch_bytes = 0, .token_bytes = 0 }, 0, first);
    defer warm.deinitOutsideExecution();
    try std.testing.expectEqual(@as(usize, 0), warm.plan.fresh_slots);
    const key = warm.plan.captured[3].buffer_identity;
    const value = warm.plan.captured[4].buffer_identity;
    try std.testing.expect(key != 0 and value != 0 and key != value);
    {
        var snapshot = try Snapshot.prepare(antfly_metal_workspace_snapshot_capacity(@ptrCast(raw)));
        defer snapshot.deinitOutsideExecution();
        try snapshot.capture(@ptrCast(raw));
        var found_key = false;
        var found_value = false;
        for (snapshot.entries[0..snapshot.count]) |entry| {
            if (entry.buffer_identity == key) found_key = true;
            if (entry.buffer_identity == value) found_value = true;
        }
        try std.testing.expect(found_key and found_value);
    }
    try Produce.run(&provider, first.sequence);
    try warm.validate(@ptrCast(raw)); // same SDK pair, no warm replacement
    var bigger = first;
    bigger.sequence = 32;
    var growing = try Auxiliary.capture(@ptrCast(raw), .{ .scratch_bytes = 0, .token_bytes = 0 }, 0, bigger);
    defer growing.deinitOutsideExecution();
    try std.testing.expectEqual(@as(usize, 2), growing.plan.fresh_slots);
    try std.testing.expectEqual(@as(usize, 0), growing.plan.captured[3].capacity_bytes);
    try std.testing.expectEqual(@as(usize, 0), growing.plan.captured[4].capacity_bytes);
    try std.testing.expectEqual(key, growing.plan.captured[3].buffer_identity);
    try std.testing.expectEqual(value, growing.plan.captured[4].buffer_identity);
    try Produce.run(&provider, bigger.sequence);
    var grown = try Auxiliary.capture(@ptrCast(raw), .{ .scratch_bytes = 0, .token_bytes = 0 }, 0, bigger);
    defer grown.deinitOutsideExecution();
    try std.testing.expectEqual(@as(usize, 0), grown.plan.fresh_slots);
    try std.testing.expectEqual(@as(usize, 8192), grown.plan.requirements.bytes[3]);
    try std.testing.expect(grown.plan.captured[3].buffer_identity != key and grown.plan.captured[4].buffer_identity != value);
}
