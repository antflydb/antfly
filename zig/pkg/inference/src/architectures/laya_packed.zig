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

//! Session contract for one tree-packed Laya row (models/laya/LAYA.md).
//!
//! Inputs, all i64: `input_ids`, `position_ids`, `token_segment`,
//! `token_qtype` as `[1, L]`; `segment_parent` as `[1, S]`; `marker_pos` as
//! `[Q, W]`; `anchor_pos` as `[Q, 1]`. Outputs: `logits` `[Q, W]` and
//! `action_logits` `[Q, n_act]`.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const modern = @import("modern_bert.zig");
const head = @import("laya_head.zig");
const tree = @import("../pipelines/laya_tree.zig");
const trunk_cache = @import("laya_trunk_cache.zig");
const build_options = @import("build_options");
const metal_compute = if (build_options.enable_metal) @import("../ops/metal_compute.zig") else struct {};
const MetalTensor = if (build_options.enable_metal) @import("../backends/metal_tensor.zig").MetalTensor else void;
const Tensor = @import("../backends/tensor.zig").Tensor;
const TensorInfo = @import("../backends/tensor.zig").TensorInfo;

pub const input_count = 7;
pub const inputs_info = [_]TensorInfo{
    .{ .name = "input_ids", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "position_ids", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "token_segment", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "segment_parent", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "token_qtype", .dtype = .i64, .shape = &.{ 1, -1 } },
    .{ .name = "marker_pos", .dtype = .i64, .shape = &.{ -1, -1 } },
    .{ .name = "anchor_pos", .dtype = .i64, .shape = &.{ -1, 1 } },
};

/// Copy an i64 matrix; request tensors carry no alignment guarantee.
fn matrix(a: std.mem.Allocator, tensor: Tensor, rows: ?usize, columns: ?usize) ![]const i64 {
    if (tensor.dtype != .i64 or tensor.shape.len != 2 or tensor.shape[0] <= 0 or tensor.shape[1] <= 0) return error.InvalidLayaInputs;
    const r: usize = @intCast(tensor.shape[0]);
    const c: usize = @intCast(tensor.shape[1]);
    if ((rows != null and rows.? != r) or (columns != null and columns.? != c)) return error.InvalidLayaInputs;
    const count = std.math.mul(usize, r, c) catch return error.InvalidLayaInputs;
    if (tensor.data.len != count * @sizeOf(i64)) return error.InvalidLayaInputs;
    const values = try a.alloc(i64, count);
    @memcpy(std.mem.sliceAsBytes(values), tensor.data);
    return values;
}

/// A validated row owned by `a`; free it with `Row.deinit`.
pub fn view(a: std.mem.Allocator, cfg: modern.Config, inputs: []const Tensor) !tree.Row {
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    if (!laya.packing.enabled() or inputs.len != input_count) return error.InvalidLayaInputs;
    const markers_tensor = inputs[5];
    if (markers_tensor.shape.len != 2 or markers_tensor.shape[0] <= 0 or markers_tensor.shape[1] <= 0) return error.InvalidLayaInputs;
    const questions: usize = @intCast(markers_tensor.shape[0]);
    const width: usize = @intCast(markers_tensor.shape[1]);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const s = arena.allocator();
    const ids = try matrix(s, inputs[0], 1, null);
    const index = try s.alloc(usize, questions);
    for (index, 0..) |*value, i| value.* = i;
    const row = tree.Row{
        .ids = ids,
        .positions = try matrix(s, inputs[1], 1, ids.len),
        .segments = try matrix(s, inputs[2], 1, ids.len),
        .parents = try matrix(s, inputs[3], 1, null),
        .kinds = try matrix(s, inputs[4], 1, ids.len),
        .markers = try matrix(s, markers_tensor, questions, width),
        .anchors = try matrix(s, inputs[6], questions, 1),
        .question_index = index,
        .width = width,
        .trunk_sees_tree = laya.packing.trunk_sees_questions,
        .fused = laya.packing.fuse_layers > 0,
    };
    if (laya.packing.fuse_layers > cfg.num_hidden_layers + laya.head_layers) return error.InvalidLayaConfig;
    try tree.validate(row, laya.max_len, laya.packing.max_packed_len, laya.maxOptions());
    for (ids) |id| if (id < 0 or id >= cfg.vocab_size) return error.InvalidLayaInputs;
    return tree.own(a, row);
}

/// Allocation-free view for cold selected-work planning. Unsupported unaligned
/// caller tensors keep the original copying route; canonical tensor builders
/// provide aligned matrices. The actual run still owns/copies its original Row.
pub fn metadataView(cfg: modern.Config, inputs: []const Tensor) !tree.Row {
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    if (!laya.packing.enabled() or inputs.len != input_count) return error.InvalidLayaInputs;
    const Matrix = struct {
        fn values(tensor: Tensor, rows: ?usize, columns: ?usize) ![]const i64 {
            if (tensor.dtype != .i64 or tensor.shape.len != 2 or tensor.shape[0] <= 0 or tensor.shape[1] <= 0 or !tensor.isAlignedFor(i64)) return error.InvalidLayaInputs;
            const r = std.math.cast(usize, tensor.shape[0]) orelse return error.InvalidLayaInputs;
            const c = std.math.cast(usize, tensor.shape[1]) orelse return error.InvalidLayaInputs;
            if ((rows != null and rows.? != r) or (columns != null and columns.? != c)) return error.InvalidLayaInputs;
            const n = std.math.mul(usize, r, c) catch return error.InvalidLayaInputs;
            if (tensor.data.len != (std.math.mul(usize, n, @sizeOf(i64)) catch return error.InvalidLayaInputs)) return error.InvalidLayaInputs;
            return tensor.asInt64();
        }
    };
    const ids = try Matrix.values(inputs[0], 1, null);
    const markers = try Matrix.values(inputs[5], null, null);
    const questions: usize = @intCast(inputs[5].shape[0]);
    const width: usize = @intCast(inputs[5].shape[1]);
    const row = tree.Row{
        .ids = ids,
        .positions = try Matrix.values(inputs[1], 1, ids.len),
        .segments = try Matrix.values(inputs[2], 1, ids.len),
        .parents = try Matrix.values(inputs[3], 1, null),
        .kinds = try Matrix.values(inputs[4], 1, ids.len),
        .markers = markers,
        .anchors = try Matrix.values(inputs[6], questions, 1),
        .question_index = &.{},
        .width = width,
        .trunk_sees_tree = laya.packing.trunk_sees_questions,
        .fused = laya.packing.fuse_layers > 0,
    };
    const layers = std.math.add(usize, cfg.num_hidden_layers, laya.head_layers) catch return error.InvalidLayaConfig;
    if (laya.packing.fuse_layers > layers) return error.InvalidLayaConfig;
    try tree.validateTensorView(row, laya.max_len, laya.packing.max_packed_len, laya.maxOptions());
    for (ids) |id| if (id < 0 or id >= cfg.vocab_size) return error.InvalidLayaInputs;
    return row;
}

pub fn run(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, inputs: []const Tensor, cache: ?*trunk_cache.Cache) ![]Tensor {
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    const row = try view(a, cfg, inputs);
    defer row.deinit(a);
    return forwardRow(cb, a, cfg, laya, row, cache);
}

/// Private Native synchronous views. Capture and cached-prefix use the SAME
/// data span in disjoint serial phases. Header allocations stay distinct and
/// are declared as their actual simultaneous cold ownership, never double-lent.
pub const AuxiliaryView = struct { values: []f32, capture_headers: [][]f32, prefix_headers: []ops.CT };
pub const AuxiliaryGeometry = struct {
    elements: usize = 0,
    headers: usize = 0,
    bytes: usize = 0,
};
pub fn auxiliaryGeometry(layers: usize, hidden: usize, max_trunk: usize) !AuxiliaryGeometry {
    if (max_trunk == 0) return .{};
    const headers = try std.math.mul(usize, 2, layers);
    const elements = try std.math.mul(usize, try std.math.mul(usize, headers, hidden), max_trunk);
    const values_bytes = try std.math.mul(usize, elements, @sizeOf(f32));
    const capture_bytes = try std.math.mul(usize, headers, @sizeOf([]f32));
    const prefix_bytes = try std.math.mul(usize, headers, @sizeOf(ops.CT));
    return .{ .elements = elements, .headers = headers, .bytes = try std.math.add(usize, values_bytes, try std.math.add(usize, capture_bytes, prefix_bytes)) };
}

/// Synchronous Source-owned cache loans. All cases, Entry pins, metadata
/// owners and funding are prepared before Model entry. This object owns no
/// quota, saved operation/Epoch, provider or independently escaping tensor.
pub const Prepared = struct {
    pub const Case = struct {
        key: [32]u8,
        tokens: usize,
        entry: ?*trunk_cache.Entry,
    };
    cache: *trunk_cache.Cache,
    snapshot_value: *trunk_cache.ColdSnapshot,
    cases: []const Case,
    config: modern.Config,
    digest: [32]u8,
    split_trees: bool,
    auxiliary: ?AuxiliaryView = null,
    workspace_declined: bool = false,
    entered: bool = false,
    cursor: usize = 0,

    fn lookup(self: *@This(), k: [32]u8, tokens: usize, layers: usize, hidden: usize, current: @import("../execution/cleanup_context.zig").Context) !trunk_cache.Lookup {
        if (!self.entered or self.cursor == self.cases.len) return error.UnpreparedTrunkCacheLookup;
        const selected = self.cases[self.cursor];
        if (selected.tokens != tokens or !std.mem.eql(u8, &selected.key, &k)) return error.TrunkCacheChangedBeforeExecution;
        if (selected.entry) |entry| {
            if (entry.tokens != tokens or entry.layers != layers or entry.hidden != hidden or !std.mem.eql(u8, &entry.key, &k))
                return error.TrunkCacheChangedBeforeExecution;
            const loan = try self.cache.consumePrepared(self.snapshot_value, entry, current);
            self.cursor += 1;
            return loan;
        }
        const bypass = try self.cache.consumePreparedBypass(self.snapshot_value, current);
        self.cursor += 1;
        return bypass;
    }
};

/// Actual model adapter's cold workload; Source owns this value at its final
/// address. Temporary arrays die before the original Source permit. Persistent
/// cache Entry/list bytes get their own exact declared Backing owners instead.
pub const ColdCall = struct {
    const Key = struct {
        probe: CacheProbe,
        state: trunk_cache.Cache.ColdKey,
        selected: bool = false,
    };
    allocator: std.mem.Allocator,
    cache: *trunk_cache.Cache,
    snapshot_value: trunk_cache.ColdSnapshot,
    config: modern.Config,
    digest: [32]u8,
    split_trees: bool,
    keys: std.ArrayListUnmanaged(Key) = .empty,
    case_keys: std.ArrayListUnmanaged(usize) = .empty,
    cases: std.ArrayListUnmanaged(Prepared.Case) = .empty,
    requirements: @import("../execution/prepared_storage_admission.zig").Builder,
    entries_extra: usize = 0,
    auxiliary_geometry: AuxiliaryGeometry = .{},
    workspace_declined: bool = false,
    auxiliary: ?struct { view: AuxiliaryView, backing: *@import("../execution/backing_retirement.zig").BackingOwner } = null,
    prepared: bool = false,
    receiver: ?Prepared = null,
    finished: bool = false,

    /// No selected Model/FIFO/lane may be held, including while checking an
    /// actual pending producer. Key/layout facts are copied and every hit owns
    /// its real Entry pin, never only a header address/capacity certificate.
    pub fn collectOutsideExecution(a: std.mem.Allocator, cfg: modern.Config, inputs: []const Tensor, cache: *trunk_cache.Cache, current: @import("../execution/cleanup_context.zig").Context) !?ColdCall {
        if (!cache.supportsColdOwnership()) return null;
        const laya = cfg.laya orelse return null;
        var row = metadataView(cfg, inputs) catch return null;
        if (row.trunk_sees_tree or row.fused) return null; // original full path
        const count = tree.treeCount(row);
        const indices = if (count != 1) try a.alloc(usize, row.questions()) else null;
        defer if (indices) |values| a.free(values);
        if (indices) |values| {
            for (values, 0..) |*value, index| value.* = index;
            row.question_index = values; // original seven-input implicit arange
        }
        const trees = if (count != 1) try tree.split(a, row) else null;
        defer if (trees) |values| {
            for (values) |value| value.deinit(a);
            a.free(values);
        };
        var first_probe: ?CacheProbe = null;
        for (0..count) |index| {
            const selected_row = if (trees) |values| values[index].row else row;
            if (cacheKeyForRow(cfg, laya, selected_row)) |probe| {
                first_probe = probe;
                break;
            }
        }
        const first = first_probe orelse return null; // original full geometry
        var snapshot_value = try cache.snapshotCold(first.key);
        // Inspect below gives ONE explicit owned pin per unique selected key.
        // Snapshot continues to own only the two real metadata owner pins.
        if (snapshot_value.hit) |entry| {
            cache.release(entry);
            snapshot_value.hit = null;
        }
        var self = ColdCall{
            .allocator = a,
            .cache = cache,
            .snapshot_value = snapshot_value,
            .config = cfg,
            .digest = rowDigest(row),
            .split_trees = false,
            .requirements = .{ .allocator = a },
        };
        errdefer self.finishOutsideExecution(current) catch @panic("unretired cold Laya collection");
        var any_hot = false;
        // The SAME anyTrunkCached key predicate controls CPU split versus
        // coalesced math. Cache min/limit only control each later lookup.
        for (0..count) |index| {
            const selected_row = if (trees) |values| values[index].row else row;
            const probe = cacheKeyForRow(cfg, laya, selected_row) orelse continue;
            var existing: ?usize = null;
            for (self.keys.items, 0..) |key, key_index| if (std.mem.eql(u8, &key.probe.key, &probe.key)) {
                existing = key_index;
                break;
            };
            if (existing == null) {
                const state = try cache.inspectColdKey(&self.snapshot_value, probe.key);
                self.keys.append(a, .{ .probe = probe, .state = state }) catch |err| {
                    if (state.entry) |entry| cache.release(entry);
                    return err;
                };
                existing = self.keys.items.len - 1;
            }
            const state = self.keys.items[existing.?].state;
            if (state.pending) return error.TrunkCachePendingBeforeExecution;
            any_hot = any_hot or state.entry != null;
        }
        self.split_trees = count != 1 and any_hot;
        if (count != 1 and !self.split_trees) return self; // original cold coalesced full call, no fill
        for (0..count) |index| {
            const selected_row = if (trees) |values| values[index].row else row;
            const probe = cacheProbe(cfg, laya, selected_row, self.snapshot_value.limit_bytes, self.snapshot_value.min_tokens) orelse continue;
            var key_index: usize = 0;
            while (!std.mem.eql(u8, &self.keys.items[key_index].probe.key, &probe.key)) : (key_index += 1) {}
            self.keys.items[key_index].selected = true;
            try self.case_keys.append(a, key_index);
            try self.cases.append(a, .{ .key = probe.key, .tokens = probe.tokens, .entry = null });
        }
        for (self.keys.items) |key| if (key.selected and key.state.entry == null) {
            self.entries_extra = try std.math.add(usize, self.entries_extra, 1);
        };
        var max_trunk: usize = 0;
        for (self.cases.items) |selected| max_trunk = @max(max_trunk, selected.tokens);
        self.auxiliary_geometry = try auxiliaryGeometry(cfg.num_hidden_layers + laya.head_layers, cfg.hidden_size, max_trunk);
        if (self.auxiliary_geometry.bytes != 0) try self.requirements.append(.{ .backend = .native, .class = .host_workspace, .bytes = self.auxiliary_geometry.bytes });
        // Two possible list publications, one pending birth plus two publish
        // mutations per unique miss, and at most every initial LRU victim.
        // Reserve counter headroom before any metadata/KV supplier or math.
        const steps = try std.math.add(usize, try std.math.add(usize, 2, try std.math.mul(usize, 3, self.entries_extra)), self.snapshot_value.entries.items.len);
        try cache.validateColdRevisionHeadroom(&self.snapshot_value, steps);
        if (self.cases.items.len != 0) {
            inline for (.{ .entries, .pending }) |which| {
                const geometry = try trunk_cache.coldListGeometry(&self.snapshot_value, which, self.entries_extra);
                if (geometry.bytes != 0) try self.requirements.append(.{ .backend = .native, .class = .host_workspace, .bytes = geometry.bytes });
            }
            for (self.keys.items) |key| if (key.selected and !key.state.metadata_owned) {
                const geometry = try trunk_cache.coldFillGeometry(key.probe.tokens, cfg.num_hidden_layers + laya.head_layers, cfg.hidden_size, self.snapshot_value.precision);
                try self.requirements.append(.{ .backend = .native, .class = .host_workspace, .bytes = geometry.metadata_bytes });
            };
        }
        return self;
    }

    pub fn physicalRequirements(self: *const ColdCall) []const @import("../execution/prepared_storage_admission.zig").Requirement {
        const all = self.requirements.items();
        return if (self.workspace_declined and self.auxiliary_geometry.bytes != 0) all[1..] else all;
    }
    pub fn declineAdditionalWorkspace(self: *ColdCall) !void {
        if (self.prepared or self.finished or self.auxiliary != null) return error.InvalidPreparedTrunkCacheCall;
        self.workspace_declined = true;
    }
    fn prepareAuxiliaryOutsideExecution(self: *ColdCall, capability: @import("../execution/storage_admission.zig").Capability) !void {
        if (self.workspace_declined) return;
        const geometry = self.auxiliary_geometry;
        if (geometry.bytes == 0) return;
        const binding = try capability.reserveBacking(self.allocator, .native, .host_workspace, geometry.bytes) orelse return error.UnpreparedPhysicalStorageReservation;
        errdefer binding.owner.release(binding.construction_epoch);
        try trunk_cache.validateMetadataBinding(binding, capability, geometry.bytes);
        const values = try self.allocator.alloc(f32, geometry.elements);
        errdefer self.allocator.free(values);
        const captures = try self.allocator.alloc([]f32, geometry.headers);
        errdefer self.allocator.free(captures);
        const prefixes = try self.allocator.alloc(ops.CT, geometry.headers);
        self.auxiliary = .{ .view = .{ .values = values, .capture_headers = captures, .prefix_headers = prefixes }, .backing = binding.owner };
    }

    /// All optional MAX + exact cache metadata funding is already admitted in
    /// the SAME original task's separate supplemental owner. No original floor
    /// is reacquired, reduced or lent. All callbacks/allocations remain outside.
    pub fn prepareOutsideExecution(self: *ColdCall, capability: @import("../execution/storage_admission.zig").Capability, domain: @import("../runtime/tier/admission_domain.zig").Pin, current: @import("../execution/cleanup_context.zig").Context) !void {
        if (self.finished or self.prepared) return error.InvalidPreparedTrunkCacheCall;
        try self.prepareAuxiliaryOutsideExecution(capability);
        if (self.cases.items.len != 0) {
            var entries_plan = try self.cache.prepareListOutsideExecution(&self.snapshot_value, .entries, self.entries_extra, capability);
            defer entries_plan.abortOutsideExecution();
            var pending_plan = try self.cache.prepareListOutsideExecution(&self.snapshot_value, .pending, self.entries_extra, capability);
            defer pending_plan.abortOutsideExecution();
            try self.cache.commitColdListsOutsideExecution(&self.snapshot_value, &entries_plan, &pending_plan, current);
            for (self.keys.items) |*key| if (key.selected) {
                if (key.state.entry) |entry| {
                    if (!key.state.metadata_owned) {
                        var hit_snapshot = self.snapshot_value;
                        hit_snapshot.hit = entry; // borrows this key's existing owned pin
                        try self.cache.prepareHitOwnershipOutsideExecution(&hit_snapshot, capability, domain);
                    }
                } else {
                    key.state.entry = try self.cache.prepareFillOutsideExecution(&self.snapshot_value, key.probe.key, key.probe.tokens, self.config.num_hidden_layers + self.config.laya.?.head_layers, self.config.hidden_size, capability, domain);
                }
            };
            for (self.cases.items, self.case_keys.items) |*selected, index| selected.entry = self.keys.items[index].state.entry;
        }
        try self.cache.validateCold(&self.snapshot_value);
        self.prepared = true;
    }

    pub fn validateBeforeExecution(self: *ColdCall) !void {
        if (!self.prepared or self.finished) return error.InvalidPreparedTrunkCacheCall;
        try self.cache.validateCold(&self.snapshot_value);
        for (self.cases.items) |selected| if (selected.entry) |entry| {
            try self.cache.validateColdEntry(&self.snapshot_value, entry, selected.key, selected.tokens, self.config.num_hidden_layers + self.config.laya.?.head_layers, self.config.hidden_size);
        };
    }

    /// Called only once this ColdCall is embedded at Source's final address.
    /// No helper-local binding, Epoch or Context pointer is returned by value.
    pub fn receiverAtFinalAddress(self: *ColdCall) !*Prepared {
        try self.validateBeforeExecution();
        if (self.receiver == null) self.receiver = .{
            .cache = self.cache,
            .snapshot_value = &self.snapshot_value,
            .cases = self.cases.items,
            .config = self.config,
            .digest = self.digest,
            .split_trees = self.split_trees,
            .auxiliary = if (self.auxiliary) |held| held.view else null,
            .workspace_declined = self.workspace_declined,
        };
        return &self.receiver.?;
    }
    pub fn finishOutsideExecution(self: *ColdCall, current: @import("../execution/cleanup_context.zig").Context) !void {
        if (self.finished) return;
        self.receiver = null; // all synchronous header/Model loans already ended
        for (self.keys.items) |*key| if (key.state.entry) |entry| {
            try self.cache.finishPreparedEntryOutsideExecution(entry, current);
            key.state.entry = null;
        };
        try self.snapshot_value.finishOutsideExecution(self.cache, current);
        if (self.auxiliary) |held| {
            // All original CT children/headers are already gone with the
            // synchronous receiving CB. Actual arrays die before quota/B.
            self.allocator.free(held.view.prefix_headers);
            self.allocator.free(held.view.capture_headers);
            self.allocator.free(held.view.values);
            if (current.boundary) |epoch| held.backing.release(epoch) else held.backing.releaseOutsideExecution();
            self.auxiliary = null;
        }
        self.keys.deinit(self.allocator);
        self.case_keys.deinit(self.allocator);
        self.cases.deinit(self.allocator);
        self.requirements.deinit();
        self.finished = true;
    }
};

pub fn rowDigest(row: tree.Row) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-laya-packed-receiving/v1");
    inline for (.{ "ids", "positions", "segments", "parents", "kinds", "markers", "anchors" }) |name| {
        const values = @field(row, name);
        const count = values.len;
        hash.update(std.mem.asBytes(&count));
        hash.update(std.mem.sliceAsBytes(values));
    }
    hash.update(std.mem.asBytes(&row.width));
    hash.update(std.mem.asBytes(&row.trunk_sees_tree));
    hash.update(std.mem.asBytes(&row.fused));
    return hash.finalResult();
}

fn byteRange(values: anytype) ![2]usize {
    const bytes = try std.math.mul(usize, values.len, @sizeOf(@TypeOf(values[0])));
    const first = @intFromPtr(values.ptr);
    return .{ first, try std.math.add(usize, first, bytes) };
}
fn rangesOverlap(a: [2]usize, b: [2]usize) bool {
    return a[0] < a[1] and b[0] < b[1] and a[0] < b[1] and b[0] < a[1];
}
fn validateAuxiliary(cb: *const ops.ComputeBackend, prepared_call: *const Prepared) !void {
    if (cb.vtable != &@import("../ops/native_compute.zig").vtable_impl) return error.UnsupportedPreparedTrunkCache;
    const context = cb.call_context orelse return error.MissingCurrentCleanupEpoch;
    const loan = context.native_weights orelse return error.ForeignNativeWeightModel;
    if (prepared_call.workspace_declined) {
        if (loan.matrix_scratch != null or prepared_call.auxiliary != null) return error.InvalidAssignedHostWorkspace;
        return; // exact cold cache receipt; original protected numerical scratch route
    }
    const matrix_values = loan.matrix_scratch orelse return error.UnpreparedPhysicalStorageReservation;
    var max_trunk: usize = 0;
    for (prepared_call.cases) |selected| max_trunk = @max(max_trunk, selected.tokens);
    const geometry = try auxiliaryGeometry(prepared_call.config.num_hidden_layers + prepared_call.config.laya.?.head_layers, prepared_call.config.hidden_size, max_trunk);
    if (geometry.bytes == 0) return;
    const bank = prepared_call.auxiliary orelse return error.UnpreparedPhysicalStorageReservation;
    if (bank.values.len < geometry.elements or bank.capture_headers.len < geometry.headers or bank.prefix_headers.len < geometry.headers)
        return error.InvalidAssignedHostWorkspace;
    const ranges = [_][2]usize{ try byteRange(matrix_values), try byteRange(bank.values), try byteRange(bank.capture_headers), try byteRange(bank.prefix_headers) };
    for (ranges, 0..) |range, index| for (ranges[index + 1 ..]) |other| {
        if (rangesOverlap(range, other)) return error.AssignedPhysicalStorageAlias;
    };
    for (prepared_call.cases) |selected| if (selected.entry) |entry| {
        if (rangesOverlap(ranges[1], try byteRange(entry.host16)) or rangesOverlap(ranges[1], try byteRange(entry.host32)))
            return error.AssignedPhysicalStorageAlias;
    };
}

pub fn runWithPrepared(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, inputs: []const Tensor, cache: ?*trunk_cache.Cache, prepared_call: *Prepared) ![]Tensor {
    if (prepared_call.entered or cache != prepared_call.cache or deviceCache(cb) or !std.meta.eql(cfg, prepared_call.config))
        return error.InvalidPreparedTrunkCacheCall;
    const context = cb.call_context orelse return error.MissingCurrentCleanupEpoch;
    const epoch = context.cleanup.boundary orelse return error.MissingCurrentCleanupEpoch;
    if (try epoch.isSealed()) return error.RetirementEpochSealed;
    const laya = cfg.laya orelse return error.InvalidLayaConfig;
    const row = try view(a, cfg, inputs);
    defer row.deinit(a);
    const digest = rowDigest(row);
    if (!std.mem.eql(u8, &digest, &prepared_call.digest)) return error.TrunkCacheChangedBeforeExecution;
    try validateAuxiliary(cb, prepared_call);
    try prepared_call.cache.beginPreparedCall(prepared_call.snapshot_value);
    defer prepared_call.cache.endPreparedCall();
    prepared_call.entered = true; // one original numerical call, never replay
    const result = try forwardRowImpl(cb, a, cfg, laya, row, cache, prepared_call);
    errdefer {
        for (result) |*output| output.deinit();
        a.free(result);
    }
    if (prepared_call.cursor != prepared_call.cases.len) return error.UnusedPreparedTrunkCacheLookup;
    return result;
}

const Laya = @import("../models/laya.zig").Config;

/// Encoder and decision head for one validated row. With a cache on the CPU
/// backend, the trunk's per-layer keys and values are reused across rows and
/// requests, and only the branch tokens are encoded (laya_trunk_cache.zig).
pub fn forwardRow(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, row: tree.Row, cache: ?*trunk_cache.Cache) anyerror![]Tensor {
    return forwardRowImpl(cb, a, cfg, laya, row, cache, null);
}
fn forwardRowImpl(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, row: tree.Row, cache: ?*trunk_cache.Cache, prepared_call: ?*Prepared) anyerror![]Tensor {
    const store = cache orelse return forwardFull(cb, a, cfg, laya, row);
    // A trunk that sees its questions depends on the question set. With
    // per-question upper layers only the layers below are shared; caching
    // them is not implemented yet.
    if (row.trunk_sees_tree or row.fused) return forwardFull(cb, a, cfg, laya, row);
    // A multi-row batch (pipelines/laya.zig, laya_tree.coalesce) packs
    // several states' trees into one row. A cold batch runs as one call:
    // segment attention keeps its cost proportional to visible keys. On the
    // CPU, once any of its trunks is cached, each tree runs on its own so hot
    // trunks are reused rather than re-encoded (and cold ones fill the
    // cache). On Metal the batch stays one call: per-call overhead outweighs
    // the saved trunk work (a fully hot four-state batch on the synthetic
    // model: 6.6 ms batched against 25.5 ms per tree; the CPU goes 34.6 ms
    // to 28.7 ms the other way).
    if (tree.treeCount(row) != 1) {
        const split = if (prepared_call) |call| call.split_trees else !deviceCache(cb) and anyTrunkCached(a, cfg, laya, row, store);
        return if (split)
            if (prepared_call != null) forwardTreesImpl(cb, a, cfg, laya, row, store, prepared_call) else forwardTrees(cb, a, cfg, laya, row, store)
        else
            forwardFull(cb, a, cfg, laya, row);
    }
    const probe = cacheProbe(cfg, laya, row, store.limit_bytes, store.min_tokens) orelse return forwardFull(cb, a, cfg, laya, row);
    const trunk = probe.tokens;
    const layers = cfg.num_hidden_layers + laya.head_layers;
    const key = probe.key;
    // The entry's bytes and admission lease are reserved before encoding;
    // a trunk that cannot fit runs uncached, and a concurrent miss on the
    // same state waits for this fill rather than repeating it.
    const selected = if (prepared_call) |call|
        try call.lookup(key, trunk, layers, cfg.hidden_size, (cb.call_context orelse return error.MissingCurrentCleanupEpoch).cleanup)
    else
        try store.lookup(key, trunk, layers, cfg.hidden_size, !deviceCache(cb));
    const entry = switch (selected) {
        .hit => |hit| hit,
        .fill => |created| blk: {
            const filled = if (prepared_call) |call|
                fillTrunkWithAuxiliary(cb, a, cfg, laya, row.ids[0..trunk], probe.first_position, created, store.allocator, call.auxiliary)
            else
                fillTrunk(cb, a, cfg, laya, row.ids[0..trunk], probe.first_position, created, store.allocator);
            filled catch |err| {
                store.abandon(created);
                return err;
            };
            if (prepared_call) |call| store.publishPrepared(created, call.snapshot_value) catch |err| {
                store.abandon(created);
                return err;
            } else store.publish(created);
            break :blk created;
        },
        .bypass => return forwardFull(cb, a, cfg, laya, row),
    };
    defer store.release(entry);
    return if (prepared_call) |call| forwardCachedWithAuxiliary(cb, a, cfg, laya, row, entry, call.auxiliary) else forwardCached(cb, a, cfg, laya, row, entry);
}

/// Whether any tree of a multi-tree row has its trunk in `store`.
fn anyTrunkCached(a: std.mem.Allocator, cfg: modern.Config, laya: Laya, row: tree.Row, store: *trunk_cache.Cache) bool {
    const trees = tree.split(a, row) catch return false;
    defer {
        for (trees) |t| t.deinit(a);
        a.free(trees);
    }
    for (trees) |t| {
        const probe = cacheKeyForRow(cfg, laya, t.row) orelse continue;
        if (store.contains(probe.key)) return true;
    }
    return false;
}

/// `forwardRow` per tree of a multi-tree row, with the outputs put back in
/// the row's question order (`laya_tree.split` keeps its width).
fn forwardTrees(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, row: tree.Row, store: *trunk_cache.Cache) anyerror![]Tensor {
    return forwardTreesImpl(cb, a, cfg, laya, row, store, null);
}
fn forwardTreesImpl(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, row: tree.Row, store: *trunk_cache.Cache, prepared_call: ?*Prepared) anyerror![]Tensor {
    const trees = try tree.split(a, row);
    defer {
        for (trees) |t| t.deinit(a);
        a.free(trees);
    }
    const questions = row.questions();
    const logits = try a.alloc(f32, questions * row.width);
    defer a.free(logits);
    const acts = try a.alloc(f32, questions * laya.n_act);
    defer a.free(acts);
    for (trees) |t| {
        const outputs = try forwardRowImpl(cb, a, cfg, laya, t.row, store, prepared_call);
        defer {
            for (outputs) |*output| output.deinit();
            a.free(outputs);
        }
        const tree_logits = outputs[0].asFloat32();
        const tree_acts = outputs[1].asFloat32();
        if (tree_logits.len != t.questions.len * row.width or tree_acts.len != t.questions.len * laya.n_act) return error.InvalidLayaInputs;
        for (t.questions, 0..) |q, i| {
            @memcpy(logits[q * row.width ..][0..row.width], tree_logits[i * row.width ..][0..row.width]);
            @memcpy(acts[q * laya.n_act ..][0..laya.n_act], tree_acts[i * laya.n_act ..][0..laya.n_act]);
        }
    }
    const result = try a.alloc(Tensor, 2);
    errdefer a.free(result);
    result[0] = try Tensor.initFloat32(a, "logits", &.{ @intCast(questions), @intCast(row.width) }, logits);
    errdefer result[0].deinit();
    result[1] = try Tensor.initFloat32(a, "action_logits", &.{ @intCast(questions), @intCast(laya.n_act) }, acts);
    return result;
}

pub const CacheProbe = struct { key: [32]u8, tokens: usize, first_position: usize };

/// SAME positive cache lookup selector used by the real numerical caller and
/// the selected cold adapter. Fused/trunk-sees, full trunk and short/disabled
/// paths do not invent a fill or a selected cache allocation.
pub fn cacheProbe(cfg: modern.Config, laya: Laya, row: tree.Row, limit_bytes: usize, min_tokens: usize) ?CacheProbe {
    if (row.trunk_sees_tree or row.fused or tree.treeCount(row) != 1) return null;
    const probe = cacheKeyForRow(cfg, laya, row) orelse return null;
    if (limit_bytes == 0 or probe.tokens < min_tokens or probe.tokens == row.ids.len) return null;
    return probe;
}
pub fn cacheKeyForRow(cfg: modern.Config, laya: Laya, row: tree.Row) ?CacheProbe {
    const layout = trunkRows(row) orelse return null;
    return .{ .key = trunk_cache.Cache.keyAt(row.ids[0..layout.tokens], cfg.num_hidden_layers + laya.head_layers, cfg.hidden_size, layout.first_position), .tokens = layout.tokens, .first_position = layout.first_position };
}

const TrunkRows = struct { tokens: usize, first_position: usize };

/// Trunk length and first position when the trunk is exactly rows `0..T` at
/// contiguous positions (`0..T-1`, or after the question budget with
/// `packing.question_first`).
fn trunkRows(row: tree.Row) ?TrunkRows {
    if (row.ids.len == 0 or row.segments[0] != 0 or row.positions[0] < 0) return null;
    const first = row.positions[0];
    var trunk: usize = 0;
    while (trunk < row.ids.len and row.segments[trunk] == 0) : (trunk += 1) {
        if (row.positions[trunk] != first + @as(i64, @intCast(trunk))) return null;
    }
    for (row.segments[trunk..]) |segment| if (segment == 0) return null;
    return .{ .tokens = trunk, .first_position = @intCast(first) };
}

/// Trunk tensors stay on Metal devices across requests; elsewhere they are
/// host memory and re-uploaded per request (cheap on CPU).
fn deviceCache(cb: *const ops.ComputeBackend) bool {
    return build_options.enable_metal and cb.kind() == .metal;
}

const DeviceTrunk = if (build_options.enable_metal) struct {
    /// `[layer][keys, values]`; half tensors when `half`.
    tensors: []MetalTensor,
    half: bool,

    fn destroy(raw: ?*anyopaque, allocator: std.mem.Allocator) void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        for (self.tensors) |*tensor| tensor.deinit();
        allocator.free(self.tensors);
        allocator.destroy(self);
    }
} else struct {};

/// Encode the trunk alone (it never attends to a branch) and keep every
/// encoder and head layer's keys and values in `entry`, at its precision.
fn fillTrunk(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, ids: []const i64, first_position: usize, entry: *trunk_cache.Entry, cache_allocator: std.mem.Allocator) !void {
    return fillTrunkWithAuxiliary(cb, a, cfg, laya, ids, first_position, entry, cache_allocator, null);
}
fn fillTrunkWithAuxiliary(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, ids: []const i64, first_position: usize, entry: *trunk_cache.Entry, cache_allocator: std.mem.Allocator, auxiliary: ?AuxiliaryView) !void {
    const n = cfg.num_hidden_layers;
    if (comptime build_options.enable_metal) if (deviceCache(cb)) {
        const tensors = try a.alloc(?ops.CT, 2 * entry.layers);
        defer a.free(tensors);
        @memset(tensors, null);
        defer for (tensors) |tensor| if (tensor) |t| cb.free(t);
        const keys = tensors[0..entry.layers];
        const values = tensors[entry.layers..];
        const encoded = try modern.forwardCapturingCT(cb, a, cfg, ids, first_position, .{ .key_tensors = keys[0..n], .value_tensors = values[0..n] });
        defer cb.free(encoded);
        try head.captureTrunk(cb, a, laya, encoded, ids.len, cfg.hidden_size, .{ .key_tensors = keys[n..], .value_tensors = values[n..] });
        const compute: *metal_compute.MetalCompute = @ptrCast(@alignCast(cb.ptr));
        const half = entry.precision == .f16;
        const trunk = try cache_allocator.create(DeviceTrunk);
        errdefer cache_allocator.destroy(trunk);
        trunk.* = .{ .tensors = try cache_allocator.alloc(MetalTensor, 2 * entry.layers), .half = half };
        var retained: usize = 0;
        errdefer {
            for (trunk.tensors[0..retained]) |*tensor| tensor.deinit();
            cache_allocator.free(trunk.tensors);
        }
        for (0..entry.layers) |layer| for ([_]?ops.CT{ keys[layer], values[layer] }) |tensor| {
            trunk.tensors[retained] = (if (half)
                try compute.deviceHalfCopy(tensor.?)
            else
                try metal_compute.MetalCompute.retainDenseDeviceTensor(tensor.?)) orelse return error.UnsupportedLayaDeviceCache;
            retained += 1;
        };
        entry.device = trunk;
        entry.device_deinit = DeviceTrunk.destroy;
        return;
    };
    const width = ids.len * cfg.hidden_size;
    const scratch = if (auxiliary) |bank| bank.values[0 .. 2 * entry.layers * width] else try a.alloc(f32, 2 * entry.layers * width);
    defer if (auxiliary == null) a.free(scratch);
    const keys = if (auxiliary) |bank| bank.capture_headers[0..entry.layers] else try a.alloc([]f32, entry.layers);
    defer if (auxiliary == null) a.free(keys);
    const values = if (auxiliary) |bank| bank.capture_headers[entry.layers .. 2 * entry.layers] else try a.alloc([]f32, entry.layers);
    defer if (auxiliary == null) a.free(values);
    for (keys, values, 0..) |*k, *v, layer| {
        k.* = scratch[(2 * layer) * width ..][0..width];
        v.* = scratch[(2 * layer + 1) * width ..][0..width];
    }
    const encoded = try modern.forwardCapturingCT(cb, a, cfg, ids, first_position, .{ .keys = keys[0..n], .values = values[0..n] });
    defer cb.free(encoded);
    try head.captureTrunk(cb, a, laya, encoded, ids.len, cfg.hidden_size, .{ .keys = keys[n..], .values = values[n..] });
    for (0..2 * entry.layers) |slot| entry.store(slot, scratch[slot * width ..][0..width]);
}

/// A request-local f32 tensor for one cached trunk layer (`which` 0 = keys).
fn trunkTensor(cb: *const ops.ComputeBackend, a: std.mem.Allocator, entry: *const trunk_cache.Entry, layer: usize, which: usize) !ops.CT {
    if (comptime build_options.enable_metal) if (entry.device) |raw| {
        const trunk: *const DeviceTrunk = @ptrCast(@alignCast(raw));
        const compute: *metal_compute.MetalCompute = @ptrCast(@alignCast(cb.ptr));
        const tensor = &trunk.tensors[2 * layer + which];
        return if (trunk.half) compute.ctFromHalfDeviceTensor(tensor) else compute.ctFromRetainedDeviceTensor(tensor);
    };
    const values = try a.alloc(f32, entry.tokens * entry.hidden);
    defer a.free(values);
    entry.load(2 * layer + which, values);
    return cb.fromFloat32Shape(values, &.{ @intCast(entry.tokens), @intCast(entry.hidden) });
}

fn trunkTensorBorrowed(cb: *const ops.ComputeBackend, entry: *const trunk_cache.Entry, layer: usize, which: usize, values: []f32) !ops.CT {
    if (cb.vtable != &@import("../ops/native_compute.zig").vtable_impl or cb.vtable.fromBorrowedFloat32Shape == null or entry.device != null)
        return error.UnsupportedPreparedTrunkCache;
    const width = try std.math.mul(usize, entry.tokens, entry.hidden);
    const offset = try std.math.mul(usize, 2 * layer + which, width);
    if (offset > values.len or width > values.len - offset) return error.InvalidAssignedHostWorkspace;
    const span = values[offset..][0..width];
    entry.load(2 * layer + which, span);
    // Original fromFloat32Shape copied these exact values. Native synchronous
    // attention reads them only; all CT children die before reuse/Source close.
    return cb.fromBorrowedFloat32Shape(span, &.{ @intCast(entry.tokens), @intCast(entry.hidden) });
}

fn forwardCached(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, row: tree.Row, entry: *const trunk_cache.Entry) ![]Tensor {
    return forwardCachedWithAuxiliary(cb, a, cfg, laya, row, entry, null);
}
fn forwardCachedWithAuxiliary(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, row: tree.Row, entry: *const trunk_cache.Entry, auxiliary: ?AuxiliaryView) ![]Tensor {
    const trunk = entry.tokens;
    const hidden = cfg.hidden_size;
    const segments = try tree.ranges(a, row, trunk);
    defer a.free(segments);
    const key_positions = try tree.positions32(a, row.positions);
    defer a.free(key_positions);
    const keys = if (auxiliary) |bank| bank.prefix_headers[0..entry.layers] else try a.alloc(ops.CT, entry.layers);
    defer if (auxiliary == null) a.free(keys);
    const values = if (auxiliary) |bank| bank.prefix_headers[entry.layers .. 2 * entry.layers] else try a.alloc(ops.CT, entry.layers);
    defer if (auxiliary == null) a.free(values);
    var uploaded: usize = 0;
    defer for (keys[0..uploaded], values[0..uploaded]) |k, v| {
        cb.free(k);
        cb.free(v);
    };
    for (keys, values, 0..) |*k, *v, layer| {
        k.* = if (auxiliary) |bank| try trunkTensorBorrowed(cb, entry, layer, 0, bank.values) else try trunkTensor(cb, a, entry, layer, 0);
        v.* = (if (auxiliary) |bank| trunkTensorBorrowed(cb, entry, layer, 1, bank.values) else trunkTensor(cb, a, entry, layer, 1)) catch |err| {
            cb.free(k.*);
            return err;
        };
        uploaded += 1;
    }
    const layers = cfg.num_hidden_layers;
    const branch: modern.Packed = .{ .positions = row.positions[trunk..], .ranges = segments, .key_positions = key_positions };
    const encoded = try modern.forwardBranchesCT(cb, a, cfg, row.ids[trunk..], branch, .{ .prefix_rows = trunk, .keys = keys[0..layers], .values = values[0..layers] });
    defer cb.free(encoded);
    return head.forwardPackedBranches(cb, a, laya, encoded, branch, row.kinds[trunk..], row.markers, row.anchors, row.width, hidden, .{ .rows = trunk, .keys = keys[layers..], .values = values[layers..] });
}

/// Encoder and decision head over every token of one validated row.
pub fn forwardFull(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: modern.Config, laya: Laya, row: tree.Row) ![]Tensor {
    const segments = try tree.ranges(a, row, 0);
    defer a.free(segments);
    const upper = if (row.fused) try tree.ranges(a, row.upper(), 0) else null;
    defer if (upper) |u| a.free(u);
    const key_positions = try tree.positions32(a, row.positions);
    defer a.free(key_positions);
    const packed_row: modern.Packed = .{
        .positions = row.positions,
        .ranges = segments,
        .key_positions = key_positions,
        .upper_ranges = upper,
        .upper_from = laya.packing.fuseFrom(cfg.num_hidden_layers + laya.head_layers),
    };
    const encoded = try modern.forwardPackedCT(cb, a, cfg, row.ids, packed_row);
    defer cb.free(encoded);
    return head.forwardPacked(cb, a, laya, encoded, packed_row.after(cfg.num_hidden_layers), row.kinds, row.markers, row.anchors, row.width, cfg.hidden_size);
}

/// Peak transient bytes for one row: FFN and projection activations plus
/// one head's segment-attention staging. Packed attention keeps no
/// `[L, L]` state. Packed rows are always batch 1.
pub fn workspaceBytes(cfg: modern.Config, sequence: usize) !usize {
    const mul = std.math.mul;
    const add = std.math.add;
    const activations = try mul(usize, sequence, try add(usize, try mul(usize, cfg.hidden_size, 8), try mul(usize, cfg.intermediate_size, 4)));
    const staging = try add(usize, try mul(usize, try mul(usize, sequence, 4), cfg.hidden_size / @max(cfg.num_attention_heads, 1)), 64 * 256);
    return mul(usize, try add(usize, activations, staging), @sizeOf(f32));
}
