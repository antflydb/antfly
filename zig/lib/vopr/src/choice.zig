// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

const std = @import("std");
const ids = @import("id.zig");
const trace = @import("trace.zig");
const transition = @import("transition.zig");

pub const Request = struct {
    site_id: ids.StableId,
    site_name: []const u8,
    occurrence: u64,
    enabled: []const transition.Transition,
};

pub const Source = struct {
    ptr: *anyopaque,
    choose_fn: *const fn (*anyopaque, Request) anyerror!ids.StableId,
    finish_fn: *const fn (*anyopaque) anyerror!void,

    pub fn choose(self: Source, request: Request) !ids.StableId {
        if (request.enabled.len == 0) return error.EmptyChoiceSet;
        const selected = try self.choose_fn(self.ptr, request);
        for (request.enabled) |alternative| if (alternative.id == selected) return selected;
        return error.ChoiceSourceSelectedDisabledAlternative;
    }

    pub fn finish(self: Source) !void {
        try self.finish_fn(self.ptr);
    }
};

pub const Seeded = struct {
    prng: std.Random.DefaultPrng,

    pub fn init(seed: u64) Seeded {
        return .{ .prng = std.Random.DefaultPrng.init(seed) };
    }

    pub fn source(self: *Seeded) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *Seeded = @ptrCast(@alignCast(ptr));
        const index = self.prng.random().intRangeLessThan(usize, 0, request.enabled.len);
        return request.enabled[index].id;
    }

    fn finish(_: *anyopaque) !void {}
};

pub const Scripted = struct {
    selections: []const ids.StableId,
    cursor: usize = 0,

    pub fn source(self: *Scripted) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, _: Request) !ids.StableId {
        const self: *Scripted = @ptrCast(@alignCast(ptr));
        if (self.cursor >= self.selections.len) return error.ScriptExhausted;
        defer self.cursor += 1;
        return self.selections[self.cursor];
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *Scripted = @ptrCast(@alignCast(ptr));
        if (self.cursor != self.selections.len) return error.UnusedScriptChoices;
    }
};

pub const Replay = struct {
    records: []const trace.ChoiceRecord,
    cursor: usize = 0,

    pub fn source(self: *Replay) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *Replay = @ptrCast(@alignCast(ptr));
        if (self.cursor >= self.records.len) return error.ReplayChoiceExhausted;
        const record = self.records[self.cursor];
        if (record.site_id != request.site_id or !std.mem.eql(u8, record.site_name, request.site_name) or record.occurrence != request.occurrence) return error.ReplayChoiceSiteDiverged;
        if (record.enabled_ids.len != request.enabled.len) return error.ReplayEnabledSetDiverged;
        for (request.enabled, record.enabled_ids) |enabled, recorded_id| {
            if (enabled.id != recorded_id) return error.ReplayEnabledSetDiverged;
        }
        self.cursor += 1;
        return record.selected_id;
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *Replay = @ptrCast(@alignCast(ptr));
        if (self.cursor != self.records.len) return error.ReplayHasTrailingChoices;
    }
};

/// Replays an exact prefix, replaces one decision with a requested enabled
/// alternative, then generates a new suffix. This is the basic branching
/// primitive; it rebuilds a clean world and never snapshots pointer state.
pub const Mutating = struct {
    base: []const trace.ChoiceRecord,
    mutation_index: usize,
    replacement_id: ids.StableId,
    prng: std.Random.DefaultPrng,
    cursor: usize = 0,
    mutated: bool = false,

    pub fn init(base: []const trace.ChoiceRecord, mutation_index: usize, replacement_id: ids.StableId, suffix_seed: u64) Mutating {
        return initAt(base, mutation_index, replacement_id, suffix_seed, 0);
    }

    /// Resume at an already-restored exact prefix. The caller is responsible
    /// for binding the restored state to `base[0..cursor]`.
    pub fn initAt(
        base: []const trace.ChoiceRecord,
        mutation_index: usize,
        replacement_id: ids.StableId,
        suffix_seed: u64,
        cursor: usize,
    ) Mutating {
        return .{
            .base = base,
            .mutation_index = mutation_index,
            .replacement_id = replacement_id,
            .prng = std.Random.DefaultPrng.init(suffix_seed),
            .cursor = cursor,
        };
    }

    pub fn source(self: *Mutating) Source {
        return .{ .ptr = self, .choose_fn = choose, .finish_fn = finish };
    }

    fn choose(ptr: *anyopaque, request: Request) !ids.StableId {
        const self: *Mutating = @ptrCast(@alignCast(ptr));
        const index = self.cursor;
        self.cursor += 1;
        if (index < self.mutation_index) {
            if (index >= self.base.len) return error.MutationPrefixExhausted;
            const record = self.base[index];
            try verifyRecord(record, request);
            return record.selected_id;
        }
        if (index == self.mutation_index) {
            if (index >= self.base.len) return error.MutationPointOutOfRange;
            const record = self.base[index];
            try verifySite(record, request);
            if (record.selected_id == self.replacement_id) return error.MutationDidNotChangeChoice;
            self.mutated = true;
            return self.replacement_id;
        }
        const selected_index = self.prng.random().intRangeLessThan(usize, 0, request.enabled.len);
        return request.enabled[selected_index].id;
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *Mutating = @ptrCast(@alignCast(ptr));
        if (!self.mutated) return error.MutationPointNotReached;
    }
};

fn verifySite(record: trace.ChoiceRecord, request: Request) !void {
    if (record.site_id != request.site_id or !std.mem.eql(u8, record.site_name, request.site_name) or record.occurrence != request.occurrence) return error.ReplayChoiceSiteDiverged;
}

fn verifyRecord(record: trace.ChoiceRecord, request: Request) !void {
    try verifySite(record, request);
    if (record.enabled_ids.len != request.enabled.len) return error.ReplayEnabledSetDiverged;
    for (request.enabled, record.enabled_ids) |enabled, recorded_id| {
        if (enabled.id != recorded_id) return error.ReplayEnabledSetDiverged;
    }
}

test "replay diagnoses enabled-set divergence" {
    const records = [_]trace.ChoiceRecord{.{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled_ids = &.{ 1, 2 }, .selected_id = 1 }};
    const enabled = [_]transition.Transition{
        .{ .id = 1, .name = "one", .kind = .workload },
        .{ .id = 3, .name = "three", .kind = .workload },
    };
    var replay = Replay{ .records = &records };
    try std.testing.expectError(error.ReplayEnabledSetDiverged, replay.source().choose(.{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled = &enabled }));
}

test "mutating choice source replays a prefix and branches" {
    const records = [_]trace.ChoiceRecord{
        .{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled_ids = &.{ 1, 2 }, .selected_id = 1 },
        .{ .site_id = 7, .site_name = "site", .occurrence = 1, .enabled_ids = &.{ 1, 2 }, .selected_id = 1 },
    };
    const enabled = [_]transition.Transition{
        .{ .id = 1, .name = "one", .kind = .workload },
        .{ .id = 2, .name = "two", .kind = .workload },
    };
    var mutating = Mutating.init(&records, 1, 2, 99);
    const source = mutating.source();
    try std.testing.expectEqual(@as(u64, 1), try source.choose(.{ .site_id = 7, .site_name = "site", .occurrence = 0, .enabled = &enabled }));
    try std.testing.expectEqual(@as(u64, 2), try source.choose(.{ .site_id = 7, .site_name = "site", .occurrence = 1, .enabled = &enabled }));
    try source.finish();
}
