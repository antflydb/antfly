// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Immutable, bounded recovery input. Validate the complete saved selection
//! before sending anything; replay never obtains newer status observations.
const std = @import("std");
const client_mod = @import("antfly-client");
const types = client_mod.types;

pub const max_file_bytes = 32 * 1024 * 1024;
pub const max_line_bytes = 256 * 1024;
pub const Envelope = struct {
    table: []const u8,
    index: []const u8,
    action: enum { retry, repair },
    request: types.IndexMaintenanceRequest,
};

fn decimal(value: []const u8, positive: bool) bool {
    if (value.len == 0 or value.len > 20 or (value.len > 1 and value[0] == '0')) return false;
    for (value) |byte| if (byte < '0' or byte > '9') return false;
    const number = std.fmt.parseInt(u64, value, 10) catch return false;
    return !positive or number > 0;
}

fn digest(value: []const u8) bool {
    if (value.len != 64) return false;
    for (value) |byte| if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return false;
    return true;
}

pub fn validateRequest(request: types.IndexMaintenanceRequest) !void {
    if (!decimal(request.table_id, true) or std.math.cast(u32, request.schema_version) == null or
        request.owners.len == 0 or request.owners.len > 128) return error.InvalidMaintenanceRecovery;
    for (request.owners, 0..) |owner, i| {
        if (!decimal(owner.group_id, true) or !decimal(owner.generation, true) or !decimal(owner.maintenance_epoch, false) or
            std.math.cast(u32, owner.slot) == null or !digest(owner.owner) or !digest(owner.comparison) or
            !digest(owner.progress_digest)) return error.InvalidMaintenanceRecovery;
        for (request.owners[0..i]) |prior| if (std.mem.eql(u8, prior.group_id, owner.group_id)) return error.InvalidMaintenanceRecovery;
    }
}

pub fn validateAcknowledgement(request: types.IndexMaintenanceRequest, response: types.IndexMaintenanceResponse) !void {
    if (response.acknowledged_groups.len != request.owners.len) return error.InvalidMaintenanceResponse;
    for (request.owners) |owner| {
        var matches: usize = 0;
        for (response.acknowledged_groups) |group| if (std.mem.eql(u8, group, owner.group_id)) {
            matches += 1;
        };
        if (matches != 1) return error.InvalidMaintenanceResponse;
    }
}

pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    batches: []const Envelope,
    ignored_tail_bytes: usize,

    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }

    /// Copies every parsed value. No caller-owned input or file descriptor is
    /// consulted during execution, eliminating a validation/replay TOCTOU.
    pub fn parse(alloc: std.mem.Allocator, bytes: []const u8, table: []const u8, index: []const u8) !Plan {
        if (bytes.len > max_file_bytes) return error.MaintenanceRecoveryTooLarge;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const owned = arena.allocator();
        var batches: std.ArrayList(Envelope) = .empty;
        var offset: usize = 0;
        var groups = std.StringHashMap(void).init(owned);
        while (std.mem.indexOfScalarPos(u8, bytes, offset, '\n')) |end| {
            const line = bytes[offset..end];
            offset = end + 1;
            if (line.len == 0 or line.len > max_line_bytes or batches.items.len >= 128) return error.InvalidMaintenanceRecovery;
            const envelope = std.json.parseFromSliceLeaky(Envelope, owned, line, .{ .allocate = .alloc_always }) catch return error.InvalidMaintenanceRecovery;
            if (!std.mem.eql(u8, envelope.table, table) or !std.mem.eql(u8, envelope.index, index)) return error.MaintenanceRecoveryTargetMismatch;
            try validateRequest(envelope.request);
            if (batches.items.len != 0) {
                const first = batches.items[0];
                if (!std.mem.eql(u8, first.request.table_id, envelope.request.table_id) or
                    first.request.schema_version != envelope.request.schema_version or first.action != envelope.action)
                    return error.InvalidMaintenanceRecovery;
            }
            for (envelope.request.owners) |owner| {
                const entry = try groups.getOrPut(owner.group_id);
                if (entry.found_existing) return error.InvalidMaintenanceRecovery;
            }
            try batches.append(owned, envelope);
        }
        if (batches.items.len == 0 or bytes.len - offset > max_line_bytes) return error.InvalidMaintenanceRecovery;
        // The sender only submits a line after its newline and fsync. A torn
        // final line was never eligible to be sent, unlike any complete prefix.
        return .{ .arena = arena, .batches = batches.items, .ignored_tail_bytes = bytes.len - offset };
    }
};

test "maintenance recovery validates the entire immutable selection before replay" {
    const alloc = std.testing.allocator;
    const request: types.IndexMaintenanceRequest = .{ .table_id = "7", .schema_version = 0, .owners = &.{.{ .group_id = "9", .generation = "1", .slot = 0, .owner = "aa" ** 32, .comparison = "bb" ** 32, .progress_digest = "cc" ** 32, .maintenance_epoch = "0" }} };
    const encoded = try std.json.Stringify.valueAlloc(alloc, Envelope{ .table = "rows", .index = "by_id", .action = .retry, .request = request }, .{});
    defer alloc.free(encoded);
    const log = try std.fmt.allocPrint(alloc, "{s}\n{{\"torn", .{encoded});
    defer alloc.free(log);
    var plan = try Plan.parse(alloc, log, "rows", "by_id");
    defer plan.deinit();
    try std.testing.expectEqual(@as(usize, 1), plan.batches.len);
    try std.testing.expectEqual(@as(usize, 6), plan.ignored_tail_bytes);
    try std.testing.expectEqual(@as(i64, 0), plan.batches[0].request.schema_version);
    try std.testing.expectError(error.MaintenanceRecoveryTargetMismatch, Plan.parse(alloc, log, "other", "by_id"));
    const invalid = try std.fmt.allocPrint(alloc, "{s}\n{{}}\n", .{encoded});
    defer alloc.free(invalid);
    try std.testing.expectError(error.InvalidMaintenanceRecovery, Plan.parse(alloc, invalid, "rows", "by_id"));
    const duplicate = try std.fmt.allocPrint(alloc, "{s}\n{s}\n", .{ encoded, encoded });
    defer alloc.free(duplicate);
    try std.testing.expectError(error.InvalidMaintenanceRecovery, Plan.parse(alloc, duplicate, "rows", "by_id"));
    try validateAcknowledgement(request, .{ .acknowledged_groups = &.{"9"} });
    try std.testing.expectError(error.InvalidMaintenanceResponse, validateAcknowledgement(request, .{ .acknowledged_groups = &.{"10"} }));
}
