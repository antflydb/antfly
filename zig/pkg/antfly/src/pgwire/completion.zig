// Completion witnesses describe durable owners; they never authorize replay.
const std = @import("std");
pub const max_outcomes = 64;
pub const Outcome = struct { reference_id: ?[]const u8 = null, registration_node_id: ?u64 = null, registration_pending: bool = false, transaction_id: ?[]const u8 = null, ddl_receipt_json: ?[]const u8 = null };
pub const Receipt = struct { version: u8 = 12, unknown: bool = false, outcomes: []const Outcome = &.{} };
pub const Completion = struct {
    unknown: bool = false,
    join_begin: ?usize = null,
    execution_owned: bool = false,
    // Admission holds producer slots through result/diagnostic publication and
    // disconnect. These are memory permits, not durable records for read queries.
    capacity: struct {
        context: ?*anyopaque = null,
        remaining: usize = 0,
        release: ?*const fn (*anyopaque, usize) void = null,
    } = .{},
    outcomes: std.ArrayList(Outcome) = .empty,
    // One execution can produce a pending result and an unknown diagnostic;
    // reserve a third slot for disconnect cleanup before any work is admitted.
    pub fn reserveExecution(self: *Completion, alloc: std.mem.Allocator) !void {
        self.execution_owned = false;
        if (self.unknown or self.outcomes.items.len > max_outcomes - 3) return error.ProgramLimitExceeded;
        try self.outcomes.ensureUnusedCapacity(alloc, 3);
    }
    fn same(a: ?[]const u8, b: ?[]const u8) bool {
        if (a == null or b == null) return a == null and b == null;
        return std.mem.eql(u8, a.?, b.?);
    }
    fn sameDdlOwner(alloc: std.mem.Allocator, left: ?[]const u8, right: ?[]const u8) bool {
        if (left == null or right == null) return left == null and right == null;
        if (same(left, right)) return true;
        const Shape = struct { table_id: []const u8, schema_version: u32, restore_job_id: ?[]const u8 = null, fk_generation_publication_id: ?[]const u8 = null };
        var a = std.json.parseFromSlice(Shape, alloc, left.?, .{ .ignore_unknown_fields = true }) catch return false;
        defer a.deinit();
        var b = std.json.parseFromSlice(Shape, alloc, right.?, .{ .ignore_unknown_fields = true }) catch return false;
        defer b.deinit();
        if (a.value.schema_version != b.value.schema_version or !same(a.value.restore_job_id, b.value.restore_job_id) or !same(a.value.fk_generation_publication_id, b.value.fk_generation_publication_id)) return false;
        return a.value.restore_job_id != null or same(a.value.table_id, b.value.table_id);
    }
    pub fn add(self: *Completion, alloc: std.mem.Allocator, transaction: ?[]const u8, ddl: ?[]const u8) void {
        if (transaction == null and ddl == null) {
            if (self.execution_owned) return; // The current execution already has a durable owner.
            self.unknown = true;
            return;
        }
        for (self.outcomes.items) |entry| {
            if (same(entry.transaction_id, transaction) and sameDdlOwner(alloc, entry.ddl_receipt_json, ddl)) return;
        }
        if (self.outcomes.items.len >= max_outcomes) {
            self.unknown = true;
            return;
        }
        const id = if (transaction) |value| alloc.dupe(u8, value) catch {
            self.unknown = true;
            return;
        } else null;
        const receipt = if (ddl) |value| alloc.dupe(u8, value) catch {
            if (id) |v| alloc.free(v);
            self.unknown = true;
            return;
        } else null;
        self.outcomes.append(alloc, .{ .transaction_id = id, .ddl_receipt_json = receipt }) catch {
            if (id) |v| alloc.free(v);
            if (receipt) |v| alloc.free(v);
            self.unknown = true;
        };
    }
    pub fn encode(self: Completion, alloc: std.mem.Allocator) ![]u8 {
        return std.json.Stringify.valueAlloc(alloc, Receipt{ .unknown = self.unknown, .outcomes = self.outcomes.items }, .{});
    }
    pub fn deinit(self: *Completion, alloc: std.mem.Allocator) void {
        if (self.capacity.release) |release| release(self.capacity.context.?, self.capacity.remaining);
        for (self.outcomes.items) |entry| {
            if (entry.reference_id) |v| alloc.free(v);
            if (entry.transaction_id) |v| alloc.free(v);
            if (entry.ddl_receipt_json) |v| alloc.free(v);
        }
        self.outcomes.deinit(alloc);
        self.* = .{};
    }
};
