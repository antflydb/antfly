//! Unapplied layout/mapping prototype for a training-only persistent-HiGHS
//! experiment. It contains no HiGHS calls and cannot replace the reviewed
//! cold adapter. Its purpose is to make physical constraint identity explicit
//! when rows are appended instead of rebuilding the canonical LP.
const std = @import("std");

pub const Sign = enum { upper, lower };
pub const Constraint = struct { full_row: usize, sign: Sign };

pub const Layout = struct {
    allocator: std.mem.Allocator,
    full_rows: usize,
    selected: []bool,
    physical: std.ArrayListUnmanaged(Constraint) = .empty,

    pub fn initCanonical(allocator: std.mem.Allocator, full_rows: usize, selected_rows: []const usize) !Layout {
        if (full_rows == 0 or full_rows > 4096 or selected_rows.len == 0 or selected_rows.len > full_rows)
            return error.InvalidGeometry;
        const selected = try allocator.alloc(bool, full_rows);
        errdefer allocator.free(selected);
        @memset(selected, false);
        var previous: ?usize = null;
        for (selected_rows) |row| {
            if (row >= full_rows or selected[row] or (previous != null and row <= previous.?)) return error.InvalidSelectedRows;
            selected[row] = true;
            previous = row;
        }
        var result = Layout{ .allocator = allocator, .full_rows = full_rows, .selected = selected };
        errdefer result.deinit();
        try result.physical.ensureTotalCapacity(allocator, 2 * selected_rows.len);
        for (selected_rows) |row| result.physical.appendAssumeCapacity(.{ .full_row = row, .sign = .upper });
        for (selected_rows) |row| result.physical.appendAssumeCapacity(.{ .full_row = row, .sign = .lower });
        return result;
    }

    pub fn deinit(self: *Layout) void {
        self.physical.deinit(self.allocator);
        self.allocator.free(self.selected);
        self.* = undefined;
    }

    /// Append upper rows followed by lower rows for exactly this CG round.
    /// Existing physical indices remain stable. This is intentionally not the
    /// canonical rebuilt order after the first append.
    pub fn appendRound(self: *Layout, added_rows: []const usize) !void {
        if (added_rows.len == 0 or self.physical.items.len + 2 * added_rows.len > 2 * self.full_rows)
            return error.InvalidAddedRows;
        for (added_rows) |row| if (row >= self.full_rows or self.selected[row]) return error.InvalidAddedRows;
        // Reject duplicates within the incoming ranked prefix before mutation.
        for (added_rows, 0..) |row, i| for (added_rows[0..i]) |prior| if (row == prior) return error.InvalidAddedRows;
        try self.physical.ensureUnusedCapacity(self.allocator, 2 * added_rows.len);
        for (added_rows) |row| self.physical.appendAssumeCapacity(.{ .full_row = row, .sign = .upper });
        for (added_rows) |row| self.physical.appendAssumeCapacity(.{ .full_row = row, .sign = .lower });
        for (added_rows) |row| self.selected[row] = true;
    }

    /// Map persistent physical row duals back to original coordinates.
    pub fn scatterDuals(self: Layout, row_duals: []const f64, alpha: []f64, beta: []f64) !void {
        if (row_duals.len != self.physical.items.len or alpha.len != self.full_rows or beta.len != self.full_rows)
            return error.InvalidDualShape;
        @memset(alpha, 0);
        @memset(beta, 0);
        for (self.physical.items, row_duals) |identity, dual| {
            if (!std.math.isFinite(dual)) return error.NonFiniteDual;
            switch (identity.sign) {
                .upper => alpha[identity.full_row] = -dual,
                .lower => beta[identity.full_row] = -dual,
            }
        }
    }

    /// Return physical indices in the cold builder's canonical order:
    /// selected upper rows in ascending full-row order, then selected lowers.
    /// This permutation is evidence only; applying a remapped basis to a
    /// rebuilt model is a different solver initialization and is not assumed
    /// equivalent to the cold solve.
    pub fn canonicalPermutation(self: Layout, allocator: std.mem.Allocator) ![]usize {
        const count = self.physical.items.len / 2;
        const out = try allocator.alloc(usize, self.physical.items.len);
        errdefer allocator.free(out);
        var upper: usize = 0;
        var lower: usize = count;
        for (0..self.full_rows) |row| if (self.selected[row]) {
            var found_upper: ?usize = null;
            var found_lower: ?usize = null;
            for (self.physical.items, 0..) |identity, physical_index| {
                if (identity.full_row == row and identity.sign == .upper) found_upper = physical_index;
                if (identity.full_row == row and identity.sign == .lower) found_lower = physical_index;
            }
            out[upper] = found_upper orelse return error.MissingConstraint;
            out[lower] = found_lower orelse return error.MissingConstraint;
            upper += 1;
            lower += 1;
        };
        if (upper != count or lower != out.len) return error.InvalidLayout;
        return out;
    }
};

test "append layout keeps physical rows stable and exposes canonical permutation" {
    var layout = try Layout.initCanonical(std.testing.allocator, 12, &.{ 0, 5, 10 });
    defer layout.deinit();
    try layout.appendRound(&.{ 1, 7 });
    const expected = [_]Constraint{
        .{ .full_row = 0, .sign = .upper }, .{ .full_row = 5, .sign = .upper }, .{ .full_row = 10, .sign = .upper },
        .{ .full_row = 0, .sign = .lower }, .{ .full_row = 5, .sign = .lower }, .{ .full_row = 10, .sign = .lower },
        .{ .full_row = 1, .sign = .upper }, .{ .full_row = 7, .sign = .upper }, .{ .full_row = 1, .sign = .lower },
        .{ .full_row = 7, .sign = .lower },
    };
    try std.testing.expectEqualSlices(Constraint, &expected, layout.physical.items);
    const permutation = try layout.canonicalPermutation(std.testing.allocator);
    defer std.testing.allocator.free(permutation);
    try std.testing.expectEqualSlices(usize, &.{ 0, 6, 1, 7, 2, 3, 8, 4, 9, 5 }, permutation);
}

test "dual scattering uses explicit identity and rejected append is atomic" {
    var layout = try Layout.initCanonical(std.testing.allocator, 6, &.{ 0, 5 });
    defer layout.deinit();
    try std.testing.expectError(error.InvalidAddedRows, layout.appendRound(&.{ 2, 2 }));
    try std.testing.expectEqual(@as(usize, 4), layout.physical.items.len);
    var alpha: [6]f64 = undefined;
    var beta: [6]f64 = undefined;
    try layout.scatterDuals(&.{ -0.25, -0.5, -0.75, -1.0 }, &alpha, &beta);
    try std.testing.expectEqualSlices(f64, &.{ 0.25, 0, 0, 0, 0, 0.5 }, &alpha);
    try std.testing.expectEqualSlices(f64, &.{ 0.75, 0, 0, 0, 0, 1.0 }, &beta);
}
