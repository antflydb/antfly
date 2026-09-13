//! Versioned, bounded f32 response frames. No native pointers, alignment, or
//! host-endian integers cross the wire. JSON remains the default representation.
const std = @import("std");

pub const content_type = "application/vnd.antfly.numeric.v1";
pub const accept = content_type ++ ", application/json";
pub const header_bytes = 24;
pub const max_body_bytes = 4 * 1024 * 1024;
pub const Kind = enum(u32) { dense = 1, scores = 2 };

pub fn requested(header: ?[]const u8) bool {
    var parts = std.mem.splitScalar(u8, header orelse return false, ',');
    while (parts.next()) |part| {
        if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, part, " \t"), content_type)) return true;
    }
    return false;
}

pub fn frameSize(rows: usize, columns: usize) !usize {
    if (columns == 0) return error.InvalidNumericResponse;
    const values = std.math.mul(usize, rows, columns) catch return error.InvalidNumericResponse;
    const bytes = std.math.mul(usize, values, 4) catch return error.InvalidNumericResponse;
    const total = std.math.add(usize, header_bytes, bytes) catch return error.InvalidNumericResponse;
    if (total > max_body_bytes) return error.NumericResponseTooLarge;
    return total;
}

/// Response capacity is an execution batch constraint, not a document limit.
pub fn maxRows(columns: usize) !usize {
    if (columns == 0) return error.InvalidNumericResponse;
    const row_bytes = std.math.mul(usize, columns, 4) catch return error.NumericResponseTooLarge;
    const rows = (max_body_bytes - header_bytes) / row_bytes;
    if (rows == 0) return error.NumericResponseTooLarge;
    return rows;
}

/// The caller fills every value before publishing. Shape is bounded before
/// allocation; setters reject nonfinite model output rather than emitting it.
pub fn allocFrame(alloc: std.mem.Allocator, kind: Kind, rows: usize, columns: usize) ![]u8 {
    if (kind == .scores and columns != 1) return error.InvalidNumericResponse;
    const frame = try alloc.alloc(u8, try frameSize(rows, columns));
    @memcpy(frame[0..4], "AFN1");
    std.mem.writeInt(u32, frame[4..8], @intFromEnum(kind), .little);
    std.mem.writeInt(u64, frame[8..16], rows, .little);
    std.mem.writeInt(u64, frame[16..24], columns, .little);
    return frame;
}

pub fn setValue(frame: []u8, index: usize, value: f32) !void {
    if (!std.math.isFinite(value)) return error.InvalidNumericResponse;
    const offset = header_bytes + index * 4;
    std.mem.writeInt(u32, frame[offset..][0..4], @bitCast(value), .little);
}

pub const View = struct {
    rows: usize,
    columns: usize,
    payload: []const u8,

    pub fn value(self: View, index: usize) f32 {
        return @bitCast(std.mem.readInt(u32, self.payload[index * 4 ..][0..4], .little));
    }

    pub fn denseAlloc(self: View, alloc: std.mem.Allocator) ![][]f32 {
        const rows = try alloc.alloc([]f32, self.rows);
        var initialized: usize = 0;
        errdefer {
            for (rows[0..initialized]) |row| alloc.free(row);
            alloc.free(rows);
        }
        for (rows, 0..) |*row, i| {
            row.* = try alloc.alloc(f32, self.columns);
            initialized += 1;
            for (row.*, 0..) |*v, j| v.* = self.value(i * self.columns + j);
        }
        return rows;
    }

    pub fn scoresAlloc(self: View, alloc: std.mem.Allocator) ![]f32 {
        if (self.columns != 1) return error.InvalidNumericResponse;
        const scores = try alloc.alloc(f32, self.rows);
        for (scores, 0..) |*score, i| score.* = self.value(i);
        return scores;
    }
};

/// Validate shape, cardinality, exact length and every value before allocating
/// descriptors or result storage. The view borrows the HTTP response body.
pub fn parse(frame: []const u8, kind: Kind, expected_rows: usize, expected_columns: ?usize) !View {
    if (frame.len < header_bytes or !std.mem.eql(u8, frame[0..4], "AFN1")) return error.InvalidNumericResponse;
    if (std.mem.readInt(u32, frame[4..8], .little) != @intFromEnum(kind)) return error.InvalidNumericResponse;
    const rows = std.math.cast(usize, std.mem.readInt(u64, frame[8..16], .little)) orelse return error.InvalidNumericResponse;
    const columns = std.math.cast(usize, std.mem.readInt(u64, frame[16..24], .little)) orelse return error.InvalidNumericResponse;
    if (rows != expected_rows or (kind == .scores and columns != 1)) return error.InvalidNumericResponse;
    if (expected_columns) |expected| if (columns != expected) return error.InvalidNumericResponse;
    if (frame.len != try frameSize(rows, columns)) return error.InvalidNumericResponse;
    const view = View{ .rows = rows, .columns = columns, .payload = frame[header_bytes..] };
    for (0..rows * columns) |i| if (!std.math.isFinite(view.value(i))) return error.InvalidNumericResponse;
    return view;
}

fn checkDenseAllocationFailures(alloc: std.mem.Allocator) !void {
    const frame = try allocFrame(alloc, .dense, 2, 2);
    defer alloc.free(frame);
    for (0..4) |i| try setValue(frame, i, @as(f32, @floatFromInt(i)) / 4);
    const view = try parse(frame, .dense, 2, 2);
    const rows = try view.denseAlloc(alloc);
    defer {
        for (rows) |row| alloc.free(row);
        alloc.free(rows);
    }
    try std.testing.expectEqualSlices(f32, &.{ 0, 0.25 }, rows[0]);
    try std.testing.expectEqualSlices(f32, &.{ 0.5, 0.75 }, rows[1]);
}

test "numeric response dense ownership is allocation failure safe" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, checkDenseAllocationFailures, .{});
}

test "numeric response row limits reserve the frame header" {
    try std.testing.expectEqual(@as(usize, 127), try maxRows(8192));
    _ = try frameSize(try maxRows(8192), 8192);
    try std.testing.expectError(error.NumericResponseTooLarge, frameSize(128, 8192));
    try std.testing.expectError(error.InvalidNumericResponse, maxRows(0));
    try std.testing.expectError(error.NumericResponseTooLarge, maxRows(max_body_bytes));
    try std.testing.expectError(error.NumericResponseTooLarge, maxRows(std.math.maxInt(usize)));
}

test "numeric response validates version kind shape cardinality and values" {
    const alloc = std.testing.allocator;
    const frame = try allocFrame(alloc, .scores, 2, 1);
    defer alloc.free(frame);
    try setValue(frame, 0, 0.25);
    try setValue(frame, 1, -0.5);
    const view = try parse(frame, .scores, 2, 1);
    const scores = try view.scoresAlloc(alloc);
    defer alloc.free(scores);
    try std.testing.expectEqualSlices(f32, &.{ 0.25, -0.5 }, scores);
    for (0..frame.len) |length| try std.testing.expectError(error.InvalidNumericResponse, parse(frame[0..length], .scores, 2, 1));
    try std.testing.expectError(error.InvalidNumericResponse, parse(frame, .dense, 2, 1));
    try std.testing.expectError(error.InvalidNumericResponse, parse(frame, .scores, 1, 1));
    try std.testing.expectError(error.InvalidNumericResponse, parse(frame, .scores, 2, 2));
    try std.testing.expectError(error.InvalidNumericResponse, setValue(frame, 0, std.math.inf(f32)));
    std.mem.writeInt(u32, frame[24..28], @bitCast(std.math.nan(f32)), .little);
    try std.testing.expectError(error.InvalidNumericResponse, parse(frame, .scores, 2, 1));
    frame[3] = '2';
    try std.testing.expectError(error.InvalidNumericResponse, parse(frame, .scores, 2, 1));
    try std.testing.expectError(error.InvalidNumericResponse, frameSize(std.math.maxInt(usize), 2));
    try std.testing.expect(!requested(null));
    try std.testing.expect(requested(accept));
    try std.testing.expect(!requested(content_type ++ ";q=0"));
}
