//! Observed execution, independent of a model's advertised batch capability.
//! Count only successful forwards, excluding static padding and failed attempts.
pub const Execution = enum { native_batch, serial, fallback };

/// Only variable-length text encoders with an explicit attention-mask input
/// may use length classes. At most 25% token padding; fixed graphs are exact.
pub fn maskedSequenceBucket(session: anytype, width: usize, maximum: usize) usize {
    const std = @import("std");
    var ids_dynamic = false;
    var mask_dynamic = false;
    for (session.inputInfo()) |info| {
        if (info.shape.len != 2 or info.shape[1] > 0) continue;
        if (std.mem.eql(u8, info.name, "input_ids")) ids_dynamic = true;
        if (std.mem.eql(u8, info.name, "attention_mask")) mask_dynamic = true;
    }
    if (!ids_dynamic or !mask_dynamic or width < 16) return width;
    const base = std.math.floorPowerOfTwo(usize, width);
    const step = @max(@as(usize, 1), base / 4);
    const rounded = std.math.add(usize, width, step - 1) catch return width;
    return @min(maximum, rounded / step * step);
}

pub const Observation = struct {
    native_items: usize = 0,
    serial_items: usize = 0,

    pub fn record(self: *Observation, logical_rows: usize) void {
        if (logical_rows > 1) self.native_items += logical_rows else self.serial_items += logical_rows;
    }

    pub fn execution(self: Observation, requested_rows: usize) Execution {
        if (self.native_items > 0 and self.serial_items == 0) return .native_batch;
        if (requested_rows > 1) return .fallback;
        return .serial;
    }
};

test "batch observation does not count singleton padding as native work" {
    const std = @import("std");
    var observation = Observation{};
    observation.record(1);
    try std.testing.expectEqual(Execution.serial, observation.execution(1));
    observation.record(2);
    try std.testing.expectEqual(Execution.fallback, observation.execution(3));
    try std.testing.expectEqual(@as(usize, 2), observation.native_items);
}

test "masked sequence buckets bound padding and preserve fixed graph widths" {
    const std = @import("std");
    const Probe = struct {
        fixed: bool = false,
        fn inputInfo(self: @This()) []const @import("../backends/tensor.zig").TensorInfo {
            return if (self.fixed) &.{
                .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, 32 } },
                .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, 32 } },
            } else &.{
                .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } },
                .{ .name = "attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } },
            };
        }
    };
    for (1..513) |width| {
        const bucket = maskedSequenceBucket(Probe{}, width, 512);
        try std.testing.expect(bucket >= width and bucket <= 512);
        try std.testing.expect(bucket * 4 <= width * 5);
    }
    try std.testing.expectEqual(@as(usize, 32), maskedSequenceBucket(Probe{}, 29, 512));
    try std.testing.expectEqual(@as(usize, 32), maskedSequenceBucket(Probe{}, 31, 512));
    try std.testing.expectEqual(@as(usize, 29), maskedSequenceBucket(Probe{ .fixed = true }, 29, 512));
    try std.testing.expectEqual(@as(usize, 30), maskedSequenceBucket(Probe{}, 29, 30));
}
