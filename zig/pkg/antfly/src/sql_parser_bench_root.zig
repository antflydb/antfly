const std = @import("std");
const parser_bench = @import("sql/parser_bench.zig");

pub fn main(init: std.process.Init) !void {
    return parser_bench.main(init);
}
