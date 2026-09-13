//! Independent, default-off qualification controls. No on-disk semantics change.
const std = @import("std");
pub fn enabled(name: [*:0]const u8) bool {
    if (!@import("builtin").link_libc) return false;
    const value = std.c.getenv(name) orelse return false;
    return std.mem.eql(u8, std.mem.span(value), "1");
}
