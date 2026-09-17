//! Read-path defaults with independent qualification overrides. No format change.
const std = @import("std");
pub fn enabled(name: [*:0]const u8) bool {
    return enabledDefault(name, false);
}

pub fn enabledDefault(name: [*:0]const u8, default: bool) bool {
    if (!@import("builtin").link_libc) return default;
    const value = std.c.getenv(name) orelse return default;
    return std.mem.eql(u8, std.mem.span(value), "1");
}
