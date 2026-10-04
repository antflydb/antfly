const std = @import("std");

pub fn indexOfIgnoreCase(haystack: []const u8, needle: []const u8) ?usize {
    if (needle.len == 0) return 0;
    if (needle.len > haystack.len) return null;
    for (0..haystack.len - needle.len + 1) |index| {
        if (std.ascii.eqlIgnoreCase(haystack[index..][0..needle.len], needle)) return index;
    }
    return null;
}

test indexOfIgnoreCase {
    try std.testing.expectEqual(@as(?usize, 2), indexOfIgnoreCase("a GEMMA-4 model", "gemma-4"));
    try std.testing.expectEqual(@as(?usize, null), indexOfIgnoreCase("gemma-3", "gemma-4"));
}
