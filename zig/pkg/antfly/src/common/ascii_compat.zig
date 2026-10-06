const std = @import("std");

pub fn indexOfIgnoreCasePos(haystack: []const u8, start_index: usize, needle: []const u8) ?usize {
    if (needle.len == 0) return start_index;
    if (start_index > haystack.len or haystack.len - start_index < needle.len) return null;
    var i = start_index;
    const end = haystack.len - needle.len;
    while (i <= end) : (i += 1) {
        if (std.ascii.eqlIgnoreCase(haystack[i .. i + needle.len], needle)) return i;
    }
    return null;
}

pub fn indexOfIgnoreCase(haystack: []const u8, needle: []const u8) ?usize {
    return indexOfIgnoreCasePos(haystack, 0, needle);
}
