// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");

pub const Sha256 = std.crypto.hash.sha2.Sha256;

/// Allocation-free callback used by native checkpoint writers to return the
/// exact identity of bytes they just durably materialized. This keeps manifest
/// construction off the corpus read path without coupling storage backends to
/// the backup manifest schema.
pub const Sink = struct {
    ptr: *anyopaque,
    record_fn: *const fn (
        ptr: *anyopaque,
        path: []const u8,
        size_bytes: u64,
        sha256: [Sha256.digest_length]u8,
    ) anyerror!void,

    pub fn record(
        self: Sink,
        path: []const u8,
        size_bytes: u64,
        sha256: [Sha256.digest_length]u8,
    ) !void {
        try self.record_fn(self.ptr, path, size_bytes, sha256);
    }
};
