// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
