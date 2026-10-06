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

//! Shared bounded-I/O primitives for provider upload implementations.

const std = @import("std");
const types = @import("types.zig");

const cancellable_file_read_chunk_bytes: usize = 1024 * 1024;
const failed_upload_cleanup_timeout_ns: i96 = 2 * std.time.ns_per_s;

pub const CleanupDeadline = struct {
    io: std.Io,
    started_at: std.Io.Timestamp,

    pub fn init(io: std.Io) @This() {
        return .{
            .io = io,
            .started_at = std.Io.Timestamp.now(io, .awake),
        };
    }

    pub fn token(self: *const @This()) types.CancellationToken {
        return .{
            .ptr = self,
            .is_cancelled_fn = struct {
                fn call(raw: *const anyopaque) bool {
                    const deadline: *const CleanupDeadline = @ptrCast(@alignCast(raw));
                    return std.Io.Timestamp.durationTo(
                        deadline.started_at,
                        std.Io.Timestamp.now(deadline.io, .awake),
                    ).toNanoseconds() >= failed_upload_cleanup_timeout_ns;
                }
            }.call,
        };
    }
};

pub fn readPositionalAllWithCancellation(
    file: std.Io.File,
    io: std.Io,
    destination: []u8,
    offset: u64,
    cancellation: ?types.CancellationToken,
) !usize {
    var read: usize = 0;
    while (read < destination.len) {
        if (cancellation) |token| try token.check();
        const chunk_len = @min(cancellable_file_read_chunk_bytes, destination.len - read);
        const count = try file.readPositionalAll(
            io,
            destination[read..][0..chunk_len],
            offset + read,
        );
        read += count;
        if (count != chunk_len) break;
    }
    if (cancellation) |token| try token.check();
    return read;
}
