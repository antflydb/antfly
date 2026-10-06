// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Synchronous, borrowed NDJSON consumer. The callback owner supplies the
//! dispatcher, including when this sink is nested inside another runtime call.
//! Byte slices stay zero-copy; compilation-local errors never cross archives.
const boundary = @import("runtime_callback_abi.zig");

pub const ScanStreamSink = struct {
    const VTable = struct {
        start: *const fn (?*anyopaque) anyerror!void,
        write: *const fn (?*anyopaque, []const u8) anyerror!void,
    };
    const Abi = boundary.Boundary(VTable);

    context: ?*anyopaque,
    start_fn: @FieldType(VTable, "start"),
    write_fn: @FieldType(VTable, "write"),
    boundary_dispatch: Abi.Dispatch = Abi.local_dispatch,

    /// Called once after routing validates the table, even for an empty scan.
    pub fn start(self: ScanStreamSink) !void {
        try Abi.call("start", self.boundary_dispatch, self.start_fn, .{self.context});
    }

    /// Completion supplies backpressure; errors stop iteration immediately.
    /// The consumer must not retain the borrowed bytes after returning.
    pub fn write(self: ScanStreamSink, bytes: []const u8) !void {
        if (bytes.len != 0)
            try Abi.call("write", self.boundary_dispatch, self.write_fn, .{ self.context, bytes });
    }
};
