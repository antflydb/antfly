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

const Sink = @import("antfly_local_sources").runtime_scan_sink.ScanStreamSink;
const errors = @import("antfly_runtime_abi").error_abi;

export fn scan_sink_test_consume(sink: *const Sink, bytes: [*]const u8, len: usize) callconv(.c) errors.Status {
    sink.start() catch |err| return errors.statusFromError(err);
    sink.write("") catch |err| return errors.statusFromError(err);
    sink.write(bytes[0..len]) catch |err| return errors.statusFromError(err);
    return .ok;
}

export fn scan_sink_test_provider_sink(out: *Sink, failure: *errors.Status) callconv(.c) void {
    out.* = .{ .context = failure, .start_fn = start, .write_fn = write };
}

fn start(_: ?*anyopaque) !void {}
fn write(raw: ?*anyopaque, _: []const u8) !void {
    const failure: *const errors.Status = @ptrCast(@alignCast(raw.?));
    if (!failure.isOk()) return errors.errorFromStatus(failure.*);
}

/// Server fixtures retain this compilation root's source and type identity.
pub const local_test_sources = if (@import("builtin").is_test) @import("local_test_sources.zig") else struct {};
