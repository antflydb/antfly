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

const Sink = @import("runtime_scan_sink.zig").ScanStreamSink;
const errors = @import("runtime_error_abi.zig");

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
