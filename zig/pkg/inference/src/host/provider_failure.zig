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

//! Normalize private provider errors at their owner, before any ABI/RPC hop.
//! Never transport arbitrary error names or log request bodies/media bytes.
const std = @import("std");
const bridge = @import("antfly_inference_bridge");
const diagnostics = @import("antfly_private_error_diagnostics");

const operation_slots = @backingInt(bridge.ProviderOperation.classify_texts) + 1;
var overflow_counts = @as([operation_slots]@import("antfly_platform").atomic.Value(u64), @splat(.init(0)));
var failures = @as([diagnostics.slots_count]diagnostics.Diagnostic, @splat(.{}));

fn shouldLog(count: u64) bool {
    return count != 0 and (count <= 4 or std.math.isPowerOfTwo(count));
}

pub fn status(operation: c_int, request_json: []const u8, has_deadline: bool, err: anyerror) bridge.Status {
    return statusWithLogger(operation, request_json, has_deadline, err, std.log.err);
}

fn statusWithLogger(operation: c_int, request_json: []const u8, has_deadline: bool, err: anyerror, comptime log: anytype) bridge.Status {
    if (bridge.errorHasStableDetail(err)) return bridge.statusFromError(err);
    const fingerprint = diagnostics.fingerprint(operation, err, request_json);
    const noted = diagnostics.note(&failures, fingerprint);
    const slot: usize = if (operation > 0 and operation < operation_slots) @intCast(operation) else 0;
    const count = noted orelse (overflow_counts[slot].fetchAdd(1, .monotonic) +% 1);
    if (shouldLog(count)) {
        const provider_operation = std.enums.fromInt(bridge.ProviderOperation, operation);
        log("standalone inference provider failed provider_operation={s} request_bytes={d} has_deadline={} diagnostic_fingerprint={x} diagnostic_table_saturated={} observed_diagnostic_failures={d} err={}", .{
            if (provider_operation) |value| @tagName(value) else "unknown",
            request_json.len,
            has_deadline,
            fingerprint,
            noted == null,
            count,
            err,
        });
    }
    return bridge.statusFromErrorWithFallback(err, error.InferenceProviderFailure);
}

test "provider failure logging stays bounded and does not suppress first errors" {
    for ([_]u64{ 1, 2, 3, 4, 8, 16, 1024 }) |count| try std.testing.expect(shouldLog(count));
    for ([_]u64{ 0, 5, 6, 7, 9, 1023 }) |count| try std.testing.expect(!shouldLog(count));
    try std.testing.expectEqual(error.Cancelled, bridge.errorFromStatus(status(0, "", false, error.Cancelled)));
}

test "provider owner logs private cause before stable transport without double reporting" {
    const Probe = struct {
        var cause: ?anyerror = null;
        var calls: usize = 0;
        fn log(comptime _: []const u8, args: anytype) void {
            cause = args[6];
            calls += 1;
        }
    };
    Probe.cause = null;
    Probe.calls = 0;
    const owned = statusWithLogger(@backingInt(bridge.ProviderOperation.read_raster_images_reported), "{\"model\":\"diagnostic-probe\"}", true, error.PrivateProviderProbe, Probe.log);
    try std.testing.expectEqual(error.PrivateProviderProbe, Probe.cause.?);
    try std.testing.expectEqual(@as(usize, 1), Probe.calls);
    // Simulate worker reply -> parent bridge -> caller, without extending the
    // stable ABI with backend-specific errors or re-logging a normalized error.
    const forwarded = statusWithLogger(0, "", false, bridge.errorFromStatus(owned), Probe.log);
    try std.testing.expectEqual(error.InferenceProviderFailure, bridge.errorFromStatus(forwarded));
    try std.testing.expectEqual(@as(usize, 1), Probe.calls);
}
