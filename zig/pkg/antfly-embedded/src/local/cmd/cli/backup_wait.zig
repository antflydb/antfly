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

pub const std = @import("std");
pub const antfly_client = @import("antfly-client");
pub const cli = @import("io.zig");
pub const platform_time = @import("antfly_platform").time;

pub const restore_poll_interval_ms: u64 = 1000;
pub const restore_poll_ambiguous_grace_polls: u8 = 5;
pub const RestorePollDisposition = enum { use_data, retry, invalid };

pub const RestorePollState = struct {
    consecutive_ambiguous_polls: u8 = 0,

    pub fn observe(self: *RestorePollState, status_code: u16, has_data: bool) RestorePollDisposition {
        if (has_data) {
            self.consecutive_ambiguous_polls = 0;
            return .use_data;
        }

        return switch (status_code) {
            // Explicitly transient responses reset the bounded ambiguity
            // streak. The overall restore timeout still bounds retries.
            408, 429, 502, 503, 504 => blk: {
                self.consecutive_ambiguous_polls = 0;
                break :blk .retry;
            },
            // A follower can briefly lack a newly committed job, and an
            // upstream can emit a short 500 burst. Bound only consecutive
            // ambiguous observations so a late blip does not terminate an
            // otherwise healthy long-running restore.
            404, 500 => blk: {
                if (self.consecutive_ambiguous_polls >= restore_poll_ambiguous_grace_polls) break :blk .invalid;
                self.consecutive_ambiguous_polls += 1;
                break :blk .retry;
            },
            else => .invalid,
        };
    }
};

pub fn waitForRestoreJob(
    client: *antfly_client.AntflyClient,
    io: std.Io,
    job_id: []const u8,
    timeout_ms: u64,
) !antfly_client.openapi.ApiResponse(antfly_client.types.RestoreJob) {
    const started_ns = platform_time.monotonicNs();
    const timeout_ns = std.math.mul(u64, timeout_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
    var poll_state = RestorePollState{};
    while (true) {
        var response = try client.getRestoreJobResponse(job_id);
        const disposition = poll_state.observe(response.status_code, response.data != null);
        if (response.data) |*data| {
            std.debug.assert(disposition == .use_data);
            if (isTerminalRestorePhase(data.value.phase)) return response;
        } else if (disposition == .invalid) {
            cli.expectHttpSuccess(response);
            response.deinit();
            return error.InvalidRestoreResponse;
        } else {
            // Followers can lag the replicated catalog, and load balancers or
            // upstreams can fail transiently while the restore remains live.
        }
        response.deinit();
        const elapsed_ns = platform_time.monotonicNs() -| started_ns;
        if (elapsed_ns >= timeout_ns) return error.RestoreWaitTimeout;
        const poll_ns = restore_poll_interval_ms * std.time.ns_per_ms;
        const delay_ns = @min(poll_ns, timeout_ns - elapsed_ns);
        io.sleep(std.Io.Duration.fromNanoseconds(@intCast(delay_ns)), .awake) catch return error.RestoreWaitInterrupted;
    }
}

pub fn isTerminalRestorePhase(phase: []const u8) bool {
    return std.mem.eql(u8, phase, "succeeded") or std.mem.eql(u8, phase, "failed") or std.mem.eql(u8, phase, "cancelled");
}

pub fn restorePhaseResult(phase: []const u8) !void {
    if (std.mem.eql(u8, phase, "failed")) return error.RestoreJobFailed;
    if (std.mem.eql(u8, phase, "cancelled")) return error.RestoreJobCancelled;
}
