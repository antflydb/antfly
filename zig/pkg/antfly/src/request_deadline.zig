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

//! Application deadlines carry their clock. Convert remaining durations at
//! boundaries; absolute timestamps from different executors are incomparable.
const std = @import("std");
const httpx = @import("httpx");
const platform = @import("antfly_platform");

fn ioNow(io: std.Io) u64 {
    return @intCast(@max(0, std.Io.Clock.now(.awake, io).nanoseconds));
}

pub fn tighten(ctx: *httpx.Context, milliseconds: u64) void {
    const now = ioNow(ctx.io);
    var deadline = now +| milliseconds *| std.time.ns_per_ms;
    if (ctx.application_deadline_ns) |existing| {
        const previous_now = if (ctx.application_deadline_io) |io| ioNow(io) else platform.time.monotonicNs();
        deadline = @min(deadline, now +| (existing -| previous_now));
    }
    ctx.application_deadline_io = ctx.io;
    ctx.application_deadline_ns = deadline;
}

/// Legacy consumers accept platform-clock timestamps. Keep their deadlines
/// clock-labelled when replacing an ingress executor deadline.
pub fn platformDeadline(ctx: *const httpx.Context) ?u64 {
    const deadline = ctx.application_deadline_ns orelse return null;
    const io = ctx.application_deadline_io orelse return deadline;
    const now = platform.time.monotonicNs();
    return now +| (deadline -| ioNow(io));
}

pub fn tightenPlatform(ctx: *httpx.Context, deadline: u64) void {
    ctx.application_deadline_ns = @min(platformDeadline(ctx) orelse deadline, deadline);
    ctx.application_deadline_io = null;
}
