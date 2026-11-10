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

//! Cancellation for synchronous image kernels. A worker installs its borrowed
//! control for the duration of one job; nested scopes restore the previous
//! control. Never keep a scope installed across an async suspension.
pub const Control = struct {
    context: ?*const anyopaque = null,
    check_fn: ?*const fn (?*const anyopaque) anyerror!void = null,

    pub fn check(self: Control) !void {
        if (self.check_fn) |callback| try callback(self.context);
    }
};

/// Error names cross owners. The origin token is opaque everywhere except
/// its originating image-control module, identified by a mutable static address.
/// This preserves caller-only errors without decoding a foreign Zig error code.
pub const Failure = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,
    origin: ?*const anyopaque = null,
    token: usize = 0,

    pub fn init(name: []const u8) Failure {
        return .{ .ptr = name.ptr, .len = name.len };
    }
    pub fn fromError(err: anyerror) Failure {
        var result = init(@errorName(err));
        result.origin = &error_domain;
        result.token = @intFromError(err);
        return result;
    }
    pub fn localError(self: Failure) ?anyerror {
        if (self.ptr == null or self.origin != @as(?*const anyopaque, &error_domain)) return null;
        return @errorFromInt(@as(u16, @intCast(self.token)));
    }
};
var error_domain: u8 = 0;
threadlocal var active: Control = .{};
threadlocal var last_failure: ?struct { err: anyerror, identity: Failure } = null;

pub fn current() Control {
    return active;
}
pub fn recordFailure(err: anyerror, identity: Failure) void {
    last_failure = .{ .err = err, .identity = identity };
}
pub fn failure(err: anyerror) Failure {
    if (last_failure) |last| if (last.err == err) return last.identity;
    return .fromError(err);
}

pub const Scope = struct {
    previous: Control,
    previous_failure: @TypeOf(last_failure),

    pub fn enter(control: Control) Scope {
        const previous = active;
        const previous_failure = last_failure;
        active = control;
        last_failure = null;
        return .{ .previous = previous, .previous_failure = previous_failure };
    }

    pub fn deinit(self: Scope) void {
        active = self.previous;
        last_failure = self.previous_failure;
    }
};

pub fn check() !void {
    try active.check();
}
