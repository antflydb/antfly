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

threadlocal var active: Control = .{};

pub const Scope = struct {
    previous: Control,

    pub fn enter(control: Control) Scope {
        const previous = active;
        active = control;
        return .{ .previous = previous };
    }

    pub fn deinit(self: Scope) void {
        active = self.previous;
    }
};

pub fn check() !void {
    try active.check();
}
