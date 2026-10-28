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

/// A linear credit carried by a physical device allocation, through cache and
/// asynchronous retirement queues. Release only after confirmed physical free.
pub const Credit = struct {
    context: *anyopaque,
    release_fn: *const fn (*anyopaque) void,

    pub fn release(self: Credit) void {
        self.release_fn(self.context);
    }
};

/// Owned capability for creating allocation credits. Each credit independently
/// retains its owner, so buffers may outlive this capability and its cache.
pub const Family = enum { kv, scratch };

pub const Budget = struct {
    family: Family = .kv,
    /// Only the session owner closes a prepaid pool; clones are capabilities,
    /// not new owners. Outstanding physical credits remain independently live.
    close_context: ?*const fn (*anyopaque) void = null,
    context: *anyopaque,
    allocator: std.mem.Allocator,
    retain_context: *const fn (*anyopaque) bool,
    release_context: *const fn (*anyopaque) void,
    acquire: *const fn (*anyopaque, std.mem.Allocator, usize) anyerror!Credit,

    pub fn clone(self: Budget) !Budget {
        if (!self.retain_context(self.context)) return error.ResourceTemporarilyUnavailable;
        return self;
    }

    pub fn deinit(self: Budget) void {
        self.release_context(self.context);
    }

    pub fn close(self: Budget) void {
        if (self.close_context) |close_owner| close_owner(self.context);
    }

    pub fn sameOwner(self: Budget, other: Budget) bool {
        return self.context == other.context and self.family == other.family;
    }

    pub fn reserve(self: Budget, bytes: usize) !Credit {
        return self.acquire(self.context, self.allocator, bytes);
    }
};
