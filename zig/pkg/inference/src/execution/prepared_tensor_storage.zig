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

const CT = @import("../ops/ops.zig").CT;
const SharedCache = @import("../runtime/tier/cache.zig").SharedCache;

/// Persistent tensor views and their cache charges. Each enclosing cache
/// record owns its metadata and returns these charges after dropping the views.
pub const Storage = struct {
    host_bytes: usize = 0,
    backend_bytes: usize = 0,
    outputs: []?CT = &.{},
    drop_ct: ?*const fn (CT) void = null,

    /// Call after dropping the backing views, while holding the cache lock.
    pub fn refundLocked(self: *const Storage, cache: *SharedCache) void {
        cache.noteRelease(.host, self.host_bytes);
        cache.noteRelease(.backend, self.backend_bytes);
    }

    pub fn deinit(self: *Storage) void {
        if (self.drop_ct) |drop| for (self.outputs) |*output| {
            if (output.*) |value| drop(value);
            output.* = null;
        };
    }
};
