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

//! Lazy root-local validation authentication; portable imports carry no key.
const std = @import("std");
const validation = @import("../document_validation.zig");
pub const Authority = struct {
    mutex: std.Io.Mutex = .init,
    cached: ?struct { root: u128, key: validation.Digest } = null,

    pub fn get(self: *Authority, a: std.mem.Allocator, io: std.Io, path: []const u8, root: u128) !validation.Digest {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        if (self.cached) |cached| if (cached.root == root) return cached.key;
        const identity = try @import("root_signing_identity.zig").loadOrCreate(a, io, path);
        if (identity.root_incarnation != root) return error.PreparedGenerationChanged;
        const key = validation.deriveKey(identity.seed);
        self.cached = .{ .root = root, .key = key };
        return key;
    }
};
