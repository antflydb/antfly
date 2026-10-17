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

//! One immutable dictionary payload per search, shared by all file workers.
//! Brotli metadata is prepared lazily: other codecs consume the raw bytes.
const std = @import("std");
const brotli = @import("antfly_compress").brotli;
const A = std.mem.Allocator;
pub const Shared = struct {
    allocator: A,
    references: std.atomic.Value(usize) = .init(1),
    bytes: []const u8,
    format: brotli.DictionaryFormat,
    mutex: std.Io.Mutex = .init,
    prepared: ?brotli.SharedDictionary = null,
    failure: ?anyerror = null,
    pub fn load(a: A, io: std.Io, path: []const u8, limit: usize, format: brotli.DictionaryFormat) !*Shared {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(io, path, a, .limited(limit));
        errdefer a.free(bytes);
        const self = try a.create(Shared);
        self.* = .{ .allocator = a, .bytes = bytes, .format = format };
        return self;
    }
    pub fn retain(self: *Shared) *Shared {
        _ = self.references.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn release(self: *Shared) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        const a = self.allocator;
        if (self.prepared) |*dictionary| dictionary.deinit();
        a.free(self.bytes);
        a.destroy(self);
    }
    pub fn getBrotli(self: *Shared, io: std.Io) !*const brotli.SharedDictionary {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.failure) |err| return err;
        if (self.prepared == null) self.prepared = brotli.SharedDictionary.init(self.allocator, self.bytes, self.format) catch |err| {
            self.failure = err;
            return err;
        };
        return &self.prepared.?;
    }
};
