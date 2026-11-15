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

//! Storage-free metadata apply-store contract shared by the concrete kernel
//! owner and its opaque client. Keep backend types and implementation tests out
//! of this module so consumers do not regain the physical storage graph.

const std = @import("std");

/// Expected domain rejections cross the owner boundary as typed data.
pub const Result = enum {
    valid,
    index_referenced_by_alias,
    index_alias_graph_conflict,

    pub fn fromError(err: anyerror) ?Result {
        return switch (err) {
            error.IndexReferencedByAlias => .index_referenced_by_alias,
            error.IndexAliasGraphConflict => .index_alias_graph_conflict,
            else => null,
        };
    }

    pub fn requireValid(self: Result) !void {
        switch (self) {
            .valid => {},
            .index_referenced_by_alias => return error.IndexReferencedByAlias,
            .index_alias_graph_conflict => return error.IndexAliasGraphConflict,
        }
    }
};

test "alias mutation rejection identities survive projection JSON" {
    const alloc = std.testing.allocator;
    try Result.valid.requireValid();
    for ([_]anyerror{ error.IndexReferencedByAlias, error.IndexAliasGraphConflict }) |err| {
        const encoded = try std.json.Stringify.valueAlloc(alloc, Result.fromError(err).?, .{});
        defer alloc.free(encoded);
        const decoded = try std.json.parseFromSlice(Result, alloc, encoded, .{});
        defer decoded.deinit();
        try std.testing.expectError(err, decoded.value.requireValid());
    }
    // Storage faults must propagate through the error boundary, never become
    // a normal domain rejection or a successful mutation.
    try std.testing.expect(Result.fromError(error.OutOfMemory) == null);
}
