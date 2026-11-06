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

//! A selected artifact is a directory plus the publication which proved its
//! requested identity. Never reduce it to a path at an execution boundary.
const std = @import("std");
const receipt = @import("managed_receipt.zig");
pub const Generation = [32]u8;

/// Value-only request capability: safe to copy into asynchronous work without
/// borrowing a request arena or retaining a selected directory allocation.
pub const Constraint = struct {
    directory: Generation,
    publication: ?Generation,
    pub fn matches(self: Constraint, path: []const u8) bool {
        return std.mem.eql(u8, &self.directory, &directoryIdentity(path));
    }
};
fn directoryIdentity(path: []const u8) Generation {
    var digest: Generation = undefined;
    std.crypto.hash.sha2.Sha256.hash(path, &digest, .{});
    return digest;
}

pub const ResolvedModel = struct {
    /// Owned by the allocator used for resolution.
    path: []const u8,
    publication: ?Generation,

    pub fn binding(self: ResolvedModel) Constraint {
        return .{ .directory = directoryIdentity(self.path), .publication = self.publication };
    }
    pub fn bind(self: ResolvedModel, control: anytype) @TypeOf(control) {
        var bound = control;
        // Preserve an earlier proof for this directory across nested dispatch.
        // Re-resolution must never upgrade a request to another publication.
        if (bound.model_selection) |existing| {
            if (existing.matches(self.path)) return bound;
        }
        bound.model_selection = self.binding();
        return bound;
    }
    pub fn withPath(self: ResolvedModel, path: []const u8) ResolvedModel {
        return .{ .path = path, .publication = self.publication };
    }
    pub fn verify(self: ResolvedModel, allocator: std.mem.Allocator, io: std.Io) !void {
        if (!std.meta.eql(self.publication, try receipt.publicationGeneration(allocator, io, self.path))) return error.ModelArtifactsChanging;
    }
};

/// Adopt an existing directory selected by legacy/default lookup. Constrained
/// Hub selectors instead obtain this proof from the receipt that verified them.
/// On failure the caller retains ownership of owned_path.
pub fn fromDirectory(allocator: std.mem.Allocator, io: std.Io, owned_path: []const u8) !ResolvedModel {
    const root = try receipt.publicationRootAlloc(allocator, io, owned_path);
    defer allocator.free(root);
    var validated = receipt.loadValidated(allocator, io, root) catch |err| switch (err) {
        error.NotDir => return .{ .path = owned_path, .publication = null },
        else => return err,
    };
    defer if (validated) |*value| value.deinit();
    return .{ .path = owned_path, .publication = if (validated) |value| value.publication_generation else null };
}
