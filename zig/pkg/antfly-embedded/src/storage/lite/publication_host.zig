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

//! Native physical participation shared by primary and index namespace hosts.
//! The caller owns the active transaction, admission and outcome classification.
const std = @import("std");
const native = @import("native.zig");
const publication = @import("../native_publication.zig");

pub const Host = struct {
    file: *native.NativeFile,
    allocator: std.mem.Allocator,
    index_prefix: []const u8,

    pub fn view(self: *Host) publication.View {
        return .{ .ptr = self, .link_fn = link, .put_fn = put, .get_metadata_fn = getMetadata, .put_metadata_fn = putMetadata, .namespace_fn = publishNamespace };
    }

    fn checkedFile(self: *Host, inode: u64) !*native.NativeFile {
        if ((try self.file.file.stat(self.file.runtime())).inode != inode) return error.StaleArtifactConstruction;
        return self.file;
    }

    fn validateDestination(self: *Host, path: []const u8) !void {
        return publication.requireDestination(self.index_prefix, path);
    }

    fn publishNamespace(raw: *anyopaque, plan: publication.NamespacePlan) !void {
        const self: *Host = @ptrCast(@alignCast(raw));
        const file = try self.checkedFile(plan.inode);
        if (!file.header.indexed_reclamation or file.header.indexed_revision < 14) return error.UnsupportedNativeFormatVersion;
        if (!std.mem.eql(u8, &file.header.reader_epoch_namespace, &plan.file_namespace)) return error.FileBusy;
        try self.validateDestination(plan.generation_key);
        if (!std.mem.endsWith(u8, plan.prefix, "/.native-state/") or
            !std.mem.startsWith(u8, plan.generation_key, plan.prefix) or
            !std.mem.eql(u8, plan.generation_key[plan.prefix.len..], "generation")) return error.InvalidNativeIndexPath;
        for (plan.mutations) |mutation| {
            try self.validateDestination(mutation.key);
            if (!std.mem.startsWith(u8, mutation.key, plan.prefix) or
                mutation.external_value_root_page != 0 or mutation.external_value_len != 0) return error.InvalidNativeIndexPath;
        }
        const current = try file.getIndexCatalogRecordAlloc(self.allocator, plan.generation_key);
        defer if (current) |value| self.allocator.free(value);
        if (current) |value| {
            if (value.len != 21 or !std.mem.startsWith(u8, value, "AFNS1")) return error.InvalidNativeIndexGeneration;
            if (plan.expected_generation == null or !std.mem.eql(u8, value[5..], &plan.expected_generation.?)) return error.FileBusy;
        } else if (plan.expected_generation != null) return error.FileBusy;
        try file.putIndexCatalogBatch(plan.mutations);
    }

    fn link(raw: *anyopaque, inode: u64, construction: []const u8, destination: []const u8, length: usize) !void {
        const self: *Host = @ptrCast(@alignCast(raw));
        try self.validateDestination(destination);
        const file = try self.checkedFile(inode);
        try file.linkArtifactConstruction(construction, destination, length);
    }

    fn put(raw: *anyopaque, inode: u64, destination: []const u8, bytes: []const u8) !void {
        const self: *Host = @ptrCast(@alignCast(raw));
        try self.validateDestination(destination);
        const file = try self.checkedFile(inode);
        if (try file.getIndexCatalogRecordSize(destination) != null) return error.PathAlreadyExists;
        try file.putIndexCatalogRecord(destination, bytes);
    }

    fn metadataKey(self: *Host, allocator: std.mem.Allocator, scope: publication.Scope, key: []const u8, writable: bool) ![]u8 {
        const permitted = @import("../control_key.zig").isScopedMetadata(key) or (!writable and @import("../internal_keys.zig").isControlRecordKey(key));
        if (!permitted or
            (scope.prefix.len != 0 and scope.prefix[scope.prefix.len - 1] != 0)) return error.InvalidCommitMetadata;
        _ = try self.checkedFile(scope.inode);
        if (!std.mem.eql(u8, &self.file.header.reader_epoch_namespace, &scope.file_namespace)) return error.StaleArtifactConstruction;
        return std.mem.concat(allocator, u8, &.{ scope.prefix, key });
    }

    fn getMetadata(raw: *anyopaque, allocator: std.mem.Allocator, scope: publication.Scope, key: []const u8) ![]u8 {
        const self: *Host = @ptrCast(@alignCast(raw));
        const full = try self.metadataKey(allocator, scope, key, false);
        defer allocator.free(full);
        return (try self.file.getDocumentAlloc(allocator, full)) orelse error.NotFound;
    }

    fn putMetadata(raw: *anyopaque, scope: publication.Scope, key: []const u8, value: []const u8) !void {
        const self: *Host = @ptrCast(@alignCast(raw));
        const full = try self.metadataKey(self.allocator, scope, key, true);
        defer self.allocator.free(full);
        try self.file.putDocument(full, value);
    }
};
