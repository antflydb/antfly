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

//! Object-native page I/O shared by Lite and hosted transactional metadata.
const std = @import("std");
const tree = @import("ordered_tree.zig");
const Client = @import("client.zig").Client;
const types = @import("types.zig");
const A = std.mem.Allocator;
pub const Pages = struct {
    client: Client,
    bucket: []const u8,
    prefix: []const u8,
    domain: [32]u8,
    attempt: [16]u8,
    remaining_read_bytes: *u64,
    remaining_write_bytes: *u64,
    cancellation: ?types.CancellationToken = null,
    pub fn domainForNamespace(namespace: []const u8) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly:graph-reclamation-domain:v1:");
        hash.update(namespace);
        return hash.finalResult();
    }
    pub fn attemptForRevision(io: std.Io, revision: u64) [16]u8 {
        var attempt: [16]u8 = undefined;
        io.random(&attempt);
        std.mem.writeInt(u64, attempt[0..8], revision, .big);
        return attempt;
    }
    pub fn key(self: Pages, a: A, ref: tree.Ref) ![]u8 {
        if (ref.pack != null) return error.OrderedPackedPageUnsupported;
        return std.fmt.allocPrint(a, "{s}/{s}/{s}", .{ self.prefix, std.fmt.bytesToHex(ref.attempt, .lower), std.fmt.bytesToHex(ref.digest, .lower) });
    }
    pub fn store(self: *Pages) tree.Store {
        return .{ .ptr = self, .domain = self.domain, .attempt = self.attempt, .prefix_keys = true, .get = get, .read_many = if (self.client.vtable.get_objects != null) .{ .ptr = self, .read = readMany } else null, .put = put, .put_many = putMany, .check = check };
    }
    fn check(raw: *anyopaque) !void {
        const self: *Pages = @ptrCast(@alignCast(raw));
        if (self.cancellation) |token| try token.check();
    }
    fn get(raw: *anyopaque, a: A, ref: tree.Ref) ![]u8 {
        const self: *Pages = @ptrCast(@alignCast(raw));
        try check(raw);
        try ref.validate();
        if (ref.bytes > self.remaining_read_bytes.*) return error.OrderedPageReadBudgetExceeded;
        self.remaining_read_bytes.* -= ref.bytes;
        const k = try self.key(a, ref);
        defer a.free(k);
        var client = self.client;
        client.allocator = a;
        var result = try client.getObject(self.bucket, k, .{ .skip_metadata_probe = true, .max_response_bytes = ref.bytes, .cancellation = self.cancellation });
        defer result.deinit(a);
        if (result.body.len != ref.bytes) return error.ArtifactIntegrityMismatch;
        const body = result.body;
        result.body = &.{};
        return body; // ordered_tree.decodePage authenticates domain and digest.
    }
    fn readMany(raw: *anyopaque, a: A, refs: []const tree.Ref, output: []tree.NavigationPage) !void {
        const self: *Pages = @ptrCast(@alignCast(raw));
        if (refs.len != output.len or refs.len > tree.max_read_batch) return error.InvalidGraphPage;
        var total: u64 = 0;
        for (refs) |ref| {
            try ref.validate();
            total += ref.bytes;
        }
        try check(raw);
        if (total > self.remaining_read_bytes.*) return error.OrderedPageReadBudgetExceeded;
        var initialized: usize = 0;
        errdefer for (output[0..initialized]) |*page| page.deinit(a);
        var begin: usize = 0;
        while (begin < refs.len) {
            if (refs[begin].bytes > 256 * 1024) {
                const encoded = try get(raw, a, refs[begin]);
                output[begin] = try tree.decodeNavigationPage(a, self.domain, refs[begin], encoded);
                initialized += 1;
                begin += 1;
                continue;
            }
            var requests: [4]types.GetObjectRequest = undefined;
            var count: usize = 0;
            defer for (requests[0..count]) |request| a.free(request.key);
            var batch_bytes: u64 = 0;
            while (begin + count < refs.len and count < requests.len) {
                const ref = refs[begin + count];
                if (ref.bytes > 256 * 1024 - batch_bytes) break;
                batch_bytes += ref.bytes;
                requests[count] = .{ .key = try self.key(a, ref), .options = .{ .skip_metadata_probe = true, .max_response_bytes = ref.bytes, .cancellation = self.cancellation } };
                count += 1;
            }
            self.remaining_read_bytes.* -= batch_bytes;
            var client = self.client;
            client.allocator = a;
            const results = try client.getObjects(self.bucket, requests[0..count]);
            defer {
                for (results) |*result| if (result.*) |*value| value.deinit(a);
                a.free(results);
            }
            try check(raw);
            for (results, 0..) |*result, i| {
                const body = (result.* orelse return error.FileNotFound).body;
                result.*.?.body = &.{};
                output[begin + i] = try tree.decodeNavigationPage(a, self.domain, refs[begin + i], body);
                initialized += 1;
            }
            begin += count;
        }
    }
    fn validateWrite(self: *Pages, ref: tree.Ref, bytes: []const u8) !void {
        try ref.validate();
        if (!std.mem.eql(u8, &ref.attempt, &self.attempt)) return error.GraphPageAttemptMismatch;
        if (bytes.len != ref.bytes) return error.ArtifactIntegrityMismatch;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        if (!std.mem.eql(u8, &digest, &ref.digest)) return error.ArtifactIntegrityMismatch;
        if (bytes.len > self.remaining_write_bytes.*) return error.OrderedPageWriteBudgetExceeded;
        self.remaining_write_bytes.* -= bytes.len;
    }
    fn put(raw: *anyopaque, ref: tree.Ref, bytes: []const u8) !void {
        const self: *Pages = @ptrCast(@alignCast(raw));
        try check(raw);
        try self.validateWrite(ref, bytes);
        return upload(raw, ref, bytes);
    }
    fn upload(raw: *anyopaque, ref: tree.Ref, bytes: []const u8) !void {
        const self: *Pages = @ptrCast(@alignCast(raw));
        try check(raw);
        const a = self.client.allocator;
        const k = try self.key(a, ref);
        defer a.free(k);
        var receipt = self.client.putObject(self.bucket, k, bytes, .{ .if_none_match = true, .cancellation = self.cancellation }) catch |err| switch (err) {
            error.PreconditionFailed => {
                const old = try get(raw, a, ref);
                defer a.free(old);
                if (!std.mem.eql(u8, old, bytes)) return error.ArtifactIntegrityMismatch;
                return;
            },
            else => return err,
        };
        receipt.deinit(a);
    }
    fn putMany(raw: *anyopaque, writes: []const tree.Write) !void {
        const self: *Pages = @ptrCast(@alignCast(raw));
        if (writes.len > 8 or writes.len == 0) return error.InvalidObjectBatch;
        const a = self.client.allocator;
        var requests: [8]types.PutObjectRequest = undefined;
        var count: usize = 0;
        defer for (requests[0..count]) |item| a.free(item.key);
        for (writes) |write| {
            try check(raw);
            try self.validateWrite(write.ref, write.bytes);
            requests[count] = .{ .key = try self.key(a, write.ref), .body = write.bytes, .options = .{ .if_none_match = true, .cancellation = self.cancellation } };
            count += 1;
        }
        const results = self.client.putObjects(self.bucket, requests[0..count]) catch |err| switch (err) {
            error.PreconditionFailed => {
                // Verify objects that already exist instead of uploading them
                // again. Missing objects need a new, separately charged attempt.
                for (writes) |write| {
                    const existing = get(raw, a, write.ref) catch |read_error| switch (read_error) {
                        error.FileNotFound => {
                            try put(raw, write.ref, write.bytes);
                            continue;
                        },
                        else => return read_error,
                    };
                    defer a.free(existing);
                    if (!std.mem.eql(u8, existing, write.bytes)) return error.ArtifactIntegrityMismatch;
                }
                return;
            },
            else => return err,
        };
        defer {
            for (results) |*result| result.deinit(a);
            a.free(results);
        }
    }
};
