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

//! Native text identities closed over a verified source observation. Caller
//! hashes, prepared JSON roots, and transformed strings cannot mint these.
const std = @import("std");
const claims = @import("index_source_claim.zig");
const payload = @import("document_payload.zig");
const projection = @import("document_projection.zig");
const canonical = @import("source_value_identity.zig");
const A = std.mem.Allocator;
const Binding = struct { text: []const u8, digest: [32]u8 };
const Context = struct {
    observation: *const claims.ObservedSource,
    produced: []const u8,
    root: std.json.Value,
    directory: projection.Directory,
    bindings: []?Binding,
};
pub const Text = struct {
    context: *const Context,
    index: u16,
    pub fn digest(self: Text, text: []const u8) ?[32]u8 {
        self.context.observation.requireProduced(self.context.produced) catch return null;
        if (self.index >= self.context.bindings.len) return null;
        const binding = self.context.bindings[self.index] orelse return null;
        if (binding.text.ptr != text.ptr or binding.text.len != text.len) return null;
        return binding.digest;
    }
    pub fn jsonParse(_: A, _: anytype, _: std.json.ParseOptions) !Text {
        return error.NativeIndexSourceCannotSerialize;
    }
    pub fn jsonStringify(_: Text, _: anytype) !void {
        return error.NativeIndexSourceCannotSerialize;
    }
};
pub const Scope = struct {
    context: *const Context,
    /// Metadata and parsed strings share the caller's projection arena. The
    /// source observation/read cut must outlive that arena and every build.
    pub fn capture(a: A, observation: *const claims.ObservedSource, produced: []const u8) !?Scope {
        try observation.requireProduced(produced);
        const metadata = (try payload.SourceMetadata.capture(a, observation.reader, observation.physical)) orelse return null;
        defer metadata.deinit(a);
        if (metadata.proof.revision != observation.mutation or !std.mem.eql(u8, &metadata.proof.digest, &observation.root)) return error.PreparedReadSetChanged;
        const directory = (try projection.Directory.open(a, metadata.directory, metadata.proof)) orelse return null;
        const certificate = canonical.Certificate.open(metadata.certificate, metadata.directory, metadata.proof) orelse return null;
        const eligible = for (directory.fields) |field| {
            if (field.kind == .leaf and field.length >= 8 * 1024 and certificate.find(field.index) != null) break true;
        } else false;
        if (!eligible) return null;
        const parsed_root = try std.json.parseFromSliceLeaky(std.json.Value, a, produced, .{});
        for (@constCast(directory.fields)) |*field| field.name = try a.dupe(u8, field.name);
        const bindings = try a.alloc(?Binding, directory.fields.len);
        @memset(bindings, null);
        for (directory.fields) |field| {
            if (field.kind != .leaf) continue;
            const value_digest = certificate.find(field.index) orelse continue;
            var chain: [projection.maximum_fields]u16 = undefined;
            var count: usize = 0;
            var index = field.index;
            while (true) {
                if (count == chain.len) return error.InvalidArgument;
                chain[count] = index;
                count += 1;
                const parent = directory.fields[index].parent;
                if (parent == projection.no_parent) break;
                index = parent;
            }
            var value = parsed_root;
            var resolved = true;
            while (count != 0) {
                count -= 1;
                if (value != .object) {
                    resolved = false;
                    break;
                }
                value = value.object.get(directory.fields[chain[count]].name) orelse {
                    resolved = false;
                    break;
                };
            }
            if (!resolved or value != .string) continue;
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update("antfly/native-text-value/v1\x00");
            hash.update(&canonical.semanticAbi());
            hash.update(&value_digest);
            bindings[field.index] = .{ .text = value.string, .digest = hash.finalResult() };
        }
        const context = try a.create(Context);
        context.* = .{ .observation = observation, .produced = produced, .root = parsed_root, .directory = directory, .bindings = bindings };
        return .{ .context = context };
    }
    pub fn root(self: Scope) std.json.Value {
        return self.context.root;
    }
    pub fn bind(self: Scope, path: []const u8, text: []const u8) ?Text {
        const field = self.context.directory.resolve(path) orelse return null;
        const result: Text = .{ .context = self.context, .index = field.index };
        _ = result.digest(text) orelse return null;
        return result;
    }
};
