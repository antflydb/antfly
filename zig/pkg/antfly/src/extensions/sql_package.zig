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

//! PostgreSQL-style SQL control metadata over Antfly's immutable WASM packages.
//! Package versions and SQL-visible installation identities are separate: a
//! default version is explicit control metadata, never a semver/latest guess.
const std = @import("std");
const packages = @import("mod.zig");
const records = @import("antfly_local_sources").sql_catalog.routines.records;
const A = std.mem.Allocator;
const domain = @import("antfly_local_sources").system_catalog_domain;
pub const max_versions = 256;
pub const max_exports = 256;
pub const max_dependencies = 128;

pub const Export = struct {
    name: []const u8,
    runtime: []const u8,
    export_name: []const u8,
    arguments: []const records.Argument = &.{},
    result: records.Type,
    strict: bool = false,
    volatility: records.Volatility = .@"volatile",

    pub fn validate(self: @This()) !void {
        try identifier(self.name);
        try identifier(self.runtime);
        try identifier(self.export_name);
        if (self.arguments.len > records.max_arguments) return error.InvalidSqlExtensionControl;
        try self.result.validate();
        for (self.arguments, 0..) |argument, i| {
            try argument.type.validate();
            if (argument.name) |name| {
                try identifier(name);
                for (self.arguments[0..i]) |prior| if (prior.name) |previous| if (std.mem.eql(u8, name, previous)) return error.InvalidSqlExtensionControl;
            }
        }
    }
    pub fn signatureAlloc(self: @This(), a: A) ![]const u8 {
        const definition: records.Definition = .{ .arguments = self.arguments, .result = self.result, .body = "SELECT NULL", .owner = .auth_disabled };
        return definition.signatureAlloc(a);
    }
};

pub const Control = struct {
    /// Every registered version of one package must agree on this default.
    default_version: []const u8,
    /// Fixed installation schema, as in a PostgreSQL control file. Null uses
    /// CREATE EXTENSION SCHEMA or the current creation namespace.
    schema: ?[]const u8 = null,
    exports: []const Export,
    /// Declarative transitions rebuild exactly this version's typed exports.
    /// They are not arbitrary PostgreSQL SQL or native-library update scripts.
    updates_from: []const []const u8 = &.{},

    pub fn validate(self: @This(), package: packages.PackageManifest) !void {
        try versionName(self.default_version);
        try versionName(package.version);
        try @import("antfly_local_sources").system_catalog_domain.object_annotations.validateDescription(package.description);
        if (!package.install.supportsScope(.embedded_db) or package.install.shapes.len != 0 or package.install.objects.len != 0 or package.capabilities_requested.len != 0) return error.UnsupportedSqlExtensionObject;
        if (self.updates_from.len > max_versions) return error.InvalidSqlExtensionControl;
        for (self.updates_from, 0..) |version, i| {
            try versionName(version);
            if (std.mem.eql(u8, version, package.version)) return error.InvalidSqlExtensionControl;
            for (self.updates_from[0..i]) |previous| if (std.mem.eql(u8, previous, version)) return error.InvalidSqlExtensionControl;
        }
        if (self.schema) |name| {
            try identifier(name);
            if (package.relocatable) return error.InvalidSqlExtensionControl;
        }
        if (self.exports.len == 0 or self.exports.len > max_exports) return error.InvalidSqlExtensionControl;
        for (self.exports, 0..) |declaration, i| {
            try declaration.validate();
            for (self.exports[0..i]) |prior| {
                if (!std.mem.eql(u8, prior.name, declaration.name) or prior.arguments.len != declaration.arguments.len) continue;
                var equal = true;
                for (prior.arguments, declaration.arguments) |left, right| if (!std.meta.eql(left.type, right.type)) {
                    equal = false;
                    break;
                };
                if (equal) return error.DuplicateSqlExtensionExport;
            }
            const runtime = try wasmRuntime(package, declaration.runtime);
            _ = try wasmArtifact(package, runtime.artifact);
        }
        for (package.dependencies) |dependency| {
            // PostgreSQL control requirements identify an extension, not a
            // semver range. Do not silently discard unsupported restrictions.
            if (dependency.version_requirement.len != 0) return error.InvalidSqlExtensionDependency;
        }
    }

    pub fn clone(self: @This(), a: A) !Control {
        const default_version = try a.dupe(u8, self.default_version);
        errdefer a.free(default_version);
        const schema = if (self.schema) |name| try a.dupe(u8, name) else null;
        errdefer if (schema) |name| a.free(name);
        const exports = try a.alloc(Export, self.exports.len);
        var initialized: usize = 0;
        errdefer {
            for (exports[0..initialized]) |declaration| freeExport(a, declaration);
            a.free(exports);
        }
        for (self.exports, exports) |declaration, *out| {
            out.* = try cloneExport(a, declaration);
            initialized += 1;
        }
        const updates_from = try a.alloc([]const u8, self.updates_from.len);
        var cloned_versions: usize = 0;
        errdefer {
            for (updates_from[0..cloned_versions]) |version| a.free(version);
            a.free(updates_from);
        }
        for (self.updates_from, updates_from) |version, *destination| {
            destination.* = try a.dupe(u8, version);
            cloned_versions += 1;
        }
        return .{ .default_version = default_version, .schema = schema, .exports = exports, .updates_from = updates_from };
    }
    pub fn deinitOwned(self: @This(), a: A) void {
        a.free(self.default_version);
        if (self.schema) |name| a.free(name);
        for (self.exports) |declaration| freeExport(a, declaration);
        a.free(self.exports);
        for (self.updates_from) |version| a.free(version);
        a.free(self.updates_from);
    }
};

pub fn wasmRuntime(package: packages.PackageManifest, name: []const u8) !packages.RuntimeDecl {
    var found: ?packages.RuntimeDecl = null;
    for (package.install.runtimes) |runtime| if (std.mem.eql(u8, runtime.name, name)) {
        if (found != null or runtime.mode != .wasm) return error.InvalidSqlExtensionRuntime;
        found = runtime;
    };
    return found orelse error.InvalidSqlExtensionRuntime;
}
pub fn wasmArtifact(package: packages.PackageManifest, path: []const u8) !packages.PackageArtifact {
    try relativePath(path);
    var found: ?packages.PackageArtifact = null;
    for (package.artifacts) |artifact| if (std.mem.eql(u8, artifact.path, path)) {
        if (found != null or artifact.kind != .wasm) return error.InvalidSqlExtensionArtifact;
        _ = try digest(artifact.digest);
        found = artifact;
    };
    return found orelse error.InvalidSqlExtensionArtifact;
}
pub fn digest(text: []const u8) ![32]u8 {
    const hex = if (std.mem.startsWith(u8, text, "sha256:")) text[7..] else text;
    if (hex.len != 64) return error.InvalidSqlExtensionArtifact;
    var out: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&out, hex) catch return error.InvalidSqlExtensionArtifact;
    if (std.mem.allEqual(u8, &out, 0)) return error.InvalidSqlExtensionArtifact;
    return out;
}
pub fn relativePath(path: []const u8) !void {
    if (path.len == 0 or path.len > 4096 or path[0] == '/' or std.mem.indexOfScalar(u8, path, '\\') != null or std.mem.indexOfScalar(u8, path, 0) != null) return error.InvalidSqlExtensionArtifact;
    var parts = std.mem.splitScalar(u8, path, '/');
    while (parts.next()) |part| if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return error.InvalidSqlExtensionArtifact;
}
fn identifier(name: []const u8) !void {
    if (name.len == 0 or name.len > 128 or !std.unicode.utf8ValidateSlice(name) or std.mem.indexOfScalar(u8, name, 0) != null) return error.InvalidSqlExtensionControl;
}
fn versionName(name: []const u8) !void {
    try identifier(name);
    if (std.mem.indexOfScalar(u8, name, '/') != null or std.mem.indexOfScalar(u8, name, '\\') != null) return error.InvalidSqlExtensionControl;
}
fn cloneExport(a: A, value: Export) !Export {
    var out = value;
    out.name = try a.dupe(u8, value.name);
    errdefer a.free(out.name);
    out.runtime = try a.dupe(u8, value.runtime);
    errdefer a.free(out.runtime);
    out.export_name = try a.dupe(u8, value.export_name);
    errdefer a.free(out.export_name);
    const arguments = try a.alloc(records.Argument, value.arguments.len);
    var initialized: usize = 0;
    errdefer {
        for (arguments[0..initialized]) |argument| if (argument.name) |name| a.free(name);
        a.free(arguments);
    }
    for (value.arguments, arguments) |argument, *destination| {
        destination.* = .{ .type = argument.type, .name = if (argument.name) |name| try a.dupe(u8, name) else null };
        initialized += 1;
    }
    out.arguments = arguments;
    return out;
}
fn freeExport(a: A, value: Export) void {
    a.free(value.name);
    a.free(value.runtime);
    a.free(value.export_name);
    for (value.arguments) |argument| if (argument.name) |name| a.free(name);
    a.free(value.arguments);
}

/// A bounded immutable view of the same owned package registry used by REST.
/// No hardcoded SQL extension names or PostgreSQL C-library fallback exists.
/// Durable package-level default, independent of immutable version manifests.
/// A null digest is a valid initial pending target, never an inferred version.
pub const PackageControl = struct {
    name: []const u8,
    identity: [32]u8,
    generation: u64,
    default_version: []const u8,
    default_digest: ?[32]u8 = null,
    promotions: []const PromotionReceipt = &.{},

    pub fn validate(self: @This()) !void {
        return self.validateInner(true);
    }
    pub fn validateCompact(self: @This()) !void {
        return self.validateInner(false);
    }
    fn validateInner(self: @This(), require_receipts: bool) !void {
        try domain.validateName(self.name);
        try versionName(self.default_version);
        if (self.generation == 0 or !std.mem.eql(u8, &self.identity, &controlIdentity(self.name)) or self.promotions.len > max_promotion_receipts) return error.InvalidSqlExtensionControl;
        if (self.default_digest) |value| if (std.mem.allEqual(u8, &value, 0)) return error.InvalidSqlExtensionControl;
        for (self.promotions, 0..) |receipt, i| {
            try versionName(receipt.version);
            if (receipt.generation <= 1 or receipt.generation > self.generation or std.mem.allEqual(u8, &receipt.nonce, 0) or std.mem.allEqual(u8, &receipt.request_digest, 0) or std.mem.allEqual(u8, &receipt.package_digest, 0)) return error.InvalidSqlExtensionControl;
            for (self.promotions[0..i]) |previous| if (previous.generation >= receipt.generation or std.mem.eql(u8, &previous.nonce, &receipt.nonce)) return error.InvalidSqlExtensionControl;
            if (i > 0 and receipt.generation - 1 != self.promotions[i - 1].generation) return error.InvalidSqlExtensionControl;
            var scratch: [4096]u8 = undefined;
            var fixed = std.heap.FixedBufferAllocator.init(&scratch);
            const request: Promotion = .{ .name = self.name, .identity = self.identity, .expected_generation = receipt.generation - 1, .version = receipt.version, .package_digest = receipt.package_digest, .nonce = receipt.nonce };
            if (!std.mem.eql(u8, &receipt.request_digest, &try request.digest(fixed.allocator()))) return error.InvalidSqlExtensionControl;
        }
        if (require_receipts) {
            // Initial readiness is generation1, or2 when an initially pending
            // target resolves. Every later increment must retain its window.
            const ready_count = @min(self.generation - 1, max_promotion_receipts);
            const pending_count = @min(self.generation -| 2, max_promotion_receipts);
            if (self.promotions.len != ready_count and self.promotions.len != pending_count) return error.InvalidSqlExtensionControl;
        }
        if (self.promotions.len == 0) {
            if (require_receipts and self.generation > 2) return error.InvalidSqlExtensionControl;
        } else {
            const latest = self.promotions[self.promotions.len - 1];
            const current_digest = self.default_digest orelse return error.InvalidSqlExtensionControl;
            if (latest.generation != self.generation or !std.mem.eql(u8, latest.version, self.default_version) or !std.mem.eql(u8, &latest.package_digest, &current_digest)) return error.InvalidSqlExtensionControl;
        }
    }
    pub fn publicProjection(self: @This()) struct { name: []const u8, identity: [32]u8, generation: u64, default_version: []const u8, default_digest: ?[32]u8 } {
        return .{ .name = self.name, .identity = self.identity, .generation = self.generation, .default_version = self.default_version, .default_digest = self.default_digest };
    }
    pub fn cloneOwned(self: @This(), a: A) !@This() {
        var owned = self;
        owned.name = try a.dupe(u8, self.name);
        errdefer a.free(owned.name);
        owned.default_version = try a.dupe(u8, self.default_version);
        errdefer a.free(owned.default_version);
        const receipts = try a.alloc(PromotionReceipt, self.promotions.len);
        errdefer a.free(receipts);
        var initialized: usize = 0;
        errdefer for (receipts[0..initialized]) |receipt| a.free(receipt.version);
        for (self.promotions, 0..) |receipt, i| {
            receipts[i] = receipt;
            receipts[i].version = try a.dupe(u8, receipt.version);
            initialized += 1;
        }
        owned.promotions = receipts;
        return owned;
    }
    pub fn deinitOwned(self: *@This(), a: A) void {
        a.free(self.name);
        a.free(self.default_version);
        for (self.promotions) |receipt| a.free(receipt.version);
        if (self.promotions.len > 0) a.free(self.promotions);
        self.* = undefined;
    }
};
pub const max_promotion_receipts = 64;
pub const PromotionReceipt = struct {
    nonce: [16]u8,
    request_digest: [32]u8,
    generation: u64,
    version: []const u8,
    package_digest: [32]u8,
};
pub const Promotion = struct {
    name: []const u8,
    identity: [32]u8,
    expected_generation: u64,
    version: []const u8,
    package_digest: [32]u8,
    nonce: [16]u8,
    pub fn validate(self: @This()) !void {
        try domain.validateName(self.name);
        try versionName(self.version);
        if (self.expected_generation == 0 or !std.mem.eql(u8, &self.identity, &controlIdentity(self.name)) or std.mem.allEqual(u8, &self.package_digest, 0) or std.mem.allEqual(u8, &self.nonce, 0)) return error.InvalidSqlExtensionControl;
    }
    pub fn digest(self: @This(), a: A) ![32]u8 {
        try self.validate();
        const bytes = try std.json.Stringify.valueAlloc(a, self, .{});
        defer a.free(bytes);
        var sha = std.crypto.hash.sha2.Sha256.init(.{});
        sha.update("antfly:sql-extension-default-promotion:v1:");
        sha.update(bytes);
        return sha.finalResult();
    }
};
pub fn controlIdentity(name: []const u8) [32]u8 {
    var sha = std.crypto.hash.sha2.Sha256.init(.{});
    sha.update("antfly:sql-extension-package-control:v1:");
    sha.update(name);
    return sha.finalResult();
}

/// Pure admission used by both standalone owner and replicated native writer.
/// Null is an exact retained replay: the current pointer must stay unchanged.
pub fn promoteControl(a: A, registry: Registry, current: PackageControl, request: Promotion) !?PackageControl {
    try request.validate();
    try current.validate();
    const request_digest = try request.digest(a);
    for (current.promotions) |receipt| if (std.mem.eql(u8, &receipt.nonce, &request.nonce)) {
        if (!std.mem.eql(u8, &receipt.request_digest, &request_digest)) return error.SqlExtensionPromotionNonceConflict;
        return null;
    };
    if (!std.mem.eql(u8, current.name, request.name) or !std.mem.eql(u8, &current.identity, &request.identity) or current.generation != request.expected_generation) return error.SqlExtensionControlConflict;
    const target = try registry.select(request.name, request.version);
    const target_digest = try digest(target.digest);
    if (!std.mem.eql(u8, &target_digest, &request.package_digest)) return error.InvalidPackageDigest;
    var next = current;
    next.generation = std.math.add(u64, current.generation, 1) catch return error.SqlProgramLimitExceeded;
    next.default_version = target.version;
    next.default_digest = target_digest;
    const retained = if (current.promotions.len == max_promotion_receipts) current.promotions[1..] else current.promotions;
    const receipts = try a.alloc(PromotionReceipt, retained.len + 1);
    @memcpy(receipts[0..retained.len], retained);
    receipts[retained.len] = .{ .nonce = request.nonce, .request_digest = request_digest, .generation = next.generation, .version = target.version, .package_digest = target_digest };
    next.promotions = receipts;
    return next;
}

pub const Registry = struct {
    controls: []const PackageControl = &.{},
    control_receipts: bool = true,
    packages: []const packages.PackageManifest,

    pub fn validate(self: @This()) !void {
        var count: usize = 0;
        for (self.packages) |package| if (package.sql != null) {
            count += 1;
        };
        if (count > max_versions) return error.SqlProgramLimitExceeded;
        for (self.packages, 0..) |package, i| {
            _ = package.sql orelse continue;
            try package.validate();
            _ = try digest(package.digest);
            for (self.packages[0..i]) |prior| {
                if (!std.mem.eql(u8, prior.name, package.name)) continue;
                if (std.mem.eql(u8, prior.version, package.version)) return error.DuplicateSqlExtensionVersion;
                if (prior.sql) |previous| if (!optionalEqual(previous.schema, package.sql.?.schema) or prior.relocatable != package.relocatable or prior.trusted != package.trusted) return error.InconsistentSqlExtensionControl;
            }
        }
    }
    /// Validate portable registry/control custody before publication or restore.
    pub fn validateControls(self: @This()) !void {
        try self.validate();
        if (self.controls.len > max_versions) return error.SqlProgramLimitExceeded;
        for (self.controls, 0..) |control, i| {
            if (self.control_receipts) try control.validate() else try control.validateCompact();
            for (self.controls[0..i]) |prior| if (std.mem.eql(u8, prior.name, control.name)) return error.InvalidSqlExtensionControl;
            var found_package = false;
            var found_target = false;
            for (self.packages) |package| {
                if (package.sql == null or !std.mem.eql(u8, control.name, package.name)) continue;
                found_package = true;
                if (!std.mem.eql(u8, control.default_version, package.version)) continue;
                found_target = true;
                const expected = control.default_digest orelse return error.InvalidSqlExtensionControl;
                if (!std.mem.eql(u8, &expected, &try digest(package.digest))) return error.InvalidSqlExtensionControl;
            }
            if (!found_package or (control.default_digest != null and !found_target)) return error.InvalidSqlExtensionControl;
            for (control.promotions) |receipt| {
                const target = try self.select(control.name, receipt.version);
                if (!std.mem.eql(u8, &receipt.package_digest, &try digest(target.digest))) return error.InvalidSqlExtensionControl;
            }
        }
        for (self.packages) |package| if (package.sql != null) {
            var found = false;
            for (self.controls) |control| if (std.mem.eql(u8, control.name, package.name)) {
                found = true;
                break;
            };
            if (!found) return error.InvalidSqlExtensionControl;
        };
    }
    pub fn select(self: @This(), name: []const u8, requested: ?[]const u8) !packages.PackageManifest {
        var any: ?packages.PackageManifest = null;
        for (self.packages) |package| if (package.sql != null and std.mem.eql(u8, name, package.name)) {
            any = package;
            break;
        };
        _ = any orelse return error.SqlExtensionUnavailable;
        const version = requested orelse blk: {
            for (self.controls) |control| if (std.mem.eql(u8, name, control.name)) {
                if (control.default_digest == null) return error.SqlExtensionVersionUnavailable;
                break :blk control.default_version;
            };
            return error.SqlExtensionVersionUnavailable;
        };
        for (self.packages) |package| if (package.sql != null and std.mem.eql(u8, name, package.name) and std.mem.eql(u8, version, package.version)) return package;
        return error.SqlExtensionVersionUnavailable;
    }
    /// Breadth-first version graph traversal chooses the shortest declared
    /// migration path, including downgrades. Version strings are opaque like
    /// PostgreSQL, and an absent update path cannot become a direct replace.
    pub fn updatePath(self: @This(), a: A, name: []const u8, from: []const u8, requested: ?[]const u8) ![]const packages.PackageManifest {
        const destination = try self.select(name, requested);
        const start = try self.select(name, from);
        if (std.mem.eql(u8, start.version, destination.version)) return &.{};
        if (self.packages.len > max_versions) return error.SqlProgramLimitExceeded;
        const Node = struct { package: packages.PackageManifest, predecessor: ?usize };
        var queue: std.ArrayList(Node) = .empty;
        defer queue.deinit(a);
        try queue.append(a, .{ .package = start, .predecessor = null });
        var index: usize = 0;
        while (index < queue.items.len) : (index += 1) {
            for (self.packages) |target| {
                if (target.sql == null or !std.mem.eql(u8, name, target.name)) continue;
                var seen = false;
                for (queue.items) |prior| if (std.mem.eql(u8, prior.package.version, target.version)) {
                    seen = true;
                    break;
                };
                if (seen) continue;
                var edge = false;
                for (target.sql.?.updates_from) |from_version| if (std.mem.eql(u8, from_version, queue.items[index].package.version)) {
                    edge = true;
                    break;
                };
                if (!edge) continue;
                const position = queue.items.len;
                try queue.append(a, .{ .package = target, .predecessor = index });
                if (!std.mem.eql(u8, target.version, destination.version)) continue;
                var count: usize = 0;
                var cursor: ?usize = position;
                while (cursor) |node| {
                    cursor = queue.items[node].predecessor;
                    if (cursor != null) count += 1;
                }
                const out = try a.alloc(packages.PackageManifest, count);
                cursor = position;
                var remaining = count;
                while (remaining > 0) {
                    remaining -= 1;
                    out[remaining] = queue.items[cursor.?].package;
                    cursor = queue.items[cursor.?].predecessor;
                }
                return out;
            }
        }
        return error.SqlExtensionUpdatePathUnavailable;
    }
};
fn optionalEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null or right == null) return left == null and right == null;
    return std.mem.eql(u8, left.?, right.?);
}

test "SQL package selection uses configured default and shortest declared opaque-version update path" {
    const a = std.testing.allocator;
    const marker: Export = .{ .name = "marker", .runtime = "sql", .export_name = "marker", .arguments = &.{.{ .type = .{ .scalar = .int64 } }}, .result = .{ .scalar = .int64 } };
    const base: packages.PackageManifest = .{ .name = "sample", .version = "alpha", .digest = "1111111111111111111111111111111111111111111111111111111111111111", .relocatable = true, .install = .{ .scopes_supported = &.{.embedded_db}, .runtimes = &.{.{ .name = "sql", .mode = .wasm, .artifact = "scalar.wasm" }} }, .artifacts = &.{.{ .kind = .wasm, .path = "scalar.wasm", .digest = "2222222222222222222222222222222222222222222222222222222222222222" }}, .sql = .{ .default_version = "alpha", .exports = &.{marker} } };
    var middle = base;
    middle.version = "beta";
    middle.sql.?.updates_from = &.{"alpha"};
    var final = base;
    final.version = "gamma";
    final.sql.?.updates_from = &.{"beta"};
    const registry: Registry = .{ .packages = &.{ final, middle, base }, .controls = &.{.{ .name = "sample", .identity = controlIdentity("sample"), .generation = 1, .default_version = "alpha", .default_digest = try digest(base.digest) }} };
    try registry.validate();
    try std.testing.expectEqualStrings("alpha", (try registry.select("sample", null)).version);
    try std.testing.expectEqualStrings("gamma", (try registry.select("sample", "gamma")).version);
    const path = try registry.updatePath(a, "sample", "alpha", "gamma");
    defer a.free(path);
    try std.testing.expectEqual(@as(usize, 2), path.len);
    try std.testing.expectEqualStrings("beta", path[0].version);
    try std.testing.expectEqualStrings("gamma", path[1].version);
    try std.testing.expectError(error.SqlExtensionUpdatePathUnavailable, registry.updatePath(a, "sample", "gamma", "alpha"));
    try std.testing.expectError(error.SqlExtensionUnavailable, registry.select("unavailable", null));
    try std.testing.expectError(error.SqlExtensionVersionUnavailable, registry.select("sample", "absent"));
    var owned = try packages.clonePackageManifestAlloc(a, final);
    defer owned.deinitOwned(a);
    try std.testing.expectEqualStrings("beta", owned.sql.?.updates_from[0]);
    try std.testing.expectEqualStrings("marker", owned.sql.?.exports[0].name);
}

test "SQL extension controls reject duplicate contracts impure objects and unverifiable artifacts" {
    const marker: Export = .{ .name = "marker", .runtime = "sql", .export_name = "marker", .result = .{ .scalar = .int64 } };
    var package: packages.PackageManifest = .{ .name = "sample", .version = "1", .digest = "1111111111111111111111111111111111111111111111111111111111111111", .install = .{ .scopes_supported = &.{.embedded_db}, .runtimes = &.{.{ .name = "sql", .mode = .wasm, .artifact = "scalar.wasm" }} }, .artifacts = &.{.{ .kind = .wasm, .path = "scalar.wasm", .digest = "2222222222222222222222222222222222222222222222222222222222222222" }}, .sql = .{ .default_version = "1", .exports = &.{ marker, marker } } };
    try std.testing.expectError(error.DuplicateSqlExtensionExport, package.validate());
    package.sql.?.exports = &.{marker};
    package.install.objects = &.{.{ .kind = .mcp_tool, .name = "unplanned" }};
    try std.testing.expectError(error.UnsupportedSqlExtensionObject, package.validate());
    package.install.objects = &.{};
    package.artifacts = &.{.{ .kind = .wasm, .path = "scalar.wasm", .digest = "unverified" }};
    try std.testing.expectError(error.InvalidSqlExtensionArtifact, package.validate());
    try std.testing.expectError(error.InvalidSqlExtensionArtifact, relativePath("../scalar.wasm"));
    try std.testing.expectError(error.InvalidSqlExtensionArtifact, relativePath("nested/../scalar.wasm"));
    try std.testing.expectError(error.InvalidSqlExtensionArtifact, digest("0000000000000000000000000000000000000000000000000000000000000000"));
}

/// Caller has observed a valid control after its exact proposal applied. An
/// absent nonce inside the retained window is a definitive losing CAS; an
/// evicted window cannot distinguish a lost success from a competing winner.
pub fn missingPromotionOutcome(retained: usize, first_generation: u64, predicted_generation: u64) error{ MetadataMutationOutcomeUnknown, SqlExtensionControlConflict } {
    if (retained == max_promotion_receipts and first_generation > predicted_generation) return error.MetadataMutationOutcomeUnknown;
    return error.SqlExtensionControlConflict;
}

test "SQL extension promotion observations distinguish evicted outcomes from losing CAS" {
    try std.testing.expect(missingPromotionOutcome(64, 37, 36) == error.MetadataMutationOutcomeUnknown);
    try std.testing.expect(missingPromotionOutcome(64, 37, 37) == error.SqlExtensionControlConflict);
    try std.testing.expect(missingPromotionOutcome(64, 37, 100) == error.SqlExtensionControlConflict);
    try std.testing.expect(missingPromotionOutcome(1, 3, 2) == error.SqlExtensionControlConflict);
    try std.testing.expect(missingPromotionOutcome(0, 0, 2) == error.SqlExtensionControlConflict);
}
