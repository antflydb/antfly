// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");
const schema_mod = @import("../schema.zig");
const schema_api = @import("../../schema/mod.zig");
const row_codec = @import("algebraic/relational_row_codec.zig");

const Allocator = std.mem.Allocator;

/// An immutable runtime schema generation. The version map owns one reference
/// and every SchemaView owns another. Historical versions remain available for
/// decoding. A version is a permanent identity: idempotent publication reuses
/// the existing epoch and callers must reject conflicting durable metadata
/// before reaching the registry.
pub const Epoch = struct {
    alloc: Allocator,
    ref_count: std.atomic.Value(usize) = .init(1),
    schema: schema_mod.TableSchema,
    physical_layout: row_codec.PhysicalLayout,
    validator: ?schema_api.CompiledTableValidator = null,

    pub fn createOwned(alloc: Allocator, owned_schema: schema_mod.TableSchema) !*Epoch {
        var physical_layout = try row_codec.PhysicalLayout.init(alloc, owned_schema);
        errdefer physical_layout.deinit();
        const epoch = try alloc.create(Epoch);
        epoch.* = .{ .alloc = alloc, .schema = owned_schema, .physical_layout = physical_layout };
        return epoch;
    }

    pub fn createCloned(alloc: Allocator, schema: schema_mod.TableSchema) !*Epoch {
        const encoded = try schema_mod.serializeSchema(alloc, schema);
        defer alloc.free(encoded);
        const owned_schema = try schema_mod.deserializeSchema(alloc, encoded);
        var owned = true;
        errdefer if (owned) schema_mod.freeSchema(alloc, owned_schema);
        const epoch = try createOwned(alloc, owned_schema);
        owned = false;
        return epoch;
    }

    pub fn createOwnedValidated(
        alloc: Allocator,
        owned_schema: schema_mod.TableSchema,
        owned_validator: ?schema_api.CompiledTableValidator,
    ) !*Epoch {
        if (owned_validator) |validator| {
            if (validator.schema.version != owned_schema.version) return error.InvalidSchemaUpdateRequest;
            const derived = try schema_api.deriveRuntimeTableSchema(alloc, validator.schema);
            defer schema_mod.freeSchema(alloc, derived);
            if (!(try schema_mod.schemasEqual(alloc, owned_schema, derived)))
                return error.InvalidSchemaUpdateRequest;
        }
        var physical_layout = try row_codec.PhysicalLayout.init(alloc, owned_schema);
        errdefer physical_layout.deinit();
        const epoch = try alloc.create(Epoch);
        epoch.* = .{
            .alloc = alloc,
            .schema = owned_schema,
            .physical_layout = physical_layout,
            .validator = owned_validator,
        };
        return epoch;
    }

    fn retain(self: *Epoch) void {
        const previous = self.ref_count.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0);
    }

    pub fn release(self: *Epoch) void {
        const previous = self.ref_count.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;
        if (self.validator) |*validator| validator.deinit(self.alloc);
        self.physical_layout.deinit();
        schema_mod.freeSchema(self.alloc, self.schema);
        self.alloc.destroy(self);
    }
};

pub const SchemaView = struct {
    epoch: *Epoch,

    pub fn tableSchema(self: SchemaView) *const schema_mod.TableSchema {
        return &self.epoch.schema;
    }

    pub fn version(self: SchemaView) u32 {
        return self.epoch.schema.version;
    }

    pub fn storageMode(self: SchemaView) schema_mod.StorageMode {
        return self.epoch.schema.storage_mode;
    }

    pub fn validator(self: SchemaView) ?schema_api.CompiledTableValidator {
        return self.epoch.validator;
    }

    pub fn physicalLayout(self: SchemaView) *const row_codec.PhysicalLayout {
        return &self.epoch.physical_layout;
    }

    pub fn release(self: *SchemaView) void {
        self.epoch.release();
        self.* = undefined;
    }
};

pub const Registry = struct {
    alloc: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    current: std.atomic.Value(?*Epoch) = .init(null),
    /// The map owns one permanent reference to every published epoch. Therefore
    /// `current` never points at reclaimable storage during the registry's
    /// lifetime and readers do not need a globally contended acquisition gate.
    epochs: std.AutoHashMapUnmanaged(u32, *Epoch) = .empty,
    /// Whole-database publication may reuse version numbers for unrelated
    /// schemas. Retain displaced generations until registry teardown so a
    /// lock-free acquire which raced the publication can still safely retain
    /// the old epoch, while new lookups only see the replacement generation.
    retired_epochs: std.ArrayListUnmanaged(*Epoch) = .empty,

    pub fn initOwned(alloc: Allocator, initial_schema: ?schema_mod.TableSchema) !Registry {
        var registry = Registry{ .alloc = alloc };
        errdefer registry.epochs.deinit(alloc);
        if (initial_schema) |schema| {
            var schema_owned = true;
            errdefer if (schema_owned) schema_mod.freeSchema(alloc, schema);
            const epoch = try Epoch.createOwned(alloc, schema);
            schema_owned = false;
            errdefer epoch.release();
            try registry.epochs.put(alloc, schema.version, epoch);
            registry.current.store(epoch, .release);
        }
        return registry;
    }

    pub fn initCloned(alloc: Allocator, initial_schema: ?schema_mod.TableSchema) !Registry {
        const owned = if (initial_schema) |schema| blk: {
            const encoded = try schema_mod.serializeSchema(alloc, schema);
            defer alloc.free(encoded);
            break :blk try schema_mod.deserializeSchema(alloc, encoded);
        } else null;
        return try initOwned(alloc, owned);
    }

    pub fn deinit(self: *Registry) void {
        var iterator = self.epochs.valueIterator();
        while (iterator.next()) |epoch| epoch.*.release();
        self.epochs.deinit(self.alloc);
        for (self.retired_epochs.items) |epoch| epoch.release();
        self.retired_epochs.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn acquire(self: *Registry) ?SchemaView {
        const epoch = self.current.load(.acquire) orelse return null;
        epoch.retain();
        return .{ .epoch = epoch };
    }

    pub fn acquireVersion(self: *Registry, version: u32) ?SchemaView {
        // Historical lookups are rare. Keep active-row materialization on the
        // same RCU fast path as ordinary request pinning.
        var current = self.acquire();
        if (current) |*view| {
            if (view.version() == version) return view.*;
            view.release();
        }
        lockMutex(&self.mutex);
        defer self.mutex.unlock();
        const epoch = self.epochs.get(version) orelse return null;
        epoch.retain();
        return .{ .epoch = epoch };
    }

    /// Returns whether `view` is the exact active schema generation. Callers
    /// which need the answer to remain stable must hold their schema mutation
    /// fence (the DB apply lock). Comparing the epoch identity, rather than
    /// only its layout version, also detects validator-only publications.
    pub fn isCurrent(self: *const Registry, view: SchemaView) bool {
        return self.current.load(.acquire) == view.epoch;
    }

    /// Reserve publication bookkeeping before the durable schema transaction.
    pub fn preparePublish(self: *Registry, version: u32) !void {
        lockMutex(&self.mutex);
        defer self.mutex.unlock();
        if (!self.epochs.contains(version)) try self.epochs.ensureUnusedCapacity(self.alloc, 1);
    }

    /// Reserve all bookkeeping needed to replace an entire durable database
    /// generation. Publication itself must remain allocation-free because it
    /// runs after the durable generation swap has committed.
    pub fn prepareReplaceAll(self: *Registry) !void {
        lockMutex(&self.mutex);
        defer self.mutex.unlock();
        try self.retired_epochs.ensureUnusedCapacity(self.alloc, self.epochs.count());
        try self.epochs.ensureTotalCapacity(self.alloc, 1);
    }

    pub fn prepareHistoricalCapacity(self: *Registry, versions: []const u32) !void {
        lockMutex(&self.mutex);
        defer self.mutex.unlock();
        var unique_missing = std.AutoHashMapUnmanaged(u32, void).empty;
        defer unique_missing.deinit(self.alloc);
        try unique_missing.ensureTotalCapacity(self.alloc, std.math.cast(u32, versions.len) orelse
            return error.SchemaCapacityExceeded);
        var missing: usize = 0;
        for (versions) |version| {
            if (self.epochs.contains(version)) continue;
            const result = unique_missing.getOrPutAssumeCapacity(version);
            if (!result.found_existing) missing += 1;
        }
        try self.epochs.ensureUnusedCapacity(
            self.alloc,
            std.math.cast(u32, missing) orelse return error.SchemaCapacityExceeded,
        );
    }

    /// Publish a fully prepared epoch. This operation cannot allocate or fail,
    /// so callers can perform all preparation before their durable commit.
    pub fn publishPrepared(self: *Registry, prepared: *Epoch) void {
        lockMutex(&self.mutex);
        if (self.epochs.get(prepared.schema.version)) |existing| {
            self.current.store(existing, .release);
            self.mutex.unlock();
            prepared.release();
            return;
        }
        self.epochs.putAssumeCapacity(prepared.schema.version, prepared);
        self.current.store(prepared, .release);
        self.mutex.unlock();
    }

    /// Replace the version namespace after an atomic whole-store restore.
    /// Displaced epochs remain retained for readers which raced publication,
    /// but can no longer satisfy lookups against the new durable generation.
    pub fn replaceAllPrepared(self: *Registry, prepared: ?*Epoch) void {
        lockMutex(&self.mutex);
        self.current.store(null, .release);
        var iterator = self.epochs.valueIterator();
        while (iterator.next()) |epoch| self.retired_epochs.appendAssumeCapacity(epoch.*);
        self.epochs.clearRetainingCapacity();
        if (prepared) |epoch| {
            self.epochs.putAssumeCapacity(epoch.schema.version, epoch);
            self.current.store(epoch, .release);
        }
        self.mutex.unlock();
    }

    /// Publish the absence of an active schema while retaining historical
    /// layouts. Readers which already pinned a view remain valid and a later
    /// row carrying an old layout version can still resolve that epoch.
    pub fn clearCurrent(self: *Registry) void {
        self.current.store(null, .release);
    }

    pub fn installHistorical(self: *Registry, prepared: *Epoch) SchemaView {
        lockMutex(&self.mutex);
        if (self.epochs.get(prepared.schema.version)) |existing| {
            existing.retain();
            self.mutex.unlock();
            prepared.release();
            return .{ .epoch = existing };
        }
        self.epochs.putAssumeCapacity(prepared.schema.version, prepared);
        prepared.retain();
        self.mutex.unlock();
        return .{ .epoch = prepared };
    }
};

fn lockMutex(mutex: *std.atomic.Mutex) void {
    var attempts: usize = 0;
    while (!mutex.tryLock()) : (attempts += 1) {
        if (attempts < 64) {
            std.atomic.spinLoopHint();
        } else if (comptime builtin.single_threaded) {
            std.atomic.spinLoopHint();
        } else {
            std.Thread.yield() catch {};
        }
    }
}

test "schema views keep retired epochs alive" {
    const alloc = std.testing.allocator;
    var registry = try Registry.initCloned(alloc, .{ .version = 1 });
    defer registry.deinit();

    var old = registry.acquire().?;
    defer old.release();
    const replacement = try Epoch.createCloned(alloc, .{ .version = 2, .storage_mode = .relational });
    registry.publishPrepared(replacement);

    try std.testing.expectEqual(@as(u32, 1), old.version());
    var current = registry.acquire().?;
    defer current.release();
    try std.testing.expectEqual(@as(u32, 2), current.version());
    try std.testing.expectEqual(schema_mod.StorageMode.relational, current.storageMode());
}

test "same-version publication preserves immutable epoch identity" {
    const alloc = std.testing.allocator;
    var registry = try Registry.initCloned(alloc, .{ .version = 4 });
    defer registry.deinit();

    var pinned = registry.acquire().?;
    const original = pinned.epoch;
    try std.testing.expect(registry.isCurrent(pinned));
    try std.testing.expectEqual(@as(usize, 2), original.ref_count.load(.acquire));
    const replacement = try Epoch.createCloned(alloc, .{ .version = 4, .storage_mode = .relational });
    registry.publishPrepared(replacement);
    try std.testing.expect(registry.isCurrent(pinned));
    try std.testing.expectEqual(@as(usize, 2), original.ref_count.load(.acquire));
    pinned.release();

    var current = registry.acquire().?;
    defer current.release();
    try std.testing.expectEqual(schema_mod.StorageMode.document, current.storageMode());
}

test "whole generation replacement isolates reused schema versions" {
    const alloc = std.testing.allocator;
    var registry = try Registry.initCloned(alloc, .{ .version = 7 });
    defer registry.deinit();

    var old = registry.acquire().?;
    defer old.release();
    const replacement = try Epoch.createCloned(alloc, .{ .version = 7, .storage_mode = .relational });
    try registry.prepareReplaceAll();
    registry.replaceAllPrepared(replacement);

    try std.testing.expectEqual(schema_mod.StorageMode.document, old.storageMode());
    var current = registry.acquire().?;
    defer current.release();
    try std.testing.expectEqual(@as(u32, 7), current.version());
    try std.testing.expectEqual(schema_mod.StorageMode.relational, current.storageMode());
    try std.testing.expect(!registry.isCurrent(old));
}
