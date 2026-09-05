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

const acquisition_stripe_count = 64;
const acquisition_bank_count = 2;
const max_resident_historical_epochs = 32;
const AcquisitionStripe = struct {
    readers: std.atomic.Value(usize) align(64) = .init(0),
    // Keep unrelated reader cohorts off the same cache line. Schema views are
    // acquired on every request, so a single acquisition-hazard counter
    // otherwise becomes a coherence bottleneck before the storage engine does.
    padding: [64 - @sizeOf(std.atomic.Value(usize))]u8 = undefined,
};

/// An immutable runtime schema generation. The version map owns one reference
/// and every pinned view owns another. Banked acquisition hazards protect the
/// small lock-free load-and-retain window while whole namespaces are replaced.
/// Historical versions remain available for decoding. A version is a permanent identity:
/// idempotent publication reuses the existing epoch and callers must reject
/// conflicting durable metadata before reaching the registry.
pub const Epoch = struct {
    alloc: Allocator,
    ref_count: std.atomic.Value(usize) = .init(1),
    schema: schema_mod.TableSchema,
    physical_layout: row_codec.PhysicalLayout,
    validator: ?schema_api.CompiledTableValidator = null,
    /// Protected by Registry.mutex. Active epochs do not participate in the
    /// historical LRU and therefore do not add an atomic write to request pins.
    historical_access: u64 = 0,

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

    fn cacheOwnsOnlyReference(self: *const Epoch) bool {
        return self.ref_count.load(.acquire) == 1;
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
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    current: std.atomic.Value(?*Epoch) = .init(null),
    /// Readers publish a short acquisition hazard without entering a mutex or
    /// suspending the current std.Io task. Replacement flips banks, then waits
    /// only for the old load-and-retain windows; returned SchemaViews own epoch
    /// references and never delay publication or reclamation admission.
    acquisition_generation: std.atomic.Value(u64) = .init(0),
    acquisition_readers: [acquisition_bank_count][acquisition_stripe_count]AcquisitionStripe =
        [_][acquisition_stripe_count]AcquisitionStripe{
            [_]AcquisitionStripe{.{}} ** acquisition_stripe_count,
        } ** acquisition_bank_count,
    namespace_generation: std.atomic.Value(u64) = .init(0),
    pending_publications: usize = 0,
    historical_clock: u64 = 0,
    /// The map owns the active epoch plus a bounded, refcount-aware cache of
    /// historical layouts. Evicted versions remain durable and are faulted back
    /// through DBCore.acquireSchemaVersionView when an old row needs them.
    epochs: std.AutoHashMapUnmanaged(u32, *Epoch) = .empty,

    pub fn initOwned(alloc: Allocator, io: std.Io, initial_schema: ?schema_mod.TableSchema) !Registry {
        var registry = Registry{ .alloc = alloc, .io = io };
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

    pub fn initCloned(alloc: Allocator, io: std.Io, initial_schema: ?schema_mod.TableSchema) !Registry {
        const owned = if (initial_schema) |schema| blk: {
            const encoded = try schema_mod.serializeSchema(alloc, schema);
            defer alloc.free(encoded);
            break :blk try schema_mod.deserializeSchema(alloc, encoded);
        } else null;
        return try initOwned(alloc, io, owned);
    }

    pub fn deinit(self: *Registry) void {
        deinitEpochMap(self.alloc, &self.epochs);
        self.* = undefined;
    }

    pub fn acquire(self: *Registry) ?SchemaView {
        const stripe_index = acquisitionStripeIndex();
        while (true) {
            // These generation/counter operations are intentionally seq_cst.
            // Their single total order guarantees one of two safe outcomes:
            // the replacer observes this hazard before reclaiming the old map,
            // or this reader observes the bank flip before dereferencing an
            // epoch. Do not weaken them independently to acquire/release.
            const generation = self.acquisition_generation.load(.seq_cst);
            const bank: usize = @intCast(generation & (acquisition_bank_count - 1));
            const stripe = &self.acquisition_readers[bank][stripe_index];
            _ = stripe.readers.fetchAdd(1, .seq_cst);
            // A replacement which changed banks may already be reclaiming the
            // old namespace. Retry before touching `current`; the writer waits
            // for this old-bank hazard before releasing any epoch references.
            if (self.acquisition_generation.load(.seq_cst) != generation) {
                const previous = stripe.readers.fetchSub(1, .seq_cst);
                std.debug.assert(previous > 0);
                std.atomic.spinLoopHint();
                continue;
            }
            const epoch = self.current.load(.acquire);
            if (epoch) |value| value.retain();
            const previous = stripe.readers.fetchSub(1, .seq_cst);
            std.debug.assert(previous > 0);
            return if (epoch) |value| .{ .epoch = value } else null;
        }
    }

    pub fn acquireVersion(self: *Registry, version: u32) ?SchemaView {
        // Historical lookups are rare. Keep active-row materialization on the
        // same RCU fast path as ordinary request pinning.
        var current = self.acquire();
        if (current) |*view| {
            if (view.version() == version) return view.*;
            view.release();
        }
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const epoch = self.epochs.get(version) orelse return null;
        self.touchHistoricalLocked(epoch);
        epoch.retain();
        return .{ .epoch = epoch };
    }

    fn touchHistoricalLocked(self: *Registry, epoch: *Epoch) void {
        if (self.current.load(.acquire) == epoch) return;
        self.historical_clock +%= 1;
        if (self.historical_clock == 0) self.historical_clock = 1;
        epoch.historical_access = self.historical_clock;
    }

    /// Remove one least-recently-used cache-only historical epoch. Pinned views
    /// are never selected; their references bound temporary overflow by active
    /// reader concurrency rather than by schema-update history.
    fn takeHistoricalEvictionLocked(self: *Registry) ?*Epoch {
        const active_allowance: usize = @intFromBool(self.current.load(.acquire) != null);
        if (self.epochs.count() <= max_resident_historical_epochs + active_allowance) return null;
        const active = self.current.load(.acquire);
        var candidate_version: ?u32 = null;
        var candidate_access: u64 = std.math.maxInt(u64);
        var iterator = self.epochs.iterator();
        while (iterator.next()) |entry| {
            const epoch = entry.value_ptr.*;
            if (epoch == active or !epoch.cacheOwnsOnlyReference()) continue;
            if (candidate_version == null or epoch.historical_access < candidate_access) {
                candidate_version = entry.key_ptr.*;
                candidate_access = epoch.historical_access;
            }
        }
        const version = candidate_version orelse return null;
        return self.epochs.fetchRemove(version).?.value;
    }

    /// Close every lock-free load-and-retain window that could still hold the
    /// previous `current` pointer. Schema publication is rare, so paying this
    /// short RCU grace period here keeps request acquisition allocation-free and
    /// lets historical cache eviction safely release its map reference.
    fn advanceAcquisitionGracePeriodLocked(self: *Registry) void {
        const retired_generation = self.acquisition_generation.load(.seq_cst);
        const retired_bank: usize = @intCast(retired_generation & (acquisition_bank_count - 1));
        self.acquisition_generation.store(retired_generation +% 1, .seq_cst);
        for (&self.acquisition_readers[retired_bank]) |*stripe| {
            var spins: usize = 0;
            while (stripe.readers.load(.seq_cst) != 0) : (spins +|= 1) {
                if (spins < 64) {
                    std.atomic.spinLoopHint();
                } else {
                    // A preempted or suspended acquirer must not turn rare
                    // schema publication into an unbounded CPU spin. Yield via
                    // the injected runtime so this remains fiber-aware.
                    self.io.sleep(.fromNanoseconds(1), .awake) catch {};
                }
            }
        }
    }

    /// Returns whether `view` is the exact active schema generation. Callers
    /// which need the answer to remain stable must hold their schema mutation
    /// fence (the DB apply lock). Comparing the epoch identity, rather than
    /// only its layout version, also detects validator-only publications.
    pub fn isCurrent(self: *const Registry, view: SchemaView) bool {
        return self.current.load(.acquire) == view.epoch;
    }

    pub const PublishReservation = struct {
        registry: *Registry,
        generation: u64,
        version: u32,
        active: bool = true,

        pub fn deinit(self: *@This()) void {
            if (!self.active) return;
            self.registry.mutex.lockUncancelable(self.registry.io);
            std.debug.assert(self.registry.pending_publications > 0);
            self.registry.pending_publications -= 1;
            self.registry.mutex.unlock(self.registry.io);
            self.active = false;
        }

        pub fn isCurrent(self: *const @This()) bool {
            return self.active and
                self.registry.namespace_generation.load(.acquire) == self.generation;
        }

        pub fn publish(self: *@This(), prepared: *Epoch) void {
            if (!self.active or prepared.schema.version != self.version)
                @panic("invalid schema publication reservation");
            self.registry.mutex.lockUncancelable(self.registry.io);
            if (self.registry.pending_publications == 0 or
                self.registry.namespace_generation.load(.acquire) != self.generation)
                @panic("stale schema publication reservation");
            self.registry.pending_publications -= 1;
            self.active = false;
            if (self.registry.epochs.get(prepared.schema.version)) |existing| {
                const previous_current = self.registry.current.load(.acquire);
                self.registry.current.store(existing, .release);
                if (previous_current != existing) self.registry.advanceAcquisitionGracePeriodLocked();
                if (previous_current) |epoch| self.registry.touchHistoricalLocked(epoch);
                const retired = self.registry.takeHistoricalEvictionLocked();
                self.registry.mutex.unlock(self.registry.io);
                prepared.release();
                if (retired) |epoch| epoch.release();
                return;
            }
            const previous_current = self.registry.current.load(.acquire);
            self.registry.epochs.putAssumeCapacity(prepared.schema.version, prepared);
            self.registry.current.store(prepared, .release);
            if (previous_current != prepared) self.registry.advanceAcquisitionGracePeriodLocked();
            if (previous_current) |epoch| self.registry.touchHistoricalLocked(epoch);
            const retired = self.registry.takeHistoricalEvictionLocked();
            self.registry.mutex.unlock(self.registry.io);
            if (retired) |epoch| epoch.release();
        }
    };

    /// Reserve a distinct map insertion before the durable schema transaction.
    /// Concurrent preparations are counted so none can consume another's slot.
    pub fn preparePublish(self: *Registry, version: u32) !PublishReservation {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const required = std.math.add(usize, self.pending_publications, 1) catch
            return error.SchemaCapacityExceeded;
        try self.epochs.ensureUnusedCapacity(
            self.alloc,
            std.math.cast(u32, required) orelse return error.SchemaCapacityExceeded,
        );
        self.pending_publications = required;
        return .{
            .registry = self,
            .generation = self.namespace_generation.load(.acquire),
            .version = version,
        };
    }

    pub const PreparedReplacement = struct {
        alloc: Allocator,
        epochs: std.AutoHashMapUnmanaged(u32, *Epoch) = .empty,
        current: ?*Epoch,
        active: bool = true,

        pub fn deinit(self: *@This()) void {
            if (!self.active) return;
            deinitEpochMap(self.alloc, &self.epochs);
            self.* = undefined;
        }
    };

    /// Allocate the complete successor namespace before the durable store
    /// swap. Publication is therefore infallible.
    /// On success this object owns `prepared`.
    pub fn prepareReplaceAll(self: *Registry, prepared: ?*Epoch) !PreparedReplacement {
        var epochs: std.AutoHashMapUnmanaged(u32, *Epoch) = .empty;
        errdefer epochs.deinit(self.alloc);
        if (prepared) |epoch| {
            try epochs.ensureTotalCapacity(self.alloc, 1);
            epochs.putAssumeCapacity(epoch.schema.version, epoch);
        }
        return .{
            .alloc = self.alloc,
            .epochs = epochs,
            .current = prepared,
        };
    }

    pub fn prepareHistoricalCapacity(self: *Registry, versions: []const u32) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
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
        const required = std.math.add(usize, self.pending_publications, missing) catch
            return error.SchemaCapacityExceeded;
        try self.epochs.ensureUnusedCapacity(
            self.alloc,
            std.math.cast(u32, required) orelse return error.SchemaCapacityExceeded,
        );
    }

    /// Publish outside a durable commit path. Reservations are preserved for
    /// callers which already promised allocation-free publication.
    pub fn publishPrepared(self: *Registry, prepared: *Epoch) !void {
        errdefer prepared.release();
        self.mutex.lockUncancelable(self.io);
        var locked = true;
        defer if (locked) self.mutex.unlock(self.io);
        if (self.epochs.get(prepared.schema.version)) |existing| {
            const previous_current = self.current.load(.acquire);
            self.current.store(existing, .release);
            if (previous_current != existing) self.advanceAcquisitionGracePeriodLocked();
            if (previous_current) |epoch| self.touchHistoricalLocked(epoch);
            const retired = self.takeHistoricalEvictionLocked();
            self.mutex.unlock(self.io);
            locked = false;
            prepared.release();
            if (retired) |epoch| epoch.release();
            return;
        }
        const required = std.math.add(usize, self.pending_publications, 1) catch
            return error.SchemaCapacityExceeded;
        self.epochs.ensureUnusedCapacity(
            self.alloc,
            std.math.cast(u32, required) orelse return error.SchemaCapacityExceeded,
        ) catch |err| {
            return err;
        };
        const previous_current = self.current.load(.acquire);
        self.epochs.putAssumeCapacity(prepared.schema.version, prepared);
        self.current.store(prepared, .release);
        if (previous_current != prepared) self.advanceAcquisitionGracePeriodLocked();
        if (previous_current) |epoch| self.touchHistoricalLocked(epoch);
        const retired = self.takeHistoricalEvictionLocked();
        self.mutex.unlock(self.io);
        locked = false;
        if (retired) |epoch| epoch.release();
    }

    /// Replace the version namespace after an atomic whole-store restore.
    /// Publication flips the acquisition bank and waits only for readers which
    /// were inside the old load-and-retain window. New readers immediately use
    /// the successor bank; request-held views retain their epochs independently.
    pub fn replaceAllPrepared(self: *Registry, replacement: *PreparedReplacement) void {
        if (!replacement.active) @panic("schema replacement already consumed");
        self.mutex.lockUncancelable(self.io);
        var retired_epochs = self.epochs;
        self.epochs = replacement.epochs;
        self.current.store(replacement.current, .release);
        // See acquire(): the seq_cst flip and counter observations form the
        // acquisition grace-period handshake across distinct atomics.
        self.advanceAcquisitionGracePeriodLocked();
        _ = self.namespace_generation.fetchAdd(1, .acq_rel);
        replacement.active = false;
        self.mutex.unlock(self.io);
        deinitEpochMap(self.alloc, &retired_epochs);
    }

    /// Publish the absence of an active schema while retaining historical
    /// layouts. Readers which already pinned a view remain valid and a later
    /// row carrying an old layout version can still resolve that epoch.
    pub fn clearCurrent(self: *Registry) void {
        self.mutex.lockUncancelable(self.io);
        const previous_current = self.current.load(.acquire);
        self.current.store(null, .release);
        if (previous_current != null) self.advanceAcquisitionGracePeriodLocked();
        if (previous_current) |epoch| self.touchHistoricalLocked(epoch);
        const retired = self.takeHistoricalEvictionLocked();
        self.mutex.unlock(self.io);
        if (retired) |epoch| epoch.release();
    }

    pub fn installHistorical(self: *Registry, prepared: *Epoch) !SchemaView {
        self.mutex.lockUncancelable(self.io);
        var locked = true;
        defer if (locked) self.mutex.unlock(self.io);
        if (self.epochs.get(prepared.schema.version)) |existing| {
            self.touchHistoricalLocked(existing);
            existing.retain();
            self.mutex.unlock(self.io);
            locked = false;
            prepared.release();
            return .{ .epoch = existing };
        }
        const required = std.math.add(usize, self.pending_publications, 1) catch
            return error.SchemaCapacityExceeded;
        self.epochs.ensureUnusedCapacity(
            self.alloc,
            std.math.cast(u32, required) orelse return error.SchemaCapacityExceeded,
        ) catch |err| {
            return err;
        };
        self.epochs.putAssumeCapacity(prepared.schema.version, prepared);
        self.touchHistoricalLocked(prepared);
        prepared.retain();
        const retired = self.takeHistoricalEvictionLocked();
        self.mutex.unlock(self.io);
        locked = false;
        if (retired) |epoch| epoch.release();
        return .{ .epoch = prepared };
    }
};

fn deinitEpochMap(alloc: Allocator, epochs: *std.AutoHashMapUnmanaged(u32, *Epoch)) void {
    var iterator = epochs.valueIterator();
    while (iterator.next()) |epoch| epoch.*.release();
    epochs.deinit(alloc);
}

fn acquisitionStripeIndex() usize {
    if (comptime builtin.single_threaded) return 0;
    var stack_marker: u8 = 0;
    const execution_address = @intFromPtr(&stack_marker);
    // Fiber stacks provide a stable discriminator across a potentially
    // suspending std.Io lock acquisition without binding schema lifetime code
    // to an OS-thread scheduler.
    var mixed = execution_address ^ (execution_address >> 16);
    mixed *%= 0x9e3779b1;
    return mixed & (acquisition_stripe_count - 1);
}

test "schema views keep retired epochs alive" {
    const alloc = std.testing.allocator;
    var registry = try Registry.initCloned(alloc, std.testing.io, .{ .version = 1 });
    defer registry.deinit();

    var old = registry.acquire().?;
    defer old.release();
    const replacement = try Epoch.createCloned(alloc, .{ .version = 2, .storage_mode = .relational });
    try registry.publishPrepared(replacement);

    try std.testing.expectEqual(@as(u32, 1), old.version());
    var current = registry.acquire().?;
    defer current.release();
    try std.testing.expectEqual(@as(u32, 2), current.version());
    try std.testing.expectEqual(schema_mod.StorageMode.relational, current.storageMode());
}

test "same-version publication preserves immutable epoch identity" {
    const alloc = std.testing.allocator;
    var registry = try Registry.initCloned(alloc, std.testing.io, .{ .version = 4 });
    defer registry.deinit();

    var pinned = registry.acquire().?;
    const original = pinned.epoch;
    try std.testing.expect(registry.isCurrent(pinned));
    try std.testing.expectEqual(@as(usize, 2), original.ref_count.load(.acquire));
    const replacement = try Epoch.createCloned(alloc, .{ .version = 4, .storage_mode = .relational });
    try registry.publishPrepared(replacement);
    try std.testing.expect(registry.isCurrent(pinned));
    try std.testing.expectEqual(@as(usize, 2), original.ref_count.load(.acquire));
    pinned.release();

    var current = registry.acquire().?;
    defer current.release();
    try std.testing.expectEqual(schema_mod.StorageMode.document, current.storageMode());
}

test "historical epoch residency is bounded while pinned views remain valid" {
    const alloc = std.testing.allocator;
    var registry = try Registry.initCloned(alloc, std.testing.io, .{ .version = 1 });
    defer registry.deinit();

    var pinned = registry.acquire().?;
    for (2..max_resident_historical_epochs + 20) |version| {
        try registry.publishPrepared(try Epoch.createCloned(alloc, .{ .version = @intCast(version) }));
    }
    try std.testing.expectEqual(@as(u32, 1), pinned.version());
    try std.testing.expect(registry.epochs.count() <= max_resident_historical_epochs + 1);
    pinned.release();

    for (max_resident_historical_epochs + 20..max_resident_historical_epochs + 40) |version| {
        try registry.publishPrepared(try Epoch.createCloned(alloc, .{ .version = @intCast(version) }));
    }
    try std.testing.expect(registry.epochs.count() <= max_resident_historical_epochs + 1);
    var current = registry.acquire().?;
    defer current.release();
    try std.testing.expectEqual(@as(u32, max_resident_historical_epochs + 39), current.version());
}

test "whole generation replacement does not wait for pinned views" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var registry = try Registry.initCloned(std.testing.allocator, io, .{ .version = 1 });
    defer registry.deinit();

    var pinned = registry.acquire().?;
    const old_epoch = pinned.epoch;
    const replacement_epoch = try Epoch.createCloned(std.testing.allocator, .{ .version = 2 });
    var replacement = try registry.prepareReplaceAll(replacement_epoch);
    defer replacement.deinit();

    const Publisher = struct {
        fn run(target: *Registry, prepared: *Registry.PreparedReplacement, done: *std.atomic.Value(bool)) void {
            target.replaceAllPrepared(prepared);
            done.store(true, .release);
        }
    };
    var done: std.atomic.Value(bool) = .init(false);
    var publisher = std.Io.async(io, Publisher.run, .{ &registry, &replacement, &done });
    var awaited = false;
    defer if (!awaited) publisher.await(io);
    for (0..5_000) |_| {
        if (done.load(.acquire)) break;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(done.load(.acquire));
    publisher.await(io);
    awaited = true;
    try std.testing.expectEqual(@as(u32, 1), pinned.version());
    try std.testing.expectEqual(@as(usize, 1), old_epoch.ref_count.load(.acquire));
    pinned.release();
}

test "concurrent publication reservations preserve capacity and generation fences" {
    const alloc = std.testing.allocator;
    var registry = try Registry.initCloned(alloc, std.testing.io, .{ .version = 1 });
    defer registry.deinit();

    var first = try registry.preparePublish(2);
    defer first.deinit();
    var second = try registry.preparePublish(3);
    defer second.deinit();
    first.publish(try Epoch.createCloned(alloc, .{ .version = 2 }));
    second.publish(try Epoch.createCloned(alloc, .{ .version = 3 }));
    try std.testing.expectEqual(@as(usize, 0), registry.pending_publications);

    var stale = try registry.preparePublish(4);
    defer stale.deinit();
    var replacement = try registry.prepareReplaceAll(null);
    defer replacement.deinit();
    registry.replaceAllPrepared(&replacement);
    try std.testing.expect(!stale.isCurrent());
}

test "whole generation replacement isolates reused schema versions" {
    const alloc = std.testing.allocator;
    var registry = try Registry.initCloned(alloc, std.testing.io, .{ .version = 7 });
    defer registry.deinit();

    var old = registry.acquire().?;
    const old_epoch = old.epoch;
    const replacement_epoch = try Epoch.createCloned(alloc, .{ .version = 7, .storage_mode = .relational });
    var replacement = try registry.prepareReplaceAll(replacement_epoch);
    defer replacement.deinit();
    registry.replaceAllPrepared(&replacement);

    // Replacement releases the registry's reference after the acquisition
    // grace period. The pinned view independently keeps the old epoch alive.
    try std.testing.expectEqual(@as(usize, 1), old_epoch.ref_count.load(.acquire));
    try std.testing.expectEqual(schema_mod.StorageMode.document, old_epoch.schema.storage_mode);
    var current = registry.acquire().?;
    defer current.release();
    try std.testing.expectEqual(@as(u32, 7), current.version());
    try std.testing.expectEqual(schema_mod.StorageMode.relational, current.storageMode());
    try std.testing.expect(registry.current.load(.acquire) != old_epoch);
    old.release();
}

test "banked acquisition remains safe across repeated concurrent replacement" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    const reader_count = 8;
    var io_impl = std.Io.Threaded.init(alloc, .{
        .concurrent_limit = .limited(reader_count),
    });
    defer io_impl.deinit();
    const io = io_impl.io();
    var registry = try Registry.initCloned(alloc, io, .{ .version = 1 });
    defer registry.deinit();

    var ready: std.atomic.Value(usize) = .init(0);
    var start: std.atomic.Value(bool) = .init(false);
    var stop: std.atomic.Value(bool) = .init(false);
    var failed: std.atomic.Value(bool) = .init(false);
    var observations: std.atomic.Value(usize) = .init(0);
    const Reader = struct {
        fn run(
            task_io: std.Io,
            target: *Registry,
            ready_flag: *std.atomic.Value(usize),
            start_flag: *std.atomic.Value(bool),
            stop_flag: *std.atomic.Value(bool),
            failed_flag: *std.atomic.Value(bool),
            observed: *std.atomic.Value(usize),
        ) std.Io.Cancelable!void {
            _ = ready_flag.fetchAdd(1, .release);
            while (!start_flag.load(.acquire)) try task_io.sleep(.fromMilliseconds(1), .awake);

            var local_count: usize = 0;
            while (!stop_flag.load(.acquire)) : (local_count += 1) {
                var view = target.acquire() orelse {
                    failed_flag.store(true, .release);
                    continue;
                };
                const version = view.version();
                const expected_mode: schema_mod.StorageMode = if (version & 1 == 0) .relational else .document;
                if (version == 0 or
                    view.storageMode() != expected_mode or
                    view.physicalLayout().schema_version != version)
                    failed_flag.store(true, .release);

                // Occasionally suspend while holding the independently retained
                // view. Replacement must wait only for the acquisition window,
                // not for this request-lifetime pin.
                if (local_count & 63 == 0) try task_io.sleep(.fromNanoseconds(1), .awake);
                view.release();
                _ = observed.fetchAdd(1, .monotonic);
            }
        }
    };

    var readers = std.Io.Group.init;
    var readers_active = true;
    defer if (readers_active) readers.cancel(io);
    for (0..reader_count) |_| {
        try readers.concurrent(io, Reader.run, .{
            io,
            &registry,
            &ready,
            &start,
            &stop,
            &failed,
            &observations,
        });
    }
    while (ready.load(.acquire) != reader_count) try io.sleep(.fromMilliseconds(1), .awake);
    start.store(true, .release);

    for (2..1_002) |version| {
        const replacement_epoch = try Epoch.createCloned(alloc, .{
            .version = @intCast(version),
            .storage_mode = if (version & 1 == 0) .relational else .document,
        });
        var replacement = try registry.prepareReplaceAll(replacement_epoch);
        registry.replaceAllPrepared(&replacement);
        replacement.deinit();
        if (version & 15 == 0) try io.sleep(.fromNanoseconds(1), .awake);
    }

    stop.store(true, .release);
    try readers.await(io);
    readers_active = false;
    try std.testing.expect(!failed.load(.acquire));
    try std.testing.expect(observations.load(.acquire) >= reader_count);
    var current = registry.acquire().?;
    defer current.release();
    try std.testing.expectEqual(@as(u32, 1_001), current.version());
    try std.testing.expectEqual(schema_mod.StorageMode.document, current.storageMode());
}
