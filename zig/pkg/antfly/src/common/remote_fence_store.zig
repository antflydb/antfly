// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Private worker-local generation floors. The directory must already be durable
//! and belong to this worker incarnation. Membership must authorize that binding;
//! a missing registry must never silently create a replacement incarnation.
const std = @import("std");
const attempts = @import("remote_attempts.zig");
pub const Wait = @import("antfly_runtime_abi").execution_wait.Boundary;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
pub const checkpoint_name = "remote-fences.v1";
const file_name = checkpoint_name;
const temporary_name = "remote-fences.v1.pending";
const lock_name = "remote-fences.lock";
// Keep the original basename so older images fail closed instead of appearing missing.
const magic = "AFRFEN03";
const header_size = 128;
const record_size = 24;
const Floor = struct { coordinator: u128, generation: u64 };

pub const InitializationId = [32]u8;
pub const InitializationGrant = struct {
    id: InitializationId,
    phase: enum { prepared, active },
};
/// Resolve from authoritative metadata AFTER the journal's exclusive lock is
/// acquired. A cached prepared grant could otherwise recreate an active journal
/// after the previous process published work and lost its local checkpoint.
pub const InitializationAuthority = struct {
    context: *anyopaque,
    resolve: *const fn (*anyopaque) anyerror!InitializationGrant,
};

pub const Store = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    worker_incarnation: u128,
    lock_file: std.Io.File,
    floors: []?Floor,
    outgoing_destinations: []u128,
    coordinator: u128 = 0,
    coordinator_generation: u64 = 0,
    initialization_id: InitializationId = @splat(0),
    publication_started: bool = false,
    buffer: []u8,
    mutex: std.Io.Mutex = .init,
    poisoned: bool = false,

    /// Explicit first initialization only, after authoritative incarnation setup.
    pub fn create(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, worker_incarnation: u128, capacity: usize) !Store {
        var self = try init(allocator, io, dir, worker_incarnation, capacity);
        errdefer self.deinit();
        _ = dir.statFile(io, file_name, .{}) catch |err| switch (err) {
            error.FileNotFound => {
                try self.publish();
                return self;
            },
            else => return err,
        };
        return error.RemoteFenceStoreAlreadyExists;
    }

    pub fn open(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, worker_incarnation: u128, capacity: usize) !Store {
        var self = try init(allocator, io, dir, worker_incarnation, capacity);
        errdefer self.deinit();
        try self.load();
        return self;
    }
    /// A prepared grant permits creation; an active grant can only reopen the
    /// exact existing journal. Never adopt unbound legacy state or replace a
    /// corrupt/mismatched checkpoint. The lock spans validation and publication.
    pub fn openAuthorized(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, worker_incarnation: u128, capacity: usize, authority: InitializationAuthority) !Store {
        var self = try init(allocator, io, dir, worker_incarnation, capacity);
        errdefer self.deinit();
        const grant = try authority.resolve(authority.context);
        if (std.mem.allEqual(u8, &grant.id, 0)) return error.InvalidRemoteAttemptConfig;
        self.load() catch |err| switch (err) {
            error.RemoteFenceStoreMissing => {
                if (grant.phase != .prepared) return err;
                self.initialization_id = grant.id;
                try self.publish();
                return self;
            },
            else => return err,
        };
        if (!std.mem.eql(u8, &self.initialization_id, &grant.id)) return error.RemoteInitializationChanged;
        if (grant.phase == .prepared and self.publication_started) return error.InvalidRemoteFenceStore;
        return self;
    }
    /// Fixed heap arrays and journal serialization buffer; Store is embedded.
    pub fn metadataBytes(capacity: usize) !u64 {
        if (capacity == 0 or capacity > 65535) return error.InvalidRemoteAttemptConfig;
        return std.math.add(u64, header_size, try std.math.mul(u64, capacity, @sizeOf(?Floor) + @sizeOf(u128) + record_size + 16));
    }

    fn init(allocator: std.mem.Allocator, io: std.Io, dir: std.Io.Dir, worker_incarnation: u128, capacity: usize) !Store {
        if (worker_incarnation == 0 or capacity == 0 or capacity > 65535) return error.InvalidRemoteAttemptConfig;
        const file = try dir.createFile(io, lock_name, .{ .read = true, .truncate = false });
        errdefer file.close(io);
        if (!try file.tryLock(io, .exclusive)) return error.RemoteFenceStoreBusy;
        errdefer file.unlock(io);
        const floors = try allocator.alloc(?Floor, capacity);
        errdefer allocator.free(floors);
        @memset(floors, null);
        const outgoing = try allocator.alloc(u128, capacity);
        errdefer allocator.free(outgoing);
        @memset(outgoing, 0);
        const buffer = try allocator.alloc(u8, header_size + capacity * (record_size + 16));
        return .{ .allocator = allocator, .io = io, .dir = dir, .worker_incarnation = worker_incarnation, .lock_file = file, .floors = floors, .outgoing_destinations = outgoing, .buffer = buffer };
    }
    pub fn deinit(self: *Store) void {
        self.lock_file.unlock(self.io);
        self.lock_file.close(self.io);
        self.allocator.free(self.buffer);
        self.allocator.free(self.outgoing_destinations);
        self.allocator.free(self.floors);
        self.* = undefined;
    }
    fn digest(bytes: []const u8) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(bytes[0..32]);
        hash.update(bytes[64..]);
        return hash.finalResult();
    }
    fn encode(self: *Store) void {
        @memset(self.buffer, 0);
        @memcpy(self.buffer[0..8], magic);
        std.mem.writeInt(u32, self.buffer[8..12], 3, .little);
        std.mem.writeInt(u32, self.buffer[12..16], @intCast(self.floors.len), .little);
        std.mem.writeInt(u128, self.buffer[16..32], self.worker_incarnation, .little);
        for (self.floors, 0..) |entry_slot, index| if (entry_slot) |value| {
            const bytes = self.buffer[header_size + index * record_size ..][0..record_size];
            std.mem.writeInt(u128, bytes[0..16], value.coordinator, .little);
            std.mem.writeInt(u64, bytes[16..24], value.generation, .little);
        };
        std.mem.writeInt(u128, self.buffer[64..80], self.coordinator, .little);
        std.mem.writeInt(u64, self.buffer[80..88], self.coordinator_generation, .little);
        self.buffer[88] = @intFromBool(self.publication_started);
        @memcpy(self.buffer[96..128], &self.initialization_id);
        const outgoing_start = header_size + self.floors.len * record_size;
        for (self.outgoing_destinations, 0..) |root, index|
            std.mem.writeInt(u128, self.buffer[outgoing_start + index * 16 ..][0..16], root, .little);
        @memcpy(self.buffer[32..64], &digest(self.buffer));
    }
    fn load(self: *Store) !void {
        const file = self.dir.openFile(self.io, file_name, .{}) catch |err| switch (err) {
            error.FileNotFound => return error.RemoteFenceStoreMissing,
            else => return err,
        };
        defer file.close(self.io);
        if ((try file.stat(self.io)).size != self.buffer.len) return error.InvalidRemoteFenceStore;
        if (try file.readPositionalAll(self.io, self.buffer, 0) != self.buffer.len) return error.InvalidRemoteFenceStore;
        if (!std.mem.eql(u8, self.buffer[0..8], magic) or std.mem.readInt(u32, self.buffer[8..12], .little) != 3 or
            std.mem.readInt(u32, self.buffer[12..16], .little) != self.floors.len or
            std.mem.readInt(u128, self.buffer[16..32], .little) != self.worker_incarnation or
            !std.mem.eql(u8, self.buffer[32..64], &digest(self.buffer))) return error.InvalidRemoteFenceStore;
        self.coordinator = std.mem.readInt(u128, self.buffer[64..80], .little);
        self.coordinator_generation = std.mem.readInt(u64, self.buffer[80..88], .little);
        if ((self.coordinator == 0) != (self.coordinator_generation == 0) or
            self.buffer[88] > 1 or !std.mem.allEqual(u8, self.buffer[89..96], 0)) return error.InvalidRemoteFenceStore;
        self.publication_started = self.buffer[88] != 0;
        @memcpy(&self.initialization_id, self.buffer[96..128]);
        const outgoing_start = header_size + self.floors.len * record_size;
        for (self.outgoing_destinations, 0..) |*root, index| {
            root.* = std.mem.readInt(u128, self.buffer[outgoing_start + index * 16 ..][0..16], .little);
            if (root.* == 0) continue;
            if (self.coordinator == 0 or !self.publication_started) return error.InvalidRemoteFenceStore;
            for (self.outgoing_destinations[0..index]) |prior| if (prior == root.*) return error.InvalidRemoteFenceStore;
        }
        for (self.floors, 0..) |*entry_slot, index| {
            const bytes = self.buffer[header_size + index * record_size ..][0..record_size];
            const coordinator = std.mem.readInt(u128, bytes[0..16], .little);
            const generation = std.mem.readInt(u64, bytes[16..24], .little);
            if (coordinator == 0 and generation == 0) continue;
            if (coordinator == 0 or generation == 0) return error.InvalidRemoteFenceStore;
            for (self.floors[0..index]) |prior| if (prior) |value| if (value.coordinator == coordinator) return error.InvalidRemoteFenceStore;
            entry_slot.* = .{ .coordinator = coordinator, .generation = generation };
        }
    }
    fn publish(self: *Store) !void {
        return self.withWait(null).publish();
    }
    pub const CoordinatorEpoch = struct { coordinator: u128, generation: u64 };
    pub fn floor(self: *Store, coordinator: u128) !u64 {
        return self.withWait(null).floor(coordinator);
    }
    pub fn advance(self: *Store, coordinator: u128, generation: u64) !void {
        return self.withWait(null).advance(coordinator, generation);
    }
    pub fn startCoordinator(self: *Store, coordinator: u128, observed_generation: u64, slots: []?attempts.Attempt, destinations: []attempts.Destination, max_bytes: usize) !attempts.Coordinator {
        return self.withWait(null).startCoordinator(coordinator, observed_generation, slots, destinations, max_bytes);
    }
    pub fn coordinatorEpoch(self: *Store) !CoordinatorEpoch {
        return self.withWait(null).coordinatorEpoch();
    }
    pub fn advanceCoordinator(self: *Store, ledger: *attempts.Coordinator) !void {
        return self.withWait(null).advanceCoordinator(ledger);
    }
    pub fn observeCoordinatorGeneration(self: *Store, coordinator: u128, generation: u64) !void {
        return self.withWait(null).observeCoordinatorGeneration(coordinator, generation);
    }
    pub fn destinationHistory(self: *Store, output: []u128) !usize {
        return self.withWait(null).destinationHistory(output);
    }
    fn rememberDestination(self: *Store, id: attempts.Id, root: u128) !void {
        return self.withWait(null).rememberDestination(id, root);
    }
    pub fn prepareDispatch(self: *Store, ledger: *attempts.Coordinator, id: attempts.Id) !void {
        return self.withWait(null).prepareDispatch(ledger, id);
    }
    /// Identity of the complete durable checkpoint under the exclusive journal
    /// lease. A poisoned or unpublished in-memory mutation is never evidence.
    pub fn checkpointDigest(self: *Store) ![32]u8 {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        if (self.poisoned) return error.RemoteFenceStoreUncertain;
        return digest(self.buffer);
    }

    pub const InitializationState = struct { id: InitializationId, publication_started: bool };
    pub fn initializationState(self: *Store) !InitializationState {
        return self.withWait(null).initializationState();
    }
    pub fn markPublicationStarted(self: *Store) !void {
        return self.withWait(null).markPublicationStarted();
    }
    pub fn restoreGate(self: *Store, coordinator: u128, records: []?attempts.WorkerRecord) !attempts.WorkerGate {
        return self.withWait(null).restoreGate(coordinator, records);
    }
    pub fn fence(self: *Store, gate: *attempts.WorkerGate, generation: u64) !attempts.FenceAck {
        return self.withWait(null).fence(gate, generation);
    }
    pub fn withWait(self: *Store, wait: ?Wait) Scope {
        return .{ .store = self, .wait = wait };
    }
    pub const Scope = struct {
        store: *Store,
        wait: ?Wait,

        fn run(self: Scope, comptime operation: anytype, args: anytype) anyerror!@typeInfo(@typeInfo(@TypeOf(operation)).@"fn".return_type.?).error_union.payload {
            if (self.wait) |boundary| return boundary.run(operation, args);
            return @call(.auto, operation, args);
        }
        fn lockOwned(store: *Store) !void {
            try store.mutex.lock(store.io);
        }
        fn unlockDiscard(_: void, store: *Store) void {
            store.mutex.unlock(store.io);
        }
        fn lock(self: Scope) !void {
            if (self.store.mutex.tryLock()) return;
            if (self.wait) |boundary| return boundary.runOwned(lockOwned, .{self.store}, unlockDiscard, .{self.store});
            try self.store.mutex.lock(self.store.io);
        }
        fn createPending(store: *Store) !std.Io.File {
            return store.dir.createFile(store.io, temporary_name, .{ .truncate = true });
        }
        fn closeDiscard(file: std.Io.File, io: std.Io) void {
            file.close(io);
        }
        fn writeAndClose(store: *Store, file: std.Io.File, live: *bool) !void {
            live.* = false;
            defer file.close(store.io);
            try file.writePositionalAll(store.io, store.buffer, 0);
            try file.sync(store.io);
        }
        fn renamePending(store: *Store) !void {
            try std.Io.Dir.rename(store.dir, temporary_name, store.dir, file_name, store.io);
        }
        fn syncDirectory(store: *Store) !void {
            try fs_paths.syncDirectoryHandlePortable(store.io, store.dir);
        }
        fn publish(self: Scope) !void {
            // Encoding and checksumming are CPU work, outside every wait loan.
            self.store.encode();
            const file = if (self.wait) |boundary|
                try boundary.runOwned(createPending, .{self.store}, closeDiscard, .{self.store.io})
            else
                try createPending(self.store);
            var live = true;
            // Mandatory discard uses the original owner even if begin/reentry
            // fails. Never acquire a new grant just to close this descriptor.
            defer if (live) file.close(self.store.io);
            try self.run(writeAndClose, .{ self.store, file, &live });
            try self.run(renamePending, .{self.store});
            try self.run(syncDirectory, .{self.store});
        }
        pub fn floor(self: Scope, coordinator: u128) !u64 {
            try self.lock();
            defer self.store.mutex.unlock(self.store.io);
            if (self.store.poisoned) return error.RemoteFenceStoreUncertain;
            for (self.store.floors) |value| if (value) |entry| if (entry.coordinator == coordinator) return entry.generation;
            return 0;
        }
        pub fn initializationState(self: Scope) !InitializationState {
            try self.lock();
            defer self.store.mutex.unlock(self.store.io);
            if (self.store.poisoned) return error.RemoteFenceStoreUncertain;
            return .{ .id = self.store.initialization_id, .publication_started = self.store.publication_started };
        }
        /// Persist before an inbound owner can start physical work. Once set,
        /// no terminal reply, fence, or epoch transition clears this history.
        pub fn markPublicationStarted(self: Scope) !void {
            try self.lock();
            defer self.store.mutex.unlock(self.store.io);
            if (self.store.poisoned) return error.RemoteFenceStoreUncertain;
            if (self.store.publication_started) return;
            self.store.publication_started = true;
            self.publish() catch |err| {
                self.store.poisoned = true;
                return err;
            };
        }
        pub fn advance(self: Scope, coordinator: u128, generation: u64) !void {
            try self.lock();
            defer self.store.mutex.unlock(self.store.io);
            if (self.store.poisoned) return error.RemoteFenceStoreUncertain;
            if (coordinator == 0 or generation == 0) return error.InvalidRemoteGeneration;
            var selected: ?*?Floor = null;
            for (self.store.floors) |*value| {
                if (value.*) |entry| {
                    if (entry.coordinator == coordinator) {
                        if (generation < entry.generation) return error.InvalidRemoteGeneration;
                        if (generation == entry.generation) return;
                        selected = value;
                        break;
                    }
                } else if (selected == null) selected = value;
            }
            const slot = selected orelse return error.RemoteFenceRegistryFull;
            slot.* = .{ .coordinator = coordinator, .generation = generation };
            self.publish() catch |err| {
                // Rename or directory sync may have published a newer floor. Do
                // not trust cached state or acknowledge anything until reopen.
                self.store.poisoned = true;
                return err;
            };
        }
        /// Persist a fresh outgoing epoch before exposing its closed ledger. Every
        /// previously published destination must remain in the fence barrier even
        /// when absent from current routing/health. History is never auto-evicted.
        pub fn startCoordinator(self: Scope, coordinator: u128, observed_generation: u64, slots: []?attempts.Attempt, destinations: []attempts.Destination, max_bytes: usize) !attempts.Coordinator {
            try self.lock();
            defer self.store.mutex.unlock(self.store.io);
            if (self.store.poisoned) return error.RemoteFenceStoreUncertain;
            if (coordinator == 0 or (self.store.coordinator != 0 and self.store.coordinator != coordinator)) return error.InvalidRemoteGeneration;
            for (self.store.outgoing_destinations) |root| {
                if (root == 0) continue;
                var found = false;
                for (destinations) |destination| if (destination.worker_incarnation == root) {
                    found = true;
                    break;
                };
                if (!found) return error.RemoteDestinationHistoryMissing;
            }
            const floor_generation = @max(self.store.coordinator_generation, observed_generation);
            if (floor_generation == std.math.maxInt(u64)) return error.RemoteGenerationExhausted;
            const generation = floor_generation + 1;
            const ledger = try attempts.Coordinator.init(coordinator, generation, slots, destinations, max_bytes);
            self.store.coordinator = coordinator;
            self.store.coordinator_generation = generation;
            self.publish() catch |err| {
                self.store.poisoned = true;
                return err;
            };
            return ledger;
        }

        pub fn coordinatorEpoch(self: Scope) !CoordinatorEpoch {
            try self.lock();
            defer self.store.mutex.unlock(self.store.io);
            if (self.store.poisoned) return error.RemoteFenceStoreUncertain;
            return .{ .coordinator = self.store.coordinator, .generation = self.store.coordinator_generation };
        }

        /// Runtime advancement retains published uncertainty in the existing
        /// ledger. Persist first; any later failure leaves publication closed by
        /// the store/ledger epoch mismatch until a newer successful transition.
        pub fn advanceCoordinator(self: Scope, ledger: *attempts.Coordinator) !void {
            const state = ledger.generationState();
            try self.lock();
            defer self.store.mutex.unlock(self.store.io);
            if (self.store.poisoned) return error.RemoteFenceStoreUncertain;
            if (self.store.coordinator == 0 or self.store.coordinator != state.coordinator) return error.InvalidRemoteGeneration;
            const floor_generation = @max(self.store.coordinator_generation, state.generation, state.observed_generation);
            if (floor_generation == std.math.maxInt(u64)) return error.RemoteGenerationExhausted;
            self.store.coordinator_generation = floor_generation + 1;
            self.publish() catch |err| {
                self.store.poisoned = true;
                return err;
            };
            try ledger.beginGeneration(self.store.coordinator_generation);
        }

        /// Only authenticated, destination-bound evidence may call this. Persist
        /// observed floors before applying a reply so restart cannot reuse them.
        pub fn observeCoordinatorGeneration(self: Scope, coordinator: u128, generation: u64) !void {
            try self.lock();
            defer self.store.mutex.unlock(self.store.io);
            if (self.store.poisoned) return error.RemoteFenceStoreUncertain;
            if (coordinator == 0 or coordinator != self.store.coordinator) return error.InvalidRemoteGeneration;
            if (generation <= self.store.coordinator_generation) return;
            self.store.coordinator_generation = generation;
            self.publish() catch |err| {
                self.store.poisoned = true;
                return err;
            };
        }

        pub fn destinationHistory(self: Scope, output: []u128) !usize {
            try self.lock();
            defer self.store.mutex.unlock(self.store.io);
            if (self.store.poisoned) return error.RemoteFenceStoreUncertain;
            var count: usize = 0;
            for (self.store.outgoing_destinations) |root| {
                if (root == 0) continue;
                if (count == output.len) return error.RemoteAttemptCapacity;
                output[count] = root;
                count += 1;
            }
            return count;
        }

        fn rememberDestination(self: Scope, id: attempts.Id, root: u128) !void {
            try self.lock();
            defer self.store.mutex.unlock(self.store.io);
            if (self.store.poisoned) return error.RemoteFenceStoreUncertain;
            if (id.coordinator != self.store.coordinator or id.generation != self.store.coordinator_generation or root == 0) return error.InvalidRemoteGeneration;
            var selected: ?*u128 = null;
            for (self.store.outgoing_destinations) |*prior| {
                if (prior.* == root) {
                    if (self.store.publication_started) return;
                    selected = prior;
                    break;
                }
                if (prior.* == 0 and selected == null) selected = prior;
            }
            const slot = selected orelse return error.RemoteDestinationHistoryFull;
            slot.* = root;
            self.store.publication_started = true;
            self.publish() catch |err| {
                self.store.poisoned = true;
                return err;
            };
        }

        /// The caller may publish wire bytes only after this returns successfully.
        /// A failed publication leaves a locally abortable reservation. No ledger
        /// mutex spans filesystem I/O; dispatch rechecks generation after persistence.
        pub fn prepareDispatch(self: Scope, ledger: *attempts.Coordinator, id: attempts.Id) !void {
            const root = try ledger.reservedDestination(id);
            try self.rememberDestination(id, root);
            try ledger.dispatch(id);
        }

        pub fn restoreGate(self: Scope, coordinator: u128, records: []?attempts.WorkerRecord) !attempts.WorkerGate {
            return attempts.WorkerGate.init(coordinator, self.store.worker_incarnation, try self.floor(coordinator), records);
        }
        pub fn fence(self: Scope, gate: *attempts.WorkerGate, generation: u64) !attempts.FenceAck {
            if (gate.worker_incarnation != self.store.worker_incarnation) return error.InvalidRemoteFence;
            // Reject delayed work before I/O. A failed publication leaves the gate
            // closed. A durable floor still cannot acknowledge live old work.
            try gate.beginFence(generation);
            try self.advance(gate.coordinator, generation);
            return gate.acknowledgeFence(generation);
        }
    };
};

test "remote fence store persists floors and reopens closed before delayed dispatch" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = std.testing.io;
    var store = try Store.create(std.testing.allocator, io, tmp.dir, 11, 2);
    var records: [2]?attempts.WorkerRecord = undefined;
    var gate = try store.restoreGate(1, &records);
    _ = try store.fence(&gate, 1);
    const old = attempts.Id{ .coordinator = 1, .generation = 1, .operation = 71, .sequence = 1 };
    try std.testing.expectEqual(attempts.Admit.start, try gate.admit(old));
    try std.testing.expectError(error.RemoteFencePending, store.fence(&gate, 2));
    try std.testing.expectEqual(@as(u64, 2), try store.floor(1));
    try std.testing.expectError(error.RemoteGenerationFenced, gate.admit(old));
    try std.testing.expect(gate.finish(old));
    _ = try store.fence(&gate, 2);
    store.deinit();
    store = try Store.open(std.testing.allocator, io, tmp.dir, 11, 2);
    defer store.deinit();
    gate = try store.restoreGate(1, &records);
    try std.testing.expectEqual(@as(u64, 2), gate.generation);
    try std.testing.expectError(error.InvalidRemoteGeneration, store.fence(&gate, 2));
    _ = try store.fence(&gate, 3);
    try std.testing.expectError(error.RemoteGenerationFenced, gate.admit(old));
}

test "remote fence store bounds identities and rejects missing corrupt or competing owners" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = std.testing.io;
    try std.testing.expectError(error.RemoteFenceStoreMissing, Store.open(std.testing.allocator, io, tmp.dir, 11, 1));
    var store = try Store.create(std.testing.allocator, io, tmp.dir, 11, 1);
    try std.testing.expectError(error.RemoteFenceStoreBusy, Store.open(std.testing.allocator, io, tmp.dir, 11, 1));
    try store.advance(1, 9);
    try std.testing.expectError(error.RemoteFenceRegistryFull, store.advance(2, 1));
    try std.testing.expectError(error.InvalidRemoteGeneration, store.advance(1, 8));
    try std.testing.expectEqual(@as(u64, 9), try store.floor(1));
    store.deinit();
    try std.testing.expectError(error.InvalidRemoteFenceStore, Store.open(std.testing.allocator, io, tmp.dir, 22, 1));
    const corrupt = try tmp.dir.openFile(io, file_name, .{ .mode = .read_write });
    try corrupt.writePositionalAll(io, "broken", header_size + 16);
    corrupt.close(io);
    try std.testing.expectError(error.InvalidRemoteFenceStore, Store.open(std.testing.allocator, io, tmp.dir, 11, 1));
    try std.testing.expectError(error.RemoteFenceStoreAlreadyExists, Store.create(std.testing.allocator, io, tmp.dir, 11, 1));
}

test "remote fence publication errors never acknowledge and require reopen" {
    const Fault = struct {
        var original: *const fn (?*anyopaque, std.Io.File) std.Io.File.SyncError!void = undefined;
        var calls: usize = 0;
        var fail_at: usize = 0;
        fn sync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
            calls += 1;
            if (calls == fail_at) return error.InputOutput;
            return original(userdata, file);
        }
    };
    const io = std.testing.io;
    for ([_]usize{ 1, 2 }) |fail_at| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var store = try Store.create(std.testing.allocator, io, tmp.dir, 11, 2);
        var records: [2]?attempts.WorkerRecord = undefined;
        var gate = try store.restoreGate(1, &records);
        _ = try store.fence(&gate, 1);
        var vtable = io.vtable.*;
        Fault.original = vtable.fileSync;
        Fault.calls = 0;
        Fault.fail_at = fail_at;
        vtable.fileSync = Fault.sync;
        store.io = .{ .userdata = io.userdata, .vtable = &vtable };
        try std.testing.expectError(error.InputOutput, store.fence(&gate, 2));
        try std.testing.expectError(error.RemoteFenceStoreUncertain, store.floor(1));
        try std.testing.expectError(error.RemoteFenceStoreUncertain, store.fence(&gate, 2));
        const old = attempts.Id{ .coordinator = 1, .generation = 1, .operation = 71, .sequence = 1 };
        try std.testing.expectError(error.RemoteGenerationFenced, gate.admit(old));
        store.deinit();
        // The pre-rename failure leaves the old file; a failed directory sync
        // may have published a newer file. Neither attempt acknowledged it.
        store = try Store.open(std.testing.allocator, io, tmp.dir, 11, 2);
        defer store.deinit();
        try std.testing.expectEqual(@as(u64, if (fail_at == 1) 1 else 2), try store.floor(1));
        gate = try store.restoreGate(1, &records);
        _ = try store.fence(&gate, 3);
        try std.testing.expectError(error.RemoteGenerationFenced, gate.admit(old));
    }
}

test "remote fence store allocation failure releases exclusive ownership" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = std.testing.io;
    var created = try Store.create(std.testing.allocator, io, tmp.dir, 11, 2);
    try created.advance(1, 9);
    created.deinit();
    const Harness = struct {
        fn run(allocator: std.mem.Allocator, dir: std.Io.Dir) !void {
            var store = try Store.open(allocator, std.testing.io, dir, 11, 2);
            defer store.deinit();
            try std.testing.expectEqual(@as(u64, 9), try store.floor(1));
            var records: [2]?attempts.WorkerRecord = undefined;
            var gate = try store.restoreGate(1, &records);
            try std.testing.expectError(error.InvalidRemoteGeneration, store.fence(&gate, 9));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{tmp.dir});
    var reopened = try Store.open(std.testing.allocator, io, tmp.dir, 11, 2);
    defer reopened.deinit();
    try std.testing.expectEqual(@as(u64, 9), try reopened.floor(1));
}

test "remote coordinator restart retains destination barrier and never reuses generation" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var remote_tmp = std.testing.tmpDir(.{});
    defer remote_tmp.cleanup();
    var store = try Store.create(std.testing.allocator, io, tmp.dir, 11, 2);
    var remote = try Store.create(std.testing.allocator, io, remote_tmp.dir, 22, 2);
    defer remote.deinit();
    var slots: [2]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{
        .{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 8192 },
        .{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 8192 },
    };
    var ledger = try store.startCoordinator(7, 0, &slots, &destinations, 16384);
    var local_records: [2]?attempts.WorkerRecord = undefined;
    var remote_records: [2]?attempts.WorkerRecord = undefined;
    var local_gate = try store.restoreGate(7, &local_records);
    var remote_gate = try remote.restoreGate(7, &remote_records);
    try ledger.acknowledgeFence(0, try store.fence(&local_gate, 1));
    try ledger.acknowledgeFence(1, try remote.fence(&remote_gate, 1));
    const old = try ledger.reserve(1, 71, 4096);
    try store.prepareDispatch(&ledger, old);
    try std.testing.expectEqual(attempts.Admit.start, try remote_gate.admit(old));
    ledger.transportLost(old);
    store.deinit();
    store = try Store.open(std.testing.allocator, io, tmp.dir, 11, 2);
    defer store.deinit();
    var history: [2]u128 = undefined;
    try std.testing.expectEqual(@as(usize, 1), try store.destinationHistory(&history));
    try std.testing.expectEqual(@as(u128, 22), history[0]);
    try std.testing.expectError(error.RemoteDestinationHistoryMissing, store.startCoordinator(7, 0, &slots, destinations[0..1], 16384));
    ledger = try store.startCoordinator(7, 0, &slots, &destinations, 16384);
    try std.testing.expectEqual(@as(u64, 2), ledger.generation);
    local_gate = try store.restoreGate(7, &local_records);
    try ledger.acknowledgeFence(0, try store.fence(&local_gate, 2));
    try std.testing.expectError(error.RemoteGenerationNotReady, ledger.reserve(0, 72, 1));
    try std.testing.expectError(error.RemoteFencePending, remote.fence(&remote_gate, 2));
    try std.testing.expectError(error.RemoteGenerationFenced, remote_gate.admit(old));
    try std.testing.expect(remote_gate.finish(old));
    try ledger.acknowledgeFence(1, try remote.fence(&remote_gate, 2));
    const next = try ledger.reserve(1, 72, 4096);
    try std.testing.expectEqual(old.sequence, next.sequence);
    try std.testing.expect(next.generation > old.generation);
    try store.prepareDispatch(&ledger, next);
    try std.testing.expectError(error.RemoteGenerationFenced, remote_gate.admit(old));
    try std.testing.expectEqual(attempts.Admit.start, try remote_gate.admit(next));
    try std.testing.expect(remote_gate.finish(next));
}

test "remote coordinator publication faults cannot expose epoch or dispatched reservation" {
    const Fault = struct {
        var original: *const fn (?*anyopaque, std.Io.File) std.Io.File.SyncError!void = undefined;
        var calls: usize = 0;
        var fail_at: usize = 0;
        fn sync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
            calls += 1;
            if (calls == fail_at) return error.InputOutput;
            return original(userdata, file);
        }
    };
    const io = std.testing.io;
    for ([_]bool{ false, true }) |epoch| for ([_]usize{ 1, 2 }) |fail_at| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var store = try Store.create(std.testing.allocator, io, tmp.dir, 11, 2);
        var slots: [2]?attempts.Attempt = undefined;
        var destinations = [_]attempts.Destination{.{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 8192 }};
        var ledger = try store.startCoordinator(7, 0, &slots, &destinations, 8192);
        try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 1, .worker_incarnation = 22 });
        const id = try ledger.reserve(0, 71, 4096);
        try std.testing.expect(!(try store.initializationState()).publication_started);
        var vtable = io.vtable.*;
        Fault.original = vtable.fileSync;
        Fault.calls = 0;
        Fault.fail_at = fail_at;
        vtable.fileSync = Fault.sync;
        store.io = .{ .userdata = io.userdata, .vtable = &vtable };
        if (epoch) {
            try std.testing.expectError(error.InputOutput, store.startCoordinator(7, 0, &slots, &destinations, 8192));
        } else {
            try std.testing.expectError(error.InputOutput, store.prepareDispatch(&ledger, id));
            try ledger.abortUnpublished(id);
            try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().bytes);
        }
        var history: [2]u128 = undefined;
        try std.testing.expectError(error.RemoteFenceStoreUncertain, store.destinationHistory(&history));
        try std.testing.expectError(error.RemoteFenceStoreUncertain, store.startCoordinator(7, 0, &slots, &destinations, 8192));
        store.deinit();
        store = try Store.open(std.testing.allocator, io, tmp.dir, 11, 2);
        defer store.deinit();
        const count = try store.destinationHistory(&history);
        try std.testing.expectEqual(@as(usize, if (!epoch and fail_at == 2) 1 else 0), count);
        try std.testing.expectEqual(!epoch and fail_at == 2, (try store.initializationState()).publication_started);
        ledger = try store.startCoordinator(7, 0, &slots, &destinations, 8192);
        try std.testing.expectEqual(@as(u64, if (epoch and fail_at == 2) 3 else 2), ledger.generation);
        try std.testing.expectEqual(@as(usize, 1), ledger.snapshot().startup_fences_remaining);
    };
}

test "remote coordinator history and generation capacity fail closed" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try Store.create(std.testing.allocator, std.testing.io, tmp.dir, 11, 1);
    defer store.deinit();
    var slots: [2]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{
        .{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 8192 },
        .{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 8192 },
    };
    var ledger = try store.startCoordinator(7, 0, &slots, &destinations, 16384);
    for (destinations, 0..) |destination, index| try ledger.acknowledgeFence(index, .{ .coordinator = 7, .generation = 1, .worker_incarnation = destination.worker_incarnation });
    const first = try ledger.reserve(0, 71, 4096);
    try store.prepareDispatch(&ledger, first);
    try std.testing.expect((try store.initializationState()).publication_started);
    const second = try ledger.reserve(1, 72, 4096);
    try std.testing.expectError(error.RemoteDestinationHistoryFull, store.prepareDispatch(&ledger, second));
    try ledger.abortUnpublished(second);
    try std.testing.expectEqual(@as(usize, 4096), ledger.snapshot().bytes);
    try std.testing.expectError(error.InvalidRemoteGeneration, store.startCoordinator(8, 0, &slots, &destinations, 16384));
    ledger = try store.startCoordinator(7, std.math.maxInt(u64) - 1, &slots, &destinations, 16384);
    try std.testing.expect((try store.initializationState()).publication_started);
    try std.testing.expectEqual(std.math.maxInt(u64), ledger.generation);
    try std.testing.expectError(error.RemoteGenerationExhausted, store.startCoordinator(7, 0, &slots, &destinations, 16384));
}

test "remote coordinator runtime advancement preserves uncertainty across publication failures" {
    const Fault = struct {
        var original: *const fn (?*anyopaque, std.Io.File) std.Io.File.SyncError!void = undefined;
        var calls: usize = 0;
        var fail_at: usize = 0;
        fn sync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
            calls += 1;
            if (calls == fail_at) return error.InputOutput;
            return original(userdata, file);
        }
    };
    const io = std.testing.io;
    for ([_]usize{ 1, 2 }) |fail_at| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var store = try Store.create(std.testing.allocator, io, tmp.dir, 11, 2);
        var slots: [2]?attempts.Attempt = undefined;
        var destinations = [_]attempts.Destination{.{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 8192 }};
        var ledger = try store.startCoordinator(7, 0, &slots, &destinations, 8192);
        try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 1, .worker_incarnation = 22 });
        const id = try ledger.reserve(0, 71, 4096);
        try store.prepareDispatch(&ledger, id);
        ledger.transportLost(id);
        var vtable = io.vtable.*;
        Fault.original = vtable.fileSync;
        Fault.calls = 0;
        Fault.fail_at = fail_at;
        vtable.fileSync = Fault.sync;
        store.io = .{ .userdata = io.userdata, .vtable = &vtable };
        try std.testing.expectError(error.InputOutput, store.advanceCoordinator(&ledger));
        try std.testing.expectEqual(@as(u64, 1), ledger.generationState().generation);
        try std.testing.expectEqual(@as(usize, 4096), ledger.snapshot().bytes);
        store.deinit();
        store = try Store.open(std.testing.allocator, io, tmp.dir, 11, 2);
        defer store.deinit();
        try store.advanceCoordinator(&ledger);
        try std.testing.expectEqual(@as(u64, if (fail_at == 1) 2 else 3), ledger.generationState().generation);
        try std.testing.expectEqual(@as(usize, 4096), ledger.snapshot().bytes);
        try std.testing.expectError(error.RemoteGenerationNotReady, ledger.reserve(0, 72, 1));
    }
}

test "remote fence scoped I/O loans CPU and discards handles when reentry fails" {
    const Case = enum { success, begin_denied, resume_denied_open, resume_denied_rename, io_failed };
    const Probe = struct {
        var current: *@This() = undefined;
        original: *const std.Io.VTable,
        case: Case,
        running: bool = true,
        begins: usize = 0,
        ends: usize = 0,
        opened: usize = 0,
        closed: usize = 0,
        syncs: usize = 0,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.running);
            self.begins += 1;
            if (self.case == .begin_denied and self.begins == 1) return error.Canceled;
            self.running = false;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.running);
            self.ends += 1;
            if ((self.case == .resume_denied_open and self.ends == 1) or (self.case == .resume_denied_rename and self.ends == 3)) return error.Forbidden;
            self.running = true;
        }
        fn create(userdata: ?*anyopaque, dir: std.Io.Dir, name: []const u8, options: std.Io.Dir.CreateFileOptions) std.Io.File.OpenError!std.Io.File {
            std.debug.assert(!current.running);
            const file = try current.original.dirCreateFile(userdata, dir, name, options);
            current.opened += 1;
            return file;
        }
        fn close(userdata: ?*anyopaque, files: []const std.Io.File) void {
            current.closed += files.len;
            current.original.fileClose(userdata, files);
        }
        fn sync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
            std.debug.assert(!current.running);
            current.syncs += 1;
            if (current.case == .io_failed and current.syncs == 1) return error.InputOutput;
            return current.original.fileSync(userdata, file);
        }
    };
    const io = std.testing.io;
    for (std.enums.values(Case)) |case| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var store = try Store.create(std.testing.allocator, io, tmp.dir, 11, 2);
        var records: [2]?attempts.WorkerRecord = undefined;
        var gate = try store.restoreGate(1, &records);
        _ = try store.fence(&gate, 1);
        var probe: Probe = .{ .original = io.vtable, .case = case };
        Probe.current = &probe;
        var vtable = io.vtable.*;
        vtable.dirCreateFile = Probe.create;
        vtable.fileClose = Probe.close;
        vtable.fileSync = Probe.sync;
        store.io = .{ .userdata = io.userdata, .vtable = &vtable };
        const result = store.withWait(.{ .ptr = &probe, .begin = Probe.begin, .end = Probe.end }).fence(&gate, 2);
        switch (case) {
            .success => {
                const ack = try result;
                try std.testing.expectEqual(@as(u64, 2), ack.generation);
                try std.testing.expect(probe.running);
                try std.testing.expectEqual(@as(usize, 4), probe.ends);
                try std.testing.expectEqual(@as(usize, 2), probe.syncs);
            },
            .begin_denied => try std.testing.expectError(error.Canceled, result),
            .resume_denied_open, .resume_denied_rename => try std.testing.expectError(error.Forbidden, result),
            .io_failed => try std.testing.expectError(error.InputOutput, result),
        }
        try std.testing.expectEqual(probe.opened, probe.closed);
        if (case != .success) {
            try std.testing.expectError(error.RemoteFenceStoreUncertain, store.floor(1));
            try std.testing.expect(!gate.accepting);
        }
        store.io = io;
        store.deinit();
        store = try Store.open(std.testing.allocator, io, tmp.dir, 11, 2);
        defer store.deinit();
        const expected: u64 = if (case == .success or case == .resume_denied_rename) 2 else 1;
        try std.testing.expectEqual(expected, try store.floor(1));
    }
}

test "remote fence contended lock discards acquisition on denied CPU reentry" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try Store.create(std.testing.allocator, io, tmp.dir, 11, 2);
    defer store.deinit();
    try store.mutex.lock(io);
    const Probe = struct {
        store: *Store,
        running: bool = true,
        ends: usize = 0,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.running = false;
            // Controlled interleaving: the previous owner releases only after
            // this invocation has observed contention and parked its CPU.
            self.store.mutex.unlock(self.store.io);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.running);
            self.ends += 1;
            return error.Forbidden;
        }
    };
    var probe: Probe = .{ .store = &store };
    try std.testing.expectError(error.Forbidden, store.withWait(.{ .ptr = &probe, .begin = Probe.begin, .end = Probe.end }).floor(1));
    try std.testing.expectEqual(@as(usize, 1), probe.ends);
    try std.testing.expect(store.mutex.tryLock());
    store.mutex.unlock(io);
    try std.testing.expectEqual(@as(u64, 0), try store.floor(1));
}

test "remote initialization journal resolves authority under lock and never recreates active state" {
    const Probe = struct {
        dir: std.Io.Dir,
        grant: InitializationGrant = .{ .id = @splat(7), .phase = .prepared },
        calls: usize = 0,
        fail: bool = false,
        fn resolve(raw: *anyopaque) !InitializationGrant {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            // Resolution cannot precede acquiring the same local lock used by
            // every opener, even across an unsuccessful bootstrap retry.
            try std.testing.expectError(error.RemoteFenceStoreBusy, Store.open(std.testing.allocator, std.testing.io, self.dir, 11, 2));
            if (self.fail) return error.MetadataMutationOutcomeUnknown;
            return self.grant;
        }
        fn open(self: *@This()) !Store {
            return Store.openAuthorized(std.testing.allocator, std.testing.io, self.dir, 11, 2, .{ .context = self, .resolve = resolve });
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = std.testing.io;
    var probe = Probe{ .dir = tmp.dir, .fail = true };
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, probe.open());
    try std.testing.expectError(error.RemoteFenceStoreMissing, Store.open(std.testing.allocator, io, tmp.dir, 11, 2));
    probe.fail = false;
    probe.grant.id = @splat(0);
    try std.testing.expectError(error.InvalidRemoteAttemptConfig, probe.open());
    probe.grant.id = @splat(7);
    var store = try probe.open();
    try std.testing.expectEqualDeep(probe.grant.id, (try store.initializationState()).id);
    try std.testing.expect(!(try store.initializationState()).publication_started);
    const calls = probe.calls;
    try std.testing.expectError(error.RemoteFenceStoreBusy, probe.open());
    try std.testing.expectEqual(calls, probe.calls);
    // The prepared checkpoint is retryable after crash before activation.
    store.deinit();
    store = try probe.open();
    store.deinit();
    // Resolution observes current authority rather than a previously captured
    // prepared result. Active may reopen only the matching existing checkpoint.
    probe.grant.phase = .active;
    store = try probe.open();
    try store.advance(19, 3);
    try store.markPublicationStarted();
    try store.markPublicationStarted();
    try std.testing.expect((try store.initializationState()).publication_started);
    store.deinit();
    probe.grant.id = @splat(8);
    try std.testing.expectError(error.RemoteInitializationChanged, probe.open());
    probe.grant.id = @splat(7);
    store = try probe.open();
    try std.testing.expectEqual(@as(u64, 3), try store.floor(19));
    try std.testing.expect((try store.initializationState()).publication_started);
    store.deinit();
    probe.grant.phase = .prepared;
    try std.testing.expectError(error.InvalidRemoteFenceStore, probe.open());
    probe.grant.phase = .active;
    try tmp.dir.deleteFile(io, file_name);
    try std.testing.expectError(error.RemoteFenceStoreMissing, probe.open());
    try std.testing.expectError(error.FileNotFound, tmp.dir.statFile(io, file_name, .{}));
}

test "remote initialization journal rejects unbound and legacy checkpoints without replacement" {
    const Grant = struct {
        fn resolve(_: *anyopaque) !InitializationGrant {
            return .{ .id = @splat(7), .phase = .prepared };
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = std.testing.io;
    var context: u8 = 0;
    var store = try Store.create(std.testing.allocator, io, tmp.dir, 11, 2);
    store.deinit();
    try std.testing.expectError(error.RemoteInitializationChanged, Store.openAuthorized(std.testing.allocator, io, tmp.dir, 11, 2, .{ .context = &context, .resolve = Grant.resolve }));
    // Older checkpoints retain the basename and must fail closed. Do not
    // silently migrate them into an unused journal with no publication history.
    const file = try tmp.dir.openFile(io, file_name, .{ .mode = .read_write });
    defer file.close(io);
    try file.writePositionalAll(io, "AFRFEN02", 0);
    try std.testing.expectError(error.InvalidRemoteFenceStore, Store.openAuthorized(std.testing.allocator, io, tmp.dir, 11, 2, .{ .context = &context, .resolve = Grant.resolve }));
    var magic_bytes: [8]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 8), try file.readPositionalAll(io, &magic_bytes, 0));
    try std.testing.expectEqualStrings("AFRFEN02", &magic_bytes);
}

test "remote journal cannot claim unused state while retaining published destinations" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = std.testing.io;
    var store = try Store.create(std.testing.allocator, io, tmp.dir, 11, 2);
    var slots: [1]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{.{ .worker_incarnation = 22, .max_attempts = 1, .max_bytes = 4096 }};
    var ledger = try store.startCoordinator(7, 0, &slots, &destinations, 4096);
    try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 1, .worker_incarnation = 22 });
    const id = try ledger.reserve(0, 71, 4096);
    try store.prepareDispatch(&ledger, id);
    // Deliberately construct an internally inconsistent but correctly hashed
    // checkpoint. The load invariant must protect the unused-state proof.
    store.publication_started = false;
    try store.publish();
    store.deinit();
    try std.testing.expectError(error.InvalidRemoteFenceStore, Store.open(std.testing.allocator, io, tmp.dir, 11, 2));
}

test "remote fence store checkpoint publishes without allocation after initialization" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var backing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var store = try Store.create(backing.allocator(), std.testing.io, tmp.dir, 11, 2);
    var live = true;
    defer if (live) store.deinit();
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    var records: [2]?attempts.WorkerRecord = undefined;
    var gate = try store.restoreGate(1, &records);
    _ = try store.fence(&gate, 1);
    const old = attempts.Id{ .coordinator = 1, .generation = 1, .operation = 71, .sequence = 1 };
    try std.testing.expectEqual(attempts.Admit.start, try gate.admit(old));
    // Fencing persists before acknowledging physical retirement. Both the
    // pending and final checkpoint use the buffer already owned at bootstrap.
    try std.testing.expectError(error.RemoteFencePending, store.fence(&gate, 2));
    try std.testing.expectEqual(@as(u64, 2), try store.floor(1));
    try std.testing.expectError(error.RemoteGenerationFenced, gate.admit(old));
    try std.testing.expect(gate.finish(old));
    _ = try store.fence(&gate, 2);
    try std.testing.expect(!backing.has_induced_failure);
    store.deinit();
    live = false;
    try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
    var reopened = try Store.open(std.testing.allocator, std.testing.io, tmp.dir, 11, 2);
    defer reopened.deinit();
    gate = try reopened.restoreGate(1, &records);
    try std.testing.expectEqual(@as(u64, 2), gate.generation);
    try std.testing.expectError(error.InvalidRemoteGeneration, reopened.fence(&gate, 2));
}
