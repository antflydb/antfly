// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native SQL relation lifetimes. Catalog definitions may be durable while
//! primary rows, indexes and participant intents deliberately are not logged.
//! These are owner capabilities, never client-selected durability hints.
const std = @import("std");

pub const Kind = enum { permanent, temporary, unlogged };
pub const OnCommit = enum { preserve_rows, delete_rows, drop };
pub const Export = enum { logical_backup, physical_replication, logical_publication };

pub const Owner = struct {
    issuer_node_id: u64 = 0,
    connection_id: [32]u8,
    wire_epoch: [16]u8,
    execution_instance: [16]u8,
    connection_generation: u64,
    principal_digest: [32]u8,

    pub fn validate(self: Owner) !void {
        if (std.mem.allEqual(u8, &self.connection_id, 0) or
            std.mem.allEqual(u8, &self.wire_epoch, 0) or
            std.mem.allEqual(u8, &self.execution_instance, 0) or
            std.mem.allEqual(u8, &self.principal_digest, 0) or
            self.connection_generation == 0) return error.InvalidSqlRelationPersistence;
    }

    pub fn eql(self: Owner, other: Owner) bool {
        return std.meta.eql(self, other);
    }

    /// A session schema is independent of user identifiers and search_path.
    /// Include the connection incarnation so recycled tokens cannot adopt it.
    pub fn namespace(self: Owner, buffer: *[72]u8) ![]const u8 {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-temporary-namespace/v1\x00");
        hash.update(&self.connection_id);
        hash.update(&self.wire_epoch);
        hash.update(&self.execution_instance);
        hash.update(&self.principal_digest);
        var generation: [8]u8 = undefined;
        std.mem.writeInt(u64, &generation, self.connection_generation, .little);
        hash.update(&generation);
        std.mem.writeInt(u64, &generation, self.issuer_node_id, .little);
        hash.update(&generation);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return std.fmt.bufPrint(buffer, "pg_temp_{s}", .{std.fmt.bytesToHex(digest[0..16].*, .lower)});
    }
};

pub const Descriptor = struct {
    version: u8 = 1,
    kind: Kind,
    owner_node_id: u64,
    owner_store_id: u64,
    /// Immutable process identity at the birth of this data generation.
    generation_instance: [16]u8,
    /// Incremented before crash/failover recovery can serve an empty generation.
    /// Every participant/read proof binds it, fencing delayed transaction apply.
    data_generation: u64,
    owner: ?Owner = null,
    on_commit: OnCommit = .preserve_rows,

    pub fn validate(self: Descriptor) !void {
        if (self.version != 1 or self.kind == .permanent or self.owner_node_id == 0 or self.owner_store_id == 0 or
            std.mem.allEqual(u8, &self.generation_instance, 0) or self.data_generation == 0)
            return error.InvalidSqlRelationPersistence;
        if (self.kind == .temporary) {
            const owner = self.owner orelse return error.InvalidSqlRelationPersistence;
            try owner.validate();
            // The SQL connection issuer and native data owner may be
            // different cluster processes. Keep both exact epochs.
        } else if (self.owner != null or self.on_commit != .preserve_rows)
            return error.InvalidSqlRelationPersistence;
    }

    pub fn eql(self: Descriptor, other: Descriptor) bool {
        return std.meta.eql(self, other);
    }

    pub fn fingerprint(self: Descriptor) ![32]u8 {
        try self.validate();
        return self.fingerprintUnchecked();
    }

    /// Definition/CAS hashing also handles untrusted ingress before its
    /// separate admission validation. No process identity is omitted.
    pub fn fingerprintUnchecked(self: Descriptor) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-relation-persistence/v1\x00");
        hash.update(&.{self.version});
        hash.update("kind\x00");
        hash.update(@tagName(self.kind));
        hash.update("on-commit\x00");
        hash.update(@tagName(self.on_commit));
        hash.update("native-host\x00");
        hash.update(&self.generation_instance);
        var number: [8]u8 = undefined;
        inline for (.{ self.owner_node_id, self.owner_store_id, self.data_generation }) |value| {
            std.mem.writeInt(u64, &number, value, .little);
            hash.update(&number);
        }
        hash.update("connection-owner\x00");
        hash.update(&.{@intFromBool(self.owner != null)});
        if (self.owner) |owner| {
            inline for (.{ owner.issuer_node_id, owner.connection_generation }) |value| {
                std.mem.writeInt(u64, &number, value, .little);
                hash.update(&number);
            }
            hash.update(&owner.connection_id);
            hash.update(&owner.wire_epoch);
            hash.update(&owner.execution_instance);
            hash.update(&owner.principal_digest);
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    pub fn requireHost(self: Descriptor, context: Context) !void {
        try self.validate();
        if (std.mem.allEqual(u8, &context.execution_instance, 0) or context.standby or context.node_id != self.owner_node_id or context.store_id != self.owner_store_id or
            !std.mem.eql(u8, &context.generation_instance, &self.generation_instance) or
            context.data_generation != self.data_generation or
            (self.kind == .temporary and !std.mem.eql(u8, &context.execution_instance, &self.generation_instance)))
            return error.SqlRelationOwnerChanged;
    }

    pub fn requireOwner(self: Descriptor, context: Context) !void {
        try self.requireHost(context);
        try self.requireConnectionOwner(context.connection);
    }

    /// API admission checks the authenticated connection independently of the
    /// native host proof, which the data owner validates at execution time.
    pub fn requireConnectionOwner(self: Descriptor, connection: ?Owner) !void {
        try self.validate();
        if (self.owner) |owner| if (connection == null or !owner.eql(connection.?))
            return error.SqlTemporaryRelationNotOwned;
    }

    /// Neither relation's row payload may enter Raft, standby WAL or a durable
    /// SQL-session replay request. Metadata/decision digests remain durable.
    pub fn rowWalEnabled(_: Descriptor) bool {
        return false;
    }

    pub fn exportsDefinition(self: Descriptor, purpose: Export) bool {
        return self.kind == .unlogged and purpose != .logical_publication;
    }

    /// PostgreSQL logical dumps can include primary UNLOGGED data; physical
    /// standby seeds contain only its empty initialization definition.
    pub fn exportsRows(self: Descriptor, purpose: Export) bool {
        return self.kind == .unlogged and purpose == .logical_backup;
    }

    pub fn afterCommit(self: Descriptor) OnCommit {
        return if (self.kind == .temporary) self.on_commit else .preserve_rows;
    }
};

/// PostgreSQL persistence classes constrain the actual referenced relation,
/// independently of SQL parsing or transport-supplied declaration strings.
pub fn requireForeignKeyClasses(child: ?Descriptor, parent: ?Descriptor) !void {
    if (child) |value| try value.validate();
    if (parent) |value| try value.validate();
    const child_kind: Kind = if (child) |value| value.kind else .permanent;
    const parent_kind: Kind = if (parent) |value| value.kind else .permanent;
    if (child_kind == .temporary or parent_kind == .temporary) {
        if (child_kind != .temporary or parent_kind != .temporary or
            !child.?.owner.?.eql(parent.?.owner.?) or child.?.owner_node_id != parent.?.owner_node_id or
            child.?.owner_store_id != parent.?.owner_store_id or
            !std.mem.eql(u8, &child.?.generation_instance, &parent.?.generation_instance))
            return error.SqlRelationPersistenceForeignKeyMismatch;
    } else if (child_kind == .permanent and parent_kind == .unlogged)
        return error.SqlRelationPersistenceForeignKeyMismatch;
}

pub const Context = struct {
    node_id: u64,
    store_id: u64,
    execution_instance: [16]u8,
    generation_instance: [16]u8,
    data_generation: u64,
    connection: ?Owner = null,
    standby: bool = false,
};

/// Native observation read from one real primary catalog under its apply
/// fence. Metadata declarations alone cannot manufacture this proof.
pub const NativeBinding = struct {
    descriptor: Descriptor,
    native_instance: [16]u8,
    table_id: u64,
    shard_id: u64,
    range_id: u64,
    physical_root_incarnation: u128,
    schema_version: u32,
    schema_digest: [32]u8,
    catalog_generation: u64,
    catalog_fingerprint: [32]u8,
    has_user_data: bool = false,
    has_transaction_state: bool = false,

    pub fn validate(self: NativeBinding) !void {
        try self.descriptor.validate();
        if (std.mem.allEqual(u8, &self.native_instance, 0) or self.table_id == 0 or self.shard_id == 0 or self.range_id == 0 or
            self.physical_root_incarnation == 0 or std.mem.allEqual(u8, &self.catalog_fingerprint, 0))
            return error.InvalidSqlRelationPersistence;
    }
    pub fn digest(self: NativeBinding) ![32]u8 {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-relation-native-binding/v1\x00");
        hash.update(&try self.descriptor.fingerprint());
        hash.update(&self.native_instance);
        var number: [8]u8 = undefined;
        for ([_]u64{ self.table_id, self.shard_id, self.range_id, self.schema_version, self.catalog_generation }) |value| {
            std.mem.writeInt(u64, &number, value, .little);
            hash.update(&number);
        }
        var root: [16]u8 = undefined;
        std.mem.writeInt(u128, &root, self.physical_root_incarnation, .little);
        hash.update(&root);
        hash.update(&self.schema_digest);
        hash.update(&self.catalog_fingerprint);
        hash.update(&.{ @intFromBool(self.has_user_data), @intFromBool(self.has_transaction_state) });
        var out: [32]u8 = undefined;
        hash.final(&out);
        return out;
    }
};

/// Immutable native participant address. Process epochs change on clean
/// restart; an existing decision may follow only this exact durable image.
pub const PhysicalPin = struct {
    physical_root_incarnation: u128,
    table_id: u64,
    shard_id: u64,
    range_id: u64,
    data_generation: u64,
    pub const encoded_len = 48;

    pub fn validate(self: PhysicalPin) !void {
        if (self.physical_root_incarnation == 0 or self.table_id == 0 or self.shard_id == 0 or self.range_id == 0 or self.data_generation == 0)
            return error.InvalidSqlRelationPersistence;
    }
    pub fn eql(self: PhysicalPin, other: PhysicalPin) bool {
        return self.physical_root_incarnation == other.physical_root_incarnation and self.table_id == other.table_id and
            self.shard_id == other.shard_id and self.range_id == other.range_id and self.data_generation == other.data_generation;
    }
    pub fn fromBinding(binding: NativeBinding) !PhysicalPin {
        try binding.validate();
        return .{ .physical_root_incarnation = binding.physical_root_incarnation, .table_id = binding.table_id, .shard_id = binding.shard_id, .range_id = binding.range_id, .data_generation = binding.descriptor.data_generation };
    }
    pub fn encode(self: PhysicalPin) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = undefined;
        std.mem.writeInt(u128, bytes[0..16], self.physical_root_incarnation, .little);
        std.mem.writeInt(u64, bytes[16..24], self.table_id, .little);
        std.mem.writeInt(u64, bytes[24..32], self.shard_id, .little);
        std.mem.writeInt(u64, bytes[32..40], self.range_id, .little);
        std.mem.writeInt(u64, bytes[40..48], self.data_generation, .little);
        return bytes;
    }
    pub fn decode(bytes: []const u8) !PhysicalPin {
        if (bytes.len != encoded_len) return error.InvalidSqlRelationPersistence;
        const value: PhysicalPin = .{ .physical_root_incarnation = std.mem.readInt(u128, bytes[0..16], .little), .table_id = std.mem.readInt(u64, bytes[16..24], .little), .shard_id = std.mem.readInt(u64, bytes[24..32], .little), .range_id = std.mem.readInt(u64, bytes[32..40], .little), .data_generation = std.mem.readInt(u64, bytes[40..48], .little) };
        try value.validate();
        return value;
    }
};

test "SQL relation lifetime immutable participant pins survive full width binary typed and Value transports" {
    const a = std.testing.allocator;
    const binding: NativeBinding = .{
        .native_instance = @splat(1),
        .descriptor = .{ .kind = .unlogged, .owner_node_id = std.math.maxInt(u64) - 1, .owner_store_id = std.math.maxInt(u64) - 2, .generation_instance = @splat(1), .data_generation = std.math.maxInt(u64) - 3 },
        .table_id = std.math.maxInt(u64) - 4,
        .shard_id = std.math.maxInt(u64) - 5,
        .range_id = std.math.maxInt(u64) - 6,
        .physical_root_incarnation = std.math.maxInt(u128) - 17,
        .schema_version = 1,
        .schema_digest = @splat(2),
        .catalog_generation = std.math.maxInt(u64) - 7,
        .catalog_fingerprint = @splat(3),
    };
    const pin = try PhysicalPin.fromBinding(binding);
    const binary = try pin.encode();
    try std.testing.expect(pin.eql(try PhysicalPin.decode(&binary)));
    try std.testing.expectError(error.InvalidSqlRelationPersistence, PhysicalPin.decode(binary[0..47]));
    const wire = try std.json.Stringify.valueAlloc(a, pin, .{});
    defer a.free(wire);
    var value = try std.json.parseFromSlice(std.json.Value, a, wire, .{});
    defer value.deinit();
    try std.testing.expect(value.value.object.get("physical_root_incarnation").? == .number_string);
    var from_value = try std.json.parseFromValue(PhysicalPin, a, value.value, .{});
    defer from_value.deinit();
    try std.testing.expect(pin.eql(from_value.value));
    const binding_wire = try std.json.Stringify.valueAlloc(a, binding, .{});
    defer a.free(binding_wire);
    var binding_value = try std.json.parseFromSlice(std.json.Value, a, binding_wire, .{});
    defer binding_value.deinit();
    var parsed_binding = try std.json.parseFromValue(NativeBinding, a, binding_value.value, .{});
    defer parsed_binding.deinit();
    try std.testing.expectEqual(try binding.digest(), try parsed_binding.value.digest());
    var changed = pin;
    changed.data_generation -= 1;
    try std.testing.expect(!pin.eql(changed));
    changed = pin;
    changed.physical_root_incarnation = 0;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, changed.validate());
}

/// Private control-only reopening of a previously clean UNLOGGED image.
/// The metadata receiver independently authenticates the recovering Boot and
/// old record before accepting its signed native postimage. It never grants
/// ordinary reads/writes while recovery is incomplete.
pub const CleanAdoption = struct {
    before: Descriptor,
    metadata_group_id: u64,
    boot_generation: u64,
    table_id: u64,
    shard_id: u64,
    range_id: u64,
    physical_root_incarnation: u128,

    pub fn validateFor(self: CleanAdoption, next: Descriptor, boot: Boot) !void {
        try self.before.validate();
        try next.validate();
        if (boot.unclean_restart or self.before.kind != .unlogged or next.kind != .unlogged or
            self.metadata_group_id == 0 or self.boot_generation == 0 or self.table_id == 0 or self.shard_id == 0 or self.range_id == 0 or self.physical_root_incarnation == 0 or
            self.before.owner_node_id != next.owner_node_id or self.before.owner_store_id != next.owner_store_id or
            self.before.data_generation != next.data_generation or
            !self.before.eql(next) or
            std.mem.eql(u8, &self.before.generation_instance, &boot.instance))
            return error.InvalidSqlRelationPersistence;
    }
};

/// First image of a metadata-owned generation which has never admitted a
/// native row/participant ACK. It contains no invented previous root or pin.
/// The server derives this from the actual uninitialized catalog/Boot cut;
/// initialized publication independently rechecks that cut before its CAS.
pub const UninitializedAdmission = struct {
    metadata_group_id: u64,
    boot_generation: u64,
    table_id: u64,
    shard_id: u64,
    range_id: u64,
    descriptor_fingerprint: [32]u8,
    schema_digest: [32]u8,
    pub fn validateFor(self: UninitializedAdmission, descriptor: Descriptor) !void {
        try descriptor.validate();
        if (descriptor.kind != .unlogged or self.metadata_group_id == 0 or self.boot_generation == 0 or
            self.table_id == 0 or self.shard_id == 0 or self.range_id == 0 or
            std.mem.allEqual(u8, &self.schema_digest, 0) or
            !std.mem.eql(u8, &self.descriptor_fingerprint, &try descriptor.fingerprint())) return error.InvalidSqlRelationPersistence;
    }
    pub fn requireBinding(self: UninitializedAdmission, binding: NativeBinding) !void {
        try binding.validate();
        try self.validateFor(binding.descriptor);
        if (binding.has_user_data or binding.has_transaction_state or binding.table_id != self.table_id or
            binding.shard_id != self.shard_id or binding.range_id != self.range_id or
            !std.mem.eql(u8, &binding.schema_digest, &self.schema_digest)) return error.SqlRelationOwnerChanged;
    }
};

pub const Boot = struct {
    instance: [16]u8,
    /// Derived from the server's durable shutdown record, never from whether
    /// one evicted table happened to close cleanly before the server crashed.
    unclean_restart: bool,
    adoption: ?CleanAdoption = null,
    initialized: ?NativeBinding = null,
    initializing: bool = false,
    uninitialized: ?UninitializedAdmission = null,
    pub fn validate(self: Boot) !void {
        if (std.mem.allEqual(u8, &self.instance, 0) or (self.adoption != null and (self.unclean_restart or self.initialized != null or self.initializing)) or
            (self.initializing and self.initialized != null)) return error.InvalidSqlRelationPersistence;
        if (self.uninitialized != null and (!self.initializing or self.adoption != null or self.initialized != null)) return error.InvalidSqlRelationPersistence;
        if (self.initialized) |binding| {
            try binding.validate();
            if (!std.mem.eql(u8, &binding.native_instance, &self.instance)) return error.SqlRelationOwnerChanged;
        }
    }
    pub fn jsonStringify(self: Boot, writer: anytype) !void {
        if (self.uninitialized) |permit| return writer.write(.{ .instance = self.instance, .unclean_restart = self.unclean_restart, .initializing = true, .uninitialized = permit });
        if (self.initializing) return writer.write(.{ .instance = self.instance, .unclean_restart = self.unclean_restart, .initializing = true });
        if (self.adoption) |adoption| return writer.write(.{ .instance = self.instance, .unclean_restart = self.unclean_restart, .adoption = adoption });
        if (self.initialized) |initialized| return writer.write(.{ .instance = self.instance, .unclean_restart = self.unclean_restart, .initialized = initialized });
        return writer.write(.{ .instance = self.instance, .unclean_restart = self.unclean_restart });
    }
};

test "SQL relation lifetime connection authority is independent of its native host and refuses recycled owners" {
    const owner: Owner = .{ .issuer_node_id = 7, .connection_id = @splat(1), .wire_epoch = @splat(2), .execution_instance = @splat(3), .connection_generation = 1, .principal_digest = @splat(4) };
    const descriptor: Descriptor = .{ .kind = .temporary, .owner_node_id = 23, .owner_store_id = 29, .generation_instance = @splat(8), .data_generation = 11, .owner = owner };
    try descriptor.requireConnectionOwner(owner);
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, descriptor.requireConnectionOwner(null));
    inline for (.{ "issuer_node_id", "connection_generation" }) |field| {
        var changed = owner;
        @field(changed, field) += 1;
        try std.testing.expectError(error.SqlTemporaryRelationNotOwned, descriptor.requireConnectionOwner(changed));
    }
    inline for (.{ "connection_id", "wire_epoch", "execution_instance", "principal_digest" }) |field| {
        var changed = owner;
        @field(changed, field)[0] ^= 0xff;
        try std.testing.expectError(error.SqlTemporaryRelationNotOwned, descriptor.requireConnectionOwner(changed));
    }
    var host: Context = .{ .node_id = 23, .store_id = 29, .execution_instance = @splat(8), .generation_instance = @splat(8), .data_generation = 11, .connection = owner };
    try descriptor.requireOwner(host);
    host.data_generation += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, descriptor.requireOwner(host));
    var unlogged = descriptor;
    unlogged.kind = .unlogged;
    unlogged.owner = null;
    try unlogged.requireConnectionOwner(null);
}

test "SQL relation persistence owner isolates temporary schemas and fences recycled sessions" {
    const owner: Owner = .{ .connection_id = @splat(1), .wire_epoch = @splat(2), .execution_instance = @splat(3), .connection_generation = 1, .principal_digest = @splat(4) };
    const descriptor: Descriptor = .{ .kind = .temporary, .owner_node_id = 7, .owner_store_id = 11, .generation_instance = owner.execution_instance, .data_generation = 1, .owner = owner };
    var context: Context = .{ .node_id = 7, .store_id = 11, .execution_instance = owner.execution_instance, .generation_instance = owner.execution_instance, .data_generation = 1, .connection = owner };
    try descriptor.requireOwner(context);
    var first: [72]u8 = undefined;
    var second: [72]u8 = undefined;
    var recycled = owner;
    recycled.connection_generation += 1;
    try std.testing.expect(!std.mem.eql(u8, try owner.namespace(&first), try recycled.namespace(&second)));
    context.connection = recycled;
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, descriptor.requireOwner(context));
    context.connection = owner;
    context.data_generation += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, descriptor.requireOwner(context));
    context.data_generation = 1;
    context.standby = true;
    try std.testing.expectError(error.SqlRelationOwnerChanged, descriptor.requireOwner(context));
}

test "SQL relation persistence excludes session rows from publication and keeps logical unlogged dumps distinct from standby seeds" {
    const unlogged: Descriptor = .{ .kind = .unlogged, .owner_node_id = 7, .owner_store_id = 11, .generation_instance = @splat(3), .data_generation = 1 };
    try unlogged.validate();
    try std.testing.expect(!unlogged.rowWalEnabled());
    try std.testing.expect(unlogged.exportsDefinition(.physical_replication));
    try std.testing.expect(!unlogged.exportsRows(.physical_replication));
    try std.testing.expect(unlogged.exportsRows(.logical_backup));
    try std.testing.expect(!unlogged.exportsDefinition(.logical_publication));
    var invalid = unlogged;
    invalid.on_commit = .delete_rows;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, invalid.validate());
    invalid = unlogged;
    invalid.kind = .temporary;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, invalid.validate());
    var recovered = unlogged;
    recovered.data_generation += 1;
    try std.testing.expect(!std.mem.eql(u8, &try unlogged.fingerprint(), &try recovered.fingerprint()));
}

test "SQL relation lifetime native foreign keys enforce PostgreSQL class matrix and exact TEMP host owner" {
    const owner: Owner = .{ .issuer_node_id = 1, .connection_id = @splat(1), .wire_epoch = @splat(2), .execution_instance = @splat(3), .connection_generation = 1, .principal_digest = @splat(4) };
    const temporary: Descriptor = .{ .kind = .temporary, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(5), .data_generation = 1, .owner = owner };
    const unlogged: Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(5), .data_generation = 1 };
    const classes = [_]?Descriptor{ null, temporary, unlogged };
    const permitted = [3][3]bool{ .{ true, false, false }, .{ false, true, false }, .{ true, false, true } };
    for (classes, 0..) |child, c| for (classes, 0..) |parent, p| {
        if (permitted[c][p]) try requireForeignKeyClasses(child, parent) else try std.testing.expectError(error.SqlRelationPersistenceForeignKeyMismatch, requireForeignKeyClasses(child, parent));
    };
    var other = temporary;
    other.owner.?.connection_generation += 1;
    try std.testing.expectError(error.SqlRelationPersistenceForeignKeyMismatch, requireForeignKeyClasses(temporary, other));
    other = temporary;
    other.generation_instance[0] ^= 1;
    try std.testing.expectError(error.SqlRelationPersistenceForeignKeyMismatch, requireForeignKeyClasses(temporary, other));
    other = temporary;
    other.owner_store_id += 1;
    try std.testing.expectError(error.SqlRelationPersistenceForeignKeyMismatch, requireForeignKeyClasses(temporary, other));
    other = temporary;
    other.data_generation += 1;
    try requireForeignKeyClasses(temporary, other);
}

test "SQL relation lifetime clean process adoption preserves immutable generation and excludes temporary generations" {
    const before: Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 7 };
    const host: Context = .{ .node_id = 9, .store_id = 11, .execution_instance = @splat(2), .generation_instance = before.generation_instance, .data_generation = before.data_generation };
    const guard: CleanAdoption = .{ .before = before, .metadata_group_id = 1, .boot_generation = 2, .table_id = 3, .shard_id = 4, .range_id = 5, .physical_root_incarnation = 6 };
    const boot: Boot = .{ .instance = host.execution_instance, .unclean_restart = false, .adoption = guard };
    try before.requireHost(host);
    try guard.validateFor(before, boot);
    var changed = before;
    changed.generation_instance = host.execution_instance;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, guard.validateFor(changed, boot));
    changed = before;
    changed.data_generation += 1;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, guard.validateFor(changed, boot));
    var crash = boot;
    crash.unclean_restart = true;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, guard.validateFor(before, crash));
    var temporary = before;
    temporary.kind = .temporary;
    temporary.owner = .{ .connection_id = @splat(1), .wire_epoch = @splat(2), .execution_instance = @splat(3), .connection_generation = 1, .principal_digest = @splat(4) };
    try std.testing.expectError(error.SqlRelationOwnerChanged, temporary.requireHost(host));
}

test "SQL relation lifetime never acknowledged admission proves only an actual empty namespace" {
    const descriptor: Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 7 };
    const permit: UninitializedAdmission = .{ .metadata_group_id = 1, .boot_generation = 4, .table_id = 3, .shard_id = 4, .range_id = 5, .descriptor_fingerprint = try descriptor.fingerprint(), .schema_digest = @splat(8) };
    const binding: NativeBinding = .{ .descriptor = descriptor, .native_instance = @splat(2), .table_id = 3, .shard_id = 4, .range_id = 5, .physical_root_incarnation = 6, .schema_version = 1, .schema_digest = @splat(8), .catalog_fingerprint = @splat(9), .catalog_generation = 1, .has_user_data = false, .has_transaction_state = false };
    try permit.requireBinding(binding);
    var wrong = binding;
    wrong.has_user_data = true;
    try std.testing.expectError(error.SqlRelationOwnerChanged, permit.requireBinding(wrong));
    wrong = binding;
    wrong.has_transaction_state = true;
    try std.testing.expectError(error.SqlRelationOwnerChanged, permit.requireBinding(wrong));
    wrong = binding;
    wrong.range_id += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, permit.requireBinding(wrong));
    wrong = binding;
    wrong.schema_digest[0] ^= 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, permit.requireBinding(wrong));
    wrong = binding;
    wrong.descriptor.data_generation += 1;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, permit.requireBinding(wrong));
    const boot: Boot = .{ .instance = binding.native_instance, .unclean_restart = true, .initializing = true, .uninitialized = permit };
    try boot.validate();
    var invalid = boot;
    invalid.initializing = false;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, invalid.validate());
}

/// Cold physical metadata used only under an irreversible unpublished-target
/// retirement. Null facts describe an incomplete first open, never a fabricated
/// NativeBinding or a statement that acknowledged row data was empty.
pub const UnpublishedImageInspection = struct {
    root_present: bool,
    primary_present: bool,
    catalog_present: bool = false,
    descriptor: ?Descriptor = null,
    namespace: ?struct { table_id: u64, shard_id: u64, range_id: u64 } = null,
    has_user_data: ?bool = null,
    has_transaction_state: ?bool = null,
    bootstrap_scope: ?[32]u8 = null,
    physical_root_incarnation: ?u128 = null,
    image_attempt: ?struct { number: u64, digest: [32]u8 } = null,
};
