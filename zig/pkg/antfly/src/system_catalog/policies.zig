// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Catalog-owned, schema-bound row-policy definitions. These records are not
//! permission to enable RLS: owners must first implement pre-page filtering and
//! guarded old-image/new-image checks on every read and mutation route.
const std = @import("std");
const ast = @import("../sql/ast.zig");
const scalar = @import("../sql/scalar.zig");
const sql_catalog = @import("../sql/catalog.zig");
const setting_catalog = @import("../sql/setting_catalog.zig");

pub const CommandScope = packed struct(u4) {
    select: bool = false,
    insert: bool = false,
    update: bool = false,
    delete: bool = false,

    pub fn any(self: @This()) bool {
        return self.select or self.insert or self.update or self.delete;
    }
};

/// Persist the binder's typed instruction DAG, never SQL text interpreted by
/// a data owner. The publisher binds against the exact schema and setting view;
/// the recipient still checks bounds, dependencies and boolean result type.
pub const Predicate = struct {
    instructions: []const scalar.Instruction,
    root: u32,

    pub fn validate(self: @This(), columns: []const sql_catalog.Column, settings: *const setting_catalog.View) !void {
        if (self.instructions.len == 0 or self.instructions.len > 8192 or self.root >= self.instructions.len) return error.InvalidRowPolicyProgram;
        if (self.instructions[self.root].type.kind != ast.ColumnType.boolean) return error.InvalidRowPolicyProgram;
        for (self.instructions, 0..) |instruction, i| {
            switch (instruction.operation) {
                .literal => |value| {
                    const actual: ?ast.ColumnType = switch (value) {
                        .null => null,
                        .bool => .boolean,
                        .integer => .integer,
                        .float, .number_string => .number,
                        .string => .string,
                        .array, .object => .json,
                    };
                    if (actual != null and instruction.type.kind != actual) return error.InvalidRowPolicyProgram;
                },
                .column => |ordinal| {
                    if (ordinal >= columns.len or instruction.type.kind != columns[ordinal].type) return error.InvalidRowPolicyProgram;
                },
                .parameter => return error.InvalidRowPolicyProgram,
                .unary => |node| {
                    try predecessor(node.operand, i);
                    const operand = self.instructions[node.operand].type.kind;
                    switch (node.op) {
                        .not, .is_true, .is_not_true, .is_false, .is_not_false => if (instruction.type.kind != .boolean or operand != .boolean) return error.InvalidRowPolicyProgram,
                        .is_null, .is_not_null => if (instruction.type.kind != .boolean) return error.InvalidRowPolicyProgram,
                        .positive, .negative => if (instruction.type.kind == null or operand != instruction.type.kind or (operand != .integer and operand != .number)) return error.InvalidRowPolicyProgram,
                    }
                },
                .binary => |node| {
                    try predecessor(node.left, i);
                    try predecessor(node.right, i);
                    const left = self.instructions[node.left].type.kind;
                    const right = self.instructions[node.right].type.kind;
                    switch (node.op) {
                        .@"and", .@"or" => if (instruction.type.kind != .boolean or left != .boolean or right != .boolean) return error.InvalidRowPolicyProgram,
                        .eq, .neq, .lt, .lte, .gt, .gte, .is_distinct, .is_not_distinct, .like, .ilike => if (instruction.type.kind != .boolean) return error.InvalidRowPolicyProgram,
                        .add, .subtract, .multiply, .divide, .modulo => if (instruction.type.kind != .integer and instruction.type.kind != .number) return error.InvalidRowPolicyProgram,
                        .concat, .json_text => if (instruction.type.kind != .string) return error.InvalidRowPolicyProgram,
                        .json_get => if (instruction.type.kind != .json) return error.InvalidRowPolicyProgram,
                    }
                },
                .call => |node| {
                    if (node.args.len > 64 or node.function == .@"$single" or node.function == .@"$pattern_quantified") return error.InvalidRowPolicyProgram;
                    for (node.args) |arg| try predecessor(arg, i);
                    if (node.function == .current_setting) {
                        if (node.args.len != 0 or instruction.type.kind != .string) return error.InvalidRowPolicyProgram;
                        const identity = node.setting_identity orelse return error.InvalidRowPolicyProgram;
                        const definition = for (settings.definitions) |candidate| {
                            if (candidate.identity.id == identity.id) break candidate;
                        } else return error.SettingCatalogChanged;
                        if (definition.identity.generation != identity.generation or !definition.policy_sensitive or definition.session_writable) return error.SettingCatalogChanged;
                    } else {
                        if (node.setting_identity != null) return error.InvalidRowPolicyProgram;
                        const count = node.args.len;
                        const valid = switch (node.function) {
                            .abs, .lower, .upper, .length, .octet_length, .ceil, .floor, .round, .sqrt, .to_timestamp => count == 1,
                            .nullif, .power, .mod, .starts_with, .date_part, .date_trunc => count == 2,
                            .substring => count == 2 or count == 3,
                            .replace => count == 3,
                            .trim, .ltrim, .rtrim => count == 1 or count == 2,
                            .coalesce, .greatest, .least => count > 0,
                            .concat => true,
                            else => false,
                        };
                        if (!valid) return error.InvalidRowPolicyProgram;
                    }
                },
                .cast => |node| {
                    try predecessor(node.operand, i);
                    if (instruction.type.kind != node.type) return error.InvalidRowPolicyProgram;
                },
                .case_when => |node| {
                    if (node.branches.len > 256) return error.InvalidRowPolicyProgram;
                    for (node.branches) |branch| {
                        try predecessor(branch.condition, i);
                        try predecessor(branch.value, i);
                        if (self.instructions[branch.condition].type.kind != .boolean or self.instructions[branch.value].type.kind != instruction.type.kind) return error.InvalidRowPolicyProgram;
                    }
                    if (node.otherwise) |otherwise| {
                        try predecessor(otherwise, i);
                        if (self.instructions[otherwise].type.kind != instruction.type.kind) return error.InvalidRowPolicyProgram;
                    }
                },
                .in_list => |node| {
                    if (node.values.len > 1024) return error.InvalidRowPolicyProgram;
                    if (instruction.type.kind != .boolean) return error.InvalidRowPolicyProgram;
                    try predecessor(node.operand, i);
                    for (node.values) |value| try predecessor(value, i);
                },
            }
        }
    }
};

fn predecessor(node: u32, index: usize) !void {
    if (node >= index) return error.InvalidRowPolicyProgram;
}

pub const Record = struct {
    id: u64,
    generation: u64,
    table_id: u64,
    schema_version: u32,
    schema_digest: [32]u8,
    name: []const u8,
    commands: CommandScope,
    roles: []const []const u8,
    permissive: bool = true,
    using: ?Predicate = null,
    with_check: ?Predicate = null,

    pub fn validateShape(self: @This()) !void {
        if (self.id == 0 or self.generation == 0 or self.table_id == 0 or self.schema_version == 0 or
            self.name.len == 0 or self.name.len > 128 or !self.commands.any() or self.roles.len == 0 or self.roles.len > 256)
            return error.InvalidRowPolicyRecord;
        for (self.name) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_') return error.InvalidRowPolicyRecord;
        for (self.roles, 0..) |role, i| {
            if (role.len == 0 or role.len > 128 or !std.unicode.utf8ValidateSlice(role)) return error.InvalidRowPolicyRecord;
            for (self.roles[0..i]) |prior| if (std.mem.eql(u8, prior, role)) return error.InvalidRowPolicyRecord;
        }
        if ((self.commands.select or self.commands.update or self.commands.delete) and self.using == null) return error.InvalidRowPolicyRecord;
        if ((self.commands.insert or self.commands.update) and self.with_check == null) return error.InvalidRowPolicyRecord;
        if (self.using == null and self.with_check == null) return error.InvalidRowPolicyRecord;
    }

    pub fn validate(self: @This(), table: sql_catalog.Table, schema_digest: [32]u8, settings: *const setting_catalog.View) !void {
        try self.validateShape();
        if (self.table_id != table.id or self.schema_version != table.schema_version or
            !std.mem.eql(u8, &self.schema_digest, &schema_digest) or table.storage_mode != .relational)
            return error.InvalidRowPolicyRecord;
        if (self.using) |program| try program.validate(table.columns, settings);
        if (self.with_check) |program| try program.validate(table.columns, settings);
    }
};

/// Internal-only Raft payload. Public policy DDL is deliberately unavailable
/// until all data owners can enforce the new generation.
pub const Command = struct {
    version: u16 = 1,
    expected_revision: u64,
    change: union(enum) { put: Record, drop: struct { id: u64, generation: u64, table_id: u64 } },
};

pub const Snapshot = struct {
    table_id: u64,
    schema_version: u32,
    schema_digest: [32]u8,
    policy_generation: u64,
    catalog_epoch: u64,
    principal: []const u8,
    database: []const u8,
    /// Trusted memberships at this catalog cut, not client-supplied claims.
    roles: []const []const u8 = &.{},
    records: []const Record,
};

pub const SnapshotRequest = struct {
    table_id: u64,
    principal: []const u8,
    database: []const u8,
    roles: []const []const u8 = &.{},
};

/// Principal-independent, linearizable metadata cut used only by owner
/// installation. A trusted provisioned metadata callback obtains this from
/// the leader after read-index; request clients cannot supply the program.
pub const InstallRequest = struct {
    table_id: u64,
    expected_generation: u64,
    expected_catalog_epoch: u64,
    expected_phase: Publication.Phase,
    owner_group_id: u64,
    expected_descriptor_digest: [32]u8,
};

pub const max_install_snapshot_bytes: usize = 4 * 1024 * 1024;

/// Canonical current physical owner identity used in publication ACKs. Both
/// metadata's current routing cut and the data owner compute this digest from
/// the same fields; an old split, relocation or schema
/// cannot satisfy a new owner's ACK slot.
pub const OwnerDescriptor = struct {
    table_id: u64,
    group_id: u64,
    shard_id: u64,
    range_id: u64,
    schema_version: u32,
    schema_digest: [32]u8,
    range_start: []const u8,
    range_end: []const u8,

    pub fn digest(self: @This()) ![32]u8 {
        if (self.table_id == 0 or self.group_id == 0 or self.range_id == 0 or
            self.schema_version == 0 or
            self.range_start.len > std.math.maxInt(u32) or self.range_end.len > std.math.maxInt(u32))
            return error.InvalidRowPolicyPublication;
        var hasher = std.crypto.hash.sha2.Sha256.init(.{});
        hasher.update("antfly/row-policy-owner-descriptor/v1");
        inline for (.{ self.table_id, self.group_id, self.shard_id, self.range_id }) |value| {
            var encoded: [8]u8 = undefined;
            std.mem.writeInt(u64, &encoded, value, .little);
            hasher.update(&encoded);
        }
        var version: [4]u8 = undefined;
        std.mem.writeInt(u32, &version, self.schema_version, .little);
        hasher.update(&version);
        hasher.update(&self.schema_digest);
        inline for (.{ self.range_start, self.range_end }) |part| {
            var size: [4]u8 = undefined;
            std.mem.writeInt(u32, &size, @intCast(part.len), .little);
            hasher.update(&size);
            hasher.update(part);
        }
        var result: [32]u8 = undefined;
        hasher.final(&result);
        return result;
    }
};

test "row-policy owner descriptor digest fences physical range and generation" {
    const descriptor: OwnerDescriptor = .{
        .table_id = 7,
        .group_id = 11,
        .shard_id = 13,
        .range_id = 17,
        .schema_version = 2,
        .schema_digest = @splat(0xab),
        .range_start = "a",
        .range_end = "m",
    };
    const baseline = try descriptor.digest();
    try std.testing.expectEqualDeep(baseline, try descriptor.digest());
    var changed = descriptor;
    changed.range_id += 1;
    const changed_generation_digest = try changed.digest();
    try std.testing.expect(!std.mem.eql(u8, &baseline, &changed_generation_digest));
    changed = descriptor;
    changed.range_end = "n";
    const changed_range_digest = try changed.digest();
    try std.testing.expect(!std.mem.eql(u8, &baseline, &changed_range_digest));
}

pub const InstallSnapshot = struct {
    table_id: u64,
    schema_version: u32,
    schema_digest: [32]u8,
    policy_generation: u64,
    catalog_epoch: u64,
    phase: Publication.Phase,
    records: []const Record,
    settings: []const @import("settings.zig").Record,

    pub fn validateShape(self: @This()) !void {
        if (self.table_id == 0 or self.schema_version == 0 or self.policy_generation == 0 or
            self.catalog_epoch == 0 or self.records.len > 1024 or self.settings.len > 1024)
            return error.InvalidRowPolicyRecord;
        if (self.phase != .disabled and self.records.len == 0) return error.InvalidRowPolicyRecord;
        for (self.records, 0..) |record, i| {
            try record.validateShape();
            if (record.table_id != self.table_id or record.schema_version != self.schema_version or
                !std.mem.eql(u8, &record.schema_digest, &self.schema_digest))
                return error.InvalidRowPolicyRecord;
            for (self.records[0..i]) |previous| if (previous.id == record.id or std.ascii.eqlIgnoreCase(previous.name, record.name))
                return error.InvalidRowPolicyRecord;
        }
        for (self.settings, 0..) |record, i| {
            try record.validate();
            for (self.settings[0..i]) |previous| if (previous.identity.id == record.identity.id or std.ascii.eqlIgnoreCase(previous.name, record.name))
                return error.InvalidSettingRecord;
        }
    }
};

/// Metadata-owned distributed publication. ACKs are exact owner-descriptor
/// identities, not just group IDs: a split, relocation, or generation swap
/// invalidates a stale ACK. Metadata may promote only when its current owner
/// cut equals `required_owners` and every member has both a candidate-install
/// ACK and a serving-install ACK. Only the final active/disabled phase may be
/// exposed to public reads.
pub const Publication = struct {
    pub const Phase = enum { pending_install, serving_install, active, pending_disable, serving_disable, disabled };
    pub const OwnerIdentity = struct { group_id: u64, descriptor_digest: [32]u8 };
    pub const OwnerAck = struct {
        owner: OwnerIdentity,
        catalog_epoch: u64,
        phase: Phase,
        applied_term: u64,
        applied_index: u64,
        bundle_digest: [32]u8,
    };

    table_id: u64,
    schema_version: u32,
    schema_digest: [32]u8,
    generation: u64,
    catalog_epoch: u64,
    /// Metadata's transactionally maintained table-topology generation at
    /// the owner-cut capture. Zero forces legacy/full-cut verification; new
    /// publications always capture a nonzero generation.
    topology_generation: u64 = 0,
    /// Digest of schema, migration, and index-capability bytes maintained
    /// transactionally with table upserts. Zero forces full validation.
    schema_record_digest: [32]u8 = @splat(0),
    /// A prior active generation remains the read authority while a new
    /// immutable candidate is staged, until metadata promotes it.
    serving_generation: ?u64 = null,
    serving_catalog_epoch: ?u64 = null,
    phase: Phase,
    required_owners: []const OwnerIdentity,
    acknowledged_owners: []const OwnerAck,
    serving_acknowledged_owners: []const OwnerAck = &.{},
    /// Final disable is metadata-first for safety. Owners remain fail-closed
    /// until each one has durably installed disabled and this receipt lands.
    disabled_acknowledged_owners: []const OwnerAck = &.{},

    pub fn validateShape(self: @This()) !void {
        if (self.table_id == 0 or self.schema_version == 0 or self.generation == 0 or
            self.catalog_epoch == 0 or self.required_owners.len == 0 or self.required_owners.len > 4096 or
            self.acknowledged_owners.len > self.required_owners.len or
            self.serving_acknowledged_owners.len > self.required_owners.len or
            self.disabled_acknowledged_owners.len > self.required_owners.len)
            return error.InvalidRowPolicyPublication;
        if (self.serving_generation) |serving| {
            if (serving == 0 or serving >= self.generation or self.serving_catalog_epoch == null or self.serving_catalog_epoch.? == 0 or self.serving_catalog_epoch.? >= self.catalog_epoch or
                (self.phase != .pending_install and self.phase != .pending_disable and
                    self.phase != .serving_install and self.phase != .serving_disable))
                return error.InvalidRowPolicyPublication;
        } else if (self.serving_catalog_epoch != null) return error.InvalidRowPolicyPublication;
        var previous_group_id: u64 = 0;
        for (self.required_owners) |owner| {
            if (owner.group_id <= previous_group_id) return error.InvalidRowPolicyPublication;
            previous_group_id = owner.group_id;
        }
        const installing = self.phase == .pending_install or self.phase == .serving_install or self.phase == .active;
        try validateOwnerAcks(self.required_owners, self.acknowledged_owners, self.catalog_epoch, if (installing) .pending_install else .pending_disable);
        try validateOwnerAcks(self.required_owners, self.serving_acknowledged_owners, self.catalog_epoch, if (installing) .serving_install else .serving_disable);
        if (self.phase != .disabled and self.disabled_acknowledged_owners.len != 0)
            return error.InvalidRowPolicyPublication;
        try validateOwnerAcks(self.required_owners, self.disabled_acknowledged_owners, self.catalog_epoch, .disabled);
        if (self.phase == .pending_install or self.phase == .pending_disable) {
            if (self.serving_acknowledged_owners.len != 0) return error.InvalidRowPolicyPublication;
        } else {
            if (self.acknowledged_owners.len != self.required_owners.len)
                return error.InvalidRowPolicyPublication;
        }
        if ((self.phase == .active or self.phase == .disabled) and
            self.serving_acknowledged_owners.len != self.required_owners.len)
            return error.InvalidRowPolicyPublication;
    }
};

fn validateOwnerAcks(required: []const Publication.OwnerIdentity, acknowledgements: []const Publication.OwnerAck, catalog_epoch: u64, phase: Publication.Phase) !void {
    var required_index: usize = 0;
    var previous_group_id: u64 = 0;
    for (acknowledgements) |ack| {
        const group_id = ack.owner.group_id;
        if (group_id <= previous_group_id or ack.applied_term == 0 or ack.applied_index == 0 or
            ack.catalog_epoch != catalog_epoch or ack.phase != phase) return error.InvalidRowPolicyPublication;
        while (required_index < required.len and required[required_index].group_id < group_id) required_index += 1;
        if (required_index == required.len or required[required_index].group_id != group_id or
            !std.mem.eql(u8, &required[required_index].descriptor_digest, &ack.owner.descriptor_digest)) return error.InvalidRowPolicyPublication;
        if (acknowledgements.len != 0 and !std.mem.eql(u8, &acknowledgements[0].bundle_digest, &ack.bundle_digest)) return error.InvalidRowPolicyPublication;
        required_index += 1;
        previous_group_id = group_id;
    }
}

/// Fixed-size authorization view. Owner arrays and ACK history belong to the
/// publication worker, never to the per-request proof issuance path.
pub const PublicationStamp = struct {
    pub const Authority = struct { generation: u64, catalog_epoch: u64 };
    table_id: u64,
    schema_version: u32,
    generation: u64,
    catalog_epoch: u64,
    topology_generation: u64 = 0,
    schema_record_digest: [32]u8 = @splat(0),
    serving_generation: ?u64 = null,
    serving_catalog_epoch: ?u64 = null,
    phase: Publication.Phase,

    pub fn fromPublication(publication: Publication) @This() {
        return .{
            .table_id = publication.table_id,
            .schema_version = publication.schema_version,
            .generation = publication.generation,
            .catalog_epoch = publication.catalog_epoch,
            .topology_generation = publication.topology_generation,
            .schema_record_digest = publication.schema_record_digest,
            .serving_generation = publication.serving_generation,
            .serving_catalog_epoch = publication.serving_catalog_epoch,
            .phase = publication.phase,
        };
    }

    pub fn validateShape(self: @This()) !void {
        if (self.table_id == 0 or self.schema_version == 0 or self.generation == 0 or self.catalog_epoch == 0) return error.InvalidRowPolicyPublication;
        if (self.serving_generation) |serving| {
            if (serving == 0 or serving >= self.generation or self.serving_catalog_epoch == null or self.serving_catalog_epoch.? == 0 or self.serving_catalog_epoch.? >= self.catalog_epoch or
                (self.phase != .pending_install and self.phase != .pending_disable and self.phase != .serving_install and self.phase != .serving_disable)) return error.InvalidRowPolicyPublication;
        } else if (self.serving_catalog_epoch != null) return error.InvalidRowPolicyPublication;
    }

    pub fn servingAuthority(self: @This()) !?Authority {
        try self.validateShape();
        return switch (self.phase) {
            .disabled => null,
            .active => .{ .generation = self.generation, .catalog_epoch = self.catalog_epoch },
            .pending_install, .pending_disable => .{
                .generation = self.serving_generation orelse return error.RowPolicyCatalogChanged,
                .catalog_epoch = self.serving_catalog_epoch orelse return error.RowPolicyCatalogChanged,
            },
            .serving_install, .serving_disable => error.RowPolicyCatalogChanged,
        };
    }
};

test "row-policy publication stamp retains prior serving authority only during candidate install" {
    const active: PublicationStamp = .{ .table_id = 7, .schema_version = 1, .generation = 4, .catalog_epoch = 9, .topology_generation = 1, .schema_record_digest = @splat(1), .phase = .active };
    try std.testing.expectEqual(@as(u64, 4), (try active.servingAuthority()).?.generation);
    var candidate = active;
    candidate.phase = .pending_install;
    candidate.generation = 5;
    candidate.catalog_epoch = 12;
    candidate.serving_generation = 4;
    candidate.serving_catalog_epoch = 9;
    const prior = (try candidate.servingAuthority()).?;
    try std.testing.expectEqual(@as(u64, 4), prior.generation);
    try std.testing.expectEqual(@as(u64, 9), prior.catalog_epoch);
    candidate.phase = .serving_install;
    try std.testing.expectError(error.RowPolicyCatalogChanged, candidate.servingAuthority());
    candidate.phase = .disabled;
    candidate.serving_generation = null;
    candidate.serving_catalog_epoch = null;
    try std.testing.expect((try candidate.servingAuthority()) == null);
}

test "row-policy publication requires canonical owner and acknowledgement order" {
    const owners = [_]Publication.OwnerIdentity{
        .{ .group_id = 7, .descriptor_digest = @splat(1) },
        .{ .group_id = 8, .descriptor_digest = @splat(2) },
    };
    const acks = [_]Publication.OwnerAck{
        .{ .owner = owners[0], .catalog_epoch = 3, .phase = .pending_install, .applied_term = 1, .applied_index = 1, .bundle_digest = @splat(4) },
        .{ .owner = owners[1], .catalog_epoch = 3, .phase = .pending_install, .applied_term = 1, .applied_index = 2, .bundle_digest = @splat(4) },
    };
    var publication: Publication = .{ .table_id = 1, .schema_version = 1, .schema_digest = @splat(3), .generation = 1, .catalog_epoch = 3, .phase = .pending_install, .required_owners = &owners, .acknowledged_owners = &acks };
    try publication.validateShape();
    publication.required_owners = &.{ owners[1], owners[0] };
    try std.testing.expectError(error.InvalidRowPolicyPublication, publication.validateShape());
    publication.required_owners = &owners;
    publication.acknowledged_owners = &.{ acks[1], acks[0] };
    try std.testing.expectError(error.InvalidRowPolicyPublication, publication.validateShape());
}

pub const PublicationCommand = struct {
    version: u16 = 1,
    expected_revision: u64,
    change: union(enum) {
        begin: Publication,
        acknowledge: struct { table_id: u64, generation: u64, receipt: Publication.OwnerAck },
        promote: struct { table_id: u64, generation: u64, phase: Publication.Phase },
    },
};

pub const PublicationWork = struct {
    revision: u64,
    publication: ?Publication,
};

/// Public SQL supplies only the desired table state. Metadata derives the
/// owner cut and schema identity from its own linearizable catalog snapshot.
pub const BeginRequest = struct {
    table_id: u64,
    enable: bool,
    expected_revision: u64,
};

/// The owner returns a snapshot from one authenticated, linearizable catalog
/// cut, with all nested slices allocated in the supplied request arena (or
/// static). A production owner can clone a retained immutable epoch into that
/// arena; borrowed mutable Raft buffers are forbidden by this contract.
pub const Owner = struct {
    ptr: *anyopaque,
    load: *const fn (*anyopaque, std.mem.Allocator, u64, []const u8, []const u8) anyerror!Snapshot,
};

pub const View = struct {
    arena: std.heap.ArenaAllocator,
    snapshot: Snapshot,
    column_count: usize = 0,

    pub fn capture(backing: std.mem.Allocator, owner: Owner, table: sql_catalog.Table, schema_digest: [32]u8, settings: *const setting_catalog.View) !View {
        var arena = std.heap.ArenaAllocator.init(backing);
        errdefer arena.deinit();
        const alloc = arena.allocator();
        const raw = try owner.load(owner.ptr, alloc, table.id, settings.scope.principal, settings.scope.database);
        if (raw.table_id != table.id or raw.schema_version != table.schema_version or
            !std.mem.eql(u8, &raw.schema_digest, &schema_digest) or raw.policy_generation == 0 or
            // Policy-sensitive settings are frozen while this published
            // generation serves; unrelated settings may advance the global
            // catalog epoch without invalidating its immutable predicates.
            raw.catalog_epoch > settings.epoch or
            !std.mem.eql(u8, raw.principal, settings.scope.principal) or
            !std.mem.eql(u8, raw.database, settings.scope.database) or raw.records.len > 1024 or raw.roles.len > 256)
            return error.RowPolicyCatalogChanged;
        for (raw.roles, 0..) |role, i| {
            if (role.len == 0 or role.len > 128 or !std.unicode.utf8ValidateSlice(role)) return error.RowPolicyCatalogChanged;
            for (raw.roles[0..i]) |previous| if (std.mem.eql(u8, previous, role)) return error.RowPolicyCatalogChanged;
        }
        for (raw.records, 0..) |record, i| {
            try record.validate(table, schema_digest, settings);
            for (raw.records[0..i]) |prior| if (prior.id == record.id or std.ascii.eqlIgnoreCase(prior.name, record.name)) return error.InvalidRowPolicyRecord;
        }
        return .{ .arena = arena, .snapshot = raw, .column_count = table.columns.len };
    }

    pub fn deinit(self: *View) void {
        self.arena.deinit();
        self.* = undefined;
    }

    pub fn requireCurrent(self: *const View, generation: u64, catalog_epoch: u64, schema_version: u32) !void {
        if (self.snapshot.policy_generation != generation or self.snapshot.catalog_epoch != catalog_epoch or self.snapshot.schema_version != schema_version)
            return error.RowPolicyCatalogChanged;
    }
};

/// Owner-side evaluator for one pinned policy epoch and one authenticated
/// command. Construction partitions the applicable programs once, so a scan
/// only evaluates predicates for its command/roles. The caller must supply
/// typed cells in the pinned table's ordinal order and must revalidate the
/// policy generation at the commit boundary for mutations.
pub const Evaluator = struct {
    alloc: std.mem.Allocator,
    permissive: []const *const Predicate,
    restrictive: []const *const Predicate,
    required_columns: []const u32,
    column_count: usize,
    schema_version: u32,
    settings: *const setting_catalog.View,

    pub const Action = enum { select, insert, update_old, update_new, delete };

    pub fn init(alloc: std.mem.Allocator, view: *const View, settings: *const setting_catalog.View, command: Action) !Evaluator {
        return initForAuthenticatedRoles(alloc, view, settings, command, &.{});
    }

    /// Role memberships come only from an owner-verified principal proof;
    /// the metadata policy snapshot itself may never assert them. This keeps
    /// policy definitions and authentication claims under separate authority.
    pub fn initForAuthenticatedRoles(alloc: std.mem.Allocator, view: *const View, settings: *const setting_catalog.View, command: Action, authenticated_roles: []const []const u8) !Evaluator {
        if (view.snapshot.catalog_epoch > settings.epoch or
            !std.mem.eql(u8, view.snapshot.principal, settings.scope.principal) or
            !std.mem.eql(u8, view.snapshot.database, settings.scope.database) or
            view.snapshot.roles.len != 0) return error.RowPolicyCatalogChanged;
        var allow: std.ArrayList(*const Predicate) = .empty;
        errdefer allow.deinit(alloc);
        var restrict: std.ArrayList(*const Predicate) = .empty;
        errdefer restrict.deinit(alloc);
        const seen = try alloc.alloc(bool, view.column_count);
        defer alloc.free(seen);
        @memset(seen, false);
        for (view.snapshot.records) |*record| {
            const applies = switch (command) {
                .select => record.commands.select,
                .insert => record.commands.insert,
                .update_old, .update_new => record.commands.update,
                .delete => record.commands.delete,
            };
            if (!applies or !roleMatches(record.roles, view.snapshot.principal, authenticated_roles)) continue;
            const predicate: *const Predicate = switch (command) {
                .insert, .update_new => if (record.with_check) |*program| program else return error.InvalidRowPolicyRecord,
                .select, .delete, .update_old => if (record.using) |*program| program else return error.InvalidRowPolicyRecord,
            };
            for (predicate.instructions) |instruction| if (instruction.operation == .column) {
                const ordinal = instruction.operation.column;
                if (ordinal >= seen.len) return error.InvalidRowPolicyProgram;
                seen[ordinal] = true;
            };
            if (record.permissive) try allow.append(alloc, predicate) else try restrict.append(alloc, predicate);
        }
        var required: std.ArrayList(u32) = .empty;
        errdefer required.deinit(alloc);
        for (seen, 0..) |needed, ordinal| if (needed) try required.append(alloc, @intCast(ordinal));
        const allow_slice = try allow.toOwnedSlice(alloc);
        errdefer alloc.free(allow_slice);
        const restrict_slice = try restrict.toOwnedSlice(alloc);
        errdefer alloc.free(restrict_slice);
        const required_slice = try required.toOwnedSlice(alloc);
        return .{ .alloc = alloc, .permissive = allow_slice, .restrictive = restrict_slice, .required_columns = required_slice, .column_count = view.column_count, .schema_version = view.snapshot.schema_version, .settings = settings };
    }

    pub fn deinit(self: *Evaluator) void {
        self.alloc.free(self.permissive);
        self.alloc.free(self.restrictive);
        self.alloc.free(self.required_columns);
        self.* = undefined;
    }

    /// SQL RLS grants only TRUE. FALSE and UNKNOWN both reject. An enabled
    /// table without an applicable permissive policy rejects by default.
    pub fn permits(self: *const Evaluator, alloc: std.mem.Allocator, cells: []const scalar.Datum) !bool {
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        return self.permitsWithScratch(arena.allocator(), cells);
    }

    /// The cursor owns and resets `scratch` between rows. This avoids a fresh
    /// arena allocation per predicate evaluation on long scans.
    pub fn permitsWithScratch(self: *const Evaluator, scratch: std.mem.Allocator, cells: []const scalar.Datum) !bool {
        var granted = false;
        for (self.permissive) |program| {
            if (try evaluatePredicate(program, self.settings, scratch, cells)) {
                granted = true;
                break;
            }
        }
        if (!granted) return false;
        for (self.restrictive) |program| if (!try evaluatePredicate(program, self.settings, scratch, cells)) return false;
        return true;
    }
};

fn roleMatches(policy_roles: []const []const u8, principal: []const u8, authenticated_roles: []const []const u8) bool {
    for (policy_roles) |policy_role| {
        if (std.mem.eql(u8, policy_role, "PUBLIC")) return true;
        if (std.mem.eql(u8, policy_role, principal)) return true;
        for (authenticated_roles) |role| if (std.mem.eql(u8, policy_role, role)) return true;
    }
    return false;
}

fn evaluatePredicate(predicate: *const Predicate, settings: *const setting_catalog.View, alloc: std.mem.Allocator, cells: []const scalar.Datum) !bool {
    // Program owns no bytes here: the immutable catalog epoch owns the DAG.
    // The caller's short-lived scratch arena owns computed values for one row.
    const program: scalar.Program = .{
        .arena = undefined,
        .instructions = predicate.instructions,
        .root = predicate.root,
        .output_type = predicate.instructions[predicate.root].type,
        .parameter_types = &.{},
        .required_columns = &.{},
        .settings = settings,
    };
    const result = try program.evaluate(alloc, cells, &.{}, .{});
    if (result.sql_null) return false;
    if (result.value != .bool) return error.InvalidRowPolicyProgram;
    return result.value.bool;
}

test "row policy capture fences revocation and rejects client-writable setting dependencies" {
    const alloc = std.testing.allocator;
    const Setting = @import("settings.zig");
    const Fake = struct {
        fn load(_: *anyopaque, _: std.mem.Allocator, scope: Setting.Scope) !Setting.Snapshot {
            return .{ .scope = scope, .epoch = 7, .definitions = &.{.{ .identity = .{ .id = 4, .generation = 2 }, .name = "app.tenant", .kind = .string, .policy_sensitive = true, .default = .{ .string = "tenant-a" } }} };
        }
    };
    var dummy: u8 = 0;
    var settings = try setting_catalog.View.capture(alloc, .{ .ptr = &dummy, .load = Fake.load }, .{ .principal = "alice", .database = "main" }, &.{});
    defer settings.deinit();
    const table: sql_catalog.Table = .{ .id = 3, .physical_name = "table:3", .schema_version = 5, .columns = &.{.{ .name = "tenant", .path = "tenant", .type = .string }} };
    const predicate: Predicate = .{ .instructions = &.{.{ .type = .{ .kind = .boolean, .nullable = false }, .operation = .{ .literal = .{ .bool = true } } }}, .root = 0 };
    const record: Record = .{ .id = 9, .generation = 1, .table_id = 3, .schema_version = 5, .schema_digest = @splat(8), .name = "tenant_guard", .commands = .{ .select = true }, .roles = &.{"alice"}, .using = predicate };
    const FakePolicy = struct {
        fn load(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !Snapshot {
            return .{ .table_id = 3, .schema_version = 5, .schema_digest = @splat(8), .policy_generation = 11, .catalog_epoch = 7, .principal = "alice", .database = "main", .records = &.{record} };
        }
    };
    var view = try View.capture(alloc, .{ .ptr = &dummy, .load = FakePolicy.load }, table, @splat(8), &settings);
    defer view.deinit();
    const NewerUnrelatedSettingEpoch = struct {
        fn load(_: *anyopaque, _: std.mem.Allocator, scope: Setting.Scope) !Setting.Snapshot {
            return .{ .scope = scope, .epoch = 8, .definitions = &.{.{ .identity = .{ .id = 4, .generation = 2 }, .name = "app.tenant", .kind = .string, .policy_sensitive = true, .default = .{ .string = "tenant-a" } }} };
        }
    };
    var newer_settings = try setting_catalog.View.capture(alloc, .{ .ptr = &dummy, .load = NewerUnrelatedSettingEpoch.load }, .{ .principal = "alice", .database = "main" }, &.{});
    defer newer_settings.deinit();
    var view_at_newer_setting_epoch = try View.capture(alloc, .{ .ptr = &dummy, .load = FakePolicy.load }, table, @splat(8), &newer_settings);
    defer view_at_newer_setting_epoch.deinit();
    var evaluator_at_newer_setting_epoch = try Evaluator.init(alloc, &view_at_newer_setting_epoch, &newer_settings, .select);
    defer evaluator_at_newer_setting_epoch.deinit();
    try std.testing.expect(try evaluator_at_newer_setting_epoch.permits(alloc, &.{.{}}));
    try view.requireCurrent(11, 7, 5);
    try std.testing.expectError(error.RowPolicyCatalogChanged, view.requireCurrent(12, 7, 5));
    try std.testing.expectError(error.RowPolicyCatalogChanged, view.requireCurrent(11, 8, 5));
    var stale_table = table;
    stale_table.schema_version = 6;
    try std.testing.expectError(error.RowPolicyCatalogChanged, View.capture(alloc, .{ .ptr = &dummy, .load = FakePolicy.load }, stale_table, @splat(8), &settings));
    stale_table = table;
    stale_table.id = 4;
    try std.testing.expectError(error.RowPolicyCatalogChanged, View.capture(alloc, .{ .ptr = &dummy, .load = FakePolicy.load }, stale_table, @splat(8), &settings));
    const spoofed: Predicate = .{ .instructions = &.{
        .{ .type = .{ .kind = .string }, .operation = .{ .literal = .{ .string = "app.tenant" } } },
        .{ .type = .{ .kind = .string }, .operation = .{ .call = .{ .function = .current_setting, .args = &.{}, .setting_identity = .{ .id = 4, .generation = 1 } } } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .literal = .{ .bool = true } } },
    }, .root = 2 };
    try std.testing.expectError(error.SettingCatalogChanged, spoofed.validate(table.columns, &settings));
    const malformed: Predicate = .{ .instructions = &.{
        .{ .type = .{ .kind = .boolean }, .operation = .{ .literal = .{ .bool = true } } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .call = .{ .function = .starts_with, .args = &.{0} } } },
    }, .root = 1 };
    try std.testing.expectError(error.InvalidRowPolicyProgram, malformed.validate(table.columns, &settings));
}

test "owner policy evaluator combines permissive and restrictive programs with SQL null semantics" {
    const alloc = std.testing.allocator;
    const Fake = struct {
        fn load(_: *anyopaque, _: std.mem.Allocator, scope: @import("settings.zig").Scope) !@import("settings.zig").Snapshot {
            return .{ .scope = scope, .epoch = 5, .definitions = &.{} };
        }
    };
    var dummy: u8 = 0;
    var settings = try setting_catalog.View.capture(alloc, .{ .ptr = &dummy, .load = Fake.load }, .{ .principal = "alice", .database = "main" }, &.{});
    defer settings.deinit();
    const tenant_is_a: Predicate = .{ .instructions = &.{
        .{ .type = .{ .kind = .string }, .operation = .{ .column = 0 } },
        .{ .type = .{ .kind = .string }, .operation = .{ .literal = .{ .string = "a" } } },
        .{ .type = .{ .kind = .boolean }, .operation = .{ .binary = .{ .op = .eq, .left = 0, .right = 1 } } },
    }, .root = 2 };
    const visible: Predicate = .{ .instructions = &.{
        .{ .type = .{ .kind = .boolean }, .operation = .{ .column = 1 } },
    }, .root = 0 };
    const table: sql_catalog.Table = .{ .id = 7, .physical_name = "table:7", .schema_version = 3, .columns = &.{
        .{ .name = "tenant", .path = "tenant", .type = .string },
        .{ .name = "visible", .path = "visible", .type = .boolean },
    } };
    const records = [_]Record{
        .{ .id = 1, .generation = 1, .table_id = 7, .schema_version = 3, .schema_digest = @splat(9), .name = "tenant", .commands = .{ .select = true }, .roles = &.{"alice"}, .using = tenant_is_a },
        .{ .id = 2, .generation = 1, .table_id = 7, .schema_version = 3, .schema_digest = @splat(9), .name = "visible", .commands = .{ .select = true }, .roles = &.{"PUBLIC"}, .permissive = false, .using = visible },
    };
    const PolicyOwner = struct {
        fn load(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !Snapshot {
            return .{ .table_id = 7, .schema_version = 3, .schema_digest = @splat(9), .policy_generation = 2, .catalog_epoch = 5, .principal = "alice", .database = "main", .records = &records };
        }
    };
    var view = try View.capture(alloc, .{ .ptr = &dummy, .load = PolicyOwner.load }, table, @splat(9), &settings);
    defer view.deinit();
    var alice = try Evaluator.init(alloc, &view, &settings, .select);
    defer alice.deinit();
    try std.testing.expect(try alice.permits(alloc, &.{ scalar.Datum.json(.{ .string = "a" }), scalar.Datum.json(.{ .bool = true }) }));
    try std.testing.expect(!try alice.permits(alloc, &.{ scalar.Datum.json(.{ .string = "b" }), scalar.Datum.json(.{ .bool = true }) }));
    try std.testing.expect(!try alice.permits(alloc, &.{ scalar.Datum.json(.{ .string = "a" }), scalar.Datum.json(.{ .bool = false }) }));
    try std.testing.expect(!try alice.permits(alloc, &.{ scalar.Datum.json(.{ .string = "a" }), .{} }));
    view.snapshot.principal = "bob";
    settings.scope.principal = "bob";
    var bob = try Evaluator.init(alloc, &view, &settings, .select);
    defer bob.deinit();
    try std.testing.expect(!try bob.permits(alloc, &.{ scalar.Datum.json(.{ .string = "a" }), scalar.Datum.json(.{ .bool = true }) }));
    var bob_with_verified_role = try Evaluator.initForAuthenticatedRoles(alloc, &view, &settings, .select, &.{"alice"});
    defer bob_with_verified_role.deinit();
    try std.testing.expect(try bob_with_verified_role.permits(alloc, &.{ scalar.Datum.json(.{ .string = "a" }), scalar.Datum.json(.{ .bool = true }) }));
    view.snapshot.principal = "alice";
    settings.scope.principal = "alice";
    var insert = try Evaluator.init(alloc, &view, &settings, .insert);
    defer insert.deinit();
    try std.testing.expect(!try insert.permits(alloc, &.{ scalar.Datum.json(.{ .string = "a" }), scalar.Datum.json(.{ .bool = true }) }));
}
