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

//! File-backed immutable field reads independent of logical source generations.
//! Capture metadata under the actual native field publication lease; later
//! resource reads need neither an open index store nor current pathname lookup.
const std = @import("std");
const fields = @import("index_field_generation.zig");
const graph = @import("immutable_graph.zig");
const publication = @import("index_publication.zig");
const storage_io = @import("lsm_backend/storage_io.zig");
const composite = @import("../segment_composite.zig");
const pack = @import("../field_pack.zig");
const segment = @import("../segment.zig");
const A = std.mem.Allocator;
const Source = @import("../segment_source.zig").Source;
const Digest = [32]u8;
const sections_per_field = @typeInfo(segment.SectionType).@"enum".field_names.len;
pub const Limits = struct {
    fields: usize = fields.maximum_fields,
    resources: usize = 64,
    metadata_bytes: usize = 32 * 1024 * 1024,
    pack_metadata_bytes: usize = 1024 * 1024,
};
const Field = struct { offset: u32, length: u16, resources: [sections_per_field]u8 };
const OpenedPack = struct { source: Source };
const Cell = struct { resource: fields.Resource, physical: ?Source = null, opened: std.atomic.Value(?*OpenedPack) = .init(null) };

const Context = struct {
    allocator: A,
    references: std.atomic.Value(usize) = .init(1),
    lease: publication.SharedReaders.Lease,
    binding: publication.Binding,
    storage: storage_io.Storage.Lease,
    root: []u8 = &.{},
    directory: fields.Directory,
    fields: []Field = &.{},
    stored: ?Field = null,
    metadata: []u8 = &.{},
    resources: []Cell = &.{},
    by_identity: std.AutoHashMapUnmanaged(Digest, u8) = .empty,
    limits: Limits,
    fn retain(raw: *anyopaque) void {
        const self: *Context = @ptrCast(@alignCast(raw));
        const before = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(before != 0 and before != std.math.maxInt(usize));
    }
    fn release(raw: *anyopaque) void {
        const self: *Context = @ptrCast(@alignCast(raw));
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        for (self.resources) |*cell| {
            if (cell.opened.load(.acquire)) |opened| {
                opened.source.close();
                self.allocator.destroy(opened);
            }
            if (cell.physical) |*physical| physical.close();
        }
        self.by_identity.deinit(self.allocator);
        self.allocator.free(self.resources);
        self.allocator.free(self.metadata);
        self.allocator.free(self.fields);
        self.allocator.free(self.root);
        self.storage.deinit();
        // Last: physical sources close before native reader drainage can allow
        // their retained root and packs to enter the reclamation frontier.
        self.lease.deinit();
        self.allocator.destroy(self);
    }
    fn check(raw: *anyopaque) !void {
        const self: *Context = @ptrCast(@alignCast(raw));
        if (!std.meta.eql(try self.lease.binding(), self.binding)) return error.IndexPublicationFenceMismatch;
    }
    fn retained(raw: *anyopaque) usize {
        const self: *Context = @ptrCast(@alignCast(raw));
        var bytes = @sizeOf(Context) +| self.root.len +| self.metadata.len +| self.fields.len * @sizeOf(Field) +| self.resources.len * @sizeOf(Cell) +| self.by_identity.capacity() * 64;
        for (self.resources) |*cell| {
            if (cell.physical) |physical| bytes +|= physical.retainedBytes();
            if (cell.opened.load(.acquire)) |opened| bytes +|= @sizeOf(OpenedPack) +| opened.source.retainedBytes();
        }
        return bytes;
    }
    fn descriptor(self: *const Context, field: Field) !fields.Descriptor {
        if (field.offset > self.metadata.len or field.length > self.metadata.len - field.offset) return error.InvalidIndexFieldGeneration;
        return fields.Descriptor.decode(self.metadata[field.offset..][0..field.length]);
    }
    fn openedPack(self: *Context, ordinal: usize) !*OpenedPack {
        const cell = &self.resources[ordinal];
        if (cell.opened.load(.acquire)) |opened| return opened;
        // Cold reads may duplicate one bounded open. There is no lock held
        // across filesystem IO and no waiter can block shutdown. Only one
        // immutable source wins; losing sources release every descriptor.
        const fresh = self.openPack(cell) catch |err| {
            if (cell.opened.load(.acquire)) |ready| return ready;
            return err;
        };
        if (cell.opened.cmpxchgStrong(null, fresh, .acq_rel, .acquire)) |winner| {
            fresh.source.close();
            self.allocator.destroy(fresh);
            return winner.?;
        }
        return fresh;
    }
    fn openPack(self: *Context, cell: *const Cell) !*OpenedPack {
        try check(self);
        const resource = cell.resource;
        // Context owns the captured inode until all proxies and pack adapters
        // close. Each adapter borrows it; speculative cold opens never consume
        // or close another thread's physical source.
        var physical = cell.physical orelse return error.InvalidIndexFieldResource;
        if (physical == .ranges) {
            physical.ranges.close = borrowedClose;
            physical.ranges.retained_bytes = null;
        }
        const opened = try self.allocator.create(OpenedPack);
        errdefer self.allocator.destroy(opened);
        var source = try pack.open(self.allocator, physical, resource.identity, .{ .metadata_bytes = self.limits.pack_metadata_bytes });
        errdefer source.close();
        if (source.len() != resource.payload_bytes) return error.FieldPackIntegrityMismatch;
        opened.* = .{ .source = source };
        return opened;
    }
    fn borrowedClose(_: *anyopaque) void {}
    fn resolve(raw: *anyopaque, a: A, identity: Digest) !Source {
        const self: *Context = @ptrCast(@alignCast(raw));
        try check(self);
        const ordinal = self.by_identity.get(identity) orelse return error.InvalidIndexFieldResource;
        const proxy = try a.create(Proxy);
        retain(self);
        proxy.* = .{ .allocator = a, .context = self, .ordinal = ordinal };
        return .{ .ranges = .{ .ptr = proxy, .length = self.resources[ordinal].resource.payload_bytes, .read_into = Proxy.read, .close = Proxy.close, .check_read_context = Proxy.check, .retained_bytes = Proxy.retained } };
    }
    fn provider(self: *Context) composite.Provider {
        return .{ .ptr = self, .resolve = resolve, .retain = retain, .release = release, .check = check, .retained_bytes = retained };
    }
};
const Proxy = struct {
    allocator: A,
    context: *Context,
    ordinal: usize,
    fn check(raw: *anyopaque) !void {
        const self: *Proxy = @ptrCast(@alignCast(raw));
        try Context.check(self.context);
    }
    fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
        const self: *Proxy = @ptrCast(@alignCast(raw));
        try Context.check(self.context);
        const resource = self.context.resources[self.ordinal].resource;
        if (offset > resource.payload_bytes or out.len > resource.payload_bytes - offset) return error.EndOfStream;
        const opened = try self.context.openedPack(self.ordinal);
        try opened.source.readInto(offset, out);
    }
    fn close(raw: *anyopaque) void {
        const self: *Proxy = @ptrCast(@alignCast(raw));
        Context.release(self.context);
        self.allocator.destroy(self);
    }
    fn retained(_: *anyopaque) usize {
        return @sizeOf(Proxy);
    }
};

pub const Inventory = struct {
    context: ?*Context,
    pub fn deinit(self: *Inventory) void {
        if (self.context) |context| Context.release(context);
        self.context = null;
    }
    pub fn lease(self: Inventory) !publication.SharedReaders.Lease {
        const context = self.context orelse return error.IndexSourceLeaseClosed;
        try Context.check(context);
        return context.lease.clone();
    }
    pub fn directory(self: Inventory) !fields.Directory {
        const context = self.context orelse return error.IndexSourceLeaseClosed;
        try Context.check(context);
        return context.directory;
    }
    pub fn retainedBytes(self: Inventory) usize {
        return if (self.context) |context| Context.retained(context) else 0;
    }
    pub fn provider(self: Inventory) !composite.Provider {
        const context = self.context orelse return error.IndexSourceLeaseClosed;
        try Context.check(context);
        return context.provider();
    }
    pub fn field(self: Inventory, ordinal: usize) !fields.Descriptor {
        const context = self.context orelse return error.IndexSourceLeaseClosed;
        try Context.check(context);
        if (ordinal >= context.fields.len) return error.InvalidIndexFieldDirectory;
        return context.descriptor(context.fields[ordinal]);
    }
    pub fn resourceCount(self: Inventory) !usize {
        const context = self.context orelse return error.IndexSourceLeaseClosed;
        try Context.check(context);
        return context.resources.len;
    }
    pub fn resourceDescriptor(self: Inventory, ordinal: usize) !fields.Resource {
        const context = self.context orelse return error.IndexSourceLeaseClosed;
        try Context.check(context);
        if (ordinal >= context.resources.len) return error.InvalidIndexFieldResource;
        return context.resources[ordinal].resource;
    }
    pub fn resourceOrdinal(self: Inventory, identity: Digest) !usize {
        const context = self.context orelse return error.IndexSourceLeaseClosed;
        try Context.check(context);
        return context.by_identity.get(identity) orelse return error.InvalidIndexFieldResource;
    }
    pub fn fieldResource(self: Inventory, ordinal: usize, section_ordinal: usize) !fields.Resource {
        const context = self.context orelse return error.IndexSourceLeaseClosed;
        try Context.check(context);
        if (ordinal >= context.fields.len) return error.InvalidIndexFieldDirectory;
        const capture = context.fields[ordinal];
        const descriptor = try context.descriptor(capture);
        const section_descriptor = try descriptor.section(section_ordinal);
        return context.resources[capture.resources[section_descriptor.resource]].resource;
    }
    pub fn section(self: Inventory, a: A, name: []const u8, kind: segment.SectionType) !?Source {
        const context = self.context orelse return error.IndexSourceLeaseClosed;
        try Context.check(context);
        var lower: usize = 0;
        var upper = context.fields.len;
        while (lower < upper) {
            const middle = lower + (upper - lower) / 2;
            const descriptor = try context.descriptor(context.fields[middle]);
            switch (std.mem.order(u8, descriptor.name(), name)) {
                .lt => lower = middle + 1,
                .gt => upper = middle,
                .eq => {
                    for (0..descriptor.sectionCount()) |index| {
                        const part = try descriptor.section(index);
                        if (part.kind != kind) continue;
                        const resource = context.resources[context.fields[middle].resources[part.resource]].resource;
                        const raw = try composite.encode(a, &.{.{ .resource = .{ .identity = resource.identity, .source_bytes = resource.payload_bytes, .offset = part.offset, .length = part.length } }}, .{});
                        defer a.free(raw);
                        return try composite.open(a, raw, context.provider(), .{});
                    }
                    return null;
                },
            }
        }
        return null;
    }
};

/// Capture actual native membership before dropping the metadata read view.
/// Pin physical versions now; defer pack metadata and body decoding until read.
/// Physical sources and the independent field pin outlive the index, while
/// unrelated logical source claims may be reclaimed.
pub fn open(a: A, reader: anytype, lease: *const publication.SharedReaders.Lease, storage: storage_io.Storage, root: []const u8, limits: Limits) !Inventory {
    if (limits.resources == 0 or limits.resources > 256 or limits.metadata_bytes == 0 or root.len > limits.metadata_bytes) return error.ResourceBudgetExceeded;
    const held = try graph.openHeldFields(reader, lease);
    const directory = try fields.describeFields(&held);
    if (directory.fields > limits.fields) return error.ResourceBudgetExceeded;
    const initial_charge = try std.math.add(usize, try std.math.add(usize, @sizeOf(Context), root.len), @as(usize, directory.fields) * @sizeOf(Field));
    if (initial_charge > limits.metadata_bytes) return error.ResourceBudgetExceeded;
    if (storage.vtable.open_leased_immutable_source == null) return error.ImmutableReadSourceUnsupported;
    var scope = try lease.clone();
    var scope_owned = true;
    defer if (scope_owned) scope.deinit();
    var io_owner = try storage.acquireLease();
    var io_owned = true;
    defer if (io_owned) io_owner.deinit();
    const binding = try scope.binding();
    const context = try a.create(Context);
    context.* = .{ .allocator = a, .lease = scope, .binding = binding, .storage = io_owner, .directory = directory, .limits = limits };
    scope_owned = false;
    io_owned = false;
    errdefer Context.release(context);
    context.root = try a.dupe(u8, root);
    context.fields = try a.alloc(Field, directory.fields);
    var raw: std.ArrayListUnmanaged(u8) = .empty;
    defer raw.deinit(a);
    var resources: std.ArrayListUnmanaged(Cell) = .empty;
    defer resources.deinit(a);
    var charge = initial_charge;
    if (directory.stored != null) {
        const stored = try fields.selectStoredFields(reader, &held);
        context.stored = try captureField(a, reader, &stored, context, &raw, &resources, &charge);
    }
    for (context.fields, 0..) |*capture, ordinal| {
        const selected = try fields.selectFields(reader, &held, @intCast(ordinal));
        capture.* = try captureField(a, reader, &selected.field, context, &raw, &resources, &charge);
        if (ordinal != 0) {
            const previous = context.fields[ordinal - 1];
            const before = try fields.Descriptor.decode(raw.items[previous.offset..][0..previous.length]);
            const after = try fields.Descriptor.decode(raw.items[capture.offset..][0..capture.length]);
            if (std.mem.order(u8, before.name(), after.name()) != .lt) return error.InvalidIndexFieldDirectory;
        }
    }
    context.metadata = try raw.toOwnedSlice(a);
    context.resources = try resources.toOwnedSlice(a);
    for (context.resources) |*cell| {
        const path = try std.fmt.allocPrint(a, "{s}/{d}.seg", .{ context.root, cell.resource.file });
        defer a.free(path);
        var physical = try context.storage.view.openLeasedImmutableSource(a, path);
        errdefer physical.close();
        if (physical.len() != cell.resource.physical_bytes) return error.FieldPackIntegrityMismatch;
        cell.physical = physical;
    }
    return .{ .context = context };
}

fn captureField(a: A, reader: anytype, member: *const fields.HeldField, context: *Context, raw: *std.ArrayListUnmanaged(u8), resources: *std.ArrayListUnmanaged(Cell), charge: *usize) !Field {
    const descriptor = try member.descriptor();
    charge.* = try std.math.add(usize, charge.*, try std.math.mul(usize, descriptor.raw.len, 3));
    if (charge.* > context.limits.metadata_bytes) return error.ResourceBudgetExceeded;
    var capture: Field = .{ .offset = std.math.cast(u32, raw.items.len) orelse return error.ResourceBudgetExceeded, .length = std.math.cast(u16, descriptor.raw.len) orelse return error.ResourceBudgetExceeded, .resources = @splat(0) };
    try raw.appendSlice(a, descriptor.raw);
    for (0..descriptor.sectionCount()) |index| {
        const part = try descriptor.section(index);
        const held = try member.resource(reader, part);
        const resource = try held.descriptor();
        const pages = resource.payload_bytes / pack.page_bytes + @intFromBool(resource.payload_bytes % pack.page_bytes != 0);
        if (try std.math.add(u64, 32, try std.math.mul(u64, pages, 32)) > context.limits.pack_metadata_bytes) return error.ResourceBudgetExceeded;
        const slot = context.by_identity.get(resource.identity) orelse added: {
            if (resources.items.len >= context.limits.resources) return error.ResourceBudgetExceeded;
            charge.* = try std.math.add(usize, charge.*, 512);
            if (charge.* > context.limits.metadata_bytes) return error.ResourceBudgetExceeded;
            const resource_slot: u8 = @intCast(resources.items.len);
            try resources.append(a, .{ .resource = resource });
            try context.by_identity.put(a, resource.identity, resource_slot);
            break :added resource_slot;
        };
        const prior = resources.items[slot].resource;
        if (prior.payload_bytes != resource.payload_bytes or prior.physical_bytes != resource.physical_bytes) return error.InvalidIndexFieldResource;
        capture.resources[part.resource] = slot;
    }
    return capture;
}

pub const PhysicalResource = struct { descriptor: fields.Resource, source: Source };
const PhysicalProxy = struct {
    allocator: A,
    context: *Context,
    ordinal: usize,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try Context.check(self.context);
    }
    fn read(raw: *anyopaque, offset: u64, output: []u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try Context.check(self.context);
        const physical = self.context.resources[self.ordinal].physical orelse return error.InvalidIndexFieldResource;
        try physical.readInto(offset, output);
    }
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        Context.release(self.context);
        self.allocator.destroy(self);
    }
    fn retained(_: *anyopaque) usize {
        return @sizeOf(@This());
    }
};

const Admitted = struct {
    allocator: A,
    context: *Context,
    source: Source,
    metadata: segment.SegmentReader.Admission,
    handle: Authenticated,
    fn read(raw: *anyopaque, offset: u64, output: []u8) !void {
        const self: *Admitted = @ptrCast(@alignCast(raw));
        try self.source.readInto(offset, output);
    }
    fn check(raw: *anyopaque) !void {
        const self: *Admitted = @ptrCast(@alignCast(raw));
        try self.source.checkReadContext();
    }
    fn close(raw: *anyopaque) void {
        const self: *Admitted = @ptrCast(@alignCast(raw));
        self.metadata.deinit(self.allocator);
        self.source.close();
        self.allocator.destroy(self);
    }
    fn retained(raw: *anyopaque) usize {
        const self: *Admitted = @ptrCast(@alignCast(raw));
        var bytes = @sizeOf(Admitted) +| self.source.retainedBytes() +| self.metadata.fields.len * @sizeOf(segment.SegmentReader.Admission.Field);
        for (self.metadata.fields) |field| bytes +|= field.name.len +| field.sections.len * @sizeOf(segment.SegmentReader.Admission.Section);
        return bytes;
    }
};
fn authenticatedSeal() void {}

/// Native, ownership-bearing decoder input. It cannot be constructed from a
/// serialized descriptor or a copied generation token. Closing source releases
/// metadata, actual field pins and every opened pack together.
pub const Authenticated = struct {
    source: Source,
    metadata: *const segment.SegmentReader.Admission,
    generation: publication.Binding,
    seal: *const fn () void,
    pub fn require(self: Authenticated) !void {
        if (self.seal != authenticatedSeal or self.source != .ranges or self.source.ranges.read_into != Admitted.read) return error.IndexPublicationFenceMismatch;
        const owner: *const Admitted = @ptrCast(@alignCast(self.source.ranges.ptr));
        if (self.metadata != &owner.metadata or self.source.len() != owner.metadata.bytes or !std.meta.eql(self.generation, owner.handle.generation)) return error.IndexPublicationFenceMismatch;
        try owner.source.checkReadContext();
    }
    pub fn inventory(self: Authenticated) !Inventory {
        try self.require();
        const owner: *const Admitted = @ptrCast(@alignCast(self.source.ranges.ptr));
        Context.retain(owner.context);
        return .{ .context = owner.context };
    }
    pub fn resourceCount(self: Authenticated) !usize {
        try self.require();
        const owner: *const Admitted = @ptrCast(@alignCast(self.source.ranges.ptr));
        return owner.context.resources.len;
    }
    /// Exact physical closure for checkpoint/transfer. The returned source
    /// retains the captured version, including a pack not previously decoded.
    /// It never resolves the logical family ID or a current file pathname.
    pub fn resource(self: Authenticated, a: A, ordinal: usize) !PhysicalResource {
        try self.require();
        const owner: *const Admitted = @ptrCast(@alignCast(self.source.ranges.ptr));
        if (ordinal >= owner.context.resources.len) return error.InvalidIndexFieldResource;
        const cell = &owner.context.resources[ordinal];
        const proxy = try a.create(PhysicalProxy);
        Context.retain(owner.context);
        proxy.* = .{ .allocator = a, .context = owner.context, .ordinal = ordinal };
        return .{ .descriptor = cell.resource, .source = .{ .ranges = .{ .ptr = proxy, .length = cell.resource.physical_bytes, .read_into = PhysicalProxy.read, .close = PhysicalProxy.close, .check_read_context = PhysicalProxy.check, .retained_bytes = PhysicalProxy.retained } } };
    }
    pub fn initReader(self: Authenticated, a: A) !segment.SegmentReader {
        try self.require();
        return segment.SegmentReader.initAuthenticated(a, self.source, self.metadata.*);
    }
    pub fn deinit(self: *Authenticated) void {
        // The handle may be embedded in the owner that close destroys. Clear
        // it before invoking that callback, with no write after destruction.
        var source = self.source;
        self.source = .{ .contiguous = &.{} };
        source.close();
    }
    pub fn nativeHandle(self: Authenticated) !*Authenticated {
        try self.require();
        const owner: *Admitted = @ptrCast(@alignCast(self.source.ranges.ptr));
        return &owner.handle;
    }
    pub fn jsonStringify(_: Authenticated, _: anytype) error{WriteFailed}!void {
        return error.WriteFailed;
    }
    pub fn jsonParse(_: A, _: anytype, _: std.json.ParseOptions) error{UnexpectedToken}!Authenticated {
        return error.UnexpectedToken;
    }
};

fn extent(context: *const Context, capture: Field, part: fields.Section) composite.Extent {
    const resource = context.resources[capture.resources[part.resource]].resource;
    return .{ .resource = .{ .identity = resource.identity, .source_bytes = resource.payload_bytes, .offset = part.offset, .length = part.length } };
}

/// Compose only live immutable sections and stored identity metadata. This
/// logical address space is consumed by initAuthenticated, never parsed as a
/// legacy serialized segment or copied as a standalone checkpoint file.
pub fn authenticated(inventory: Inventory, a: A) !Authenticated {
    const context = inventory.context orelse return error.IndexSourceLeaseClosed;
    try Context.check(context);
    const stored = context.directory.stored orelse return error.InvalidIndexFieldDirectory;
    const stored_field = context.stored orelse return error.InvalidIndexFieldDirectory;
    if (context.fields.len > std.math.maxInt(u16)) return error.SegmentMetadataTooLarge;
    var extent_count: usize = 2;
    var name_bytes: usize = 0;
    for (context.fields) |capture| {
        const descriptor = try context.descriptor(capture);
        extent_count = try std.math.add(usize, extent_count, descriptor.sectionCount());
        name_bytes = try std.math.add(usize, name_bytes, descriptor.name().len);
    }
    // Reserve for transient extent/descriptor growth, native navigation and
    // copied admission. The producer falls back before publishing on pressure.
    const planned = try std.math.add(usize, try std.math.mul(usize, extent_count, 256), try std.math.add(usize, name_bytes, try std.math.mul(usize, context.fields.len, @sizeOf(segment.SegmentReader.Admission.Field))));
    if (planned > context.limits.metadata_bytes) return error.ResourceBudgetExceeded;
    const extents = try a.alloc(composite.Extent, extent_count);
    defer a.free(extents);
    const admitted_fields = try a.alloc(segment.SegmentReader.Admission.Field, context.fields.len);
    var initialized: usize = 0;
    var owns_metadata = true;
    defer if (owns_metadata) {
        for (admitted_fields[0..initialized]) |field| {
            a.free(field.name);
            a.free(field.sections);
        }
        a.free(admitted_fields);
    };
    const inventory_descriptor = try context.descriptor(stored_field);
    extents[0] = extent(context, stored_field, try inventory_descriptor.section(0));
    var extent_index: usize = 1;
    var position: u64 = stored.bytes;
    for (context.fields, admitted_fields) |capture, *field| {
        const descriptor = try context.descriptor(capture);
        const name = try a.dupe(u8, descriptor.name());
        errdefer a.free(name);
        const sections = try a.alloc(segment.SegmentReader.Admission.Section, descriptor.sectionCount());
        errdefer a.free(sections);
        for (sections, 0..) |*section, index| {
            const part = try descriptor.section(index);
            section.* = .{ .kind = part.kind, .offset = position, .length = part.length, .checksum = part.checksum };
            position = try std.math.add(u64, position, part.length);
            extents[extent_index] = extent(context, capture, part);
            extent_index += 1;
        }
        field.* = .{ .name = name, .stats = descriptor.stats(), .sections = sections };
        initialized += 1;
    }
    var footer: [56]u8 = @splat(0);
    extents[extent_index] = .{ .inline_bytes = &footer };
    const metadata: segment.SegmentReader.Admission = .{
        .bytes = try std.math.add(u64, position, footer.len),
        .stored_offset = 0,
        .stored_length = stored.bytes,
        .stored_metadata_length = stored.metadata_bytes,
        .stored_header = stored.header,
        .index_offset = position,
        .doc_count = context.directory.documents,
        .fields = admitted_fields,
    };
    try metadata.validate();
    const limits: composite.Limits = .{ .metadata_bytes = context.limits.metadata_bytes, .extents = extent_count, .resources = context.limits.resources };
    const raw = try composite.encode(a, extents, limits);
    defer a.free(raw);
    var source = try composite.open(a, raw, context.provider(), limits);
    errdefer source.close();
    const owner = try a.create(Admitted);
    owner.* = .{ .allocator = a, .context = context, .source = source, .metadata = metadata, .handle = undefined };
    owns_metadata = false;
    owner.handle = .{
        .source = .{ .ranges = .{ .ptr = owner, .length = metadata.bytes, .read_into = Admitted.read, .close = Admitted.close, .check_read_context = Admitted.check, .retained_bytes = Admitted.retained } },
        .metadata = &owner.metadata,
        .generation = context.binding,
        .seal = authenticatedSeal,
    };
    return owner.handle;
}
