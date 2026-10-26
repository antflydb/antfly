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

//! Prepare immutable field sections from one retained native decoder. This is
//! used for initial admission/flattening; incremental producers reuse actual
//! held field nodes and encode only fields whose compiled inputs changed.
const std = @import("std");
const fields = @import("index_field_generation.zig");
const publication = @import("index_publication.zig");
const graph = @import("immutable_graph.zig");
const segment = @import("../segment.zig");
const pack = @import("../field_pack.zig");
const Source = @import("../segment_source.zig").Source;
const A = std.mem.Allocator;
const Digest = [32]u8;
const Reused = struct { ordinal: u32, node: Digest };
pub const Record = struct { inputs: Digest, sections: []fields.Section, reused: ?Reused = null, ledger: ?[]u8 = null };
pub const Plan = struct {
    allocator: A,
    resource: fields.Resource,
    admission: segment.SegmentReader.Admission,
    records: []Record,
    stored: fields.Section,
    stored_inputs: Digest,
    ordinal_inventory: Digest,
    configuration: Digest,
    reuse_scope: ?publication.SharedReaders.Lease = null,
    pub fn deinit(self: *Plan) void {
        for (self.records) |record| {
            self.allocator.free(record.sections);
            if (record.ledger) |raw| self.allocator.free(raw);
        }
        self.allocator.free(self.records);
        self.admission.deinit(self.allocator);
        if (self.reuse_scope) |*scope| scope.deinit();
        self.* = undefined;
    }
    pub fn jsonStringify(_: Plan, _: anytype) error{WriteFailed}!void {
        return error.WriteFailed;
    }
    pub fn jsonParse(_: A, _: anytype, _: std.json.ParseOptions) error{UnexpectedToken}!Plan {
        return error.UnexpectedToken;
    }

    /// The IO coordinator must force the prepared pack before entering this
    /// transaction. Fields, stored identities, independent field receipt and
    /// the logical family CAS are published together, never as partial upserts.
    pub fn stage(self: *const Plan, writer: anytype, binding: publication.Binding, limits: graph.Limits) !Digest {
        errdefer writer.poison();
        const resource = try fields.stageResource(writer, binding.owner, self.resource, limits);
        const stored = try fields.stageField(writer, binding.owner, .{
            .name = fields.stored_inventory_field,
            .documents = self.admission.doc_count,
            .ordinal_inventory = self.ordinal_inventory,
            .configuration = self.configuration,
            .inputs = self.stored_inputs,
            .sections = &.{self.stored},
        }, &.{resource}, limits);
        var builder = try fields.DirectoryBuilder.initWithStored(binding.owner, .{
            .fields = @intCast(self.admission.fields.len),
            .documents = self.admission.doc_count,
            .ordinal_inventory = self.ordinal_inventory,
            .configuration = self.configuration,
            .publication_id = binding.publication,
            .stored = .{ .bytes = self.admission.stored_length, .metadata_bytes = self.admission.stored_metadata_length, .header = self.admission.stored_header },
        }, stored);
        for (self.admission.fields, self.records) |field, record| {
            if (record.reused) |prior| {
                const scope = if (self.reuse_scope) |*value| value else return error.IndexPublicationFenceMismatch;
                const held = try graph.openHeldFields(writer, scope);
                const selected = try fields.selectFields(writer, &held, prior.ordinal);
                const descriptor = try selected.field.descriptor();
                if (!std.mem.eql(u8, &selected.field.node.id, &prior.node) or
                    !compatible(descriptor, field.name, record.inputs, self.admission.doc_count, self.ordinal_inventory, self.configuration) or
                    !std.mem.eql(u8, &held.binding.owner, &binding.owner)) return error.IndexPublicationFenceMismatch;
                if (record.ledger) |raw| try @import("index_field_ledger.zig").stage(writer, binding.owner, raw, record.inputs);
                try builder.append(writer, selected.field.node.id, limits);
                continue;
            }
            const next = try fields.stageField(writer, binding.owner, .{
                .name = field.name,
                .documents = self.admission.doc_count,
                .ordinal_inventory = self.ordinal_inventory,
                .configuration = self.configuration,
                .inputs = record.inputs,
                .stats = field.stats,
                .sections = record.sections,
            }, &.{resource}, limits);
            if (record.ledger) |raw| try @import("index_field_ledger.zig").stage(writer, binding.owner, raw, record.inputs);
            try builder.append(writer, next, limits);
        }
        const root = try builder.finish(writer, limits);
        try fields.stagePublication(self.allocator, writer, binding, root, limits);
        return root;
    }
};

/// Input calculation belongs to the native compiler under its pinned schema
/// and source view. The calculator must bind the complete actual dependency set;
/// an opaque/non-deterministic consumer must not supply an equality shortcut.
/// This function performs no publication and grants no reads from digests.
pub fn prepare(a: A, reader: *const segment.SegmentReader, encoder: *pack.Encoder, file: u64, ordinal_inventory: Digest, configuration: Digest, inputs: anytype) !Plan {
    const NoReuse = struct {
        pub fn find(_: *@This(), _: []const u8, _: Digest) !?Reused {
            return null;
        }
        pub fn cloneScope(_: *@This()) !?publication.SharedReaders.Lease {
            return null;
        }
    };
    var none: NoReuse = .{};
    return prepareImpl(a, reader, encoder, file, ordinal_inventory, configuration, inputs, &none, true);
}

/// Same-owner incremental preparation. Comparison is against the actual held
/// field publication, never a caller-supplied node ID. Only changed sections
/// enter the new pack; the plan retains its own reader lease until consumed.
/// Compiler inputs must include every analyzer/projection dependency. Unknown
/// dependencies must force a changed input or use the full prepare route.
pub fn prepareIncremental(a: A, reader: *const segment.SegmentReader, encoder: *pack.Encoder, file: u64, ordinal_inventory: Digest, configuration: Digest, inputs: anytype, metadata: anytype, lease: *const publication.SharedReaders.Lease) !Plan {
    const Reuse = struct {
        metadata: @TypeOf(metadata),
        lease: *const publication.SharedReaders.Lease,
        documents: u32,
        ordinal_inventory: Digest,
        configuration: Digest,
        pub fn cloneScope(self: *@This()) !?publication.SharedReaders.Lease {
            return try self.lease.clone();
        }
        pub fn find(self: *@This(), name: []const u8, input: Digest) !?Reused {
            if (name.len != 0 and name[0] == 0) return null;
            const root = try graph.openHeldFields(self.metadata, self.lease);
            const directory = try fields.describeFields(&root);
            if (directory.documents != self.documents or !std.mem.eql(u8, &directory.ordinal_inventory, &self.ordinal_inventory) or !std.mem.eql(u8, &directory.configuration, &self.configuration)) return null;
            var lower: u32 = 0;
            var upper = directory.fields;
            while (lower < upper) {
                const middle = lower + (upper - lower) / 2;
                const selected = try fields.selectFields(self.metadata, &root, middle);
                const descriptor = try selected.field.descriptor();
                switch (std.mem.order(u8, descriptor.name(), name)) {
                    .lt => lower = middle + 1,
                    .gt => upper = middle,
                    .eq => return if (compatible(descriptor, name, input, self.documents, self.ordinal_inventory, self.configuration)) .{ .ordinal = middle, .node = selected.field.node.id } else null,
                }
            }
            return null;
        }
    };
    var reuse: Reuse = .{ .metadata = metadata, .lease = lease, .documents = reader.doc_count, .ordinal_inventory = ordinal_inventory, .configuration = configuration };
    return prepareImpl(a, reader, encoder, file, ordinal_inventory, configuration, inputs, &reuse, true);
}

/// Extend a changed-fields-only artifact with unchanged fields selected from
/// the actual held publication. Raw-input equality is checked again here; a
/// skipped name, copied digest or mutable planner list is never sufficient.
pub fn prepareProjected(a: A, reader: *const segment.SegmentReader, encoder: *pack.Encoder, file: u64, ordinal_inventory: Digest, configuration: Digest, inputs: anytype, metadata: anytype, lease: *const publication.SharedReaders.Lease, skipped: []const []const u8, copy_inventory: ?*const @import("index_field_source.zig").Inventory, remap_rows: ?[]const u32, remap_evidence: ?[]const []u8) !Plan {
    // Only the explicitly admitted skipped fields retain old resources. Copy
    // freshly compiled internal sections (such as canonical ordinals) so they
    // cannot introduce an unaccounted extra pack beyond the admission bound.
    const None = struct {
        pub fn find(_: *@This(), _: []const u8, _: Digest) !?Reused {
            return null;
        }
        pub fn cloneScope(_: *@This()) !?publication.SharedReaders.Lease {
            return null;
        }
    };
    var none: None = .{};
    var plan = try prepareImpl(a, reader, encoder, file, ordinal_inventory, configuration, inputs, &none, false);
    errdefer plan.deinit();
    if (copy_inventory == null) plan.reuse_scope = try lease.clone();
    const total = try std.math.add(usize, plan.records.len, skipped.len);
    if (total > fields.maximum_fields or total > std.math.maxInt(u16)) return error.ResourceBudgetExceeded;
    const combined_fields = try a.alloc(segment.SegmentReader.Admission.Field, total);
    errdefer a.free(combined_fields);
    const combined_records = try a.alloc(Record, total);
    errdefer a.free(combined_records);
    const base = plan.records.len;
    @memcpy(combined_fields[0..base], plan.admission.fields);
    @memcpy(combined_records[0..base], plan.records);
    var initialized: usize = 0;
    errdefer {
        for (combined_fields[base..][0..initialized], combined_records[base..][0..initialized]) |field, record| {
            a.free(field.name);
            a.free(record.sections);
            if (record.ledger) |raw| a.free(raw);
        }
    }
    const root = try graph.openHeldFields(metadata, lease);
    const directory = try fields.describeFields(&root);
    // One decoder and one identity pass per remap, rather than rebuilding
    // all field navigation and rereading row IDs for every selected field.
    const source_module = @import("index_field_source.zig");
    var authenticated: ?source_module.Authenticated = null;
    defer if (authenticated) |*held| held.deinit();
    var old_reader: ?segment.SegmentReader = null;
    defer if (old_reader) |*held| held.deinit();
    if (remap_rows) |rows| {
        const inventory = copy_inventory orelse return error.IndexPublicationFenceMismatch;
        if (rows.len != reader.doc_count) return error.IndexPublicationFenceMismatch;
        authenticated = try source_module.authenticated(inventory.*, a);
        old_reader = try authenticated.?.initReader(a);
        for (rows, 0..) |old, current| {
            const old_id = (try old_reader.?.storedIdAlloc(a, old)) orelse return error.IndexPublicationFenceMismatch;
            defer a.free(old_id);
            if (!std.mem.eql(u8, old_id, try documentId(inputs, current))) return error.IndexPublicationFenceMismatch;
        }
    }
    for (skipped, 0..) |name, index| {
        if (index != 0 and std.mem.order(u8, skipped[index - 1], name) != .lt) return error.InvalidIndexFieldGeneration;
        for (reader.fields) |compiled| if (std.mem.eql(u8, compiled.name, name)) return error.InvalidIndexFieldGeneration;
        var low: u32 = 0;
        var high = directory.fields;
        var selected_ordinal: u32 = 0;
        const selected = found: while (low < high) {
            const middle = low + (high - low) / 2;
            const field = try fields.selectFields(metadata, &root, middle);
            const descriptor = try field.field.descriptor();
            switch (std.mem.order(u8, descriptor.name(), name)) {
                .lt => low = middle + 1,
                .gt => high = middle,
                .eq => {
                    selected_ordinal = middle;
                    break :found field;
                },
            }
        } else return error.IndexPublicationFenceMismatch;
        const descriptor = try selected.field.descriptor();
        const input = try inputs.inputForField(name);
        if (remap_rows) |rows| {
            if (rows.len != reader.doc_count or copy_inventory == null or !std.mem.eql(u8, &descriptor.configuration(), &configuration)) return error.IndexPublicationFenceMismatch;
            const retained = remap_evidence orelse return error.IndexPublicationFenceMismatch;
            if (retained.len != skipped.len) return error.IndexPublicationFenceMismatch;
            const evidence = retained[index];
            const proof = try @import("index_field_ledger.zig").open(evidence, descriptor.inputs());
            if (proof.documents() != descriptor.documents()) return error.IndexPublicationFenceMismatch;
            for (rows, 0..) |old, current| {
                const before = (try proof.row(old)) orelse return error.IndexPublicationFenceMismatch;
                const after = (try ledgerRow(inputs, name, current)) orelse return error.IndexPublicationFenceMismatch;
                if (!std.mem.eql(u8, &before, &after)) return error.IndexPublicationFenceMismatch;
            }
        } else if (!compatible(descriptor, name, input, reader.doc_count, ordinal_inventory, configuration)) return error.IndexPublicationFenceMismatch;
        const owned_name = try a.dupe(u8, name);
        errdefer a.free(owned_name);
        const copied_sections: []fields.Section = if (copy_inventory != null) try a.alloc(fields.Section, descriptor.sectionCount()) else &.{};
        errdefer a.free(copied_sections);
        var output_stats = descriptor.stats();
        if (copy_inventory) |inventory| {
            const captured = try inventory.field(selected_ordinal);
            if (!std.mem.eql(u8, captured.raw, descriptor.raw)) return error.IndexPublicationFenceMismatch;
            if (remap_rows) |rows| {
                var remapped = try segment.remapFieldAlloc(a, &old_reader.?, name, rows);
                defer remapped.deinit(a);
                if (remapped.sections.len != copied_sections.len) return error.IndexPublicationFenceMismatch;
                const start = encoder.payload_bytes;
                try encoder.append(remapped.bytes);
                for (remapped.sections, copied_sections) |part, *section| section.* = .{ .kind = part.kind, .resource = 0, .offset = start + part.offset, .length = part.length, .checksum = part.checksum };
                output_stats = remapped.stats;
            } else for (copied_sections, 0..) |*section, section_index| {
                const original = try descriptor.section(section_index);
                var source = (try inventory.section(a, name, original.kind)) orelse return error.InvalidIndexFieldGeneration;
                defer source.close();
                if (source.len() != original.length) return error.InvalidIndexFieldGeneration;
                section.* = .{ .kind = original.kind, .resource = 0, .offset = encoder.payload_bytes, .length = original.length, .checksum = original.checksum };
                try copyRange(source, 0, original.length, encoder, null);
            }
        }
        combined_fields[base + index] = .{ .name = owned_name, .sections = &.{}, .stats = output_stats };
        const evidence = try inputLedger(a, inputs, name, true);
        combined_records[base + index] = .{ .inputs = input, .sections = copied_sections, .reused = if (copy_inventory == null) .{ .ordinal = selected_ordinal, .node = selected.field.node.id } else null, .ledger = evidence };
        initialized += 1;
    }
    plan.resource = try finishResource(encoder, file);
    // Sort a permutation so section/name allocations and proof records move
    // together, and only transfer the original plan's allocations on success.
    const Pair = struct { field: segment.SegmentReader.Admission.Field, record: Record };
    const pairs = try a.alloc(Pair, total);
    defer a.free(pairs);
    for (pairs, combined_fields, combined_records) |*pair, field, record| pair.* = .{ .field = field, .record = record };
    std.mem.sort(Pair, pairs, {}, struct {
        fn less(_: void, left: Pair, right: Pair) bool {
            return std.mem.order(u8, left.field.name, right.field.name) == .lt;
        }
    }.less);
    for (pairs, combined_fields, combined_records) |pair, *field, *record| {
        field.* = pair.field;
        record.* = pair.record;
    }
    a.free(plan.admission.fields);
    a.free(plan.records);
    plan.admission.fields = combined_fields;
    plan.records = combined_records;
    return plan;
}

fn compatible(descriptor: fields.Descriptor, name: []const u8, input: Digest, documents: u32, ordinal_inventory: Digest, configuration: Digest) bool {
    return descriptor.documents() == documents and std.mem.eql(u8, descriptor.name(), name) and
        std.mem.eql(u8, &descriptor.inputs(), &input) and std.mem.eql(u8, &descriptor.ordinalInventory(), &ordinal_inventory) and std.mem.eql(u8, &descriptor.configuration(), &configuration);
}

fn prepareImpl(a: A, reader: *const segment.SegmentReader, encoder: *pack.Encoder, file: u64, ordinal_inventory: Digest, configuration: Digest, inputs: anytype, reuse: anytype, comptime finish_pack: bool) !Plan {
    var scope = try reuse.cloneScope();
    var scope_owned = true;
    defer if (scope_owned) if (scope) |*held| held.deinit();
    const admission = try reader.admission(a);
    var admission_owned = true;
    defer if (admission_owned) admission.deinit(a);
    if (admission.doc_count == 0 or admission.fields.len > std.math.maxInt(u16) or file == 0 or std.mem.allEqual(u8, &ordinal_inventory, 0) or std.mem.allEqual(u8, &configuration, 0)) return error.InvalidIndexFieldGeneration;
    const records = try a.alloc(Record, admission.fields.len);
    var initialized: usize = 0;
    var records_owned = true;
    defer if (records_owned) {
        for (records[0..initialized]) |record| {
            a.free(record.sections);
            if (record.ledger) |raw| a.free(raw);
        }
        a.free(records);
    };
    var stored_hash = std.crypto.hash.sha2.Sha256.init(.{});
    stored_hash.update("antfly/stored-field-inventory/v1\x00");
    stored_hash.update(&ordinal_inventory);
    const stored_offset = encoder.payload_bytes;
    try copyRange(reader.source(), admission.stored_offset, admission.stored_length, encoder, &stored_hash);
    for (admission.fields, records) |field, *record| {
        const input = try inputs.inputForField(field.name);
        if (try reuse.find(field.name, input)) |prior| {
            record.* = .{ .inputs = input, .sections = &.{}, .reused = prior, .ledger = try inputLedger(a, inputs, field.name, true) };
            initialized += 1;
            continue;
        }
        const sections = try a.alloc(fields.Section, field.sections.len);
        errdefer a.free(sections);
        for (field.sections, sections) |section, *output| {
            output.* = .{ .kind = section.kind, .resource = 0, .offset = encoder.payload_bytes, .length = section.length, .checksum = section.checksum };
            try copyRange(reader.source(), section.offset, section.length, encoder, null);
        }
        const evidence = try inputLedger(a, inputs, field.name, false);
        record.* = .{ .inputs = input, .sections = sections, .ledger = evidence };
        initialized += 1;
    }
    const resource: fields.Resource = if (finish_pack) try finishResource(encoder, file) else undefined;
    admission_owned = false;
    records_owned = false;
    scope_owned = false;
    return .{
        .allocator = a,
        .resource = resource,
        .admission = admission,
        .records = records,
        .stored = .{ .kind = .columnar_stored, .resource = 0, .offset = stored_offset, .length = admission.stored_length, .checksum = 0 },
        .stored_inputs = stored_hash.finalResult(),
        .ordinal_inventory = ordinal_inventory,
        .configuration = configuration,
        .reuse_scope = scope,
    };
}

/// Adopt a forced native pack written by the merge encoder itself. The
/// temporary flat directory is only a decoder preview, never persisted data.
pub fn adopt(a: A, reader: *const segment.SegmentReader, resource: fields.Resource, ordinal: Digest, configuration: Digest, inputs: anytype) !Plan {
    _ = try resource.encode();
    const admission = try reader.admission(a);
    errdefer admission.deinit(a);
    const records = try a.alloc(Record, admission.fields.len);
    var initialized: usize = 0;
    errdefer {
        for (records[0..initialized]) |record| {
            a.free(record.sections);
            if (record.ledger) |raw| a.free(raw);
        }
        a.free(records);
    }
    if (admission.stored_offset > resource.payload_bytes or admission.stored_length > resource.payload_bytes - admission.stored_offset) return error.InvalidIndexFieldGeneration;
    for (admission.fields, records) |field, *record| {
        const input = try inputs.inputForField(field.name);
        const sections = try a.alloc(fields.Section, field.sections.len);
        errdefer a.free(sections);
        for (field.sections, sections) |section, *out| {
            if (section.offset > resource.payload_bytes or section.length > resource.payload_bytes - section.offset) return error.InvalidIndexFieldGeneration;
            out.* = .{ .kind = section.kind, .resource = 0, .offset = section.offset, .length = section.length, .checksum = section.checksum };
        }
        const evidence = try inputLedger(a, inputs, field.name, false);
        record.* = .{ .inputs = input, .sections = sections, .ledger = evidence };
        initialized += 1;
    }
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/stored-field-inventory/v1\x00");
    hash.update(&ordinal);
    var bytes: [64 * 1024]u8 = undefined;
    var copied: u64 = 0;
    while (copied < admission.stored_length) {
        const take: usize = @intCast(@min(bytes.len, admission.stored_length - copied));
        try reader.source().readInto(admission.stored_offset + copied, bytes[0..take]);
        hash.update(bytes[0..take]);
        copied += take;
    }
    return .{ .allocator = a, .resource = resource, .admission = admission, .records = records, .stored = .{ .kind = .columnar_stored, .resource = 0, .offset = admission.stored_offset, .length = admission.stored_length, .checksum = 0 }, .stored_inputs = hash.finalResult(), .ordinal_inventory = ordinal, .configuration = configuration };
}

fn inputLedger(a: A, inputs: anytype, name: []const u8, unchanged: bool) !?[]u8 {
    const T = switch (@typeInfo(@TypeOf(inputs))) {
        .pointer => |ptr| ptr.child,
        else => @TypeOf(inputs),
    };
    if (comptime @hasDecl(T, "persistLedger")) if (!inputs.persistLedger(unchanged)) return null;
    if (comptime @hasDecl(T, "ledgerForField")) return inputs.ledgerForField(a, name);
    return null;
}

fn finishResource(encoder: *pack.Encoder, file: u64) !fields.Resource {
    const metadata = try encoder.finish();
    const resource: fields.Resource = .{ .file = file, .payload_bytes = metadata.payload_bytes, .physical_bytes = encoder.sink.len(), .identity = metadata.identity };
    _ = try resource.encode();
    return resource;
}

fn copyRange(source: Source, offset: u64, length: u64, encoder: *pack.Encoder, hash: ?*std.crypto.hash.sha2.Sha256) !void {
    if (offset > source.len() or length > source.len() - offset) return error.InvalidIndexFieldGeneration;
    var scratch: [64 * 1024]u8 = undefined;
    var copied: u64 = 0;
    while (copied < length) {
        const take: usize = @intCast(@min(length - copied, scratch.len));
        try source.readInto(offset + copied, scratch[0..take]);
        if (hash) |state| state.update(scratch[0..take]);
        try encoder.append(scratch[0..take]);
        copied += take;
    }
}

fn ledgerRow(inputs: anytype, name: []const u8, ordinal: usize) !?Digest {
    const T = @typeInfo(@TypeOf(inputs)).pointer.child;
    if (comptime @hasDecl(T, "ledgerRow")) return inputs.ledgerRow(name, ordinal);
    return error.IndexPublicationFenceMismatch;
}
fn documentId(inputs: anytype, ordinal: usize) ![]const u8 {
    const T = @typeInfo(@TypeOf(inputs)).pointer.child;
    if (comptime @hasDecl(T, "documentId")) return inputs.documentId(ordinal);
    return error.IndexPublicationFenceMismatch;
}
