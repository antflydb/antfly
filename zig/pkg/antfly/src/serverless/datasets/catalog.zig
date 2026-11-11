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

//! Durable dataset metadata on the existing conditional authority and ordered
//! pages. A segment has one authoritative placement; migration never adds rows.
const std = @import("std");
const objects = @import("objectstore");
const coordinator = objects.transactional_store;
const t = @import("types.zig");
const closure = @import("closure_manifest.zig");
const local = @import("antfly_local_sources");
const series = local.storage_retained_series;
const codec = local.storage_retained_series_archive;
const A = std.mem.Allocator;
pub const max_page = 128;
pub const max_segment_bytes = 16 * 1024 * 1024;
pub const pin_duration_ns = 10 * std.time.ns_per_min;
pub const pin_grace_ns = 30 * std.time.ns_per_s;
const Header = struct { state: objects.immutable_log.State, root_cursor: ?objects.conditional_root.Cursor = null };
const Row = struct { key: []const u8, value: []const u8 };
pub const Read = struct { results: []const []const Row };
const RetirementFloor = struct { through_ns: []const u8, through_key: ?t.RowKey = null };
const Coverage = struct {
    segment: t.Id,
    start_ns: []const u8,
    end_ns: []const u8,
    key_span: ?t.KeySpan = null,
    fn span(self: Coverage) !t.KeySpan {
        const result = self.key_span orelse try t.KeySpan.fromRange(.{ .start_ns = self.start_ns, .end_ns = self.end_ns });
        try result.validate();
        return result;
    }
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try t.writeFields(self, .{}, writer);
    }
};
const expiry_prefix = "archive_expiry/";
pub const ExpiryCursor = [expiry_prefix.len + 32 + 1 + 64]u8;
pub const Expiry = struct { cursor: ExpiryCursor, segment: t.Id };

fn expiryKey(segment: t.Segment, retention_ns: u64) !?ExpiryCursor {
    const expires = std.math.add(i128, try segment.coverage.end(), retention_ns) catch return null;
    const biased = @as(u128, @bitCast(expires)) ^ (@as(u128, 1) << 127);
    var key: ExpiryCursor = undefined;
    const rendered = try std.fmt.bufPrint(&key, "{s}{x:0>32}/{s}", .{ expiry_prefix, biased, std.fmt.bytesToHex(segment.id, .lower) });
    if (rendered.len != key.len) return error.InvalidDatasetCoverage;
    return key;
}

pub const RangePlan = struct {
    arena: std.heap.ArenaAllocator,
    segments: []const t.Segment,
    pub fn deinit(self: *RangePlan) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Locations are host capabilities. Persist only their opaque identity, never
/// credentials or a user-supplied URL. The host retains their bindings while
/// catalog records, reader pins or reclamation jobs refer to them.
pub const Location = struct {
    id: t.Id,
    /// Stable fingerprint supplied by the host for the physical store binding.
    storage_identity: t.Id,
    client: objects.Client,
    bucket: []const u8,
    prefix: []const u8,
    access: t.Access,
};

pub const Dependency = struct {
    id: t.Id,
    consumer: t.Id,
    consumer_generation: u64,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try t.writeFields(self, .{}, writer);
    }
};

pub const Pin = struct {
    id: t.Id,
    segment: t.Id,
    location: t.Id,
    placement_revision: u64,
    expires_ns: u64,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try t.writeFields(self, .{}, writer);
    }
};

pub const Clock = struct {
    ptr: ?*const anyopaque = null,
    unix_fn: ?*const fn (*const anyopaque) u64 = null,
    authority_fn: ?*const fn (*const anyopaque) u64 = null,
    pub fn unix(self: Clock) u64 {
        return if (self.unix_fn) |f| f(self.ptr.?) else @import("antfly_platform").time.realtimeNs();
    }
    fn authority(self: Clock) u64 {
        return if (self.authority_fn) |f| f(self.ptr.?) else @import("antfly_platform").time.authorityNs();
    }
};

pub const Lease = struct {
    pin: Pin,
    authority_deadline: u64,
};

pub const Catalog = struct {
    context: coordinator.Context,
    dataset: t.Id,
    owner: [16]u8,
    clock: Clock = .{},

    fn open(self: Catalog, write: bool) !std.json.Parsed(Header) {
        const expected = std.fmt.bytesToHex(self.dataset, .lower);
        if (!std.mem.eql(u8, self.context.namespace, &expected) or t.zero(self.dataset) or std.mem.allEqual(u8, &self.owner, 0)) return error.InvalidDatasetBinding;
        const raw = try self.context.execute(.{ .root_backend = .conditional, .action = if (write) .begin else .observe_committed, .owner = self.owner });
        defer self.context.alloc.free(raw);
        return std.json.parseFromSlice(Header, self.context.alloc, raw, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
    }

    fn commit(self: Catalog, session: Header, commands: []const coordinator.Command) !void {
        const raw = try self.context.execute(.{ .root_backend = .conditional, .action = .commit, .owner = self.owner, .state = session.state, .root_cursor = session.root_cursor, .commands = commands });
        self.context.alloc.free(raw);
    }

    fn get(self: Catalog, session: Header, comptime T: type, key: []const u8) !?std.json.Parsed(T) {
        const values = try self.context.readSnapshotValuesAlloc(.conditional, session.state, &.{key}, .{});
        defer {
            for (values) |value| if (value) |bytes| self.context.alloc.free(bytes);
            self.context.alloc.free(values);
        }
        const raw = values[0] orelse return null;
        return try std.json.parseFromSlice(T, self.context.alloc, raw, .{ .allocate = .alloc_always });
    }

    fn definition(self: Catalog, session: Header) !std.json.Parsed(t.Definition) {
        const value = (try self.get(session, t.Definition, "definition")) orelse return error.DatasetNotFound;
        errdefer value.deinit();
        try value.value.validate();
        if (!std.mem.eql(u8, &value.value.id, &self.dataset)) return error.InvalidDatasetBinding;
        return value;
    }

    fn segment(self: Catalog, session: Header, id: t.Id) !std.json.Parsed(t.Segment) {
        const key = try segmentKey(self.context.alloc, id);
        defer self.context.alloc.free(key);
        const result = (try self.get(session, t.Segment, key)) orelse return error.DatasetSegmentNotFound;
        errdefer result.deinit();
        const definition_key = try std.fmt.allocPrint(self.context.alloc, "definitions/{d}", .{result.value.definition_generation});
        defer self.context.alloc.free(definition_key);
        const def = (try self.get(session, t.Definition, definition_key)) orelse return error.InvalidDatasetDefinition;
        defer def.deinit();
        try result.value.validate(def.value);
        if (!std.mem.eql(u8, &result.value.id, &id)) return error.InvalidDatasetSegment;
        return result;
    }

    fn checkLocation(self: Catalog, session: Header, location: Location, allow_new: bool) !bool {
        if (t.zero(location.id) or t.zero(location.storage_identity)) return error.InvalidDatasetBinding;
        const key = try locationKey(self.context.alloc, location.id);
        defer self.context.alloc.free(key);
        const Binding = struct { storage_identity: t.Id, access: t.Access };
        const existing = (try self.get(session, Binding, key)) orelse {
            if (!allow_new) return error.DatasetLocationUnavailable;
            return false;
        };
        defer existing.deinit();
        if (!std.mem.eql(u8, &existing.value.storage_identity, &location.storage_identity) or existing.value.access != location.access) return error.DatasetLocationConflict;
        return true;
    }

    pub fn create(self: Catalog, value: t.Definition) !void {
        try value.validate();
        if (!std.mem.eql(u8, &value.id, &self.dataset)) return error.InvalidDatasetBinding;
        var session = try self.open(true);
        defer session.deinit();
        if (try self.get(session.value, t.Definition, "definition")) |existing| {
            existing.deinit();
            return error.DatasetAlreadyExists;
        }
        const raw = try std.json.Stringify.valueAlloc(self.context.alloc, value, .{});
        defer self.context.alloc.free(raw);
        const generation_key = try std.fmt.allocPrint(self.context.alloc, "definitions/{d}", .{value.generation});
        defer self.context.alloc.free(generation_key);
        try self.commit(session.value, &.{ .{ .kind = .put, .key = "definition", .value = raw }, .{ .kind = .put, .key = generation_key, .value = raw } });
    }

    /// Definition versions remain readable after schema/recipe evolution. The
    /// new generation has no implied backfill coverage and cannot reinterpret
    /// older immutable state using a new decoder or aggregate expression.
    pub fn evolve(self: Catalog, expected_generation: u64, next: t.Definition) !void {
        try next.validate();
        var session = try self.open(true);
        defer session.deinit();
        const current = try self.definition(session.value);
        defer current.deinit();
        const generation = std.math.add(u64, expected_generation, 1) catch return error.DatasetDefinitionConflict;
        if (current.value.kind != next.kind or current.value.generation != expected_generation or next.generation != generation or !std.mem.eql(u8, &next.id, &self.dataset)) return error.DatasetDefinitionConflict;
        const key = try std.fmt.allocPrint(self.context.alloc, "definitions/{d}", .{next.generation});
        defer self.context.alloc.free(key);
        const raw = try std.json.Stringify.valueAlloc(self.context.alloc, next, .{});
        defer self.context.alloc.free(raw);
        try self.commit(session.value, &.{ .{ .kind = .put, .key = "definition", .value = raw }, .{ .kind = .put, .key = key, .value = raw } });
    }

    pub fn getDefinition(self: Catalog) !std.json.Parsed(t.Definition) {
        var session = try self.open(false);
        defer session.deinit();
        return self.definition(session.value);
    }

    pub fn getSegment(self: Catalog, id: t.Id) !std.json.Parsed(t.Segment) {
        var session = try self.open(false);
        defer session.deinit();
        return self.segment(session.value, id);
    }

    /// The caller supplies a bounded, complete immutable segment. Content is
    /// uploaded and read-verified before the authoritative metadata is committed.
    pub fn publish(self: Catalog, value: t.Segment, location: Location, bytes: []const u8) !void {
        return self.publishImpl(value, location, bytes, null);
    }

    const ClosedPage = struct { spec: series.Spec, cut: closure.Cut };
    pub fn publishClosedPage(self: Catalog, spec: series.Spec, cut: closure.Cut, value: t.Segment, location: Location, bytes: []const u8) !void {
        return self.publishImpl(value, location, bytes, .{ .spec = spec, .cut = cut });
    }
    fn closedKey(a: A, generation: u64, partition: t.Id, incarnation: u64) ![]u8 {
        return std.fmt.allocPrint(a, "closed/{d}/{s}/{d}", .{ generation, std.fmt.bytesToHex(partition, .lower), incarnation });
    }
    fn closedProgress(self: Catalog, session: Header, key: []const u8, initial: closure.Manifest) !?closure.Manifest {
        const observed = (try self.get(session, closure.Manifest, key)) orelse return null;
        defer observed.deinit();
        try observed.value.requireScope(initial);
        return observed.value;
    }
    fn checkClosedSpec(self: Catalog, session: Header, spec: series.Spec) !void {
        if (!std.mem.eql(u8, &spec.id, &self.dataset)) return error.InvalidDatasetBinding;
        const def_key = try std.fmt.allocPrint(self.context.alloc, "definitions/{d}", .{spec.generation});
        defer self.context.alloc.free(def_key);
        const def = (try self.get(session, t.Definition, def_key)) orelse return error.InvalidDatasetDefinition;
        defer def.deinit();
        const summary = def.value.summary orelse return error.InvalidDatasetDefinition;
        if (def.value.generation != spec.generation or !std.mem.eql(u8, &try def.value.fingerprint(self.context.alloc), &spec.definition) or
            summary.interval_ns != spec.interval_ns or summary.mutable_for_ns != spec.mutable_for_ns or def.value.retention_ns != spec.retention_ns or
            try std.fmt.parseInt(i128, summary.origin_ns, 10) != spec.origin_ns or summary.measures.len != spec.laws.len) return error.RetainedSeriesDefinitionMismatch;
        for (summary.measures, spec.laws, 0..) |measure, law, i| {
            if (measure.law != law or (law == .count and (measure.field != null) != spec.countsNonNull(i))) return error.RetainedSeriesDefinitionMismatch;
        }
    }
    pub fn getClosedExport(self: Catalog, spec: series.Spec, cut: closure.Cut) !?closure.Manifest {
        const initial = try closure.Manifest.init(spec, cut);
        const key = try closedKey(self.context.alloc, spec.generation, spec.source_partition, spec.source_incarnation);
        defer self.context.alloc.free(key);
        const session = try self.open(false);
        defer session.deinit();
        try self.checkClosedSpec(session.value, spec);
        return self.closedProgress(session.value, key, initial);
    }
    /// The trusted native scan supplies the terminal empty boundary at the
    /// same closure cut. Only the exact durable frontier can be sealed.
    pub fn sealClosedExport(self: Catalog, spec: series.Spec, cut: closure.Cut, empty_tail: t.RowKey) !closure.Manifest {
        const initial = try closure.Manifest.init(spec, cut);
        const key = try closedKey(self.context.alloc, spec.generation, spec.source_partition, spec.source_incarnation);
        defer self.context.alloc.free(key);
        const session = try self.open(true);
        defer session.deinit();
        try self.checkClosedSpec(session.value, spec);
        const current = (try self.closedProgress(session.value, key, initial)) orelse initial;
        const sealed = try current.seal(empty_tail);
        if (!current.sealed) {
            const raw = try std.json.Stringify.valueAlloc(self.context.alloc, sealed, .{});
            defer self.context.alloc.free(raw);
            try self.commit(session.value, &.{.{ .kind = .put, .key = key, .value = raw }});
        }
        return sealed;
    }

    fn publishImpl(self: Catalog, value: t.Segment, location: Location, bytes: []const u8, closed_page: ?ClosedPage) !void {
        var session = try self.open(true);
        defer session.deinit();
        const def = try self.definition(session.value);
        defer def.deinit();
        try value.validate(def.value);
        if (value.retiring or value.local_retiring or value.previous_local != null or value.archive != null or value.location_collected or value.archive_collected or value.placement_revision != 1 or
            !std.mem.eql(u8, &value.location, &location.id) or value.access != location.access or value.byte_len != bytes.len) return error.InvalidDatasetSegment;
        const key = try segmentKey(self.context.alloc, value.id);
        defer self.context.alloc.free(key);
        var progress_key: ?[]const u8 = null;
        defer if (progress_key) |owned| self.context.alloc.free(owned);
        var progress_bytes: ?[]const u8 = null;
        defer if (progress_bytes) |owned| self.context.alloc.free(owned);
        if (closed_page) |page| {
            const spec = page.spec;
            const initial = try closure.Manifest.init(spec, page.cut);
            try self.checkClosedSpec(session.value, spec);
            const progress = try closedKey(self.context.alloc, spec.generation, spec.source_partition, spec.source_incarnation);
            progress_key = progress;
            const current = (try self.closedProgress(session.value, progress, initial)) orelse initial;
            if (current.last != null and std.mem.eql(u8, &current.last.?, &value.id)) {
                // A lost commit response is retryable only for the same bytes
                // and location at the exact last committed frontier.
                const existing = try self.segment(session.value, value.id);
                defer existing.deinit();
                const actual = existing.value;
                if (!std.meta.eql(actual.source, value.source) or !std.meta.eql(try actual.span(), try value.span()) or
                    !std.mem.eql(u8, &current.next, &(try actual.span()).end) or actual.retiring or !actual.finalized or
                    actual.byte_len != value.byte_len or actual.state_encoding != value.state_encoding or
                    !std.mem.eql(u8, &actual.checksum, &value.checksum) or !std.mem.eql(u8, &actual.location, &location.id)) return error.InvalidDatasetCoverage;
                _ = try self.checkLocation(session.value, location, false);
                const verified = try self.readVerified(actual, location);
                defer self.context.alloc.free(verified);
                if (!std.mem.eql(u8, bytes, verified)) return error.DatasetSegmentCorrupt;
                return;
            }
            const next = try current.append(value);
            if (value.state_encoding != codec.encoding or try codec.formatEncoding(bytes) != codec.encoding) return error.InvalidDatasetSegment;
            var arena = std.heap.ArenaAllocator.init(self.context.alloc);
            defer arena.deinit();
            const decoded = try codec.decode(arena.allocator(), spec, bytes);
            if (!decoded.receipt.source_closed or decoded.rows.len == 0 or decoded.rows.len > series.max_changes or
                decoded.receipt.source_sequence != page.cut.sequence or decoded.receipt.admitted_ns != page.cut.admitted_ns or
                decoded.receipt.complete_from_ns != page.cut.complete_from_ns or !std.meta.eql(decoded.receipt.span, try value.span())) return error.InvalidDatasetCoverage;
            progress_bytes = try std.json.Stringify.valueAlloc(self.context.alloc, next, .{});
        }
        if (try self.get(session.value, t.Segment, key)) |existing| {
            existing.deinit();
            return error.DatasetSegmentAlreadyExists;
        }
        const floor_key = try retirementKey(self.context.alloc, value);
        defer self.context.alloc.free(floor_key);
        if (try self.get(session.value, RetirementFloor, floor_key)) |floor| {
            defer floor.deinit();
            const through = floor.value.through_key orelse t.rowKey(std.fmt.parseInt(i128, floor.value.through_ns, 10) catch return error.InvalidDatasetCoverage, @splat(0));
            if (std.mem.order(u8, &(try value.span()).start, &through) == .lt) return error.DatasetSegmentRetired;
        }
        const prefix = try coveragePrefix(self.context.alloc, value.definition_generation, value.source.partition, value.source.incarnation);
        defer self.context.alloc.free(prefix);
        const after = try coverageAfter(self.context.alloc, prefix, (try value.span()).start);
        defer self.context.alloc.free(after);
        const successor = try self.list(session.value, prefix, after, 1);
        defer successor.deinit();
        if (successor.value.results[0].len != 0) {
            const index = try std.json.parseFromSlice(Coverage, self.context.alloc, successor.value.results[0][0].value, .{});
            defer index.deinit();
            if ((try value.span()).overlaps(try index.value.span())) return error.DatasetCoverageOverlap;
        }
        _ = try self.checkLocation(session.value, location, true);
        // Encode before object I/O to reject metadata that exceeds page limits.
        const encoded = try std.json.Stringify.valueAlloc(self.context.alloc, value, .{});
        defer self.context.alloc.free(encoded);
        if (encoded.len > 32768) return error.DatasetSegmentBudgetExceeded;
        try self.writeVerified(value, location, bytes);
        var extra: [2]coordinator.Command = undefined;
        var extra_len: usize = 0;
        var expiry: ?ExpiryCursor = null;
        if (progress_key) |progress| {
            extra[0] = .{ .kind = .put, .key = progress, .value = progress_bytes.? };
            extra_len = 1;
            expiry = try expiryKey(value, def.value.retention_ns);
            if (expiry) |*due_key| {
                extra[1] = .{ .kind = .put, .key = due_key, .value = "1" };
                extra_len = 2;
            }
        }
        try self.putSegmentWithLocation(session.value, value, location, extra[0..extra_len]);
    }

    pub fn hold(self: Catalog, id: t.Id, dependency: Dependency) !void {
        if (t.zero(dependency.id) or t.zero(dependency.consumer) or dependency.consumer_generation == 0 or dependency.consumer_generation > t.max_public_counter) return error.InvalidDatasetDependency;
        var session = try self.open(true);
        defer session.deinit();
        const current = try self.segment(session.value, id);
        defer current.deinit();
        if (current.value.retiring) return error.DatasetSegmentRetired;
        const key = try dependencyKey(self.context.alloc, id, dependency.id);
        defer self.context.alloc.free(key);
        if (try self.get(session.value, Dependency, key)) |existing| {
            defer existing.deinit();
            if (!std.meta.eql(existing.value, dependency)) return error.DatasetDependencyConflict;
            return;
        }
        const raw = try std.json.Stringify.valueAlloc(self.context.alloc, dependency, .{});
        defer self.context.alloc.free(raw);
        try self.commit(session.value, &.{.{ .kind = .put, .key = key, .value = raw }});
    }

    /// Consumer authority must be checked by the host. Exact identity prevents
    /// a stale release from clearing a replacement generation's dependency.
    pub fn releaseHold(self: Catalog, id: t.Id, dependency: Dependency) !void {
        var session = try self.open(true);
        defer session.deinit();
        const key = try dependencyKey(self.context.alloc, id, dependency.id);
        defer self.context.alloc.free(key);
        const existing = (try self.get(session.value, Dependency, key)) orelse return;
        defer existing.deinit();
        if (!std.meta.eql(existing.value, dependency)) return error.DatasetDependencyConflict;
        try self.commit(session.value, &.{.{ .kind = .delete, .key = key }});
    }

    /// Copy first, then change one authoritative placement with exact-root CAS.
    /// A failed/cancelled attempt cannot authorize local reclamation.
    pub fn migrate(self: Catalog, id: t.Id, expected_revision: u64, source: Location, destination: Location) !void {
        var session = try self.open(true);
        defer session.deinit();
        const current = try self.segment(session.value, id);
        defer current.deinit();
        var next = current.value;
        if (next.retiring or !next.finalized or next.previous_local != null or next.local_retiring or next.placement_revision != expected_revision or
            !std.mem.eql(u8, &next.location, &source.id) or next.access != source.access or std.mem.eql(u8, &source.id, &destination.id)) return error.DatasetPlacementConflict;
        if (source.access != .local or destination.access == .local or (next.archive != null and !std.mem.eql(u8, &next.archive.?, &destination.id))) return error.DatasetMigrationUnsupported;
        _ = try self.checkLocation(session.value, source, false);
        _ = try self.checkLocation(session.value, destination, true);
        const bytes = try self.readVerified(next, source);
        defer self.context.alloc.free(bytes);
        try self.writeVerified(next, destination, bytes);
        next.previous_local = source.id;
        next.archive = destination.id;
        next.location = destination.id;
        next.access = destination.access;
        next.placement_revision = nextPlacementRevision(next.placement_revision) catch return error.DatasetPlacementConflict;
        try self.putSegmentWithLocation(session.value, next, destination, &.{});
    }

    fn list(self: Catalog, session: Header, prefix: []const u8, after: []const u8, limit: usize) !std.json.Parsed(Read) {
        if (limit == 0 or limit > max_page) return error.DatasetPageBudgetExceeded;
        const raw = try self.context.execute(.{ .root_backend = .conditional, .action = .read, .owner = session.state.owner, .state = session.state, .root_cursor = session.root_cursor, .commands = &.{.{ .kind = .list, .key = prefix, .after = after, .limit = limit }}, .read_limits = .{ .max_encoded_bytes = 4 * 1024 * 1024, .max_value_bytes = 32768 } });
        defer self.context.alloc.free(raw);
        return std.json.parseFromSlice(Read, self.context.alloc, raw, .{ .allocate = .alloc_always });
    }

    pub fn listSegments(self: Catalog, after: []const u8, limit: usize) !std.json.Parsed(Read) {
        var session = try self.open(false);
        defer session.deinit();
        const def = try self.definition(session.value);
        defer def.deinit();
        return self.list(session.value, "segments/", after, limit);
    }

    /// Resolve one source partition/generation by ordered event-time seeks.
    /// This is a preparation result, not a read lease: acquire every selected
    /// segment before exposing any result. Empty history requires an explicit
    /// empty-state segment; absence is never silently converted into zero.
    pub fn planRange(self: Catalog, generation: u64, partition: t.Id, incarnation: u64, range: t.Range, limit: usize) !RangePlan {
        return self.planExtent(generation, partition, incarnation, try t.KeySpan.fromRange(range), limit, false);
    }

    /// Select complete immutable segments covering a requested range. Consumers
    /// must filter decoded row keys to the requested extent. This preserves
    /// exact aggregate states without requiring queries to align with files.
    pub fn planCoveringRange(self: Catalog, generation: u64, partition: t.Id, incarnation: u64, range: t.Range, limit: usize) !RangePlan {
        return self.planExtent(generation, partition, incarnation, try t.KeySpan.fromRange(range), limit, true);
    }

    fn planExtent(self: Catalog, generation: u64, partition: t.Id, incarnation: u64, extent_requested: t.KeySpan, limit: usize, covering: bool) !RangePlan {
        try extent_requested.validate();
        if (limit == 0 or limit > max_page or generation == 0 or incarnation == 0 or t.zero(partition)) return error.DatasetPageBudgetExceeded;
        var arena = std.heap.ArenaAllocator.init(self.context.alloc);
        errdefer arena.deinit();
        const temp = arena.allocator();
        var catalog = self;
        catalog.context.alloc = temp;
        // Everything belongs to the returned arena; no allocator capability or
        // parsed-header arena escapes by reference to this stack frame.
        const session = try catalog.open(false);
        const prefix = try coveragePrefix(temp, generation, partition, incarnation);
        const after = try coverageAfter(temp, prefix, extent_requested.start);
        const page = try catalog.list(session.value, prefix, after, limit);
        var selected = std.ArrayList(Coverage).empty;
        var keys = std.ArrayList([]const u8).empty;
        var covered = extent_requested.start;
        const end = extent_requested.end;
        for (page.value.results[0]) |row| {
            const index = try std.json.parseFromSliceLeaky(Coverage, temp, row.value, .{});
            const extent = try index.span();
            if (!std.mem.eql(u8, &extent.start, &covered) and
                !(covering and selected.items.len == 0 and std.mem.order(u8, &extent.start, &covered) == .lt)) return error.DatasetCoverageUnavailable;
            if (!covering and std.mem.order(u8, &extent.end, &end) == .gt) return error.DatasetResolutionUnavailable;
            try selected.append(temp, index);
            try keys.append(temp, try segmentKey(temp, index.segment));
            covered = extent.end;
            if (std.mem.order(u8, &covered, &end) != .lt) break;
        }
        if (std.mem.order(u8, &covered, &end) == .lt) return if (selected.items.len == limit) error.DatasetPageBudgetExceeded else error.DatasetCoverageUnavailable;
        const definition_key = try std.fmt.allocPrint(temp, "definitions/{d}", .{generation});
        const def = (try catalog.get(session.value, t.Definition, definition_key)) orelse return error.InvalidDatasetDefinition;
        try def.value.validate();
        // One bounded multi-key traversal shares navigation pages across all
        // selected segments; definitions are resolved once per range plan.
        const raw = try catalog.context.readSnapshotValuesAlloc(.conditional, session.value.state, keys.items, .{});
        const entries = try temp.alloc(t.Segment, selected.items.len);
        const now = catalog.clock.unix();
        for (raw, selected.items, entries) |value, index, *part| {
            part.* = try std.json.parseFromSliceLeaky(t.Segment, temp, value orelse return error.InvalidDatasetCoverage, .{ .allocate = .alloc_always });
            try part.validate(def.value);
            if (!std.mem.eql(u8, &part.id, &index.segment) or !std.mem.eql(u8, &part.source.partition, &partition) or part.source.incarnation != incarnation or
                try part.coverage.start() != try std.fmt.parseInt(i128, index.start_ns, 10) or try part.coverage.end() != try std.fmt.parseInt(i128, index.end_ns, 10) or !std.meta.eql(try part.span(), try index.span())) return error.InvalidDatasetCoverage;
            if (part.retiring or try part.expired(def.value, now)) return error.DatasetCoverageUnavailable;
            if (part.access == .restore_required) return error.DatasetRestoreRequired;
        }
        return .{ .arena = arena, .segments = entries };
    }

    /// Register protection before returning an authoritative location. The root
    /// CAS races atomically with migration/retirement; no GET-then-pin gap.
    pub fn acquire(self: Catalog, id: t.Id) !Lease {
        var session = try self.open(true);
        defer session.deinit();
        const current = try self.segment(session.value, id);
        defer current.deinit();
        const value = current.value;
        if (value.retiring) return error.DatasetSegmentRetired;
        if (value.access == .restore_required) return error.DatasetRestoreRequired;
        const now = self.clock.unix();
        const authority = self.clock.authority();
        if (now == 0 or authority == 0) return error.DatasetLeaseExpired;
        const definition_key = try std.fmt.allocPrint(self.context.alloc, "definitions/{d}", .{value.definition_generation});
        defer self.context.alloc.free(definition_key);
        const def = (try self.get(session.value, t.Definition, definition_key)) orelse return error.InvalidDatasetDefinition;
        defer def.deinit();
        if (try value.expired(def.value, now)) return error.DatasetSegmentExpired;
        var id_bytes: t.Id = undefined;
        self.context.io.random(&id_bytes);
        if (t.zero(id_bytes)) return error.InvalidDatasetBinding;
        const authority_deadline = std.math.add(u64, authority, pin_duration_ns) catch return error.DatasetLeaseExpired;
        const pin = Pin{ .id = id_bytes, .segment = id, .location = value.location, .placement_revision = value.placement_revision, .expires_ns = std.math.add(u64, now, pin_duration_ns) catch return error.DatasetLeaseExpired };
        const prefix = try pinPrefix(self.context.alloc, id);
        defer self.context.alloc.free(prefix);
        const pins = try self.list(session.value, prefix, "", 65);
        defer pins.deinit();
        if (pins.value.results[0].len > 64) return error.DatasetReaderBudgetExceeded;
        var commands = std.ArrayList(coordinator.Command).empty;
        defer commands.deinit(self.context.alloc);
        var live: usize = 0;
        for (pins.value.results[0]) |row| {
            const existing = try std.json.parseFromSlice(Pin, self.context.alloc, row.value, .{});
            defer existing.deinit();
            if (!std.mem.eql(u8, &existing.value.segment, &id) or std.mem.eql(u8, &existing.value.id, &pin.id)) return error.InvalidDatasetPin;
            if (existing.value.expires_ns +| pin_grace_ns < now) try commands.append(self.context.alloc, .{ .kind = .delete, .key = row.key }) else live += 1;
        }
        if (live >= 64) return error.DatasetReaderBudgetExceeded;
        const key = try pinKey(self.context.alloc, pin);
        defer self.context.alloc.free(key);
        const raw = try std.json.Stringify.valueAlloc(self.context.alloc, pin, .{});
        defer self.context.alloc.free(raw);
        try commands.append(self.context.alloc, .{ .kind = .put, .key = key, .value = raw });
        try self.commit(session.value, commands.items);
        return .{ .pin = pin, .authority_deadline = authority_deadline };
    }

    fn checkLease(self: Catalog, lease: Lease) !void {
        const now = self.clock.unix();
        const authority = self.clock.authority();
        if (now == 0 or authority == 0 or now >= lease.pin.expires_ns or authority >= lease.authority_deadline) return error.DatasetLeaseExpired;
    }

    pub fn read(self: Catalog, lease: Lease, location: Location) ![]u8 {
        try self.checkLease(lease);
        var session = try self.open(false);
        defer session.deinit();
        const key = try pinKey(self.context.alloc, lease.pin);
        defer self.context.alloc.free(key);
        const stored = (try self.get(session.value, Pin, key)) orelse return error.DatasetLeaseExpired;
        defer stored.deinit();
        if (!std.meta.eql(stored.value, lease.pin) or !std.mem.eql(u8, &lease.pin.location, &location.id)) return error.InvalidDatasetPin;
        const current = try self.segment(session.value, lease.pin.segment);
        defer current.deinit();
        _ = try self.checkLocation(session.value, location, false);
        const bytes = try self.readVerified(current.value, location);
        errdefer self.context.alloc.free(bytes);
        try self.checkLease(lease);
        return bytes;
    }

    pub fn release(self: Catalog, lease: Lease) !void {
        var session = try self.open(true);
        defer session.deinit();
        const key = try pinKey(self.context.alloc, lease.pin);
        defer self.context.alloc.free(key);
        const stored = (try self.get(session.value, Pin, key)) orelse return;
        defer stored.deinit();
        if (!std.meta.eql(stored.value, lease.pin)) return error.InvalidDatasetPin;
        try self.commit(session.value, &.{.{ .kind = .delete, .key = key }});
    }

    fn requireNoReaders(self: Catalog, session: Header, id: t.Id, location: ?t.Id) !void {
        const now = self.clock.unix();
        if (now == 0) return error.DatasetLeaseExpired;
        const prefix = try pinPrefix(self.context.alloc, id);
        defer self.context.alloc.free(prefix);
        const pins = try self.list(session, prefix, "", 65);
        defer pins.deinit();
        if (pins.value.results[0].len > 64) return error.DatasetReaderBudgetExceeded;
        for (pins.value.results[0]) |row| {
            const pin = try std.json.parseFromSlice(Pin, self.context.alloc, row.value, .{});
            defer pin.deinit();
            if (!std.mem.eql(u8, &pin.value.segment, &id)) return error.InvalidDatasetPin;
            if ((location == null or std.mem.eql(u8, &location.?, &pin.value.location)) and pin.value.expires_ns +| pin_grace_ns >= now) return error.DatasetReadersActive;
        }
    }

    /// Two durable steps: fence readers first, then delete the old copy. A
    /// restart resumes the same deletion; no new rights can target that copy.
    pub fn reclaimLocal(self: Catalog, id: t.Id, location: Location) !bool {
        var session = try self.open(true);
        defer session.deinit();
        const current = try self.segment(session.value, id);
        defer current.deinit();
        var next = current.value;
        const previous = next.previous_local orelse return true;
        if (location.access != .local or !std.mem.eql(u8, &previous, &location.id)) return error.DatasetPlacementConflict;
        if (!next.local_retiring) {
            try self.requireNoReaders(session.value, id, previous);
            next.local_retiring = true;
            try self.putSegment(session.value, next);
            return false;
        }
        _ = try self.checkLocation(session.value, location, false);
        try self.removeObject(next, location);
        next.previous_local = null;
        next.local_retiring = false;
        try self.putSegment(session.value, next);
        return true;
    }

    /// Expiration is independent of local residence. Finalized historical state
    /// may be retired only after the caller has checked downstream dependencies.
    /// Retirement rejects new readers before any object deletion is attempted.
    /// Ordered expiry seek. The caller advances the cursor when a pin or
    /// dependency defers one extent, so unrelated history can still retire.
    pub fn nextExpiry(self: Catalog, after: ?ExpiryCursor) !?Expiry {
        const now = self.clock.unix();
        if (now == 0) return error.DatasetLeaseExpired;
        const session = try self.open(false);
        defer session.deinit();
        const page = try self.list(session.value, expiry_prefix, if (after) |*key| key else "", 1);
        defer page.deinit();
        if (page.value.results[0].len == 0) return null;
        const row = page.value.results[0][0];
        if (row.key.len != @sizeOf(ExpiryCursor) or row.key[expiry_prefix.len + 32] != '/' or !std.mem.eql(u8, row.value, "1")) return error.InvalidDatasetCoverage;
        const biased = std.fmt.parseInt(u128, row.key[expiry_prefix.len..][0..32], 16) catch return error.InvalidDatasetCoverage;
        const expires: i128 = @bitCast(biased ^ (@as(u128, 1) << 127));
        if (expires > now) return null;
        var id: t.Id = undefined;
        _ = std.fmt.hexToBytes(&id, row.key[expiry_prefix.len + 33 ..]) catch return error.InvalidDatasetCoverage;
        return .{ .cursor = row.key[0..@sizeOf(ExpiryCursor)].*, .segment = id };
    }

    pub fn collectExpired(self: Catalog, id: t.Id, location: Location) !void {
        const segment_value = self.getSegment(id) catch |err| switch (err) {
            error.DatasetSegmentNotFound => return,
            else => return err,
        };
        defer segment_value.deinit();
        const value = segment_value.value;
        if (!std.mem.eql(u8, &value.location, &location.id) or value.previous_local != null or (value.archive != null and !std.mem.eql(u8, &value.archive.?, &location.id))) return error.DatasetLocationUnavailable;
        try self.retire(id, value.placement_revision);
        try self.collect(id, location);
        try self.compact(id);
    }

    pub fn retire(self: Catalog, id: t.Id, expected_revision: u64) !void {
        var session = try self.open(true);
        defer session.deinit();
        const current = try self.segment(session.value, id);
        defer current.deinit();
        const definition_key = try std.fmt.allocPrint(self.context.alloc, "definitions/{d}", .{current.value.definition_generation});
        defer self.context.alloc.free(definition_key);
        const def = (try self.get(session.value, t.Definition, definition_key)) orelse return error.InvalidDatasetDefinition;
        defer def.deinit();
        var next = current.value;
        if (next.retiring) return;
        const now = self.clock.unix();
        const expires = std.math.add(i128, try next.coverage.end(), def.value.retention_ns) catch return error.DatasetRetentionPending;
        if (now == 0 or expires > now or !next.finalized or next.previous_local != null or expected_revision != next.placement_revision) return error.DatasetRetentionPending;
        const prefix = try std.fmt.allocPrint(self.context.alloc, "dependencies/{s}/", .{std.fmt.bytesToHex(id, .lower)});
        defer self.context.alloc.free(prefix);
        const dependencies = try self.list(session.value, prefix, "", 1);
        defer dependencies.deinit();
        if (dependencies.value.results[0].len != 0) return error.DatasetDependencyPending;
        try self.requireNoReaders(session.value, id, null);
        next.retiring = true;
        const closed_key = try closedKey(self.context.alloc, next.definition_generation, next.source.partition, next.source.incarnation);
        defer self.context.alloc.free(closed_key);
        if (try self.get(session.value, closure.Manifest, closed_key)) |owned| closed: {
            defer owned.deinit();
            var manifest = owned.value;
            const span = try next.span();
            // Older segments outside the closed export remain governed by
            // their own retirement records.
            if (std.mem.order(u8, &span.end, &manifest.first) != .gt) break :closed;
            if (!std.meta.eql(manifest.source, next.source) or !std.mem.eql(u8, &next.id, &closure.segmentId(manifest.cut.closure, span))) return error.InvalidDatasetCoverage;
            if (!manifest.sealed) return error.DatasetDependencyPending;
            const end = try next.coverage.end();
            manifest.expired_before_ns = @max(end, manifest.expired_before_ns orelse end);
            const closed_bytes = try std.json.Stringify.valueAlloc(self.context.alloc, manifest, .{});
            defer self.context.alloc.free(closed_bytes);
            const segment_key = try segmentKey(self.context.alloc, id);
            defer self.context.alloc.free(segment_key);
            const segment_bytes = try std.json.Stringify.valueAlloc(self.context.alloc, next, .{});
            defer self.context.alloc.free(segment_bytes);
            try self.commit(session.value, &.{ .{ .kind = .put, .key = segment_key, .value = segment_bytes }, .{ .kind = .put, .key = closed_key, .value = closed_bytes } });
            return;
        }
        try self.putSegment(session.value, next);
    }

    pub fn collect(self: Catalog, id: t.Id, location: Location) !void {
        var session = try self.open(true);
        defer session.deinit();
        const current = try self.segment(session.value, id);
        defer current.deinit();
        var next = current.value;
        const primary = std.mem.eql(u8, &location.id, &next.location);
        const archive = next.archive != null and std.mem.eql(u8, &location.id, &next.archive.?);
        if (!next.retiring or next.previous_local != null or (!primary and !archive)) return error.DatasetRetentionPending;
        if ((!primary or next.location_collected) and (!archive or next.archive_collected)) return;
        _ = try self.checkLocation(session.value, location, false);
        try self.removeObject(next, location);
        if (primary) next.location_collected = true;
        if (archive) next.archive_collected = true;
        // Keep the tombstone: old publication retries must never resurrect it.
        try self.putSegment(session.value, next);
    }

    /// Retire detailed tombstones in event order into one durable lower bound.
    /// This bounds metadata growth for an indefinitely running retained dataset
    /// without allowing delayed publications to resurrect expired history.
    pub fn compact(self: Catalog, id: t.Id) !void {
        var session = try self.open(true);
        defer session.deinit();
        const current = self.segment(session.value, id) catch |err| switch (err) {
            error.DatasetSegmentNotFound => return,
            else => return err,
        };
        defer current.deinit();
        const value = current.value;
        if (!value.retiring or !value.location_collected or value.previous_local != null or (value.archive != null and !value.archive_collected)) return error.DatasetRetentionPending;
        const prefix = try coveragePrefix(self.context.alloc, value.definition_generation, value.source.partition, value.source.incarnation);
        defer self.context.alloc.free(prefix);
        const first = try self.list(session.value, prefix, "", 1);
        defer first.deinit();
        if (first.value.results[0].len != 1) return error.InvalidDatasetCoverage;
        const index = try std.json.parseFromSlice(Coverage, self.context.alloc, first.value.results[0][0].value, .{});
        defer index.deinit();
        if (!std.mem.eql(u8, &index.value.segment, &id)) return error.DatasetCompactionOrder;
        const floor_key = try retirementKey(self.context.alloc, value);
        defer self.context.alloc.free(floor_key);
        if (try self.get(session.value, RetirementFloor, floor_key)) |floor| {
            defer floor.deinit();
            const previous = floor.value.through_key orelse t.rowKey(std.fmt.parseInt(i128, floor.value.through_ns, 10) catch return error.InvalidDatasetCoverage, @splat(0));
            if (std.mem.order(u8, &(try value.span()).end, &previous) != .gt) return error.InvalidDatasetCoverage;
        }
        const pins_prefix = try pinPrefix(self.context.alloc, id);
        defer self.context.alloc.free(pins_prefix);
        const pins = try self.list(session.value, pins_prefix, "", 65);
        defer pins.deinit();
        if (pins.value.results[0].len > 64) return error.DatasetReaderBudgetExceeded;
        var commands = std.ArrayList(coordinator.Command).empty;
        defer commands.deinit(self.context.alloc);
        const segment_key = try segmentKey(self.context.alloc, id);
        defer self.context.alloc.free(segment_key);
        const coverage_key = try coverageKey(self.context.alloc, value);
        defer self.context.alloc.free(coverage_key);
        const floor = try std.json.Stringify.valueAlloc(self.context.alloc, RetirementFloor{ .through_ns = value.coverage.end_ns, .through_key = (try value.span()).end }, .{});
        defer self.context.alloc.free(floor);
        try commands.appendSlice(self.context.alloc, &.{ .{ .kind = .delete, .key = segment_key }, .{ .kind = .delete, .key = coverage_key }, .{ .kind = .put, .key = floor_key, .value = floor } });
        for (pins.value.results[0]) |row| try commands.append(self.context.alloc, .{ .kind = .delete, .key = row.key });
        const definition_key = try std.fmt.allocPrint(self.context.alloc, "definitions/{d}", .{value.definition_generation});
        defer self.context.alloc.free(definition_key);
        const definition_value = (try self.get(session.value, t.Definition, definition_key)) orelse return error.InvalidDatasetDefinition;
        defer definition_value.deinit();
        const expiry = try expiryKey(value, definition_value.value.retention_ns);
        if (expiry) |*key| try commands.append(self.context.alloc, .{ .kind = .delete, .key = key });
        try self.commit(session.value, commands.items);
    }

    fn putSegmentWithLocation(self: Catalog, session: Header, value: t.Segment, location: Location, extra: []const coordinator.Command) !void {
        const bound = try self.checkLocation(session, location, true);
        const key = try segmentKey(self.context.alloc, value.id);
        defer self.context.alloc.free(key);
        const binding_key = try locationKey(self.context.alloc, location.id);
        defer self.context.alloc.free(binding_key);
        const binding = try std.json.Stringify.valueAlloc(self.context.alloc, .{ .storage_identity = location.storage_identity, .access = location.access }, .{});
        defer self.context.alloc.free(binding);
        const raw = try std.json.Stringify.valueAlloc(self.context.alloc, value, .{});
        defer self.context.alloc.free(raw);
        const coverage_key = try coverageKey(self.context.alloc, value);
        defer self.context.alloc.free(coverage_key);
        const coverage = try std.json.Stringify.valueAlloc(self.context.alloc, Coverage{ .segment = value.id, .start_ns = value.coverage.start_ns, .end_ns = value.coverage.end_ns, .key_span = value.key_span }, .{});
        defer self.context.alloc.free(coverage);
        if (extra.len > 2) return error.DatasetPageBudgetExceeded;
        var commands: [5]coordinator.Command = undefined;
        commands[0] = .{ .kind = .put, .key = binding_key, .value = binding };
        commands[1] = .{ .kind = .put, .key = key, .value = raw };
        commands[2] = .{ .kind = .put, .key = coverage_key, .value = coverage };
        @memcpy(commands[3..][0..extra.len], extra);
        try self.commit(session, commands[if (bound) @as(usize, 1) else 0 .. 3 + extra.len]);
    }

    fn putSegment(self: Catalog, session: Header, value: t.Segment) !void {
        const key = try segmentKey(self.context.alloc, value.id);
        defer self.context.alloc.free(key);
        const raw = try std.json.Stringify.valueAlloc(self.context.alloc, value, .{});
        defer self.context.alloc.free(raw);
        try self.commit(session, &.{.{ .kind = .put, .key = key, .value = raw }});
    }

    fn removeObject(self: Catalog, value: t.Segment, location: Location) !void {
        const key = try self.objectKey(self.context.alloc, value, location);
        defer self.context.alloc.free(key);
        var client = location.client;
        client.allocator = self.context.alloc;
        client.deleteObject(location.bucket, key, .{ .cancellation = self.context.cancellation }) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }

    pub fn restore(self: Catalog, id: t.Id, expected_revision: u64, source: Location, destination: Location) !void {
        var session = try self.open(true);
        defer session.deinit();
        const current = try self.segment(session.value, id);
        defer current.deinit();
        var next = current.value;
        if (next.retiring or next.previous_local != null or next.placement_revision != expected_revision or destination.access != .local or
            source.access == .local or next.access != source.access or !std.mem.eql(u8, &next.location, &source.id) or std.mem.eql(u8, &source.id, &destination.id)) return error.DatasetPlacementConflict;
        _ = try self.checkLocation(session.value, source, false);
        _ = try self.checkLocation(session.value, destination, true);
        const bytes = try self.readVerified(next, source);
        defer self.context.alloc.free(bytes);
        try self.writeVerified(next, destination, bytes);
        // The archive remains durable until an independent logical-retention
        // decision. Restore does not authorize deleting its source copy.
        next.archive = source.id;
        next.location = destination.id;
        next.access = .local;
        next.placement_revision = nextPlacementRevision(next.placement_revision) catch return error.DatasetPlacementConflict;
        try self.putSegmentWithLocation(session.value, next, destination, &.{});
    }

    fn objectKey(self: Catalog, alloc: A, value: t.Segment, location: Location) ![]u8 {
        if (t.zero(location.id) or location.prefix.len == 0 or location.bucket.len == 0) return error.InvalidDatasetBinding;
        return std.fmt.allocPrint(alloc, "{s}/datasets/{s}/{s}/{d}/{s}/{s}", .{ location.prefix, std.fmt.bytesToHex(location.id, .lower), std.fmt.bytesToHex(self.dataset, .lower), value.definition_generation, std.fmt.bytesToHex(value.id, .lower), std.fmt.bytesToHex(value.checksum, .lower) });
    }

    fn readVerified(self: Catalog, value: t.Segment, location: Location) ![]u8 {
        if (value.byte_len > max_segment_bytes) return error.DatasetSegmentBudgetExceeded;
        const key = try self.objectKey(self.context.alloc, value, location);
        defer self.context.alloc.free(key);
        var client = location.client;
        client.allocator = self.context.alloc;
        var result = try client.getObject(location.bucket, key, .{ .skip_metadata_probe = true, .max_response_bytes = @intCast(value.byte_len), .cancellation = self.context.cancellation });
        errdefer result.deinit(self.context.alloc);
        try verify(value, result.body);
        result.metadata.deinit(self.context.alloc);
        return result.body;
    }

    fn writeVerified(self: Catalog, value: t.Segment, location: Location, bytes: []const u8) !void {
        try verify(value, bytes);
        const key = try self.objectKey(self.context.alloc, value, location);
        defer self.context.alloc.free(key);
        var client = location.client;
        client.allocator = self.context.alloc;
        var result = client.putObject(location.bucket, key, bytes, .{ .if_none_match = true, .cancellation = self.context.cancellation }) catch |err| switch (err) {
            error.PreconditionFailed => {
                const existing = try self.readVerified(value, location);
                self.context.alloc.free(existing);
                return;
            },
            else => return err,
        };
        result.deinit(self.context.alloc);
        const copied = try self.readVerified(value, location);
        self.context.alloc.free(copied);
    }
};

fn segmentKey(a: A, id: t.Id) ![]u8 {
    return std.fmt.allocPrint(a, "segments/{s}", .{std.fmt.bytesToHex(id, .lower)});
}
fn verify(value: t.Segment, bytes: []const u8) !void {
    if (bytes.len > max_segment_bytes) return error.DatasetSegmentBudgetExceeded;
    var checksum: t.Id = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &checksum, .{});
    if (value.byte_len != bytes.len or !std.mem.eql(u8, &checksum, &value.checksum)) return error.DatasetSegmentCorrupt;
}

fn pinPrefix(a: A, id: t.Id) ![]u8 {
    return std.fmt.allocPrint(a, "pins/{s}/", .{std.fmt.bytesToHex(id, .lower)});
}
fn pinKey(a: A, pin: Pin) ![]u8 {
    return std.fmt.allocPrint(a, "pins/{s}/{s}", .{ std.fmt.bytesToHex(pin.segment, .lower), std.fmt.bytesToHex(pin.id, .lower) });
}

fn dependencyKey(a: A, segment: t.Id, id: t.Id) ![]u8 {
    return std.fmt.allocPrint(a, "dependencies/{s}/{s}", .{ std.fmt.bytesToHex(segment, .lower), std.fmt.bytesToHex(id, .lower) });
}

fn locationKey(a: A, id: t.Id) ![]u8 {
    return std.fmt.allocPrint(a, "locations/{s}", .{std.fmt.bytesToHex(id, .lower)});
}

fn coveragePrefix(a: A, generation: u64, partition: t.Id, incarnation: u64) ![]u8 {
    return std.fmt.allocPrint(a, "coverage/{d}/{s}/{d}/", .{ generation, std.fmt.bytesToHex(partition, .lower), incarnation });
}
fn coverageAfter(a: A, prefix: []const u8, start: t.RowKey) ![]u8 {
    // Legacy whole-time keys have only the time prefix, and consequently sort
    // before the equivalent time+zero-hash boundary as required.
    return std.fmt.allocPrint(a, "{s}{s}~", .{ prefix, start });
}
fn coverageKey(a: A, value: t.Segment) ![]u8 {
    const prefix = try coveragePrefix(a, value.definition_generation, value.source.partition, value.source.incarnation);
    defer a.free(prefix);
    if (value.key_span == null) {
        const end = (try value.span()).end;
        // Preserve the physical key of existing legacy entries during updates.
        return std.fmt.allocPrint(a, "{s}{s}/{s}", .{ prefix, end[0..32], std.fmt.bytesToHex(value.id, .lower) });
    }
    return std.fmt.allocPrint(a, "{s}{s}/{s}", .{ prefix, (try value.span()).end, std.fmt.bytesToHex(value.id, .lower) });
}

fn nextPlacementRevision(value: u64) !u64 {
    if (value >= t.max_public_counter) return error.DatasetPlacementConflict;
    return value + 1;
}

fn retirementKey(a: A, value: t.Segment) ![]u8 {
    return std.fmt.allocPrint(a, "retired/{d}/{s}/{d}", .{ value.definition_generation, std.fmt.bytesToHex(value.source.partition, .lower), value.source.incarnation });
}
