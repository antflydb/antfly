// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! An object-store authoritative Iceberg catalog. Immutable metadata and commit
//! records precede one conditional HEAD replacement. Worker memory is no authority.
const std = @import("std");
const storage = @import("objectstore");
const types = @import("types.zig");
const metadata = @import("metadata.zig");
const A = std.mem.Allocator;

pub const Record = struct {
    format: u8 = 1,
    commit_id: []const u8,
    request_hash: []const u8,
    metadata_location: []const u8,
    metadata_key: []const u8,
    metadata_hash: []const u8,
    previous_record: ?[]const u8 = null,
    previous_version: ?[]const u8 = null,
};
pub const Managed = struct {
    client: storage.Client,
    bucket: []const u8,
    prefix: []const u8,
    source_uri: []const u8,
    context: types.Context = .{},
    max_history_records: usize = 4096,

    fn key(self: Managed, a: A, suffix: []const u8) ![]u8 {
        const prefix = std.mem.trim(u8, self.prefix, "/");
        return if (prefix.len == 0) a.dupe(u8, suffix) else std.fmt.allocPrint(a, "{s}/{s}", .{ prefix, suffix });
    }
    fn catalogKey(self: Managed, a: A, name: []const u8) ![]u8 {
        const relative = try std.fmt.allocPrint(a, "metadata/antfly-catalog/{s}", .{name});
        defer a.free(relative);
        return self.key(a, relative);
    }
    fn token(self: *const Managed) storage.CancellationToken {
        return .{ .ptr = self, .is_cancelled_fn = canceled };
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Managed = @ptrCast(@alignCast(raw));
        self.context.ensureActive() catch return true;
        return false;
    }
    fn get(self: *const Managed, a: A, key_: []const u8, limit: usize) !?storage.GetResult {
        try self.context.ensureActive();
        var client = self.client;
        client.allocator = a;
        return client.getObject(self.bucket, key_, .{ .max_response_bytes = limit, .cancellation = self.token() }) catch |err| switch (err) {
            error.ObjectNotFound, error.FileNotFound => null,
            else => {
                try self.context.ensureActive();
                return err;
            },
        };
    }
    fn recordKey(self: Managed, a: A, record_bytes: []const u8) ![]u8 {
        const name = try std.fmt.allocPrint(a, "records/{s}.json", .{types.digestHex(record_bytes)});
        defer a.free(name);
        return self.catalogKey(a, name);
    }
    fn record(a: A, bytes: []const u8) !std.json.Parsed(Record) {
        const result = try std.json.parseFromSlice(Record, a, bytes, .{ .allocate = .alloc_always });
        errdefer result.deinit();
        const r = result.value;
        if (r.format != 1 or r.commit_id.len == 0 or r.request_hash.len != 64 or r.metadata_hash.len != 64 or r.metadata_key.len == 0 or r.metadata_location.len == 0) return error.InvalidLakeCatalog;
        return result;
    }
    pub fn load(self: *const Managed, a: A) !types.Table {
        const head_key = try self.catalogKey(a, "head.json");
        defer a.free(head_key);
        var head = (try self.get(a, head_key, 64 * 1024)) orelse return error.LakeTableNotFound;
        defer head.deinit(a);
        const parsed = try record(a, head.body);
        defer parsed.deinit();
        const r = parsed.value;
        try self.checkMetadataKey(a, r.metadata_key);
        var data = (try self.get(a, r.metadata_key, types.max_metadata_bytes)) orelse return error.InvalidLakeCatalog;
        defer data.deinit(a);
        if (!std.mem.eql(u8, &types.digestHex(data.body), r.metadata_hash)) return error.InvalidLakeCatalog;
        const version = head.metadata.etag orelse return error.LakeCatalogConditionalWritesRequired;
        const location = try a.dupe(u8, r.metadata_location);
        errdefer a.free(location);
        const body = try a.dupe(u8, data.body);
        errdefer a.free(body);
        const etag = try a.dupe(u8, version);
        errdefer a.free(etag);
        return .{ .metadata_location = location, .metadata_json = body, .version = etag, .record_key = try self.recordKey(a, head.body) };
    }
    fn checkMetadataKey(self: Managed, a: A, candidate: []const u8) !void {
        const allowed = try self.key(a, "metadata/antfly-");
        defer a.free(allowed);
        if (!std.mem.startsWith(u8, candidate, allowed) or !std.mem.endsWith(u8, candidate, ".metadata.json") or std.mem.indexOf(u8, candidate, "..") != null) return error.InvalidLakeCatalog;
    }
    fn immutable(self: *const Managed, a: A, key_: []const u8, bytes: []const u8) !void {
        var client = self.client;
        client.allocator = a;
        try self.context.ensureActive();
        var result = client.putObject(self.bucket, key_, bytes, .{ .if_none_match = true, .content_type = "application/json", .cancellation = self.token() }) catch |err| {
            // This also resolves a lost successful immutable-create response.
            var existing = (try self.get(a, key_, @max(bytes.len, 64 * 1024))) orelse return err;
            defer existing.deinit(a);
            if (!std.mem.eql(u8, existing.body, bytes)) return error.LakeCommitIdReused;
            return;
        };
        result.deinit(a);
    }
    pub fn create(self: *const Managed, a: A, id: []const u8, request: []const u8, timestamp: i64) !types.Table {
        if (id.len == 0 or id.len > 256 or request.len > types.max_commit_bytes or timestamp < 0) return error.InvalidLakeCommit;
        const c: types.Commit = .{ .id = id, .body = request, .timestamp_ms = timestamp, .expected_metadata_location = "<create>" };
        const intent_key = try self.intentKey(a, id);
        defer a.free(intent_key);
        if (try self.get(a, intent_key, 64 * 1024)) |value| {
            var intent = value;
            defer intent.deinit(a);
            const parsed = try record(a, intent.body);
            defer parsed.deinit();
            if (!std.mem.eql(u8, parsed.value.request_hash, &types.commitHash(c))) return error.LakeCommitIdReused;
            try self.publish(a, intent.body);
            return self.loadCommitted(a, parsed.value);
        }
        const identity_input = try std.fmt.allocPrint(a, "{s}:{s}", .{ self.source_uri, id });
        defer a.free(identity_input);
        const identity = types.digestHex(identity_input);
        const uuid = try std.fmt.allocPrint(a, "{s}-{s}-{s}-{s}-{s}", .{ identity[0..8], identity[8..12], identity[12..16], identity[16..20], identity[20..32] });
        defer a.free(uuid);
        const bytes = try metadata.createAlloc(a, request, uuid, timestamp, self.source_uri);
        defer a.free(bytes);
        var parsed_metadata = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
        defer parsed_metadata.deinit();
        if (!std.mem.eql(u8, try metadata.str(try metadata.get(parsed_metadata.value, "location")), self.source_uri)) return error.LakeRelocationRequired;
        const r = try self.stage(a, c, bytes, null);
        defer a.free(r);
        try self.publish(a, r);
        const prepared = try record(a, r);
        defer prepared.deinit();
        return self.loadCommitted(a, prepared.value);
    }
    pub fn commit(self: *const Managed, a: A, c: types.Commit) !types.Table {
        try c.validate();
        // Resume the exact candidate, never rebase a stable commit ID.
        const intent_key = try self.intentKey(a, c.id);
        defer a.free(intent_key);
        if (try self.get(a, intent_key, 64 * 1024)) |value| {
            var intent = value;
            defer intent.deinit(a);
            const p = try record(a, intent.body);
            defer p.deinit();
            if (!std.mem.eql(u8, p.value.request_hash, &types.commitHash(c))) return error.LakeCommitIdReused;
            try self.publish(a, intent.body);
            return self.loadCommitted(a, p.value);
        }
        var table = try self.load(a);
        defer table.deinit(a);
        if (!std.mem.eql(u8, table.metadata_location, c.expected_metadata_location)) return error.LakeCommitConflict;
        const bytes = try metadata.applyAlloc(a, table.metadata_json, table.metadata_location, c);
        defer a.free(bytes);
        const candidate = try self.stage(a, c, bytes, table);
        defer a.free(candidate);
        try self.publish(a, candidate);
        const p = try record(a, candidate);
        defer p.deinit();
        return self.loadCommitted(a, p.value);
    }
    fn loadCommitted(self: *const Managed, a: A, r: Record) !types.Table {
        var data = (try self.get(a, r.metadata_key, types.max_metadata_bytes)) orelse return error.InvalidLakeCatalog;
        defer data.deinit(a);
        if (!std.mem.eql(u8, &types.digestHex(data.body), r.metadata_hash)) return error.InvalidLakeCatalog;
        const location = try a.dupe(u8, r.metadata_location);
        errdefer a.free(location);
        return .{ .metadata_location = location, .metadata_json = try a.dupe(u8, data.body) };
    }
    fn intentKey(self: Managed, a: A, id: []const u8) ![]u8 {
        const name = try std.fmt.allocPrint(a, "intents/{s}.json", .{types.digestHex(id)});
        defer a.free(name);
        return self.catalogKey(a, name);
    }
    fn stage(self: *const Managed, a: A, c: types.Commit, bytes: []const u8, previous: ?types.Table) ![]u8 {
        const relative = try std.fmt.allocPrint(a, "metadata/antfly-{s}.metadata.json", .{types.digestHex(bytes)});
        defer a.free(relative);
        const data_key = try self.key(a, relative);
        defer a.free(data_key);
        const uri = try std.fmt.allocPrint(a, "{s}/{s}", .{ std.mem.trimEnd(u8, self.source_uri, "/"), relative });
        defer a.free(uri);
        try self.immutable(a, data_key, bytes);
        const r: Record = .{ .commit_id = c.id, .request_hash = &types.commitHash(c), .metadata_location = uri, .metadata_key = data_key, .metadata_hash = &types.digestHex(bytes), .previous_record = if (previous) |p| p.record_key else null, .previous_version = if (previous) |p| p.version else null };
        const record_bytes = try std.json.Stringify.valueAlloc(a, r, .{});
        errdefer a.free(record_bytes);
        const record_key = try self.recordKey(a, record_bytes);
        defer a.free(record_key);
        try self.immutable(a, record_key, record_bytes);
        const intent = try self.intentKey(a, c.id);
        defer a.free(intent);
        try self.immutable(a, intent, record_bytes);
        return record_bytes;
    }
    fn publish(self: *const Managed, a: A, bytes: []const u8) !void {
        const p = try record(a, bytes);
        defer p.deinit();
        const r = p.value;
        const head = try self.catalogKey(a, "head.json");
        defer a.free(head);
        var current = try self.get(a, head, 64 * 1024);
        defer if (current) |*value| value.deinit(a);
        const expected_matches = if (current) |value| if (r.previous_version) |expected| if (value.metadata.etag) |actual| std.mem.eql(u8, expected, actual) else false else false else r.previous_version == null;
        if (!expected_matches) switch (try self.resolve(a, r.commit_id, r.request_hash)) {
            .committed => return,
            .unknown => return error.LakeCommitOutcomeUnknown,
            .not_committed => return error.LakeCommitConflict,
        };
        var client = self.client;
        client.allocator = a;
        try self.context.ensureActive();
        var result = client.putObject(self.bucket, head, bytes, .{ .if_match_etag = r.previous_version, .if_none_match = r.previous_version == null, .content_type = "application/json", .cancellation = self.token() }) catch |err| {
            // A canceled caller can inspect this same intent after restart.
            try self.context.ensureActive();
            return switch (try self.resolve(a, r.commit_id, r.request_hash)) {
                .committed => {},
                .not_committed => if (err == error.PreconditionFailed) error.LakeCommitConflict else error.LakeCommitOutcomeUnknown,
                .unknown => error.LakeCommitOutcomeUnknown,
            };
        };
        result.deinit(a);
        const receipt_name = try std.fmt.allocPrint(a, "receipts/{s}.json", .{types.digestHex(r.commit_id)});
        defer a.free(receipt_name);
        const receipt_key = try self.catalogKey(a, receipt_name);
        defer a.free(receipt_key);
        // A receipt is an optimization; the immutable commit chain remains the
        // recovery authority if we crash before this write.
        self.immutable(a, receipt_key, bytes) catch return error.LakeCommitOutcomeUnknown;
    }
    pub fn resolve(self: *const Managed, a: A, id: []const u8, request_hash: []const u8) !types.Outcome {
        const receipt_name = try std.fmt.allocPrint(a, "receipts/{s}.json", .{types.digestHex(id)});
        defer a.free(receipt_name);
        const receipt_key = try self.catalogKey(a, receipt_name);
        defer a.free(receipt_key);
        if (try self.get(a, receipt_key, 64 * 1024)) |value| {
            var receipt = value;
            defer receipt.deinit(a);
            const parsed = try record(a, receipt.body);
            defer parsed.deinit();
            if (!std.mem.eql(u8, parsed.value.commit_id, id) or !std.mem.eql(u8, parsed.value.request_hash, request_hash)) return error.LakeCommitIdReused;
            return .committed;
        }
        const head_key = try self.catalogKey(a, "head.json");
        defer a.free(head_key);
        var data = (try self.get(a, head_key, 64 * 1024)) orelse return .not_committed;
        defer data.deinit(a);
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        defer {
            var it = seen.keyIterator();
            while (it.next()) |k| a.free(k.*);
            seen.deinit(a);
        }
        var depth: usize = 0;
        while (depth < self.max_history_records) : (depth += 1) {
            const p = try record(a, data.body);
            defer p.deinit();
            const r = p.value;
            if (std.mem.eql(u8, r.commit_id, id)) {
                if (!std.mem.eql(u8, r.request_hash, request_hash)) return error.LakeCommitIdReused;
                return .committed;
            }
            const previous = r.previous_record orelse return .not_committed;
            const allowed = try self.catalogKey(a, "records/");
            defer a.free(allowed);
            if (!std.mem.startsWith(u8, previous, allowed) or std.mem.indexOf(u8, previous, "..") != null) return error.InvalidLakeCatalog;
            if (seen.contains(previous)) return error.InvalidLakeCatalog;
            const copy = try a.dupe(u8, previous);
            errdefer a.free(copy);
            try seen.put(a, copy, {});
            var next = (try self.get(a, previous, 64 * 1024)) orelse return .unknown;
            errdefer next.deinit(a);
            const expected_key = try self.recordKey(a, next.body);
            defer a.free(expected_key);
            if (!std.mem.eql(u8, expected_key, previous)) return error.InvalidLakeCatalog;
            data.deinit(a);
            data = next;
        }
        return .unknown;
    }
};
