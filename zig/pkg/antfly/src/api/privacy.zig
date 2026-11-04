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

//! Table privacy operations. Configuration and scan cursors share the metadata
//! definition CAS, so restart/leadership transfer never loses an acknowledged
//! rescan or acknowledges a pass for a superseded policy.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.privacy_catalog;
const key_checkpoints = @import("privacy_checkpoints.zig");
const contract = local.privacy_contract;
const projection = local.privacy_document;
const metadata = @import("../metadata/table_manager.zig");
const operation = local.api_operation;
const http = @import("public_table_http.zig");
const A = std.mem.Allocator;
const platform_time = @import("antfly_platform").time;
const platform_allocator = @import("antfly_platform").allocator;
pub const Action = enum { get, put, preview, start_scan, get_scan, findings };
pub const Arguments = struct {
    body: []const u8 = "",
    if_match: ?[]const u8 = null,
    scan_id: []const u8 = "",
    cursor: ?[]const u8 = null,
    document: ?[]const u8 = null,
    field: ?[]const u8 = null,
    category: ?[]const u8 = null,
    policy_version: ?[]const u8 = null,
    limit: u32 = 100,
};
pub const Response = struct {
    response: http.OwnedResponse,
    etag: ?[]u8 = null,
    pub fn deinit(self: *Response, a: A) void {
        self.response.deinit(a);
        if (self.etag) |tag| a.free(tag);
    }
};
fn json(a: A, status: u16, value: anytype) !Response {
    return .{ .response = .{ .status = status, .body = try std.json.Stringify.valueAlloc(a, value, .{}), .json = true } };
}
fn failure(a: A, status: u16, code: []const u8, message: []const u8) !Response {
    return json(a, status, .{ .code = code, .message = message });
}

pub fn handle(server: anytype, a: A, table: []const u8, action: Action, args: Arguments, request: operation.RequestContext) !Response {
    return execute(server, a, table, action, args, try request.platformDeadline()) catch |err| switch (err) {
        error.PrivacyPreconditionRequired => failure(a, 428, "privacy_precondition_required", "Replacing a policy requires its exact If-Match ETag."),
        error.PrivacyPreconditionFailed => failure(a, 412, "privacy_precondition_failed", "The policy changed; reload it before saving."),
        error.PrivacySampleTooLarge => failure(a, 422, "privacy_sample_too_large", "Preview samples (including identifiers) and responses are limited to 8 MiB each; request fewer or smaller documents."),
        error.PrivacyScanLimitExceeded => failure(a, 409, "privacy_scan_limit_exceeded", "Sixteen scans are active; wait for one to finish before requesting another."),
        error.PrivacyInternalAuthRequired => failure(a, 422, "privacy_internal_auth_required", "Distributed privacy inspection requires internal-service credentials."),
        error.PrivacyProtocolUpgradeRequired => failure(a, 422, "privacy_upgrade_required", "Upgrade every metadata and data node before installing a privacy policy."),
        error.PrivacyProtectiveInstallationRequired => failure(a, 422, "privacy_protection_unavailable", "Protected consumers and serving fences must be available before enabling redaction."),
        error.PrivacySynchronousIngestionRequired => failure(a, 422, "privacy_synchronous_ingestion_required", "Pre-storage redaction requires a synchronous pre-persistence transformation path."),
        error.MetadataMutationOutcomeUnknown => blk: {
            var response = try failure(a, 409, "privacy_mutation_outcome_unknown", "The mutation outcome is unknown; inspect policy and scan status before retrying.");
            response.response.metadata_mutation_outcome = .unknown;
            break :blk response;
        },
        error.TableGenerationChanged, error.TableTransitionActive => failure(a, 409, "privacy_catalog_changed", "The table changed; reload and retry."),
        error.StorageBusy, error.IndexRebuilding, error.StorageReadTemporarilyUnavailable => failure(a, 503, "privacy_storage_unavailable", "Privacy storage is rebuilding or busy; retry."),
        error.NotLeader, error.ProposalDropped, error.LeaderTransferInProgress => failure(a, 503, "privacy_leader_unavailable", "The metadata leader is unavailable; retry."),
        error.Canceled, error.Cancelled => failure(a, 499, "privacy_canceled", "Inspection was canceled."),
        error.Timeout, error.DeadlineExceeded => failure(a, 504, "privacy_timeout", "Inspection did not finish before its deadline."),
        error.OutOfMemory => return err,
        else => failure(a, 422, @errorName(err), "The privacy operation failed; no clean-scan receipt was created."),
    };
}

fn load(a: A, raw: []const u8) !?std.json.Parsed(catalog.Resource) {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    defer parsed.deinit();
    return catalog.current(a, parsed.value);
}

fn external(producer: contract.Producer) bool {
    if (producer.config != .object) return false;
    const config = producer.config.object;
    if (config.get("generator")) |generator| {
        if (generator == .object) return external(.{ .config = generator });
    }
    const provider = config.get("provider") orelse return false;
    if (provider != .string) return false;
    if (!std.mem.eql(u8, provider.string, "antfly") and !std.mem.eql(u8, provider.string, "apple") and !std.mem.eql(u8, provider.string, "mock")) return true;
    const url = config.get("url") orelse config.get("api_url") orelse .null;
    return url == .string and url.string.len != 0;
}

const RawJson = struct {
    bytes: []const u8,
    pub fn jsonStringify(self: @This(), writer: anytype) !void {
        try writer.print("{s}", .{self.bytes});
    }
};

fn statusResponse(server: anytype, a: A, table: []const u8, resource: ?catalog.Resource, code: u16, request: operation.RequestContext) !Response {
    _ = server;
    _ = table;
    _ = request;
    const value = resource orelse return json(a, code, .{ .configured = false });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const producer = try projection.resolve(arena.allocator(), value.policy, false);
    const tag = try catalog.etag(a, value);
    errdefer a.free(tag);
    var result = try json(a, code, .{ .configured = true, .policy = value.policy, .policy_version = value.policy_version[0..], .resource_version = value.resource_version, .generation = value.generation, .scan_id = value.scan_id[0..], .etag = tag, .resolved_producer = producer, .external_processing = external(producer), .coverage = RawJson{ .bytes = try catalog.coverage.response(arena.allocator(), value.coverage) } });
    result.etag = tag;
    return result;
}

const CoverageCensus = struct {
    const status = local.privacy_status;
    a: A,
    version: [64]u8,
    totals: status.Totals = .{},
    buffer: std.ArrayListUnmanaged(u8) = .empty,
    last_key: std.ArrayListUnmanaged(u8) = .empty,
    line_arena: std.heap.ArenaAllocator,
    failures: std.ArrayListUnmanaged(Failure) = .empty,
    failure_bytes: usize = 2,
    soft_deadline_ns: u64 = 0,
    yielded: bool = false,
    const Failure = struct { key: []const u8, error_code: []const u8 };
    fn start(_: ?*anyopaque) !void {}
    fn write(raw: ?*anyopaque, bytes: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        var lines = std.mem.splitScalar(u8, bytes, '\n');
        while (lines.next()) |line| {
            if (self.buffer.items.len +| line.len > @import("antfly_public_limits").max_privacy_status_row_bytes) return error.InvalidPrivacyReceipt;
            try self.buffer.appendSlice(self.a, line);
            if (lines.index == null) break;
            if (self.buffer.items.len != 0) {
                defer _ = self.line_arena.reset(.retain_capacity);
                const scratch = self.line_arena.allocator();
                const envelope = try std.json.parseFromSliceLeaky(std.json.Value, scratch, self.buffer.items, .{});
                const parsed = (try std.json.parseFromValue(status.Wire, scratch, envelope, .{ .ignore_unknown_fields = true })).value;
                const id = if (envelope == .object) envelope.object.get("_id") orelse return error.InvalidPrivacyReceipt else return error.InvalidPrivacyReceipt;
                if (id != .string) return error.InvalidPrivacyReceipt;
                try self.totals.add(self.a, self.version, parsed);
                self.last_key.clearRetainingCapacity();
                try self.last_key.appendSlice(self.a, id.string);
                if (parsed.failed and self.failures.items.len < 100 and id.string.len <= catalog.coverage.max_failure_detail_bytes) {
                    const detail = Failure{ .key = id.string, .error_code = parsed.error_code orelse "PrivacyInspectionFailed" };
                    const size = try catalog.coverage.failureDetailSize(detail);
                    if (size +| self.failure_bytes +| 1 <= catalog.coverage.max_failure_detail_bytes) {
                        try self.failures.append(self.a, .{ .key = try self.a.dupe(u8, detail.key), .error_code = try self.a.dupe(u8, detail.error_code) });
                        self.failure_bytes += size + 1;
                    }
                }
                const now = platform_time.monotonicNs();
                if (self.soft_deadline_ns == 0) {
                    self.soft_deadline_ns = now +| 2 * std.time.ns_per_s;
                } else if (now >= self.soft_deadline_ns) {
                    self.buffer.clearRetainingCapacity();
                    self.yielded = true;
                    return error.Canceled;
                }
            }
            self.buffer.clearRetainingCapacity();
        }
    }
};

fn scanResponse(a: A, code: u16, scan: catalog.Scan) !Response {
    // Public progress is a bounded DTO. Keys and owner continuation state
    // remain durable implementation details, never polling response payload.
    return json(a, code, .{
        .scan_id = scan.scan_id[0..],
        .policy_version = scan.policy_version[0..],
        .phase = scan.phase,
        .scanned = scan.scanned,
        .reprocessed = scan.reprocessed,
        .skipped = scan.skipped,
        .failed = scan.failed,
        .failures = scan.failures,
        .last_error = scan.last_error,
        .consecutive_errors = scan.consecutive_errors,
        .retry_after_millis = scan.retry_after_millis,
        .updated_at_millis = scan.updated_at_millis,
        .verification_current = scan.verification_current,
        .verification_pending = scan.verification_pending,
        .verification_failed = scan.verification_failed,
        .verification_blocked = scan.verification_blocked,
    });
}

fn coverageForRange(server: anytype, a: A, table: []const u8, resource: catalog.Resource, collect: bool, request: operation.RequestContext, from_key: []const u8, to_key: []const u8, limit: u32) ![]const u8 {
    const unknown = "{\"available\":false,\"total\":null,\"current\":null,\"pending\":null,\"failed\":null,\"with_findings\":null,\"protected_ready\":null,\"protected_blocked\":null}";
    if (!collect) return unknown;
    const reads = server.table_reads orelse return unknown;
    var census = CoverageCensus{ .a = a, .version = resource.policy_version, .line_arena = std.heap.ArenaAllocator.init(platform_allocator.reclaimingScratch(a)) };
    defer census.totals.deinit(a);
    defer census.buffer.deinit(a);
    defer census.line_arena.deinit();
    defer census.last_key.deinit(a);
    defer {
        for (census.failures.items) |failure_entry| {
            a.free(failure_entry.key);
            a.free(failure_entry.error_code);
        }
        census.failures.deinit(a);
    }
    // The census yields only after a complete row. Keep a separate hard
    // deadline for routing, setup and an oversized row; caller deadlines win.
    const budget = platform_time.monotonicNs() +| 30 * std.time.ns_per_s;
    const complete = reads.scanStream(a, table, from_key, to_key, .{
        .privacy_original = true,
        .privacy_status = true,
        .privacy_policy_version = resource.policy_version,
        .include_documents = true,
        .limit = limit,
        .execution_deadline_ns = if (request.deadline_ns) |deadline| @min(deadline, budget) else budget,
        .cancellation = request.cancellation,
    }, .read_index, .{ .context = &census, .start_fn = CoverageCensus.start, .write_fn = CoverageCensus.write }) catch |err| blk: {
        if (census.yielded) break :blk false;
        switch (err) {
            error.OutOfMemory => return err,
            error.Canceled, error.Cancelled => return err,
            error.DeadlineExceeded => {
                // Fully validated rows preceding a slow/incomplete row still
                // form a resumable prefix. Discard that row's partial bytes.
                if (census.totals.total != 0) {
                    census.yielded = true;
                    census.buffer.clearRetainingCapacity();
                    break :blk false;
                }
            },
            else => {},
        }
        std.log.debug("privacy coverage unavailable limit={} cursor_bytes={} err={s}", .{ limit, from_key.len, @errorName(err) });
        return unknown;
    };
    try request.ensureActive();
    if ((!complete and !census.yielded) or census.buffer.items.len != 0) return unknown;
    var categories: std.json.Value = .{ .object = .empty };
    var iterator = census.totals.categories.iterator();
    while (iterator.next()) |entry| try categories.object.put(a, entry.key_ptr.*, .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{entry.value_ptr.*}) });
    var fields: std.json.Value = .{ .object = .empty };
    var field_counts = census.totals.fields.iterator();
    while (field_counts.next()) |entry| try fields.object.put(a, entry.key_ptr.*, .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{entry.value_ptr.*}) });
    const total = census.totals;
    const raw = try std.json.Stringify.valueAlloc(a, .{
        .available = true,
        .total = total.total,
        .current = total.current,
        .pending = total.total - total.current - total.failed,
        // Provider failures without a source-bound durable failure receipt
        // remain unclassified pending work. Never infer a zero failure count.
        .failed = if (total.current + total.failed == total.total) @as(?u64, total.failed) else null,
        .with_findings = total.with_findings,
        .protected_ready = total.protected_ready,
        .protected_blocked = if (resource.policy.mode == .monitor) 0 else total.total - total.protected_ready,
        .categories = categories,
        .fields = fields,
        .failed_known = total.failed,
        .failures = census.failures.items,
        .next_key = if (census.yielded or total.total >= limit) @as(?[]const u8, census.last_key.items) else null,
    }, .{});
    return raw;
}

fn execute(server: anytype, a: A, table: []const u8, action: Action, args: Arguments, request: operation.RequestContext) !Response {
    if (action != .put and action != .start_scan) return executeAttempt(server, a, table, action, args, request, null);
    var uploads: key_checkpoints.RetryCache = .{ .a = a };
    defer uploads.deinit();
    for (0..8) |attempt| {
        // The request allocator may itself be an arena. Independent backing
        // ensures contention releases each attempt before loading another cut.
        var arena = std.heap.ArenaAllocator.init(platform_allocator.reclaimingScratch(a));
        defer arena.deinit();
        const scratch = arena.allocator();
        var result = executeAttempt(server, scratch, table, action, args, request, &uploads) catch |err| {
            if (err != error.PrivacyCatalogRetry) return err;
            if (attempt == 7) return error.TableGenerationChanged;
            continue;
        };
        defer result.deinit(scratch);
        var owned = result;
        owned.etag = if (result.etag) |tag| try a.dupe(u8, tag) else null;
        errdefer if (owned.etag) |tag| a.free(tag);
        owned.response.body = try a.dupe(u8, result.response.body);
        return owned;
    }
    unreachable;
}

fn executeAttempt(server: anytype, a: A, table: []const u8, action: Action, args: Arguments, request: operation.RequestContext, uploads: ?*key_checkpoints.RetryCache) !Response {
    try request.ensureActive();
    if (comptime @hasDecl(@TypeOf(server.source), "forwardPrivacyMutation")) {
        if (action == .put or action == .start_scan) if (try server.source.forwardPrivacyMutation(a, table, args.body, args.if_match, action == .start_scan, request)) |response| return response;
    }
    const before = (try server.loadOwnedTableRecord(a, table)) orelse return failure(a, 404, "table_not_found", "Table not found.");
    defer metadata.freeTable(a, before);
    var stored = try load(a, before.indexes_json);
    defer if (stored) |*value| value.deinit();
    if (action == .get) return statusResponse(server, a, table, if (stored) |value| value.value else null, 200, request);
    if (action == .preview or action == .put or action == .start_scan) {
        var schema = try local.schema_mod.parseValidatedTableSchema(a, before.schema_json);
        defer schema.deinit(a);
        if (schema.storage_mode != .document or schema.external_base_source != null) return error.PrivacyRelationalStorageUnsupported;
    }
    if (action == .preview) return preview(server, a, table, args.body, request);
    if (action == .put) {
        var policy = try std.json.parseFromSlice(contract.Policy, a, args.body, .{});
        defer policy.deinit();
        if (policy.value.mode == .redact_before_storage) return error.PrivacySynchronousIngestionRequired;
        try server.ensurePrivacyAdmission(request);
        var resource = try catalog.next(a, if (stored) |value| value.value else null, policy.value, args.if_match);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        // Allocate durable scan identity before the same policy-install CAS.
        if (stored == null or !std.mem.eql(u8, &stored.?.value.policy_version, &resource.policy_version)) {
            _ = try catalog.startScan(arena.allocator(), &resource, "", "", 100);
        }
        const term = (try server.source.privacyAuthorityTerm()) orelse return error.NotLeader;
        var checkpoint = try key_checkpoints.Context.init(server, a, before.table_id, term, if (stored) |value| value.value else resource, request);
        if (uploads) |cache| checkpoint.reuseUploads(cache);
        try checkpoint.encodeResource(&resource);
        const raw = try catalog.install(a, before.indexes_json, resource);
        defer a.free(raw);
        try @import("indexes.zig").validateArtifactEnrichmentsForTableIndexesJson(a, raw);
        try request.ensureActive();
        var replacement = before;
        replacement.indexes_json = raw;
        server.source.replaceTableDefinition(before, replacement) catch |err| {
            if (err == error.TableGenerationChanged) return error.PrivacyCatalogRetry;
            return err;
        };
        try acknowledgePolicy(server, a, table, resource, request);
        if (server.table_writes) |writes| _ = try writes.requestTableStructuralReconcile(a, table);
        return statusResponse(server, a, table, resource, 202, request);
    }
    const current = if (stored) |value| value.value else return failure(a, 404, "privacy_not_configured", "No privacy policy is configured.");
    if (action == .get_scan) {
        for (current.scans) |scan| if (std.mem.eql(u8, &scan.scan_id, args.scan_id)) {
            var result = scan;
            if (!std.mem.eql(u8, &scan.policy_version, &current.policy_version)) result.phase = .superseded;
            return scanResponse(a, 200, result);
        };
        return failure(a, 404, "privacy_scan_not_found", "Scan not found or outside its retention period.");
    }
    if (action == .start_scan) {
        const Start = struct { from_key: []const u8 = "", to_key: []const u8 = "", limit: u32 = 100 };
        var input = try std.json.parseFromSlice(Start, a, if (args.body.len == 0) "{}" else args.body, .{});
        defer input.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var resource = current;
        const scan = try catalog.startScan(arena.allocator(), &resource, input.value.from_key, input.value.to_key, input.value.limit);
        const term = (try server.source.privacyAuthorityTerm()) orelse return error.NotLeader;
        var checkpoint = try key_checkpoints.Context.init(server, a, before.table_id, term, current, request);
        if (uploads) |cache| checkpoint.reuseUploads(cache);
        try checkpoint.encodeResource(&resource);
        const raw = try catalog.replaceResource(a, before.indexes_json, resource);
        defer a.free(raw);
        var replacement = before;
        replacement.indexes_json = raw;
        try request.ensureActive();
        server.source.replaceTableDefinition(before, replacement) catch |err| {
            if (err == error.TableGenerationChanged) return error.PrivacyCatalogRetry;
            return err;
        };
        return scanResponse(a, 202, scan);
    }
    if (args.body.len != 0) {
        var input = try std.json.parseFromSlice(FindingsRequest, a, args.body, .{});
        defer input.deinit();
        return findings(server, a, table, current, .{
            .cursor = input.value.cursor,
            .document = input.value.document,
            .field = input.value.field,
            .category = input.value.category,
            .policy_version = input.value.policy_version,
            .limit = input.value.limit,
        }, request);
    }
    return findings(server, a, table, current, args, request);
}

// Progress is independently mutable. Acknowledge the installed resource identity,
// never its incidental coverage counters or worker cursor JSON.
fn acknowledged(installed: catalog.Resource, visible: catalog.Resource) bool {
    return visible.resource_version > installed.resource_version or
        (visible.resource_version == installed.resource_version and std.mem.eql(u8, &visible.policy_version, &installed.policy_version));
}
fn acknowledgePolicy(server: anytype, a: A, table: []const u8, installed: catalog.Resource, request: operation.RequestContext) !void {
    const deadline = platform_time.monotonicNs() +| 10 * std.time.ns_per_s;
    while (true) {
        try request.ensureActive();
        var arena = std.heap.ArenaAllocator.init(platform_allocator.reclaimingScratch(a));
        defer arena.deinit();
        const scratch = arena.allocator();
        if (try server.loadOwnedTableRecord(scratch, table)) |record| {
            if (try load(scratch, record.indexes_json)) |visible| {
                if (acknowledged(installed, visible.value)) return;
            }
        }
        if (platform_time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        var delay = std.posix.timespec{ .sec = 0, .nsec = 10 * std.time.ns_per_ms };
        _ = std.posix.system.nanosleep(&delay, null);
    }
}

const Sample = struct { keys: ?[]const []const u8 = null, limit: u32 = 10 };
const Preview = struct { policy: contract.Policy, sample: Sample = .{} };
const Document = struct { key: []const u8, value: std.json.Value };

const preview_sample_bytes = @import("antfly_public_limits").privacy_preview_sample_bytes;

// Count escaped JSON without materializing it. Stop at the budget even when a
// single string is repeated by many findings.
const PreviewSize = struct {
    remaining: usize,
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },
    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *PreviewSize = @alignCast(@fieldParentPtr("writer", w));
        var count: usize = 0;
        for (data, 0..) |part, i| {
            const size = std.math.mul(usize, part.len, if (i + 1 == data.len) splat else 1) catch return error.WriteFailed;
            if (size > self.remaining) return error.WriteFailed;
            self.remaining -= size;
            count += size;
        }
        return count;
    }
    fn add(self: *PreviewSize, value: anytype) !void {
        std.json.Stringify.value(value, .{}, &self.writer) catch return error.PrivacySampleTooLarge;
        self.writer.flush() catch return error.PrivacySampleTooLarge;
    }
};

const PreviewSamples = struct {
    a: A,
    transient: A,
    documents: std.ArrayList(Document) = .empty,
    buffer: std.ArrayListUnmanaged(u8) = .empty,
    remaining: usize = preview_sample_bytes,
    limit: usize,
    stopped: bool = false,
    failure: ?anyerror = null,

    fn deinit(self: *PreviewSamples) void {
        self.buffer.deinit(self.transient);
    }
    fn start(_: ?*anyopaque) !void {}
    fn row(self: *PreviewSamples) !void {
        if (self.buffer.items.len == 0) return;
        const value = try std.json.parseFromSliceLeaky(std.json.Value, self.a, self.buffer.items, .{ .allocate = .alloc_always });
        if (value != .object) return error.InvalidPrivacySample;
        const id = value.object.get("_id") orelse return error.InvalidPrivacySample;
        if (id != .string) return error.InvalidPrivacySample;
        try self.documents.append(self.a, .{ .key = id.string, .value = value });
        self.buffer.clearRetainingCapacity();
        if (self.documents.items.len == self.limit) {
            self.stopped = true;
            return error.Canceled;
        }
    }
    fn append(self: *PreviewSamples, bytes: []const u8) !void {
        var rest = bytes;
        while (rest.len != 0) {
            const end = std.mem.indexOfScalar(u8, rest, '\n');
            const size = if (end) |n| n + 1 else rest.len;
            if (size > self.remaining) return error.PrivacySampleTooLarge;
            self.remaining -= size;
            try self.buffer.appendSlice(self.transient, rest[0 .. size - @intFromBool(end != null)]);
            rest = rest[size..];
            if (end != null) try self.row();
        }
    }
    fn write(raw: ?*anyopaque, bytes: []const u8) !void {
        const self: *PreviewSamples = @ptrCast(@alignCast(raw.?));
        self.append(bytes) catch |err| {
            if (!self.stopped) self.failure = err;
            return error.Canceled;
        };
    }
};

fn sampleDocuments(server: anytype, a: A, table: []const u8, sample: Sample, request: operation.RequestContext, missing_ok: bool) ![]Document {
    if (sample.limit == 0 or sample.limit > 50) return error.InvalidPrivacySample;
    const source = server.table_reads orelse return error.PrivacyReadUnavailable;
    var documents: std.ArrayList(Document) = .empty;
    if (sample.keys) |keys| {
        if (keys.len == 0 or keys.len > 50) return error.InvalidPrivacySample;
        var size = PreviewSize{ .remaining = preview_sample_bytes };
        for (keys, 0..) |key, i| {
            for (keys[0..i]) |prior| if (std.mem.eql(u8, prior, key)) return error.InvalidPrivacySample;
            try request.ensureActive();
            try size.add(key);
            var found = (try source.lookup(a, table, key, .{ .privacy_original = true, .execution_deadline_ns = request.deadline_ns, .cancellation = request.cancellation }, .read_index)) orelse {
                if (missing_ok) continue;
                return error.PrivacySampleDocumentMissing;
            };
            defer found.deinit(a);
            if (found.json.len > size.remaining) return error.PrivacySampleTooLarge;
            size.remaining -= found.json.len;
            const value = (try std.json.parseFromSlice(std.json.Value, a, found.json, .{ .allocate = .alloc_always })).value;
            try documents.append(a, .{ .key = key, .value = value });
        }
    } else {
        var samples = PreviewSamples{ .a = a, .transient = platform_allocator.reclaimingScratch(a), .limit = sample.limit };
        defer samples.deinit();
        const complete = source.scanStream(a, table, "", "", .{ .privacy_original = true, .limit = sample.limit, .include_documents = true, .execution_deadline_ns = request.deadline_ns, .cancellation = request.cancellation }, .read_index, .{ .context = &samples, .start_fn = PreviewSamples.start, .write_fn = PreviewSamples.write }) catch |err| blk: {
            if (samples.stopped or samples.failure != null) {
                if (err == error.Canceled or err == error.Cancelled) break :blk false;
            }
            return err;
        };
        try request.ensureActive();
        if (samples.failure) |err| return err;
        if (!complete and !samples.stopped) return error.PrivacyReadUnavailable;
        if (!samples.stopped) samples.row() catch |err| {
            if (!samples.stopped) return err;
        };
        documents = samples.documents;
    }
    return documents.items;
}

fn preview(server: anytype, a: A, table: []const u8, body: []const u8, request: operation.RequestContext) !Response {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const input = try std.json.parseFromSlice(Preview, scratch, body, .{});
    try input.value.policy.validate();
    const producer = try projection.resolve(scratch, input.value.policy, true);
    const config = try std.json.Stringify.valueAlloc(scratch, producer.config, .{});
    const documents = try sampleDocuments(server, scratch, table, input.value.sample, request, false);
    const runtime = try local.asset_producer_runtime.Runtime.createOwned(scratch, server.inferenceIo(), .{
        .provider_runtime = &server.embedding_provider_runtime,
        .antfly_provider = server.antfly_provider,
        .inference_api_url = server.configuredInferenceAPIURL(),
        .secret_store = server.cfg.secret_store,
        .source_table = table,
    });
    defer runtime.deinit();
    const detector = runtime.producer().withInvocationContext(.{ .io = server.inferenceIo(), .cancellation = request.cancellation, .deadline_ns = request.deadline_ns, .max_response_bytes = 8 * 1024 * 1024 });
    const inputs = try scratch.alloc(local.asset_producer_runtime.Request, documents.len);
    for (documents, inputs) |document, *input_request| input_request.* = .{ .producer_type = if (producer.type == .extractor) .extractor else .generator, .config_json = config, .source_text = try std.json.Stringify.valueAlloc(scratch, document.value, .{}), .content_type = "application/json" };
    var batch = try detector.produceBatchReported(scratch, inputs);
    defer batch.deinit(scratch);
    var output: std.json.Value = .{ .array = std.array_list.Managed(std.json.Value).init(scratch) };
    for (documents, batch.items) |document, item| {
        try request.ensureActive();
        const projected = switch (item.result) {
            .value => |value| value,
            .item_error => |problem| return problem.cause,
        };
        const value = (try std.json.parseFromSlice(std.json.Value, scratch, projected, .{})).value;
        const fields = value.object.get("_privacy").?.object.get("fields").?;
        for (fields.array.items) |*field| {
            const name = field.object.get("field").?.string;
            const original = document.value.object.get(name) orelse std.json.Value{ .string = "" };
            try field.object.put(scratch, "original", if (original == .null) .{ .string = "" } else original);
            for (field.object.getPtr("findings").?.array.items) |*finding| {
                try finding.object.put(scratch, "document", .{ .string = document.key });
                try finding.object.put(scratch, "policy_version", .{ .string = try scratch.dupe(u8, &(try input.value.policy.identity(scratch))) });
            }
        }
        var row: std.json.Value = .{ .object = .empty };
        try row.object.put(scratch, "document", .{ .string = document.key });
        try row.object.put(scratch, "fields", fields);
        try output.array.append(row);
    }
    const result = .{ .policy_version = (try input.value.policy.identity(scratch))[0..], .sampled = documents.len, .complete_table_scan = false, .external_processing = external(producer), .documents = output };
    var size = PreviewSize{ .remaining = @import("antfly_public_limits").privacy_preview_response_bytes };
    try size.add(result);
    return json(a, 200, result);
}

const FindingsRequest = struct {
    cursor: ?[]const u8 = null,
    document: ?[]const u8 = null,
    field: ?[]const u8 = null,
    category: ?[]const u8 = null,
    policy_version: ?[]const u8 = null,
    limit: u32 = 100,
};
const Cursor = struct { policy_version: [64]u8, document: []const u8 = "", offset: usize = 0, inclusive: bool = false, filter: [64]u8 };
// A binary payload avoids JSON escaping amplification for arbitrary document
// identifiers. Bound both issuance and decoding with the public document budget.
const cursor_prefix = "p1.";
const cursor_header_bytes = @import("antfly_public_limits").privacy_cursor_header_bytes;
const cursor_max_key_bytes = @import("http_server.zig").public_api_max_request_body_bytes;
const cursor_max_payload_bytes = @import("antfly_public_limits").max_privacy_cursor_payload_bytes;
fn cursorEncode(a: A, cursor: Cursor) ![]u8 {
    if (cursor.document.len > cursor_max_key_bytes) return error.InvalidPrivacyCursor;
    const raw = try a.alloc(u8, cursor_header_bytes + cursor.document.len);
    defer a.free(raw);
    @memcpy(raw[0..64], &cursor.policy_version);
    @memcpy(raw[64..128], &cursor.filter);
    std.mem.writeInt(u64, raw[128..136], @intCast(cursor.offset), .little);
    raw[136] = @intFromBool(cursor.inclusive);
    @memcpy(raw[cursor_header_bytes..], cursor.document);
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const out = try a.alloc(u8, cursor_prefix.len + encoder.calcSize(raw.len));
    @memcpy(out[0..cursor_prefix.len], cursor_prefix);
    _ = encoder.encode(out[cursor_prefix.len..], raw);
    return out;
}
fn cursorDecode(a: A, raw: []const u8) !Cursor {
    const modern = std.mem.startsWith(u8, raw, cursor_prefix);
    const encoded = if (modern) raw[cursor_prefix.len..] else raw;
    // Legacy JSON tokens remain readable across upgrades. Their key may expand
    // sixfold in JSON before base64 encoding; modern tokens have no expansion.
    const maximum = if (modern) cursor_max_payload_bytes else 6 * cursor_max_key_bytes + 2048;
    const decoder = std.base64.url_safe_no_pad.Decoder;
    if (encoded.len > std.base64.url_safe_no_pad.Encoder.calcSize(maximum)) return error.InvalidPrivacyCursor;
    const size = decoder.calcSizeForSlice(encoded) catch return error.InvalidPrivacyCursor;
    if (size > maximum) return error.InvalidPrivacyCursor;
    const decoded = try a.alloc(u8, size);
    defer a.free(decoded);
    decoder.decode(decoded, encoded) catch return error.InvalidPrivacyCursor;
    if (!modern) {
        var parsed = std.json.parseFromSlice(Cursor, a, decoded, .{}) catch return error.InvalidPrivacyCursor;
        defer parsed.deinit();
        if (parsed.value.document.len > cursor_max_key_bytes) return error.InvalidPrivacyCursor;
        var result = parsed.value;
        result.document = try a.dupe(u8, parsed.value.document);
        return result;
    }
    if (decoded.len < cursor_header_bytes or decoded[136] > 1) return error.InvalidPrivacyCursor;
    return .{
        .policy_version = decoded[0..64].*,
        .filter = decoded[64..128].*,
        .offset = std.math.cast(usize, std.mem.readInt(u64, decoded[128..136], .little)) orelse return error.InvalidPrivacyCursor,
        .inclusive = decoded[136] == 1,
        .document = try a.dupe(u8, decoded[cursor_header_bytes..]),
    };
}

test "privacy cursors round trip large escaped keys and reject malformed payloads" {
    const a = std.testing.allocator;
    const key_bytes = try a.alloc(u8, 32000);
    defer a.free(key_bytes);
    for (key_bytes, 0..) |*byte, i| byte.* = switch (i % 4) {
        0 => '"',
        1 => '\\',
        2 => 0,
        else => 'x',
    };
    const cursor = Cursor{ .policy_version = contract.fingerprint("policy"), .filter = contract.fingerprint("filter"), .document = key_bytes, .offset = 231, .inclusive = true };
    const encoded = try cursorEncode(a, cursor);
    defer a.free(encoded);
    try std.testing.expect(encoded.len > 4096);
    const decoded = try cursorDecode(a, encoded);
    defer a.free(decoded.document);
    try std.testing.expectEqualStrings(key_bytes, decoded.document);
    try std.testing.expectEqualDeep(cursor, decoded);
    for ([_][]const u8{ "p1.", "p1.!", "???" }) |bad| try std.testing.expectError(error.InvalidPrivacyCursor, cursorDecode(a, bad));
    const legacy_json = try std.json.Stringify.valueAlloc(a, cursor, .{});
    defer a.free(legacy_json);
    const legacy = try a.alloc(u8, std.base64.url_safe_no_pad.Encoder.calcSize(legacy_json.len));
    defer a.free(legacy);
    _ = std.base64.url_safe_no_pad.Encoder.encode(legacy, legacy_json);
    const legacy_decoded = try cursorDecode(a, legacy);
    defer a.free(legacy_decoded.document);
    try std.testing.expectEqualDeep(cursor, legacy_decoded);
}

/// Collect only a bounded identifier prefix. The independent line scratch is
/// released before any source/manifest read, including for an oversized ID.
const FindingsIDs = struct {
    a: A,
    transient: A,
    keys: std.ArrayList([]const u8) = .empty,
    buffer: std.ArrayListUnmanaged(u8) = .empty,
    arena: std.heap.ArenaAllocator,
    bytes: usize = 0,
    budget: usize = @import("antfly_public_limits").privacy_findings_page_bytes,
    stopped: bool = false,

    fn init(a: A) FindingsIDs {
        const transient = platform_allocator.reclaimingScratch(a);
        return .{ .a = a, .transient = transient, .arena = std.heap.ArenaAllocator.init(transient) };
    }
    fn deinit(self: *FindingsIDs) void {
        self.buffer.deinit(self.transient);
        self.arena.deinit();
    }
    fn start(_: ?*anyopaque) !void {}
    fn write(raw: ?*anyopaque, bytes: []const u8) !void {
        const self: *FindingsIDs = @ptrCast(@alignCast(raw.?));
        var lines = std.mem.splitScalar(u8, bytes, '\n');
        while (lines.next()) |part| {
            if (part.len > 6 * cursor_max_key_bytes + 64 -| self.buffer.items.len) return error.InvalidPrivacyCursor;
            try self.buffer.appendSlice(self.transient, part);
            if (lines.index == null) break;
            if (self.buffer.items.len == 0) continue;
            defer self.buffer.clearRetainingCapacity();
            if (self.keys.items.len != 0 and self.buffer.items.len > self.budget -| self.bytes) {
                self.stopped = true;
                return error.Canceled;
            }
            defer _ = self.arena.reset(.retain_capacity);
            const row = try std.json.parseFromSliceLeaky(struct { _id: []const u8 }, self.arena.allocator(), self.buffer.items, .{ .ignore_unknown_fields = true });
            if (row._id.len > cursor_max_key_bytes) return error.InvalidPrivacyCursor;
            try self.keys.append(self.a, try self.a.dupe(u8, row._id));
            self.bytes +|= self.buffer.items.len + 1;
            if (self.keys.items.len == 50 or self.bytes >= self.budget) {
                self.stopped = true;
                return error.Canceled;
            }
        }
    }
};

test "privacy findings ID enumeration budgets wire bytes and admits one oversized ID" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var ids = FindingsIDs.init(a);
    defer ids.deinit();
    ids.budget = 30;
    try std.testing.expectError(error.Canceled, FindingsIDs.write(&ids, "{\"_id\":\"first\"}\n{\"_id\":\"second\"}\n"));
    try std.testing.expect(ids.stopped);
    try std.testing.expectEqual(@as(usize, 1), ids.keys.items.len);
    try std.testing.expectEqualStrings("first", ids.keys.items[0]);
    var oversized = FindingsIDs.init(a);
    defer oversized.deinit();
    oversized.budget = 1;
    try FindingsIDs.write(&oversized, "{\"_id\":");
    try std.testing.expectError(error.Canceled, FindingsIDs.write(&oversized, "\"large\"}\n"));
    try std.testing.expectEqualStrings("large", oversized.keys.items[0]);
    try std.testing.expect(oversized.stopped);
}

fn findings(server: anytype, a: A, table: []const u8, resource: catalog.Resource, args: Arguments, request: operation.RequestContext) !Response {
    if (args.limit == 0 or args.limit > 1000) return error.InvalidPrivacyPage;
    if (args.policy_version) |version| if (!std.mem.eql(u8, version, &resource.policy_version)) return failure(a, 410, "privacy_policy_superseded", "Findings for this policy version are no longer current.");
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const filter_raw = try std.json.Stringify.valueAlloc(scratch, .{ table, args.document, args.field, args.category }, .{});
    const filter = contract.fingerprint(filter_raw);
    const cursor = if (args.cursor) |value| try cursorDecode(scratch, value) else Cursor{ .policy_version = resource.policy_version, .filter = filter };
    if (!std.mem.eql(u8, &cursor.policy_version, &resource.policy_version) or !std.mem.eql(u8, &filter, &cursor.filter)) return error.InvalidPrivacyCursor;
    const source = server.table_reads orelse return error.PrivacyReadUnavailable;
    var documents: std.ArrayList([]const u8) = .empty;
    var more_ids = false;
    if (args.document) |key| {
        try documents.append(scratch, key);
    } else {
        var ids = FindingsIDs.init(scratch);
        defer ids.deinit();
        const complete = source.scanStream(scratch, table, cursor.document, "", .{ .privacy_original = true, .inclusive_from = cursor.inclusive, .limit = 50, .include_documents = false, .execution_deadline_ns = request.deadline_ns, .cancellation = request.cancellation }, .read_index, .{ .context = &ids, .start_fn = FindingsIDs.start, .write_fn = FindingsIDs.write }) catch |err| blk: {
            if (ids.stopped and (err == error.Canceled or err == error.Cancelled)) break :blk false;
            return err;
        };
        try request.ensureActive();
        if (!complete and !ids.stopped) return error.PrivacyReadUnavailable;
        if (ids.buffer.items.len != 0) return error.InvalidPrivacyReceipt;
        documents = ids.keys;
        more_ids = ids.stopped or documents.items.len == 50;
    }
    var output: std.json.Value = .{ .array = std.array_list.Managed(std.json.Value).init(scratch) };
    var examined: usize = 0;
    var pending: usize = 0;
    var continuation: ?[]const u8 = null;
    var processed_bytes: usize = 0;
    const page_byte_budget = @import("antfly_public_limits").privacy_findings_page_bytes;
    var output_bytes: usize = 0;
    for (documents.items) |key| {
        if (examined != 0 and processed_bytes >= page_byte_budget) {
            continuation = try cursorEncode(scratch, .{ .policy_version = resource.policy_version, .filter = filter, .document = key, .inclusive = true });
            break;
        }
        // Request arenas cannot reclaim interleaved per-document allocations.
        // Give each document independently releasable backing.
        var document_arena = std.heap.ArenaAllocator.init(platform_allocator.reclaimingScratch(a));
        defer document_arena.deinit();
        const doc = document_arena.allocator();
        try request.ensureActive();
        examined += 1;
        var manifest = (try source.documentArtifactManifestControlled(doc, table, key, resource.artifact_name, .read_index, .{ .execution_deadline_ns = request.deadline_ns, .cancellation = request.cancellation })) orelse {
            pending += 1;
            continue;
        };
        defer manifest.deinit(doc);
        processed_bytes +|= manifest.manifest_json.len;
        const value = (try std.json.parseFromSlice(std.json.Value, doc, manifest.manifest_json, .{ .allocate = .alloc_always })).value;
        const receipt = if (value == .object) value.object.get("_privacy") orelse {
            pending += 1;
            continue;
        } else return error.InvalidPrivacyReceipt;
        if (receipt != .object) return error.InvalidPrivacyReceipt;
        const version = receipt.object.get("policy_version") orelse return error.InvalidPrivacyReceipt;
        if (version != .string or !std.mem.eql(u8, version.string, &resource.policy_version)) {
            pending += 1;
            continue;
        }
        // Re-read the authoritative source around artifact inspection; a source
        // edit racing this page cannot turn an old receipt into a current one.
        var original = (try source.lookup(doc, table, key, .{ .privacy_original = true, .execution_deadline_ns = request.deadline_ns, .cancellation = request.cancellation }, .read_index)) orelse {
            pending += 1;
            continue;
        };
        defer original.deinit(doc);
        processed_bytes +|= original.json.len;
        try request.ensureActive();
        const original_value = (try std.json.parseFromSlice(std.json.Value, doc, original.json, .{ .parse_numbers = false })).value;
        if (!try currentReceipt(doc, resource, original_value, receipt)) {
            pending += 1;
            continue;
        }
        var ordinal: usize = 0;
        for (receipt.object.get("fields").?.array.items) |field| {
            const name = field.object.get("field").?.string;
            const source_value = original_value.object.get(name) orelse .null;
            const source_text = if (source_value == .string) source_value.string else "";
            const proof = try std.json.parseFromValue(contract.Receipt, doc, field.object.get("receipt").?, .{});
            if (!proof.value.current(resource.policy_version, source_text, try resource.policy.revision())) {
                pending += 1;
                break;
            }
            for (field.object.get("findings").?.array.items) |finding| {
                const index = ordinal;
                ordinal += 1;
                if (std.mem.eql(u8, cursor.document, key) and index < cursor.offset) continue;
                if (args.field) |wanted| if (!std.mem.eql(u8, wanted, name)) continue;
                if (args.category) |wanted| if (!std.mem.eql(u8, wanted, finding.object.get("category").?.string)) continue;
                if (output.array.items.len == args.limit) {
                    continuation = try cursorEncode(scratch, .{ .policy_version = resource.policy_version, .filter = filter, .document = key, .offset = index, .inclusive = true });
                    break;
                }
                var entry = finding;
                try entry.object.put(doc, "document", .{ .string = key });
                try entry.object.put(doc, "policy_version", version);
                try entry.object.put(doc, "source_fingerprint", .{ .string = try doc.dupe(u8, &proof.value.source_fingerprint) });
                try entry.object.put(doc, "producer_revision", .{ .string = proof.value.producer_revision });
                const encoded = try std.json.Stringify.valueAlloc(doc, entry, .{});
                if (output.array.items.len != 0 and output_bytes +| encoded.len > page_byte_budget) {
                    continuation = try cursorEncode(scratch, .{ .policy_version = resource.policy_version, .filter = filter, .document = key, .offset = index, .inclusive = true });
                    break;
                }
                output_bytes +|= encoded.len;
                const retained = try std.json.parseFromSlice(std.json.Value, scratch, encoded, .{ .allocate = .alloc_always });
                try output.array.append(retained.value);
            }
            if (continuation != null) break;
        }
        if (continuation != null) break;
    }
    if (continuation == null and args.document == null and more_ids and documents.items.len != 0) continuation = try cursorEncode(scratch, .{ .policy_version = resource.policy_version, .filter = filter, .document = documents.items[documents.items.len - 1] });
    return json(a, 200, .{ .policy_version = resource.policy_version[0..], .findings = output, .next_cursor = continuation, .examined_documents = examined, .pending_documents = pending });
}

// Validate the COMPLETE document before adding any findings to a page. A
// valid first field must never make a stale second field partially visible.
fn currentReceipt(a: A, resource: catalog.Resource, original: std.json.Value, receipt: std.json.Value) !bool {
    if (original != .object or receipt != .object) return error.InvalidPrivacyReceipt;
    const recorded = receipt.object.get("source_fingerprint") orelse return error.InvalidPrivacyReceipt;
    const canonical = try std.json.Stringify.valueAlloc(a, original, .{});
    defer a.free(canonical);
    if (recorded != .string or !std.mem.eql(u8, recorded.string, &contract.fingerprint(canonical))) return false;
    const fields = receipt.object.get("fields") orelse return error.InvalidPrivacyReceipt;
    if (fields != .array or fields.array.items.len != resource.policy.fields.len) return error.InvalidPrivacyReceipt;
    for (resource.policy.fields) |name| {
        var matched: usize = 0;
        for (fields.array.items) |field| {
            if (field != .object) return error.InvalidPrivacyReceipt;
            const found = field.object.get("field") orelse return error.InvalidPrivacyReceipt;
            if (found != .string) return error.InvalidPrivacyReceipt;
            if (!std.mem.eql(u8, found.string, name)) continue;
            matched += 1;
            const source = original.object.get(name) orelse .null;
            if (source != .string and source != .null) return false;
            const proof = try std.json.parseFromValue(contract.Receipt, a, field.object.get("receipt") orelse return error.InvalidPrivacyReceipt, .{});
            defer proof.deinit();
            if (!proof.value.current(resource.policy_version, if (source == .string) source.string else "", try resource.policy.revision())) return false;
            const findings_value = field.object.get("findings") orelse return error.InvalidPrivacyReceipt;
            if (findings_value != .array) return error.InvalidPrivacyReceipt;
            for (findings_value.array.items) |finding| {
                const parsed = try std.json.parseFromValue(contract.Finding, a, finding, .{});
                defer parsed.deinit();
            }
        }
        if (matched != 1) return error.InvalidPrivacyReceipt;
    }
    return true;
}

/// One bounded pass, admitted by the server supervisor only on a local metadata
/// leader. Source writes remain routed through the current shard leaders.
fn nextRunnableScan(scans: []const catalog.Scan, now: i64) ?usize {
    var selected: ?usize = null;
    for (scans, 0..) |scan, index| {
        if ((scan.phase != .queued and scan.phase != .running and scan.phase != .processing) or scan.retry_after_millis > now) continue;
        if (selected == null or scan.updated_at_millis < scans[selected.?].updated_at_millis) selected = index;
    }
    return selected;
}

test "privacy scans schedule the oldest runnable job within a table" {
    const id = contract.fingerprint("policy");
    var scans = [_]catalog.Scan{
        .{ .scan_id = id, .policy_version = id, .phase = .running, .updated_at_millis = 100 },
        .{ .scan_id = id, .policy_version = id, .updated_at_millis = 0 },
        .{ .scan_id = id, .policy_version = id, .phase = .succeeded, .updated_at_millis = -1 },
    };
    try std.testing.expectEqual(@as(?usize, 1), nextRunnableScan(&scans, 200));
    scans[1].updated_at_millis = 200;
    try std.testing.expectEqual(@as(?usize, 0), nextRunnableScan(&scans, 200));
    scans[0].retry_after_millis = 300;
    try std.testing.expectEqual(@as(?usize, 1), nextRunnableScan(&scans, 200));
    scans[1].phase = .failed;
    try std.testing.expectEqual(@as(?usize, null), nextRunnableScan(&scans, 200));
}

pub fn advanceOnce(server: anytype, a: A) !void {
    if (!server.cfg.catalog_publication_authority_available) return;
    if (server.cfg.deployment_mode != .standalone and !server.cfg.privacy_scan_coordinator) return;
    try key_checkpoints.collect(server, a);
    const writes = server.table_writes orelse return;
    const now: i64 = @intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_ms));
    const work = (server.source.nextPrivacyWork(a, now) catch |err| {
        std.log.warn("privacy progress selection failed err={s}", .{@errorName(err)});
        return err;
    }) orelse return;
    const table = work.table;
    defer metadata.freeTable(a, table);
    var current = (load(a, table.indexes_json) catch |err| {
        std.log.warn("privacy progress decode failed bytes={} err={s}", .{ table.indexes_json.len, @errorName(err) });
        return err;
    }) orelse return error.InvalidPrivacyCatalog;
    defer current.deinit();
    var resource = current.value;
    var checkpoint = try key_checkpoints.Context.init(server, a, table.table_id, work.term, resource, .{});
    var chosen = .{ .scan = nextRunnableScan(resource.scans, now) };
    if (resource.coverage.retry_after_millis <= now and
        (chosen.scan == null or resource.coverage.updated_at_millis < resource.scans[chosen.scan.?].updated_at_millis)) chosen.scan = null;
    {
        if (chosen.scan == null) {
            if (resource.coverage.started_at_millis == 0) resource.coverage.started_at_millis = now;
            const cursor = try checkpoint.decode(resource.coverage.cursor orelse "");
            const page = try coverageForRange(server, a, table.name, resource, true, .{}, cursor, "", resource.coverage.page_size);
            const completed_at: i64 = @intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_ms));
            try catalog.coverage.accept(a, &resource.coverage, page, completed_at);
            try checkpoint.encodeResource(&resource);
            try persistScan(server, a, table, resource, work.term);
            return;
        }
        for (resource.scans[chosen.scan.?..][0..1], chosen.scan.?..) |stored_scan, index| {
            const scan = try checkpoint.decodeScan(stored_scan);
            if (scan.phase != .queued and scan.phase != .running and scan.phase != .processing) continue;
            if (scan.retry_after_millis > now) continue;
            const scans = try a.dupe(catalog.Scan, resource.scans);
            defer a.free(scans);
            resource.scans = scans;
            const updated = &scans[index];
            updated.* = scan;
            updated.updated_at_millis = now;
            if (!std.mem.eql(u8, &scan.policy_version, &resource.policy_version)) {
                updated.phase = .superseded;
            } else if (scan.phase == .processing) {
                var verification: ?std.json.Parsed(std.json.Value) = null;
                defer if (verification) |*value| value.deinit();
                const available = if (scan.from_key.len == 0 and scan.to_key.len == 0)
                    acceptCoverageVerification(updated, resource.coverage, now)
                else blk: {
                    const raw_status = try coverageForRange(server, a, table.name, resource, true, .{}, scan.verification_cursor orelse scan.from_key, scan.to_key, scan.verification_page_size);
                    verification = try std.json.parseFromSlice(std.json.Value, a, raw_status, .{});
                    break :blk try acceptVerification(a, updated, verification.?.value.object, now);
                };
                if (available) {
                    if (updated.verification_cursor == null) {
                        updated.scanned = @max(updated.scanned, updated.verification_current +| updated.verification_pending +| updated.verification_failed);
                        if (updated.verification_pending == 0 and updated.verification_failed != 0) {
                            updated.phase = .failed;
                            updated.failed = updated.verification_failed;
                            updated.last_error = "PrivacyInspectionFailed";
                        } else if (updated.verification_pending == 0 and updated.verification_blocked == 0) {
                            if (resource.policy.mode == .redact_for_search_and_ai) {
                                if (try verifyServing(server, a, table.name, resource.policy_version, updated, now)) updated.phase = .succeeded;
                            } else updated.phase = .succeeded;
                            if (updated.phase == .succeeded) updated.last_error = null;
                        }
                        if (updated.phase == .processing) {
                            updated.verification_page_size = 100;
                            updated.verification_current = 0;
                            updated.verification_pending = 0;
                            updated.verification_failed = 0;
                            updated.verification_blocked = 0;
                            updated.failures = .null;
                        }
                    }
                }
                try checkpoint.encodeResource(&resource);
                try persistScan(server, a, table, resource, work.term);
                return;
            } else {
                const Resumes = []const local.storage_db_selected_root.db.types.DocumentArtifactReprocessShardResume;
                var parsed_resumes = if (scan.shard_cursors == .null) null else try std.json.parseFromValue(Resumes, a, scan.shard_cursors, .{ .ignore_unknown_fields = true });
                defer if (parsed_resumes) |*value| value.deinit();
                const shard_resumes = if (parsed_resumes) |value| value.value else &.{};
                const result = writes.reprocessDocumentArtifactRange(a, table.name, resource.artifact_name, .{
                    .from_key = if (shard_resumes.len != 0) "" else scan.next_key orelse scan.from_key,
                    .to_key = scan.to_key,
                    .limit = scan.limit,
                    .shard_cursors = shard_resumes,
                }) catch |err| {
                    switch (err) {
                        error.Canceled, error.Cancelled, error.OutOfMemory => return err,
                        error.NotLeader, error.ProposalDropped, error.LeaderTransferInProgress => {
                            // A shard election is not loss of this metadata
                            // worker's authority. Yield its queue position;
                            // persistScan independently fences our own term.
                            verificationUnavailable(updated, now, @errorName(err));
                            try checkpoint.encodeResource(&resource);
                            try persistScan(server, a, table, resource, work.term);
                            return;
                        },
                        else => {},
                    }
                    scanError(updated, now, @errorName(err));
                    try checkpoint.encodeResource(&resource);
                    try persistScan(server, a, table, resource, work.term);
                    return;
                };
                if (result == null) {
                    scanError(updated, now, "PrivacyReadUnavailable");
                    try checkpoint.encodeResource(&resource);
                    try persistScan(server, a, table, resource, work.term);
                    return;
                }
                var pass = result.?;
                updated.consecutive_errors = 0;
                updated.retry_after_millis = if (result.?.scanned == 0) now + 2000 else 0;
                updated.last_error = null;
                defer pass.deinit(a);
                updated.scanned +|= pass.scanned;
                updated.reprocessed +|= pass.reprocessed;
                updated.skipped +|= pass.skipped;
                updated.failed +|= pass.failed;
                // Shard resumes are authoritative when present. Do not also
                // retain a second copy of an oversized global document key.
                updated.next_key = if (pass.shard_cursors.len == 0) pass.next_key else null;
                const resumes_raw = try std.json.Stringify.valueAlloc(a, pass.shard_cursors, .{});
                defer a.free(resumes_raw);
                var resumes = try std.json.parseFromSlice(std.json.Value, a, resumes_raw, .{});
                defer resumes.deinit();
                updated.shard_cursors = resumes.value;
                const failures_raw = try std.json.Stringify.valueAlloc(a, pass.failures[0..@min(pass.failures.len, 100)], .{});
                defer a.free(failures_raw);
                var failures = try std.json.parseFromSlice(std.json.Value, a, failures_raw, .{});
                defer failures.deinit();
                updated.failures = try catalog.coverage.appendFailures(a, updated.failures, failures.value);
                updated.phase = if (pass.next_key != null or pass.shard_cursors.len != 0) .running else if (updated.failed != 0) .failed else .processing;
                try checkpoint.encodeResource(&resource);
                try persistScan(server, a, table, resource, work.term);
                return;
            }
            try checkpoint.encodeResource(&resource);
            try persistScan(server, a, table, resource, work.term);
            return;
        }
    }
}

/// A readiness probe is availability work, not a terminal inspection failure.
/// Persist its backoff through the ordinary term-fenced CAS even for routing
/// and transport errors, so one unavailable table cannot pin the global head.
fn verifyServing(server: anytype, a: A, table: []const u8, version: [64]u8, scan: *catalog.Scan, now: i64) !bool {
    const reads = server.table_reads orelse {
        verificationUnavailable(scan, now, "PrivacyReadUnavailable");
        return false;
    };
    var ready = reads.preflightQuery(a, table, .{
        .privacy_policy_version = version,
        .execution_deadline_ns = platform_time.monotonicNs() +| 2 * std.time.ns_per_s,
    }, .read_index, 10000) catch |err| switch (err) {
        error.OutOfMemory, error.Canceled, error.Cancelled => return err,
        else => {
            verificationUnavailable(scan, now, @errorName(err));
            return false;
        },
    };
    defer if (ready) |*value| value.deinit(a);
    if (ready == null) {
        verificationUnavailable(scan, now, "PrivacyReadUnavailable");
        return false;
    }
    scan.consecutive_errors = 0;
    scan.last_error = null;
    return true;
}

fn verificationUnavailable(scan: *catalog.Scan, now: i64, code: []const u8) void {
    scan.consecutive_errors +|= 1;
    scan.last_error = code;
    scan.retry_after_millis = now + @min(@as(i64, 30000), @as(i64, 1000) << @intCast(@min(scan.consecutive_errors, 5)));
}

// A full-table job shares the policy's durable census instead of generating
// another shard-wide read workload for every active job. Only a pass started
// after this job reached verification may certify completion. Coverage is
// reset atomically on semantic policy replacement, so its version is implicit
// in the containing Resource. Serving still checks individual source receipts.
fn acceptCoverageVerification(scan: *catalog.Scan, state: catalog.coverage.State, now: i64) bool {
    scan.retry_after_millis = now + 2000;
    if (scan.verification_started_at_millis == 0) scan.verification_started_at_millis = now;
    const snapshot = state.snapshot orelse return false;
    if (snapshot.started_at_millis <= scan.verification_started_at_millis) return false;
    scan.verification_cursor = null;
    scan.verification_current = snapshot.counts.current;
    scan.verification_pending = snapshot.counts.pending;
    scan.verification_failed = snapshot.counts.failed_known;
    scan.verification_blocked = snapshot.counts.protected_blocked;
    scan.failures = snapshot.counts.failures;
    return true;
}

// Page evidence and cursors commit together. An unavailable read reduces work
// without discarding earlier pages or claiming progress through a slow range.
fn acceptVerification(a: A, updated: *catalog.Scan, object: std.json.ObjectMap, now: i64) !bool {
    updated.retry_after_millis = now + 2000;
    if (!object.get("available").?.bool) {
        updated.verification_page_size = @max(1, updated.verification_page_size / 2);
        return false;
    }
    updated.verification_current +|= @intCast(object.get("current").?.integer);
    updated.verification_pending +|= @intCast(object.get("pending").?.integer);
    updated.verification_failed +|= @intCast(object.get("failed_known").?.integer);
    updated.verification_blocked +|= @intCast(object.get("protected_blocked").?.integer);
    const cursor = object.get("next_key").?;
    updated.verification_cursor = if (cursor == .string) cursor.string else null;
    if (object.get("failed_known").?.integer != 0) {
        updated.failures = try catalog.coverage.appendFailures(a, updated.failures, object.get("failures").?);
    }
    return true;
}

// An RPC may straddle a metadata election. Its result cannot acknowledge scan
// progress from the previous term, even if the same node becomes leader again.
fn persistScan(server: anytype, a: A, table: metadata.TableRecord, resource: catalog.Resource, term: u64) !void {
    if (!server.cfg.catalog_publication_authority_available or
        (server.cfg.deployment_mode != .standalone and !server.cfg.privacy_scan_coordinator)) return error.NotLeader;
    const current_term = (try server.source.privacyAuthorityTerm()) orelse return error.NotLeader;
    if (current_term != term) return error.NotLeader;
    const raw = try catalog.replaceResource(a, table.indexes_json, resource);
    defer a.free(raw);
    var replacement = table;
    replacement.indexes_json = raw;
    server.source.replaceTableDefinition(table, replacement) catch |err| {
        std.log.warn("privacy progress persistence failed bytes={} err={s}", .{ raw.len, @errorName(err) });
        return err;
    };
}

fn scanError(scan: *catalog.Scan, now: i64, code: []const u8) void {
    scan.consecutive_errors +|= 1;
    scan.last_error = code;
    scan.phase = if (scan.consecutive_errors >= 5) .failed else .running;
    scan.retry_after_millis = now + (@as(i64, 1000) << @intCast(@min(scan.consecutive_errors, 6)));
}

test "privacy scans bound unavailable-owner retries durably" {
    var scan = catalog.Scan{ .scan_id = contract.fingerprint("scan"), .policy_version = contract.fingerprint("policy") };
    var now: i64 = 100;
    for (0..5) |attempt| {
        scanError(&scan, now, "Unavailable");
        try std.testing.expect(scan.retry_after_millis > now);
        try std.testing.expectEqual(attempt + 1, scan.consecutive_errors);
        try std.testing.expectEqualStrings("Unavailable", scan.last_error.?);
        try std.testing.expectEqual(attempt == 4, scan.phase == .failed);
        now = scan.retry_after_millis;
    }
}

test "privacy findings invalidate the complete source before accepting any field" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const policy = contract.Policy{ .mode = .monitor, .fields = &.{ "first", "second" } };
    const resource = try catalog.next(a, null, policy, null);
    const resolved = try projection.resolve(a, policy, true);
    const config = try std.json.Stringify.valueAlloc(a, resolved.config, .{});
    const Fake = struct {
        fn check(_: *anyopaque) !void {}
        fn call(_: *anyopaque, out: A, _: contract.Producer, _: []const u8) ![]u8 {
            return out.dupe(u8, "{\"findings\":[],\"long_document\":{\"window_count\":1}}");
        }
    };
    var context: u8 = 0;
    const source = "{\"first\":\"A\",\"second\":\"B\",\"large\":9007199254740993}";
    const produced = try projection.produce(a, config, source, .{ .ptr = &context, .produce = Fake.call, .check = Fake.check });
    const result = (try std.json.parseFromSlice(std.json.Value, a, produced, .{ .parse_numbers = false })).value;
    var original = (try std.json.parseFromSlice(std.json.Value, a, source, .{ .parse_numbers = false })).value;
    const receipt = result.object.get("_privacy").?;
    try std.testing.expect(try currentReceipt(a, resource, original, receipt));
    try original.object.put(a, "second", .{ .string = "Changed" });
    try std.testing.expect(!try currentReceipt(a, resource, original, receipt));
    try original.object.put(a, "second", .{ .string = "B" });
    try original.object.put(a, "large", .{ .integer = 3 });
    try std.testing.expect(!try currentReceipt(a, resource, original, receipt));
}

test {
    _ = @import("privacy_transport_test.zig");
}

test "privacy status returns persisted coverage without opening data shards" {
    const a = std.testing.allocator;
    const resource = try catalog.next(a, null, .{ .mode = .monitor, .fields = &.{"body"} }, null);
    var response = try statusResponse(struct {}{}, a, "docs", resource, 200, .{});
    defer response.deinit(a);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, response.response.body, .{});
    defer parsed.deinit();
    try std.testing.expect(!parsed.value.object.get("coverage").?.object.get("available").?.bool);
}

test "privacy acknowledgement ignores worker progress and accepts a newer committed policy" {
    const id = contract.fingerprint("policy");
    const installed = catalog.Resource{ .policy = .{ .mode = .monitor, .fields = &.{"body"} }, .policy_version = id, .resource_version = 3, .generation = 1, .scan_id = id };
    var visible = installed;
    visible.coverage.updated_at_millis = 999;
    try std.testing.expect(acknowledged(installed, visible));
    visible.resource_version = 2;
    try std.testing.expect(!acknowledged(installed, visible));
    visible.resource_version = 4;
    visible.policy_version = contract.fingerprint("replacement");
    try std.testing.expect(acknowledged(installed, visible));
}

test "privacy verification shrinks unavailable pages and durably resumes accumulated evidence" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const id = contract.fingerprint("policy");
    var scan = catalog.Scan{ .scan_id = id, .policy_version = id, .phase = .processing };
    const unavailable = try std.json.parseFromSlice(std.json.Value, a, "{\"available\":false}", .{});
    for (0..3) |_| try std.testing.expect(!try acceptVerification(a, &scan, unavailable.value.object, 100));
    try std.testing.expectEqual(@as(u32, 12), scan.verification_page_size);
    const page = try std.json.parseFromSlice(std.json.Value, a, "{\"available\":true,\"current\":12,\"pending\":0,\"failed_known\":0,\"protected_blocked\":0,\"next_key\":\"key-12\"}", .{});
    try std.testing.expect(try acceptVerification(a, &scan, page.value.object, 2100));
    const restored = try std.json.parseFromSlice(catalog.Scan, a, try std.json.Stringify.valueAlloc(a, scan, .{}), .{});
    scan = restored.value;
    try std.testing.expect(!try acceptVerification(a, &scan, unavailable.value.object, 4100));
    try std.testing.expectEqualStrings("key-12", scan.verification_cursor.?);
    try std.testing.expectEqual(@as(u64, 12), scan.verification_current);
    try std.testing.expectEqual(@as(u32, 6), scan.verification_page_size);
}

test "privacy findings enumerate IDs and release document arenas before byte-budget continuation" {
    var fixtures = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer fixtures.deinit();
    const f = fixtures.allocator();
    const policy = contract.Policy{ .mode = .monitor, .fields = &.{"body"} };
    const resource = try catalog.next(f, null, policy, null);
    const producer = try projection.resolve(f, policy, true);
    const padding = try f.alloc(u8, 1024 * 1024);
    @memset(padding, 'p');
    const original = try std.json.Stringify.valueAlloc(f, .{ .body = "B", .padding = padding }, .{});
    const Detector = struct {
        fn check(_: *anyopaque) !void {}
        fn call(_: *anyopaque, a: A, _: contract.Producer, _: []const u8) ![]u8 {
            return a.dupe(u8, "{\"findings\":[],\"long_document\":{\"window_count\":1}}");
        }
    };
    var context: u8 = 0;
    const manifest = try projection.produce(f, try std.json.Stringify.valueAlloc(f, producer.config, .{}), original, .{ .ptr = &context, .produce = Detector.call, .check = Detector.check });
    const Fake = struct {
        original: []const u8,
        manifest: []const u8,
        tracker: *std.testing.FailingAllocator,
        reads: usize = 0,
        fn scan(_: *@This(), a: A, _: []const u8, _: []const u8, _: []const u8, opts: local.storage_db_types.ScanOptions, _: local.storage_read_consistency.ReadConsistency) !?local.api_table_read_source.ScanResponse {
            try std.testing.expect(!opts.include_documents);
            var rows: std.ArrayList(u8) = .empty;
            for (0..50) |i| try rows.appendSlice(a, try std.fmt.allocPrint(a, "{{\"_id\":\"doc-{d}\"}}\n", .{i}));
            return .{ .ndjson = try rows.toOwnedSlice(a) };
        }
        fn scanStream(self: *@This(), a: A, table: []const u8, from: []const u8, to: []const u8, opts: local.storage_db_types.ScanOptions, consistency: local.storage_read_consistency.ReadConsistency, sink: local.api_table_read_source.ScanStreamSink) !bool {
            var page = (try self.scan(a, table, from, to, opts, consistency)).?;
            defer page.deinit(a);
            try sink.start();
            try sink.write(page.ndjson);
            return true;
        }
        fn documentArtifactManifestControlled(self: *@This(), a: A, table: []const u8, key: []const u8, artifact: []const u8, consistency: local.storage_read_consistency.ReadConsistency, _: local.storage_db_types.LookupOptions) !?local.storage_db_types.DocumentArtifactManifest {
            return self.documentArtifactManifest(a, table, key, artifact, consistency);
        }
        fn documentArtifactManifest(self: *@This(), a: A, _: []const u8, key: []const u8, artifact: []const u8, _: local.storage_read_consistency.ReadConsistency) !?local.storage_db_types.DocumentArtifactManifest {
            // The previous document's complete JSON trees must be gone.
            try std.testing.expect(self.tracker.allocated_bytes - self.tracker.freed_bytes < 256 * 1024);
            self.reads += 1;
            return .{ .document_id = try a.dupe(u8, key), .artifact_name = try a.dupe(u8, artifact), .artifact_id = try a.dupe(u8, "receipt"), .manifest_json = try a.dupe(u8, self.manifest) };
        }
        fn lookup(self: *@This(), a: A, _: []const u8, _: []const u8, _: local.storage_db_types.LookupOptions, _: local.storage_read_consistency.ReadConsistency) !?local.api_table_read_source.LookupResponse {
            return .{ .json = try a.dupe(u8, self.original), .version = 1 };
        }
    };
    var tracker = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var fake = Fake{ .original = original, .manifest = manifest, .tracker = &tracker };
    var server = .{ .table_reads = @as(?*Fake, &fake) };
    var response = try findings(&server, tracker.allocator(), "docs", resource, .{}, .{});
    defer response.deinit(tracker.allocator());
    const result = try std.json.parseFromSlice(std.json.Value, f, response.response.body, .{});
    try std.testing.expect(fake.reads > 1 and fake.reads < 10);
    const cursor = try cursorDecode(f, result.value.object.get("next_cursor").?.string);
    try std.testing.expect(cursor.inclusive);
    try std.testing.expectEqual(@as(usize, 0), cursor.offset);
    try std.testing.expectEqualStrings(try std.fmt.allocPrint(f, "doc-{d}", .{fake.reads}), cursor.document);
}

test "privacy progress rejects publication authority revoked during a page" {
    const Source = struct {
        fn privacyAuthorityTerm(_: @This()) !?u64 {
            return 0;
        }
        fn replaceTableDefinition(_: @This(), _: metadata.TableRecord, _: metadata.TableRecord) !void {
            unreachable;
        }
    };
    const Mode = enum { standalone };
    var server = .{ .source = Source{}, .cfg = .{ .catalog_publication_authority_available = false, .deployment_mode = Mode.standalone, .privacy_scan_coordinator = false } };
    const id = contract.fingerprint("policy");
    const resource = catalog.Resource{ .policy = .{ .mode = .monitor, .fields = &.{"body"} }, .policy_version = id, .resource_version = 1, .generation = 1, .scan_id = id };
    try std.testing.expectError(error.NotLeader, persistScan(&server, std.testing.allocator, .{ .table_id = 1, .name = "docs" }, resource, 0));
}

test "privacy full-table verification shares only a fresh completed census" {
    const id = contract.fingerprint("policy");
    var scan = catalog.Scan{ .scan_id = id, .policy_version = id, .phase = .processing };
    var state: catalog.coverage.State = .{ .snapshot = .{ .started_at_millis = 50, .completed_at_millis = 150, .counts = .{ .total = 100, .current = 100 } } };
    // Even a recently completed pass cannot certify this job if it started
    // before reprocess finished. In-flight partial counts are not evidence.
    try std.testing.expect(!acceptCoverageVerification(&scan, state, 100));
    try std.testing.expectEqual(@as(i64, 100), scan.verification_started_at_millis);
    state.snapshot.?.started_at_millis = 100;
    try std.testing.expect(!acceptCoverageVerification(&scan, state, 200));
    state.pending = .{ .total = 100, .current = 100 };
    state.started_at_millis = 200;
    try std.testing.expect(!acceptCoverageVerification(&scan, state, 300));
    state.snapshot = .{ .started_at_millis = 200, .completed_at_millis = 250, .counts = .{ .total = 100, .current = 90, .pending = 7, .failed_known = 3, .protected_blocked = 10 } };
    try std.testing.expect(acceptCoverageVerification(&scan, state, 300));
    try std.testing.expectEqual(@as(u64, 90), scan.verification_current);
    try std.testing.expectEqual(@as(u64, 7), scan.verification_pending);
    try std.testing.expectEqual(@as(u64, 3), scan.verification_failed);
    try std.testing.expectEqual(@as(u64, 10), scan.verification_blocked);
    try std.testing.expectEqual(@as(i64, 100), scan.verification_started_at_millis);
}

test "privacy readiness transport failures persist backoff and release the global scheduler head" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Types = local.storage_db_types;
    const Consistency = local.storage_read_consistency.ReadConsistency;
    const Reads = struct {
        failure: ?anyerror,
        const Summary = struct {
            fn deinit(_: *@This(), _: A) void {}
        };
        fn preflightQuery(self: *@This(), _: A, _: []const u8, request: Types.SearchRequest, _: Consistency, _: u32) !?Summary {
            try std.testing.expect(request.execution_deadline_ns.? > platform_time.monotonicNs());
            if (self.failure) |err| return err;
            return .{};
        }
        fn scanStream(_: *@This(), _: A, _: []const u8, _: []const u8, _: []const u8, _: Types.ScanOptions, _: Consistency, _: local.api_table_read_source.ScanStreamSink) !bool {
            unreachable;
        }
    };
    const Writes = struct {
        failure: ?anyerror,
        fn reprocessDocumentArtifactRange(self: *@This(), _: A, _: []const u8, _: []const u8, request: Types.DocumentArtifactTableReprocessRequest) !?Types.DocumentArtifactTableReprocessResult {
            try std.testing.expectEqualStrings("after", request.from_key);
            try std.testing.expectEqual(@as(usize, 0), request.shard_cursors.len);
            if (self.failure) |err| return err;
            unreachable;
        }
    };
    const Source = struct {
        a: A,
        table: metadata.TableRecord,
        fn nextPrivacyWork(self: *@This(), out: A, _: i64) !?metadata.PrivacyWork {
            return .{ .table = try metadata.cloneTable(out, self.table), .term = 7 };
        }
        fn privacyAuthorityTerm(_: *@This()) !?u64 {
            return 7;
        }
        fn replaceTableDefinition(self: *@This(), expected: metadata.TableRecord, replacement: metadata.TableRecord) !void {
            try std.testing.expectEqualStrings(self.table.indexes_json, expected.indexes_json);
            self.table = try metadata.cloneTable(self.a, replacement);
        }
    };
    const Mode = enum { standalone };
    for ([_]@FieldType(catalog.Scan, "phase"){ .processing, .running }) |phase| for ([_]anyerror{ error.Timeout, error.DeadlineExceeded, error.CatalogRoutingSnapshotTimeout, error.NotLeader, error.StorageReadTemporarilyUnavailable }) |err| {
        var resource = try catalog.next(a, null, .{ .mode = .redact_for_search_and_ai, .fields = &.{"body"} }, null);
        _ = try catalog.startScan(a, &resource, "", "", 100);
        const scans = @constCast(resource.scans);
        scans[0].phase = phase;
        if (phase == .running) {
            scans[0].next_key = "after";
            scans[0].shard_cursors = .{ .array = std.array_list.Managed(std.json.Value).init(a) };
        }
        scans[0].updated_at_millis = 10;
        scans[0].verification_started_at_millis = 1;
        resource.coverage = .{ .updated_at_millis = 5000, .retry_after_millis = std.math.maxInt(i64), .snapshot = .{ .counts = .{}, .started_at_millis = 2, .completed_at_millis = 3 } };
        var source = Source{ .a = a, .table = .{ .table_id = 1, .name = "blocked", .indexes_json = try catalog.install(a, "{}", resource) } };
        var reads = Reads{ .failure = err };
        var writes = Writes{ .failure = err };
        var server = .{ .cfg = .{ .deployment_mode = Mode.standalone, .catalog_publication_authority_available = true, .privacy_scan_coordinator = false }, .table_reads = @as(?*Reads, &reads), .table_writes = @as(?*Writes, &writes), .source = &source };
        var index: catalog.WorkIndex = .{};
        defer index.deinit(a);
        try index.update(a, 1, try catalog.workDue(a, source.table.indexes_json));
        try index.update(a, 2, 20);
        try std.testing.expectEqual(@as(?u64, 1), index.first(100));
        try advanceOnce(&server, a);
        const saved = (try load(a, source.table.indexes_json)).?.value;
        try std.testing.expectEqual(phase, saved.scans[0].phase);
        try std.testing.expectEqualStrings(@errorName(err), saved.scans[0].last_error.?);
        try std.testing.expect(saved.scans[0].retry_after_millis > saved.scans[0].updated_at_millis);
        try index.update(a, 1, try catalog.workDue(a, source.table.indexes_json));
        try std.testing.expectEqual(@as(?u64, 2), index.first(std.math.maxInt(i64)));
        reads.failure = null;
        var recovered = saved.scans[0];
        try std.testing.expect(try verifyServing(&server, a, "blocked", saved.policy_version, &recovered, recovered.retry_after_millis));
        try std.testing.expectEqual(@as(u8, 0), recovered.consecutive_errors);
        try std.testing.expect(recovered.last_error == null);
    };
}

test "privacy census yields a complete prefix and resumes without losing a buffered row" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const version: [64]u8 = @splat('a');
    const first = try std.json.Stringify.valueAlloc(a, .{ ._id = "first", .policy_version = &version, .current = true, .failed = false, .protected_ready = true, .categories = &[_]local.privacy_status.CategoryCount{} }, .{});
    const second = try std.json.Stringify.valueAlloc(a, .{ ._id = "second", .policy_version = &version, .current = true, .failed = false, .protected_ready = true, .categories = &[_]local.privacy_status.CategoryCount{} }, .{});
    const both = try std.mem.concat(a, u8, &.{ first, "\n", second, "\n" });
    var census: CoverageCensus = .{ .a = a, .version = version, .line_arena = std.heap.ArenaAllocator.init(std.testing.allocator), .soft_deadline_ns = 1 };
    defer census.line_arena.deinit();
    try std.testing.expectError(error.Canceled, CoverageCensus.write(&census, both));
    try std.testing.expect(census.yielded);
    try std.testing.expectEqual(@as(u64, 1), census.totals.total);
    try std.testing.expectEqualStrings("first", census.last_key.items);
    try std.testing.expectEqual(@as(usize, 0), census.buffer.items.len);
    var resumed: CoverageCensus = .{ .a = a, .version = version, .line_arena = std.heap.ArenaAllocator.init(std.testing.allocator) };
    defer resumed.line_arena.deinit();
    try CoverageCensus.write(&resumed, try std.mem.concat(a, u8, &.{ second, "\n" }));
    try std.testing.expectEqual(@as(u64, 1), resumed.totals.total);
    try std.testing.expectEqualStrings("second", resumed.last_key.items);
}

test "privacy scan polling omits large private shard checkpoints without mutating durable state" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const key = try a.alloc(u8, 1_100_000);
    @memset(key, 'x');
    var checkpoint: std.json.Value = .{ .object = .empty };
    try checkpoint.object.put(a, "next_key", .{ .string = key });
    var checkpoints: std.json.Value = .{ .array = std.array_list.Managed(std.json.Value).init(a) };
    try checkpoints.array.append(checkpoint);
    const scan: catalog.Scan = .{ .scan_id = @splat('a'), .policy_version = @splat('b'), .phase = .running, .from_key = key, .to_key = key, .next_key = key, .verification_cursor = key, .shard_cursors = checkpoints };
    const result = try scanResponse(a, 200, scan);
    try std.testing.expect(result.response.body.len < 4096);
    try std.testing.expectEqual(@as(usize, 1_100_000), scan.shard_cursors.array.items[0].object.get("next_key").?.string.len);
    const parsed = try std.json.parseFromSlice(std.json.Value, a, result.response.body, .{});
    inline for (.{ "shard_cursors", "from_key", "to_key", "next_key", "verification_cursor" }) |name| {
        try std.testing.expect(!parsed.value.object.contains(name));
    }
    try std.testing.expectEqualStrings("running", parsed.value.object.get("phase").?.string);
}

test "privacy preview counts escaped and repeated identifiers without output allocation" {
    var size = PreviewSize{ .remaining = 8 };
    try size.add("\x01");
    try std.testing.expectEqual(@as(usize, 0), size.remaining);
    try std.testing.expectError(error.PrivacySampleTooLarge, size.add(""));
    var repeated = PreviewSize{ .remaining = 20 };
    try std.testing.expectError(error.PrivacySampleTooLarge, repeated.add(.{ "12345678", "12345678" }));
}

test "privacy preview streaming bounds partial rows and stops at the row limit" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var samples = PreviewSamples{ .a = arena.allocator(), .transient = std.testing.allocator, .limit = 2, .remaining = 100 };
    defer samples.deinit();
    try PreviewSamples.write(&samples, "{\"_id\":\"one");
    try PreviewSamples.write(&samples, "\",\"body\":1}\n");
    try std.testing.expectError(error.Canceled, PreviewSamples.write(&samples, "{\"_id\":\"two\"}\nignored"));
    try std.testing.expect(samples.stopped);
    try std.testing.expectEqual(@as(usize, 2), samples.documents.items.len);
    try std.testing.expect(samples.failure == null);
    try std.testing.expectEqualStrings("one", samples.documents.items[0].key);
    var oversized = PreviewSamples{ .a = arena.allocator(), .transient = std.testing.allocator, .limit = 2, .remaining = 4 };
    defer oversized.deinit();
    try PreviewSamples.write(&oversized, "1234");
    try std.testing.expectError(error.Canceled, PreviewSamples.write(&oversized, "5"));
    try std.testing.expectEqual(error.PrivacySampleTooLarge, oversized.failure.?);
    try std.testing.expectEqual(@as(usize, 4), oversized.buffer.items.len);
    var final = PreviewSamples{ .a = arena.allocator(), .transient = std.testing.allocator, .limit = 2 };
    defer final.deinit();
    try PreviewSamples.write(&final, "{\"_id\":\"tail\"}");
    try final.row();
    try std.testing.expectEqualStrings("tail", final.documents.items[0].key);
}
