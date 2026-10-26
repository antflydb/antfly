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

//! Invocation-local facade over the host's retained document persistence
//! capability. The task owner claims/renews the lease; this shared executor
//! admits sources, pins identities and publishes stages and final results.
const std = @import("std");
const readers = @import("antfly_readers");
const runs = readers.document_runs;
const c = @import("contract.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const A = std.mem.Allocator;
pub const Session = runs.Session;
pub const Record = runs.Record;
pub const Ref = runs.Ref;
pub const Digest = runs.Digest;
pub const Charge = runs.Charge;
pub fn now(_: ?*anyopaque) u64 {
    return @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
}
pub fn hash(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
pub fn inputKey(request: Digest, kind: []const u8, ordinal: usize) Digest {
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    h.update("antfly-document-input-v1");
    h.update(&request);
    h.update(kind);
    var index: [8]u8 = undefined;
    std.mem.writeInt(u64, &index, @intCast(ordinal), .big);
    h.update(&index);
    return h.finalResult();
}
pub fn delta(after: c.Usage, before: c.Usage) c.Usage {
    var result: c.Usage = .{};
    inline for (@typeInfo(c.Usage).@"struct".field_names) |field| @field(result, field) = @field(after, field) - @field(before, field);
    return result;
}
pub fn leafCharge(usage: c.Usage, bytes: usize, calls: u64) Charge {
    return .{ .calls = calls, .tokens = usage.total_tokens, .output_bytes = bytes };
}
pub fn restore(run: *c.Run, usage: c.Usage, include_calls: bool) !void {
    var value = usage;
    if (!include_calls) value.model_calls = 0; // caller already admitted its logical call
    try run.replayUsage(value);
}
pub const Runtime = struct {
    allocator: A,
    client: readers.document_rpc.Client,
    session: Session,
    control: Control,
    pub fn create(a: A, io: std.Io, bytes: []const u8, control: Control) !*Runtime {
        const self = try a.create(Runtime);
        errdefer a.destroy(self);
        self.allocator = a;
        self.control = control;
        self.client = try readers.document_rpc.Client.initBound(a, control.document_store, io);
        errdefer self.client.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const record = try self.client.store().loadAt(arena.allocator(), self.client.id, self.client.owner, now(null));
        if (!std.mem.eql(u8, &record.identity.request, &hash(bytes))) return error.DocumentModelRevisionMismatch;
        const lease: runs.Lease = .{ .run_id = self.client.id, .owner = self.client.owner, .attempt = record.attempt, .nonce = record.nonce };
        const max_stages: usize = @intCast(@min(100_000, @max(512, record.sequence +| 512)));
        self.session = try Session.init(a, self.client.store(), lease, record.identity, .{ .now_fn = now }, @max(512, max_stages), 32 * 1024 * 1024);
        self.session.io = io;
        self.session.meter = control.document_dispatch_meter;
        self.session.check_context = self;
        self.session.check_fn = check;
        return self;
    }
    fn check(raw: ?*anyopaque) !void {
        const self: *Runtime = @ptrCast(@alignCast(raw.?));
        try self.control.check();
    }
    pub fn deinit(self: *Runtime) void {
        const a = self.allocator;
        self.session.deinit();
        self.client.deinit();
        a.destroy(self);
    }
    pub fn finish(self: *Runtime, bytes: []const u8) !void {
        const events = "[{\"phase\":\"complete\",\"source_id\":\"\",\"page_number\":0}]";
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const key = self.session.key("final-response-v1");
        const reference = try self.session.publish(arena.allocator(), key, bytes, events);
        try self.session.store.finish(arena.allocator(), self.session.lease, reference, now(null));
    }
};
pub fn pin(session: *Session, a: A, definition: anytype, models: anytype, adapter: []const u8) !void {
    const defs = hash(try std.json.Stringify.valueAlloc(a, definition, .{}));
    const leaves = hash(try std.json.Stringify.valueAlloc(a, models, .{}));
    const runtime = hash(try std.json.Stringify.valueAlloc(a, .{ .version = @import("build_info").version(), .adapter = adapter, .renderer = @import("antfly_documents").renderer_version }, .{}));
    const record = try session.store.loadAt(a, session.lease.run_id, session.lease.owner, now(null));
    if (record.prepared and (!std.mem.eql(u8, &record.identity.definition, &defs) or !std.mem.eql(u8, &record.identity.models, &leaves) or !std.mem.eql(u8, &record.identity.runtime, &runtime))) return error.DocumentModelRevisionMismatch;
    session.identity.definition = defs;
    session.identity.models = leaves;
    session.identity.runtime = runtime;
}
pub fn seal(session: *Session, a: A, limits: c.Limits, sources: []const Ref) !void {
    const record = try session.store.loadAt(a, session.lease.run_id, session.lease.owner, now(null));
    if (record.inputs.len == 0) return error.InvalidDocumentCheckpoint;
    var h = std.crypto.hash.sha2.Sha256.init(.{});
    for (sources) |source| {
        h.update(&source.sha256);
        var len: [8]u8 = undefined;
        std.mem.writeInt(u64, &len, source.bytes, .big);
        h.update(&len);
    }
    const identity = h.finalResult();
    if (record.prepared) {
        if (!std.mem.eql(u8, &record.identity.sources, &identity)) return error.DocumentModelRevisionMismatch;
        session.identity.sources = identity;
        return;
    }
    var next = session.prepared_limits orelse record.limits;
    if (session.prepared_limits == null) {
        next.calls = @min(next.calls, limits.max_model_calls);
        next.tokens = @min(next.tokens, limits.max_total_tokens);
        next.pixels = @min(next.pixels, limits.max_total_pixels);
        next.source_bytes = @min(next.source_bytes, limits.max_source_bytes +| record.inputs[0].bytes);
    }
    const inputs = try a.alloc(Ref, sources.len + 1);
    inputs[0] = record.inputs[0];
    @memcpy(inputs[1..], sources);
    session.identity.sources = identity;
    try session.store.prepare(a, session.lease, session.identity, next, inputs, now(null));
}

pub const RasterHeader = struct {
    mime_type: []const u8,
    width: u32,
    height: u32,
    effective_dpi: u16,
    raster_to_source: c.Transform,
    usage: c.Usage,
};
pub fn encodeRaster(a: A, header: RasterHeader, bytes: []const u8) ![]u8 {
    const metadata = try std.json.Stringify.valueAlloc(a, header, .{});
    defer a.free(metadata);
    if (metadata.len > 4096 or bytes.len > readers.document_rpc.max_payload_bytes - 4 - metadata.len) return error.DocumentOutputTooLarge;
    const result = try a.alloc(u8, 4 + metadata.len + bytes.len);
    std.mem.writeInt(u32, result[0..4], @intCast(metadata.len), .big);
    @memcpy(result[4..][0..metadata.len], metadata);
    @memcpy(result[4 + metadata.len ..], bytes);
    return result;
}
pub fn decodeRaster(a: A, bytes: []const u8) !struct { header: RasterHeader, bytes: []const u8 } {
    if (bytes.len < 4) return error.InvalidDocumentCheckpoint;
    const length = std.mem.readInt(u32, bytes[0..4], .big);
    if (length > 4096 or length > bytes.len - 4) return error.InvalidDocumentCheckpoint;
    return .{ .header = try std.json.parseFromSliceLeaky(RasterHeader, a, bytes[4..][0..length], .{ .allocate = .alloc_always, .duplicate_field_behavior = .@"error" }), .bytes = bytes[4 + length ..] };
}
