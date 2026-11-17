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

const std = @import("std");
const control = @import("metadata/changefeed_control.zig");
const retained = @import("metadata/pg_spool_stream.zig");
const backend = @import("pgwire/backend.zig");
const bridge = @import("pgwire/retained_stream.zig");
const server = @import("pgwire/server.zig");
const Request = @import("pg_spool_cleanup_test.zig").Request;
const A = std.mem.Allocator;
const Port = @import("api/changefeed_metadata.zig").Port;
const Context = @import("antfly_local_sources").api_operation.RequestContext;
const close_context: Context = .{ .principal = .{ .kind = .internal, .subject = "changefeed-maintenance" } };
fn frame(writer: *std.Io.Writer, tag: u8, bytes: []const u8) !void {
    try writer.writeByte(tag);
    try writer.writeInt(u32, @intCast(bytes.len + 4), .big);
    try writer.writeAll(bytes);
}
fn response(reader: *std.Io.Reader) !struct { tag: u8, bytes: []const u8 } {
    const tag = try reader.takeByte();
    const size = try reader.takeInt(u32, .big);
    if (size < 4 or size > 65536) return error.TestUnexpectedResult;
    return .{ .tag = tag, .bytes = try reader.take(size - 4) };
}
pub fn run(svc: anytype, registry: anytype, clock: u64) !void {
    const Host = struct {
        service: *const Port,
        registry: @TypeOf(registry),
        id: control.Id,
        actor: control.Id = @splat(3),
        config: retained.Config = .{ .require_shared = false },
        clock: u64,
        denied: bool = false,
        closes: usize = 0,
        close_errors: usize = 0,
        pub fn binding(_: *@This(), a: A, start: backend.replication_commands.Start, _: backend.Request) !control.pg_slot.Binding {
            return control.pg_slot.bindingAlloc(a, start.publications, @splat(8));
        }
        pub fn request(_: *@This(), value: backend.Request) !Context {
            return bridge.metadataContext(value);
        }
        pub fn authorize(self: *@This(), state: control.State, selected: control.pg_slot.Binding, req: anytype) !void {
            try req.ensureActive();
            if (self.denied or state.definition == null or state.definition.?.tables.len != 1 or state.definition.?.tables[0].table_id != 7 or !std.mem.eql(u8, &selected.projection, &@as(control.Digest, @splat(8)))) return error.Forbidden;
        }
        pub fn nowSeconds(self: *@This(), _: anytype) !u64 {
            return self.clock;
        }
        pub fn failedOpen(self: *@This()) void {
            retained.releaseActor(self.service, self, std.testing.allocator, self.id, self.actor, close_context) catch {
                self.close_errors += 1;
            };
        }
        pub fn closed(self: *@This(), owner: *retained.Owner) void {
            owner.release(self.service, self, std.testing.allocator, close_context) catch {
                self.close_errors += 1;
            };
            self.closes += 1;
        }
        fn authenticate(raw: *anyopaque, _: A, user: []const u8, password: []const u8) !backend.Identity {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.eql(u8, user, "alice") or !std.mem.eql(u8, password, "pw")) return error.Unauthorized;
            return .{ .context = self, .release = releaseIdentity };
        }
        fn releaseIdentity(_: *anyopaque, _: A) void {}
        fn describe(_: *anyopaque, _: A, _: backend.Identity, _: backend.Request) !backend.Description {
            return error.UnsupportedReplicationMode;
        }
        fn execute(_: *anyopaque, _: A, _: backend.Identity, _: backend.Request) !backend.Result {
            return error.UnsupportedReplicationMode;
        }
        fn disconnect(_: *anyopaque, _: backend.Identity, _: ?[]const u8) void {}
        fn replication(raw: *anyopaque, _: A, _: backend.Identity, req: backend.Request) !backend.ReplicationSession {
            try req.check();
            return .{ .context = raw, .authorize = authorizeWire, .command = command, .close = closeSession, .open_stream = bridge.Bridge(@This()).open };
        }
        fn authorizeWire(raw: *anyopaque, _: A, req: backend.Request, _: ?backend.replication_commands.Command) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try req.check();
            if (self.denied) return error.Forbidden;
        }
        fn command(_: *anyopaque, _: A, _: backend.Request, input: backend.replication_commands.Command) !backend.Result {
            if (input != .identify_system) return error.UnsupportedReplicationMode;
            return .{
                .command_tag = "IDENTIFY_SYSTEM",
                .columns = &.{
                    .{ .name = "systemid", .type = .string },
                    .{ .name = "timeline", .type = .integer },
                    .{ .name = "xlogpos", .type = .string },
                    .{ .name = "dbname", .type = .string },
                },
                .rows = &.{&.{ .{ .string = "719" }, .{ .integer = 1 }, .{ .string = "0/0" }, .{ .string = "db" } }},
            };
        }
        fn closeSession(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.failedOpen();
        }
        fn source(self: *@This()) backend.Backend {
            return .{ .context = self, .vtable = &vtable };
        }
        const vtable: backend.Backend.VTable = .{ .authenticate = authenticate, .describe = describe, .execute = execute, .disconnect = disconnect, .open_replication_session = replication };
    };
    const a = std.testing.allocator;
    const metadata = Port.fromService(svc);
    var host: Host = .{ .service = &metadata, .registry = registry, .id = svc.id, .clock = clock };
    try svc.mutate(a, .{ .slot = .{ .release = .{ .session = .{ .actor = @splat(2), .fence = 1 }, .now = clock } } });
    const selected: control.pg_slot.Binding = .{ .publications = &.{"rows_pub"}, .projection = @splat(8) };
    const changed: control.pg_slot.Binding = .{ .publications = &.{"other_pub"}, .projection = @splat(8) };
    try std.testing.expectError(error.ChangefeedResynchronizationRequired, retained.Owner.open(svc, registry, &host, a, svc.id, "upload_slot", @splat(3), 0, changed, host.config, Request{}));
    try std.testing.expectError(error.PgSpoolStorageUnsupported, retained.Owner.open(svc, registry, &host, a, svc.id, "upload_slot", @splat(3), 0, selected, .{}, Request{}));
    svc.lose_ack = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, retained.Owner.open(svc, registry, &host, a, svc.id, "upload_slot", @splat(6), 0, selected, host.config, Request{}));
    svc.lose_ack = false;
    try retained.releaseActor(svc, &host, a, svc.id, @splat(6), Request{});
    {
        var recovered = try svc.load(a);
        defer recovered.deinit();
        try std.testing.expect(recovered.value.slot.?.session == null);
        try std.testing.expectEqual(@as(u64, 1), recovered.value.slot.?.confirmed_lsn);
    }
    if (std.testing.environ.getPosix("ANTFLY_TEST_PG_RECVLOGICAL")) |program| {
        var listener = try server.start(a, .{ .io = std.testing.io, .backend = host.source(), .bind_port = 0, .limits = .{ .replication_keepalive_ms = 50 } });
        defer listener.deinit();
        const dsn = try std.fmt.allocPrint(a, "host=127.0.0.1 port={d} dbname=db user=alice password=pw sslmode=disable gssencmode=disable", .{listener.address().getPort()});
        defer a.free(dsn);
        const child = try std.process.run(a, std.testing.io, .{
            .argv = &.{ program, "--dbname", dsn, "--slot", "upload_slot", "--start", "--startpos=0/1", "--endpos=0/69", "--file=-", "--no-loop", "--option=proto_version=1", "--option=publication_names=rows_pub" },
            .timeout = .{ .duration = .{ .raw = .fromSeconds(10), .clock = .awake } },
        });
        defer a.free(child.stdout);
        defer a.free(child.stderr);
        if (child.term != .exited or child.term.exited != 0) {
            std.debug.print("pg_recvlogical failed: {s}\n", .{child.stderr});
            return error.TestUnexpectedResult;
        }
        try std.testing.expectEqual(@as(usize, 109), child.stdout.len);
        try std.testing.expectEqual(@as(u8, 'B'), child.stdout[0]);
        try std.testing.expectEqual(@as(u8, 'C'), child.stdout[82]);
        return;
    }
    // Real CopyBoth reads the actual slot/spool implementation. The wire client
    // intentionally sends no feedback: sending and closing must retain history.
    {
        var listener = try server.start(a, .{ .io = std.testing.io, .backend = host.source(), .bind_port = 0, .limits = .{ .idle_timeout_ms = 3000, .startup_timeout_ms = 3000, .replication_timeout_ms = 3000, .replication_keepalive_ms = 50, .replication_poll_ms = 5, .replication_callback_ms = 1000, .replication_write_ms = 1000 } });
        defer listener.deinit();
        const socket = try listener.address().connect(std.testing.io, .{ .mode = .stream });
        defer socket.close(std.testing.io);
        var writer = socket.writer(std.testing.io, &.{});
        const options = "user\x00alice\x00database\x00db\x00replication\x00database\x00\x00";
        try writer.interface.writeInt(u32, options.len + 8, .big);
        try writer.interface.writeInt(u32, 196608, .big);
        try writer.interface.writeAll(options);
        try writer.interface.flush();
        var buffer: [4096]u8 = undefined;
        var reader = socket.reader(std.testing.io, &buffer);
        const auth = try response(&reader.interface);
        try std.testing.expectEqual(@as(u8, 'R'), auth.tag);
        try std.testing.expectEqual(@as(u32, 3), std.mem.readInt(u32, auth.bytes[0..4], .big));
        try frame(&writer.interface, 'p', "pw\x00");
        try writer.interface.flush();
        while (true) {
            const value = try response(&reader.interface);
            try std.testing.expect(value.tag != 'E');
            if (value.tag == 'Z') break;
        }
        try frame(&writer.interface, 'Q', "START_REPLICATION SLOT upload_slot LOGICAL 0/0 (proto_version '1', publication_names 'rows_pub')\x00");
        try writer.interface.flush();
        const copy = try response(&reader.interface);
        try std.testing.expectEqual(@as(u8, 'W'), copy.tag);
        var messages: usize = 0;
        var frontier: u64 = 1;
        while (messages < 5) {
            const value = try response(&reader.interface);
            try std.testing.expectEqual(@as(u8, 'd'), value.tag);
            if (value.bytes[0] != 'w') continue;
            try std.testing.expectEqual(frontier, std.mem.readInt(u64, value.bytes[1..9], .big));
            try std.testing.expectEqual(@as(u64, 105), std.mem.readInt(u64, value.bytes[9..17], .big));
            try std.testing.expectEqual(@as(u8, if (messages == 0) 'B' else if (messages == 4) 'C' else 'I'), value.bytes[25]);
            frontier += value.bytes.len - 25;
            messages += 1;
        }
        try std.testing.expectEqual(@as(u64, 105), frontier);
        try frame(&writer.interface, 'c', "");
        try writer.interface.flush();
        var completions: usize = 0;
        while (true) {
            const value = try response(&reader.interface);
            try std.testing.expect(value.tag != 'E');
            if (value.tag == 'C') completions += 1;
            if (value.tag == 'Z') break;
        }
        try std.testing.expectEqual(@as(usize, 2), completions);
        try frame(&writer.interface, 'X', "");
        try writer.interface.flush();
        listener.deinit();
    }
    try std.testing.expectEqual(@as(usize, 1), host.closes);
    try std.testing.expectEqual(@as(usize, 0), host.close_errors);
    {
        var state = try svc.load(a);
        defer state.deinit();
        try std.testing.expectEqual(@as(u64, 1), state.value.slot.?.confirmed_lsn);
        try std.testing.expect(state.value.slot.?.session == null);
        try std.testing.expectEqual(.live, state.value.slot.?.spool.records[0].phase);
    }
    host.clock += 10;
    var rewind = try retained.Owner.open(svc, registry, &host, a, svc.id, "upload_slot", @splat(4), 22, selected, host.config, Request{});
    try std.testing.expectEqual(@as(u64, 1), rewind.initial_lsn);
    try std.testing.expectEqual(@as(u64, 22), rewind.resume_lsn);
    try rewind.feedback(svc, &host, a, .{ .written = 22, .flushed = 22, .applied = 22 }, Request{});
    var begin = try rewind.poll(svc, registry, &host, a, 1, 1024, Request{});
    defer begin.deinit();
    try std.testing.expectEqual(@as(u8, 'B'), begin.bytes.?[0]);
    try std.testing.expectError(error.ChangefeedConflict, rewind.feedback(svc, &host, a, .{ .written = 105, .flushed = 105, .applied = 105 }, Request{}));
    host.denied = true;
    try std.testing.expectError(error.Forbidden, rewind.poll(svc, registry, &host, a, 2, 1024, Request{}));
    host.denied = false;
    try rewind.release(svc, &host, a, Request{});
    var owner = try retained.Owner.open(svc, registry, &host, a, svc.id, "upload_slot", @splat(5), 0, selected, host.config, Request{});
    while (owner.frontier < 105) {
        var page = try owner.poll(svc, registry, &host, a, owner.frontier, 1024, Request{});
        defer page.deinit();
        try std.testing.expect(page.bytes != null);
    }
    const Concurrent = struct {
        service: @TypeOf(svc),
        rejected: bool = false,
        attempts: usize = 0,

        pub fn readChangefeedState(self: *@This(), alloc: A, id: control.Id, req: Request) ![]u8 {
            return control.readOnService(self.service, alloc, id, req);
        }

        pub fn mutateChangefeedState(self: *@This(), alloc: A, write: control.Write, req: Request) !void {
            self.attempts += 1;
            if (!self.rejected) {
                self.rejected = true;
                var adjacent = write.mutation.slot.feedback;
                adjacent.flushed = 1;
                try self.service.mutate(alloc, .{ .slot = .{ .feedback = adjacent } });
            }
            return control.mutateOnService(self.service, alloc, write, req);
        }
    };
    var concurrent: Concurrent = .{ .service = svc };
    try owner.feedback(&concurrent, &host, a, .{ .written = 104, .flushed = 104, .applied = 104 }, Request{});
    try std.testing.expectEqual(@as(usize, 2), concurrent.attempts);
    {
        var partial = try svc.load(a);
        defer partial.deinit();
        try std.testing.expectEqual(@as(u64, 1), partial.value.slot.?.confirmed_lsn);
    }
    svc.lose_ack = true;
    try std.testing.expectError(error.MetadataMutationOutcomeUnknown, owner.feedback(svc, &host, a, .{ .written = 105, .flushed = 105, .applied = 105 }, Request{}));
    svc.lose_ack = false;
    try owner.feedback(svc, &host, a, .{ .written = 105, .flushed = 105, .applied = 105 }, Request{});
    try owner.release(svc, &host, a, Request{});
    var confirmed = try svc.load(a);
    defer confirmed.deinit();
    try std.testing.expectEqual(@as(u64, 105), confirmed.value.slot.?.confirmed_lsn);
    try std.testing.expectEqual(@as(u64, 12), confirmed.value.roots[0].acknowledged.sequence);
    try std.testing.expectEqual(.garbage, confirmed.value.slot.?.spool.records[0].phase);
    try std.testing.expect(confirmed.value.slot.?.spool.charged_bytes != 0);
    try std.testing.expect(confirmed.value.slot.?.session == null);
}
