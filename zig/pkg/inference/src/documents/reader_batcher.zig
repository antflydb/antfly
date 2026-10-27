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

//! A wave-local ready queue borrows encoded crops until one shared native
//! reader invocation drains. Queue ownership never retains a model permit.
const std = @import("std");
const c = @import("contract.zig");
const readers = @import("antfly_readers");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const A = std.mem.Allocator;

pub fn Batcher(comptime Node: type) type {
    return struct {
        const Self = @This();
        const max_items = 64;
        io: std.Io,
        node: *Node,
        backing: A,
        max_parallel: usize = 4,
        mutex: std.atomic.Mutex = .unlocked,
        changed: std.Io.Event = .unset,
        pending: [max_items]*Ticket = undefined,
        count: usize = 0,
        stopping: bool = false,
        const Ticket = struct {
            allocator: A,
            model: []const u8,
            request: readers.EncodedRequest,
            control: Control,
            run: *c.Run,
            done: std.Io.Event = .unset,
            result: ?readers.BatchResult = null,
            failure: ?anyerror = null,
        };
        fn lock(self: *Self) void {
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        }
        pub fn stop(self: *Self) void {
            self.lock();
            self.stopping = true;
            self.mutex.unlock();
            self.changed.set(self.io);
        }
        pub fn read(self: *Self, a: A, model: []const u8, request: readers.EncodedRequest, control: Control, run: *c.Run) !readers.BatchResult {
            std.debug.assert(request.images.len == 1);
            if (request.images[0].bytes.len > run.limits.max_source_bytes) return error.DocumentSourceLimitExceeded;
            try control.check();
            var ticket = Ticket{ .allocator = a, .model = model, .request = request, .control = control, .run = run };
            self.lock();
            if (self.stopping or self.count == max_items) {
                self.mutex.unlock();
                return error.DocumentBudgetExhausted;
            }
            self.pending[self.count] = &ticket;
            self.count += 1;
            self.mutex.unlock();
            self.changed.set(self.io);
            // A queued stack borrow must survive cancellation until dispatch
            // drains it. Provider controls still carry the original cancellation
            // and deadline; no cancelled task can free a queued image early.
            const protection = self.io.swapCancelProtection(.blocked);
            defer _ = self.io.swapCancelProtection(protection);
            ticket.done.wait(self.io) catch unreachable;
            if (ticket.failure) |err| {
                if (ticket.result) |*result| result.deinit(a);
                return err;
            }
            return ticket.result.?;
        }
        fn compatible(left: *const Ticket, right: *const Ticket) bool {
            const shared = left.run.shared orelse return false;
            // Aggregate usage has no trustworthy per-item attribution. Share
            // it only within one owner and its shared token ceiling. A narrower
            // child must retain its own physical call/usage report; charging
            // the entire batch to one child could exhaust its allowance while
            // letting a sibling spend uncharged tokens on a later call.
            const ceiling = if (left.run.scope) |scope| scope.limits.max_total_tokens else shared.limits.max_total_tokens;
            if (right.run.durable != left.run.durable or right.run.scope != left.run.scope or right.run.shared != shared or left.run.limits.max_total_tokens != ceiling or right.run.limits.max_total_tokens != ceiling) return false;
            return std.mem.eql(u8, left.model, right.model) and
                std.meta.eql(left.request.prompt, right.request.prompt) and
                left.request.max_tokens == right.request.max_tokens and
                left.request.max_response_bytes == right.request.max_response_bytes and
                left.request.allow_incomplete == right.request.allow_incomplete and
                std.meta.eql(left.request.source_fingerprint, right.request.source_fingerprint) and
                std.meta.eql(left.run.limits, right.run.limits) and
                std.meta.eql(left.control, right.control);
        }
        pub fn execute(self: *Self) std.Io.Cancelable!void {
            // Dispatcher lifetime contains every queued stack borrow, including
            // while the parent is joining cancelled page workers.
            const protection = self.io.swapCancelProtection(.blocked);
            defer _ = self.io.swapCancelProtection(protection);
            while (true) {
                self.lock();
                if (self.count == 0) {
                    const stopping = self.stopping;
                    self.changed.reset();
                    self.mutex.unlock();
                    if (stopping) return;
                    try self.changed.wait(self.io);
                    continue;
                }
                self.mutex.unlock();
                // Bound coalescing latency independently of page complexity.
                try self.io.sleep(.fromMilliseconds(2), .awake);
                var ready: [max_items]*Ticket = undefined;
                self.lock();
                const count = self.count;
                @memcpy(ready[0..count], self.pending[0..count]);
                self.count = 0;
                self.mutex.unlock();
                var jobs: [max_items]Dispatch = undefined;
                var job_count: usize = 0;
                var width = @min(max_items, @max(1, self.max_parallel));
                for (ready[0..count]) |ticket| {
                    width = @min(width, ticket.run.limits.max_parallel_calls);
                    var selected: ?usize = null;
                    for (jobs[0..job_count], 0..) |*job, index| if (compatible(job.tickets[0], ticket) and ticket.request.images[0].bytes.len <= ticket.run.limits.max_source_bytes -| job.encoded_bytes) {
                        selected = index;
                        break;
                    };
                    const index = selected orelse new: {
                        jobs[job_count] = .{ .owner = self };
                        job_count += 1;
                        break :new job_count - 1;
                    };
                    const job = &jobs[index];
                    job.tickets[job.count] = ticket;
                    job.count += 1;
                    job.encoded_bytes += ticket.request.images[0].bytes.len;
                }
                var start: usize = 0;
                width = @max(1, width);
                while (start < job_count) {
                    const end = start + @min(width, job_count - start);
                    var group: std.Io.Group = .init;
                    for (jobs[start..end]) |*job| group.concurrent(self.io, Dispatch.execute, .{job}) catch job.execute() catch unreachable;
                    group.await(self.io) catch unreachable;
                    start = end;
                }
            }
        }
        const Dispatch = struct {
            owner: *Self,
            tickets: [max_items]*Ticket = undefined,
            count: usize = 0,
            encoded_bytes: usize = 0,
            fn execute(self: *Dispatch) std.Io.Cancelable!void {
                const protection = self.owner.io.swapCancelProtection(.blocked);
                defer _ = self.owner.io.swapCancelProtection(protection);
                const tickets = self.tickets[0..self.count];
                self.owner.dispatch(tickets) catch |err| for (tickets) |ticket| {
                    ticket.failure = err;
                };
                for (tickets) |ticket| ticket.done.set(self.owner.io);
            }
        };
        fn cloneResult(a: A, item: readers.Result) !readers.Result {
            var result = item;
            inline for (@typeInfo(readers.Result).@"struct".field_names, @typeInfo(readers.Result).@"struct".field_types) |name, T| {
                if (T == []const u8) @field(result, name) = "";
                if (T == ?[]const u8) @field(result, name) = null;
            }
            errdefer readers.deinitResult(a, &result);
            inline for (@typeInfo(readers.Result).@"struct".field_names, @typeInfo(readers.Result).@"struct".field_types) |name, T| {
                if (T == []const u8) @field(result, name) = try a.dupe(u8, @field(item, name));
                if (T == ?[]const u8) if (@field(item, name)) |value| {
                    @field(result, name) = try a.dupe(u8, value);
                };
            }
            return result;
        }
        fn dispatch(self: *Self, tickets: []const *Ticket) !void {
            var scratch = std.heap.ArenaAllocator.init(self.backing);
            defer scratch.deinit();
            const work = scratch.allocator();
            const first = tickets[0];
            try first.control.check();
            var images: [max_items]readers.EncodedImage = undefined;
            for (tickets, images[0..tickets.len]) |ticket, *item| item.* = ticket.request.images[0];
            var request = first.request;
            request.images = images[0..tickets.len];
            const Cancel = struct {
                fn cancelled(raw: *const anyopaque) bool {
                    const control: *const Control = @ptrCast(@alignCast(raw));
                    control.check() catch return true;
                    return false;
                }
            };
            if (tickets.len > 1) if (first.run.durable) |session| {
                var keys: [max_items]@import("antfly_readers").document_runs.Digest = undefined;
                for (tickets, 0..) |ticket, index| keys[index] = ticket.run.durable_stage orelse return error.InvalidDocumentCheckpoint;
                try session.combineTokenReservations(work, keys[0..tickets.len]);
            };
            first.run.physicalCall();
            const result = try self.node.readEncodedImagesReportedDirectWithContext(work, first.model, request, first.control.deadline_ns, .{ .ptr = &first.control, .is_cancelled_fn = Cancel.cancelled });
            try result.execution.validate(tickets.len);
            if (result.items.len != tickets.len) return error.InvalidDocumentModelResult;
            const physical = result.execution.native_batches + result.execution.serial_items;
            for (0..physical -| 1) |_| first.run.physicalCall();
            if (result.usage) |usage| {
                try first.run.tokens(@intCast(@max(0, usage.prompt_tokens)), @intCast(@max(0, usage.completion_tokens)));
                // Every item depends on the same measured batch cost. Preserve
                // that receipt on all item/page checkpoints, regardless of which
                // ticket reported it or which page is restored first.
                if (first.run.durable) |session| {
                    const receipt = session.modelTokenReceipt(first.run.durable_stage orelse return error.InvalidDocumentCheckpoint) orelse return error.InvalidDocumentCheckpoint;
                    for (tickets[1..]) |ticket| try ticket.run.touchCost(receipt);
                }
            }
            for (tickets, result.items) |ticket, item| {
                const output = try ticket.allocator.alloc(readers.Result, 1);
                errdefer ticket.allocator.free(output);
                output[0] = try cloneResult(ticket.allocator, item);
                // Aggregate usage was charged once before any waiter wakes.
                ticket.result = .{ .items = output, .execution = .{ .requested_items = 1, .serial_items = 1 } };
            }
        }
    };
}

test "document ready reader batching groups compatible work and charges aggregate usage once" {
    const Fake = struct {
        calls: usize = 0,
        pub fn readEncodedImagesReportedDirectWithContext(self: *@This(), a: A, _: []const u8, req: readers.EncodedRequest, _: ?u64, _: anytype) !readers.BatchResult {
            self.calls += 1;
            const items = try a.alloc(readers.Result, req.images.len);
            for (items, req.images) |*item, input| item.* = .{ .text = input.item_id, .item_id = input.item_id, .page_number = input.page_number };
            return .{ .items = items, .execution = .{ .requested_items = items.len, .native_batches = 1, .native_items = items.len }, .usage = .{ .prompt_tokens = 10, .completion_tokens = 3, .total_tokens = 13 } };
        }
    };
    const a = std.testing.allocator;
    var pool = std.Io.Threaded.init(a, .{});
    defer pool.deinit();
    for ([_]bool{ false, true }) |child_budget| {
        var node = Fake{};
        const Batch = Batcher(Fake);
        var batch = Batch{ .io = pool.io(), .node = &node, .backing = a, .stopping = true, .max_parallel = 1 };
        var ledger = c.SharedAccounting{ .limits = .{}, .usage = .{}, .event_count = 0 };
        const local_limits: c.Limits = if (child_budget) .{ .max_total_tokens = 100 } else .{};
        var scope = c.ScopeAccounting{ .limits = local_limits, .usage = .{} };
        var runs = [_]c.Run{ .{ .limits = local_limits, .shared = &ledger }, .{ .limits = local_limits, .shared = &ledger }, .{ .limits = local_limits, .shared = &ledger } };
        if (child_budget) for (&runs) |*run| {
            run.scope = &scope;
        };
        const images = [_]readers.EncodedImage{
            .{ .bytes = "first", .mime_type = "image/png", .item_id = "page-one", .page_number = 1 },
            .{ .bytes = "second", .mime_type = "image/png", .item_id = "page-two", .page_number = 2 },
            .{ .bytes = "third", .mime_type = "image/png", .item_id = "page-three", .page_number = 3 },
        };
        var tickets: [3]Batch.Ticket = undefined;
        for (&tickets, &runs, 0..) |*ticket, *run, i| {
            try run.call();
            ticket.* = .{ .allocator = a, .model = if (i == 2) "other-model" else "same-model", .request = .{ .images = images[i..][0..1] }, .control = .{}, .run = run };
            batch.pending[i] = ticket;
        }
        batch.count = tickets.len;
        try batch.execute();
        defer for (&tickets) |*ticket| if (ticket.result) |*result| result.deinit(a);
        try std.testing.expectEqual(@as(usize, 2), node.calls);
        try std.testing.expectEqual(@as(usize, 3), ledger.usage.model_calls);
        try std.testing.expectEqual(@as(usize, 2), ledger.usage.physical_model_calls);
        try std.testing.expectEqual(@as(u64, 26), ledger.usage.total_tokens);
        for (&tickets, images) |ticket, input| {
            try std.testing.expect(ticket.failure == null);
            try std.testing.expectEqualStrings(input.item_id, ticket.result.?.items[0].text);
            try std.testing.expectEqual(input.page_number, ticket.result.?.items[0].page_number);
            try std.testing.expect(ticket.result.?.usage == null);
        }
        if (child_budget) try std.testing.expectEqual(ledger.usage, scope.usage);
    }
}

test "document incompatible ready batches preserve concurrent calls and independent child token ceilings" {
    const Fake = struct {
        io: std.Io,
        entered: std.atomic.Value(usize) = .init(0),
        both_entered: std.Io.Event = .unset,
        pub fn readEncodedImagesReportedDirectWithContext(self: *@This(), a: A, model: []const u8, req: readers.EncodedRequest, _: ?u64, _: anytype) !readers.BatchResult {
            if (self.entered.fetchAdd(1, .acq_rel) == 1) self.both_entered.set(self.io);
            // Neither invocation may return until the other has entered. A
            // serial dispatcher fails within five seconds instead of hanging.
            try self.both_entered.waitTimeout(self.io, .{ .duration = .{ .raw = .fromMilliseconds(5000), .clock = .awake } });
            const items = try a.alloc(readers.Result, req.images.len);
            for (items) |*item| item.* = .{ .text = model };
            return .{ .items = items, .execution = .{ .requested_items = items.len, .native_batches = 1, .native_items = items.len }, .usage = .{ .prompt_tokens = 10, .completion_tokens = 3, .total_tokens = 13 } };
        }
    };
    const a = std.testing.allocator;
    var pool = std.Io.Threaded.init(a, .{});
    defer pool.deinit();
    for ([_]enum { different_models, child_budget, different_scopes }{ .different_models, .child_budget, .different_scopes }) |mode| {
        const child_budget = mode == .child_budget;
        var node = Fake{ .io = pool.io() };
        const Batch = Batcher(Fake);
        var batch = Batch{ .io = pool.io(), .node = &node, .backing = a, .stopping = true, .max_parallel = 2 };
        var ledger = c.SharedAccounting{ .limits = .{ .max_parallel_calls = 2, .max_total_tokens = 100 }, .usage = .{}, .event_count = 0 };
        const local_limits: c.Limits = .{ .max_parallel_calls = 2, .max_total_tokens = if (child_budget) 20 else 100 };
        var scopes = [_]c.ScopeAccounting{ .{ .limits = local_limits, .usage = .{} }, .{ .limits = local_limits, .usage = .{} } };
        var runs = [_]c.Run{ .{ .limits = local_limits, .shared = &ledger }, .{ .limits = local_limits, .shared = &ledger } };
        if (mode == .different_scopes) for (&runs, &scopes) |*run, *scope| {
            run.scope = scope;
        };
        const input = [_]readers.EncodedImage{.{ .bytes = "image", .mime_type = "image/png" }};
        var tickets: [2]Batch.Ticket = undefined;
        for (&tickets, &runs, 0..) |*ticket, *run, index| {
            try run.call();
            ticket.* = .{ .allocator = a, .model = if (mode != .different_models or index == 0) "ocr-fast" else "ocr-tables", .request = .{ .images = &input }, .control = .{}, .run = run };
            batch.pending[index] = ticket;
        }
        batch.count = tickets.len;
        try batch.execute();
        defer for (&tickets) |*ticket| if (ticket.result) |*result| result.deinit(a);
        for (&tickets) |ticket| {
            if (ticket.failure) |err| return err;
            try std.testing.expectEqualStrings(ticket.model, ticket.result.?.items[0].text);
        }
        try std.testing.expectEqual(@as(usize, 2), ledger.usage.physical_model_calls);
        try std.testing.expectEqual(@as(u64, 26), ledger.usage.total_tokens);
        for (runs) |run| try std.testing.expectEqual(@as(u64, 13), run.usage.total_tokens);
        if (mode == .different_scopes) for (scopes) |scope| try std.testing.expectEqual(@as(u64, 13), scope.usage.total_tokens);
    }
}

test "document reader batch results own every JSON field and release failed clones" {
    const Check = struct {
        fn run(a: A) !void {
            var result = try Batcher(struct {}).cloneResult(a, .{ .text = "OCR", .document_json = "{\"text\":\"OCR\"}", .fields_json = "{}", .regions_json = "[]", .item_id = "page", .source_fingerprint = "source", .page_number = 7 });
            defer readers.deinitResult(a, &result);
            try std.testing.expectEqualStrings("OCR", result.text);
            try std.testing.expectEqualStrings("{\"text\":\"OCR\"}", result.document_json.?);
            try std.testing.expectEqual(@as(?u32, 7), result.page_number);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "document durable native batch attaches one measured receipt to every dependent item" {
    const readers_lib = @import("antfly_readers");
    const runs_lib = readers_lib.document_runs;
    const Fake = struct {
        fn readEncodedImagesReportedDirectWithContext(_: *@This(), a: A, _: []const u8, req: readers.EncodedRequest, _: ?u64, _: anytype) !readers.BatchResult {
            const items = try a.alloc(readers.Result, req.images.len);
            for (items) |*item| item.* = .{ .text = "Text" };
            return .{ .items = items, .execution = .{ .requested_items = items.len, .native_batches = 1, .native_items = items.len }, .usage = .{ .prompt_tokens = 10, .completion_tokens = 3, .total_tokens = 13 } };
        }
    };
    const a = std.testing.allocator;
    var memory = readers_lib.testing.Store{ .a = a };
    defer memory.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const work = arena.allocator();
    const store = memory.interface();
    const identity: runs_lib.Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
    try store.create(.{ .run_id = "batch", .owner = "alice", .identity = identity, .limits = .{ .calls = 3, .tokens = 300 }, .inputs = &.{} });
    const Clock = struct {
        fn now(_: ?*anyopaque) u64 {
            return 1;
        }
    };
    const lease = try store.claim(work, "batch", "alice", identity, 1, 100);
    var session = try runs_lib.Session.init(a, store, lease, identity, .{ .now_fn = Clock.now }, 16, 32768);
    defer session.deinit();
    var pool = std.Io.Threaded.init(a, .{});
    defer pool.deinit();
    const Batch = Batcher(Fake);
    var node = Fake{};
    var batch = Batch{ .io = pool.io(), .node = &node, .backing = a, .stopping = true, .max_parallel = 1 };
    var ledger = c.SharedAccounting{ .limits = .{}, .usage = .{}, .event_count = 0 };
    const keys = [_]runs_lib.Digest{ @splat(1), @splat(2), @splat(3) };
    var children: [3]c.Run = undefined;
    var initialized: usize = 0;
    defer for (children[0..initialized]) |*child| child.deinit();
    const images = [_]readers.EncodedImage{.{ .bytes = "image", .mime_type = "image/png" }};
    var tickets: [3]Batch.Ticket = undefined;
    var initialized_tickets: usize = 0;
    defer for (tickets[0..initialized_tickets]) |*ticket| if (ticket.result) |*result| result.deinit(a);
    for (&children, &tickets, keys, 0..) |*child, *ticket, key, index| {
        child.* = .{ .limits = .{}, .shared = &ledger, .durable = &session, .durable_stage = key };
        initialized += 1;
        try session.beginIntent(key, .{ .calls = 1, .tokens = 100 });
        try child.call();
        ticket.* = .{ .allocator = a, .model = if (index < 2) "same" else "other", .request = .{ .images = &images }, .control = .{}, .run = child };
        initialized_tickets += 1;
        batch.pending[index] = ticket;
    }
    batch.count = 3;
    try batch.execute();
    for (&tickets) |*ticket| if (ticket.failure) |failure| return failure;
    try std.testing.expectEqual(@as(u64, 26), ledger.usage.total_tokens);
    try std.testing.expectEqual(@as(usize, 2), ledger.usage.physical_model_calls);
    for (children[0..2]) |*child| {
        const receipts = try child.measured_costs.snapshot(work);
        try std.testing.expectEqual(@as(usize, 1), receipts.len);
        try std.testing.expectEqual(session.modelCostKey(keys[0]), receipts[0].key);
        try std.testing.expectEqual(@as(u64, 10), receipts[0].measured.prompt_tokens);
        try std.testing.expectEqual(@as(u64, 3), receipts[0].measured.completion_tokens);
    }
    const other = try children[2].measured_costs.snapshot(work);
    try std.testing.expectEqual(session.modelCostKey(keys[2]), other[0].key);
}

test "document durable native batch costs survive failed reporter retries in either replay order" {
    const runs = readers.document_runs;
    const Fake = struct {
        fn readEncodedImagesReportedDirectWithContext(_: *@This(), a: A, _: []const u8, req: readers.EncodedRequest, _: ?u64, _: anytype) !readers.BatchResult {
            const items = try a.alloc(readers.Result, req.images.len);
            for (items) |*item| item.* = .{ .text = "Text" };
            return .{ .items = items, .execution = .{ .requested_items = items.len, .native_batches = 1, .native_items = items.len }, .usage = .{ .prompt_tokens = 10, .completion_tokens = 3, .total_tokens = 13 } };
        }
    };
    const Retry = struct { prompt: u64, completion: u64 };
    for ([_]Retry{ .{ .prompt = 10, .completion = 3 }, .{ .prompt = 3, .completion = 10 }, .{ .prompt = 4, .completion = 2 } }) |retry| {
        for ([_]bool{ true, false }) |replay_first| {
            const a = std.testing.allocator;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const work = arena.allocator();
            var memory = readers.testing.Store{ .a = a };
            defer memory.deinit();
            const store = memory.interface();
            const identity: runs.Identity = .{ .request = @splat(1), .sources = @splat(2), .definition = @splat(3), .models = @splat(4), .runtime = @splat(5) };
            try store.create(.{ .run_id = "batch-retry", .owner = "alice", .identity = identity, .limits = .{ .calls = 3, .tokens = 300, .output_bytes = 1048576 }, .inputs = &.{} });
            const Clock = struct {
                fn now(_: ?*anyopaque) u64 {
                    return 1;
                }
            };
            const lease = try store.claim(work, "batch-retry", "alice", identity, 1, 100);
            var session = try runs.Session.init(a, store, lease, identity, .{ .now_fn = Clock.now }, 16, 32768);
            defer session.deinit();
            var pool = std.Io.Threaded.init(a, .{});
            defer pool.deinit();
            const Batch = Batcher(Fake);
            var node = Fake{};
            var batch = Batch{ .io = pool.io(), .node = &node, .backing = a, .stopping = true, .max_parallel = 1 };
            const keys = [_]runs.Digest{ @splat(7), @splat(8) };
            const upper: runs.Charge = .{ .calls = 1, .tokens = 100, .output_bytes = 4096 };
            var ledger = c.SharedAccounting{ .limits = .{}, .usage = .{}, .event_count = 0 };
            var children = [_]c.Run{
                .{ .limits = .{}, .durable = &session, .durable_stage = keys[0], .shared = &ledger },
                .{ .limits = .{}, .durable = &session, .durable_stage = keys[1], .shared = &ledger },
            };
            defer for (&children) |*child| child.deinit();
            const images = [_]readers.EncodedImage{.{ .bytes = "image", .mime_type = "image/png" }};
            var tickets = [_]Batch.Ticket{
                .{ .allocator = a, .model = "same", .request = .{ .images = &images }, .control = .{}, .run = &children[0] },
                .{ .allocator = a, .model = "same", .request = .{ .images = &images }, .control = .{}, .run = &children[1] },
            };
            defer for (&tickets) |*ticket| if (ticket.result) |*result| result.deinit(a);
            for (&tickets, 0..) |*ticket, index| {
                try ticket.run.call();
                batch.pending[index] = ticket;
            }
            batch.count = 2;
            try session.beginIntent(keys[1], upper);
            const Saved = struct { usage: c.Usage, costs: []const c.costs.Receipt };
            const Context = struct {
                batch: *Batch,
                child: *c.Run,
                session: *runs.Session,
                key: runs.Digest,
                fn invoke(self: @This(), alloc: A) !runs.Session.Output {
                    try self.batch.execute();
                    for (self.batch.pending[0..2]) |ticket| if (ticket.failure) |err| return err;
                    const bytes = try std.json.Stringify.valueAlloc(alloc, Saved{ .usage = self.child.usage, .costs = try self.child.measured_costs.snapshot(alloc) }, .{});
                    _ = try self.session.commitIntent(alloc, self.key, .{ .calls = 1, .output_bytes = bytes.len }, bytes, "");
                    // The sibling accepted its result, but the reporting item's decode
                    // failed after aggregate usage had been measured for the batch.
                    return error.InvalidDocumentModelResult;
                }
            };
            try std.testing.expectError(error.InvalidDocumentModelResult, session.execute(work, keys[0], upper, 4096, Context{ .batch = &batch, .child = &children[1], .session = &session, .key = keys[1] }, Context.invoke));
            try std.testing.expectEqual(@as(u64, 13), ledger.usage.total_tokens);
            try store.yieldLease(work, lease, 1);
            const next = try store.claim(work, "batch-retry", "alice", identity, 1, 100);
            var resumed = try runs.Session.init(a, store, next, identity, .{ .now_fn = Clock.now }, 16, 32768);
            defer resumed.deinit();
            const expected_tokens = 13 + retry.prompt + retry.completion;
            var fresh = c.Run{ .limits = .{ .max_total_tokens = expected_tokens, .max_model_calls = 2 }, .durable = &resumed, .durable_stage = keys[0] };
            defer fresh.deinit();
            const cached = (try resumed.replay(work, keys[1], 4096)).?;
            const saved = try std.json.parseFromSliceLeaky(Saved, work, cached.bytes, .{});
            if (replay_first) try fresh.restoreCosts(saved.usage, saved.costs);
            try resumed.beginIntent(keys[0], upper);
            try fresh.call();
            fresh.physicalCall();
            try fresh.tokens(retry.prompt, retry.completion);
            const current_cost = resumed.modelTokenReceipt(keys[0]).?;
            try std.testing.expectEqual(@as(usize, 1), saved.costs.len);
            try std.testing.expect(!std.mem.eql(u8, &saved.costs[0].key, &current_cost.key));
            const current_usage: c.Usage = .{ .model_calls = 1, .prompt_tokens = retry.prompt, .completion_tokens = retry.completion, .total_tokens = retry.prompt + retry.completion };
            const payload = try std.json.Stringify.valueAlloc(work, Saved{ .usage = current_usage, .costs = &.{current_cost} }, .{});
            _ = try resumed.commitIntent(work, keys[0], .{ .calls = 1, .tokens = current_usage.total_tokens, .output_bytes = payload.len }, payload, "");
            if (!replay_first) try fresh.restoreCosts(saved.usage, saved.costs);
            try std.testing.expectEqual(expected_tokens, fresh.usage.total_tokens);
            try std.testing.expectEqual(10 + retry.prompt, fresh.usage.prompt_tokens);
            try std.testing.expectEqual(3 + retry.completion, fresh.usage.completion_tokens);
            try std.testing.expectEqual(@as(usize, 2), fresh.usage.model_calls);
            try std.testing.expectEqual(@as(usize, 1), fresh.usage.physical_model_calls);
            const record = try store.load(work, "batch-retry", "alice");
            try std.testing.expectEqual(@as(u64, 200), record.uncertain.tokens);
            try std.testing.expectEqual(200 + current_usage.total_tokens, record.used.tokens);
            try store.yieldLease(work, next, 1);
            const again = try store.claim(work, "batch-retry", "alice", identity, 1, 100);
            var final_session = try runs.Session.init(a, store, again, identity, .{ .now_fn = Clock.now }, 16, 32768);
            defer final_session.deinit();
            var final_run = c.Run{ .limits = fresh.limits, .durable = &final_session };
            defer final_run.deinit();
            // A second handoff replays both accepted receipts, including the
            // original sibling, without remapping their execution identities.
            const order = if (replay_first) [_]usize{ 0, 1 } else [_]usize{ 1, 0 };
            for (order) |index| {
                const accepted = (try final_session.replay(work, keys[index], 4096)).?;
                const value = try std.json.parseFromSliceLeaky(Saved, work, accepted.bytes, .{});
                try final_run.restoreCosts(value.usage, value.costs);
            }
            try std.testing.expectEqual(expected_tokens, final_run.usage.total_tokens);
            try std.testing.expectEqual(@as(usize, 2), final_run.usage.model_calls);
            try std.testing.expectEqual(@as(usize, 0), final_run.usage.physical_model_calls);
        }
    }
}
