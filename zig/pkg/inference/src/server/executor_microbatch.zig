// Copyright 2026 Antfly, Inc.
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

//! Lazy, task-neutral coalescing for resolved native inference executors.
//!
//! This lives beside the inference Node rather than the storage BackendRuntime:
//! the Node owns loaded model generations, authoritative capability contracts,
//! request cancellation, and the concrete fused executor functions. The broker
//! owns no thread pool and allocates no idle workers; the first item in a group
//! becomes its bounded leader and uses the caller's existing `std.Io` executor.

const std = @import("std");

pub const Task = enum {
    read,
    generate,
    embed,
    rerank,
    chunk,
    extract,
    rewrite,
    transcribe,
};

pub const BatchMode = enum { none, serial_compatibility, native };
pub const ResourceClass = enum { cpu, gpu, remote };
pub const Execution = enum { native_batch, serial, fallback };

pub const Identity = struct {
    item_id: []const u8 = "",
    source_fingerprint: ?[]const u8 = null,
    page_number: ?u32 = null,
};

/// Exact grouping identity. String fields are compared byte-for-byte and stay
/// borrowed until synchronous `submit` returns. `option_key` is for compact
/// scalar configuration such as an output-token ceiling.
pub const Key = struct {
    model: []const u8,
    /// Opaque process-local identity of the immutable loaded executor
    /// generation. A stable model path can be republished while older requests
    /// still hold the retired generation alive.
    generation: usize = 0,
    task: Task,
    prompt: []const u8 = "",
    schema: []const u8 = "",
    transform: []const u8 = "",
    option_key: u64 = 0,
    resource_class: ResourceClass,

    fn eql(a: Key, b: Key) bool {
        return a.generation == b.generation and
            a.task == b.task and
            a.option_key == b.option_key and
            a.resource_class == b.resource_class and
            std.mem.eql(u8, a.model, b.model) and
            std.mem.eql(u8, a.prompt, b.prompt) and
            std.mem.eql(u8, a.schema, b.schema) and
            std.mem.eql(u8, a.transform, b.transform);
    }
};

pub const Limits = struct {
    mode: BatchMode,
    preferred_items: usize,
    max_items: usize,
    max_bytes: usize = std.math.maxInt(usize),
    max_pixels: u64 = std.math.maxInt(u64),
    max_tokens: usize = std.math.maxInt(usize),
    max_wait_us: u64 = 0,

    pub fn validate(self: Limits) !void {
        if (self.preferred_items == 0 or self.max_items == 0 or
            self.preferred_items > self.max_items)
        {
            return error.InvalidMicrobatchLimits;
        }
        if (self.mode != .native and (self.preferred_items != 1 or self.max_items != 1))
            return error.InvalidMicrobatchLimits;
    }

    fn eql(a: Limits, b: Limits) bool {
        return std.meta.eql(a, b);
    }
};

pub const Shape = struct {
    bytes: usize = 0,
    pixels: u64 = 0,
    tokens: usize = 0,
};

/// Transport-neutral cancellation probe. The broker may retain this borrowed
/// context only for the synchronous duration of submitControlled.
pub const Cancellation = struct {
    ptr: ?*const anyopaque = null,
    is_cancelled_fn: ?*const fn (*const anyopaque) bool = null,

    pub fn fromAtomic(signal: ?*const std.atomic.Value(bool)) Cancellation {
        const value = signal orelse return .{};
        return .{
            .ptr = value,
            .is_cancelled_fn = struct {
                fn call(ptr: *const anyopaque) bool {
                    const atomic: *const std.atomic.Value(bool) = @ptrCast(@alignCast(ptr));
                    return atomic.load(.acquire);
                }
            }.call,
        };
    }

    pub fn isCancelled(self: Cancellation) bool {
        const ptr = self.ptr orelse return false;
        const callback = self.is_cancelled_fn orelse return false;
        return callback(ptr);
    }
};

pub const ItemError = struct {
    cause: anyerror,
};

pub fn ItemResult(comptime T: type) type {
    return struct {
        identity: Identity,
        execution: Execution,
        /// Process-local identity of the fused executor invocation. Zero means
        /// the item bypassed native grouping before an invocation was formed.
        execution_id: u64 = 0,
        result: union(enum) {
            value: T,
            item_error: ItemError,
        },
    };
}

pub const ResultSlot = struct {
    output: *anyopaque,
    completed: bool = false,
    err: ?anyerror = null,
    execution: Execution = .serial,
    execution_id: u64 = 0,

    pub fn setValue(self: *ResultSlot, comptime T: type, value: T, execution: Execution) void {
        const output: *T = @ptrCast(@alignCast(self.output));
        output.* = value;
        self.execution = execution;
        self.err = null;
        self.completed = true;
    }

    pub fn fail(self: *ResultSlot, err: anyerror) void {
        self.err = err;
        self.completed = true;
    }
};

pub const ExecuteItem = struct {
    allocator: std.mem.Allocator,
    identity: Identity,
    payload: *const anyopaque,
    slot: *ResultSlot,

    pub fn payloadAs(self: ExecuteItem, comptime T: type) *const T {
        return @ptrCast(@alignCast(self.payload));
    }
};

pub const ExecuteFn = *const fn (ctx: *anyopaque, items: []const ExecuteItem) void;

const TicketState = enum { queued, executing, complete };

const Ticket = struct {
    item: ExecuteItem,
    done: std.Io.Event = .unset,
    state: TicketState = .queued,
    cancel_requested: std.atomic.Value(bool) = .init(false),
    cancellation: Cancellation,
    deadline: ?std.Io.Clock.Timestamp,
};

const Group = struct {
    key: Key,
    limits: Limits,
    execute_ctx: *anyopaque,
    execute_fn: ExecuteFn,
    tickets: std.ArrayListUnmanaged(*Ticket) = .empty,
    bytes: usize = 0,
    pixels: u64 = 0,
    tokens: usize = 0,
    created_at: std.Io.Clock.Timestamp,
    earliest_deadline: ?std.Io.Clock.Timestamp = null,
    flush_requested: bool = false,
    wake: std.Io.Event = .unset,

    fn fits(self: *const Group, shape: Shape) bool {
        if (self.tickets.items.len >= self.limits.max_items) return false;
        const bytes = std.math.add(usize, self.bytes, shape.bytes) catch return false;
        const pixels = std.math.add(u64, self.pixels, shape.pixels) catch return false;
        const tokens = std.math.add(usize, self.tokens, shape.tokens) catch return false;
        return bytes <= self.limits.max_bytes and
            pixels <= self.limits.max_pixels and
            tokens <= self.limits.max_tokens;
    }

    fn append(self: *Group, allocator: std.mem.Allocator, ticket: *Ticket, shape: Shape) !bool {
        try self.tickets.append(allocator, ticket);
        self.bytes += shape.bytes;
        self.pixels += shape.pixels;
        self.tokens += shape.tokens;
        const deadline = ticket.deadline orelse return false;
        const previous = self.earliest_deadline;
        if (previous == null or deadline.raw.nanoseconds < previous.?.raw.nanoseconds) {
            self.earliest_deadline = deadline;
            return true;
        }
        return false;
    }
};

pub const Stats = struct {
    native_batches: u64 = 0,
    native_items: u64 = 0,
    bypass_items: u64 = 0,
    canceled_items: u64 = 0,
};

const broker_shard_count: usize = 16;

const BrokerShard = struct {
    mutex: std.Io.Mutex = .init,
    groups: std.ArrayListUnmanaged(*Group) = .empty,
    stats: Stats = .{},
};

fn updateBrokerKeyHash(hasher: *std.hash.Wyhash, value: []const u8) void {
    var len = value.len;
    hasher.update(std.mem.asBytes(&len));
    hasher.update(value);
}

pub const Broker = struct {
    allocator: std.mem.Allocator,
    /// Independent model/task cohorts never contend on one process-wide lock.
    /// Exact-key grouping remains unchanged within a shard.
    shards: [broker_shard_count]BrokerShard = [_]BrokerShard{.{}} ** broker_shard_count,
    next_execution_id: std.atomic.Value(u64) = .init(1),

    pub fn init(allocator: std.mem.Allocator) Broker {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Broker) void {
        for (&self.shards) |*shard| {
            std.debug.assert(shard.groups.items.len == 0);
            shard.groups.deinit(self.allocator);
        }
        self.* = undefined;
    }

    pub fn snapshot(self: *Broker, io: std.Io) Stats {
        var total = Stats{};
        for (&self.shards) |*shard| {
            shard.mutex.lockUncancelable(io);
            total.native_batches +|= shard.stats.native_batches;
            total.native_items +|= shard.stats.native_items;
            total.bypass_items +|= shard.stats.bypass_items;
            total.canceled_items +|= shard.stats.canceled_items;
            shard.mutex.unlock(io);
        }
        return total;
    }

    pub fn submit(
        self: *Broker,
        comptime Payload: type,
        comptime Output: type,
        io: std.Io,
        result_allocator: std.mem.Allocator,
        key: Key,
        limits: Limits,
        shape: Shape,
        identity: Identity,
        deadline: ?std.Io.Clock.Timestamp,
        cancellation: ?*const std.atomic.Value(bool),
        payload: *const Payload,
        execute_ctx: *anyopaque,
        execute_fn: ExecuteFn,
    ) !ItemResult(Output) {
        return self.submitControlled(
            Payload,
            Output,
            io,
            result_allocator,
            key,
            limits,
            shape,
            identity,
            deadline,
            Cancellation.fromAtomic(cancellation),
            payload,
            execute_ctx,
            execute_fn,
        );
    }

    pub fn submitControlled(
        self: *Broker,
        comptime Payload: type,
        comptime Output: type,
        io: std.Io,
        result_allocator: std.mem.Allocator,
        key: Key,
        limits: Limits,
        shape: Shape,
        identity: Identity,
        deadline: ?std.Io.Clock.Timestamp,
        cancellation: Cancellation,
        payload: *const Payload,
        execute_ctx: *anyopaque,
        execute_fn: ExecuteFn,
    ) !ItemResult(Output) {
        try limits.validate();
        if (shape.bytes > limits.max_bytes or shape.pixels > limits.max_pixels or shape.tokens > limits.max_tokens)
            return resultError(Output, identity, error.MicrobatchResourceLimitExceeded);
        if (isCanceled(cancellation))
            return resultError(Output, identity, error.Canceled);
        if (deadlineExpired(io, deadline))
            return resultError(Output, identity, error.DeadlineExceeded);

        var output: Output = undefined;
        var slot = ResultSlot{ .output = @ptrCast(&output) };
        var ticket = Ticket{
            .item = .{
                .allocator = result_allocator,
                .identity = identity,
                .payload = payload,
                .slot = &slot,
            },
            .cancellation = cancellation,
            .deadline = deadline,
        };

        // Serial compatibility is deliberately not queued: accepting a batch
        // envelope is not evidence that the underlying model fuses work.
        if (limits.mode != .native or limits.max_items == 1) {
            execute_fn(execute_ctx, &.{ticket.item});
            if (!slot.completed) slot.fail(error.MissingMicrobatchResult);
            const shard = self.shardFor(key);
            shard.mutex.lockUncancelable(io);
            shard.stats.bypass_items +|= 1;
            shard.mutex.unlock(io);
            return takeResult(Output, identity, &output, slot);
        }

        const shard = self.shardFor(key);
        var group: *Group = undefined;
        var leader = false;
        shard.mutex.lockUncancelable(io);
        for (shard.groups.items) |candidate| {
            if (candidate.key.eql(key) and candidate.limits.eql(limits) and
                candidate.execute_ctx == execute_ctx and candidate.execute_fn == execute_fn and
                candidate.fits(shape))
            {
                group = candidate;
                break;
            }
        } else {
            group = self.allocator.create(Group) catch |err| {
                shard.mutex.unlock(io);
                return err;
            };
            group.* = .{
                .key = key,
                .limits = limits,
                .execute_ctx = execute_ctx,
                .execute_fn = execute_fn,
                .created_at = std.Io.Clock.Timestamp.now(io, .awake),
            };
            shard.groups.append(self.allocator, group) catch |err| {
                self.allocator.destroy(group);
                shard.mutex.unlock(io);
                return err;
            };
            leader = true;
        }
        const deadline_shortened = group.append(self.allocator, &ticket, shape) catch |err| {
            if (leader) {
                _ = shard.groups.pop();
                self.allocator.destroy(group);
            }
            shard.mutex.unlock(io);
            return err;
        };
        if (group.tickets.items.len >= group.limits.preferred_items or
            group.tickets.items.len == group.limits.max_items)
        {
            group.flush_requested = true;
            group.wake.set(io);
        } else if (deadline_shortened and !leader) {
            // Wake the leader to recompute the bounded window. An earlier
            // follower deadline shortens the wait; it does not force a tiny
            // batch when there is still time to admit compatible work.
            group.wake.set(io);
        }
        shard.mutex.unlock(io);

        if (leader) {
            self.waitForGroupWindow(io, shard, group, &ticket);
            self.executeGroup(io, shard, group);
        } else {
            ticket.done.wait(io) catch |err| switch (err) {
                error.Canceled => {
                    ticket.cancel_requested.store(true, .release);
                    // The queued ticket borrows this stack frame. It cannot be
                    // returned until the leader has observed cancellation.
                    ticket.done.waitUncancelable(io);
                },
            };
        }

        // Cancellation can only remove queued work. Once the leader has moved
        // this ticket to executing, the typed completed value/error wins: the
        // broker has no generic destructor with which to discard an owned T.
        // The canceling caller already waited above for the borrowed stack
        // ticket to complete, so returning that result is lifetime-safe.
        return takeResult(Output, identity, &output, slot);
    }

    /// Submit an existing request batch as independently attributable work.
    /// Lightweight fibers enqueue the items concurrently, allowing them to
    /// fill the same native group and to coalesce with compatible items from
    /// other requests. Per-item payloads, shapes, identities, and cancellation
    /// contexts remain borrowed until this synchronous method returns.
    pub fn submitBatchControlled(
        self: *Broker,
        comptime Payload: type,
        comptime Output: type,
        io: std.Io,
        result_allocator: std.mem.Allocator,
        key: Key,
        limits: Limits,
        shapes: []const Shape,
        identities: []const Identity,
        deadline: ?std.Io.Clock.Timestamp,
        cancellation: Cancellation,
        payloads: []const Payload,
        execute_ctx: *anyopaque,
        execute_fn: ExecuteFn,
    ) ![]ItemResult(Output) {
        if (payloads.len != shapes.len or payloads.len != identities.len)
            return error.InvalidMicrobatchInput;
        try limits.validate();
        const results = try result_allocator.alloc(ItemResult(Output), payloads.len);
        errdefer result_allocator.free(results);
        if (payloads.len == 0) return results;
        for (results, identities) |*result, identity|
            result.* = resultError(Output, identity, error.Canceled);

        // Compatibility executors are explicitly singleton and may own
        // thread-confined state. Preserve that contract for array callers;
        // only genuine native batching is fanned into concurrent tickets.
        if (limits.mode != .native or limits.max_items == 1) {
            for (payloads, shapes, identities, results) |*payload, shape, identity, *result| {
                result.* = self.submitControlled(
                    Payload,
                    Output,
                    io,
                    result_allocator,
                    key,
                    limits,
                    shape,
                    identity,
                    deadline,
                    cancellation,
                    payload,
                    execute_ctx,
                    execute_fn,
                ) catch |err| resultError(Output, identity, err);
            }
            return results;
        }

        const BatchSubmit = struct {
            broker: *Broker,
            io: std.Io,
            result_allocator: std.mem.Allocator,
            key: Key,
            limits: Limits,
            shape: Shape,
            identity: Identity,
            deadline: ?std.Io.Clock.Timestamp,
            cancellation: Cancellation,
            payload: *const Payload,
            execute_ctx: *anyopaque,
            execute_fn: ExecuteFn,
            result: *ItemResult(Output),

            fn run(task: *@This()) std.Io.Cancelable!void {
                task.result.* = task.broker.submitControlled(
                    Payload,
                    Output,
                    task.io,
                    task.result_allocator,
                    task.key,
                    task.limits,
                    task.shape,
                    task.identity,
                    task.deadline,
                    task.cancellation,
                    task.payload,
                    task.execute_ctx,
                    task.execute_fn,
                ) catch |err| resultError(Output, task.identity, err);
            }
        };
        const tasks = try result_allocator.alloc(BatchSubmit, payloads.len);
        defer result_allocator.free(tasks);
        var group: std.Io.Group = .init;
        for (tasks, payloads, shapes, identities, results) |*task, *payload, shape, identity, *result| {
            task.* = .{
                .broker = self,
                .io = io,
                .result_allocator = result_allocator,
                .key = key,
                .limits = limits,
                .shape = shape,
                .identity = identity,
                .deadline = deadline,
                .cancellation = cancellation,
                .payload = payload,
                .execute_ctx = execute_ctx,
                .execute_fn = execute_fn,
                .result = result,
            };
            group.async(io, BatchSubmit.run, .{task});
        }
        // Each child converts broker/control failures into its typed item
        // envelope. Await still joins every borrowed ticket before returning.
        group.await(io) catch {};
        return results;
    }

    fn shardFor(self: *Broker, key: Key) *BrokerShard {
        var hasher = std.hash.Wyhash.init(0);
        updateBrokerKeyHash(&hasher, key.model);
        hasher.update(std.mem.asBytes(&key.generation));
        hasher.update(std.mem.asBytes(&key.task));
        updateBrokerKeyHash(&hasher, key.prompt);
        updateBrokerKeyHash(&hasher, key.schema);
        updateBrokerKeyHash(&hasher, key.transform);
        hasher.update(std.mem.asBytes(&key.option_key));
        hasher.update(std.mem.asBytes(&key.resource_class));
        const index: usize = @intCast(hasher.final() % broker_shard_count);
        return &self.shards[index];
    }

    fn waitForGroupWindow(_: *Broker, io: std.Io, shard: *BrokerShard, group: *Group, leader: *Ticket) void {
        if (group.limits.max_wait_us == 0) return;
        while (true) {
            shard.mutex.lockUncancelable(io);
            if (group.flush_requested) {
                shard.mutex.unlock(io);
                return;
            }
            const wait_deadline = boundedLeaderWaitDeadline(
                group.created_at,
                group.limits.max_wait_us,
                group.earliest_deadline,
            );
            const now = std.Io.Clock.Timestamp.now(io, .awake);
            if (wait_deadline.raw.nanoseconds <= now.raw.nanoseconds) {
                shard.mutex.unlock(io);
                return;
            }
            // Reset while holding the group lock. A follower either published
            // its deadline before this recomputation, or will set the event
            // after acquiring the same lock; no update can be lost.
            group.wake.reset();
            shard.mutex.unlock(io);

            const timeout: std.Io.Timeout = .{ .deadline = wait_deadline };
            group.wake.waitTimeout(io, timeout) catch |err| switch (err) {
                error.Timeout => return,
                error.Canceled => {
                    leader.cancel_requested.store(true, .release);
                    return;
                },
            };
        }
    }

    fn executeGroup(self: *Broker, io: std.Io, shard: *BrokerShard, group: *Group) void {
        shard.mutex.lockUncancelable(io);
        for (shard.groups.items, 0..) |candidate, index| {
            if (candidate == group) {
                _ = shard.groups.swapRemove(index);
                break;
            }
        }
        const items = self.allocator.alloc(ExecuteItem, group.tickets.items.len) catch {
            for (group.tickets.items) |ticket| ticket.item.slot.fail(error.OutOfMemory);
            self.finishGroupLocked(io, shard, group, 0);
            return;
        };
        var active_count: usize = 0;
        const now = std.Io.Clock.Timestamp.now(io, .awake);
        for (group.tickets.items) |ticket| {
            const canceled = ticket.cancel_requested.load(.acquire) or isCanceled(ticket.cancellation);
            const expired = if (ticket.deadline) |deadline|
                deadline.clock != .awake or deadline.raw.nanoseconds <= now.raw.nanoseconds
            else
                false;
            if (canceled or expired) {
                ticket.item.slot.fail(if (expired) error.DeadlineExceeded else error.Canceled);
                shard.stats.canceled_items +|= 1;
                continue;
            }
            ticket.state = .executing;
            items[active_count] = ticket.item;
            active_count += 1;
        }
        shard.mutex.unlock(io);

        if (active_count > 0) {
            const execution_id = self.next_execution_id.fetchAdd(1, .monotonic);
            group.execute_fn(group.execute_ctx, items[0..active_count]);
            for (items[0..active_count]) |item| {
                if (!item.slot.completed) item.slot.fail(error.MissingMicrobatchResult);
                item.slot.execution_id = execution_id;
            }
        }
        self.allocator.free(items);

        shard.mutex.lockUncancelable(io);
        shard.stats.native_batches +|= @intFromBool(active_count > 0);
        shard.stats.native_items +|= active_count;
        self.finishGroupLocked(io, shard, group, active_count);
    }

    fn finishGroupLocked(self: *Broker, io: std.Io, shard: *BrokerShard, group: *Group, _: usize) void {
        for (group.tickets.items) |ticket| {
            ticket.state = .complete;
            ticket.done.set(io);
        }
        group.tickets.deinit(self.allocator);
        self.allocator.destroy(group);
        shard.mutex.unlock(io);
    }
};

fn isCanceled(cancellation: Cancellation) bool {
    return cancellation.isCancelled();
}

fn deadlineExpired(io: std.Io, deadline: ?std.Io.Clock.Timestamp) bool {
    const value = deadline orelse return false;
    if (value.clock != .awake) return true;
    return value.raw.nanoseconds <= std.Io.Clock.Timestamp.now(io, .awake).raw.nanoseconds;
}

fn boundedLeaderWaitDeadline(
    now: std.Io.Clock.Timestamp,
    max_wait_us: u64,
    caller_deadline: ?std.Io.Clock.Timestamp,
) std.Io.Clock.Timestamp {
    std.debug.assert(now.clock == .awake);
    const wait_duration = std.Io.Clock.Duration{
        .raw = std.Io.Duration.fromMicroseconds(@intCast(@min(
            max_wait_us,
            @as(u64, std.math.maxInt(i64)),
        ))),
        .clock = .awake,
    };
    const max_wait_deadline = now.addDuration(wait_duration);
    const request_deadline = caller_deadline orelse return max_wait_deadline;
    // submit's pre-enqueue check rejects foreign clocks. Keep this helper
    // fail-closed as well if it is reused independently.
    if (request_deadline.clock != .awake) return now;
    return if (request_deadline.raw.nanoseconds < max_wait_deadline.raw.nanoseconds)
        request_deadline
    else
        max_wait_deadline;
}

fn resultError(comptime T: type, identity: Identity, err: anyerror) ItemResult(T) {
    return .{
        .identity = identity,
        .execution = .serial,
        .result = .{ .item_error = .{ .cause = err } },
    };
}

fn takeResult(comptime T: type, identity: Identity, output: *const T, slot: ResultSlot) ItemResult(T) {
    return .{
        .identity = identity,
        .execution = slot.execution,
        .execution_id = slot.execution_id,
        .result = if (slot.err) |err|
            .{ .item_error = .{ .cause = err } }
        else
            .{ .value = output.* },
    };
}

const TestExecutor = struct {
    calls: std.atomic.Value(usize) = .init(0),
    largest_batch: std.atomic.Value(usize) = .init(0),

    fn run(raw: *anyopaque, items: []const ExecuteItem) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        _ = self.calls.fetchAdd(1, .monotonic);
        var largest = self.largest_batch.load(.monotonic);
        while (items.len > largest) {
            largest = self.largest_batch.cmpxchgWeak(largest, items.len, .monotonic, .monotonic) orelse break;
        }
        for (items) |item| {
            const input = item.payloadAs(usize).*;
            item.slot.setValue(usize, input * 2, if (items.len > 1) .native_batch else .serial);
        }
    }
};

test "microbatch grouping key is exact across task conditioning and resource class" {
    const base = Key{
        .model = "owner/model",
        .task = .extract,
        .prompt = "prompt",
        .schema = "schema",
        .transform = "render-v1",
        .option_key = 7,
        .resource_class = .gpu,
    };
    try std.testing.expect(base.eql(base));
    inline for (.{
        Key{ .model = "owner/model", .generation = 2, .task = .extract, .prompt = "prompt", .schema = "schema", .transform = "render-v1", .option_key = 7, .resource_class = .gpu },
        Key{ .model = "other/model", .task = .extract, .prompt = "prompt", .schema = "schema", .transform = "render-v1", .option_key = 7, .resource_class = .gpu },
        Key{ .model = "owner/model", .task = .read, .prompt = "prompt", .schema = "schema", .transform = "render-v1", .option_key = 7, .resource_class = .gpu },
        Key{ .model = "owner/model", .task = .extract, .prompt = "other", .schema = "schema", .transform = "render-v1", .option_key = 7, .resource_class = .gpu },
        Key{ .model = "owner/model", .task = .extract, .prompt = "prompt", .schema = "other", .transform = "render-v1", .option_key = 7, .resource_class = .gpu },
        Key{ .model = "owner/model", .task = .extract, .prompt = "prompt", .schema = "schema", .transform = "other", .option_key = 7, .resource_class = .gpu },
        Key{ .model = "owner/model", .task = .extract, .prompt = "prompt", .schema = "schema", .transform = "render-v1", .option_key = 8, .resource_class = .gpu },
        Key{ .model = "owner/model", .task = .extract, .prompt = "prompt", .schema = "schema", .transform = "render-v1", .option_key = 7, .resource_class = .cpu },
    }) |different| try std.testing.expect(!base.eql(different));
}

test "microbatch broker groups native work and preserves provenance" {
    const allocator = std.testing.allocator;
    var broker = Broker.init(allocator);
    defer broker.deinit();
    var executor = TestExecutor{};
    const limits = Limits{
        .mode = .native,
        .preferred_items = 2,
        .max_items = 4,
        .max_bytes = 10,
        .max_pixels = 10,
        .max_tokens = 10,
        .max_wait_us = 500_000,
    };
    const key = Key{ .model = "reader", .task = .read, .prompt = "ocr", .resource_class = .gpu };
    const Submit = struct {
        broker: *Broker,
        executor: *TestExecutor,
        key: Key,
        limits: Limits,
        input: usize,
        identity: Identity,
        result: ?ItemResult(usize) = null,
        err: ?anyerror = null,

        fn run(self: *@This()) std.Io.Cancelable!void {
            self.result = self.broker.submit(
                usize,
                usize,
                std.testing.io,
                std.testing.allocator,
                self.key,
                self.limits,
                .{ .bytes = 2, .pixels = 2, .tokens = 2 },
                self.identity,
                null,
                null,
                &self.input,
                self.executor,
                TestExecutor.run,
            ) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var first = Submit{
        .broker = &broker,
        .executor = &executor,
        .key = key,
        .limits = limits,
        .input = 3,
        .identity = .{ .item_id = "page-1", .source_fingerprint = "doc-a", .page_number = 1 },
    };
    var second = Submit{
        .broker = &broker,
        .executor = &executor,
        .key = key,
        .limits = limits,
        .input = 4,
        .identity = .{ .item_id = "page-2", .source_fingerprint = "doc-b", .page_number = 2 },
    };
    var group = std.Io.Group.init;
    try group.concurrent(std.testing.io, Submit.run, .{&first});
    try group.concurrent(std.testing.io, Submit.run, .{&second});
    try group.await(std.testing.io);

    try std.testing.expect(first.err == null);
    try std.testing.expect(second.err == null);
    try std.testing.expectEqual(@as(usize, 1), executor.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 2), executor.largest_batch.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 6), first.result.?.result.value);
    try std.testing.expectEqual(@as(usize, 8), second.result.?.result.value);
    try std.testing.expectEqualStrings("doc-a", first.result.?.identity.source_fingerprint.?);
    try std.testing.expectEqual(@as(?u32, 2), second.result.?.identity.page_number);
    try std.testing.expectEqual(Execution.native_batch, first.result.?.execution);
    const stats = broker.snapshot(std.testing.io);
    try std.testing.expectEqual(@as(u64, 1), stats.native_batches);
    try std.testing.expectEqual(@as(u64, 2), stats.native_items);
}

test "microbatch broker flattens an existing request batch" {
    const allocator = std.testing.allocator;
    var broker = Broker.init(allocator);
    defer broker.deinit();
    var executor = TestExecutor{};
    const inputs = [_]usize{ 3, 5, 7 };
    const shapes = [_]Shape{
        .{ .bytes = 1 },
        .{ .bytes = 1 },
        .{ .bytes = 1 },
    };
    const identities = [_]Identity{
        .{ .item_id = "page-1", .page_number = 1 },
        .{ .item_id = "page-2", .page_number = 2 },
        .{ .item_id = "page-3", .page_number = 3 },
    };
    const results = try broker.submitBatchControlled(
        usize,
        usize,
        std.testing.io,
        allocator,
        .{ .model = "reader", .task = .read, .prompt = "ocr", .resource_class = .gpu },
        .{
            .mode = .native,
            .preferred_items = 3,
            .max_items = 4,
            .max_bytes = 4,
            .max_wait_us = 500_000,
        },
        &shapes,
        &identities,
        null,
        .{},
        &inputs,
        &executor,
        TestExecutor.run,
    );
    defer allocator.free(results);

    try std.testing.expectEqual(@as(usize, 1), executor.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 3), executor.largest_batch.load(.monotonic));
    for (results, inputs, identities) |result, input, identity| {
        try std.testing.expectEqualStrings(identity.item_id, result.identity.item_id);
        try std.testing.expectEqual(Execution.native_batch, result.execution);
        try std.testing.expect(result.execution_id != 0);
        try std.testing.expectEqual(results[0].execution_id, result.execution_id);
        try std.testing.expectEqual(input * 2, result.result.value);
    }
}

test "microbatch array preserves serial compatibility execution" {
    const allocator = std.testing.allocator;
    var broker = Broker.init(allocator);
    defer broker.deinit();
    var executor = TestExecutor{};
    const inputs = [_]usize{ 2, 4, 6 };
    const shapes = [_]Shape{ .{}, .{}, .{} };
    const identities = [_]Identity{
        .{ .item_id = "item-1" },
        .{ .item_id = "item-2" },
        .{ .item_id = "item-3" },
    };
    const results = try broker.submitBatchControlled(
        usize,
        usize,
        std.testing.io,
        allocator,
        .{ .model = "compat", .task = .extract, .resource_class = .cpu },
        .{ .mode = .serial_compatibility, .preferred_items = 1, .max_items = 1 },
        &shapes,
        &identities,
        null,
        .{},
        &inputs,
        &executor,
        TestExecutor.run,
    );
    defer allocator.free(results);

    try std.testing.expectEqual(@as(usize, inputs.len), executor.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), executor.largest_batch.load(.monotonic));
    for (results) |result| {
        try std.testing.expectEqual(Execution.serial, result.execution);
        try std.testing.expectEqual(@as(u64, 0), result.execution_id);
    }
}

test "microbatch follower deadline shortens window without forcing an immediate flush" {
    var broker = Broker.init(std.testing.allocator);
    defer broker.deinit();
    var executor = TestExecutor{};
    const limits = Limits{
        .mode = .native,
        .preferred_items = 3,
        .max_items = 3,
        .max_wait_us = 500_000,
    };
    const Submit = struct {
        broker: *Broker,
        executor: *TestExecutor,
        limits: Limits,
        input: usize,
        deadline: ?std.Io.Clock.Timestamp = null,
        result: ?ItemResult(usize) = null,
        err: ?anyerror = null,

        fn run(self: *@This()) std.Io.Cancelable!void {
            self.result = self.broker.submit(
                usize,
                usize,
                std.testing.io,
                std.testing.allocator,
                .{ .model = "reader", .task = .read, .resource_class = .gpu },
                self.limits,
                .{},
                .{},
                self.deadline,
                null,
                &self.input,
                self.executor,
                TestExecutor.run,
            ) catch |err| {
                self.err = err;
                return;
            };
        }
    };

    const now = std.Io.Clock.Timestamp.now(std.testing.io, .awake);
    const follower_deadline = now.addDuration(.{
        .raw = std.Io.Duration.fromMilliseconds(100),
        .clock = .awake,
    });
    var first = Submit{ .broker = &broker, .executor = &executor, .limits = limits, .input = 1 };
    var second = Submit{ .broker = &broker, .executor = &executor, .limits = limits, .input = 2, .deadline = follower_deadline };
    var third = Submit{ .broker = &broker, .executor = &executor, .limits = limits, .input = 3 };
    var first_future = try std.testing.io.concurrent(Submit.run, .{&first});
    try std.testing.io.sleep(std.Io.Duration.fromMilliseconds(2), .awake);
    var second_future = try std.testing.io.concurrent(Submit.run, .{&second});
    try std.testing.io.sleep(std.Io.Duration.fromMilliseconds(2), .awake);
    try std.testing.expectEqual(@as(usize, 0), executor.calls.load(.monotonic));
    var third_future = try std.testing.io.concurrent(Submit.run, .{&third});
    try first_future.await(std.testing.io);
    try second_future.await(std.testing.io);
    try third_future.await(std.testing.io);

    try std.testing.expect(first.err == null);
    try std.testing.expect(second.err == null);
    try std.testing.expect(third.err == null);
    try std.testing.expectEqual(@as(usize, 1), executor.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 3), executor.largest_batch.load(.monotonic));
}

test "microbatch broker never coalesces republished model generations" {
    var broker = Broker.init(std.testing.allocator);
    defer broker.deinit();
    var executor = TestExecutor{};
    const limits = Limits{
        .mode = .native,
        .preferred_items = 2,
        .max_items = 2,
        .max_wait_us = 1_000,
    };
    const Submit = struct {
        broker: *Broker,
        executor: *TestExecutor,
        generation: usize,
        input: usize,
        result: ?ItemResult(usize) = null,
        err: ?anyerror = null,

        fn run(self: *@This()) std.Io.Cancelable!void {
            self.result = self.broker.submit(
                usize,
                usize,
                std.testing.io,
                std.testing.allocator,
                .{
                    .model = "readers/owner/model",
                    .generation = self.generation,
                    .task = .read,
                    .resource_class = .cpu,
                },
                limits,
                .{},
                .{},
                null,
                null,
                &self.input,
                self.executor,
                TestExecutor.run,
            ) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    var old = Submit{ .broker = &broker, .executor = &executor, .generation = 41, .input = 3 };
    var current = Submit{ .broker = &broker, .executor = &executor, .generation = 42, .input = 4 };
    var group = std.Io.Group.init;
    try group.concurrent(std.testing.io, Submit.run, .{&old});
    try group.concurrent(std.testing.io, Submit.run, .{&current});
    try group.await(std.testing.io);

    try std.testing.expect(old.err == null);
    try std.testing.expect(current.err == null);
    try std.testing.expectEqual(@as(usize, 2), executor.calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 1), executor.largest_batch.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 6), old.result.?.result.value);
    try std.testing.expectEqual(@as(usize, 8), current.result.?.result.value);
}

test "microbatch cancellation after execution starts returns owned output" {
    const allocator = std.testing.allocator;
    var broker = Broker.init(allocator);
    defer broker.deinit();
    const OwnedExecutor = struct {
        started: std.Io.Event = .unset,
        release: std.atomic.Value(bool) = .init(false),

        fn run(raw: *anyopaque, items: []const ExecuteItem) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.started.set(std.testing.io);
            while (!self.release.load(.acquire)) std.Thread.yield() catch {};
            for (items) |item| {
                const value = item.allocator.dupe(u8, "owned") catch |err| {
                    item.slot.fail(err);
                    continue;
                };
                item.slot.setValue([]u8, value, .native_batch);
            }
        }

        fn releaseLater(self: *@This(), io: std.Io) !void {
            try io.sleep(std.Io.Duration.fromMilliseconds(5), .awake);
            self.release.store(true, .release);
        }
    };
    const Submit = struct {
        broker: *Broker,
        executor: *OwnedExecutor,
        payload: usize,
        result: ?ItemResult([]u8) = null,
        err: ?anyerror = null,

        fn run(self: *@This(), io: std.Io) std.Io.Cancelable!void {
            self.result = self.broker.submit(
                usize,
                []u8,
                io,
                std.testing.allocator,
                .{ .model = "owned-reader", .task = .read, .resource_class = .gpu },
                .{ .mode = .native, .preferred_items = 2, .max_items = 2, .max_wait_us = 500_000 },
                .{},
                .{},
                null,
                null,
                &self.payload,
                self.executor,
                OwnedExecutor.run,
            ) catch |err| {
                self.err = err;
                return;
            };
        }

        fn deinitResult(self: *@This()) void {
            const result = self.result orelse return;
            switch (result.result) {
                .value => |value| std.testing.allocator.free(value),
                .item_error => {},
            }
        }
    };

    var executor = OwnedExecutor{};
    var first = Submit{ .broker = &broker, .executor = &executor, .payload = 1 };
    defer first.deinitResult();
    var second = Submit{ .broker = &broker, .executor = &executor, .payload = 2 };
    defer second.deinitResult();
    const io = std.testing.io;
    var first_future = try io.concurrent(Submit.run, .{ &first, io });
    try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    var second_future = try io.concurrent(Submit.run, .{ &second, io });
    try executor.started.wait(io);
    var releaser = try io.concurrent(OwnedExecutor.releaseLater, .{ &executor, io });
    try second_future.cancel(io);
    try first_future.await(io);
    try releaser.await(io);

    try std.testing.expect(first.err == null);
    try std.testing.expect(second.err == null);
    try std.testing.expectEqualStrings("owned", second.result.?.result.value);
    try std.testing.expectEqual(Execution.native_batch, second.result.?.execution);
}

test "microbatch broker enforces grouping resource caps and max wait flush" {
    const allocator = std.testing.allocator;
    var broker = Broker.init(allocator);
    defer broker.deinit();
    var executor = TestExecutor{};
    const limits = Limits{
        .mode = .native,
        .preferred_items = 3,
        .max_items = 3,
        .max_bytes = 3,
        .max_pixels = 3,
        .max_tokens = 3,
        .max_wait_us = 1,
    };
    const payload: usize = 5;
    const result = try broker.submit(
        usize,
        usize,
        std.testing.io,
        allocator,
        .{ .model = "embedder", .task = .embed, .prompt = "a", .resource_class = .cpu },
        limits,
        .{ .bytes = 3, .pixels = 3, .tokens = 3 },
        .{ .item_id = "chunk-1" },
        null,
        null,
        &payload,
        &executor,
        TestExecutor.run,
    );
    try std.testing.expectEqual(@as(usize, 10), result.result.value);
    try std.testing.expectEqual(@as(usize, 1), executor.largest_batch.load(.monotonic));

    const rejected = try broker.submit(
        usize,
        usize,
        std.testing.io,
        allocator,
        .{ .model = "embedder", .task = .embed, .prompt = "b", .resource_class = .cpu },
        limits,
        .{ .bytes = 4 },
        .{},
        null,
        null,
        &payload,
        &executor,
        TestExecutor.run,
    );
    try std.testing.expectEqual(error.MicrobatchResourceLimitExceeded, rejected.result.item_error.cause);
}

test "microbatch leader wait is bounded by max wait before a long caller deadline" {
    const now = std.Io.Timestamp.fromNanoseconds(1_000_000_000).withClock(.awake);
    const long_deadline = std.Io.Timestamp.fromNanoseconds(11_000_000_000).withClock(.awake);
    const short_deadline = std.Io.Timestamp.fromNanoseconds(1_001_000_000).withClock(.awake);

    const bounded = boundedLeaderWaitDeadline(now, 2_000, long_deadline);
    try std.testing.expectEqual(@as(i96, 1_002_000_000), bounded.raw.nanoseconds);
    const caller_bounded = boundedLeaderWaitDeadline(now, 2_000, short_deadline);
    try std.testing.expectEqual(short_deadline.raw.nanoseconds, caller_bounded.raw.nanoseconds);
}

test "microbatch broker bypasses nonnative work and honors cancellation deadline" {
    const allocator = std.testing.allocator;
    var broker = Broker.init(allocator);
    defer broker.deinit();
    var executor = TestExecutor{};
    const payload: usize = 7;
    inline for (std.meta.tags(Task)) |task| {
        const result = try broker.submit(
            usize,
            usize,
            std.testing.io,
            allocator,
            .{ .model = "model", .task = task, .resource_class = .remote },
            .{ .mode = .serial_compatibility, .preferred_items = 1, .max_items = 1 },
            .{},
            .{},
            null,
            null,
            &payload,
            &executor,
            TestExecutor.run,
        );
        try std.testing.expectEqual(@as(usize, 14), result.result.value);
        try std.testing.expectEqual(Execution.serial, result.execution);
    }

    var canceled = std.atomic.Value(bool).init(true);
    const canceled_result = try broker.submit(
        usize,
        usize,
        std.testing.io,
        allocator,
        .{ .model = "model", .task = .read, .resource_class = .cpu },
        .{ .mode = .native, .preferred_items = 2, .max_items = 2 },
        .{},
        .{ .item_id = "canceled" },
        null,
        &canceled,
        &payload,
        &executor,
        TestExecutor.run,
    );
    try std.testing.expectEqual(error.Canceled, canceled_result.result.item_error.cause);

    const expired = std.Io.Timestamp.zero.withClock(.awake);
    const expired_result = try broker.submit(
        usize,
        usize,
        std.testing.io,
        allocator,
        .{ .model = "model", .task = .read, .resource_class = .cpu },
        .{ .mode = .native, .preferred_items = 2, .max_items = 2 },
        .{},
        .{ .item_id = "expired" },
        expired,
        null,
        &payload,
        &executor,
        TestExecutor.run,
    );
    try std.testing.expectEqual(error.DeadlineExceeded, expired_result.result.item_error.cause);

    const foreign_clock = std.Io.Timestamp.fromNanoseconds(std.math.maxInt(i96)).withClock(.real);
    const foreign_clock_result = try broker.submit(
        usize,
        usize,
        std.testing.io,
        allocator,
        .{ .model = "model", .task = .read, .resource_class = .cpu },
        .{ .mode = .native, .preferred_items = 2, .max_items = 2 },
        .{},
        .{ .item_id = "wrong-clock" },
        foreign_clock,
        null,
        &payload,
        &executor,
        TestExecutor.run,
    );
    try std.testing.expectEqual(error.DeadlineExceeded, foreign_clock_result.result.item_error.cause);
}
