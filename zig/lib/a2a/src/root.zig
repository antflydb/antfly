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

const std = @import("std");

pub const protocol_version = "0.3.0";

pub const Skill = struct {
    id: []const u8,
    name: []const u8,
    description: []const u8,
    tags: []const []const u8 = &.{},
};

pub const RequestContext = struct {
    task_id: []const u8,
    context_id: []const u8,
    message: std.json.Value,
    metadata: ?std.json.Value = null,
};

pub const StreamSink = struct {
    ptr: *anyopaque,
    emit_fn: *const fn (*anyopaque, std.mem.Allocator, std.json.Value) anyerror!void,

    pub fn emit(self: StreamSink, alloc: std.mem.Allocator, event: std.json.Value) !void {
        try self.emit_fn(self.ptr, alloc, event);
    }
};

pub const EventQueue = struct {
    events: std.json.Array,
    sink: ?StreamSink = null,
    sink_alloc: ?std.mem.Allocator = null,

    pub fn init(alloc: std.mem.Allocator) EventQueue {
        return .{ .events = std.json.Array.init(alloc) };
    }

    pub fn initWithSink(alloc: std.mem.Allocator, sink: StreamSink, sink_alloc: std.mem.Allocator) EventQueue {
        return .{ .events = std.json.Array.init(alloc), .sink = sink, .sink_alloc = sink_alloc };
    }

    pub fn deinit(self: *EventQueue, alloc: std.mem.Allocator) void {
        _ = alloc;
        self.events.deinit();
    }

    pub fn status(self: *EventQueue, alloc: std.mem.Allocator, task_id: []const u8, context_id: []const u8, state: []const u8, message_text: ?[]const u8) !void {
        var status_obj = std.json.ObjectMap.empty;
        try status_obj.put(alloc, "state", .{ .string = state });
        if (message_text) |text| {
            try status_obj.put(alloc, "message", try textMessage(alloc, context_id, text));
        }

        var result = std.json.ObjectMap.empty;
        try result.put(alloc, "taskId", .{ .string = task_id });
        try result.put(alloc, "contextId", .{ .string = context_id });
        try result.put(alloc, "kind", .{ .string = "status-update" });
        try result.put(alloc, "status", .{ .object = status_obj });
        const event: std.json.Value = .{ .object = result };
        try self.events.append(event);
        if (self.sink) |sink| try sink.emit(self.sink_alloc orelse alloc, event);
    }

    pub fn artifact(self: *EventQueue, alloc: std.mem.Allocator, task_id: []const u8, context_id: []const u8, name: []const u8, parts: std.json.Value) !void {
        var artifact_obj = std.json.ObjectMap.empty;
        try artifact_obj.put(alloc, "name", .{ .string = name });
        try artifact_obj.put(alloc, "parts", parts);

        var result = std.json.ObjectMap.empty;
        try result.put(alloc, "taskId", .{ .string = task_id });
        try result.put(alloc, "contextId", .{ .string = context_id });
        try result.put(alloc, "kind", .{ .string = "artifact-update" });
        try result.put(alloc, "artifact", .{ .object = artifact_obj });
        const event: std.json.Value = .{ .object = result };
        try self.events.append(event);
        if (self.sink) |sink| try sink.emit(self.sink_alloc orelse alloc, event);
    }
};

pub const AgentHandler = struct {
    ptr: *anyopaque,
    skill_id_fn: *const fn (*anyopaque) []const u8,
    skill_fn: *const fn (*anyopaque, std.mem.Allocator) anyerror!Skill,
    execute_fn: *const fn (*anyopaque, std.mem.Allocator, RequestContext, *EventQueue) anyerror!void,
    cancel_fn: ?*const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror!void = null,

    pub fn skillId(self: AgentHandler) []const u8 {
        return self.skill_id_fn(self.ptr);
    }

    pub fn skill(self: AgentHandler, alloc: std.mem.Allocator) !Skill {
        return try self.skill_fn(self.ptr, alloc);
    }

    pub fn execute(self: AgentHandler, alloc: std.mem.Allocator, ctx: RequestContext, queue: *EventQueue) !void {
        try self.execute_fn(self.ptr, alloc, ctx, queue);
    }

    pub fn cancel(self: AgentHandler, alloc: std.mem.Allocator, task_id: []const u8) !void {
        if (self.cancel_fn) |cancel_fn| try cancel_fn(self.ptr, alloc, task_id);
    }
};

pub const TaskStore = struct {
    ptr: *anyopaque,
    reserve_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, std.json.Value) anyerror!u64,
    save_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, u64, std.json.Value) anyerror!void,
    release_fn: *const fn (*anyopaque, []const u8, []const u8, u64) void,
    get_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror!std.json.Value,
    cancel_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror!std.json.Value,

    pub fn reserve(
        self: TaskStore,
        alloc: std.mem.Allocator,
        authority: []const u8,
        task_id: []const u8,
        task: std.json.Value,
    ) !u64 {
        return try self.reserve_fn(self.ptr, alloc, authority, task_id, task);
    }

    pub fn save(
        self: TaskStore,
        alloc: std.mem.Allocator,
        authority: []const u8,
        task_id: []const u8,
        expected_generation: u64,
        task: std.json.Value,
    ) !void {
        try self.save_fn(self.ptr, alloc, authority, task_id, expected_generation, task);
    }

    pub fn release(self: TaskStore, authority: []const u8, task_id: []const u8, expected_generation: u64) void {
        self.release_fn(self.ptr, authority, task_id, expected_generation);
    }

    pub fn get(self: TaskStore, alloc: std.mem.Allocator, authority: []const u8, task_id: []const u8) !std.json.Value {
        return try self.get_fn(self.ptr, alloc, authority, task_id);
    }

    pub fn cancel(self: TaskStore, alloc: std.mem.Allocator, authority: []const u8, task_id: []const u8) !std.json.Value {
        return try self.cancel_fn(self.ptr, alloc, authority, task_id);
    }
};

pub const InMemoryTaskStore = struct {
    pub const Options = struct {
        max_tasks: usize = 4096,
        max_tasks_per_authority: usize = 1024,
        max_authority_bytes: usize = 1024,
        max_task_id_bytes: usize = 512,
        max_task_bytes: usize = 1024 * 1024,
        max_bytes_per_authority: usize = 16 * 1024 * 1024,
        max_total_bytes: usize = 64 * 1024 * 1024,
        retention_ttl_ns: u64 = 24 * std.time.ns_per_hour,
        cleanup_interval_ns: u64 = std.time.ns_per_min,
        now_ns_fn: ?*const fn () u64 = null,
    };

    const TaskState = struct {
        body: []u8,
        updated_at_ns: u64,
        generation: u64,
    };

    const AuthorityState = struct {
        tasks: std.StringHashMapUnmanaged(TaskState) = .empty,
        total_bytes: usize = 0,

        fn deinit(self: *AuthorityState, alloc: std.mem.Allocator) void {
            var it = self.tasks.iterator();
            while (it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                alloc.free(entry.value_ptr.body);
            }
            self.tasks.deinit(alloc);
            self.* = undefined;
        }
    };

    const max_cancel_retries = 8;

    alloc: ?std.mem.Allocator = null,
    io: ?std.Io = null,
    options: Options = .{},
    mutex: std.Io.Mutex = .init,
    authorities: std.StringHashMapUnmanaged(AuthorityState) = .empty,
    task_count: usize = 0,
    total_bytes: usize = 0,
    next_generation: u64 = 1,
    next_cleanup_ns: u64 = 0,

    pub fn init(alloc: std.mem.Allocator) InMemoryTaskStore {
        return .{
            .alloc = alloc,
            .io = std.Io.Threaded.global_single_threaded.io(),
        };
    }

    pub fn initWithOptions(alloc: std.mem.Allocator, io: std.Io, options: Options) InMemoryTaskStore {
        return .{ .alloc = alloc, .io = io, .options = options };
    }

    pub fn deinit(self: *InMemoryTaskStore, fallback_alloc: std.mem.Allocator) void {
        const alloc = self.alloc orelse fallback_alloc;
        const io = self.io orelse std.Io.Threaded.global_single_threaded.io();
        self.mutex.lockUncancelable(io);
        var it = self.authorities.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            entry.value_ptr.deinit(alloc);
        }
        self.authorities.deinit(alloc);
        self.mutex.unlock(io);
        self.* = undefined;
    }

    pub fn iface(self: *InMemoryTaskStore) TaskStore {
        return .{
            .ptr = self,
            .reserve_fn = reserve,
            .save_fn = save,
            .release_fn = release,
            .get_fn = get,
            .cancel_fn = cancel,
        };
    }

    fn reserve(
        ptr: *anyopaque,
        request_alloc: std.mem.Allocator,
        authority: []const u8,
        task_id: []const u8,
        task: std.json.Value,
    ) !u64 {
        const self: *InMemoryTaskStore = @ptrCast(@alignCast(ptr));
        const store_alloc = self.alloc orelse request_alloc;
        const io = self.io orelse return error.MissingIo;
        try self.validateIdentity(authority, task_id);

        var body: ?[]u8 = try stringifyValue(store_alloc, task);
        defer if (body) |owned| store_alloc.free(owned);
        if (body.?.len > self.options.max_task_bytes) return error.A2aTaskTooLarge;

        var owned_task_id: ?[]u8 = try store_alloc.dupe(u8, task_id);
        defer if (owned_task_id) |owned| store_alloc.free(owned);

        const now_ns = self.nowNs();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.cleanupExpiredLocked(store_alloc, now_ns, false);
        if (self.authorities.get(authority)) |state| {
            if (state.tasks.get(task_id)) |entry| {
                if (!self.isExpired(entry, now_ns)) return error.A2aTaskAlreadyExists;
                _ = self.removeLocked(store_alloc, authority, task_id);
            }
        }

        const task_charge = owned_task_id.?.len + body.?.len;
        if (!self.reservationFitsLocked(authority, task_charge)) {
            self.cleanupExpiredLocked(store_alloc, now_ns, true);
        }
        if (!self.reservationFitsLocked(authority, task_charge))
            return error.A2aTaskCapacityExceeded;

        if (self.authorities.getPtr(authority)) |state| {
            const generation = try self.allocateGenerationLocked();
            try state.tasks.put(store_alloc, owned_task_id.?, .{
                .body = body.?,
                .updated_at_ns = now_ns,
                .generation = generation,
            });
            state.total_bytes += task_charge;
            self.task_count += 1;
            self.total_bytes += task_charge;
            owned_task_id = null;
            body = null;
            return generation;
        }

        var owned_authority: ?[]u8 = try store_alloc.dupe(u8, authority);
        defer if (owned_authority) |owned| store_alloc.free(owned);
        const generation = try self.allocateGenerationLocked();
        var state = AuthorityState{};
        errdefer state.deinit(store_alloc);
        try state.tasks.put(store_alloc, owned_task_id.?, .{
            .body = body.?,
            .updated_at_ns = now_ns,
            .generation = generation,
        });
        state.total_bytes = task_charge;
        owned_task_id = null;
        body = null;
        try self.authorities.put(store_alloc, owned_authority.?, state);
        const global_charge = owned_authority.?.len + task_charge;
        owned_authority = null;
        self.total_bytes += global_charge;
        self.task_count += 1;
        return generation;
    }

    fn save(
        ptr: *anyopaque,
        request_alloc: std.mem.Allocator,
        authority: []const u8,
        task_id: []const u8,
        expected_generation: u64,
        task: std.json.Value,
    ) !void {
        const self: *InMemoryTaskStore = @ptrCast(@alignCast(ptr));
        const store_alloc = self.alloc orelse request_alloc;
        const io = self.io orelse return error.MissingIo;
        try self.validateIdentity(authority, task_id);

        var body: ?[]u8 = try stringifyValue(store_alloc, task);
        defer if (body) |owned| store_alloc.free(owned);
        if (body.?.len > self.options.max_task_bytes) return error.A2aTaskTooLarge;

        const now_ns = self.nowNs();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.cleanupExpiredLocked(store_alloc, now_ns, false);
        var state = self.authorities.getPtr(authority) orelse return error.TaskNotFound;
        var entry = state.tasks.getPtr(task_id) orelse return error.TaskNotFound;
        if (self.isExpired(entry.*, now_ns)) {
            _ = self.removeLocked(store_alloc, authority, task_id);
            return error.TaskNotFound;
        }
        if (entry.generation != expected_generation) return error.A2aTaskStoreBusy;
        if (!self.replacementFitsLocked(state, entry.body.len, body.?.len)) {
            self.cleanupExpiredLocked(store_alloc, now_ns, true);
            state = self.authorities.getPtr(authority) orelse return error.TaskNotFound;
            entry = state.tasks.getPtr(task_id) orelse return error.TaskNotFound;
            if (self.isExpired(entry.*, now_ns)) {
                _ = self.removeLocked(store_alloc, authority, task_id);
                return error.TaskNotFound;
            }
            if (entry.generation != expected_generation) return error.A2aTaskStoreBusy;
        }
        if (!self.replacementFitsLocked(state, entry.body.len, body.?.len))
            return error.A2aTaskCapacityExceeded;

        const base_bytes = self.total_bytes - entry.body.len;
        const authority_base_bytes = state.total_bytes - entry.body.len;
        const next_generation = try self.allocateGenerationLocked();
        const committed_at_ns = self.nowNs();
        store_alloc.free(entry.body);
        self.total_bytes = base_bytes + body.?.len;
        state.total_bytes = authority_base_bytes + body.?.len;
        entry.* = .{
            .body = body.?,
            .updated_at_ns = committed_at_ns,
            .generation = next_generation,
        };
        body = null;
    }

    fn release(ptr: *anyopaque, authority: []const u8, task_id: []const u8, expected_generation: u64) void {
        const self: *InMemoryTaskStore = @ptrCast(@alignCast(ptr));
        const store_alloc = self.alloc orelse return;
        const io = self.io orelse std.Io.Threaded.global_single_threaded.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        const state = self.authorities.getPtr(authority) orelse return;
        const entry = state.tasks.get(task_id) orelse return;
        if (entry.generation != expected_generation) return;
        _ = self.removeLocked(store_alloc, authority, task_id);
    }

    fn get(ptr: *anyopaque, alloc: std.mem.Allocator, authority: []const u8, task_id: []const u8) !std.json.Value {
        const self: *InMemoryTaskStore = @ptrCast(@alignCast(ptr));
        const store_alloc = self.alloc orelse alloc;
        const io = self.io orelse return error.MissingIo;
        try self.validateIdentity(authority, task_id);
        const now_ns = self.nowNs();

        self.mutex.lockUncancelable(io);
        self.cleanupExpiredLocked(store_alloc, now_ns, false);
        const state = self.authorities.getPtr(authority) orelse {
            self.mutex.unlock(io);
            return error.TaskNotFound;
        };
        const entry = state.tasks.get(task_id) orelse {
            self.mutex.unlock(io);
            return error.TaskNotFound;
        };
        if (self.isExpired(entry, now_ns)) {
            _ = self.removeLocked(store_alloc, authority, task_id);
            self.mutex.unlock(io);
            return error.TaskNotFound;
        }
        const body = alloc.dupe(u8, entry.body) catch |err| {
            self.mutex.unlock(io);
            return err;
        };
        self.mutex.unlock(io);
        defer alloc.free(body);
        return try std.json.parseFromSliceLeaky(std.json.Value, alloc, body, .{
            .allocate = .alloc_always,
        });
    }

    fn cancel(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        authority: []const u8,
        task_id: []const u8,
    ) !std.json.Value {
        const self: *InMemoryTaskStore = @ptrCast(@alignCast(ptr));
        const store_alloc = self.alloc orelse alloc;
        const io = self.io orelse return error.MissingIo;
        try self.validateIdentity(authority, task_id);

        for (0..max_cancel_retries) |_| {
            const now_ns = self.nowNs();
            self.mutex.lockUncancelable(io);
            self.cleanupExpiredLocked(store_alloc, now_ns, false);
            const state = self.authorities.getPtr(authority) orelse {
                self.mutex.unlock(io);
                return error.TaskNotFound;
            };
            const current = state.tasks.get(task_id) orelse {
                self.mutex.unlock(io);
                return error.TaskNotFound;
            };
            if (self.isExpired(current, now_ns)) {
                _ = self.removeLocked(store_alloc, authority, task_id);
                self.mutex.unlock(io);
                return error.TaskNotFound;
            }
            const generation = current.generation;
            const snapshot = store_alloc.dupe(u8, current.body) catch |err| {
                self.mutex.unlock(io);
                return err;
            };
            self.mutex.unlock(io);
            defer store_alloc.free(snapshot);

            var arena_impl = std.heap.ArenaAllocator.init(alloc);
            defer arena_impl.deinit();
            const temp_alloc = arena_impl.allocator();
            var task = try std.json.parseFromSliceLeaky(std.json.Value, temp_alloc, snapshot, .{
                .allocate = .alloc_always,
            });
            if (task != .object) return error.TaskNotFound;
            var status = std.json.ObjectMap.empty;
            try status.put(temp_alloc, "state", .{ .string = "canceled" });
            try task.object.put(temp_alloc, "status", .{ .object = status });
            const candidate = try stringifyValue(store_alloc, task);
            if (candidate.len > self.options.max_task_bytes) {
                store_alloc.free(candidate);
                return error.A2aTaskTooLarge;
            }

            self.mutex.lockUncancelable(io);
            const commit_now_ns = self.nowNs();
            self.cleanupExpiredLocked(store_alloc, commit_now_ns, false);
            var current_state = self.authorities.getPtr(authority) orelse {
                self.mutex.unlock(io);
                store_alloc.free(candidate);
                return error.TaskNotFound;
            };
            var entry = current_state.tasks.getPtr(task_id) orelse {
                self.mutex.unlock(io);
                store_alloc.free(candidate);
                return error.TaskNotFound;
            };
            if (self.isExpired(entry.*, commit_now_ns)) {
                _ = self.removeLocked(store_alloc, authority, task_id);
                self.mutex.unlock(io);
                store_alloc.free(candidate);
                return error.TaskNotFound;
            }
            if (entry.generation != generation) {
                self.mutex.unlock(io);
                store_alloc.free(candidate);
                continue;
            }
            if (!self.replacementFitsLocked(current_state, entry.body.len, candidate.len)) {
                self.cleanupExpiredLocked(store_alloc, commit_now_ns, true);
                current_state = self.authorities.getPtr(authority) orelse {
                    self.mutex.unlock(io);
                    store_alloc.free(candidate);
                    return error.TaskNotFound;
                };
                entry = current_state.tasks.getPtr(task_id) orelse {
                    self.mutex.unlock(io);
                    store_alloc.free(candidate);
                    return error.TaskNotFound;
                };
                if (self.isExpired(entry.*, commit_now_ns)) {
                    _ = self.removeLocked(store_alloc, authority, task_id);
                    self.mutex.unlock(io);
                    store_alloc.free(candidate);
                    return error.TaskNotFound;
                }
                if (entry.generation != generation) {
                    self.mutex.unlock(io);
                    store_alloc.free(candidate);
                    continue;
                }
            }
            if (!self.replacementFitsLocked(current_state, entry.body.len, candidate.len)) {
                self.mutex.unlock(io);
                store_alloc.free(candidate);
                return error.A2aTaskCapacityExceeded;
            }
            const base_bytes = self.total_bytes - entry.body.len;
            const authority_base_bytes = current_state.total_bytes - entry.body.len;
            const response_body = alloc.dupe(u8, candidate) catch |err| {
                self.mutex.unlock(io);
                store_alloc.free(candidate);
                return err;
            };
            const next_generation = self.allocateGenerationLocked() catch |err| {
                self.mutex.unlock(io);
                alloc.free(response_body);
                store_alloc.free(candidate);
                return err;
            };
            const committed_at_ns = self.nowNs();
            store_alloc.free(entry.body);
            self.total_bytes = base_bytes + candidate.len;
            current_state.total_bytes = authority_base_bytes + candidate.len;
            entry.* = .{
                .body = candidate,
                .updated_at_ns = committed_at_ns,
                .generation = next_generation,
            };
            self.mutex.unlock(io);
            defer alloc.free(response_body);
            return try std.json.parseFromSliceLeaky(std.json.Value, alloc, response_body, .{
                .allocate = .alloc_always,
            });
        }
        return error.A2aTaskStoreBusy;
    }

    fn validateIdentity(
        self: *const InMemoryTaskStore,
        authority: []const u8,
        task_id: []const u8,
    ) !void {
        if (authority.len == 0 or authority.len > self.options.max_authority_bytes)
            return error.InvalidTaskAuthority;
        if (task_id.len == 0 or task_id.len > self.options.max_task_id_bytes)
            return error.InvalidTaskId;
    }

    fn nowNs(self: *const InMemoryTaskStore) u64 {
        if (self.options.now_ns_fn) |now_ns_fn| return now_ns_fn();
        const io = self.io orelse return 0;
        return @intCast(std.Io.Timestamp.now(io, .awake).toNanoseconds());
    }

    fn allocateGenerationLocked(self: *InMemoryTaskStore) !u64 {
        if (self.next_generation == std.math.maxInt(u64))
            return error.A2aTaskGenerationExhausted;
        const generation = self.next_generation;
        self.next_generation += 1;
        return generation;
    }

    fn reservationFitsLocked(
        self: *const InMemoryTaskStore,
        authority: []const u8,
        task_charge: usize,
    ) bool {
        if (self.task_count >= self.options.max_tasks) return false;

        if (self.authorities.get(authority)) |state| {
            if (state.tasks.count() >= self.options.max_tasks_per_authority)
                return false;
            if (task_charge > self.options.max_bytes_per_authority -| state.total_bytes)
                return false;
            return task_charge <= self.options.max_total_bytes -| self.total_bytes;
        }

        if (self.options.max_tasks_per_authority == 0) return false;
        if (task_charge > self.options.max_bytes_per_authority) return false;
        const global_remaining = self.options.max_total_bytes -| self.total_bytes;
        return authority.len <= global_remaining and
            task_charge <= global_remaining - authority.len;
    }

    fn replacementFitsLocked(
        self: *const InMemoryTaskStore,
        state: *const AuthorityState,
        old_body_len: usize,
        new_body_len: usize,
    ) bool {
        const global_base = self.total_bytes - old_body_len;
        const authority_base = state.total_bytes - old_body_len;
        return new_body_len <= self.options.max_total_bytes -| global_base and
            new_body_len <= self.options.max_bytes_per_authority -| authority_base;
    }

    fn cleanupExpiredLocked(self: *InMemoryTaskStore, alloc: std.mem.Allocator, now_ns: u64, force: bool) void {
        if (self.options.retention_ttl_ns == 0) return;
        if (!force and self.next_cleanup_ns != 0 and now_ns < self.next_cleanup_ns) return;
        self.next_cleanup_ns = now_ns +| @min(
            self.options.cleanup_interval_ns,
            self.options.retention_ttl_ns,
        );
        var authority_it = self.authorities.iterator();
        while (authority_it.next()) |authority_entry| {
            var task_it = authority_entry.value_ptr.tasks.iterator();
            while (task_it.next()) |task_entry| {
                if (!self.isExpired(task_entry.value_ptr.*, now_ns)) continue;
                const task_id = task_entry.key_ptr.*;
                const body = task_entry.value_ptr.body;
                authority_entry.value_ptr.tasks.removeByPtr(task_entry.key_ptr);
                authority_entry.value_ptr.total_bytes -= task_id.len + body.len;
                self.total_bytes -= task_id.len + body.len;
                self.task_count -= 1;
                alloc.free(@constCast(task_id));
                alloc.free(body);
            }
            if (authority_entry.value_ptr.tasks.count() != 0) continue;
            const authority = authority_entry.key_ptr.*;
            authority_entry.value_ptr.tasks.deinit(alloc);
            self.authorities.removeByPtr(authority_entry.key_ptr);
            self.total_bytes -= authority.len;
            alloc.free(@constCast(authority));
        }
    }

    fn isExpired(self: *const InMemoryTaskStore, state: TaskState, now_ns: u64) bool {
        return self.options.retention_ttl_ns != 0 and
            now_ns >= state.updated_at_ns and
            now_ns - state.updated_at_ns >= self.options.retention_ttl_ns;
    }

    fn removeLocked(
        self: *InMemoryTaskStore,
        alloc: std.mem.Allocator,
        authority: []const u8,
        task_id: []const u8,
    ) bool {
        const state = self.authorities.getPtr(authority) orelse return false;
        const removed = state.tasks.fetchRemove(task_id) orelse return false;
        state.total_bytes -= removed.key.len + removed.value.body.len;
        self.total_bytes -= removed.key.len + removed.value.body.len;
        self.task_count -= 1;
        alloc.free(removed.key);
        alloc.free(removed.value.body);
        if (state.tasks.count() == 0) {
            const removed_authority = self.authorities.fetchRemove(authority) orelse unreachable;
            var empty_state = removed_authority.value;
            empty_state.tasks.deinit(alloc);
            self.total_bytes -= removed_authority.key.len;
            alloc.free(removed_authority.key);
        }
        return true;
    }
};

pub const Dispatcher = struct {
    io: std.Io,
    name: []const u8 = "Antfly",
    version: []const u8 = "1.0.0",
    base_url: []const u8 = "",
    handlers: std.ArrayListUnmanaged(AgentHandler) = .empty,
    task_store: ?TaskStore = null,
    task_authority: []const u8 = "anonymous",

    pub fn deinit(self: *Dispatcher, alloc: std.mem.Allocator) void {
        self.handlers.deinit(alloc);
    }

    pub fn addHandler(self: *Dispatcher, alloc: std.mem.Allocator, handler: AgentHandler) !void {
        try self.handlers.append(alloc, handler);
    }

    pub fn handleJsonRpc(self: *Dispatcher, alloc: std.mem.Allocator, body: []const u8) ![]u8 {
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const temp_alloc = arena_impl.allocator();

        const request = std.json.parseFromSliceLeaky(std.json.Value, temp_alloc, body, .{}) catch {
            return try stringifyValue(alloc, try errorResponse(temp_alloc, .null, -32700, "parse error"));
        };
        if (request != .object) {
            return try stringifyValue(alloc, try errorResponse(temp_alloc, .null, -32600, "invalid request"));
        }

        const root = request.object;
        const id = root.get("id") orelse .null;
        const method = stringField(root, "method") orelse {
            return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32600, "invalid request"));
        };
        if (std.mem.eql(u8, method, "agent/getAuthenticatedExtendedCard")) {
            return try stringifyValue(alloc, try successResponse(temp_alloc, id, try self.agentCard(temp_alloc)));
        }
        if (std.mem.eql(u8, method, "message/send") or std.mem.eql(u8, method, "message/stream")) {
            const params = root.get("params") orelse .null;
            const result = self.executeMessage(temp_alloc, params, std.mem.eql(u8, method, "message/stream")) catch |err| switch (err) {
                error.InvalidParams => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "invalid params")),
                error.UnknownSkill => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "unknown skill")),
                error.InvalidTaskId, error.InvalidTaskAuthority => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "invalid task identity")),
                error.A2aTaskTooLarge => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32010, "task exceeds storage limit")),
                error.A2aTaskCapacityExceeded => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32011, "task storage capacity exceeded")),
                error.A2aTaskStoreBusy => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32012, "task changed concurrently; retry")),
                error.A2aTaskAlreadyExists => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32013, "task id already exists")),
                else => return err,
            };
            return try stringifyValue(alloc, try successResponse(temp_alloc, id, result));
        }
        if (std.mem.eql(u8, method, "tasks/get")) {
            const params = root.get("params") orelse .null;
            const task_id = taskIdParam(params) orelse {
                return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "invalid params"));
            };
            const store = self.task_store orelse return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32001, "task storage not configured"));
            const task = store.get(temp_alloc, self.task_authority, task_id) catch |err| switch (err) {
                error.TaskNotFound => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32004, "task not found")),
                error.InvalidTaskId, error.InvalidTaskAuthority => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "invalid task identity")),
                else => return err,
            };
            return try stringifyValue(alloc, try successResponse(temp_alloc, id, task));
        }
        if (std.mem.eql(u8, method, "tasks/cancel")) {
            const params = root.get("params") orelse .null;
            const task_id = taskIdParam(params) orelse {
                return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "invalid params"));
            };
            const store = self.task_store orelse return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32001, "task storage not configured"));
            const task = store.cancel(temp_alloc, self.task_authority, task_id) catch |err| switch (err) {
                error.TaskNotFound => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32004, "task not found")),
                error.InvalidTaskId, error.InvalidTaskAuthority => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32602, "invalid task identity")),
                error.A2aTaskTooLarge => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32010, "task exceeds storage limit")),
                error.A2aTaskCapacityExceeded => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32011, "task storage capacity exceeded")),
                error.A2aTaskStoreBusy => return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32012, "task changed concurrently; retry")),
                else => return err,
            };
            return try stringifyValue(alloc, try successResponse(temp_alloc, id, task));
        }
        return try stringifyValue(alloc, try errorResponse(temp_alloc, id, -32601, "method not found"));
    }

    pub fn handleJsonRpcStream(self: *Dispatcher, alloc: std.mem.Allocator, body: []const u8, sink: StreamSink) !void {
        var arena_impl = std.heap.ArenaAllocator.init(alloc);
        defer arena_impl.deinit();
        const temp_alloc = arena_impl.allocator();

        const request = std.json.parseFromSliceLeaky(std.json.Value, temp_alloc, body, .{}) catch {
            try sink.emit(alloc, try errorResponse(temp_alloc, .null, -32700, "parse error"));
            return;
        };
        if (request != .object) {
            try sink.emit(alloc, try errorResponse(temp_alloc, .null, -32600, "invalid request"));
            return;
        }

        const root = request.object;
        const id = root.get("id") orelse .null;
        const method = stringField(root, "method") orelse {
            try sink.emit(alloc, try errorResponse(temp_alloc, id, -32600, "invalid request"));
            return;
        };
        if (!std.mem.eql(u8, method, "message/stream")) {
            try sink.emit(alloc, try errorResponse(temp_alloc, id, -32601, "method not found"));
            return;
        }

        const params = root.get("params") orelse .null;
        self.executeMessageStream(temp_alloc, params, sink, alloc) catch |err| switch (err) {
            error.InvalidParams => try sink.emit(alloc, try errorResponse(temp_alloc, id, -32602, "invalid params")),
            error.UnknownSkill => try sink.emit(alloc, try errorResponse(temp_alloc, id, -32602, "unknown skill")),
            error.InvalidTaskId, error.InvalidTaskAuthority => try sink.emit(alloc, try errorResponse(temp_alloc, id, -32602, "invalid task identity")),
            error.A2aTaskTooLarge => try sink.emit(alloc, try errorResponse(temp_alloc, id, -32010, "task exceeds storage limit")),
            error.A2aTaskCapacityExceeded => try sink.emit(alloc, try errorResponse(temp_alloc, id, -32011, "task storage capacity exceeded")),
            error.A2aTaskStoreBusy => try sink.emit(alloc, try errorResponse(temp_alloc, id, -32012, "task changed concurrently; retry")),
            error.A2aTaskAlreadyExists => try sink.emit(alloc, try errorResponse(temp_alloc, id, -32013, "task id already exists")),
            else => return err,
        };
    }

    pub fn agentCard(self: *Dispatcher, alloc: std.mem.Allocator) !std.json.Value {
        var skills = std.json.Array.init(alloc);
        for (self.handlers.items) |handler| {
            const skill_info = try handler.skill(alloc);
            try skills.append(try skillValue(alloc, skill_info));
        }

        var capabilities = std.json.ObjectMap.empty;
        try capabilities.put(alloc, "streaming", .{ .bool = true });
        try capabilities.put(alloc, "stateTransitionHistory", .{ .bool = true });

        var modes = std.json.Array.init(alloc);
        try modes.append(.{ .string = "text" });
        try modes.append(.{ .string = "data" });

        var card = std.json.ObjectMap.empty;
        try card.put(alloc, "name", .{ .string = self.name });
        try card.put(alloc, "url", .{ .string = self.base_url });
        try card.put(alloc, "version", .{ .string = self.version });
        try card.put(alloc, "protocolVersion", .{ .string = protocol_version });
        try card.put(alloc, "preferredTransport", .{ .string = "JSONRPC" });
        try card.put(alloc, "capabilities", .{ .object = capabilities });
        try card.put(alloc, "defaultInputModes", .{ .array = modes });
        try card.put(alloc, "defaultOutputModes", try textDataModes(alloc));
        try card.put(alloc, "skills", .{ .array = skills });
        return .{ .object = card };
    }

    fn executeMessage(self: *Dispatcher, alloc: std.mem.Allocator, params: std.json.Value, stream: bool) !std.json.Value {
        if (params != .object) return error.InvalidParams;
        const message = params.object.get("message") orelse return error.InvalidParams;
        const metadata = params.object.get("metadata");
        const handler = self.resolve(message, metadata) orelse return error.UnknownSkill;
        const task_id = try self.requestIdentity(alloc, params.object, "taskId", "id", "a2a-task-");
        const context_id = try self.requestIdentity(alloc, params.object, "contextId", null, "a2a-context-");
        const reservation = if (self.task_store) |store| blk: {
            const initial_task = try taskFromEvents(alloc, task_id, context_id, &.{});
            break :blk try store.reserve(alloc, self.task_authority, task_id, initial_task);
        } else null;
        var reservation_live = reservation != null;
        errdefer if (reservation_live) {
            self.task_store.?.release(self.task_authority, task_id, reservation.?);
        };

        var queue = EventQueue.init(alloc);
        try handler.execute(alloc, .{
            .task_id = task_id,
            .context_id = context_id,
            .message = message,
            .metadata = metadata,
        }, &queue);

        const task = try taskFromEvents(alloc, task_id, context_id, queue.events.items);
        if (self.task_store) |store| {
            try store.save(alloc, self.task_authority, task_id, reservation.?, task);
            reservation_live = false;
        }
        if (stream) {
            return .{ .array = queue.events };
        }
        return task;
    }

    fn executeMessageStream(self: *Dispatcher, alloc: std.mem.Allocator, params: std.json.Value, sink: StreamSink, sink_alloc: std.mem.Allocator) !void {
        if (params != .object) return error.InvalidParams;
        const message = params.object.get("message") orelse return error.InvalidParams;
        const metadata = params.object.get("metadata");
        const handler = self.resolve(message, metadata) orelse return error.UnknownSkill;
        const task_id = try self.requestIdentity(alloc, params.object, "taskId", "id", "a2a-task-");
        const context_id = try self.requestIdentity(alloc, params.object, "contextId", null, "a2a-context-");
        const reservation = if (self.task_store) |store| blk: {
            const initial_task = try taskFromEvents(alloc, task_id, context_id, &.{});
            break :blk try store.reserve(alloc, self.task_authority, task_id, initial_task);
        } else null;
        var reservation_live = reservation != null;
        errdefer if (reservation_live) {
            self.task_store.?.release(self.task_authority, task_id, reservation.?);
        };

        var queue = EventQueue.initWithSink(alloc, sink, sink_alloc);
        try handler.execute(alloc, .{
            .task_id = task_id,
            .context_id = context_id,
            .message = message,
            .metadata = metadata,
        }, &queue);

        const task = try taskFromEvents(alloc, task_id, context_id, queue.events.items);
        if (self.task_store) |store| {
            try store.save(alloc, self.task_authority, task_id, reservation.?, task);
            reservation_live = false;
        }
    }

    fn requestIdentity(
        self: *Dispatcher,
        alloc: std.mem.Allocator,
        params: std.json.ObjectMap,
        primary_field: []const u8,
        fallback_field: ?[]const u8,
        prefix: []const u8,
    ) ![]const u8 {
        if (stringField(params, primary_field) orelse
            if (fallback_field) |field| stringField(params, field) else null) |value|
        {
            if (value.len == 0) return error.InvalidTaskId;
            return value;
        }

        var entropy: [16]u8 = undefined;
        try self.io.randomSecure(&entropy);
        const encoded = std.fmt.bytesToHex(entropy, .lower);
        return try std.fmt.allocPrint(alloc, "{s}{s}", .{ prefix, &encoded });
    }

    pub fn resolve(self: *const Dispatcher, message: std.json.Value, request_metadata: ?std.json.Value) ?AgentHandler {
        if (metadataSkill(message)) |skill| {
            if (self.findSkill(skill)) |handler| return handler;
        }
        if (request_metadata) |metadata| {
            if (metadata == .object) {
                if (stringField(metadata.object, "skill")) |skill| {
                    if (self.findSkill(skill)) |handler| return handler;
                }
            }
        }
        if (self.handlers.items.len == 1) return self.handlers.items[0];
        return null;
    }

    fn findSkill(self: *const Dispatcher, skill: []const u8) ?AgentHandler {
        for (self.handlers.items) |handler| {
            if (std.mem.eql(u8, handler.skillId(), skill)) return handler;
        }
        return null;
    }
};

pub fn messageText(alloc: std.mem.Allocator, message: std.json.Value) ![]const u8 {
    if (message != .object) return "";
    const parts = message.object.get("parts") orelse return "";
    if (parts != .array) return "";
    var out = std.ArrayListUnmanaged(u8).empty;
    for (parts.array.items) |part| {
        if (part != .object) continue;
        const kind = stringField(part.object, "kind") orelse stringField(part.object, "type") orelse "";
        if (!std.mem.eql(u8, kind, "text")) continue;
        const text = stringField(part.object, "text") orelse "";
        if (out.items.len != 0 and text.len != 0) try out.append(alloc, '\n');
        try out.appendSlice(alloc, text);
    }
    return try out.toOwnedSlice(alloc);
}

pub fn firstDataPart(message: std.json.Value) ?std.json.Value {
    if (message != .object) return null;
    const parts = message.object.get("parts") orelse return null;
    if (parts != .array) return null;
    for (parts.array.items) |part| {
        if (part != .object) continue;
        const kind = stringField(part.object, "kind") orelse stringField(part.object, "type") orelse "";
        if (!std.mem.eql(u8, kind, "data")) continue;
        return part.object.get("data") orelse part.object.get("value");
    }
    return null;
}

pub fn textPart(alloc: std.mem.Allocator, text: []const u8) !std.json.Value {
    var part = std.json.ObjectMap.empty;
    try part.put(alloc, "kind", .{ .string = "text" });
    try part.put(alloc, "text", .{ .string = text });
    var parts = std.json.Array.init(alloc);
    try parts.append(.{ .object = part });
    return .{ .array = parts };
}

pub fn dataPart(alloc: std.mem.Allocator, data: std.json.Value) !std.json.Value {
    var part = std.json.ObjectMap.empty;
    try part.put(alloc, "kind", .{ .string = "data" });
    try part.put(alloc, "data", data);
    var parts = std.json.Array.init(alloc);
    try parts.append(.{ .object = part });
    return .{ .array = parts };
}

fn taskIdParam(params: std.json.Value) ?[]const u8 {
    if (params != .object) return null;
    return stringField(params.object, "taskId") orelse stringField(params.object, "id");
}

fn taskFromEvents(alloc: std.mem.Allocator, task_id: []const u8, context_id: []const u8, events: []const std.json.Value) !std.json.Value {
    var artifacts = std.json.Array.init(alloc);
    var status: ?std.json.Value = null;

    for (events) |event| {
        if (event != .object) continue;
        const kind = stringField(event.object, "kind") orelse "";
        if (std.mem.eql(u8, kind, "artifact-update")) {
            if (event.object.get("artifact")) |artifact| try artifacts.append(artifact);
        } else if (std.mem.eql(u8, kind, "status-update")) {
            status = event.object.get("status");
        }
    }

    if (status == null) {
        var submitted = std.json.ObjectMap.empty;
        try submitted.put(alloc, "state", .{ .string = "submitted" });
        status = .{ .object = submitted };
    }

    var task = std.json.ObjectMap.empty;
    try task.put(alloc, "id", .{ .string = task_id });
    try task.put(alloc, "contextId", .{ .string = context_id });
    try task.put(alloc, "status", status.?);
    try task.put(alloc, "artifacts", .{ .array = artifacts });
    return .{ .object = task };
}

fn metadataSkill(message: std.json.Value) ?[]const u8 {
    if (message != .object) return null;
    const metadata = message.object.get("metadata") orelse return null;
    if (metadata != .object) return null;
    return stringField(metadata.object, "skill");
}

fn textMessage(alloc: std.mem.Allocator, context_id: []const u8, text: []const u8) !std.json.Value {
    var message = std.json.ObjectMap.empty;
    try message.put(alloc, "kind", .{ .string = "message" });
    try message.put(alloc, "role", .{ .string = "agent" });
    try message.put(alloc, "contextId", .{ .string = context_id });
    try message.put(alloc, "parts", try textPart(alloc, text));
    return .{ .object = message };
}

fn skillValue(alloc: std.mem.Allocator, skill: Skill) !std.json.Value {
    var tags = std.json.Array.init(alloc);
    for (skill.tags) |tag| try tags.append(.{ .string = tag });

    var input_modes = std.json.Array.init(alloc);
    try input_modes.append(.{ .string = "text" });
    try input_modes.append(.{ .string = "data" });

    var out = std.json.ObjectMap.empty;
    try out.put(alloc, "id", .{ .string = skill.id });
    try out.put(alloc, "name", .{ .string = skill.name });
    try out.put(alloc, "description", .{ .string = skill.description });
    try out.put(alloc, "tags", .{ .array = tags });
    try out.put(alloc, "inputModes", .{ .array = input_modes });
    try out.put(alloc, "outputModes", try textDataModes(alloc));
    return .{ .object = out };
}

fn textDataModes(alloc: std.mem.Allocator) !std.json.Value {
    var modes = std.json.Array.init(alloc);
    try modes.append(.{ .string = "text" });
    try modes.append(.{ .string = "data" });
    return .{ .array = modes };
}

fn stringField(object: anytype, key: []const u8) ?[]const u8 {
    const value = object.get(key) orelse return null;
    return if (value == .string) value.string else null;
}

fn successResponse(alloc: std.mem.Allocator, id: std.json.Value, result: std.json.Value) !std.json.Value {
    var out = std.json.ObjectMap.empty;
    try out.put(alloc, "jsonrpc", .{ .string = "2.0" });
    try out.put(alloc, "id", id);
    try out.put(alloc, "result", result);
    return .{ .object = out };
}

fn errorResponse(alloc: std.mem.Allocator, id: std.json.Value, code: i64, message: []const u8) !std.json.Value {
    var err = std.json.ObjectMap.empty;
    try err.put(alloc, "code", .{ .integer = code });
    try err.put(alloc, "message", .{ .string = message });

    var out = std.json.ObjectMap.empty;
    try out.put(alloc, "jsonrpc", .{ .string = "2.0" });
    try out.put(alloc, "id", id);
    try out.put(alloc, "error", .{ .object = err });
    return .{ .object = out };
}

fn stringifyValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

test "a2a dispatches message by metadata skill" {
    const alloc = std.testing.allocator;
    const Handler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "echo";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "echo", .name = "Echo", .description = "Echo skill" };
        }
        fn execute(_: *anyopaque, a: std.mem.Allocator, ctx: RequestContext, queue: *EventQueue) !void {
            const text = try messageText(a, ctx.message);
            try queue.artifact(a, ctx.task_id, ctx.context_id, "echo", try textPart(a, text));
            try queue.status(a, ctx.task_id, ctx.context_id, "completed", text);
        }
    };

    var ctx: u8 = 0;
    var dispatcher = Dispatcher{
        .io = std.Io.Threaded.global_single_threaded.io(),
        .base_url = "http://127.0.0.1/a2a",
    };
    defer dispatcher.deinit(alloc);
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = Handler.skillId,
        .skill_fn = Handler.skill,
        .execute_fn = Handler.execute,
    });

    const body =
        \\{"jsonrpc":"2.0","id":1,"method":"message/stream","params":{"taskId":"t1","contextId":"c1","message":{"kind":"message","role":"user","metadata":{"skill":"echo"},"parts":[{"kind":"text","text":"hello"}]}}}
    ;
    const resp = try dispatcher.handleJsonRpc(alloc, body);
    defer alloc.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "\"kind\":\"artifact-update\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "\"state\":\"completed\"") != null);
}

test "a2a message stream emits queue events through sink" {
    const alloc = std.testing.allocator;
    const Handler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "echo";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "echo", .name = "Echo", .description = "Echo skill" };
        }
        fn execute(_: *anyopaque, a: std.mem.Allocator, ctx: RequestContext, queue: *EventQueue) !void {
            try queue.artifact(a, ctx.task_id, ctx.context_id, "echo", try textPart(a, "hello"));
            try queue.status(a, ctx.task_id, ctx.context_id, "completed", "done");
        }
    };
    const Sink = struct {
        out: std.ArrayListUnmanaged(u8) = .empty,

        fn iface(self: *@This()) StreamSink {
            return .{ .ptr = self, .emit_fn = emit };
        }

        fn emit(ptr: *anyopaque, a: std.mem.Allocator, event: std.json.Value) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const line = try stringifyValue(a, event);
            defer a.free(line);
            try self.out.appendSlice(a, line);
            try self.out.append(a, '\n');
        }
    };

    var store = InMemoryTaskStore.init(alloc);
    defer store.deinit(alloc);

    var ctx: u8 = 0;
    var dispatcher = Dispatcher{
        .io = std.Io.Threaded.global_single_threaded.io(),
        .task_store = store.iface(),
    };
    defer dispatcher.deinit(alloc);
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = Handler.skillId,
        .skill_fn = Handler.skill,
        .execute_fn = Handler.execute,
    });

    var sink = Sink{};
    defer sink.out.deinit(alloc);
    try dispatcher.handleJsonRpcStream(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"message/stream","params":{"taskId":"t1","contextId":"c1","message":{"kind":"message","role":"user","parts":[{"kind":"text","text":"hi"}]}}}
    , sink.iface());
    try std.testing.expect(std.mem.indexOf(u8, sink.out.items, "\"kind\":\"artifact-update\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, sink.out.items, "\"kind\":\"status-update\"") != null);

    const get_resp = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":2,"method":"tasks/get","params":{"id":"t1"}}
    );
    defer alloc.free(get_resp);
    try std.testing.expect(std.mem.indexOf(u8, get_resp, "\"state\":\"completed\"") != null);
}

test "a2a agent card lists skills" {
    const alloc = std.testing.allocator;
    const Handler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "s";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "s", .name = "Skill", .description = "Desc" };
        }
        fn execute(_: *anyopaque, _: std.mem.Allocator, _: RequestContext, _: *EventQueue) !void {}
    };

    var ctx: u8 = 0;
    var dispatcher = Dispatcher{
        .io = std.Io.Threaded.global_single_threaded.io(),
        .base_url = "http://example/a2a",
    };
    defer dispatcher.deinit(alloc);
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = Handler.skillId,
        .skill_fn = Handler.skill,
        .execute_fn = Handler.execute,
    });
    const resp = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":"card","method":"agent/getAuthenticatedExtendedCard","params":{}}
    );
    defer alloc.free(resp);
    try std.testing.expect(std.mem.indexOf(u8, resp, "\"protocolVersion\":\"0.3.0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "\"id\":\"s\"") != null);
}

test "a2a maps malformed requests, unknown skills, and missing tasks to JSON-RPC errors" {
    const alloc = std.testing.allocator;
    const Handler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "echo";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "echo", .name = "Echo", .description = "Echo skill" };
        }
        fn execute(_: *anyopaque, a: std.mem.Allocator, ctx: RequestContext, queue: *EventQueue) !void {
            try queue.status(a, ctx.task_id, ctx.context_id, "completed", "done");
        }
    };
    const OtherHandler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "other";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "other", .name = "Other", .description = "Other skill" };
        }
    };

    var store = InMemoryTaskStore.init(alloc);
    defer store.deinit(alloc);
    var ctx: u8 = 0;
    var dispatcher = Dispatcher{
        .io = std.Io.Threaded.global_single_threaded.io(),
        .task_store = store.iface(),
    };
    defer dispatcher.deinit(alloc);
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = Handler.skillId,
        .skill_fn = Handler.skill,
        .execute_fn = Handler.execute,
    });
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = OtherHandler.skillId,
        .skill_fn = OtherHandler.skill,
        .execute_fn = Handler.execute,
    });

    const parse_resp = try dispatcher.handleJsonRpc(alloc, "{");
    defer alloc.free(parse_resp);
    try std.testing.expect(std.mem.indexOf(u8, parse_resp, "\"code\":-32700") != null);

    const invalid_params = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"message/send","params":[]}
    );
    defer alloc.free(invalid_params);
    try std.testing.expect(std.mem.indexOf(u8, invalid_params, "\"code\":-32602") != null);
    try std.testing.expect(std.mem.indexOf(u8, invalid_params, "\"message\":\"invalid params\"") != null);

    const unknown_skill = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":2,"method":"message/send","params":{"taskId":"t1","message":{"kind":"message","role":"user","metadata":{"skill":"missing"},"parts":[{"kind":"text","text":"hi"}]}}}
    );
    defer alloc.free(unknown_skill);
    try std.testing.expect(std.mem.indexOf(u8, unknown_skill, "\"code\":-32602") != null);
    try std.testing.expect(std.mem.indexOf(u8, unknown_skill, "\"message\":\"unknown skill\"") != null);

    const missing_task = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":3,"method":"tasks/get","params":{"id":"missing"}}
    );
    defer alloc.free(missing_task);
    try std.testing.expect(std.mem.indexOf(u8, missing_task, "\"code\":-32004") != null);
    try std.testing.expect(std.mem.indexOf(u8, missing_task, "\"message\":\"task not found\"") != null);
}

test "a2a task store supports get and cancel" {
    const alloc = std.testing.allocator;
    const Handler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "echo";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "echo", .name = "Echo", .description = "Echo skill" };
        }
        fn execute(_: *anyopaque, a: std.mem.Allocator, ctx: RequestContext, queue: *EventQueue) !void {
            try queue.artifact(a, ctx.task_id, ctx.context_id, "echo", try textPart(a, "hello"));
            try queue.status(a, ctx.task_id, ctx.context_id, "completed", "done");
        }
    };

    var store = InMemoryTaskStore.init(alloc);
    defer store.deinit(alloc);

    var ctx: u8 = 0;
    var dispatcher = Dispatcher{
        .io = std.Io.Threaded.global_single_threaded.io(),
        .base_url = "http://127.0.0.1/a2a",
        .task_store = store.iface(),
    };
    defer dispatcher.deinit(alloc);
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = Handler.skillId,
        .skill_fn = Handler.skill,
        .execute_fn = Handler.execute,
    });

    const send_resp = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"message/send","params":{"taskId":"t1","contextId":"c1","message":{"kind":"message","role":"user","parts":[{"kind":"text","text":"hi"}]}}}
    );
    defer alloc.free(send_resp);
    try std.testing.expect(std.mem.indexOf(u8, send_resp, "\"id\":\"t1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, send_resp, "\"state\":\"completed\"") != null);

    const get_resp = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":2,"method":"tasks/get","params":{"id":"t1"}}
    );
    defer alloc.free(get_resp);
    try std.testing.expect(std.mem.indexOf(u8, get_resp, "\"name\":\"echo\"") != null);

    const cancel_resp = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":3,"method":"tasks/cancel","params":{"id":"t1"}}
    );
    defer alloc.free(cancel_resp);
    try std.testing.expect(std.mem.indexOf(u8, cancel_resp, "\"state\":\"canceled\"") != null);
}

test "a2a generates unique task and context identities when omitted" {
    const alloc = std.testing.allocator;
    const Handler = struct {
        fn skillId(_: *anyopaque) []const u8 {
            return "echo";
        }
        fn skill(_: *anyopaque, _: std.mem.Allocator) !Skill {
            return .{ .id = "echo", .name = "Echo", .description = "Echo skill" };
        }
        fn execute(_: *anyopaque, a: std.mem.Allocator, ctx: RequestContext, queue: *EventQueue) !void {
            try queue.status(a, ctx.task_id, ctx.context_id, "completed", "done");
        }
    };

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var store = InMemoryTaskStore.initWithOptions(alloc, io_impl.io(), .{});
    defer store.deinit(alloc);

    var ctx: u8 = 0;
    var dispatcher = Dispatcher{
        .io = io_impl.io(),
        .task_store = store.iface(),
    };
    defer dispatcher.deinit(alloc);
    try dispatcher.addHandler(alloc, .{
        .ptr = &ctx,
        .skill_id_fn = Handler.skillId,
        .skill_fn = Handler.skill,
        .execute_fn = Handler.execute,
    });

    const send_response = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":1,"method":"message/send","params":{"message":{"kind":"message","role":"user","parts":[{"kind":"text","text":"one"}]}}}
    );
    defer alloc.free(send_response);
    var send = try std.json.parseFromSlice(std.json.Value, alloc, send_response, .{});
    defer send.deinit();
    const send_result = send.value.object.get("result").?.object;
    const first_task_id = send_result.get("id").?.string;
    const first_context_id = send_result.get("contextId").?.string;

    const stream_response = try dispatcher.handleJsonRpc(alloc,
        \\{"jsonrpc":"2.0","id":2,"method":"message/stream","params":{"message":{"kind":"message","role":"user","parts":[{"kind":"text","text":"two"}]}}}
    );
    defer alloc.free(stream_response);
    var stream = try std.json.parseFromSlice(std.json.Value, alloc, stream_response, .{});
    defer stream.deinit();
    const stream_event = stream.value.object.get("result").?.array.items[0].object;
    const second_task_id = stream_event.get("taskId").?.string;
    const second_context_id = stream_event.get("contextId").?.string;

    try std.testing.expect(std.mem.startsWith(u8, first_task_id, "a2a-task-"));
    try std.testing.expect(std.mem.startsWith(u8, second_task_id, "a2a-task-"));
    try std.testing.expect(!std.mem.eql(u8, first_task_id, second_task_id));
    try std.testing.expect(std.mem.startsWith(u8, first_context_id, "a2a-context-"));
    try std.testing.expect(std.mem.startsWith(u8, second_context_id, "a2a-context-"));
    try std.testing.expect(!std.mem.eql(u8, first_context_id, second_context_id));
    const anonymous_tasks = store.authorities.get("anonymous").?.tasks;
    try std.testing.expect(anonymous_tasks.contains(first_task_id));
    try std.testing.expect(anonymous_tasks.contains(second_task_id));
    try std.testing.expectEqual(@as(usize, 2), anonymous_tasks.count());
}

test "a2a task store bounds memory and reclaims expired tasks" {
    const alloc = std.testing.allocator;
    const Clock = struct {
        var now_ns: std.atomic.Value(u64) = .init(100);

        fn read() u64 {
            return now_ns.load(.acquire);
        }
    };

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var store = InMemoryTaskStore.initWithOptions(alloc, io_impl.io(), .{
        .max_tasks = 1,
        .max_task_bytes = 128,
        .max_total_bytes = 256,
        .retention_ttl_ns = 10,
        .cleanup_interval_ns = 100,
        .now_ns_fn = Clock.read,
    });
    defer store.deinit(alloc);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    var first_object = std.json.ObjectMap.empty;
    try first_object.put(arena, "id", .{ .string = "first" });
    const first: std.json.Value = .{ .object = first_object };
    _ = try store.iface().reserve(arena, "principal:alice", "task-1", first);

    var second_object = std.json.ObjectMap.empty;
    try second_object.put(arena, "id", .{ .string = "second" });
    const second: std.json.Value = .{ .object = second_object };
    try std.testing.expectError(
        error.A2aTaskCapacityExceeded,
        store.iface().reserve(arena, "principal:alice", "task-2", second),
    );

    Clock.now_ns.store(110, .release);
    _ = try store.iface().reserve(arena, "principal:alice", "task-2", second);
    try std.testing.expectError(error.TaskNotFound, store.iface().get(arena, "principal:alice", "task-1"));
    const stored = try store.iface().get(arena, "principal:alice", "task-2");
    try std.testing.expectEqualStrings("second", stored.object.get("id").?.string);
}

test "a2a task store reclaims expired tasks under byte pressure" {
    const alloc = std.testing.allocator;
    const Clock = struct {
        var now_ns: std.atomic.Value(u64) = .init(100);

        fn read() u64 {
            return now_ns.load(.acquire);
        }
    };

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var store = InMemoryTaskStore.initWithOptions(alloc, io_impl.io(), .{
        .max_tasks = 8,
        .max_bytes_per_authority = 18,
        .retention_ttl_ns = 10,
        .cleanup_interval_ns = 1000,
        .now_ns_fn = Clock.read,
    });
    defer store.deinit(alloc);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const iface = store.iface();
    _ = try iface.reserve(arena, "principal:alice", "t1", .{ .string = "1234567890" });
    try std.testing.expectError(
        error.A2aTaskCapacityExceeded,
        iface.reserve(arena, "principal:alice", "t2", .{ .string = "x" }),
    );

    Clock.now_ns.store(110, .release);
    store.next_cleanup_ns = 1000;
    _ = try iface.reserve(arena, "principal:alice", "t2", .{ .string = "x" });
    try std.testing.expectError(error.TaskNotFound, iface.get(arena, "principal:alice", "t1"));
}

test "a2a task store scopes identities and reservations to authority" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var store = InMemoryTaskStore.initWithOptions(alloc, io_impl.io(), .{});
    defer store.deinit(alloc);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const iface = store.iface();
    _ = try iface.reserve(arena, "principal:alice", "shared-id", .{ .string = "alice" });
    _ = try iface.reserve(arena, "principal:bob", "shared-id", .{ .string = "bob" });

    try std.testing.expectError(
        error.A2aTaskAlreadyExists,
        iface.reserve(arena, "principal:alice", "shared-id", .{ .string = "replacement" }),
    );
    try std.testing.expectEqualStrings(
        "alice",
        (try iface.get(arena, "principal:alice", "shared-id")).string,
    );
    try std.testing.expectEqualStrings(
        "bob",
        (try iface.get(arena, "principal:bob", "shared-id")).string,
    );
    try std.testing.expectError(
        error.TaskNotFound,
        iface.get(arena, "principal:mallory", "shared-id"),
    );
}

test "a2a task store reuses expired ids and preserves per-authority capacity" {
    const alloc = std.testing.allocator;
    const Clock = struct {
        var now_ns: std.atomic.Value(u64) = .init(100);

        fn read() u64 {
            return now_ns.load(.acquire);
        }
    };
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var store = InMemoryTaskStore.initWithOptions(alloc, io_impl.io(), .{
        .max_tasks = 8,
        .max_bytes_per_authority = 20,
        .retention_ttl_ns = 10,
        .cleanup_interval_ns = 1000,
        .now_ns_fn = Clock.read,
    });
    defer store.deinit(alloc);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const iface = store.iface();
    const old_generation = try iface.reserve(arena, "principal:alice", "t1", .{ .string = "1234567890" });
    try std.testing.expectError(
        error.A2aTaskCapacityExceeded,
        iface.reserve(arena, "principal:alice", "t2", .{ .string = "1234567890" }),
    );
    _ = try iface.reserve(arena, "principal:bob", "t2", .{ .string = "1234567890" });

    Clock.now_ns.store(110, .release);
    const replacement_generation = try iface.reserve(arena, "principal:alice", "t1", .{ .string = "replacement" });
    try std.testing.expect(replacement_generation != old_generation);
    iface.release("principal:alice", "t1", old_generation);
    try std.testing.expectEqualStrings(
        "replacement",
        (try iface.get(arena, "principal:alice", "t1")).string,
    );
    try std.testing.expectError(
        error.A2aTaskStoreBusy,
        iface.save(
            arena,
            "principal:alice",
            "t1",
            old_generation,
            .{ .string = "stale" },
        ),
    );
}

test "a2a task store rejects oversized records and identifiers" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var store = InMemoryTaskStore.initWithOptions(alloc, io_impl.io(), .{
        .max_task_id_bytes = 8,
        .max_task_bytes = 32,
    });
    defer store.deinit(alloc);

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const oversized: std.json.Value = .{
        .string = "this task body is intentionally larger than the configured bound",
    };
    try std.testing.expectError(
        error.A2aTaskTooLarge,
        store.iface().reserve(arena, "principal:alice", "task", oversized),
    );
    try std.testing.expectError(
        error.InvalidTaskId,
        store.iface().reserve(arena, "principal:alice", "identifier-too-long", .null),
    );
}
