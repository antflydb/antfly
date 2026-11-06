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

//! Fixed-size owned publication parameters. Allocate and encode expensive
//! immutable inputs before admission; fill authoritative identity bindings
//! before the host stages them. Unbound values fail before durable commit.
const std = @import("std");
const abi = @import("../runtime_callback_abi.zig");

/// Optional allocator/accounting lifetime retained by a value independently
/// of its producer. Native callers use an independently owned registry lease.
pub const Owner = struct {
    ptr: *anyopaque,
    retain_fn: *const fn (*anyopaque) void,
    release_fn: *const fn (*anyopaque) void,
    fn retained(self: Owner) Owner {
        self.retain_fn(self.ptr);
        return self;
    }
    fn release(self: Owner) void {
        self.release_fn(self.ptr);
    }
};

pub const Value = struct {
    ptr: *anyopaque,
    /// Stable storage for a prepared mutation descriptor. Its bytes may not
    /// be inspected or published before read() establishes the sealed fence.
    initialization_view: []const u8,
    vtable: *const VTable,
    dispatch: Boundary.Dispatch = Boundary.local_dispatch,
    pub const VTable = struct {
        read: *const fn (*anyopaque) anyerror![]const u8,
        retain: *const fn (*anyopaque) void,
        release: *const fn (*anyopaque) void,
    };
    const Boundary = abi.Boundary(VTable);

    pub fn read(self: Value) ![]const u8 {
        const bytes = try Boundary.call("read", self.dispatch, self.vtable.read, .{self.ptr});
        if (bytes.len != self.initialization_view.len or bytes.ptr != self.initialization_view.ptr) return error.InvalidNativePublicationValue;
        return bytes;
    }
    pub fn retained(self: Value) Value {
        self.vtable.retain(self.ptr);
        return self;
    }
    pub fn deinit(self: *Value) void {
        self.vtable.release(self.ptr);
        self.* = undefined;
    }
};

/// One producer owns mutable initialization. Readers own only Value handles;
/// none can observe initialization bytes before the release/acquire seal.
/// Initialization slices must not escape their preparation scope.
pub const Builder = struct {
    state: *State,
    active: bool = true,

    pub fn create(a: std.mem.Allocator, length: usize) !Builder {
        const state = try a.create(State);
        errdefer a.destroy(state);
        const buffer = try a.alloc(u8, length);
        @memset(buffer, 0);
        state.* = .{ .alloc = a, .bytes = buffer };
        return .{ .state = state };
    }
    pub fn createWithOwner(a: std.mem.Allocator, length: usize, owner: Owner) !Builder {
        const builder = try create(a, length);
        builder.state.owner = owner.retained();
        return builder;
    }
    /// The caller retains credit on failure; success transfers it into the
    /// value's allocation owner until the final reader releases the buffer.
    pub fn createBudgeted(a: std.mem.Allocator, length: usize, owner: Owner, credit: anytype) !Builder {
        const Holder = BudgetOwner(@TypeOf(credit));
        const holder = try a.create(Holder);
        errdefer a.destroy(holder);
        const builder = try create(a, length);
        holder.* = .{ .alloc = a, .owner = owner.retained(), .credit = credit };
        builder.state.owner = .{ .ptr = holder, .retain_fn = Holder.retain, .release_fn = Holder.release };
        return builder;
    }
    pub fn budgetedBytes(comptime Credit: type, length: usize) !usize {
        return std.math.add(usize, @sizeOf(State) + @sizeOf(BudgetOwner(Credit)), length);
    }
    pub fn value(self: *const Builder) Value {
        State.retain(self.state);
        return .{ .ptr = self.state, .initialization_view = self.state.bytes, .vtable = &State.vtable };
    }
    pub fn bytes(self: *Builder) ![]u8 {
        if (!self.active) return error.NativePublicationValueAlreadyBound;
        return self.state.bytes;
    }
    pub fn write(self: *Builder, offset: usize, input: []const u8) !void {
        const target = try self.bytes();
        if (offset > target.len or input.len > target.len - offset) return error.InvalidNativePublicationValue;
        @memcpy(target[offset..][0..input.len], input);
    }
    pub fn seal(self: *Builder) !void {
        if (!self.active) return error.NativePublicationValueAlreadyBound;
        self.active = false;
        self.state.ready.store(true, .release);
    }
    pub fn deinit(self: *Builder) void {
        State.release(self.state);
        self.* = undefined;
    }
};

fn BudgetOwner(comptime Credit: type) type {
    return struct {
        alloc: std.mem.Allocator,
        owner: Owner,
        credit: Credit,
        references: std.atomic.Value(usize) = .init(1),
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const previous = self.references.fetchAdd(1, .monotonic);
            std.debug.assert(previous != 0);
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.references.fetchSub(1, .acq_rel) != 1) return;
            const a = self.alloc;
            const owner = self.owner;
            self.credit.release();
            a.destroy(self);
            owner.release();
        }
    };
}

const State = struct {
    alloc: std.mem.Allocator,
    bytes: []u8,
    owner: ?Owner = null,
    references: std.atomic.Value(usize) = .init(1),
    ready: std.atomic.Value(bool) = .init(false),
    const vtable: Value.VTable = .{ .read = read, .retain = retain, .release = release };
    fn read(raw: *anyopaque) ![]const u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (!self.ready.load(.acquire)) return error.NativePublicationValueNotBound;
        return self.bytes;
    }
    fn retain(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0);
    }
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        const a = self.alloc;
        const owner = self.owner;
        a.free(self.bytes);
        a.destroy(self);
        if (owner) |lease| lease.release();
    }
};

test "native publication values bind once and outlive their preparation owner" {
    const a = std.testing.allocator;
    var builder = try Builder.create(a, 8);
    var builder_live = true;
    defer if (builder_live) builder.deinit();
    var value = builder.value();
    defer value.deinit();
    var retained = value.retained();
    defer retained.deinit();
    try std.testing.expectError(error.NativePublicationValueNotBound, value.read());
    try builder.write(0, "prepared");
    try std.testing.expectError(error.NativePublicationValueNotBound, retained.read());
    try builder.seal();
    try std.testing.expectError(error.NativePublicationValueAlreadyBound, builder.bytes());
    try std.testing.expectError(error.NativePublicationValueAlreadyBound, builder.seal());
    builder.deinit();
    builder_live = false;
    try std.testing.expectEqualStrings("prepared", try retained.read());
}

test "native publication values reject invalid writes without changing prepared bytes" {
    var builder = try Builder.create(std.testing.allocator, 4);
    defer builder.deinit();
    var value = builder.value();
    defer value.deinit();
    try builder.write(0, "safe");
    try std.testing.expectError(error.InvalidNativePublicationValue, builder.write(5, ""));
    try std.testing.expectError(error.InvalidNativePublicationValue, builder.write(3, "xx"));
    try std.testing.expectError(error.InvalidNativePublicationValue, builder.write(std.math.maxInt(usize), "x"));
    try builder.seal();
    try std.testing.expectEqualStrings("safe", try value.read());
}

test "native publication values preserve unbound refusal after producer cancellation" {
    var builder = try Builder.create(std.testing.allocator, 0);
    var value = builder.value();
    defer value.deinit();
    builder.deinit();
    try std.testing.expectError(error.NativePublicationValueNotBound, value.read());
}

test "native publication values retain their allocation owner until the last reader closes" {
    const Probe = struct {
        references: usize = 1,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references -= 1;
        }
    };
    var owner: Probe = .{};
    var builder = try Builder.createWithOwner(std.testing.allocator, 0, .{ .ptr = &owner, .retain_fn = Probe.retain, .release_fn = Probe.release });
    try std.testing.expectEqual(@as(usize, 2), owner.references);
    var value = builder.value();
    try builder.seal();
    builder.deinit();
    try std.testing.expectEqual(@as(usize, 2), owner.references);
    try std.testing.expectEqual(@as(usize, 0), (try value.read()).len);
    value.deinit();
    try std.testing.expectEqual(@as(usize, 1), owner.references);
}

test "native publication value allocation failures preserve external owner references" {
    const Probe = struct {
        references: usize = 1,
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references -= 1;
        }
        fn run(a: std.mem.Allocator) !void {
            var owner: @This() = .{};
            defer std.debug.assert(owner.references == 1);
            var builder = try Builder.createWithOwner(a, 4, .{ .ptr = &owner, .retain_fn = retain, .release_fn = release });
            defer builder.deinit();
            var value = builder.value();
            defer value.deinit();
            try builder.write(0, "safe");
            try builder.seal();
            try std.testing.expectEqualStrings("safe", try value.read());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "native publication values reject a provider rebinding their fixed buffer" {
    const Provider = struct {
        fn read(_: *anyopaque) ![]const u8 {
            return "mismatch";
        }
        fn retain(_: *anyopaque) void {}
        fn release(_: *anyopaque) void {}
        const vtable: Value.VTable = .{ .read = read, .retain = retain, .release = release };
    };
    var context: u8 = 0;
    var value: Value = .{ .ptr = &context, .initialization_view = "expected", .vtable = &Provider.vtable };
    defer value.deinit();
    try std.testing.expectError(error.InvalidNativePublicationValue, value.read());
}

test "native publication value budget ownership survives producer close and unwinds allocation failures" {
    const Probe = struct {
        owners: usize = 1,
        credits: usize = 1,
        const ProbeType = @This();
        const Credit = struct {
            probe: *ProbeType,
            fn release(self: *@This()) void {
                self.probe.credits -= 1;
            }
        };
        fn retain(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.owners += 1;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.owners -= 1;
        }
        fn run(a: std.mem.Allocator) !void {
            var probe: @This() = .{};
            defer std.debug.assert(probe.owners == 1 and probe.credits == 0);
            var credit: Credit = .{ .probe = &probe };
            var transferred = false;
            defer if (!transferred) credit.release();
            var builder = try Builder.createBudgeted(a, 4, .{ .ptr = &probe, .retain_fn = retain, .release_fn = release }, credit);
            transferred = true;
            var builder_live = true;
            defer if (builder_live) builder.deinit();
            var value = builder.value();
            defer value.deinit();
            try builder.write(0, "safe");
            try builder.seal();
            builder.deinit();
            builder_live = false;
            try std.testing.expectEqual(@as(usize, 2), probe.owners);
            try std.testing.expectEqual(@as(usize, 1), probe.credits);
            try std.testing.expectEqualStrings("safe", try value.read());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
    try std.testing.expect((try Builder.budgetedBytes(Probe.Credit, 4)) > 4);
    try std.testing.expectError(error.Overflow, Builder.budgetedBytes(Probe.Credit, std.math.maxInt(usize)));
}

test "native publication values preserve seal errors and bytes through checked foreign dispatch" {
    const Bridge = struct {
        const Function = @typeInfo(abi.CallbackDispatch).pointer.child;
        const Arguments = std.meta.ArgsTuple(Function);
        const Return = @typeInfo(Function).@"fn".return_type.?;
        fn dispatch(contract: @FieldType(Arguments, "0"), callback: @FieldType(Arguments, "1"), args: @FieldType(Arguments, "2"), output: @FieldType(Arguments, "3")) callconv(.c) Return {
            return Value.Boundary.local_dispatch(contract, callback, args, output);
        }
    };
    var builder = try Builder.create(std.testing.allocator, 4);
    defer builder.deinit();
    var value = builder.value();
    defer value.deinit();
    value.dispatch = Bridge.dispatch;
    try std.testing.expectError(error.NativePublicationValueNotBound, value.read());
    try builder.write(0, "safe");
    try builder.seal();
    try std.testing.expectEqualStrings("safe", try value.read());
}
