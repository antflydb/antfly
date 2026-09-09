// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Same-toolchain executor bridge between independent Zig error domains.
//! Handles and buffers remain borrowed. Error identities, including embedded
//! operation/batch results, cross as names and are reconstructed by the receiver.
//! The Receiver must stay at a stable address until its tasks and I/O finish.
const std = @import("std");
const native = @import("runtime_native_abi.zig");
const fields = std.meta.fields(std.Io.VTable);
const ErrorName = extern struct { ptr: [*]const u8, len: usize };
const ErrorNames = *const fn (u16) callconv(.c) ErrorName;

fn errorName(code: u16) callconv(.c) ErrorName {
    const name = @errorName(@errorFromInt(code));
    return .{ .ptr = name.ptr, .len = name.len };
}

fn decodeError(comptime E: type, name: ErrorName) E {
    inline for (@typeInfo(E).error_set.?) |item| {
        if (std.mem.eql(u8, name.ptr[0..name.len], item.name))
            return @field(E, item.name);
    }
    @panic("incompatible executor error domain");
}

fn hasErrors(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .error_set, .error_union => true,
        .optional => |info| hasErrors(info.child),
        .@"struct" => |info| blk: {
            for (info.fields) |field| if (hasErrors(field.type)) break :blk true;
            break :blk false;
        },
        .@"union" => |info| blk: {
            for (info.fields) |field| if (hasErrors(field.type)) break :blk true;
            break :blk false;
        },
        else => false,
    };
}

fn Wire(comptime T: type) type {
    @setEvalBranchQuota(100_000);
    if (!hasErrors(T)) return T;
    return switch (@typeInfo(T)) {
        .error_set => ErrorName,
        .error_union => |info| struct { failure: ?ErrorName, value: Wire(info.payload) },
        .optional => |info| ?Wire(info.child),
        .@"struct" => |info| blk: {
            var names: [info.fields.len][]const u8 = undefined;
            var types: [info.fields.len]type = undefined;
            for (info.fields, 0..) |field, i| {
                names[i] = field.name;
                types[i] = Wire(field.type);
            }
            break :blk @Struct(.auto, null, &names, &types, &@splat(.{}));
        },
        .@"union" => |info| blk: {
            var names: [info.fields.len][]const u8 = undefined;
            var types: [info.fields.len]type = undefined;
            for (info.fields, 0..) |field, i| {
                names[i] = field.name;
                types[i] = Wire(field.type);
            }
            break :blk @Union(.auto, info.tag_type.?, &names, &types, &@splat(.{}));
        },
        else => unreachable,
    };
}

fn encode(comptime T: type, value: T) Wire(T) {
    if (comptime !hasErrors(T)) return value;
    return switch (@typeInfo(T)) {
        .error_set => errorName(@intFromError(value)),
        .error_union => |info| if (value) |payload|
            .{ .failure = null, .value = encode(info.payload, payload) }
        else |err|
            .{ .failure = errorName(@intFromError(err)), .value = undefined },
        .optional => |info| if (value) |payload| encode(info.child, payload) else null,
        .@"struct" => |info| blk: {
            var result: Wire(T) = undefined;
            inline for (info.fields) |field| @field(result, field.name) = encode(field.type, @field(value, field.name));
            break :blk result;
        },
        .@"union" => switch (value) {
            inline else => |payload, tag| @unionInit(Wire(T), @tagName(tag), encode(@TypeOf(payload), payload)),
        },
        else => unreachable,
    };
}

fn decode(comptime T: type, value: Wire(T)) T {
    if (comptime !hasErrors(T)) return value;
    return switch (@typeInfo(T)) {
        .error_set => decodeError(T, value),
        .error_union => |info| if (value.failure) |name| decodeError(info.error_set, name) else decode(info.payload, value.value),
        .optional => |info| if (value) |payload| decode(info.child, payload) else null,
        .@"struct" => |info| blk: {
            var result: T = undefined;
            inline for (info.fields) |field| @field(result, field.name) = decode(field.type, @field(value, field.name));
            break :blk result;
        },
        .@"union" => switch (value) {
            inline else => |payload, tag| @unionInit(T, @tagName(tag), decode(@FieldType(T, @tagName(tag)), payload)),
        },
        else => unreachable,
    };
}

// Batch storage is caller-owned and contains native error unions. Translate
// only completed results at the API boundary; pending OS state stays opaque.
fn remap(comptime T: type, value: T, names: ErrorNames) T {
    if (comptime !hasErrors(T)) return value;
    return switch (@typeInfo(T)) {
        .error_set => decodeError(T, names(@intFromError(value))),
        .error_union => |info| if (value) |payload| remap(info.payload, payload, names) else |err| decodeError(info.error_set, names(@intFromError(err))),
        .optional => |info| if (value) |payload| remap(info.child, payload, names) else null,
        .@"struct" => |info| blk: {
            var result: T = undefined;
            inline for (info.fields) |field| @field(result, field.name) = remap(field.type, @field(value, field.name), names);
            break :blk result;
        },
        .@"union" => switch (value) {
            inline else => |payload, tag| @unionInit(T, @tagName(tag), remap(@TypeOf(payload), payload, names)),
        },
        else => unreachable,
    };
}

fn remapBatch(batch: *std.Io.Batch, names: ErrorNames) void {
    var index = batch.completed.head;
    while (index != .none) {
        const completion = &batch.storage[index.toIndex()].completion;
        completion.result = remap(std.Io.Operation.Result, completion.result, names);
        index = completion.node.next;
    }
}

const ReaderState = struct {
    file: std.Io.File,
    err: ?std.Io.File.Reader.Error,
    mode: std.Io.File.Reader.Mode,
    pos: u64,
    size: ?u64,
    size_err: ?std.Io.File.Reader.SizeError,
    seek_err: ?std.Io.File.Reader.SeekError,
    buffer: []u8,
    seek: usize,
    end: usize,

    fn capture(reader: *std.Io.File.Reader) ReaderState {
        return .{ .file = reader.file, .err = reader.err, .mode = reader.mode, .pos = reader.pos, .size = reader.size, .size_err = reader.size_err, .seek_err = reader.seek_err, .buffer = reader.interface.buffer, .seek = reader.interface.seek, .end = reader.interface.end };
    }

    fn restore(self: ReaderState, reader: *std.Io.File.Reader) void {
        reader.file = self.file;
        reader.err = self.err;
        reader.mode = self.mode;
        reader.pos = self.pos;
        reader.size = self.size;
        reader.size_err = self.size_err;
        reader.seek_err = self.seek_err;
        reader.interface.buffer = self.buffer;
        reader.interface.seek = self.seek;
        reader.interface.end = self.end;
    }
};
const ReaderTransfer = struct { executor: Borrow, state: Wire(ReaderState) };
const Dispatch = *const fn (*const Borrow, u16, *const anyopaque, *anyopaque, ?*ReaderTransfer, ErrorNames) callconv(.c) void;

pub const Borrow = extern struct {
    userdata: ?*anyopaque,
    vtable: *const anyopaque,
    contract: native.TypeContract,
    dispatch: Dispatch,
    error_names: ErrorNames,

    pub fn init(io: *const std.Io) Borrow {
        return .{ .userdata = io.userdata, .vtable = io.vtable, .contract = .of(std.Io), .dispatch = dispatchLocal, .error_names = errorName };
    }

    pub fn receive(self: Borrow) !Receiver {
        if (!self.contract.matches(.of(std.Io))) return error.InvalidArgument;
        if (@intFromPtr(self.vtable) % @alignOf(std.Io.VTable) != 0) return error.InvalidArgument;
        return .{ .borrow = self };
    }
};

pub const Receiver = struct {
    borrow: Borrow,
    stderr_writer: std.Io.File.Writer = undefined,
    stderr_depth: usize = 0,

    pub fn io(self: *Receiver) std.Io {
        if (self.borrow.dispatch == &dispatchLocal)
            return .{ .userdata = self.borrow.userdata, .vtable = @ptrCast(@alignCast(self.borrow.vtable)) };
        return .{ .userdata = self, .vtable = &vtable };
    }

    fn call(self: *Receiver, comptime name: []const u8, args_value: anytype) Return(name) {
        const Args = std.meta.ArgsTuple(Function(name));
        var args: Args = args_value;
        var reader_transfer: ReaderTransfer = undefined;
        var reader_ptr: ?*std.Io.File.Reader = null;
        inline for (std.meta.fields(Args)) |field| {
            if (comptime field.type == *std.Io.File.Reader) {
                reader_ptr = @field(args, field.name);
                reader_transfer = .{ .executor = Borrow.init(&reader_ptr.?.io), .state = encode(ReaderState, ReaderState.capture(reader_ptr.?)) };
            }
        }
        var output: Wire(Return(name)) = undefined;
        self.borrow.dispatch(&self.borrow, @intCast(std.meta.fieldIndex(std.Io.VTable, name).?), &args, @ptrCast(&output), if (reader_ptr != null) &reader_transfer else null, errorName);
        if (reader_ptr) |reader| decode(ReaderState, reader_transfer.state).restore(reader);
        inline for (std.meta.fields(Args)) |field| {
            if (comptime field.type == *std.Io.Batch) remapBatch(@field(args, field.name), self.borrow.error_names);
        }
        return decode(Return(name), output);
    }

    fn stderr(self: *Receiver, locked: std.Io.LockedStderr) std.Io.LockedStderr {
        if (self.stderr_depth > 0) self.stderr_writer.interface.flush() catch {};
        self.stderr_depth += 1;
        self.stderr_writer = std.Io.File.Writer.initStreaming(locked.file_writer.file, self.io(), &.{});
        return .{ .file_writer = &self.stderr_writer, .terminal_mode = locked.terminal_mode };
    }
};

fn Function(comptime name: []const u8) type {
    return @typeInfo(@FieldType(std.Io.VTable, name)).pointer.child;
}
fn Return(comptime name: []const u8) type {
    return @typeInfo(Function(name)).@"fn".return_type.?;
}
fn parameter(comptime name: []const u8, comptime i: usize) type {
    return @typeInfo(Function(name)).@"fn".params[i].type.?;
}
fn receiver(ptr: ?*anyopaque) *Receiver {
    return @ptrCast(@alignCast(ptr.?));
}
fn wrapper(comptime name: []const u8) @FieldType(std.Io.VTable, name) {
    const W = struct {
        fn f1(p0: parameter(name, 0)) Return(name) {
            return receiver(p0).call(name, .{p0});
        }
        fn f2(p0: parameter(name, 0), p1: parameter(name, 1)) Return(name) {
            return receiver(p0).call(name, .{ p0, p1 });
        }
        fn f3(p0: parameter(name, 0), p1: parameter(name, 1), p2: parameter(name, 2)) Return(name) {
            return receiver(p0).call(name, .{ p0, p1, p2 });
        }
        fn f4(p0: parameter(name, 0), p1: parameter(name, 1), p2: parameter(name, 2), p3: parameter(name, 3)) Return(name) {
            return receiver(p0).call(name, .{ p0, p1, p2, p3 });
        }
        fn f5(p0: parameter(name, 0), p1: parameter(name, 1), p2: parameter(name, 2), p3: parameter(name, 3), p4: parameter(name, 4)) Return(name) {
            return receiver(p0).call(name, .{ p0, p1, p2, p3, p4 });
        }
        fn f6(p0: parameter(name, 0), p1: parameter(name, 1), p2: parameter(name, 2), p3: parameter(name, 3), p4: parameter(name, 4), p5: parameter(name, 5)) Return(name) {
            return receiver(p0).call(name, .{ p0, p1, p2, p3, p4, p5 });
        }
        fn f7(p0: parameter(name, 0), p1: parameter(name, 1), p2: parameter(name, 2), p3: parameter(name, 3), p4: parameter(name, 4), p5: parameter(name, 5), p6: parameter(name, 6)) Return(name) {
            return receiver(p0).call(name, .{ p0, p1, p2, p3, p4, p5, p6 });
        }
        fn f8(p0: parameter(name, 0), p1: parameter(name, 1), p2: parameter(name, 2), p3: parameter(name, 3), p4: parameter(name, 4), p5: parameter(name, 5), p6: parameter(name, 6), p7: parameter(name, 7)) Return(name) {
            return receiver(p0).call(name, .{ p0, p1, p2, p3, p4, p5, p6, p7 });
        }
    };
    return switch (@typeInfo(Function(name)).@"fn".params.len) {
        1 => &W.f1,
        2 => &W.f2,
        3 => &W.f3,
        4 => &W.f4,
        5 => &W.f5,
        6 => &W.f6,
        7 => &W.f7,
        8 => &W.f8,
        else => @compileError("unsupported std.Io vtable arity"),
    };
}

fn lockStderr(ptr: ?*anyopaque, mode: ?std.Io.Terminal.Mode) std.Io.Cancelable!std.Io.LockedStderr {
    const self = receiver(ptr);
    return self.stderr(try self.call("lockStderr", .{ ptr, mode }));
}
fn tryLockStderr(ptr: ?*anyopaque, mode: ?std.Io.Terminal.Mode) std.Io.Cancelable!?std.Io.LockedStderr {
    const self = receiver(ptr);
    return self.stderr((try self.call("tryLockStderr", .{ ptr, mode })) orelse return null);
}
fn unlockStderr(ptr: ?*anyopaque) void {
    const self = receiver(ptr);
    std.debug.assert(self.stderr_depth > 0);
    self.stderr_writer.interface.flush() catch {};
    self.stderr_depth -= 1;
    self.call("unlockStderr", .{ptr});
}

const vtable: std.Io.VTable = blk: {
    @setEvalBranchQuota(100_000);
    var result: std.Io.VTable = undefined;
    for (fields) |field| @field(result, field.name) = wrapper(field.name);
    result.lockStderr = lockStderr;
    result.tryLockStderr = tryLockStderr;
    result.unlockStderr = unlockStderr;
    break :blk result;
};

fn dispatchLocal(borrow: *const Borrow, method: u16, raw_args: *const anyopaque, output: *anyopaque, reader_transfer: ?*ReaderTransfer, names: ErrorNames) callconv(.c) void {
    @setEvalBranchQuota(100_000);
    inline for (fields, 0..) |field, index| {
        if (method == index) {
            const Args = std.meta.ArgsTuple(Function(field.name));
            var args = @as(*const Args, @ptrCast(@alignCast(raw_args))).*;
            args[0] = borrow.userdata;
            const owner_vtable: *const std.Io.VTable = @ptrCast(@alignCast(borrow.vtable));
            var reader_io: Receiver = undefined;
            var local_reader: std.Io.File.Reader = undefined;
            inline for (std.meta.fields(Args)) |arg| {
                if (comptime arg.type == *std.Io.File.Reader) {
                    const transfer = reader_transfer.?;
                    reader_io = transfer.executor.receive() catch @panic("invalid reader executor borrow");
                    const state = decode(ReaderState, transfer.state);
                    local_reader = std.Io.File.Reader.init(state.file, reader_io.io(), state.buffer);
                    state.restore(&local_reader);
                    @field(args, arg.name) = &local_reader;
                }
                if (comptime arg.type == *std.Io.Batch) remapBatch(@field(args, arg.name), names);
            }
            const result = @call(.auto, @field(owner_vtable, field.name), args);
            if (comptime std.mem.eql(u8, field.name, "lockStderr")) {
                if (result) |locked| locked.file_writer.interface.flush() catch {} else |_| {}
            } else if (comptime std.mem.eql(u8, field.name, "tryLockStderr")) {
                if (result) |maybe_locked| {
                    if (maybe_locked) |locked| locked.file_writer.interface.flush() catch {};
                } else |_| {}
            }
            if (reader_transfer) |transfer| {
                // Only the file-copy methods accept ReaderTransfer.
                inline for (std.meta.fields(Args)) |arg| {
                    if (comptime arg.type == *std.Io.File.Reader)
                        transfer.state = encode(ReaderState, ReaderState.capture(&local_reader));
                }
            }
            @as(*Wire(Return(field.name)), @ptrCast(@alignCast(output))).* = encode(Return(field.name), result);
            return;
        }
    }
    @panic("invalid executor bridge method");
}
