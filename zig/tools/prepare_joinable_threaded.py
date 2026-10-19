#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Add owned stack mappings and physical joins to pinned Zig 0.17 Threaded.

Only a generated worktree-local library overlay is written. The installed/Nix
toolchain remains immutable. Unknown source layouts fail before copying output.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import sys


def replace_once(source: str, old: str, new: str, count: int = 1) -> str:
    if source.count(old) != count:
        raise RuntimeError(f"unknown pinned Threaded layout for {old[:80]!r}")
    return source.replace(old, new)


def patch_threaded(source: str) -> str:
    source = replace_once(
        source,
        "worker_threads: std.atomic.Value(?*Thread),\n",
        """worker_threads: std.atomic.Value(?*Thread),
// Antfly: bounded owned pools retain handles until actual thread termination.
joinable_workers: bool = false,
joinable_worker_count: usize = 0,
joinable_worker_handles: [256]OwnedWorker = undefined,
worker_stack_allocator: ?std.mem.Allocator = null,
""",
    )
    source = replace_once(
        source,
        "    stack_size: usize = std.Thread.SpawnConfig.default_stack_size,\n",
        """    stack_size: usize = std.Thread.SpawnConfig.default_stack_size,
    /// Require explicit finite async/concurrent limits <= 256. Deinit joins
    /// thread exit, including TLS destructors, after draining worker bodies.
    joinable_workers: bool = false,
    /// Page-aligned exclusively mapped storage. Free must unmap without
    /// reading the protected guard pages. Requires joinable pthread workers.
    worker_stack_allocator: ?std.mem.Allocator = null,
""",
    )
    source = replace_once(
        source,
        "    if (builtin.single_threaded) return .{\n        .allocator = gpa,",
        """    if (options.worker_stack_allocator != null and (!options.joinable_workers or !std.Thread.use_pthreads))
        @panic("owned stack mappings require joinable pthread workers");
    if (options.joinable_workers) {
        const async_limit = options.async_limit orelse @panic("joinable pool requires explicit async limit");
        if (@backingInt(async_limit) > 256 or @backingInt(options.concurrent_limit) > 256)
            @panic("joinable pool exceeds bounded handle capacity");
    }
    if (builtin.single_threaded) return .{
        .allocator = gpa,""",
    )
    source = replace_once(
        source,
        "        .stack_size = options.stack_size,\n",
        "        .stack_size = options.stack_size,\n        .joinable_workers = options.joinable_workers,\n        .worker_stack_allocator = options.worker_stack_allocator,\n",
        2,
    )
    source = replace_once(
        source,
        "std.Thread.spawn(.{ .stack_size = t.stack_size }, worker, .{t})",
        "spawnWorker(t)",
        4,
    )
    source = replace_once(
        source, "        thread.detach();\n", "        retainWorker(t, thread);\n", 4
    )
    source = replace_once(
        source,
        "    if (busy_count >= @backingInt(t.async_limit)) {",
        "    if ((t.joinable_workers and t.join_requested) or busy_count >= @backingInt(t.async_limit)) {",
        2,
    )
    source = replace_once(
        source,
        "    if (busy_count >= @backingInt(t.concurrent_limit))",
        "    if ((t.joinable_workers and t.join_requested) or busy_count >= @backingInt(t.concurrent_limit))",
        2,
    )
    source = replace_once(
        source,
        "    Thread.current = &thread;\n",
        """    Thread.current = &thread;
    // Do not leave TLS destructors pointing at the returned worker's stack
    // frame. Capture the immutable flag before a detached pool can be freed.
    const joinable_worker = t.joinable_workers;
    defer if (joinable_worker) { Thread.current = null; };
""",
    )
    source = replace_once(
        source,
        "pub fn setAsyncLimit(t: *Threaded, new_limit: Io.Limit) void {\n",
        """pub fn setAsyncLimit(t: *Threaded, new_limit: Io.Limit) void {
    if (t.joinable_workers and @backingInt(new_limit) > 256)
        @panic("joinable pool exceeds bounded handle capacity");
""",
    )
    source = replace_once(
        source,
        "    t.wait_group.wait();\n}\n\nfn worker(t: *Threaded) void {",
        """    t.wait_group.wait();
    if (t.joinable_workers) {
        for (t.joinable_worker_handles[0..t.joinable_worker_count]) |owned| {
            owned.thread.join();
            if (owned.mapping) |mapping| t.worker_stack_allocator.?.rawFree(mapping, .fromByteUnits(std.heap.pageSize()), @returnAddress());
        }
        t.joinable_worker_count = 0;
    }
}

// All four submission paths call this while holding t.mutex. A worker cannot
// leave the pool before join_requested, so the finite busy limits bound the
// permanent handle array as well as simultaneously running callbacks.
const OwnedWorker = struct { thread: std.Thread, mapping: ?[]u8 = null };

/// Exact page mapping including guards at both ends; no unchecked rounding.
pub fn workerStackMappingSize(stack_size: usize) error{OutOfMemory}!usize {
    const page = std.heap.pageSize();
    const rounded = (std.math.add(usize, @max(stack_size, 16 * 1024), page - 1) catch return error.OutOfMemory) & ~(page - 1);
    return std.math.add(usize, rounded, std.math.mul(usize, page, 2) catch return error.OutOfMemory) catch return error.OutOfMemory;
}

fn spawnWorker(t: *Threaded) std.Thread.SpawnError!OwnedWorker {
    const allocator = t.worker_stack_allocator orelse return .{
        .thread = try std.Thread.spawn(.{ .stack_size = t.stack_size }, worker, .{t}),
    };
    if (comptime !std.Thread.use_pthreads) return error.SystemResources;
    const page = std.heap.pageSize();
    const length = try workerStackMappingSize(t.stack_size);
    const alignment: std.mem.Alignment = .fromByteUnits(page);
    const mapping = (allocator.rawAlloc(length, alignment, @returnAddress()) orelse return error.OutOfMemory)[0..length];
    errdefer allocator.rawFree(mapping, alignment, @returnAddress());
    if (std.c.mprotect(@ptrCast(@alignCast(mapping.ptr)), page, .{}) != 0 or
        std.c.mprotect(@ptrCast(@alignCast(mapping.ptr + length - page)), page, .{}) != 0)
        return error.SystemResources;
    const stack = mapping[page .. length - page];
    return .{
        .thread = try std.Thread.spawn(.{ .stack_size = stack.len, .caller_stack = stack, .allocator = t.allocator }, worker, .{t}),
        .mapping = mapping,
    };
}

fn retainWorker(t: *Threaded, thread: OwnedWorker) void {
    if (!t.joinable_workers) return thread.thread.detach();
    if (t.joinable_worker_count == t.joinable_worker_handles.len)
        @panic("joinable worker handle capacity exceeded");
    t.joinable_worker_handles[t.joinable_worker_count] = thread;
    t.joinable_worker_count += 1;
}

fn worker(t: *Threaded) void {""",
    )
    return source


def patch_thread(source: str) -> str:
    source = replace_once(
        source,
        "    allocator: ?std.mem.Allocator = null,\n",
        """    allocator: ?std.mem.Allocator = null,
    /// Borrowed caller-owned pthread stack, retained until join. The caller
    /// supplies guard pages and may reclaim it only after actual thread exit.
    caller_stack: ?[]u8 = null,
""",
    )
    source = replace_once(
        source,
        """        const Args = @TypeOf(args);
        const allocator = std.heap.c_allocator;

        const Instance = struct {
            fn entryFn(raw_arg: ?*anyopaque) callconv(.c) ?*anyopaque {
                const args_ptr: *Args = @ptrCast(@alignCast(raw_arg));
                defer allocator.destroy(args_ptr);
                return callFn(f, args_ptr.*);
            }
        };

        const args_ptr = try allocator.create(Args);
        args_ptr.* = args;
""",
        """        const Args = @TypeOf(args);
        const allocator = if (config.caller_stack != null) config.allocator orelse std.heap.c_allocator else std.heap.c_allocator;

        const Instance = struct {
            allocator: std.mem.Allocator,
            args: Args,
            fn entryFn(raw_arg: ?*anyopaque) callconv(.c) ?*anyopaque {
                const args_ptr: *@This() = @ptrCast(@alignCast(raw_arg));
                defer args_ptr.allocator.destroy(args_ptr);
                return callFn(f, args_ptr.args);
            }
        };

        const args_ptr = try allocator.create(Instance);
        args_ptr.* = .{ .allocator = allocator, .args = args };
""",
    )
    source = replace_once(
        source,
        """        const stack_size = @max(config.stack_size, 16 * 1024);
        assert(c.pthread_attr_setstacksize(&attr, stack_size) == .SUCCESS);
        assert(c.pthread_attr_setguardsize(&attr, std.heap.pageSize()) == .SUCCESS);
""",
        """        if (config.caller_stack) |stack| {
            if (c.pthread_attr_setstack(&attr, @ptrCast(stack.ptr), stack.len) != .SUCCESS) return error.SystemResources;
            if (c.pthread_attr_setguardsize(&attr, 0) != .SUCCESS) return error.SystemResources;
        } else {
            const stack_size = @max(config.stack_size, 16 * 1024);
            assert(c.pthread_attr_setstacksize(&attr, stack_size) == .SUCCESS);
            assert(c.pthread_attr_setguardsize(&attr, std.heap.pageSize()) == .SUCCESS);
        }
""",
    )
    source = replace_once(
        source,
        """            .PERM => unreachable,
            .INVAL => unreachable,
            else => |err| return posix.unexpectedErrno(err),
""",
        """            .PERM => unreachable,
            .INVAL => {
                // Caller stacks can be too small for the linked image's TLS.
                if (config.caller_stack != null) return error.SystemResources;
                unreachable;
            },
            else => |err| return posix.unexpectedErrno(err),
""",
    )
    return source


def prepare(
    lib_dir: Path, threaded_source: Path, thread_source: Path, output: Path
) -> None:
    source = threaded_source.read_text()
    patched = patch_threaded(source)
    thread = thread_source.read_text()
    patched_thread = patch_thread(thread)
    output.mkdir(parents=True, exist_ok=True)
    # Copy std so relative imports cannot resolve through a symlink back to the
    # unpatched installation. Other compiler/libc resources remain read-only.
    shutil.copytree(lib_dir / "std", output / "std", dirs_exist_ok=True)
    for path in lib_dir.iterdir():
        if path.name == "std":
            continue
        destination = output / path.name
        if destination.exists() or destination.is_symlink():
            continue
        try:
            destination.symlink_to(path.resolve(), target_is_directory=path.is_dir())
        except OSError:
            # Ordinary Windows accounts may lack symlink privileges. A copy
            # preserves the same immutable installed-toolchain contract.
            if path.is_dir():
                shutil.copytree(path, destination)
            else:
                shutil.copy2(path, destination)
    (output / "std/Io/Threaded.zig").write_text(patched)
    (output / "std/Thread.zig").write_text(patched_thread)
    (output / "antfly-joinable-threaded.json").write_text(
        json.dumps(
            {
                "original_sha256": hashlib.sha256(source.encode()).hexdigest(),
                "patched_sha256": hashlib.sha256(patched.encode()).hexdigest(),
                "original_thread_sha256": hashlib.sha256(thread.encode()).hexdigest(),
                "patched_thread_sha256": hashlib.sha256(
                    patched_thread.encode()
                ).hexdigest(),
                "generator_sha256": hashlib.sha256(
                    Path(__file__).read_bytes()
                ).hexdigest(),
                "opt_in_handle_limit": 256,
                "default_join_policy_changed": False,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    prepare(*(Path(arg) for arg in sys.argv[1:]))
