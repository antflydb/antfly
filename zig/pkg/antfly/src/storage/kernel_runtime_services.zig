// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Optional process-local services for embedded storage owners. These use
//! the existing allocator and same-toolchain executor bridges. Provider
//! implementations retain copies, never pointers to this request.
const std = @import("std");
const abi = @import("kernel_owner_abi");
pub const memory = @import("runtime_memory_abi");
pub const executor = @import("antfly_runtime_abi").io_abi;

pub const abi_version: u32 = 4;
pub const Request = extern struct {
    version: u32 = abi_version,
    _reserved: u32 = 0,
    context: abi.ContextRequest = .{},
    /// Explicit process budget. Zero with borrowed I/O uses fixed defaults;
    /// it must not probe host RAM during deterministic execution.
    memory_limit_bytes: u64 = 0,
    /// The allocator's callback context must outlive the storage context and
    /// every borrowed owner. The callback table itself is copied.
    allocator: ?*const memory.Allocator = null,
    /// A borrowed executor supplies storage, clock, scheduling, and
    /// cancellation together. Its runtime must outlive context destruction.
    io: ?*const executor.Borrow = null,
    /// Copied and retained at runtime birth, released after physical joins.
    /// The request and this port value may disappear after creation returns.
    runnable_owner: ?*const @import("antfly_runtime_abi").runnable_abi.Owner = null,
    /// Optional common process root. Its callback context outlives this storage
    /// context, including bootstrap allocation and all borrowed child owners.
    /// The table is copied before construction; this request may disappear.
    memory_parent: ?*const memory.Parent = null,

    /// Shared node execution; the offer table is copied and retained. The
    /// producer remains alive through storage's final physical destruction.
    execution_services: ?*const @import("antfly_local_sources").storage_execution_services.Offer = null,

    pub fn readMemoryParent(self: *const Request) !?memory.Parent {
        // Check the old request prefix before touching the appended field.
        if (self.version != abi_version or self._reserved != 0 or self.context.version != abi.abi_version)
            return error.InvalidAbiVersion;
        const value = self.memory_parent orelse return null;
        const Header = extern struct { version: u32, struct_size: u32 };
        const header: *const Header = @ptrCast(value);
        if (header.version != memory.Parent.abi_version or header.struct_size != @sizeOf(memory.Parent))
            return error.InvalidArgument;
        return value.*;
    }
};

/// Stable for the synchronous create call. Providers copy every offered table;
/// no pointer into this request builder survives context construction.
pub const RuntimeRequest = struct {
    offer: @import("antfly_local_sources").storage_execution_services.Offer,
    parent: ?memory.Parent,
    owner: ?@import("antfly_runtime_abi").runnable_abi.Owner,
    options: Options,
    pub const Options = struct {
        context: abi.ContextRequest = .{},
        memory_limit_bytes: u64 = 0,
        memory_parent: ?memory.Parent = null,
        allocator: ?memory.Allocator = null,
    };
    pub fn init(runtime: *@import("antfly_local_sources").storage_background_runtime.BackendRuntime, options: Options) !RuntimeRequest {
        const parent = options.memory_parent orelse runtime.memoryParent();
        if (parent) |value| if (!value.valid()) return error.InvalidArgument;
        var resolved = options;
        if (resolved.memory_limit_bytes == 0) if (parent) |value| {
            resolved.memory_limit_bytes = value.snapshot().hard_limit_bytes;
        };
        return .{
            .offer = .init(runtime),
            .parent = parent,
            .owner = runtime.runnableOwner(),
            .options = resolved,
        };
    }
    pub fn request(self: *const RuntimeRequest) Request {
        return .{
            .context = self.options.context,
            .memory_limit_bytes = self.options.memory_limit_bytes,
            .memory_parent = if (self.parent) |*parent| parent else null,
            .allocator = if (self.options.allocator) |*allocator| allocator else null,
            .runnable_owner = if (self.owner) |*owner| owner else null,
            .execution_services = &self.offer,
        };
    }
};

/// Context headers have no child manager yet. Hold their outer-root charge
/// across physical allocation/destruction, separately from resource children.
pub const BootstrapAllocation = struct {
    pub fn allocate(comptime T: type, allocator: std.mem.Allocator, parent: ?memory.Parent) !*T {
        if (parent) |value| {
            if (!value.valid()) return error.InvalidArgument;
            if (!value.reserve_fn(value.ptr, @sizeOf(T))) return error.ResourceBudgetExceeded;
        }
        errdefer if (parent) |value| value.release_fn(value.ptr, @sizeOf(T));
        return try allocator.create(T);
    }
    pub fn destroy(comptime T: type, allocator: std.mem.Allocator, parent: ?memory.Parent, value: *T) void {
        allocator.destroy(value);
        if (parent) |owner| owner.release_fn(owner.ptr, @sizeOf(T));
    }
};

pub extern fn antfly_storage_context_create_with_runtime(
    request: *const Request,
    out_context: *?*anyopaque,
) abi.Status;
