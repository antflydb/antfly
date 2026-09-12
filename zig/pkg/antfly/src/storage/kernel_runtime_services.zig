// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Optional process-local services for embedded storage owners. These use
//! the existing allocator and same-toolchain executor bridges. Provider
//! implementations retain copies, never pointers to this request.
const abi = @import("kernel_owner_abi");
pub const memory = @import("runtime_memory_abi");
pub const executor = @import("../runtime_io_abi.zig");

pub const abi_version: u32 = 1;
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
};

pub extern fn antfly_storage_context_create_with_runtime(
    request: *const Request,
    out_context: *?*anyopaque,
) abi.Status;
