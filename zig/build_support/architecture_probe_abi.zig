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

//! Private experimental operation boundaries, never a public product ABI.
//! Clients compile this contract without importing either implementation.
pub const failure = @import("runtime_failure_abi");
pub const version: u32 = 1;
pub const Bytes = extern struct { ptr: ?[*]const u8 = null, len: u64 = 0 };
pub const Input = extern struct { ptr: ?[*]const f32 = null, len: u64 = 0 };
pub const Output = extern struct { ptr: ?[*]f32 = null, len: u64 = 0 };
pub const IndexOpen = extern struct { abi_version: u32 = version, name: Bytes, config: Bytes };
pub const Tensor = extern struct {
    abi_version: u32 = version,
    operation: enum(u32) { rope = 1, qkv_rope = 2, geglu = 3, geglu_backward = 4, _ },
    output: Output,
    input: Input,
    cos: Input = .{},
    sin: Input = .{},
    gradient: Input = .{},
    width: u64,
    head_dim: u64 = 0,
    inverse: u32 = 0,
};
pub extern fn antfly_probe_index_open(*const IndexOpen, *?*anyopaque, *failure.FailureIdentity) callconv(.c) failure.Status;
pub extern fn antfly_probe_index_close(?*anyopaque) callconv(.c) void;
pub extern fn antfly_probe_fused_cpu(*const Tensor, *failure.FailureIdentity) callconv(.c) failure.Status;
