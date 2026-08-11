// Copyright 2026 Antfly, Inc.
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

//! Narrow link boundary between the top-level CLI and separately code-generated
//! server runtimes. The pointers are intentionally opaque at the C ABI; both
//! sides are built together with the same Zig toolchain and recover the concrete
//! std.process types internally.

pub const Context = extern struct {
    init: *const anyopaque,
    args: *anyopaque,
    command_ptr: [*]const u8,
    command_len: usize,
};

pub const BorrowedBytes = extern struct {
    ptr: ?[*]const u8 = null,
    len: usize = 0,

    pub fn fromSlice(value: []const u8) BorrowedBytes {
        return .{ .ptr = value.ptr, .len = value.len };
    }

    pub fn slice(self: BorrowedBytes) []const u8 {
        if (self.len == 0) return "";
        return self.ptr.?[0..self.len];
    }
};

/// Reverse link used only when physical Lite administration is compiled in
/// the storage kernel but `lite serve` must enter the distributed standalone
/// composition unit. Every slice is borrowed for the duration of the call.
pub const LiteServeContext = extern struct {
    init: *const anyopaque,
    path: BorrowedBytes,
    host: BorrowedBytes,
    extra_args: ?[*]const BorrowedBytes = null,
    extra_args_len: usize = 0,
    port: u16,
    fsync: u8,
    _reserved0: [5]u8 = @splat(0),
};
