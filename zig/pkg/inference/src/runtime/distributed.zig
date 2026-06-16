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
const platform = @import("antfly_platform");

pub const Mode = enum {
    none,
    data_parallel,
    tensor_parallel,
};

pub const Backend = enum {
    ring,
    mpi,
};

pub const Config = struct {
    enabled: bool = false,
    mode: Mode = .none,
    backend: Backend = .ring,
    rank: usize = 0,
    world_size: usize = 1,
    local_rank: usize = 0,

    pub fn isPrimary(self: @This()) bool {
        return self.rank == 0;
    }

    pub fn isTensorParallel(self: @This()) bool {
        return self.enabled and self.mode == .tensor_parallel and self.world_size > 1;
    }

    pub fn isDataParallel(self: @This()) bool {
        return self.enabled and self.mode == .data_parallel and self.world_size > 1;
    }
};

pub fn configFromEnv() Config {
    const enabled = envVarBool("TERMITE_DISTRIBUTED_ENABLE");
    const mode = envVarMode("TERMITE_DISTRIBUTED_MODE") orelse .none;
    const backend = envVarBackend("TERMITE_DISTRIBUTED_BACKEND") orelse .ring;
    const rank = envVarUnsigned("TERMITE_DISTRIBUTED_RANK") orelse 0;
    const world_size = envVarUnsigned("TERMITE_DISTRIBUTED_WORLD_SIZE") orelse 1;
    const local_rank = envVarUnsigned("TERMITE_DISTRIBUTED_LOCAL_RANK") orelse rank;
    return .{
        .enabled = enabled and world_size > 1 and mode != .none,
        .mode = if (enabled) mode else .none,
        .backend = backend,
        .rank = rank,
        .world_size = world_size,
        .local_rank = local_rank,
    };
}

fn envVarUnsigned(name: [:0]const u8) ?usize {
    return platform.env.getenvUsize(name.ptr);
}

fn envVarBool(name: [:0]const u8) bool {
    return platform.env.getenvBool(name.ptr);
}

fn envVarMode(name: [:0]const u8) ?Mode {
    const slice = platform.env.getenvSlice(name) orelse return null;
    if (slice.len == 0) return null;
    if (std.ascii.eqlIgnoreCase(slice, "data_parallel") or std.ascii.eqlIgnoreCase(slice, "dp")) return .data_parallel;
    if (std.ascii.eqlIgnoreCase(slice, "tensor_parallel") or std.ascii.eqlIgnoreCase(slice, "tp")) return .tensor_parallel;
    if (std.ascii.eqlIgnoreCase(slice, "none") or std.ascii.eqlIgnoreCase(slice, "off")) return .none;
    return null;
}

fn envVarBackend(name: [:0]const u8) ?Backend {
    const slice = platform.env.getenvSlice(name) orelse return null;
    if (slice.len == 0) return null;
    if (std.ascii.eqlIgnoreCase(slice, "ring")) return .ring;
    if (std.ascii.eqlIgnoreCase(slice, "mpi")) return .mpi;
    return null;
}

test "distributed config helpers reflect active modes" {
    const cfg = Config{ .enabled = true, .mode = .tensor_parallel, .world_size = 2, .rank = 1, .local_rank = 1 };
    try std.testing.expect(!cfg.isPrimary());
    try std.testing.expect(cfg.isTensorParallel());
    try std.testing.expect(!cfg.isDataParallel());
}
