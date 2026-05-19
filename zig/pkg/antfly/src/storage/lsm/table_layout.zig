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

const std = @import("std");

pub const Usage = enum {
    general,
    secondary_index,
};

pub const Config = struct {
    block_size: usize,
    header_size: usize,
    key_size: usize,
    value_size: usize,
    value_count_max: usize,
    cache_line_size: usize = 64,
    usage: Usage = .general,
};

pub const Layout = struct {
    usage: Usage,
    block_size: usize,
    header_size: usize,
    block_body_size: usize,
    key_size: usize,
    value_size: usize,
    value_count_max: usize,
    block_value_count_max: usize,
    index_block_count: usize,
    value_block_count_max: usize,
    block_count_max: usize,
};

pub const Error = error{
    InvalidBlockSize,
    InvalidHeaderSize,
    InvalidKeySize,
    InvalidValueSize,
    InvalidValueCount,
    InvalidCacheLineSize,
    EmptyBlockBody,
    ValueDoesNotFitBlock,
    CacheLineMisalignedValues,
};

pub fn calculate(config: Config) Error!Layout {
    if (config.block_size == 0) return Error.InvalidBlockSize;
    if (config.header_size >= config.block_size) return Error.InvalidHeaderSize;
    if (config.key_size == 0) return Error.InvalidKeySize;
    if (config.value_size == 0) return Error.InvalidValueSize;
    if (config.value_count_max == 0) return Error.InvalidValueCount;
    if (config.cache_line_size == 0) return Error.InvalidCacheLineSize;

    const block_body_size = config.block_size - config.header_size;
    if (block_body_size == 0) return Error.EmptyBlockBody;
    if (block_body_size < config.value_size) return Error.ValueDoesNotFitBlock;

    const block_value_count_max = @divFloor(block_body_size, config.value_size);
    if (block_value_count_max == 0) return Error.ValueDoesNotFitBlock;
    if ((block_value_count_max * config.value_size) % config.cache_line_size != 0) {
        return Error.CacheLineMisalignedValues;
    }

    const value_block_count_max = std.math.divCeil(usize, config.value_count_max, block_value_count_max) catch unreachable;
    const index_block_count = 1;

    return .{
        .usage = config.usage,
        .block_size = config.block_size,
        .header_size = config.header_size,
        .block_body_size = block_body_size,
        .key_size = config.key_size,
        .value_size = config.value_size,
        .value_count_max = config.value_count_max,
        .block_value_count_max = block_value_count_max,
        .index_block_count = index_block_count,
        .value_block_count_max = value_block_count_max,
        .block_count_max = index_block_count + value_block_count_max,
    };
}

test "table layout computes compact geometry" {
    const layout = try calculate(.{
        .block_size = 4096,
        .header_size = 128,
        .key_size = 16,
        .value_size = 128,
        .value_count_max = 1000,
        .usage = .secondary_index,
    });

    try std.testing.expectEqual(Usage.secondary_index, layout.usage);
    try std.testing.expectEqual(@as(usize, 3968), layout.block_body_size);
    try std.testing.expectEqual(@as(usize, 31), layout.block_value_count_max);
    try std.testing.expectEqual(@as(usize, 33), layout.value_block_count_max);
    try std.testing.expectEqual(@as(usize, 34), layout.block_count_max);
}

test "table layout rejects values that do not fit block geometry" {
    try std.testing.expectError(Error.ValueDoesNotFitBlock, calculate(.{
        .block_size = 256,
        .header_size = 128,
        .key_size = 8,
        .value_size = 200,
        .value_count_max = 1,
    }));
}

test "table layout enforces cache-line aligned value packing" {
    try std.testing.expectError(Error.CacheLineMisalignedValues, calculate(.{
        .block_size = 4096,
        .header_size = 128,
        .key_size = 8,
        .value_size = 96,
        .value_count_max = 64,
    }));
}
