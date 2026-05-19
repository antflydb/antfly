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
const arithmetic = @import("arithmetic.zig");
const tile = @import("tile.zig");

pub const native_port_available = true;

const lut_ctxno_sc = [256]u8{
    0x9, 0x9, 0xa, 0xa, 0x9, 0x9, 0xa, 0xa, 0xc, 0xc, 0xd, 0xb, 0xc, 0xc, 0xd, 0xb,
    0x9, 0x9, 0xa, 0xa, 0x9, 0x9, 0xa, 0xa, 0xc, 0xc, 0xb, 0xd, 0xc, 0xc, 0xb, 0xd,
    0xc, 0xc, 0xd, 0xd, 0xc, 0xc, 0xb, 0xb, 0xc, 0x9, 0xd, 0xa, 0x9, 0xc, 0xa, 0xb,
    0xc, 0xc, 0xb, 0xb, 0xc, 0xc, 0xd, 0xd, 0xc, 0x9, 0xb, 0xa, 0x9, 0xc, 0xa, 0xd,
    0x9, 0x9, 0xa, 0xa, 0x9, 0x9, 0xa, 0xa, 0xc, 0xc, 0xd, 0xb, 0xc, 0xc, 0xd, 0xb,
    0x9, 0x9, 0xa, 0xa, 0x9, 0x9, 0xa, 0xa, 0xc, 0xc, 0xb, 0xd, 0xc, 0xc, 0xb, 0xd,
    0xc, 0xc, 0xd, 0xd, 0xc, 0xc, 0xb, 0xb, 0xc, 0x9, 0xd, 0xa, 0x9, 0xc, 0xa, 0xb,
    0xc, 0xc, 0xb, 0xb, 0xc, 0xc, 0xd, 0xd, 0xc, 0x9, 0xb, 0xa, 0x9, 0xc, 0xa, 0xd,
    0xa, 0xa, 0xa, 0xa, 0xa, 0xa, 0xa, 0xa, 0xd, 0xb, 0xd, 0xb, 0xd, 0xb, 0xd, 0xb,
    0xa, 0xa, 0x9, 0x9, 0xa, 0xa, 0x9, 0x9, 0xd, 0xb, 0xc, 0xc, 0xd, 0xb, 0xc, 0xc,
    0xd, 0xd, 0xd, 0xd, 0xb, 0xb, 0xb, 0xb, 0xd, 0xa, 0xd, 0xa, 0xa, 0xb, 0xa, 0xb,
    0xd, 0xd, 0xc, 0xc, 0xb, 0xb, 0xc, 0xc, 0xd, 0xa, 0xc, 0x9, 0xa, 0xb, 0x9, 0xc,
    0xa, 0xa, 0x9, 0x9, 0xa, 0xa, 0x9, 0x9, 0xb, 0xd, 0xc, 0xc, 0xb, 0xd, 0xc, 0xc,
    0xa, 0xa, 0xa, 0xa, 0xa, 0xa, 0xa, 0xa, 0xb, 0xd, 0xb, 0xd, 0xb, 0xd, 0xb, 0xd,
    0xb, 0xb, 0xc, 0xc, 0xd, 0xd, 0xc, 0xc, 0xb, 0xa, 0xc, 0x9, 0xa, 0xd, 0x9, 0xc,
    0xb, 0xb, 0xb, 0xb, 0xd, 0xd, 0xd, 0xd, 0xb, 0xa, 0xb, 0xa, 0xa, 0xd, 0xa, 0xd,
};

const lut_spb = [256]u1{
    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1,
    0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1, 1,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 1, 1, 0, 0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1,
    0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1, 1,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1,
    0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 1, 0, 1,
    1, 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1,
    0, 0, 0, 0, 1, 1, 1, 1, 0, 1, 0, 0, 1, 1, 0, 1, 0, 0, 0, 0, 1, 1, 1, 1, 0, 1, 0, 1, 1, 1, 1, 1,
};

pub const lut_ctxno_zc = [2048]u8{
    0, 1, 3, 3, 1, 2, 3, 3, 5, 6, 7, 7, 6, 6, 7, 7, 0, 1, 3, 3, 1, 2, 3, 3, 5, 6, 7, 7, 6, 6, 7, 7,
    5, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 5, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    1, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7, 1, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7,
    6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    1, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7, 1, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7,
    6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    2, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7, 2, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7,
    6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    0, 1, 5, 6, 1, 2, 6, 6, 3, 3, 7, 7, 3, 3, 7, 7, 0, 1, 5, 6, 1, 2, 6, 6, 3, 3, 7, 7, 3, 3, 7, 7,
    3, 3, 7, 7, 3, 3, 7, 7, 4, 4, 7, 7, 4, 4, 7, 7, 3, 3, 7, 7, 3, 3, 7, 7, 4, 4, 7, 7, 4, 4, 7, 7,
    1, 2, 6, 6, 2, 2, 6, 6, 3, 3, 7, 7, 3, 3, 7, 7, 1, 2, 6, 6, 2, 2, 6, 6, 3, 3, 7, 7, 3, 3, 7, 7,
    3, 3, 7, 7, 3, 3, 7, 7, 4, 4, 7, 7, 4, 4, 7, 7, 3, 3, 7, 7, 3, 3, 7, 7, 4, 4, 7, 7, 4, 4, 7, 7,
    5, 6, 8, 8, 6, 6, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 5, 6, 8, 8, 6, 6, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8,
    7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8,
    6, 6, 8, 8, 6, 6, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 6, 6, 8, 8, 6, 6, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8,
    7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8,
    1, 2, 6, 6, 2, 2, 6, 6, 3, 3, 7, 7, 3, 3, 7, 7, 1, 2, 6, 6, 2, 2, 6, 6, 3, 3, 7, 7, 3, 3, 7, 7,
    3, 3, 7, 7, 3, 3, 7, 7, 4, 4, 7, 7, 4, 4, 7, 7, 3, 3, 7, 7, 3, 3, 7, 7, 4, 4, 7, 7, 4, 4, 7, 7,
    2, 2, 6, 6, 2, 2, 6, 6, 3, 3, 7, 7, 3, 3, 7, 7, 2, 2, 6, 6, 2, 2, 6, 6, 3, 3, 7, 7, 3, 3, 7, 7,
    3, 3, 7, 7, 3, 3, 7, 7, 4, 4, 7, 7, 4, 4, 7, 7, 3, 3, 7, 7, 3, 3, 7, 7, 4, 4, 7, 7, 4, 4, 7, 7,
    6, 6, 8, 8, 6, 6, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 6, 6, 8, 8, 6, 6, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8,
    7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8,
    6, 6, 8, 8, 6, 6, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 6, 6, 8, 8, 6, 6, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8,
    7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8, 7, 7, 8, 8,
    0, 1, 3, 3, 1, 2, 3, 3, 5, 6, 7, 7, 6, 6, 7, 7, 0, 1, 3, 3, 1, 2, 3, 3, 5, 6, 7, 7, 6, 6, 7, 7,
    5, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 5, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    1, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7, 1, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7,
    6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    1, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7, 1, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7,
    6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    2, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7, 2, 2, 3, 3, 2, 2, 3, 3, 6, 6, 7, 7, 6, 6, 7, 7,
    6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 6, 6, 7, 7, 6, 6, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7, 3, 3, 4, 4, 3, 3, 4, 4, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7, 7, 8, 8, 8, 8, 8, 8, 8, 8,
    0, 3, 1, 4, 3, 6, 4, 7, 1, 4, 2, 5, 4, 7, 5, 7, 0, 3, 1, 4, 3, 6, 4, 7, 1, 4, 2, 5, 4, 7, 5, 7,
    1, 4, 2, 5, 4, 7, 5, 7, 2, 5, 2, 5, 5, 7, 5, 7, 1, 4, 2, 5, 4, 7, 5, 7, 2, 5, 2, 5, 5, 7, 5, 7,
    3, 6, 4, 7, 6, 8, 7, 8, 4, 7, 5, 7, 7, 8, 7, 8, 3, 6, 4, 7, 6, 8, 7, 8, 4, 7, 5, 7, 7, 8, 7, 8,
    4, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8, 4, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8,
    1, 4, 2, 5, 4, 7, 5, 7, 2, 5, 2, 5, 5, 7, 5, 7, 1, 4, 2, 5, 4, 7, 5, 7, 2, 5, 2, 5, 5, 7, 5, 7,
    2, 5, 2, 5, 5, 7, 5, 7, 2, 5, 2, 5, 5, 7, 5, 7, 2, 5, 2, 5, 5, 7, 5, 7, 2, 5, 2, 5, 5, 7, 5, 7,
    4, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8, 4, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8,
    5, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8,
    3, 6, 4, 7, 6, 8, 7, 8, 4, 7, 5, 7, 7, 8, 7, 8, 3, 6, 4, 7, 6, 8, 7, 8, 4, 7, 5, 7, 7, 8, 7, 8,
    4, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8, 4, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8,
    6, 8, 7, 8, 8, 8, 8, 8, 7, 8, 7, 8, 8, 8, 8, 8, 6, 8, 7, 8, 8, 8, 8, 8, 7, 8, 7, 8, 8, 8, 8, 8,
    7, 8, 7, 8, 8, 8, 8, 8, 7, 8, 7, 8, 8, 8, 8, 8, 7, 8, 7, 8, 8, 8, 8, 8, 7, 8, 7, 8, 8, 8, 8, 8,
    4, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8, 4, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8,
    5, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8, 5, 7, 5, 7, 7, 8, 7, 8,
    7, 8, 7, 8, 8, 8, 8, 8, 7, 8, 7, 8, 8, 8, 8, 8, 7, 8, 7, 8, 8, 8, 8, 8, 7, 8, 7, 8, 8, 8, 8, 8,
    7, 8, 7, 8, 8, 8, 8, 8, 7, 8, 7, 8, 8, 8, 8, 8, 7, 8, 7, 8, 8, 8, 8, 8, 7, 8, 7, 8, 8, 8, 8, 8,
};

// Code-block coding-style bits (COD/COC Scod) exposed as a packed bool
// struct for decode-time lookup. BYPASS (bypass), RESET (reset_context),
// TERMALL (termination), VSC (vertically_causal), PTERM
// (predictable_termination), and SEGSYM (segmentation_symbol) are honored or
// accepted by decode-time lookup. PTERM is accepted via the raw Scod byte but
// not independently verified on decode (MVP).
pub const CodeBlockStyle = packed struct(u8) {
    bypass: bool = false,
    reset_context: bool = false,
    termination: bool = false,
    vertically_causal: bool = false,
    predictable_termination: bool = false,
    segmentation_symbol: bool = false,
    _reserved_bit6: bool = false,
    _reserved_bit7: bool = false,

    pub fn fromByte(byte: u8) CodeBlockStyle {
        return @bitCast(byte);
    }

    pub const NONE: CodeBlockStyle = .{};
};

pub const PassKind = enum {
    cleanup,
    significance,
    refinement,
};

pub const CodingPass = struct {
    pass_index: u16,
    kind: PassKind,
    bitplane: i16,
};

pub const ContributionPassPlan = struct {
    component_index: u16,
    subband: tile.SubbandType,
    zero_bit_planes: u8,
    start_pass_index: u16,
    num_passes: u16,
    passes: []CodingPass,

    pub fn deinit(self: *ContributionPassPlan, allocator: std.mem.Allocator) void {
        allocator.free(self.passes);
        self.* = undefined;
    }
};

pub const MqExecutionTrace = struct {
    symbol_count: usize,
    preview: [16]u8,
};

pub const PassSnapshot = struct {
    pass_index: u16,
    kind: PassKind,
    bitplane: i16,
    mq_symbol_count: usize,
    magnitudes: [4]i32,
    signs: [4]u8,
    significant: [4]u8,
};

pub const FirstSignificanceEvent = struct {
    x: u16,
    y: u16,
    pass_index: u16,
    kind: PassKind,
    bitplane: i16,
    zero_ctx_index: u8,
    sign_lut_index: u8,
    significant_symbol: u1,
    sign_symbol: u1,
    negative: bool,
};

pub const RefinementEvent = struct {
    x: u16,
    y: u16,
    pass_index: u16,
    bitplane: i16,
    context_index: u8,
    context_state_before: u8,
    context_state_after: u8,
    bit: u1,
    magnitude_before: i32,
    magnitude_after: i32,
};

pub const MqEvent = struct {
    symbol_index: usize,
    context_state_before: u8,
    context_state_after: u8,
    a_before: u32,
    c_before: u32,
    ct_before: u8,
    bp_before: usize,
    a_after: u32,
    c_after: u32,
    ct_after: u8,
    bp_after: usize,
    symbol: u1,
};

pub const ContributionPassTrace = struct {
    mq: MqExecutionTrace,
    snapshots: []PassSnapshot,
    first_significance_events: []FirstSignificanceEvent,
    refinement_events: []RefinementEvent,
    mq_events: []MqEvent,

    pub fn deinit(self: *ContributionPassTrace, allocator: std.mem.Allocator) void {
        allocator.free(self.mq_events);
        allocator.free(self.refinement_events);
        allocator.free(self.first_significance_events);
        allocator.free(self.snapshots);
        self.* = undefined;
    }
};

pub const SignPolicy = enum {
    standard,
    no_neighbor_positive,
    decomposed_single_component_split,
    rgb_component0_no_neighbor_positive,
    rgb_first_only_component0_positive,
    rgb_component1_positive_on_west_negative_case,
};

pub const RefinementPolicy = enum {
    standard_additive,
    midpoint_signed,
    openjpeg_midpoint_signed,
    exact_bitplane,
    signed_delta,
    skip_first_delta,
};

pub const MagnitudePolicy = enum {
    midpoint,
    openjpeg_midpoint,
    exact_bitplane,
};

pub const ContextInitPolicy = enum {
    standard,
    single_component_zc0_ctx5,
    single_component_full_zc_bank,
    decomposed_single_component_relaxed_zc0,
    decomposed_single_component_relaxed_zc0_pair,
};

pub const CoefficientFlags = packed struct(u8) {
    significant: bool = false,
    sign: bool = false,
    visited: bool = false,
    refined: bool = false,
    _padding: u4 = 0,
};

pub const CoefficientCell = struct {
    magnitude: i32 = 0,
    flags: CoefficientFlags = .{},
};

pub const NeighborSigns = struct {
    positive: u8,
    negative: u8,
};

const SignContribution = struct {
    horizontal: i2,
    vertical: i2,
};

pub const CoefficientGrid = struct {
    allocator: std.mem.Allocator,
    width: usize,
    height: usize,
    cells: []CoefficientCell,

    pub fn init(allocator: std.mem.Allocator, width: usize, height: usize) !CoefficientGrid {
        if (width == 0 or height == 0) return error.InvalidCodeblockShape;
        return .{
            .allocator = allocator,
            .width = width,
            .height = height,
            .cells = try allocator.alloc(CoefficientCell, width * height),
        };
    }

    pub fn deinit(self: *CoefficientGrid) void {
        self.allocator.free(self.cells);
        self.* = undefined;
    }

    pub fn clear(self: *CoefficientGrid) void {
        @memset(self.cells, .{});
    }

    pub fn index(self: *const CoefficientGrid, x: usize, y: usize) usize {
        return y * self.width + x;
    }

    pub fn at(self: *CoefficientGrid, x: usize, y: usize) *CoefficientCell {
        return &self.cells[self.index(x, y)];
    }

    pub fn markSignificant(self: *CoefficientGrid, x: usize, y: usize, negative: bool, bitplane: i16, magnitude_policy: MagnitudePolicy) void {
        const cell = self.at(x, y);
        cell.flags.significant = true;
        cell.flags.sign = negative;
        cell.flags.visited = true;
        cell.flags.refined = false;
        cell.magnitude = switch (magnitude_policy) {
            .openjpeg_midpoint => if (bitplane <= 0)
                3
            else
                (@as(i32, 1) << @intCast(bitplane + 1)) | (@as(i32, 1) << @intCast(bitplane)),
            .midpoint => blk: {
                if (bitplane <= 0) break :blk 1;
                const one = @as(i32, 1) << @intCast(bitplane);
                break :blk one + (@as(i32, 1) << @intCast(bitplane - 1));
            },
            .exact_bitplane => if (bitplane <= 0) 1 else @as(i32, 1) << @intCast(bitplane),
        };
    }

    pub fn applyRefinement(self: *CoefficientGrid, x: usize, y: usize, bit: u1, bitplane: i16, policy: RefinementPolicy) void {
        const cell = self.at(x, y);
        const was_refined = cell.flags.refined;
        cell.flags.refined = true;
        switch (policy) {
            .exact_bitplane => {
                if (bitplane < 0) return;
                if (bit == 1) cell.magnitude += @as(i32, 1) << @intCast(bitplane);
            },
            .openjpeg_midpoint_signed => {
                if (bitplane < 0) return;
                const delta: i32 = @as(i32, 1) << @intCast(bitplane);
                if (bit == 1) {
                    cell.magnitude += delta;
                } else {
                    cell.magnitude = @max(0, cell.magnitude - delta);
                }
            },
            else => {
                if (bitplane <= 0) return;
                const delta: i32 = @as(i32, 1) << @intCast(bitplane - 1);
                switch (policy) {
                    .standard_additive => {
                        if (bit == 1) cell.magnitude += delta;
                    },
                    .midpoint_signed => {
                        if (bit == 1) {
                            cell.magnitude += delta;
                        } else {
                            cell.magnitude = @max(0, cell.magnitude - delta);
                        }
                    },
                    .signed_delta => {
                        if ((bit == 1) != cell.flags.sign) {
                            cell.magnitude += delta;
                        } else {
                            cell.magnitude = @max(0, cell.magnitude - delta);
                        }
                    },
                    .skip_first_delta => {
                        if (!was_refined) return;
                        if (bit == 1) cell.magnitude += delta;
                    },
                    .exact_bitplane => unreachable,
                    .openjpeg_midpoint_signed => unreachable,
                }
            },
        }
    }

    pub fn resetVisited(self: *CoefficientGrid) void {
        for (self.cells) |*cell| cell.flags.visited = false;
    }

    pub fn hasAnySignificant(self: *const CoefficientGrid) bool {
        for (self.cells) |cell| {
            if (cell.flags.significant) return true;
        }
        return false;
    }

    pub fn significantNeighborCount(self: *const CoefficientGrid, x: usize, y: usize) u8 {
        return self.significantNeighborCountCausal(x, y, false, 0);
    }

    pub fn significantNeighborCountCausal(self: *const CoefficientGrid, x: usize, y: usize, vertically_causal: bool, stripe_y: usize) u8 {
        var count: u8 = 0;
        const min_x = if (x == 0) 0 else x - 1;
        const max_x = @min(x + 1, self.width - 1);
        const min_y = if (y == 0) 0 else y - 1;
        const max_y = if (vertically_causal)
            @min(y + 1, @min(stripe_y + 3, self.height - 1))
        else
            @min(y + 1, self.height - 1);
        var yy = min_y;
        while (yy <= max_y) : (yy += 1) {
            var xx = min_x;
            while (xx <= max_x) : (xx += 1) {
                if (xx == x and yy == y) continue;
                if (self.cells[self.index(xx, yy)].flags.significant) count += 1;
            }
        }
        return count;
    }

    pub fn significantNeighborSigns(self: *const CoefficientGrid, x: usize, y: usize) NeighborSigns {
        var out: NeighborSigns = .{ .positive = 0, .negative = 0 };
        const min_x = if (x == 0) 0 else x - 1;
        const max_x = @min(x + 1, self.width - 1);
        const min_y = if (y == 0) 0 else y - 1;
        const max_y = @min(y + 1, self.height - 1);
        var yy = min_y;
        while (yy <= max_y) : (yy += 1) {
            var xx = min_x;
            while (xx <= max_x) : (xx += 1) {
                if (xx == x and yy == y) continue;
                const cell = self.cells[self.index(xx, yy)];
                if (!cell.flags.significant) continue;
                if (cell.flags.sign) {
                    out.negative += 1;
                } else {
                    out.positive += 1;
                }
            }
        }
        return out;
    }
};

const Tier1Contexts = struct {
    contexts: [19]arithmetic.MqContext,

    fn init(policy: ContextInitPolicy) Tier1Contexts {
        var out: Tier1Contexts = undefined;
        for (&out.contexts) |*ctx| ctx.reset(0);
        switch (policy) {
            .standard => out.contexts[0].reset(2 * 4), // ZC context 0 at state pair 4 (matches OpenJPEG)
            .single_component_zc0_ctx5 => {
                out.contexts[0].reset(4);
                out.contexts[5].reset(1);
            },
            .single_component_full_zc_bank => {
                for (out.contexts[0..9]) |*ctx| ctx.reset(4);
            },
            .decomposed_single_component_relaxed_zc0 => {
                for (out.contexts[0..9]) |*ctx| ctx.reset(4);
                out.contexts[0].reset(0);
            },
            .decomposed_single_component_relaxed_zc0_pair => {
                for (out.contexts[0..9]) |*ctx| ctx.reset(4);
                out.contexts[0].reset(0);
                out.contexts[1].reset(1);
            },
        }
        out.contexts[17].reset(2 * 3);
        out.contexts[18].reset(2 * 46);
        return out;
    }

    fn at(self: *Tier1Contexts, index: u8) *arithmetic.MqContext {
        return &self.contexts[index];
    }
};

const ScriptedSymbolSource = struct {
    symbols: []const u1,
    index: usize = 0,

    fn nextSymbol(self: *ScriptedSymbolSource, _: *arithmetic.MqContext) !u1 {
        if (self.index >= self.symbols.len) return error.EndOfSymbolStream;
        const symbol = self.symbols[self.index];
        self.index += 1;
        return symbol;
    }
};

const MqSymbolSource = struct {
    decoder: *arithmetic.MqDecoder,

    fn nextSymbol(self: *MqSymbolSource, context: *arithmetic.MqContext) !u1 {
        return self.decoder.decode(context);
    }
};

const TracingMqSymbolSource = struct {
    decoder: *arithmetic.MqDecoder,
    symbol_count: usize = 0,
    preview: [16]u8 = [_]u8{0} ** 16,
    mq_events: ?*std.ArrayListUnmanaged(MqEvent) = null,
    allocator: ?std.mem.Allocator = null,

    fn nextSymbol(self: *TracingMqSymbolSource, context: *arithmetic.MqContext) !u1 {
        const before = self.decoder.debugState();
        const context_state_before = context.state_index;
        const symbol = self.decoder.decode(context);
        const after = self.decoder.debugState();
        if (self.symbol_count < self.preview.len) {
            self.preview[self.symbol_count] = symbol;
        }
        if (self.mq_events) |events| {
            try events.append(self.allocator.?, .{
                .symbol_index = self.symbol_count,
                .context_state_before = context_state_before,
                .context_state_after = context.state_index,
                .a_before = before.a,
                .c_before = before.c,
                .ct_before = before.ct,
                .bp_before = before.bp,
                .a_after = after.a,
                .c_after = after.c,
                .ct_after = after.ct,
                .bp_after = after.bp,
                .symbol = symbol,
            });
        }
        self.symbol_count += 1;
        return symbol;
    }
};

pub fn decodeNumCodingPasses(reader: anytype) !u16 {
    if (try reader.readBit() == 0) return 1;
    if (try reader.readBit() == 0) return 2;

    const next_two = try reader.readBits(2);
    switch (next_two) {
        0 => return 3,
        1 => return 4,
        2 => return 5,
        3 => {
            const next_five = try reader.readBits(5);
            if (next_five != 31) return @as(u16, 6) + @as(u16, @intCast(next_five));
            return @as(u16, 37) + @as(u16, @intCast(try reader.readBits(7)));
        },
        else => unreachable,
    }
}

pub fn decodeCommaCode(reader: anytype) !u8 {
    var count: u8 = 0;
    while (try reader.readBit() == 1) {
        if (count == std.math.maxInt(u8)) return error.IntegerOverflow;
        count += 1;
    }
    return count;
}

pub fn buildPassSchedule(
    allocator: std.mem.Allocator,
    num_passes: u16,
    zero_bit_planes: u8,
    bits_per_component: u8,
) ![]CodingPass {
    return buildPassScheduleRange(allocator, 0, num_passes, zero_bit_planes, bits_per_component);
}

pub fn buildPassScheduleRange(
    allocator: std.mem.Allocator,
    start_pass_index: u16,
    num_passes: u16,
    zero_bit_planes: u8,
    bits_per_component: u8,
) ![]CodingPass {
    if (num_passes == 0) return allocator.alloc(CodingPass, 0);
    if (bits_per_component == 0) return error.InvalidBitplaneCount;

    const schedule = try allocator.alloc(CodingPass, num_passes);
    errdefer allocator.free(schedule);

    const first_bitplane: i16 = @as(i16, bits_per_component - 1) - @as(i16, zero_bit_planes);
    var rel_index: u16 = 0;
    while (rel_index < num_passes) : (rel_index += 1) {
        const absolute_pass_index = start_pass_index + rel_index;
        const kind: PassKind = if (absolute_pass_index == 0)
            .cleanup
        else switch (@as(u2, @intCast((absolute_pass_index - 1) % 3))) {
            0 => .significance,
            1 => .refinement,
            2 => .cleanup,
            else => unreachable,
        };
        const bitplane: i16 = if (absolute_pass_index == 0)
            first_bitplane
        else
            first_bitplane - 1 - @as(i16, @intCast((absolute_pass_index - 1) / 3));
        schedule[rel_index] = .{
            .pass_index = absolute_pass_index,
            .kind = kind,
            .bitplane = bitplane,
        };
    }
    return schedule;
}

pub fn planContributionPasses(
    allocator: std.mem.Allocator,
    component_index: u16,
    subband: tile.SubbandType,
    zero_bit_planes: u8,
    num_passes: u16,
    bits_per_component: u8,
) !ContributionPassPlan {
    return planContributionPassRange(allocator, component_index, subband, zero_bit_planes, 0, num_passes, bits_per_component);
}

pub fn planContributionPassRange(
    allocator: std.mem.Allocator,
    component_index: u16,
    subband: tile.SubbandType,
    zero_bit_planes: u8,
    start_pass_index: u16,
    num_passes: u16,
    bits_per_component: u8,
) !ContributionPassPlan {
    return .{
        .component_index = component_index,
        .subband = subband,
        .zero_bit_planes = zero_bit_planes,
        .start_pass_index = start_pass_index,
        .num_passes = num_passes,
        .passes = try buildPassScheduleRange(allocator, start_pass_index, num_passes, zero_bit_planes, bits_per_component),
    };
}

pub fn executeContributionPassPlanMq(
    allocator: std.mem.Allocator,
    grid: *CoefficientGrid,
    plan: *const ContributionPassPlan,
    body: []const u8,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    context_init_policy: ContextInitPolicy,
    code_block_style: CodeBlockStyle,
) !void {
    _ = allocator;
    var decoder = arithmetic.MqDecoder.init(body);
    var source: MqSymbolSource = .{ .decoder = &decoder };
    try executeContributionPassPlanGeneric(&source, grid, plan, sign_policy, refinement_policy, magnitude_policy, context_init_policy, code_block_style);
}

pub fn traceContributionPassPlanMq(
    allocator: std.mem.Allocator,
    grid: *CoefficientGrid,
    plan: *const ContributionPassPlan,
    body: []const u8,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    context_init_policy: ContextInitPolicy,
    code_block_style: CodeBlockStyle,
) !MqExecutionTrace {
    _ = allocator;
    var decoder = arithmetic.MqDecoder.init(body);
    var source: TracingMqSymbolSource = .{ .decoder = &decoder };
    try executeContributionPassPlanGeneric(&source, grid, plan, sign_policy, refinement_policy, magnitude_policy, context_init_policy, code_block_style);
    return .{
        .symbol_count = source.symbol_count,
        .preview = source.preview,
    };
}

pub fn traceContributionPassPlanDetailed(
    allocator: std.mem.Allocator,
    grid: *CoefficientGrid,
    plan: *const ContributionPassPlan,
    body: []const u8,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    context_init_policy: ContextInitPolicy,
    code_block_style: CodeBlockStyle,
) !ContributionPassTrace {
    var decoder = arithmetic.MqDecoder.init(body);
    var source: TracingMqSymbolSource = .{ .decoder = &decoder };
    var snapshots = std.ArrayListUnmanaged(PassSnapshot).empty;
    var first_significance_events = std.ArrayListUnmanaged(FirstSignificanceEvent).empty;
    var refinement_events = std.ArrayListUnmanaged(RefinementEvent).empty;
    var mq_events = std.ArrayListUnmanaged(MqEvent).empty;
    errdefer snapshots.deinit(allocator);
    errdefer first_significance_events.deinit(allocator);
    errdefer refinement_events.deinit(allocator);
    errdefer mq_events.deinit(allocator);
    source.mq_events = &mq_events;
    source.allocator = allocator;
    try executeContributionPassPlanGenericTraced(
        &source,
        grid,
        plan,
        sign_policy,
        refinement_policy,
        magnitude_policy,
        context_init_policy,
        code_block_style,
        &snapshots,
        &first_significance_events,
        &refinement_events,
    );
    return .{
        .mq = .{
            .symbol_count = source.symbol_count,
            .preview = source.preview,
        },
        .snapshots = try snapshots.toOwnedSlice(allocator),
        .first_significance_events = try first_significance_events.toOwnedSlice(allocator),
        .refinement_events = try refinement_events.toOwnedSlice(allocator),
        .mq_events = try mq_events.toOwnedSlice(allocator),
    };
}

pub fn executeContributionPassPlanScripted(
    grid: *CoefficientGrid,
    plan: *const ContributionPassPlan,
    symbols: []const u1,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    context_init_policy: ContextInitPolicy,
    code_block_style: CodeBlockStyle,
) !void {
    var source: ScriptedSymbolSource = .{ .symbols = symbols };
    try executeContributionPassPlanGeneric(&source, grid, plan, sign_policy, refinement_policy, magnitude_policy, context_init_policy, code_block_style);
}

/// Execute a contribution pass plan where BYPASS (0x01), TERMALL (0x04), or
/// PTERM (0x10) style bits may be set. Segment byte lengths let the decoder
/// carve the body into independently decodable segments. TERMALL contributes
/// one segment per pass. BYPASS without TERMALL uses the JPEG 2000 segment
/// pattern: first ten passes in one MQ segment, then alternating two-pass raw
/// and one-pass MQ cleanup segments.
///
/// When none of BYPASS/TERMALL is set, this function is equivalent to
/// `executeContributionPassPlanMq`.
pub fn executeContributionPassPlanMqWithSegments(
    allocator: std.mem.Allocator,
    grid: *CoefficientGrid,
    plan: *const ContributionPassPlan,
    body: []const u8,
    segment_lengths: []const u32,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    context_init_policy: ContextInitPolicy,
    code_block_style: CodeBlockStyle,
) !void {
    _ = allocator;
    if (!code_block_style.bypass and !code_block_style.termination) {
        // Fast path: no segment boundaries matter.
        var decoder = arithmetic.MqDecoder.init(body);
        var source: MqSymbolSource = .{ .decoder = &decoder };
        try executeContributionPassPlanGeneric(&source, grid, plan, sign_policy, refinement_policy, magnitude_policy, context_init_policy, code_block_style);
        return;
    }

    var contexts = Tier1Contexts.init(context_init_policy);
    var current_bitplane: ?i16 = null;
    var segment_start: usize = 0;
    const lengths_are_cumulative = segmentLengthsAreCumulativePassEnds(segment_lengths, plan.passes.len, body.len);

    if (code_block_style.termination) {
        if (segment_lengths.len != plan.passes.len) return error.PassLengthMismatch;
        var i: usize = 0;
        while (i < plan.passes.len) : (i += 1) {
            const pass = plan.passes[i];
            if (current_bitplane == null or current_bitplane.? != pass.bitplane) {
                grid.resetVisited();
                current_bitplane = pass.bitplane;
            }
            const segment_end = if (lengths_are_cumulative)
                @as(usize, @intCast(segment_lengths[i]))
            else
                segment_start + segment_lengths[i];
            if (segment_end > body.len or segment_start > segment_end) return error.PassLengthMismatch;
            const segment = body[segment_start..segment_end];
            const raw = isBypassRawPass(code_block_style, pass, i);
            if (raw) {
                var bit_reader = arithmetic.PacketHeaderBitReader.init(segment);
                var source: RawBitSymbolSource = .{ .reader = &bit_reader };
                try executeRawBypassPass(&source, grid, pass, plan, refinement_policy, magnitude_policy, code_block_style);
            } else {
                var decoder = arithmetic.MqDecoder.init(segment);
                var source: MqSymbolSource = .{ .decoder = &decoder };
                try executePassWithSource(&source, &contexts, grid, pass, plan, sign_policy, refinement_policy, magnitude_policy, code_block_style);
                if (code_block_style.reset_context) {
                    contexts = Tier1Contexts.init(context_init_policy);
                }
            }
            segment_start = segment_end;
        }
        if (segment_start != body.len) return error.PassLengthMismatch;
        return;
    }

    if (code_block_style.bypass) {
        if (lengths_are_cumulative) {
            try executeContributionPassPlanMqWithCumulativePassEnds(
                grid,
                plan,
                body,
                segment_lengths,
                sign_policy,
                refinement_policy,
                magnitude_policy,
                context_init_policy,
                code_block_style,
            );
            return;
        }

        var segment_index: usize = 0;
        var previous_segment_max: usize = 0;
        var pass_index: usize = 0;
        while (pass_index < plan.passes.len) : (segment_index += 1) {
            if (!lengths_are_cumulative and segment_index >= segment_lengths.len) return error.PassLengthMismatch;
            const max_passes = bypassSegmentMaxPasses(segment_index, previous_segment_max);
            const pass_count = @min(max_passes, plan.passes.len - pass_index);
            const segment_end = if (lengths_are_cumulative)
                @as(usize, @intCast(segment_lengths[pass_index + pass_count - 1]))
            else
                segment_start + segment_lengths[segment_index];
            if (segment_end > body.len or segment_start > segment_end) return error.PassLengthMismatch;
            const segment = body[segment_start..segment_end];
            const raw_segment = segment_index > 0 and max_passes == 2;
            if (raw_segment) {
                var bit_reader = arithmetic.PacketHeaderBitReader.init(segment);
                var source: RawBitSymbolSource = .{ .reader = &bit_reader };
                var local_pass: usize = 0;
                while (local_pass < pass_count) : (local_pass += 1) {
                    const pass = plan.passes[pass_index + local_pass];
                    if (current_bitplane == null or current_bitplane.? != pass.bitplane) {
                        grid.resetVisited();
                        current_bitplane = pass.bitplane;
                    }
                    try executeRawBypassPass(&source, grid, pass, plan, refinement_policy, magnitude_policy, code_block_style);
                }
            } else {
                var decoder = arithmetic.MqDecoder.init(segment);
                var source: MqSymbolSource = .{ .decoder = &decoder };
                var local_pass: usize = 0;
                while (local_pass < pass_count) : (local_pass += 1) {
                    const pass = plan.passes[pass_index + local_pass];
                    if (current_bitplane == null or current_bitplane.? != pass.bitplane) {
                        grid.resetVisited();
                        current_bitplane = pass.bitplane;
                    }
                    try executePassWithSource(&source, &contexts, grid, pass, plan, sign_policy, refinement_policy, magnitude_policy, code_block_style);
                    if (code_block_style.reset_context) {
                        contexts = Tier1Contexts.init(context_init_policy);
                    }
                }
            }
            pass_index += pass_count;
            segment_start = segment_end;
            previous_segment_max = max_passes;
        }
        if (!lengths_are_cumulative and segment_index != segment_lengths.len) return error.PassLengthMismatch;
        if (segment_start != body.len) return error.PassLengthMismatch;
        return;
    }

    return error.PassLengthMismatch;
}

fn bypassSegmentMaxPasses(segment_index: usize, previous_segment_max: usize) usize {
    if (segment_index == 0) return 10;
    if (previous_segment_max == 1 or previous_segment_max == 10) return 2;
    return 1;
}

fn segmentLengthsAreCumulativePassEnds(lengths: []const u32, pass_count: usize, body_len: usize) bool {
    if (lengths.len != pass_count or lengths.len == 0) return false;
    if (lengths[lengths.len - 1] != body_len) return false;
    var previous: u32 = 0;
    for (lengths) |length| {
        if (length < previous) return false;
        previous = length;
    }
    return true;
}

fn isBypassRawPass(code_block_style: CodeBlockStyle, pass: CodingPass, pass_index: usize) bool {
    if (!code_block_style.bypass) return false;
    if (pass.kind == .cleanup) return false;
    return pass_index >= 10;
}

fn executeRawBypassPass(
    source: anytype,
    grid: *CoefficientGrid,
    pass: CodingPass,
    plan: *const ContributionPassPlan,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    code_block_style: CodeBlockStyle,
) !void {
    switch (pass.kind) {
        .significance => try executeSignificancePassBypass(source, grid, pass, plan.subband, magnitude_policy, code_block_style),
        .refinement => try executeRefinementPassBypass(source, grid, pass, refinement_policy, code_block_style),
        .cleanup => return error.PassLengthMismatch,
    }
}

fn executeContributionPassPlanMqWithCumulativePassEnds(
    grid: *CoefficientGrid,
    plan: *const ContributionPassPlan,
    body: []const u8,
    pass_lengths: []const u32,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    context_init_policy: ContextInitPolicy,
    code_block_style: CodeBlockStyle,
) !void {
    var contexts = Tier1Contexts.init(context_init_policy);
    var current_bitplane: ?i16 = null;
    const first_bitplane: i16 = if (plan.passes.len > 0) plan.passes[0].bitplane else 0;
    const bypass_threshold: i16 = first_bitplane - 4;
    var persistent_decoder: ?arithmetic.MqDecoder = null;
    var segment_start: usize = 0;

    var i: usize = 0;
    while (i < plan.passes.len) : (i += 1) {
        const pass = plan.passes[i];
        if (current_bitplane == null or current_bitplane.? != pass.bitplane) {
            grid.resetVisited();
            current_bitplane = pass.bitplane;
        }
        const pass_end: usize = @intCast(pass_lengths[i]);
        if (pass_end > body.len or segment_start > pass_end) return error.PassLengthMismatch;

        const in_bypass_region = code_block_style.bypass and pass.bitplane <= bypass_threshold;
        const is_raw_bits = in_bypass_region and (pass.kind == .significance or pass.kind == .refinement);
        const is_own_mq_segment = in_bypass_region and pass.kind == .cleanup;
        const is_pre_bypass_cleanup = code_block_style.bypass and pass.kind == .cleanup and pass.bitplane == bypass_threshold + 1;
        const is_terminator = code_block_style.termination or is_raw_bits or is_own_mq_segment or is_pre_bypass_cleanup or (i + 1 == plan.passes.len);

        if (is_raw_bits) {
            const segment = body[segment_start..pass_end];
            var bit_reader = arithmetic.PacketHeaderBitReader.init(segment);
            var source: RawBitSymbolSource = .{ .reader = &bit_reader };
            try executeRawBypassPass(&source, grid, pass, plan, refinement_policy, magnitude_policy, code_block_style);
            segment_start = pass_end;
        } else if (is_own_mq_segment) {
            const segment = body[segment_start..pass_end];
            var decoder = arithmetic.MqDecoder.init(segment);
            var source: MqSymbolSource = .{ .decoder = &decoder };
            try executePassWithSource(&source, &contexts, grid, pass, plan, sign_policy, refinement_policy, magnitude_policy, code_block_style);
            segment_start = pass_end;
        } else {
            if (persistent_decoder == null) {
                var end_idx: usize = i;
                while (end_idx < plan.passes.len) : (end_idx += 1) {
                    const p2 = plan.passes[end_idx];
                    const p2_in_bypass = code_block_style.bypass and p2.bitplane <= bypass_threshold;
                    const p2_raw = p2_in_bypass and (p2.kind == .significance or p2.kind == .refinement);
                    const p2_own = p2_in_bypass and p2.kind == .cleanup;
                    const p2_pre_bypass = code_block_style.bypass and p2.kind == .cleanup and p2.bitplane == bypass_threshold + 1;
                    if (code_block_style.termination or p2_raw or p2_own or p2_pre_bypass or (end_idx + 1 == plan.passes.len)) break;
                }
                const seg_end: usize = @intCast(pass_lengths[end_idx]);
                persistent_decoder = arithmetic.MqDecoder.init(body[segment_start..seg_end]);
            }
            var source: MqSymbolSource = .{ .decoder = &persistent_decoder.? };
            try executePassWithSource(&source, &contexts, grid, pass, plan, sign_policy, refinement_policy, magnitude_policy, code_block_style);
            if (is_terminator) {
                persistent_decoder = null;
                segment_start = pass_end;
            }
        }
        if (code_block_style.reset_context and !is_raw_bits) {
            contexts = Tier1Contexts.init(context_init_policy);
        }
    }
}

fn executePassWithSource(
    source: anytype,
    contexts: *Tier1Contexts,
    grid: *CoefficientGrid,
    pass: CodingPass,
    plan: *const ContributionPassPlan,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    code_block_style: CodeBlockStyle,
) !void {
    switch (pass.kind) {
        .cleanup => {
            try executeCleanupPass(source, contexts, grid, pass, plan.subband, sign_policy, refinement_policy, magnitude_policy, plan.component_index, null, code_block_style);
            if (code_block_style.segmentation_symbol) {
                _ = try source.nextSymbol(contexts.at(18));
                _ = try source.nextSymbol(contexts.at(18));
                _ = try source.nextSymbol(contexts.at(18));
                _ = try source.nextSymbol(contexts.at(18));
            }
        },
        .significance => try executeSignificancePass(source, contexts, grid, pass, plan.subband, sign_policy, refinement_policy, magnitude_policy, plan.component_index, null, code_block_style),
        .refinement => try executeRefinementPass(source, contexts, grid, pass, refinement_policy, null, code_block_style),
    }
}

const RawBitSymbolSource = struct {
    reader: *arithmetic.PacketHeaderBitReader,

    fn nextSymbol(self: *RawBitSymbolSource, _: *arithmetic.MqContext) !u1 {
        // OpenJPEG's raw BYPASS decoder synthesizes 0xff after segment end.
        return self.reader.readBit() catch |err| switch (err) {
            error.EndOfBitstream => 1,
        };
    }
};

/// Significance propagation pass decoded from a raw-bit segment (BYPASS mode).
/// Significance decision and sign bit are read verbatim; we do not consult
/// the MQ contexts for these symbols. Segmentation-symbol and run-length
/// paths do not apply.
fn executeSignificancePassBypass(
    source: anytype,
    grid: *CoefficientGrid,
    pass: CodingPass,
    subband: tile.SubbandType,
    magnitude_policy: MagnitudePolicy,
    code_block_style: CodeBlockStyle,
) !void {
    _ = subband;
    var stripe_y: usize = 0;
    while (stripe_y < grid.height) : (stripe_y += 4) {
        var x: usize = 0;
        while (x < grid.width) : (x += 1) {
            const stripe_end = @min(stripe_y + 4, grid.height);
            var y = stripe_y;
            while (y < stripe_end) : (y += 1) {
                const cell = grid.at(x, y);
                if (cell.flags.significant or cell.flags.visited) continue;
                if (grid.significantNeighborCountCausal(x, y, code_block_style.vertically_causal, stripe_y) == 0) continue;
                var dummy_ctx: arithmetic.MqContext = .{};
                const significant = try source.nextSymbol(&dummy_ctx);
                if (significant == 0) {
                    grid.at(x, y).flags.visited = true;
                    continue;
                }
                const sign = try source.nextSymbol(&dummy_ctx);
                const negative = sign == 1;
                grid.markSignificant(x, y, negative, pass.bitplane, magnitude_policy);
            }
        }
    }
}

/// Magnitude refinement pass decoded from a raw-bit segment (BYPASS mode).
fn executeRefinementPassBypass(
    source: anytype,
    grid: *CoefficientGrid,
    pass: CodingPass,
    refinement_policy: RefinementPolicy,
    _: CodeBlockStyle,
) !void {
    var stripe_y: usize = 0;
    while (stripe_y < grid.height) : (stripe_y += 4) {
        var x: usize = 0;
        while (x < grid.width) : (x += 1) {
            const stripe_end = @min(stripe_y + 4, grid.height);
            var y = stripe_y;
            while (y < stripe_end) : (y += 1) {
                const cell = grid.at(x, y);
                if (!cell.flags.significant or cell.flags.visited) continue;
                var dummy_ctx: arithmetic.MqContext = .{};
                const bit = try source.nextSymbol(&dummy_ctx);
                grid.applyRefinement(x, y, bit, pass.bitplane, refinement_policy);
                cell.flags.visited = true;
            }
        }
    }
}

fn executeContributionPassPlanGeneric(
    source: anytype,
    grid: *CoefficientGrid,
    plan: *const ContributionPassPlan,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    context_init_policy: ContextInitPolicy,
    code_block_style: CodeBlockStyle,
) !void {
    var contexts = Tier1Contexts.init(context_init_policy);
    var current_bitplane: ?i16 = null;
    for (plan.passes) |pass| {
        if (current_bitplane == null or current_bitplane.? != pass.bitplane) {
            grid.resetVisited();
            current_bitplane = pass.bitplane;
        }
        switch (pass.kind) {
            .cleanup => {
                try executeCleanupPass(source, &contexts, grid, pass, plan.subband, sign_policy, refinement_policy, magnitude_policy, plan.component_index, null, code_block_style);
                // Segmentation symbol: decode 4 bits using uniform context after cleanup (bit 5).
                if (code_block_style.segmentation_symbol) {
                    _ = try source.nextSymbol(contexts.at(18));
                    _ = try source.nextSymbol(contexts.at(18));
                    _ = try source.nextSymbol(contexts.at(18));
                    _ = try source.nextSymbol(contexts.at(18));
                }
            },
            .significance => try executeSignificancePass(source, &contexts, grid, pass, plan.subband, sign_policy, refinement_policy, magnitude_policy, plan.component_index, null, code_block_style),
            .refinement => try executeRefinementPass(source, &contexts, grid, pass, refinement_policy, null, code_block_style),
        }
        if (code_block_style.reset_context) {
            contexts = Tier1Contexts.init(context_init_policy);
        }
    }
}

fn executeContributionPassPlanGenericTraced(
    source: anytype,
    grid: *CoefficientGrid,
    plan: *const ContributionPassPlan,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    context_init_policy: ContextInitPolicy,
    code_block_style: CodeBlockStyle,
    snapshots: *std.ArrayListUnmanaged(PassSnapshot),
    first_significance_events: *std.ArrayListUnmanaged(FirstSignificanceEvent),
    refinement_events: *std.ArrayListUnmanaged(RefinementEvent),
) !void {
    var contexts = Tier1Contexts.init(context_init_policy);
    var current_bitplane: ?i16 = null;
    for (plan.passes) |pass| {
        if (current_bitplane == null or current_bitplane.? != pass.bitplane) {
            grid.resetVisited();
            current_bitplane = pass.bitplane;
        }
        switch (pass.kind) {
            .cleanup => {
                try executeCleanupPass(source, &contexts, grid, pass, plan.subband, sign_policy, refinement_policy, magnitude_policy, plan.component_index, first_significance_events, code_block_style);
                // Segmentation symbol: decode 4 bits using uniform context after cleanup (bit 5).
                if (code_block_style.segmentation_symbol) {
                    _ = try source.nextSymbol(contexts.at(18));
                    _ = try source.nextSymbol(contexts.at(18));
                    _ = try source.nextSymbol(contexts.at(18));
                    _ = try source.nextSymbol(contexts.at(18));
                }
            },
            .significance => try executeSignificancePass(source, &contexts, grid, pass, plan.subband, sign_policy, refinement_policy, magnitude_policy, plan.component_index, first_significance_events, code_block_style),
            .refinement => try executeRefinementPass(source, &contexts, grid, pass, refinement_policy, refinement_events, code_block_style),
        }
        if (code_block_style.reset_context) {
            contexts = Tier1Contexts.init(context_init_policy);
        }
        var snapshot = capturePassSnapshot(grid, pass);
        if (@hasField(@TypeOf(source.*), "symbol_count")) {
            snapshot.mq_symbol_count = source.symbol_count;
        }
        try snapshots.append(grid.allocator, snapshot);
    }
}

fn capturePassSnapshot(grid: *const CoefficientGrid, pass: CodingPass) PassSnapshot {
    var snapshot: PassSnapshot = .{
        .pass_index = pass.pass_index,
        .kind = pass.kind,
        .bitplane = pass.bitplane,
        .mq_symbol_count = 0,
        .magnitudes = [_]i32{0} ** 4,
        .signs = [_]u8{0} ** 4,
        .significant = [_]u8{0} ** 4,
    };
    const limit = @min(snapshot.magnitudes.len, grid.cells.len);
    var i: usize = 0;
    while (i < limit) : (i += 1) {
        const cell = grid.cells[i];
        snapshot.magnitudes[i] = cell.magnitude;
        snapshot.signs[i] = @intFromBool(cell.flags.sign);
        snapshot.significant[i] = @intFromBool(cell.flags.significant);
    }
    return snapshot;
}

fn executeCleanupPass(
    source: anytype,
    contexts: *Tier1Contexts,
    grid: *CoefficientGrid,
    pass: CodingPass,
    subband: tile.SubbandType,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    component_index: u16,
    first_significance_events: ?*std.ArrayListUnmanaged(FirstSignificanceEvent),
    code_block_style: CodeBlockStyle,
) !void {
    var stripe_y: usize = 0;
    while (stripe_y < grid.height) : (stripe_y += 4) {
        var x: usize = 0;
        while (x < grid.width) : (x += 1) {
            const stripe_end = @min(stripe_y + 4, grid.height);
            if (cleanupRunModeEligible(grid, x, stripe_y, stripe_end, code_block_style)) {
                const run_symbol = try source.nextSymbol(contexts.at(17));
                if (run_symbol == 0) {
                    var run_y = stripe_y;
                    while (run_y < stripe_end) : (run_y += 1) grid.at(x, run_y).flags.visited = true;
                    continue;
                }

                const msb = try source.nextSymbol(contexts.at(18));
                const lsb = try source.nextSymbol(contexts.at(18));
                var run_index: usize = (@as(usize, msb) << 1) | lsb;
                if (run_index >= stripe_end - stripe_y) run_index = stripe_end - stripe_y - 1;

                var before_y = stripe_y;
                while (before_y < stripe_y + run_index) : (before_y += 1) grid.at(x, before_y).flags.visited = true;
                // Per ITU-T T.800 D.6: the first significant cell after a run has
                // its significance implied. Decode only the sign, then mark significant.
                {
                    const sig_y = stripe_y + run_index;
                    const sign_contribution = signContributionCausal(grid, x, sig_y, code_block_style.vertically_causal, stripe_y);
                    const sign_ctx_index = signContributionContextIndex(sign_contribution);
                    const sign = try source.nextSymbol(contexts.at(sign_ctx_index));
                    const predicted_sign = signContributionPredictor(sign_contribution);
                    const negative = (sign ^ predicted_sign) == 1;
                    grid.markSignificant(x, sig_y, negative, pass.bitplane, magnitude_policy);
                }

                var after_y = stripe_y + run_index + 1;
                while (after_y < stripe_end) : (after_y += 1) {
                    if (grid.at(x, after_y).flags.significant or grid.at(x, after_y).flags.visited) continue;
                    try decodeCleanupSample(source, contexts, grid, x, after_y, pass, subband, sign_policy, refinement_policy, magnitude_policy, component_index, first_significance_events, code_block_style, stripe_y);
                }
                continue;
            }

            var y = stripe_y;
            while (y < stripe_end) : (y += 1) {
                if (grid.at(x, y).flags.significant or grid.at(x, y).flags.visited) continue;
                try decodeCleanupSample(source, contexts, grid, x, y, pass, subband, sign_policy, refinement_policy, magnitude_policy, component_index, first_significance_events, code_block_style, stripe_y);
            }
        }
    }
}

fn executeSignificancePass(
    source: anytype,
    contexts: *Tier1Contexts,
    grid: *CoefficientGrid,
    pass: CodingPass,
    subband: tile.SubbandType,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    component_index: u16,
    first_significance_events: ?*std.ArrayListUnmanaged(FirstSignificanceEvent),
    code_block_style: CodeBlockStyle,
) !void {
    var stripe_y: usize = 0;
    while (stripe_y < grid.height) : (stripe_y += 4) {
        var x: usize = 0;
        while (x < grid.width) : (x += 1) {
            const stripe_end = @min(stripe_y + 4, grid.height);
            var y = stripe_y;
            while (y < stripe_end) : (y += 1) {
                const cell = grid.at(x, y);
                if (cell.flags.significant or cell.flags.visited) continue;
                if (grid.significantNeighborCountCausal(x, y, code_block_style.vertically_causal, stripe_y) == 0) continue;
                try decodeSignificanceAndSign(
                    source,
                    contexts,
                    grid,
                    x,
                    y,
                    pass,
                    subband,
                    zeroCodingStateIndexCausal(grid, x, y, subband, code_block_style.vertically_causal, stripe_y),
                    sign_policy,
                    refinement_policy,
                    magnitude_policy,
                    component_index,
                    first_significance_events,
                    code_block_style,
                    stripe_y,
                );
            }
        }
    }
}

fn executeRefinementPass(
    source: anytype,
    contexts: *Tier1Contexts,
    grid: *CoefficientGrid,
    pass: CodingPass,
    refinement_policy: RefinementPolicy,
    refinement_events: ?*std.ArrayListUnmanaged(RefinementEvent),
    code_block_style: CodeBlockStyle,
) !void {
    var stripe_y: usize = 0;
    while (stripe_y < grid.height) : (stripe_y += 4) {
        var x: usize = 0;
        while (x < grid.width) : (x += 1) {
            const stripe_end = @min(stripe_y + 4, grid.height);
            var y = stripe_y;
            while (y < stripe_end) : (y += 1) {
                const cell = grid.at(x, y);
                if (!cell.flags.significant or cell.flags.visited) continue;
                const context_index = refinementStateIndexCausal(grid, x, y, cell.flags.refined, code_block_style.vertically_causal, stripe_y);
                const context = contexts.at(context_index);
                const context_state_before = context.state_index;
                const magnitude_before = cell.magnitude;
                const bit = try source.nextSymbol(context);
                grid.applyRefinement(x, y, bit, pass.bitplane, refinement_policy);
                if (refinement_events) |events| {
                    try events.append(grid.allocator, .{
                        .x = @intCast(x),
                        .y = @intCast(y),
                        .pass_index = pass.pass_index,
                        .bitplane = pass.bitplane,
                        .context_index = context_index,
                        .context_state_before = context_state_before,
                        .context_state_after = context.state_index,
                        .bit = bit,
                        .magnitude_before = magnitude_before,
                        .magnitude_after = cell.magnitude,
                    });
                }
                cell.flags.visited = true;
            }
        }
    }
}

fn decodeSignificanceAndSign(
    source: anytype,
    contexts: *Tier1Contexts,
    grid: *CoefficientGrid,
    x: usize,
    y: usize,
    pass: CodingPass,
    subband: tile.SubbandType,
    zero_ctx_index: u8,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    component_index: u16,
    first_significance_events: ?*std.ArrayListUnmanaged(FirstSignificanceEvent),
    code_block_style: CodeBlockStyle,
    stripe_y: usize,
) !void {
    _ = refinement_policy;
    const significant = try source.nextSymbol(contexts.at(zero_ctx_index));
    if (significant == 0) {
        grid.at(x, y).flags.visited = true;
        return;
    }
    const sign_contribution = signContributionCausal(grid, x, y, code_block_style.vertically_causal, stripe_y);
    const sign_lut_index = signCodingLutIndexCausal(grid, x, y, code_block_style.vertically_causal, stripe_y);
    const sign_ctx_index = signContributionContextIndex(sign_contribution);
    const sign = try source.nextSymbol(contexts.at(sign_ctx_index));
    const predicted_sign = signContributionPredictor(sign_contribution);
    const negative = switch (sign_policy) {
        .standard => (sign ^ predicted_sign) == 1,
        .no_neighbor_positive => if (sign_lut_index == 0 and !grid.hasAnySignificant())
            sign == 0
        else
            (sign ^ predicted_sign) == 1,
        .decomposed_single_component_split => switch (subband) {
            .ll, .hl => (sign ^ predicted_sign) == 1,
            .lh, .hh => if (sign_lut_index == 0 and !grid.hasAnySignificant())
                sign == 0
            else
                (sign ^ predicted_sign) == 1,
        },
        .rgb_component0_no_neighbor_positive => if (component_index == 0 and sign_lut_index == 0) sign == 0 else (sign ^ predicted_sign) == 1,
        .rgb_first_only_component0_positive => if (component_index == 0 and sign_lut_index == 0 and grid.significantNeighborCount(x, y) == 0)
            sign == 0
        else
            (sign ^ predicted_sign) == 1,
        .rgb_component1_positive_on_west_negative_case => if (component_index == 1 and sign_lut_index == 9 and sign == 0)
            false
        else if (component_index == 0 and sign_lut_index == 0 and grid.significantNeighborCount(x, y) == 0)
            sign == 0
        else
            (sign ^ predicted_sign) == 1,
    };
    grid.markSignificant(x, y, negative, pass.bitplane, magnitude_policy);
    if (first_significance_events) |events| {
        try events.append(grid.allocator, .{
            .x = @intCast(x),
            .y = @intCast(y),
            .pass_index = pass.pass_index,
            .kind = pass.kind,
            .bitplane = pass.bitplane,
            .zero_ctx_index = zero_ctx_index,
            .sign_lut_index = sign_lut_index,
            .significant_symbol = significant,
            .sign_symbol = sign,
            .negative = negative,
        });
    }
}

fn decodeCleanupSample(
    source: anytype,
    contexts: *Tier1Contexts,
    grid: *CoefficientGrid,
    x: usize,
    y: usize,
    pass: CodingPass,
    subband: tile.SubbandType,
    sign_policy: SignPolicy,
    refinement_policy: RefinementPolicy,
    magnitude_policy: MagnitudePolicy,
    component_index: u16,
    first_significance_events: ?*std.ArrayListUnmanaged(FirstSignificanceEvent),
    code_block_style: CodeBlockStyle,
    stripe_y: usize,
) !void {
    try decodeSignificanceAndSign(
        source,
        contexts,
        grid,
        x,
        y,
        pass,
        subband,
        zeroCodingStateIndexCausal(grid, x, y, subband, code_block_style.vertically_causal, stripe_y),
        sign_policy,
        refinement_policy,
        magnitude_policy,
        component_index,
        first_significance_events,
        code_block_style,
        stripe_y,
    );
}

fn cleanupRunModeEligible(grid: *const CoefficientGrid, x: usize, stripe_y: usize, stripe_end: usize, code_block_style: CodeBlockStyle) bool {
    if (stripe_end - stripe_y != 4) return false;
    var y = stripe_y;
    while (y < stripe_end) : (y += 1) {
        const cell = grid.cells[grid.index(x, y)];
        if (cell.flags.significant or cell.flags.visited) return false;
        if (grid.significantNeighborCountCausal(x, y, code_block_style.vertically_causal, stripe_y) != 0) return false;
    }
    return true;
}

fn zeroCodingStateIndex(grid: *const CoefficientGrid, x: usize, y: usize, subband: tile.SubbandType) u8 {
    return zeroCodingStateIndexCausal(grid, x, y, subband, false, 0);
}

fn zeroCodingStateIndexCausal(grid: *const CoefficientGrid, x: usize, y: usize, subband: tile.SubbandType, vertically_causal: bool, stripe_y: usize) u8 {
    const orient: usize = switch (subband) {
        .ll => 0,
        .hl => 1,
        .lh => 2,
        .hh => 3,
    };
    return lut_ctxno_zc[orient * 512 + @as(usize, zeroCodingLutIndexSparseCausal(grid, x, y, vertically_causal, stripe_y))];
}

fn signCodingStateIndex(grid: *const CoefficientGrid, x: usize, y: usize) u8 {
    return signContributionContextIndex(signContribution(grid, x, y));
}

fn refinementStateIndex(grid: *const CoefficientGrid, x: usize, y: usize, already_refined: bool) u8 {
    return refinementStateIndexCausal(grid, x, y, already_refined, false, 0);
}

fn refinementStateIndexCausal(grid: *const CoefficientGrid, x: usize, y: usize, already_refined: bool, vertically_causal: bool, stripe_y: usize) u8 {
    if (already_refined) return 16;
    return if (grid.significantNeighborCountCausal(x, y, vertically_causal, stripe_y) != 0) 15 else 14;
}

fn signCodingLutIndex(grid: *const CoefficientGrid, x: usize, y: usize) u8 {
    return signCodingLutIndexCausal(grid, x, y, false, 0);
}

fn signCodingLutIndexCausal(grid: *const CoefficientGrid, x: usize, y: usize, vertically_causal: bool, stripe_y: usize) u8 {
    var index: u8 = 0;
    const max_causal_y = if (vertically_causal) @min(stripe_y + 3, grid.height - 1) else grid.height - 1;
    if (x > 0) {
        const west = grid.cells[grid.index(x - 1, y)];
        if (west.flags.significant) {
            index |= 1 << 3;
            if (west.flags.sign) index |= 1 << 0;
        }
    }
    if (x + 1 < grid.width) {
        const east = grid.cells[grid.index(x + 1, y)];
        if (east.flags.significant) {
            index |= 1 << 5;
            if (east.flags.sign) index |= 1 << 2;
        }
    }
    if (y > 0) {
        const north = grid.cells[grid.index(x, y - 1)];
        if (north.flags.significant) {
            index |= 1 << 1;
            if (north.flags.sign) index |= 1 << 4;
        }
    }
    if (y + 1 <= max_causal_y) {
        const south = grid.cells[grid.index(x, y + 1)];
        if (south.flags.significant) {
            index |= 1 << 7;
            if (south.flags.sign) index |= 1 << 6;
        }
    }
    return index;
}

const ZeroCodingNeighborCounts = struct {
    horizontal: u8,
    vertical: u8,
    diagonal: u8,
};

fn zeroCodingNeighborCounts(grid: *const CoefficientGrid, x: usize, y: usize, subband: tile.SubbandType) ZeroCodingNeighborCounts {
    return zeroCodingNeighborCountsCausal(grid, x, y, subband, false, 0);
}

fn zeroCodingNeighborCountsCausal(grid: *const CoefficientGrid, x: usize, y: usize, subband: tile.SubbandType, vertically_causal: bool, stripe_y: usize) ZeroCodingNeighborCounts {
    var horizontal: u8 = 0;
    var vertical: u8 = 0;
    var diagonal: u8 = 0;

    // The stripe bottom boundary: when vertically causal, clamp south neighbors
    // to the last row of the current stripe (stripe_y + 3).
    const max_causal_y = if (vertically_causal) @min(stripe_y + 3, grid.height - 1) else grid.height - 1;

    if (x > 0 and grid.cells[grid.index(x - 1, y)].flags.significant) horizontal += 1;
    if (x + 1 < grid.width and grid.cells[grid.index(x + 1, y)].flags.significant) horizontal += 1;
    if (y > 0 and grid.cells[grid.index(x, y - 1)].flags.significant) vertical += 1;
    if (y + 1 <= max_causal_y and grid.cells[grid.index(x, y + 1)].flags.significant) vertical += 1;
    if (x > 0 and y > 0 and grid.cells[grid.index(x - 1, y - 1)].flags.significant) diagonal += 1;
    if (x + 1 < grid.width and y > 0 and grid.cells[grid.index(x + 1, y - 1)].flags.significant) diagonal += 1;
    if (x > 0 and y + 1 <= max_causal_y and grid.cells[grid.index(x - 1, y + 1)].flags.significant) diagonal += 1;
    if (x + 1 < grid.width and y + 1 <= max_causal_y and grid.cells[grid.index(x + 1, y + 1)].flags.significant) diagonal += 1;

    if (subband == .hl) {
        return .{
            .horizontal = vertical,
            .vertical = horizontal,
            .diagonal = diagonal,
        };
    }
    return .{
        .horizontal = horizontal,
        .vertical = vertical,
        .diagonal = diagonal,
    };
}

fn zeroCodingContextIndexLlLike(horizontal: u8, vertical: u8, diagonal: u8) u8 {
    if (horizontal == 0) {
        if (vertical == 0) {
            if (diagonal == 0) return 0;
            if (diagonal == 1) return 1;
            return 2;
        }
        if (vertical == 1) return 3;
        return 4;
    }
    if (horizontal == 1) {
        if (vertical == 0) {
            if (diagonal == 0) return 5;
            return 6;
        }
        return 7;
    }
    return 8;
}

fn zeroCodingContextIndexHh(horizontal: u8, vertical: u8, diagonal: u8) u8 {
    const hv = horizontal + vertical;
    if (diagonal == 0) {
        if (hv == 0) return 0;
        if (hv == 1) return 1;
        return 2;
    }
    if (diagonal == 1) {
        if (hv == 0) return 3;
        if (hv == 1) return 4;
        return 5;
    }
    if (diagonal == 2) {
        if (hv == 0) return 6;
        return 7;
    }
    return 8;
}

fn signContribution(grid: *const CoefficientGrid, x: usize, y: usize) SignContribution {
    return signContributionCausal(grid, x, y, false, 0);
}

fn signContributionCausal(grid: *const CoefficientGrid, x: usize, y: usize, vertically_causal: bool, stripe_y: usize) SignContribution {
    var east_pos: i2 = 0;
    var east_neg: i2 = 0;
    var west_pos: i2 = 0;
    var west_neg: i2 = 0;
    var north_pos: i2 = 0;
    var north_neg: i2 = 0;
    var south_pos: i2 = 0;
    var south_neg: i2 = 0;
    const max_causal_y = if (vertically_causal) @min(stripe_y + 3, grid.height - 1) else grid.height - 1;

    if (x > 0) {
        const west = grid.cells[grid.index(x - 1, y)];
        if (west.flags.significant) {
            if (west.flags.sign) west_neg = 1 else west_pos = 1;
        }
    }
    if (x + 1 < grid.width) {
        const east = grid.cells[grid.index(x + 1, y)];
        if (east.flags.significant) {
            if (east.flags.sign) east_neg = 1 else east_pos = 1;
        }
    }
    if (y > 0) {
        const north = grid.cells[grid.index(x, y - 1)];
        if (north.flags.significant) {
            if (north.flags.sign) north_neg = 1 else north_pos = 1;
        }
    }
    if (y + 1 <= max_causal_y) {
        const south = grid.cells[grid.index(x, y + 1)];
        if (south.flags.significant) {
            if (south.flags.sign) south_neg = 1 else south_pos = 1;
        }
    }

    const horizontal_pos = @min(@as(i8, east_pos) + @as(i8, west_pos), 1);
    const horizontal_neg = @min(@as(i8, east_neg) + @as(i8, west_neg), 1);
    const vertical_pos = @min(@as(i8, north_pos) + @as(i8, south_pos), 1);
    const vertical_neg = @min(@as(i8, north_neg) + @as(i8, south_neg), 1);

    const horizontal: i2 = @intCast(horizontal_pos - horizontal_neg);
    const vertical: i2 = @intCast(vertical_pos - vertical_neg);
    return .{ .horizontal = horizontal, .vertical = vertical };
}

fn signContributionContextIndex(contribution: SignContribution) u8 {
    var horizontal = contribution.horizontal;
    var vertical = contribution.vertical;
    var n: u8 = 0;
    if (horizontal < 0) {
        horizontal = -horizontal;
        vertical = -vertical;
    }
    if (horizontal == 0) {
        n = if (vertical == 0) 0 else 1;
    } else if (horizontal == 1) {
        if (vertical == -1) {
            n = 2;
        } else if (vertical == 0) {
            n = 3;
        } else {
            n = 4;
        }
    }
    return 9 + n;
}

fn signContributionPredictor(contribution: SignContribution) u1 {
    if (contribution.horizontal == 0 and contribution.vertical == 0) return 0;
    return @intFromBool(!(contribution.horizontal > 0 or (contribution.horizontal == 0 and contribution.vertical > 0)));
}

fn zeroCodingLutIndexSparse(grid: *const CoefficientGrid, x: usize, y: usize) u9 {
    return zeroCodingLutIndexSparseCausal(grid, x, y, false, 0);
}

fn zeroCodingLutIndexSparseCausal(grid: *const CoefficientGrid, x: usize, y: usize, vertically_causal: bool, stripe_y: usize) u9 {
    var index: u9 = 0;
    const max_causal_y = if (vertically_causal) @min(stripe_y + 3, grid.height - 1) else grid.height - 1;
    if (x > 0 and y > 0 and grid.cells[grid.index(x - 1, y - 1)].flags.significant) index |= 1 << 0;
    if (y > 0 and grid.cells[grid.index(x, y - 1)].flags.significant) index |= 1 << 1;
    if (x + 1 < grid.width and y > 0 and grid.cells[grid.index(x + 1, y - 1)].flags.significant) index |= 1 << 2;
    if (x > 0 and grid.cells[grid.index(x - 1, y)].flags.significant) index |= 1 << 3;
    if (x + 1 < grid.width and grid.cells[grid.index(x + 1, y)].flags.significant) index |= 1 << 5;
    if (x > 0 and y + 1 <= max_causal_y and grid.cells[grid.index(x - 1, y + 1)].flags.significant) index |= 1 << 6;
    if (y + 1 <= max_causal_y and grid.cells[grid.index(x, y + 1)].flags.significant) index |= 1 << 7;
    if (x + 1 < grid.width and y + 1 <= max_causal_y and grid.cells[grid.index(x + 1, y + 1)].flags.significant) index |= 1 << 8;
    return index;
}

test "decode coding pass counts" {
    {
        var reader = arithmetic.BitReader.init(&.{0b0000_0000});
        try std.testing.expectEqual(@as(u16, 1), try decodeNumCodingPasses(&reader));
    }
    {
        var reader = arithmetic.BitReader.init(&.{0b1000_0000});
        try std.testing.expectEqual(@as(u16, 2), try decodeNumCodingPasses(&reader));
    }
    {
        var reader = arithmetic.BitReader.init(&.{0b1100_0000});
        try std.testing.expectEqual(@as(u16, 3), try decodeNumCodingPasses(&reader));
    }
    {
        var reader = arithmetic.BitReader.init(&.{0b1101_0000});
        try std.testing.expectEqual(@as(u16, 4), try decodeNumCodingPasses(&reader));
    }
    {
        var reader = arithmetic.BitReader.init(&.{0b1110_0000});
        try std.testing.expectEqual(@as(u16, 5), try decodeNumCodingPasses(&reader));
    }
    {
        var reader = arithmetic.BitReader.init(&.{ 0b1111_0101, 0b0000_0000 });
        try std.testing.expectEqual(@as(u16, 16), try decodeNumCodingPasses(&reader));
    }
    {
        var reader = arithmetic.BitReader.init(&.{ 0b1111_1111, 0b1000_0100, 0b0000_0000 });
        try std.testing.expectEqual(@as(u16, 41), try decodeNumCodingPasses(&reader));
    }
}

test "decode comma code" {
    var reader = arithmetic.BitReader.init(&.{0b1110_0000});
    try std.testing.expectEqual(@as(u8, 3), try decodeCommaCode(&reader));
}

test "build pass schedule starts with cleanup and cycles by bitplane" {
    const allocator = std.testing.allocator;
    const schedule = try buildPassSchedule(allocator, 6, 1, 8);
    defer allocator.free(schedule);

    try std.testing.expectEqual(@as(usize, 6), schedule.len);
    try std.testing.expectEqual(PassKind.cleanup, schedule[0].kind);
    try std.testing.expectEqual(PassKind.significance, schedule[1].kind);
    try std.testing.expectEqual(PassKind.refinement, schedule[2].kind);
    try std.testing.expectEqual(PassKind.cleanup, schedule[3].kind);
    try std.testing.expectEqual(@as(i16, 6), schedule[0].bitplane);
    try std.testing.expectEqual(@as(i16, 5), schedule[1].bitplane);
    try std.testing.expectEqual(@as(i16, 5), schedule[2].bitplane);
    try std.testing.expectEqual(@as(i16, 5), schedule[3].bitplane);
    try std.testing.expectEqual(PassKind.significance, schedule[4].kind);
    try std.testing.expectEqual(@as(i16, 4), schedule[4].bitplane);
}

test "plan contribution passes preserves component metadata" {
    const allocator = std.testing.allocator;
    var plan = try planContributionPasses(allocator, 2, .ll, 0, 2, 8);
    defer plan.deinit(allocator);
    try std.testing.expectEqual(@as(u16, 2), plan.component_index);
    try std.testing.expectEqual(@as(u16, 0), plan.start_pass_index);
    try std.testing.expectEqual(@as(u16, 2), plan.num_passes);
    try std.testing.expectEqual(PassKind.cleanup, plan.passes[0].kind);
    try std.testing.expectEqual(PassKind.significance, plan.passes[1].kind);
}

test "plan contribution pass range continues absolute tier-1 pass order" {
    const allocator = std.testing.allocator;
    var plan = try planContributionPassRange(allocator, 0, .ll, 1, 1, 3, 8);
    defer plan.deinit(allocator);

    try std.testing.expectEqual(@as(u16, 1), plan.start_pass_index);
    try std.testing.expectEqual(@as(u16, 3), plan.num_passes);
    try std.testing.expectEqual(@as(u16, 1), plan.passes[0].pass_index);
    try std.testing.expectEqual(PassKind.significance, plan.passes[0].kind);
    try std.testing.expectEqual(@as(i16, 5), plan.passes[0].bitplane);
    try std.testing.expectEqual(PassKind.refinement, plan.passes[1].kind);
    try std.testing.expectEqual(PassKind.cleanup, plan.passes[2].kind);
    try std.testing.expectEqual(@as(i16, 5), plan.passes[2].bitplane);
}

test "coefficient grid tracks significance and refinement state" {
    const allocator = std.testing.allocator;
    var grid = try CoefficientGrid.init(allocator, 3, 2);
    defer grid.deinit();
    grid.clear();

    grid.markSignificant(1, 0, false, 4, .exact_bitplane);
    grid.markSignificant(2, 1, true, 2, .exact_bitplane);
    try std.testing.expect(grid.at(1, 0).flags.significant);
    try std.testing.expect(!grid.at(1, 0).flags.sign);
    try std.testing.expect(grid.at(2, 1).flags.sign);
    try std.testing.expectEqual(@as(i32, 16), grid.at(1, 0).magnitude);
    try std.testing.expectEqual(@as(u8, 2), grid.significantNeighborCount(1, 1));

    const signs = grid.significantNeighborSigns(1, 1);
    try std.testing.expectEqual(@as(u8, 1), signs.positive);
    try std.testing.expectEqual(@as(u8, 1), signs.negative);

    grid.applyRefinement(1, 0, 1, 1, .exact_bitplane);
    try std.testing.expect(grid.at(1, 0).flags.refined);
    try std.testing.expectEqual(@as(i32, 18), grid.at(1, 0).magnitude);

    grid.resetVisited();
    try std.testing.expect(!grid.at(1, 0).flags.visited);
}

test "execute scripted cleanup significance and refinement passes" {
    const allocator = std.testing.allocator;
    var grid = try CoefficientGrid.init(allocator, 2, 1);
    defer grid.deinit();
    grid.clear();

    var plan = try planContributionPasses(allocator, 0, .ll, 0, 3, 8);
    defer plan.deinit(allocator);
    try executeContributionPassPlanScripted(&grid, &plan, &.{ 1, 0, 1, 1, 1, 0 }, .standard, .standard_additive, .midpoint, .standard, .{});

    try std.testing.expect(grid.at(0, 0).flags.significant);
    try std.testing.expect(!grid.at(0, 0).flags.sign);
    try std.testing.expect(grid.at(1, 0).flags.significant);
    try std.testing.expect(grid.at(1, 0).flags.sign);
    try std.testing.expect(grid.at(0, 0).flags.refined);
    try std.testing.expect(grid.at(0, 0).magnitude >= (@as(i32, 1) << 7));
}

test "sign coding LUT index follows significant signed cardinals" {
    const allocator = std.testing.allocator;
    var grid = try CoefficientGrid.init(allocator, 3, 3);
    defer grid.deinit();
    grid.clear();

    grid.markSignificant(0, 1, true, 3, .midpoint);
    grid.markSignificant(1, 0, false, 3, .midpoint);
    grid.markSignificant(2, 1, false, 3, .midpoint);
    grid.markSignificant(1, 2, true, 3, .midpoint);

    const index = signCodingLutIndex(&grid, 1, 1);
    const contribution = signContribution(&grid, 1, 1);
    try std.testing.expectEqual(@as(u8, 0b1110_1011), index);
    try std.testing.expectEqual(@as(i2, 0), contribution.horizontal);
    try std.testing.expectEqual(@as(i2, 0), contribution.vertical);
    try std.testing.expectEqual(@as(u8, 0x09), signCodingStateIndex(&grid, 1, 1));
    try std.testing.expectEqual(@as(u1, 0), signContributionPredictor(contribution));
}

test "sign contribution handles negative-only neighbors without unsigned overflow" {
    const allocator = std.testing.allocator;
    var grid = try CoefficientGrid.init(allocator, 3, 3);
    defer grid.deinit();
    grid.clear();

    grid.markSignificant(0, 1, true, 3, .midpoint);
    grid.markSignificant(1, 0, true, 3, .midpoint);

    const contribution = signContribution(&grid, 1, 1);
    try std.testing.expectEqual(@as(i2, -1), contribution.horizontal);
    try std.testing.expectEqual(@as(i2, -1), contribution.vertical);
}

test "execute bounded mq pass plan against contribution body" {
    const allocator = std.testing.allocator;
    var grid = try CoefficientGrid.init(allocator, 2, 1);
    defer grid.deinit();
    grid.clear();

    var plan = try planContributionPasses(allocator, 0, .ll, 1, 2, 8);
    defer plan.deinit(allocator);
    try executeContributionPassPlanMq(allocator, &grid, &plan, &.{ 0xb2, 0x8a }, .standard, .standard_additive, .midpoint, .standard, .{});
    try std.testing.expect(grid.cells.len == 2);
}

test "CodeBlockStyle fromByte decodes individual style bits" {
    // All bits clear
    {
        const style = CodeBlockStyle.fromByte(0x00);
        try std.testing.expect(!style.bypass);
        try std.testing.expect(!style.reset_context);
        try std.testing.expect(!style.termination);
        try std.testing.expect(!style.vertically_causal);
        try std.testing.expect(!style.segmentation_symbol);
    }
    // Bypass only (bit 0)
    {
        const style = CodeBlockStyle.fromByte(0x01);
        try std.testing.expect(style.bypass);
        try std.testing.expect(!style.reset_context);
        try std.testing.expect(!style.termination);
        try std.testing.expect(!style.vertically_causal);
        try std.testing.expect(!style.segmentation_symbol);
    }
    // Reset context only (bit 1)
    {
        const style = CodeBlockStyle.fromByte(0x02);
        try std.testing.expect(!style.bypass);
        try std.testing.expect(style.reset_context);
        try std.testing.expect(!style.termination);
        try std.testing.expect(!style.vertically_causal);
        try std.testing.expect(!style.segmentation_symbol);
    }
    // Termination only (bit 2)
    {
        const style = CodeBlockStyle.fromByte(0x04);
        try std.testing.expect(!style.bypass);
        try std.testing.expect(!style.reset_context);
        try std.testing.expect(style.termination);
        try std.testing.expect(!style.vertically_causal);
        try std.testing.expect(!style.segmentation_symbol);
    }
    // Vertically causal only (bit 3)
    {
        const style = CodeBlockStyle.fromByte(0x08);
        try std.testing.expect(!style.bypass);
        try std.testing.expect(!style.reset_context);
        try std.testing.expect(!style.termination);
        try std.testing.expect(style.vertically_causal);
        try std.testing.expect(!style.predictable_termination);
        try std.testing.expect(!style.segmentation_symbol);
    }
    // Predictable termination only (bit 4)
    {
        const style = CodeBlockStyle.fromByte(0x10);
        try std.testing.expect(!style.bypass);
        try std.testing.expect(!style.reset_context);
        try std.testing.expect(!style.termination);
        try std.testing.expect(!style.vertically_causal);
        try std.testing.expect(style.predictable_termination);
        try std.testing.expect(!style.segmentation_symbol);
    }
    // Segmentation symbol only (bit 5)
    {
        const style = CodeBlockStyle.fromByte(0x20);
        try std.testing.expect(!style.bypass);
        try std.testing.expect(!style.reset_context);
        try std.testing.expect(!style.termination);
        try std.testing.expect(!style.vertically_causal);
        try std.testing.expect(style.segmentation_symbol);
    }
    // All supported bits set (0x3f = bits 0..5)
    {
        const style = CodeBlockStyle.fromByte(0x3f);
        try std.testing.expect(style.bypass);
        try std.testing.expect(style.reset_context);
        try std.testing.expect(style.termination);
        try std.testing.expect(style.vertically_causal);
        try std.testing.expect(style.predictable_termination);
        try std.testing.expect(style.segmentation_symbol);
    }
    // Round-trip: fromByte and back to u8
    {
        const original: u8 = 0x25; // bypass + termination + segmentation_symbol
        const style = CodeBlockStyle.fromByte(original);
        try std.testing.expect(style.bypass);
        try std.testing.expect(!style.reset_context);
        try std.testing.expect(style.termination);
        try std.testing.expect(!style.vertically_causal);
        try std.testing.expect(style.segmentation_symbol);
        const back: u8 = @bitCast(style);
        try std.testing.expectEqual(original, back);
    }
    // Default NONE constant
    {
        const style = CodeBlockStyle.NONE;
        try std.testing.expectEqual(@as(u8, 0), @as(u8, @bitCast(style)));
    }
}
