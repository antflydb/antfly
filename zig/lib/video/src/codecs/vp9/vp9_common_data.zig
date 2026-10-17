// Copyright (c) 2010, The WebM Project authors. All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// Redistributions of source code must retain the above copyright
//     notice, this list of conditions and the following disclaimer.
//
// Redistributions in binary form must reproduce the above copyright
//     notice, this list of conditions and the following disclaimer in
//     the documentation and/or other materials provided with the
//     distribution.
//
// Neither the name of Google, nor the WebM Project, nor the names
//     of its contributors may be used to endorse or promote products
//     derived from this software without specific prior written
//     permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
//
// Maintained Antfly Zig port; see LICENSE and PORTING.md.
// SPDX-License-Identifier: BSD-3-Clause
// /*
//  *  Copyright (c) 2010 The WebM project authors. All Rights Reserved.
//  *
//  *  Use of this source code is governed by a BSD-style license
//  *  that can be found in the LICENSE file in the root of the source
//  *  tree. An additional intellectual property rights grant can be found
//  *  in the file PATENTS.  All contributing project authors may
//  *  be found in the AUTHORS file in the root of the source tree.
//  */
const common = @import("common.zig");
pub const BLOCK_SIZE = common.BLOCK_SIZE;
pub const PARTITION_CONTEXT = common.PARTITION_CONTEXT;
pub const TX_SIZE = common.TX_SIZE;
pub export const antfly_vp9_num_4x4_blocks_wide_lookup: [13]u8 = [13]u8{
    1,
    1,
    2,
    2,
    2,
    4,
    4,
    4,
    8,
    8,
    8,
    16,
    16,
};
pub export const antfly_vp9_num_4x4_blocks_high_lookup: [13]u8 = [13]u8{
    1,
    2,
    1,
    2,
    4,
    2,
    4,
    8,
    4,
    8,
    16,
    8,
    16,
};
pub export const antfly_vp9_num_8x8_blocks_wide_lookup: [13]u8 = [13]u8{
    1,
    1,
    1,
    1,
    1,
    2,
    2,
    2,
    4,
    4,
    4,
    8,
    8,
};
pub export const antfly_vp9_num_8x8_blocks_high_lookup: [13]u8 = [13]u8{
    1,
    1,
    1,
    1,
    2,
    1,
    2,
    4,
    2,
    4,
    8,
    4,
    8,
};
pub export const antfly_vp9_size_group_lookup: [13]u8 = [13]u8{
    0,
    0,
    0,
    1,
    1,
    1,
    2,
    2,
    2,
    3,
    3,
    3,
    3,
};
pub export var antfly_vp9_subsize_lookup: [4][13]BLOCK_SIZE = [4][13]BLOCK_SIZE{
    [13]BLOCK_SIZE{
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
    },
    [13]BLOCK_SIZE{
        13,
        13,
        13,
        2,
        13,
        13,
        5,
        13,
        13,
        8,
        13,
        13,
        11,
    },
    [13]BLOCK_SIZE{
        13,
        13,
        13,
        1,
        13,
        13,
        4,
        13,
        13,
        7,
        13,
        13,
        10,
    },
    [13]BLOCK_SIZE{
        13,
        13,
        13,
        0,
        13,
        13,
        3,
        13,
        13,
        6,
        13,
        13,
        9,
    },
};
pub export const antfly_vp9_max_txsize_lookup: [13]TX_SIZE = [13]TX_SIZE{
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
};
pub export const antfly_vp9_tx_mode_to_biggest_tx_size: [5]TX_SIZE = [5]TX_SIZE{
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
    @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
};
pub export var antfly_vp9_ss_size_lookup: [13][2][2]BLOCK_SIZE = [13][2][2]BLOCK_SIZE{
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            0,
            13,
        },
        [2]BLOCK_SIZE{
            13,
            13,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            1,
            0,
        },
        [2]BLOCK_SIZE{
            13,
            13,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            2,
            13,
        },
        [2]BLOCK_SIZE{
            0,
            13,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            3,
            2,
        },
        [2]BLOCK_SIZE{
            1,
            0,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            4,
            3,
        },
        [2]BLOCK_SIZE{
            13,
            1,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            5,
            13,
        },
        [2]BLOCK_SIZE{
            3,
            2,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            6,
            5,
        },
        [2]BLOCK_SIZE{
            4,
            3,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            7,
            6,
        },
        [2]BLOCK_SIZE{
            13,
            4,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            8,
            13,
        },
        [2]BLOCK_SIZE{
            6,
            5,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            9,
            8,
        },
        [2]BLOCK_SIZE{
            7,
            6,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            10,
            9,
        },
        [2]BLOCK_SIZE{
            13,
            7,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            11,
            13,
        },
        [2]BLOCK_SIZE{
            9,
            8,
        },
    },
    [2][2]BLOCK_SIZE{
        [2]BLOCK_SIZE{
            12,
            11,
        },
        [2]BLOCK_SIZE{
            10,
            9,
        },
    },
};
pub export var antfly_vp9_uv_txsize_lookup: [13][4][2][2]TX_SIZE = [13][4][2][2]TX_SIZE{
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
        },
    },
    [4][2][2]TX_SIZE{
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 0))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 1))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 2))))),
            },
        },
        [2][2]TX_SIZE{
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
            },
            [2]TX_SIZE{
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
                @as(TX_SIZE, @bitCast(@as(i8, @truncate(@as(c_int, 3))))),
            },
        },
    },
};
const struct_unnamed_2 = extern struct {
    above: PARTITION_CONTEXT = 0,
    left: PARTITION_CONTEXT = 0,
};
pub export const antfly_vp9_partition_context_lookup: [13]struct_unnamed_2 = [13]struct_unnamed_2{
    struct_unnamed_2{
        .above = 15,
        .left = 15,
    },
    struct_unnamed_2{
        .above = 15,
        .left = 14,
    },
    struct_unnamed_2{
        .above = 14,
        .left = 15,
    },
    struct_unnamed_2{
        .above = 14,
        .left = 14,
    },
    struct_unnamed_2{
        .above = 14,
        .left = 12,
    },
    struct_unnamed_2{
        .above = 12,
        .left = 14,
    },
    struct_unnamed_2{
        .above = 12,
        .left = 12,
    },
    struct_unnamed_2{
        .above = 12,
        .left = 8,
    },
    struct_unnamed_2{
        .above = 8,
        .left = 12,
    },
    struct_unnamed_2{
        .above = 8,
        .left = 8,
    },
    struct_unnamed_2{
        .above = 8,
        .left = 0,
    },
    struct_unnamed_2{
        .above = 0,
        .left = 8,
    },
    struct_unnamed_2{
        .above = 0,
        .left = 0,
    },
};
