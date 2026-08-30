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
const DecodeLimits = @import("limits.zig").DecodeLimits;

const Allocator = std.mem.Allocator;

const riff_header_len = 12;
const chunk_header_len = 8;
const vp8_frame_tag_len = 3;
const vp8_keyframe_header_len = 10;

const vp8x_flag_icc: u8 = 0x20;
const vp8x_flag_alpha: u8 = 0x10;
const vp8x_flag_exif: u8 = 0x08;
const vp8x_flag_xmp: u8 = 0x04;
const vp8x_flag_animation: u8 = 0x02;
const vp8x_reserved_mask: u8 = 0xc1;
const vp8_transform_ac3_c1: i32 = 20091;
const vp8_transform_ac3_c2: i32 = 35468;
const vp8_coeff_type_count = 4;
const vp8_coeff_band_count = 8;
const vp8_coeff_context_count = 3;
const vp8_coeff_proba_count = 11;
const vp8_coeff_block_size = 16;
const vp8_coeff_probs_flat_count = vp8_coeff_type_count * vp8_coeff_band_count * vp8_coeff_context_count * vp8_coeff_proba_count;
const vp8_zigzag = [_]u4{ 0, 1, 4, 8, 5, 2, 3, 6, 9, 12, 13, 10, 7, 11, 14, 15 };
const vp8_coeff_bands = [_]u3{ 0, 1, 2, 3, 6, 4, 5, 6, 6, 6, 6, 6, 6, 6, 6, 7, 0 };
const vp8_coeff_cat3 = [_]u8{ 173, 148, 140 };
const vp8_coeff_cat4 = [_]u8{ 176, 155, 140, 135 };
const vp8_coeff_cat5 = [_]u8{ 180, 157, 141, 134, 130 };
const vp8_coeff_cat6 = [_]u8{ 254, 254, 243, 230, 196, 177, 153, 140, 133, 130, 129 };
const vp8_dc_quant_table = [_]u16{
    4,   5,   6,   7,   8,   9,   10,  10,  11,  12,  13,  14,  15,  16,  17,  17,
    18,  19,  20,  20,  21,  21,  22,  22,  23,  23,  24,  25,  25,  26,  27,  28,
    29,  30,  31,  32,  33,  34,  35,  36,  37,  37,  38,  39,  40,  41,  42,  43,
    44,  45,  46,  46,  47,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  58,
    59,  60,  61,  62,  63,  64,  65,  66,  67,  68,  69,  70,  71,  72,  73,  74,
    75,  76,  76,  77,  78,  79,  80,  81,  82,  83,  84,  85,  86,  87,  88,  89,
    91,  93,  95,  96,  98,  100, 101, 102, 104, 106, 108, 110, 112, 114, 116, 118,
    122, 124, 126, 128, 130, 132, 134, 136, 138, 140, 143, 145, 148, 151, 154, 157,
};
const vp8_ac_quant_table = [_]u16{
    4,   5,   6,   7,   8,   9,   10,  11,  12,  13,  14,  15,  16,  17,  18,  19,
    20,  21,  22,  23,  24,  25,  26,  27,  28,  29,  30,  31,  32,  33,  34,  35,
    36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,  49,  50,  51,
    52,  53,  54,  55,  56,  57,  58,  60,  62,  64,  66,  68,  70,  72,  74,  76,
    78,  80,  82,  84,  86,  88,  90,  92,  94,  96,  98,  100, 102, 104, 106, 108,
    110, 112, 114, 116, 119, 122, 125, 128, 131, 134, 137, 140, 143, 146, 149, 152,
    155, 158, 161, 164, 167, 170, 173, 177, 181, 185, 189, 193, 197, 201, 205, 209,
    213, 217, 221, 225, 229, 234, 239, 245, 249, 254, 259, 264, 269, 274, 279, 284,
};
const vp8_default_coeff_probs_flat = [_]u8{
    128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
    128, 253, 136, 254, 255, 228, 219, 128, 128, 128, 128, 128, 189, 129, 242, 255, 227, 213, 255, 219, 128, 128, 128, 106, 126, 227, 252, 214, 209, 255, 255, 128,
    128, 128, 1,   98,  248, 255, 236, 226, 255, 255, 128, 128, 128, 181, 133, 238, 254, 221, 234, 255, 154, 128, 128, 128, 78,  134, 202, 247, 198, 180, 255, 219,
    128, 128, 128, 1,   185, 249, 255, 243, 255, 128, 128, 128, 128, 128, 184, 150, 247, 255, 236, 224, 128, 128, 128, 128, 128, 77,  110, 216, 255, 236, 230, 128,
    128, 128, 128, 128, 1,   101, 251, 255, 241, 255, 128, 128, 128, 128, 128, 170, 139, 241, 252, 236, 209, 255, 255, 128, 128, 128, 37,  116, 196, 243, 228, 255,
    255, 255, 128, 128, 128, 1,   204, 254, 255, 245, 255, 128, 128, 128, 128, 128, 207, 160, 250, 255, 238, 128, 128, 128, 128, 128, 128, 102, 103, 231, 255, 211,
    171, 128, 128, 128, 128, 128, 1,   152, 252, 255, 240, 255, 128, 128, 128, 128, 128, 177, 135, 243, 255, 234, 225, 128, 128, 128, 128, 128, 80,  129, 211, 255,
    194, 224, 128, 128, 128, 128, 128, 1,   1,   255, 128, 128, 128, 128, 128, 128, 128, 128, 246, 1,   255, 128, 128, 128, 128, 128, 128, 128, 128, 255, 128, 128,
    128, 128, 128, 128, 128, 128, 128, 128, 198, 35,  237, 223, 193, 187, 162, 160, 145, 155, 62,  131, 45,  198, 221, 172, 176, 220, 157, 252, 221, 1,   68,  47,
    146, 208, 149, 167, 221, 162, 255, 223, 128, 1,   149, 241, 255, 221, 224, 255, 255, 128, 128, 128, 184, 141, 234, 253, 222, 220, 255, 199, 128, 128, 128, 81,
    99,  181, 242, 176, 190, 249, 202, 255, 255, 128, 1,   129, 232, 253, 214, 197, 242, 196, 255, 255, 128, 99,  121, 210, 250, 201, 198, 255, 202, 128, 128, 128,
    23,  91,  163, 242, 170, 187, 247, 210, 255, 255, 128, 1,   200, 246, 255, 234, 255, 128, 128, 128, 128, 128, 109, 178, 241, 255, 231, 245, 255, 255, 128, 128,
    128, 44,  130, 201, 253, 205, 192, 255, 255, 128, 128, 128, 1,   132, 239, 251, 219, 209, 255, 165, 128, 128, 128, 94,  136, 225, 251, 218, 190, 255, 255, 128,
    128, 128, 22,  100, 174, 245, 186, 161, 255, 199, 128, 128, 128, 1,   182, 249, 255, 232, 235, 128, 128, 128, 128, 128, 124, 143, 241, 255, 227, 234, 128, 128,
    128, 128, 128, 35,  77,  181, 251, 193, 211, 255, 205, 128, 128, 128, 1,   157, 247, 255, 236, 231, 255, 255, 128, 128, 128, 121, 141, 235, 255, 225, 227, 255,
    255, 128, 128, 128, 45,  99,  188, 251, 195, 217, 255, 224, 128, 128, 128, 1,   1,   251, 255, 213, 255, 128, 128, 128, 128, 128, 203, 1,   248, 255, 255, 128,
    128, 128, 128, 128, 128, 137, 1,   177, 255, 224, 255, 128, 128, 128, 128, 128, 253, 9,   248, 251, 207, 208, 255, 192, 128, 128, 128, 175, 13,  224, 243, 193,
    185, 249, 198, 255, 255, 128, 73,  17,  171, 221, 161, 179, 236, 167, 255, 234, 128, 1,   95,  247, 253, 212, 183, 255, 255, 128, 128, 128, 239, 90,  244, 250,
    211, 209, 255, 255, 128, 128, 128, 155, 77,  195, 248, 188, 195, 255, 255, 128, 128, 128, 1,   24,  239, 251, 218, 219, 255, 205, 128, 128, 128, 201, 51,  219,
    255, 196, 186, 128, 128, 128, 128, 128, 69,  46,  190, 239, 201, 218, 255, 228, 128, 128, 128, 1,   191, 251, 255, 255, 128, 128, 128, 128, 128, 128, 223, 165,
    249, 255, 213, 255, 128, 128, 128, 128, 128, 141, 124, 248, 255, 255, 128, 128, 128, 128, 128, 128, 1,   16,  248, 255, 255, 128, 128, 128, 128, 128, 128, 190,
    36,  230, 255, 236, 255, 128, 128, 128, 128, 128, 149, 1,   255, 128, 128, 128, 128, 128, 128, 128, 128, 1,   226, 255, 128, 128, 128, 128, 128, 128, 128, 128,
    247, 192, 255, 128, 128, 128, 128, 128, 128, 128, 128, 240, 128, 255, 128, 128, 128, 128, 128, 128, 128, 128, 1,   134, 252, 255, 255, 128, 128, 128, 128, 128,
    128, 213, 62,  250, 255, 255, 128, 128, 128, 128, 128, 128, 55,  93,  255, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
    128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 202, 24,  213, 235, 186, 191, 220, 160,
    240, 175, 255, 126, 38,  182, 232, 169, 184, 228, 174, 255, 187, 128, 61,  46,  138, 219, 151, 178, 240, 170, 255, 216, 128, 1,   112, 230, 250, 199, 191, 247,
    159, 255, 255, 128, 166, 109, 228, 252, 211, 215, 255, 174, 128, 128, 128, 39,  77,  162, 232, 172, 180, 245, 178, 255, 255, 128, 1,   52,  220, 246, 198, 199,
    249, 220, 255, 255, 128, 124, 74,  191, 243, 183, 193, 250, 221, 255, 255, 128, 24,  71,  130, 219, 154, 170, 243, 182, 255, 255, 128, 1,   182, 225, 249, 219,
    240, 255, 224, 128, 128, 128, 149, 150, 226, 252, 216, 205, 255, 171, 128, 128, 128, 28,  108, 170, 242, 183, 194, 254, 223, 255, 255, 128, 1,   81,  230, 252,
    204, 203, 255, 192, 128, 128, 128, 123, 102, 209, 247, 188, 196, 255, 233, 128, 128, 128, 20,  95,  153, 243, 164, 173, 255, 203, 128, 128, 128, 1,   222, 248,
    255, 216, 213, 128, 128, 128, 128, 128, 168, 175, 246, 252, 235, 205, 255, 255, 128, 128, 128, 47,  116, 215, 255, 211, 212, 255, 255, 128, 128, 128, 1,   121,
    236, 253, 212, 214, 255, 255, 128, 128, 128, 141, 84,  213, 252, 201, 202, 255, 219, 128, 128, 128, 42,  80,  160, 240, 162, 185, 255, 205, 128, 128, 128, 1,
    1,   255, 128, 128, 128, 128, 128, 128, 128, 128, 244, 1,   255, 128, 128, 128, 128, 128, 128, 128, 128, 238, 1,   255, 128, 128, 128, 128, 128, 128, 128, 128,
};

comptime {
    std.debug.assert(vp8_default_coeff_probs_flat.len == vp8_coeff_probs_flat_count);
}

const Vp8CoeffProbOverride = struct {
    index: u16,
    value: u8,
};

const vp8_coeff_update_prob_overrides = [_]Vp8CoeffProbOverride{
    .{ .index = 33, .value = 176 },   .{ .index = 34, .value = 246 },   .{ .index = 44, .value = 223 },  .{ .index = 45, .value = 241 },
    .{ .index = 46, .value = 252 },   .{ .index = 55, .value = 249 },   .{ .index = 56, .value = 253 },  .{ .index = 57, .value = 253 },
    .{ .index = 67, .value = 244 },   .{ .index = 68, .value = 252 },   .{ .index = 77, .value = 234 },  .{ .index = 78, .value = 254 },
    .{ .index = 79, .value = 254 },   .{ .index = 88, .value = 253 },   .{ .index = 100, .value = 246 }, .{ .index = 101, .value = 254 },
    .{ .index = 110, .value = 239 },  .{ .index = 111, .value = 253 },  .{ .index = 112, .value = 254 }, .{ .index = 121, .value = 254 },
    .{ .index = 123, .value = 254 },  .{ .index = 133, .value = 248 },  .{ .index = 134, .value = 254 }, .{ .index = 143, .value = 251 },
    .{ .index = 145, .value = 254 },  .{ .index = 166, .value = 253 },  .{ .index = 167, .value = 254 }, .{ .index = 176, .value = 251 },
    .{ .index = 177, .value = 254 },  .{ .index = 178, .value = 254 },  .{ .index = 187, .value = 254 }, .{ .index = 189, .value = 254 },
    .{ .index = 199, .value = 254 },  .{ .index = 200, .value = 253 },  .{ .index = 202, .value = 254 }, .{ .index = 209, .value = 250 },
    .{ .index = 211, .value = 254 },  .{ .index = 213, .value = 254 },  .{ .index = 220, .value = 254 }, .{ .index = 264, .value = 217 },
    .{ .index = 275, .value = 225 },  .{ .index = 276, .value = 252 },  .{ .index = 277, .value = 241 }, .{ .index = 278, .value = 253 },
    .{ .index = 281, .value = 254 },  .{ .index = 286, .value = 234 },  .{ .index = 287, .value = 250 }, .{ .index = 288, .value = 241 },
    .{ .index = 289, .value = 250 },  .{ .index = 290, .value = 253 },  .{ .index = 292, .value = 253 }, .{ .index = 293, .value = 254 },
    .{ .index = 298, .value = 254 },  .{ .index = 308, .value = 223 },  .{ .index = 309, .value = 254 }, .{ .index = 310, .value = 254 },
    .{ .index = 319, .value = 238 },  .{ .index = 320, .value = 253 },  .{ .index = 321, .value = 254 }, .{ .index = 322, .value = 254 },
    .{ .index = 331, .value = 248 },  .{ .index = 332, .value = 254 },  .{ .index = 341, .value = 249 }, .{ .index = 342, .value = 254 },
    .{ .index = 364, .value = 253 },  .{ .index = 374, .value = 247 },  .{ .index = 375, .value = 254 }, .{ .index = 397, .value = 253 },
    .{ .index = 398, .value = 254 },  .{ .index = 407, .value = 252 },  .{ .index = 430, .value = 254 }, .{ .index = 431, .value = 254 },
    .{ .index = 440, .value = 253 },  .{ .index = 463, .value = 254 },  .{ .index = 464, .value = 253 }, .{ .index = 473, .value = 250 },
    .{ .index = 484, .value = 254 },  .{ .index = 528, .value = 186 },  .{ .index = 529, .value = 251 }, .{ .index = 530, .value = 250 },
    .{ .index = 539, .value = 234 },  .{ .index = 540, .value = 251 },  .{ .index = 541, .value = 244 }, .{ .index = 542, .value = 254 },
    .{ .index = 550, .value = 251 },  .{ .index = 551, .value = 251 },  .{ .index = 552, .value = 243 }, .{ .index = 553, .value = 253 },
    .{ .index = 554, .value = 254 },  .{ .index = 556, .value = 254 },  .{ .index = 562, .value = 253 }, .{ .index = 563, .value = 254 },
    .{ .index = 572, .value = 236 },  .{ .index = 573, .value = 253 },  .{ .index = 574, .value = 254 }, .{ .index = 583, .value = 251 },
    .{ .index = 584, .value = 253 },  .{ .index = 585, .value = 253 },  .{ .index = 586, .value = 254 }, .{ .index = 587, .value = 254 },
    .{ .index = 595, .value = 254 },  .{ .index = 596, .value = 254 },  .{ .index = 605, .value = 254 }, .{ .index = 606, .value = 254 },
    .{ .index = 607, .value = 254 },  .{ .index = 628, .value = 254 },  .{ .index = 638, .value = 254 }, .{ .index = 639, .value = 254 },
    .{ .index = 649, .value = 254 },  .{ .index = 671, .value = 254 },  .{ .index = 792, .value = 248 }, .{ .index = 803, .value = 250 },
    .{ .index = 804, .value = 254 },  .{ .index = 805, .value = 252 },  .{ .index = 806, .value = 254 }, .{ .index = 814, .value = 248 },
    .{ .index = 815, .value = 254 },  .{ .index = 816, .value = 249 },  .{ .index = 817, .value = 253 }, .{ .index = 826, .value = 253 },
    .{ .index = 827, .value = 253 },  .{ .index = 836, .value = 246 },  .{ .index = 837, .value = 253 }, .{ .index = 838, .value = 253 },
    .{ .index = 847, .value = 252 },  .{ .index = 848, .value = 254 },  .{ .index = 849, .value = 251 }, .{ .index = 850, .value = 254 },
    .{ .index = 851, .value = 254 },  .{ .index = 859, .value = 254 },  .{ .index = 860, .value = 252 }, .{ .index = 869, .value = 248 },
    .{ .index = 870, .value = 254 },  .{ .index = 871, .value = 253 },  .{ .index = 880, .value = 253 }, .{ .index = 882, .value = 254 },
    .{ .index = 883, .value = 254 },  .{ .index = 892, .value = 251 },  .{ .index = 893, .value = 254 }, .{ .index = 902, .value = 245 },
    .{ .index = 903, .value = 251 },  .{ .index = 904, .value = 254 },  .{ .index = 913, .value = 253 }, .{ .index = 914, .value = 253 },
    .{ .index = 915, .value = 254 },  .{ .index = 925, .value = 251 },  .{ .index = 926, .value = 253 }, .{ .index = 935, .value = 252 },
    .{ .index = 936, .value = 253 },  .{ .index = 937, .value = 254 },  .{ .index = 947, .value = 254 }, .{ .index = 958, .value = 252 },
    .{ .index = 968, .value = 249 },  .{ .index = 970, .value = 254 },  .{ .index = 981, .value = 254 }, .{ .index = 992, .value = 253 },
    .{ .index = 1001, .value = 250 }, .{ .index = 1034, .value = 254 },
};

const vp8_default_luma4_probs = Vp8Luma4Probs{
    .{
        .{ 231, 120, 48, 89, 115, 113, 120, 152, 112 },
        .{ 152, 179, 64, 126, 170, 118, 46, 70, 95 },
        .{ 175, 69, 143, 80, 85, 82, 72, 155, 103 },
        .{ 56, 58, 10, 171, 218, 189, 17, 13, 152 },
        .{ 144, 71, 10, 38, 171, 213, 144, 34, 26 },
        .{ 114, 26, 17, 163, 44, 195, 21, 10, 173 },
        .{ 121, 24, 80, 195, 26, 62, 44, 64, 85 },
        .{ 170, 46, 55, 19, 136, 160, 33, 206, 71 },
        .{ 63, 20, 8, 114, 114, 208, 12, 9, 226 },
        .{ 81, 40, 11, 96, 182, 84, 29, 16, 36 },
    },
    .{
        .{ 134, 183, 89, 137, 98, 101, 106, 165, 148 },
        .{ 72, 187, 100, 130, 157, 111, 32, 75, 80 },
        .{ 66, 102, 167, 99, 74, 62, 40, 234, 128 },
        .{ 41, 53, 9, 178, 241, 141, 26, 8, 107 },
        .{ 104, 79, 12, 27, 217, 255, 87, 17, 7 },
        .{ 74, 43, 26, 146, 73, 166, 49, 23, 157 },
        .{ 65, 38, 105, 160, 51, 52, 31, 115, 128 },
        .{ 87, 68, 71, 44, 114, 51, 15, 186, 23 },
        .{ 47, 41, 14, 110, 182, 183, 21, 17, 194 },
        .{ 66, 45, 25, 102, 197, 189, 23, 18, 22 },
    },
    .{
        .{ 88, 88, 147, 150, 42, 46, 45, 196, 205 },
        .{ 43, 97, 183, 117, 85, 38, 35, 179, 61 },
        .{ 39, 53, 200, 87, 26, 21, 43, 232, 171 },
        .{ 56, 34, 51, 104, 114, 102, 29, 93, 77 },
        .{ 107, 54, 32, 26, 51, 1, 81, 43, 31 },
        .{ 39, 28, 85, 171, 58, 165, 90, 98, 64 },
        .{ 34, 22, 116, 206, 23, 34, 43, 166, 73 },
        .{ 68, 25, 106, 22, 64, 171, 36, 225, 114 },
        .{ 34, 19, 21, 102, 132, 188, 16, 76, 124 },
        .{ 62, 18, 78, 95, 85, 57, 50, 48, 51 },
    },
    .{
        .{ 193, 101, 35, 159, 215, 111, 89, 46, 111 },
        .{ 60, 148, 31, 172, 219, 228, 21, 18, 111 },
        .{ 112, 113, 77, 85, 179, 255, 38, 120, 114 },
        .{ 40, 42, 1, 196, 245, 209, 10, 25, 109 },
        .{ 100, 80, 8, 43, 154, 1, 51, 26, 71 },
        .{ 88, 43, 29, 140, 166, 213, 37, 43, 154 },
        .{ 61, 63, 30, 155, 67, 45, 68, 1, 209 },
        .{ 142, 78, 78, 16, 255, 128, 34, 197, 171 },
        .{ 41, 40, 5, 102, 211, 183, 4, 1, 221 },
        .{ 51, 50, 17, 168, 209, 192, 23, 25, 82 },
    },
    .{
        .{ 125, 98, 42, 88, 104, 85, 117, 175, 82 },
        .{ 95, 84, 53, 89, 128, 100, 113, 101, 45 },
        .{ 75, 79, 123, 47, 51, 128, 81, 171, 1 },
        .{ 57, 17, 5, 71, 102, 57, 53, 41, 49 },
        .{ 115, 21, 2, 10, 102, 255, 166, 23, 6 },
        .{ 38, 33, 13, 121, 57, 73, 26, 1, 85 },
        .{ 41, 10, 67, 138, 77, 110, 90, 47, 114 },
        .{ 101, 29, 16, 10, 85, 128, 101, 196, 26 },
        .{ 57, 18, 10, 102, 102, 213, 34, 20, 43 },
        .{ 117, 20, 15, 36, 163, 128, 68, 1, 26 },
    },
    .{
        .{ 138, 31, 36, 171, 27, 166, 38, 44, 229 },
        .{ 67, 87, 58, 169, 82, 115, 26, 59, 179 },
        .{ 63, 59, 90, 180, 59, 166, 93, 73, 154 },
        .{ 40, 40, 21, 116, 143, 209, 34, 39, 175 },
        .{ 57, 46, 22, 24, 128, 1, 54, 17, 37 },
        .{ 47, 15, 16, 183, 34, 223, 49, 45, 183 },
        .{ 46, 17, 33, 183, 6, 98, 15, 32, 183 },
        .{ 65, 32, 73, 115, 28, 128, 23, 128, 205 },
        .{ 40, 3, 9, 115, 51, 192, 18, 6, 223 },
        .{ 87, 37, 9, 115, 59, 77, 64, 21, 47 },
    },
    .{
        .{ 104, 55, 44, 218, 9, 54, 53, 130, 226 },
        .{ 64, 90, 70, 205, 40, 41, 23, 26, 57 },
        .{ 54, 57, 112, 184, 5, 41, 38, 166, 213 },
        .{ 30, 34, 26, 133, 152, 116, 10, 32, 134 },
        .{ 75, 32, 12, 51, 192, 255, 160, 43, 51 },
        .{ 39, 19, 53, 221, 26, 114, 32, 73, 255 },
        .{ 31, 9, 65, 234, 2, 15, 1, 118, 73 },
        .{ 88, 31, 35, 67, 102, 85, 55, 186, 85 },
        .{ 56, 21, 23, 111, 59, 205, 45, 37, 192 },
        .{ 55, 38, 70, 124, 73, 102, 1, 34, 98 },
    },
    .{
        .{ 102, 61, 71, 37, 34, 53, 31, 243, 192 },
        .{ 69, 60, 71, 38, 73, 119, 28, 222, 37 },
        .{ 68, 45, 128, 34, 1, 47, 11, 245, 171 },
        .{ 62, 17, 19, 70, 146, 85, 55, 62, 70 },
        .{ 75, 15, 9, 9, 64, 255, 184, 119, 16 },
        .{ 37, 43, 37, 154, 100, 163, 85, 160, 1 },
        .{ 63, 9, 92, 136, 28, 64, 32, 201, 85 },
        .{ 86, 6, 28, 5, 64, 255, 25, 248, 1 },
        .{ 56, 8, 17, 132, 137, 255, 55, 116, 128 },
        .{ 58, 15, 20, 82, 135, 57, 26, 121, 40 },
    },
    .{
        .{ 164, 50, 31, 137, 154, 133, 25, 35, 218 },
        .{ 51, 103, 44, 131, 131, 123, 31, 6, 158 },
        .{ 86, 40, 64, 135, 148, 224, 45, 183, 128 },
        .{ 22, 26, 17, 131, 240, 154, 14, 1, 209 },
        .{ 83, 12, 13, 54, 192, 255, 68, 47, 28 },
        .{ 45, 16, 21, 91, 64, 222, 7, 1, 197 },
        .{ 56, 21, 39, 155, 60, 138, 23, 102, 213 },
        .{ 85, 26, 85, 85, 128, 128, 32, 146, 171 },
        .{ 18, 11, 7, 63, 144, 171, 4, 4, 246 },
        .{ 35, 27, 10, 146, 174, 171, 12, 26, 128 },
    },
    .{
        .{ 190, 80, 35, 99, 180, 80, 126, 54, 45 },
        .{ 85, 126, 47, 87, 176, 51, 41, 20, 32 },
        .{ 101, 75, 128, 139, 118, 146, 116, 128, 85 },
        .{ 56, 41, 15, 176, 236, 85, 37, 9, 62 },
        .{ 146, 36, 19, 30, 171, 255, 97, 27, 20 },
        .{ 71, 30, 17, 119, 118, 255, 17, 18, 138 },
        .{ 101, 38, 60, 138, 55, 70, 43, 26, 142 },
        .{ 138, 45, 61, 62, 219, 1, 81, 188, 64 },
        .{ 32, 41, 20, 117, 151, 142, 20, 21, 163 },
        .{ 112, 19, 12, 61, 195, 128, 48, 4, 24 },
    },
};

pub const DecodedImage = struct {
    rgba: []u8,
    width: u32,
    height: u32,
};

pub const Bitstream = enum {
    vp8,
    vp8l,
};

pub const Info = struct {
    bitstream: ?Bitstream = null,
    width: ?u32 = null,
    height: ?u32 = null,
    extended: bool = false,
    alpha: bool = false,
    animated: bool = false,
    icc: bool = false,
    exif: bool = false,
    xmp: bool = false,
};

const ParsedChunks = struct {
    info: Info = .{},
    vp8_payload: ?[]const u8 = null,
    vp8l_payload: ?[]const u8 = null,
    alph_payload: ?[]const u8 = null,
};

pub fn hasSignature(bytes: []const u8) bool {
    return bytes.len >= riff_header_len and
        std.mem.eql(u8, bytes[0..4], "RIFF") and
        std.mem.eql(u8, bytes[8..12], "WEBP");
}

pub fn decodeRgba(alloc: Allocator, webp_bytes: []const u8) !DecodedImage {
    return try decodeRgbaChecked(alloc, webp_bytes, null);
}

pub fn decodeRgbaLimited(alloc: Allocator, webp_bytes: []const u8, limits: DecodeLimits) !DecodedImage {
    return try decodeRgbaChecked(alloc, webp_bytes, limits);
}

fn decodeRgbaChecked(alloc: Allocator, webp_bytes: []const u8, limits: ?DecodeLimits) !DecodedImage {
    const parsed = try parseChunks(webp_bytes);
    if (parsed.info.animated) return error.AnimatedWebpUnsupported;
    if (limits) |limit| {
        const width = parsed.info.width orelse return error.WebpDecodeFailed;
        const height = parsed.info.height orelse return error.WebpDecodeFailed;
        try limit.validate(width, height);
    }
    if (parsed.vp8l_payload) |payload| return try decodeVp8lRgba(alloc, payload);
    if (parsed.vp8_payload) |payload| {
        var decoded = try decodeVp8Rgba(alloc, payload);
        errdefer alloc.free(decoded.rgba);
        if (parsed.alph_payload) |alpha_payload| {
            const alpha = try decodeAlphPlane(alloc, alpha_payload, decoded.width, decoded.height);
            defer alloc.free(alpha);
            try composeAlphaPlane(&decoded, alpha);
        } else if (parsed.info.alpha) {
            return error.WebpDecodeFailed;
        }
        return decoded;
    }
    if (parsed.info.alpha) return error.UnsupportedWebpFormat;
    return error.UnsupportedWebpFormat;
}

pub fn probe(webp_bytes: []const u8) !Info {
    return (try parseChunks(webp_bytes)).info;
}

fn parseChunks(webp_bytes: []const u8) !ParsedChunks {
    if (!hasSignature(webp_bytes)) return error.WebpDecodeFailed;

    const declared_payload_len = try readU32Le(webp_bytes, 4);
    const riff_len = @as(usize, declared_payload_len) + 8;
    if (riff_len < riff_header_len or riff_len > webp_bytes.len) return error.WebpDecodeFailed;

    var parsed = ParsedChunks{};
    var cursor: usize = riff_header_len;
    var saw_vp8x = false;
    var vp8x_alpha = false;
    var saw_alph = false;
    var saw_image = false;
    var saw_animation_control = false;
    var saw_animation_frame = false;

    while (cursor < riff_len) {
        if (riff_len - cursor < chunk_header_len) return error.WebpDecodeFailed;
        const fourcc = webp_bytes[cursor .. cursor + 4];
        const chunk_size = try readU32Le(webp_bytes, cursor + 4);
        const payload_start = cursor + chunk_header_len;
        const payload_len: usize = @intCast(chunk_size);
        if (payload_len > riff_len - payload_start) return error.WebpDecodeFailed;
        const payload = webp_bytes[payload_start .. payload_start + payload_len];

        if (std.mem.eql(u8, fourcc, "VP8X")) {
            if (saw_vp8x or saw_image or saw_alph) return error.WebpDecodeFailed;
            saw_vp8x = true;
            try parseVp8x(payload, &parsed.info);
            vp8x_alpha = parsed.info.alpha;
        } else if (std.mem.eql(u8, fourcc, "ALPH")) {
            if (saw_alph or saw_image) return error.WebpDecodeFailed;
            saw_alph = true;
            parsed.alph_payload = payload;
        } else if (std.mem.eql(u8, fourcc, "VP8 ")) {
            if (saw_image) return error.WebpDecodeFailed;
            saw_image = true;
            parsed.info.bitstream = .vp8;
            parsed.vp8_payload = payload;
            const dim = try parseVp8Dimensions(payload);
            try mergeDimensions(&parsed.info, dim.width, dim.height);
        } else if (std.mem.eql(u8, fourcc, "VP8L")) {
            if (saw_image) return error.WebpDecodeFailed;
            saw_image = true;
            parsed.info.bitstream = .vp8l;
            parsed.vp8l_payload = payload;
            const header = try parseVp8lHeader(payload);
            if (saw_vp8x and header.alpha != vp8x_alpha) return error.WebpDecodeFailed;
            parsed.info.alpha = parsed.info.alpha or header.alpha;
            try mergeDimensions(&parsed.info, header.width, header.height);
        } else if (std.mem.eql(u8, fourcc, "ANIM")) {
            if (!saw_vp8x or !parsed.info.animated) return error.WebpDecodeFailed;
            if (saw_animation_control or saw_animation_frame) return error.WebpDecodeFailed;
            if (payload.len != 6) return error.WebpDecodeFailed;
            saw_animation_control = true;
        } else if (std.mem.eql(u8, fourcc, "ANMF")) {
            if (!saw_vp8x or !parsed.info.animated) return error.WebpDecodeFailed;
            if (!saw_animation_control) return error.WebpDecodeFailed;
            if (payload.len < 16) return error.WebpDecodeFailed;
            saw_animation_frame = true;
        }

        const padded_len = payload_len + (payload_len & 1);
        cursor = payload_start + padded_len;
    }

    if (cursor != riff_len) return error.WebpDecodeFailed;
    if (parsed.info.animated) {
        if (saw_image) return error.WebpDecodeFailed;
        if (!saw_animation_control or !saw_animation_frame) return error.WebpDecodeFailed;
    } else if (!saw_image) return error.WebpDecodeFailed;
    if (saw_alph and !saw_vp8x) return error.WebpDecodeFailed;
    if (saw_alph and !vp8x_alpha) return error.WebpDecodeFailed;
    if (saw_alph and parsed.info.bitstream != .vp8) return error.WebpDecodeFailed;
    if (saw_vp8x and parsed.info.alpha and !saw_alph and parsed.info.bitstream == .vp8) return error.WebpDecodeFailed;

    return parsed;
}

fn parseVp8x(payload: []const u8, info: *Info) !void {
    if (payload.len != 10) return error.WebpDecodeFailed;
    const flags = payload[0];
    if ((flags & vp8x_reserved_mask) != 0) return error.UnsupportedWebpFormat;
    if (payload[1] != 0 or payload[2] != 0 or payload[3] != 0) return error.UnsupportedWebpFormat;
    info.extended = true;
    info.icc = (flags & vp8x_flag_icc) != 0;
    info.alpha = (flags & vp8x_flag_alpha) != 0;
    info.exif = (flags & vp8x_flag_exif) != 0;
    info.xmp = (flags & vp8x_flag_xmp) != 0;
    info.animated = (flags & vp8x_flag_animation) != 0;
    info.width = try readU24LePlusOne(payload, 4);
    info.height = try readU24LePlusOne(payload, 7);
}

fn parseVp8Dimensions(payload: []const u8) !struct { width: u32, height: u32 } {
    const header = try parseVp8FrameHeader(payload);
    return .{
        .width = header.width,
        .height = header.height,
    };
}

const Vp8FrameHeader = struct {
    keyframe: bool,
    version: u3,
    show_frame: bool,
    first_partition_size: u32,
    width: u32,
    height: u32,
    horizontal_scale: u2,
    vertical_scale: u2,
};

const Vp8PartitionInfo = struct {
    frame: Vp8FrameHeader,
    first_partition: []const u8,
    token_partitions: []const []const u8,
};

const Vp8KeyframeInfo = struct {
    partitions: Vp8PartitionInfo,
    syntax: Vp8FirstPartitionSyntax,
};

const Vp8SegmentationHeader = struct {
    enabled: bool = false,
    update_map: bool = false,
    update_data: bool = false,
    absolute_delta: bool = true,
    quantizer: [4]i16 = .{ 0, 0, 0, 0 },
    filter_strength: [4]i16 = .{ 0, 0, 0, 0 },
    segment_probs: [3]u8 = .{ 255, 255, 255 },
};

const Vp8LoopFilterHeader = struct {
    simple: bool = false,
    level: u6 = 0,
    sharpness: u3 = 0,
    use_lf_delta: bool = false,
    ref_lf_delta: [4]i16 = .{ 0, 0, 0, 0 },
    mode_lf_delta: [4]i16 = .{ 0, 0, 0, 0 },
};

const Vp8QuantHeader = struct {
    base_q: u7,
    y1_dc_delta: i16 = 0,
    y2_dc_delta: i16 = 0,
    y2_ac_delta: i16 = 0,
    uv_dc_delta: i16 = 0,
    uv_ac_delta: i16 = 0,
};

const Vp8FirstPartitionSyntax = struct {
    color_space: bool,
    clamp_type: bool,
    segmentation: Vp8SegmentationHeader,
    loop_filter: Vp8LoopFilterHeader,
    token_partition_count: usize,
    quant: Vp8QuantHeader,
    refresh_entropy_probs: bool,
};

const Vp8MacroblockGrid = struct {
    width: u32,
    height: u32,
};

const Vp8FramePlanes = struct {
    y: []u8,
    u: []u8,
    v: []u8,
    width: u32,
    height: u32,
    y_stride: usize,
    uv_stride: usize,
    y_padded_height: usize,
    uv_padded_height: usize,

    fn deinit(self: *Vp8FramePlanes, alloc: Allocator) void {
        alloc.free(self.y);
        alloc.free(self.u);
        alloc.free(self.v);
        self.* = undefined;
    }
};

const Vp8CoeffProbs = [vp8_coeff_type_count][vp8_coeff_band_count][vp8_coeff_context_count][vp8_coeff_proba_count]u8;

const Vp8CoeffBlock = struct {
    coeffs: [vp8_coeff_block_size]i16 = @splat(0),
    last_nonzero_plus_one: u5 = 0,
};

const Vp8MacroblockCoeffs = struct {
    y2: Vp8CoeffBlock = .{},
    y: [16]Vp8CoeffBlock = @as([16]Vp8CoeffBlock, @splat(.{})),
    u: [4]Vp8CoeffBlock = @as([4]Vp8CoeffBlock, @splat(.{})),
    v: [4]Vp8CoeffBlock = @as([4]Vp8CoeffBlock, @splat(.{})),
};

const Vp8MacroblockFilterInfo = struct {
    segment: u2 = 0,
    is_i4x4: bool = false,
    has_coeffs: bool = false,
};

const Vp8LoopFilterParams = struct {
    edge_limit: i32,
    interior_limit: i32,
    hev_threshold: i32,
};

const Vp8MacroblockTokenContext = struct {
    y_above: [4]u1 = .{ 0, 0, 0, 0 },
    y_left: [4]u1 = .{ 0, 0, 0, 0 },
    y2_above: u1 = 0,
    y2_left: u1 = 0,
    u_above: [2]u1 = .{ 0, 0 },
    u_left: [2]u1 = .{ 0, 0 },
    v_above: [2]u1 = .{ 0, 0 },
    v_left: [2]u1 = .{ 0, 0 },

    fn reset(self: *Vp8MacroblockTokenContext) void {
        self.* = .{};
    }
};

const Vp8EntropyHeader = struct {
    coeff_probs: Vp8CoeffProbs,
    skip_probability: ?u8 = null,
};

const Vp8KeyframeControl = struct {
    syntax: Vp8FirstPartitionSyntax,
    entropy: Vp8EntropyHeader,
    mode_reader: Vp8BoolReader,
};

const Vp8QuantMatrix = struct {
    y1: [2]i16,
    y2: [2]i16,
    uv: [2]i16,
    uv_quant: i16,
};

const Vp8MacroblockHeader = struct {
    segment: u2 = 0,
    skip: bool = false,
    is_i4x4: bool = false,
    luma16_mode: ?Vp8Luma16Mode = null,
    luma4_modes: ?[16]Vp8Luma4Mode = null,
    chroma_mode: Vp8ChromaMode,
};

const Vp8Luma4Probs = [10][10][9]u8;

const Vp8Luma16Mode = enum(u3) {
    dc = 0,
    true_motion = 1,
    vertical = 2,
    horizontal = 3,
    dc_no_top = 4,
    dc_no_left = 5,
    dc_no_top_left = 6,
};

const Vp8Luma4Mode = enum(u4) {
    dc = 0,
    true_motion = 1,
    vertical = 2,
    horizontal = 3,
    down_right = 4,
    vertical_right = 5,
    down_left = 6,
    vertical_left = 7,
    horizontal_down = 8,
    horizontal_up = 9,
};

const Vp8ChromaMode = enum(u2) {
    dc = 0,
    vertical = 1,
    horizontal = 2,
    true_motion = 3,
};

const Vp8BoolReader = struct {
    bytes: []const u8,
    pos: usize = 0,
    range_m1: u32 = 254,
    bits: u32 = 0,
    n_bits: u8 = 0,

    fn init(bytes: []const u8) Vp8BoolReader {
        return .{ .bytes = bytes };
    }

    fn readBool(self: *Vp8BoolReader, probability: u8) !bool {
        if (self.n_bits < 8) {
            if (self.pos >= self.bytes.len) return error.WebpDecodeFailed;
            const shift: u5 = @intCast(8 - self.n_bits);
            self.bits |= @as(u32, self.bytes[self.pos]) << shift;
            self.pos += 1;
            self.n_bits += 8;
        }

        const split = ((self.range_m1 * @as(u32, probability)) >> 8) + 1;
        const bit = self.bits >= (split << 8);
        if (bit) {
            self.range_m1 -= split;
            self.bits -= split << 8;
        } else {
            self.range_m1 = split - 1;
        }
        while (self.range_m1 < 127) {
            self.range_m1 = (self.range_m1 << 1) | 1;
            self.bits <<= 1;
            self.n_bits -%= 1;
        }
        return bit;
    }

    fn readBit(self: *Vp8BoolReader) !bool {
        return try self.readBool(0x80);
    }

    fn readValue(self: *Vp8BoolReader, bit_count: u5) !u32 {
        var value: u32 = 0;
        var remaining = bit_count;
        while (remaining > 0) {
            remaining -= 1;
            if (try self.readBit()) value |= @as(u32, 1) << remaining;
        }
        return value;
    }

    fn readSignedValue(self: *Vp8BoolReader, bit_count: u5) !i16 {
        const value: i16 = @intCast(try self.readValue(bit_count));
        return if (try self.readBit()) -value else value;
    }
};

fn parseVp8FrameHeader(payload: []const u8) !Vp8FrameHeader {
    if (payload.len < vp8_keyframe_header_len) return error.WebpDecodeFailed;
    const frame_tag = @as(u32, payload[0]) | (@as(u32, payload[1]) << 8) | (@as(u32, payload[2]) << 16);
    const keyframe = (frame_tag & 0x01) == 0;
    if (!keyframe) return error.WebpDecodeFailed;
    if (!std.mem.eql(u8, payload[3..6], &.{ 0x9d, 0x01, 0x2a })) return error.WebpDecodeFailed;
    const raw_width = try readU16Le(payload, 6);
    const raw_height = try readU16Le(payload, 8);
    return .{
        .keyframe = keyframe,
        .version = @intCast((frame_tag >> 1) & 0x07),
        .show_frame = ((frame_tag >> 4) & 0x01) != 0,
        .first_partition_size = (frame_tag >> 5) & 0x7ffff,
        .width = @as(u32, raw_width) & 0x3fff,
        .height = @as(u32, raw_height) & 0x3fff,
        .horizontal_scale = @intCast(raw_width >> 14),
        .vertical_scale = @intCast(raw_height >> 14),
    };
}

fn parseVp8PartitionInfo(alloc: Allocator, payload: []const u8, token_partition_count: usize) !Vp8PartitionInfo {
    const frame = try parseVp8FrameHeader(payload);
    if (frame.width == 0 or frame.height == 0) return error.WebpDecodeFailed;
    if (token_partition_count == 0 or token_partition_count > 8 or !std.math.isPowerOfTwo(token_partition_count)) return error.WebpDecodeFailed;

    const first_partition_start = vp8_keyframe_header_len;
    const first_partition_size: usize = @intCast(frame.first_partition_size);
    if (first_partition_size > payload.len - first_partition_start) return error.WebpDecodeFailed;
    const first_partition_end = first_partition_start + first_partition_size;
    const partition_size_table_len = (token_partition_count - 1) * 3;
    if (partition_size_table_len > payload.len - first_partition_end) return error.WebpDecodeFailed;

    const token_partitions = try alloc.alloc([]const u8, token_partition_count);
    errdefer alloc.free(token_partitions);
    var cursor = first_partition_end + partition_size_table_len;
    var i: usize = 0;
    while (i + 1 < token_partition_count) : (i += 1) {
        const size_offset = first_partition_end + i * 3;
        const partition_size = @as(usize, payload[size_offset]) |
            (@as(usize, payload[size_offset + 1]) << 8) |
            (@as(usize, payload[size_offset + 2]) << 16);
        if (partition_size > payload.len - cursor) return error.WebpDecodeFailed;
        token_partitions[i] = payload[cursor .. cursor + partition_size];
        cursor += partition_size;
    }
    token_partitions[token_partition_count - 1] = payload[cursor..];

    return .{
        .frame = frame,
        .first_partition = payload[first_partition_start..first_partition_end],
        .token_partitions = token_partitions,
    };
}

fn parseVp8KeyframeInfo(alloc: Allocator, payload: []const u8) !Vp8KeyframeInfo {
    const frame = try parseVp8FrameHeader(payload);
    const first_partition_start = vp8_keyframe_header_len;
    const first_partition_size: usize = @intCast(frame.first_partition_size);
    if (first_partition_size > payload.len - first_partition_start) return error.WebpDecodeFailed;
    const first_partition = payload[first_partition_start .. first_partition_start + first_partition_size];
    const syntax = try parseVp8FirstPartitionSyntax(first_partition);
    const partitions = try parseVp8PartitionInfo(alloc, payload, syntax.token_partition_count);
    return .{
        .partitions = partitions,
        .syntax = syntax,
    };
}

fn decodeVp8Rgba(alloc: Allocator, payload: []const u8) !DecodedImage {
    const frame = try parseVp8FrameHeader(payload);
    if (!frame.show_frame) return error.UnsupportedWebpFormat;

    const first_partition_start = vp8_keyframe_header_len;
    const first_partition_size: usize = @intCast(frame.first_partition_size);
    if (first_partition_size > payload.len - first_partition_start) return error.WebpDecodeFailed;
    const first_partition = payload[first_partition_start .. first_partition_start + first_partition_size];

    const control = try parseVp8KeyframeControl(first_partition);
    const partitions = try parseVp8PartitionInfo(alloc, payload, control.syntax.token_partition_count);
    defer alloc.free(partitions.token_partitions);

    if (partitions.frame.width != frame.width or partitions.frame.height != frame.height) return error.WebpDecodeFailed;
    return try assembleVp8KeyframeDefaultRgba(alloc, frame, control, partitions.token_partitions);
}

fn vp8MacroblockGrid(frame: Vp8FrameHeader) Vp8MacroblockGrid {
    return .{
        .width = (frame.width + 15) >> 4,
        .height = (frame.height + 15) >> 4,
    };
}

fn vp8TokenPartitionForRow(row: u32, token_partition_count: usize) !usize {
    if (token_partition_count == 0 or token_partition_count > 8 or !std.math.isPowerOfTwo(token_partition_count)) {
        return error.WebpDecodeFailed;
    }
    return @as(usize, @intCast(row)) & (token_partition_count - 1);
}

fn allocateVp8FramePlanes(alloc: Allocator, frame: Vp8FrameHeader) !Vp8FramePlanes {
    if (frame.width == 0 or frame.height == 0) return error.WebpDecodeFailed;
    const grid = vp8MacroblockGrid(frame);
    const y_stride: usize = @as(usize, @intCast(grid.width)) * 16;
    const y_padded_height: usize = @as(usize, @intCast(grid.height)) * 16;
    const uv_stride: usize = @as(usize, @intCast(grid.width)) * 8;
    const uv_padded_height: usize = @as(usize, @intCast(grid.height)) * 8;

    const y = try alloc.alloc(u8, try checkedByteCount(y_stride, y_padded_height));
    errdefer alloc.free(y);
    const u = try alloc.alloc(u8, try checkedByteCount(uv_stride, uv_padded_height));
    errdefer alloc.free(u);
    const v = try alloc.alloc(u8, try checkedByteCount(uv_stride, uv_padded_height));
    errdefer alloc.free(v);
    @memset(y, 0);
    @memset(u, 128);
    @memset(v, 128);

    return .{
        .y = y,
        .u = u,
        .v = v,
        .width = frame.width,
        .height = frame.height,
        .y_stride = y_stride,
        .uv_stride = uv_stride,
        .y_padded_height = y_padded_height,
        .uv_padded_height = uv_padded_height,
    };
}

fn vp8PlanesToRgbaAlloc(alloc: Allocator, planes: Vp8FramePlanes) !DecodedImage {
    const pixel_count = try checkedPixelCount(planes.width, planes.height);
    const rgba = try alloc.alloc(u8, try checkedByteCount(pixel_count, 4));
    errdefer alloc.free(rgba);

    const width: usize = @intCast(planes.width);
    const height: usize = @intCast(planes.height);
    if (width > planes.y_stride or ((width + 1) >> 1) > planes.uv_stride) return error.WebpDecodeFailed;
    if (height > planes.y_padded_height or ((height + 1) >> 1) > planes.uv_padded_height) return error.WebpDecodeFailed;

    var y_pos: usize = 0;
    while (y_pos < height) : (y_pos += 1) {
        var x: usize = 0;
        while (x < width) : (x += 1) {
            const y_index = y_pos * planes.y_stride + x;
            const uv_index = (y_pos >> 1) * planes.uv_stride + (x >> 1);
            const pixel = vp8YuvToRgba(planes.y[y_index], planes.u[uv_index], planes.v[uv_index], 255);
            @memcpy(rgba[(y_pos * width + x) * 4 ..][0..4], pixel[0..]);
        }
    }

    return .{
        .rgba = rgba,
        .width = planes.width,
        .height = planes.height,
    };
}

fn vp8Luma16AsLuma4Mode(mode: Vp8Luma16Mode) Vp8Luma4Mode {
    return switch (mode) {
        .dc, .dc_no_top, .dc_no_left, .dc_no_top_left => .dc,
        .true_motion => .true_motion,
        .vertical => .vertical,
        .horizontal => .horizontal,
    };
}

fn vp8MergeTokenContext(above: Vp8MacroblockTokenContext, left: Vp8MacroblockTokenContext) Vp8MacroblockTokenContext {
    return .{
        .y_above = above.y_above,
        .y_left = left.y_left,
        .y2_above = above.y2_above,
        .y2_left = left.y2_left,
        .u_above = above.u_above,
        .u_left = left.u_left,
        .v_above = above.v_above,
        .v_left = left.v_left,
    };
}

fn vp8StoreAboveTokenContext(dst: *Vp8MacroblockTokenContext, src: Vp8MacroblockTokenContext) void {
    dst.y_above = src.y_above;
    dst.y2_above = src.y2_above;
    dst.u_above = src.u_above;
    dst.v_above = src.v_above;
}

fn vp8StoreLeftTokenContext(dst: *Vp8MacroblockTokenContext, src: Vp8MacroblockTokenContext) void {
    dst.y_left = src.y_left;
    dst.y2_left = src.y2_left;
    dst.u_left = src.u_left;
    dst.v_left = src.v_left;
}

fn vp8MacroblockHasCoeffs(coeffs: *const Vp8MacroblockCoeffs) bool {
    if (coeffs.y2.last_nonzero_plus_one != 0) return true;
    for (coeffs.y) |block| if (block.last_nonzero_plus_one != 0) return true;
    for (coeffs.u) |block| if (block.last_nonzero_plus_one != 0) return true;
    for (coeffs.v) |block| if (block.last_nonzero_plus_one != 0) return true;
    return false;
}

fn vp8ClipFilterLevel(value: i32) i32 {
    if (value < 0) return 0;
    if (value > 63) return 63;
    return value;
}

fn vp8LoopFilterParamsFor(syntax: Vp8FirstPartitionSyntax, info: Vp8MacroblockFilterInfo) Vp8LoopFilterParams {
    var filter_level: i32 = syntax.loop_filter.level;
    if (syntax.segmentation.enabled) {
        const segment_strength = syntax.segmentation.filter_strength[info.segment];
        filter_level = if (syntax.segmentation.absolute_delta) segment_strength else filter_level + segment_strength;
    }
    filter_level = vp8ClipFilterLevel(filter_level);

    if (syntax.loop_filter.use_lf_delta) {
        filter_level += syntax.loop_filter.ref_lf_delta[0];
        if (info.is_i4x4) filter_level += syntax.loop_filter.mode_lf_delta[0];
    }
    filter_level = vp8ClipFilterLevel(filter_level);

    var interior_limit = filter_level;
    if (syntax.loop_filter.sharpness != 0) {
        interior_limit >>= if (syntax.loop_filter.sharpness > 4) 2 else 1;
        interior_limit = @min(interior_limit, 9 - @as(i32, syntax.loop_filter.sharpness));
    }
    interior_limit = @max(interior_limit, 1);

    var hev_threshold: i32 = if (filter_level >= 15) 1 else 0;
    if (filter_level >= 40) hev_threshold += 1;

    return .{
        .edge_limit = filter_level,
        .interior_limit = interior_limit,
        .hev_threshold = hev_threshold,
    };
}

fn vp8AbsDiff(a: u8, b: u8) i32 {
    return @intCast(@abs(@as(i32, a) - @as(i32, b)));
}

fn vp8SaturateInt8(value: i32) i32 {
    if (value < -128) return -128;
    if (value > 127) return 127;
    return value;
}

fn vp8SaturateUint8(value: i32) u8 {
    return clip8(value);
}

fn vp8SimpleThreshold(plane: []const u8, q0_index: usize, step: usize, filter_limit: i32) !bool {
    if (q0_index < 2 * step or q0_index + step >= plane.len) return error.WebpDecodeFailed;
    const p1 = plane[q0_index - 2 * step];
    const p0 = plane[q0_index - step];
    const q0 = plane[q0_index];
    const q1 = plane[q0_index + step];
    return vp8AbsDiff(p0, q0) * 2 + (vp8AbsDiff(p1, q1) >> 1) <= filter_limit;
}

fn vp8NormalThreshold(plane: []const u8, q0_index: usize, step: usize, filter_limit: i32, interior_limit: i32) !bool {
    if (q0_index < 4 * step or q0_index + 3 * step >= plane.len) return error.WebpDecodeFailed;
    return (try vp8SimpleThreshold(plane, q0_index, step, filter_limit)) and
        vp8AbsDiff(plane[q0_index - 4 * step], plane[q0_index - 3 * step]) <= interior_limit and
        vp8AbsDiff(plane[q0_index - 3 * step], plane[q0_index - 2 * step]) <= interior_limit and
        vp8AbsDiff(plane[q0_index - 2 * step], plane[q0_index - step]) <= interior_limit and
        vp8AbsDiff(plane[q0_index + 3 * step], plane[q0_index + 2 * step]) <= interior_limit and
        vp8AbsDiff(plane[q0_index + 2 * step], plane[q0_index + step]) <= interior_limit and
        vp8AbsDiff(plane[q0_index + step], plane[q0_index]) <= interior_limit;
}

fn vp8HighEdgeVariance(plane: []const u8, q0_index: usize, step: usize, hev_threshold: i32) !bool {
    if (q0_index < 2 * step or q0_index + step >= plane.len) return error.WebpDecodeFailed;
    return vp8AbsDiff(plane[q0_index - 2 * step], plane[q0_index - step]) > hev_threshold or
        vp8AbsDiff(plane[q0_index + step], plane[q0_index]) > hev_threshold;
}

fn vp8FilterCommon(plane: []u8, q0_index: usize, step: usize, use_outer_taps: bool) !void {
    if (q0_index < 2 * step or q0_index + step >= plane.len) return error.WebpDecodeFailed;
    const p1_index = q0_index - 2 * step;
    const p0_index = q0_index - step;
    const q1_index = q0_index + step;

    var a = 3 * (@as(i32, plane[q0_index]) - @as(i32, plane[p0_index]));
    if (use_outer_taps) a += vp8SaturateInt8(@as(i32, plane[p1_index]) - @as(i32, plane[q1_index]));
    a = vp8SaturateInt8(a);

    const f1 = (@min(a + 4, 127)) >> 3;
    const f2 = (@min(a + 3, 127)) >> 3;
    plane[p0_index] = vp8SaturateUint8(@as(i32, plane[p0_index]) + f2);
    plane[q0_index] = vp8SaturateUint8(@as(i32, plane[q0_index]) - f1);

    if (!use_outer_taps) {
        const outer_adjust = (f1 + 1) >> 1;
        plane[p1_index] = vp8SaturateUint8(@as(i32, plane[p1_index]) + outer_adjust);
        plane[q1_index] = vp8SaturateUint8(@as(i32, plane[q1_index]) - outer_adjust);
    }
}

fn vp8FilterMacroblockEdge(plane: []u8, q0_index: usize, step: usize) !void {
    if (q0_index < 3 * step or q0_index + 2 * step >= plane.len) return error.WebpDecodeFailed;
    const p2_index = q0_index - 3 * step;
    const p1_index = q0_index - 2 * step;
    const p0_index = q0_index - step;
    const q1_index = q0_index + step;
    const q2_index = q0_index + 2 * step;

    const w = vp8SaturateInt8(vp8SaturateInt8(@as(i32, plane[p1_index]) - @as(i32, plane[q1_index])) + 3 * (@as(i32, plane[q0_index]) - @as(i32, plane[p0_index])));

    var a = (27 * w + 63) >> 7;
    plane[p0_index] = vp8SaturateUint8(@as(i32, plane[p0_index]) + a);
    plane[q0_index] = vp8SaturateUint8(@as(i32, plane[q0_index]) - a);
    a = (18 * w + 63) >> 7;
    plane[p1_index] = vp8SaturateUint8(@as(i32, plane[p1_index]) + a);
    plane[q1_index] = vp8SaturateUint8(@as(i32, plane[q1_index]) - a);
    a = (9 * w + 63) >> 7;
    plane[p2_index] = vp8SaturateUint8(@as(i32, plane[p2_index]) + a);
    plane[q2_index] = vp8SaturateUint8(@as(i32, plane[q2_index]) - a);
}

fn vp8FilterNormalEdge(plane: []u8, q0_index: usize, step: usize, advance: usize, count: usize, params: Vp8LoopFilterParams, macroblock_edge: bool) !void {
    var i: usize = 0;
    var index = q0_index;
    while (i < count) : ({
        i += 1;
        index += advance;
    }) {
        const edge_bonus: i32 = if (macroblock_edge) 4 else 0;
        const filter_limit = 2 * params.edge_limit + params.interior_limit + edge_bonus;
        if (try vp8NormalThreshold(plane, index, step, filter_limit, params.interior_limit)) {
            if (macroblock_edge and !(try vp8HighEdgeVariance(plane, index, step, params.hev_threshold))) {
                try vp8FilterMacroblockEdge(plane, index, step);
            } else {
                try vp8FilterCommon(plane, index, step, try vp8HighEdgeVariance(plane, index, step, params.hev_threshold));
            }
        }
    }
}

fn vp8FilterSimpleEdge(plane: []u8, q0_index: usize, step: usize, advance: usize, count: usize, filter_limit: i32) !void {
    var i: usize = 0;
    var index = q0_index;
    while (i < count) : ({
        i += 1;
        index += advance;
    }) {
        if (try vp8SimpleThreshold(plane, index, step, filter_limit)) {
            try vp8FilterCommon(plane, index, step, true);
        }
    }
}

fn vp8ApplySimpleLoopFilter(planes: *Vp8FramePlanes, mb_x: usize, mb_y: usize, params: Vp8LoopFilterParams, filter_subblocks: bool) !void {
    const y_x = mb_x * 16;
    const y_y = mb_y * 16;
    const y_base = y_y * planes.y_stride + y_x;
    const mb_limit = (params.edge_limit + 2) * 2 + params.interior_limit;
    const block_limit = params.edge_limit * 2 + params.interior_limit;

    if (mb_x > 0) try vp8FilterSimpleEdge(planes.y, y_base, 1, planes.y_stride, 16, mb_limit);
    if (filter_subblocks) {
        try vp8FilterSimpleEdge(planes.y, y_base + 4, 1, planes.y_stride, 16, block_limit);
        try vp8FilterSimpleEdge(planes.y, y_base + 8, 1, planes.y_stride, 16, block_limit);
        try vp8FilterSimpleEdge(planes.y, y_base + 12, 1, planes.y_stride, 16, block_limit);
    }

    if (mb_y > 0) try vp8FilterSimpleEdge(planes.y, y_base, planes.y_stride, 1, 16, mb_limit);
    if (filter_subblocks) {
        try vp8FilterSimpleEdge(planes.y, y_base + 4 * planes.y_stride, planes.y_stride, 1, 16, block_limit);
        try vp8FilterSimpleEdge(planes.y, y_base + 8 * planes.y_stride, planes.y_stride, 1, 16, block_limit);
        try vp8FilterSimpleEdge(planes.y, y_base + 12 * planes.y_stride, planes.y_stride, 1, 16, block_limit);
    }
}

fn vp8ApplyNormalLoopFilter(planes: *Vp8FramePlanes, mb_x: usize, mb_y: usize, params: Vp8LoopFilterParams, filter_subblocks: bool) !void {
    const y_x = mb_x * 16;
    const y_y = mb_y * 16;
    const y_base = y_y * planes.y_stride + y_x;
    const uv_x = mb_x * 8;
    const uv_y = mb_y * 8;
    const uv_base = uv_y * planes.uv_stride + uv_x;

    if (mb_x > 0) {
        try vp8FilterNormalEdge(planes.y, y_base, 1, planes.y_stride, 16, params, true);
        try vp8FilterNormalEdge(planes.u, uv_base, 1, planes.uv_stride, 8, params, true);
        try vp8FilterNormalEdge(planes.v, uv_base, 1, planes.uv_stride, 8, params, true);
    }
    if (filter_subblocks) {
        try vp8FilterNormalEdge(planes.y, y_base + 4, 1, planes.y_stride, 16, params, false);
        try vp8FilterNormalEdge(planes.y, y_base + 8, 1, planes.y_stride, 16, params, false);
        try vp8FilterNormalEdge(planes.y, y_base + 12, 1, planes.y_stride, 16, params, false);
        try vp8FilterNormalEdge(planes.u, uv_base + 4, 1, planes.uv_stride, 8, params, false);
        try vp8FilterNormalEdge(planes.v, uv_base + 4, 1, planes.uv_stride, 8, params, false);
    }

    if (mb_y > 0) {
        try vp8FilterNormalEdge(planes.y, y_base, planes.y_stride, 1, 16, params, true);
        try vp8FilterNormalEdge(planes.u, uv_base, planes.uv_stride, 1, 8, params, true);
        try vp8FilterNormalEdge(planes.v, uv_base, planes.uv_stride, 1, 8, params, true);
    }
    if (filter_subblocks) {
        try vp8FilterNormalEdge(planes.y, y_base + 4 * planes.y_stride, planes.y_stride, 1, 16, params, false);
        try vp8FilterNormalEdge(planes.y, y_base + 8 * planes.y_stride, planes.y_stride, 1, 16, params, false);
        try vp8FilterNormalEdge(planes.y, y_base + 12 * planes.y_stride, planes.y_stride, 1, 16, params, false);
        try vp8FilterNormalEdge(planes.u, uv_base + 4 * planes.uv_stride, planes.uv_stride, 1, 8, params, false);
        try vp8FilterNormalEdge(planes.v, uv_base + 4 * planes.uv_stride, planes.uv_stride, 1, 8, params, false);
    }
}

fn vp8ApplyLoopFilter(planes: *Vp8FramePlanes, syntax: Vp8FirstPartitionSyntax, infos: []const Vp8MacroblockFilterInfo, grid: Vp8MacroblockGrid) !void {
    if (syntax.loop_filter.level == 0) return;
    const grid_width: usize = @intCast(grid.width);
    const grid_height: usize = @intCast(grid.height);
    if (infos.len != grid_width * grid_height) return error.WebpDecodeFailed;

    var mb_y: usize = 0;
    while (mb_y < grid_height) : (mb_y += 1) {
        var mb_x: usize = 0;
        while (mb_x < grid_width) : (mb_x += 1) {
            const info = infos[mb_y * grid_width + mb_x];
            const params = vp8LoopFilterParamsFor(syntax, info);
            if (params.edge_limit == 0) continue;

            const filter_subblocks = info.has_coeffs or info.is_i4x4;
            if (syntax.loop_filter.simple) {
                try vp8ApplySimpleLoopFilter(planes, mb_x, mb_y, params, filter_subblocks);
            } else {
                try vp8ApplyNormalLoopFilter(planes, mb_x, mb_y, params, filter_subblocks);
            }
        }
    }
}

fn assembleVp8KeyframeRgba(
    alloc: Allocator,
    frame: Vp8FrameHeader,
    control: Vp8KeyframeControl,
    token_partitions: []const []const u8,
    luma4_probs: *const Vp8Luma4Probs,
) !DecodedImage {
    if (frame.horizontal_scale != 0 or frame.vertical_scale != 0) return error.UnsupportedWebpFormat;
    if (control.syntax.color_space) return error.UnsupportedWebpFormat;
    if (token_partitions.len != control.syntax.token_partition_count) return error.WebpDecodeFailed;

    const grid = vp8MacroblockGrid(frame);
    const grid_width: usize = @intCast(grid.width);
    const grid_height: usize = @intCast(grid.height);
    var planes = try allocateVp8FramePlanes(alloc, frame);
    errdefer planes.deinit(alloc);
    const filter_info_count = try checkedPixelCount(grid.width, grid.height);
    var filter_infos = try alloc.alloc(Vp8MacroblockFilterInfo, filter_info_count);
    defer alloc.free(filter_infos);

    var token_readers: [8]Vp8BoolReader = undefined;
    var partition_index: usize = 0;
    while (partition_index < token_partitions.len) : (partition_index += 1) {
        token_readers[partition_index] = Vp8BoolReader.init(token_partitions[partition_index]);
    }

    const quant_matrices = vp8BuildSegmentQuantMatrices(control.syntax);
    var mode_reader = control.mode_reader;
    var above_luma4_modes = try alloc.alloc([4]Vp8Luma4Mode, grid_width);
    defer alloc.free(above_luma4_modes);
    for (above_luma4_modes) |*modes| modes.* = @as([4]Vp8Luma4Mode, @splat(.dc));

    var above_token_contexts = try alloc.alloc(Vp8MacroblockTokenContext, grid_width);
    defer alloc.free(above_token_contexts);
    for (above_token_contexts) |*context| context.* = .{};

    var mb_y: usize = 0;
    while (mb_y < grid_height) : (mb_y += 1) {
        const row_partition = try vp8TokenPartitionForRow(@intCast(mb_y), control.syntax.token_partition_count);
        var left_luma4_modes = @as([4]Vp8Luma4Mode, @splat(.dc));
        var left_token_context = Vp8MacroblockTokenContext{};

        var mb_x: usize = 0;
        while (mb_x < grid_width) : (mb_x += 1) {
            const header = try vp8ReadMacroblockHeader(
                &mode_reader,
                control.syntax,
                control.entropy.skip_probability,
                &above_luma4_modes[mb_x],
                &left_luma4_modes,
                luma4_probs,
            );

            var token_context = vp8MergeTokenContext(above_token_contexts[mb_x], left_token_context);
            const coeffs = try vp8ReadMacroblockCoeffs(
                &token_readers[row_partition],
                &control.entropy.coeff_probs,
                header,
                quant_matrices[header.segment],
                &token_context,
            );
            filter_infos[mb_y * grid_width + mb_x] = .{
                .segment = header.segment,
                .is_i4x4 = header.is_i4x4,
                .has_coeffs = vp8MacroblockHasCoeffs(&coeffs),
            };
            vp8StoreAboveTokenContext(&above_token_contexts[mb_x], token_context);
            vp8StoreLeftTokenContext(&left_token_context, token_context);
            if (header.is_i4x4) {
                try vp8ReconstructMacroblock4x4(&planes, @intCast(mb_x), @intCast(mb_y), header, &coeffs);
            } else {
                try vp8PredictMacroblock16x16(&planes, @intCast(mb_x), @intCast(mb_y), header);
                try vp8ApplyMacroblockResidual16x16(&planes, @intCast(mb_x), @intCast(mb_y), &coeffs);
            }
        }
    }

    try vp8ApplyLoopFilter(&planes, control.syntax, filter_infos, grid);
    const decoded = try vp8PlanesToRgbaAlloc(alloc, planes);
    planes.deinit(alloc);
    return decoded;
}

fn assembleVp8KeyframeDefaultRgba(
    alloc: Allocator,
    frame: Vp8FrameHeader,
    control: Vp8KeyframeControl,
    token_partitions: []const []const u8,
) !DecodedImage {
    return try assembleVp8KeyframeRgba(alloc, frame, control, token_partitions, &vp8_default_luma4_probs);
}

fn vp8YuvToRgba(y: u8, u: u8, v: u8, alpha: u8) [4]u8 {
    const yy = @as(i32, y) * 0x10101;
    const cb = @as(i32, u) - 128;
    const cr = @as(i32, v) - 128;
    return .{
        clip8((yy + 91881 * cr) >> 16),
        clip8((yy - 22554 * cb - 46802 * cr) >> 16),
        clip8((yy + 116130 * cb) >> 16),
        alpha,
    };
}

fn vp8TransformAc3Mul1(value: i32) i32 {
    return ((value * vp8_transform_ac3_c1) >> 16) + value;
}

fn vp8TransformAc3Mul2(value: i32) i32 {
    return (value * vp8_transform_ac3_c2) >> 16;
}

fn vp8StoreTransformPixel(dst: []u8, index: usize, value: i32) void {
    dst[index] = clip8(@as(i32, dst[index]) + (value >> 3));
}

fn vp8InverseDct4x4Add(coeffs: *const [16]i16, dst: []u8, stride: usize) !void {
    if (stride < 4 or dst.len < stride * 3 + 4) return error.WebpDecodeFailed;

    var tmp: [16]i32 = undefined;
    var col: usize = 0;
    while (col < 4) : (col += 1) {
        const in0: i32 = coeffs[col + 0];
        const in4: i32 = coeffs[col + 4];
        const in8: i32 = coeffs[col + 8];
        const in12: i32 = coeffs[col + 12];
        const a = in0 + in8;
        const b = in0 - in8;
        const c = vp8TransformAc3Mul2(in4) - vp8TransformAc3Mul1(in12);
        const d = vp8TransformAc3Mul1(in4) + vp8TransformAc3Mul2(in12);
        tmp[col * 4 + 0] = a + d;
        tmp[col * 4 + 1] = b + c;
        tmp[col * 4 + 2] = b - c;
        tmp[col * 4 + 3] = a - d;
    }

    var row: usize = 0;
    while (row < 4) : (row += 1) {
        const dc = tmp[row + 0] + 4;
        const a = dc + tmp[row + 8];
        const b = dc - tmp[row + 8];
        const c = vp8TransformAc3Mul2(tmp[row + 4]) - vp8TransformAc3Mul1(tmp[row + 12]);
        const d = vp8TransformAc3Mul1(tmp[row + 4]) + vp8TransformAc3Mul2(tmp[row + 12]);
        const row_start = row * stride;
        vp8StoreTransformPixel(dst, row_start + 0, a + d);
        vp8StoreTransformPixel(dst, row_start + 1, b + c);
        vp8StoreTransformPixel(dst, row_start + 2, b - c);
        vp8StoreTransformPixel(dst, row_start + 3, a - d);
    }
}

fn vp8InverseDct4x4DcAdd(dc_coeff: i16, dst: []u8, stride: usize) !void {
    if (stride < 4 or dst.len < stride * 3 + 4) return error.WebpDecodeFailed;
    const dc = @as(i32, dc_coeff) + 4;
    var y: usize = 0;
    while (y < 4) : (y += 1) {
        var x: usize = 0;
        while (x < 4) : (x += 1) {
            vp8StoreTransformPixel(dst, y * stride + x, dc);
        }
    }
}

fn vp8InverseWht4x4(input: *const [16]i16) [16]i16 {
    var tmp: [16]i32 = undefined;
    var i: usize = 0;
    while (i < 4) : (i += 1) {
        const a0 = @as(i32, input[0 + i]) + @as(i32, input[12 + i]);
        const a1 = @as(i32, input[4 + i]) + @as(i32, input[8 + i]);
        const a2 = @as(i32, input[4 + i]) - @as(i32, input[8 + i]);
        const a3 = @as(i32, input[0 + i]) - @as(i32, input[12 + i]);
        tmp[0 + i] = a0 + a1;
        tmp[8 + i] = a0 - a1;
        tmp[4 + i] = a3 + a2;
        tmp[12 + i] = a3 - a2;
    }

    var out: [16]i16 = undefined;
    i = 0;
    while (i < 4) : (i += 1) {
        const row = i * 4;
        const dc = tmp[row + 0] + 3;
        const a0 = dc + tmp[row + 3];
        const a1 = tmp[row + 1] + tmp[row + 2];
        const a2 = tmp[row + 1] - tmp[row + 2];
        const a3 = dc - tmp[row + 3];
        out[row + 0] = @intCast((a0 + a1) >> 3);
        out[row + 1] = @intCast((a3 + a2) >> 3);
        out[row + 2] = @intCast((a0 - a1) >> 3);
        out[row + 3] = @intCast((a3 - a2) >> 3);
    }
    return out;
}

fn vp8ApplyResidualBlock(plane: []u8, stride: usize, start: usize, block: *const Vp8CoeffBlock) !void {
    if (start + stride * 3 + 4 > plane.len) return error.WebpDecodeFailed;
    if (block.last_nonzero_plus_one == 0) return;
    if (block.last_nonzero_plus_one == 1) {
        try vp8InverseDct4x4DcAdd(block.coeffs[0], plane[start..], stride);
    } else {
        try vp8InverseDct4x4Add(&block.coeffs, plane[start..], stride);
    }
}

fn vp8ApplyMacroblockResidual16x16(planes: *Vp8FramePlanes, mb_x: u32, mb_y: u32, coeffs: *const Vp8MacroblockCoeffs) !void {
    const x: usize = @as(usize, @intCast(mb_x)) * 16;
    const y: usize = @as(usize, @intCast(mb_y)) * 16;
    if (x + 16 > planes.y_stride or y + 16 > planes.y_padded_height) return error.WebpDecodeFailed;
    const y_base = y * planes.y_stride + x;

    const y2_dc = vp8InverseWht4x4(&coeffs.y2.coeffs);
    var block_index: usize = 0;
    while (block_index < coeffs.y.len) : (block_index += 1) {
        var block = coeffs.y[block_index];
        if (coeffs.y2.last_nonzero_plus_one != 0) {
            block.coeffs[0] = y2_dc[block_index];
            if (block.last_nonzero_plus_one == 0) block.last_nonzero_plus_one = 1;
        }
        const block_x = (block_index & 3) * 4;
        const block_y = (block_index >> 2) * 4;
        try vp8ApplyResidualBlock(planes.y, planes.y_stride, y_base + block_y * planes.y_stride + block_x, &block);
    }

    const uv_x = @as(usize, @intCast(mb_x)) * 8;
    const uv_y = @as(usize, @intCast(mb_y)) * 8;
    if (uv_x + 8 > planes.uv_stride or uv_y + 8 > planes.uv_padded_height) return error.WebpDecodeFailed;
    const uv_base = uv_y * planes.uv_stride + uv_x;
    block_index = 0;
    while (block_index < coeffs.u.len) : (block_index += 1) {
        const block_x = (block_index & 1) * 4;
        const block_y = (block_index >> 1) * 4;
        const start = uv_base + block_y * planes.uv_stride + block_x;
        try vp8ApplyResidualBlock(planes.u, planes.uv_stride, start, &coeffs.u[block_index]);
        try vp8ApplyResidualBlock(planes.v, planes.uv_stride, start, &coeffs.v[block_index]);
    }
}

fn vp8CollectTopSamples4x4(out: []u8, plane: []const u8, stride: usize, x: usize, y: usize) !void {
    if (out.len < 8 or stride == 0 or x >= stride) return error.WebpDecodeFailed;
    if (y == 0) {
        @memset(out[0..8], 127);
        return;
    }
    const top_base = (y - 1) * stride;
    if (top_base + stride > plane.len) return error.WebpDecodeFailed;
    const block_x_in_macroblock = x & 15;
    const block_y_in_macroblock = y & 15;
    const overhang_y = y - block_y_in_macroblock;
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        if (block_x_in_macroblock == 12 and block_y_in_macroblock != 0 and i >= 4) {
            if (overhang_y == 0) {
                out[i] = 127;
            } else {
                const overhang_top_base = (overhang_y - 1) * stride;
                if (overhang_top_base + stride > plane.len) return error.WebpDecodeFailed;
                out[i] = plane[overhang_top_base + @min(x + i, stride - 1)];
            }
        } else {
            out[i] = plane[top_base + @min(x + i, stride - 1)];
        }
    }
}

fn vp8CollectLeftSamples4x4(out: []u8, plane: []const u8, stride: usize, x: usize, y: usize) !void {
    if (out.len < 4 or stride == 0 or x >= stride) return error.WebpDecodeFailed;
    if (x == 0) {
        @memset(out[0..4], 129);
        return;
    }
    var i: usize = 0;
    while (i < 4) : (i += 1) {
        const index = (y + i) * stride + x - 1;
        if (index >= plane.len) return error.WebpDecodeFailed;
        out[i] = plane[index];
    }
}

fn vp8TopLeftSample4x4(plane: []const u8, stride: usize, x: usize, y: usize) !u8 {
    if (y == 0) return 127;
    if (x == 0) return 129;
    const index = (y - 1) * stride + x - 1;
    if (index >= plane.len) return error.WebpDecodeFailed;
    return plane[index];
}

fn vp8PredictAndApplyChroma(planes: *Vp8FramePlanes, mb_x: u32, mb_y: u32, header: Vp8MacroblockHeader, coeffs: *const Vp8MacroblockCoeffs) !void {
    const has_left = mb_x > 0;
    const has_top = mb_y > 0;
    const uv_x = @as(usize, @intCast(mb_x)) * 8;
    const uv_y = @as(usize, @intCast(mb_y)) * 8;
    if (uv_x + 8 > planes.uv_stride or uv_y + 8 > planes.uv_padded_height) return error.WebpDecodeFailed;
    const uv_start = uv_y * planes.uv_stride + uv_x;
    if (uv_start + planes.uv_stride * 7 + 8 > planes.u.len or uv_start + planes.uv_stride * 7 + 8 > planes.v.len) {
        return error.WebpDecodeFailed;
    }

    const uv_top_u: ?[]const u8 = if (has_top) planes.u[uv_start - planes.uv_stride ..][0..8] else null;
    const uv_top_v: ?[]const u8 = if (has_top) planes.v[uv_start - planes.uv_stride ..][0..8] else null;
    var u_left_buf: [8]u8 = undefined;
    var v_left_buf: [8]u8 = undefined;
    var u_left: ?[]const u8 = null;
    var v_left: ?[]const u8 = null;
    if (has_left) {
        try vp8CollectLeftSamples(u_left_buf[0..], planes.u, planes.uv_stride, uv_start, 8);
        try vp8CollectLeftSamples(v_left_buf[0..], planes.v, planes.uv_stride, uv_start, 8);
        u_left = u_left_buf[0..];
        v_left = v_left_buf[0..];
    }
    const u_top_left = if (has_top and has_left) planes.u[uv_start - planes.uv_stride - 1] else 0x80;
    const v_top_left = if (has_top and has_left) planes.v[uv_start - planes.uv_stride - 1] else 0x80;
    try vp8PredictChroma(header.chroma_mode, planes.u[uv_start..], planes.uv_stride, uv_top_u, u_left, u_top_left);
    try vp8PredictChroma(header.chroma_mode, planes.v[uv_start..], planes.uv_stride, uv_top_v, v_left, v_top_left);

    var block_index: usize = 0;
    while (block_index < coeffs.u.len) : (block_index += 1) {
        const block_x = (block_index & 1) * 4;
        const block_y = (block_index >> 1) * 4;
        const start = uv_start + block_y * planes.uv_stride + block_x;
        try vp8ApplyResidualBlock(planes.u, planes.uv_stride, start, &coeffs.u[block_index]);
        try vp8ApplyResidualBlock(planes.v, planes.uv_stride, start, &coeffs.v[block_index]);
    }
}

fn vp8ReconstructMacroblock4x4(planes: *Vp8FramePlanes, mb_x: u32, mb_y: u32, header: Vp8MacroblockHeader, coeffs: *const Vp8MacroblockCoeffs) !void {
    if (!header.is_i4x4 or header.luma4_modes == null) return error.WebpDecodeFailed;

    const base_x = @as(usize, @intCast(mb_x)) * 16;
    const base_y = @as(usize, @intCast(mb_y)) * 16;
    if (base_x + 16 > planes.y_stride or base_y + 16 > planes.y_padded_height) return error.WebpDecodeFailed;

    var block_index: usize = 0;
    while (block_index < 16) : (block_index += 1) {
        const block_x = (block_index & 3) * 4;
        const block_y = (block_index >> 2) * 4;
        const x = base_x + block_x;
        const y = base_y + block_y;
        const start = y * planes.y_stride + x;
        if (start + planes.y_stride * 3 + 4 > planes.y.len) return error.WebpDecodeFailed;

        var top: [8]u8 = undefined;
        var left: [4]u8 = undefined;
        try vp8CollectTopSamples4x4(top[0..], planes.y, planes.y_stride, x, y);
        try vp8CollectLeftSamples4x4(left[0..], planes.y, planes.y_stride, x, y);
        const top_left = try vp8TopLeftSample4x4(planes.y, planes.y_stride, x, y);
        try vp8PredictLuma4(header.luma4_modes.?[block_index], planes.y[start..], planes.y_stride, top[0..], left[0..], top_left);
        try vp8ApplyResidualBlock(planes.y, planes.y_stride, start, &coeffs.y[block_index]);
    }

    try vp8PredictAndApplyChroma(planes, mb_x, mb_y, header, coeffs);
}

fn vp8PutBlock(dst: []u8, stride: usize, width: usize, height: usize, value: u8) !void {
    if (stride < width or dst.len < stride * (height - 1) + width) return error.WebpDecodeFailed;
    var y: usize = 0;
    while (y < height) : (y += 1) @memset(dst[y * stride ..][0..width], value);
}

fn vp8PredictDcValue(top: ?[]const u8, left: ?[]const u8, size: usize) !u8 {
    if (top) |top_values| {
        if (top_values.len < size) return error.WebpDecodeFailed;
        if (left) |left_values| {
            if (left_values.len < size) return error.WebpDecodeFailed;
            var sum: u32 = @intCast(size);
            for (top_values[0..size]) |value| sum += value;
            for (left_values[0..size]) |value| sum += value;
            return @intCast(sum >> @intCast(std.math.log2_int(usize, size * 2)));
        }
        var sum: u32 = @intCast(size / 2);
        for (top_values[0..size]) |value| sum += value;
        return @intCast(sum >> @intCast(std.math.log2_int(usize, size)));
    }
    if (left) |left_values| {
        if (left_values.len < size) return error.WebpDecodeFailed;
        var sum: u32 = @intCast(size / 2);
        for (left_values[0..size]) |value| sum += value;
        return @intCast(sum >> @intCast(std.math.log2_int(usize, size)));
    }
    return 0x80;
}

fn vp8PredictVertical(dst: []u8, stride: usize, size: usize, top: []const u8) !void {
    if (stride < size or top.len < size or dst.len < stride * (size - 1) + size) return error.WebpDecodeFailed;
    var y: usize = 0;
    while (y < size) : (y += 1) @memcpy(dst[y * stride ..][0..size], top[0..size]);
}

fn vp8PredictHorizontal(dst: []u8, stride: usize, size: usize, left: []const u8) !void {
    if (stride < size or left.len < size or dst.len < stride * (size - 1) + size) return error.WebpDecodeFailed;
    var y: usize = 0;
    while (y < size) : (y += 1) @memset(dst[y * stride ..][0..size], left[y]);
}

fn vp8PredictTrueMotion(dst: []u8, stride: usize, size: usize, top: []const u8, left: []const u8, top_left: u8) !void {
    if (stride < size or top.len < size or left.len < size or dst.len < stride * (size - 1) + size) return error.WebpDecodeFailed;
    var y: usize = 0;
    while (y < size) : (y += 1) {
        var x: usize = 0;
        while (x < size) : (x += 1) {
            dst[y * stride + x] = clip8(@as(i32, left[y]) + @as(i32, top[x]) - @as(i32, top_left));
        }
    }
}

fn vp8PredictLuma16(mode: Vp8Luma16Mode, dst: []u8, stride: usize, top: ?[]const u8, left: ?[]const u8, top_left: u8) !void {
    const default_top = @as([16]u8, @splat(127));
    const default_left = @as([16]u8, @splat(129));
    switch (mode) {
        .dc => try vp8PutBlock(dst, stride, 16, 16, try vp8PredictDcValue(top, left, 16)),
        .true_motion => try vp8PredictTrueMotion(dst, stride, 16, top orelse default_top[0..], left orelse default_left[0..], top_left),
        .vertical => try vp8PredictVertical(dst, stride, 16, top orelse default_top[0..]),
        .horizontal => try vp8PredictHorizontal(dst, stride, 16, left orelse default_left[0..]),
        .dc_no_top => try vp8PutBlock(dst, stride, 16, 16, try vp8PredictDcValue(null, left, 16)),
        .dc_no_left => try vp8PutBlock(dst, stride, 16, 16, try vp8PredictDcValue(top, null, 16)),
        .dc_no_top_left => try vp8PutBlock(dst, stride, 16, 16, 0x80),
    }
}

fn vp8PredictChroma(mode: Vp8ChromaMode, dst: []u8, stride: usize, top: ?[]const u8, left: ?[]const u8, top_left: u8) !void {
    const default_top = @as([8]u8, @splat(127));
    const default_left = @as([8]u8, @splat(129));
    switch (mode) {
        .dc => try vp8PutBlock(dst, stride, 8, 8, try vp8PredictDcValue(top, left, 8)),
        .true_motion => try vp8PredictTrueMotion(dst, stride, 8, top orelse default_top[0..], left orelse default_left[0..], top_left),
        .vertical => try vp8PredictVertical(dst, stride, 8, top orelse default_top[0..]),
        .horizontal => try vp8PredictHorizontal(dst, stride, 8, left orelse default_left[0..]),
    }
}

fn vp8NormalizeLuma16Mode(mode: Vp8Luma16Mode, has_top: bool, has_left: bool) !Vp8Luma16Mode {
    if (mode == .dc) {
        if (has_top and has_left) return .dc;
        if (has_top) return .dc_no_left;
        if (has_left) return .dc_no_top;
        return .dc_no_top_left;
    }
    return mode;
}

fn vp8CollectLeftSamples(out: []u8, plane: []const u8, stride: usize, start: usize, count: usize) !void {
    if (out.len < count or start == 0) return error.WebpDecodeFailed;
    var y: usize = 0;
    while (y < count) : (y += 1) {
        const index = start + y * stride - 1;
        if (index >= plane.len) return error.WebpDecodeFailed;
        out[y] = plane[index];
    }
}

fn vp8PredictMacroblock16x16(planes: *Vp8FramePlanes, mb_x: u32, mb_y: u32, header: Vp8MacroblockHeader) !void {
    if (header.is_i4x4 or header.luma16_mode == null) return error.UnsupportedWebpFormat;

    const x: usize = @as(usize, @intCast(mb_x)) * 16;
    const y: usize = @as(usize, @intCast(mb_y)) * 16;
    if (x + 16 > planes.y_stride or y + 16 > planes.y_padded_height) return error.WebpDecodeFailed;
    const y_start = y * planes.y_stride + x;
    if (y_start + planes.y_stride * 15 + 16 > planes.y.len) return error.WebpDecodeFailed;

    const has_left = mb_x > 0;
    const has_top = mb_y > 0;
    const y_top: ?[]const u8 = if (has_top) planes.y[y_start - planes.y_stride ..][0..16] else null;
    var y_left_buf: [16]u8 = undefined;
    var y_left: ?[]const u8 = null;
    if (has_left) {
        try vp8CollectLeftSamples(y_left_buf[0..], planes.y, planes.y_stride, y_start, 16);
        y_left = y_left_buf[0..];
    }
    const y_top_left = if (has_top and has_left) planes.y[y_start - planes.y_stride - 1] else 0x80;
    const luma_mode = try vp8NormalizeLuma16Mode(header.luma16_mode.?, has_top, has_left);
    try vp8PredictLuma16(luma_mode, planes.y[y_start..], planes.y_stride, y_top, y_left, y_top_left);

    const uv_x = @as(usize, @intCast(mb_x)) * 8;
    const uv_y = @as(usize, @intCast(mb_y)) * 8;
    if (uv_x + 8 > planes.uv_stride or uv_y + 8 > planes.uv_padded_height) return error.WebpDecodeFailed;
    const uv_start = uv_y * planes.uv_stride + uv_x;
    if (uv_start + planes.uv_stride * 7 + 8 > planes.u.len or uv_start + planes.uv_stride * 7 + 8 > planes.v.len) {
        return error.WebpDecodeFailed;
    }

    const uv_top_u: ?[]const u8 = if (has_top) planes.u[uv_start - planes.uv_stride ..][0..8] else null;
    const uv_top_v: ?[]const u8 = if (has_top) planes.v[uv_start - planes.uv_stride ..][0..8] else null;
    var u_left_buf: [8]u8 = undefined;
    var v_left_buf: [8]u8 = undefined;
    var u_left: ?[]const u8 = null;
    var v_left: ?[]const u8 = null;
    if (has_left) {
        try vp8CollectLeftSamples(u_left_buf[0..], planes.u, planes.uv_stride, uv_start, 8);
        try vp8CollectLeftSamples(v_left_buf[0..], planes.v, planes.uv_stride, uv_start, 8);
        u_left = u_left_buf[0..];
        v_left = v_left_buf[0..];
    }
    const u_top_left = if (has_top and has_left) planes.u[uv_start - planes.uv_stride - 1] else 0x80;
    const v_top_left = if (has_top and has_left) planes.v[uv_start - planes.uv_stride - 1] else 0x80;
    try vp8PredictChroma(header.chroma_mode, planes.u[uv_start..], planes.uv_stride, uv_top_u, u_left, u_top_left);
    try vp8PredictChroma(header.chroma_mode, planes.v[uv_start..], planes.uv_stride, uv_top_v, v_left, v_top_left);
}

fn vp8Avg2(a: u8, b: u8) u8 {
    return @intCast((@as(u16, a) + @as(u16, b) + 1) >> 1);
}

fn vp8Avg3(a: u8, b: u8, c: u8) u8 {
    return @intCast((@as(u16, a) + 2 * @as(u16, b) + @as(u16, c) + 2) >> 2);
}

fn vp8Set4(dst: []u8, stride: usize, x: usize, y: usize, value: u8) !void {
    if (stride < 4 or dst.len < stride * 3 + 4) return error.WebpDecodeFailed;
    dst[y * stride + x] = value;
}

fn vp8PredictLuma4(mode: Vp8Luma4Mode, dst: []u8, stride: usize, top: []const u8, left: []const u8, top_left: u8) !void {
    if (stride < 4 or dst.len < stride * 3 + 4) return error.WebpDecodeFailed;
    switch (mode) {
        .dc => try vp8PutBlock(dst, stride, 4, 4, try vp8PredictDcValue(top, left, 4)),
        .true_motion => try vp8PredictTrueMotion(dst, stride, 4, top, left, top_left),
        .vertical => {
            if (top.len < 5) return error.WebpDecodeFailed;
            const vals = [_]u8{
                vp8Avg3(top_left, top[0], top[1]),
                vp8Avg3(top[0], top[1], top[2]),
                vp8Avg3(top[1], top[2], top[3]),
                vp8Avg3(top[2], top[3], top[4]),
            };
            var y: usize = 0;
            while (y < 4) : (y += 1) @memcpy(dst[y * stride ..][0..4], vals[0..]);
        },
        .horizontal => {
            if (left.len < 4) return error.WebpDecodeFailed;
            const vals = [_]u8{
                vp8Avg3(top_left, left[0], left[1]),
                vp8Avg3(left[0], left[1], left[2]),
                vp8Avg3(left[1], left[2], left[3]),
                vp8Avg3(left[2], left[3], left[3]),
            };
            var y: usize = 0;
            while (y < 4) : (y += 1) @memset(dst[y * stride ..][0..4], vals[y]);
        },
        .down_right => {
            if (top.len < 4 or left.len < 4) return error.WebpDecodeFailed;
            try vp8Set4(dst, stride, 0, 3, vp8Avg3(left[1], left[2], left[3]));
            try vp8Set4(dst, stride, 1, 3, vp8Avg3(left[0], left[1], left[2]));
            try vp8Set4(dst, stride, 0, 2, vp8Avg3(left[0], left[1], left[2]));
            try vp8Set4(dst, stride, 2, 3, vp8Avg3(top_left, left[0], left[1]));
            try vp8Set4(dst, stride, 1, 2, vp8Avg3(top_left, left[0], left[1]));
            try vp8Set4(dst, stride, 0, 1, vp8Avg3(top_left, left[0], left[1]));
            try vp8Set4(dst, stride, 3, 3, vp8Avg3(top[0], top_left, left[0]));
            try vp8Set4(dst, stride, 2, 2, vp8Avg3(top[0], top_left, left[0]));
            try vp8Set4(dst, stride, 1, 1, vp8Avg3(top[0], top_left, left[0]));
            try vp8Set4(dst, stride, 0, 0, vp8Avg3(top[0], top_left, left[0]));
            try vp8Set4(dst, stride, 3, 2, vp8Avg3(top[1], top[0], top_left));
            try vp8Set4(dst, stride, 2, 1, vp8Avg3(top[1], top[0], top_left));
            try vp8Set4(dst, stride, 1, 0, vp8Avg3(top[1], top[0], top_left));
            try vp8Set4(dst, stride, 3, 1, vp8Avg3(top[2], top[1], top[0]));
            try vp8Set4(dst, stride, 2, 0, vp8Avg3(top[2], top[1], top[0]));
            try vp8Set4(dst, stride, 3, 0, vp8Avg3(top[3], top[2], top[1]));
        },
        .down_left => {
            if (top.len < 8) return error.WebpDecodeFailed;
            try vp8Set4(dst, stride, 0, 0, vp8Avg3(top[0], top[1], top[2]));
            try vp8Set4(dst, stride, 1, 0, vp8Avg3(top[1], top[2], top[3]));
            try vp8Set4(dst, stride, 0, 1, vp8Avg3(top[1], top[2], top[3]));
            try vp8Set4(dst, stride, 2, 0, vp8Avg3(top[2], top[3], top[4]));
            try vp8Set4(dst, stride, 1, 1, vp8Avg3(top[2], top[3], top[4]));
            try vp8Set4(dst, stride, 0, 2, vp8Avg3(top[2], top[3], top[4]));
            try vp8Set4(dst, stride, 3, 0, vp8Avg3(top[3], top[4], top[5]));
            try vp8Set4(dst, stride, 2, 1, vp8Avg3(top[3], top[4], top[5]));
            try vp8Set4(dst, stride, 1, 2, vp8Avg3(top[3], top[4], top[5]));
            try vp8Set4(dst, stride, 0, 3, vp8Avg3(top[3], top[4], top[5]));
            try vp8Set4(dst, stride, 3, 1, vp8Avg3(top[4], top[5], top[6]));
            try vp8Set4(dst, stride, 2, 2, vp8Avg3(top[4], top[5], top[6]));
            try vp8Set4(dst, stride, 1, 3, vp8Avg3(top[4], top[5], top[6]));
            try vp8Set4(dst, stride, 3, 2, vp8Avg3(top[5], top[6], top[7]));
            try vp8Set4(dst, stride, 2, 3, vp8Avg3(top[5], top[6], top[7]));
            try vp8Set4(dst, stride, 3, 3, vp8Avg3(top[6], top[7], top[7]));
        },
        .vertical_right => {
            if (top.len < 4 or left.len < 3) return error.WebpDecodeFailed;
            try vp8Set4(dst, stride, 0, 0, vp8Avg2(top_left, top[0]));
            try vp8Set4(dst, stride, 1, 2, vp8Avg2(top_left, top[0]));
            try vp8Set4(dst, stride, 1, 0, vp8Avg2(top[0], top[1]));
            try vp8Set4(dst, stride, 2, 2, vp8Avg2(top[0], top[1]));
            try vp8Set4(dst, stride, 2, 0, vp8Avg2(top[1], top[2]));
            try vp8Set4(dst, stride, 3, 2, vp8Avg2(top[1], top[2]));
            try vp8Set4(dst, stride, 3, 0, vp8Avg2(top[2], top[3]));
            try vp8Set4(dst, stride, 0, 3, vp8Avg3(left[2], left[1], left[0]));
            try vp8Set4(dst, stride, 0, 2, vp8Avg3(left[1], left[0], top_left));
            try vp8Set4(dst, stride, 0, 1, vp8Avg3(left[0], top_left, top[0]));
            try vp8Set4(dst, stride, 1, 3, vp8Avg3(left[0], top_left, top[0]));
            try vp8Set4(dst, stride, 1, 1, vp8Avg3(top_left, top[0], top[1]));
            try vp8Set4(dst, stride, 2, 3, vp8Avg3(top_left, top[0], top[1]));
            try vp8Set4(dst, stride, 2, 1, vp8Avg3(top[0], top[1], top[2]));
            try vp8Set4(dst, stride, 3, 3, vp8Avg3(top[0], top[1], top[2]));
            try vp8Set4(dst, stride, 3, 1, vp8Avg3(top[1], top[2], top[3]));
        },
        .vertical_left => {
            if (top.len < 8) return error.WebpDecodeFailed;
            try vp8Set4(dst, stride, 0, 0, vp8Avg2(top[0], top[1]));
            try vp8Set4(dst, stride, 1, 0, vp8Avg2(top[1], top[2]));
            try vp8Set4(dst, stride, 0, 2, vp8Avg2(top[1], top[2]));
            try vp8Set4(dst, stride, 2, 0, vp8Avg2(top[2], top[3]));
            try vp8Set4(dst, stride, 1, 2, vp8Avg2(top[2], top[3]));
            try vp8Set4(dst, stride, 3, 0, vp8Avg2(top[3], top[4]));
            try vp8Set4(dst, stride, 2, 2, vp8Avg2(top[3], top[4]));
            try vp8Set4(dst, stride, 0, 1, vp8Avg3(top[0], top[1], top[2]));
            try vp8Set4(dst, stride, 1, 1, vp8Avg3(top[1], top[2], top[3]));
            try vp8Set4(dst, stride, 0, 3, vp8Avg3(top[1], top[2], top[3]));
            try vp8Set4(dst, stride, 2, 1, vp8Avg3(top[2], top[3], top[4]));
            try vp8Set4(dst, stride, 1, 3, vp8Avg3(top[2], top[3], top[4]));
            try vp8Set4(dst, stride, 3, 1, vp8Avg3(top[3], top[4], top[5]));
            try vp8Set4(dst, stride, 2, 3, vp8Avg3(top[3], top[4], top[5]));
            try vp8Set4(dst, stride, 3, 2, vp8Avg3(top[4], top[5], top[6]));
            try vp8Set4(dst, stride, 3, 3, vp8Avg3(top[5], top[6], top[7]));
        },
        .horizontal_up => {
            if (left.len < 4) return error.WebpDecodeFailed;
            try vp8Set4(dst, stride, 0, 0, vp8Avg2(left[0], left[1]));
            try vp8Set4(dst, stride, 2, 0, vp8Avg2(left[1], left[2]));
            try vp8Set4(dst, stride, 0, 1, vp8Avg2(left[1], left[2]));
            try vp8Set4(dst, stride, 2, 1, vp8Avg2(left[2], left[3]));
            try vp8Set4(dst, stride, 0, 2, vp8Avg2(left[2], left[3]));
            try vp8Set4(dst, stride, 1, 0, vp8Avg3(left[0], left[1], left[2]));
            try vp8Set4(dst, stride, 3, 0, vp8Avg3(left[1], left[2], left[3]));
            try vp8Set4(dst, stride, 1, 1, vp8Avg3(left[1], left[2], left[3]));
            try vp8Set4(dst, stride, 3, 1, vp8Avg3(left[2], left[3], left[3]));
            try vp8Set4(dst, stride, 1, 2, vp8Avg3(left[2], left[3], left[3]));
            try vp8Set4(dst, stride, 3, 2, left[3]);
            try vp8Set4(dst, stride, 2, 2, left[3]);
            try vp8Set4(dst, stride, 0, 3, left[3]);
            try vp8Set4(dst, stride, 1, 3, left[3]);
            try vp8Set4(dst, stride, 2, 3, left[3]);
            try vp8Set4(dst, stride, 3, 3, left[3]);
        },
        .horizontal_down => {
            if (top.len < 3 or left.len < 4) return error.WebpDecodeFailed;
            try vp8Set4(dst, stride, 0, 0, vp8Avg2(left[0], top_left));
            try vp8Set4(dst, stride, 2, 1, vp8Avg2(left[0], top_left));
            try vp8Set4(dst, stride, 0, 1, vp8Avg2(left[1], left[0]));
            try vp8Set4(dst, stride, 2, 2, vp8Avg2(left[1], left[0]));
            try vp8Set4(dst, stride, 0, 2, vp8Avg2(left[2], left[1]));
            try vp8Set4(dst, stride, 2, 3, vp8Avg2(left[2], left[1]));
            try vp8Set4(dst, stride, 0, 3, vp8Avg2(left[3], left[2]));
            try vp8Set4(dst, stride, 3, 0, vp8Avg3(top[0], top[1], top[2]));
            try vp8Set4(dst, stride, 2, 0, vp8Avg3(top_left, top[0], top[1]));
            try vp8Set4(dst, stride, 1, 0, vp8Avg3(left[0], top_left, top[0]));
            try vp8Set4(dst, stride, 3, 1, vp8Avg3(left[0], top_left, top[0]));
            try vp8Set4(dst, stride, 1, 1, vp8Avg3(left[1], left[0], top_left));
            try vp8Set4(dst, stride, 3, 2, vp8Avg3(left[1], left[0], top_left));
            try vp8Set4(dst, stride, 1, 2, vp8Avg3(left[2], left[1], left[0]));
            try vp8Set4(dst, stride, 3, 3, vp8Avg3(left[2], left[1], left[0]));
            try vp8Set4(dst, stride, 1, 3, vp8Avg3(left[3], left[2], left[1]));
        },
    }
}

fn clip8(value: i32) u8 {
    if (value < 0) return 0;
    if (value > 255) return 255;
    return @intCast(value);
}

fn parseVp8FirstPartitionSyntax(first_partition: []const u8) !Vp8FirstPartitionSyntax {
    var reader = Vp8BoolReader.init(first_partition);
    return try readVp8FirstPartitionSyntax(&reader);
}

fn parseVp8KeyframeControl(first_partition: []const u8) !Vp8KeyframeControl {
    var reader = Vp8BoolReader.init(first_partition);
    const syntax = try readVp8FirstPartitionSyntax(&reader);
    const entropy = try vp8ReadDefaultEntropyHeader(&reader);
    return .{
        .syntax = syntax,
        .entropy = entropy,
        .mode_reader = reader,
    };
}

fn readVp8FirstPartitionSyntax(reader: *Vp8BoolReader) !Vp8FirstPartitionSyntax {
    const color_space = try reader.readBit();
    const clamp_type = try reader.readBit();
    const segmentation = try parseVp8SegmentationHeader(reader);
    const loop_filter = try parseVp8LoopFilterHeader(reader);
    const token_partition_count = @as(usize, 1) << @as(u3, @intCast(try reader.readValue(2)));
    const quant = try parseVp8QuantHeader(reader);
    const refresh_entropy_probs = try reader.readBit();
    return .{
        .color_space = color_space,
        .clamp_type = clamp_type,
        .segmentation = segmentation,
        .loop_filter = loop_filter,
        .token_partition_count = token_partition_count,
        .quant = quant,
        .refresh_entropy_probs = refresh_entropy_probs,
    };
}

fn parseVp8SegmentationHeader(reader: *Vp8BoolReader) !Vp8SegmentationHeader {
    var header = Vp8SegmentationHeader{};
    header.enabled = try reader.readBit();
    if (!header.enabled) return header;

    header.update_map = try reader.readBit();
    header.update_data = try reader.readBit();
    if (header.update_data) {
        header.absolute_delta = try reader.readBit();
        for (&header.quantizer) |*quantizer| {
            quantizer.* = if (try reader.readBit()) try reader.readSignedValue(7) else 0;
        }
        for (&header.filter_strength) |*strength| {
            strength.* = if (try reader.readBit()) try reader.readSignedValue(6) else 0;
        }
    }
    if (header.update_map) {
        for (&header.segment_probs) |*prob| {
            prob.* = if (try reader.readBit()) @intCast(try reader.readValue(8)) else 255;
        }
    }
    return header;
}

fn parseVp8LoopFilterHeader(reader: *Vp8BoolReader) !Vp8LoopFilterHeader {
    var header = Vp8LoopFilterHeader{};
    header.simple = try reader.readBit();
    header.level = @intCast(try reader.readValue(6));
    header.sharpness = @intCast(try reader.readValue(3));
    header.use_lf_delta = try reader.readBit();
    if (header.use_lf_delta and try reader.readBit()) {
        for (&header.ref_lf_delta) |*delta| {
            if (try reader.readBit()) delta.* = try reader.readSignedValue(6);
        }
        for (&header.mode_lf_delta) |*delta| {
            if (try reader.readBit()) delta.* = try reader.readSignedValue(6);
        }
    }
    return header;
}

fn parseVp8QuantHeader(reader: *Vp8BoolReader) !Vp8QuantHeader {
    return .{
        .base_q = @intCast(try reader.readValue(7)),
        .y1_dc_delta = try readVp8OptionalSignedDelta(reader),
        .y2_dc_delta = try readVp8OptionalSignedDelta(reader),
        .y2_ac_delta = try readVp8OptionalSignedDelta(reader),
        .uv_dc_delta = try readVp8OptionalSignedDelta(reader),
        .uv_ac_delta = try readVp8OptionalSignedDelta(reader),
    };
}

fn readVp8OptionalSignedDelta(reader: *Vp8BoolReader) !i16 {
    return if (try reader.readBit()) try reader.readSignedValue(4) else 0;
}

fn vp8ClipQuantIndex(value: i16, max_value: u7) u7 {
    if (value < 0) return 0;
    if (value > max_value) return max_value;
    return @intCast(value);
}

fn vp8QuantTableDc(index: i16, max_value: u7) i16 {
    return @intCast(vp8_dc_quant_table[vp8ClipQuantIndex(index, max_value)]);
}

fn vp8QuantTableAc(index: i16, max_value: u7) i16 {
    return @intCast(vp8_ac_quant_table[vp8ClipQuantIndex(index, max_value)]);
}

fn vp8BuildQuantMatrix(base_q: i16, quant: Vp8QuantHeader) Vp8QuantMatrix {
    const y2_ac = (@as(i32, vp8QuantTableAc(base_q + quant.y2_ac_delta, 127)) * 101581) >> 16;
    return .{
        .y1 = .{
            vp8QuantTableDc(base_q + quant.y1_dc_delta, 127),
            vp8QuantTableAc(base_q, 127),
        },
        .y2 = .{
            vp8QuantTableDc(base_q + quant.y2_dc_delta, 127) * 2,
            @intCast(@max(y2_ac, 8)),
        },
        .uv = .{
            vp8QuantTableDc(base_q + quant.uv_dc_delta, 117),
            vp8QuantTableAc(base_q + quant.uv_ac_delta, 127),
        },
        .uv_quant = base_q + quant.uv_ac_delta,
    };
}

fn vp8BuildSegmentQuantMatrices(syntax: Vp8FirstPartitionSyntax) [4]Vp8QuantMatrix {
    var matrices: [4]Vp8QuantMatrix = undefined;
    const base_q: i16 = syntax.quant.base_q;
    var segment: usize = 0;
    while (segment < matrices.len) : (segment += 1) {
        if (!syntax.segmentation.enabled) {
            if (segment == 0) {
                matrices[segment] = vp8BuildQuantMatrix(base_q, syntax.quant);
            } else {
                matrices[segment] = matrices[0];
            }
            continue;
        }

        const segment_q = if (syntax.segmentation.absolute_delta)
            syntax.segmentation.quantizer[segment]
        else
            base_q + syntax.segmentation.quantizer[segment];
        matrices[segment] = vp8BuildQuantMatrix(segment_q, syntax.quant);
    }
    return matrices;
}

fn vp8ReadSegmentId(reader: *Vp8BoolReader, segmentation: Vp8SegmentationHeader) !u2 {
    if (!segmentation.enabled or !segmentation.update_map) return 0;
    if (!try reader.readBool(segmentation.segment_probs[0])) {
        return if (try reader.readBool(segmentation.segment_probs[1])) 1 else 0;
    }
    return 2 + @as(u2, @intFromBool(try reader.readBool(segmentation.segment_probs[2])));
}

fn vp8ReadLuma16Mode(reader: *Vp8BoolReader) !Vp8Luma16Mode {
    if (try reader.readBool(156)) {
        return if (try reader.readBool(128)) .true_motion else .horizontal;
    }
    return if (try reader.readBool(163)) .vertical else .dc;
}

fn vp8ReadChromaMode(reader: *Vp8BoolReader) !Vp8ChromaMode {
    if (!try reader.readBool(142)) return .dc;
    if (!try reader.readBool(114)) return .vertical;
    return if (try reader.readBool(183)) .true_motion else .horizontal;
}

fn vp8ReadLuma4Mode(reader: *Vp8BoolReader, probs: *const [9]u8) !Vp8Luma4Mode {
    if (!try reader.readBool(probs[0])) return .dc;
    if (!try reader.readBool(probs[1])) return .true_motion;
    if (!try reader.readBool(probs[2])) return .vertical;
    if (!try reader.readBool(probs[3])) {
        if (!try reader.readBool(probs[4])) return .horizontal;
        return if (!try reader.readBool(probs[5])) .down_right else .vertical_right;
    }
    if (!try reader.readBool(probs[6])) return .down_left;
    if (!try reader.readBool(probs[7])) return .vertical_left;
    return if (!try reader.readBool(probs[8])) .horizontal_down else .horizontal_up;
}

fn vp8Luma4ProbIndex(mode: Vp8Luma4Mode) usize {
    return switch (mode) {
        .down_left => 4,
        .down_right => 5,
        .vertical_right => 6,
        else => @backingInt(mode),
    };
}

fn vp8ReadLuma4ModeGrid(
    reader: *Vp8BoolReader,
    probs: *const Vp8Luma4Probs,
    top_modes: *[4]Vp8Luma4Mode,
    left_modes: *[4]Vp8Luma4Mode,
) ![16]Vp8Luma4Mode {
    var modes: [16]Vp8Luma4Mode = undefined;
    var y: usize = 0;
    while (y < 4) : (y += 1) {
        var left = left_modes[y];
        var x: usize = 0;
        while (x < 4) : (x += 1) {
            const mode = try vp8ReadLuma4Mode(reader, &probs[vp8Luma4ProbIndex(top_modes[x])][vp8Luma4ProbIndex(left)]);
            modes[y * 4 + x] = mode;
            top_modes[x] = mode;
            left = mode;
        }
        left_modes[y] = left;
    }
    return modes;
}

fn vp8ReadMacroblockHeader(
    reader: *Vp8BoolReader,
    syntax: Vp8FirstPartitionSyntax,
    skip_probability: ?u8,
    top_luma4_modes: *[4]Vp8Luma4Mode,
    left_luma4_modes: *[4]Vp8Luma4Mode,
    luma4_probs: *const Vp8Luma4Probs,
) !Vp8MacroblockHeader {
    var header = Vp8MacroblockHeader{
        .segment = try vp8ReadSegmentId(reader, syntax.segmentation),
        .skip = false,
        .is_i4x4 = false,
        .luma16_mode = null,
        .luma4_modes = null,
        .chroma_mode = .dc,
    };
    if (skip_probability) |prob| header.skip = try reader.readBool(prob);

    header.is_i4x4 = !try reader.readBool(145);
    if (header.is_i4x4) {
        header.luma4_modes = try vp8ReadLuma4ModeGrid(reader, luma4_probs, top_luma4_modes, left_luma4_modes);
    } else {
        const mode = try vp8ReadLuma16Mode(reader);
        header.luma16_mode = mode;
        const mode4: Vp8Luma4Mode = switch (mode) {
            .dc, .dc_no_top, .dc_no_left, .dc_no_top_left => .dc,
            .true_motion => .true_motion,
            .vertical => .vertical,
            .horizontal => .horizontal,
        };
        @memset(top_luma4_modes, mode4);
        @memset(left_luma4_modes, mode4);
    }

    header.chroma_mode = try vp8ReadChromaMode(reader);
    return header;
}

fn vp8ReadMacroblockHeader16x16(
    reader: *Vp8BoolReader,
    syntax: Vp8FirstPartitionSyntax,
    skip_probability: ?u8,
) !Vp8MacroblockHeader {
    var header = Vp8MacroblockHeader{
        .segment = try vp8ReadSegmentId(reader, syntax.segmentation),
        .skip = false,
        .is_i4x4 = false,
        .luma16_mode = null,
        .luma4_modes = null,
        .chroma_mode = .dc,
    };
    if (skip_probability) |prob| header.skip = try reader.readBool(prob);

    header.is_i4x4 = !try reader.readBool(145);
    if (header.is_i4x4) return error.UnsupportedWebpFormat;

    header.luma16_mode = try vp8ReadLuma16Mode(reader);
    header.chroma_mode = try vp8ReadChromaMode(reader);
    return header;
}

fn vp8CoeffProbsFromFlat(flat: *const [vp8_coeff_probs_flat_count]u8) Vp8CoeffProbs {
    var out: Vp8CoeffProbs = undefined;
    var i: usize = 0;
    var coeff_type: usize = 0;
    while (coeff_type < vp8_coeff_type_count) : (coeff_type += 1) {
        var band: usize = 0;
        while (band < vp8_coeff_band_count) : (band += 1) {
            var context: usize = 0;
            while (context < vp8_coeff_context_count) : (context += 1) {
                var proba: usize = 0;
                while (proba < vp8_coeff_proba_count) : (proba += 1) {
                    out[coeff_type][band][context][proba] = flat[i];
                    i += 1;
                }
            }
        }
    }
    return out;
}

fn vp8DefaultCoeffProbs() Vp8CoeffProbs {
    return vp8CoeffProbsFromFlat(&vp8_default_coeff_probs_flat);
}

fn vp8CoeffUpdateProbs() Vp8CoeffProbs {
    var flat = @as([vp8_coeff_probs_flat_count]u8, @splat(255));
    for (vp8_coeff_update_prob_overrides) |override| {
        flat[override.index] = override.value;
    }
    return vp8CoeffProbsFromFlat(&flat);
}

fn vp8ReadDefaultEntropyHeader(reader: *Vp8BoolReader) !Vp8EntropyHeader {
    const base = vp8DefaultCoeffProbs();
    const update = vp8CoeffUpdateProbs();
    return try vp8ReadEntropyHeader(reader, base, &update);
}

fn vp8ReadEntropyHeader(
    reader: *Vp8BoolReader,
    base_coeff_probs: Vp8CoeffProbs,
    update_probs: *const Vp8CoeffProbs,
) !Vp8EntropyHeader {
    var entropy = Vp8EntropyHeader{
        .coeff_probs = base_coeff_probs,
        .skip_probability = null,
    };
    var coeff_type: usize = 0;
    while (coeff_type < vp8_coeff_type_count) : (coeff_type += 1) {
        var band: usize = 0;
        while (band < vp8_coeff_band_count) : (band += 1) {
            var context: usize = 0;
            while (context < vp8_coeff_context_count) : (context += 1) {
                var proba: usize = 0;
                while (proba < vp8_coeff_proba_count) : (proba += 1) {
                    if (try reader.readBool(update_probs[coeff_type][band][context][proba])) {
                        entropy.coeff_probs[coeff_type][band][context][proba] = @intCast(try reader.readValue(8));
                    }
                }
            }
        }
    }
    if (try reader.readBit()) {
        entropy.skip_probability = @intCast(try reader.readValue(8));
    }
    return entropy;
}

fn vp8ReadLargeCoeffValue(reader: *Vp8BoolReader, probs: *const [vp8_coeff_proba_count]u8) !i16 {
    if (!try reader.readBool(probs[3])) {
        return if (!try reader.readBool(probs[4])) 2 else 3 + @as(i16, @intFromBool(try reader.readBool(probs[5])));
    }
    if (!try reader.readBool(probs[6])) {
        if (!try reader.readBool(probs[7])) return 5 + @as(i16, @intFromBool(try reader.readBool(159)));
        return 7 + 2 * @as(i16, @intFromBool(try reader.readBool(165))) + @as(i16, @intFromBool(try reader.readBool(145)));
    }

    const bit1: usize = @intFromBool(try reader.readBool(probs[8]));
    const bit0: usize = @intFromBool(try reader.readBool(probs[9 + bit1]));
    const category = 2 * bit1 + bit0;
    const cat_probs = switch (category) {
        0 => vp8_coeff_cat3[0..],
        1 => vp8_coeff_cat4[0..],
        2 => vp8_coeff_cat5[0..],
        3 => vp8_coeff_cat6[0..],
        else => unreachable,
    };

    var value: i16 = 0;
    for (cat_probs) |prob| {
        value = value * 2 + @as(i16, @intFromBool(try reader.readBool(prob)));
    }
    return value + 3 + (@as(i16, 8) << @intCast(category));
}

fn vp8SignedCoeff(reader: *Vp8BoolReader, magnitude: i16, quant: i16) !i16 {
    const signed = if (try reader.readBit()) -magnitude else magnitude;
    const dequantized = @as(i32, signed) * @as(i32, quant);
    return std.math.cast(i16, dequantized) orelse error.UnsupportedWebpFormat;
}

fn vp8ReadCoeffBlock(
    reader: *Vp8BoolReader,
    probs: *const Vp8CoeffProbs,
    coeff_type: usize,
    context: usize,
    start: u5,
    dequant: [2]i16,
) !Vp8CoeffBlock {
    if (coeff_type >= vp8_coeff_type_count or context >= vp8_coeff_context_count or start > vp8_coeff_block_size) {
        return error.WebpDecodeFailed;
    }

    var block = Vp8CoeffBlock{};
    var n: usize = start;
    var ctx = context;
    while (n < vp8_coeff_block_size) : (n += 1) {
        var p = &probs[coeff_type][vp8_coeff_bands[n]][ctx];
        if (!try reader.readBool(p[0])) return block;

        while (!try reader.readBool(p[1])) {
            n += 1;
            if (n == vp8_coeff_block_size) {
                block.last_nonzero_plus_one = vp8_coeff_block_size;
                return block;
            }
            p = &probs[coeff_type][vp8_coeff_bands[n]][0];
        }

        const magnitude = if (!try reader.readBool(p[2])) @as(i16, 1) else try vp8ReadLargeCoeffValue(reader, p);
        const quant = if (n == 0) dequant[0] else dequant[1];
        block.coeffs[vp8_zigzag[n]] = try vp8SignedCoeff(reader, magnitude, quant);
        block.last_nonzero_plus_one = @intCast(n + 1);
        ctx = if (magnitude == 1) 1 else 2;
    }
    return block;
}

fn vp8CoeffHasToken(block: Vp8CoeffBlock, first_coeff: u5) u1 {
    return @intFromBool(block.last_nonzero_plus_one > first_coeff);
}

fn vp8ReadMacroblockCoeffs(
    reader: *Vp8BoolReader,
    probs: *const Vp8CoeffProbs,
    header: Vp8MacroblockHeader,
    quant: Vp8QuantMatrix,
    context: *Vp8MacroblockTokenContext,
) !Vp8MacroblockCoeffs {
    var coeffs = Vp8MacroblockCoeffs{};
    if (header.skip) {
        const y2_above = context.y2_above;
        const y2_left = context.y2_left;
        context.reset();
        if (header.is_i4x4) {
            // I4x4 macroblocks do not carry a Y2 block, so their skip flag must
            // not clear the Y2 token context used by the next Y16 macroblock.
            context.y2_above = y2_above;
            context.y2_left = y2_left;
        }
        return coeffs;
    }

    const y_coeff_type: usize = if (header.is_i4x4) 3 else 0;
    const y_start: u5 = if (header.is_i4x4) 0 else 1;
    if (!header.is_i4x4) {
        const y2_context: usize = @as(usize, context.y2_above) + @as(usize, context.y2_left);
        coeffs.y2 = try vp8ReadCoeffBlock(reader, probs, 1, y2_context, 0, quant.y2);
        const y2_has_token = vp8CoeffHasToken(coeffs.y2, 0);
        context.y2_above = y2_has_token;
        context.y2_left = y2_has_token;
    }

    var block_index: usize = 0;
    while (block_index < coeffs.y.len) : (block_index += 1) {
        const bx = block_index & 3;
        const by = block_index >> 2;
        const token_context: usize = @as(usize, context.y_above[bx]) + @as(usize, context.y_left[by]);
        coeffs.y[block_index] = try vp8ReadCoeffBlock(reader, probs, y_coeff_type, token_context, y_start, quant.y1);
        const has_token = vp8CoeffHasToken(coeffs.y[block_index], y_start);
        context.y_above[bx] = has_token;
        context.y_left[by] = has_token;
    }

    block_index = 0;
    while (block_index < coeffs.u.len) : (block_index += 1) {
        const bx = block_index & 1;
        const by = block_index >> 1;
        const u_context: usize = @as(usize, context.u_above[bx]) + @as(usize, context.u_left[by]);
        coeffs.u[block_index] = try vp8ReadCoeffBlock(reader, probs, 2, u_context, 0, quant.uv);
        const u_has_token = vp8CoeffHasToken(coeffs.u[block_index], 0);
        context.u_above[bx] = u_has_token;
        context.u_left[by] = u_has_token;
    }

    block_index = 0;
    while (block_index < coeffs.v.len) : (block_index += 1) {
        const bx = block_index & 1;
        const by = block_index >> 1;
        const v_context: usize = @as(usize, context.v_above[bx]) + @as(usize, context.v_left[by]);
        coeffs.v[block_index] = try vp8ReadCoeffBlock(reader, probs, 2, v_context, 0, quant.uv);
        const v_has_token = vp8CoeffHasToken(coeffs.v[block_index], 0);
        context.v_above[bx] = v_has_token;
        context.v_left[by] = v_has_token;
    }

    return coeffs;
}

fn parseVp8lHeader(payload: []const u8) !struct { width: u32, height: u32, alpha: bool } {
    if (payload.len < 5) return error.WebpDecodeFailed;
    if (payload[0] != 0x2f) return error.WebpDecodeFailed;
    const b0: u32 = payload[1];
    const b1: u32 = payload[2];
    const b2: u32 = payload[3];
    const b3: u32 = payload[4];
    const version = b3 >> 5;
    if (version != 0) return error.UnsupportedWebpFormat;
    return .{
        .width = 1 + (((b1 & 0x3f) << 8) | b0),
        .height = 1 + (((b3 & 0x0f) << 10) | (b2 << 2) | ((b1 & 0xc0) >> 6)),
        .alpha = (b3 & 0x10) != 0,
    };
}

fn mergeDimensions(info: *Info, width: u32, height: u32) !void {
    if (width == 0 or height == 0) return error.WebpDecodeFailed;
    if (info.width) |existing_width| {
        if (existing_width != width) return error.WebpDecodeFailed;
    } else {
        info.width = width;
    }
    if (info.height) |existing_height| {
        if (existing_height != height) return error.WebpDecodeFailed;
    } else {
        info.height = height;
    }
}

const BitReader = struct {
    bytes: []const u8,
    bit_pos: usize = 0,

    fn init(bytes: []const u8) BitReader {
        return .{ .bytes = bytes };
    }

    fn readBits(self: *BitReader, count: u5) !u32 {
        var value: u32 = 0;
        var shift: u5 = 0;
        while (shift < count) : (shift += 1) {
            if (self.bit_pos >= self.bytes.len * 8) return error.WebpDecodeFailed;
            const byte = self.bytes[self.bit_pos / 8];
            const bit_index: u3 = @intCast(self.bit_pos & 7);
            const bit = (byte >> bit_index) & 1;
            value |= @as(u32, bit) << shift;
            self.bit_pos += 1;
        }
        return value;
    }
};

const max_vp8l_color_cache_bits = 11;
const max_vp8l_color_cache_size = 1 << max_vp8l_color_cache_bits;
const max_vp8l_symbol_count = 256 + 24 + max_vp8l_color_cache_size;
const vp8l_code_length_code_count = 19;
const vp8l_max_code_bits = 15;
const vp8l_code_length_order = [_]u8{ 17, 18, 0, 1, 2, 3, 4, 5, 16, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
const vp8l_literal_count = 256;
const vp8l_length_code_count = 24;
const vp8l_plane_code_count = 120;
const vp8l_transform_predictor = 0;
const vp8l_transform_cross_color = 1;
const vp8l_transform_subtract_green = 2;
const vp8l_transform_color_indexing = 3;
const vp8l_code_to_plane = [_]u8{
    0x18, 0x07, 0x17, 0x19, 0x28, 0x06, 0x27, 0x29, 0x16, 0x1a, 0x26, 0x2a,
    0x38, 0x05, 0x37, 0x39, 0x15, 0x1b, 0x36, 0x3a, 0x25, 0x2b, 0x48, 0x04,
    0x47, 0x49, 0x14, 0x1c, 0x35, 0x3b, 0x46, 0x4a, 0x24, 0x2c, 0x58, 0x45,
    0x4b, 0x34, 0x3c, 0x03, 0x57, 0x59, 0x13, 0x1d, 0x56, 0x5a, 0x23, 0x2d,
    0x44, 0x4c, 0x55, 0x5b, 0x33, 0x3d, 0x68, 0x02, 0x67, 0x69, 0x12, 0x1e,
    0x66, 0x6a, 0x22, 0x2e, 0x54, 0x5c, 0x43, 0x4d, 0x65, 0x6b, 0x32, 0x3e,
    0x78, 0x01, 0x77, 0x79, 0x53, 0x5d, 0x11, 0x1f, 0x64, 0x6c, 0x42, 0x4e,
    0x76, 0x7a, 0x21, 0x2f, 0x75, 0x7b, 0x31, 0x3f, 0x63, 0x6d, 0x52, 0x5e,
    0x00, 0x74, 0x7c, 0x41, 0x4f, 0x10, 0x20, 0x62, 0x6e, 0x30, 0x73, 0x7d,
    0x51, 0x5f, 0x40, 0x72, 0x7e, 0x61, 0x6f, 0x50, 0x71, 0x7f, 0x60, 0x70,
};

const PrefixCode = struct {
    const Entry = struct {
        code: u16,
        bits: u4,
        symbol: u16,
    };

    entries: [max_vp8l_symbol_count]Entry = undefined,
    entry_count: usize = 0,
    max_bits: u4 = 0,

    fn read(reader: *BitReader, alphabet_size: usize) !PrefixCode {
        if (alphabet_size == 0 or alphabet_size > max_vp8l_symbol_count) return error.WebpDecodeFailed;
        if ((try reader.readBits(1)) != 0) {
            return try readSimple(reader, alphabet_size);
        }
        return try readNormal(reader, alphabet_size);
    }

    fn readSimple(reader: *BitReader, alphabet_size: usize) !PrefixCode {
        var lengths = @as([max_vp8l_symbol_count]u8, @splat(0));
        const num_symbols = (try reader.readBits(1)) + 1;
        const is_first_8bits = try reader.readBits(1);
        const first_bits: u5 = if (is_first_8bits == 0) 1 else 8;
        const symbol0: usize = @intCast(try reader.readBits(first_bits));
        if (symbol0 >= alphabet_size) return error.WebpDecodeFailed;
        lengths[symbol0] = 1;
        if (num_symbols == 2) {
            const symbol1: usize = @intCast(try reader.readBits(8));
            if (symbol1 >= alphabet_size) return error.WebpDecodeFailed;
            if (symbol1 == symbol0) return error.WebpDecodeFailed;
            lengths[symbol1] = 1;
        }
        return try build(lengths[0..alphabet_size]);
    }

    fn readNormal(reader: *BitReader, alphabet_size: usize) !PrefixCode {
        const num_code_lengths = 4 + try reader.readBits(4);
        var code_length_lengths = @as([vp8l_code_length_code_count]u8, @splat(0));
        var i: usize = 0;
        while (i < num_code_lengths) : (i += 1) {
            code_length_lengths[vp8l_code_length_order[i]] = @intCast(try reader.readBits(3));
        }

        const code_length_code = try build(code_length_lengths[0..]);
        const max_symbol = if ((try reader.readBits(1)) == 0) alphabet_size else blk: {
            const length_nbits: u5 = @intCast(2 + 2 * try reader.readBits(3));
            const limited = 2 + try reader.readBits(length_nbits);
            if (limited > alphabet_size) return error.WebpDecodeFailed;
            break :blk @as(usize, @intCast(limited));
        };
        if (max_symbol == 0) return error.WebpDecodeFailed;

        var lengths = @as([max_vp8l_symbol_count]u8, @splat(0));
        var symbol_index: usize = 0;
        var previous_nonzero: ?u8 = null;
        while (symbol_index < max_symbol) {
            const code = try code_length_code.readSymbol(reader);
            switch (code) {
                0...15 => {
                    lengths[symbol_index] = @intCast(code);
                    if (code != 0) previous_nonzero = @intCast(code);
                    symbol_index += 1;
                },
                16 => {
                    const repeat = 3 + try reader.readBits(2);
                    const repeated_length = previous_nonzero orelse return error.WebpDecodeFailed;
                    if (symbol_index + repeat > max_symbol) return error.WebpDecodeFailed;
                    for (0..repeat) |_| {
                        lengths[symbol_index] = repeated_length;
                        symbol_index += 1;
                    }
                },
                17 => {
                    const repeat = 3 + try reader.readBits(3);
                    if (symbol_index + repeat > max_symbol) return error.WebpDecodeFailed;
                    symbol_index += repeat;
                },
                18 => {
                    const repeat = 11 + try reader.readBits(7);
                    if (symbol_index + repeat > max_symbol) return error.WebpDecodeFailed;
                    symbol_index += repeat;
                },
                else => return error.WebpDecodeFailed,
            }
        }

        return try build(lengths[0..alphabet_size]);
    }

    fn build(lengths: []const u8) !PrefixCode {
        if (lengths.len == 0 or lengths.len > max_vp8l_symbol_count) return error.WebpDecodeFailed;

        var counts = @as([(vp8l_max_code_bits + 1)]u16, @splat(0));
        var nonzero_count: usize = 0;
        var max_bits: u4 = 0;
        for (lengths) |raw_len| {
            if (raw_len > vp8l_max_code_bits) return error.WebpDecodeFailed;
            if (raw_len == 0) continue;
            counts[raw_len] += 1;
            nonzero_count += 1;
            if (raw_len > max_bits) max_bits = @intCast(raw_len);
        }
        if (nonzero_count == 0) return error.WebpDecodeFailed;

        var left: i32 = 1;
        for (1..vp8l_max_code_bits + 1) |bits| {
            left = (left << 1) - counts[bits];
            if (left < 0) return error.WebpDecodeFailed;
        }
        if (left != 0 and nonzero_count != 1) return error.WebpDecodeFailed;

        var next_code = @as([(vp8l_max_code_bits + 1)]u16, @splat(0));
        var code: u16 = 0;
        for (1..vp8l_max_code_bits + 1) |bits| {
            code = (code + counts[bits - 1]) << 1;
            next_code[bits] = code;
        }

        var out = PrefixCode{ .max_bits = max_bits };
        for (lengths, 0..) |raw_len, symbol| {
            if (raw_len == 0) continue;
            const bits: u4 = @intCast(raw_len);
            const canonical = next_code[bits];
            next_code[bits] += 1;
            out.entries[out.entry_count] = .{
                .code = reverseBits(canonical, bits),
                .bits = bits,
                .symbol = @intCast(symbol),
            };
            out.entry_count += 1;
        }
        return out;
    }

    fn readSymbol(self: PrefixCode, reader: *BitReader) !u16 {
        if (self.entry_count == 1) return self.entries[0].symbol;

        var code: u16 = 0;
        var bits: u4 = 1;
        while (bits <= self.max_bits) : (bits += 1) {
            const bit = try reader.readBits(1);
            code |= @as(u16, @intCast(bit)) << (bits - 1);
            for (self.entries[0..self.entry_count]) |entry| {
                if (entry.bits == bits and entry.code == code) return entry.symbol;
            }
        }
        return error.WebpDecodeFailed;
    }
};

fn reverseBits(value: u16, bit_count: u4) u16 {
    var out: u16 = 0;
    var i: u4 = 0;
    while (i < bit_count) : (i += 1) {
        out = (out << 1) | ((value >> i) & 1);
    }
    return out;
}

const PrefixCodeGroup = struct {
    green: PrefixCode,
    red: PrefixCode,
    blue: PrefixCode,
    alpha: PrefixCode,
    distance: PrefixCode,

    fn read(reader: *BitReader, color_cache_bits: u5) !PrefixCodeGroup {
        const color_cache_size: usize = if (color_cache_bits == 0) 0 else @as(usize, 1) << color_cache_bits;
        return .{
            .green = try PrefixCode.read(reader, vp8l_literal_count + vp8l_length_code_count + color_cache_size),
            .red = try PrefixCode.read(reader, vp8l_literal_count),
            .blue = try PrefixCode.read(reader, vp8l_literal_count),
            .alpha = try PrefixCode.read(reader, vp8l_literal_count),
            .distance = try PrefixCode.read(reader, 40),
        };
    }
};

const PrefixCodeGroups = struct {
    groups: []PrefixCodeGroup,
    huffman_bits: u5 = 0,
    huffman_width: u32 = 0,
    group_indices: []u16 = &.{},

    fn deinit(self: *PrefixCodeGroups, alloc: Allocator) void {
        alloc.free(self.groups);
        alloc.free(self.group_indices);
        self.* = undefined;
    }

    fn groupForPixel(self: PrefixCodeGroups, pixel_index: usize, width: u32) !*const PrefixCodeGroup {
        if (self.group_indices.len == 0) return &self.groups[0];
        const x = pixel_index % @as(usize, @intCast(width));
        const y = pixel_index / @as(usize, @intCast(width));
        const hx = x >> self.huffman_bits;
        const hy = y >> self.huffman_bits;
        const huffman_index = hy * @as(usize, @intCast(self.huffman_width)) + hx;
        if (huffman_index >= self.group_indices.len) return error.WebpDecodeFailed;
        const group_index = self.group_indices[huffman_index];
        if (group_index >= self.groups.len) return error.WebpDecodeFailed;
        return &self.groups[group_index];
    }
};

fn readCopyLength(reader: *BitReader, length_symbol: u16) !usize {
    if (length_symbol >= vp8l_length_code_count) return error.WebpDecodeFailed;
    return try readCopyPrefixValue(reader, length_symbol);
}

fn readCopyDistance(reader: *BitReader, distance_symbol: u16) !usize {
    if (distance_symbol >= 40) return error.WebpDecodeFailed;
    return try readCopyPrefixValue(reader, distance_symbol);
}

fn readCopyPrefixValue(reader: *BitReader, symbol: u16) !usize {
    if (symbol < 4) return @as(usize, symbol) + 1;

    const extra_bits_u16 = (symbol - 2) >> 1;
    const extra_bits: u5 = @intCast(extra_bits_u16);
    const offset = @as(usize, 2 + (symbol & 1)) << extra_bits;
    return offset + @as(usize, @intCast(try reader.readBits(extra_bits))) + 1;
}

fn planeCodeToDistance(width: u32, plane_code: usize) !usize {
    if (plane_code == 0) return error.WebpDecodeFailed;
    if (plane_code > vp8l_plane_code_count) return plane_code - vp8l_plane_code_count;

    const dist_code = vp8l_code_to_plane[plane_code - 1];
    const yoffset: usize = dist_code >> 4;
    const xoffset: isize = 8 - @as(isize, dist_code & 0x0f);
    const dist = @as(isize, @intCast(yoffset * @as(usize, @intCast(width)))) + xoffset;
    return if (dist >= 1) @intCast(dist) else 1;
}

const ColorCache = struct {
    colors: []u32,
    hash_shift: u5,

    fn init(alloc: Allocator, hash_bits: u5) !ColorCache {
        if (hash_bits < 1 or hash_bits > max_vp8l_color_cache_bits) return error.WebpDecodeFailed;
        const colors = try alloc.alloc(u32, @as(usize, 1) << hash_bits);
        @memset(colors, 0);
        return .{
            .colors = colors,
            .hash_shift = @intCast(32 - @as(u6, hash_bits)),
        };
    }

    fn deinit(self: *ColorCache, alloc: Allocator) void {
        alloc.free(self.colors);
        self.* = undefined;
    }

    fn insert(self: *ColorCache, argb: u32) void {
        self.colors[vp8lColorCacheKey(argb, self.hash_shift)] = argb;
    }

    fn lookup(self: ColorCache, key: usize) !u32 {
        if (key >= self.colors.len) return error.WebpDecodeFailed;
        return self.colors[key];
    }
};

const PredictorTransform = struct {
    bits: u5,
    width: u32,
    height: u32,
    rgba: []u8,
};

const CrossColorTransform = struct {
    bits: u5,
    width: u32,
    height: u32,
    rgba: []u8,
};

const ColorIndexingTransform = struct {
    bits: u5,
    width: u32,
    height: u32,
    palette: []u8,
};

const Vp8lTransform = union(enum) {
    subtract_green,
    predictor: PredictorTransform,
    cross_color: CrossColorTransform,
    color_indexing: ColorIndexingTransform,

    fn deinit(self: *Vp8lTransform, alloc: Allocator) void {
        switch (self.*) {
            .predictor => |predictor| alloc.free(predictor.rgba),
            .cross_color => |cross_color| alloc.free(cross_color.rgba),
            .color_indexing => |color_indexing| alloc.free(color_indexing.palette),
            .subtract_green => {},
        }
    }
};

fn vp8lColorCacheKey(argb: u32, hash_shift: u5) usize {
    return @intCast((argb *% 0x1e35a7bd) >> hash_shift);
}

fn packArgb(rgba: []const u8) u32 {
    return (@as(u32, rgba[3]) << 24) |
        (@as(u32, rgba[0]) << 16) |
        (@as(u32, rgba[1]) << 8) |
        @as(u32, rgba[2]);
}

fn writeArgbToRgba(argb: u32, rgba: []u8) void {
    rgba[0] = @intCast((argb >> 16) & 0xff);
    rgba[1] = @intCast((argb >> 8) & 0xff);
    rgba[2] = @intCast(argb & 0xff);
    rgba[3] = @intCast((argb >> 24) & 0xff);
}

fn applySubtractGreen(rgba: []u8) void {
    var pixel_index: usize = 0;
    while (pixel_index < rgba.len) : (pixel_index += 4) {
        const green = rgba[pixel_index + 1];
        rgba[pixel_index + 0] +%= green;
        rgba[pixel_index + 2] +%= green;
    }
}

fn addPixelsArgb(a: u32, b: u32) u32 {
    const alpha_green = (a & 0xff00ff00) +% (b & 0xff00ff00);
    const red_blue = (a & 0x00ff00ff) +% (b & 0x00ff00ff);
    return (alpha_green & 0xff00ff00) | (red_blue & 0x00ff00ff);
}

fn average2Argb(a: u32, b: u32) u32 {
    return (((a ^ b) & 0xfefefefe) >> 1) + (a & b);
}

fn average3Argb(a: u32, b: u32, c: u32) u32 {
    return average2Argb(average2Argb(a, c), b);
}

fn average4Argb(a: u32, b: u32, c: u32, d: u32) u32 {
    return average2Argb(average2Argb(a, b), average2Argb(c, d));
}

fn clip255(value: i32) u8 {
    if (value < 0) return 0;
    if (value > 255) return 255;
    return @intCast(value);
}

fn clampedAddSubtractFull(a: u32, b: u32, c: u32) u32 {
    const alpha = clip255(@as(i32, @intCast((a >> 24) & 0xff)) + @as(i32, @intCast((b >> 24) & 0xff)) - @as(i32, @intCast((c >> 24) & 0xff)));
    const red = clip255(@as(i32, @intCast((a >> 16) & 0xff)) + @as(i32, @intCast((b >> 16) & 0xff)) - @as(i32, @intCast((c >> 16) & 0xff)));
    const green = clip255(@as(i32, @intCast((a >> 8) & 0xff)) + @as(i32, @intCast((b >> 8) & 0xff)) - @as(i32, @intCast((c >> 8) & 0xff)));
    const blue = clip255(@as(i32, @intCast(a & 0xff)) + @as(i32, @intCast(b & 0xff)) - @as(i32, @intCast(c & 0xff)));
    return (@as(u32, alpha) << 24) | (@as(u32, red) << 16) | (@as(u32, green) << 8) | @as(u32, blue);
}

fn clampedAddSubtractHalf(a: u32, b: u32, c: u32) u32 {
    const average = average2Argb(a, b);
    const alpha = clampedAddSubtractHalfComponent((average >> 24) & 0xff, (c >> 24) & 0xff);
    const red = clampedAddSubtractHalfComponent((average >> 16) & 0xff, (c >> 16) & 0xff);
    const green = clampedAddSubtractHalfComponent((average >> 8) & 0xff, (c >> 8) & 0xff);
    const blue = clampedAddSubtractHalfComponent(average & 0xff, c & 0xff);
    return (@as(u32, alpha) << 24) | (@as(u32, red) << 16) | (@as(u32, green) << 8) | @as(u32, blue);
}

fn clampedAddSubtractHalfComponent(average: u32, c: u32) u8 {
    const avg_i32: i32 = @intCast(average);
    const c_i32: i32 = @intCast(c);
    return clip255(avg_i32 + @divTrunc(avg_i32 - c_i32, 2));
}

fn selectPredictor(a: u32, b: u32, c: u32) u32 {
    const pa_minus_pb =
        componentSelectDelta((a >> 24) & 0xff, (b >> 24) & 0xff, (c >> 24) & 0xff) +
        componentSelectDelta((a >> 16) & 0xff, (b >> 16) & 0xff, (c >> 16) & 0xff) +
        componentSelectDelta((a >> 8) & 0xff, (b >> 8) & 0xff, (c >> 8) & 0xff) +
        componentSelectDelta(a & 0xff, b & 0xff, c & 0xff);
    return if (pa_minus_pb <= 0) a else b;
}

fn componentSelectDelta(a: u32, b: u32, c: u32) i32 {
    const pb = @as(i32, @intCast(b)) - @as(i32, @intCast(c));
    const pa = @as(i32, @intCast(a)) - @as(i32, @intCast(c));
    return @as(i32, @intCast(@abs(pb))) - @as(i32, @intCast(@abs(pa)));
}

fn predictorArgb(mode: u8, left: u32, top_left: u32, top: u32, top_right: u32) !u32 {
    return switch (mode) {
        0 => 0xff000000,
        1 => left,
        2 => top,
        3 => top_right,
        4 => top_left,
        5 => average3Argb(left, top, top_right),
        6 => average2Argb(left, top_left),
        7 => average2Argb(left, top),
        8 => average2Argb(top_left, top),
        9 => average2Argb(top, top_right),
        10 => average4Argb(left, top_left, top, top_right),
        11 => selectPredictor(top, left, top_left),
        12 => clampedAddSubtractFull(left, top, top_left),
        13 => clampedAddSubtractHalf(left, top, top_left),
        else => error.WebpDecodeFailed,
    };
}

fn applyPredictorTransform(predictor: PredictorTransform, rgba: []u8, width: u32, height: u32) !void {
    if (predictor.width != vp8lSubSampleSize(width, predictor.bits)) return error.WebpDecodeFailed;
    if (predictor.height != vp8lSubSampleSize(height, predictor.bits)) return error.WebpDecodeFailed;
    if (rgba.len != try checkedByteCount(try checkedPixelCount(width, height), 4)) return error.WebpDecodeFailed;

    const width_usize: usize = @intCast(width);
    const height_usize: usize = @intCast(height);
    const predictor_width_usize: usize = @intCast(predictor.width);
    var y: usize = 0;
    while (y < height_usize) : (y += 1) {
        var x: usize = 0;
        while (x < width_usize) : (x += 1) {
            const pixel_index = y * width_usize + x;
            const byte_index = pixel_index * 4;
            const residual = packArgb(rgba[byte_index..][0..4]);
            const pred = if (y == 0 and x == 0)
                @as(u32, 0xff000000)
            else if (y == 0)
                packArgb(rgba[byte_index - 4 ..][0..4])
            else if (x == 0)
                packArgb(rgba[byte_index - width_usize * 4 ..][0..4])
            else blk: {
                const predictor_x = x >> predictor.bits;
                const predictor_y = y >> predictor.bits;
                const predictor_index = (predictor_y * predictor_width_usize + predictor_x) * 4;
                const mode = predictor.rgba[predictor_index + 1] & 0x0f;
                const left = packArgb(rgba[byte_index - 4 ..][0..4]);
                const top = packArgb(rgba[byte_index - width_usize * 4 ..][0..4]);
                const top_left = packArgb(rgba[byte_index - (width_usize + 1) * 4 ..][0..4]);
                const top_right = if (x + 1 < width_usize)
                    packArgb(rgba[byte_index - (width_usize - 1) * 4 ..][0..4])
                else
                    top;
                break :blk try predictorArgb(mode, left, top_left, top, top_right);
            };
            writeArgbToRgba(addPixelsArgb(residual, pred), rgba[byte_index..][0..4]);
        }
    }
}

fn colorTransformDelta(multiplier: u8, color: u8) i32 {
    const multiplier_i8: i8 = @bitCast(multiplier);
    const color_i8: i8 = @bitCast(color);
    return (@as(i32, multiplier_i8) * @as(i32, color_i8)) >> 5;
}

fn applyCrossColorTransform(cross_color: CrossColorTransform, rgba: []u8, width: u32, height: u32) !void {
    if (cross_color.width != vp8lSubSampleSize(width, cross_color.bits)) return error.WebpDecodeFailed;
    if (cross_color.height != vp8lSubSampleSize(height, cross_color.bits)) return error.WebpDecodeFailed;
    if (rgba.len != try checkedByteCount(try checkedPixelCount(width, height), 4)) return error.WebpDecodeFailed;

    const width_usize: usize = @intCast(width);
    const height_usize: usize = @intCast(height);
    const cross_color_width_usize: usize = @intCast(cross_color.width);
    var y: usize = 0;
    while (y < height_usize) : (y += 1) {
        var x: usize = 0;
        while (x < width_usize) : (x += 1) {
            const pixel_index = y * width_usize + x;
            const byte_index = pixel_index * 4;
            const transform_x = x >> cross_color.bits;
            const transform_y = y >> cross_color.bits;
            const transform_index = (transform_y * cross_color_width_usize + transform_x) * 4;

            const green = rgba[byte_index + 1];
            var red: i32 = rgba[byte_index + 0];
            var blue: i32 = rgba[byte_index + 2];
            red += colorTransformDelta(cross_color.rgba[transform_index + 2], green);
            red &= 0xff;
            blue += colorTransformDelta(cross_color.rgba[transform_index + 1], green);
            blue += colorTransformDelta(cross_color.rgba[transform_index + 0], @intCast(red));
            blue &= 0xff;
            rgba[byte_index + 0] = @intCast(red);
            rgba[byte_index + 2] = @intCast(blue);
        }
    }
}

fn expandColorMap(palette: []u8, num_colors: u32, bits: u5) !void {
    const palette_len = try checkedByteCount(@intCast(num_colors), 4);
    if (palette.len != palette_len) return error.WebpDecodeFailed;
    var i: usize = 4;
    while (i < palette_len) : (i += 1) {
        palette[i] +%= palette[i - 4];
    }

    const bits_per_pixel: u5 = @intCast(@as(u16, 8) >> @as(u4, @intCast(bits)));
    const final_num_colors: usize = @as(usize, 1) << bits_per_pixel;
    if (final_num_colors < num_colors) return error.WebpDecodeFailed;
}

fn applyColorIndexingTransform(alloc: Allocator, color_indexing: ColorIndexingTransform, decoded: DecodedImage) !DecodedImage {
    if (color_indexing.height != decoded.height) return error.WebpDecodeFailed;
    if (decoded.width != vp8lSubSampleSize(color_indexing.width, color_indexing.bits)) return error.WebpDecodeFailed;
    if (decoded.rgba.len != try checkedByteCount(try checkedPixelCount(decoded.width, decoded.height), 4)) return error.WebpDecodeFailed;

    const output_pixel_count = try checkedPixelCount(color_indexing.width, color_indexing.height);
    const output_byte_count = try checkedByteCount(output_pixel_count, 4);
    const out = try alloc.alloc(u8, output_byte_count);
    errdefer alloc.free(out);

    const input_width: usize = @intCast(decoded.width);
    const output_width: usize = @intCast(color_indexing.width);
    const height: usize = @intCast(color_indexing.height);
    const bits_per_pixel: u5 = @intCast(@as(u16, 8) >> @as(u4, @intCast(color_indexing.bits)));
    const pixels_per_byte: usize = @as(usize, 1) << color_indexing.bits;
    const index_mask: u8 = @intCast((@as(u16, 1) << @as(u4, @intCast(bits_per_pixel))) - 1);

    var y: usize = 0;
    while (y < height) : (y += 1) {
        var x: usize = 0;
        while (x < output_width) {
            const packed_pixel_index = y * input_width + x / pixels_per_byte;
            if (packed_pixel_index >= input_width * height) return error.WebpDecodeFailed;
            var packed_indices = decoded.rgba[packed_pixel_index * 4 + 1];
            var lane: usize = 0;
            while (lane < pixels_per_byte and x < output_width) : ({
                lane += 1;
                x += 1;
            }) {
                const palette_index = packed_indices & index_mask;
                packed_indices = if (bits_per_pixel == 8) 0 else packed_indices >> @as(u3, @intCast(bits_per_pixel));
                const palette_offset = @as(usize, palette_index) * 4;
                const out_offset = (y * output_width + x) * 4;
                if (palette_offset + 4 <= color_indexing.palette.len) {
                    @memcpy(out[out_offset..][0..4], color_indexing.palette[palette_offset..][0..4]);
                } else {
                    @memset(out[out_offset..][0..4], 0);
                }
            }
        }
    }

    alloc.free(decoded.rgba);
    return .{
        .rgba = out,
        .width = color_indexing.width,
        .height = color_indexing.height,
    };
}

fn vp8lSubSampleSize(size: u32, bits: u5) u32 {
    return (size + (@as(u32, 1) << bits) - 1) >> bits;
}

fn decodeVp8lRgba(alloc: Allocator, payload: []const u8) !DecodedImage {
    if (payload.len < 5) return error.WebpDecodeFailed;
    if (payload[0] != 0x2f) return error.WebpDecodeFailed;

    var reader = BitReader.init(payload[1..]);
    const width = (try reader.readBits(14)) + 1;
    const height = (try reader.readBits(14)) + 1;
    const alpha_is_used = (try reader.readBits(1)) != 0;
    const version = try reader.readBits(3);
    if (version != 0) return error.UnsupportedWebpFormat;

    const decoded = try decodeVp8lImageStream(alloc, &reader, width, height, true);
    errdefer alloc.free(decoded.rgba);
    if (!alpha_is_used and decodedRgbaHasNonOpaqueAlpha(decoded.rgba)) return error.WebpDecodeFailed;
    return decoded;
}

fn decodedRgbaHasNonOpaqueAlpha(rgba: []const u8) bool {
    var alpha_index: usize = 3;
    while (alpha_index < rgba.len) : (alpha_index += 4) {
        if (rgba[alpha_index] != 255) return true;
    }
    return false;
}

fn decodeAlphPlane(alloc: Allocator, payload: []const u8, width: u32, height: u32) ![]u8 {
    if (payload.len <= 1) return error.WebpDecodeFailed;
    const header = payload[0];
    const method = header & 0x03;
    const filter = (header >> 2) & 0x03;
    const preprocessing = (header >> 4) & 0x03;
    const reserved = header >> 6;
    if (reserved != 0 or preprocessing > 1) return error.WebpDecodeFailed;
    if (preprocessing != 0) return error.UnsupportedWebpFormat;

    const pixel_count = try checkedPixelCount(width, height);
    var deltas_alloc: ?[]u8 = null;
    defer if (deltas_alloc) |deltas| alloc.free(deltas);

    const deltas = switch (method) {
        0 => blk: {
            if (payload.len - 1 < pixel_count) return error.WebpDecodeFailed;
            break :blk payload[1 .. 1 + pixel_count];
        },
        1 => blk: {
            var reader = BitReader.init(payload[1..]);
            const decoded = try decodeVp8lImageStream(alloc, &reader, width, height, true);
            defer alloc.free(decoded.rgba);
            const plane = try alloc.alloc(u8, pixel_count);
            for (plane, 0..) |*alpha, i| {
                alpha.* = decoded.rgba[i * 4 + 1];
            }
            deltas_alloc = plane;
            break :blk plane;
        },
        else => return error.WebpDecodeFailed,
    };

    const out = try alloc.alloc(u8, pixel_count);
    errdefer alloc.free(out);

    const width_usize: usize = @intCast(width);
    const height_usize: usize = @intCast(height);
    var y: usize = 0;
    while (y < height_usize) : (y += 1) {
        const row_start = y * width_usize;
        const previous = if (y == 0) null else out[row_start - width_usize .. row_start];
        try unfilterAlphaRow(filter, previous, deltas[row_start..][0..width_usize], out[row_start..][0..width_usize]);
    }
    return out;
}

fn unfilterAlphaRow(filter: u8, previous: ?[]const u8, deltas: []const u8, out: []u8) !void {
    if (deltas.len != out.len) return error.WebpDecodeFailed;
    switch (filter) {
        0 => @memcpy(out, deltas),
        1 => alphaHorizontalUnfilter(previous, deltas, out),
        2 => alphaVerticalUnfilter(previous, deltas, out),
        3 => alphaGradientUnfilter(previous, deltas, out),
        else => return error.WebpDecodeFailed,
    }
}

fn alphaHorizontalUnfilter(previous: ?[]const u8, deltas: []const u8, out: []u8) void {
    var pred: u8 = if (previous) |prev| prev[0] else 0;
    for (deltas, 0..) |delta, i| {
        out[i] = pred +% delta;
        pred = out[i];
    }
}

fn alphaVerticalUnfilter(previous: ?[]const u8, deltas: []const u8, out: []u8) void {
    if (previous) |prev| {
        for (deltas, 0..) |delta, i| out[i] = prev[i] +% delta;
    } else {
        alphaHorizontalUnfilter(null, deltas, out);
    }
}

fn alphaGradientUnfilter(previous: ?[]const u8, deltas: []const u8, out: []u8) void {
    const prev = previous orelse {
        alphaHorizontalUnfilter(null, deltas, out);
        return;
    };
    var top = prev[0];
    var top_left = top;
    var left = top;
    for (deltas, 0..) |delta, i| {
        top = prev[i];
        left = delta +% alphaGradientPredictor(left, top, top_left);
        top_left = top;
        out[i] = left;
    }
}

fn alphaGradientPredictor(left: u8, top: u8, top_left: u8) u8 {
    const value = @as(i32, left) + @as(i32, top) - @as(i32, top_left);
    if (value < 0) return 0;
    if (value > 255) return 255;
    return @intCast(value);
}

fn composeAlphaPlane(decoded: *DecodedImage, alpha: []const u8) !void {
    const pixel_count = try checkedPixelCount(decoded.width, decoded.height);
    if (alpha.len != pixel_count or decoded.rgba.len != try checkedByteCount(pixel_count, 4)) return error.WebpDecodeFailed;
    for (alpha, 0..) |value, i| {
        decoded.rgba[i * 4 + 3] = value;
    }
}

fn decodeVp8lImageStream(alloc: Allocator, reader: *BitReader, width: u32, height: u32, top_level: bool) anyerror!DecodedImage {
    var transforms: [4]Vp8lTransform = undefined;
    var transform_count: usize = 0;
    errdefer {
        var i: usize = 0;
        while (i < transform_count) : (i += 1) transforms[i].deinit(alloc);
    }

    var current_width = width;
    var current_height = height;
    var saw_subtract_green = false;
    var saw_predictor = false;
    var saw_cross_color = false;
    var saw_color_indexing = false;
    if (top_level) {
        while ((try reader.readBits(1)) != 0) {
            if (transform_count == transforms.len) return error.WebpDecodeFailed;
            try readVp8lTransform(
                alloc,
                reader,
                &current_width,
                &current_height,
                &transforms,
                &transform_count,
                &saw_subtract_green,
                &saw_predictor,
                &saw_cross_color,
                &saw_color_indexing,
            );
        }
    }

    var decoded = try decodeVp8lEntropyImage(alloc, reader, current_width, current_height, top_level);
    errdefer alloc.free(decoded.rgba);

    while (transform_count > 0) {
        const transform_index = transform_count - 1;
        decoded = applyVp8lTransform(alloc, transforms[transform_index], decoded) catch |err| {
            transforms[transform_index].deinit(alloc);
            transform_count = transform_index;
            return err;
        };
        transforms[transform_index].deinit(alloc);
        transform_count = transform_index;
    }

    return decoded;
}

fn readVp8lTransform(
    alloc: Allocator,
    reader: *BitReader,
    width: *u32,
    height: *u32,
    transforms: *[4]Vp8lTransform,
    transform_count: *usize,
    saw_subtract_green: *bool,
    saw_predictor: *bool,
    saw_cross_color: *bool,
    saw_color_indexing: *bool,
) anyerror!void {
    const transform_type = try reader.readBits(2);
    switch (transform_type) {
        vp8l_transform_subtract_green => {
            if (saw_subtract_green.*) return error.WebpDecodeFailed;
            saw_subtract_green.* = true;
            transforms[transform_count.*] = .subtract_green;
            transform_count.* += 1;
        },
        vp8l_transform_predictor => {
            if (saw_predictor.*) return error.WebpDecodeFailed;
            saw_predictor.* = true;
            const bits: u5 = @intCast(2 + try reader.readBits(3));
            const predictor_width = vp8lSubSampleSize(width.*, bits);
            const predictor_height = vp8lSubSampleSize(height.*, bits);
            const predictor = try decodeVp8lImageStream(alloc, reader, predictor_width, predictor_height, false);
            transforms[transform_count.*] = .{ .predictor = .{
                .bits = bits,
                .width = predictor.width,
                .height = predictor.height,
                .rgba = predictor.rgba,
            } };
            transform_count.* += 1;
        },
        vp8l_transform_cross_color => {
            if (saw_cross_color.*) return error.WebpDecodeFailed;
            saw_cross_color.* = true;
            const bits: u5 = @intCast(2 + try reader.readBits(3));
            const cross_color_width = vp8lSubSampleSize(width.*, bits);
            const cross_color_height = vp8lSubSampleSize(height.*, bits);
            const cross_color = try decodeVp8lImageStream(alloc, reader, cross_color_width, cross_color_height, false);
            transforms[transform_count.*] = .{ .cross_color = .{
                .bits = bits,
                .width = cross_color.width,
                .height = cross_color.height,
                .rgba = cross_color.rgba,
            } };
            transform_count.* += 1;
        },
        vp8l_transform_color_indexing => {
            if (saw_color_indexing.*) return error.WebpDecodeFailed;
            saw_color_indexing.* = true;
            const num_colors = 1 + try reader.readBits(8);
            const bits: u5 = if (num_colors > 16) 0 else if (num_colors > 4) 1 else if (num_colors > 2) 2 else 3;
            const palette = try decodeVp8lImageStream(alloc, reader, num_colors, 1, false);
            errdefer alloc.free(palette.rgba);
            try expandColorMap(palette.rgba, num_colors, bits);
            transforms[transform_count.*] = .{ .color_indexing = .{
                .bits = bits,
                .width = width.*,
                .height = height.*,
                .palette = palette.rgba,
            } };
            transform_count.* += 1;
            width.* = vp8lSubSampleSize(width.*, bits);
        },
        else => unreachable,
    }
}

fn readVp8lPrefixCodeGroups(alloc: Allocator, reader: *BitReader, width: u32, height: u32, color_cache_bits: u5, top_level: bool) anyerror!PrefixCodeGroups {
    var huffman_bits: u5 = 0;
    var huffman_width: u32 = 0;
    var group_indices: []u16 = &.{};
    errdefer alloc.free(group_indices);

    var group_count: usize = 1;
    if (top_level) {
        const meta_prefix_present = try reader.readBits(1);
        if (meta_prefix_present != 0) {
            huffman_bits = @intCast(2 + try reader.readBits(3));
            huffman_width = vp8lSubSampleSize(width, huffman_bits);
            const huffman_height = vp8lSubSampleSize(height, huffman_bits);
            const huffman_image = try decodeVp8lImageStream(alloc, reader, huffman_width, huffman_height, false);
            defer alloc.free(huffman_image.rgba);

            const huffman_pixel_count = try checkedPixelCount(huffman_width, huffman_height);
            group_indices = try alloc.alloc(u16, huffman_pixel_count);
            var max_group: u16 = 0;
            for (group_indices, 0..) |*group_index, i| {
                const rgba = huffman_image.rgba[i * 4 ..][0..4];
                const raw_group = (@as(u16, rgba[0]) << 8) | @as(u16, rgba[1]);
                group_index.* = raw_group;
                max_group = @max(max_group, raw_group);
            }
            group_count = @as(usize, max_group) + 1;
            if (group_count > 1000 or group_count > try checkedPixelCount(width, height)) return error.WebpDecodeFailed;
        }
    }

    const groups = try alloc.alloc(PrefixCodeGroup, group_count);
    errdefer alloc.free(groups);
    for (groups) |*group| {
        group.* = try PrefixCodeGroup.read(reader, color_cache_bits);
    }

    return .{
        .groups = groups,
        .huffman_bits = huffman_bits,
        .huffman_width = huffman_width,
        .group_indices = group_indices,
    };
}

fn decodeVp8lEntropyImage(alloc: Allocator, reader: *BitReader, width: u32, height: u32, top_level: bool) !DecodedImage {
    var color_cache_bits: u5 = 0;
    var color_cache: ?ColorCache = null;
    errdefer if (color_cache) |*cache| cache.deinit(alloc);

    const color_cache_present = try reader.readBits(1);
    if (color_cache_present != 0) {
        color_cache_bits = @intCast(try reader.readBits(4));
        color_cache = try ColorCache.init(alloc, color_cache_bits);
    }

    var prefix_groups = try readVp8lPrefixCodeGroups(alloc, reader, width, height, color_cache_bits, top_level);
    defer prefix_groups.deinit(alloc);

    const pixel_count = try checkedPixelCount(width, height);
    const byte_count = try checkedByteCount(pixel_count, 4);
    const rgba = try alloc.alloc(u8, byte_count);
    errdefer alloc.free(rgba);

    var pixel_index: usize = 0;
    while (pixel_index < pixel_count) {
        const group = try prefix_groups.groupForPixel(pixel_index, width);
        const green = try group.green.readSymbol(reader);
        if (green >= vp8l_literal_count + vp8l_length_code_count) {
            const out_index = pixel_index * 4;
            const argb = if (color_cache) |cache|
                try cache.lookup(green - (vp8l_literal_count + vp8l_length_code_count))
            else
                return error.WebpDecodeFailed;
            writeArgbToRgba(argb, rgba[out_index..][0..4]);
            if (color_cache) |*cache| cache.insert(argb);
            pixel_index += 1;
            continue;
        } else if (green >= vp8l_literal_count) {
            const length = try readCopyLength(reader, green - vp8l_literal_count);
            const dist_symbol = try group.distance.readSymbol(reader);
            const dist_code = try readCopyDistance(reader, dist_symbol);
            const dist = try planeCodeToDistance(width, dist_code);
            if (dist > pixel_index or length > pixel_count - pixel_index) return error.WebpDecodeFailed;

            var copied: usize = 0;
            while (copied < length) : (copied += 1) {
                const src_index = (pixel_index - dist) * 4;
                const out_index = pixel_index * 4;
                rgba[out_index + 0] = rgba[src_index + 0];
                rgba[out_index + 1] = rgba[src_index + 1];
                rgba[out_index + 2] = rgba[src_index + 2];
                rgba[out_index + 3] = rgba[src_index + 3];
                if (color_cache) |*cache| cache.insert(packArgb(rgba[out_index..][0..4]));
                pixel_index += 1;
            }
            continue;
        }
        const red = try group.red.readSymbol(reader);
        const blue = try group.blue.readSymbol(reader);
        const alpha = try group.alpha.readSymbol(reader);
        if (red >= vp8l_literal_count or blue >= vp8l_literal_count or alpha >= vp8l_literal_count) return error.WebpDecodeFailed;

        const out_index = pixel_index * 4;
        rgba[out_index + 0] = @intCast(red);
        rgba[out_index + 1] = @intCast(green);
        rgba[out_index + 2] = @intCast(blue);
        rgba[out_index + 3] = @intCast(alpha);
        if (color_cache) |*cache| cache.insert(packArgb(rgba[out_index..][0..4]));
        pixel_index += 1;
    }

    if (color_cache) |*cache| cache.deinit(alloc);

    return .{
        .rgba = rgba,
        .width = width,
        .height = height,
    };
}

fn applyVp8lTransform(alloc: Allocator, transform: Vp8lTransform, decoded: DecodedImage) !DecodedImage {
    switch (transform) {
        .subtract_green => {
            applySubtractGreen(decoded.rgba);
            return decoded;
        },
        .predictor => |predictor| {
            try applyPredictorTransform(predictor, decoded.rgba, decoded.width, decoded.height);
            return decoded;
        },
        .cross_color => |cross_color| {
            try applyCrossColorTransform(cross_color, decoded.rgba, decoded.width, decoded.height);
            return decoded;
        },
        .color_indexing => |color_indexing| return try applyColorIndexingTransform(alloc, color_indexing, decoded),
    }
}

fn checkedPixelCount(width: u32, height: u32) !usize {
    const pixel_count_u64 = @as(u64, width) * @as(u64, height);
    if (pixel_count_u64 > std.math.maxInt(usize)) return error.UnsupportedWebpFormat;
    return @intCast(pixel_count_u64);
}

fn checkedByteCount(count: usize, bytes_per_pixel: usize) !usize {
    if (count > std.math.maxInt(usize) / bytes_per_pixel) return error.UnsupportedWebpFormat;
    return count * bytes_per_pixel;
}

fn readU16Le(bytes: []const u8, offset: usize) !u16 {
    if (offset + 2 > bytes.len) return error.WebpDecodeFailed;
    return @as(u16, bytes[offset]) | (@as(u16, bytes[offset + 1]) << 8);
}

fn readU32Le(bytes: []const u8, offset: usize) !u32 {
    if (offset + 4 > bytes.len) return error.WebpDecodeFailed;
    return @as(u32, bytes[offset]) |
        (@as(u32, bytes[offset + 1]) << 8) |
        (@as(u32, bytes[offset + 2]) << 16) |
        (@as(u32, bytes[offset + 3]) << 24);
}

fn readU24LePlusOne(bytes: []const u8, offset: usize) !u32 {
    if (offset + 3 > bytes.len) return error.WebpDecodeFailed;
    return 1 + @as(u32, bytes[offset]) +
        (@as(u32, bytes[offset + 1]) << 8) +
        (@as(u32, bytes[offset + 2]) << 16);
}

const webp_vp8_1x1 = [_]u8{
    'R',  'I',  'F', 'F', 22,  0,   0,   0,
    'W',  'E',  'B', 'P', 'V', 'P', '8', ' ',
    10,   0,    0,   0,   0,   0,   0,   0x9d,
    0x01, 0x2a, 1,   0,   1,   0,
};

const webp_vp8l_2x3 = [_]u8{
    'R', 'I', 'F', 'F', 18,   0,   0,    0,
    'W', 'E', 'B', 'P', 'V',  'P', '8',  'L',
    5,   0,   0,   0,   0x2f, 1,   0x80, 0,
    0,   0,
};

const webp_vp8x_alpha_1x1 = [_]u8{
    'R', 'I', 'F', 'F',  50,              0,    0,   0,
    'W', 'E', 'B', 'P',  'V',             'P',  '8', 'X',
    10,  0,   0,   0,    vp8x_flag_alpha, 0,    0,   0,
    0,   0,   0,   0,    0,               0,    'A', 'L',
    'P', 'H', 1,   0,    0,               0,    0,   0,
    'V', 'P', '8', ' ',  10,              0,    0,   0,
    0,   0,   0,   0x9d, 0x01,            0x2a, 1,   0,
    1,   0,
};

const webp_vp8x_animated_1x1 = [_]u8{
    'R', 'I', 'F', 'F', 60,                  0,   0,   0,
    'W', 'E', 'B', 'P', 'V',                 'P', '8', 'X',
    10,  0,   0,   0,   vp8x_flag_animation, 0,   0,   0,
    0,   0,   0,   0,   0,                   0,   'A', 'N',
    'I', 'M', 6,   0,   0,                   0,   0,   0,
    0,   0,   0,   0,   'A',                 'N', 'M', 'F',
    16,  0,   0,   0,   0,                   0,   0,   0,
    0,   0,   0,   0,   0,                   0,   0,   0,
    0,   0,   0,   0,
};

fn testBuildVp8Payload(alloc: Allocator, width: u16, height: u16, first_partition: []const u8, token_partitions: []const []const u8) ![]u8 {
    if (token_partitions.len == 0 or token_partitions.len > 8 or !std.math.isPowerOfTwo(token_partitions.len)) return error.InvalidTestFixture;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    const frame_tag = (@as(u32, @intCast(first_partition.len)) << 5) | 0x10;
    try out.append(alloc, @intCast(frame_tag & 0xff));
    try out.append(alloc, @intCast((frame_tag >> 8) & 0xff));
    try out.append(alloc, @intCast((frame_tag >> 16) & 0xff));
    try out.appendSlice(alloc, &.{ 0x9d, 0x01, 0x2a });
    try out.append(alloc, @intCast(width & 0xff));
    try out.append(alloc, @intCast(width >> 8));
    try out.append(alloc, @intCast(height & 0xff));
    try out.append(alloc, @intCast(height >> 8));
    try out.appendSlice(alloc, first_partition);
    for (token_partitions[0 .. token_partitions.len - 1]) |partition| {
        const len = partition.len;
        try out.append(alloc, @intCast(len & 0xff));
        try out.append(alloc, @intCast((len >> 8) & 0xff));
        try out.append(alloc, @intCast((len >> 16) & 0xff));
    }
    for (token_partitions) |partition| try out.appendSlice(alloc, partition);
    return try out.toOwnedSlice(alloc);
}

fn testBuildVp8Webp(alloc: Allocator, width: u16, height: u16, first_partition: []const u8, token_partitions: []const []const u8) ![]u8 {
    const payload = try testBuildVp8Payload(alloc, width, height, first_partition, token_partitions);
    defer alloc.free(payload);

    const payload_len_u32: u32 = @intCast(payload.len);
    const padded_payload_len = payload.len + (payload.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8 ");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload);
    if ((payload.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testBuildVp8xAlphVp8Webp(alloc: Allocator, alpha_payload: []const u8, width: u16, height: u16, first_partition: []const u8, token_partitions: []const []const u8) ![]u8 {
    const vp8_payload = try testBuildVp8Payload(alloc, width, height, first_partition, token_partitions);
    defer alloc.free(vp8_payload);

    const alpha_padded_len = alpha_payload.len + (alpha_payload.len & 1);
    const vp8_padded_len = vp8_payload.len + (vp8_payload.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + 10 + chunk_header_len + alpha_padded_len + chunk_header_len + vp8_padded_len);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8X");
    try appendU32Le(alloc, &out, 10);
    try out.append(alloc, vp8x_flag_alpha);
    try out.appendSlice(alloc, &.{ 0, 0, 0 });
    try out.append(alloc, @intCast((width - 1) & 0xff));
    try out.append(alloc, @intCast((width - 1) >> 8));
    try out.append(alloc, 0);
    try out.append(alloc, @intCast((height - 1) & 0xff));
    try out.append(alloc, @intCast((height - 1) >> 8));
    try out.append(alloc, 0);
    try out.appendSlice(alloc, "ALPH");
    try appendU32Le(alloc, &out, @intCast(alpha_payload.len));
    try out.appendSlice(alloc, alpha_payload);
    if ((alpha_payload.len & 1) != 0) try out.append(alloc, 0);
    try out.appendSlice(alloc, "VP8 ");
    try appendU32Le(alloc, &out, @intCast(vp8_payload.len));
    try out.appendSlice(alloc, vp8_payload);
    if ((vp8_payload.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

const TestBitWriter = struct {
    bytes: std.ArrayListUnmanaged(u8) = .empty,
    bit_pos: u3 = 0,

    fn deinit(self: *TestBitWriter, alloc: Allocator) void {
        self.bytes.deinit(alloc);
        self.* = undefined;
    }

    fn writeBits(self: *TestBitWriter, alloc: Allocator, value: u32, count: u5) !void {
        var i: u5 = 0;
        while (i < count) : (i += 1) {
            try self.writeBit(alloc, @intCast((value >> i) & 1));
        }
    }

    fn writeBit(self: *TestBitWriter, alloc: Allocator, bit: u1) !void {
        if (self.bit_pos == 0) try self.bytes.append(alloc, 0);
        const last = self.bytes.items.len - 1;
        self.bytes.items[last] |= @as(u8, bit) << self.bit_pos;
        self.bit_pos +%= 1;
    }
};

fn testWriteSimplePrefixSymbol(writer: *TestBitWriter, alloc: Allocator, symbol: u8) !void {
    try writer.writeBits(alloc, 1, 1);
    try writer.writeBits(alloc, 0, 1);
    try writer.writeBits(alloc, 1, 1);
    try writer.writeBits(alloc, symbol, 8);
}

fn testBuildLiteralVp8lWebp(alloc: Allocator, width: u32, height: u32, rgba: [4]u8) ![]u8 {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, width - 1, 14);
    try bits.writeBits(alloc, height - 1, 14);
    try bits.writeBits(alloc, if (rgba[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[1]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[0]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[2]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try payload.appendSlice(alloc, bits.bytes.items);

    const payload_len_u32: u32 = @intCast(payload.items.len);
    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testWriteSimplePrefixPair(writer: *TestBitWriter, alloc: Allocator, symbol0: u8, symbol1: u8) !void {
    try writer.writeBits(alloc, 1, 1);
    try writer.writeBits(alloc, 1, 1);
    try writer.writeBits(alloc, 1, 1);
    try writer.writeBits(alloc, symbol0, 8);
    try writer.writeBits(alloc, symbol1, 8);
}

fn testWriteNormalPrefixSingleSymbol(writer: *TestBitWriter, alloc: Allocator, symbol: u8) !void {
    try testWriteNormalPrefixSymbols(writer, alloc, &.{symbol});
}

fn testWriteNormalPrefixSymbols(writer: *TestBitWriter, alloc: Allocator, symbols: []const u16) !void {
    if (symbols.len == 0) return error.InvalidTestFixture;
    try writer.writeBits(alloc, 0, 1);
    try writer.writeBits(alloc, 0, 4);
    try writer.writeBits(alloc, 0, 3);
    try writer.writeBits(alloc, 0, 3);
    try writer.writeBits(alloc, 1, 3);
    try writer.writeBits(alloc, 1, 3);

    var max_seen: u16 = 0;
    for (symbols) |symbol| max_seen = @max(max_seen, symbol);
    const max_symbol: u32 = @max(@as(u32, max_seen) + 1, 2);
    const max_symbol_minus_two = max_symbol - 2;
    var length_nbits_selector: u32 = 0;
    while (length_nbits_selector < 7 and max_symbol_minus_two >= (@as(u32, 1) << @intCast(2 + 2 * length_nbits_selector))) {
        length_nbits_selector += 1;
    }
    const length_nbits: u5 = @intCast(2 + 2 * length_nbits_selector);
    try writer.writeBits(alloc, 1, 1);
    try writer.writeBits(alloc, length_nbits_selector, 3);
    try writer.writeBits(alloc, max_symbol_minus_two, length_nbits);

    var i: u32 = 0;
    while (i < max_symbol) : (i += 1) {
        var present = false;
        for (symbols) |symbol| {
            if (i == symbol) {
                present = true;
                break;
            }
        }
        try writer.writeBits(alloc, if (present) 1 else 0, 1);
    }
}

fn testWriteNormalPrefixSingleSymbolWithZeroRepeat(writer: *TestBitWriter, alloc: Allocator, symbol: u8) !void {
    if (symbol < 11) return error.InvalidTestFixture;
    try writer.writeBits(alloc, 0, 1);
    try writer.writeBits(alloc, 0, 4);
    try writer.writeBits(alloc, 0, 3);
    try writer.writeBits(alloc, 1, 3);
    try writer.writeBits(alloc, 0, 3);
    try writer.writeBits(alloc, 1, 3);

    const max_symbol: u32 = @as(u32, symbol) + 1;
    try writer.writeBits(alloc, 1, 1);
    try writer.writeBits(alloc, 3, 3);
    try writer.writeBits(alloc, max_symbol - 2, 8);

    try writer.writeBits(alloc, 1, 1);
    try writer.writeBits(alloc, @as(u32, symbol) - 11, 7);
    try writer.writeBits(alloc, 0, 1);
}

fn testBuildNormalPrefixVp8lWebp(alloc: Allocator, rgba: [4]u8) ![]u8 {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, 0, 14);
    try bits.writeBits(alloc, 0, 14);
    try bits.writeBits(alloc, if (rgba[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try testWriteNormalPrefixSingleSymbol(&bits, alloc, rgba[1]);
    try testWriteNormalPrefixSingleSymbol(&bits, alloc, rgba[0]);
    try testWriteNormalPrefixSingleSymbol(&bits, alloc, rgba[2]);
    try testWriteNormalPrefixSingleSymbol(&bits, alloc, rgba[3]);
    try testWriteNormalPrefixSingleSymbol(&bits, alloc, 0);
    try payload.appendSlice(alloc, bits.bytes.items);

    const payload_len_u32: u32 = @intCast(payload.items.len);
    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testBuildNormalPrefixRepeatVp8lWebp(alloc: Allocator, rgba: [4]u8) ![]u8 {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, 0, 14);
    try bits.writeBits(alloc, 0, 14);
    try bits.writeBits(alloc, if (rgba[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try testWriteNormalPrefixSingleSymbolWithZeroRepeat(&bits, alloc, rgba[1]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[0]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[2]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try payload.appendSlice(alloc, bits.bytes.items);

    const payload_len_u32: u32 = @intCast(payload.items.len);
    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testBuildTwoPixelVp8lWebp(alloc: Allocator, first: [4]u8, second: [4]u8) ![]u8 {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, 1, 14);
    try bits.writeBits(alloc, 0, 14);
    try bits.writeBits(alloc, if (first[3] == 255 and second[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try testWriteSimplePrefixPair(&bits, alloc, first[1], second[1]);
    try testWriteSimplePrefixPair(&bits, alloc, first[0], second[0]);
    try testWriteSimplePrefixPair(&bits, alloc, first[2], second[2]);
    try testWriteSimplePrefixPair(&bits, alloc, first[3], second[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, 1, 1);
    try payload.appendSlice(alloc, bits.bytes.items);

    const payload_len_u32: u32 = @intCast(payload.items.len);
    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testBuildLz77Vp8lWebp(alloc: Allocator, width: u32, repeat_count: u16, rgba: [4]u8) ![]u8 {
    if (width < 2 or @as(u32, repeat_count) + 1 != width) return error.InvalidTestFixture;
    if (repeat_count == 0 or repeat_count > 4) return error.InvalidTestFixture;

    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, width - 1, 14);
    try bits.writeBits(alloc, 0, 14);
    try bits.writeBits(alloc, if (rgba[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);

    const length_symbol: u16 = repeat_count - 1;
    try testWriteNormalPrefixSymbols(&bits, alloc, &.{ rgba[1], vp8l_literal_count + length_symbol });
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[0]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[2]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 1);

    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 1, 1);
    try payload.appendSlice(alloc, bits.bytes.items);

    const payload_len_u32: u32 = @intCast(payload.items.len);
    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testBuildColorCacheVp8lWebp(alloc: Allocator, rgba: [4]u8) ![]u8 {
    const color_cache_bits: u5 = 1;
    const cache_key: u16 = @intCast(vp8lColorCacheKey(packArgb(&rgba), @intCast(32 - @as(u6, color_cache_bits))));

    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, 1, 14);
    try bits.writeBits(alloc, 0, 14);
    try bits.writeBits(alloc, if (rgba[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, color_cache_bits, 4);
    try bits.writeBits(alloc, 0, 1);

    try testWriteNormalPrefixSymbols(&bits, alloc, &.{ rgba[1], vp8l_literal_count + vp8l_length_code_count + cache_key });
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[0]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[2]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);

    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 1, 1);
    try payload.appendSlice(alloc, bits.bytes.items);

    const payload_len_u32: u32 = @intCast(payload.items.len);
    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testBuildSingleTransformVp8lWebp(alloc: Allocator, transform_type: u2, rgba: [4]u8) ![]u8 {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, 0, 14);
    try bits.writeBits(alloc, 0, 14);
    try bits.writeBits(alloc, if (rgba[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);
    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, transform_type, 2);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[1]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[0]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[2]);
    try testWriteSimplePrefixSymbol(&bits, alloc, rgba[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try payload.appendSlice(alloc, bits.bytes.items);

    const payload_len_u32: u32 = @intCast(payload.items.len);
    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testBuildSubtractGreenVp8lWebp(alloc: Allocator, rgba: [4]u8) ![]u8 {
    const transformed = [4]u8{
        rgba[0] -% rgba[1],
        rgba[1],
        rgba[2] -% rgba[1],
        rgba[3],
    };
    return try testBuildSingleTransformVp8lWebp(alloc, vp8l_transform_subtract_green, transformed);
}

fn testBuildPredictorVp8lWebp(alloc: Allocator, width: u32, height: u32, mode: u8, residual: [4]u8) ![]u8 {
    if (width == 0 or height == 0 or mode > 15) return error.InvalidTestFixture;

    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, width - 1, 14);
    try bits.writeBits(alloc, height - 1, 14);
    try bits.writeBits(alloc, if (residual[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);

    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, vp8l_transform_predictor, 2);
    try bits.writeBits(alloc, 0, 3);

    try bits.writeBits(alloc, 0, 1);
    try testWriteSimplePrefixSymbol(&bits, alloc, mode);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try testWriteSimplePrefixSymbol(&bits, alloc, 255);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);

    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try testWriteSimplePrefixSymbol(&bits, alloc, residual[1]);
    try testWriteSimplePrefixSymbol(&bits, alloc, residual[0]);
    try testWriteSimplePrefixSymbol(&bits, alloc, residual[2]);
    try testWriteSimplePrefixSymbol(&bits, alloc, residual[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try payload.appendSlice(alloc, bits.bytes.items);

    const payload_len_u32: u32 = @intCast(payload.items.len);
    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testBuildCrossColorVp8lWebp(alloc: Allocator, residual: [4]u8, multipliers: [4]u8) ![]u8 {
    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, 0, 14);
    try bits.writeBits(alloc, 0, 14);
    try bits.writeBits(alloc, if (residual[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);

    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, vp8l_transform_cross_color, 2);
    try bits.writeBits(alloc, 0, 3);

    try bits.writeBits(alloc, 0, 1);
    try testWriteSimplePrefixSymbol(&bits, alloc, multipliers[1]);
    try testWriteSimplePrefixSymbol(&bits, alloc, multipliers[0]);
    try testWriteSimplePrefixSymbol(&bits, alloc, multipliers[2]);
    try testWriteSimplePrefixSymbol(&bits, alloc, multipliers[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);

    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try testWriteSimplePrefixSymbol(&bits, alloc, residual[1]);
    try testWriteSimplePrefixSymbol(&bits, alloc, residual[0]);
    try testWriteSimplePrefixSymbol(&bits, alloc, residual[2]);
    try testWriteSimplePrefixSymbol(&bits, alloc, residual[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try payload.appendSlice(alloc, bits.bytes.items);

    const payload_len_u32: u32 = @intCast(payload.items.len);
    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testBuildColorIndexingVp8lWebp(alloc: Allocator, color0: [4]u8, color1: [4]u8) ![]u8 {
    const width: u32 = 8;
    const height: u32 = 1;
    const delta1 = [4]u8{
        color1[0] -% color0[0],
        color1[1] -% color0[1],
        color1[2] -% color0[2],
        color1[3] -% color0[3],
    };

    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, width - 1, 14);
    try bits.writeBits(alloc, height - 1, 14);
    try bits.writeBits(alloc, if (color0[3] == 255 and color1[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);

    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, vp8l_transform_color_indexing, 2);
    try bits.writeBits(alloc, 1, 8);

    try bits.writeBits(alloc, 0, 1);
    try testWriteSimplePrefixPair(&bits, alloc, color0[1], delta1[1]);
    try testWriteSimplePrefixPair(&bits, alloc, color0[0], delta1[0]);
    try testWriteSimplePrefixPair(&bits, alloc, color0[2], delta1[2]);
    try testWriteSimplePrefixPair(&bits, alloc, color0[3], delta1[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, 1, 1);

    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0xaa);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try testWriteSimplePrefixSymbol(&bits, alloc, 255);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try payload.appendSlice(alloc, bits.bytes.items);

    const payload_len_u32: u32 = @intCast(payload.items.len);
    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testBuildMetaPrefixVp8lWebp(alloc: Allocator, color0: [4]u8, color1: [4]u8) ![]u8 {
    const width: u32 = 5;
    const height: u32 = 1;

    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(alloc);
    try payload.append(alloc, 0x2f);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, width - 1, 14);
    try bits.writeBits(alloc, height - 1, 14);
    try bits.writeBits(alloc, if (color0[3] == 255 and color1[3] == 255) 0 else 1, 1);
    try bits.writeBits(alloc, 0, 3);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 1, 1);
    try bits.writeBits(alloc, 0, 3);

    try bits.writeBits(alloc, 0, 1);
    try testWriteSimplePrefixPair(&bits, alloc, 0, 1);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try testWriteSimplePrefixSymbol(&bits, alloc, 255);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 1, 1);

    try testWriteSimplePrefixSymbol(&bits, alloc, color0[1]);
    try testWriteSimplePrefixSymbol(&bits, alloc, color0[0]);
    try testWriteSimplePrefixSymbol(&bits, alloc, color0[2]);
    try testWriteSimplePrefixSymbol(&bits, alloc, color0[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);

    try testWriteSimplePrefixSymbol(&bits, alloc, color1[1]);
    try testWriteSimplePrefixSymbol(&bits, alloc, color1[0]);
    try testWriteSimplePrefixSymbol(&bits, alloc, color1[2]);
    try testWriteSimplePrefixSymbol(&bits, alloc, color1[3]);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try payload.appendSlice(alloc, bits.bytes.items);

    const payload_len_u32: u32 = @intCast(payload.items.len);
    const padded_payload_len = payload.items.len + (payload.items.len & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + padded_payload_len);
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8L");
    try appendU32Le(alloc, &out, payload_len_u32);
    try out.appendSlice(alloc, payload.items);
    if ((payload.items.len & 1) != 0) try out.append(alloc, 0);
    return try out.toOwnedSlice(alloc);
}

fn testBuildVp8xVp8lWebp(alloc: Allocator, rgba: [4]u8) ![]u8 {
    const vp8l_webp = try testBuildLiteralVp8lWebp(alloc, 1, 1, rgba);
    defer alloc.free(vp8l_webp);
    if (!std.mem.eql(u8, vp8l_webp[12..16], "VP8L")) return error.InvalidTestFixture;
    const vp8l_payload_len = try readU32Le(vp8l_webp, 16);
    const vp8l_chunk_len: usize = chunk_header_len + @as(usize, @intCast(vp8l_payload_len)) + (@as(usize, @intCast(vp8l_payload_len)) & 1);
    const riff_size: u32 = @intCast(4 + chunk_header_len + 10 + vp8l_chunk_len);

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "RIFF");
    try appendU32Le(alloc, &out, riff_size);
    try out.appendSlice(alloc, "WEBP");
    try out.appendSlice(alloc, "VP8X");
    try appendU32Le(alloc, &out, 10);
    try out.append(alloc, vp8x_flag_alpha);
    try out.appendSlice(alloc, &.{ 0, 0, 0 });
    try out.appendSlice(alloc, &.{ 0, 0, 0 });
    try out.appendSlice(alloc, &.{ 0, 0, 0 });
    try out.appendSlice(alloc, vp8l_webp[12 .. 12 + vp8l_chunk_len]);
    return try out.toOwnedSlice(alloc);
}

fn testBuildCompressedAlphPayload(alloc: Allocator, width: u32, height: u32, alpha: u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, 1);

    var bits = TestBitWriter{};
    defer bits.deinit(alloc);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 0, 1);
    try testWriteSimplePrefixSymbol(&bits, alloc, alpha);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try testWriteSimplePrefixSymbol(&bits, alloc, 255);
    try testWriteSimplePrefixSymbol(&bits, alloc, 0);
    try out.appendSlice(alloc, bits.bytes.items);
    _ = width;
    _ = height;
    return try out.toOwnedSlice(alloc);
}

fn appendU32Le(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: u32) !void {
    try out.append(alloc, @intCast(value & 0xff));
    try out.append(alloc, @intCast((value >> 8) & 0xff));
    try out.append(alloc, @intCast((value >> 16) & 0xff));
    try out.append(alloc, @intCast((value >> 24) & 0xff));
}

fn testVp8CoeffProbs(probability: u8) Vp8CoeffProbs {
    var probs: Vp8CoeffProbs = undefined;
    @memset(std.mem.asBytes(&probs), probability);
    return probs;
}

fn testVp8Luma4Probs(probability: u8) Vp8Luma4Probs {
    var probs: Vp8Luma4Probs = undefined;
    @memset(std.mem.asBytes(&probs), probability);
    return probs;
}

fn testVp8Syntax() Vp8FirstPartitionSyntax {
    return .{
        .color_space = false,
        .clamp_type = false,
        .segmentation = .{},
        .loop_filter = .{},
        .token_partition_count = 1,
        .quant = .{ .base_q = 0 },
        .refresh_entropy_probs = false,
    };
}

test "probe basic vp8 webp dimensions" {
    const info = try probe(&webp_vp8_1x1);
    try std.testing.expectEqual(Bitstream.vp8, info.bitstream.?);
    try std.testing.expectEqual(@as(u32, 1), info.width.?);
    try std.testing.expectEqual(@as(u32, 1), info.height.?);
    try std.testing.expect(!info.extended);
    try std.testing.expect(!info.animated);
}

test "parse vp8 keyframe partition metadata" {
    const alloc = std.testing.allocator;
    const p0 = [_]u8{ 1, 2, 3 };
    const t0 = [_]u8{ 4, 5 };
    const t1 = [_]u8{6};
    const payload = try testBuildVp8Payload(alloc, 7, 9, &p0, &.{ &t0, &t1 });
    defer alloc.free(payload);

    const parsed = try parseVp8PartitionInfo(alloc, payload, 2);
    defer alloc.free(parsed.token_partitions);

    try std.testing.expect(parsed.frame.keyframe);
    try std.testing.expect(parsed.frame.show_frame);
    try std.testing.expectEqual(@as(u32, 7), parsed.frame.width);
    try std.testing.expectEqual(@as(u32, 9), parsed.frame.height);
    try std.testing.expectEqualSlices(u8, &p0, parsed.first_partition);
    try std.testing.expectEqualSlices(u8, &t0, parsed.token_partitions[0]);
    try std.testing.expectEqualSlices(u8, &t1, parsed.token_partitions[1]);
}

test "parse vp8 keyframe first partition default syntax" {
    const alloc = std.testing.allocator;
    const first_partition = [_]u8{ 0, 0, 0, 0, 0, 0 };
    const token_partition = [_]u8{0xaa};
    const payload = try testBuildVp8Payload(alloc, 16, 16, &first_partition, &.{&token_partition});
    defer alloc.free(payload);

    const parsed = try parseVp8KeyframeInfo(alloc, payload);
    defer alloc.free(parsed.partitions.token_partitions);

    try std.testing.expect(!parsed.syntax.color_space);
    try std.testing.expect(!parsed.syntax.clamp_type);
    try std.testing.expect(!parsed.syntax.segmentation.enabled);
    try std.testing.expect(!parsed.syntax.loop_filter.simple);
    try std.testing.expectEqual(@as(u6, 0), parsed.syntax.loop_filter.level);
    try std.testing.expectEqual(@as(u3, 0), parsed.syntax.loop_filter.sharpness);
    try std.testing.expectEqual(@as(usize, 1), parsed.syntax.token_partition_count);
    try std.testing.expectEqual(@as(u7, 0), parsed.syntax.quant.base_q);
    try std.testing.expect(!parsed.syntax.refresh_entropy_probs);
    try std.testing.expectEqualSlices(u8, &token_partition, parsed.partitions.token_partitions[0]);
}

test "vp8 macroblock grid rounds frame dimensions up to 16x16 blocks" {
    const header = try parseVp8FrameHeader(webp_vp8_1x1[20..]);
    const grid = vp8MacroblockGrid(header);
    try std.testing.expectEqual(@as(u32, 1), grid.width);
    try std.testing.expectEqual(@as(u32, 1), grid.height);

    const alloc = std.testing.allocator;
    const payload = try testBuildVp8Payload(alloc, 17, 33, &.{ 0, 0, 0, 0 }, &.{&.{0}});
    defer alloc.free(payload);
    const parsed = try parseVp8FrameHeader(payload);
    const larger = vp8MacroblockGrid(parsed);
    try std.testing.expectEqual(@as(u32, 2), larger.width);
    try std.testing.expectEqual(@as(u32, 3), larger.height);
}

test "vp8 token partitions are selected by macroblock row modulo partition count" {
    try std.testing.expectEqual(@as(usize, 0), try vp8TokenPartitionForRow(0, 4));
    try std.testing.expectEqual(@as(usize, 1), try vp8TokenPartitionForRow(1, 4));
    try std.testing.expectEqual(@as(usize, 2), try vp8TokenPartitionForRow(2, 4));
    try std.testing.expectEqual(@as(usize, 3), try vp8TokenPartitionForRow(3, 4));
    try std.testing.expectEqual(@as(usize, 0), try vp8TokenPartitionForRow(4, 4));
    try std.testing.expectError(error.WebpDecodeFailed, vp8TokenPartitionForRow(0, 3));
}

test "vp8 yuv to rgba conversion clamps black and white" {
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 255 }, &vp8YuvToRgba(0, 128, 128, 255));
    try std.testing.expectEqualSlices(u8, &.{ 255, 255, 255, 77 }, &vp8YuvToRgba(255, 128, 128, 77));
}

test "vp8 frame planes allocate padded yuv buffers and crop rgba output" {
    const alloc = std.testing.allocator;
    const payload = try testBuildVp8Payload(alloc, 17, 17, &.{ 0, 0, 0, 0 }, &.{&.{0}});
    defer alloc.free(payload);
    const frame = try parseVp8FrameHeader(payload);

    var planes = try allocateVp8FramePlanes(alloc, frame);
    defer planes.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 32), planes.y_stride);
    try std.testing.expectEqual(@as(usize, 16), planes.uv_stride);
    try std.testing.expectEqual(@as(usize, 32 * 32), planes.y.len);
    try std.testing.expectEqual(@as(usize, 16 * 16), planes.u.len);
    try std.testing.expectEqual(@as(usize, 16 * 16), planes.v.len);

    planes.y[0] = 0;
    planes.y[1] = 255;
    planes.u[0] = 128;
    planes.v[0] = 128;

    const decoded = try vp8PlanesToRgbaAlloc(alloc, planes);
    defer alloc.free(decoded.rgba);
    try std.testing.expectEqual(@as(u32, 17), decoded.width);
    try std.testing.expectEqual(@as(u32, 17), decoded.height);
    try std.testing.expectEqual(@as(usize, 17 * 17 * 4), decoded.rgba.len);
    try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 255 }, decoded.rgba[0..4]);
    try std.testing.expectEqualSlices(u8, &.{ 255, 255, 255, 255 }, decoded.rgba[4..8]);
}

test "vp8 inverse dct add reconstructs dc-only and ac blocks" {
    var coeffs = @as([16]i16, @splat(0));
    coeffs[0] = 32;

    var dc_block = @as([16]u8, @splat(100));
    try vp8InverseDct4x4DcAdd(coeffs[0], dc_block[0..], 4);
    try std.testing.expectEqualSlices(u8, &(@as([16]u8, @splat(104))), dc_block[0..]);

    @memset(dc_block[0..], 100);
    try vp8InverseDct4x4Add(&coeffs, dc_block[0..], 4);
    try std.testing.expectEqualSlices(u8, &(@as([16]u8, @splat(104))), dc_block[0..]);

    const ac_coeffs = [_]i16{
        32, 16, -8, 4,
        12, -6, 3,  -1,
        5,  -3, 2,  -2,
        1,  -1, 1,  0,
    };
    var ac_block = @as([16]u8, @splat(128));
    try vp8InverseDct4x4Add(&ac_coeffs, ac_block[0..], 4);
    try std.testing.expectEqualSlices(u8, &.{
        135, 135, 135, 134,
        134, 134, 133, 128,
        133, 132, 132, 126,
        133, 133, 131, 126,
    }, ac_block[0..]);
}

test "vp8 inverse wht expands y2 coefficients into luma block dc values" {
    const input = [_]i16{
        32, -16, 8, -4,
        20, -12, 6, -2,
        10, -6,  4, -2,
        4,  -2,  2, 0,
    };
    const out = vp8InverseWht4x4(&input);
    try std.testing.expectEqualSlices(i16, &.{
        5, 2, 9, 16,
        3, 2, 6, 9,
        1, 1, 1, 1,
        1, 1, 2, 4,
    }, out[0..]);
}

test "vp8 luma16 predictors cover dc vertical horizontal and true-motion modes" {
    const top = [_]u8{
        0, 1, 2,  3,  4,  5,  6,  7,
        8, 9, 10, 11, 12, 13, 14, 15,
    };
    const left = @as([16]u8, @splat(16));

    var block = @as([(16 * 16)]u8, @splat(0));
    try vp8PredictLuma16(.dc, block[0..], 16, top[0..], left[0..], 10);
    try std.testing.expectEqual(@as(u8, 12), block[0]);
    try std.testing.expectEqual(@as(u8, 12), block[255]);

    try vp8PredictLuma16(.vertical, block[0..], 16, top[0..], left[0..], 10);
    try std.testing.expectEqualSlices(u8, top[0..], block[0..16]);
    try std.testing.expectEqualSlices(u8, top[0..], block[16 * 15 ..][0..16]);

    try vp8PredictLuma16(.horizontal, block[0..], 16, top[0..], left[0..], 10);
    try std.testing.expectEqualSlices(u8, &(@as([16]u8, @splat(16))), block[0..16]);

    try vp8PredictLuma16(.true_motion, block[0..], 16, top[0..], left[0..], 10);
    try std.testing.expectEqual(@as(u8, 6), block[0]);
    try std.testing.expectEqual(@as(u8, 21), block[15]);
}

test "vp8 chroma predictors cover dc vertical horizontal and true-motion modes" {
    const top = @as([8]u8, @splat(20));
    const left = @as([8]u8, @splat(36));

    var block = @as([(8 * 8)]u8, @splat(0));
    try vp8PredictChroma(.dc, block[0..], 8, top[0..], left[0..], 0);
    try std.testing.expectEqual(@as(u8, 28), block[0]);
    try std.testing.expectEqual(@as(u8, 28), block[63]);

    try vp8PredictChroma(.vertical, block[0..], 8, top[0..], null, 0);
    try std.testing.expectEqual(@as(u8, 20), block[0]);
    try std.testing.expectEqual(@as(u8, 20), block[7 * 8 + 7]);

    try vp8PredictChroma(.horizontal, block[0..], 8, null, left[0..], 0);
    try std.testing.expectEqual(@as(u8, 36), block[0]);
    try std.testing.expectEqual(@as(u8, 36), block[7 * 8 + 7]);

    try vp8PredictChroma(.true_motion, block[0..], 8, top[0..], left[0..], 16);
    try std.testing.expectEqual(@as(u8, 40), block[0]);
    try std.testing.expectEqual(@as(u8, 40), block[7 * 8 + 7]);
}

test "vp8 macroblock prediction writes luma and chroma planes" {
    const alloc = std.testing.allocator;
    const payload = try testBuildVp8Payload(alloc, 16, 16, &.{ 0, 0, 0, 0 }, &.{&.{0}});
    defer alloc.free(payload);
    const frame = try parseVp8FrameHeader(payload);

    var planes = try allocateVp8FramePlanes(alloc, frame);
    defer planes.deinit(alloc);
    try vp8PredictMacroblock16x16(&planes, 0, 0, .{
        .luma16_mode = .dc,
        .chroma_mode = .dc,
    });

    try std.testing.expectEqual(@as(u8, 128), planes.y[0]);
    try std.testing.expectEqual(@as(u8, 128), planes.y[15 * planes.y_stride + 15]);
    try std.testing.expectEqual(@as(u8, 128), planes.u[0]);
    try std.testing.expectEqual(@as(u8, 128), planes.v[7 * planes.uv_stride + 7]);
}

test "vp8 macroblock horizontal prediction uses left yuv samples" {
    const alloc = std.testing.allocator;
    const payload = try testBuildVp8Payload(alloc, 32, 16, &.{ 0, 0, 0, 0 }, &.{&.{0}});
    defer alloc.free(payload);
    const frame = try parseVp8FrameHeader(payload);

    var planes = try allocateVp8FramePlanes(alloc, frame);
    defer planes.deinit(alloc);

    var row: usize = 0;
    while (row < 16) : (row += 1) {
        planes.y[row * planes.y_stride + 15] = 70;
    }
    row = 0;
    while (row < 8) : (row += 1) {
        planes.u[row * planes.uv_stride + 7] = 90;
        planes.v[row * planes.uv_stride + 7] = 110;
    }

    try vp8PredictMacroblock16x16(&planes, 1, 0, .{
        .luma16_mode = .horizontal,
        .chroma_mode = .horizontal,
    });

    try std.testing.expectEqual(@as(u8, 70), planes.y[16]);
    try std.testing.expectEqual(@as(u8, 70), planes.y[15 * planes.y_stride + 31]);
    try std.testing.expectEqual(@as(u8, 90), planes.u[8]);
    try std.testing.expectEqual(@as(u8, 110), planes.v[7 * planes.uv_stride + 15]);
}

test "vp8 macroblock residual applies dc transforms to y u and v planes" {
    const alloc = std.testing.allocator;
    const payload = try testBuildVp8Payload(alloc, 16, 16, &.{ 0, 0, 0, 0 }, &.{&.{0}});
    defer alloc.free(payload);
    const frame = try parseVp8FrameHeader(payload);

    var planes = try allocateVp8FramePlanes(alloc, frame);
    defer planes.deinit(alloc);
    @memset(planes.y, 100);
    @memset(planes.u, 120);
    @memset(planes.v, 140);

    var coeffs = Vp8MacroblockCoeffs{};
    coeffs.y[0].coeffs[0] = 32;
    coeffs.y[0].last_nonzero_plus_one = 1;
    coeffs.u[0].coeffs[0] = 16;
    coeffs.u[0].last_nonzero_plus_one = 1;
    coeffs.v[0].coeffs[0] = -16;
    coeffs.v[0].last_nonzero_plus_one = 1;

    try vp8ApplyMacroblockResidual16x16(&planes, 0, 0, &coeffs);

    try std.testing.expectEqual(@as(u8, 104), planes.y[0]);
    try std.testing.expectEqual(@as(u8, 104), planes.y[3 * planes.y_stride + 3]);
    try std.testing.expectEqual(@as(u8, 100), planes.y[4]);
    try std.testing.expectEqual(@as(u8, 122), planes.u[0]);
    try std.testing.expectEqual(@as(u8, 138), planes.v[0]);
}

test "vp8 macroblock residual propagates y2 dc into all luma blocks" {
    const alloc = std.testing.allocator;
    const payload = try testBuildVp8Payload(alloc, 16, 16, &.{ 0, 0, 0, 0 }, &.{&.{0}});
    defer alloc.free(payload);
    const frame = try parseVp8FrameHeader(payload);

    var planes = try allocateVp8FramePlanes(alloc, frame);
    defer planes.deinit(alloc);
    @memset(planes.y, 100);

    var coeffs = Vp8MacroblockCoeffs{};
    coeffs.y2.coeffs[0] = 32;
    coeffs.y2.last_nonzero_plus_one = 1;

    try vp8ApplyMacroblockResidual16x16(&planes, 0, 0, &coeffs);

    try std.testing.expectEqual(@as(u8, 101), planes.y[0]);
    try std.testing.expectEqual(@as(u8, 101), planes.y[3 * planes.y_stride + 3]);
    try std.testing.expectEqual(@as(u8, 101), planes.y[15 * planes.y_stride + 15]);
}

test "vp8 loop filter common adjustment matches reference arithmetic" {
    var plane = [_]u8{ 0, 0, 100, 90, 120, 130, 0, 0 };

    try vp8FilterCommon(plane[0..], 4, 1, true);

    try std.testing.expectEqual(@as(u8, 100), plane[2]);
    try std.testing.expectEqual(@as(u8, 97), plane[3]);
    try std.testing.expectEqual(@as(u8, 112), plane[4]);
    try std.testing.expectEqual(@as(u8, 130), plane[5]);
}

test "vp8 loop filter macroblock edge adjusts p2 through q2" {
    var plane = [_]u8{ 0, 100, 100, 90, 120, 100, 100, 0 };

    try vp8FilterMacroblockEdge(plane[0..], 4, 1);

    try std.testing.expectEqual(@as(u8, 106), plane[1]);
    try std.testing.expectEqual(@as(u8, 113), plane[2]);
    try std.testing.expectEqual(@as(u8, 109), plane[3]);
    try std.testing.expectEqual(@as(u8, 101), plane[4]);
    try std.testing.expectEqual(@as(u8, 87), plane[5]);
    try std.testing.expectEqual(@as(u8, 94), plane[6]);
}

test "vp8 loop filter parameters honor segmentation sharpness and keyframe deltas" {
    var syntax = testVp8Syntax();
    syntax.loop_filter = .{
        .level = 20,
        .sharpness = 5,
        .use_lf_delta = true,
        .ref_lf_delta = .{ -5, 0, 0, 0 },
        .mode_lf_delta = .{ 3, 0, 0, 0 },
    };
    syntax.segmentation = .{
        .enabled = true,
        .absolute_delta = false,
        .filter_strength = .{ 0, 10, -10, 60 },
    };

    const params = vp8LoopFilterParamsFor(syntax, .{
        .segment = 1,
        .is_i4x4 = true,
        .has_coeffs = true,
    });

    try std.testing.expectEqual(@as(i32, 28), params.edge_limit);
    try std.testing.expectEqual(@as(i32, 4), params.interior_limit);
    try std.testing.expectEqual(@as(i32, 1), params.hev_threshold);
}

test "vp8 frame assembly decodes cropped all-eob 16x16 intra keyframe" {
    const alloc = std.testing.allocator;
    const payload = try testBuildVp8Payload(alloc, 1, 1, &.{ 0, 0, 0, 0 }, &.{&.{0}});
    defer alloc.free(payload);
    const frame = try parseVp8FrameHeader(payload);
    const token_payload = @as([32]u8, @splat(0));
    const expected = vp8YuvToRgba(128, 128, 128, 255);

    const decoded = try assembleVp8KeyframeDefaultRgba(alloc, frame, .{
        .syntax = testVp8Syntax(),
        .entropy = .{ .coeff_probs = testVp8CoeffProbs(255) },
        .mode_reader = Vp8BoolReader.init(&(@as([16]u8, @splat(144)))),
    }, &.{&token_payload});
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, expected[0..], decoded.rgba);
}

test "vp8 frame assembly walks macroblocks in raster order and crops output" {
    const alloc = std.testing.allocator;
    const payload = try testBuildVp8Payload(alloc, 17, 1, &.{ 0, 0, 0, 0 }, &.{&.{0}});
    defer alloc.free(payload);
    const frame = try parseVp8FrameHeader(payload);
    const token_payload = @as([64]u8, @splat(0));
    const expected = vp8YuvToRgba(128, 128, 128, 255);

    const decoded = try assembleVp8KeyframeDefaultRgba(alloc, frame, .{
        .syntax = testVp8Syntax(),
        .entropy = .{ .coeff_probs = testVp8CoeffProbs(255) },
        .mode_reader = Vp8BoolReader.init(&(@as([32]u8, @splat(158)))),
    }, &.{&token_payload});
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 17), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqual(@as(usize, 17 * 4), decoded.rgba.len);
    try std.testing.expectEqualSlices(u8, expected[0..], decoded.rgba[0..4]);
    try std.testing.expectEqualSlices(u8, expected[0..], decoded.rgba[16 * 4 ..][0..4]);
}

test "vp8 frame assembly supports all-eob 4x4 luma macroblocks" {
    const alloc = std.testing.allocator;
    const payload = try testBuildVp8Payload(alloc, 1, 1, &.{ 0, 0, 0, 0 }, &.{&.{0}});
    defer alloc.free(payload);
    const frame = try parseVp8FrameHeader(payload);
    const token_payload = @as([64]u8, @splat(0));
    const expected = vp8YuvToRgba(128, 128, 128, 255);

    const decoded = try assembleVp8KeyframeDefaultRgba(alloc, frame, .{
        .syntax = testVp8Syntax(),
        .entropy = .{ .coeff_probs = testVp8CoeffProbs(255) },
        .mode_reader = Vp8BoolReader.init(&.{ 0, 0, 0, 0 }),
    }, &.{&token_payload});
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, expected[0..], decoded.rgba);
}

test "vp8 frame assembly accepts nonzero loop filter on smooth keyframe" {
    const alloc = std.testing.allocator;
    const payload = try testBuildVp8Payload(alloc, 1, 1, &.{ 0, 0, 0, 0 }, &.{&.{0}});
    defer alloc.free(payload);
    const frame = try parseVp8FrameHeader(payload);
    const token_payload = @as([64]u8, @splat(0));
    const expected = vp8YuvToRgba(128, 128, 128, 255);
    var syntax = testVp8Syntax();
    syntax.loop_filter.level = 16;

    const decoded = try assembleVp8KeyframeDefaultRgba(alloc, frame, .{
        .syntax = syntax,
        .entropy = .{ .coeff_probs = testVp8CoeffProbs(255) },
        .mode_reader = Vp8BoolReader.init(&.{ 0, 0, 0, 0 }),
    }, &.{&token_payload});
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, expected[0..], decoded.rgba);
}

test "vp8 4x4 macroblock reconstruction uses edge defaults and residuals" {
    const alloc = std.testing.allocator;
    const payload = try testBuildVp8Payload(alloc, 16, 16, &.{ 0, 0, 0, 0 }, &.{&.{0}});
    defer alloc.free(payload);
    const frame = try parseVp8FrameHeader(payload);

    var planes = try allocateVp8FramePlanes(alloc, frame);
    defer planes.deinit(alloc);
    var coeffs = Vp8MacroblockCoeffs{};
    coeffs.y[0].coeffs[0] = 32;
    coeffs.y[0].last_nonzero_plus_one = 1;

    try vp8ReconstructMacroblock4x4(&planes, 0, 0, .{
        .is_i4x4 = true,
        .luma4_modes = @as([16]Vp8Luma4Mode, @splat(.dc)),
        .chroma_mode = .dc,
    }, &coeffs);

    try std.testing.expectEqual(@as(u8, 132), planes.y[0]);
    try std.testing.expectEqual(@as(u8, 132), planes.y[3 * planes.y_stride + 3]);
    try std.testing.expectEqual(@as(u8, 130), planes.y[4]);
    try std.testing.expectEqual(@as(u8, 128), planes.u[0]);
    try std.testing.expectEqual(@as(u8, 128), planes.v[0]);
}

test "vp8 luma4 directional predictors match scalar formulas" {
    const top = [_]u8{ 10, 20, 30, 40, 50, 60, 70, 80 };
    const left = [_]u8{ 90, 100, 110, 120 };

    var block = @as([16]u8, @splat(0));
    try vp8PredictLuma4(.down_left, block[0..], 4, top[0..], left[0..], 80);
    try std.testing.expectEqualSlices(u8, &.{
        20, 30, 40, 50,
        30, 40, 50, 60,
        40, 50, 60, 70,
        50, 60, 70, 78,
    }, block[0..]);

    try vp8PredictLuma4(.horizontal_up, block[0..], 4, top[0..], left[0..], 80);
    try std.testing.expectEqualSlices(u8, &.{
        95,  100, 105, 110,
        105, 110, 115, 118,
        115, 118, 120, 120,
        120, 120, 120, 120,
    }, block[0..]);
}

test "vp8 macroblock mode trees decode 16x16 luma and chroma modes" {
    var low_luma = Vp8BoolReader.init(&.{0});
    try std.testing.expectEqual(Vp8Luma16Mode.dc, try vp8ReadLuma16Mode(&low_luma));

    const high_mode_bits = @as([8]u8, @splat(255));
    var high_luma = Vp8BoolReader.init(&high_mode_bits);
    try std.testing.expectEqual(Vp8Luma16Mode.true_motion, try vp8ReadLuma16Mode(&high_luma));

    var low_chroma = Vp8BoolReader.init(&.{0});
    try std.testing.expectEqual(Vp8ChromaMode.dc, try vp8ReadChromaMode(&low_chroma));

    var high_chroma = Vp8BoolReader.init(&high_mode_bits);
    try std.testing.expectEqual(Vp8ChromaMode.true_motion, try vp8ReadChromaMode(&high_chroma));
}

test "vp8 macroblock segment tree and skip flag feed 16x16 header parser" {
    var syntax = testVp8Syntax();
    syntax.segmentation = .{
        .enabled = true,
        .update_map = true,
        .segment_probs = .{ 128, 128, 128 },
    };

    var segment_low = Vp8BoolReader.init(&.{0});
    try std.testing.expectEqual(@as(u2, 0), try vp8ReadSegmentId(&segment_low, syntax.segmentation));

    const high_segment_bits = @as([8]u8, @splat(255));
    var segment_high = Vp8BoolReader.init(&high_segment_bits);
    try std.testing.expectEqual(@as(u2, 3), try vp8ReadSegmentId(&segment_high, syntax.segmentation));

    const high_header_bits = @as([16]u8, @splat(255));
    var reader = Vp8BoolReader.init(&high_header_bits);
    const header = try vp8ReadMacroblockHeader16x16(&reader, syntax, 128);
    try std.testing.expectEqual(@as(u2, 3), header.segment);
    try std.testing.expect(header.skip);
    try std.testing.expect(!header.is_i4x4);
    try std.testing.expectEqual(Vp8Luma16Mode.true_motion, header.luma16_mode.?);
    try std.testing.expectEqual(Vp8ChromaMode.true_motion, header.chroma_mode);
}

test "vp8 luma4 mode tree decodes low and high branches" {
    const probs = @as([9]u8, @splat(128));

    var low = Vp8BoolReader.init(&.{0});
    try std.testing.expectEqual(Vp8Luma4Mode.dc, try vp8ReadLuma4Mode(&low, &probs));

    const high_luma4_bits = @as([8]u8, @splat(255));
    var high = Vp8BoolReader.init(&high_luma4_bits);
    try std.testing.expectEqual(Vp8Luma4Mode.horizontal_up, try vp8ReadLuma4Mode(&high, &probs));
}

test "vp8 default keyframe luma4 probabilities match reference entries" {
    try std.testing.expectEqual(@as(u8, 231), vp8_default_luma4_probs[0][0][0]);
    try std.testing.expectEqual(@as(u8, 112), vp8_default_luma4_probs[0][0][8]);
    try std.testing.expectEqual(@as(u8, 255), vp8_default_luma4_probs[1][4][5]);
    try std.testing.expectEqual(@as(u8, 1), vp8_default_luma4_probs[3][6][7]);
    try std.testing.expectEqual(@as(u8, 255), vp8_default_luma4_probs[7][7][5]);
    try std.testing.expectEqual(@as(u8, 24), vp8_default_luma4_probs[9][9][8]);
}

test "vp8 luma4 grid parser updates top and left prediction contexts" {
    const probs = testVp8Luma4Probs(128);
    var top_modes = [_]Vp8Luma4Mode{ .dc, .dc, .dc, .dc };
    var left_modes = [_]Vp8Luma4Mode{ .dc, .dc, .dc, .dc };

    const low_grid_bits = @as([8]u8, @splat(0));
    var low = Vp8BoolReader.init(&low_grid_bits);
    const low_modes = try vp8ReadLuma4ModeGrid(&low, &probs, &top_modes, &left_modes);
    try std.testing.expectEqualSlices(Vp8Luma4Mode, &(@as([16]Vp8Luma4Mode, @splat(.dc))), low_modes[0..]);
    try std.testing.expectEqualSlices(Vp8Luma4Mode, &(@as([4]Vp8Luma4Mode, @splat(.dc))), top_modes[0..]);
    try std.testing.expectEqualSlices(Vp8Luma4Mode, &(@as([4]Vp8Luma4Mode, @splat(.dc))), left_modes[0..]);

    top_modes = [_]Vp8Luma4Mode{ .dc, .dc, .dc, .dc };
    left_modes = [_]Vp8Luma4Mode{ .dc, .dc, .dc, .dc };
    const high_bits = @as([32]u8, @splat(255));
    var high = Vp8BoolReader.init(&high_bits);
    const high_modes = try vp8ReadLuma4ModeGrid(&high, &probs, &top_modes, &left_modes);
    try std.testing.expectEqualSlices(Vp8Luma4Mode, &(@as([16]Vp8Luma4Mode, @splat(.horizontal_up))), high_modes[0..]);
    try std.testing.expectEqualSlices(Vp8Luma4Mode, &(@as([4]Vp8Luma4Mode, @splat(.horizontal_up))), top_modes[0..]);
    try std.testing.expectEqualSlices(Vp8Luma4Mode, &(@as([4]Vp8Luma4Mode, @splat(.horizontal_up))), left_modes[0..]);
}

test "vp8 macroblock header parser consumes 4x4 modes and chroma mode" {
    const probs = testVp8Luma4Probs(128);
    var top_modes = [_]Vp8Luma4Mode{ .dc, .dc, .dc, .dc };
    var left_modes = [_]Vp8Luma4Mode{ .dc, .dc, .dc, .dc };
    var reader = Vp8BoolReader.init(&.{ 0, 0, 0, 0 });

    const header = try vp8ReadMacroblockHeader(&reader, testVp8Syntax(), null, &top_modes, &left_modes, &probs);
    try std.testing.expect(header.is_i4x4);
    try std.testing.expect(header.luma16_mode == null);
    try std.testing.expectEqualSlices(Vp8Luma4Mode, &(@as([16]Vp8Luma4Mode, @splat(.dc))), header.luma4_modes.?[0..]);
    try std.testing.expectEqual(Vp8ChromaMode.dc, header.chroma_mode);
}

test "vp8 specialized 16x16 macroblock parser rejects 4x4 mode payloads" {
    var reader = Vp8BoolReader.init(&.{0});
    try std.testing.expectError(error.UnsupportedWebpFormat, vp8ReadMacroblockHeader16x16(&reader, testVp8Syntax(), null));
}

test "vp8 coefficient reader handles eob and literal one token" {
    const probs = testVp8CoeffProbs(128);

    var eob_reader = Vp8BoolReader.init(&.{0});
    const eob = try vp8ReadCoeffBlock(&eob_reader, &probs, 0, 0, 0, .{ 3, 5 });
    try std.testing.expectEqual(@as(u5, 0), eob.last_nonzero_plus_one);
    try std.testing.expectEqualSlices(i16, &(@as([16]i16, @splat(0))), eob.coeffs[0..]);

    var one_reader = Vp8BoolReader.init(&(.{0b1100_0000} ++ (@as([8]u8, @splat(0)))));
    const one = try vp8ReadCoeffBlock(&one_reader, &probs, 0, 0, 0, .{ 3, 5 });
    try std.testing.expectEqual(@as(u5, 1), one.last_nonzero_plus_one);
    try std.testing.expectEqual(@as(i16, 3), one.coeffs[0]);
    try std.testing.expectEqualSlices(i16, &(@as([15]i16, @splat(0))), one.coeffs[1..]);
}

test "vp8 coefficient reader applies zigzag ac position and sign" {
    const probs = testVp8CoeffProbs(128);
    var reader = Vp8BoolReader.init(&(.{0b1101_0000} ++ (@as([8]u8, @splat(0)))));
    const block = try vp8ReadCoeffBlock(&reader, &probs, 3, 2, 1, .{ 3, 7 });
    try std.testing.expectEqual(@as(u5, 2), block.last_nonzero_plus_one);
    try std.testing.expectEqual(@as(i16, -7), block.coeffs[1]);
    try std.testing.expectEqual(@as(i16, 0), block.coeffs[0]);
}

test "vp8 coefficient reader decodes zero runs and large category entry points" {
    const probs = testVp8CoeffProbs(128);

    var zero_run_reader = Vp8BoolReader.init(&(.{0b1010_0000} ++ (@as([8]u8, @splat(0)))));
    const zero_run = try vp8ReadCoeffBlock(&zero_run_reader, &probs, 0, 0, 0, .{ 3, 5 });
    try std.testing.expectEqual(@as(u5, 2), zero_run.last_nonzero_plus_one);
    try std.testing.expectEqual(@as(i16, 0), zero_run.coeffs[0]);
    try std.testing.expectEqual(@as(i16, 5), zero_run.coeffs[1]);

    const low_large_bits = @as([8]u8, @splat(0));
    var large_reader = Vp8BoolReader.init(&low_large_bits);
    const p = @as([vp8_coeff_proba_count]u8, @splat(128));
    try std.testing.expectEqual(@as(i16, 2), try vp8ReadLargeCoeffValue(&large_reader, &p));
}

test "vp8 macroblock coefficient reader resets token contexts for skipped blocks" {
    const probs = testVp8CoeffProbs(128);
    var context = Vp8MacroblockTokenContext{
        .y_above = .{ 1, 1, 1, 1 },
        .y_left = .{ 1, 1, 1, 1 },
        .y2_above = 1,
        .y2_left = 1,
        .u_above = .{ 1, 1 },
        .u_left = .{ 1, 1 },
        .v_above = .{ 1, 1 },
        .v_left = .{ 1, 1 },
    };
    var reader = Vp8BoolReader.init(&.{});
    const coeffs = try vp8ReadMacroblockCoeffs(&reader, &probs, .{
        .skip = true,
        .luma16_mode = .dc,
        .chroma_mode = .dc,
    }, vp8BuildQuantMatrix(0, .{ .base_q = 0 }), &context);

    try std.testing.expectEqualSlices(i16, &(@as([16]i16, @splat(0))), coeffs.y2.coeffs[0..]);
    try std.testing.expectEqual(Vp8MacroblockTokenContext{}, context);
}

test "vp8 skipped i4x4 macroblocks preserve y2 token contexts" {
    const probs = testVp8CoeffProbs(128);
    var context = Vp8MacroblockTokenContext{
        .y_above = .{ 1, 1, 1, 1 },
        .y_left = .{ 1, 1, 1, 1 },
        .y2_above = 1,
        .y2_left = 1,
        .u_above = .{ 1, 1 },
        .u_left = .{ 1, 1 },
        .v_above = .{ 1, 1 },
        .v_left = .{ 1, 1 },
    };
    var reader = Vp8BoolReader.init(&.{});
    const coeffs = try vp8ReadMacroblockCoeffs(&reader, &probs, .{
        .skip = true,
        .is_i4x4 = true,
        .luma4_modes = @as([16]Vp8Luma4Mode, @splat(.dc)),
        .chroma_mode = .dc,
    }, vp8BuildQuantMatrix(0, .{ .base_q = 0 }), &context);

    try std.testing.expectEqual(@as(u5, 0), coeffs.y2.last_nonzero_plus_one);
    try std.testing.expectEqualSlices(u1, &(@as([4]u1, @splat(0))), context.y_above[0..]);
    try std.testing.expectEqualSlices(u1, &(@as([4]u1, @splat(0))), context.y_left[0..]);
    try std.testing.expectEqual(@as(u1, 1), context.y2_above);
    try std.testing.expectEqual(@as(u1, 1), context.y2_left);
    try std.testing.expectEqualSlices(u1, &(@as([2]u1, @splat(0))), context.u_above[0..]);
    try std.testing.expectEqualSlices(u1, &(@as([2]u1, @splat(0))), context.u_left[0..]);
    try std.testing.expectEqualSlices(u1, &(@as([2]u1, @splat(0))), context.v_above[0..]);
    try std.testing.expectEqualSlices(u1, &(@as([2]u1, @splat(0))), context.v_left[0..]);
}

test "vp8 macroblock coefficient reader handles all-eob 16x16 blocks" {
    const probs = testVp8CoeffProbs(255);
    var context = Vp8MacroblockTokenContext{};
    const payload = @as([16]u8, @splat(0));
    var reader = Vp8BoolReader.init(&payload);
    const coeffs = try vp8ReadMacroblockCoeffs(&reader, &probs, .{
        .luma16_mode = .dc,
        .chroma_mode = .dc,
    }, vp8BuildQuantMatrix(0, .{ .base_q = 0 }), &context);

    try std.testing.expectEqual(@as(u5, 0), coeffs.y2.last_nonzero_plus_one);
    for (coeffs.y) |block| try std.testing.expectEqual(@as(u5, 0), block.last_nonzero_plus_one);
    for (coeffs.u) |block| try std.testing.expectEqual(@as(u5, 0), block.last_nonzero_plus_one);
    for (coeffs.v) |block| try std.testing.expectEqual(@as(u5, 0), block.last_nonzero_plus_one);
    try std.testing.expectEqual(Vp8MacroblockTokenContext{}, context);
}

test "vp8 macroblock coefficient reader decodes y2 before y1 and uv blocks" {
    const probs = testVp8CoeffProbs(128);
    var context = Vp8MacroblockTokenContext{};
    const payload = [_]u8{0b1100_0000} ++ (@as([32]u8, @splat(0)));
    var reader = Vp8BoolReader.init(&payload);
    const quant = Vp8QuantMatrix{
        .y1 = .{ 3, 5 },
        .y2 = .{ 7, 11 },
        .uv = .{ 13, 17 },
        .uv_quant = 0,
    };
    const coeffs = try vp8ReadMacroblockCoeffs(&reader, &probs, .{
        .luma16_mode = .dc,
        .chroma_mode = .dc,
    }, quant, &context);

    try std.testing.expectEqual(@as(u5, 1), coeffs.y2.last_nonzero_plus_one);
    try std.testing.expectEqual(@as(i16, 7), coeffs.y2.coeffs[0]);
    try std.testing.expectEqual(@as(u1, 1), context.y2_above);
    try std.testing.expectEqual(@as(u1, 1), context.y2_left);
}

test "vp8 macroblock coefficient reader decodes 4x4 luma with y-without-y2 type" {
    const probs = testVp8CoeffProbs(128);
    var context = Vp8MacroblockTokenContext{};
    const payload = [_]u8{0b1100_0000} ++ (@as([32]u8, @splat(0)));
    var reader = Vp8BoolReader.init(&payload);
    const quant = Vp8QuantMatrix{
        .y1 = .{ 3, 5 },
        .y2 = .{ 7, 11 },
        .uv = .{ 13, 17 },
        .uv_quant = 0,
    };
    const coeffs = try vp8ReadMacroblockCoeffs(&reader, &probs, .{
        .is_i4x4 = true,
        .luma4_modes = @as([16]Vp8Luma4Mode, @splat(.dc)),
        .chroma_mode = .dc,
    }, quant, &context);

    try std.testing.expectEqual(@as(u5, 0), coeffs.y2.last_nonzero_plus_one);
    try std.testing.expectEqual(@as(u5, 1), coeffs.y[0].last_nonzero_plus_one);
    try std.testing.expectEqual(@as(i16, 3), coeffs.y[0].coeffs[0]);
    try std.testing.expectEqual(@as(u1, 0), context.y2_above);
    try std.testing.expectEqual(@as(u1, 0), context.y2_left);
}

test "vp8 entropy header preserves coefficient probabilities when no updates are present" {
    const base = testVp8CoeffProbs(42);
    const update_probs = testVp8CoeffProbs(255);
    const payload = @as([200]u8, @splat(0));
    var reader = Vp8BoolReader.init(&payload);

    const entropy = try vp8ReadEntropyHeader(&reader, base, &update_probs);
    try std.testing.expectEqualSlices(u8, std.mem.asBytes(&base), std.mem.asBytes(&entropy.coeff_probs));
    try std.testing.expect(entropy.skip_probability == null);
}

test "vp8 default coefficient probabilities match reference table entries" {
    const probs = vp8DefaultCoeffProbs();
    try std.testing.expectEqual(@as(u8, 128), probs[0][0][0][0]);
    try std.testing.expectEqual(@as(u8, 253), probs[0][1][0][0]);
    try std.testing.expectEqual(@as(u8, 62), probs[1][0][0][10]);
    try std.testing.expectEqual(@as(u8, 238), probs[3][7][2][0]);
    try std.testing.expectEqual(@as(u8, 255), probs[3][7][2][2]);
}

test "vp8 coefficient update probabilities match reference table entries" {
    const probs = vp8CoeffUpdateProbs();
    try std.testing.expectEqual(@as(u8, 255), probs[0][0][0][0]);
    try std.testing.expectEqual(@as(u8, 176), probs[0][1][0][0]);
    try std.testing.expectEqual(@as(u8, 217), probs[1][0][0][0]);
    try std.testing.expectEqual(@as(u8, 186), probs[2][0][0][0]);
    try std.testing.expectEqual(@as(u8, 248), probs[3][0][0][0]);
    try std.testing.expectEqual(@as(u8, 254), probs[3][7][1][0]);
    try std.testing.expectEqual(@as(u8, 255), probs[3][7][2][1]);
}

test "vp8 entropy header can preserve default coefficient probabilities" {
    const base = vp8DefaultCoeffProbs();
    const update_probs = testVp8CoeffProbs(255);
    const payload = @as([200]u8, @splat(0));
    var reader = Vp8BoolReader.init(&payload);

    const entropy = try vp8ReadEntropyHeader(&reader, base, &update_probs);
    try std.testing.expectEqual(@as(u8, 253), entropy.coeff_probs[0][1][0][0]);
    try std.testing.expectEqual(@as(u8, 62), entropy.coeff_probs[1][0][0][10]);
    try std.testing.expect(entropy.skip_probability == null);
}

test "vp8 default entropy header uses reference default and update probability tables" {
    const payload = @as([200]u8, @splat(0));
    var reader = Vp8BoolReader.init(&payload);

    const entropy = try vp8ReadDefaultEntropyHeader(&reader);
    try std.testing.expectEqual(@as(u8, 253), entropy.coeff_probs[0][1][0][0]);
    try std.testing.expectEqual(@as(u8, 62), entropy.coeff_probs[1][0][0][10]);
    try std.testing.expect(entropy.skip_probability == null);
}

test "vp8 keyframe control parser leaves reader at macroblock mode records" {
    const first_partition = @as([200]u8, @splat(0));
    const control = try parseVp8KeyframeControl(&first_partition);
    try std.testing.expectEqual(@as(usize, 1), control.syntax.token_partition_count);
    try std.testing.expectEqual(@as(u7, 0), control.syntax.quant.base_q);
    try std.testing.expectEqual(@as(u8, 253), control.entropy.coeff_probs[0][1][0][0]);
    try std.testing.expect(control.entropy.skip_probability == null);

    const probs = testVp8Luma4Probs(128);
    var top_modes = [_]Vp8Luma4Mode{ .dc, .dc, .dc, .dc };
    var left_modes = [_]Vp8Luma4Mode{ .dc, .dc, .dc, .dc };
    var mode_reader = control.mode_reader;
    const header = try vp8ReadMacroblockHeader(
        &mode_reader,
        control.syntax,
        control.entropy.skip_probability,
        &top_modes,
        &left_modes,
        &probs,
    );

    try std.testing.expect(header.is_i4x4);
    try std.testing.expectEqualSlices(Vp8Luma4Mode, &(@as([16]Vp8Luma4Mode, @splat(.dc))), header.luma4_modes.?[0..]);
    try std.testing.expectEqual(Vp8ChromaMode.dc, header.chroma_mode);
}

test "vp8 quant matrices derive dequant factors from base and deltas" {
    const syntax = Vp8FirstPartitionSyntax{
        .color_space = false,
        .clamp_type = false,
        .segmentation = .{},
        .loop_filter = .{},
        .token_partition_count = 1,
        .quant = .{
            .base_q = 10,
            .y1_dc_delta = 2,
            .y2_dc_delta = -3,
            .y2_ac_delta = -10,
            .uv_dc_delta = 120,
            .uv_ac_delta = 5,
        },
        .refresh_entropy_probs = false,
    };
    const matrices = vp8BuildSegmentQuantMatrices(syntax);

    try std.testing.expectEqual(@as(i16, 15), matrices[0].y1[0]);
    try std.testing.expectEqual(@as(i16, 14), matrices[0].y1[1]);
    try std.testing.expectEqual(@as(i16, 20), matrices[0].y2[0]);
    try std.testing.expectEqual(@as(i16, 8), matrices[0].y2[1]);
    try std.testing.expectEqual(@as(i16, 132), matrices[0].uv[0]);
    try std.testing.expectEqual(@as(i16, 19), matrices[0].uv[1]);
    try std.testing.expectEqual(@as(i16, 15), matrices[0].uv_quant);
    try std.testing.expectEqual(matrices[0], matrices[3]);
}

test "vp8 quant matrices honor absolute and delta segment quantizers" {
    const quant = Vp8QuantHeader{
        .base_q = 20,
        .y1_dc_delta = 0,
        .y2_dc_delta = 0,
        .y2_ac_delta = 0,
        .uv_dc_delta = 0,
        .uv_ac_delta = 0,
    };

    const absolute = vp8BuildSegmentQuantMatrices(.{
        .color_space = false,
        .clamp_type = false,
        .segmentation = .{
            .enabled = true,
            .absolute_delta = true,
            .quantizer = .{ 0, 10, 20, 127 },
        },
        .loop_filter = .{},
        .token_partition_count = 1,
        .quant = quant,
        .refresh_entropy_probs = false,
    });
    try std.testing.expectEqual(@as(i16, 4), absolute[0].y1[0]);
    try std.testing.expectEqual(@as(i16, 13), absolute[1].y1[0]);
    try std.testing.expectEqual(@as(i16, 21), absolute[2].y1[0]);
    try std.testing.expectEqual(@as(i16, 157), absolute[3].y1[0]);

    const delta = vp8BuildSegmentQuantMatrices(.{
        .color_space = false,
        .clamp_type = false,
        .segmentation = .{
            .enabled = true,
            .absolute_delta = false,
            .quantizer = .{ -30, 0, 10, 120 },
        },
        .loop_filter = .{},
        .token_partition_count = 1,
        .quant = quant,
        .refresh_entropy_probs = false,
    });
    try std.testing.expectEqual(@as(i16, 4), delta[0].y1[0]);
    try std.testing.expectEqual(@as(i16, 21), delta[1].y1[0]);
    try std.testing.expectEqual(@as(i16, 27), delta[2].y1[0]);
    try std.testing.expectEqual(@as(i16, 157), delta[3].y1[0]);
}

test "compose alph plane overwrites decoded alpha channel" {
    var rgba = [_]u8{
        1, 2, 3, 255,
        4, 5, 6, 255,
    };
    var decoded = DecodedImage{
        .rgba = rgba[0..],
        .width = 2,
        .height = 1,
    };
    try composeAlphaPlane(&decoded, &.{ 9, 10 });
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3, 9, 4, 5, 6, 10 }, decoded.rgba);
}

test "probe basic vp8l webp dimensions" {
    const info = try probe(&webp_vp8l_2x3);
    try std.testing.expectEqual(Bitstream.vp8l, info.bitstream.?);
    try std.testing.expectEqual(@as(u32, 2), info.width.?);
    try std.testing.expectEqual(@as(u32, 3), info.height.?);
}

test "probe standalone vp8l alpha bit" {
    const alloc = std.testing.allocator;
    const webp = try testBuildLiteralVp8lWebp(alloc, 1, 1, .{ 0x10, 0x20, 0x30, 0x40 });
    defer alloc.free(webp);

    const info = try probe(webp);
    try std.testing.expectEqual(Bitstream.vp8l, info.bitstream.?);
    try std.testing.expect(info.alpha);
}

test "decode rejects vp8l stream with undeclared alpha pixels" {
    const alloc = std.testing.allocator;
    const webp = try testBuildLiteralVp8lWebp(alloc, 1, 1, .{ 0x10, 0x20, 0x30, 0x40 });
    defer alloc.free(webp);

    webp[24] &= ~@as(u8, 0x10);
    const info = try probe(webp);
    try std.testing.expect(!info.alpha);
    try std.testing.expectError(error.WebpDecodeFailed, decodeRgba(alloc, webp));
}

test "probe vp8x alpha chunk before lossy image" {
    const info = try probe(&webp_vp8x_alpha_1x1);
    try std.testing.expectEqual(Bitstream.vp8, info.bitstream.?);
    try std.testing.expect(info.extended);
    try std.testing.expect(info.alpha);
    try std.testing.expect(!info.animated);
}

test "probe rejects alph chunk when vp8x alpha flag is clear" {
    const alloc = std.testing.allocator;
    const first_partition = @as([200]u8, @splat(0));
    const token_partition = @as([64]u8, @splat(0));
    const alpha_payload = [_]u8{ 0, 0x7d };
    const webp = try testBuildVp8xAlphVp8Webp(alloc, &alpha_payload, 1, 1, &first_partition, &.{&token_partition});
    defer alloc.free(webp);

    webp[20] = 0;
    try std.testing.expectError(error.WebpDecodeFailed, probe(webp));
    try std.testing.expectError(error.WebpDecodeFailed, decodeRgba(alloc, webp));
}

test "decode vp8x alpha flag with vp8l uses lossless alpha channel" {
    const alloc = std.testing.allocator;
    const expected = [4]u8{ 0x10, 0x20, 0x30, 0x40 };
    const webp = try testBuildVp8xVp8lWebp(alloc, expected);
    defer alloc.free(webp);

    const info = try probe(webp);
    try std.testing.expect(info.extended);
    try std.testing.expect(info.alpha);
    try std.testing.expectEqual(Bitstream.vp8l, info.bitstream.?);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &expected, decoded.rgba);
}

test "probe rejects vp8x alpha flag mismatch with vp8l header" {
    const alloc = std.testing.allocator;
    const opaque_vp8l = try testBuildVp8xVp8lWebp(alloc, .{ 0x10, 0x20, 0x30, 0xff });
    defer alloc.free(opaque_vp8l);

    try std.testing.expectError(error.WebpDecodeFailed, probe(opaque_vp8l));
    try std.testing.expectError(error.WebpDecodeFailed, decodeRgba(alloc, opaque_vp8l));
}

test "decode explicitly rejects animated webp" {
    try std.testing.expectError(error.AnimatedWebpUnsupported, decodeRgba(std.testing.allocator, &webp_vp8x_animated_1x1));
}

test "probe rejects animation chunks without vp8x animation flag" {
    const anim_without_vp8x = [_]u8{
        'R', 'I', 'F', 'F', 12,  0,   0,   0,
        'W', 'E', 'B', 'P', 'A', 'N', 'I', 'M',
        0,   0,   0,   0,
    };
    try std.testing.expectError(error.WebpDecodeFailed, probe(&anim_without_vp8x));

    var vp8x_without_animation_flag = webp_vp8x_animated_1x1;
    vp8x_without_animation_flag[20] = 0;
    try std.testing.expectError(error.WebpDecodeFailed, probe(&vp8x_without_animation_flag));
}

test "probe rejects vp8x animation flag without animation chunks" {
    const vp8x_animation_only = [_]u8{
        'R', 'I', 'F', 'F', 22,                  0,   0,   0,
        'W', 'E', 'B', 'P', 'V',                 'P', '8', 'X',
        10,  0,   0,   0,   vp8x_flag_animation, 0,   0,   0,
        0,   0,   0,   0,   0,                   0,
    };
    try std.testing.expectError(error.WebpDecodeFailed, probe(&vp8x_animation_only));
}

test "probe rejects animated webp without animation frame" {
    const anim_without_frame = [_]u8{
        'R', 'I', 'F', 'F', 36,                  0,   0,   0,
        'W', 'E', 'B', 'P', 'V',                 'P', '8', 'X',
        10,  0,   0,   0,   vp8x_flag_animation, 0,   0,   0,
        0,   0,   0,   0,   0,                   0,   'A', 'N',
        'I', 'M', 6,   0,   0,                   0,   0,   0,
        0,   0,   0,   0,
    };
    try std.testing.expectError(error.WebpDecodeFailed, probe(&anim_without_frame));
}

test "probe rejects animation frame before animation control" {
    const frame_without_anim = [_]u8{
        'R', 'I', 'F', 'F', 46,                  0,   0,   0,
        'W', 'E', 'B', 'P', 'V',                 'P', '8', 'X',
        10,  0,   0,   0,   vp8x_flag_animation, 0,   0,   0,
        0,   0,   0,   0,   0,                   0,   'A', 'N',
        'M', 'F', 16,  0,   0,                   0,   0,   0,
        0,   0,   0,   0,   0,                   0,   0,   0,
        0,   0,   0,   0,   0,                   0,
    };
    try std.testing.expectError(error.WebpDecodeFailed, probe(&frame_without_anim));
}

test "decode rejects unsupported lossy vp8 frame flags" {
    try std.testing.expectError(error.UnsupportedWebpFormat, decodeRgba(std.testing.allocator, &webp_vp8_1x1));
}

test "decode minimal unfiltered vp8 keyframe to rgba" {
    const alloc = std.testing.allocator;
    const first_partition = @as([200]u8, @splat(0));
    const token_partition = @as([64]u8, @splat(0));
    const webp = try testBuildVp8Webp(alloc, 1, 1, &first_partition, &.{&token_partition});
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    const expected = vp8YuvToRgba(128, 128, 128, 255);
    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &expected, decoded.rgba);
}

test "decode vp8 with raw alph chunk composes alpha plane" {
    const alloc = std.testing.allocator;
    const first_partition = @as([200]u8, @splat(0));
    const token_partition = @as([64]u8, @splat(0));
    const alpha_payload = [_]u8{ 0, 0x7d };
    const webp = try testBuildVp8xAlphVp8Webp(alloc, &alpha_payload, 1, 1, &first_partition, &.{&token_partition});
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    var expected = vp8YuvToRgba(128, 128, 128, 255);
    expected[3] = 0x7d;
    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &expected, decoded.rgba);
}

test "decode raw alph plane with none and gradient filters" {
    const alloc = std.testing.allocator;

    const none = try decodeAlphPlane(alloc, &.{ 0, 1, 2, 3, 4 }, 2, 2);
    defer alloc.free(none);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3, 4 }, none);

    const gradient = try decodeAlphPlane(alloc, &.{ 3 << 2, 10, 10, 20, 5 }, 2, 2);
    defer alloc.free(gradient);
    try std.testing.expectEqualSlices(u8, &.{ 10, 20, 30, 45 }, gradient);
}

test "decode compressed alph plane uses vp8l green channel" {
    const alloc = std.testing.allocator;
    const payload = try testBuildCompressedAlphPayload(alloc, 3, 2, 0x7d);
    defer alloc.free(payload);

    const alpha = try decodeAlphPlane(alloc, payload, 3, 2);
    defer alloc.free(alpha);

    try std.testing.expectEqualSlices(u8, &.{ 0x7d, 0x7d, 0x7d, 0x7d, 0x7d, 0x7d }, alpha);
}

test "decode alph plane rejects preprocessing until dequantization lands" {
    try std.testing.expectError(error.UnsupportedWebpFormat, decodeAlphPlane(std.testing.allocator, &.{ 1 << 4, 0 }, 1, 1));
}

test "decode minimal vp8l literal image to rgba" {
    const alloc = std.testing.allocator;
    const webp = try testBuildLiteralVp8lWebp(alloc, 2, 3, .{ 0x44, 0x22, 0x11, 0xff });
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 2), decoded.width);
    try std.testing.expectEqual(@as(u32, 3), decoded.height);
    try std.testing.expectEqual(@as(usize, 2 * 3 * 4), decoded.rgba.len);
    for (0..6) |i| {
        const pixel = decoded.rgba[i * 4 ..][0..4];
        try std.testing.expectEqualSlices(u8, &.{ 0x44, 0x22, 0x11, 0xff }, pixel);
    }
}

test "decode limited rejects oversized webp before full decode" {
    const alloc = std.testing.allocator;
    const webp = try testBuildLiteralVp8lWebp(alloc, 2, 3, .{ 0x44, 0x22, 0x11, 0xff });
    defer alloc.free(webp);

    const limits = DecodeLimits{
        .max_pixels = 1,
        .max_rgba_bytes = 4,
    };
    try std.testing.expectError(error.ImageTooLarge, decodeRgbaLimited(alloc, webp, limits));
}

test "decode vp8l two-symbol simple prefix literal pixels" {
    const alloc = std.testing.allocator;
    const first = [4]u8{ 0x10, 0x20, 0x30, 0x40 };
    const second = [4]u8{ 0xa0, 0xb0, 0xc0, 0xd0 };
    const webp = try testBuildTwoPixelVp8lWebp(alloc, first, second);
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 2), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &first, decoded.rgba[0..4]);
    try std.testing.expectEqualSlices(u8, &second, decoded.rgba[4..8]);
}

test "decode vp8l normal prefix literal pixel" {
    const alloc = std.testing.allocator;
    const expected = [4]u8{ 0x23, 0x45, 0x67, 0x89 };
    const webp = try testBuildNormalPrefixVp8lWebp(alloc, expected);
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &expected, decoded.rgba);
}

test "decode vp8l normal prefix repeat-code lengths" {
    const alloc = std.testing.allocator;
    const expected = [4]u8{ 0x12, 0x35, 0x56, 0x78 };
    const webp = try testBuildNormalPrefixRepeatVp8lWebp(alloc, expected);
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &expected, decoded.rgba);
}

test "decode vp8l rejects duplicate simple prefix symbols" {
    const alloc = std.testing.allocator;
    var bits = TestBitWriter{};
    defer bits.deinit(alloc);

    try testWriteSimplePrefixPair(&bits, alloc, 7, 7);

    var reader = BitReader.init(bits.bytes.items);
    try std.testing.expectError(error.WebpDecodeFailed, PrefixCode.read(&reader, 256));
}

test "decode vp8l rejects repeat-code length before previous nonzero length" {
    const alloc = std.testing.allocator;
    var bits = TestBitWriter{};
    defer bits.deinit(alloc);

    try bits.writeBits(alloc, 0, 1);
    try bits.writeBits(alloc, 5, 4);
    for (0..8) |_| try bits.writeBits(alloc, 0, 3);
    try bits.writeBits(alloc, 1, 3);
    try bits.writeBits(alloc, 0, 1);
    for (0..42) |_| try bits.writeBits(alloc, 3, 2);
    try bits.writeBits(alloc, 1, 2);

    var reader = BitReader.init(bits.bytes.items);
    try std.testing.expectError(error.WebpDecodeFailed, PrefixCode.read(&reader, 256));
}

test "decode vp8l lz77 backward reference copies previous pixel" {
    const alloc = std.testing.allocator;
    const expected = [4]u8{ 0x32, 0x54, 0x76, 0x98 };
    const webp = try testBuildLz77Vp8lWebp(alloc, 2, 1, expected);
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 2), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &expected, decoded.rgba[0..4]);
    try std.testing.expectEqualSlices(u8, &expected, decoded.rgba[4..8]);
}

test "decode vp8l lz77 backward reference supports overlapping copy" {
    const alloc = std.testing.allocator;
    const expected = [4]u8{ 0xaa, 0xbb, 0xcc, 0xdd };
    const webp = try testBuildLz77Vp8lWebp(alloc, 4, 3, expected);
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 4), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    for (0..4) |i| {
        try std.testing.expectEqualSlices(u8, &expected, decoded.rgba[i * 4 ..][0..4]);
    }
}

test "decode vp8l color cache restores prior pixel" {
    const alloc = std.testing.allocator;
    const expected = [4]u8{ 0x13, 0x37, 0x59, 0x7b };
    const webp = try testBuildColorCacheVp8lWebp(alloc, expected);
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 2), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &expected, decoded.rgba[0..4]);
    try std.testing.expectEqualSlices(u8, &expected, decoded.rgba[4..8]);
}

test "decode vp8l subtract-green transform restores red and blue" {
    const alloc = std.testing.allocator;
    const expected = [4]u8{ 0x10, 0xf0, 0x30, 0x80 };
    const webp = try testBuildSubtractGreenVp8lWebp(alloc, expected);
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &expected, decoded.rgba);
}

test "decode vp8l predictor transform restores border and tile-predicted pixels" {
    const alloc = std.testing.allocator;
    const residual = [4]u8{ 5, 7, 9, 0 };
    const webp = try testBuildPredictorVp8lWebp(alloc, 3, 2, 1, residual);
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 3), decoded.width);
    try std.testing.expectEqual(@as(u32, 2), decoded.height);
    const expected = [_][4]u8{
        .{ 5, 7, 9, 255 },
        .{ 10, 14, 18, 255 },
        .{ 15, 21, 27, 255 },
        .{ 10, 14, 18, 255 },
        .{ 15, 21, 27, 255 },
        .{ 20, 28, 36, 255 },
    };
    for (expected, 0..) |pixel, i| {
        try std.testing.expectEqualSlices(u8, &pixel, decoded.rgba[i * 4 ..][0..4]);
    }
}

test "decode vp8l invalid predictor mode cleans transform allocations" {
    const alloc = std.testing.allocator;
    const residual = [4]u8{ 0, 0, 0, 0 };
    const webp = try testBuildPredictorVp8lWebp(alloc, 3, 2, 15, residual);
    defer alloc.free(webp);

    try std.testing.expectError(error.WebpDecodeFailed, decodeRgba(alloc, webp));
}

test "decode vp8l cross-color transform restores red and blue" {
    const alloc = std.testing.allocator;
    const webp = try testBuildCrossColorVp8lWebp(alloc, .{ 10, 20, 30, 255 }, .{ 32, 64, 32, 255 });
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 1), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    try std.testing.expectEqualSlices(u8, &.{ 30, 20, 100, 255 }, decoded.rgba);
}

test "decode vp8l color-indexing transform expands packed palette indices" {
    const alloc = std.testing.allocator;
    const color0 = [4]u8{ 0x10, 0x20, 0x30, 0x01 };
    const color1 = [4]u8{ 0xa0, 0xb0, 0xc0, 0xff };
    const webp = try testBuildColorIndexingVp8lWebp(alloc, color0, color1);
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 8), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    for (0..8) |i| {
        const expected = if ((i & 1) == 0) color0 else color1;
        try std.testing.expectEqualSlices(u8, &expected, decoded.rgba[i * 4 ..][0..4]);
    }
}

test "decode vp8l meta-prefix huffman groups select per tile" {
    const alloc = std.testing.allocator;
    const color0 = [4]u8{ 0x11, 0x22, 0x33, 0x44 };
    const color1 = [4]u8{ 0xaa, 0xbb, 0xcc, 0xdd };
    const webp = try testBuildMetaPrefixVp8lWebp(alloc, color0, color1);
    defer alloc.free(webp);

    const decoded = try decodeRgba(alloc, webp);
    defer alloc.free(decoded.rgba);

    try std.testing.expectEqual(@as(u32, 5), decoded.width);
    try std.testing.expectEqual(@as(u32, 1), decoded.height);
    for (0..5) |i| {
        const expected = if (i < 4) color0 else color1;
        try std.testing.expectEqualSlices(u8, &expected, decoded.rgba[i * 4 ..][0..4]);
    }
}

test "probe rejects reserved vp8x feature bits" {
    var bytes = webp_vp8x_animated_1x1;
    bytes[20] = 0x01;
    try std.testing.expectError(error.UnsupportedWebpFormat, probe(&bytes));
}
