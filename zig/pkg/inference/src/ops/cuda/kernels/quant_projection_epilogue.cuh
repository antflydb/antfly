// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native-scope phase; intentionally no include guard. Caller retains storage and schedule.
                if (MODE == 1u || MODE == 2u || MODE == 3u || MODE == 4u) y += bias[col];
                if (MODE == 2u) y = 0.5f * y * (1.0f + tanhf(0.7978845608028654f * (y + 0.044715f * y * y * y)));
                if (MODE == 3u) y += residual[idx];
                if (MODE == 4u && y < 0.0f) y = 0.0f;
                dst[idx] = y;
