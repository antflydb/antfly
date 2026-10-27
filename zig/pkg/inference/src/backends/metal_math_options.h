// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

// Preserve both effects of fastMathEnabled = NO. mathMode alone leaves the
// default single-precision functions in metal::fast on current Metal runtimes.
static inline void termite_metal_disable_fast_math(MTLCompileOptions *options) {
    options.mathMode = MTLMathModeSafe;
    options.mathFloatingPointFunctions = MTLMathFloatingPointFunctionsPrecise;
}
