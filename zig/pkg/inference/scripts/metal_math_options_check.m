// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#import <Foundation/Foundation.h>
#import <Metal/Metal.h>
#include "../src/backends/metal_math_options.h"

// This checks the framework's actual compatibility behavior without a GPU.
// Keep the deprecated setter only in the independent reference.
int main(void) {
    @autoreleasepool {
        if (@available(macOS 15.0, *)) {
            MTLCompileOptions *legacy = [MTLCompileOptions new];
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
            legacy.fastMathEnabled = NO;
#pragma clang diagnostic pop
            MTLCompileOptions *candidate = [MTLCompileOptions new];
            termite_metal_disable_fast_math(candidate);
            if (legacy.mathMode != candidate.mathMode ||
                legacy.mathFloatingPointFunctions != candidate.mathFloatingPointFunctions) {
                fprintf(stderr, "Metal precise math options differ from the legacy contract\n");
                return 1;
            }
            puts("Metal precise math options preserve the legacy compiler and function modes");
            return 0;
        }
        return 1;
    }
}
