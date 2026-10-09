// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#import <Metal/Metal.h>
void *av_benchmark_device_create(void) { return (__bridge_retained void *)MTLCreateSystemDefaultDevice(); }
void av_benchmark_device_destroy(void *p) { if (p) { id object = (__bridge_transfer id)p; (void)object; } }
