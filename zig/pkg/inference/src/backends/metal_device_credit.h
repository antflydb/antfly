// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

#ifndef ANTFLY_METAL_DEVICE_CREDIT_H
#define ANTFLY_METAL_DEVICE_CREDIT_H
#include <stddef.h>
#include <stdint.h>

typedef struct termite_metal_device_budget {
    void *context;
    void *identity;
    int (*retain_context)(void *context);
    void (*release_context)(void *context);
    int (*reserve)(void *context, size_t bytes, void **credit_out);
    void (*release_credit)(void *credit);
} termite_metal_device_budget;

#ifdef __OBJC__
#import <Metal/Metal.h>
id<MTLBuffer> termite_metal_device_budget_new_buffer(
    id<MTLDevice> device, size_t bytes, MTLResourceOptions options,
    const termite_metal_device_budget *budget);
#endif
void *termite_metal_device_credit_fixture_buffer(const termite_metal_device_budget *, size_t, int);
void termite_metal_device_credit_fixture_retain(void *);
void termite_metal_device_credit_fixture_release(void *);
size_t termite_metal_device_credit_fixture_allocations(void);
size_t termite_metal_device_credit_fixture_frees(void);
#endif
