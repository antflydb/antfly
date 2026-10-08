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
#include "antfly.h"
#include <assert.h>

#if defined(__APPLE__) && defined(__aarch64__)
// Match aws-lc's direct local relocation, rather than a GOT-indirect reference.
extern void *consumer_dso_address(void);
__asm__(".text\n.globl _consumer_dso_address\n.p2align 2\n"
        "_consumer_dso_address:\n"
        "adrp x0, ___dso_handle@PAGE\n"
        "add x0, x0, ___dso_handle@PAGEOFF\nret\n");
#elif defined(__APPLE__) && defined(__x86_64__)
extern void *consumer_dso_address(void);
__asm__(".text\n.globl _consumer_dso_address\n"
        "_consumer_dso_address:\nleaq ___dso_handle(%rip), %rax\nret\n");
#else
extern void *__dso_handle;
static void *consumer_dso_address(void) { return &__dso_handle; }
#endif

static void *constructor_handle;
__attribute__((constructor)) static void init(void) {
    constructor_handle = consumer_dso_address();
}

int consumer_check(void) {
    antfly_open_options options;
    assert(constructor_handle != 0);
    assert(constructor_handle == consumer_dso_address());
    assert(antfly_abi_version() != 0);
    assert(antfly_open_options_init(&options) == ANTFLY_OK);
    return 0;
}

#ifndef CONSUMER_SHARED
int main(void) { return consumer_check(); }
#endif
