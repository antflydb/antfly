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

#ifndef ANTFLY_APPLE_LOADER_H
#define ANTFLY_APPLE_LOADER_H
#include <stddef.h>
#include <stdint.h>

typedef int (*antfly_apple_cancel)(void *);
typedef int (*antfly_apple_output)(void *, const uint8_t *, size_t);
typedef int (*antfly_apple_invoke_fn)(int, const uint8_t *, size_t,
    const uint8_t *, size_t, size_t, void *, antfly_apple_cancel, antfly_apple_output);

// OS-only preflight: does not load Swift or check model/locale availability.
int antfly_apple_loader_available(void);

int antfly_apple_loader_invoke(int, const uint8_t *, size_t, const uint8_t *,
    size_t, size_t, void *, antfly_apple_cancel, antfly_apple_output);
#endif
