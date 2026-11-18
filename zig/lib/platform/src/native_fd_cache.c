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

#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>

/* Keep the descriptor cache anchor in one native object per linked image.
 * The server links several Zig archives that each compile storage_io.zig. */
__attribute__((visibility("hidden")))
void *antfly_platform_native_fd_cache(uint64_t expected_layout, void *(*init)(void)) {
    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    static _Atomic(void *) cache;
    static uint64_t layout;

    void *result = atomic_load_explicit(&cache, memory_order_acquire);
    if (result != NULL) {
        return layout == expected_layout ? result : NULL;
    }

    pthread_mutex_lock(&mutex);
    result = atomic_load_explicit(&cache, memory_order_relaxed);
    if (result == NULL) {
        result = init();
        layout = expected_layout;
        atomic_store_explicit(&cache, result, memory_order_release);
    }
    pthread_mutex_unlock(&mutex);
    return layout == expected_layout ? result : NULL;
}
