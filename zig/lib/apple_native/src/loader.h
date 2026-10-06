// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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
