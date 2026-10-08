/* Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0 */
#ifndef ANTFLY_SQL_REGEX_BRIDGE_H
#define ANTFLY_SQL_REGEX_BRIDGE_H
#include <stddef.h>
#include <stdint.h>
struct antfly_regex_context {
    void *user;
    void *(*allocate)(void *, size_t);
    void *(*resize)(void *, void *, size_t);
    void (*release)(void *, void *);
    int (*poll)(void *);
    int (*work)(void *, size_t);
    uintptr_t stack_base;
    size_t stack_limit;
    void *classes[14];
};
struct antfly_regex_match { long start; long end; };
void *antfly_regex_compile(struct antfly_regex_context *, const uint32_t *, size_t, int, int *);
int antfly_regex_search(struct antfly_regex_context *, void *, const uint32_t *, size_t, size_t, struct antfly_regex_match *, size_t);
size_t antfly_regex_captures(void *);
void antfly_regex_destroy(struct antfly_regex_context *, void *);
#endif
