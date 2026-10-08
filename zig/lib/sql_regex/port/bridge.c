/* Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0 */
#include "../vendor/regex/regguts.h"
#include "bridge.h"
/* A C call never yields. Restore the prior context for nested invocations;
 * compiled patterns retain no pointer to this thread-local operation state. */
#if defined(ANTFLY_REGEX_SINGLE_THREADED)
static struct antfly_regex_context *active;
#else
static _Thread_local struct antfly_regex_context *active;
#endif
void *antfly_regex_allocate(size_t n) { return active->allocate(active->user,n); }
void *antfly_regex_resize(void *p, size_t n) { return active->resize(active->user,p,n); }
void antfly_regex_release(void *p) { if (p) active->release(active->user,p); }
void *antfly_regex_array(size_t width, size_t count) {
    if (count && width > SIZE_MAX / count) return NULL;
    return antfly_regex_allocate(width * count);
}
void *antfly_regex_resize_array(void *p, size_t width, size_t count) {
    if (count && width > SIZE_MAX / count) return NULL;
    return antfly_regex_resize(p,width * count);
}
int antfly_regex_poll(void) { return active->poll(active->user); }
int antfly_regex_work(size_t amount) { return active->work(active->user,amount); }
void *antfly_regex_cached_class(unsigned code) { assert(code < 14); return active->classes[code]; }
void antfly_regex_cache_class(unsigned code, void *value) { assert(code < 14); active->classes[code] = value; }
int stack_is_too_deep(void) {
    char marker;
    uintptr_t here = (uintptr_t)&marker;
    uintptr_t distance = here < active->stack_base ? active->stack_base-here : here-active->stack_base;
    return distance > active->stack_limit || !antfly_regex_poll();
}
void pg_set_regex_collation(Oid id) { assert(id == 1); }
void *antfly_regex_compile(struct antfly_regex_context *ctx, const uint32_t *pattern, size_t len, int flags, int *status) {
    struct antfly_regex_context *prior = active;
    char marker;
    active = ctx;
    ctx->stack_base = (uintptr_t)&marker;
    regex_t *re = (regex_t *)MALLOC(sizeof(*re));
    if (!re) { *status = REG_ESPACE; active = prior; return NULL; }
    memset(re,0,sizeof(*re));
    *status = pg_regcomp(re,pattern,len,flags,1);
    /* Class vectors are compile scratch, never retained in a compiled NFA. */
    for (unsigned i = 0; i < 14; ++i) if (ctx->classes[i]) {
        struct cvec *cv = (struct cvec *)ctx->classes[i];
        FREE(cv->chrs);
        FREE(cv);
        ctx->classes[i] = NULL;
    }
    if (*status != REG_OKAY) { FREE(re); re = NULL; }
    active = prior;
    return re;
}
int antfly_regex_search(struct antfly_regex_context *ctx, void *pattern, const uint32_t *input, size_t len, size_t start, struct antfly_regex_match *matches, size_t count) {
    struct antfly_regex_context *prior = active;
    char marker;
    active = ctx;
    ctx->stack_base = (uintptr_t)&marker;
    int result = pg_regexec((regex_t *)pattern,input,len,start,NULL,count,(regmatch_t *)matches,0);
    active = prior;
    return result;
}
size_t antfly_regex_captures(void *pattern) { return ((regex_t *)pattern)->re_nsub; }
void antfly_regex_destroy(struct antfly_regex_context *ctx, void *pattern) {
    struct antfly_regex_context *prior = active;
    active = ctx;
    pg_regfree((regex_t *)pattern);
    FREE(pattern);
    active = prior;
}
