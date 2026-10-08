/* Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0 */
#ifndef ANTFLY_SQL_REGEX_CUSTOM_H
#define ANTFLY_SQL_REGEX_CUSTOM_H
#define ANTFLY_SQL_REGEX 1
#define pg_regcomp antfly_pg_regcomp
#define pg_regexec antfly_pg_regexec
#define pg_regfree antfly_pg_regfree
#define pg_reg_getcolor antfly_pg_reg_getcolor
#define pg_set_regex_collation antfly_pg_set_regex_collation
#define stack_is_too_deep antfly_pg_stack_is_too_deep
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

typedef uint32_t pg_wchar;
typedef uint32_t Oid;
#define FLEXIBLE_ARRAY_MEMBER
typedef pg_wchar chr;
typedef unsigned uchr;
void *antfly_regex_allocate(size_t n);
void *antfly_regex_resize(void *p, size_t n);
void antfly_regex_release(void *p);
void *antfly_regex_array(size_t width, size_t count);
int antfly_regex_poll(void);
int stack_is_too_deep(void);
void pg_set_regex_collation(Oid id);
void *antfly_regex_cached_class(unsigned code);
void antfly_regex_cache_class(unsigned code, void *value);
#define MALLOC(n) antfly_regex_allocate(n)
#define FREE(p) antfly_regex_release((void *)(p))
#define REALLOC(p,n) antfly_regex_resize((void *)(p),n)
#define MALLOC_ARRAY(type,n) ((type *)antfly_regex_array(sizeof(type),n))
#define REALLOC_ARRAY(p,type,n) ((type *)antfly_regex_resize_array((void *)(p),sizeof(type),n))
void *antfly_regex_resize_array(void *p, size_t width, size_t count);
/* Runtime interrupt sites return either int or a pointer. The
 * wrapper additionally checks the sticky caller error, never exposing a match
 * if an interrupted subroutine returned zero during unwinding. */
#define INTERRUPT(re) do { if (!antfly_regex_poll()) { v->err = REG_ETOOBIG; return 0; } } while (0)
#define NFA_INTERRUPT(nfa,result) do { if (!antfly_regex_poll()) { (nfa)->v->err = REG_ETOOBIG; return result; } } while (0)
#define CHR(c) ((unsigned char)(c))
#define DIGITVAL(c) ((c)-'0')
#define CHRBITS 32
#define CHR_MIN 0x00000000
#define CHR_MAX 0x7ffffffe
#define CHR_IS_IN_RANGE(c) ((c) <= CHR_MAX)
#define MAX_SIMPLE_CHR 0x7ff
#define iscalnum(x) pg_wc_isalnum(x)
#define iscalpha(x) pg_wc_isalpha(x)
#define iscdigit(x) pg_wc_isdigit(x)
#define iscspace(x) pg_wc_isspace(x)
#include "../vendor/regex/regex.h"
#endif
