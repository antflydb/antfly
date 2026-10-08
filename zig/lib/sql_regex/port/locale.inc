/* Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0 */
/* C collation is explicit, independent of host libc locale. Subjects and
 * patterns remain Unicode codepoints: only locale classes/case are ASCII. */
static int pg_wc_isdigit(chr c) { return c >= '0' && c <= '9'; }
static int pg_wc_islower(chr c) { return c >= 'a' && c <= 'z'; }
static int pg_wc_isupper(chr c) { return c >= 'A' && c <= 'Z'; }
static int pg_wc_isalpha(chr c) { return pg_wc_islower(c) || pg_wc_isupper(c); }
static int pg_wc_isalnum(chr c) { return pg_wc_isalpha(c) || pg_wc_isdigit(c); }
static int pg_wc_isword(chr c) { return pg_wc_isalnum(c) || c == '_'; }
static int pg_wc_isspace(chr c) { return c == ' ' || (c >= '\t' && c <= '\r'); }
static int pg_wc_isprint(chr c) { return c >= 32 && c < 127; }
static int pg_wc_isgraph(chr c) { return c > 32 && c < 127; }
static int pg_wc_ispunct(chr c) { return pg_wc_isgraph(c) && !pg_wc_isalnum(c); }
static chr pg_wc_tolower(chr c) { return pg_wc_isupper(c) ? c + ('a'-'A') : c; }
static chr pg_wc_toupper(chr c) { return pg_wc_islower(c) ? c - ('a'-'A') : c; }
static int pg_char_and_wchar_strncmp(const char *a, const chr *b, size_t n) {
    for (size_t i = 0; i < n; ++i) if ((unsigned char)a[i] != b[i]) return (unsigned char)a[i] < b[i] ? -1 : 1;
    return 0;
}
static struct cvec *pg_ctype_get_cache(int (*probe)(chr), int code) {
    struct cvec *cached = (struct cvec *)antfly_regex_cached_class((unsigned)code);
    if (cached) return cached;
    struct cvec *cv = (struct cvec *)MALLOC(sizeof(*cv));
    if (!cv) return NULL;
    memset(cv, 0, sizeof(*cv));
    cv->chrs = (chr *)MALLOC(128 * sizeof(chr));
    if (!cv->chrs) { FREE(cv); return NULL; }
    cv->chrspace = 128;
    cv->cclasscode = -1; /* C classes have no high-codepoint runtime probes. */
    for (chr c = 0; c < 128; ++c) if (probe(c)) cv->chrs[cv->nchrs++] = c;
    antfly_regex_cache_class((unsigned)code,cv);
    return cv;
}
