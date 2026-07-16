#define _POSIX_C_SOURCE 200809L

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <openssl/crypto.h>
#include <openssl/ssl.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

enum {
    ANTFLY_OPENSSL_OK = 0,
    ANTFLY_OPENSSL_INVALID_REQUEST = 1,
    ANTFLY_OPENSSL_RESOLVE_FAILED = 2,
    ANTFLY_OPENSSL_CONNECT_FAILED = 3,
    ANTFLY_OPENSSL_TIMEOUT = 4,
    ANTFLY_OPENSSL_TLS_INIT_FAILED = 5,
    ANTFLY_OPENSSL_TLS_VERIFY_FAILED = 6,
    ANTFLY_OPENSSL_WRITE_FAILED = 7,
    ANTFLY_OPENSSL_READ_FAILED = 8,
    ANTFLY_OPENSSL_RESPONSE_TOO_LARGE = 9,
    ANTFLY_OPENSSL_INVALID_RESPONSE = 10,
    ANTFLY_OPENSSL_OUT_OF_MEMORY = 11,
};

enum { HEADER_CAP = 16 * 1024, MAX_BODY_CAP = 16 * 1024 * 1024 };

static uint64_t monotonic_ms(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) return 0;
    return (uint64_t)now.tv_sec * 1000u + (uint64_t)now.tv_nsec / 1000000u;
}

static int wait_fd(int fd, short events, uint64_t deadline_ms) {
    uint64_t now = monotonic_ms();
    if (now == 0 || now >= deadline_ms) return 0;
    uint64_t remaining = deadline_ms - now;
    int timeout = remaining > INT_MAX ? INT_MAX : (int)remaining;
    struct pollfd pfd = {.fd = fd, .events = events};
    for (;;) {
        int rc = poll(&pfd, 1, timeout);
        if (rc >= 0) return rc;
        if (errno != EINTR) return -1;
        now = monotonic_ms();
        if (now == 0 || now >= deadline_ms) return 0;
        remaining = deadline_ms - now;
        timeout = remaining > INT_MAX ? INT_MAX : (int)remaining;
    }
}

static int connect_with_deadline(const char *host, uint16_t port, uint64_t deadline_ms, int *out_fd) {
    char port_text[6];
    if (snprintf(port_text, sizeof(port_text), "%u", (unsigned)port) <= 0) return ANTFLY_OPENSSL_INVALID_REQUEST;
    struct addrinfo hints = {0};
    struct addrinfo *results = NULL;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    int gai = getaddrinfo(host, port_text, &hints, &results);
    if (gai != 0) return ANTFLY_OPENSSL_RESOLVE_FAILED;

    int result = ANTFLY_OPENSSL_CONNECT_FAILED;
    for (struct addrinfo *it = results; it != NULL; it = it->ai_next) {
        int fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (fd < 0) continue;
        int flags = fcntl(fd, F_GETFL, 0);
        if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
            close(fd);
            continue;
        }
        int rc = connect(fd, it->ai_addr, it->ai_addrlen);
        if (rc != 0 && errno != EINPROGRESS) {
            close(fd);
            continue;
        }
        if (rc != 0) {
            rc = wait_fd(fd, POLLOUT, deadline_ms);
            if (rc == 0) {
                close(fd);
                result = ANTFLY_OPENSSL_TIMEOUT;
                break;
            }
            if (rc < 0) {
                close(fd);
                continue;
            }
            int socket_error = 0;
            socklen_t socket_error_len = sizeof(socket_error);
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &socket_error_len) != 0 || socket_error != 0) {
                close(fd);
                continue;
            }
        }
        *out_fd = fd;
        result = ANTFLY_OPENSSL_OK;
        break;
    }
    freeaddrinfo(results);
    return result;
}

static int ssl_wait(SSL *ssl, int fd, int rc, uint64_t deadline_ms, int fallback_error) {
    int ssl_error = SSL_get_error(ssl, rc);
    short events;
    if (ssl_error == SSL_ERROR_WANT_READ) {
        events = POLLIN;
    } else if (ssl_error == SSL_ERROR_WANT_WRITE) {
        events = POLLOUT;
    } else {
        return fallback_error;
    }
    int ready = wait_fd(fd, events, deadline_ms);
    if (ready == 0) return ANTFLY_OPENSSL_TIMEOUT;
    if (ready < 0) return fallback_error;
    return ANTFLY_OPENSSL_OK;
}

static int ssl_connect_with_deadline(SSL *ssl, int fd, uint64_t deadline_ms) {
    for (;;) {
        int rc = SSL_connect(ssl);
        if (rc == 1) return ANTFLY_OPENSSL_OK;
        int wait_result = ssl_wait(ssl, fd, rc, deadline_ms, ANTFLY_OPENSSL_TLS_INIT_FAILED);
        if (wait_result != ANTFLY_OPENSSL_OK) return wait_result;
    }
}

static int ssl_write_all(SSL *ssl, int fd, const unsigned char *data, size_t len, uint64_t deadline_ms) {
    size_t offset = 0;
    while (offset < len) {
        size_t written = 0;
        int rc = SSL_write_ex(ssl, data + offset, len - offset, &written);
        if (rc == 1) {
            if (written == 0) return ANTFLY_OPENSSL_WRITE_FAILED;
            offset += written;
            continue;
        }
        int wait_result = ssl_wait(ssl, fd, rc, deadline_ms, ANTFLY_OPENSSL_WRITE_FAILED);
        if (wait_result != ANTFLY_OPENSSL_OK) return wait_result;
    }
    return ANTFLY_OPENSSL_OK;
}

static const unsigned char *find_bytes(const unsigned char *haystack, size_t haystack_len, const char *needle, size_t needle_len) {
    if (needle_len == 0 || haystack_len < needle_len) return NULL;
    for (size_t i = 0; i <= haystack_len - needle_len; i++) {
        if (memcmp(haystack + i, needle, needle_len) == 0) return haystack + i;
    }
    return NULL;
}

static int ascii_equal_ignore_case(const unsigned char *left, size_t left_len, const char *right) {
    size_t right_len = strlen(right);
    if (left_len != right_len) return 0;
    for (size_t i = 0; i < left_len; i++) {
        if (tolower(left[i]) != tolower((unsigned char)right[i])) return 0;
    }
    return 1;
}

static int value_contains_chunked(const unsigned char *value, size_t len) {
    const char *word = "chunked";
    const size_t word_len = 7;
    if (len < word_len) return 0;
    for (size_t i = 0; i <= len - word_len; i++) {
        if (ascii_equal_ignore_case(value + i, word_len, word)) return 1;
    }
    return 0;
}

static int parse_hex_size(const unsigned char *text, size_t len, size_t *value) {
    size_t result = 0;
    size_t digits = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char ch = text[i];
        if (ch == ';' || ch == ' ' || ch == '\t') break;
        unsigned digit;
        if (ch >= '0' && ch <= '9') digit = ch - '0';
        else if (ch >= 'a' && ch <= 'f') digit = ch - 'a' + 10;
        else if (ch >= 'A' && ch <= 'F') digit = ch - 'A' + 10;
        else return 0;
        if (result > (SIZE_MAX - digit) / 16) return 0;
        result = result * 16 + digit;
        digits++;
    }
    if (digits == 0) return 0;
    *value = result;
    return 1;
}

static int decode_response(
    unsigned char *response,
    size_t response_len,
    size_t max_body,
    uint16_t *out_status,
    unsigned char **out_body,
    size_t *out_body_len
) {
    const unsigned char *first_line_end = find_bytes(response, response_len, "\r\n", 2);
    const unsigned char *headers_end = find_bytes(response, response_len, "\r\n\r\n", 4);
    if (first_line_end == NULL || headers_end == NULL || first_line_end >= headers_end) return ANTFLY_OPENSSL_INVALID_RESPONSE;
    unsigned status = 0;
    if (sscanf((char *)response, "HTTP/%*s %u", &status) != 1 || status > UINT16_MAX) return ANTFLY_OPENSSL_INVALID_RESPONSE;
    *out_status = (uint16_t)status;

    int chunked = 0;
    int has_content_length = 0;
    size_t content_length = 0;
    const unsigned char *line = first_line_end + 2;
    while (line < headers_end) {
        const unsigned char *line_end = find_bytes(line, (size_t)(headers_end + 2 - line), "\r\n", 2);
        if (line_end == NULL) return ANTFLY_OPENSSL_INVALID_RESPONSE;
        const unsigned char *colon = memchr(line, ':', (size_t)(line_end - line));
        if (colon == NULL) return ANTFLY_OPENSSL_INVALID_RESPONSE;
        const unsigned char *value = colon + 1;
        while (value < line_end && (*value == ' ' || *value == '\t')) value++;
        const unsigned char *value_end = line_end;
        while (value_end > value && (value_end[-1] == ' ' || value_end[-1] == '\t')) value_end--;
        if (ascii_equal_ignore_case(line, (size_t)(colon - line), "transfer-encoding")) {
            chunked = value_contains_chunked(value, (size_t)(value_end - value));
        } else if (ascii_equal_ignore_case(line, (size_t)(colon - line), "content-length")) {
            size_t parsed = 0;
            if (value == value_end) return ANTFLY_OPENSSL_INVALID_RESPONSE;
            for (const unsigned char *it = value; it < value_end; it++) {
                if (*it < '0' || *it > '9') return ANTFLY_OPENSSL_INVALID_RESPONSE;
                unsigned digit = *it - '0';
                if (parsed > (SIZE_MAX - digit) / 10) return ANTFLY_OPENSSL_INVALID_RESPONSE;
                parsed = parsed * 10 + digit;
            }
            content_length = parsed;
            has_content_length = 1;
        }
        line = line_end + 2;
    }

    unsigned char *body_start = (unsigned char *)headers_end + 4;
    size_t body_available = response_len - (size_t)(body_start - response);
    if (chunked) {
        unsigned char *decoded = malloc(max_body == 0 ? 1 : max_body);
        if (decoded == NULL) return ANTFLY_OPENSSL_OUT_OF_MEMORY;
        size_t decoded_len = 0;
        unsigned char *cursor = body_start;
        unsigned char *response_end = response + response_len;
        for (;;) {
            const unsigned char *size_end = find_bytes(cursor, (size_t)(response_end - cursor), "\r\n", 2);
            if (size_end == NULL) {
                free(decoded);
                return ANTFLY_OPENSSL_INVALID_RESPONSE;
            }
            size_t chunk_size = 0;
            if (!parse_hex_size(cursor, (size_t)(size_end - cursor), &chunk_size)) {
                free(decoded);
                return ANTFLY_OPENSSL_INVALID_RESPONSE;
            }
            cursor = (unsigned char *)size_end + 2;
            if (chunk_size == 0) break;
            if (chunk_size > max_body - decoded_len || (size_t)(response_end - cursor) < chunk_size + 2) {
                free(decoded);
                return chunk_size > max_body - decoded_len ? ANTFLY_OPENSSL_RESPONSE_TOO_LARGE : ANTFLY_OPENSSL_INVALID_RESPONSE;
            }
            memcpy(decoded + decoded_len, cursor, chunk_size);
            decoded_len += chunk_size;
            cursor += chunk_size;
            if (cursor[0] != '\r' || cursor[1] != '\n') {
                free(decoded);
                return ANTFLY_OPENSSL_INVALID_RESPONSE;
            }
            cursor += 2;
        }
        *out_body = decoded;
        *out_body_len = decoded_len;
        return ANTFLY_OPENSSL_OK;
    }

    size_t body_len = has_content_length ? content_length : body_available;
    if (body_len > max_body) return ANTFLY_OPENSSL_RESPONSE_TOO_LARGE;
    if (body_available < body_len) return ANTFLY_OPENSSL_INVALID_RESPONSE;
    unsigned char *body = malloc(body_len == 0 ? 1 : body_len);
    if (body == NULL) return ANTFLY_OPENSSL_OUT_OF_MEMORY;
    if (body_len > 0) memcpy(body, body_start, body_len);
    *out_body = body;
    *out_body_len = body_len;
    return ANTFLY_OPENSSL_OK;
}

int antfly_openssl_lease_get(
    const char *host,
    uint16_t port,
    const char *ca_path,
    const char *authorization,
    const char *path,
    uint32_t timeout_ms,
    size_t max_body,
    uint16_t *out_status,
    unsigned char **out_body,
    size_t *out_body_len
) {
    if (host == NULL || host[0] == '\0' || port == 0 || ca_path == NULL || ca_path[0] == '\0' ||
        authorization == NULL || authorization[0] == '\0' || path == NULL || path[0] != '/' ||
        timeout_ms == 0 || max_body == 0 || max_body > MAX_BODY_CAP || out_status == NULL ||
        out_body == NULL || out_body_len == NULL) {
        return ANTFLY_OPENSSL_INVALID_REQUEST;
    }
    *out_body = NULL;
    *out_body_len = 0;
    uint64_t started = monotonic_ms();
    if (started == 0 || started > UINT64_MAX - timeout_ms) return ANTFLY_OPENSSL_INVALID_REQUEST;
    uint64_t deadline_ms = started + timeout_ms;

    int fd = -1;
    SSL_CTX *ctx = NULL;
    SSL *ssl = NULL;
    unsigned char *request = NULL;
    size_t request_cap = 0;
    unsigned char *response = NULL;
    int result = connect_with_deadline(host, port, deadline_ms, &fd);
    if (result != ANTFLY_OPENSSL_OK) goto done;

    ctx = SSL_CTX_new(TLS_client_method());
    if (ctx == NULL) {
        result = ANTFLY_OPENSSL_TLS_INIT_FAILED;
        goto done;
    }
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);
    if (SSL_CTX_load_verify_locations(ctx, ca_path, NULL) != 1) {
        result = ANTFLY_OPENSSL_TLS_INIT_FAILED;
        goto done;
    }
    ssl = SSL_new(ctx);
    if (ssl == NULL || SSL_set_tlsext_host_name(ssl, host) != 1 || SSL_set1_host(ssl, host) != 1 || SSL_set_fd(ssl, fd) != 1) {
        result = ANTFLY_OPENSSL_TLS_INIT_FAILED;
        goto done;
    }
    result = ssl_connect_with_deadline(ssl, fd, deadline_ms);
    if (result != ANTFLY_OPENSSL_OK) {
        if (result == ANTFLY_OPENSSL_TLS_INIT_FAILED && SSL_get_verify_result(ssl) != X509_V_OK) {
            result = ANTFLY_OPENSSL_TLS_VERIFY_FAILED;
        }
        goto done;
    }
    if (SSL_get_verify_result(ssl) != X509_V_OK) {
        result = ANTFLY_OPENSSL_TLS_VERIFY_FAILED;
        goto done;
    }

    request_cap = strlen(path) + strlen(host) + strlen(authorization) + 256;
    request = malloc(request_cap);
    if (request == NULL) {
        result = ANTFLY_OPENSSL_OUT_OF_MEMORY;
        goto done;
    }
    int request_len = snprintf((char *)request, request_cap,
        "GET %s HTTP/1.1\r\nHost: %s\r\nAuthorization: %s\r\nAccept: application/json\r\nConnection: close\r\n\r\n",
        path, host, authorization);
    if (request_len < 0 || (size_t)request_len >= request_cap) {
        result = ANTFLY_OPENSSL_INVALID_REQUEST;
        goto done;
    }
    result = ssl_write_all(ssl, fd, request, (size_t)request_len, deadline_ms);
    OPENSSL_cleanse(request, request_cap);
    free(request);
    request = NULL;
    if (result != ANTFLY_OPENSSL_OK) goto done;

    if (max_body > (SIZE_MAX - HEADER_CAP - 1) / 2) {
        result = ANTFLY_OPENSSL_INVALID_REQUEST;
        goto done;
    }
    size_t response_cap = max_body * 2 + HEADER_CAP + 1;
    response = malloc(response_cap);
    if (response == NULL) {
        result = ANTFLY_OPENSSL_OUT_OF_MEMORY;
        goto done;
    }
    size_t response_len = 0;
    for (;;) {
        if (response_len == response_cap - 1) {
            result = ANTFLY_OPENSSL_RESPONSE_TOO_LARGE;
            goto done;
        }
        size_t got = 0;
        int rc = SSL_read_ex(ssl, response + response_len, response_cap - 1 - response_len, &got);
        response_len += got;
        if (find_bytes(response, response_len, "\r\n\r\n", 4) == NULL && response_len > HEADER_CAP) {
            result = ANTFLY_OPENSSL_INVALID_RESPONSE;
            goto done;
        }
        if (rc == 1) continue;
        int ssl_error = SSL_get_error(ssl, rc);
        if (ssl_error == SSL_ERROR_ZERO_RETURN || (ssl_error == SSL_ERROR_SYSCALL && got == 0 && errno == 0)) break;
        result = ssl_wait(ssl, fd, rc, deadline_ms, ANTFLY_OPENSSL_READ_FAILED);
        if (result != ANTFLY_OPENSSL_OK) goto done;
    }
    response[response_len] = '\0';
    result = decode_response(response, response_len, max_body, out_status, out_body, out_body_len);

done:
    if (request != NULL) {
        OPENSSL_cleanse(request, request_cap);
        free(request);
    }
    if (response != NULL) free(response);
    if (ssl != NULL) {
        SSL_shutdown(ssl);
        SSL_free(ssl);
    }
    if (ctx != NULL) SSL_CTX_free(ctx);
    if (fd >= 0) close(fd);
    return result;
}

void antfly_openssl_lease_free(void *ptr) {
    free(ptr);
}
