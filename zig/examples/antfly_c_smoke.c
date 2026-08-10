// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

#include "antfly.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static antfly_slice slice_from_cstr(const char *value) {
    antfly_slice slice;
    slice.ptr = (const uint8_t *)value;
    slice.len = strlen(value);
    return slice;
}

static bool buffer_contains(antfly_buffer buffer, const char *needle) {
    const size_t needle_len = strlen(needle);
    if (needle_len == 0) {
        return true;
    }
    if (buffer.len < needle_len) {
        return false;
    }

    for (size_t i = 0; i <= buffer.len - needle_len; i += 1) {
        if (memcmp(buffer.ptr + i, needle, needle_len) == 0) {
            return true;
        }
    }
    return false;
}

static int expect_ok(antfly_error_code code, const char *operation) {
    if (code == ANTFLY_OK) {
        return 0;
    }
    fprintf(
        stderr,
        "%s failed: %s (%s)\n",
        operation,
        antfly_error_code_name(code),
        antfly_error_code_description(code)
    );
    return 1;
}

static int fail_with_buffer(const char *message, antfly_buffer *buffer) {
    fprintf(stderr, "%s\n", message);
    antfly_buffer_free(buffer);
    return 1;
}

int main(void) {
    const char *path = "/tmp/antfly-c-smoke.aflite";
    const char *restored_path = "/tmp/antfly-c-smoke-restored.aflite";
    const char *snapshot_path = "/tmp/antfly-c-smoke-snapshot.aflite";
    const char *missing_path = "/tmp/antfly-c-smoke-missing.aflite";
    const char *bad_path = "/tmp/antfly-c-smoke-bad.aflite";
    (void)remove(path);
    (void)remove(restored_path);
    (void)remove(snapshot_path);
    (void)remove(missing_path);
    (void)remove(bad_path);

    if (antfly_abi_version() != 1) {
        fprintf(stderr, "unexpected Antfly ABI version: %u\n", antfly_abi_version());
        return 1;
    }
    if (antfly_lite_open_options_size() != sizeof(antfly_lite_open_options)) {
        fprintf(
            stderr,
            "Lite open options size mismatch: runtime=%u header=%zu\n",
            antfly_lite_open_options_size(),
            sizeof(antfly_lite_open_options)
        );
        return 1;
    }

    antfly_lite_open_options options;
    if (expect_ok(antfly_lite_open_options_init(&options), "initialize open options") != 0) {
        return 1;
    }
    if (options.abi_size != sizeof(antfly_lite_open_options)) {
        fprintf(stderr, "initialized open options ABI size did not match public header\n");
        return 1;
    }
    options.open_mode = ANTFLY_LITE_OPEN_MODE_WRITER;
    options.profile = ANTFLY_LITE_PROFILE_NATIVE;
    options.flags = ANTFLY_LITE_OPEN_FLAG_NO_SYNC;

    int missing_sentinel = 0;
    void *missing_handle = &missing_sentinel;
    antfly_error_code missing_code = antfly_lite_open(missing_path, &missing_handle);
    if (missing_code != ANTFLY_NOT_FOUND || missing_handle != NULL) {
        fprintf(
            stderr,
            "missing lite open returned %s with handle=%p, expected ANTFLY_NOT_FOUND and null handle\n",
            antfly_error_code_name(missing_code),
            missing_handle
        );
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(missing_path);
        (void)remove(bad_path);
        return 1;
    }
    FILE *unexpected_missing_file = fopen(missing_path, "rb");
    if (unexpected_missing_file != NULL) {
        fclose(unexpected_missing_file);
        fprintf(stderr, "missing lite open created a database file unexpectedly\n");
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(missing_path);
        (void)remove(bad_path);
        return 1;
    }

    const char *bad_body = "short native lite header";
    FILE *bad_file = fopen(bad_path, "wb");
    if (bad_file == NULL) {
        fprintf(stderr, "failed to create truncated lite file\n");
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(missing_path);
        (void)remove(bad_path);
        return 1;
    }
    if (fwrite(bad_body, 1, strlen(bad_body), bad_file) != strlen(bad_body)) {
        fprintf(stderr, "failed to write truncated lite file\n");
        fclose(bad_file);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(missing_path);
        (void)remove(bad_path);
        return 1;
    }
    if (fclose(bad_file) != 0) {
        fprintf(stderr, "failed to close truncated lite file\n");
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(missing_path);
        (void)remove(bad_path);
        return 1;
    }

    antfly_buffer check_file = {0};
    if (expect_ok(antfly_lite_check_file_json(bad_path, &check_file), "check invalid lite file") != 0) {
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(missing_path);
        (void)remove(bad_path);
        return 1;
    }
    if (!buffer_contains(check_file, "\"valid\":false") ||
        !buffer_contains(check_file, "\"issue\":\"truncated_header\"")) {
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(missing_path);
        (void)remove(bad_path);
        return fail_with_buffer("file-level check did not report truncated lite file", &check_file);
    }
    antfly_buffer_free(&check_file);
    (void)remove(bad_path);

    void *handle = NULL;
    if (expect_ok(antfly_lite_create_with_options(path, &options, &handle), "create lite database") != 0) {
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(missing_path);
        (void)remove(bad_path);
        return 1;
    }

    antfly_write_intent write;
    write.key = slice_from_cstr("doc:c-smoke");
    write.value = slice_from_cstr("{\"title\":\"c api lite\"}");
    write.is_delete = false;

    if (expect_ok(antfly_db_batch(handle, &write, 1, NULL, 0, 1, 0), "write batch") != 0) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(bad_path);
        return 1;
    }

    antfly_buffer exported_backup = {0};
    if (expect_ok(antfly_lite_export(handle, &exported_backup), "export lite database") != 0) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(bad_path);
        return 1;
    }
    if (exported_backup.len == 0) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(bad_path);
        return fail_with_buffer("export alias returned an empty portable backup", &exported_backup);
    }

    antfly_slice exported_slice;
    exported_slice.ptr = exported_backup.ptr;
    exported_slice.len = exported_backup.len;
    antfly_buffer restore_report = {0};
    if (expect_ok(antfly_lite_restore_json(restored_path, exported_slice, false, &restore_report), "restore lite database") != 0) {
        antfly_buffer_free(&exported_backup);
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(bad_path);
        return 1;
    }
    if (!buffer_contains(restore_report, "aflite")) {
        antfly_buffer_free(&exported_backup);
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(bad_path);
        return fail_with_buffer("restore alias did not return an aflite report", &restore_report);
    }
    antfly_buffer_free(&restore_report);
    antfly_buffer_free(&exported_backup);

    antfly_buffer lookup = {0};
    if (expect_ok(antfly_db_lookup_json(handle, write.key, &lookup), "lookup json") != 0) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(bad_path);
        return 1;
    }
    if (!buffer_contains(lookup, "c api lite")) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(bad_path);
        return fail_with_buffer("lookup json did not contain expected value", &lookup);
    }
    antfly_buffer_free(&lookup);

    antfly_buffer snapshot_report = {0};
    if (expect_ok(
            antfly_lite_copy_stable_snapshot_file_json(path, snapshot_path, false, &snapshot_report),
            "copy stable lite snapshot file"
        ) != 0) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(bad_path);
        return 1;
    }
    if (!buffer_contains(snapshot_report, "snapshot_size") ||
        !buffer_contains(snapshot_report, "checkpoint_sequence")) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(bad_path);
        return fail_with_buffer("path-level snapshot report did not include expected fields", &snapshot_report);
    }
    antfly_buffer_free(&snapshot_report);

    antfly_buffer overwrite_report = {0};
    antfly_error_code overwrite_code =
        antfly_lite_copy_stable_snapshot_file_json(path, snapshot_path, false, &overwrite_report);
    if (overwrite_code != ANTFLY_INVALID_ARGUMENT) {
        fprintf(
            stderr,
            "snapshot overwrite without replace returned %s, expected ANTFLY_INVALID_ARGUMENT\n",
            antfly_error_code_name(overwrite_code)
        );
        antfly_buffer_free(&overwrite_report);
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(bad_path);
        return 1;
    }

    void *snapshot_handle = NULL;
    if (expect_ok(antfly_lite_open_readonly(snapshot_path, &snapshot_handle), "open snapshot readonly") != 0) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(bad_path);
        return 1;
    }
    antfly_buffer snapshot_lookup = {0};
    if (expect_ok(antfly_db_lookup_json(snapshot_handle, write.key, &snapshot_lookup), "lookup snapshot json") != 0) {
        antfly_db_close(snapshot_handle);
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(bad_path);
        return 1;
    }
    if (!buffer_contains(snapshot_lookup, "c api lite")) {
        antfly_db_close(snapshot_handle);
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(bad_path);
        return fail_with_buffer("snapshot lookup json did not contain expected value", &snapshot_lookup);
    }
    antfly_buffer_free(&snapshot_lookup);
    antfly_db_close(snapshot_handle);

    antfly_buffer compact = {0};
    if (expect_ok(antfly_lite_compact_json(handle, &compact), "compact lite database") != 0) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(bad_path);
        return 1;
    }
    if (!buffer_contains(compact, "compacted") || !buffer_contains(compact, "vacuum")) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(bad_path);
        return fail_with_buffer("compact json did not include compact/vacuum fields", &compact);
    }
    antfly_buffer_free(&compact);

    antfly_buffer status = {0};
    if (expect_ok(antfly_lite_status_json(handle, &status), "status json") != 0) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(bad_path);
        return 1;
    }
    if (!buffer_contains(status, "aflite") || !buffer_contains(status, "native_single_file")) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(bad_path);
        return fail_with_buffer("status json did not describe native aflite storage", &status);
    }
    if (!buffer_contains(status, ANTFLY_LITE_INFERENCE_MODE_CALLER_SUPPLIED_OR_DISABLED) ||
        !buffer_contains(status, ANTFLY_LITE_INFERENCE_MODE_CALLER_SUPPLIED_ARTIFACTS) ||
        !buffer_contains(status, ANTFLY_LITE_INFERENCE_MODE_DISABLED_DEFERRED)) {
        antfly_db_close(handle);
        (void)remove(path);
        (void)remove(restored_path);
        (void)remove(snapshot_path);
        (void)remove(bad_path);
        return fail_with_buffer("status json did not expose expected Lite inference modes", &status);
    }
    antfly_db_buffer_free_zero(&status);

    antfly_db_close(handle);
    (void)remove(path);
    (void)remove(restored_path);
    (void)remove(snapshot_path);
    (void)remove(missing_path);
    (void)remove(bad_path);
    return 0;
}
