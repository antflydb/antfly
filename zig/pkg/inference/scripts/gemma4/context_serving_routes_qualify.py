#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Strict request/report contract for CLI and serial mixed-batch soak routes."""
from pathlib import Path
import hashlib, json, re, struct

HEX = re.compile(r"[0-9a-f]{64}")
ROUTES = {"native", "cli", "http", "stream", "serial_mixed_batch"}
SINGLE_FIELDS = {
    "route", "variant", "document_file", "document_sha256", "question_file",
    "question_sha256", "request_content_sha256", "prompt_sha256", "prompt_tokens",
    "reserved_output_tokens", "settings_sha256", "variant_settings_sha256",
}
SIBLING_FIELDS = SINGLE_FIELDS - {"route"}
RUNTIME_FIELDS = {"model", "max_context_tokens", "max_kv_tokens", "keep_prefix_tokens",
                  "keep_recent_tokens", "optimize_decode", "optimize_prefill",
                  "implementation_sha256", "profile_artifact_sha256", "profile_semantic_sha256"}

CLI_ARGV_DOMAIN = b"antfly.context-serving.cli-argv.v1\0"
CLI_ARGV_MAX_BYTES = 64 * 1024
CLI_ARGV_MAX_ITEMS = 64
CLI_LOOPBACK = re.compile(r"http://127\.0\.0\.1:([1-9][0-9]{0,4})")


def cli_argv_preimage(argv):
    require(type(argv) is list and 0 < len(argv) <= CLI_ARGV_MAX_ITEMS and
            all(isinstance(arg, str) and arg and "\0" not in arg for arg in argv),
            "invalid CLI argv")
    encoded = [arg.encode("utf-8") for arg in argv]
    raw = CLI_ARGV_DOMAIN + b"".join(struct.pack("<Q", len(arg)) + arg for arg in encoded)
    require(len(raw) <= CLI_ARGV_MAX_BYTES, "CLI argv exceeds byte bound")
    return raw


def expected_cli_argv(row, runtime, binary, server_url):
    return [binary, "inference", "generate", runtime["model"], "-",
            "--server", server_url, "--require-server", "--backend", "metal",
            "--max-tokens", str(row["reserved_output_tokens"]),
            "--temperature", "0", "--top-p", "0", "--top-k", "1",
            "--ignore-eos", "--disable-thinking", "--context-mode", "attention_matching",
            "--context-window", str(runtime["max_context_tokens"]),
            "--max-kv-tokens", str(runtime["max_kv_tokens"]),
            "--keep-prefix-tokens", str(runtime["keep_prefix_tokens"]),
            "--keep-recent-tokens", str(runtime["keep_recent_tokens"]),
            "--compact-part", "0:0", "--context-document-file", row["document_file"],
            "--context-document-sha256", row["document_sha256"],
            "--context-question-file", row["question_file"],
            "--context-question-sha256", row["question_sha256"], "--stream"]


def cli_argv_contract(path, row, runtime):
    raw = Path(path).read_bytes()
    require(len(raw) <= CLI_ARGV_MAX_BYTES and raw.startswith(CLI_ARGV_DOMAIN),
            "invalid CLI argv artifact")
    cursor, argv = len(CLI_ARGV_DOMAIN), []
    while cursor < len(raw):
        require(len(argv) < CLI_ARGV_MAX_ITEMS and cursor + 8 <= len(raw),
                "invalid CLI argv framing")
        length = struct.unpack_from("<Q", raw, cursor)[0]; cursor += 8
        require(0 < length <= CLI_ARGV_MAX_BYTES and cursor + length <= len(raw),
                "invalid CLI argv item")
        try:
            arg = raw[cursor:cursor + length].decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError("invalid CLI argv UTF-8") from error
        require("\0" not in arg, "invalid CLI argv item")
        argv.append(arg); cursor += length
    require(cursor == len(raw) and argv and Path(argv[0]).name == "antfly",
            "invalid CLI executable argv")
    require(len(argv) > 6 and CLI_LOOPBACK.fullmatch(argv[6]) is not None and
            int(CLI_LOOPBACK.fullmatch(argv[6]).group(1)) <= 65535,
            "invalid CLI loopback argv")
    require(argv == expected_cli_argv(row, runtime, argv[0], argv[6]),
            "CLI argv does not match sealed request")
    require(raw == cli_argv_preimage(argv), "noncanonical CLI argv artifact")
    return hashlib.sha256(raw).hexdigest()


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def structured_request_identity(document, question):
    raw = bytearray(b"antfly.context-serving.structured-request.v1\0user\0")
    raw.append(2)
    for selected, value in ((1, document), (0, question)):
        raw.extend((1, selected)); raw.extend(struct.pack("<Q", len(value))); raw.extend(value)
    raw.extend(struct.pack("<II", 0, 0))
    return hashlib.sha256(raw).hexdigest()


def generation_settings(reserved):
    return hashlib.sha256(b"antfly.context-serving.generation.v1\0" +
                          struct.pack("<I", reserved) + bytes((1, 0, 0, 0, 0, 0))).hexdigest()


def variant_settings(variant, runtime):
    encoded = variant.encode("ascii")
    raw = bytearray(b"antfly.context-serving.variant.v2\0")
    raw.extend(struct.pack("<I", len(encoded))); raw.extend(encoded)
    raw.extend(struct.pack("<IIII", *(runtime[name] for name in
        ("max_context_tokens", "max_kv_tokens", "keep_prefix_tokens", "keep_recent_tokens"))))
    raw.extend(bytes((int(runtime["optimize_decode"]), int(runtime["optimize_prefill"]))))
    for name in ("implementation_sha256", "profile_artifact_sha256", "profile_semantic_sha256"):
        raw.extend(bytes.fromhex(runtime[name]))
    return hashlib.sha256(raw).hexdigest()


def cli_request_identity(row, runtime):
    raw = bytearray(b"antfly.context-serving.cli-request.v1\0")
    for value in (runtime["model"], row["document_file"], row["document_sha256"],
                  row["question_file"], row["question_sha256"]):
        encoded = value.encode("utf-8"); raw.extend(struct.pack("<Q", len(encoded))); raw.extend(encoded)
    raw.extend(struct.pack("<IIIII", runtime["max_context_tokens"], runtime["max_kv_tokens"],
                           runtime["keep_prefix_tokens"], runtime["keep_recent_tokens"],
                           row["reserved_output_tokens"]))
    raw.append(1)  # streaming is mandatory for genuine TTFT evidence
    return hashlib.sha256(raw).hexdigest()


def campaign_file(root, ref, label, maximum=16 * 1024 * 1024):
    require(type(ref) is dict and set(ref) == {"path", "sha256"}, f"invalid {label} reference")
    path_text = ref["path"]
    require(isinstance(path_text, str) and path_text and not Path(path_text).is_absolute() and
            all(part not in ("", ".", "..") for part in Path(path_text).parts), f"invalid {label} path")
    path = Path(root) / path_text
    require(path.is_file() and not path.is_symlink() and path.stat().st_size <= maximum, f"invalid {label} file")
    require(HEX.fullmatch(ref["sha256"] or "") and digest(path) == ref["sha256"], f"invalid {label} digest")
    return path


def cli_public_output_contract(path, expected_bytes, expected_sha256):
    require(type(expected_bytes) is int and 0 <= expected_bytes < 1024 * 1024 and
            HEX.fullmatch(expected_sha256 or ""), "invalid CLI public output binding")
    raw = Path(path).read_bytes()
    require(len(raw) == expected_bytes + 1 and raw[-1:] == b"\n",
            "CLI stdout is not exact public output plus newline")
    require(hashlib.sha256(raw[:-1]).hexdigest() == expected_sha256,
            "CLI stdout public output digest mismatch")


def _request(row, root, runtime, mixed=False):
    expected = SIBLING_FIELDS if mixed else SINGLE_FIELDS
    require(type(row) is dict and set(row) == expected, "invalid serving route request")
    if not mixed:
        require(row["route"] in ROUTES - {"serial_mixed_batch"}, "invalid single serving route")
    require(row["variant"] in ({"exact", "attention_matching"} if mixed else {"attention_matching"}),
            "soak single request must exercise attention matching")
    files = {}
    for name in ("document", "question"):
        files[name] = campaign_file(root, {"path": row[name + "_file"],
                                          "sha256": row[name + "_sha256"]}, name).read_bytes()
    require(structured_request_identity(files["document"], files["question"]) ==
            row["request_content_sha256"], "serving structured request identity mismatch")
    for name in ("request_content_sha256", "prompt_sha256", "settings_sha256", "variant_settings_sha256"):
        require(HEX.fullmatch(row[name] or ""), "invalid serving request digest")
    require(type(row["prompt_tokens"]) is int and row["prompt_tokens"] > 0 and
            type(row["reserved_output_tokens"]) is int and row["reserved_output_tokens"] > 0,
            "invalid serving token geometry")
    require(row["settings_sha256"] == generation_settings(row["reserved_output_tokens"]) and
            row["variant_settings_sha256"] == variant_settings(row["variant"], runtime),
            "serving request settings identity mismatch")
    return dict(row)


def plan_contract(plan, root):
    require(type(plan) is dict and set(plan) == {"schema_version", "cli_binary_sha256",
                                                "cli_source_build_sha256", "runtime", "requests"} and
            plan["schema_version"] == 2 and HEX.fullmatch(plan["cli_binary_sha256"] or "") and
            HEX.fullmatch(plan["cli_source_build_sha256"] or "") and
            type(plan["requests"]) is list and 5 <= len(plan["requests"]) <= 4096,
            "invalid serving route plan")
    runtime = plan["runtime"]
    require(type(runtime) is dict and set(runtime) == RUNTIME_FIELDS and
            isinstance(runtime["model"], str) and runtime["model"] and
            all(type(runtime[name]) is int and 0 <= runtime[name] < 2**32 for name in
                ("max_context_tokens", "max_kv_tokens", "keep_prefix_tokens", "keep_recent_tokens")) and
            runtime["max_context_tokens"] > 0 and runtime["max_kv_tokens"] > 0 and
            type(runtime["optimize_decode"]) is bool and type(runtime["optimize_prefill"]) is bool and
            all(HEX.fullmatch(runtime[name] or "") for name in
                ("implementation_sha256", "profile_artifact_sha256", "profile_semantic_sha256")),
            "invalid serving route runtime")
    normalized = []
    seen = set()
    for row in plan["requests"]:
        require(type(row) is dict and isinstance(row.get("route"), str), "invalid serving route row")
        route = row["route"]
        if route != "serial_mixed_batch":
            item = _request(row, root, runtime)
        else:
            require(set(row) == {"route", "settings_sha256", "siblings"} and
                    HEX.fullmatch(row["settings_sha256"] or "") and
                    type(row["siblings"]) is list and len(row["siblings"]) >= 2,
                    "invalid serial mixed batch request")
            siblings = []
            exact = attention = 0
            for sibling in row["siblings"]:
                require(type(sibling) is dict and set(sibling) == SIBLING_FIELDS,
                        "invalid serial mixed sibling")
                variant = sibling["variant"]
                require(variant in {"exact", "attention_matching"}, "invalid serial mixed variant")
                value = dict(sibling)
                # Exact siblings use the same sealed structured renderer input,
                # but do not publish. Validation otherwise matches AM inputs.
                if variant == "exact":
                    exact += 1
                    _request(value, root, runtime, True)
                else:
                    attention += 1
                    _request(value, root, runtime, True)
                siblings.append(value)
            require(exact >= 1 and attention == 1, "serial mixed batch must contain exact siblings and one AM singleton")
            require(len({sibling["reserved_output_tokens"] for sibling in siblings}) == 1 and
                    row["settings_sha256"] == generation_settings(siblings[0]["reserved_output_tokens"]),
                    "mixed batch generation settings mismatch")
            item = {"route": route, "settings_sha256": row["settings_sha256"], "siblings": siblings}
        normalized.append(item)
        seen.add(route)
    require(seen == ROUTES, "serving route plan misses a required route")
    return {"schema_version": 2, "cli_binary_sha256": plan["cli_binary_sha256"],
            "cli_source_build_sha256": plan["cli_source_build_sha256"],
            "runtime": dict(runtime), "requests": normalized}


def requests(plan):
    return [dict(row) for row in plan["requests"]]


def report_contract(report, expected, identity, provenance, runtime, cli_binary_sha256, root, release):
    route = expected["route"]
    require(report.get("route") == route, "serving route report mismatch")
    if route == "serial_mixed_batch":
        require(report.get("semantic_identity") == identity and report.get("provenance") == provenance and
                report.get("profile_semantic_sha256") == runtime["profile_semantic_sha256"],
                "mixed batch identity or provenance mismatch")
        audit = dict(report)
        for field in ("route", "semantic_identity", "provenance", "profile_semantic_sha256"):
            audit.pop(field)
        mixed = release.mixed_batch_serving_contract(audit, expected["siblings"])
        return {
            "report": report,
            "load_generation": mixed["loaded_identity"]["load_generation"],
            "baseline_live_bytes": None,
            "context_tokens": [row["prompt_tokens"] + row["reserved_output_tokens"]
                               for row in expected["siblings"]],
            "elapsed_ns": max(item["elapsed_ns"] for item in mixed["items"]),
        }
    release.report_contract_v2(report, identity, provenance)
    require(all(report.get(field) == expected[field] for field in (
        "variant", "document_file", "document_sha256", "question_file", "question_sha256",
        "request_content_sha256", "prompt_sha256", "prompt_tokens", "reserved_output_tokens",
        "settings_sha256", "variant_settings_sha256")),
        "serving route report/request binding mismatch")
    if route != "cli":
        require(report.get("cli_process") is None, "non-CLI route carries CLI process evidence")
        return {
            "report": report,
            "load_generation": release.loaded_serving_identity(report, identity),
            "baseline_live_bytes": report["live_bytes_before"],
            "context_tokens": [report["prompt_tokens"] + report["reserved_output_tokens"]],
            "elapsed_ns": report["elapsed_ns"],
        }
    process = report.get("cli_process")
    require(type(process) is dict and set(process) == {
        "separate_process", "binary_sha256", "argv_sha256", "argv", "request_sha256", "exit_code", "elapsed_ns",
        "public_output_sha256", "public_output_bytes", "public_output_match", "stdout", "stderr"} and
        process["separate_process"] is True and process["binary_sha256"] == cli_binary_sha256 and
        HEX.fullmatch(process["argv_sha256"] or "") and
        process["request_sha256"] == cli_request_identity(expected, runtime) and process["exit_code"] == 0 and
        type(process["elapsed_ns"]) is int and process["elapsed_ns"] >= report["elapsed_ns"] and
        process["public_output_match"] is True,
        "invalid CLI child process evidence")
    argv_path = campaign_file(root, process["argv"], "CLI argv", CLI_ARGV_MAX_BYTES)
    require(cli_argv_contract(argv_path, expected, runtime) == process["argv_sha256"],
            "CLI argv digest mismatch")
    stdout_path = campaign_file(root, process["stdout"], "CLI stdout", 1024 * 1024)
    cli_public_output_contract(stdout_path, process["public_output_bytes"],
                               process["public_output_sha256"])
    campaign_file(root, process["stderr"], "CLI stderr", 1024 * 1024)
    return {
        "report": report,
        "load_generation": release.loaded_serving_identity(report, identity),
        "baseline_live_bytes": report["live_bytes_before"],
        "context_tokens": [report["prompt_tokens"] + report["reserved_output_tokens"]],
        "elapsed_ns": report["elapsed_ns"],
    }
