"""Chat-to-chat llama.cpp reference measurements; no automatic parity claim.

Replay and resident-prefix reuse are separate workloads. Portable checkpoint
restore is not equivalent to resident reuse. Run serially after CUDA compilation
and other model campaigns finish. The inspected source checkout differs from the
pinned release: actual HTTP token/cache accounting must validate its behavior.
The optional token_ids mode is an inference diagnostic, not end-to-end chat.
"""

import argparse
import copy
import http.client
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.request

ROOT = Path.cwd()
BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "zig/pkg/inference/scripts/gemma4"))
from qualify_context_management import (
    CudaProcessSampler,
    cuda_device_evidence,
    sha256_file,
)
from qualify_attention_match_queries import active_group_members


from native_benchmark_fixture import qualified_fixture
from native_phase_timing import latency_sampling_qualified
from matched_chat import (
    post_json,
    read_chat,
    request_identity,
    validate_chat,
    verify_template,
)


def percentiles(values, allow_zero=False):
    ordered = sorted(values)
    if not ordered or any(
        not math.isfinite(x) or x < 0 or (x == 0 and not allow_zero) for x in ordered
    ):
        raise AssertionError("invalid measured timings")
    return {
        name: ordered[math.ceil(q * len(ordered)) - 1]
        for name, q in [("p50", 0.5), ("p95", 0.95), ("max", 1)]
    }


def read_completion(response, started):
    content = []
    tokens = []
    first_token = None
    first_content = None
    for _ in range(1024):
        raw = response.readline(1024 * 1024 + 1)
        if not raw:
            raise AssertionError("completion stream ended without stop=true")
        if len(raw) > 1024 * 1024:
            raise AssertionError("oversized completion event")
        line = raw.decode().strip()
        if line.startswith("event:") and line[6:].strip() == "error":
            raise AssertionError("llama SSE error")
        if not line.startswith("data:"):
            continue
        value = line[5:].strip()
        if value == "[DONE]":
            raise AssertionError("DONE without final completion accounting")
        event = json.loads(value)
        if event.get("error") or type(event.get("stop")) is not bool:
            raise AssertionError("invalid completion event")
        if event.get("index", 0) != 0:
            raise AssertionError("unexpected completion index")
        part = event.get("content", "")
        ids = event.get("tokens", [])
        if (
            not isinstance(part, str)
            or not isinstance(ids, list)
            or any(type(x) is not int or x < 0 for x in ids)
        ):
            raise AssertionError("invalid generated content/token IDs")
        now = time.monotonic()
        if ids and first_token is None:
            first_token = now - started
        if part and first_content is None:
            first_content = now - started
        content.append(part)
        tokens.extend(ids)
        if event["stop"]:
            if first_token is None or first_content is None:
                raise AssertionError("no streamed token/content evidence")
            event.pop("prompt", None)
            event.update(content="".join(content), tokens=tokens)
            return {
                "result": event,
                "total_seconds": now - started,
                "ttft_seconds": first_token,
                "first_content_seconds": first_content,
            }
    raise AssertionError("excessive completion events")


def content_token_count(result):
    """Compare content tokens while retaining llama's raw EOS-inclusive counters.

    The pinned Gemma GGUF maps 106 to <turn|>, an Antfly stop token. Antfly
    excludes that token from completion_tokens; this llama release includes it.
    Only that verified terminal token may account for a one-token difference.
    """
    count = result.get("tokens_predicted")
    tokens = result.get("tokens")
    if (
        type(count) is not int
        or count <= 0
        or not isinstance(tokens, list)
        or len(tokens) != count
        or any(type(t) is not int or t < 0 for t in tokens)
        or type(result.get("timings", {}).get("predicted_n")) is not int
        or result["timings"]["predicted_n"] != count
    ):
        raise AssertionError("invalid generated token IDs or raw output accounting")
    if 106 in tokens:
        if result.get("stop_type") != "eos" or tokens[-1] != 106 or 106 in tokens[:-1]:
            raise AssertionError("invalid terminal Gemma turn token")
        return count - 1
    return count


def validate_result(result, logical_tokens, output_tokens, expected_content, mode):
    timings = result["timings"]
    for key in ("prompt_n", "cache_n", "predicted_n"):
        if type(timings.get(key)) is not int or timings[key] < 0:
            raise AssertionError("invalid token accounting")
    if any(
        type(result.get(key)) is not int
        for key in ("tokens_evaluated", "tokens_predicted")
    ):
        raise AssertionError("invalid response token accounting")
    if result.get("truncated") is not False:
        raise AssertionError("context truncation/shift occurred")
    if (
        result.get("tokens_evaluated") != logical_tokens
        or timings["prompt_n"] + timings["cache_n"] != logical_tokens
    ):
        raise AssertionError("actual prompt differs from checkpoint token IDs")
    if content_token_count(result) != output_tokens:
        raise AssertionError("output token count differs from Antfly reference")
    if (
        result.get("content", "").strip() != expected_content.strip()
        or result.get("stop_type") != "eos"
    ):
        raise AssertionError("deterministic answer or EOS behavior differs")
    if mode == "replay" and timings["cache_n"] != 0:
        raise AssertionError("cached work contaminated replay comparison")
    if mode == "resident" and (
        timings["cache_n"] < logical_tokens - 1 or timings["prompt_n"] > 1
    ):
        raise AssertionError("resident workload did not reuse the saved prefix")
    for key in ("prompt_ms", "predicted_ms"):
        value = timings.get(key)
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise AssertionError("invalid engine phase timing")
        if value == 0 and (key == "predicted_ms" or timings["prompt_n"] > 0):
            raise AssertionError("executed phase has no measured duration")


def require_idle():
    output = subprocess.check_output(
        ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True
    ).strip()
    if output:
        raise RuntimeError(f"GPU occupied: {output}")
    for process in Path("/proc").iterdir():
        if not process.name.isdigit():
            continue
        try:
            words = (process / "cmdline").read_bytes().split(b"\0")
            if (
                words
                and Path(words[0].decode(errors="replace")).name == "zig"
                and any(
                    word in (b"build-lib", b"build-exe", b"test") for word in words[1:3]
                )
            ):
                if (process / "stat").read_text().rsplit(")", 1)[1].split()[0] not in (
                    "Z",
                    "X",
                ):
                    raise RuntimeError(f"compiler active: {process.name}")
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            continue


def finish_campaign(evidence, process, sampler, timeout=30):
    """Retire the entire owned group and preserve primary and cleanup failures."""
    errors = []
    if sampler is not None:
        try:
            memory = sampler.finish()
            evidence["memory"] = memory
            if (
                memory["samples"] < 2
                or memory["errors"]
                or memory["gpu_uuids"] != [evidence["hardware"][0]["uuid"]]
                or memory["peak_host_rss_bytes"] <= 0
                or memory["peak_gpu_memory_bytes"] <= 0
            ):
                raise AssertionError("incomplete or mismatched memory evidence")
        except Exception as error:
            errors.append(f"memory sampling: {type(error).__name__}: {error}")
    if process is not None:

        def signal_group(sig):
            try:
                os.killpg(process.pid, sig)
            except ProcessLookupError:
                pass

        def wait_group():
            deadline = time.monotonic() + timeout
            while active_group_members(process.pid):
                if time.monotonic() >= deadline:
                    return False
                time.sleep(0.01)
            return True

        try:
            signal_group(signal.SIGTERM)
            try:
                process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                signal_group(signal.SIGKILL)
                evidence["forced_kill"] = True
                errors.append("llama required forced shutdown")
                process.wait(timeout=timeout)
            if not wait_group():
                signal_group(signal.SIGKILL)
                evidence["forced_kill"] = True
                errors.append("llama descendants required forced shutdown")
                if not wait_group():
                    errors.append("llama process group did not retire after SIGKILL")
        except Exception as error:
            errors.append(f"process cleanup: {type(error).__name__}: {error}")
            # A sampling/wait failure must not bypass the last cleanup attempt.
            try:
                signal_group(signal.SIGKILL)
                evidence["forced_kill"] = True
                process.wait(timeout=timeout)
                if not wait_group():
                    errors.append("llama process group survived final cleanup")
            except Exception as final_error:
                errors.append(
                    f"final process cleanup: {type(final_error).__name__}: {final_error}"
                )
        evidence["server_returncode"] = process.poll()
        try:
            evidence["process_group_stopped"] = not active_group_members(process.pid)
        except Exception as error:
            evidence["process_group_stopped"] = False
            errors.append(
                f"process group verification: {type(error).__name__}: {error}"
            )
        if not evidence["process_group_stopped"]:
            errors.append("process group cleanup unverified")
        if process.returncode not in (0, -signal.SIGTERM):
            errors.append(f"llama unexpected exit: {process.returncode}")
    if errors:
        evidence["cleanup_errors"] = errors
        evidence["passed"] = False
        evidence.setdefault("error", errors[0])
    if not evidence["passed"]:
        evidence["sampling_qualified"] = False


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--assets-root",
        type=Path,
        default=ROOT,
        help="Root containing the pinned .models and .cache/llama-* assets",
    )
    parser.add_argument("--size", choices=["8k", "32k"], required=True)
    parser.add_argument("--mode", choices=["replay", "resident"], default="replay")
    parser.add_argument(
        "--input-mode",
        choices=["chat", "token_ids"],
        default="chat",
        help="Chat includes server formatting/tokenization; token_ids is a separate inference diagnostic",
    )
    parser.add_argument(
        "--reference",
        type=Path,
        required=True,
        help="passing Antfly public checkpoint campaign for this exact token fixture",
    )
    parser.add_argument(
        "--checkpoint",
        type=Path,
        required=True,
        help="checkpoint exported by the passing reference campaign",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8522)
    parser.add_argument("--warmups", type=int, default=5)
    parser.add_argument(
        "--sampling-profile", choices=["bounded", "extended"], default="bounded"
    )
    parser.add_argument("--samples", type=int)
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="permit fewer samples; never qualifies release sampling",
    )
    args = parser.parse_args()
    if args.input_mode == "chat" and args.mode != "replay":
        parser.error("chat comparison requires full replay")
    if args.samples is None:
        args.samples = {"bounded": 20, "extended": 100}[args.sampling_profile]
    args.assets_root = args.assets_root.resolve()
    if args.warmups < 0 or args.samples < 1:
        parser.error("invalid sample counts")
    if not args.smoke and not latency_sampling_qualified(
        args.sampling_profile, args.warmups, args.samples
    ):
        parser.error(
            "sampling requires five warmups and 20 bounded or 100 extended measurements"
        )
    if args.output.exists():
        raise FileExistsError(args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    evidence = {
        "passed": False,
        "production_ready": False,
        "generation_latency_gate_qualified": False,
        "scope": __doc__,
        "mode": args.mode,
        "size": args.size,
        "smoke": args.smoke,
        "input_mode": args.input_mode,
        "warmups_requested": args.warmups,
        "samples_requested": args.samples,
        "sampling_profile": args.sampling_profile,
        "observations": [],
        "runner_sha256": sha256_file(Path(__file__)),
        "reference_sha256": sha256_file(args.reference),
        "assets_root": str(args.assets_root),
    }

    def publish():
        temporary = args.output.with_suffix(".json.part")
        temporary.write_text(json.dumps(evidence, indent=2) + "\n")
        temporary.replace(args.output)

    process = None
    sampler = None
    try:
        require_idle()
        reference, fixture = qualified_fixture(
            args.reference, args.checkpoint, args.size
        )
        token_ids = fixture.pop("token_ids")
        body = {
            "prompt": token_ids,
            "n_predict": 16,
            "temperature": 0,
            "repeat_penalty": 1,
            "cache_prompt": False,
            "stream": False,
            "return_tokens": True,
        }
        endpoint = "/completion"
        if args.input_mode == "chat":
            from qualify_attention_match_queries import requests

            body = next(
                iter(
                    requests(
                        "google/gemma-4-E4B-it-qat-q4_0-gguf",
                        "f16",
                        [742 if args.size == "8k" else 2975],
                        True,
                        "fact",
                    )
                )
            )
            for key in ("cache_dtype", "context_management"):
                body.pop(key)
            body.update(cache_prompt=False, repeat_penalty=1)
            endpoint = "/v1/chat/completions"
            evidence["chat_request_sha256"] = request_identity(body)
            evidence["request"] = body
        evidence["endpoint"] = endpoint
        evidence["chat_helper_sha256"] = sha256_file(BASE / "matched_chat.py")
        evidence["fixture_helper_sha256"] = sha256_file(
            BASE / "native_benchmark_fixture.py"
        )
        prior = reference["exporter"]["requests"]
        prepared = next(row["result"] for row in prior if row["kind"] == "prepare")
        expected = next(row["result"] for row in prior if row["kind"] == "same_process")
        if prepared["file_sha256"] != fixture["checkpoint_sha256_to_reverify"]:
            raise AssertionError("Antfly reference used different checkpoint tokens")
        expected_output = expected["choices"][0]["message"]["content"]
        expected_count = expected["usage"]["completion_tokens"]
        model = (
            args.assets_root
            / ".models/google/gemma-4-E4B-it-qat-q4_0-gguf/gemma-4-E4B_q4_0-it.gguf"
        )
        expected_model = (
            "676c35070db6dbe52f93e9c864ee0fba4eddea94b9c875d9cb10daff453fbaee"
        )
        if sha256_file(model) != expected_model:
            raise AssertionError("GGUF revision changed")
        release = args.assets_root / ".cache/llama-release/llama-b11669"
        runtime = args.assets_root / ".cache/llama-runtime/usr/lib/x86_64-linux-gnu"
        cuda = (
            args.assets_root
            / ".cache/llama-release/cudart-llama-b11669-bin-ubuntu-cuda-12.8-x64"
        )
        binary = release / "llama-server"
        loader = runtime / "ld-linux-x86-64.so.2"
        files = {
            p.resolve()
            for directory in (release, runtime, cuda)
            for p in directory.iterdir()
            if p.is_file() and (".so" in p.name or p.name == "llama-server")
        }
        files.add(loader.resolve())
        evidence["runtime_sha256"] = {
            str(path): sha256_file(path) for path in sorted(files)
        }
        if (
            sha256_file(binary)
            != "3ac6495070ae9627825dbe706de3b580fdbedd2fbe079197b2c6c81982dbd0f9"
        ):
            raise AssertionError("pinned llama launcher changed")
        if (
            sha256_file(release / "libllama-server-impl.so")
            != "25bcb97b2a8587588a542218bdb2ce3c44ba8e16ce6f87de98dc0fec93e28d78"
        ):
            raise AssertionError("pinned llama server implementation changed")
        body.update(stream=True, cache_prompt=args.mode == "resident")
        library_path = ":".join(
            str(path)
            for path in (runtime, release, cuda, Path("/usr/lib/x86_64-linux-gnu"))
        )
        command = [
            str(loader),
            "--library-path",
            library_path,
            str(binary),
            "-m",
            str(model),
            "--host",
            "127.0.0.1",
            "--port",
            str(args.port),
            "-c",
            "32768",
            "-np",
            "1",
            "-b",
            "512",
            "-ub",
            "512",
            "-ngl",
            "999",
            "-fa",
            "on",
            "-ctk",
            "f16",
            "-ctv",
            "f16",
            "--no-context-shift",
        ]
        if args.input_mode == "chat":
            command += ["--jinja", "--alias", body["model"]]
        hardware = cuda_device_evidence()
        if len(hardware) != 1 or hardware[0]["name"] != "NVIDIA L4":
            raise AssertionError("expected the single designated L4")
        evidence.update(
            command=command,
            model_sha256=expected_model,
            fixture=fixture,
            model_path=str(model.resolve()),
            hardware=hardware,
            request_settings={
                k: v for k, v in body.items() if k not in ("prompt", "messages")
            },
        )
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith(("LLAMA_", "GGML_")) and key != "LD_PRELOAD"
        }
        evidence.update(
            server_cwd=str(release),
            environment_policy="Remove inherited LLAMA_* and GGML_* overrides and LD_PRELOAD; explicit pinned loader/library paths and CLI settings",
        )
        require_idle()
        publish()
        with args.output.with_suffix(".server.log").open("x") as log:
            # The explicit glibc loader changes /proc/self/exe. The release
            # directory lets GGML discover its pinned CPU/CUDA backend plugins
            # through its documented current-directory search as well.
            process = subprocess.Popen(
                command,
                cwd=release,
                env=environment,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            evidence["server_pid"] = process.pid
            sampler = CudaProcessSampler(process.pid)
            sampler.start()
            deadline = time.monotonic() + 180
            while True:
                if process.poll() is not None:
                    raise RuntimeError("llama exited during startup")
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{args.port}/health", timeout=1
                    ) as response:
                        if response.status == 200:
                            break
                except OSError:
                    pass
                if time.monotonic() > deadline:
                    raise TimeoutError("llama startup deadline exceeded")
                time.sleep(0.1)
            gpu_pids = subprocess.check_output(
                ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"],
                text=True,
            )
            if {int(pid) for pid in gpu_pids.split()} != {process.pid}:
                raise AssertionError(
                    "GPU attribution does not match the isolated llama process"
                )
            if args.input_mode == "chat":
                evidence["template_preflight"] = verify_template(
                    args.port, body, token_ids
                )
                preflight = post_json(
                    args.port,
                    endpoint,
                    dict(body, stream=False, verbose=True, return_tokens=True),
                )
                raw = preflight["__verbose"]
                validate_result(
                    raw, len(token_ids), expected_count, expected_output, "replay"
                )
                raw.pop("prompt", None)
                evidence["chat_preflight"] = raw
                evidence["cold_scope"] = (
                    "First timed request after untimed template and generation preflight; excluded from warm percentiles, not a model cold-start measurement"
                )
                publish()
            generated_tokens = None
            for index in range(1 + args.warmups + args.samples):
                request = copy.deepcopy(body)
                if index == 0:
                    request["cache_prompt"] = False
                connection = http.client.HTTPConnection(
                    "127.0.0.1", args.port, timeout=1800
                )
                row = {
                    "index": index,
                    "cold": index == 0,
                    "warmup": 0 < index <= args.warmups,
                }
                evidence["observations"].append(row)
                publish()
                try:
                    started = time.monotonic()
                    connection.request(
                        "POST",
                        endpoint,
                        json.dumps(request).encode(),
                        {"Content-Type": "application/json"},
                    )
                    response = connection.getresponse()
                    if response.status != 200:
                        raise AssertionError(
                            f"llama HTTP {response.status}: {response.read(65536)!r}"
                        )
                    if not response.getheader("Content-Type", "").startswith(
                        "text/event-stream"
                    ):
                        raise AssertionError("missing streamed completion")
                    row.update(
                        (read_chat if args.input_mode == "chat" else read_completion)(
                            response, started
                        )
                    )
                finally:
                    connection.close()
                if args.input_mode == "chat":
                    validate_chat(
                        row["result"], len(token_ids), evidence["chat_preflight"]
                    )
                    row["passed"] = True
                    publish()
                    continue
                validate_result(
                    row["result"],
                    len(token_ids),
                    expected_count,
                    expected_output,
                    "replay" if index == 0 else args.mode,
                )
                row["output_accounting"] = {
                    "raw_predicted_tokens": row["result"]["tokens_predicted"],
                    "content_tokens": content_token_count(row["result"]),
                    "terminal_turn_token_count": int(
                        row["result"]["tokens"][-1] == 106
                    ),
                }
                if generated_tokens is None:
                    generated_tokens = row["result"]["tokens"]
                elif row["result"]["tokens"] != generated_tokens:
                    raise AssertionError("generated token IDs changed between runs")
                row["passed"] = True
                publish()
            measured = [
                row
                for row in evidence["observations"]
                if not row["cold"] and not row["warmup"]
            ]
            evidence["latency_seconds"] = {
                key: percentiles([row[key] for row in measured])
                for key in (
                    ("total_seconds", "first_content_seconds")
                    if args.input_mode == "chat"
                    else ("total_seconds", "ttft_seconds", "first_content_seconds")
                )
            }
            evidence["engine_phase_ms"] = {
                key: percentiles(
                    [row["result"]["timings"][key] for row in measured], allow_zero=True
                )
                for key in ("prompt_ms", "predicted_ms")
            }
        if (
            sha256_file(model) != expected_model
            or sha256_file(args.reference) != evidence["reference_sha256"]
        ):
            raise AssertionError("model/reference changed during measurement")
        for path, digest in evidence["runtime_sha256"].items():
            if sha256_file(Path(path)) != digest:
                raise AssertionError(f"loaded runtime artifact changed: {path}")
        if sha256_file(Path(__file__)) != evidence["runner_sha256"]:
            raise AssertionError("runner changed")
        if sha256_file(BASE / "matched_chat.py") != evidence["chat_helper_sha256"]:
            raise AssertionError("chat helper changed")
        if (
            sha256_file(BASE / "native_benchmark_fixture.py")
            != evidence["fixture_helper_sha256"]
            or sha256_file(args.checkpoint) != fixture["checkpoint_sha256_to_reverify"]
        ):
            raise AssertionError("checkpoint or fixture extractor changed")
        evidence.update(
            passed=True,
            sampling_qualified=latency_sampling_qualified(
                args.sampling_profile, args.warmups, len(measured), args.smoke
            ),
        )
    except Exception as error:
        evidence.update(passed=False, error=f"{type(error).__name__}: {error}")
    finally:
        finish_campaign(evidence, process, sampler)
        publish()
    print(
        json.dumps(
            {
                "passed": evidence["passed"],
                "sampling_qualified": evidence.get("sampling_qualified", False),
                "error": evidence.get("error"),
            }
        ),
        flush=True,
    )
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
