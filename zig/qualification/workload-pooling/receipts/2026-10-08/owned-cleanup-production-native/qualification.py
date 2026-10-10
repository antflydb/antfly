import pathlib, json, subprocess, time, os, hashlib

root = pathlib.Path(
    "/Users/ajroetker/go/src/github.com/antflydb/antfly/.worktrees/pooling-control-cleanup-owner"
)
files = [
    "zig/build.zig",
    "zig/lib/httpx/src/httpx.zig",
    "zig/lib/httpx/src/net/socket.zig",
    "zig/lib/httpx/src/protocol/h2_connection.zig",
    "zig/lib/httpx/src/protocol/stream.zig",
    "zig/lib/httpx/src/server/server.zig",
    "zig/lib/httpx/src/util/common.zig",
    "zig/lib/json/src/mod.zig",
    "zig/lib/runtime/src/root.zig",
    "zig/lib/runtime/src/runtime_http_abi.zig",
    "zig/lib/runtime/src/runtime_http_bridge.zig",
    "zig/lib/runtime/src/runtime_http_cleanup.zig",
    "zig/lib/runtime/src/runtime_runnable_abi.zig",
    "zig/pkg/antfly-embedded/src/common/bounded_admission.zig",
    "zig/pkg/antfly-embedded/src/common/runnable_scheduler.zig",
    "zig/pkg/antfly-embedded/src/storage/inference_provider.zig",
    "zig/pkg/antfly/build/api_tests.zig",
    "zig/pkg/antfly/build/tests.zig",
    "zig/pkg/antfly/src/api/runnable_http_cleanup_test_root.zig",
    "zig/pkg/antfly/src/api/runnable_inference_test_root.zig",
    "zig/pkg/antfly/src/api/runnable_owner_execution.zig",
    "zig/pkg/antfly/src/api/runnable_worker_process_fixture.zig",
    "zig/pkg/antfly/src/api/runnable_worker_process_test_root.zig",
    "zig/pkg/antfly/src/api/runnable_worker_test_root.zig",
    "zig/pkg/antfly/src/standalone/runtime.zig",
    "zig/pkg/inference/src/execution_control.zig",
    "zig/pkg/inference/src/host/bridge.zig",
    "zig/pkg/inference/src/host/host.zig",
    "zig/pkg/inference/src/host/provider_failure.zig",
    "zig/pkg/inference/src/host/worker.zig",
    "zig/pkg/inference/src/host/worker_execution_phase.zig",
    "zig/pkg/inference/src/host/worker_output_error.zig",
    "zig/pkg/inference/src/host/worker_rpc.zig",
    "zig/pkg/inference/src/host/worker_wire.zig",
    "zig/pkg/inference/src/server/executor_microbatch.zig",
    "zig/pkg/inference/src/server/runnable_execution.zig",
    "zig/pkg/inference/src/server/server.zig",
]
hashes = {f: hashlib.sha256((root / f).read_bytes()).hexdigest() for f in files}
result = pathlib.Path("/private/tmp/antfly-owned-cleanup-production-native.json")
assert not result.exists()
r = {
    "pid": os.getpid(),
    "state": "queued",
    "queued_at": time.time(),
    "source_sha256": hashes,
    "base_commit": subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip(),
}
result.write_text(json.dumps(r, indent=2))
print("Dispatcher", os.getpid(), flush=True)
# Avoid overcommitting the host while already-live unrelated builds finish.
waiting_pids = [55877, 57136, 40983]
r["host_build_dependencies"] = waiting_pids
result.write_text(json.dumps(r, indent=2))
for pid in waiting_pids:
    while True:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(2)
argv = [
    "/Users/ajroetker/bin/zig-0.17.0/zig",
    "build",
    "antfly-inference-provider-context-test",
    "antfly-inference-worker-owner-handoff-test",
    "antfly-inference-worker-rpc-test",
    "antfly-workload-pooling-test",
    "antfly-inference-worker-output-error-test",
    "antfly-inference-worker-process-output-test",
    "antfly-http-cleanup-owner-test",
    "-Doptimize=ReleaseSafe",
    "-j1",
    "--maxrss",
    "34359738368",
    "--cache-dir",
    "/private/tmp/antfly-default-pooling-cache",
    "--summary",
    "all",
]

for f, h in hashes.items():
    assert hashlib.sha256((root / f).read_bytes()).hexdigest() == h, f
r.update(argv=argv, state="running", started=time.time())
with result.with_suffix(".log").open("wb") as out:
    p = subprocess.Popen(argv, cwd=root / "zig", stdout=out, stderr=subprocess.STDOUT)
    r["compiler_pid"] = p.pid
    result.write_text(json.dumps(r, indent=2))
    r["exit_code"] = p.wait()
r.update(state="terminal", finished=time.time())
result.write_text(json.dumps(r, indent=2))
print(r["exit_code"], flush=True)
