import pathlib, json, subprocess, time, os, hashlib

root = pathlib.Path(
    "/Users/ajroetker/go/src/github.com/antflydb/antfly/.worktrees/pooling-completion-capability"
)
result = pathlib.Path("/private/tmp/antfly-completion-capability-abi-linux.json")
assert not result.exists()
argv = json.loads(
    pathlib.Path("/private/tmp/antfly-worker-process-output-linux.json").read_text()
)["argv"]
argv = [
    x.replace(
        "/.worktrees/pooling-worker-process-output,",
        "/.worktrees/pooling-completion-capability,",
    )
    for x in argv
]
argv[argv.index("--name") + 1] = "antfly-completion-capability-abi-linux"
argv = argv[: argv.index("build")] + [
    "test",
    "-OReleaseSafe",
    "-lc",
    "-target",
    "aarch64-linux-gnu",
    "--cache-dir",
    "/cache",
    "-Mroot=lib/runtime/src/runtime_runnable_abi.zig",
]
files = [
    "zig/lib/runtime/src/runtime_runnable_abi.zig",
    "zig/lib/runtime/src/runtime_native_abi.zig",
    "zig/lib/runtime/src/runtime_io_abi.zig",
    "zig/lib/runtime/src/runtime_http_abi.zig",
    "zig/lib/runtime/src/runtime_error_abi.zig",
]
r = {
    "argv": argv,
    "pid": os.getpid(),
    "state": "running",
    "started": time.time(),
    "source_sha256": {
        f: hashlib.sha256((root / f).read_bytes()).hexdigest() for f in files
    },
}
with result.with_suffix(".log").open("wb") as out:
    p = subprocess.Popen(argv, cwd=root / "zig", stdout=out, stderr=subprocess.STDOUT)
    r["compiler_pid"] = p.pid
    result.write_text(json.dumps(r, indent=2))
    r["exit_code"] = p.wait()
r.update(state="terminal", finished=time.time())
result.write_text(json.dumps(r, indent=2))
print(r["exit_code"], flush=True)
