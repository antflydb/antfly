import pathlib, json, subprocess, time, os, hashlib

root = pathlib.Path(
    "/Users/ajroetker/go/src/github.com/antflydb/antfly/.worktrees/pooling-completion-task"
)
result = pathlib.Path("/private/tmp/antfly-completion-task-linux.json")
assert not result.exists()
argv = json.loads(
    pathlib.Path(
        "/private/tmp/antfly-pooling-integration-candidate-linux.json"
    ).read_text()
)["argv"]
argv = [
    x.replace(
        "/.worktrees/pooling-integration-candidate,",
        "/.worktrees/pooling-completion-task,",
    ).replace(
        "/private/tmp/antfly-parent-phase-linux-cache,",
        "/private/tmp/antfly-worker-phase-linux-cache,",
    )
    for x in argv
]
argv[argv.index("--name") + 1] = "antfly-completion-task-linux"
argv[argv.index("--memory") + 1] = "3g"
argv[argv.index("--memory-swap") + 1] = "3g"
i = argv.index("antfly-inference-provider-context-test")
j = argv.index("-Doptimize=ReleaseSafe")
argv[i:j] = [
    "antfly-completion-task-test",
    "antfly-sql-write-team-test",
    "antfly-inference-worker-owner-handoff-test",
    "antfly-inference-owner-handoff-test",
    "antfly-http-cleanup-owner-test",
]
argv[argv.index("--maxrss") + 1] = "2147483648"
files = list(
    json.loads(
        pathlib.Path(
            "/private/tmp/antfly-owned-cleanup-owner-linux-qualified.json"
        ).read_text()
    )["source_sha256"]
)
files.append("zig/pkg/antfly-embedded/src/api/table_writes.zig")
files.extend(
    [
        "zig/pkg/antfly/src/api/runnable_write_team.zig",
        "zig/pkg/antfly/src/api/runnable_write_team_test_root.zig",
        "zig/pkg/antfly/src/common/allocation_budget.zig",
    ]
)
files.extend(
    [
        "zig/pkg/antfly/src/api/distributed_txn.zig",
        "zig/pkg/antfly-embedded/src/api/table_write_source.zig",
        "zig/pkg/antfly-embedded/src/api/operation.zig",
        "zig/pkg/antfly/src/api/table_writes.zig",
        "zig/pkg/antfly/src/api/sql_execution.zig",
        "zig/pkg/antfly/src/api/sql_session.zig",
        "zig/pkg/antfly/src/api/http_server.zig",
        "zig/pkg/antfly/src/api/httpx_handler.zig",
        "zig/pkg/antfly/src/api/sql_pgwire.zig",
        "zig/pkg/antfly-embedded/src/table_write_execution_test_root.zig",
    ]
)
files.extend(
    [
        "zig/pkg/antfly/src/api/runnable_completion_task.zig",
        "zig/pkg/antfly/src/api/runnable_completion_task_test_root.zig",
        "zig/pkg/antfly/build/api_tests.zig",
    ]
)
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
