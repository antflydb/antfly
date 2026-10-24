import hashlib, json, re, struct, subprocess, sys
from pathlib import Path

records = json.loads(Path("/evidence/artifacts.json").read_text())
expected = {
    "default-workload-pooling-tests-local": 36,
    "default-workload-pooling-tests": 39,
}
assert len(records) == 2 and {Path(r["path"]).name for r in records} == set(expected)
for record in records:
    data = Path(record["path"]).read_bytes()
    assert data[:4] == b"\x7fELF" and struct.unpack_from("<H", data, 18)[0] == 183
    assert hashlib.sha256(data).hexdigest() == record["sha256"]
    print("Exact artifact:", record["path"], record["sha256"], flush=True)
    result = subprocess.run(
        [record["path"], *record["args"]],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    sys.stdout.buffer.write(result.stdout)
    sys.stdout.flush()
    print("Artifact exit status:", result.returncode, flush=True)
    if result.returncode:
        sys.exit(result.returncode)
    count = expected[Path(record["path"]).name]
    assert f"{count} passed; 0 skipped; 0 failed; 0 leaked.".encode() in result.stdout
    if count == 39:
        assert (
            b"api.httpx_handler.test.httpx SQL executes one relational page with exact integer parameters..."
            in result.stdout
        )
        assert (
            b"api.httpx_handler.test.SQL protected executor reserves bounded workers and rolls back exhaustion..."
            in result.stdout
        )
    else:
        assert (
            b"common.runnable_scheduler.test.runnable scheduler weighted sharing charges slots across busy classes..."
            in result.stdout
        )
print("Exact Linux API runtime: 75 passed, zero skips/failures/leaks", flush=True)
