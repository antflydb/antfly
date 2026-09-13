"""Run existing qualification gates and retain scoped, versioned evidence.

Plans are trusted executable configuration, not downloaded evidence. The runner
never reimplements model validators or promotes contract tests into hardware proof.
"""

import argparse
import hashlib
import json
import math
import os
import platform
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

PLAN_SCHEMA = "antfly.pdf.qualification_plan.v1"
REPORT_SCHEMA = "antfly.pdf.qualification_run.v1"
GATE_SCHEMA = "antfly.pdf.qualification_gate.v1"
LAYERS = ("model", "execution", "document")
KINDS = ("contract", "hardware", "performance")


def read_json(path):
    def reject(value):
        raise ValueError(f"non-finite JSON number: {value}")

    return json.loads(path.read_text(), parse_constant=reject)


def save(path, payload):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n"
    )
    temporary.replace(path)


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def expand(value, variables):
    def replace(match):
        name = match[1]
        if name not in variables:
            raise ValueError(f"missing variable: {name}")
        return variables[name]

    return re.sub(r"\$\{([a-zA-Z_][a-zA-Z_0-9]*)\}", replace, value)


def validate_plan(plan):
    if (
        not isinstance(plan, dict)
        or plan.get("schema") != PLAN_SCHEMA
        or not isinstance(plan.get("gates"), list)
        or not plan["gates"]
    ):
        raise ValueError("expected a versioned plan with nonempty gates")
    seen = set()
    artifacts = plan.get("artifacts", {})
    if not isinstance(artifacts, dict) or any(
        not isinstance(value, str) for value in artifacts.values()
    ):
        raise ValueError("artifacts must map names to paths")
    for gate in plan["gates"]:
        if not isinstance(gate, dict):
            raise TypeError("gates must be objects")
        identity = gate.get("id", "")
        if (
            not isinstance(identity, str)
            or not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", identity)
            or identity in seen
        ):
            raise ValueError("gate IDs must be unique safe directory names")
        seen.add(identity)
        if not isinstance(gate.get("cwd", "${repo}"), str):
            raise TypeError(f"{identity}: cwd must be a path string")
        if gate.get("layer") not in LAYERS or gate.get("kind") not in KINDS:
            raise ValueError(f"{identity}: missing layer or evidence kind")
        if not isinstance(gate.get("scope"), str) or not gate["scope"].strip():
            raise ValueError(f"{identity}: an explicit, bounded scope is required")
        required = gate.get("artifacts", [])
        if not isinstance(required, list) or any(
            not isinstance(key, str) or key not in artifacts for key in required
        ):
            raise ValueError(f"{identity}: unknown artifact binding")
        if gate["kind"] != "contract" and not required:
            raise ValueError(
                f"{identity}: hardware/performance gates require artifact bindings"
            )
        command = gate.get("command")
        if (
            not isinstance(command, list)
            or not command
            or any(not isinstance(arg, str) or not arg for arg in command)
        ):
            raise ValueError(f"{identity}: command must be a nonempty argv array")
        timeout = gate.get("timeout_seconds", 1800)
        if (
            type(timeout) not in (int, float)
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValueError(f"{identity}: invalid timeout")
        result = gate.get("report")
        if result is not None:
            if not isinstance(result, dict) or not all(
                isinstance(result.get(key), str) and result[key]
                for key in ("path", "schema", "pass_field")
            ):
                raise ValueError(
                    f"{identity}: report needs path, schema and pass_field"
                )
        # Legacy shell/compiled gates publish a versioned exit-code envelope;
        # opting in explicitly prevents accidentally dropping a JSON verdict.
        elif gate.get("legacy_exit_code") is not True:
            raise ValueError(
                f"{identity}: report or explicit legacy_exit_code required"
            )


def snapshot(paths):
    result = {}
    for label, value in sorted(paths.items()):
        path = Path(value).resolve(strict=True)
        files = sorted(path.rglob("*")) if path.is_dir() else [path]
        entries = []
        for file in files:
            if file.is_symlink() and file.is_dir():
                raise ValueError(
                    f"artifact directory symlink must be bound explicitly: {file}"
                )
            if file.is_file():
                entries.append(
                    {
                        "path": (
                            str(file.relative_to(path)) if path.is_dir() else file.name
                        ),
                        "bytes": file.stat().st_size,
                        "sha256": sha256(file),
                    }
                )
        if not entries:
            raise ValueError(f"artifact {label} is empty")
        result[label] = {"path": str(path), "files": entries}
    return result


def source_state(repo):
    def git(*args):
        return subprocess.check_output(
            ["git", "-C", str(repo), *args], text=True
        ).strip()

    untracked = (
        subprocess.check_output(
            ["git", "-C", str(repo), "ls-files", "--others", "--exclude-standard", "-z"]
        )
        .decode()
        .split("\0")
    )
    return {
        "revision": git("rev-parse", "HEAD"),
        "status": git("status", "--porcelain", "--untracked-files=normal"),
        "untracked_sha256": {
            name: sha256(repo / name)
            for name in untracked
            if name and (repo / name).is_file()
        },
        "diff_sha256": hashlib.sha256(
            subprocess.check_output(
                ["git", "-C", str(repo), "diff", "HEAD", "--binary"]
            )
        ).hexdigest(),
    }


def run_command(command, cwd, log_path, timeout):
    if os.name != "posix":
        raise ValueError("qualification process-group cleanup requires POSIX")
    started = time.monotonic()
    with log_path.open("wb") as log:
        child = subprocess.Popen(
            command,
            cwd=cwd,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        timed_out = False
        try:
            try:
                code = child.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                code = None
        finally:
            # Only this invocation's process group; never kill by executable name.
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()
    return {
        "returncode": code,
        "timed_out": timed_out,
        "seconds": time.monotonic() - started,
    }


def run_gate(gate, repo, folder, variables):
    folder = folder.resolve()
    folder.mkdir()
    values = {
        **variables,
        "repo": str(repo),
        "run": str(folder),
        "python": sys.executable,
    }
    result = {
        "schema": GATE_SCHEMA,
        "id": gate["id"],
        "layer": gate["layer"],
        "kind": gate["kind"],
        "scope": gate["scope"],
        "pass": False,
    }
    try:
        command = [expand(arg, values) for arg in gate["command"]]
        cwd = Path(expand(gate.get("cwd", "${repo}"), values)).resolve(strict=True)
        result["command"] = command
        result["cwd"] = str(cwd)
        spec = gate.get("report")
        report_path = None
        if spec:
            report_path = Path(expand(spec["path"], values)).resolve()
            if not report_path.is_relative_to(folder) or report_path.exists():
                raise ValueError(
                    "gate reports must be fresh files inside their run directory"
                )
        result.update(
            run_command(
                command, cwd, folder / "command.log", gate.get("timeout_seconds", 1800)
            )
        )
        result["pass"] = result["returncode"] == 0 and not result["timed_out"]
        if report_path:
            if (
                not report_path.resolve().is_relative_to(folder)
                or report_path.is_symlink()
            ):
                raise ValueError("native report escaped its fresh run directory")
            payload = read_json(report_path)
            result["report"] = {
                "path": str(report_path.relative_to(folder)),
                "sha256": sha256(report_path),
            }
            if (
                not isinstance(payload, dict)
                or payload.get(spec.get("schema_field", "schema")) != spec["schema"]
            ):
                raise ValueError("unexpected native report schema")
            verdict = payload.get(spec["pass_field"])
            if type(verdict) is not bool:
                raise ValueError("native report verdict must be a boolean")
            result["pass"] = result["pass"] and verdict
        else:
            result["legacy_exit_code"] = True
    except KeyboardInterrupt:
        result.update({"pass": False, "interrupted": True})
        save(folder / "gate.json", result)
        raise
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        result.update({"pass": False, "error": str(exc)})
    save(folder / "gate.json", result)
    return result


def run_plan(plan, repo, out, variables, only=None):
    validate_plan(plan)
    out = out.resolve()
    repo = repo.resolve(strict=True)
    # A fresh directory forbids stale successful reports from previous attempts.
    out.mkdir(parents=True, exist_ok=False)
    save(out / "plan.json", plan)
    before = source_state(repo)
    report = {
        "schema": REPORT_SCHEMA,
        "pass": False,
        "complete": False,
        "source": before,
        "host": {"platform": platform.platform(), "machine": platform.machine()},
        "gates": [],
        "limitations": plan.get("limitations", []),
        "claim": "Only the listed gate scopes; not universal model, hardware or PDF qualification.",
    }
    save(out / "summary.json", report)
    try:
        required = {
            key
            for gate in plan["gates"]
            if only is None or gate["layer"] == only
            for key in gate.get("artifacts", [])
        }
        paths = {
            key: expand(plan["artifacts"][key], {**variables, "repo": str(repo)})
            for key in required
        }
        report["artifacts"] = snapshot(paths)
        for gate in plan["gates"]:
            if only is not None and gate["layer"] != only:
                report["gates"].append(
                    {
                        "id": gate["id"],
                        "layer": gate["layer"],
                        "kind": gate["kind"],
                        "scope": gate["scope"],
                        "pass": False,
                        "not_run": True,
                    }
                )
            else:
                print(
                    f"Qualifying {gate['id']} ({gate['layer']}/{gate['kind']})",
                    flush=True,
                )
                report["gates"].append(
                    run_gate(gate, repo, out / gate["id"], variables)
                )
            save(out / "summary.json", report)
        report["artifacts_unchanged"] = report["artifacts"] == snapshot(paths)
        report["source_unchanged"] = before == source_state(repo)
        report["complete"] = not any(row.get("not_run") for row in report["gates"])
        report["layers"] = {
            layer: {
                "selected": sum(
                    row["layer"] == layer and not row.get("not_run")
                    for row in report["gates"]
                ),
                "pass": any(row["layer"] == layer for row in report["gates"])
                and all(
                    row["pass"] for row in report["gates"] if row["layer"] == layer
                ),
            }
            for layer in LAYERS
        }
        report["pass"] = (
            report["complete"]
            and all(row["pass"] for row in report["gates"])
            and report["artifacts_unchanged"]
            and report["source_unchanged"]
        )
        report["all_layers_represented"] = all(
            row["selected"] > 0 for row in report["layers"].values()
        )
    except KeyboardInterrupt:
        report["interrupted"] = True
        raise
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        report["error"] = str(exc)
    finally:
        save(out / "summary.json", report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--repo", type=Path, default=Path(__file__).resolve().parents[3]
    )
    parser.add_argument("--var", action="append", default=[], metavar="NAME=VALUE")
    parser.add_argument(
        "--only", choices=LAYERS, help="Run one layer; omitted gates remain unqualified"
    )
    args = parser.parse_args(argv)
    variables = {}
    for item in args.var:
        key, separator, value = item.partition("=")
        if (
            not separator
            or not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", key)
            or key in ("repo", "run", "python")
            or key in variables
        ):
            parser.error(
                "variables must be unique NAME=VALUE pairs; repo/run/python are reserved"
            )
        variables[key] = value

    def interrupt(signum, _frame):
        raise KeyboardInterrupt(signum)

    # Batch launchers may inherit SIGINT=SIG_IGN. Establish explicit handlers so
    # both Ctrl-C and orchestrator SIGTERM unwind the owned process-group lease.
    previous = {
        signum: signal.signal(signum, interrupt)
        for signum in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        report = run_plan(
            read_json(args.plan), args.repo, args.output, variables, args.only
        )
    except (OSError, ValueError, TypeError, subprocess.SubprocessError) as exc:
        parser.exit(2, f"qualification: {exc}\n")
    except KeyboardInterrupt as exc:
        signum = (
            exc.args[0] if exc.args and isinstance(exc.args[0], int) else signal.SIGINT
        )
        parser.exit(
            128 + signum, "qualification interrupted; non-passing evidence retained\n"
        )
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
    return 0 if report["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
