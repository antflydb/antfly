#!/usr/bin/env python3
"""Metadata-only checks for the unapplied real-HiGHS driver."""
from __future__ import annotations
import hashlib
import json
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DRIVER = ROOT / "attention_mass_highs_v180_real_fixture_driver_v3.zig"
DESIGN = ROOT / "attention-mass-highs-v180-real-fixture-driver-v3.md"
ADAPTER = ROOT / "attention_mass_highs_v180_adapter_v3.zig"
CG = ROOT / "attention_mass_constraint_generation_v5.zig"
LP = Path("/private/tmp/attention_mass_lp_v4.zig")

def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()

def require(value: bool, message: str) -> None:
    if not value:
        raise AssertionError(message)

def main() -> None:
    source = DRIVER.read_text()
    design = DESIGN.read_text()
    require("extern fn Highs_create()" in source, "missing real C ABI")
    require("extern fn Highs_githash()" in source, "missing linked commit check")
    require('args[3], "--fixture-sha256"' in source and 'args[7], "--library-receipt-sha256"' in source, "external manifest hashes not required")
    require('args[11], "--executable-receipt-sha256"' in source, "executable receipt digest not required")
    require(source.count("readJsonExpected(") == 7, "initial and terminal receipt checks missing")
    require(source.count("verifyAllFixtureEdges(") == 3, "fixture edge checks missing")
    require(source.count("verifyLibraryClosure(") == 3, "library closure checks missing")
    require(source.index("terminal_fixture") < source.index("createDir(io, args[14]"), "output created before terminal rehash")
    require("runtime_githash" in source and "highs_no_default_threads" in source, "runtime hash or thread policy absent")
    require("receipt.source.commit, highs.pinned_highs_commit" in source, "full source commit not enforced")
    require("std.mem.span(Highs_githash()), library.value.source.runtime_githash" in source, "generated runtime hash not enforced")
    require("cg.original_feasibility_tolerance" in source and "cg.retry_feasibility_tolerance" in source, "retry tolerances drifted")
    require(source.count("runAttempt(allocator") == 2, "must have two attempt call sites")
    require("if (!first.witness_rejected)" in source, "retry is not witness-only")
    require("if (retry.witness_rejected or retry.audit == null)" in source, "retry witness not mandatory")
    require(".creates_attempted != 2" in source and "lifecycle.destroys != 2" in source, "fresh lifecycle not closed")
    require("expected_vs_observed" in source and "retry_vector_oracle_available = false" in source, "expected/observed scope missing")
    require("persistent_model_or_basis_reuse = false" in source, "persistent reuse entered experiment")
    require("createDir(io, args[14]" in source and ".{ .exclusive = true }" in source, "output not fresh/exclusive")
    require("all 1128 exported restricted rows; not the unexported full constraint-generation matrix" in source, "scope overstated")
    require("No command below has been executed." in design, "execution status unclear")
    require("1 GiB process-group RSS" in design and "zero swap growth" in design, "bounded run missing")
    require(sha(ADAPTER) == "493637b21fec53d07813fddac5cc874d232ae6680befe504bf639e9e3e6c5525", "adapter changed")
    require(sha(CG) == "abdcc63e7d914b174f3fb8db2302db16af126386b610fcf6e80bf1dbb2ec82fa", "controller changed")
    require(sha(LP) == "ff1d98cab21450271694ab524dd0733294ae0ff15793c57ed105f4d18d9354de", "LP changed")
    # Exercise the terminal mutation premise without loading the numerical
    # fixture or solver: an initially valid edge must reject after same-path
    # byte substitution, including a same-length substitution.
    with tempfile.TemporaryDirectory() as raw:
        edge = Path(raw) / "edge"
        edge.write_bytes(b"first")
        expected = (len(edge.read_bytes()), sha(edge))
        require((edge.stat().st_size, sha(edge)) == expected, "initial edge invalid")
        edge.write_bytes(b"other")
        require((edge.stat().st_size, sha(edge)) != expected, "same-length mutation survived")
    print(json.dumps({"completed": True, "tests": "2/2", "driver": sha(DRIVER), "design": sha(DESIGN)}, sort_keys=True))

if __name__ == "__main__":
    main()
