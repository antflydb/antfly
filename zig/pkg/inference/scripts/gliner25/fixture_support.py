"""Verify explicitly omitted fixtures when restored; never download test data."""
from __future__ import annotations

from pathlib import Path
import unittest

import oracle


def external_inventory():
    manifest = oracle.read_json(oracle.FIXTURES / "reference_manifest.json")
    if manifest.get("format_version") != 1 or manifest.get("upstream_commit") != oracle.UPSTREAM_COMMIT:
        raise oracle.ContractError("external fixtures require the pinned reference manifest")
    external = manifest["external_files"]
    if not isinstance(external, dict) or external.keys() & manifest["files"].keys():
        raise oracle.ContractError("external and checked-in fixture inventories must be disjoint")
    for name, pin in external.items():
        path = Path(name)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != name or not path.parts:
            raise oracle.ContractError("unsafe external fixture path")
        if (set(pin) != {"size_bytes", "sha256"} or type(pin["size_bytes"]) is not int
                or pin["size_bytes"] <= 0 or not isinstance(pin["sha256"], str)
                or len(pin["sha256"]) != 64 or any(c not in "0123456789abcdef" for c in pin["sha256"])):
            raise oracle.ContractError("invalid external fixture identity")
    return external


def verify_external_fixtures():
    verified, missing = {}, []
    for name, pin in external_inventory().items():
        path = oracle.FIXTURES / name
        if not path.exists() and not path.is_symlink():
            missing.append(name)
        else:
            verified[name] = oracle.verify_file(path, pin)
    return {"files": verified, "missing": missing, "complete": not missing}


def require_fixtures(*names):
    """Skip only absent declared external inputs; reject every present mismatch."""
    external = external_inventory()
    missing = []
    for name in names:
        path = oracle.FIXTURES / name
        if name not in external:
            raise oracle.ContractError(f"fixture is not declared external: {name}")
        if not path.exists() and not path.is_symlink():
            missing.append(name)
        else:
            oracle.verify_file(path, external[name])
    if missing:
        raise unittest.SkipTest("GLiNER2.5 external fixtures omitted; restore pinned files: " + ", ".join(missing))


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-all", action="store_true", help="fail if any external fixture is unavailable")
    args = parser.parse_args()
    oracle.verify_config_fixtures()
    oracle.verify_reference_fixtures()
    report = verify_external_fixtures()
    print(json.dumps(report, indent=2, sort_keys=True))
    raise SystemExit(1 if args.require_all and not report["complete"] else 0)
