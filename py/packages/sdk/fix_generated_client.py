#!/usr/bin/env python3
"""Fail-closed fixes for known openapi-python-client 0.28 multi-body output."""

from __future__ import annotations

import re
import sys
from pathlib import Path

FILES = {
    Path("api/query_operations/global_query.py"): 5,
    Path("api/query_operations/query_table.py"): 5,
}
# The public HTTP operations intentionally expose the stateful compatibility
# envelope, while the SDK's primary QueryRequest model remains canonical.
# Keep this post-generation check tied to the endpoint body contract rather
# than the canonical model's historical name.
REQUIRED_BODY = re.compile(
    r"(body:\s+(?:GlobalStatefulQueryRequest|StatefulQueryRequest)\s+\|\s+File)"
    r"\s+\|\s+Unset\s*=\s*UNSET"
)
NDJSON_HEADER = 'headers["Content-Type"] = "application/x-ndjson"'


def fix_generated_client(root: Path) -> None:
    updates: dict[Path, str] = {}
    for relative, expected in FILES.items():
        path = root / relative
        source = path.read_text(encoding="utf-8")
        count = len(REQUIRED_BODY.findall(source))
        if count != expected or source.count(NDJSON_HEADER) != 1:
            raise RuntimeError(
                f"unexpected generated shape in {relative}: "
                f"required-body signatures={count}, NDJSON headers={source.count(NDJSON_HEADER)}"
            )
        updates[path] = REQUIRED_BODY.sub(r"\1", source)

    for path, source in updates.items():
        path.write_text(source, encoding="utf-8")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {Path(sys.argv[0]).name} GENERATED_CLIENT_ROOT")
    fix_generated_client(Path(sys.argv[1]))
