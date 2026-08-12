#!/usr/bin/env python3
"""Backport correct max_rss wake-up accounting to Zig 0.16's build runner."""

from __future__ import annotations

import argparse
from pathlib import Path


OLD_WAKE_LOOP = """\
            while (run.memory_blocked_steps.getLastOrNull()) |candidate| {
                if (run.available_rss < candidate.max_rss) break;
                assert(run.memory_blocked_steps.pop() == candidate);
                dispatch_set.appendAssumeCapacity(candidate);
            }
"""

NEW_WAKE_LOOP = """\
            var retained_count: usize = 0;
            for (run.memory_blocked_steps.items) |candidate| {
                if (candidate.max_rss <= run.available_rss) {
                    run.available_rss -= candidate.max_rss;
                    dispatch_set.appendAssumeCapacity(candidate);
                } else {
                    run.memory_blocked_steps.items[retained_count] = candidate;
                    retained_count += 1;
                }
            }
            run.memory_blocked_steps.shrinkRetainingCapacity(retained_count);
"""


def patch_build_runner(source: Path, destination: Path) -> str:
    contents = source.read_text(encoding="utf-8")
    old_occurrences = contents.count(OLD_WAKE_LOOP)
    new_occurrences = contents.count(NEW_WAKE_LOOP)
    if old_occurrences == 1 and new_occurrences == 0:
        patched = contents.replace(OLD_WAKE_LOOP, NEW_WAKE_LOOP)
        result = "patched"
    elif old_occurrences == 0 and new_occurrences == 1:
        # Accept an upstream runner that already contains the corrected
        # accounting. Keeping a copied runner makes the CI/release CLI stable
        # across the transition to a fixed Zig toolchain.
        patched = contents
        result = "already-fixed"
    else:
        raise RuntimeError(
            "expected exactly one known max_rss wake loop in "
            f"{source}, found old={old_occurrences} fixed={new_occurrences}; "
            "do not apply this backport to an unknown Zig build runner"
        )

    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(patched, encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path, help="Zig 0.16 build_runner.zig")
    parser.add_argument("destination", type=Path, help="path for the patched copy")
    args = parser.parse_args()
    patch_build_runner(args.source, args.destination)


if __name__ == "__main__":
    main()
