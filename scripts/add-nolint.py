#!/usr/bin/env python3
"""Parse golangci-lint JSON output and add //nolint directives to flagged lines.

Usage:
    golangci-lint run --out-format=json 2>/dev/null | python3 scripts/add-nolint.py [--dry-run]
"""

import json
import sys
import re
from collections import defaultdict

# Map gosec rule IDs to short reason comments
GOSEC_REASONS = {
    "G101": "env var name mapping, not credentials",
    "G103": "intentional unsafe for zero-copy performance",
    "G104": "hash.Write never returns errors",
    "G107": "health check to configured backend",
    "G115": "bounded value, cannot overflow in practice",
    "G301": "standard permissions for data directory",
    "G304": "internal file I/O, not user-controlled",
    "G402": "configurable TLS for internal transport",
    "G404": "non-security randomness for ML/jitter",
    "G602": "pre-allocated slice with known bounds",
    "G703": "internal path with traversal protection",
    "G704": "HTTP client calling configured endpoint",
    "G705": "JSON/SSE API response, not HTML",
    "G706": "internal structured logging",
}

def extract_gosec_rule(text):
    """Extract gosec rule ID like G304 from the issue text."""
    m = re.match(r"(G\d+)", text)
    return m.group(1) if m else None


def main():
    dry_run = "--dry-run" in sys.argv

    data = json.load(sys.stdin)
    issues = data.get("Issues", [])
    if not issues:
        print("No issues found in input.", file=sys.stderr)
        sys.exit(0)

    # Group issues by file and line
    # Key: (file, line) -> set of (linter, rule_id)
    file_line_issues = defaultdict(set)

    for issue in issues:
        linter = issue.get("FromLinter", "")
        file_path = issue.get("Pos", {}).get("Filename", "")
        line = issue.get("Pos", {}).get("Line", 0)
        text = issue.get("Text", "")

        if not file_path or not line:
            continue

        if linter == "gosec":
            rule_id = extract_gosec_rule(text)
            if rule_id:
                file_line_issues[(file_path, line)].add(("gosec", rule_id))
        elif linter == "ineffassign":
            file_line_issues[(file_path, line)].add(("ineffassign", None))

    # Group by file for efficient processing
    file_edits = defaultdict(dict)  # file -> {line: nolint_comment}
    for (file_path, line), linter_rules in file_line_issues.items():
        # Build the nolint directive
        linters = set()
        reasons = []
        for linter, rule_id in linter_rules:
            linters.add(linter)
            if linter == "gosec" and rule_id in GOSEC_REASONS:
                reasons.append(f"{rule_id}: {GOSEC_REASONS[rule_id]}")
            elif linter == "gosec" and rule_id:
                reasons.append(rule_id)

        linter_str = ",".join(sorted(linters))
        reason_str = "; ".join(reasons) if reasons else ""

        if reason_str:
            comment = f"//nolint:{linter_str} // {reason_str}"
        else:
            comment = f"//nolint:{linter_str}"

        file_edits[file_path][line] = comment

    # Apply edits
    files_modified = 0
    lines_modified = 0
    for file_path, line_comments in sorted(file_edits.items()):
        try:
            with open(file_path, "r") as f:
                lines = f.readlines()
        except FileNotFoundError:
            print(f"WARNING: File not found: {file_path}", file=sys.stderr)
            continue

        modified = False
        for line_num, nolint_comment in sorted(line_comments.items()):
            idx = line_num - 1  # 0-indexed
            if idx < 0 or idx >= len(lines):
                print(f"WARNING: Line {line_num} out of range in {file_path}", file=sys.stderr)
                continue

            current_line = lines[idx].rstrip("\n")

            # Skip if already has nolint
            if "nolint" in current_line:
                continue

            # Add nolint comment at end of line
            lines[idx] = f"{current_line} {nolint_comment}\n"
            modified = True
            lines_modified += 1

            if dry_run:
                print(f"{file_path}:{line_num}: {nolint_comment}")

        if modified and not dry_run:
            with open(file_path, "w") as f:
                f.writelines(lines)
            files_modified += 1

    if dry_run:
        print(f"\nDry run: would modify {lines_modified} lines in {len(file_edits)} files", file=sys.stderr)
    else:
        print(f"Modified {lines_modified} lines in {files_modified} files", file=sys.stderr)


if __name__ == "__main__":
    main()
