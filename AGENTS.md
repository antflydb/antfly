# AGENTS.md

Guidance for coding agents (Claude Code, Codex, and others) working in this repository. Project layout, commands, and testing live in `CLAUDE.md`.

## Worktrees

- Do branch work in a git worktree under `.worktrees/` at the repository root, never in `/tmp`, `~/.cache`, or a sibling directory:

  ```bash
  git fetch origin
  git worktree add -b <branch> .worktrees/<name> origin/main
  ```

- Name the directory after the branch (`fix/foo-bar` → `.worktrees/fix-foo-bar`).
- When the branch is merged or abandoned, run `git worktree remove .worktrees/<name>` and `git worktree prune`.

## Build caches

Zig and Cargo never evict build output; one throwaway Zig cache reached 150 GB.

- Use the worktree's own caches: `.zig-cache/` and `zig-out/` for Zig, `rs/target/` for Rust. Never point `--cache-dir`, `ZIG_LOCAL_CACHE_DIR`, or `CARGO_TARGET_DIR` outside the worktree.
- Never create a temporary cache directory (`mktemp`, `~/.cache/antfly-*`, `/tmp/...`) for builds or tests. If a task needs an isolated cache, put it inside the worktree and delete it before finishing.
- Reuse one `zig-out` prefix instead of accumulating `zig-out-<variant>` directories.
- Run `make gc` after long build or test sessions and whenever disk space is low (`make gc DRY_RUN=1` previews). It cleans every worktree plus the Zig, Go, uv, and Node caches but skips anything a running build wrote in the last 30 minutes; see `scripts/cache_gc.sh` for the rules.
