# AGENTS.md

Guidance for coding agents (Claude Code, Codex, and others) working in this repository. Project architecture, commands, and testing live in `CLAUDE.md`.

## Worktrees

- Do branch work in a git worktree under `.worktrees/` at the repository root, never in `/tmp`, `~/.cache`, or a sibling directory:

  ```bash
  git fetch origin
  git worktree add -b <branch> .worktrees/<name> origin/main
  ```

- Name the directory after the branch (`fix/foo-bar` → `.worktrees/fix-foo-bar`). `.worktrees/` is gitignored.
- When the branch is merged or abandoned, remove the worktree with `git worktree remove .worktrees/<name>`, then run `git worktree prune`. Removing the worktree also removes its build caches.

## Build caches

Zig and Cargo never evict build output, so caches grow until something deletes them. A single throwaway Zig cache has reached 150 GB.

- Let builds use the worktree's own cache: `.zig-cache/` and `zig-out/` for Zig, `rs/target/` for Rust. Do not pass `--cache-dir`, `--prefix`, `ZIG_LOCAL_CACHE_DIR`, or `CARGO_TARGET_DIR` that points outside the worktree.
- Never create a temporary cache directory (`mktemp`, `~/.cache/antfly-*`, `/tmp/...`) for builds or tests. If a task truly needs an isolated cache, put it inside the worktree and delete it before finishing.
- Avoid accumulating extra output prefixes (`zig-out-<variant>`). Reuse one, and delete it when the comparison is done.
- Run `make gc` when you finish a long build or test session, and whenever disk space is low. `make gc DRY_RUN=1` shows what would be removed.

## `make gc`

`scripts/cache_gc.sh` cleans the main checkout, every worktree under `.worktrees/`, and the per-user caches:

| Cache | Action |
| --- | --- |
| Zig `.zig-cache`, `zig-out*`, `zig-local`; stray Zig caches under `~/.cache` and `$TMPDIR` | Removed when idle for `GC_MAX_AGE_DAYS` (default 3) or larger than `GC_MAX_GB` (default 20) |
| Zig global cache (`~/.cache/zig`) | Compiled output (`o/`, `h/`, `z/`, `tmp/`) removed under the same rules; fetched packages (`p/`) kept |
| Rust `rs/target` | `cargo sweep` when installed, then the same idle/size rules |
| Go | `go clean -cache` |
| Python | `uv cache prune` |
| Node | `pnpm store prune`, `npm cache verify` |

Anything written in the last `GC_ACTIVE_MINUTES` (default 30) is treated as belonging to a running build and skipped.
