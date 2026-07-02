#!/usr/bin/env bash
# Resolve a TLA+ check into (generated cfg path, spec path) for TLC.
#
# Layout convention: each model <Model>.tla lives beside a <Model>.cfgs file
# holding ALL of its checks as named sections in verbatim TLC config syntax:
#
#   ==== positive
#   SPECIFICATION ...
#   ==== BadSomething
#   SPECIFICATION ...
#
# Section names are the historical per-check config basenames minus the
# model prefix ("positive" for the model's own name, "BadX" for mutants,
# "heavy-depth"/"safety" for variants), so Makefile call sites are unchanged.
#
# Vendored/legacy specs (etcdraft family, occ-2pc) keep plain checked-in
# .cfg files; if no .cfgs file exists the check base's .cfg is used as-is.
#
# Usage: tla-cfg.sh <check-base> <module> [spec-module]
#   spec-module: optional module whose .tla should be run instead of the cfg
#   owner's (MC wrapper pattern, e.g. check AntflyTransaction spec MC).
# Prints: <cfg-path> <spec-path>

set -euo pipefail

SPEC_ROOT="${SPEC_ROOT:-specs/tla}"
GEN_DIR="${SPEC_ROOT}/.generated"

check="$1"
module="$2"
spec_module="${3:-$2}"

owner="$(find "${SPEC_ROOT}" -name "${module}.tla" -not -path "*/.generated/*" | head -1)"
if [ -z "$owner" ]; then
    echo "tla-cfg: module ${module}.tla not found under ${SPEC_ROOT}" >&2
    exit 1
fi
dir="$(dirname "$owner")"
if [ "$spec_module" = "$module" ]; then
    spec="$owner"
else
    spec="$(find "${SPEC_ROOT}" -name "${spec_module}.tla" -not -path "*/.generated/*" | head -1)"
    if [ -z "$spec" ]; then
        echo "tla-cfg: spec module ${spec_module}.tla not found under ${SPEC_ROOT}" >&2
        exit 1
    fi
fi

cfgs="${dir}/${module}.cfgs"
if [ ! -f "$cfgs" ]; then
    # Legacy layout: standalone .cfg next to the spec.
    legacy="${dir}/${check}.cfg"
    if [ ! -f "$legacy" ]; then
        echo "tla-cfg: neither ${cfgs} nor ${legacy} exists" >&2
        exit 1
    fi
    echo "$legacy $spec"
    exit 0
fi

section="${check#"${module}"}"
section="${section#-}"
[ -z "$section" ] && section="positive"

mkdir -p "$GEN_DIR"
out="${GEN_DIR}/${check}.cfg"
awk -v want="$section" '
    /^==== / { insec = ($2 == want); next }
    insec { print }
' "$cfgs" > "$out"

if [ ! -s "$out" ]; then
    echo "tla-cfg: section '${section}' not found in ${cfgs}" >&2
    exit 1
fi
echo "$out $spec"
