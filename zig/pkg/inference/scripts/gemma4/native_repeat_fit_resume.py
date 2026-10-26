#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run and assemble immutable, resumable native-repeat whole-head fits.

Each invocation runs exactly one explicitly selected physical head in a fresh
attempt directory.  Failed and interrupted attempts remain diagnostic evidence;
retries use a new six-digit attempt id.  Terminal results are written only from
an explicit, complete selection of eight independently revalidated seals.
"""
import argparse
import json
from pathlib import Path
import re

import native_repeat_campaign as campaign
import native_repeat_attention_matching as preparer
import native_repeat_profile as profile
import native_repeat_training_authority as authority


RESULT_SCHEMA = "antfly.context.native.repeat-resumable-results.v1"
SEAL_SCHEMA = "antfly.context.native.resumable-head-fit-seal.v1"
SELECTION_SCHEMA = "antfly.context.native.repeat-fit-selection.v1"
FIT_LAYOUT = "immutable_attempt_v1"
ATTEMPT = re.compile(r"[0-9]{6}")


def require(ok, reason):
    if not ok:
        raise ValueError(reason)


def _attempt_id(value):
    require(isinstance(value, str) and ATTEMPT.fullmatch(value),
            "attempt id must be exactly six decimal digits")
    return value


def _coordinates(spec):
    rows = [(owner["owner_layer"], head)
            for owner in spec["expected_owners"]
            for head in range(owner["source_heads"])]
    require(len(rows) == 8 and len(set(rows)) == 8,
            "resumable fit requires exactly eight physical heads")
    return rows


def _context(spec, spec_sha256, output):
    """Revalidate the complete pre-fit graph without reading query numerics."""
    output = Path(output)
    capture_path = output / "capture-phase.json"
    capture = campaign._phase_contract(
        campaign.json_checked(capture_path, campaign.sha(capture_path)),
        "capture", spec_sha256)
    coverage = output / "coverage"
    require(coverage.is_dir() and not coverage.is_symlink(),
            "missing canonical coverage directory")
    campaign._bound_source(capture, "plan_phase", output / "plan-phase.json")
    campaign._bound_source(capture, "resource", coverage / "capture-resource.json")
    campaign._bound_source(capture, "source_index",
        coverage / (spec["capture_stem"] + "-native-head-index.json"))
    campaign._bound_source(capture, "repeat_source",
        coverage / (spec["capture_stem"] + "-native-repeat-index.json"))
    auth_ref = capture["sources"]["training_authority"]
    auth_path = campaign._bound_source(capture, "training_authority",
                                       coverage / "training-authority.json")
    binding = authority.validate_authority(auth_path, auth_ref["sha256"])
    coordinates = _coordinates(spec)
    require([(row["owner_layer"], row["kv_head"])
             for row in binding["plan"]["coordinates"]] == coordinates,
            "training authority coordinates differ from campaign spec")

    bootstrap, _ = campaign._load_identity(spec, spec_sha256, output)
    identity = bootstrap["identity"]
    expected_identity = {"artifact_sha256": identity["artifact"],
        "tokenizer_sha256": identity["tokenizer"],
        "template_sha256": identity["template"],
        "layout_sha256": identity["layout"],
        "implementation_sha256": identity["implementation"]}
    actual_identity = binding["authority"].get("loaded_identity")
    require(type(actual_identity) is dict and
            type(actual_identity.get("load_generation")) is int and
            actual_identity["load_generation"] > 0 and
            {key: actual_identity.get(key) for key in expected_identity} == expected_identity,
            "training authority identity differs from bootstrap")
    owner_geometry = {owner["owner_layer"]: owner for owner in spec["expected_owners"]}
    cells = binding["authority"].get("cells")
    require(type(cells) is list and
            [(cell.get("owner_layer"), cell.get("kv_head")) for cell in cells] == coordinates and
            all(cell.get("source_heads") == owner_geometry[cell["owner_layer"]]["source_heads"] and
                cell.get("head_dim") == owner_geometry[cell["owner_layer"]]["head_dim"]
                for cell in cells),
            "training authority cell geometry differs from bootstrap/spec")
    _, spec_build_sha = campaign.ref(spec["source_build_receipt"],
                                     "source build receipt")
    require(binding["digests"].get("source_build_receipt") == spec_build_sha,
            "training authority source build differs from campaign spec")
    build = campaign.validate_build(binding["paths"]["source_build_receipt"])
    require(auth_path.parent == coverage and
            build["receipt_sha256"] == spec_build_sha and
            campaign.build_for(spec)["binary_sha256"] == build["binary_sha256"] and
            campaign.sha(build["binary_path"]) == build["binary_sha256"],
            "training authority build/campaign path differs from spec")
    return {"output": output, "coverage": coverage,
            "capture_path": capture_path, "capture": capture,
            "auth_path": auth_path, "auth_ref": auth_ref,
            "binding": binding, "build": build, "coordinates": coordinates}


def _attempt_dir(context, owner_layer, kv_head, attempt_id, *, create=False):
    attempt_id = _attempt_id(attempt_id)
    coordinate = (owner_layer, kv_head)
    require(type(owner_layer) is int and type(kv_head) is int and
            coordinate in context["coordinates"], "unknown physical head coordinate")
    root = context["coverage"] / "fit-attempts"
    owner = root / f"owner{owner_layer}-head{kv_head}"
    attempt = owner / ("attempt-" + attempt_id)
    if create:
        for path in (root, owner):
            if path.exists() or path.is_symlink():
                require(path.is_dir() and not path.is_symlink(),
                        "fit attempt parent is not a real directory")
            else:
                path.mkdir()
        require(not attempt.exists() and not attempt.is_symlink(),
                "fit attempt already exists")
        attempt.mkdir()
    else:
        require(attempt.is_dir() and not attempt.is_symlink(),
                "missing immutable fit attempt")
    return attempt


def _paths(attempt):
    return {"input": attempt / "input.bin",
            "input_manifest": attempt / "input.bin.json",
            "report": attempt / "result.json",
            "candidate": attempt / "result.candidate",
            "resource": attempt / "resource.json",
            "stdout": attempt / "resource.stdout",
            "stderr": attempt / "resource.stderr",
            "seal": attempt / "seal.json"}


def _command(context, paths):
    return ["/usr/bin/env", "MTL_DEBUG_LAYER=1",
        "ANTFLY_CONTEXT_NATIVE_HEAD_INPUT=" + str(paths["input"]),
        "ANTFLY_CONTEXT_NATIVE_HEAD_SHA256=" + campaign.sha(paths["input"]),
        "ANTFLY_CONTEXT_NATIVE_HEAD_OUTPUT=" + str(paths["report"].with_suffix("")),
        str(context["build"]["binary_path"]), "--test-filter",
        "context captured native whole-head Gemma qualification"]


def _row(paths, owner_layer, kv_head, accepted_head):
    return {"owner_layer": owner_layer, "kv_head": kv_head,
        "accepted_head": accepted_head,
        "input": campaign._phase_ref(paths["input"]),
        "input_manifest": campaign._phase_ref(paths["input_manifest"]),
        "report": campaign._phase_ref(paths["report"]),
        "resource": campaign._phase_ref(paths["resource"]),
        "stdout": campaign._phase_ref(paths["stdout"]),
        "stderr": campaign._phase_ref(paths["stderr"]),
        "candidate": (campaign._phase_ref(paths["candidate"]) |
                      {"bytes": paths["candidate"].stat().st_size}
                      if paths["candidate"].exists() else None)}


def run_head(spec, spec_sha256, output, owner_layer, kv_head, attempt_id):
    """Run one explicit head. Never selects a head or retries automatically."""
    context = _context(spec, spec_sha256, output)
    attempt = _attempt_dir(context, owner_layer, kv_head, attempt_id, create=True)
    paths = _paths(attempt)
    manifest = preparer.prepare(
        repeat_training_authority=context["auth_path"],
        repeat_authority_sha256=context["auth_ref"]["sha256"],
        owner=owner_layer, kv_head=kv_head, output=paths["input"])
    command = _command(context, paths)
    guarded = campaign._run(command, paths["resource"], campaign.FIT_LIMITS,
                            numerical=True)
    report = campaign._qualification_report(paths["report"], paths["input"],
        guarded["returncode"], owner_layer, kv_head, manifest)
    campaign.validate_head_artifacts(report, paths["report"], paths["candidate"],
                                     paths["stderr"])
    row = _row(paths, owner_layer, kv_head, report["accepted_head"])
    seal = {"schema": SEAL_SCHEMA, "fit_layout": FIT_LAYOUT,
        "attempt_id": _attempt_id(attempt_id), "diagnostic_only": True,
        "serving_qualified": False, "fresh_unseen_qualified": False,
        "campaign_spec_sha256": spec_sha256,
        "capture_phase": campaign._phase_ref(context["capture_path"]),
        "training_authority": campaign._phase_ref(context["auth_path"]), **row}
    campaign.write_new(paths["seal"], seal)
    return {"accepted_head": report["accepted_head"],
            "owner_layer": owner_layer, "kv_head": kv_head,
            "attempt_id": attempt_id,
            "seal": campaign._phase_ref(paths["seal"])}


def validate_head(spec, spec_sha256, output, owner_layer, kv_head,
                  attempt_id, seal_sha256, *, context=None):
    """Return an accepted terminal row only after complete graph revalidation."""
    context = context or _context(spec, spec_sha256, output)
    attempt = _attempt_dir(context, owner_layer, kv_head, attempt_id)
    paths = _paths(attempt)
    seal = profile._json(paths["seal"], seal_sha256)
    base_fields = {"schema", "fit_layout", "attempt_id", "diagnostic_only",
        "serving_qualified", "fresh_unseen_qualified", "campaign_spec_sha256",
        "capture_phase", "training_authority", "owner_layer", "kv_head",
        "accepted_head", "input", "input_manifest", "report", "resource",
        "stdout", "stderr", "candidate"}
    require(type(seal) is dict and set(seal) == base_fields and
            seal["schema"] == SEAL_SCHEMA and seal["fit_layout"] == FIT_LAYOUT and
            seal["attempt_id"] == _attempt_id(attempt_id) and
            seal["diagnostic_only"] is True and seal["serving_qualified"] is False and
            seal["fresh_unseen_qualified"] is False and
            seal["campaign_spec_sha256"] == spec_sha256 and
            seal["capture_phase"] == campaign._phase_ref(context["capture_path"]) and
            seal["training_authority"] == campaign._phase_ref(context["auth_path"]) and
            seal["owner_layer"] == owner_layer and seal["kv_head"] == kv_head and
            seal["accepted_head"] is True,
            "invalid or non-accepted immutable head seal")
    for name in ("input", "input_manifest", "report", "resource", "stdout", "stderr"):
        path, _ = profile._ref(attempt, seal[name], "resumable head " + name,
                               nonempty=name not in ("stdout",))
        require(path == paths[name], "noncanonical resumable head " + name)
    candidate_path, _ = profile._ref(attempt, seal["candidate"],
        "resumable head candidate", nonempty=True, with_bytes=True)
    require(candidate_path == paths["candidate"] and
            seal["candidate"]["bytes"] == candidate_path.stat().st_size,
            "noncanonical resumable head candidate")
    manifest = profile._manifest(paths["input_manifest"],
        seal["input_manifest"]["sha256"], paths["input"], owner_layer, kv_head,
        context["binding"])
    command = _command(context, paths)
    resource = campaign._resource_contract(paths["resource"], command,
                                            campaign.FIT_LIMITS, numerical=True)
    report = campaign._qualification_report(paths["report"], paths["input"],
        resource["returncode"], owner_layer, kv_head, manifest)
    artifacts = campaign.validate_head_artifacts(report, paths["report"],
                                                  paths["candidate"], paths["stderr"])
    require(report["accepted_head"] is True and
            artifacts["candidate_sha256"] == seal["candidate"]["sha256"] and
            artifacts["candidate_bytes"] == seal["candidate"]["bytes"],
            "sealed native head differs from accepted child artifacts")
    row = {key: seal[key] for key in ("owner_layer", "kv_head", "accepted_head",
        "input", "input_manifest", "report", "resource", "stdout", "stderr",
        "candidate")}
    row["attempt_id"] = attempt_id
    row["seal"] = {"path": str(paths["seal"]), "sha256": seal_sha256}
    return row


def assemble(spec, spec_sha256, output, selections):
    """Write terminal results from eight explicit accepted attempt seals."""
    context = _context(spec, spec_sha256, output)
    require(type(selections) is list and len(selections) == len(context["coordinates"]),
            "terminal assembly requires one explicit selection per physical head")
    coordinates = []
    rows = []
    for selected in selections:
        require(type(selected) is dict and set(selected) == {
            "owner_layer", "kv_head", "attempt_id", "seal_sha256"},
            "invalid resumable fit selection")
        coordinate = (selected["owner_layer"], selected["kv_head"])
        coordinates.append(coordinate)
        rows.append(validate_head(spec, spec_sha256, output, *coordinate,
            selected["attempt_id"], selected["seal_sha256"], context=context))
    require(coordinates == context["coordinates"],
            "resumable fit selections are missing, extra, or reordered")
    value = {"schema": RESULT_SCHEMA, "fit_layout": FIT_LAYOUT,
        "completed": True, "outcome": "accepted",
        "evaluation_role": "internal_development", "diagnostic_only": True,
        "serving_qualified": False, "release_authority": False,
        "full_context_memory_qualified": False, "fresh_unseen_qualified": False,
        "campaign_spec_sha256": spec_sha256,
        "capture_phase": campaign._phase_ref(context["capture_path"]),
        "training_authority": campaign._phase_ref(context["auth_path"]),
        "accepted_head_count": len(rows), "head_count": len(rows),
        "all_heads_accepted": True, "results": rows}
    destination = context["coverage"] / "results.json"
    if destination.exists() or destination.is_symlink():
        require(destination.is_file() and not destination.is_symlink(),
                "terminal native repeat results path is not a real file")
        existing = profile._json(destination, campaign.sha(destination))
        require(existing == value,
                "terminal native repeat results differ from revalidated selections")
        return value
    campaign.write_new(destination, value)
    return value


def _load_selections(path, digest, spec_sha256):
    value = profile._json(path, digest)
    require(type(value) is dict and set(value) == {
        "schema", "campaign_spec_sha256", "selections"} and
        value["schema"] == SELECTION_SCHEMA and
        value["campaign_spec_sha256"] == spec_sha256,
        "invalid resumable fit selection manifest")
    return value["selections"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("run-head", "assemble"):
        command = sub.add_parser(name)
        command.add_argument("--campaign-spec", type=Path, required=True)
        command.add_argument("--campaign-spec-sha256", required=True)
        command.add_argument("--output", type=Path, required=True)
        if name == "run-head":
            command.add_argument("--owner-layer", type=int, required=True)
            command.add_argument("--kv-head", type=int, required=True)
            command.add_argument("--attempt-id", required=True)
        else:
            command.add_argument("--selections", type=Path, required=True)
            command.add_argument("--selections-sha256", required=True)
    args = parser.parse_args(argv)
    spec = campaign.load_spec(args.campaign_spec, args.campaign_spec_sha256)
    if args.command == "run-head":
        value = run_head(spec, args.campaign_spec_sha256, args.output,
            args.owner_layer, args.kv_head, args.attempt_id)
    else:
        selections = _load_selections(args.selections, args.selections_sha256,
                                      args.campaign_spec_sha256)
        value = assemble(spec, args.campaign_spec_sha256, args.output, selections)
    print(json.dumps(value, sort_keys=True))


if __name__ == "__main__":
    main()
