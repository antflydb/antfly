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

"""Normalize the published Decide repositories without rewriting weight bytes.

Prepare reviewable metadata with --output; publish exactly that plan with
--apply. Server-side copies preserve GGUF bytes, and optimistic parent commits
reject concurrent edits. Historical evidence and its original paths are retained.
Requires huggingface_hub. Uses the configured Hugging Face credential.
"""

import argparse
import hashlib
import json
from pathlib import Path

MIGRATIONS = (
    ("gliner2.5-decide-1B-gguf", "GLiNER2.5-Decide-1B"),
    ("gliner2.5-decide-gguf", "GLiNER2.5-Decide"),
)
OLD_HEAD = "gliner_head.gguf"
NEW_HEAD = "gliner2-head.Q8_0.gguf"
ENCODER = "gliner2-encoder.Q8_0.gguf"


def json_bytes(value):
    return (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()


def pin(data):
    return {"size_bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def metadata(name, card, descriptor):
    descriptor = dict(descriptor, encoder=ENCODER, head=NEW_HEAD)
    card = card.replace(
        "antflydb/gliner2.5-decide-1B-gguf", "antflydb/GLiNER2.5-Decide-1B"
    )
    card = card.replace("antflydb/gliner2.5-decide-gguf", "antflydb/GLiNER2.5-Decide")
    card = card.replace(OLD_HEAD, NEW_HEAD)
    note = """## Canonical Antfly layout (2026-10-17)

This repository follows the upstream Fastino checkpoint name. The previous
`-gguf` repository URL redirects here. The paired filenames identify the
**Q8_0 bundle variant**: the encoder matrices are Q8_0, while the auxiliary
head remains **FP32** despite its `Q8_0` filename suffix. The weight bytes,
tokenizer, and source configuration have not changed. The bundle descriptor
and `antfly_inference_variants.json` name both required components explicitly.
Historical validation reports retain the original paths and measurements;
`gliner_head.gguf` in those reports is now `gliner2-head.Q8_0.gguf`.

"""
    if name.endswith("1B"):
        note += """This exact Q8 bundle currently serves typed Decide and classification on
Native CPU and Metal: one request item, at most 64 prepared sequences and
1024 prepared tokens per sequence including schema. Q8 has its own independent
upstream, Native, Metal, and HTTP capacity evidence. Entity and relation extraction are not
advertised: the pinned upstream FP32 span heads produce empty results in
the retained extraction investigation. Long-document graph extraction and
ModernBERT fine-tuning remain unsupported for this artifact. See
[the investigation](https://github.com/antflydb/antfly/blob/main/zig/pkg/inference/scripts/gliner25/SPAN_1B_INVESTIGATION.md)
and [issue #1083](https://github.com/antflydb/antfly/issues/1083).

The runtime patch and commands below document the original qualification.
Current Antfly main includes that runtime integration; do not reapply the
historical patch to current main. For exact reproduction against the old
base, use the pre-migration repository revision or the original head filename.

"""
    else:
        note += """This DeBERTa Q8 conversion supports ordinary extraction/classification
compatibility. It does **not** qualify the public typed Decide endpoint:
the exporter strips `decide` and `typed_decisions`. The source checkpoint's
name and the historical browser smoke test do not confer that qualification.
Use a qualified FP32 Decide checkpoint or the separate qualified 1B Q8 bundle
for typed decisions. Native/Metal full-task and long-document qualification
must be established separately for these converted bytes.

"""
    # Keep the original card and evidence intact below a current contract note.
    end = card.find("\n---", 3) if card.startswith("---") else -1
    insert = end + len("\n---\n") if end >= 0 else 0
    card = card[:insert] + "\n" + note + card[insert:]
    variants = {
        "family": "gliner2_variants/v1",
        "variants": [
            {
                "id": "gguf-Q8_0",
                "target": "gguf",
                "format": "Q8_0",
                "encoder": ENCODER,
                "head": NEW_HEAD,
            }
        ],
    }
    return {
        "README.md": card.encode(),
        "antfly_inference_bundle.json": json_bytes(descriptor),
        "antfly_inference_variants.json": json_bytes(variants),
    }


def prepare(api, output):
    from huggingface_hub import hf_hub_download

    plans = []
    for old, new in MIGRATIONS:
        repo = "antflydb/" + old
        info = api.model_info(repo, files_metadata=True)
        folder = output / new
        folder.mkdir(parents=True, exist_ok=True)
        files = {item.rfilename: item for item in info.siblings}
        if OLD_HEAD not in files or NEW_HEAD in files:
            raise ValueError(f"unexpected source layout: {repo}")

        def read(name, repo=repo, revision=info.sha):
            return Path(
                hf_hub_download(
                    repo, name, revision=revision, cache_dir=output / "cache"
                )
            ).read_bytes()

        updates = metadata(
            new,
            read("README.md").decode(),
            json.loads(read("antfly_inference_bundle.json")),
        )
        if "convert.py" in files:
            updates["convert.py"] = read("convert.py").replace(
                OLD_HEAD.encode(), NEW_HEAD.encode()
            )
        # Recompute release integrity metadata, including every changed sidecar.
        pins = {}
        for name, item in files.items():
            if name in (
                OLD_HEAD,
                "release-manifest.json",
                "SHA256SUMS",
                ".gitattributes",
            ):
                continue
            if name in updates:
                pins[name] = pin(updates[name])
            elif item.lfs is not None:
                pins[name] = {"size_bytes": item.size, "sha256": item.lfs.sha256}
            else:
                pins[name] = pin(read(name))
        head = files[OLD_HEAD]
        pins[NEW_HEAD] = (
            {"size_bytes": head.size, "sha256": head.lfs.sha256}
            if head.lfs
            else pin(read(OLD_HEAD))
        )
        for name, data in updates.items():
            pins[name] = pin(data)
        if "release-manifest.json" in files:
            manifest = json.loads(read("release-manifest.json"))
            manifest["files"] = pins
            updates["release-manifest.json"] = json_bytes(manifest)
        if "SHA256SUMS" in files:
            updates["SHA256SUMS"] = "".join(
                f"{item['sha256']}  {name}\n" for name, item in sorted(pins.items())
            ).encode()
        for name, data in updates.items():
            (folder / name).write_bytes(data)
        plans.append(
            {
                "source": repo,
                "destination": "antflydb/" + new,
                "revision": info.sha,
                "copy": {OLD_HEAD: NEW_HEAD},
                "updates": {name: pin(data) for name, data in updates.items()},
            }
        )
    (output / "plan.json").write_bytes(json_bytes(plans))
    return plans


def apply(api, output):
    from huggingface_hub import (
        CommitOperationAdd,
        CommitOperationCopy,
        CommitOperationDelete,
    )

    plans = json.loads((output / "plan.json").read_text())
    for plan in plans:
        folder = output / plan["destination"].split("/")[1]
        for name, expected in plan["updates"].items():
            if pin((folder / name).read_bytes()) != expected:
                raise ValueError(f"reviewed metadata changed: {name}")
        if api.model_info(plan["source"]).sha != plan["revision"]:
            raise ValueError("source changed after preparation")
        operations = []
        for source, destination in plan["copy"].items():
            operations.append(
                CommitOperationCopy(
                    src_path_in_repo=source,
                    path_in_repo=destination,
                    src_revision=plan["revision"],
                )
            )
            operations.append(CommitOperationDelete(path_in_repo=source))
        operations += [
            CommitOperationAdd(path_in_repo=name, path_or_fileobj=folder / name)
            for name in plan["updates"]
        ]
        commit = api.create_commit(
            repo_id=plan["source"],
            operations=operations,
            parent_commit=plan["revision"],
            commit_message="Normalize GLiNER2.5 split bundle layout and document serving contract",
        )
        api.move_repo(
            from_id=plan["source"], to_id=plan["destination"], repo_type="model"
        )
        print(json.dumps({"repo": plan["destination"], "commit": commit.oid}))


def main():
    from huggingface_hub import HfApi

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    api = HfApi()
    if args.apply:
        apply(api, args.output)
    else:
        print(json.dumps(prepare(api, args.output), indent=2))


if __name__ == "__main__":
    main()
