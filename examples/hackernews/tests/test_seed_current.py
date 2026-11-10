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

import io
import json
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))
from lite_state import State
import native_ingest
from seed_current import seed_current


def test_seed_resumes_exact_acceptance_and_keeps_older_edits_and_deletes(
    tmp_path, monkeypatch
):
    source = tmp_path / "archive.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"id": 1, "type": "story", "time": 10, "title": "old", "parent": 0},
                {
                    "id": 2,
                    "type": "comment",
                    "time": 30,
                    "title": "recent",
                    "parent": 1,
                },
                {"id": 3, "type": "story", "time": 40, "title": "new", "parent": 0},
            ]
        ),
        source,
    )
    accepted, requests = {}, []
    lose_response = [True]

    def send(request, **kwargs):
        body = json.loads(request.data)
        requests.append(body)
        accepted.setdefault(body["batch_id"], len(accepted) + 1)
        if lose_response[0]:
            lose_response[0] = False
            raise OSError("lost acceptance response")
        response = io.BytesIO(
            json.dumps(
                {"state": "accepted", "wal_lsn": accepted[body["batch_id"]]}
            ).encode()
        )
        response.status = 202
        return response

    monkeypatch.setattr(native_ingest, "urlopen", send)
    state = State(tmp_path / "state.aflite")

    def publish():
        return native_ingest.publish(state, "https://native/db/v1", "hn_current", 1)

    try:
        with pytest.raises(OSError):
            seed_current(state, [source], 20, publish, 1)
    finally:
        state.db.close()
    state = State(tmp_path / "state.aflite")
    try:
        with pytest.raises(RuntimeError, match="boundary changed"):
            seed_current(state, [source], 21, publish, 1)
        seed_current(state, [source], 20, publish, 1)
        assert requests[0] == requests[1]
        assert [
            change["row"]["hn_id"]
            for body in requests[1:]
            for change in body["changes"]
        ] == [2, 3]
        assert state.item(1)["payload"]["title"] == "old"
        assert state.item(2)["root"] == 1
        assert seed_current(state, [source], 20, publish, 1)["resumed"]
        state.put({"id": 1, "title": "older corrected"})
        publish()
        assert requests[-1]["changes"][0]["row"]["hn_id"] == 1
        assert requests[-1]["changes"][0]["op"] == "upsert"
        state.put({"id": 1, "deleted": True})
        publish()
        assert requests[-1]["changes"][0] == {"op": "delete", "row": {"hn_id": 1}}
    finally:
        state.db.close()
