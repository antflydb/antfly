#!/usr/bin/env python3
"""Qualify relationship postimages, nulls, MERGE rollback, and deleted entities."""

import argparse
import base64
import json
import os
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:18591")
    parser.add_argument("--shards", type=int, choices=[1, 2], default=2)
    args = parser.parse_args()
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Basic "
        + base64.b64encode(os.environ["ANTFLY_CYPHER_TEST_AUTH"].encode()).decode(),
    }
    table = "cypher_mutations_" + uuid.uuid4().hex
    graph = table + "_graph"
    created_table = created_graph = False

    def call(path, body=None, method="POST", status=None):
        request = urllib.request.Request(
            args.url.rstrip("/") + "/db/v1" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
            method=method,
        )
        for attempt in range(40):
            try:
                with urllib.request.urlopen(request, timeout=40) as response:
                    code, content = response.status, response.read()
            except urllib.error.HTTPError as error:
                code, content = error.code, error.read()
            if (
                code == 503
                and attempt < 39
                and json.loads(content).get("write_outcome") == "not_started"
            ):
                time.sleep(0.25)
                continue
            break
        assert (200 <= code < 300) if status is None else code == status, (
            path,
            code,
            content.decode(),
        )
        return json.loads(content) if content else None

    def query(statement, parameters=None):
        result = call(
            f"/graphs/{graph}/query",
            {"query": statement, "parameters": parameters or {}},
        )
        assert result["complete"] is True, result
        return result["rows"]

    try:
        call(
            f"/tables/{table}",
            {"num_shards": args.shards, "indexes": {"links": {"type": "graph"}}},
        )
        created_table = True
        call(
            f"/graphs/{graph}",
            {
                "labels": [{"name": "Document", "table": table}],
                "relationships": [
                    {
                        "name": "LINK",
                        "index": "links",
                        "source_label": "Document",
                        "target_label": "Document",
                        "stored_type": "link",
                    }
                ],
            },
        )
        created_graph = True
        row = query(
            "CREATE p=(:Document {id:'a'})-[r:LINK {x:1,absent:null,data:{nested:null}}]->(:Document {id:'b'}) WITH p,r,[r] AS list,{edge:r} AS map CALL { WITH r SET r.x=2 } SET r.y=r.x RETURN r.x,r.y,relationships(p)[0].x,list[0].x,map.edge.x,keys(r),r.data"
        )[0]
        assert row[:5] == [{"type": "integer", "value": "2"}] * 5, row
        assert {value["value"] for value in row[5]["value"]} == {"x", "y", "data"}, row
        assert row[6] == {
            "type": "map",
            "value": {"nested": {"type": "null", "value": None}},
        }, row
        stored = query("MATCH ()-[r:LINK]->() RETURN r.x,r.y,keys(r),r.data")[0]
        assert stored[:2] == [{"type": "integer", "value": "2"}] * 2, stored
        assert {value["value"] for value in stored[2]["value"]} == {"x", "y", "data"}, (
            stored
        )
        assert stored[3] == row[6], stored
        for statement in (
            "MERGE (n:Document {id:$key}) RETURN n",
            "CREATE (n:Document {id:'rollback'}) WITH n MERGE (m:Document {id:$key}) RETURN m",
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) MERGE (a)-[r:LINK {x:$key}]->(b) RETURN r",
        ):
            failure = call(
                f"/graphs/{graph}/query",
                {"query": statement, "parameters": {"key": None}},
                status=400,
            )
            assert failure["code"] == "CypherInvalidPropertyValue", failure
            assert (
                query("MATCH (n:Document) WITH n WHERE n.id='rollback' RETURN n") == []
            )
        deleted = query("MATCH ()-[r:LINK]->() DELETE r RETURN count(r)")
        assert deleted == [[{"type": "integer", "value": "1"}]], deleted
        assert query("MATCH ()-[r:LINK]->() RETURN count(r)") == [
            [{"type": "integer", "value": "0"}]
        ]
        for statement in (
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) DELETE b CREATE (a)-[:LINK]->(b) RETURN a",
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) DELETE a MERGE (a)-[:LINK]->(b) RETURN b",
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) CALL { WITH b DELETE b } CREATE (a)-[:LINK]->(b) RETURN a",
            "MATCH (a:Document {id:'a'}) DELETE a RETURN a.x",
            "MATCH (a:Document {id:'a'}) DELETE a RETURN a{.*}",
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) CREATE (a)-[r:LINK {x:42}]->(b) DELETE r SET a.x=r.x RETURN a.x",
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) CREATE p=(a)-[r:LINK {x:42}]->(b) DETACH DELETE a RETURN relationships(p)[0].x",
        ):
            failure = call(f"/graphs/{graph}/query", {"query": statement}, status=400)
            assert failure["code"] == "CypherDeletedEntity", failure
            assert query("MATCH (n:Document) RETURN n.id ORDER BY n.id") == [
                [{"type": "string", "value": "a"}],
                [{"type": "string", "value": "b"}],
            ]
            assert query("MATCH ()-[r:LINK]->() RETURN count(r)") == [
                [{"type": "integer", "value": "0"}]
            ]
        row = query(
            "CREATE p=(n:Document {x:1})-[r:LINK {x:2}]->(:Document) DETACH DELETE n RETURN count(n),count(r),elementId(n),elementId(r),n,r,relationships(p)[0]"
        )[0]
        assert row[:2] == [{"type": "integer", "value": "1"}] * 2
        assert row[2]["type"] == row[3]["type"] == "string"
        assert row[4]["value"]["properties"]["value"] == {}
        assert row[4]["value"]["labels"] == []
        assert row[5]["value"]["properties"]["value"] == {}
        assert row[6]["value"]["properties"]["value"] == {}
        print("14 authenticated Cypher mutation semantics checks passed")
    finally:
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        if created_table:
            call(f"/tables/{table}", method="DELETE")


if __name__ == "__main__":
    main()
