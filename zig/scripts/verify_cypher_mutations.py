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
    parser.add_argument("--preexisting-producers", action="store_true")
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
            # Producer publication may invalidate a newly captured read cut.
            # Restart only this complete, stateless read; never a write/page.
            if (
                code in (409, 503)
                and attempt < 39
                and status is None
                and path == "/sql"
                and body is not None
                and "session_id" not in body
                and body.get("statement", "").lstrip().upper().startswith("SELECT ")
                and json.loads(content).get("code") in ("40001", "53300")
                and json.loads(content).get("retryable") is True
            ):
                time.sleep(0.1)
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
            {
                "num_shards": args.shards,
                "indexes": {
                    "links": {
                        "type": "graph",
                        "edge_types": [{"name": "link", "field": "ref"}],
                    }
                },
            },
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
        assert query(
            "MATCH (a:Document {id:'a'}),(b:Document {id:'b'}) SET a.ref=antfly.documentKey(b) WITH a MATCH (a)-[r:LINK]->(b) RETURN r.weight"
        ) == [[{"type": "number", "value": 1}]]
        row = query(
            "MATCH (a:Document {id:'a'})-[r:LINK]->(b) SET a.ref=null WITH a,b,r,count(r) AS seen SET a.ref=antfly.documentKey(b) WITH a,b,r MATCH (a)-[s:LINK]->(b) RETURN r.weight,s.weight"
        )[0]
        assert row == [{"type": "number", "value": 1}] * 2
        row = query(
            "MATCH p=(a:Document {id:'a'})-[r:LINK]->(b) WITH a,b,p,[r] AS list,{edge:r} AS map SET a.ref=null WITH a,b,p,list,map,relationships(p)[0] AS missing SET a.ref=antfly.documentKey(b) RETURN missing.weight,list[0].weight,map.edge.weight,relationships(p)[0].weight"
        )[0]
        assert row == [{"type": "number", "value": 1}] * 4
        missing = call(
            f"/graphs/{graph}/query",
            {
                "query": "MATCH (a:Document {id:'a'})-[r:LINK]->(b) SET a.ref=null RETURN r.weight"
            },
            status=400,
        )
        assert missing["code"] == "CypherDeletedEntity", missing
        assert query("MATCH (a:Document {id:'a'})-[r:LINK]->(b) RETURN r.weight") == [
            [{"type": "number", "value": 1}]
        ]
        for statement in (
            "CREATE (n:Document {id:'invalid-handle'}) SET n.payload=n RETURN n",
            "CREATE (n:Document {id:'invalid-handle'}) SET n.payload={nested:[n]} RETURN n",
            "CREATE (a:Document {id:'invalid-handle'}),(b:Document {id:'invalid-handle',payload:a}) RETURN b",
            "CREATE p=(a:Document {id:'invalid-handle'})-[r:LINK]->(b:Document {id:'invalid-handle'}) SET r.payload=p RETURN r",
            "MATCH (a:Document {id:'a'}) SET a.payload=a RETURN a.id",
        ):
            invalid = call(f"/graphs/{graph}/query", {"query": statement}, status=400)
            assert invalid["code"] == "CypherInvalidPropertyValue", invalid
        assert query("MATCH (n:Document {id:'invalid-handle'}) RETURN count(n)") == [
            [{"type": "integer", "value": "0"}]
        ]
        for prefix in ("", "EXPLAIN "):
            for verb in ("CREATE", "MERGE"):
                conflict = call(
                    f"/graphs/{graph}/query",
                    {
                        "query": f"{prefix}MATCH (a:Document {{id:'missing'}})-[r:LINK]->(b) {verb} (a)-[r:LINK]->(b) RETURN r"
                    },
                    status=400,
                )
                assert conflict["code"] == "CypherVariableConflict", conflict
        filtered = graph + "_filtered"
        call(
            f"/graphs/{filtered}",
            {
                "labels": [
                    {"name": "Document", "table": table},
                    {"name": "Source", "table": table, "where": {"s": True}},
                    {"name": "Target", "table": table, "where": {"t": True}},
                ],
                "relationships": [
                    {
                        "name": "LINK",
                        "index": "links",
                        "source_label": "Source",
                        "target_label": "Target",
                        "stored_type": "link",
                    }
                ],
            },
        )
        try:
            query(
                "CREATE (a:Document {id:'target-loop',s:false,t:true})-[:LINK]->(a), (b:Document {id:'both-loop',s:true,t:true})-[:LINK]->(b)"
            )
            for pattern in (
                "(n:Target)<-[r:LINK]-(m)",
                "(n)-[r:LINK]-(m)",
                "(n:Source)-[r:LINK]->(m)",
            ):
                result = call(
                    f"/graphs/{filtered}/query",
                    {"query": f"MATCH {pattern} RETURN n.id"},
                )
                assert result["rows"] == [[{"type": "string", "value": "both-loop"}]], (
                    result
                )
        finally:
            call(f"/graphs/{filtered}", method="DELETE")
        query(
            "CREATE (a:Document {id:'short-a'})-[:LINK {weight:1}]->(b:Document {id:'short-b'}),"
            "(a)-[:LINK {weight:2}]->(c:Document {id:'short-c'})-[:LINK {weight:2}]->(b),"
            "(x:Document {id:'short-x',amount:2})"
        )
        for selector in ("shortestPath", "allShortestPaths"):
            path = f"p={selector}((a:Document {{id:'short-a'}})-[:LINK*1..3 {{weight:x.amount}}]->(b:Document {{id:'short-b'}}))"
            for patterns in (
                f"{path},(x:Document {{id:'short-x'}})",
                f"(x:Document {{id:'short-x'}}),{path}",
            ):
                assert query(f"MATCH {patterns} RETURN length(p)") == [
                    [{"type": "integer", "value": "2"}]
                ]
        # Use a relational fixture for SQL writes; the earlier graph exercises
        # dynamic document properties. Distributed transactions support READ
        # COMMITTED, while bare BEGIN requests serializable range guards.
        owned_table, owned_graph = table + "_owned", graph + "_owned"
        call(
            "/sql",
            {
                "statement": f"CREATE TABLE {owned_table} (id TEXT, kind TEXT, ref TEXT, kg JSONB)"
            },
        )
        try:
            call(
                f"/tables/{owned_table}/indexes/links",
                {"type": "graph", "edge_types": [{"name": "link"}]},
            )
            # Index creation is asynchronous. Establish the complete generation
            # on its required owners before publishing raw graph effects.
            ready_deadline = time.monotonic() + 60
            while True:
                index = call(f"/tables/{owned_table}/indexes/links", method="GET")
                complete = (
                    index.get("status", {}).get("milestones", {}).get("complete", {})
                )
                if complete.get("reached") is True and complete.get("blockers") == []:
                    break
                assert time.monotonic() < ready_deadline, index
                time.sleep(0.2)
            call(
                f"/graphs/{owned_graph}",
                {
                    "labels": [{"name": "Document", "table": owned_table}],
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
            try:
                call(
                    f"/tables/{owned_table}/batch",
                    {
                        "inserts": {
                            key: {
                                "id": key,
                                "kind": "vertex"
                                if key in ("owned-a", "owned-b")
                                else "producer",
                            }
                            for key in (
                                "owned-a",
                                "owned-b",
                                "owned-claim",
                                "owned-other",
                            )
                        },
                        "graph_writes": [
                            {
                                "index_name": "links",
                                "source": "owned-a",
                                "target": "owned-b",
                                "edge_type": "link",
                                "edge_id": "fact:1",
                                "owner_document": owner,
                            }
                            for owner in ("owned-claim", "owned-other")
                        ],
                        "sync_level": "full_index",
                    },
                )
                count = f"SELECT count(*) FROM GRAPH_TABLE({owned_graph} MATCH ()-[r IS LINK]->() COLUMNS(r AS edge)) g"
                assert call("/sql", {"statement": count})["rows"] == [["2"]]
                preserved = call(
                    f"/graphs/{owned_graph}/query",
                    {
                        "query": "MATCH (n:Document {id:'owned-claim'}) SET n.kind='producer' WITH 1 AS ignored MATCH ()-[r:LINK]->() RETURN count(r)"
                    },
                )
                assert preserved["rows"] == [[{"type": "integer", "value": "2"}]], (
                    preserved
                )
                nonvertex_graph = owned_graph + "_vertices"
                call(
                    f"/graphs/{nonvertex_graph}",
                    {
                        "labels": [
                            {
                                "name": "Document",
                                "table": owned_table,
                                "where": {"kind": "vertex"},
                            }
                        ],
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
                try:
                    nonvertex_count = count.replace(owned_graph, nonvertex_graph)
                    assert call("/sql", {"statement": nonvertex_count})["rows"] == [
                        ["2"]
                    ]
                    session = call(
                        "/sql", {"statement": "BEGIN ISOLATION LEVEL READ COMMITTED"}
                    )["session_id"]
                    try:
                        call(
                            "/sql",
                            {
                                "statement": f"UPDATE {owned_table} SET kind='producer' WHERE _id='owned-claim'",
                                "session_id": session,
                            },
                        )
                        assert call(
                            "/sql",
                            {"statement": nonvertex_count, "session_id": session},
                        )["rows"] == [["2"]]
                    finally:
                        call("/sql", {"statement": "ROLLBACK", "session_id": session})
                finally:
                    call(f"/graphs/{nonvertex_graph}", method="DELETE")
                session = call(
                    "/sql", {"statement": "BEGIN ISOLATION LEVEL READ COMMITTED"}
                )["session_id"]
                try:
                    call(
                        "/sql",
                        {
                            "statement": f"DELETE FROM {owned_table} WHERE _id='owned-claim'",
                            "session_id": session,
                        },
                    )
                    assert call("/sql", {"statement": count, "session_id": session})[
                        "rows"
                    ] == [["1"]]
                finally:
                    call("/sql", {"statement": "ROLLBACK", "session_id": session})
                assert call("/sql", {"statement": count})["rows"] == [["2"]]
                result = call(
                    f"/graphs/{owned_graph}/query",
                    {
                        "query": "MATCH (n:Document {id:'owned-claim'}) DELETE n WITH 1 AS ignored MATCH ()-[r:LINK]->() RETURN count(r)"
                    },
                )
                assert result["rows"] == [[{"type": "integer", "value": "1"}]], result
                # Field projections and published assertions can share both
                # their producer and endpoints across replicated owners.
                extraction = {
                    "entities": [
                        {"id": "a", "label": "thing", "text": "A"},
                        {"id": "b", "label": "thing", "text": "B"},
                    ],
                    "relations": [{"type": "link", "source": "a", "target": "b"}],
                }

                def seed_mixed_documents():
                    call(
                        f"/tables/{owned_table}/batch",
                        {
                            "inserts": {
                                "mixed/a": {
                                    "id": "mixed-a",
                                    "ref": "mixed/b",
                                    "kg": extraction,
                                },
                                "mixed/b": {"id": "mixed-b"},
                            },
                            "sync_level": "write",
                        },
                    )

                if args.preexisting_producers:
                    seed_mixed_documents()
                call(
                    f"/tables/{owned_table}/indexes/mixed",
                    {
                        "type": "graph",
                        "source": {
                            "artifact": "kg",
                            "format": "extraction_graph",
                            "mention_edge_type": "mentions",
                        },
                        "artifact": {
                            "name": "kg",
                            "kind": "asset",
                            "source": {"type": "field", "value": "kg"},
                            "content_type": "application/json",
                        },
                        "edge_types": [
                            {"name": "mentions"},
                            {"name": "link", "field": "ref"},
                        ],
                        "resolvers": [
                            {
                                "name": "entities",
                                "table": owned_table,
                                "promote": False,
                                "source_artifact": "kg",
                                "resolution_artifact": "resolved",
                                "key_template": "mixed/{{ uslug _entity.text }}",
                                "config_generation": 1,
                            }
                        ],
                    },
                )
                if not args.preexisting_producers:
                    seed_mixed_documents()
                ready_deadline = time.monotonic() + 60
                while True:
                    index = call(f"/tables/{owned_table}/indexes/mixed", method="GET")
                    complete = (
                        index.get("status", {})
                        .get("milestones", {})
                        .get("complete", {})
                    )
                    if (
                        complete.get("reached") is True
                        and complete.get("blockers") == []
                    ):
                        break
                    assert time.monotonic() < ready_deadline, (
                        index,
                        call(f"/tables/{owned_table}", method="GET"),
                        call(
                            "/sql",
                            {"statement": f"SELECT id, ref, kg FROM {owned_table}"},
                        ),
                    )
                    time.sleep(0.2)
                mixed_graph = owned_graph + "_mixed"
                call(
                    f"/graphs/{mixed_graph}",
                    {
                        "labels": [{"name": "Document", "table": owned_table}],
                        "relationships": [
                            {
                                "name": "LINK",
                                "index": "mixed",
                                "source_label": "Document",
                                "target_label": "Document",
                                "stored_type": "link",
                            }
                        ],
                    },
                )
                try:
                    mixed_count = count.replace(owned_graph, mixed_graph)
                    mixed_baseline = call("/sql", {"statement": mixed_count})
                    assert mixed_baseline["rows"] == [["2"]], (
                        mixed_baseline,
                        call(
                            f"/graphs/{mixed_graph}/query",
                            {"query": "MATCH ()-[r:LINK]->() RETURN r"},
                        ),
                    )
                    # This fixture never commits. A transient coherent-cut
                    # failure can restart the complete transaction after its
                    # staged changes have been explicitly rolled back.
                    for attempt in range(40):
                        session = call(
                            "/sql",
                            {"statement": "BEGIN ISOLATION LEVEL READ COMMITTED"},
                        )["session_id"]
                        try:
                            call(
                                "/sql",
                                {
                                    "statement": f"UPDATE {owned_table} SET ref=NULL WHERE _id='mixed/a'",
                                    "session_id": session,
                                },
                            )
                            assert call(
                                "/sql",
                                {"statement": mixed_count, "session_id": session},
                            )["rows"] == [["1"]]
                            break
                        except AssertionError as error:
                            detail = error.args[0]
                            if not isinstance(detail, tuple) or len(detail) != 3:
                                raise
                            failure = json.loads(detail[2])
                            if (
                                attempt == 39
                                or failure.get("code") not in ("53300", "40001")
                                or failure.get("retryable") is not True
                            ):
                                raise
                        finally:
                            call(
                                "/sql", {"statement": "ROLLBACK", "session_id": session}
                            )
                        time.sleep(0.1)
                    mixed = call(
                        f"/graphs/{mixed_graph}/query",
                        {
                            "query": "MATCH (a:Document {id:'mixed-a'}) SET a.ref=null WITH 1 AS ignored MATCH ()-[r:LINK]->() RETURN r"
                        },
                    )
                    assert (
                        len(mixed["rows"]) == 1
                        and mixed["rows"][0][0]["value"]["ownership"] == "artifact"
                    ), mixed
                    assert call("/sql", {"statement": mixed_count})["rows"] == [["1"]]
                finally:
                    call(f"/graphs/{mixed_graph}", method="DELETE")
            finally:
                call(f"/graphs/{owned_graph}", method="DELETE")
        finally:
            call(f"/tables/{owned_table}", method="DELETE")
        print(
            "32 authenticated Cypher mutation semantics checks passed; producer postimages and deferred shortest paths passed"
        )
    finally:
        if created_graph:
            call(f"/graphs/{graph}", method="DELETE")
        if created_table:
            call(f"/tables/{table}", method="DELETE")


if __name__ == "__main__":
    main()
