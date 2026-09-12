#!/usr/bin/env python3
"""Summarize run_vdbbench_qualification.sh arms side by side.

usage: compare_vdbbench_arms.py RUN_ROOT [RUN_ROOT ...]
Reads phases.jsonl, qualification-summary.json, disk-after-restart.json and the
table status snapshots so paired arms can be compared without opening each file.
"""
import json, sys
from pathlib import Path

def phases(root):
    rows=[json.loads(l) for l in (root/"phases.jsonl").read_text().splitlines() if l.strip()]
    t={r["phase"]:r["monotonic_ns"] for r in rows}
    d=lambda a,b:(t[b]-t[a])/1e9 if a in t and b in t else None
    return {
        "live_load_query_s": d("live_load_and_query_start","live_load_and_query_end"),
        "mixed_s": d("mixed_profile_start","mixed_profile_end"),
        "churn_s": d("source_churn_begin","source_churn_end"),
        "enrich_s": d("source_enrichment_begin","source_enrichment_end"),
        "shutdown_s": d("restart_begin","shutdown_complete"),
        "restart_listen_s": d("shutdown_complete","restart_ready"),
        "restart_index_ready_s": d("restart_ready","restart_index_ready"),
        "cold_query_s": d("restart_index_ready","reopened_cold_query_end"),
        "warm_query_s": d("reopened_cold_query_end","reopened_warm_query_end"),
    }

def summary(root):
    s=json.loads((root/"qualification-summary.json").read_text())
    out={}
    for run in s.get("runs",[]):
        label=run.get("label") or run.get("db_label") or "?"
        key="live" if "cold" not in label and "warm" not in label else ("cold" if "cold" in label else "warm")
        out[f"{key}_ready_s"]=run.get("ready_seconds")
        out[f"{key}_max_qps"]=run.get("max_qps")
        out[f"{key}_recall"]=run.get("recall")
        out[f"{key}_p99_ms"]=(run.get("serial_latency_ms") or {}).get("p99")
    m=s.get("public_mixed_profiles",{}).get("public-mixed-profile",{})
    out["mixed_query_qps"]=m.get("query_qps"); out["mixed_query_p99_ms"]=(m.get("query_latency") or {}).get("p99_ms")
    out["mixed_write_rows_s"]=m.get("write_rows_per_second"); out["mixed_write_p99_ms"]=(m.get("write_latency") or {}).get("p99_ms")
    q=s.get("public_query_profiles",{}).get("public-query-profile",{})
    out["profile_p50_ms"]=q.get("p50_ms"); out["profile_p99_ms"]=q.get("p99_ms")
    out["rss_live_peak_mib"]=s["rss_profiles"]["rss-live"]["peak_rss_bytes"]/2**20
    out["rss_restart_peak_mib"]=s["rss_profiles"]["rss-restart"]["peak_rss_bytes"]/2**20
    return out

def disk(root):
    try:
        d=json.loads((root/"disk-after-restart.json").read_text())
        return {"disk_after_restart_mib": sum(g.get("allocated_bytes",0) for g in d.get("groups",{}).values())/2**20}
    except Exception: return {}

def status_counters(root):
    out={}
    import re
    for name,tag in (("table-after-restart.json","after_restart"),("table-before-restart.json","before_restart")):
        try:
            text=(root/name).read_text()
            for key in ("inventory_update_ns","preparation_ns","checkpoint_ns","durable_append_ns"):
                m=re.search('"%s":(\d+)'%key,text)
                if m: out[f"src_{key.replace('_ns','')}_{tag}_ms"]=int(m.group(1))/1e6
            for key in ("heap_bytes","immutable_block_bytes","source_segments"):
                m=re.search('"%s":(\d+)'%key,text)
                if m: out[f"src_{key}_{tag}"]=int(m.group(1))/(2**20 if key.endswith("bytes") else 1)
        except Exception: pass
    for name in ("source-churn.json","source-enrichment.json"):
        try:
            d=json.loads((root/name).read_text()); out[name.split(".")[0]+"_qualified"]=d.get("qualified")
        except Exception: pass
    return out

rows={}
for arg in sys.argv[1:]:
    root=Path(arg); r={}
    r.update(phases(root)); r.update(summary(root)); r.update(disk(root)); r.update(status_counters(root))
    rows[root.name]=r
keys=[k for k in rows[next(iter(rows))].keys()]
print("metric".ljust(26)+"".join(n.rjust(16) for n in rows))
for k in keys:
    line=k.ljust(26)
    for n in rows:
        v=rows[n].get(k)
        line+=(f"{v:16.3f}" if isinstance(v,(int,float)) and not isinstance(v,bool) else str(v).rjust(16))
    print(line)
