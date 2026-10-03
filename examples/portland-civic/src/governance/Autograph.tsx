import { useState } from "react";
export function Autograph({ passage }: { passage: string }) {
  const [result, setResult] = useState<{
    graph?: {
      nodes?: {
        key: string;
        document?: Record<string, unknown>;
        path_edges?: { type: string }[];
      }[];
    };
    review?: string;
    relations?: {
      type: string;
      subject: string;
      object: string;
      score: number;
    }[];
  }>();
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  async function load() {
    setBusy(true);
    setError("");
    try {
      const response = await fetch(
        `/api/governance/autograph?passage=${encodeURIComponent(passage)}`,
      );
      const body = await response.json();
      if (!response.ok) throw new Error(body.error);
      setResult(body);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  return (
    <section className="gov-notice">
      <button className="gov-secondary" disabled={busy} onClick={load}>
        {busy ? "Loading extracted entities…" : "Inspect extracted entities"}
      </button>
      {error && <p>{error}</p>}
      {result && (
        <>
          <p>{result.review}</p>
          <ul>
            {result.graph?.nodes
              ?.filter((n) => !n.document?.passage_id)
              .map((n) => (
                <li key={n.key}>
                  {String(
                    n.document?.canonical_name ||
                      n.document?.canonical_text ||
                      n.document?.text ||
                      n.key,
                  )}
                  {n.path_edges?.length
                    ? ` · ${n.path_edges.map((e) => e.type).join(" → ")}`
                    : ""}
                </li>
              ))}
          </ul>
          {!!result.relations?.length && (
            <>
              <h4>Candidate relationships in this passage</h4>
              <ul>
                {result.relations.map((r, i) => (
                  <li key={i}>
                    {r.subject} → {r.type.replaceAll("_", " ")} → {r.object}
                  </li>
                ))}
              </ul>
              <p>
                Direction and meaning require review against the quoted passage.
              </p>
            </>
          )}
          {!result.graph?.nodes?.length && (
            <p>No extracted entities recorded for this passage.</p>
          )}
        </>
      )}
    </section>
  );
}
