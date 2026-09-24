import { useEffect, useState } from "react";
interface Pipeline {
  reader: {
    model: string;
    native_pages: number;
    ocr_pages: number;
    unreadable_pages: number;
  };
  embeddings: { model: string; ready: boolean };
  generator: { model: string; provider: string };
  reranker: { model: string; candidate_count: number };
  extraction: {
    model: string;
    state: string;
    completed?: number;
    total?: number;
    error?: string;
  };
  autograph_ready: boolean;
}
export function PipelineStatus() {
  const [state, setState] = useState<Pipeline>();
  const [error, setError] = useState("");
  useEffect(() => {
    const controller = new AbortController();
    fetch("/api/governance/pipeline", { signal: controller.signal })
      .then(async (r) => {
        const data = await r.json();
        if (!r.ok) throw new Error(data.error);
        setState(data);
      })
      .catch((e) => {
        if (e.name !== "AbortError") setError(e.message);
      });
    return () => controller.abort();
  }, []);
  return (
    <details className="gov-notice">
      <summary>
        Pipeline status ·{" "}
        {state?.extraction.state === "blocked"
          ? "native graph extraction blocked"
          : "models and coverage"}
      </summary>
      {error && <p>{error}</p>}
      {state && (
        <>
          <p>
            Text extraction: {state.reader.native_pages} native pages,{" "}
            {state.reader.ocr_pages} Florence OCR pages,{" "}
            {state.reader.unreadable_pages} unreadable pages. OCR and captions
            use {state.reader.model} when native text fails its quality check.
          </p>
          <p>
            Semantic search: {state.embeddings.model} ·{" "}
            {state.embeddings.ready ? "ready" : "enrichment incomplete"}.
            Reranker: {state.reranker.model}. Generator: {state.generator.model}{" "}
            ({state.generator.provider}).
          </p>
          <p>
            Automatic graph: {state.extraction.model} · {state.extraction.state}{" "}
            · {state.extraction.completed || 0}/{state.extraction.total || "—"}{" "}
            passages extracted. Resolution:{" "}
            {state.autograph_ready ? "ready" : "not ready"}.
          </p>
          {state.extraction.error && <p>{state.extraction.error}</p>}
          <p>
            The decision timeline and maintained evidence links remain separate
            from machine-extracted relationships. All extracted relationships
            require review.
          </p>
        </>
      )}
    </details>
  );
}
