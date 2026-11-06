import {
  Alert,
  AlertDescription,
  Badge,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Field,
  FieldLabel,
  Input,
} from "@antfly/design-system";
import type {
  PrivacyFindingsPage,
  PrivacyPolicy,
  PrivacyPreview,
  PrivacyScan,
  PrivacyStatus,
} from "@antfly/sdk";
import { useCallback, useEffect, useState } from "react";
import { useApi } from "@/hooks/use-api-config";
import { privacyHighlights } from "./privacy-preview";

const groups = ["contact_details", "personal_identifiers", "financial", "credentials"] as const;
const labels = {
  contact_details: "Contact details",
  personal_identifiers: "Personal identifiers",
  financial: "Financial information",
  credentials: "Credentials",
};
const control = "w-full rounded-md border bg-background px-3 py-2 text-sm";
const initial: PrivacyPolicy = {
  mode: "monitor",
  fields: ["body"],
  category_groups: [...groups],
  sensitivity: "balanced",
  replacement: "category",
};

function safeHighlights(text: string, findings: Parameters<typeof privacyHighlights>[1]) {
  let parts: ReturnType<typeof privacyHighlights>;
  try {
    parts = privacyHighlights(text, findings);
  } catch {
    parts = [{ text, categories: [] }];
  }
  let offset = 0;
  return parts.map((part) => {
    const key = String(offset);
    offset += part.text.length;
    return { ...part, key };
  });
}

export function TablePrivacyPanel({ tableName }: { tableName: string }) {
  const api = useApi();
  const [policy, setPolicy] = useState<PrivacyPolicy>(initial);
  const [status, setStatus] = useState<PrivacyStatus>();
  const [etag, setEtag] = useState<string>();
  const [producer, setProducer] = useState("");
  const [categories, setCategories] = useState("");
  const [sampleKeys, setSampleKeys] = useState("");
  const [preview, setPreview] = useState<PrivacyPreview>();
  const [previewPolicy, setPreviewPolicy] = useState("");
  const [scan, setScan] = useState<PrivacyScan>();
  const [scanError, setScanError] = useState<string>();
  const [scanRefresh, setScanRefresh] = useState(0);
  const [findings, setFindings] = useState<PrivacyFindingsPage>();
  const [document, setDocument] = useState("");
  const [findingField, setFindingField] = useState("");
  const [findingCategory, setFindingCategory] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string>();
  const [message, setMessage] = useState<string>();
  const [impactAccepted, setImpactAccepted] = useState(false);

  const effectivePolicy = (): PrivacyPolicy => {
    const selected = producer.trim() ? JSON.parse(producer) : undefined;
    return {
      ...policy,
      producer: selected,
      categories: categories.trim() ? JSON.parse(categories) : undefined,
    };
  };
  const refresh = useCallback(
    async (signal?: AbortSignal) => {
      const result = await api.privacy.get(tableName, signal);
      if (!signal?.aborted) setStatus(result.data);
      return result;
    },
    [api, tableName]
  );
  useEffect(() => {
    const controller = new AbortController();
    refresh(controller.signal)
      .then(({ data, etag: loadedEtag }) => {
        if (controller.signal.aborted) return;
        setEtag(loadedEtag ?? data.etag);
        setPreview(undefined);
        setPreviewPolicy("");
        setImpactAccepted(false);
        setPolicy(data.policy ?? initial);
        setProducer(data.policy?.producer ? JSON.stringify(data.policy.producer, null, 2) : "");
        setCategories(
          data.policy?.categories ? JSON.stringify(data.policy.categories, null, 2) : ""
        );
      })
      .catch((reason) => {
        if (!controller.signal.aborted) setError(String(reason.message ?? reason));
      });
    return () => controller.abort();
  }, [refresh]);
  const statusPollInterval =
    !scan || ["queued", "running", "processing"].includes(scan.phase) ? 2500 : 10000;
  useEffect(() => {
    if (!status?.configured) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const poll = async () => {
      try {
        await refresh(controller.signal);
      } catch (reason) {
        if (!controller.signal.aborted)
          setError(reason instanceof Error ? reason.message : String(reason));
      }
      if (!controller.signal.aborted) timer = setTimeout(poll, statusPollInterval);
    };
    timer = setTimeout(poll, statusPollInterval);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [statusPollInterval, refresh, status?.configured]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: scanRefresh cancels and restarts progress polling on an explicit refresh.
  useEffect(() => {
    const scanId = status?.configured ? status.scan_id : undefined;
    setScanError(undefined);
    setScan((current) => (current?.scan_id === scanId ? current : undefined));
    if (!scanId) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    let phase: PrivacyScan["phase"] = "queued";
    const poll = async () => {
      try {
        const result = await api.privacy.getScan(tableName, scanId, controller.signal);
        if (controller.signal.aborted) return;
        phase = result.data.phase;
        setScan(result.data);
        setScanError(undefined);
      } catch (reason) {
        if (controller.signal.aborted) return;
        setScanError(reason instanceof Error ? reason.message : String(reason));
      }
      timer = setTimeout(poll, ["queued", "running", "processing"].includes(phase) ? 2500 : 10000);
    };
    void poll();
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [api, tableName, status?.configured, status?.scan_id, scanRefresh]);
  const run = async (operation: () => Promise<void>) => {
    setBusy(true);
    setError(undefined);
    setMessage(undefined);
    try {
      await operation();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setBusy(false);
    }
  };
  const reloadPolicy = () =>
    run(async () => {
      const { data, etag: loadedEtag } = await refresh();
      setPolicy(data.policy ?? initial);
      setProducer(data.policy?.producer ? JSON.stringify(data.policy.producer, null, 2) : "");
      setCategories(data.policy?.categories ? JSON.stringify(data.policy.categories, null, 2) : "");
      setEtag(loadedEtag ?? data.etag);
      setPreview(undefined);
      setPreviewPolicy("");
      setImpactAccepted(false);
    });
  const scanSample = () =>
    run(async () => {
      const current = effectivePolicy();
      const keys = sampleKeys
        .split("\n")
        .map((key) => key.trim())
        .filter(Boolean);
      const result = await api.privacy.preview(tableName, {
        policy: current,
        sample: keys.length ? { keys } : { limit: 5 },
      });
      setPreview(result.data);
      setPreviewPolicy(JSON.stringify(current));
    });
  const save = () =>
    run(async () => {
      const current = effectivePolicy();
      if (
        current.mode !== "monitor" &&
        (!impactAccepted || previewPolicy !== JSON.stringify(current))
      )
        throw new Error(
          "Preview the current policy and acknowledge its availability impact before saving."
        );
      const result = await api.privacy.put(tableName, current, { ifMatch: etag });
      setStatus(result.data);
      setEtag(result.etag ?? result.data.etag);
      setMessage(
        "Policy accepted. Existing documents are being scanned; new writes use this policy."
      );
    });
  const reviewFalsePositive = (category: string) =>
    run(async () => {
      const current = effectivePolicy();
      const overrides = [...(current.categories ?? [])];
      const index = overrides.findIndex((entry) => entry.name === category);
      const existing = index >= 0 ? overrides[index] : { name: category };
      const defaults = { conservative: 0.75, balanced: 0.5, thorough: 0.3 };
      const threshold = Math.min(
        1,
        Math.round(
          ((existing.threshold ?? defaults[current.sensitivity ?? "balanced"]) + 0.1) * 100
        ) / 100
      );
      const changed = { ...existing, threshold };
      if (index >= 0) overrides[index] = changed;
      else overrides.push(changed);
      setCategories(JSON.stringify(overrides, null, 2));
      setMessage(
        `Raised the ${category} threshold to ${threshold}. Scan the sample again before saving. Thresholds apply to findings with detector confidence.`
      );
    });
  const rescan = () =>
    run(async () => {
      const result = await api.privacy.startScan(tableName);
      setScan(result.data);
      setStatus((current) => (current ? { ...current, scan_id: result.data.scan_id } : current));
      setMessage("Rescan queued. Progress and cursors are saved durably.");
    });
  const loadFindings = (cursor?: string) =>
    run(async () => {
      const result = await api.privacy.findings(tableName, {
        document: document || undefined,
        field: findingField || undefined,
        category: findingCategory || undefined,
        cursor,
        limit: 100,
      });
      setFindings(result.data);
    });
  const coverage = status?.coverage;

  return (
    <div className="flex flex-col gap-4">
      {scanError && (
        <Alert>
          <AlertDescription>
            Scan status is unavailable: {scanError}. Retrying automatically.
          </AlertDescription>
        </Alert>
      )}
      {error && (
        <Alert variant="destructive">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      )}
      {message && (
        <Alert>
          <AlertDescription>{message}</AlertDescription>
        </Alert>
      )}
      <Card>
        <CardHeader>
          <CardTitle>Privacy policy</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <p className="text-sm text-muted-foreground">
            Find sensitive information in selected fields and choose how Antfly uses it. Detection
            reduces exposure of identified information; “No findings detected” does not prove an
            absence of sensitive data.
          </p>
          <Field>
            <FieldLabel htmlFor="privacy-fields">Fields (comma separated)</FieldLabel>
            <Input
              id="privacy-fields"
              value={policy.fields.join(", ")}
              onChange={(event) =>
                setPolicy({
                  ...policy,
                  fields: event.target.value.split(",").map((value) => value.trim()),
                })
              }
            />
          </Field>
          <fieldset className="flex flex-wrap gap-4">
            <legend className="mb-2 text-sm font-medium">Category groups</legend>
            {groups.map((group) => (
              <label key={group} className="flex items-center gap-2 text-sm">
                <input
                  type="checkbox"
                  checked={policy.category_groups?.includes(group) ?? false}
                  onChange={(event) =>
                    setPolicy({
                      ...policy,
                      category_groups: event.target.checked
                        ? [...(policy.category_groups ?? []), group]
                        : policy.category_groups?.filter((value) => value !== group),
                    })
                  }
                />
                {labels[group]}
              </label>
            ))}
          </fieldset>
          <div className="grid gap-4 md:grid-cols-3">
            <Field>
              <FieldLabel htmlFor="privacy-mode">Mode</FieldLabel>
              <select
                id="privacy-mode"
                className={control}
                value={policy.mode}
                onChange={(event) => {
                  setPolicy({ ...policy, mode: event.target.value as PrivacyPolicy["mode"] });
                  setImpactAccepted(false);
                }}
              >
                <option value="monitor">Monitor</option>
                <option value="redact_for_search_and_ai">Redact for search and AI</option>
                <option value="redact_before_storage" disabled>
                  Redact before storage (not available)
                </option>
              </select>
            </Field>
            <Field>
              <FieldLabel htmlFor="privacy-sensitivity">Sensitivity</FieldLabel>
              <select
                id="privacy-sensitivity"
                className={control}
                value={policy.sensitivity ?? "balanced"}
                onChange={(event) =>
                  setPolicy({
                    ...policy,
                    sensitivity: event.target.value as PrivacyPolicy["sensitivity"],
                  })
                }
              >
                <option value="conservative">Conservative</option>
                <option value="balanced">Balanced</option>
                <option value="thorough">Thorough</option>
              </select>
            </Field>
            <Field>
              <FieldLabel htmlFor="privacy-replacement">Replacement</FieldLabel>
              <select
                id="privacy-replacement"
                className={control}
                value={policy.replacement ?? "category"}
                onChange={(event) =>
                  setPolicy({
                    ...policy,
                    replacement: event.target.value as PrivacyPolicy["replacement"],
                  })
                }
              >
                <option value="category">Category label</option>
                <option value="mask">[REDACTED]</option>
                <option value="remove">Remove detected text</option>
              </select>
            </Field>
          </div>
          <details>
            <summary className="cursor-pointer text-sm font-medium">
              Advanced: extractor or generator
            </summary>
            <div className="mt-3 flex flex-col gap-3">
              <p className="text-sm text-muted-foreground">
                Leave empty to use the pinned native GLiNER2 detector. Customize the artifact
                producer’s type, config and immutable revision. Use secret references for
                credentials. External providers receive original selected field content.
              </p>
              <Field>
                <FieldLabel htmlFor="privacy-categories">Custom categories JSON</FieldLabel>
                <textarea
                  id="privacy-categories"
                  rows={5}
                  className={`${control} font-mono`}
                  value={categories}
                  placeholder={
                    '[{"name":"customer_id","description":"Customer account identifiers","threshold":0.7}]'
                  }
                  onChange={(event) => setCategories(event.target.value)}
                />
                <p className="text-xs text-muted-foreground">
                  Add categories or override a group category’s description and threshold.
                </p>
              </Field>
              <Field>
                <FieldLabel htmlFor="privacy-producer">Producer JSON</FieldLabel>
                <textarea
                  id="privacy-producer"
                  rows={10}
                  className={`${control} font-mono`}
                  value={producer}
                  placeholder={
                    '{"type":"extractor","revision":"deployment-v1","config":{"provider":"antfly","model":"my-model"}}'
                  }
                  onChange={(event) => setProducer(event.target.value)}
                />
              </Field>
            </div>
          </details>
          {policy.mode !== "monitor" && (
            <Alert>
              <AlertDescription>
                <label className="flex items-start gap-2">
                  <input
                    type="checkbox"
                    className="mt-1"
                    checked={impactAccepted}
                    onChange={(event) => setImpactAccepted(event.target.checked)}
                  />
                  <span>
                    {policy.mode === "redact_before_storage"
                      ? "Pre-storage redaction is not available. The server rejects this mode until a synchronous ingestion transformation is supported."
                      : "Pending, failed or stale documents can be unavailable to protected search and AI until inspection and index publication finish."}
                  </span>
                </label>
              </AlertDescription>
            </Alert>
          )}
          <Field>
            <FieldLabel htmlFor="privacy-sample-keys">
              Sample document keys (one per line, optional)
            </FieldLabel>
            <textarea
              id="privacy-sample-keys"
              rows={3}
              className={control}
              value={sampleKeys}
              onChange={(event) => setSampleKeys(event.target.value)}
            />
            <p className="text-xs text-muted-foreground">
              Empty selects the first five documents by key. A preview is a bounded sample, never a
              complete table scan.
            </p>
          </Field>
          {etag && status?.etag && etag !== status.etag && (
            <Alert>
              <AlertDescription>
                The saved policy changed while you were editing. Reload it before saving your
                changes.
              </AlertDescription>
            </Alert>
          )}
          <div className="flex gap-2">
            <Button variant="outline" disabled={busy} onClick={reloadPolicy}>
              Reload policy
            </Button>
            <Button variant="default" disabled={busy} onClick={scanSample}>
              Scan sample
            </Button>
            <Button disabled={busy} onClick={save}>
              Save policy
            </Button>
          </div>
        </CardContent>
      </Card>
      {preview && (
        <Card>
          <CardHeader>
            <CardTitle>Sample preview · {preview.sampled} documents</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-4">
            {preview.external_processing && (
              <Alert>
                <AlertDescription>
                  The selected provider processed original sample text outside this Antfly process.
                </AlertDescription>
              </Alert>
            )}
            {preview.documents.length === 0 && (
              <p className="text-sm">No documents in the selected sample.</p>
            )}
            {preview.documents.map((row) => (
              <section key={row.document} className="flex flex-col gap-3">
                <h4 className="font-medium">{row.document}</h4>
                {row.fields?.map((field) => (
                  <div key={field.field} className="rounded-md border p-3">
                    <p className="mb-2 text-sm font-medium">
                      {field.field} ·{" "}
                      {field.findings.length
                        ? `${field.findings.length} findings`
                        : "No findings detected"}
                    </p>
                    {field.findings.length > 0 && (
                      <div className="mb-3 flex flex-wrap items-center gap-2">
                        <span className="text-xs text-muted-foreground">False positives?</span>
                        {[...new Set(field.findings.map((finding) => finding.category))].map(
                          (category) => (
                            <Button
                              key={category}
                              variant="default"
                              disabled={busy}
                              onClick={() => reviewFalsePositive(category)}
                            >
                              Raise {category} threshold
                            </Button>
                          )
                        )}
                      </div>
                    )}
                    <div className="grid gap-4 md:grid-cols-2">
                      <div>
                        <p className="mb-1 text-xs text-muted-foreground">Original</p>
                        <pre className="whitespace-pre-wrap break-words text-sm">
                          {safeHighlights(field.original, field.findings).map((part) =>
                            part.categories.length ? (
                              <mark
                                key={part.key}
                                title={part.categories.join(", ")}
                                className="rounded bg-accent px-0.5 text-accent-foreground"
                              >
                                {part.text}
                              </mark>
                            ) : (
                              part.text
                            )
                          )}
                        </pre>
                      </div>
                      <div>
                        <p className="mb-1 text-xs text-muted-foreground">Redaction preview</p>
                        <pre className="whitespace-pre-wrap break-words text-sm">{field.text}</pre>
                      </div>
                    </div>
                  </div>
                ))}
              </section>
            ))}
          </CardContent>
        </Card>
      )}
      <Card>
        <CardHeader>
          <CardTitle>Coverage and findings</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-3">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="default">
              {status?.configured ? "Policy enabled" : "No policy configured"}
            </Badge>
            {status?.policy_version && (
              <span className="font-mono text-xs">{status.policy_version.slice(0, 16)}</span>
            )}
          </div>
          {coverage?.available ? (
            <p className="text-sm">
              {coverage.current} / {coverage.total} documents current · {coverage.with_findings}{" "}
              with findings · {coverage.pending ?? "Unknown"} pending ·{" "}
              {coverage.failed ?? "Unknown"} failed
            </p>
          ) : (
            <p className="text-sm text-muted-foreground">
              Full-table coverage is being established. Unknown counts are not treated as completed
              inspection.
            </p>
          )}
          {coverage?.available && coverage.completed_at_millis != null && (
            <p className="text-xs text-muted-foreground">
              Observed {new Date(coverage.completed_at_millis).toLocaleString()}.
              {coverage.refreshing ? " Updating coverage…" : " Counts refresh in the background."}
            </p>
          )}
          {coverage?.available && status?.policy?.mode === "redact_for_search_and_ai" && (
            <p className="text-sm">
              {coverage.protected_ready ?? "Unknown"} protected-ready ·{" "}
              {coverage.protected_blocked ?? "Unknown"} blocked
            </p>
          )}
          {coverage?.categories && Object.keys(coverage.categories).length > 0 && (
            <ul className="flex flex-wrap gap-3 text-sm" aria-label="Current findings by category">
              {Object.entries(coverage.categories).map(([category, count]) => (
                <li key={category}>
                  {category}: {count}
                </li>
              ))}
            </ul>
          )}
          {coverage?.fields && Object.keys(coverage.fields).length > 0 && (
            <ul className="flex flex-wrap gap-3 text-sm" aria-label="Current findings by field">
              {Object.entries(coverage.fields).map(([field, count]) => (
                <li key={field}>
                  {field}: {count} findings
                </li>
              ))}
            </ul>
          )}
          {scan && (
            <p className="text-sm">
              Scan: {scan.phase} · {scan.scanned ?? 0} examined · {scan.reprocessed ?? 0} scheduled
              · {scan.failed ?? 0} failed
            </p>
          )}
          {scan?.last_error && <p className="text-sm text-destructive">{scan.last_error}</p>}
          {!!scan?.failures?.length && (
            <details className="text-sm">
              <summary>Document failures ({scan.failures.length})</summary>
              <ul className="mt-2 space-y-1">
                {scan.failures.map((failure) => (
                  <li key={`${String(failure.key)}:${String(failure.error_code)}`}>
                    {String(failure.key ?? "Unknown document")}:{" "}
                    {String(failure.error_code ?? "Inspection failed")}
                  </li>
                ))}
              </ul>
            </details>
          )}
          <div className="flex gap-2">
            <Button
              variant="default"
              disabled={busy}
              onClick={() =>
                run(async () => {
                  setScanRefresh((current) => current + 1);
                  await refresh();
                })
              }
            >
              Refresh
            </Button>
            <Button variant="default" disabled={busy || !status?.configured} onClick={rescan}>
              Rescan / retry
            </Button>
          </div>
          <Field>
            <FieldLabel htmlFor="privacy-document">Findings for document (optional)</FieldLabel>
            <Input
              id="privacy-document"
              value={document}
              onChange={(event) => {
                setDocument(event.target.value);
                setFindings(undefined);
              }}
            />
          </Field>
          <div className="grid gap-3 md:grid-cols-2">
            <Field>
              <FieldLabel htmlFor="privacy-finding-field">Field filter</FieldLabel>
              <Input
                id="privacy-finding-field"
                value={findingField}
                onChange={(event) => {
                  setFindingField(event.target.value);
                  setFindings(undefined);
                }}
              />
            </Field>
            <Field>
              <FieldLabel htmlFor="privacy-finding-category">Category filter</FieldLabel>
              <Input
                id="privacy-finding-category"
                value={findingCategory}
                onChange={(event) => {
                  setFindingCategory(event.target.value);
                  setFindings(undefined);
                }}
              />
            </Field>
          </div>
          <Button
            variant="default"
            disabled={busy || !status?.configured}
            onClick={() => loadFindings()}
          >
            Inspect findings
          </Button>
          {findings && (
            <>
              <p className="text-sm">
                {findings.findings.length
                  ? `${findings.findings.length} findings on this page`
                  : "No findings detected on this page"}{" "}
                · {findings.examined_documents ?? 0} documents examined ·{" "}
                {findings.pending_documents ?? 0} pending
              </p>
              <div className="overflow-auto">
                <table className="w-full text-left text-sm">
                  <thead>
                    <tr>
                      <th>Document</th>
                      <th>Field</th>
                      <th>Category</th>
                      <th>UTF-8 span</th>
                      <th>Detector confidence</th>
                    </tr>
                  </thead>
                  <tbody>
                    {findings.findings.map((finding) => (
                      <tr
                        key={`${finding.document}:${finding.field}:${finding.category}:${finding.start}:${finding.end}`}
                      >
                        <td>{finding.document}</td>
                        <td>{finding.field}</td>
                        <td>{finding.category}</td>
                        <td>
                          {finding.start}–{finding.end}
                        </td>
                        <td>
                          {finding.confidence == null
                            ? "Unavailable"
                            : finding.confidence.toFixed(3)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {findings.next_cursor && (
                <Button
                  variant="default"
                  disabled={busy}
                  onClick={() => loadFindings(findings.next_cursor ?? undefined)}
                >
                  Next page
                </Button>
              )}
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
