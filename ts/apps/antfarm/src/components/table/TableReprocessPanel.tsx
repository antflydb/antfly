import {
  Alert,
  AlertDescription,
  Badge,
  Button,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  Input,
  Label,
} from "@antfly/design-system";
import type {
  DocumentArtifactReprocessJob,
  DocumentArtifactTableReprocessRequest,
  DocumentArtifactTableReprocessResponse,
} from "@antfly/sdk";
import { useState } from "react";
import { useApi } from "@/hooks/use-api-config";
import JsonViewer from "../JsonViewer";

function formatCount(value: number | null | undefined) {
  return value === null || value === undefined ? "-" : Intl.NumberFormat().format(value);
}

function parseOptionalLimit(value: string) {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : undefined;
}

function jobStatusClass(job: DocumentArtifactReprocessJob) {
  if (job.phase === "failed" || job.phase === "cancelled") return "af-status-badge-error";
  if (job.phase === "running" || job.reprocess_status === "in_progress") {
    return "af-status-badge-warning";
  }
  return "af-status-badge-success";
}

function isTerminalJob(job: DocumentArtifactReprocessJob) {
  return ["succeeded", "failed", "cancelled"].includes(job.phase);
}

interface TableReprocessPanelProps {
  tableName: string;
}

export function TableReprocessPanel({ tableName }: TableReprocessPanelProps) {
  const api = useApi();
  const [artifactName, setArtifactName] = useState("document_units_v1");
  const [fromKey, setFromKey] = useState("");
  const [toKey, setToKey] = useState("");
  const [limit, setLimit] = useState("100");
  const [job, setJob] = useState<DocumentArtifactReprocessJob | null>(null);
  const [pass, setPass] = useState<DocumentArtifactTableReprocessResponse | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const buildRequest = (): DocumentArtifactTableReprocessRequest => {
    const parsedLimit = parseOptionalLimit(limit);
    return {
      ...(fromKey.trim() ? { from_key: fromKey.trim() } : {}),
      ...(toKey.trim() ? { to_key: toKey.trim() } : {}),
      ...(parsedLimit ? { limit: parsedLimit } : {}),
    };
  };

  const handleRunPass = async () => {
    const name = artifactName.trim();
    if (!tableName || !name) return;
    setIsLoading(true);
    setError(null);
    try {
      const response = await api.tables.artifacts.reprocessRange(tableName, name, buildRequest());
      setPass(response ?? null);
      setJob(null);
    } catch (e) {
      setError(`Failed to run reprocess pass for ${name}.`);
      console.error(e);
    } finally {
      setIsLoading(false);
    }
  };

  const handleStartJob = async () => {
    const name = artifactName.trim();
    if (!tableName || !name) return;
    setIsLoading(true);
    setError(null);
    try {
      const response = await api.tables.artifacts.startReprocessJob(tableName, name, {
        ...buildRequest(),
        advance: true,
      });
      setJob(response ?? null);
      setPass(null);
    } catch (e) {
      setError(`Failed to start reprocess job for ${name}.`);
      console.error(e);
    } finally {
      setIsLoading(false);
    }
  };

  const handleAdvanceJob = async () => {
    if (!tableName || !job) return;
    setIsLoading(true);
    setError(null);
    try {
      const response = await api.tables.artifacts.advanceReprocessJob(
        tableName,
        job.artifact_name,
        String(job.job_id)
      );
      setJob(response ?? null);
    } catch (e) {
      setError(`Failed to advance job ${job.job_id}.`);
      console.error(e);
    } finally {
      setIsLoading(false);
    }
  };

  const handleCancelJob = async () => {
    if (!tableName || !job) return;
    setIsLoading(true);
    setError(null);
    try {
      const response = await api.tables.artifacts.cancelReprocessJob(
        tableName,
        job.artifact_name,
        String(job.job_id)
      );
      setJob(response ?? null);
    } catch (e) {
      setError(`Failed to cancel job ${job.job_id}.`);
      console.error(e);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="grid gap-4 xl:grid-cols-[420px_1fr]">
      <Card>
        <CardHeader>
          <CardTitle>Table Reprocess</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="reprocess-artifact-name">Artifact name</Label>
            <Input
              id="reprocess-artifact-name"
              value={artifactName}
              onChange={(event) => setArtifactName(event.target.value)}
              placeholder="document_units_v1"
            />
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="reprocess-from-key">From key</Label>
              <Input
                id="reprocess-from-key"
                value={fromKey}
                onChange={(event) => setFromKey(event.target.value)}
                placeholder="exclusive"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="reprocess-to-key">To key</Label>
              <Input
                id="reprocess-to-key"
                value={toKey}
                onChange={(event) => setToKey(event.target.value)}
                placeholder="inclusive"
              />
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="reprocess-limit">Pass limit</Label>
            <Input
              id="reprocess-limit"
              value={limit}
              onChange={(event) => setLimit(event.target.value)}
              inputMode="numeric"
            />
          </div>

          {error && (
            <Alert variant="destructive">
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          <div className="flex flex-wrap gap-2">
            <Button onClick={handleStartJob} disabled={isLoading || !artifactName.trim()}>
              Start job
            </Button>
            <Button
              variant="outline"
              onClick={handleRunPass}
              disabled={isLoading || !artifactName.trim()}
            >
              Run bounded pass
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Reprocess Status</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {job ? (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <Badge className={jobStatusClass(job)}>{job.phase}</Badge>
                <Badge>{job.reprocess_status}</Badge>
                <span className="text-sm text-muted-foreground">
                  Job {job.job_id} for {job.artifact_name}
                </span>
              </div>
              <div className="grid gap-3 md:grid-cols-5">
                <div>
                  <div className="text-xs text-muted-foreground">Scanned</div>
                  <div className="text-lg font-semibold">{formatCount(job.scanned)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Reprocessed</div>
                  <div className="text-lg font-semibold">{formatCount(job.reprocessed)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Skipped</div>
                  <div className="text-lg font-semibold">{formatCount(job.skipped)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Failed</div>
                  <div className="text-lg font-semibold">{formatCount(job.failed)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Pending Shards</div>
                  <div className="text-lg font-semibold">{formatCount(job.pending_shards)}</div>
                </div>
              </div>
              <div className="flex flex-wrap gap-2">
                <Button
                  variant="outline"
                  onClick={handleAdvanceJob}
                  disabled={isLoading || isTerminalJob(job)}
                >
                  Advance
                </Button>
                <Button
                  variant="ghost"
                  onClick={handleCancelJob}
                  disabled={isLoading || isTerminalJob(job)}
                >
                  Cancel
                </Button>
              </div>
              {job.failures.length > 0 && <JsonViewer json={{ failures: job.failures }} />}
            </>
          ) : pass ? (
            <>
              <div className="flex flex-wrap items-center gap-2">
                <Badge>{pass.reprocess_status}</Badge>
                <span className="text-sm text-muted-foreground">
                  Bounded pass for {pass.artifact_name}
                </span>
              </div>
              <div className="grid gap-3 md:grid-cols-4">
                <div>
                  <div className="text-xs text-muted-foreground">Scanned</div>
                  <div className="text-lg font-semibold">{formatCount(pass.scanned)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Reprocessed</div>
                  <div className="text-lg font-semibold">{formatCount(pass.reprocessed)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Skipped</div>
                  <div className="text-lg font-semibold">{formatCount(pass.skipped)}</div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Failed</div>
                  <div className="text-lg font-semibold">{formatCount(pass.failed)}</div>
                </div>
              </div>
              <JsonViewer json={pass} />
            </>
          ) : (
            <div className="rounded border border-dashed p-6 text-sm text-muted-foreground">
              Start a durable job for table-wide repair, or run one bounded pass when you want to
              manually inspect cursor progress.
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
