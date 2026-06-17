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
import type { DocumentArtifactManifest } from "@antfly/sdk";
import { useCallback, useState } from "react";
import { useApi } from "@/hooks/use-api-config";
import JsonViewer from "../JsonViewer";

function formatCount(value: number | null | undefined) {
  return value === null || value === undefined ? "-" : Intl.NumberFormat().format(value);
}

function artifactHealthClass(artifact: DocumentArtifactManifest) {
  if (artifact.last_error_code || artifact.unsupported_reason) return "af-status-badge-error";
  if (artifact.merge_status && artifact.merge_status !== "complete") {
    return "af-status-badge-warning";
  }
  return "af-status-badge-success";
}

function artifactHealthLabel(artifact: DocumentArtifactManifest) {
  if (artifact.last_error_code) return artifact.last_error_code;
  if (artifact.unsupported_reason) return "unsupported";
  return artifact.merge_status || "ready";
}

interface DocumentArtifactsPanelProps {
  tableName: string;
}

export function DocumentArtifactsPanel({ tableName }: DocumentArtifactsPanelProps) {
  const api = useApi();
  const [documentKey, setDocumentKey] = useState("");
  const [manifests, setManifests] = useState<DocumentArtifactManifest[]>([]);
  const [selectedArtifactName, setSelectedArtifactName] = useState("");
  const [selectedArtifactDetail, setSelectedArtifactDetail] =
    useState<DocumentArtifactManifest | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const resetArtifactState = () => {
    setManifests([]);
    setSelectedArtifactName("");
    setSelectedArtifactDetail(null);
    setMessage(null);
    setError(null);
  };

  const handleDocumentKeyChange = (value: string) => {
    setDocumentKey(value);
    resetArtifactState();
  };

  const loadArtifactDetail = useCallback(
    async (artifactName: string) => {
      const key = documentKey.trim();
      if (!tableName || !key || !artifactName) return;
      setIsLoading(true);
      setError(null);
      try {
        const detail = await api.tables.artifacts.get(tableName, key, artifactName, "raw");
        setSelectedArtifactDetail(detail ?? null);
      } catch (e) {
        setError(`Failed to load artifact ${artifactName}.`);
        console.error(e);
      } finally {
        setIsLoading(false);
      }
    },
    [api, documentKey, tableName]
  );

  const loadManifests = useCallback(async () => {
    const key = documentKey.trim();
    if (!tableName || !key) return;
    setIsLoading(true);
    setError(null);
    setMessage(null);
    try {
      const response = await api.tables.artifacts.list(tableName, key);
      const artifacts = response?.artifacts ?? [];
      setManifests(artifacts);
      const nextSelected =
        artifacts.find((artifact) => artifact.artifact_name === selectedArtifactName)
          ?.artifact_name ??
        artifacts[0]?.artifact_name ??
        "";
      setSelectedArtifactName(nextSelected);
      if (nextSelected) {
        const detail = await api.tables.artifacts.get(tableName, key, nextSelected, "raw");
        setSelectedArtifactDetail(detail ?? null);
      } else {
        setSelectedArtifactDetail(null);
      }
    } catch (e) {
      setManifests([]);
      setSelectedArtifactDetail(null);
      setError(`No artifact manifests found for ${key}.`);
      console.error(e);
    } finally {
      setIsLoading(false);
    }
  }, [api, documentKey, selectedArtifactName, tableName]);

  const handleSelectArtifact = async (artifactName: string) => {
    setSelectedArtifactName(artifactName);
    await loadArtifactDetail(artifactName);
  };

  const handleReprocessArtifact = async () => {
    const key = documentKey.trim();
    if (!tableName || !key || !selectedArtifactName) return;
    setIsLoading(true);
    setError(null);
    setMessage(null);
    try {
      await api.tables.artifacts.reprocessDocument(tableName, key, selectedArtifactName);
      setMessage(`Reprocess accepted for ${selectedArtifactName}.`);
      await loadArtifactDetail(selectedArtifactName);
    } catch (e) {
      setError(`Failed to reprocess ${selectedArtifactName}.`);
      console.error(e);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="grid gap-4 xl:grid-cols-[360px_1fr]">
      <Card>
        <CardHeader>
          <CardTitle>Document Artifacts</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="artifact-document-key">Document key</Label>
            <div className="flex gap-2">
              <Input
                id="artifact-document-key"
                value={documentKey}
                onChange={(event) => handleDocumentKeyChange(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    event.preventDefault();
                    loadManifests();
                  }
                }}
                placeholder="doc:a"
              />
              <Button onClick={loadManifests} disabled={isLoading || !documentKey.trim()}>
                Load
              </Button>
            </div>
          </div>

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

          <div className="space-y-2">
            <div className="text-sm font-medium">Manifests</div>
            {manifests.length === 0 ? (
              <div className="rounded border border-dashed p-4 text-sm text-muted-foreground">
                Enter a document key to inspect derived artifacts.
              </div>
            ) : (
              <div className="space-y-2">
                {manifests.map((artifact) => (
                  <button
                    key={artifact.artifact_name}
                    type="button"
                    className={`w-full rounded border p-3 text-left transition-colors hover:bg-muted ${
                      selectedArtifactName === artifact.artifact_name
                        ? "border-primary bg-muted"
                        : "border-border"
                    }`}
                    onClick={() => handleSelectArtifact(artifact.artifact_name)}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="font-medium">{artifact.artifact_name}</span>
                      <Badge className={artifactHealthClass(artifact)}>
                        {artifactHealthLabel(artifact)}
                      </Badge>
                    </div>
                    <div className="mt-2 grid grid-cols-3 gap-2 text-xs text-muted-foreground">
                      <span>{formatCount(artifact.unit_count)} units</span>
                      <span>{formatCount(artifact.chunk_count)} chunks</span>
                      <span>gen {artifact.generation}</span>
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>

          <Button
            variant="outline"
            onClick={handleReprocessArtifact}
            disabled={isLoading || !selectedArtifactName}
            className="w-full"
          >
            Reprocess selected artifact
          </Button>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Artifact Detail</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {selectedArtifactDetail ? (
            <>
              <div className="grid gap-3 md:grid-cols-4">
                <div>
                  <div className="text-xs text-muted-foreground">Content Type</div>
                  <div className="text-sm font-medium">
                    {selectedArtifactDetail.content_type || "-"}
                  </div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Route</div>
                  <div className="text-sm font-medium">
                    {selectedArtifactDetail.route_type || "-"}
                  </div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Ranges</div>
                  <div className="text-sm font-medium">
                    {formatCount(selectedArtifactDetail.child_range_count)}
                  </div>
                </div>
                <div>
                  <div className="text-xs text-muted-foreground">Merge</div>
                  <div className="text-sm font-medium">
                    {selectedArtifactDetail.merge_status || "-"}
                  </div>
                </div>
              </div>

              {(selectedArtifactDetail.last_error_message ||
                selectedArtifactDetail.unsupported_reason) && (
                <Alert variant="destructive">
                  <AlertDescription>
                    {selectedArtifactDetail.last_error_message ||
                      selectedArtifactDetail.unsupported_reason}
                  </AlertDescription>
                </Alert>
              )}

              <div className="space-y-2">
                <div className="text-sm font-medium">Manifest</div>
                <JsonViewer json={selectedArtifactDetail} />
              </div>
            </>
          ) : (
            <div className="rounded border border-dashed p-6 text-sm text-muted-foreground">
              Select an artifact manifest to inspect its route, generation, chunks, ranges, errors,
              and raw manifest state.
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
