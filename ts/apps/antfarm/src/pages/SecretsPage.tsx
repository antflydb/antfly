import { AlertTriangle, Eye, EyeOff, KeyRound, Plus, Trash2 } from "lucide-react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Alert } from "../components/ui/alert";
import { Badge } from "../components/ui/badge";
import { Button } from "../components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "../components/ui/dialog";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../components/ui/table";
import { useApiConfig } from "../hooks/use-api-config";

interface SecretEntry {
  key: string;
  status: "configured_keystore" | "configured_env" | "configured_both";
  env_var?: string;
  created_at?: string;
  updated_at?: string;
}

interface SecretList {
  secrets: SecretEntry[];
}

const COMMON_SECRETS = [
  { key: "openai.api_key", label: "OpenAI" },
  { key: "anthropic.api_key", label: "Anthropic" },
  { key: "gemini.api_key", label: "Gemini" },
  { key: "cohere.api_key", label: "Cohere" },
  { key: "openrouter.api_key", label: "OpenRouter" },
];

function statusBadge(status: SecretEntry["status"]) {
  switch (status) {
    case "configured_keystore":
      return (
        <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
          Keystore
        </Badge>
      );
    case "configured_env":
      return (
        <Badge className="bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200">
          Env Var
        </Badge>
      );
    case "configured_both":
      return (
        <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
          Both
        </Badge>
      );
  }
}

export function SecretsPage() {
  const { apiUrl } = useApiConfig();
  const [secrets, setSecrets] = useState<SecretEntry[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState("");
  const [swarmMode, setSwarmMode] = useState(false);

  // Add secret dialog state
  const [addDialogOpen, setAddDialogOpen] = useState(false);
  const [newKey, setNewKey] = useState("");
  const [newValue, setNewValue] = useState("");
  const [showValue, setShowValue] = useState(false);
  const [addError, setAddError] = useState("");
  const [selectedPreset, setSelectedPreset] = useState("");

  // Build auth headers from stored credentials
  const authHeaders = useMemo(() => {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    const stored = localStorage.getItem("antfly_auth");
    if (stored) {
      try {
        const { username, password } = JSON.parse(stored);
        headers.Authorization = `Basic ${btoa(`${username}:${password}`)}`;
      } catch {
        // ignore
      }
    }
    return headers;
  }, []);

  // Check swarm mode from status endpoint
  useEffect(() => {
    const checkMode = async () => {
      try {
        const response = await fetch(`${apiUrl}/status`, { headers: authHeaders });
        if (response.ok) {
          const data = await response.json();
          setSwarmMode(data.swarm_mode === true);
        }
      } catch {
        // ignore
      }
    };
    checkMode();
  }, [apiUrl, authHeaders]);

  // Fetch secrets list
  const fetchSecrets = useCallback(async () => {
    try {
      setIsLoading(true);
      setError("");
      const response = await fetch(`${apiUrl}/secrets`, { headers: authHeaders });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
      const data = (await response.json()) as SecretList;
      setSecrets(data.secrets || []);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load secrets");
    } finally {
      setIsLoading(false);
    }
  }, [apiUrl, authHeaders]);

  // Add secret
  const handleAddSecret = async () => {
    setAddError("");
    const key = newKey.trim();
    if (!key || !newValue) {
      setAddError("Key and value are required");
      return;
    }

    if (!/^[a-zA-Z0-9._-]+$/.test(key)) {
      setAddError("Invalid key format. Use letters, numbers, dots, dashes, or underscores.");
      return;
    }

    try {
      const response = await fetch(`${apiUrl}/secrets/${encodeURIComponent(key)}`, {
        method: "PUT",
        headers: authHeaders,
        body: JSON.stringify({ value: newValue }),
      });
      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to store secret");
      }
      setAddDialogOpen(false);
      setNewKey("");
      setNewValue("");
      setShowValue(false);
      setSelectedPreset("");
      fetchSecrets();
    } catch (err) {
      setAddError(err instanceof Error ? err.message : "Failed to store secret");
    }
  };

  // Delete secret
  const handleDeleteSecret = async (key: string) => {
    if (!confirm(`Are you sure you want to delete secret "${key}" from the keystore?`)) {
      return;
    }

    try {
      const response = await fetch(`${apiUrl}/secrets/${encodeURIComponent(key)}`, {
        method: "DELETE",
        headers: authHeaders,
      });
      if (!response.ok && response.status !== 204) {
        const data = await response.json();
        throw new Error(data.error || "Failed to delete secret");
      }
      fetchSecrets();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete secret");
    }
  };

  // Handle preset selection
  const handlePresetChange = (value: string) => {
    setSelectedPreset(value);
    if (value && value !== "custom") {
      setNewKey(value);
    } else {
      setNewKey("");
    }
  };

  // Load secrets on mount
  useEffect(() => {
    fetchSecrets();
  }, [fetchSecrets]);

  const isHttpNonLocal =
    typeof window !== "undefined" &&
    window.location.protocol === "http:" &&
    !["localhost", "127.0.0.1", "[::1]"].includes(window.location.hostname);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Secrets</h1>
          <p className="text-muted-foreground mt-1">
            Manage API keys and credentials for AI providers
          </p>
        </div>
        {swarmMode && (
          <Dialog open={addDialogOpen} onOpenChange={setAddDialogOpen}>
            <DialogTrigger asChild>
              <Button>
                <Plus className="mr-2 size-4" />
                Add Secret
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Add Secret</DialogTitle>
                <DialogDescription>
                  Store an API key or credential. Values are encrypted at rest and never returned by
                  the API.
                </DialogDescription>
              </DialogHeader>
              <div className="space-y-4">
                {addError && (
                  <Alert variant="destructive">
                    <p className="text-sm">{addError}</p>
                  </Alert>
                )}
                {isHttpNonLocal && (
                  <Alert>
                    <AlertTriangle className="size-4" />
                    <p className="text-sm">
                      You are connected over HTTP (unencrypted). Secrets will be transmitted in
                      plaintext. Use HTTPS for production deployments.
                    </p>
                  </Alert>
                )}
                <div className="space-y-2">
                  <Label>Provider</Label>
                  <Select value={selectedPreset} onValueChange={handlePresetChange}>
                    <SelectTrigger>
                      <SelectValue placeholder="Select a provider or enter custom..." />
                    </SelectTrigger>
                    <SelectContent>
                      {COMMON_SECRETS.map((s) => (
                        <SelectItem key={s.key} value={s.key}>
                          {s.label}
                        </SelectItem>
                      ))}
                      <SelectItem value="custom">Custom...</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                {(selectedPreset === "custom" || !selectedPreset) && (
                  <div className="space-y-2">
                    <Label htmlFor="secret-key">Key</Label>
                    <Input
                      id="secret-key"
                      value={newKey}
                      onChange={(e) => setNewKey(e.target.value)}
                      placeholder="e.g., openai.api_key"
                    />
                  </div>
                )}
                <div className="space-y-2">
                  <Label htmlFor="secret-value">Value</Label>
                  <div className="relative">
                    <Input
                      id="secret-value"
                      type={showValue ? "text" : "password"}
                      value={newValue}
                      onChange={(e) => setNewValue(e.target.value)}
                      placeholder="Enter API key or secret"
                      className="pr-10"
                    />
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      className="absolute right-0 top-0 h-full px-3 hover:bg-transparent"
                      onClick={() => setShowValue(!showValue)}
                    >
                      {showValue ? (
                        <EyeOff className="size-4 text-muted-foreground" />
                      ) : (
                        <Eye className="size-4 text-muted-foreground" />
                      )}
                    </Button>
                  </div>
                </div>
              </div>
              <DialogFooter>
                <Button variant="outline" onClick={() => setAddDialogOpen(false)}>
                  Cancel
                </Button>
                <Button onClick={handleAddSecret}>Store Secret</Button>
              </DialogFooter>
            </DialogContent>
          </Dialog>
        )}
      </div>

      {!swarmMode && (
        <Alert>
          <AlertTriangle className="size-4" />
          <p className="text-sm">
            Secret management via the dashboard is only available in single-node (swarm) mode. In
            multi-node deployments, configure secrets using environment variables, Kubernetes
            secrets, or the <code>antfly keystore add</code> CLI on each node.
          </p>
        </Alert>
      )}

      {error && (
        <Alert variant="destructive">
          <p className="text-sm">{error}</p>
        </Alert>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <KeyRound className="size-5" />
            Configured Secrets
          </CardTitle>
          <CardDescription>
            API keys and credentials available to the server. Values are never exposed.
          </CardDescription>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <p className="text-muted-foreground">Loading secrets...</p>
          ) : secrets.length === 0 ? (
            <p className="text-muted-foreground">
              No secrets configured. Add API keys for AI providers to enable embedding generation
              and RAG features.
            </p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Key</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Env Var</TableHead>
                  <TableHead>Updated</TableHead>
                  {swarmMode && <TableHead className="text-right">Actions</TableHead>}
                </TableRow>
              </TableHeader>
              <TableBody>
                {secrets.map((secret) => (
                  <TableRow key={secret.key}>
                    <TableCell className="font-mono text-sm">{secret.key}</TableCell>
                    <TableCell>{statusBadge(secret.status)}</TableCell>
                    <TableCell className="font-mono text-sm text-muted-foreground">
                      {secret.env_var || "-"}
                    </TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {secret.updated_at ? new Date(secret.updated_at).toLocaleDateString() : "-"}
                    </TableCell>
                    {swarmMode && (
                      <TableCell className="text-right">
                        {(secret.status === "configured_keystore" ||
                          secret.status === "configured_both") && (
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleDeleteSecret(secret.key)}
                          >
                            <Trash2 className="size-4 text-destructive" />
                          </Button>
                        )}
                      </TableCell>
                    )}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {swarmMode && (
        <Card>
          <CardHeader>
            <CardTitle className="text-sm font-medium">Quick Add</CardTitle>
            <CardDescription>Common AI provider API keys</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="flex flex-wrap gap-2">
              {COMMON_SECRETS.filter(
                (s) => !secrets.some((existing) => existing.key === s.key)
              ).map((s) => (
                <Button
                  key={s.key}
                  variant="outline"
                  size="sm"
                  onClick={() => {
                    setSelectedPreset(s.key);
                    setNewKey(s.key);
                    setNewValue("");
                    setAddDialogOpen(true);
                  }}
                >
                  <Plus className="mr-1 size-3" />
                  {s.label}
                </Button>
              ))}
              {COMMON_SECRETS.every((s) => secrets.some((existing) => existing.key === s.key)) && (
                <p className="text-sm text-muted-foreground">
                  All common provider keys are configured.
                </p>
              )}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
