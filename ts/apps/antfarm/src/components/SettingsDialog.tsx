import { Settings } from "lucide-react";
import type { ReactNode } from "react";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useApiConfig } from "@/hooks/use-api-config";

interface SettingsDialogProps {
  trigger?: ReactNode;
}

export function SettingsDialog({ trigger }: SettingsDialogProps = {}) {
  const { apiUrl, setApiUrl, resetToDefault, termiteApiUrl, setTermiteApiUrl, resetTermiteApiUrl } =
    useApiConfig();
  const [tempUrl, setTempUrl] = useState(apiUrl);
  const [tempTermiteUrl, setTempTermiteUrl] = useState(termiteApiUrl);
  const [isOpen, setIsOpen] = useState(false);

  const handleSave = () => {
    setApiUrl(tempUrl);
    setTermiteApiUrl(tempTermiteUrl);
    setIsOpen(false);
  };

  const handleReset = () => {
    resetToDefault();
    resetTermiteApiUrl();
    setTempUrl(apiUrl);
    setTempTermiteUrl(termiteApiUrl);
  };

  const handleCancel = () => {
    setTempUrl(apiUrl);
    setTempTermiteUrl(termiteApiUrl);
    setIsOpen(false);
  };

  return (
    <Dialog open={isOpen} onOpenChange={setIsOpen}>
      <DialogTrigger asChild>
        {trigger || (
          <Button variant="ghost" size="icon" title="Settings">
            <Settings className="h-4 w-4" />
          </Button>
        )}
      </DialogTrigger>
      <DialogContent className="sm:max-w-131.25">
        <DialogHeader>
          <DialogTitle>API Settings</DialogTitle>
          <DialogDescription>
            Configure the Antfly and Termite servers to connect to. This is useful when accessing
            the dashboard remotely or connecting to different servers.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-6 py-4">
          {/* Antfly API URL */}
          <div className="grid gap-2">
            <Label htmlFor="api-url">Antfly API URL</Label>
            <Input
              id="api-url"
              value={tempUrl}
              onChange={(e) => setTempUrl(e.target.value)}
              placeholder="http://localhost:8082/api/v1"
            />
            <p className="text-sm text-muted-foreground">
              Current: <code className="text-xs bg-muted px-1 py-0.5 rounded">{apiUrl}</code>
            </p>
            <div className="text-xs text-muted-foreground">
              Examples: <code className="bg-muted px-1 py-0.5 rounded">/api/v1</code> (default),{" "}
              <code className="bg-muted px-1 py-0.5 rounded">http://server:8082/api/v1</code>
            </div>
          </div>

          {/* Termite API URL */}
          <div className="grid gap-2">
            <Label htmlFor="termite-url">Termite API URL</Label>
            <Input
              id="termite-url"
              value={tempTermiteUrl}
              onChange={(e) => setTempTermiteUrl(e.target.value)}
              placeholder="http://localhost:11433"
            />
            <p className="text-sm text-muted-foreground">
              Current: <code className="text-xs bg-muted px-1 py-0.5 rounded">{termiteApiUrl}</code>
            </p>
            <div className="text-xs text-muted-foreground">
              Examples: <code className="bg-muted px-1 py-0.5 rounded">http://localhost:11433</code>{" "}
              (default),{" "}
              <code className="bg-muted px-1 py-0.5 rounded">https://termite.company.com</code>
            </div>
          </div>
        </div>
        <DialogFooter className="gap-2">
          <Button variant="outline" onClick={handleReset}>
            Reset to Default
          </Button>
          <Button variant="outline" onClick={handleCancel}>
            Cancel
          </Button>
          <Button onClick={handleSave}>Save</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
