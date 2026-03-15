import { Check, Copy, Download, ExternalLink } from "lucide-react";
import { useState } from "react";
import { Link } from "react-router-dom";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  getDownloadCommand,
  getModelsByType,
  getModelsWithCapability,
  type ModelType,
  type RecognizerCapability,
} from "@/data/termite-models";
import { useTermiteRegistry } from "@/hooks/use-termite-registry";

function CopyCommandButton({ command }: { command: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(command);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <button
      type="button"
      onClick={handleCopy}
      className="shrink-0 p-1.5 rounded hover:bg-black/10 dark:hover:bg-white/10 transition-colors"
      title="Copy command"
    >
      {copied ? (
        <Check className="h-3.5 w-3.5 text-green-600 dark:text-green-400" />
      ) : (
        <Copy className="h-3.5 w-3.5 text-muted-foreground" />
      )}
    </button>
  );
}

interface NoModelsGuideProps {
  modelType: ModelType;
  requiredCapability?: RecognizerCapability;
  typeName: string;
  /** Use muted palette instead of amber (for playgrounds with a fallback mode) */
  soft?: boolean;
}

export function NoModelsGuide({
  modelType,
  requiredCapability,
  typeName,
  soft = false,
}: NoModelsGuideProps) {
  const { models, loading } = useTermiteRegistry();

  // Don't flash while loading
  if (loading) return null;

  // Get recommended models from the registry
  let recommended = getModelsByType(models, modelType);
  if (requiredCapability) {
    recommended = getModelsWithCapability(recommended, requiredCapability);
  }

  // Show up to 2 recommendations
  const suggestions = recommended.slice(0, 2);

  const containerClasses = soft
    ? "mb-4 p-4 rounded-lg border bg-muted/30 border-border"
    : "mb-4 p-4 rounded-lg border bg-amber-50 dark:bg-amber-950/30 border-amber-200 dark:border-amber-800";

  const titleClasses = soft
    ? "text-sm font-medium text-foreground"
    : "text-sm font-medium text-amber-900 dark:text-amber-100";

  const descClasses = soft
    ? "text-xs text-muted-foreground"
    : "text-xs text-amber-800 dark:text-amber-200";

  return (
    <div className={containerClasses}>
      <div className="flex items-start gap-3">
        <Download
          className={`h-4 w-4 mt-0.5 shrink-0 ${soft ? "text-muted-foreground" : "text-amber-600 dark:text-amber-400"}`}
        />
        <div className="flex-1 min-w-0">
          <p className={titleClasses}>No {typeName} models installed</p>
          <p className={`${descClasses} mt-1`}>
            {soft
              ? `Download a model for semantic ${typeName} capabilities. The fixed-token mode still works without a model.`
              : `Download a ${typeName} model to get started. Use the commands below or browse the model catalog.`}
          </p>

          {suggestions.length > 0 && (
            <div className="mt-3 space-y-2">
              {suggestions.map((model) => {
                const command = getDownloadCommand(model);
                return (
                  <div
                    key={model.id}
                    className="flex items-center gap-2 rounded-md border bg-background/80 p-2"
                  >
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 mb-1">
                        <span className="text-xs font-medium truncate">{model.name}</span>
                        {model.size && (
                          <Badge variant="outline" className="text-[10px] px-1.5 py-0 h-4 shrink-0">
                            {model.size}
                          </Badge>
                        )}
                      </div>
                      <code className="text-[11px] text-muted-foreground font-mono block truncate">
                        {command}
                      </code>
                    </div>
                    <CopyCommandButton command={command} />
                  </div>
                );
              })}
            </div>
          )}

          <div className="mt-3">
            <Button variant="link" size="sm" className="h-auto p-0 text-xs" asChild>
              <Link to="/models">
                Browse all models
                <ExternalLink className="h-3 w-3 ml-1" />
              </Link>
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
