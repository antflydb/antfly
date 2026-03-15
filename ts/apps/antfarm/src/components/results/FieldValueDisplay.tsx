import { Check, ChevronDown, ChevronRight, Copy, ExternalLink } from "lucide-react";
import type React from "react";
import { useMemo, useState } from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark, oneLight } from "react-syntax-highlighter/dist/esm/styles/prism";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { useTheme } from "@/hooks/use-theme";

interface FieldValueDisplayProps {
  value: unknown;
  fieldName: string;
  maxLength?: number;
  compact?: boolean;
}

const FieldValueDisplay: React.FC<FieldValueDisplayProps> = ({
  value,
  fieldName,
  maxLength = 200,
  compact = false,
}) => {
  const { theme } = useTheme();
  const [copied, setCopied] = useState(false);
  const [isExpanded, setIsExpanded] = useState(false);

  const handleCopy = (text: string) => {
    navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  // Detect if value is a URL
  const isUrl = useMemo(() => {
    if (typeof value !== "string") return false;
    try {
      new URL(value);
      return value.startsWith("http://") || value.startsWith("https://");
    } catch {
      return false;
    }
  }, [value]);

  // Detect if value is an image URL
  const isImageUrl = useMemo(() => {
    if (!isUrl || typeof value !== "string") return false;
    return /\.(jpg|jpeg|png|gif|webp|svg)$/i.test(value);
  }, [isUrl, value]);

  // Render null/undefined
  if (value === null || value === undefined) {
    return (
      <Badge variant="secondary" className="font-mono text-xs">
        null
      </Badge>
    );
  }

  // Render boolean
  if (typeof value === "boolean") {
    return (
      <Badge variant={value ? "default" : "secondary"} className="font-mono text-xs">
        {value.toString()}
      </Badge>
    );
  }

  // Render number
  if (typeof value === "number") {
    // Check if it's an embedding dimension indicator
    if (
      fieldName.toLowerCase().includes("embedding") ||
      fieldName.toLowerCase().includes("vector")
    ) {
      return (
        <span className="text-sm font-mono text-muted-foreground">{value.toLocaleString()}</span>
      );
    }
    return <span className="text-sm font-mono">{value.toLocaleString()}</span>;
  }

  // Render string
  if (typeof value === "string") {
    // Handle embeddings (don't show raw vectors)
    if (
      (fieldName.toLowerCase().includes("embedding") ||
        fieldName.toLowerCase().includes("vector")) &&
      value.startsWith("[")
    ) {
      let parsed: unknown;
      try {
        parsed = JSON.parse(value);
      } catch {
        // Not parseable, continue
      }

      if (Array.isArray(parsed) && parsed.every((v) => typeof v === "number")) {
        return (
          <Badge variant="outline" className="font-mono text-xs">
            Vector ({parsed.length}D)
          </Badge>
        );
      }
    }

    // Handle image URLs
    if (isImageUrl) {
      return (
        <div className="flex items-center gap-2">
          <a
            href={value}
            target="_blank"
            rel="noopener noreferrer"
            className="text-blue-600 dark:text-blue-400 hover:underline text-sm flex items-center gap-1"
          >
            <img
              src={value}
              alt={fieldName}
              className="w-12 h-12 object-cover rounded border"
              onError={(e) => {
                e.currentTarget.style.display = "none";
              }}
            />
            <ExternalLink className="h-3 w-3" />
          </a>
          <Button
            variant="ghost"
            size="sm"
            className="h-6 w-6 p-0"
            onClick={() => handleCopy(value)}
          >
            {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
          </Button>
        </div>
      );
    }

    // Handle regular URLs
    if (isUrl) {
      return (
        <div className="flex items-center gap-2">
          <a
            href={value}
            target="_blank"
            rel="noopener noreferrer"
            className="text-blue-600 dark:text-blue-400 hover:underline text-sm flex items-center gap-1 truncate max-w-md"
          >
            {value}
            <ExternalLink className="h-3 w-3 shrink-0" />
          </a>
          <Button
            variant="ghost"
            size="sm"
            className="h-6 w-6 p-0 shrink-0"
            onClick={() => handleCopy(value)}
          >
            {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
          </Button>
        </div>
      );
    }

    // Handle long strings
    const needsTruncation = value.length > maxLength && !compact;
    const displayValue =
      needsTruncation && !isExpanded ? `${value.substring(0, maxLength)}...` : value;

    return (
      <div className="flex items-start gap-2">
        <div className="flex-1 min-w-0">
          <p className={`text-sm whitespace-pre-wrap break-words ${compact ? "line-clamp-2" : ""}`}>
            {displayValue}
          </p>
          {needsTruncation && (
            <Button
              variant="link"
              size="sm"
              className="h-auto p-0 text-xs mt-1"
              onClick={() => setIsExpanded(!isExpanded)}
            >
              {isExpanded ? "Show less" : "Show more"}
            </Button>
          )}
        </div>
        <Button
          variant="ghost"
          size="sm"
          className="h-6 w-6 p-0 shrink-0"
          onClick={() => handleCopy(value)}
        >
          {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
        </Button>
      </div>
    );
  }

  // Render arrays
  if (Array.isArray(value)) {
    // Handle embedding vectors
    if (
      (fieldName.toLowerCase().includes("embedding") ||
        fieldName.toLowerCase().includes("vector")) &&
      value.every((v) => typeof v === "number")
    ) {
      return (
        <Badge variant="outline" className="font-mono text-xs">
          Vector ({value.length}D)
        </Badge>
      );
    }

    // Handle empty arrays
    if (value.length === 0) {
      return (
        <Badge variant="secondary" className="font-mono text-xs">
          []
        </Badge>
      );
    }

    // Handle simple arrays (strings, numbers, booleans)
    const isSimpleArray = value.every(
      (v) => typeof v === "string" || typeof v === "number" || typeof v === "boolean"
    );

    if (isSimpleArray && value.length <= 5) {
      return (
        <div className="flex flex-wrap gap-1">
          {value.map((item, idx) => (
            // biome-ignore lint/suspicious/noArrayIndexKey: array items may have duplicate values
            <Badge key={idx} variant="outline" className="text-xs">
              {String(item)}
            </Badge>
          ))}
        </div>
      );
    }

    // Handle complex arrays with collapsible
    return (
      <Collapsible open={isExpanded} onOpenChange={setIsExpanded}>
        <div className="flex items-center gap-2">
          <CollapsibleTrigger asChild>
            <Button variant="ghost" size="sm" className="h-auto p-0 hover:bg-transparent">
              {isExpanded ? (
                <ChevronDown className="h-4 w-4" />
              ) : (
                <ChevronRight className="h-4 w-4" />
              )}
              <Badge variant="secondary" className="ml-1 font-mono text-xs">
                Array ({value.length})
              </Badge>
            </Button>
          </CollapsibleTrigger>
          <Button
            variant="ghost"
            size="sm"
            className="h-6 w-6 p-0"
            onClick={() => handleCopy(JSON.stringify(value, null, 2))}
          >
            {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
          </Button>
        </div>
        <CollapsibleContent className="mt-2">
          <div className="pl-4 border-l-2 border-muted space-y-2">
            {value.map((item, idx) => (
              // biome-ignore lint/suspicious/noArrayIndexKey: array items identified by position
              <div key={idx} className="text-sm">
                <span className="text-muted-foreground font-mono mr-2">[{idx}]</span>
                <FieldValueDisplay value={item} fieldName={`${fieldName}[${idx}]`} compact={true} />
              </div>
            ))}
          </div>
        </CollapsibleContent>
      </Collapsible>
    );
  }

  // Render objects
  if (typeof value === "object" && value !== null) {
    const entries = Object.entries(value);

    // Handle empty objects
    if (entries.length === 0) {
      return (
        <Badge variant="secondary" className="font-mono text-xs">
          {"{}"}
        </Badge>
      );
    }

    // Show as JSON with syntax highlighting for complex objects
    const jsonString = JSON.stringify(value, null, 2);
    const isComplex = entries.length > 3 || jsonString.length > 200;

    if (isComplex) {
      return (
        <Collapsible open={isExpanded} onOpenChange={setIsExpanded}>
          <div className="flex items-center gap-2">
            <CollapsibleTrigger asChild>
              <Button variant="ghost" size="sm" className="h-auto p-0 hover:bg-transparent">
                {isExpanded ? (
                  <ChevronDown className="h-4 w-4" />
                ) : (
                  <ChevronRight className="h-4 w-4" />
                )}
                <Badge variant="secondary" className="ml-1 font-mono text-xs">
                  Object ({entries.length} fields)
                </Badge>
              </Button>
            </CollapsibleTrigger>
            <Button
              variant="ghost"
              size="sm"
              className="h-6 w-6 p-0"
              onClick={() => handleCopy(jsonString)}
            >
              {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
            </Button>
          </div>
          <CollapsibleContent className="mt-2">
            <SyntaxHighlighter
              language="json"
              style={theme === "dark" ? oneDark : oneLight}
              customStyle={{
                margin: 0,
                fontSize: "0.75rem",
                lineHeight: "1.25rem",
                padding: "0.5rem",
                borderRadius: "0.375rem",
                maxHeight: "16rem",
                overflowX: "auto",
              }}
            >
              {jsonString}
            </SyntaxHighlighter>
          </CollapsibleContent>
        </Collapsible>
      );
    }

    // Render simple objects inline
    return (
      <div className="pl-4 border-l-2 border-muted space-y-1">
        {entries.map(([key, val]) => (
          <div key={key} className="text-sm">
            <span className="text-muted-foreground font-mono mr-2">{key}:</span>
            <FieldValueDisplay value={val} fieldName={`${fieldName}.${key}`} compact={true} />
          </div>
        ))}
      </div>
    );
  }

  // Fallback for unknown types
  return <code className="text-xs bg-muted px-1 py-0.5 rounded">{String(value)}</code>;
};

export default FieldValueDisplay;
