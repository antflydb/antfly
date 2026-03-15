import type React from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark, oneLight } from "react-syntax-highlighter/dist/esm/styles/prism";
import { useTheme } from "@/components/theme-provider";
import { cn } from "@/lib/utils";

interface QueryDiffViewProps {
  currentQuery: object;
  proposedQuery: object;
  className?: string;
}

export const QueryDiffView: React.FC<QueryDiffViewProps> = ({
  currentQuery,
  proposedQuery,
  className,
}) => {
  const { theme } = useTheme();
  const style = theme === "dark" ? oneDark : oneLight;
  const syntaxStyle = {
    margin: 0,
    fontSize: "0.75rem",
    lineHeight: "1.125rem",
    maxHeight: "16rem",
    borderRadius: "0.375rem",
  };

  return (
    <div className={cn("grid grid-cols-2 gap-3", className)}>
      <div className="space-y-1.5">
        <span className="text-xs font-medium text-muted-foreground">Current Query</span>
        <div className="rounded-md border overflow-hidden">
          <SyntaxHighlighter
            language="json"
            style={style}
            customStyle={syntaxStyle}
            wrapLongLines={false}
          >
            {JSON.stringify(currentQuery, null, 2)}
          </SyntaxHighlighter>
        </div>
      </div>
      <div className="space-y-1.5">
        <span className="text-xs font-medium text-muted-foreground">Proposed Query</span>
        <div className="rounded-md border border-green-500/30 overflow-hidden">
          <SyntaxHighlighter
            language="json"
            style={style}
            customStyle={syntaxStyle}
            wrapLongLines={false}
          >
            {JSON.stringify(proposedQuery, null, 2)}
          </SyntaxHighlighter>
        </div>
      </div>
    </div>
  );
};
