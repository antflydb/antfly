import {
  CodeBlock,
  CodeBlockActions,
  CodeBlockContent,
  CodeBlockCopyButton,
  CodeBlockLine,
  useHighlightedCode,
} from "@antfly/design-system";
import type React from "react";
import { cn } from "@/lib/utils";

interface QueryDiffViewProps {
  currentQuery: object;
  proposedQuery: object;
  className?: string;
}

interface DiffSideProps {
  code: string;
  /** Lines of the other side. A line missing there is marked as changed. */
  otherLines: Set<string>;
  changedVariant: "added" | "removed";
  className?: string;
}

const DiffSide: React.FC<DiffSideProps> = ({ code, otherLines, changedVariant, className }) => {
  const highlighted = useHighlightedCode(code, "json");
  const rawLines = code.split("\n");

  return (
    <CodeBlock size="sm" className={className}>
      <CodeBlockActions>
        <CodeBlockCopyButton value={code} />
      </CodeBlockActions>
      <CodeBlockContent className="max-h-64">
        {highlighted.map((line, index) => (
          <CodeBlockLine
            // biome-ignore lint/suspicious/noArrayIndexKey: lines are positional
            key={index}
            variant={otherLines.has(rawLines[index] ?? "") ? "default" : changedVariant}
          >
            {line}
          </CodeBlockLine>
        ))}
      </CodeBlockContent>
    </CodeBlock>
  );
};

export const QueryDiffView: React.FC<QueryDiffViewProps> = ({
  currentQuery,
  proposedQuery,
  className,
}) => {
  const currentCode = JSON.stringify(currentQuery, null, 2);
  const proposedCode = JSON.stringify(proposedQuery, null, 2);
  const currentLines = new Set(currentCode.split("\n"));
  const proposedLines = new Set(proposedCode.split("\n"));

  return (
    <div className={cn("grid grid-cols-2 gap-3", className)}>
      <div className="min-w-0 space-y-1.5">
        <span className="text-xs font-medium text-muted-foreground">Current Query</span>
        <DiffSide code={currentCode} otherLines={proposedLines} changedVariant="removed" />
      </div>
      <div className="min-w-0 space-y-1.5">
        <span className="text-xs font-medium text-muted-foreground">Proposed Query</span>
        {/* The success border sits on a wrapper: the layer it comes from loses to
            the block's own border color. */}
        <div className="af-status-border-success rounded-lg border">
          <DiffSide
            code={proposedCode}
            otherLines={currentLines}
            changedVariant="added"
            className="border-0"
          />
        </div>
      </div>
    </div>
  );
};
