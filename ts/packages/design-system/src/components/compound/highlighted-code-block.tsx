"use client";

import type * as React from "react";
import {
  CodeBlock,
  CodeBlockActions,
  CodeBlockContent,
  CodeBlockCopyButton,
  CodeBlockHeader,
  CodeBlockLine,
  CodeBlockTitle,
} from "@/components/ui/code-block";
import { useHighlightedCode } from "@/hooks/use-highlighted-code";

interface HighlightedCodeBlockProps
  extends Omit<React.ComponentProps<typeof CodeBlock>, "children" | "title"> {
  /** The code to show. Streaming text is fine: only new lines are re-tokenized. */
  code: string;
  /** A shiki language such as "json", "sql", "bash", or "tsx". Unknown languages render plain. */
  language?: string;
  /** Header text, usually a file name or "Response". Omit for no header. */
  title?: React.ReactNode;
  /** Shows a copy button. On by default. */
  copyable?: boolean;
  lineNumbers?: boolean;
  wrap?: boolean;
  /** Extra actions shown next to the copy button. */
  actions?: React.ReactNode;
  /** Classes for the scrolling content area, such as a max height. */
  contentClassName?: string;
}

/**
 * CodeBlock with highlighting, an optional title, and a copy button wired
 * up. Compose the CodeBlock parts directly for diffs or custom lines.
 */
function HighlightedCodeBlock({
  code,
  language = "text",
  title,
  copyable = true,
  lineNumbers = false,
  wrap = false,
  actions,
  contentClassName,
  ...props
}: HighlightedCodeBlockProps) {
  const lines = useHighlightedCode(code, language);
  const hasActions = copyable || actions;
  const actionNodes = hasActions ? (
    <CodeBlockActions>
      {actions}
      {copyable ? <CodeBlockCopyButton value={code} /> : null}
    </CodeBlockActions>
  ) : null;

  return (
    <CodeBlock {...props}>
      {title ? (
        <CodeBlockHeader>
          <CodeBlockTitle>{title}</CodeBlockTitle>
          {actionNodes}
        </CodeBlockHeader>
      ) : (
        actionNodes
      )}
      <CodeBlockContent lineNumbers={lineNumbers} wrap={wrap} className={contentClassName}>
        {lines.map((line, index) => (
          // biome-ignore lint/suspicious/noArrayIndexKey: lines are positional
          <CodeBlockLine key={index}>{line}</CodeBlockLine>
        ))}
      </CodeBlockContent>
    </CodeBlock>
  );
}

export { HighlightedCodeBlock, type HighlightedCodeBlockProps };
