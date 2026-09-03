import { Textarea } from "@antfly/design-system";
import type React from "react";

interface AdvancedIndexEditorProps {
  source: string;
  validationError: string | null;
  onChange: (source: string) => void;
}

const AdvancedIndexEditor: React.FC<AdvancedIndexEditorProps> = ({
  source,
  validationError,
  onChange,
}) => (
  <div className="space-y-2">
    <Textarea
      aria-label="Advanced index JSON"
      className="min-h-96 resize-y font-mono text-xs"
      spellCheck={false}
      value={source}
      onChange={(event) => onChange(event.target.value)}
    />
    {validationError ? (
      <p className="text-sm text-destructive" role="alert">
        {validationError}
      </p>
    ) : (
      <p className="text-xs text-muted-foreground">
        JSON syntax and source structure are valid. The server validates all advanced options when
        you create the index.
      </p>
    )}
  </div>
);

export default AdvancedIndexEditor;
