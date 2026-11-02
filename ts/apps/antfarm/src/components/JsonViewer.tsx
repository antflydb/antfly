import { HighlightedCodeBlock } from "@antfly/design-system";
import type React from "react";

interface JsonViewerProps {
  json: object;
}

const JsonViewer: React.FC<JsonViewerProps> = ({ json }) => {
  const jsonString = JSON.stringify(json, null, 2);

  return (
    <HighlightedCodeBlock
      code={jsonString}
      language="json"
      className="w-full"
      contentClassName="max-h-96"
    />
  );
};

export default JsonViewer;
