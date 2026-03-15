import type React from "react";
import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneDark, oneLight } from "react-syntax-highlighter/dist/esm/styles/prism";
import { useTheme } from "@/components/theme-provider";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";

interface JsonViewerProps {
  json: object;
}

const JsonViewer: React.FC<JsonViewerProps> = ({ json }) => {
  const { theme } = useTheme();
  const jsonString = JSON.stringify(json, null, 2);

  return (
    <Card className="w-full">
      <CardContent className="p-4">
        <SyntaxHighlighter
          language="json"
          style={theme === "dark" ? oneDark : oneLight}
          customStyle={{
            margin: 0,
            fontSize: "0.875rem",
            lineHeight: "1.25rem",
            maxHeight: "24rem",
            borderRadius: "0.375rem",
          }}
          wrapLongLines={false}
        >
          {jsonString}
        </SyntaxHighlighter>
        <div className="flex justify-end mt-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              navigator.clipboard.writeText(jsonString);
            }}
          >
            Copy JSON
          </Button>
        </div>
      </CardContent>
    </Card>
  );
};

export default JsonViewer;
