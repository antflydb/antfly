import { type ChangeEvent, useState } from "react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";

interface ImportJsonDialogProps {
  open: boolean;
  onClose: () => void;
  onImport: (json: string) => void;
}

export function ImportJsonDialog({ open, onClose, onImport }: ImportJsonDialogProps) {
  const [jsonInput, setJsonInput] = useState("");
  const [fileContent, setFileContent] = useState("");
  const [activeTab, setActiveTab] = useState("paste");

  const handleImport = () => {
    const content = activeTab === "paste" ? jsonInput : fileContent;
    onImport(content);
    onClose();
  };

  const handleFileChange = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target?.result as string;
        setFileContent(text);
      };
      reader.readAsText(file);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onClose}>
      <DialogContent className="max-w-[600px]">
        <DialogTitle>Import from JSON</DialogTitle>
        <Tabs defaultValue="paste" onValueChange={setActiveTab}>
          <TabsList>
            <TabsTrigger value="paste">Paste JSON</TabsTrigger>
            <TabsTrigger value="file">Import from File</TabsTrigger>
          </TabsList>

          <div className="px-4 pt-3 pb-2">
            <TabsContent value="paste">
              <Textarea
                value={jsonInput}
                onChange={(e) => setJsonInput(e.target.value)}
                placeholder="Paste your JSON here"
                rows={10}
                className="w-full max-h-64 overflow-auto break-all whitespace-pre-wrap"
              />
            </TabsContent>

            <TabsContent value="file">
              <div className="flex flex-col gap-3">
                <input
                  type="file"
                  accept=".json,.jsonl"
                  onChange={handleFileChange}
                  className="w-full max-w-full text-sm text-grey-500 truncate
                    file:mr-5 file:py-2 file:px-6
                    file:rounded-full file:border-0
                    file:text-sm file:font-medium
                    file:bg-blue-50 file:text-blue-700
                    hover:file:cursor-pointer hover:file:bg-amber-50
                    hover:file:text-amber-700"
                />
                <Textarea
                  value={fileContent}
                  readOnly
                  placeholder="File content will appear here"
                  rows={8}
                  className="w-full max-h-64 overflow-auto break-all whitespace-pre-wrap"
                />
              </div>
            </TabsContent>
          </div>
        </Tabs>
        <div className="flex gap-3 mt-4 justify-end">
          <DialogTrigger>
            <Button variant="destructive" color="gray" onClick={onClose}>
              Cancel
            </Button>
          </DialogTrigger>
          <Button onClick={handleImport}>Import</Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
