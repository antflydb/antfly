import {
  Button,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  Form,
  FormActions,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  Input,
} from "@antfly/design-system";
import { Settings } from "lucide-react";
import type { ReactNode } from "react";
import { useState } from "react";
import { useForm } from "react-hook-form";
import { useApiConfig } from "@/hooks/use-api-config";

interface SettingsDialogProps {
  trigger?: ReactNode;
}

interface SettingsFormValues {
  apiUrl: string;
  inferenceUrl: string;
}

export function SettingsDialog({ trigger }: SettingsDialogProps = {}) {
  const { apiUrl, setApiUrl, resetToDefault, inferenceApiUrl, setInferenceApiUrl, resetInferenceApiUrl } =
    useApiConfig();
  const [isOpen, setIsOpen] = useState(false);

  const form = useForm<SettingsFormValues>({
    defaultValues: { apiUrl, inferenceUrl: inferenceApiUrl },
  });

  const handleSave = (data: SettingsFormValues) => {
    setApiUrl(data.apiUrl);
    setInferenceApiUrl(data.inferenceUrl);
    setIsOpen(false);
  };

  const handleReset = () => {
    resetToDefault();
    resetInferenceApiUrl();
    form.reset({ apiUrl, inferenceUrl: inferenceApiUrl });
  };

  const handleCancel = () => {
    form.reset({ apiUrl, inferenceUrl: inferenceApiUrl });
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
            Configure the Antfly and Inference servers to connect to. This is useful when accessing
            the dashboard remotely or connecting to different servers.
          </DialogDescription>
        </DialogHeader>
        <Form form={form} onSubmit={form.handleSubmit(handleSave)} className="gap-6 py-4">
          <FormField
            control={form.control}
            name="apiUrl"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Antfly API URL</FormLabel>
                <FormControl>
                  <Input placeholder="http://localhost:8082/db/v1" {...field} />
                </FormControl>
                <FormDescription>
                  Current: <code className="text-xs bg-muted px-1 py-0.5 rounded">{apiUrl}</code>
                </FormDescription>
                <FormDescription className="text-xs">
                  Examples: <code className="bg-muted px-1 py-0.5 rounded">/db/v1</code> (default),{" "}
                  <code className="bg-muted px-1 py-0.5 rounded">http://server:8082/db/v1</code>
                </FormDescription>
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="inferenceUrl"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Inference API URL</FormLabel>
                <FormControl>
                  <Input placeholder="http://localhost:11433" {...field} />
                </FormControl>
                <FormDescription>
                  Current:{" "}
                  <code className="text-xs bg-muted px-1 py-0.5 rounded">{inferenceApiUrl}</code>
                </FormDescription>
                <FormDescription className="text-xs">
                  Examples:{" "}
                  <code className="bg-muted px-1 py-0.5 rounded">http://localhost:11433</code>{" "}
                  (default),{" "}
                  <code className="bg-muted px-1 py-0.5 rounded">https://inference.company.com</code>
                </FormDescription>
              </FormItem>
            )}
          />

          <FormActions>
            <Button variant="outline" type="button" onClick={handleReset}>
              Reset to Default
            </Button>
            <Button variant="outline" type="button" onClick={handleCancel}>
              Cancel
            </Button>
            <Button type="submit">Save</Button>
          </FormActions>
        </Form>
      </DialogContent>
    </Dialog>
  );
}
