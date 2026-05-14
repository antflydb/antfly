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
  termiteUrl: string;
}

export function SettingsDialog({ trigger }: SettingsDialogProps = {}) {
  const { apiUrl, setApiUrl, resetToDefault, termiteApiUrl, setTermiteApiUrl, resetTermiteApiUrl } =
    useApiConfig();
  const [isOpen, setIsOpen] = useState(false);

  const form = useForm<SettingsFormValues>({
    defaultValues: { apiUrl, termiteUrl: termiteApiUrl },
  });

  const handleSave = (data: SettingsFormValues) => {
    setApiUrl(data.apiUrl);
    setTermiteApiUrl(data.termiteUrl);
    setIsOpen(false);
  };

  const handleReset = () => {
    resetToDefault();
    resetTermiteApiUrl();
    form.reset({ apiUrl, termiteUrl: termiteApiUrl });
  };

  const handleCancel = () => {
    form.reset({ apiUrl, termiteUrl: termiteApiUrl });
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
            Configure the Antfly and Termite servers to connect to. This is useful when accessing
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
                  <Input placeholder="http://localhost:8082/api/v1" {...field} />
                </FormControl>
                <FormDescription>
                  Current: <code className="text-xs bg-muted px-1 py-0.5 rounded">{apiUrl}</code>
                </FormDescription>
                <FormDescription className="text-xs">
                  Examples: <code className="bg-muted px-1 py-0.5 rounded">/api/v1</code> (default),{" "}
                  <code className="bg-muted px-1 py-0.5 rounded">http://server:8082/api/v1</code>
                </FormDescription>
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="termiteUrl"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Termite API URL</FormLabel>
                <FormControl>
                  <Input placeholder="http://localhost:11433" {...field} />
                </FormControl>
                <FormDescription>
                  Current:{" "}
                  <code className="text-xs bg-muted px-1 py-0.5 rounded">{termiteApiUrl}</code>
                </FormDescription>
                <FormDescription className="text-xs">
                  Examples:{" "}
                  <code className="bg-muted px-1 py-0.5 rounded">http://localhost:11433</code>{" "}
                  (default),{" "}
                  <code className="bg-muted px-1 py-0.5 rounded">https://termite.company.com</code>
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
