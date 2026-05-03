"use client";

import {
  Button,
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
  Input,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@antfly/design-system";
import { useForm } from "react-hook-form";

interface FormValues {
  name: string;
  region: string;
}

export function FormDemo() {
  const form = useForm<FormValues>({
    defaultValues: { name: "", region: "" },
  });

  return (
    <Form {...form}>
      <form onSubmit={form.handleSubmit(() => undefined)} className="max-w-sm space-y-4">
        <FormField
          control={form.control}
          name="name"
          rules={{ required: "Cluster name is required." }}
          render={({ field }) => (
            <FormItem>
              <FormLabel>Cluster name</FormLabel>
              <FormControl>
                <Input placeholder="antfly-prod" {...field} />
              </FormControl>
              <FormDescription>A unique name within your org.</FormDescription>
              <FormMessage />
            </FormItem>
          )}
        />
        <FormField
          control={form.control}
          name="region"
          rules={{ required: "Region is required." }}
          render={({ field }) => (
            <FormItem>
              <FormLabel>Region</FormLabel>
              <Select onValueChange={field.onChange} defaultValue={field.value}>
                <FormControl>
                  <SelectTrigger>
                    <SelectValue placeholder="Pick a region" />
                  </SelectTrigger>
                </FormControl>
                <SelectContent>
                  <SelectItem value="us-east-1">us-east-1</SelectItem>
                  <SelectItem value="us-west-2">us-west-2</SelectItem>
                  <SelectItem value="eu-central-1">eu-central-1</SelectItem>
                </SelectContent>
              </Select>
              <FormMessage />
            </FormItem>
          )}
        />
        <Button type="submit">Create</Button>
      </form>
    </Form>
  );
}
