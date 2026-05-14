"use client";

import {
  Button,
  Checkbox,
  Form,
  FormActions,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
  FormRow,
  FormSection,
  Input,
  RadioGroup,
  RadioGroupItem,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
  Switch,
  Textarea,
} from "@antfly/design-system";
import { useForm } from "react-hook-form";

interface FormValues {
  firstName: string;
  lastName: string;
  email: string;
  bio: string;
  region: string;
  role: string;
  marketingEmails: boolean;
  darkMode: boolean;
}

export function FormDemo() {
  const form = useForm<FormValues>({
    defaultValues: {
      firstName: "",
      lastName: "",
      email: "",
      bio: "",
      region: "",
      role: "viewer",
      marketingEmails: false,
      darkMode: false,
    },
  });

  return (
    <Form form={form} onSubmit={form.handleSubmit(() => undefined)} className="max-w-lg">
      <FormSection title="Personal Information" description="Your basic profile details.">
        <FormRow>
          <FormField
            control={form.control}
            name="firstName"
            rules={{ required: "First name is required." }}
            render={({ field }) => (
              <FormItem>
                <FormLabel>First name</FormLabel>
                <FormControl>
                  <Input placeholder="Drew" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />
          <FormField
            control={form.control}
            name="lastName"
            rules={{ required: "Last name is required." }}
            render={({ field }) => (
              <FormItem>
                <FormLabel>Last name</FormLabel>
                <FormControl>
                  <Input placeholder="Lanenga" {...field} />
                </FormControl>
                <FormMessage />
              </FormItem>
            )}
          />
        </FormRow>
        <FormField
          control={form.control}
          name="email"
          rules={{ required: "Email is required." }}
          render={({ field }) => (
            <FormItem>
              <FormLabel>Email</FormLabel>
              <FormControl>
                <Input type="email" placeholder="drew@example.com" {...field} />
              </FormControl>
              <FormDescription>We'll never share your email.</FormDescription>
              <FormMessage />
            </FormItem>
          )}
        />
        <FormField
          control={form.control}
          name="bio"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Bio</FormLabel>
              <FormControl>
                <Textarea placeholder="Tell us about yourself…" {...field} />
              </FormControl>
              <FormDescription>Brief description for your profile.</FormDescription>
              <FormMessage />
            </FormItem>
          )}
        />
      </FormSection>

      <FormSection title="Preferences">
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
        <FormField
          control={form.control}
          name="role"
          render={({ field }) => (
            <FormItem>
              <FormLabel>Role</FormLabel>
              <FormControl>
                <RadioGroup onValueChange={field.onChange} defaultValue={field.value}>
                  <FormItem horizontal>
                    <FormControl>
                      <RadioGroupItem value="admin" />
                    </FormControl>
                    <FormLabel className="font-normal">Admin</FormLabel>
                  </FormItem>
                  <FormItem horizontal>
                    <FormControl>
                      <RadioGroupItem value="editor" />
                    </FormControl>
                    <FormLabel className="font-normal">Editor</FormLabel>
                  </FormItem>
                  <FormItem horizontal>
                    <FormControl>
                      <RadioGroupItem value="viewer" />
                    </FormControl>
                    <FormLabel className="font-normal">Viewer</FormLabel>
                  </FormItem>
                </RadioGroup>
              </FormControl>
              <FormMessage />
            </FormItem>
          )}
        />
      </FormSection>

      <FormSection title="Notifications" description="Control how you receive updates.">
        <FormField
          control={form.control}
          name="marketingEmails"
          render={({ field }) => (
            <FormItem horizontal>
              <FormControl>
                <Checkbox checked={field.value} onCheckedChange={field.onChange} />
              </FormControl>
              <div className="grid gap-1">
                <FormLabel>Marketing emails</FormLabel>
                <FormDescription>Receive product updates and announcements.</FormDescription>
              </div>
            </FormItem>
          )}
        />
        <FormField
          control={form.control}
          name="darkMode"
          render={({ field }) => (
            <FormItem horizontal>
              <FormControl>
                <Switch checked={field.value} onCheckedChange={field.onChange} />
              </FormControl>
              <div className="grid gap-1">
                <FormLabel>Dark mode</FormLabel>
                <FormDescription>Toggle dark color theme.</FormDescription>
              </div>
            </FormItem>
          )}
        />
      </FormSection>

      <FormActions>
        <Button variant="outline" type="button">
          Cancel
        </Button>
        <Button type="submit">Save changes</Button>
      </FormActions>
    </Form>
  );
}
