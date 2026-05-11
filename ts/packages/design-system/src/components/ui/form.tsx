"use client";

import type * as LabelPrimitive from "@radix-ui/react-label";
import { Slot } from "@radix-ui/react-slot";
import * as React from "react";
import {
  Controller,
  type ControllerProps,
  type FieldPath,
  type FieldValues,
  FormProvider,
  type UseFormReturn,
  useFormContext,
  useFormState,
} from "react-hook-form";

import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

function Form<T extends FieldValues>({
  form,
  className,
  children,
  ...formProps
}: Omit<React.ComponentProps<"form">, "children"> & {
  form: UseFormReturn<T>;
  children: React.ReactNode;
}) {
  return (
    <FormProvider {...form}>
      <form className={cn("flex flex-col gap-4", className)} {...formProps}>
        {children}
      </form>
    </FormProvider>
  );
}

type FormFieldContextValue<
  TFieldValues extends FieldValues = FieldValues,
  TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
> = {
  name: TName;
};

const FormFieldContext = React.createContext<FormFieldContextValue>({} as FormFieldContextValue);

const FormField = <
  TFieldValues extends FieldValues = FieldValues,
  TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
>({
  ...props
}: ControllerProps<TFieldValues, TName>) => {
  return (
    <FormFieldContext.Provider value={{ name: props.name }}>
      <Controller {...props} />
    </FormFieldContext.Provider>
  );
};

const useFormField = () => {
  const fieldContext = React.useContext(FormFieldContext);
  const itemContext = React.useContext(FormItemContext);
  const { getFieldState } = useFormContext();
  const formState = useFormState({ name: fieldContext.name });
  const fieldState = getFieldState(fieldContext.name, formState);

  if (!fieldContext) {
    throw new Error("useFormField should be used within <FormField>");
  }

  const { id } = itemContext;

  return {
    id,
    name: fieldContext.name,
    formItemId: `${id}-form-item`,
    formDescriptionId: `${id}-form-item-description`,
    formMessageId: `${id}-form-item-message`,
    ...fieldState,
  };
};

type FormItemContextValue = {
  id: string;
};

const FormItemContext = React.createContext<FormItemContextValue>({} as FormItemContextValue);

function FormItem({
  horizontal,
  className,
  ...props
}: React.ComponentProps<"div"> & { horizontal?: boolean }) {
  const id = React.useId();

  return (
    <FormItemContext.Provider value={{ id }}>
      <div
        data-slot="form-item"
        className={cn(horizontal ? "flex items-start gap-3" : "grid gap-2", className)}
        {...props}
      />
    </FormItemContext.Provider>
  );
}

function FormLabel({ className, ...props }: React.ComponentProps<typeof LabelPrimitive.Root>) {
  const { error, formItemId } = useFormField();

  return (
    <Label
      data-slot="form-label"
      data-error={!!error}
      className={cn("data-[error=true]:text-destructive", className)}
      htmlFor={formItemId}
      {...props}
    />
  );
}

function FormControl({ ...props }: React.ComponentProps<typeof Slot>) {
  const { error, formItemId, formDescriptionId, formMessageId } = useFormField();

  return (
    <Slot
      data-slot="form-control"
      id={formItemId}
      aria-describedby={!error ? `${formDescriptionId}` : `${formDescriptionId} ${formMessageId}`}
      aria-invalid={!!error}
      {...props}
    />
  );
}

function FormDescription({ className, ...props }: React.ComponentProps<"p">) {
  const { formDescriptionId } = useFormField();

  return (
    <p
      data-slot="form-description"
      id={formDescriptionId}
      className={cn("text-muted-foreground text-sm", className)}
      {...props}
    />
  );
}

function FormMessage({ className, ...props }: React.ComponentProps<"p">) {
  const { error, formMessageId } = useFormField();
  const body = error ? String(error?.message ?? "") : props.children;

  if (!body) {
    return null;
  }

  return (
    <p
      data-slot="form-message"
      id={formMessageId}
      className={cn("text-destructive text-sm", className)}
      {...props}
    >
      {body}
    </p>
  );
}

function FormSection({
  title,
  description,
  className,
  children,
}: {
  title: string;
  description?: string;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <fieldset data-slot="form-section" className={cn("flex flex-col gap-4", className)}>
      <div className="flex flex-col gap-1">
        <legend className="mono-label">{title}</legend>
        {description && <p className="text-sm text-muted-foreground">{description}</p>}
      </div>
      {children}
    </fieldset>
  );
}

function FormRow({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="form-row"
      className={cn("grid grid-cols-1 gap-4 sm:grid-cols-2", className)}
      {...props}
    />
  );
}

function FormActions({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="form-actions"
      className={cn("flex justify-end gap-2 pt-2", className)}
      {...props}
    />
  );
}

export {
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
  useFormField,
};
