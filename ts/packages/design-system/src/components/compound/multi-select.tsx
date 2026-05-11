"use client";

import * as React from "react";
import { ChevronDown } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { cn } from "@/lib/utils";

interface MultiSelectContextValue {
  open: boolean;
  setOpen: (open: boolean) => void;
  value: string[];
  toggle: (item: string) => void;
}

const MultiSelectContext = React.createContext<MultiSelectContextValue | null>(null);

function useMultiSelectContext() {
  const ctx = React.useContext(MultiSelectContext);
  if (!ctx) throw new Error("MultiSelect sub-components must be used within <MultiSelect>");
  return ctx;
}

function MultiSelect({
  open,
  onOpenChange,
  value,
  onValueChange,
  children,
}: {
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  value: string[];
  onValueChange: (value: string[]) => void;
  children: React.ReactNode;
}) {
  const [internalOpen, setInternalOpen] = React.useState(false);
  const isControlled = open !== undefined;
  const isOpen = isControlled ? open : internalOpen;
  const handleOpenChange = React.useCallback(
    (next: boolean) => {
      if (!isControlled) setInternalOpen(next);
      onOpenChange?.(next);
    },
    [isControlled, onOpenChange],
  );

  const toggle = React.useCallback(
    (item: string) => {
      onValueChange(
        value.includes(item) ? value.filter((v) => v !== item) : [...value, item],
      );
    },
    [value, onValueChange],
  );

  return (
    <MultiSelectContext.Provider value={{ open: isOpen, setOpen: handleOpenChange, value, toggle }}>
      <Popover open={isOpen} onOpenChange={handleOpenChange}>
        {children}
      </Popover>
    </MultiSelectContext.Provider>
  );
}

function MultiSelectTrigger({
  className,
  placeholder = "Select…",
  disabled,
  children,
  ...props
}: React.ComponentProps<typeof Button> & { placeholder?: string }) {
  const { value } = useMultiSelectContext();

  return (
    <PopoverTrigger asChild>
      <Button
        data-slot="multi-select-trigger"
        variant="outline"
        size="sm"
        role="combobox"
        disabled={disabled}
        className={cn("w-full justify-between gap-2 h-auto min-h-8 py-1.5", className)}
        {...props}
      >
        <span className="flex min-w-0 flex-wrap items-center gap-1">
          {children || (
            value.length > 0 ? (
              value.map((v) => (
                <Badge key={v} variant="secondary" className="text-xs">
                  {v}
                </Badge>
              ))
            ) : (
              <span className="text-muted-foreground">{placeholder}</span>
            )
          )}
        </span>
        <ChevronDown className="size-3.5 shrink-0 opacity-50" />
      </Button>
    </PopoverTrigger>
  );
}

function MultiSelectContent({
  searchPlaceholder = "Search…",
  emptyMessage = "No results.",
  heading,
  className,
  children,
}: {
  searchPlaceholder?: string;
  emptyMessage?: string;
  heading?: string;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <PopoverContent
      data-slot="multi-select-content"
      className={cn("w-[--radix-popover-trigger-width] p-0", className)}
    >
      <Command>
        <CommandInput placeholder={searchPlaceholder} />
        <CommandList>
          <CommandEmpty>{emptyMessage}</CommandEmpty>
          <CommandGroup
            heading={heading}
            className={heading ? "[&_[cmdk-group-heading]]:mono-label" : undefined}
          >
            {children}
          </CommandGroup>
        </CommandList>
      </Command>
    </PopoverContent>
  );
}

function MultiSelectItem({
  value,
  className,
  children,
  ...props
}: Omit<React.ComponentProps<typeof CommandItem>, "onSelect"> & {
  value: string;
}) {
  const { value: selected, toggle } = useMultiSelectContext();
  const isSelected = selected.includes(value);

  return (
    <CommandItem
      data-slot="multi-select-item"
      value={value}
      onSelect={() => toggle(value)}
      className={cn("flex items-center gap-2.5", className)}
      {...props}
    >
      <span
        aria-hidden
        className={cn(
          "size-1.5 shrink-0 rounded-full",
          isSelected ? "bg-primary" : "bg-transparent",
        )}
      />
      <span className="flex-1 truncate">{children}</span>
    </CommandItem>
  );
}

export {
  MultiSelect,
  MultiSelectContent,
  MultiSelectItem,
  MultiSelectTrigger,
};
