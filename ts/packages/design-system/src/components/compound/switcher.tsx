"use client";

import * as React from "react";
import { ChevronDown } from "lucide-react";
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
import { Separator } from "@/components/ui/separator";
import { cn } from "@/lib/utils";

interface SwitcherContextValue {
  open: boolean;
  setOpen: (open: boolean) => void;
}

const SwitcherContext = React.createContext<SwitcherContextValue | null>(null);

function useSwitcherContext() {
  const ctx = React.useContext(SwitcherContext);
  if (!ctx) throw new Error("Switcher sub-components must be used within <Switcher>");
  return ctx;
}

function Switcher({
  open,
  onOpenChange,
  children,
}: {
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
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

  return (
    <SwitcherContext.Provider value={{ open: isOpen, setOpen: handleOpenChange }}>
      <Popover open={isOpen} onOpenChange={handleOpenChange}>
        {children}
      </Popover>
    </SwitcherContext.Provider>
  );
}

function SwitcherTrigger({
  className,
  placeholder = "Select…",
  disabled,
  children,
  ...props
}: React.ComponentProps<typeof Button> & { placeholder?: string }) {
  return (
    <PopoverTrigger asChild>
      <Button
        data-slot="switcher-trigger"
        variant="outline"
        size="sm"
        role="combobox"
        disabled={disabled}
        className={cn("w-full justify-between gap-2", className)}
        {...props}
      >
        <span className="flex min-w-0 items-center gap-2 [&>span]:truncate">
          {children || <span>{placeholder}</span>}
        </span>
        <ChevronDown className="size-3.5 shrink-0 opacity-50" />
      </Button>
    </PopoverTrigger>
  );
}

function SwitcherContent({
  searchPlaceholder = "Search…",
  emptyMessage = "No results.",
  heading,
  footer,
  className,
  children,
}: {
  searchPlaceholder?: string;
  emptyMessage?: string;
  heading?: string;
  footer?: React.ReactNode;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <PopoverContent
      data-slot="switcher-content"
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
        {footer}
      </Command>
    </PopoverContent>
  );
}

function SwitcherItem({
  value,
  selected = false,
  icon,
  onSelect,
  className,
  children,
  ...props
}: Omit<React.ComponentProps<typeof CommandItem>, "onSelect"> & {
  selected?: boolean;
  icon?: React.ReactNode;
  onSelect?: (value: string) => void;
}) {
  const { setOpen } = useSwitcherContext();

  return (
    <CommandItem
      data-slot="switcher-item"
      value={value}
      onSelect={(val) => {
        onSelect?.(val);
        setOpen(false);
      }}
      className={cn("flex items-center gap-2.5", className)}
      {...props}
    >
      <span
        aria-hidden
        className={cn(
          "size-1.5 shrink-0",
          selected ? "bg-primary" : "bg-transparent",
        )}
      />
      {icon && (
        <span className="shrink-0 text-muted-foreground [&>svg]:size-4">
          {icon}
        </span>
      )}
      <span className="flex-1 truncate">{children}</span>
    </CommandItem>
  );
}

function SwitcherFooter({
  className,
  children,
  ...props
}: React.ComponentProps<"div">) {
  const { setOpen } = useSwitcherContext();

  return (
    <>
      <Separator />
      <div
        data-slot="switcher-footer"
        role="button"
        tabIndex={0}
        className={cn(
          "flex items-center gap-2 px-3 py-2 text-sm text-muted-foreground cursor-pointer hover:bg-accent rounded-b-md",
          className,
        )}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            setOpen(false);
            props.onClick?.(e as unknown as React.MouseEvent<HTMLDivElement>);
          }
        }}
        {...props}
        onClick={(e) => {
          setOpen(false);
          props.onClick?.(e);
        }}
      >
        {children}
      </div>
    </>
  );
}

export {
  Switcher,
  SwitcherContent,
  SwitcherFooter,
  SwitcherItem,
  SwitcherTrigger,
};
