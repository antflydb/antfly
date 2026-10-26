import {
  Alert,
  AlertDescription,
  AlertTitle,
  Button,
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
  DashboardPage,
  DashboardPageActions,
  DashboardPageDescription,
  DashboardPageHeader,
  DashboardPageTitle,
  FileUpload,
  type FileUploadProps,
} from "@antfly/design-system";
import {
  AlertCircle,
  ChevronDown,
  CircleStop,
  LoaderCircle,
  RotateCcw,
  Sparkles,
} from "lucide-react";
import type * as React from "react";
import { cn } from "@/lib/utils";

export function AIWorkflowPage({
  className,
  ...props
}: React.ComponentProps<typeof DashboardPage>) {
  return <DashboardPage className={cn("af-ai-workflow", className)} {...props} />;
}

interface AIWorkflowHeaderProps extends Omit<React.ComponentProps<"div">, "title"> {
  title: React.ReactNode;
  description?: React.ReactNode;
  eyebrow?: React.ReactNode;
  actions?: React.ReactNode;
}

export function AIWorkflowHeader({
  title,
  description,
  eyebrow = "AI workspace",
  actions,
  className,
  ...props
}: AIWorkflowHeaderProps) {
  return (
    <DashboardPageHeader className={cn("af-ai-workflow-header", className)} {...props}>
      <div className="space-y-2">
        {eyebrow ? (
          <span className="text-xs font-medium text-muted-foreground">{eyebrow}</span>
        ) : null}
        <DashboardPageTitle>{title}</DashboardPageTitle>
        {description ? <DashboardPageDescription>{description}</DashboardPageDescription> : null}
      </div>
      {actions ? <DashboardPageActions>{actions}</DashboardPageActions> : null}
    </DashboardPageHeader>
  );
}

export function AIWorkflowContextBar({ className, ...props }: React.ComponentProps<"section">) {
  return (
    <section
      aria-label="Workflow context"
      className={cn("af-ai-context-bar", className)}
      {...props}
    />
  );
}

interface AIWorkflowPanelProps extends Omit<React.ComponentProps<"section">, "title"> {
  title?: React.ReactNode;
  description?: React.ReactNode;
  actions?: React.ReactNode;
}

export function AIWorkflowPanel({
  title,
  description,
  actions,
  children,
  className,
  ...props
}: AIWorkflowPanelProps) {
  return (
    <section className={cn("af-ai-panel", className)} {...props}>
      {title || description || actions ? (
        <div className="af-ai-panel-header">
          <div className="min-w-0 space-y-1">
            {title ? <h2 className="text-base font-medium">{title}</h2> : null}
            {description ? <p className="text-sm text-muted-foreground">{description}</p> : null}
          </div>
          {actions ? (
            <div className="flex shrink-0 flex-wrap items-center gap-2">{actions}</div>
          ) : null}
        </div>
      ) : null}
      <div className="af-ai-panel-content">{children}</div>
    </section>
  );
}

export function AIWorkflowWorkbench({ className, ...props }: React.ComponentProps<"div">) {
  return <div className={cn("af-ai-workbench", className)} {...props} />;
}

interface AIWorkflowSettingsProps extends Omit<React.ComponentProps<typeof Collapsible>, "title"> {
  title?: React.ReactNode;
  description?: React.ReactNode;
}

export function AIWorkflowSettings({
  title = "Advanced settings",
  description,
  children,
  className,
  ...props
}: AIWorkflowSettingsProps) {
  return (
    <Collapsible className={cn("af-ai-settings", className)} {...props}>
      <CollapsibleTrigger className="af-ai-settings-trigger">
        <span className="min-w-0 text-left">
          <span className="block text-xs font-medium text-muted-foreground">{title}</span>
          {description ? (
            <span className="mt-0.5 block text-xs text-muted-foreground">{description}</span>
          ) : null}
        </span>
        <ChevronDown className="size-4 shrink-0 transition-transform [[data-state=open]_&]:rotate-180" />
      </CollapsibleTrigger>
      <CollapsibleContent className="af-ai-settings-content">{children}</CollapsibleContent>
    </Collapsible>
  );
}

export function AIWorkflowMetrics({ className, ...props }: React.ComponentProps<"dl">) {
  return <dl className={cn("af-ai-metrics", className)} {...props} />;
}

export function AIWorkflowMetric({
  label,
  value,
  className,
  ...props
}: React.ComponentProps<"div"> & { label: React.ReactNode; value: React.ReactNode }) {
  return (
    <div className={cn("af-ai-metric", className)} {...props}>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

interface AIWorkflowErrorProps extends Omit<React.ComponentProps<typeof Alert>, "title"> {
  title?: React.ReactNode;
  onRetry?: () => void;
  onDismiss?: () => void;
}

export function AIWorkflowError({
  title = "Request failed",
  children,
  onRetry,
  onDismiss,
  className,
  ...props
}: AIWorkflowErrorProps) {
  return (
    <Alert variant="destructive" className={className} {...props}>
      <AlertCircle />
      <AlertTitle>{title}</AlertTitle>
      <AlertDescription>
        <div>{children}</div>
        {onRetry || onDismiss ? (
          <div className="mt-2 flex flex-wrap gap-2">
            {onRetry ? (
              <Button type="button" size="sm" variant="outline" onClick={onRetry}>
                <RotateCcw className="size-3.5" /> Retry
              </Button>
            ) : null}
            {onDismiss ? (
              <Button type="button" size="sm" variant="ghost" onClick={onDismiss}>
                Dismiss
              </Button>
            ) : null}
          </div>
        ) : null}
      </AlertDescription>
    </Alert>
  );
}

interface AIWorkflowEmptyProps extends Omit<React.ComponentProps<"div">, "title"> {
  title: React.ReactNode;
  description?: React.ReactNode;
  icon?: React.ReactNode;
  action?: React.ReactNode;
}

export function AIWorkflowEmpty({
  title,
  description,
  icon = <Sparkles className="size-5" />,
  action,
  className,
  ...props
}: AIWorkflowEmptyProps) {
  return (
    <div className={cn("af-ai-empty", className)} {...props}>
      <div className="af-ai-empty-icon" aria-hidden="true">
        {icon}
      </div>
      <div className="space-y-1">
        <h3 className="text-base font-medium">{title}</h3>
        {description ? (
          <p className="max-w-md text-sm text-muted-foreground">{description}</p>
        ) : null}
      </div>
      {action}
    </div>
  );
}

interface AIWorkflowSubmitButtonProps extends React.ComponentProps<typeof Button> {
  running?: boolean;
  runningLabel?: React.ReactNode;
  onCancel?: () => void;
}

export function AIWorkflowSubmitButton({
  running = false,
  runningLabel = "Running",
  onCancel,
  children = "Run",
  disabled,
  ...props
}: AIWorkflowSubmitButtonProps) {
  if (running && onCancel) {
    return (
      <Button type="button" variant="outline" {...props} onClick={onCancel}>
        <CircleStop className="size-4" /> Stop
      </Button>
    );
  }
  return (
    <Button disabled={disabled || running} {...props}>
      {running ? <LoaderCircle className="size-4 animate-spin" /> : null}
      {running ? runningLabel : children}
    </Button>
  );
}

export function AIWorkflowFileUpload({ containerClassName, ...props }: FileUploadProps) {
  return <FileUpload containerClassName={cn("af-ai-file-upload", containerClassName)} {...props} />;
}
