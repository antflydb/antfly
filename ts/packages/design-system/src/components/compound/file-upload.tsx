import * as React from "react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface FileUploadProps
  extends Omit<React.InputHTMLAttributes<HTMLInputElement>, "children" | "type" | "value"> {
  label?: React.ReactNode;
  description?: React.ReactNode;
  actionLabel?: React.ReactNode;
  clearLabel?: React.ReactNode;
  file?: File | null;
  error?: React.ReactNode;
  inputRef?: React.Ref<HTMLInputElement>;
  containerClassName?: string;
  onFilesChange?: (files: FileList | null) => void;
  onClear?: () => void;
}

const formatFileSize = (bytes: number) => {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
};

function FileUpload({
  label = "Select file",
  description = "Drag a file here or choose one from your computer.",
  actionLabel = "Choose file",
  clearLabel = "Clear",
  file,
  error,
  accept,
  disabled,
  inputRef,
  containerClassName,
  className,
  onChange,
  onFilesChange,
  onClear,
  ...props
}: FileUploadProps) {
  const internalInputRef = React.useRef<HTMLInputElement>(null);
  const [dragActive, setDragActive] = React.useState(false);

  const setInputRef = React.useCallback(
    (node: HTMLInputElement | null) => {
      internalInputRef.current = node;
      if (typeof inputRef === "function") {
        inputRef(node);
      } else if (inputRef) {
        inputRef.current = node;
      }
    },
    [inputRef]
  );

  const openFileDialog = () => {
    if (!disabled) internalInputRef.current?.click();
  };

  const handleInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    onChange?.(event);
    onFilesChange?.(event.target.files);
  };

  const handleDrop = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    setDragActive(false);
    if (disabled) return;
    onFilesChange?.(event.dataTransfer.files);
  };

  const handleClear = () => {
    if (internalInputRef.current) {
      internalInputRef.current.value = "";
    }
    onFilesChange?.(null);
    onClear?.();
  };

  return (
    <div className={cn("space-y-2", containerClassName)}>
      <div
        role="button"
        tabIndex={disabled ? -1 : 0}
        className={cn(
          "flex min-h-36 flex-col items-center justify-center gap-3 border-(length:--border-width) border-dashed border-input bg-muted/20 p-6 text-center transition-colors",
          dragActive && "border-ring bg-muted/40",
          disabled && "pointer-events-none opacity-50",
          error && "border-destructive",
          className
        )}
        onDragEnter={(event) => {
          event.preventDefault();
          setDragActive(true);
        }}
        onDragOver={(event) => {
          event.preventDefault();
          setDragActive(true);
        }}
        onDragLeave={(event) => {
          event.preventDefault();
          setDragActive(false);
        }}
        onDrop={handleDrop}
        onKeyDown={(event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            openFileDialog();
          }
        }}
      >
        <input
          {...props}
          ref={setInputRef}
          type="file"
          accept={accept}
          disabled={disabled}
          className="sr-only"
          onChange={handleInputChange}
        />
        <div className="space-y-1">
          <div className="font-mono font-medium text-[13px] text-foreground">{label}</div>
          {description ? <div className="text-muted-foreground text-sm">{description}</div> : null}
          {accept ? <div className="text-muted-foreground text-xs">Accepted: {accept}</div> : null}
        </div>
        <Button type="button" variant="outline" size="sm" onClick={openFileDialog}>
          {actionLabel}
        </Button>
      </div>

      {file ? (
        <div className="flex items-center justify-between gap-3 border-(length:--border-width) border-input bg-background px-3 py-2 text-sm">
          <div className="min-w-0">
            <div className="truncate font-mono text-[13px] text-foreground">{file.name}</div>
            <div className="text-muted-foreground text-xs">{formatFileSize(file.size)}</div>
          </div>
          <Button type="button" variant="ghost" size="sm" onClick={handleClear}>
            {clearLabel}
          </Button>
        </div>
      ) : null}

      {error ? <div className="text-destructive text-sm">{error}</div> : null}
    </div>
  );
}

export type { FileUploadProps };
export { FileUpload };
