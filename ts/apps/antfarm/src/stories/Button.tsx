export interface ButtonProps {
  /** Is this the principal call to action on the page? */
  primary?: boolean;
  /** What background color to use */
  backgroundColor?: string;
  /** How large should the button be? */
  size?: "small" | "medium" | "large";
  /** Button contents */
  label: string;
  /** Optional click handler */
  onClick?: () => void;
}

/** Primary UI component for user interaction */
export const Button = ({
  primary = false,
  size = "medium",
  backgroundColor,
  label,
  ...props
}: ButtonProps) => {
  const sizeClasses = {
    small: "px-4 py-2.5 text-xs",
    medium: "px-5 py-3 text-sm",
    large: "px-6 py-3 text-base",
  };

  const baseClasses =
    "inline-block cursor-pointer border-0 rounded-3xl font-bold leading-none font-mono";
  const primaryClasses = primary
    ? "bg-primary text-primary-foreground"
    : "shadow-[rgba(0,0,0,0.15)_0px_0px_0px_1px_inset] bg-transparent text-foreground";

  return (
    <button
      type="button"
      className={[baseClasses, sizeClasses[size], primaryClasses].join(" ")}
      style={backgroundColor ? { backgroundColor } : {}}
      {...props}
    >
      {label}
    </button>
  );
};
