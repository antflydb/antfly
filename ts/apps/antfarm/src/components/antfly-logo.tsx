import { useTheme } from "@/hooks/use-theme";
import antflyIconDark from "/antfly-logo_icon-dark.png";
import antflyIconWhite from "/antfly-logo_icon-white.png";

/**
 * AntflyLogo Component
 *
 * Displays Antfly logo with proper dark/light mode support.
 */
export default function AntflyLogo({
  size = "medium",
  variant = "auto",
}: {
  size?: "small" | "medium" | "large" | number;
  variant?: "dark" | "white" | "auto";
}) {
  const { theme } = useTheme();
  const dimensions = {
    small: { width: 24, height: 24 },
    medium: { width: 32, height: 32 },
    large: { width: 48, height: 48 },
  };

  const { width, height } =
    typeof size === "number"
      ? { width: size, height: size }
      : dimensions[size] || dimensions.medium;

  // Determine which icon to use based on variant and theme
  // Light mode: use dark icon (black ant on light background)
  // Dark mode: use white icon (white ant on dark background)
  let iconSrc: string;
  if (variant === "white") {
    iconSrc = antflyIconWhite;
  } else if (variant === "dark") {
    iconSrc = antflyIconDark;
  } else {
    // Auto mode: dark icon in light mode, white icon in dark mode
    iconSrc = theme === "light" ? antflyIconDark : antflyIconWhite;
  }

  return (
    <div style={{ display: "inline-flex", alignItems: "center" }}>
      <img
        key={`${iconSrc}-${theme}`}
        src={iconSrc}
        alt="Antfly"
        width={width}
        height={height}
        style={{ display: "block" }}
      />
    </div>
  );
}
