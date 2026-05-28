import * as React from "react";
import { cn } from "@/lib/utils";

export interface TypeOnProps extends Omit<React.HTMLAttributes<HTMLSpanElement>, "children"> {
  /** Text to reveal one character at a time. */
  text: string;
  /** Milliseconds per character. @default 60 */
  msPerChar?: number;
  /** Delay before the reveal begins, in ms. @default 400 */
  delay?: number;
  /** Render the blinking caret. @default true */
  caret?: boolean;
}

/**
 * Mono `steps()` typewriter — used as the loud-register tagline under a hero
 * headline, splash captions, and other deliberate brand moments. NOT for
 * working-UI text, which should be static.
 *
 *   <TypeOn text="hybrid vector + BM25 across every shard_" />
 *
 * Width and animation duration scale with the text length. The caret is
 * amber and stepped (no smooth fade) to match the 8-bit emphasis register.
 */
export const TypeOn = React.forwardRef<HTMLSpanElement, TypeOnProps>(
  ({ text, msPerChar = 60, delay = 400, caret = true, className, style, ...props }, ref) => {
    const chars = Math.max(text.length, 1);
    const duration = Math.max(chars * msPerChar, 400);
    const animation = caret
      ? `type-on-reveal ${duration}ms steps(${chars}, end) ${delay}ms forwards, type-on-caret 700ms steps(1) infinite`
      : `type-on-reveal ${duration}ms steps(${chars}, end) ${delay}ms forwards`;
    const inlineStyle: React.CSSProperties = {
      width: 0,
      animation,
      ...style,
    };
    if (!caret) inlineStyle.borderRight = "none";
    return (
      <span
        ref={ref}
        className={cn("type-on", className)}
        style={{ ...inlineStyle, ["--type-on-target" as string]: `${chars}ch` }}
        {...props}
      >
        {text}
      </span>
    );
  }
);
TypeOn.displayName = "TypeOn";
