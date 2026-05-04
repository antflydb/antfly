"use client";

import { forwardRef, useEffect, useImperativeHandle, useMemo, useRef, useState } from "react";
import {
  createLookAnimation,
  createReturnFromLookAnimation,
} from "./animation/definitions/eye-animations";
import { getEyeDimensions, getEyeShape } from "./animation/definitions/eye-shapes";
import { ENABLE_ANIMATION_DEBUG_LOGS, logAnimationEvent } from "./animation/feature-flags";
import {
  AnimationState,
  type EmotionType,
  type ExpressionName,
  type EyeStyle,
} from "./animation/types";
import { useAnimationController } from "./use-animation-controller";

export type AntyPreset = "default" | "hero" | "assistant" | "icon" | "logo";

/**
 * Discrete size variants matching `<Logo>` — 24/32/48/64px.
 * `<Anty>` also accepts a raw pixel number for full control.
 */
export type AntySize = "sm" | "md" | "lg" | "xl" | number;

const SIZE_MAP: Record<"sm" | "md" | "lg" | "xl", number> = {
  sm: 24,
  md: 32,
  lg: 48,
  xl: 64,
};

function resolveSize(size: AntySize | undefined, fallback: number): number {
  if (size === undefined) return fallback;
  return typeof size === "number" ? size : SIZE_MAP[size];
}

export const PRESETS: Record<AntyPreset, Partial<AntyProps>> = {
  default: {
    size: 160,
    showShadow: true,
    showGlow: true,
  },
  hero: {
    size: 240,
    showShadow: true,
    showGlow: true,
  },
  assistant: {
    size: 80,
    showShadow: true,
    showGlow: false,
  },
  icon: {
    size: 32,
    showShadow: false,
    showGlow: false,
    frozen: false,
  },
  logo: {
    logoMode: true,
    showShadow: false,
    showGlow: false,
  },
};

export interface AntyProps {
  /** Preset configuration for common use cases. Explicit props override preset values. */
  preset?: AntyPreset;
  /** Current expression/emotion to display */
  expression?: ExpressionName;
  /**
   * Character size. Accepts the same discrete variants as `<Logo>`
   * (`sm` 24px, `md` 32px, `lg` 48px, `xl` 64px) or a raw pixel number for
   * hero/custom contexts. Default: 160px.
   */
  size?: AntySize;
  /**
   * Accessible label. When provided, the character is rendered as
   * `role="img"` with the given label. When omitted, it's marked aria-hidden
   * (decorative).
   */
  alt?: string;
  /** Freeze all animations (idle, breathing, etc.) for static display */
  frozen?: boolean;
  /** Logo mode: OFF eyes at full color, no shadow/glow, no blinks. */
  logoMode?: boolean;
  /** Whether to show shadow (default: true) */
  showShadow?: boolean;
  /** Whether to show glow effects (default: true) */
  showGlow?: boolean;
  /** Whether the character floats up/down while idle (default: true) */
  float?: boolean;
  /** Whether the character blinks spontaneously while idle (default: true) */
  blink?: boolean;
  /**
   * Scale multiplier applied to the character while active (default: 1).
   * Set equal to `offScale` to keep Anty the same size whether on or off.
   */
  activeScale?: number;
  /**
   * Scale multiplier applied to the character while off (default: 0.65).
   * Set equal to `activeScale` to suppress the power-off shrink/snap.
   */
  offScale?: number;
  /**
   * Which eye shape to use as the resting "on" state.
   * - `'alive'` (default): tall pill/oval shape with blink animations.
   * - `'original'`: triangle eyes (logo arrows), no blinking, static during idle.
   */
  eyeStyle?: EyeStyle;
  /** Float amplitude in pixels (default: 12). Controls how far Anty bobs vertically. */
  floatAmplitude?: number;
  /** Float cycle duration in seconds (default: 2.5). Lower = faster bob. */
  floatDuration?: number;
  /** Float easing curve (default: 'sine.inOut'). Any GSAP ease string. */
  floatEase?: string;
  /** Callback when an emotion animation completes */
  onEmotionComplete?: (emotion: string) => void;
  /** Callback when animation sequence changes (for debugging) */
  onAnimationSequenceChange?: (sequence: string) => void;
  /** Additional CSS class name */
  className?: string;
  /** Additional inline styles */
  style?: React.CSSProperties;
}

export interface AntyHandle {
  /** Play an emotion animation */
  playEmotion?: (emotion: ExpressionName) => boolean;
  /** Kill all running animations */
  killAll?: () => void;
  /** Pause idle animation */
  pauseIdle?: () => void;
  /** Resume idle animation */
  resumeIdle?: () => void;
  /** Begin a hold-style look (eyes-only) */
  startLook?: (direction: "left" | "right") => void;
  /** End a hold-style look */
  endLook?: () => void;
  /** Transition to OFF state */
  powerOff?: () => void;
  /** Transition to IDLE state */
  wakeUp?: () => void;
}

// ============================================================================
// Inline Style Helpers
// ============================================================================

const styles = {
  container: (size: number): React.CSSProperties => ({
    position: "relative",
    width: size,
    height: size,
    overflow: "visible",
  }),

  fullContainer: (size: number): React.CSSProperties => ({
    position: "relative",
    width: size,
    height: size * 1.5,
    overflow: "visible",
  }),

  characterArea: (size: number): React.CSSProperties => ({
    position: "absolute",
    top: 0,
    left: 0,
    width: size,
    height: size,
    overflow: "visible",
  }),

  character: {
    position: "relative" as const,
    width: "100%",
    height: "100%",
    willChange: "transform",
    overflow: "visible" as const,
  },

  rightBody: {
    position: "absolute" as const,
    top: "13.46%",
    right: "0",
    bottom: "0",
    left: "13.46%",
  },

  leftBody: {
    position: "absolute" as const,
    top: "0",
    right: "13.15%",
    bottom: "13.15%",
    left: "0",
  },

  bodyImage: {
    display: "block",
    maxWidth: "none",
    width: "100%",
    height: "100%",
    overflow: "visible",
  },

  leftEyeContainer: {
    position: "absolute" as const,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    top: "33.44%",
    right: "56.93%",
    bottom: "38.44%",
    left: "30.57%",
  },

  rightEyeContainer: {
    position: "absolute" as const,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    top: "33.44%",
    right: "31.21%",
    bottom: "38.44%",
    left: "56.29%",
  },

  eyeWrapper: (width: number, height: number, scale: number = 1): React.CSSProperties => ({
    flexShrink: 0,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    position: "relative",
    width: `${width * scale}px`,
    height: `${height * scale}px`,
  }),

  innerGlow: (scale: number = 1): React.CSSProperties => {
    const width = 120 * scale;
    const height = 90 * scale;
    const centerY = 95 * scale;
    return {
      position: "absolute" as const,
      left: `calc(50% - ${width / 2}px)`,
      top: `${centerY - height / 2}px`,
      width: `${width}px`,
      height: `${height}px`,
      borderRadius: "50%",
      opacity: 1,
      background: "var(--anty-glow-inner, linear-gradient(90deg, #C5D4FF 0%, #E0C5FF 100%))",
      filter: `blur(${25 * scale}px)`,
      transformOrigin: "center center",
      pointerEvents: "none" as const,
    };
  },

  outerGlow: (scale: number = 1): React.CSSProperties => {
    const width = 170 * scale;
    const height = 130 * scale;
    const centerY = 95 * scale;
    return {
      position: "absolute" as const,
      left: `calc(50% - ${width / 2}px)`,
      top: `${centerY - height / 2}px`,
      width: `${width}px`,
      height: `${height}px`,
      borderRadius: "50%",
      opacity: 1,
      background: "var(--anty-glow-outer, linear-gradient(90deg, #D5E2FF 0%, #EED5FF 100%))",
      filter: `blur(${32 * scale}px)`,
      transformOrigin: "center center",
      pointerEvents: "none" as const,
    };
  },

  shadow: (scale: number = 1): React.CSSProperties => ({
    position: "absolute" as const,
    left: "50%",
    transform: "translateX(-50%) scaleX(1) scaleY(1)",
    bottom: "0px",
    width: `${160 * scale}px`,
    height: `${40 * scale}px`,
    background: "radial-gradient(ellipse, rgba(0, 0, 0, 0.5) 0%, rgba(0, 0, 0, 0) 70%)",
    filter: `blur(${12 * scale}px)`,
    borderRadius: "50%",
    opacity: 0.7,
    transformOrigin: "center center",
    pointerEvents: "none" as const,
  }),
};

// ============================================================================
// Component
// ============================================================================

export const Anty = forwardRef<AntyHandle, AntyProps>((props, ref) => {
  const presetDefaults = props.preset ? PRESETS[props.preset] : {};

  const {
    preset: _preset,
    expression = "idle",
    size: sizeProp,
    alt,
    frozen = presetDefaults.frozen ?? false,
    logoMode = presetDefaults.logoMode ?? false,
    showShadow = presetDefaults.showShadow ?? true,
    showGlow = presetDefaults.showGlow ?? true,
    float = presetDefaults.float ?? true,
    blink = presetDefaults.blink ?? true,
    activeScale = presetDefaults.activeScale ?? 1,
    offScale = presetDefaults.offScale ?? 0.65,
    eyeStyle = presetDefaults.eyeStyle ?? "alive",
    floatAmplitude,
    floatDuration,
    floatEase,
    onEmotionComplete,
    onAnimationSequenceChange,
    className = "",
    style,
  } = props;

  const presetSize = resolveSize(presetDefaults.size, 160);
  const size = resolveSize(sizeProp, presetSize);

  // Refs for DOM elements
  const containerRef = useRef<HTMLDivElement>(null);
  const characterRef = useRef<HTMLDivElement>(null);
  const leftEyeRef = useRef<HTMLDivElement>(null);
  const rightEyeRef = useRef<HTMLDivElement>(null);
  const leftEyePathRef = useRef<SVGPathElement>(null);
  const rightEyePathRef = useRef<SVGPathElement>(null);
  const leftEyeSvgRef = useRef<SVGSVGElement>(null);
  const rightEyeSvgRef = useRef<SVGSVGElement>(null);
  const leftBodyRef = useRef<HTMLDivElement>(null);
  const rightBodyRef = useRef<HTMLDivElement>(null);
  const shadowRef = useRef<HTMLDivElement>(null);
  const innerGlowRef = useRef<HTMLDivElement>(null);
  const outerGlowRef = useRef<HTMLDivElement>(null);

  // State
  const [isOffInternal, setIsOffInternal] = useState(false);
  const isOff = expression === "off" || isOffInternal;
  const useOriginalEyes = eyeStyle === "original";
  const initialEyeDimensions = useOriginalEyes
    ? getEyeDimensions("OFF_LEFT")
    : getEyeDimensions("IDLE");
  const sizeScale = size / 160;

  const [refsReady, setRefsReady] = useState(false);
  useEffect(() => {
    if (containerRef.current && characterRef.current && !refsReady) {
      setRefsReady(true);
    }
  }, [refsReady]);

  // Respect prefers-reduced-motion: pause float + blink unless the user has
  // opted out via explicit `float`/`blink={true}` overrides at the prop level.
  const [reducedMotion, setReducedMotion] = useState(false);
  useEffect(() => {
    if (typeof window === "undefined" || !window.matchMedia) return;
    const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
    setReducedMotion(mq.matches);
    const onChange = (e: MediaQueryListEvent) => setReducedMotion(e.matches);
    mq.addEventListener("change", onChange);
    return () => mq.removeEventListener("change", onChange);
  }, []);

  const effectiveFloat = float && !reducedMotion;
  const effectiveBlink = blink && !reducedMotion && !useOriginalEyes;

  // Memoize so the idle-animation effect doesn't restart on every parent render.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  // biome-ignore lint/correctness/useExhaustiveDependencies: refsReady triggers recapture of DOM refs after mount
  const elements = useMemo(
    () => ({
      container: containerRef.current,
      character: characterRef.current,
      shadow: shadowRef.current,
      eyeLeft: leftEyeRef.current,
      eyeRight: rightEyeRef.current,
      eyeLeftPath: leftEyePathRef.current,
      eyeRightPath: rightEyePathRef.current,
      eyeLeftSvg: leftEyeSvgRef.current,
      eyeRightSvg: rightEyeSvgRef.current,
      leftBody: leftBodyRef.current,
      rightBody: rightBodyRef.current,
      innerGlow: innerGlowRef.current,
      outerGlow: outerGlowRef.current,
    }),
    [refsReady]
  );

  // Animation controller
  const animationController = useAnimationController(elements, {
    enableLogging: ENABLE_ANIMATION_DEBUG_LOGS,
    enableQueue: true,
    maxQueueSize: 10,
    defaultPriority: 2,
    isOff,
    logoMode,
    autoStartIdle: !frozen && !logoMode,
    sizeScale,
    enableFloat: effectiveFloat,
    enableBlinks: effectiveBlink,
    activeScale,
    offScale,
    eyeStyle,
    floatAmplitude,
    floatDuration,
    floatEase,
    onStateChange: (from, to) => {
      if (onAnimationSequenceChange) {
        onAnimationSequenceChange(`CONTROLLER: ${from} → ${to}`);
      }
    },
    onAnimationSequenceChange,
    callbacks: {
      onEmotionMotionComplete: (emotion) => {
        onEmotionComplete?.(emotion);
      },
    },
  });

  // Expose imperative API
  useImperativeHandle(
    ref,
    () => ({
      playEmotion: (emotion: ExpressionName) => {
        if (ENABLE_ANIMATION_DEBUG_LOGS) {
          logAnimationEvent("playEmotion called via handle", { emotion });
        }

        const validEmotions: Record<string, EmotionType> = {
          excited: "excited",
          shocked: "shocked",
          wink: "wink",
          nod: "nod",
          headshake: "headshake",
          "back-forth": "back-forth",
          "look-around": "look-around",
          "look-left": "look-left",
          "look-right": "look-right",
        };

        const emotionType = validEmotions[emotion];
        if (emotionType) {
          return animationController.playEmotion(emotionType, { priority: 2 });
        }

        return false;
      },
      startLook: (direction: "left" | "right") => {
        if (
          !leftEyeRef.current ||
          !rightEyeRef.current ||
          !leftEyePathRef.current ||
          !rightEyePathRef.current ||
          !leftEyeSvgRef.current ||
          !rightEyeSvgRef.current
        ) {
          return;
        }

        animationController.pause();

        const lookTl = createLookAnimation(
          {
            leftEye: leftEyeRef.current,
            rightEye: rightEyeRef.current,
            leftEyePath: leftEyePathRef.current,
            rightEyePath: rightEyePathRef.current,
            leftEyeSvg: leftEyeSvgRef.current,
            rightEyeSvg: rightEyeSvgRef.current,
          },
          { direction }
        );
        lookTl.play();
      },
      endLook: () => {
        if (
          !leftEyeRef.current ||
          !rightEyeRef.current ||
          !leftEyePathRef.current ||
          !rightEyePathRef.current ||
          !leftEyeSvgRef.current ||
          !rightEyeSvgRef.current
        ) {
          return;
        }

        const returnTl = createReturnFromLookAnimation(
          {
            leftEye: leftEyeRef.current,
            rightEye: rightEyeRef.current,
            leftEyePath: leftEyePathRef.current,
            rightEyePath: rightEyePathRef.current,
            leftEyeSvg: leftEyeSvgRef.current,
            rightEyeSvg: rightEyeSvgRef.current,
          },
          useOriginalEyes ? { restingShape: { left: "OFF_LEFT", right: "OFF_RIGHT" } } : {}
        );
        returnTl.eventCallback("onComplete", () => {
          animationController.resume();
        });
        returnTl.play();
      },
      killAll: () => {
        animationController.killAll();
      },
      pauseIdle: () => {
        animationController.pause();
      },
      resumeIdle: () => {
        animationController.resume();
      },
      powerOff: () => {
        setIsOffInternal(true);
        animationController.transitionTo(AnimationState.OFF);
      },
      wakeUp: () => {
        setIsOffInternal(false);
        animationController.transitionTo(AnimationState.IDLE);
      },
    }),
    [animationController, useOriginalEyes]
  );

  // Play emotion when expression changes
  useEffect(() => {
    if (!animationController.isReady) return;
    if (isOff) return;

    const validEmotions: Record<string, EmotionType> = {
      excited: "excited",
      shocked: "shocked",
      wink: "wink",
      nod: "nod",
      headshake: "headshake",
      "back-forth": "back-forth",
      "look-around": "look-around",
      "look-left": "look-left",
      "look-right": "look-right",
    };

    const emotionType = validEmotions[expression];
    if (emotionType) {
      animationController.playEmotion(emotionType, { priority: 2 });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [expression, isOff, animationController.playEmotion, animationController.isReady]);

  // Shadow/glow elements are always rendered so the tracker/glow system can
  // attach once. When neither is visible, the outer container collapses to a
  // square so the footprint matches `<Logo>` of the same size — swappable in
  // nav/footer chrome without layout shift.
  const needsFootprintSlot = showShadow || showGlow;
  const containerStyle = needsFootprintSlot ? styles.fullContainer(size) : styles.container(size);

  const ariaProps: React.HTMLAttributes<HTMLDivElement> = alt
    ? { role: "img", "aria-label": alt }
    : { "aria-hidden": true };

  return (
    <div
      ref={containerRef}
      {...ariaProps}
      style={{
        color: "var(--foreground, #052333)",
        ...containerStyle,
        touchAction: "manipulation",
        ...style,
      }}
      className={className}
    >
      <div
        style={
          needsFootprintSlot
            ? styles.characterArea(size)
            : { position: "relative", width: size, height: size, overflow: "visible" }
        }
      >
        <div
          ref={outerGlowRef}
          style={{
            ...styles.outerGlow(sizeScale),
            visibility: showGlow ? "visible" : "hidden",
          }}
        />
        <div
          ref={innerGlowRef}
          style={{
            ...styles.innerGlow(sizeScale),
            visibility: showGlow ? "visible" : "hidden",
          }}
        />

        <div ref={characterRef} style={styles.character}>
          <div ref={rightBodyRef} style={styles.rightBody}>
            <svg
              aria-hidden="true"
              preserveAspectRatio="none"
              viewBox="0 0 173.082 173.082"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
              style={styles.bodyImage}
            >
              <path
                d="M173.082 115.977C173.082 147.515 147.515 173.082 115.976 173.082H4.18192C0.463682 173.082 -1.39842 168.586 1.23077 165.957L29.5407 137.647H115.976C127.945 137.647 137.647 127.945 137.647 115.977V29.5407L165.957 1.23077C168.586 -1.39842 173.082 0.463679 173.082 4.18192V115.977Z"
                fill="currentColor"
              />
            </svg>
          </div>
          <div ref={leftBodyRef} style={styles.leftBody}>
            <svg
              aria-hidden="true"
              preserveAspectRatio="none"
              viewBox="0 0 173.694 173.694"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
              style={styles.bodyImage}
            >
              <path
                d="M144.153 35.4344H57.1051C45.1368 35.4345 35.4344 45.1368 35.4344 57.1051V144.153L7.12469 172.463C4.4955 175.092 0 173.23 0 169.512V57.1051C2.28235e-05 25.5668 25.5668 4.78163e-05 57.1051 0H169.512C173.23 0 175.092 4.49551 172.463 7.1247L144.153 35.4344Z"
                fill="currentColor"
              />
            </svg>
          </div>

          <div style={styles.leftEyeContainer}>
            <div
              ref={leftEyeRef}
              style={styles.eyeWrapper(
                initialEyeDimensions.width,
                initialEyeDimensions.height,
                sizeScale
              )}
            >
              <svg
                aria-hidden="true"
                ref={leftEyeSvgRef}
                width="100%"
                height="100%"
                viewBox={initialEyeDimensions.viewBox}
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                style={{ display: "block" }}
              >
                <path
                  ref={leftEyePathRef}
                  d={getEyeShape(useOriginalEyes ? "OFF_LEFT" : "IDLE", "left")}
                  fill="currentColor"
                />
              </svg>
            </div>
          </div>

          <div style={styles.rightEyeContainer}>
            <div
              ref={rightEyeRef}
              style={styles.eyeWrapper(
                initialEyeDimensions.width,
                initialEyeDimensions.height,
                sizeScale
              )}
            >
              <svg
                aria-hidden="true"
                ref={rightEyeSvgRef}
                width="100%"
                height="100%"
                viewBox={initialEyeDimensions.viewBox}
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                style={{ display: "block" }}
              >
                <path
                  ref={rightEyePathRef}
                  d={getEyeShape(useOriginalEyes ? "OFF_RIGHT" : "IDLE", "right")}
                  fill="currentColor"
                />
              </svg>
            </div>
          </div>
        </div>
      </div>

      <div
        ref={shadowRef}
        style={{
          ...styles.shadow(sizeScale),
          visibility: showShadow ? "visible" : "hidden",
        }}
      />
    </div>
  );
});

Anty.displayName = "Anty";
