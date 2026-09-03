"use client";

import {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from "react";
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
import { LOGO_BRACKET_BR, LOGO_BRACKET_TL, LOGO_VIEWBOX } from "./pixelate/logo-mark";
import type { PixelateCycleOptions, PixelateOptions } from "./pixelate/pixelate-system";
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
  /**
   * Declarative pixelation. When it flips to `true`, Anty ripples into its
   * pixel form; back to `false` ripples to the smooth vector. Composes with
   * idle/emotions (the pixel form still floats and can play emotions). For
   * finer control (cycle, origin), use the imperative handle.
   */
  pixelated?: boolean;
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
  /** Ripple into the pixel form and hold. */
  pixelate?: (opts?: PixelateOptions) => void;
  /** Ripple back to the smooth vector. */
  depixelate?: (opts?: PixelateOptions) => void;
  /** Toggle between pixel and vector forms. */
  togglePixelate?: (opts?: PixelateOptions) => void;
  /** Start a looping pixelation/depixelation "waiting" animation. */
  startPixelateCycle?: (opts?: PixelateCycleOptions) => void;
  /** Stop the cycle and settle back to the vector. */
  stopPixelateCycle?: () => void;
  /** Whether Anty is currently in (or transitioning to) pixel form. */
  isPixelated?: () => boolean;
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

  vectorLayer: {
    position: "absolute" as const,
    inset: 0,
  },

  pixelCanvas: {
    position: "absolute" as const,
    inset: 0,
    width: "100%",
    height: "100%",
    display: "none",
    imageRendering: "pixelated" as const,
    pointerEvents: "none" as const,
  },

  // Both brackets fill the whole (square) character box and render their slice
  // of the canonical af-logo viewBox at 1:1 — so the live mark is the logo, and
  // the pixelate texture (also the logo) cross-fades without shifting.
  rightBody: {
    position: "absolute" as const,
    inset: 0,
  },

  leftBody: {
    position: "absolute" as const,
    inset: 0,
  },

  bodyImage: {
    display: "block",
    maxWidth: "none",
    width: "100%",
    height: "100%",
    overflow: "visible",
  },

  // Eye-container centers match the canonical af-logo eye positions (37.375% /
  // 60.375% horizontal, 49.375% vertical) so the vector eyes sit exactly where
  // the pixelate texture (the logo) draws them.
  leftEyeContainer: {
    position: "absolute" as const,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    top: "35.32%",
    right: "56.38%",
    bottom: "36.56%",
    left: "31.13%",
  },

  rightEyeContainer: {
    position: "absolute" as const,
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    top: "35.32%",
    right: "33.38%",
    bottom: "36.56%",
    left: "54.13%",
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
    pixelated = false,
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
  const vectorLayerRef = useRef<HTMLDivElement>(null);
  const pixelCanvasRef = useRef<HTMLCanvasElement>(null);
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

  // Re-capture refs once the DOM is mounted so the animation controller
  // receives real elements instead of the initial nulls.
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
      vectorLayer: vectorLayerRef.current,
      pixelCanvas: pixelCanvasRef.current,
      innerGlow: innerGlowRef.current,
      outerGlow: outerGlowRef.current,
    }),
    [refsReady]
  );

  const handleControllerStateChange = useCallback(
    (from: AnimationState, to: AnimationState) => {
      onAnimationSequenceChange?.(`CONTROLLER: ${from} → ${to}`);
    },
    [onAnimationSequenceChange]
  );

  const animationCallbacks = useMemo(
    () => ({
      onEmotionMotionComplete: (emotion: string) => {
        onEmotionComplete?.(emotion);
      },
    }),
    [onEmotionComplete]
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
    reducedMotion,
    onStateChange: handleControllerStateChange,
    onAnimationSequenceChange,
    callbacks: animationCallbacks,
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
      pixelate: (opts) => animationController.pixelate(opts),
      depixelate: (opts) => animationController.depixelate(opts),
      togglePixelate: (opts) => animationController.togglePixelate(opts),
      startPixelateCycle: (opts) => animationController.startPixelateCycle(opts),
      stopPixelateCycle: () => animationController.stopPixelateCycle(),
      isPixelated: () => animationController.isPixelated(),
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

  // Declarative pixelation: ripple in/out when the `pixelated` prop flips.
  // No isReady gate: isReady is a ref snapshotted into the memoized controller
  // object, so it reads as a stale `false` on mount and never re-fires the
  // effect (refs don't re-render). Callback refs create the pixelate system
  // before effects run, and pixelate/depixelate are idempotent no-ops until it
  // exists, so calling directly is safe — and makes `pixelated` work at mount.
  useEffect(() => {
    if (pixelated) animationController.pixelate();
    else animationController.depixelate();
  }, [pixelated, animationController.pixelate, animationController.depixelate]);

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
          <div ref={vectorLayerRef} style={styles.vectorLayer}>
            <div ref={rightBodyRef} style={styles.rightBody}>
              <svg
                aria-hidden="true"
                viewBox={`0 0 ${LOGO_VIEWBOX} ${LOGO_VIEWBOX}`}
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                style={styles.bodyImage}
              >
                <path d={LOGO_BRACKET_BR} fill="currentColor" />
              </svg>
            </div>
            <div ref={leftBodyRef} style={styles.leftBody}>
              <svg
                aria-hidden="true"
                viewBox={`0 0 ${LOGO_VIEWBOX} ${LOGO_VIEWBOX}`}
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                style={styles.bodyImage}
              >
                <path d={LOGO_BRACKET_TL} fill="currentColor" />
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
          <canvas ref={pixelCanvasRef} style={styles.pixelCanvas} />
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
