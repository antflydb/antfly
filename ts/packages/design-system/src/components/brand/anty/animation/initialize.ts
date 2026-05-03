/**
 * Character Initialization
 *
 * Single function that sets ALL animatable properties once on mount.
 * This is the SINGLE SOURCE OF TRUTH for initial animation state.
 *
 * Call this once when elements are registered to ensure clean starting state.
 */

import gsap from "gsap";
import { type EyeShapeName, getEyeDimensions, getEyeShape } from "./definitions/eye-shapes";
import type { EyeStyle } from "./types";

/**
 * Elements that can be animated on the character
 */
export interface CharacterElements {
  character: HTMLElement;
  shadow?: HTMLElement | null;
  eyeLeft?: HTMLElement | null;
  eyeRight?: HTMLElement | null;
  eyeLeftPath?: SVGPathElement | null;
  eyeRightPath?: SVGPathElement | null;
  eyeLeftSvg?: SVGSVGElement | null;
  eyeRightSvg?: SVGSVGElement | null;
  innerGlow?: HTMLElement | null;
  outerGlow?: HTMLElement | null;
  leftBody?: HTMLElement | null;
  rightBody?: HTMLElement | null;
}

/**
 * Initial state options
 */
export interface InitializeOptions {
  /** Whether character starts in OFF state */
  isOff?: boolean;
  /** Logo mode: OFF eyes at full color, no shadow/glow */
  logoMode?: boolean;
  /** Scale factor for the character (size / 160) */
  sizeScale?: number;
  /** Character scale while active (default: 1) */
  activeScale?: number;
  /** Character scale while off (default: 0.65) */
  offScale?: number;
  /** Eye style for resting state (default: 'alive') */
  eyeStyle?: EyeStyle;
}

/**
 * Initialize all animatable properties on the character
 *
 * Sets ALL properties that GSAP will animate to their base values.
 * This ensures:
 * 1. No conflicts with inline CSS styles
 * 2. Consistent starting state for all animations
 * 3. Single source of truth for initial values
 *
 * @param elements - All character elements to initialize
 * @param options - Initial state configuration
 */
export function initializeCharacter(
  elements: CharacterElements,
  options: InitializeOptions = {}
): void {
  const {
    isOff = false,
    logoMode = false,
    sizeScale = 1,
    activeScale = 1,
    offScale = 0.65,
    eyeStyle = "alive",
  } = options;
  const {
    character,
    shadow,
    eyeLeft,
    eyeRight,
    eyeLeftPath,
    eyeRightPath,
    eyeLeftSvg,
    eyeRightSvg,
    innerGlow,
    outerGlow,
    leftBody,
    rightBody,
  } = elements;

  // Use OFF eyes for isOff, logoMode, or original eye style
  const useOriginalEyes = eyeStyle === "original";
  const useOffEyes = isOff || logoMode || useOriginalEyes;

  // ===========================
  // Character transforms
  // ===========================
  gsap.set(character, {
    x: 0,
    y: 0,
    scale: isOff ? offScale : activeScale,
    rotation: 0,
    rotationY: 0,
    opacity: 1,
    transformOrigin: "center center",
  });

  // ===========================
  // Eye containers - position and transforms
  // ===========================
  // OFF/logo state: eyes move closer together (logo position)
  // Original eye style uses triangle shapes but at normal position when active
  // Pixel values scaled by sizeScale (designed for 160px base)
  const eyeOffsetX = isOff || logoMode ? 3 * sizeScale : 0;
  const eyeOffsetY = isOff || logoMode ? 3 * sizeScale : 0;

  if (eyeLeft) {
    gsap.set(eyeLeft, {
      x: eyeOffsetX, // Move right toward center when OFF
      y: eyeOffsetY,
      scaleX: 1,
      scaleY: 1,
      rotation: 0,
      transformOrigin: "center center",
    });
  }
  if (eyeRight) {
    gsap.set(eyeRight, {
      x: -eyeOffsetX, // Move left toward center when OFF
      y: eyeOffsetY,
      scaleX: 1,
      scaleY: 1,
      rotation: 0,
      transformOrigin: "center center",
    });
  }

  // ===========================
  // Eye SVG paths - IDLE or OFF shape based on state
  // ===========================
  if (eyeLeftPath) {
    const leftShape: EyeShapeName = useOffEyes ? "OFF_LEFT" : "IDLE";
    gsap.set(eyeLeftPath, {
      attr: { d: getEyeShape(leftShape, "left") },
    });
  }
  if (eyeRightPath) {
    const rightShape: EyeShapeName = useOffEyes ? "OFF_RIGHT" : "IDLE";
    gsap.set(eyeRightPath, {
      attr: { d: getEyeShape(rightShape, "right") },
    });
  }

  // ===========================
  // Eye SVG viewBox - IDLE or OFF dimensions
  // ===========================
  if (eyeLeftSvg && eyeRightSvg) {
    const dimensions = useOffEyes ? getEyeDimensions("OFF_LEFT") : getEyeDimensions("IDLE");
    gsap.set([eyeLeftSvg, eyeRightSvg], {
      attr: { viewBox: dimensions.viewBox },
    });
  }

  // ===========================
  // Eye container dimensions - match the shape, scaled appropriately
  // ===========================
  if (eyeLeft && eyeRight) {
    const dimensions = useOffEyes ? getEyeDimensions("OFF_LEFT") : getEyeDimensions("IDLE");
    gsap.set([eyeLeft, eyeRight], {
      width: dimensions.width * sizeScale,
      height: dimensions.height * sizeScale,
    });
  }

  // ===========================
  // Shadow - initial state before ShadowTracker takes over
  // When ON: grounded state (full size, visible)
  // When OFF: shrunk state (wake-up will fade it in)
  // When logoMode: hidden (no shadow for logo state)
  // After init, ShadowTracker dynamically updates based on character Y
  // ===========================
  if (shadow) {
    gsap.set(shadow, {
      xPercent: -50,
      scaleX: isOff ? 0.7 : 1,
      scaleY: isOff ? 0.55 : 1,
      opacity: logoMode ? 0 : isOff ? 0.2 : 0.7, // Hidden in logoMode
    });
  }

  // ===========================
  // Glows - follow character at 75% distance
  // ===========================
  const glowElements = [innerGlow, outerGlow].filter(Boolean) as HTMLElement[];
  if (glowElements.length > 0) {
    gsap.set(glowElements, {
      x: 0,
      y: 0,
      scale: isOff ? offScale : activeScale,
      opacity: isOff || logoMode ? 0 : 1, // Hidden in both OFF and logoMode
    });
  }

  // ===========================
  // Body brackets - for shocked animation
  // ===========================
  if (leftBody && rightBody) {
    gsap.set([leftBody, rightBody], {
      x: 0,
      y: 0,
    });
  }
}

/**
 * Reset eyes to IDLE state
 *
 * Used after emotion animations to return eyes to neutral.
 * This is a common pattern extracted to avoid duplication.
 *
 * @param elements - Eye elements to reset
 * @param duration - Animation duration (0 for instant)
 */
export function resetEyesToIdle(
  elements: Pick<
    CharacterElements,
    "eyeLeft" | "eyeRight" | "eyeLeftPath" | "eyeRightPath" | "eyeLeftSvg" | "eyeRightSvg"
  >,
  duration = 0,
  sizeScale = 1
): gsap.core.Timeline | undefined {
  const { eyeLeft, eyeRight, eyeLeftPath, eyeRightPath, eyeLeftSvg, eyeRightSvg } = elements;

  if (!eyeLeft || !eyeRight || !eyeLeftPath || !eyeRightPath || !eyeLeftSvg || !eyeRightSvg) {
    return;
  }

  const idleDimensions = getEyeDimensions("IDLE");

  if (duration === 0) {
    // Instant reset - use mirrored path for right eye
    gsap.set(eyeLeftPath, {
      attr: { d: getEyeShape("IDLE", "left") },
    });
    gsap.set(eyeRightPath, {
      attr: { d: getEyeShape("IDLE", "right") },
    });
    gsap.set([eyeLeftSvg, eyeRightSvg], {
      attr: { viewBox: idleDimensions.viewBox },
    });
    // Reset container dimensions to IDLE size (scaled)
    gsap.set([eyeLeft, eyeRight], {
      width: idleDimensions.width * sizeScale,
      height: idleDimensions.height * sizeScale,
    });
    gsap.set([eyeLeft, eyeRight], {
      scaleX: 1,
      scaleY: 1,
      rotation: 0,
      x: 0,
      y: 0,
    });
    return;
  }

  // Animated reset - use mirrored path for right eye
  const timeline = gsap.timeline();

  timeline.to(
    eyeLeftPath,
    {
      attr: { d: getEyeShape("IDLE", "left") },
      duration,
      ease: "power2.inOut",
    },
    0
  );

  timeline.to(
    eyeRightPath,
    {
      attr: { d: getEyeShape("IDLE", "right") },
      duration,
      ease: "power2.inOut",
    },
    0
  );

  timeline.to(
    [eyeLeftSvg, eyeRightSvg],
    {
      attr: { viewBox: idleDimensions.viewBox },
      duration,
      ease: "power2.inOut",
    },
    0
  );

  timeline.to(
    [eyeLeft, eyeRight],
    {
      width: idleDimensions.width * sizeScale,
      height: idleDimensions.height * sizeScale,
      scaleX: 1,
      scaleY: 1,
      rotation: 0,
      x: 0,
      y: 0,
      duration,
      ease: "power2.inOut",
    },
    0
  );

  return timeline;
}

/**
 * Reset eyes to LOGO/OFF state
 *
 * Used after emotion animations in logo mode to return eyes to logo state.
 * Similar to resetEyesToIdle but uses OFF eye shapes.
 *
 * @param elements - Eye elements to reset
 * @param duration - Animation duration (0 for instant)
 * @param sizeScale - Scale factor for the character
 */
export function resetEyesToLogo(
  elements: Pick<
    CharacterElements,
    "eyeLeft" | "eyeRight" | "eyeLeftPath" | "eyeRightPath" | "eyeLeftSvg" | "eyeRightSvg"
  >,
  duration = 0,
  sizeScale = 1
): gsap.core.Timeline | undefined {
  const { eyeLeft, eyeRight, eyeLeftPath, eyeRightPath, eyeLeftSvg, eyeRightSvg } = elements;

  if (!eyeLeft || !eyeRight || !eyeLeftPath || !eyeRightPath || !eyeLeftSvg || !eyeRightSvg) {
    return;
  }

  const offDimensions = getEyeDimensions("OFF_LEFT");
  const eyeOffsetX = 3 * sizeScale; // Logo position offset
  const eyeOffsetY = 3 * sizeScale;

  if (duration === 0) {
    // Instant reset to logo eyes
    gsap.set(eyeLeftPath, {
      attr: { d: getEyeShape("OFF_LEFT", "left") },
    });
    gsap.set(eyeRightPath, {
      attr: { d: getEyeShape("OFF_RIGHT", "right") },
    });
    gsap.set([eyeLeftSvg, eyeRightSvg], {
      attr: { viewBox: offDimensions.viewBox },
    });
    gsap.set([eyeLeft, eyeRight], {
      width: offDimensions.width * sizeScale,
      height: offDimensions.height * sizeScale,
    });
    gsap.set(eyeLeft, {
      scaleX: 1,
      scaleY: 1,
      rotation: 0,
      x: eyeOffsetX,
      y: eyeOffsetY,
    });
    gsap.set(eyeRight, {
      scaleX: 1,
      scaleY: 1,
      rotation: 0,
      x: -eyeOffsetX,
      y: eyeOffsetY,
    });
    return;
  }

  // Animated reset to logo eyes
  const timeline = gsap.timeline();

  timeline.to(
    eyeLeftPath,
    {
      attr: { d: getEyeShape("OFF_LEFT", "left") },
      duration,
      ease: "power2.inOut",
    },
    0
  );

  timeline.to(
    eyeRightPath,
    {
      attr: { d: getEyeShape("OFF_RIGHT", "right") },
      duration,
      ease: "power2.inOut",
    },
    0
  );

  timeline.to(
    [eyeLeftSvg, eyeRightSvg],
    {
      attr: { viewBox: offDimensions.viewBox },
      duration,
      ease: "power2.inOut",
    },
    0
  );

  timeline.to(
    [eyeLeft, eyeRight],
    {
      width: offDimensions.width * sizeScale,
      height: offDimensions.height * sizeScale,
      scaleX: 1,
      scaleY: 1,
      rotation: 0,
      duration,
      ease: "power2.inOut",
    },
    0
  );

  timeline.to(
    eyeLeft,
    {
      x: eyeOffsetX,
      y: eyeOffsetY,
      duration,
      ease: "power2.inOut",
    },
    0
  );

  timeline.to(
    eyeRight,
    {
      x: -eyeOffsetX,
      y: eyeOffsetY,
      duration,
      ease: "power2.inOut",
    },
    0
  );

  return timeline;
}

/**
 * Reset eyes to original triangle state (no logo offset)
 *
 * Used after emotion animations when eyeStyle='original' to return eyes
 * to the triangle resting shape without the 3px logo bunching offset.
 */
export function resetEyesToOriginal(
  elements: Pick<
    CharacterElements,
    "eyeLeft" | "eyeRight" | "eyeLeftPath" | "eyeRightPath" | "eyeLeftSvg" | "eyeRightSvg"
  >,
  duration = 0,
  sizeScale = 1
): gsap.core.Timeline | undefined {
  const { eyeLeft, eyeRight, eyeLeftPath, eyeRightPath, eyeLeftSvg, eyeRightSvg } = elements;

  if (!eyeLeft || !eyeRight || !eyeLeftPath || !eyeRightPath || !eyeLeftSvg || !eyeRightSvg) {
    return;
  }

  const offDimensions = getEyeDimensions("OFF_LEFT");

  if (duration === 0) {
    gsap.set(eyeLeftPath, {
      attr: { d: getEyeShape("OFF_LEFT", "left") },
    });
    gsap.set(eyeRightPath, {
      attr: { d: getEyeShape("OFF_RIGHT", "right") },
    });
    gsap.set([eyeLeftSvg, eyeRightSvg], {
      attr: { viewBox: offDimensions.viewBox },
    });
    gsap.set([eyeLeft, eyeRight], {
      width: offDimensions.width * sizeScale,
      height: offDimensions.height * sizeScale,
      scaleX: 1,
      scaleY: 1,
      rotation: 0,
      x: 0,
      y: 0,
    });
    return;
  }

  const timeline = gsap.timeline();

  timeline.to(
    eyeLeftPath,
    {
      attr: { d: getEyeShape("OFF_LEFT", "left") },
      duration,
      ease: "power2.inOut",
    },
    0
  );

  timeline.to(
    eyeRightPath,
    {
      attr: { d: getEyeShape("OFF_RIGHT", "right") },
      duration,
      ease: "power2.inOut",
    },
    0
  );

  timeline.to(
    [eyeLeftSvg, eyeRightSvg],
    {
      attr: { viewBox: offDimensions.viewBox },
      duration,
      ease: "power2.inOut",
    },
    0
  );

  timeline.to(
    [eyeLeft, eyeRight],
    {
      width: offDimensions.width * sizeScale,
      height: offDimensions.height * sizeScale,
      scaleX: 1,
      scaleY: 1,
      rotation: 0,
      x: 0,
      y: 0,
      duration,
      ease: "power2.inOut",
    },
    0
  );

  return timeline;
}
