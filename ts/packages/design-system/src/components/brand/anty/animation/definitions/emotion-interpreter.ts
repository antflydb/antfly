/**
 * Emotion Animation Interpreter
 *
 * Generic interpreter that builds GSAP timelines from declarative EmotionConfig.
 * This is the ONLY place where emotion animations are created from configs.
 *
 * ~100 lines replacing the 960-line createEmotionAnimation function.
 */

import gsap from "gsap";
import { resetEyesToIdle, resetEyesToLogo, resetEyesToOriginal } from "../initialize";
import type {
  BodyConfig,
  CharacterPhase,
  EmotionConfig,
  EyeConfig,
  EyePhase,
  EyeStyle,
  GlowConfig,
} from "../types";
import { createEyeAnimation } from "./eye-animations";

// NOTE: GLOW_CONSTANTS removed - glow following now handled by GlowSystem

/**
 * Duration scale factor - adjusts how much sizeScale affects animation speed.
 * Uses INVERSE scaling: smaller = slower, larger = faster
 * 0 = no scaling (same speed at all sizes)
 * 1 = full inverse scaling
 * 0.3 = subtle inverse scaling (recommended)
 */
const DURATION_SCALE_FACTOR = 0;

/** Calculate scaled duration based on sizeScale and DURATION_SCALE_FACTOR */
function getScaledDuration(baseDuration: number, sizeScale: number): number {
  // Inverse scaling: smaller characters get longer durations (slower)
  // larger characters get shorter durations (faster)
  const inverseScale = 1 / sizeScale;
  const effectiveScale = 1 + (inverseScale - 1) * DURATION_SCALE_FACTOR;
  return baseDuration * effectiveScale;
}

// Module-level tracking of pending eye reset calls
// This allows us to kill pending resets when a new emotion starts
let globalPendingResetCall: gsap.core.Tween | null = null;

/**
 * Kill any pending eye reset from a previous emotion
 * Call this before starting a new emotion animation
 */
export function killPendingEyeReset(): void {
  if (globalPendingResetCall) {
    globalPendingResetCall.kill();
    globalPendingResetCall = null;
  }
}

/**
 * Elements required for emotion animations
 */
export interface EmotionElements {
  character: HTMLElement;
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
 * Interpret an EmotionConfig and build a GSAP timeline
 *
 * @param config - Declarative emotion configuration
 * @param elements - DOM elements to animate
 * @param sizeScale - Scale factor for the character (size / 160)
 * @param logoMode - If true, reset to logo eyes instead of idle after emotion
 * @returns GSAP timeline ready to play
 */
export function interpretEmotionConfig(
  config: EmotionConfig,
  elements: EmotionElements,
  sizeScale: number = 1,
  logoMode: boolean = false,
  eyeStyle: EyeStyle = "alive"
): gsap.core.Timeline {
  const {
    character,
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
  const glowElements = [innerGlow, outerGlow].filter(Boolean) as HTMLElement[];

  // Kill any pending reset from previous emotion BEFORE setting up new one
  killPendingEyeReset();

  const doReset = () => {
    // Clear the global pending reset since we're executing it
    globalPendingResetCall = null;

    // Reset rotation if configured
    if (config.resetRotation) {
      gsap.set(character, { rotation: 0 });
    }
    if (config.resetRotationY) {
      gsap.set(character, { rotationY: 0 });
    }

    // Defensive: ensure character x is reset to 0 after emotion completes
    // This ensures glow tracking stays aligned even if emotion animation
    // didn't fully return to x=0 due to timing edge cases
    gsap.set(character, { x: 0 });

    // Reset eyes based on mode and eye style
    if (logoMode) {
      resetEyesToLogo(elements, config.eyeResetDuration ?? 0, sizeScale);
    } else if (eyeStyle === "original") {
      resetEyesToOriginal(elements, config.eyeResetDuration ?? 0, sizeScale);
    } else {
      resetEyesToIdle(elements, config.eyeResetDuration ?? 0, sizeScale);
    }

    // Reset body brackets if they were animated
    if (config.body && leftBody && rightBody) {
      gsap.set([leftBody, rightBody], { x: 0, y: 0 });
    }
  };

  const timeline = gsap.timeline({
    paused: true, // Don't auto-play - controller will play when ready
    onComplete: () => {
      // If holdDuration is set, wait before resetting (for look animations)
      // Duration NOT scaled - same timing at all sizes
      if (config.holdDuration) {
        globalPendingResetCall = gsap.delayedCall(config.holdDuration, doReset);
      } else {
        doReset();
      }
    },
    onInterrupt: () => {
      // Kill any pending delayed reset when interrupted
      killPendingEyeReset();
      // Reset eye rotation/position immediately so next emotion starts clean
      // (critical for sad→idea where eye rotations would otherwise persist)
      if (eyeLeft && eyeRight) {
        gsap.set([eyeLeft, eyeRight], { rotation: 0, x: 0, y: 0, scaleX: 1, scaleY: 1 });
      }
      // Reset body brackets immediately (critical for shocked interruption)
      if (leftBody && rightBody) {
        gsap.set([leftBody, rightBody], { x: 0, y: 0 });
      }
      // Reset character x position (critical for sad/angry interrupted mid-shake)
      // Note: Don't reset y as idle handles that, but x must be 0 for glow tracking
      gsap.set(character, { x: 0 });
    },
  });

  // ===========================
  // Eye Animation (static, applied once at start)
  // ===========================
  if (
    config.eyes &&
    eyeLeft &&
    eyeRight &&
    eyeLeftPath &&
    eyeRightPath &&
    eyeLeftSvg &&
    eyeRightSvg
  ) {
    addEyeAnimation(timeline, config.eyes, elements, sizeScale);
  }

  // ===========================
  // Body Bracket Animation (shocked)
  // ===========================
  if (config.body && leftBody && rightBody) {
    addBodyAnimation(timeline, config.body, leftBody, rightBody, sizeScale);
  }

  // ===========================
  // Character Movement Phases
  // ===========================
  addCharacterPhases(timeline, config.character, character, glowElements, config.glow, sizeScale);

  // ===========================
  // Eye Phases (dynamic, per-keyframe)
  // Must be added AFTER character phases so timeline has correct duration
  // ===========================
  if (
    config.eyePhases &&
    eyeLeft &&
    eyeRight &&
    eyeLeftPath &&
    eyeRightPath &&
    eyeLeftSvg &&
    eyeRightSvg
  ) {
    addEyePhases(timeline, config.eyePhases, elements, sizeScale);
  }

  return timeline;
}

/**
 * Add eye animation to timeline
 */
function addEyeAnimation(
  timeline: gsap.core.Timeline,
  eyeConfig: EyeConfig,
  elements: EmotionElements,
  sizeScale: number
): void {
  const { eyeLeft, eyeRight, eyeLeftPath, eyeRightPath, eyeLeftSvg, eyeRightSvg } = elements;

  if (!eyeLeft || !eyeRight || !eyeLeftPath || !eyeRightPath || !eyeLeftSvg || !eyeRightSvg) {
    return;
  }

  // Scale duration based on DURATION_SCALE_FACTOR (0 = no scaling, 1 = full scaling)
  const scaledDuration = getScaledDuration(eyeConfig.duration ?? 0.2, sizeScale);

  // Create eye morph animation
  const eyeTl = createEyeAnimation(
    {
      leftEye: eyeLeft,
      rightEye: eyeRight,
      leftEyePath: eyeLeftPath,
      rightEyePath: eyeRightPath,
      leftEyeSvg: eyeLeftSvg,
      rightEyeSvg: eyeRightSvg,
    },
    eyeConfig.shape,
    { duration: scaledDuration, sizeScale }
  );

  // Timeline position NOT scaled - same timing at all sizes
  const eyePosition = eyeConfig.delay ?? 0;
  timeline.add(eyeTl, eyePosition);

  // Eye position/transform animations
  // All pixel values are scaled by sizeScale (designed for 160px base)
  if (
    eyeConfig.yOffset !== undefined ||
    eyeConfig.xOffset !== undefined ||
    eyeConfig.scale !== undefined ||
    eyeConfig.bunch !== undefined
  ) {
    const bunch = (eyeConfig.bunch ?? 0) * sizeScale;

    // Handle per-eye or shared offsets (scaled)
    const leftYOffset =
      (typeof eyeConfig.yOffset === "object" ? eyeConfig.yOffset.left : (eyeConfig.yOffset ?? 0)) *
      sizeScale;
    const rightYOffset =
      (typeof eyeConfig.yOffset === "object" ? eyeConfig.yOffset.right : (eyeConfig.yOffset ?? 0)) *
      sizeScale;
    const leftXOffset =
      (typeof eyeConfig.xOffset === "object" ? eyeConfig.xOffset.left : (eyeConfig.xOffset ?? 0)) *
      sizeScale;
    const rightXOffset =
      (typeof eyeConfig.xOffset === "object" ? eyeConfig.xOffset.right : (eyeConfig.xOffset ?? 0)) *
      sizeScale;

    // Left eye: moves in direction + bunches towards center
    timeline.to(
      eyeLeft,
      {
        y: leftYOffset,
        x: leftXOffset + bunch,
        scaleX: eyeConfig.scale ?? 1,
        scaleY: eyeConfig.scale ?? 1,
        duration: scaledDuration,
        ease: "power2.out",
      },
      eyePosition
    );

    // Right eye: moves in direction - bunches towards center
    timeline.to(
      eyeRight,
      {
        y: rightYOffset,
        x: rightXOffset - bunch,
        scaleX: eyeConfig.scale ?? 1,
        scaleY: eyeConfig.scale ?? 1,
        duration: scaledDuration,
        ease: "power2.out",
      },
      eyePosition
    );
  }

  // Eye rotations (for sad/angry) - happen during the morph, not after
  // Note: Right eye shape is already mirrored at the path level,
  // so rotation values are simply applied directly (no scaleX flip needed)
  if (eyeConfig.leftRotation !== undefined) {
    timeline.to(
      eyeLeft,
      {
        rotation: eyeConfig.leftRotation,
        duration: scaledDuration,
        ease: "power2.out",
      },
      eyePosition
    );
  }

  if (eyeConfig.rightRotation !== undefined) {
    timeline.to(
      eyeRight,
      {
        rotation: eyeConfig.rightRotation,
        duration: scaledDuration,
        ease: "power2.out",
      },
      eyePosition
    );
  }

  // Eye return animation (for shocked - scale back down)
  if (eyeConfig.returnPosition !== undefined) {
    // Duration/position NOT scaled - same timing at all sizes
    const returnDuration = eyeConfig.returnDuration ?? 0.25;
    const scaledReturnPosition = eyeConfig.returnPosition;
    timeline.to(
      [eyeLeft, eyeRight],
      {
        scaleX: 1,
        scaleY: 1,
        y: 0,
        x: 0,
        duration: returnDuration,
        ease: "power2.out",
      },
      scaledReturnPosition
    );
  }
}

/**
 * Add per-phase eye animations (for dynamic eye changes during animation)
 */
function addEyePhases(
  timeline: gsap.core.Timeline,
  eyePhases: EyePhase[],
  elements: EmotionElements,
  sizeScale: number
): void {
  const { eyeLeft, eyeRight, eyeLeftPath, eyeRightPath, eyeLeftSvg, eyeRightSvg } = elements;

  if (!eyeLeft || !eyeRight || !eyeLeftPath || !eyeRightPath || !eyeLeftSvg || !eyeRightSvg) {
    return;
  }

  for (const phase of eyePhases) {
    // Scale duration based on DURATION_SCALE_FACTOR (0 = no scaling, 1 = full scaling)
    const scaledDuration = getScaledDuration(phase.duration ?? 0.2, sizeScale);
    // Timeline position NOT scaled - same timing at all sizes
    const scaledPosition = phase.position;

    const eyeTl = createEyeAnimation(
      {
        leftEye: eyeLeft,
        rightEye: eyeRight,
        leftEyePath: eyeLeftPath,
        rightEyePath: eyeRightPath,
        leftEyeSvg: eyeLeftSvg,
        rightEyeSvg: eyeRightSvg,
      },
      phase.shape,
      { duration: scaledDuration, sizeScale }
    );

    timeline.add(eyeTl, scaledPosition);

    // Handle eye position changes (xOffset, bunch)
    // All pixel values are scaled by sizeScale (designed for 160px base)
    if (phase.xOffset !== undefined || phase.bunch !== undefined) {
      const bunch = (phase.bunch ?? 0) * sizeScale;
      const xOffset = (phase.xOffset ?? 0) * sizeScale;

      // Left eye: xOffset + bunch towards center
      timeline.to(
        eyeLeft,
        {
          x: xOffset + bunch,
          duration: scaledDuration,
          ease: "power2.out",
        },
        scaledPosition
      );

      // Right eye: xOffset - bunch towards center
      timeline.to(
        eyeRight,
        {
          x: xOffset - bunch,
          duration: scaledDuration,
          ease: "power2.out",
        },
        scaledPosition
      );
    }
  }
}

/**
 * Add body bracket animation (shocked)
 */
function addBodyAnimation(
  timeline: gsap.core.Timeline,
  bodyConfig: BodyConfig,
  leftBody: HTMLElement,
  rightBody: HTMLElement,
  sizeScale: number = 1
): void {
  // Duration/position NOT scaled - same timing at all sizes
  const duration = bodyConfig.duration ?? 0.2;
  const ease = bodyConfig.ease ?? "back.out(2)";
  const returnPosition = bodyConfig.returnPosition ?? "+=1.15";
  const returnDuration = bodyConfig.returnDuration ?? 0.25;
  const returnEase = bodyConfig.returnEase ?? "elastic.out(1, 0.5)";

  // Separate brackets - scale pixel values by sizeScale (designed for 160px base)
  timeline.to(
    leftBody,
    {
      x: (bodyConfig.leftX ?? 0) * sizeScale,
      y: (bodyConfig.leftY ?? 0) * sizeScale,
      duration,
      ease,
    },
    0
  );

  timeline.to(
    rightBody,
    {
      x: (bodyConfig.rightX ?? 0) * sizeScale,
      y: (bodyConfig.rightY ?? 0) * sizeScale,
      duration,
      ease,
    },
    0
  );

  // Return brackets
  timeline.to(
    [leftBody, rightBody],
    {
      x: 0,
      y: 0,
      duration: returnDuration,
      ease: returnEase,
    },
    returnPosition
  );
}

/**
 * Add character movement phases
 * NOTE: Glow following is now handled by GlowSystem via physics-based tracking
 */
function addCharacterPhases(
  timeline: gsap.core.Timeline,
  phases: CharacterPhase[],
  character: HTMLElement,
  _glowElements: HTMLElement[],
  _glowConfig?: GlowConfig,
  sizeScale: number = 1
): void {
  let isFirstPhase = true;
  for (const phase of phases) {
    // First phase starts at 0, others sequence naturally or use explicit position
    // Timeline position NOT scaled - same timing at all sizes
    const position = phase.position ?? (isFirstPhase ? 0 : undefined);
    isFirstPhase = false;

    // Scale x and y pixel values by sizeScale (designed for 160px base)
    // Other props like rotation, scale, opacity remain unchanged
    const scaledProps = { ...phase.props };
    if (typeof scaledProps.x === "number") {
      scaledProps.x = scaledProps.x * sizeScale;
    }
    if (typeof scaledProps.y === "number") {
      scaledProps.y = scaledProps.y * sizeScale;
    }

    // Scale duration based on DURATION_SCALE_FACTOR (0 = no scaling, 1 = full scaling)
    const scaledDuration = getScaledDuration(phase.duration, sizeScale);

    // Animate character
    timeline.to(
      character,
      {
        ...scaledProps,
        duration: scaledDuration,
        ease: phase.ease,
      },
      position
    );

    // NOTE: Glow following removed - GlowSystem tracks character position via physics
  }
}
