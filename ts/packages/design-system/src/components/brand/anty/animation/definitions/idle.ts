/**
 * Idle Animation Definitions
 * Pure functions that create GSAP timelines for idle/breathing animations
 *
 * Includes built-in periodic blinking scheduler for natural secondary motion
 */

import gsap from "gsap";
import { IDLE_BREATHE, IDLE_FLOAT, IDLE_ROTATION } from "../constants";
import { ENABLE_ANIMATION_DEBUG_LOGS } from "../feature-flags";
import type { EyeStyle } from "../types";
import {
  createBlinkAnimation,
  createDoubleBlinkAnimation,
  type EyeAnimationElements,
} from "./eye-animations";
import { getEyeDimensions, getEyeShape } from "./eye-shapes";

export interface IdleAnimationElements {
  character: HTMLElement;
  shadow: HTMLElement;
  // Eye elements for morphing
  eyeLeft?: HTMLElement;
  eyeRight?: HTMLElement;
  eyeLeftPath?: SVGPathElement;
  eyeRightPath?: SVGPathElement;
  eyeLeftSvg?: SVGSVGElement;
  eyeRightSvg?: SVGSVGElement;
}

export interface IdleAnimationOptions {
  /** Delay before starting animation (seconds) */
  delay?: number;
  /** Base scale for character (default: 1, use 1.45 for super mode) */
  baseScale?: number;
  /** Size scale factor (size / 160) for proportional animations */
  sizeScale?: number;
  /** Whether the character floats up/down with gentle rotation/breathing (default: true) */
  enableFloat?: boolean;
  /** Whether the spontaneous blink scheduler runs (default: true) */
  enableBlinks?: boolean;
  /** Eye style for resting state (default: 'alive') */
  eyeStyle?: EyeStyle;
  /** Override float amplitude in pixels (default: IDLE_FLOAT.amplitude = 12) */
  floatAmplitude?: number;
  /** Override float cycle duration in seconds (default: IDLE_FLOAT.duration = 2.5) */
  floatDuration?: number;
  /** Override float easing curve (default: IDLE_FLOAT.ease = 'sine.inOut') */
  floatEase?: string;
}

/**
 * Return type for createIdleAnimation
 * Includes timeline and blink scheduler controls
 */
export interface IdleAnimationResult {
  /** The GSAP timeline for idle float/breathe animation */
  timeline: gsap.core.Timeline;
  /** Pause the blink scheduler (call when emotion starts) */
  pauseBlinks: () => void;
  /** Resume the blink scheduler (call when emotion ends) */
  resumeBlinks: () => void;
  /** Kill the blink scheduler entirely (call on cleanup) */
  killBlinks: () => void;
}

/**
 * Creates the idle breathing animation timeline
 *
 * Coordinates three synchronized animations:
 * 1. Vertical floating (up/down motion)
 * 2. Gentle rotation (synchronized with float)
 * 3. Breathing scale (subtle size changes)
 * 4. Periodic blinking (5-12 second intervals, 20% chance of double blink)
 *
 * Shadow inversely follows character movement (shrinks when character floats up)
 *
 * @param elements - Character and shadow DOM elements (plus optional eye elements for blinking)
 * @param options - Optional delay configuration
 * @returns Object with timeline and blink scheduler controls
 */
export function createIdleAnimation(
  elements: IdleAnimationElements,
  options: IdleAnimationOptions = {}
): IdleAnimationResult {
  const {
    character,
    shadow: _shadow,
    eyeLeft,
    eyeRight,
    eyeLeftPath,
    eyeRightPath,
    eyeLeftSvg,
    eyeRightSvg,
  } = elements;
  // Note: shadow is handled by ShadowTracker, not the idle timeline
  void _shadow; // Mark as intentionally unused
  const {
    delay = 0.2,
    baseScale = 1,
    sizeScale = 1,
    enableFloat = true,
    enableBlinks = true,
    eyeStyle = "alive",
    floatAmplitude = IDLE_FLOAT.amplitude,
    floatDuration = IDLE_FLOAT.duration,
    floatEase = IDLE_FLOAT.ease,
  } = options;

  // Scale float amplitude by sizeScale so smaller characters have proportionally smaller bounce
  const scaledAmplitude = floatAmplitude * sizeScale;

  // Create coordinated timeline with infinite repeat
  const timeline = gsap.timeline({
    repeat: -1,
    yoyo: true,
    delay,
  });

  // Reset character to base state immediately
  // Use baseScale to preserve super mode scale (1.45) when applicable
  gsap.set(character, {
    scale: baseScale,
    rotation: 0,
    y: 0,
  });

  // Set eyes to resting shape immediately (not animated, to avoid yoyo oscillation)
  const isOriginal = eyeStyle === "original";
  if (eyeLeft && eyeRight && eyeLeftPath && eyeRightPath && eyeLeftSvg && eyeRightSvg) {
    const restingLeftShape = isOriginal ? ("OFF_LEFT" as const) : ("IDLE" as const);
    const restingRightShape = isOriginal ? ("OFF_RIGHT" as const) : ("IDLE" as const);
    const restingDimensions = getEyeDimensions(restingLeftShape);

    gsap.set(eyeLeftPath, {
      attr: { d: getEyeShape(restingLeftShape, "left") },
    });
    gsap.set(eyeRightPath, {
      attr: { d: getEyeShape(restingRightShape, "right") },
    });

    gsap.set([eyeLeftSvg, eyeRightSvg], {
      attr: { viewBox: restingDimensions.viewBox },
    });

    // Reset eye transforms
    gsap.set([eyeLeft, eyeRight], {
      scaleX: 1,
      scaleY: 1,
      rotation: 0,
      x: 0,
      y: 0,
    });

    // REMOVED: width/height properties - containers stay fixed at CSS defaults
  }

  // Character floats up with rotation and breathing
  // CRITICAL: Use relative scale (*=) so super mode scale persists across idle restarts
  // The timeline captures values at creation time, so absolute scale (baseScale * 1.02)
  // would target the wrong value if super mode was set after idle was created.
  if (enableFloat) {
    timeline.to(
      character,
      {
        y: -scaledAmplitude,
        rotation: IDLE_ROTATION.degrees,
        scale: `*=${IDLE_BREATHE.scaleMax}`,
        duration: floatDuration,
        ease: floatEase,
      },
      0 // Start at timeline beginning
    );
  }

  // NOTE: Shadow is now handled by the ShadowTracker (shadow.ts)
  // which dynamically responds to character Y position for ALL animations,
  // not just idle. This eliminates disjointed shadow behavior.

  // ===========================
  // Spontaneous Action Scheduler
  // ===========================
  // Handles random eye-only actions: blinks and looks
  // Separate from idle timeline so it can be paused/resumed independently
  let schedulerActive = false;
  let schedulerPaused = false;
  let currentDelayedCall: gsap.core.Tween | null = null;

  const hasEyeElements =
    eyeLeft && eyeRight && eyeLeftPath && eyeRightPath && eyeLeftSvg && eyeRightSvg;

  const scheduleNextAction = () => {
    if (!schedulerActive || schedulerPaused || !hasEyeElements) return;

    const actionDelay = gsap.utils.random(8, 15); // 8-15 seconds between actions (less frequent)

    currentDelayedCall = gsap.delayedCall(actionDelay, () => {
      if (!schedulerActive || schedulerPaused) return;

      const eyeElements: EyeAnimationElements = {
        leftEye: eyeLeft!,
        rightEye: eyeRight!,
        leftEyePath: eyeLeftPath!,
        rightEyePath: eyeRightPath!,
        leftEyeSvg: eyeLeftSvg!,
        rightEyeSvg: eyeRightSvg!,
      };

      // Pick a random spontaneous action:
      // 80% single blink, 20% double blink
      const roll = Math.random();

      if (roll < 0.2) {
        // Double blink
        if (ENABLE_ANIMATION_DEBUG_LOGS) {
          console.log("[Spontaneous] Double blink");
        }
        createDoubleBlinkAnimation(eyeElements).play();
      } else {
        // Single blink
        if (ENABLE_ANIMATION_DEBUG_LOGS) {
          console.log("[Spontaneous] Single blink");
        }
        createBlinkAnimation(eyeElements).play();
      }

      // Schedule next action
      scheduleNextAction();
    });
  };

  const pauseBlinks = () => {
    schedulerPaused = true;
    // Kill any pending delayed call
    if (currentDelayedCall) {
      currentDelayedCall.kill();
      currentDelayedCall = null;
    }
  };

  const resumeBlinks = () => {
    if (!schedulerActive) return;
    schedulerPaused = false;
    // Schedule a new action (fresh random delay)
    scheduleNextAction();
  };

  const killBlinks = () => {
    schedulerActive = false;
    schedulerPaused = false;
    if (currentDelayedCall) {
      currentDelayedCall.kill();
      currentDelayedCall = null;
    }
  };

  // Start the spontaneous action scheduler if we have eye elements and blinks are enabled
  if (hasEyeElements && enableBlinks) {
    schedulerActive = true;
    scheduleNextAction();
  }

  // Also cleanup on timeline kill
  const originalKill = timeline.kill.bind(timeline);
  timeline.kill = function (this: gsap.core.Timeline) {
    killBlinks();
    return originalKill();
  } as typeof timeline.kill;

  return {
    timeline,
    pauseBlinks,
    resumeBlinks,
    killBlinks,
  };
}
