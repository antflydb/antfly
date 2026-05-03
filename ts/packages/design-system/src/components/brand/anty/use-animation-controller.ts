/**
 * React Hook for Animation Controller
 *
 * Provides a clean React API for the AnimationController.
 * Manages lifecycle, initialization, and state synchronization.
 */

import gsap from "gsap";
import { useCallback, useEffect, useMemo, useRef } from "react";
import { AnimationController } from "./animation/controller";
import { interpretEmotionConfig } from "./animation/definitions/emotion-interpreter";
import { EMOTION_CONFIGS } from "./animation/definitions/emotions";
import { createIdleAnimation } from "./animation/definitions/idle";
import {
  createPowerOffAnimation,
  createWakeUpAnimation,
} from "./animation/definitions/transitions";
import { ENABLE_ANIMATION_DEBUG_LOGS } from "./animation/feature-flags";
import { createGlowSystem, type GlowSystemControls } from "./animation/glow-system";
import { initializeCharacter } from "./animation/initialize";
import { createShadowTracker, type ShadowTrackerControls } from "./animation/shadow";
import {
  type AnimationCallbacks,
  type AnimationOptions,
  AnimationState,
  type ControllerConfig,
  type EmotionType,
  type EyeStyle,
  isEmotionType,
} from "./animation/types";

export interface AnimationElements {
  container?: HTMLElement | null;
  character?: HTMLElement | null;
  shadow?: HTMLElement | null;
  eyeLeft?: HTMLElement | null;
  eyeRight?: HTMLElement | null;
  eyeLeftPath?: SVGPathElement | null;
  eyeRightPath?: SVGPathElement | null;
  eyeLeftSvg?: SVGSVGElement | null;
  eyeRightSvg?: SVGSVGElement | null;
  antennaLeft?: HTMLElement | null;
  antennaRight?: HTMLElement | null;
  glow?: HTMLElement | null;
  innerGlow?: HTMLElement | null;
  outerGlow?: HTMLElement | null;
  leftBody?: HTMLElement | null;
  rightBody?: HTMLElement | null;
  [key: string]: HTMLElement | SVGPathElement | SVGSVGElement | null | undefined;
}

export interface UseAnimationControllerOptions extends ControllerConfig {
  onStateChange?: (from: AnimationState, to: AnimationState) => void;
  onAnimationSequenceChange?: (sequence: string) => void;
  isOff?: boolean;
  logoMode?: boolean;
  autoStartIdle?: boolean;
  sizeScale?: number;
  /** Whether the idle float animation runs (default: true) */
  enableFloat?: boolean;
  /** Whether spontaneous blinks fire during idle (default: true) */
  enableBlinks?: boolean;
  /** Character scale while active (default: 1) */
  activeScale?: number;
  /** Character scale while off (default: 0.65) */
  offScale?: number;
  /** Eye style for resting state (default: 'alive') */
  eyeStyle?: EyeStyle;
  /** Override float amplitude in pixels (default: 12) */
  floatAmplitude?: number;
  /** Override float cycle duration in seconds (default: 2.5) */
  floatDuration?: number;
  /** Override float easing curve (default: 'sine.inOut') */
  floatEase?: string;
}

export interface UseAnimationControllerReturn {
  playEmotion: (emotion: EmotionType, options?: AnimationOptions) => boolean;
  transitionTo: (state: AnimationState, options?: AnimationOptions) => boolean;
  startIdle: () => void;
  pause: () => void;
  resume: () => void;
  killAll: () => void;
  getState: () => AnimationState;
  getEmotion: () => EmotionType | null;
  isIdle: () => boolean;
  isIdlePlaying: () => boolean;
  getDebugInfo: () => ReturnType<AnimationController["getDebugInfo"]>;
  isReady: boolean;
}

export function useAnimationController(
  elements: AnimationElements,
  options: UseAnimationControllerOptions = {}
): UseAnimationControllerReturn {
  const {
    enableLogging = false,
    enableQueue = true,
    maxQueueSize = 10,
    defaultPriority = 2,
    callbacks = {},
    onStateChange,
    onAnimationSequenceChange,
    isOff = false,
    logoMode = false,
    autoStartIdle = true,
    sizeScale = 1,
    enableFloat = true,
    enableBlinks = true,
    activeScale = 1,
    offScale = 0.65,
    eyeStyle = "alive",
    floatAmplitude,
    floatDuration,
    floatEase,
  } = options;

  const controllerRef = useRef<AnimationController | null>(null);
  const isInitialized = useRef(false);
  const isReady = useRef(false);
  const previousIsOffRef = useRef(isOff);
  const logoModeRef = useRef(logoMode);
  logoModeRef.current = logoMode;
  const enableFloatRef = useRef(enableFloat);
  enableFloatRef.current = enableFloat;
  const enableBlinksRef = useRef(enableBlinks);
  enableBlinksRef.current = enableBlinks;
  const activeScaleRef = useRef(activeScale);
  activeScaleRef.current = activeScale;
  const offScaleRef = useRef(offScale);
  offScaleRef.current = offScale;
  const eyeStyleRef = useRef(eyeStyle);
  eyeStyleRef.current = eyeStyle;
  const floatAmplitudeRef = useRef(floatAmplitude);
  floatAmplitudeRef.current = floatAmplitude;
  const floatDurationRef = useRef(floatDuration);
  floatDurationRef.current = floatDuration;
  const floatEaseRef = useRef(floatEase);
  floatEaseRef.current = floatEase;
  const isWakingUpRef = useRef(false);
  const shadowTrackerRef = useRef<ShadowTrackerControls | null>(null);
  const glowSystemRef = useRef<GlowSystemControls | null>(null);

  useEffect(() => {
    if (isInitialized.current) return;

    if (enableLogging && ENABLE_ANIMATION_DEBUG_LOGS) {
      console.log("[useAnimationController] Initializing controller");
    }

    const mergedCallbacks: AnimationCallbacks = {
      ...callbacks,
      onStateChange: (from, to) => {
        callbacks.onStateChange?.(from, to);
        onStateChange?.(from, to);
      },
      onEmotionMotionStart: (emotion, timelineId) => {
        const isEyeOnlyAction = emotion === "look-left" || emotion === "look-right";

        if (enableLogging && ENABLE_ANIMATION_DEBUG_LOGS && !isEyeOnlyAction) {
          console.log(`[useAnimationController] Emotion motion START: ${emotion}`);
        }
        if (!isEyeOnlyAction) {
          onAnimationSequenceChange?.(`MOTION_START:${emotion.toUpperCase()}`);
        }
        callbacks.onEmotionMotionStart?.(emotion, timelineId);
      },
      onEmotionMotionComplete: (emotion, timelineId, duration) => {
        const isEyeOnlyAction = emotion === "look-left" || emotion === "look-right";

        if (enableLogging && ENABLE_ANIMATION_DEBUG_LOGS && !isEyeOnlyAction) {
          console.log(
            `[useAnimationController] Emotion motion COMPLETE: ${emotion} (${duration}ms)`
          );
        }
        if (!isEyeOnlyAction) {
          onAnimationSequenceChange?.(`MOTION_COMPLETE:${emotion.toUpperCase()}:${duration}`);
        }

        callbacks.onEmotionMotionComplete?.(emotion, timelineId, duration);

        if (!isEyeOnlyAction) {
          setTimeout(() => {
            onAnimationSequenceChange?.("CONTROLLER: Idle animation");
          }, 100);
        }
      },
    };

    controllerRef.current = new AnimationController(mergedCallbacks, {
      enableLogging,
      enableQueue,
      maxQueueSize,
      defaultPriority,
    });

    isInitialized.current = true;

    return () => {
      shadowTrackerRef.current?.stop();
      shadowTrackerRef.current = null;
      glowSystemRef.current?.stop();
      glowSystemRef.current = null;
      controllerRef.current?.destroy();
      controllerRef.current = null;
      isInitialized.current = false;
      isReady.current = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [onStateChange, onAnimationSequenceChange, maxQueueSize, callbacks.onStateChange, enableQueue, callbacks, defaultPriority, enableLogging]);

  useEffect(() => {
    const hasRequiredElements =
      elements.container !== null &&
      elements.container !== undefined &&
      elements.character !== null &&
      elements.character !== undefined;

    const wasReady = isReady.current;
    isReady.current = hasRequiredElements;

    if (hasRequiredElements && !wasReady && elements.character) {
      initializeCharacter(
        {
          character: elements.character,
          shadow: elements.shadow || null,
          eyeLeft: elements.eyeLeft,
          eyeRight: elements.eyeRight,
          eyeLeftPath: elements.eyeLeftPath,
          eyeRightPath: elements.eyeRightPath,
          eyeLeftSvg: elements.eyeLeftSvg,
          eyeRightSvg: elements.eyeRightSvg,
          innerGlow: elements.innerGlow || null,
          outerGlow: elements.outerGlow || null,
          leftBody: elements.leftBody,
          rightBody: elements.rightBody,
        },
        { isOff, logoMode, sizeScale, activeScale, offScale, eyeStyle }
      );

      if (elements.shadow && !shadowTrackerRef.current) {
        shadowTrackerRef.current = createShadowTracker(elements.character, elements.shadow);
        if (!isOff && !logoMode) {
          shadowTrackerRef.current.start();
        }
      }

      if (elements.innerGlow && elements.outerGlow && !glowSystemRef.current) {
        glowSystemRef.current = createGlowSystem(
          elements.character,
          elements.outerGlow,
          elements.innerGlow,
          sizeScale
        );
        glowSystemRef.current.snapToCharacter();
        if (!isOff && !logoMode) {
          glowSystemRef.current.start();
          glowSystemRef.current.show();
        } else {
          glowSystemRef.current.hide();
        }
      }
    }
  }, [elements, isOff, logoMode, activeScale, sizeScale, offScale, eyeStyle]);

  useEffect(() => {
    if (glowSystemRef.current) {
      glowSystemRef.current.updateSizeScale(sizeScale);
      glowSystemRef.current.snapToCharacter();
    }
  }, [sizeScale]);

  useEffect(() => {
    if (!controllerRef.current) return;

    const wasOff = previousIsOffRef.current;
    const isNowOff = isOff;
    previousIsOffRef.current = isOff;

    if (wasOff && !isNowOff) {
      isWakingUpRef.current = true;
      onAnimationSequenceChange?.("CONTROLLER: Wake-up (OFF → ON)");
      controllerRef.current.killAll();

      if (elements.character && elements.shadow) {
        const wakeUpTl = createWakeUpAnimation(
          {
            character: elements.character,
            shadow: elements.shadow,
            innerGlow: elements.innerGlow || undefined,
            outerGlow: elements.outerGlow || undefined,
            eyeLeft: elements.eyeLeft || undefined,
            eyeRight: elements.eyeRight || undefined,
            eyeLeftPath: elements.eyeLeftPath || undefined,
            eyeRightPath: elements.eyeRightPath || undefined,
            eyeLeftSvg: elements.eyeLeftSvg || undefined,
            eyeRightSvg: elements.eyeRightSvg || undefined,
          },
          sizeScale,
          { activeScale: activeScaleRef.current, offScale: offScaleRef.current },
          eyeStyleRef.current
        );

        if (glowSystemRef.current) {
          glowSystemRef.current.snapToCharacter();
          glowSystemRef.current.start();
          glowSystemRef.current.fadeIn(0.3);
        }

        wakeUpTl.eventCallback("onComplete", () => {
          isWakingUpRef.current = false;

          if (autoStartIdle && elements.character && elements.shadow && controllerRef.current) {
            const idleResult = createIdleAnimation(
              {
                character: elements.character,
                shadow: elements.shadow,
                eyeLeft: elements.eyeLeft || undefined,
                eyeRight: elements.eyeRight || undefined,
                eyeLeftPath: elements.eyeLeftPath || undefined,
                eyeRightPath: elements.eyeRightPath || undefined,
                eyeLeftSvg: elements.eyeLeftSvg || undefined,
                eyeRightSvg: elements.eyeRightSvg || undefined,
              },
              {
                delay: 0,
                baseScale: activeScaleRef.current,
                sizeScale,
                enableFloat: enableFloatRef.current,
                enableBlinks: enableBlinksRef.current,
                eyeStyle: eyeStyleRef.current,
                floatAmplitude: floatAmplitudeRef.current,
                floatDuration: floatDurationRef.current,
                floatEase: floatEaseRef.current,
              }
            );

            const idleElements = [elements.character, elements.shadow].filter(Boolean) as Element[];
            controllerRef.current.startIdle(idleResult.timeline, idleElements, {
              pauseBlinks: idleResult.pauseBlinks,
              resumeBlinks: idleResult.resumeBlinks,
              killBlinks: idleResult.killBlinks,
            });

            onAnimationSequenceChange?.("CONTROLLER: Idle animation");
          }

          if (shadowTrackerRef.current) {
            shadowTrackerRef.current.start();
          }
        });

        wakeUpTl.play();
      } else {
        isWakingUpRef.current = false;
      }
    }

    if (!wasOff && isNowOff) {
      if (shadowTrackerRef.current) {
        shadowTrackerRef.current.stop();
      }

      if (glowSystemRef.current) {
        glowSystemRef.current.fadeOut(0.7);
        setTimeout(() => {
          glowSystemRef.current?.stop();
        }, 700);
      }

      onAnimationSequenceChange?.("CONTROLLER: Power-off (ON → OFF)");
      controllerRef.current.killAll();

      if (elements.character && elements.shadow) {
        const powerOffTl = createPowerOffAnimation(
          {
            character: elements.character,
            shadow: elements.shadow,
            innerGlow: elements.innerGlow || undefined,
            outerGlow: elements.outerGlow || undefined,
            eyeLeft: elements.eyeLeft || undefined,
            eyeRight: elements.eyeRight || undefined,
            eyeLeftPath: elements.eyeLeftPath || undefined,
            eyeRightPath: elements.eyeRightPath || undefined,
            eyeLeftSvg: elements.eyeLeftSvg || undefined,
            eyeRightSvg: elements.eyeRightSvg || undefined,
          },
          sizeScale,
          { activeScale: activeScaleRef.current, offScale: offScaleRef.current },
          eyeStyleRef.current
        );

        powerOffTl.play();
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isOff, elements.eyeRightPath, sizeScale, elements.outerGlow, elements.innerGlow, elements.eyeLeftPath, elements.eyeRight, onAnimationSequenceChange, elements.eyeRightSvg, elements.character, elements.shadow, elements.eyeLeftSvg, elements.eyeLeft, autoStartIdle]);

  useEffect(() => {
    if (!autoStartIdle || !isReady.current || !controllerRef.current) return;
    if (isOff) return;
    if (isWakingUpRef.current) return;
    if (controllerRef.current.isIdlePrevented()) return;

    // If an idle is already running, kill it before restarting with new flags
    if (controllerRef.current.isIdle()) {
      controllerRef.current.killAll();
      // Reset character to neutral so the new idle starts cleanly
      if (elements.character) {
        gsap.set(elements.character, { y: 0, rotation: 0, scale: activeScale });
      }
    }

    const idleResult = createIdleAnimation(
      {
        character: elements.character!,
        shadow: elements.shadow!,
        eyeLeft: elements.eyeLeft || undefined,
        eyeRight: elements.eyeRight || undefined,
        eyeLeftPath: elements.eyeLeftPath || undefined,
        eyeRightPath: elements.eyeRightPath || undefined,
        eyeLeftSvg: elements.eyeLeftSvg || undefined,
        eyeRightSvg: elements.eyeRightSvg || undefined,
      },
      {
        delay: 0,
        baseScale: activeScale,
        sizeScale,
        enableFloat,
        enableBlinks,
        eyeStyle,
        floatAmplitude,
        floatDuration,
        floatEase,
      }
    );

    const idleElements = Array.from(
      new Set([elements.character, elements.shadow].filter(Boolean))
    ) as Element[];

    controllerRef.current.startIdle(idleResult.timeline, idleElements, {
      pauseBlinks: idleResult.pauseBlinks,
      resumeBlinks: idleResult.resumeBlinks,
      killBlinks: idleResult.killBlinks,
    });
  }, [
    autoStartIdle,
    isOff,
    elements,
    enableFloat,
    enableBlinks,
    sizeScale,
    activeScale,
    eyeStyle,
    floatAmplitude,
    floatDuration,
    floatEase,
  ]);

  const playEmotion = useCallback(
    (emotion: EmotionType, animationOptions: AnimationOptions = {}): boolean => {
      if (!controllerRef.current) return false;
      if (!isReady.current) return false;

      if (!isEmotionType(emotion)) {
        if (ENABLE_ANIMATION_DEBUG_LOGS) {
          console.error(`[useAnimationController] Invalid emotion: ${emotion}`);
        }
        return false;
      }

      const emotionConfig = EMOTION_CONFIGS[emotion];
      if (!emotionConfig) return false;

      const eyeElements = [
        elements.eyeLeft,
        elements.eyeRight,
        elements.eyeLeftPath,
        elements.eyeRightPath,
        elements.eyeLeftSvg,
        elements.eyeRightSvg,
      ].filter(Boolean);
      if (eyeElements.length > 0) {
        gsap.killTweensOf(eyeElements);
      }

      gsap.set(elements.character!, {
        rotation: 0,
        rotationY: 0,
        rotationX: 0,
        transformPerspective: 0,
        x: 0,
        y: 0,
        scale: 1,
      });

      const tl = interpretEmotionConfig(
        emotionConfig,
        {
          character: elements.character!,
          eyeLeft: elements.eyeLeft,
          eyeRight: elements.eyeRight,
          eyeLeftPath: elements.eyeLeftPath,
          eyeRightPath: elements.eyeRightPath,
          eyeLeftSvg: elements.eyeLeftSvg,
          eyeRightSvg: elements.eyeRightSvg,
          innerGlow: elements.innerGlow,
          outerGlow: elements.outerGlow,
          leftBody: elements.leftBody,
          rightBody: elements.rightBody,
        },
        sizeScale,
        logoModeRef.current,
        eyeStyleRef.current
      );

      const emotionElements = Array.from(
        new Set([elements.character, elements.eyeLeft, elements.eyeRight].filter(Boolean))
      ) as Element[];

      const optionsWithFlags = {
        ...animationOptions,
        resetIdle: emotionConfig.resetIdle,
        preserveIdle: emotionConfig.preserveIdle,
      };

      return controllerRef.current.playEmotion(emotion, tl, emotionElements, optionsWithFlags);
    },
    [elements, sizeScale]
  );

  const transitionTo = useCallback(
    (state: AnimationState, animationOptions: AnimationOptions = {}): boolean => {
      if (!controllerRef.current) return false;
      if (!isReady.current) return false;

      const tl = gsap.timeline();
      const transitionElements = [elements.character, elements.container].filter(
        Boolean
      ) as Element[];

      return controllerRef.current.transitionTo(
        state,
        null,
        tl,
        transitionElements,
        animationOptions
      );
    },
    [elements]
  );

  const startIdle = useCallback(() => {
    if (!controllerRef.current || !isReady.current) return;

    const idleResult = createIdleAnimation(
      {
        character: elements.character!,
        shadow: elements.shadow!,
        eyeLeft: elements.eyeLeft || undefined,
        eyeRight: elements.eyeRight || undefined,
        eyeLeftPath: elements.eyeLeftPath || undefined,
        eyeRightPath: elements.eyeRightPath || undefined,
        eyeLeftSvg: elements.eyeLeftSvg || undefined,
        eyeRightSvg: elements.eyeRightSvg || undefined,
      },
      {
        delay: 0,
        baseScale: activeScaleRef.current,
        sizeScale,
        enableFloat: enableFloatRef.current,
        enableBlinks: enableBlinksRef.current,
        eyeStyle: eyeStyleRef.current,
        floatAmplitude: floatAmplitudeRef.current,
        floatDuration: floatDurationRef.current,
        floatEase: floatEaseRef.current,
      }
    );

    const idleElements = [elements.character, elements.shadow].filter(Boolean) as Element[];

    controllerRef.current.startIdle(idleResult.timeline, idleElements, {
      pauseBlinks: idleResult.pauseBlinks,
      resumeBlinks: idleResult.resumeBlinks,
      killBlinks: idleResult.killBlinks,
    });
  }, [elements, sizeScale]);

  const pause = useCallback(() => {
    controllerRef.current?.pauseIdle();
  }, []);

  const resume = useCallback(() => {
    controllerRef.current?.resumeIdle();
  }, []);

  const killAll = useCallback(() => {
    controllerRef.current?.killAll();
  }, []);

  const getState = useCallback((): AnimationState => {
    return controllerRef.current?.getCurrentState() ?? AnimationState.IDLE;
  }, []);

  const getEmotion = useCallback((): EmotionType | null => {
    return controllerRef.current?.getCurrentEmotion() ?? null;
  }, []);

  const isIdleActive = useCallback((): boolean => {
    return controllerRef.current?.isIdle() ?? false;
  }, []);

  const isIdlePlaying = useCallback((): boolean => {
    return controllerRef.current?.isIdlePlaying() ?? false;
  }, []);

  const getDebugInfo = useCallback(() => {
    if (!controllerRef.current) {
      throw new Error("Controller not initialized");
    }
    return controllerRef.current.getDebugInfo();
  }, []);

  return useMemo(
    () => ({
      playEmotion,
      transitionTo,
      startIdle,
      pause,
      resume,
      killAll,
      getState,
      getEmotion,
      isIdle: isIdleActive,
      isIdlePlaying,
      getDebugInfo,
      isReady: isReady.current,
    }),
    [
      playEmotion,
      transitionTo,
      startIdle,
      pause,
      resume,
      killAll,
      getState,
      getEmotion,
      isIdleActive,
      isIdlePlaying,
      getDebugInfo,
    ]
  );
}
