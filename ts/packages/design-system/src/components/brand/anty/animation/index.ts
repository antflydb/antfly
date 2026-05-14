/**
 * Animation System for the Anty brand character.
 */

export * from "./constants";

export { AnimationController } from "./controller";
export { interpretEmotionConfig } from "./definitions/emotion-interpreter";
export { EMOTION_CONFIGS, GLOW_CONSTANTS, getEmotionConfig } from "./definitions/emotions";
export {
  createEyeAnimation,
  createLookAnimation,
  createReturnFromLookAnimation,
} from "./definitions/eye-animations";
export {
  EYE_DIMENSIONS,
  EYE_SHAPES,
  type EyeShapeName,
  getEyeDimensions,
  getEyeShape,
} from "./definitions/eye-shapes";
export { createIdleAnimation } from "./definitions/idle";
export { createPowerOffAnimation, createWakeUpAnimation } from "./definitions/transitions";
export { ENABLE_ANIMATION_DEBUG_LOGS, logAnimationEvent } from "./feature-flags";
export { createGlowSystem, type GlowSystemControls } from "./glow-system";
export {
  initializeCharacter,
  resetEyesToIdle,
  resetEyesToLogo,
  resetEyesToOriginal,
} from "./initialize";
export { createShadowTracker, type ShadowTrackerControls } from "./shadow";
export { StateMachine } from "./state-machine";
export {
  type AnimationCallbacks,
  type AnimationConfig,
  type AnimationContext,
  type AnimationEvent,
  type AnimationEventCallback,
  type AnimationMetrics,
  type AnimationOptions,
  AnimationState,
  type AnimationSystemConfig,
  type BodyConfig,
  type CharacterPhase,
  type ControllerConfig,
  type EasingFunction,
  type ElementOwnership,
  type EmotionConfig,
  type EmotionOptions,
  type EmotionType,
  type ExpressionName,
  type EyeConfig,
  type EyePhase,
  type EyeStyle,
  type GestureConfig,
  type GlowConfig,
  isAnimationState,
  isEmotionType,
  type QueuedAnimation,
  type SpringConfig,
  type StateTransition,
  type TimelineConfig,
  type TimelineRef,
  type TransitionOptions,
  type UseAnimationReturn,
} from "./types";
