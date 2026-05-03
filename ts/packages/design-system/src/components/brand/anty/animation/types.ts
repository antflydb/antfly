/**
 * Animation System Types for the Anty brand character.
 */

import type gsap from "gsap";
import type { EyeShapeName } from "./definitions/eye-shapes";

export enum AnimationState {
  IDLE = "IDLE",
  EMOTION = "EMOTION",
  TRANSITION = "TRANSITION",
  INTERACTION = "INTERACTION",
  OFF = "OFF",
}

export type EmotionType =
  | "shocked"
  | "excited"
  | "back-forth"
  | "look-around"
  | "wink"
  | "nod"
  | "headshake"
  | "look-left"
  | "look-right";

export type ExpressionName = EmotionType | "idle" | "off";

export type EasingFunction =
  | "linear"
  | "ease"
  | "ease-in"
  | "ease-out"
  | "ease-in-out"
  | "cubic-bezier"
  | "spring"
  | "bounce"
  | "power1.in"
  | "power1.out"
  | "power1.inOut"
  | "power2.in"
  | "power2.out"
  | "power2.inOut"
  | "power3.in"
  | "power3.out"
  | "power3.inOut"
  | "power4.in"
  | "power4.out"
  | "power4.inOut"
  | "back.in"
  | "back.out"
  | "back.inOut"
  | "elastic.in"
  | "elastic.out"
  | "elastic.inOut";

export interface EmotionOptions {
  interrupt?: boolean;
  duration?: number;
  repeat?: number;
  delay?: number;
  onStart?: () => void;
  onComplete?: () => void;
  easing?: EasingFunction;
  priority?: number;
  force?: boolean;
  metadata?: Record<string, unknown>;
}

export interface TransitionOptions {
  duration?: number;
  easing?: EasingFunction;
  onStart?: () => void;
  onComplete?: () => void;
  immediate?: boolean;
  priority?: number;
}

export interface AnimationKeyframe {
  time: number | string;
  properties: Record<string, string | number>;
  easing?: EasingFunction;
}

export interface AnimationConfig {
  target: string | SVGElement | HTMLElement;
  keyframes: AnimationKeyframe[];
  duration: number;
  easing?: EasingFunction;
  iterations?: number;
  delay?: number;
  fill?: "forwards" | "backwards" | "both" | "none";
  id?: string;
}

export interface TimelineConfig {
  id: string;
  animations: AnimationConfig[];
  mode?: "parallel" | "sequence";
  onStart?: () => void;
  onComplete?: () => void;
  onUpdate?: (progress: number) => void;
  defaultEasing?: EasingFunction;
  repeat?: number;
  delay?: number;
}

export interface AnimationMetrics {
  fps: number;
  frameTime: number;
  droppedFrames: number;
  state: AnimationState;
  activeEmotion: EmotionType | null;
  totalAnimations: number;
  lastUpdate: number;
  activeTimelines: number;
  queueSize: number;
  memory?: {
    used: number;
    total: number;
  };
}

export interface AnimationContext {
  state: AnimationState;
  previousState: AnimationState | null;
  currentEmotion: EmotionType | null;
  queue: Array<{
    id: string;
    emotion: EmotionType;
    options: EmotionOptions;
    queuedAt: number;
  }>;
  isPaused: boolean;
  metrics: AnimationMetrics;
  activeTimelines: Map<string, gsap.core.Timeline>;
}

export interface UseAnimationReturn {
  playEmotion: (emotion: EmotionType, options?: EmotionOptions) => Promise<void>;
  transitionTo: (state: AnimationState, options?: TransitionOptions) => Promise<void>;
  getContext: () => AnimationContext;
  pause: () => void;
  resume: () => void;
  clearQueue: () => void;
  isReady: boolean;
  currentState: AnimationState;
  currentEmotion: EmotionType | null;
  killAll: () => void;
}

export interface AnimationSystemConfig {
  debug?: boolean;
  enableMetrics?: boolean;
  defaultEmotionDuration?: number;
  defaultTransitionDuration?: number;
  maxQueueSize?: number;
  autoIdle?: boolean;
  idleInterval?: number;
  reducedMotion?: boolean;
  defaultPriority?: number;
  enableQueue?: boolean;
}

export interface AnimationEvent {
  type:
    | "stateChange"
    | "emotionStart"
    | "emotionEnd"
    | "transitionStart"
    | "transitionEnd"
    | "error"
    | "queueUpdate"
    | "pause"
    | "resume"
    | "metricsUpdate";
  timestamp: number;
  payload: {
    state?: AnimationState;
    previousState?: AnimationState;
    emotion?: EmotionType;
    error?: Error;
    queueSize?: number;
    metrics?: AnimationMetrics;
    [key: string]: unknown;
  };
}

export type AnimationEventCallback = (event: AnimationEvent) => void;

export interface SpringConfig {
  mass?: number;
  tension?: number;
  friction?: number;
  velocity?: number;
  precision?: number;
}

export interface GestureConfig {
  enableDrag?: boolean;
  enableHover?: boolean;
  enableClick?: boolean;
  dragBounds?: {
    left?: number;
    right?: number;
    top?: number;
    bottom?: number;
  };
  onGestureStart?: (type: "drag" | "hover" | "click") => void;
  onGestureEnd?: (type: "drag" | "hover" | "click") => void;
}

export interface StateTransition {
  from: AnimationState;
  to: AnimationState;
  allowed: boolean;
  priority: number;
}

export interface TimelineRef {
  timeline: gsap.core.Timeline;
  element: Element | string;
  state: AnimationState;
  startedAt: number;
  priority: number;
  id: string;
  emotion?: EmotionType;
}

export interface QueuedAnimation {
  id: string;
  state: AnimationState;
  emotion?: EmotionType;
  callback: () => void | Promise<void>;
  priority: number;
  queuedAt: number;
  options?: EmotionOptions | TransitionOptions;
}

export interface ElementOwnership {
  element: Element | string;
  owner: string;
  timeline: gsap.core.Timeline;
  acquiredAt: number;
  priority: number;
}

export interface AnimationCallbacks {
  onStart?: (state: AnimationState, emotion?: EmotionType) => void;
  onComplete?: (state: AnimationState, emotion?: EmotionType) => void;
  onInterrupt?: (state: AnimationState, emotion?: EmotionType) => void;
  onStateChange?: (from: AnimationState, to: AnimationState) => void;
  onQueueAdd?: (animation: QueuedAnimation) => void;
  onQueueProcess?: (animation: QueuedAnimation) => void;
  onError?: (error: Error) => void;
  onEmotionMotionStart?: (emotion: EmotionType, timelineId: string) => void;
  onEmotionMotionComplete?: (emotion: EmotionType, timelineId: string, duration: number) => void;
}

export interface ControllerConfig {
  enableLogging?: boolean;
  enableQueue?: boolean;
  maxQueueSize?: number;
  defaultPriority?: number;
  callbacks?: AnimationCallbacks;
  system?: AnimationSystemConfig;
}

export interface AnimationOptions {
  priority?: number;
  force?: boolean;
  onComplete?: () => void;
  onStart?: () => void;
  duration?: number;
  easing?: EasingFunction;
  delay?: number;
  resetIdle?: boolean;
  preserveIdle?: boolean;
}

const EMOTION_NAMES: readonly EmotionType[] = [
  "shocked",
  "excited",
  "back-forth",
  "look-around",
  "wink",
  "nod",
  "headshake",
  "look-left",
  "look-right",
];

export function isEmotionType(value: string): value is EmotionType {
  return (EMOTION_NAMES as readonly string[]).includes(value);
}

export function isAnimationState(value: string): value is AnimationState {
  return Object.values(AnimationState).includes(value as AnimationState);
}

// ============================================================================
// Emotion Configuration Types (declarative system)
// ============================================================================

export interface EyeConfig {
  shape: EyeShapeName | { left: EyeShapeName; right: EyeShapeName };
  duration: number;
  delay?: number;
  yOffset?: number | { left: number; right: number };
  xOffset?: number | { left: number; right: number };
  scale?: number;
  bunch?: number;
  leftRotation?: number;
  rightRotation?: number;
  returnPosition?: number | string;
  returnDuration?: number;
}

export interface CharacterPhase {
  props: {
    x?: number;
    y?: number;
    scale?: number;
    rotation?: number;
    rotationY?: number;
    rotationX?: number;
    transformPerspective?: number;
  };
  duration: number;
  ease: string;
  position?: string | number;
}

export interface GlowConfig {
  follow: boolean;
  distanceRatio?: number;
  lag?: number;
}

export interface BodyConfig {
  leftX?: number;
  leftY?: number;
  rightX?: number;
  rightY?: number;
  duration?: number;
  ease?: string;
  returnPosition?: number | string;
  returnDuration?: number;
  returnEase?: string;
}

export interface EyePhase {
  position: number;
  shape: EyeShapeName | { left: EyeShapeName; right: EyeShapeName };
  duration: number;
  xOffset?: number;
  bunch?: number;
}

export interface EmotionConfig {
  id: string;
  eyes?: EyeConfig;
  eyePhases?: EyePhase[];
  character: CharacterPhase[];
  glow?: GlowConfig;
  body?: BodyConfig;
  totalDuration: number;
  holdDuration?: number;
  resetRotation?: boolean;
  resetRotationY?: boolean;
  eyeResetDuration?: number;
  resetIdle?: boolean;
  preserveIdle?: boolean;
}

export type EyeStyle = "alive" | "original";

// Re-export EyeShapeName for convenience
export type { EyeShapeName };
