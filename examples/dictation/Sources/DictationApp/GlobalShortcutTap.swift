// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import ApplicationServices
import Carbon
import DictationCore

/// Keyboard callbacks run on their own run loop. Starting the microphone or doing
/// UI work must never block an event tap and trigger the system watchdog.
final class GlobalShortcutTap: @unchecked Sendable {
  private let lock = NSLock()
  private var tracker: ShortcutTracker
  private var physical = PhysicalModifierFlags()
  private var suspended = false
  private var tap: CFMachPort?
  private var source: CFRunLoopSource?
  private var loop: CFRunLoop?
  private var thread: Thread?
  private let ready = DispatchSemaphore(value: 0)
  private let action: @Sendable (ShortcutTracker.Action) -> Void
  init(shortcut: KeyboardShortcut, action: @escaping @Sendable (ShortcutTracker.Action) -> Void)
    throws
  {
    tracker = ShortcutTracker(shortcut: shortcut)
    self.action = action
    guard AXIsProcessTrusted() else {
      throw DictationError.invalid(
        "Shortcut not active: enable Antfly Dictation in Accessibility using Set up shortcut & paste access, then return to the app."
      )
    }
    let mask =
      (CGEventMask(1) << CGEventType.flagsChanged.rawValue)
      | (CGEventMask(1) << CGEventType.keyDown.rawValue)
      | (CGEventMask(1) << CGEventType.keyUp.rawValue)
    tap = CGEvent.tapCreate(
      tap: .cgSessionEventTap, place: .headInsertEventTap,
      options: .defaultTap, eventsOfInterest: mask,
      callback: { _, type, event, context in
        guard let context else { return Unmanaged.passUnretained(event) }
        return Unmanaged<GlobalShortcutTap>.fromOpaque(context).takeUnretainedValue().receive(
          type, event)
      }, userInfo: Unmanaged.passUnretained(self).toOpaque())
    guard let tap, let source = CFMachPortCreateRunLoopSource(kCFAllocatorDefault, tap, 0) else {
      if let tap { CFMachPortInvalidate(tap) }
      throw DictationError.invalid(
        "Cannot monitor the shortcut. Check Accessibility/Input Monitoring permissions, then save settings again."
      )
    }
    self.source = source
    let worker = Thread { [self] in
      let current = CFRunLoopGetCurrent()!
      lock.lock()
      loop = current
      lock.unlock()
      guard let source = self.source, let tap = self.tap else {
        ready.signal()
        return
      }
      CFRunLoopAddSource(current, source, .commonModes)
      CGEvent.tapEnable(tap: tap, enable: true)
      ready.signal()
      CFRunLoopRun()
      CFRunLoopRemoveSource(current, source, .commonModes)
    }
    worker.name = "Antfly dictation shortcut"
    thread = worker
    worker.start()
    ready.wait()
  }
  var isAvailable: Bool {
    guard AXIsProcessTrusted(), let tap else { return false }
    return CGEvent.tapIsEnabled(tap: tap)
  }
  func setSuspended(_ value: Bool) {
    lock.lock()
    suspended = value
    tracker.reset()
    physical.reset()
    lock.unlock()
  }
  func stop() {
    lock.lock()
    suspended = true
    let current = loop
    thread = nil
    tracker.reset()
    physical.reset()
    lock.unlock()
    if let tap {
      CGEvent.tapEnable(tap: tap, enable: false)
      CFMachPortInvalidate(tap)
    }
    if let current {
      CFRunLoopStop(current)
      CFRunLoopWakeUp(current)
    }
  }
  private func receive(_ type: CGEventType, _ event: CGEvent) -> Unmanaged<CGEvent>? {
    if type == .tapDisabledByTimeout || type == .tapDisabledByUserInput {
      lock.lock()
      tracker.reset()
      physical.reset()
      let paused = suspended
      lock.unlock()
      if let tap { CGEvent.tapEnable(tap: tap, enable: true) }
      if !paused { action(.interrupt) }
      return Unmanaged.passUnretained(event)
    }
    let kind: ShortcutTracker.Kind =
      type == .flagsChanged ? .flags : type == .keyDown ? .keyDown : .keyUp
    lock.lock()
    if suspended {
      lock.unlock()
      return Unmanaged.passUnretained(event)
    }
    let key = UInt32(event.getIntegerValueField(.keyboardEventKeycode))
    let flags = physical.normalize(
      kind, key: key, raw: UInt32(truncatingIfNeeded: event.flags.rawValue))
    let result = tracker.event(kind, key: key, flags: flags)
    lock.unlock()
    if result.action != .none { action(result.action) }
    return result.consume ? nil : Unmanaged.passUnretained(event)
  }
}

// Native handles have no Swift actor-bound callbacks during their destruction.
// Ownership removes registrations and stops the dedicated tap run loop together.
final class HotkeyResources: @unchecked Sendable {
  var reference: EventHotKeyRef?
  var handler: EventHandlerRef?
  var tap: GlobalShortcutTap?
  deinit {
    tap?.stop()
    if let reference { UnregisterEventHotKey(reference) }
    if let handler { RemoveEventHandler(handler) }
  }
}
