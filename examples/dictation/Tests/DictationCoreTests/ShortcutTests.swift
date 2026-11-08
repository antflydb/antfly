// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import Foundation
import Testing

@testable import DictationApp
@testable import DictationCore

@Test func newShortcutDefaultsToFnAndLegacySettingsPreserveTheirBinding() throws {
  let config = Configuration()
  #expect(config.effectiveShortcut == .fn)
  #expect(config.effectiveShortcut.label == "Fn")
  var legacy =
    try JSONSerialization.jsonObject(with: JSONEncoder().encode(config)) as! [String: Any]
  legacy.removeValue(forKey: "shortcut")
  let loaded = try Configuration.decode(JSONSerialization.data(withJSONObject: legacy))
  #expect(loaded.effectiveShortcut == .controlOptionSpace)
  #expect(loaded.effectiveShortcut.carbonModifiers == 6144)
  #expect(try Configuration.decode(JSONEncoder().encode(config)) == config)
  #expect(try KeyboardShortcut.controlOption.validated().usesEventTap)
}
@Test func shortcutValidationRejectsBareKeysAndCommonSystemCommands() {
  #expect(throws: DictationError.self) {
    try KeyboardShortcut(keyCode: 0, modifiers: 0).validated()
  }
  #expect(throws: DictationError.self) {
    try KeyboardShortcut(keyCode: 8, modifiers: KeyboardShortcut.command).validated()
  }
  #expect(throws: DictationError.self) {
    try KeyboardShortcut(keyCode: 63, modifiers: KeyboardShortcut.function).validated()
  }
  #expect(throws: DictationError.self) { try KeyboardShortcut(modifiers: UInt32.max).validated() }
}
@Test func fnHoldReleaseIgnoresRepeatAndUnrelatedFlags() {
  var tracker = ShortcutTracker(shortcut: .fn)
  #expect(tracker.event(.flags, key: 63, flags: KeyboardShortcut.function).action == .press)
  #expect(
    tracker.event(.flags, key: 63, flags: KeyboardShortcut.function | (1 << 16)).action == .none)
  let released = tracker.event(.flags, key: 63, flags: 0)
  #expect(released.action == .release && released.consume)
  #expect(tracker.event(.flags, key: 63, flags: 0).action == .none)
}
@Test func modifierOnlyShortcutYieldsToNavigationAndRearmsAfterRelease() {
  var tracker = ShortcutTracker(shortcut: .fn)
  _ = tracker.event(.flags, key: 63, flags: KeyboardShortcut.function)
  let navigation = tracker.event(.keyDown, key: 123, flags: KeyboardShortcut.function)
  #expect(navigation.action == .interrupt && !navigation.consume)
  #expect(tracker.event(.flags, key: 63, flags: 0).action == .none)
  #expect(tracker.event(.flags, key: 63, flags: KeyboardShortcut.function).action == .press)
  tracker.reset()
  #expect(tracker.event(.flags, key: 63, flags: 0).action == .none)
}
@Test func modifierCombinationsWaitForAllRequiredKeys() {
  var tracker = ShortcutTracker(shortcut: .controlOption)
  #expect(tracker.event(.flags, key: 59, flags: KeyboardShortcut.control).action == .none)
  #expect(
    tracker.event(.flags, key: 58, flags: KeyboardShortcut.control | KeyboardShortcut.option).action
      == .press)
  #expect(tracker.event(.flags, key: 58, flags: KeyboardShortcut.control).action == .release)
  #expect(tracker.event(.flags, key: 59, flags: 0).action == .none)
}
@Test func fnKeyChordSuppressesItsKeyAndHandlesEarlyModifierRelease() {
  var tracker = ShortcutTracker(
    shortcut: KeyboardShortcut(keyCode: 49, modifiers: KeyboardShortcut.function))
  #expect(tracker.event(.flags, key: 63, flags: KeyboardShortcut.function).action == .none)
  #expect(tracker.event(.keyDown, key: 49, flags: KeyboardShortcut.function).action == .press)
  #expect(tracker.event(.keyDown, key: 49, flags: KeyboardShortcut.function).action == .none)
  #expect(tracker.event(.flags, key: 63, flags: 0).action == .release)
  let up = tracker.event(.keyUp, key: 49, flags: 0)
  #expect(up.consume && up.action == .none)
}
@Test @MainActor func shortcutRecorderCapturesModifierUnionAndPausesDictation() {
  let recorder = ShortcutRecorder(monitorEvents: false)
  var paused = false
  var captured: KeyboardShortcut?
  recorder.captureChanged = { paused = $0 }
  recorder.completed = { captured = $0 }
  recorder.start()
  #expect(paused)
  recorder.receiveFlags(KeyboardShortcut.control)
  recorder.receiveFlags(KeyboardShortcut.control | KeyboardShortcut.option)
  recorder.receiveFlags(KeyboardShortcut.option)
  recorder.receiveFlags(0)
  #expect(!paused && !recorder.recording)
  #expect(captured == .controlOption)
}
@Test @MainActor func shortcutRecordingConsumesKeysAndEscapeKeepsThePreviousChoice() {
  let recorder = ShortcutRecorder(monitorEvents: false)
  var captured: KeyboardShortcut?
  recorder.completed = { captured = $0 }
  recorder.start()
  let down = NSEvent.keyEvent(
    with: .keyDown, location: .zero, modifierFlags: [.control, .option],
    timestamp: 0, windowNumber: 0, context: nil, characters: "d", charactersIgnoringModifiers: "d",
    isARepeat: false, keyCode: 2)!
  let up = NSEvent.keyEvent(
    with: .keyUp, location: .zero, modifierFlags: [.control, .option],
    timestamp: 0, windowNumber: 0, context: nil, characters: "d", charactersIgnoringModifiers: "d",
    isARepeat: false, keyCode: 2)!
  #expect(recorder.receive(down) == nil)
  #expect(recorder.receive(up) == nil)
  #expect(captured?.keyCode == 2 && captured?.label == "Control + Option + D")
  recorder.start()
  let escape = NSEvent.keyEvent(
    with: .keyDown, location: .zero, modifierFlags: [],
    timestamp: 0, windowNumber: 0, context: nil, characters: "", charactersIgnoringModifiers: "",
    isARepeat: false, keyCode: 53)!
  #expect(recorder.receive(escape) == nil)
  #expect(!recorder.recording && captured?.keyCode == 2)
}

@Test func functionAndArrowKeysDoNotMasqueradeAsThePhysicalFnModifier() {
  var physical = PhysicalModifierFlags()
  #expect(physical.normalize(.keyDown, key: 122, raw: KeyboardShortcut.function) == 0)
  #expect(
    physical.normalize(.flags, key: 56, raw: KeyboardShortcut.function | KeyboardShortcut.shift)
      == KeyboardShortcut.shift)
  #expect(
    physical.normalize(.flags, key: 63, raw: KeyboardShortcut.function) == KeyboardShortcut.function
  )
  #expect(
    physical.normalize(.keyDown, key: 123, raw: KeyboardShortcut.function)
      == KeyboardShortcut.function)
  #expect(physical.normalize(.flags, key: 63, raw: 0) == 0)
}
