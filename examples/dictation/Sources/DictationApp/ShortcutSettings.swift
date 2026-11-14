// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import DictationCore
import SwiftUI

typealias DictationShortcut = DictationCore.KeyboardShortcut

@MainActor final class ShortcutRecorder: ObservableObject {
  @Published var recording = false
  @Published var candidate: DictationShortcut?
  @Published var error = ""
  var completed: ((DictationShortcut) -> Void)?
  var captureChanged: ((Bool) -> Void)?
  private var physical = PhysicalModifierFlags()
  private let monitorEvents: Bool
  init(monitorEvents: Bool = true) { self.monitorEvents = monitorEvents }
  private var monitor: Any?
  private var activation: NSObjectProtocol?
  func start() {
    stop()
    candidate = nil
    physical.reset()
    error = ""
    recording = true
    captureChanged?(true)
    guard monitorEvents else { return }
    monitor = NSEvent.addLocalMonitorForEvents(matching: [.flagsChanged, .keyDown, .keyUp]) {
      [weak self] event in
      let consume = MainActor.assumeIsolated {
        guard let self else { return false }
        return self.receive(event) == nil
      }
      return consume ? nil : event
    }
    activation = NotificationCenter.default.addObserver(
      forName: NSApplication.didResignActiveNotification,
      object: nil, queue: .main
    ) { [weak self] _ in MainActor.assumeIsolated { self?.stop() } }
  }
  func stop() {
    if let monitor {
      NSEvent.removeMonitor(monitor)
      self.monitor = nil
    }
    if let activation {
      NotificationCenter.default.removeObserver(activation)
      self.activation = nil
    }
    if recording {
      recording = false
      captureChanged?(false)
    }
  }
  func finish() {
    guard let candidate else { return }
    do {
      let chosen = try candidate.validated()
      stop()
      completed?(chosen)
    } catch {
      stop()
      self.error = error.localizedDescription
    }
  }
  func receiveFlags(_ flags: UInt32) {
    guard recording else { return }
    let flags = flags & DictationShortcut.allowed
    if flags != 0 {
      if candidate?.keyCode == nil {
        candidate = DictationShortcut(modifiers: (candidate?.modifiers ?? 0) | flags)
      }
    } else if candidate?.keyCode == nil {
      finish()
    }
  }
  func receive(_ event: NSEvent) -> NSEvent? {
    guard recording else { return event }
    let kind: ShortcutTracker.Kind =
      event.type == .flagsChanged ? .flags : event.type == .keyDown ? .keyDown : .keyUp
    let flags = physical.normalize(
      kind, key: UInt32(event.keyCode),
      raw: UInt32(truncatingIfNeeded: event.modifierFlags.rawValue))
    switch event.type {
    case .flagsChanged:
      receiveFlags(flags)
    case .keyDown:
      if event.keyCode == 53 {
        stop()
        return nil
      }  // Escape cancels selection.
      if !event.isARepeat {
        let code = UInt32(event.keyCode)
        let special = [
          36, 48, 49, 51, 53, 96, 97, 98, 99, 100, 101, 103, 109, 111, 118, 120, 122, 123, 124, 125,
          126,
        ].contains(code)
        let label =
          special
          ? DictationShortcut.keyName(code) : event.charactersIgnoringModifiers?.uppercased()
        candidate = DictationShortcut(keyCode: code, modifiers: flags, keyLabel: label)
      }
    case .keyUp:
      if candidate?.keyCode == UInt32(event.keyCode) { finish() }
    default: break
    }
    return nil
  }
}

struct ShortcutSettings: View {
  @ObservedObject var store: Store
  @StateObject private var recorder = ShortcutRecorder()
  private let modifiers: [(String, UInt32)] = [
    ("Fn", DictationShortcut.function), ("Control", DictationShortcut.control),
    ("Option", DictationShortcut.option), ("Shift", DictationShortcut.shift),
    ("Command", DictationShortcut.command),
  ]
  var body: some View {
    VStack(alignment: .leading, spacing: 10) {
      HStack {
        Text("Dictation shortcut").font(.headline)
        Spacer()
        Text(
          recorder.recording
            ? recorder.candidate?.label ?? "Press your shortcut…"
            : store.config.effectiveShortcut.label
        )
        .font(.system(.body, design: .monospaced)).padding(.horizontal, 10).padding(.vertical, 5)
        .background(.quaternary, in: RoundedRectangle(cornerRadius: 6))
      }
      HStack {
        Button(
          store.shortcutSetupPending
            ? "Open Accessibility settings…" : "Set up shortcut & paste access"
        ) {
          store.setUpShortcut()
        }.buttonStyle(.borderedProminent).disabled(recorder.recording)
        if store.shortcutSetupPending { Button("Cancel setup") { store.cancelShortcutSetup() } }
        Button("Keyboard settings…") { store.openKeyboardSettings() }
      }
      if !store.shortcutSetupStatus.isEmpty { Text(store.shortcutSetupStatus).font(.caption) }
      if !store.shortcutStatus.isEmpty {
        Text(store.shortcutStatus).font(.caption).textSelection(.enabled)
      }
      Text(
        "Setup saves and activates your selected shortcut. Other preferences save automatically."
      ).font(
        .caption
      ).foregroundStyle(.secondary)
      HStack {
        ForEach(modifiers, id: \.1) { item in
          Toggle(
            item.0,
            isOn: Binding(
              get: {
                store.config.effectiveShortcut.modifiers & item.1 != 0
              },
              set: { enabled in
                var shortcut = store.config.effectiveShortcut
                if enabled { shortcut.modifiers |= item.1 } else { shortcut.modifiers &= ~item.1 }
                store.config.effectiveShortcut = shortcut
              })
          ).toggleStyle(.button)
        }
        if store.config.effectiveShortcut.keyCode != nil {
          Button("Modifiers only") {
            var shortcut = store.config.effectiveShortcut
            shortcut.keyCode = nil
            shortcut.keyLabel = nil
            store.config.effectiveShortcut = shortcut
          }
        }
      }.disabled(recorder.recording)
      HStack {
        Button(recorder.recording ? "Cancel selection" : "Record shortcut…") {
          if recorder.recording { recorder.stop() } else { recorder.start() }
        }
        if recorder.recording {
          Button("Use this shortcut") { recorder.finish() }.disabled(recorder.candidate == nil)
        } else {
          Menu("Presets") {
            Button("Fn (default)") { store.config.effectiveShortcut = .fn }
            Button("Control + Option (no Fn key)") {
              store.config.effectiveShortcut = .controlOption
            }
            Button("Control + Option + Space") {
              store.config.effectiveShortcut = .controlOptionSpace
            }
          }
          Button("Reset to Fn") { store.config.effectiveShortcut = .fn }
        }
      }
      if recorder.recording {
        Text(
          "Hold your modifier keys, optionally press another key, then release. Escape cancels. Dictation is paused during selection."
        ).font(.caption)
      } else {
        Text(
          "Hold Fn/Globe to dictate, or choose Control + Option for keyboards without Fn. Shortcut changes save automatically when selection is complete. Existing shortcuts are preserved until you change them."
        ).font(.caption).foregroundStyle(.secondary)
      }
      if let issue = validationError { Text(issue).font(.caption).foregroundStyle(.red) }
      if !recorder.error.isEmpty { Text(recorder.error).font(.caption).foregroundStyle(.red) }
      Text(
        "Modifier-only and Fn shortcuts need Accessibility access. If Globe also opens the emoji picker, set “Press Globe key to” to “Do Nothing” in macOS Keyboard settings."
      ).font(.caption).foregroundStyle(.secondary)
    }
    .onAppear {
      recorder.completed = { store.config.effectiveShortcut = $0 }
      recorder.captureChanged = { store.captureShortcut($0) }
      store.refreshShortcutAccess()
    }
    .onChange(of: store.config.effectiveShortcut) { _, _ in
      if store.shortcutSetupPending { store.cancelShortcutSetup() }
    }
    .onDisappear { recorder.stop() }
    .onReceive(NotificationCenter.default.publisher(for: NSApplication.didBecomeActiveNotification))
    { _ in
      store.refreshShortcutAccess()
    }
  }
  private var validationError: String? {
    do {
      _ = try store.config.effectiveShortcut.validated()
      return nil
    } catch { return error.localizedDescription }
  }
}
