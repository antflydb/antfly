// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import ApplicationServices
import Foundation

/// Follow only the focused chain/advertised editable ancestor, never scan a
/// window for a convenient field. Chromium can focus a rich editor descendant.
struct FocusedTextResolver<Node> {
  struct Traits {
    var editable: Bool
    var secure = false
    var enabled = true
  }
  let traits: (Node) -> Traits
  let next: (Node) -> Node?
  let equal: (Node, Node) -> Bool

  func resolve(original: Node, current: Node) -> Result<Node, TextCaptureFailure> {
    let originalEditor = resolve(original)
    if equal(original, current) { return originalEditor }
    switch (originalEditor, resolve(current)) {
    case (.success(let original), .success(let current)):
      return equal(original, current) ? .success(original) : .failure(.focusChanged)
    case (.failure(let failure), _): return .failure(failure)
    case (_, .failure(let failure)): return .failure(failure)
    }
  }

  func resolve(_ focused: Node) -> Result<Node, TextCaptureFailure> {
    var current: Node? = focused
    var visited: [Node] = []
    while let node = current, visited.count < 8 {
      guard !visited.contains(where: { equal($0, node) }) else { break }
      visited.append(node)
      let info = traits(node)
      if info.secure { return .failure(.secure) }
      if !info.enabled { return .failure(.disabled) }
      if info.editable { return .success(node) }
      current = next(node)
    }
    return .failure(.unsupported)
  }
}

enum TextCaptureFailure: Error, LocalizedError {
  case permission, unsupported, secure, disabled, focusChanged
  var errorDescription: String? {
    switch self {
    case .permission: return "Enable Accessibility for Antfly Dictation to paste into other apps."
    case .secure:
      return "Dictation cannot paste into a secure field. Your transcript is ready to copy."
    case .disabled: return "The focused field is disabled. Your transcript is ready to copy."
    case .focusChanged:
      return "Target focus changed while capturing the editor. Your transcript is ready to copy."
    case .unsupported:
      return
        "The app did not expose a supported editable field. Click inside the editor and try again. Your transcript is ready to copy."
    }
  }
}

struct CapturedTextElement: @unchecked Sendable {
  var element: AXUIElement?
  var failure: TextCaptureFailure?
}

/// Bounded IPC: a short focus snapshot at the shortcut, then background resolution.
final class NativeFocusReader {
  let application: AXUIElement
  let deadline: ContinuousClock.Instant
  private var queryFailed = false
  private let messagingTimeout: Float
  init(application: AXUIElement, budget: Duration, messagingTimeout: Float = 0.08) {
    self.application = application
    deadline = .now.advanced(by: budget)
    self.messagingTimeout = messagingTimeout
    AXUIElementSetMessagingTimeout(application, messagingTimeout)
  }
  func attribute(_ name: String, of node: AXUIElement) -> CFTypeRef? {
    guard ContinuousClock.now < deadline else {
      queryFailed = true
      return nil
    }
    AXUIElementSetMessagingTimeout(node, messagingTimeout)
    var value: CFTypeRef?
    let status = AXUIElementCopyAttributeValue(node, name as CFString, &value)
    guard status == .success else {
      if status != .attributeUnsupported && status != .noValue { queryFailed = true }
      return nil
    }
    return value
  }
  func element(_ name: String, of node: AXUIElement) -> AXUIElement? {
    guard let value = attribute(name, of: node), CFGetTypeID(value) == AXUIElementGetTypeID() else {
      return nil
    }
    return unsafeDowncast(value, to: AXUIElement.self)
  }
  func focusedText(anchoredTo original: AXUIElement? = nil) -> Result<
    AXUIElement, TextCaptureFailure
  > {
    queryFailed = false
    guard let focused = element(kAXFocusedUIElementAttribute, of: application) else {
      return .failure(.unsupported)
    }
    let resolver = FocusedTextResolver<AXUIElement>(
      traits: { node in
        let role = self.attribute(kAXRoleAttribute, of: node) as? String
        let subrole = self.attribute(kAXSubroleAttribute, of: node) as? String
        let secure = subrole == kAXSecureTextFieldSubrole || role == kAXSecureTextFieldSubrole
        let enabled = self.attribute(kAXEnabledAttribute, of: node) as? Bool != false
        let editable =
          [kAXTextFieldRole, kAXTextAreaRole, kAXComboBoxRole].contains(role ?? "")
          || self.attribute("AXEditable", of: node) as? Bool == true
        return .init(editable: editable, secure: secure, enabled: enabled)
      },
      next: { node in
        self.element("AXEditableAncestor", of: node)
          ?? self.element("AXHighestEditableAncestor", of: node)
      }, equal: { CFEqual($0, $1) }
    )
    let result =
      original.map { resolver.resolve(original: $0, current: focused) } ?? resolver.resolve(focused)
    return queryFailed ? .failure(.unsupported) : result
  }
}

struct NativeFocusAnchor: @unchecked Sendable {
  let application: AXUIElement
  let focused: AXUIElement
  let window: AXUIElement?
}

/// Establish identity at shortcut time. Never let a delayed task choose a new
/// editor. Failed/slow snapshots fail closed; retries only resolve this anchor.
func snapshotTextFocus(pid: pid_t) -> NativeFocusAnchor? {
  guard AXIsProcessTrusted() else { return nil }
  let application = AXUIElementCreateApplication(pid)
  let reader = NativeFocusReader(
    application: application, budget: .milliseconds(80), messagingTimeout: 0.04)
  // This documented role query activates Chromium's on-demand accessibility.
  _ = reader.attribute(kAXRoleAttribute, of: application)
  guard let focused = reader.element(kAXFocusedUIElementAttribute, of: application) else {
    return nil
  }
  return NativeFocusAnchor(
    application: application, focused: focused,
    window: reader.element(kAXFocusedWindowAttribute, of: application))
}

func captureTextElement(pid: pid_t, anchor: NativeFocusAnchor) async -> CapturedTextElement {
  guard AXIsProcessTrusted() else { return .init(failure: .permission) }
  let reader = NativeFocusReader(application: anchor.application, budget: .seconds(1))
  while ContinuousClock.now < reader.deadline, !Task.isCancelled {
    guard
      await MainActor.run(body: {
        NSWorkspace.shared.frontmostApplication?.processIdentifier == pid
      })
    else { return .init(failure: .focusChanged) }
    if let originalWindow = anchor.window {
      guard let window = reader.element(kAXFocusedWindowAttribute, of: anchor.application),
        CFEqual(originalWindow, window)
      else { return .init(failure: .focusChanged) }
    }
    switch reader.focusedText(anchoredTo: anchor.focused) {
    case .success(let element): return .init(element: element)
    case .failure(.unsupported): break
    case .failure(let failure): return .init(failure: failure)
    }
    try? await Task.sleep(for: .milliseconds(40))
  }
  return .init(failure: .unsupported)
}

/// Prime Chromium's lazy tree before a shortcut, without reading editor contents.
/// Observer and work live only while the global shortcut is registered.
@MainActor final class BrowserAccessibilityWarmup {
  private let resources = BrowserWarmupResources()
  private var work: Task<Void, Never>?
  func start() {
    guard resources.observer == nil else { return }
    resources.observer = NSWorkspace.shared.notificationCenter.addObserver(
      forName: NSWorkspace.didActivateApplicationNotification, object: nil, queue: .main
    ) { [weak self] _ in
      MainActor.assumeIsolated { self?.warmFrontmost() }
    }
    warmFrontmost()
  }
  func stop() {
    if let observer = resources.observer {
      NSWorkspace.shared.notificationCenter.removeObserver(observer)
      resources.observer = nil
    }
    work?.cancel()
    work = nil
  }
  private func warmFrontmost() {
    work?.cancel()
    guard AXIsProcessTrusted(), let app = NSWorkspace.shared.frontmostApplication,
      app.bundleIdentifier != Bundle.main.bundleIdentifier
    else { return }
    let pid = app.processIdentifier
    work = Task.detached(priority: .utility) {
      guard !Task.isCancelled else { return }
      let application = AXUIElementCreateApplication(pid)
      for attempt in 0..<3 {
        guard !Task.isCancelled else { return }
        let reader = NativeFocusReader(application: application, budget: .milliseconds(80))
        _ = reader.attribute(kAXRoleAttribute, of: application)
        if attempt < 2 { try? await Task.sleep(for: .milliseconds(200)) }
      }
    }
  }
  deinit { work?.cancel() }
}
private final class BrowserWarmupResources: @unchecked Sendable {
  var observer: NSObjectProtocol?
  deinit {
    if let observer { NSWorkspace.shared.notificationCenter.removeObserver(observer) }
  }
}
