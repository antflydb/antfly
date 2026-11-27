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

/// Bounded synchronous IPC, used only from background capture/validation work.
final class NativeFocusReader {
  let application: AXUIElement
  let deadline: ContinuousClock.Instant
  private var queryFailed = false
  init(application: AXUIElement, budget: Duration) {
    self.application = application
    deadline = .now.advanced(by: budget)
    AXUIElementSetMessagingTimeout(application, 0.08)
  }
  func attribute(_ name: String, of node: AXUIElement) -> CFTypeRef? {
    guard ContinuousClock.now < deadline else {
      queryFailed = true
      return nil
    }
    AXUIElementSetMessagingTimeout(node, 0.08)
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
  func focusedText() -> Result<AXUIElement, TextCaptureFailure> {
    queryFailed = false
    guard let focused = element(kAXFocusedUIElementAttribute, of: application) else {
      return .failure(.unsupported)
    }
    let result = FocusedTextResolver<AXUIElement>(
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
          ?? self.element(kAXFocusedUIElementAttribute, of: node)
      }, equal: { CFEqual($0, $1) }
    ).resolve(focused)
    return queryFailed ? .failure(.unsupported) : result
  }
}

/// Requesting the application role activates Chromium's on-demand AX tree.
/// Resolve while recording begins, with a single bounded budget off the UI actor.
func captureTextElement(pid: pid_t) async -> CapturedTextElement {
  guard AXIsProcessTrusted() else { return .init(failure: .permission) }
  let application = AXUIElementCreateApplication(pid)
  let reader = NativeFocusReader(application: application, budget: .seconds(1))
  _ = reader.attribute(kAXRoleAttribute, of: application)
  var initialWindow: AXUIElement?
  while ContinuousClock.now < reader.deadline, !Task.isCancelled {
    // System-wide AXFocusedApplication can time out even when Chrome’s own
    // focused-element API works. NSWorkspace is the same foreground authority
    // used before paste; this main-actor hop performs no accessibility IPC.
    guard
      await MainActor.run(body: {
        NSWorkspace.shared.frontmostApplication?.processIdentifier == pid
      })
    else { return .init(failure: .focusChanged) }
    let window = reader.element(kAXFocusedWindowAttribute, of: application)
    if let initialWindow {
      guard let window, CFEqual(initialWindow, window) else { return .init(failure: .focusChanged) }
    } else {
      initialWindow = window
    }
    switch reader.focusedText() {
    case .success(let element): return .init(element: element)
    case .failure(.unsupported): break
    case .failure(let failure): return .init(failure: failure)
    }
    try? await Task.sleep(for: .milliseconds(40))
  }
  return .init(failure: .unsupported)
}
