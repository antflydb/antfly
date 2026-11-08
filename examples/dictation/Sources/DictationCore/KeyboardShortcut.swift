// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import Foundation

public struct KeyboardShortcut: Codable, Equatable, Sendable {
  // Native NSEvent/CGEvent flag values; legacy Carbon masks convert at the boundary.
  public static let shift: UInt32 = 1 << 17
  public static let control: UInt32 = 1 << 18
  public static let option: UInt32 = 1 << 19
  public static let command: UInt32 = 1 << 20
  public static let function: UInt32 = 1 << 23
  public static let allowed = shift | control | option | command | function
  public static let fn = KeyboardShortcut(modifiers: function)
  public static let controlOption = KeyboardShortcut(modifiers: control | option)
  public static let controlOptionSpace = KeyboardShortcut(
    keyCode: 49, modifiers: control | option, keyLabel: "Space")
  public static func == (lhs: Self, rhs: Self) -> Bool {
    lhs.keyCode == rhs.keyCode && lhs.modifiers == rhs.modifiers
  }
  public var keyCode: UInt32?
  public var modifiers: UInt32
  public var keyLabel: String?
  public init(keyCode: UInt32? = nil, modifiers: UInt32, keyLabel: String? = nil) {
    self.keyCode = keyCode
    self.modifiers = modifiers
    self.keyLabel = keyLabel
  }
  public init(legacyKey: UInt32, legacyModifiers: UInt32) {
    keyCode = legacyKey
    modifiers = 0
    for (old, new) in [
      (UInt32(256), Self.command), (512, Self.shift), (2048, Self.option), (4096, Self.control),
    ] {
      if legacyModifiers & old != 0 { modifiers |= new }
    }
  }
  public var usesEventTap: Bool { keyCode == nil || modifiers & Self.function != 0 }
  public var carbonModifiers: UInt32 {
    var result: UInt32 = 0
    for (old, new) in [
      (UInt32(256), Self.command), (512, Self.shift), (2048, Self.option), (4096, Self.control),
    ] {
      if modifiers & new != 0 { result |= old }
    }
    return result
  }
  public func validated() throws -> Self {
    guard modifiers != 0, modifiers & ~Self.allowed == 0 else {
      throw DictationError.invalid("Choose at least one of Fn, Control, Option, Shift, or Command.")
    }
    if let keyCode {
      guard keyCode < 128, ![54, 55, 56, 57, 58, 59, 60, 61, 62, 63].contains(keyCode) else {
        throw DictationError.invalid("Use the modifier buttons for modifier-only shortcuts.")
      }
      if modifiers == Self.command
        && [0, 1, 3, 4, 6, 7, 8, 9, 12, 13, 17, 35, 45, 46, 48, 49].contains(keyCode)
      {
        throw DictationError.invalid(
          "Choose a shortcut that does not replace common editing or system shortcuts.")
      }
    }
    guard (keyLabel?.utf8.count ?? 0) <= 64 else {
      throw DictationError.invalid("Shortcut label is too long.")
    }
    return self
  }
  public var label: String {
    var parts: [String] = []
    for (flag, text) in [
      (Self.control, "Control"), (Self.option, "Option"), (Self.shift, "Shift"),
      (Self.command, "Command"), (Self.function, "Fn"),
    ] {
      if modifiers & flag != 0 { parts.append(text) }
    }
    if let keyCode { parts.append(keyLabel ?? Self.keyName(keyCode)) }
    return parts.isEmpty ? "Choose a shortcut" : parts.joined(separator: " + ")
  }
  public static func keyName(_ code: UInt32) -> String {
    let names: [UInt32: String] = [
      0: "A", 1: "S", 2: "D", 3: "F", 4: "H", 5: "G", 6: "Z", 7: "X", 8: "C", 9: "V", 11: "B",
      12: "Q", 13: "W", 14: "E", 15: "R", 16: "Y", 17: "T", 18: "1", 19: "2", 20: "3", 21: "4",
      22: "6", 23: "5", 24: "=", 25: "9", 26: "7", 27: "−", 28: "8", 29: "0", 30: "]", 31: "O",
      32: "U", 33: "[", 34: "I", 35: "P", 36: "Return", 37: "L", 38: "J", 39: "'", 40: "K", 41: ";",
      42: "\\", 43: ",", 44: "/", 45: "N", 46: "M", 47: ".", 48: "Tab", 49: "Space", 50: "`",
      51: "Delete", 53: "Escape", 96: "F5", 97: "F6", 98: "F7", 99: "F3", 100: "F8", 101: "F9",
      103: "F11", 109: "F10", 111: "F12", 118: "F4", 120: "F2", 122: "F1", 123: "←", 124: "→",
      125: "↓", 126: "↑",
    ]
    return names[code] ?? "Key \(code)"
  }
}

/// Pure transition logic shared by the native event tap and deterministic tests.
/// Modifier-only shortcuts yield to real keyboard chords rather than hijacking them.
public struct ShortcutTracker: Sendable {
  public enum Kind: Equatable, Sendable { case flags, keyDown, keyUp }
  public enum Action: Equatable, Sendable { case none, press, release, interrupt }
  public struct Result: Sendable {
    public var action: Action
    public var consume: Bool
  }
  public var shortcut: KeyboardShortcut
  private var down = false
  private var blocked = false
  private var suppressedKey: UInt32?
  public init(shortcut: KeyboardShortcut) { self.shortcut = shortcut }
  public mutating func reset() {
    down = false
    blocked = false
    suppressedKey = nil
  }
  public mutating func event(_ kind: Kind, key: UInt32, flags: UInt32) -> Result {
    let flags = flags & KeyboardShortcut.allowed
    let matches = flags == shortcut.modifiers
    if kind == .keyUp, suppressedKey == key {
      suppressedKey = nil
      let wasDown = down
      down = false
      return Result(action: wasDown ? .release : .none, consume: true)
    }
    if blocked {
      if flags & shortcut.modifiers == 0 { blocked = false }
      return Result(action: .none, consume: false)
    }
    if shortcut.keyCode == nil {
      if kind == .keyDown && down {
        down = false
        blocked = true
        return Result(action: .interrupt, consume: false)
      }
      if kind == .flags {
        if matches && !down {
          down = true
          return Result(action: .press, consume: true)
        }
        if down && !matches {
          down = false
          if flags & ~shortcut.modifiers != 0 {
            blocked = true
            return Result(action: .interrupt, consume: false)
          }
          return Result(action: .release, consume: true)
        }
        return Result(action: .none, consume: down)
      }
    } else {
      if kind == .keyDown && key == shortcut.keyCode && matches {
        suppressedKey = key
        if down { return Result(action: .none, consume: true) }
        down = true
        return Result(action: .press, consume: true)
      }
      if kind == .flags && down && !matches {
        down = false
        if flags & ~shortcut.modifiers != 0 {
          blocked = true
          return Result(action: .interrupt, consume: false)
        }
        return Result(action: .release, consume: true)
      }
    }
    return Result(action: .none, consume: false)
  }
}

/// macOS also sets its function flag for navigation/function keys. Only a
/// physical Fn/Globe flags-change event should arm an Fn shortcut.
public struct PhysicalModifierFlags: Sendable {
  private var functionDown = false
  public init() {}
  public mutating func reset() { functionDown = false }
  public mutating func normalize(_ kind: ShortcutTracker.Kind, key: UInt32, raw: UInt32) -> UInt32 {
    if kind == .flags && key == 63 { functionDown = raw & KeyboardShortcut.function != 0 }
    return (raw & KeyboardShortcut.allowed & ~KeyboardShortcut.function)
      | (functionDown ? KeyboardShortcut.function : 0)
  }
}
