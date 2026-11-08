// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AVFoundation
import AppKit
import ApplicationServices
import Carbon
import DictationCore
import Security

@MainActor enum Credentials {
  static func read(server: String) -> String {
    var result: CFTypeRef?
    let query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: "dev.antfly.dictation", kSecAttrAccount as String: server,
      kSecReturnData as String: true, kSecMatchLimit as String: kSecMatchLimitOne,
    ]
    guard SecItemCopyMatching(query as CFDictionary, &result) == errSecSuccess,
      let data = result as? Data
    else { return "" }
    return String(decoding: data, as: UTF8.self)
  }
  static func save(_ token: String, server: String) throws {
    let query: [String: Any] = [
      kSecClass as String: kSecClassGenericPassword,
      kSecAttrService as String: "dev.antfly.dictation", kSecAttrAccount as String: server,
    ]
    let status: OSStatus
    if token.isEmpty {
      status = SecItemDelete(query as CFDictionary)
    } else {
      let attributes = [kSecValueData as String: Data(token.utf8)]
      let update = SecItemUpdate(query as CFDictionary, attributes as CFDictionary)
      if update == errSecItemNotFound {
        status = SecItemAdd(query.merging(attributes) { _, new in new } as CFDictionary, nil)
      } else {
        status = update
      }
    }
    guard status == errSecSuccess || status == errSecItemNotFound else {
      throw DictationError.invalid("Keychain error \(status).")
    }
  }
}

@MainActor protocol ShortcutManaging: AnyObject {
  var changed: ((Bool) -> Void)? { get set }
  var interrupted: (() -> Void)? { get set }
  var isAvailable: Bool { get }
  func register(_ shortcut: KeyboardShortcut) throws
  func unregister()
  func setSuspended(_ value: Bool)
}

@MainActor final class Hotkey: ShortcutManaging {
  private let resources = HotkeyResources()
  var changed: ((Bool) -> Void)?
  var interrupted: (() -> Void)?
  private var suspended = false
  private var pressed = false
  private var bindingVersion = UUID()
  private var binding: KeyboardShortcut?
  private func receive(_ down: Bool) {
    guard !suspended, down != pressed else { return }
    pressed = down
    changed?(down)
  }
  init() {
    var events = [
      EventTypeSpec(
        eventClass: OSType(kEventClassKeyboard), eventKind: UInt32(kEventHotKeyPressed)),
      EventTypeSpec(
        eventClass: OSType(kEventClassKeyboard), eventKind: UInt32(kEventHotKeyReleased)),
    ]
    InstallEventHandler(
      GetApplicationEventTarget(),
      { _, event, context in
        guard let context, let event else { return OSStatus(eventNotHandledErr) }
        let down = GetEventKind(event) == kEventHotKeyPressed
        MainActor.assumeIsolated {
          Unmanaged<Hotkey>.fromOpaque(context).takeUnretainedValue().receive(down)
        }
        return noErr
      }, events.count, &events, Unmanaged.passUnretained(self).toOpaque(), &resources.handler)
  }
  func unregister() {
    binding = nil
    bindingVersion = UUID()
    pressed = false
    if let reference = resources.reference {
      UnregisterEventHotKey(reference)
      resources.reference = nil
    }
    resources.tap?.stop()
    resources.tap = nil
  }
  var isAvailable: Bool {
    resources.tap?.isAvailable ?? (resources.reference != nil)
  }
  func setSuspended(_ value: Bool) {
    suspended = value
    resources.tap?.setSuspended(value)
    if pressed {
      pressed = false
      interrupted?()
    }
  }
  func register(_ requested: KeyboardShortcut) throws {
    let shortcut = try requested.validated()
    guard binding != shortcut || !isAvailable else { return }
    let version = UUID()
    if shortcut.usesEventTap {
      let next = try GlobalShortcutTap(shortcut: shortcut) { [weak self] action in
        DispatchQueue.main.async { [weak self] in
          guard let self, !self.suspended, self.bindingVersion == version else { return }
          switch action {
          case .press: self.receive(true)
          case .release: self.receive(false)
          case .interrupt:
            self.pressed = false
            self.interrupted?()
          case .none: break
          }
        }
      }
      next.setSuspended(suspended)
      resources.tap?.stop()
      resources.tap = next
      if let reference = resources.reference {
        UnregisterEventHotKey(reference)
        resources.reference = nil
      }
    } else {
      var next: EventHotKeyRef?
      let status = RegisterEventHotKey(
        shortcut.keyCode!, shortcut.carbonModifiers,
        EventHotKeyID(signature: 0x414E_5446, id: 1), GetApplicationEventTarget(), 0, &next)
      guard status == noErr else {
        throw DictationError.invalid(
          "Shortcut is unavailable (\(status)); the previous shortcut remains registered.")
      }
      if let reference = resources.reference { UnregisterEventHotKey(reference) }
      resources.reference = next
      resources.tap?.stop()
      resources.tap = nil
    }
    bindingVersion = version
    binding = shortcut
    pressed = false
  }
}

// The audio callback copies native samples into a bounded synchronized buffer.
// AVAudioEngine owns callback scheduling; no UI or networking runs on its thread.
final class SpeechAudioBuffer: @unchecked Sendable {
  private let lock = NSLock()
  private var recording = Data()
  private var pending = Data()
  private var overflow = false
  private var previewOverflow = false
  private var failure: String?
  func fail(_ message: String) {
    lock.lock()
    failure = message
    lock.unlock()
  }
  func append(_ buffer: AVAudioPCMBuffer) {
    guard let channels = buffer.floatChannelData else { return }
    let frames = Int(buffer.frameLength)
    let count = Int(buffer.format.channelCount)
    var mono = [Float](repeating: 0, count: frames)
    for frame in 0..<frames {
      for channel in 0..<count { mono[frame] += channels[channel][frame] / Float(count) }
    }
    let data = mono.withUnsafeBytes { Data($0) }
    lock.lock()
    defer { lock.unlock() }
    guard recording.count + data.count <= 120 * 1024 * 1024 else {
      overflow = true
      return
    }
    recording.append(data)
    if pending.count + data.count <= 8 * 1024 * 1024 {
      pending.append(data)
    } else {
      previewOverflow = true
    }
  }
  func drain() throws -> Data {
    lock.lock()
    defer { lock.unlock() }
    if let failure { throw DictationError.invalid(failure) }
    guard !overflow else {
      throw DictationError.invalid(
        "Recording or live preview exceeded its buffer limit. Stop and retry a shorter recording.")
    }
    if previewOverflow {
      previewOverflow = false
      pending.removeAll(keepingCapacity: true)
      throw DictationError.invalid(
        "Live audio processing fell behind. The complete recording remains available for clip processing."
      )
    }
    let result = pending
    pending.removeAll(keepingCapacity: true)
    return result
  }
  func all() throws -> Data {
    lock.lock()
    defer { lock.unlock() }
    if let failure { throw DictationError.invalid(failure) }
    guard !overflow else { throw DictationError.invalid("Recording exceeded its buffer limit.") }
    return recording
  }
}

// AVAudioEngine serializes this node's tap. The converter is confined to that
// callback until the engine stops, then flushed on the main actor.
final class CaptureConverter: @unchecked Sendable {
  let converter: AVAudioConverter
  let format: AVAudioFormat
  let sink: SpeechAudioBuffer
  init(input: AVAudioFormat, sink: SpeechAudioBuffer) throws {
    let output = AVAudioFormat(
      commonFormat: .pcmFormatFloat32, sampleRate: 16000, channels: 1, interleaved: false)!
    guard let converter = AVAudioConverter(from: input, to: output) else {
      throw DictationError.invalid("Cannot convert microphone audio to mono speech audio.")
    }
    converter.primeMethod = .none
    self.converter = converter
    format = output
    self.sink = sink
  }
  func append(_ input: AVAudioPCMBuffer) {
    let capacity =
      AVAudioFrameCount(ceil(Double(input.frameLength) * 16000 / input.format.sampleRate)) + 256
    guard let output = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: capacity) else {
      sink.fail("Cannot allocate speech audio buffer.")
      return
    }
    var supplied = false
    var failure: NSError?
    let status = converter.convert(to: output, error: &failure) { _, state in
      if supplied {
        state.pointee = .noDataNow
        return nil
      }
      supplied = true
      state.pointee = .haveData
      return input
    }
    if status == .error { sink.fail(failure?.localizedDescription ?? "Audio conversion failed.") }
    if output.frameLength > 0 { sink.append(output) }
  }
  func finish() {
    guard let output = AVAudioPCMBuffer(pcmFormat: format, frameCapacity: 4096) else { return }
    var failure: NSError?
    let status = converter.convert(to: output, error: &failure) { _, state in
      state.pointee = .endOfStream
      return nil
    }
    if status == .error { sink.fail(failure?.localizedDescription ?? "Audio conversion failed.") }
    if output.frameLength > 0 { sink.append(output) }
  }
}

@MainActor final class Microphone {
  private let engine = AVAudioEngine()
  private(set) var buffer = SpeechAudioBuffer()
  private(set) var sampleRate: Double = 16000
  private var tapped = false
  private var conversion: CaptureConverter?
  static var devices: [AVCaptureDevice] {
    AVCaptureDevice.DiscoverySession(
      deviceTypes: [.microphone], mediaType: .audio, position: .unspecified
    ).devices
  }
  func start(deviceID: String) throws {
    buffer = SpeechAudioBuffer()
    let input = engine.inputNode
    if !deviceID.isEmpty {
      // AVAudioEngine selects its device through the HAL AudioUnit.
      var size: UInt32 = 0
      var address = AudioObjectPropertyAddress(
        mSelector: kAudioHardwarePropertyDevices, mScope: kAudioObjectPropertyScopeGlobal,
        mElement: kAudioObjectPropertyElementMain)
      guard
        AudioObjectGetPropertyDataSize(
          AudioObjectID(kAudioObjectSystemObject), &address, 0, nil, &size) == noErr
      else { throw DictationError.invalid("Cannot enumerate audio devices.") }
      var ids = [AudioDeviceID](repeating: 0, count: Int(size) / MemoryLayout<AudioDeviceID>.size)
      let status = ids.withUnsafeMutableBytes {
        AudioObjectGetPropertyData(
          AudioObjectID(kAudioObjectSystemObject), &address, 0, nil, &size, $0.baseAddress!)
      }
      guard status == noErr else { throw DictationError.invalid("Cannot read audio devices.") }
      var selected: AudioDeviceID?
      for id in ids {
        var uid: Unmanaged<CFString>?
        var uidSize = UInt32(MemoryLayout<Unmanaged<CFString>?>.size)
        var uidAddress = AudioObjectPropertyAddress(
          mSelector: kAudioDevicePropertyDeviceUID, mScope: kAudioObjectPropertyScopeGlobal,
          mElement: kAudioObjectPropertyElementMain)
        if AudioObjectGetPropertyData(id, &uidAddress, 0, nil, &uidSize, &uid) == noErr,
          let value = uid?.takeRetainedValue(), value as String == deviceID
        {
          selected = id
          break
        }
      }
      guard var selected, let unit = input.audioUnit,
        AudioUnitSetProperty(
          unit, kAudioOutputUnitProperty_CurrentDevice, kAudioUnitScope_Global, 0, &selected,
          UInt32(MemoryLayout<AudioDeviceID>.size)) == noErr
      else { throw DictationError.invalid("Selected microphone is unavailable.") }
    }
    let format = input.outputFormat(forBus: 0)
    guard format.sampleRate >= 8000, format.sampleRate <= 192000, format.channelCount > 0 else {
      throw DictationError.invalid("Microphone has no usable audio format.")
    }
    sampleRate = 16000
    let conversion = try CaptureConverter(input: format, sink: buffer)
    self.conversion = conversion
    input.installTap(onBus: 0, bufferSize: 4096, format: format) { block, _ in
      conversion.append(block)
    }
    tapped = true
    do {
      engine.prepare()
      try engine.start()
    } catch {
      stop()
      throw error
    }
  }
  func stop() {
    engine.stop()
    if tapped {
      engine.inputNode.removeTap(onBus: 0)
      tapped = false
    }
    conversion?.finish()
    conversion = nil
  }
  func recording() throws -> PCMRecording {
    PCMRecording(samples: try buffer.all(), sampleRate: sampleRate)
  }
  func wav() throws -> Data { try recording().wav() }

}

@MainActor struct InsertionTarget {
  let pid: pid_t
  let bundleID: String
  let element: AXUIElement?
  static func capture() -> InsertionTarget? {
    guard let app = NSWorkspace.shared.frontmostApplication,
      app.processIdentifier != ProcessInfo.processInfo.processIdentifier
    else { return nil }
    var focused: CFTypeRef?
    AXUIElementCopyAttributeValue(
      AXUIElementCreateApplication(app.processIdentifier), kAXFocusedUIElementAttribute as CFString,
      &focused)
    var element = focused.flatMap {
      CFGetTypeID($0) == AXUIElementGetTypeID() ? unsafeDowncast($0, to: AXUIElement.self) : nil
    }
    if let candidate = element {
      var role: CFTypeRef?
      var subrole: CFTypeRef?
      AXUIElementCopyAttributeValue(candidate, kAXRoleAttribute as CFString, &role)
      AXUIElementCopyAttributeValue(candidate, kAXSubroleAttribute as CFString, &subrole)
      let textRoles = [kAXTextFieldRole, kAXTextAreaRole, kAXComboBoxRole]
      if !textRoles.contains(role as? String ?? "")
        || subrole as? String == kAXSecureTextFieldSubrole
      {
        element = nil
      }
    }
    return InsertionTarget(
      pid: app.processIdentifier, bundleID: app.bundleIdentifier ?? "", element: element)
  }
  private func validatePasteTarget() throws {
    guard AXIsProcessTrusted() else {
      throw DictationError.invalid(
        "Enable Accessibility for Antfly Dictation to paste. The transcript is ready to copy.")
    }
    guard let element else {
      throw DictationError.invalid(
        "No supported text field was captured. Copy the transcript to insert it manually.")
    }
    guard NSWorkspace.shared.frontmostApplication?.processIdentifier == pid else {
      throw DictationError.invalid(
        "Target focus changed. Copy the transcript to insert it manually.")
    }
    var current: CFTypeRef?
    AXUIElementCopyAttributeValue(
      AXUIElementCreateApplication(pid), kAXFocusedUIElementAttribute as CFString, &current)
    guard let current, CFEqual(element, current) else {
      throw DictationError.invalid(
        "Focused text field changed. Copy the transcript to insert it manually.")
    }
  }
  func paste(_ text: String, validate: @MainActor () throws -> Void = {}) async throws {
    try validate()
    try validatePasteTarget()
    try await DesktopClipboard.shared.paste(text) {
      try validate()
      try validatePasteTarget()
      guard let down = CGEvent(keyboardEventSource: nil, virtualKey: 9, keyDown: true),
        let up = CGEvent(keyboardEventSource: nil, virtualKey: 9, keyDown: false)
      else { throw DictationError.invalid("Cannot generate paste event.") }
      down.flags = .maskCommand
      up.flags = .maskCommand
      down.post(tap: .cghidEventTap)
      up.post(tap: .cghidEventTap)
    }
  }
}

@MainActor enum DesktopSettings {
  static func open(_ pane: String) {
    if let url = URL(string: "x-apple.systempreferences:" + pane), NSWorkspace.shared.open(url) {
      return
    }
    _ = NSWorkspace.shared.open(URL(fileURLWithPath: "/System/Applications/System Settings.app"))
  }
}

@MainActor private enum DesktopClipboard {
  static let shared = ClipboardCoordinator(
    read: {
      try await Task.detached(priority: .userInitiated) {
        let board = NSPasteboard.general
        let count = board.changeCount
        let items = board.pasteboardItems ?? []
        guard items.count <= 16 else {
          throw DictationError.invalid(
            "Clipboard is too large to preserve. Copy the transcript manually.")
        }
        var bytes = 0
        var representations = 0
        var saved: [[ClipboardCoordinator.Representation]] = []
        for item in items {
          var entries: [ClipboardCoordinator.Representation] = []
          for type in item.types {
            representations += 1
            guard representations <= 64 else {
              throw DictationError.invalid(
                "Clipboard has too many formats to preserve. Copy the transcript manually.")
            }
            guard let data = item.data(forType: type) else {
              throw DictationError.invalid(
                "Clipboard cannot be preserved. Copy the transcript manually.")
            }
            bytes += data.count
            guard bytes <= 8 * 1024 * 1024 else {
              throw DictationError.invalid(
                "Clipboard is too large to preserve. Copy the transcript manually.")
            }
            entries.append(.init(type: type.rawValue, data: data))
          }
          saved.append(entries)
        }
        return ClipboardCoordinator.Snapshot(count: count, items: saved)
      }.value
    }, count: { NSPasteboard.general.changeCount },
    writeText: { text in
      let board = NSPasteboard.general
      board.clearContents()
      board.setString(text, forType: .string)
      return board.changeCount
    },
    restore: { saved in
      let board = NSPasteboard.general
      board.clearContents()
      let items = saved.map { entries in
        let item = NSPasteboardItem()
        for entry in entries { item.setData(entry.data, forType: .init(entry.type)) }
        return item
      }
      board.writeObjects(items)
    })
}
