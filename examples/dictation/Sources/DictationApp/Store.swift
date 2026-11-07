// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AVFoundation
@preconcurrency import ApplicationServices
import DictationCore
import SwiftUI

@MainActor final class Store: ObservableObject {
  @Published var config = Configuration()
  @Published var history: [HistoryEntry] = []
  @Published var state = RecordingState.idle
  @Published var preview = ""
  @Published var result = ""
  @Published var message = "Ready. Control–Option–Space to dictate."
  @Published var discovery = ""
  @Published var models: [DiscoveredModel] = []
  @Published var searchText = ""
  @Published var searchResult = ""
  @Published var token = ""
  @Published var working = false
  private let hotkey = Hotkey()
  private var microphone: Microphone?
  private var target: InsertionTarget?
  private var selected: Preset?
  private var recordingConfig: Configuration?
  private var requestTask: Task<Void, Never>?
  private var previewTask: Task<Void, Never>?
  private var timeoutTask: Task<Void, Never>?
  private var generation = UUID()
  private var registered: (UInt32, UInt32)?
  private var retryAudio: Data?
  private var retryPreset: Preset?
  private let directory: URL
  private var settingsFile: URL { directory.appendingPathComponent("settings.json") }
  private var historyFile: URL { directory.appendingPathComponent("history.json") }
  init() {
    directory = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
      .appendingPathComponent("AntflyDictation")
    do {
      try FileManager.default.createDirectory(
        at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
      if FileManager.default.fileExists(atPath: settingsFile.path) {
        config = try Configuration.decode(Data(contentsOf: settingsFile))
      }
      if config.retainHistory, let data = try? Data(contentsOf: historyFile) {
        history = try JSONDecoder().decode([HistoryEntry].self, from: data)
      }
      try registerShortcut()
    } catch { message = error.localizedDescription }
    token = Credentials.read(server: config.server)
    hotkey.changed = { [weak self] down in self?.shortcut(down: down) }
  }
  private func client(_ snapshot: Configuration? = nil) throws -> AntflyClient {
    let settings = snapshot ?? config
    return try AntflyClient(
      server: settings.server, token: Credentials.read(server: settings.server))
  }
  private func registerShortcut() throws {
    if registered?.0 == config.shortcutKey && registered?.1 == config.shortcutModifiers { return }
    try hotkey.register(key: config.shortcutKey, modifiers: config.shortcutModifiers)
    registered = (config.shortcutKey, config.shortcutModifiers)
  }
  private func write(_ data: Data, to url: URL) throws {
    try data.write(to: url, options: .atomic)
    try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
  }
  private func persistSettings() throws {
    config = try config.validated()
    try registerShortcut()
    try Credentials.save(token, server: config.server)
    try write(JSONEncoder().encode(config), to: settingsFile)
    if !config.retainHistory {
      history = []
      try write(JSONEncoder().encode(history), to: historyFile)
    }
    if !config.retainAudio { try removeAudio() }
  }
  func save() {
    perform {
      try self.persistSettings()
      self.message = "Settings saved locally. Publish a snapshot to save them in Antfly."
    }
  }
  private func perform(_ body: () throws -> Void) {
    do { try body() } catch { message = error.localizedDescription }
  }
  func requestMicrophone() {
    Task {
      let allowed = await AVCaptureDevice.requestAccess(for: .audio)
      message =
        allowed ? "Microphone ready." : "Microphone access denied. Enable it in System Settings."
    }
  }
  func requestAccessibility() {
    let options =
      [kAXTrustedCheckOptionPrompt.takeUnretainedValue() as String: true] as CFDictionary
    _ = AXIsProcessTrustedWithOptions(options)
  }
  func shortcut(down: Bool) {
    guard !working else { return }
    var next = state
    let toggle = recordingConfig?.toggle ?? config.toggle
    let transition = down ? next.press(toggle: toggle) : next.release(toggle: toggle)
    guard transition else { return }
    if next == .recording { start() } else if next == .processing { finish() }
  }
  func start() {
    guard state == .idle, !working else { return }
    do {
      let snapshot = try config.validated()
      guard AVCaptureDevice.authorizationStatus(for: .audio) == .authorized else {
        throw DictationError.invalid("Allow microphone access in Settings before recording.")
      }
      generation = UUID()
      let run = generation
      target = InsertionTarget.capture()
      let preset = snapshot.preset(for: target?.bundleID)
      if preset.livePreview,
        models.first(where: { $0.id == preset.model })?.dictation?.live_preview == false
      {
        throw DictationError.invalid(
          "This transcriber supports recorded clips only. Disable live preview for this preset.")
      }
      if let error = models.first(where: { $0.id == preset.model })?.dictation?.readiness_error {
        throw DictationError.invalid("Transcriber unavailable on this server: " + error)
      }
      selected = preset
      recordingConfig = snapshot
      let mic = Microphone()
      try mic.start(deviceID: snapshot.microphoneID)
      microphone = mic
      state = .recording
      preview = ""
      result = ""
      retryAudio = nil
      message = "Recording · \(preset.name) · \(snapshot.server)"
      timeoutTask = Task { [weak self] in
        try? await Task.sleep(for: .seconds(600))
        guard !Task.isCancelled, let self, self.generation == run, self.state == .recording else {
          return
        }
        self.finish()
      }
      let buffer = mic.buffer
      let rate = mic.sampleRate
      let connection = try client(snapshot)
      previewTask = Task { [weak self] in
        var sessionID: String?
        do {
          if preset.livePreview {
            sessionID = try await connection.createSession(
              preset: preset, dictionary: snapshot.dictionary)
          }
          while !Task.isCancelled {
            try await Task.sleep(for: .milliseconds(750))
            let chunk = try buffer.drain()
            if let id = sessionID, !chunk.isEmpty {
              let text = try await connection.appendSession(id, audio: chunk, sampleRate: rate)
              if !Task.isCancelled, self?.generation == run, self?.state == .recording,
                !text.isEmpty
              {
                self?.preview = text
              }
            }
          }
        } catch {
          if !Task.isCancelled, self?.generation == run {
            self?.message =
              "Live preview unavailable: \(error.localizedDescription). Final clip will still be submitted on release."
          }
          // Keep draining the preview queue after a preview failure.
          while !Task.isCancelled {
            try? await Task.sleep(for: .milliseconds(750))
            _ = try? buffer.drain()
          }
        }
        if let id = sessionID { await Task.detached { await connection.closeSession(id) }.value }
      }
    } catch {
      microphone?.stop()
      microphone = nil
      state = .idle
      message = error.localizedDescription
    }
  }
  func finish() {
    guard state == .recording, let mic = microphone, let preset = selected,
      let snapshot = recordingConfig
    else { return }
    mic.stop()
    microphone = nil
    previewTask?.cancel()
    timeoutTask?.cancel()
    do {
      let audio = try mic.wav()
      guard audio.count > 44 else {
        throw DictationError.invalid("No microphone audio was captured.")
      }
      retryAudio = audio
      retryPreset = preset
      if snapshot.retainAudio {
        try write(audio, to: directory.appendingPathComponent("last-recording.wav"))
      }
      submit(audio: audio, preset: preset, snapshot: snapshot, destination: target)
    } catch {
      state = .idle
      message = error.localizedDescription
    }
  }
  private func submit(
    audio: Data, preset: Preset, snapshot: Configuration, destination: InsertionTarget?
  ) {
    state = .processing
    message = "Transcribing · \(snapshot.server)"
    let run = generation
    requestTask = Task { [weak self] in
      guard let self else { return }
      do {
        let connection = try client(snapshot)
        let output = try await connection.dictate(
          audio: audio, preset: preset, dictionary: snapshot.dictionary
        ) { [weak self] text in
          await self?.updatePreview(text, run: run)
        }
        try Task.checkCancellation()
        guard generation == run else { return }
        result = output.text
        preview = output.raw
        var status = "ready"
        if snapshot.autoPaste
          && !output.text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        {
          if let destination {
            do {
              try await destination.paste(output.text)
              status = "paste_sent"
              message = "Paste sent. Transcript remains available to copy."
            } catch {
              status = "insertion_failed"
              message = error.localizedDescription
            }
          } else {
            status = "insertion_failed"
            message = "No external text field was captured. Transcript is ready to copy."
          }
        } else {
          message = "Transcript ready to copy."
        }
        guard generation == run else { return }
        if snapshot.retainHistory {
          let entry = HistoryEntry(
            preset: preset.name, model: preset.model, application: destination?.bundleID ?? "",
            raw: output.raw, text: output.text, status: status)
          history.insert(entry, at: 0)
          history = Array(history.prefix(500))
          try write(JSONEncoder().encode(history), to: historyFile)
          do { try await connection.saveHistory(entry, table: snapshot.table) } catch {
            message += " History saved locally; Antfly save failed: \(error.localizedDescription)"
          }
        }
        guard generation == run else { return }
        if !snapshot.retainAudio { retryAudio = nil }
        state = .idle
      } catch {
        guard generation == run else { return }
        state = .idle
        message = error.localizedDescription
        let transcriptionFailed = result.isEmpty
        if result.isEmpty && !preview.isEmpty { result = preview }
        if snapshot.retainHistory && transcriptionFailed {
          let entry = HistoryEntry(
            preset: preset.name, model: preset.model, application: destination?.bundleID ?? "",
            raw: preview, text: "",
            status: "error: " + String(error.localizedDescription.prefix(2048)))
          history.insert(entry, at: 0)
          history = Array(history.prefix(500))
          try? write(JSONEncoder().encode(history), to: historyFile)
          if let connection = try? client(snapshot) {
            try? await connection.saveHistory(entry, table: snapshot.table)
          }
        }
      }
    }
  }
  private func updatePreview(_ text: String, run: UUID) { if generation == run { preview = text } }
  func cancel() {
    generation = UUID()
    requestTask?.cancel()
    previewTask?.cancel()
    timeoutTask?.cancel()
    microphone?.stop()
    microphone = nil
    state = .idle
    retryAudio = nil
    message = "Canceled remaining work."
  }
  func retry() {
    guard state == .idle, !working, let audio = retryAudio, let original = retryPreset else {
      message = "No failed recording retained in memory."
      return
    }
    do {
      var settings = try config.validated()
      let preset =
        settings.presets.first(where: { $0.id == original.id }) ?? settings.preset(for: nil)
      generation = UUID()
      // Use corrected settings, but never reuse a stale insertion target.
      settings.autoPaste = false
      submit(audio: audio, preset: preset, snapshot: settings, destination: nil)
    } catch { message = error.localizedDescription }
  }
  func copy(_ text: String) {
    NSPasteboard.general.clearContents()
    NSPasteboard.general.setString(text, forType: .string)
  }
  func run(_ action: @escaping @MainActor () async throws -> Void) {
    guard !working, state == .idle else { return }
    do { _ = try config.validated() } catch {
      message = error.localizedDescription
      return
    }
    working = true
    Task {
      defer { working = false }
      do { try await action() } catch { message = error.localizedDescription }
    }
  }
  func discover() {
    run {
      self.discovery = try await self.client().models()
      self.models = try JSONDecoder().decode(ModelCatalog.self, from: Data(self.discovery.utf8))
        .data
      self.message =
        "Models discovered. Apple native status and installed locales are included below; transcription checks the requested locale."
    }
  }
  func setup() {
    run {
      try await self.client().setup(table: self.config.table)
      self.message = "Antfly table ready."
    }
  }
  func publish() {
    run {
      try self.persistSettings()
      try await self.client().saveSettings(self.config)
      self.message = "Settings snapshot published to Antfly."
    }
  }
  func download() {
    run {
      let current = self.config
      var incoming = try await self.client().loadSettings(
        table: current.table, profile: current.profile)
      // A snapshot cannot silently change the connection or credential scope.
      incoming.server = current.server
      incoming.table = current.table
      incoming.profile = current.profile
      self.config = incoming
      self.token = Credentials.read(server: incoming.server)
      try self.persistSettings()
      self.message = "Settings replaced from Antfly."
    }
  }
  func search() {
    run {
      self.searchResult = try await self.client().search(self.searchText, table: self.config.table)
    }
  }
  func syncHistory() {
    run {
      let connection = try self.client()
      for entry in self.history {
        try await connection.saveHistory(entry, table: self.config.table)
      }
      self.message = "Local history uploaded."
    }
  }
  func clearHistory() {
    run {
      try await self.client().deleteHistory(ids: self.history.map(\.id), table: self.config.table)
      self.history = []
      try self.write(JSONEncoder().encode(self.history), to: self.historyFile)
      self.message = "Local history and its corresponding Antfly documents deleted."
    }
  }
  func clearLocalHistory() {
    perform {
      self.history = []
      try self.write(JSONEncoder().encode(self.history), to: self.historyFile)
      self.message = "Local history cleared. Remote documents are unchanged."
    }
  }
  private func removeAudio() throws {
    let file = directory.appendingPathComponent("last-recording.wav")
    if FileManager.default.fileExists(atPath: file.path) {
      try FileManager.default.removeItem(at: file)
    }
  }
  func exportSettings() {
    let panel = NSSavePanel()
    panel.nameFieldStringValue = "antfly-dictation.json"
    if panel.runModal() == .OK, let url = panel.url {
      perform { try self.write(JSONEncoder().encode(self.config.validated()), to: url) }
    }
  }
  func importSettings() {
    let panel = NSOpenPanel()
    panel.allowsMultipleSelection = false
    panel.canChooseDirectories = false
    if panel.runModal() == .OK, let url = panel.url {
      perform {
        let imported = try Configuration.decode(Data(contentsOf: url))
        self.config = imported
        self.token = Credentials.read(server: imported.server)
        try self.persistSettings()
        self.message = "Settings imported."
      }
    }
  }
  func exportResult() {
    let panel = NSSavePanel()
    panel.nameFieldStringValue = "transcript.txt"
    if panel.runModal() == .OK, let url = panel.url {
      perform { try self.write(Data(self.result.utf8), to: url) }
    }
  }
}
