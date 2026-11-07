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
  @Published var phase = DictationPhase.idle { didSet { updateOverlay() } }
  @Published var draft = "" { didSet { updateOverlay() } }
  @Published var processingNote = ""
  @Published var processingWarning = "" { didSet { updateOverlay() } }
  @Published var timing: DictationTiming?
  @Published var canRecoverRaw = false { didSet { updateOverlay() } }
  @Published var preview = "" { didSet { updateOverlay() } }
  @Published var result = ""
  @Published var message = "Ready. Hold Fn to dictate." { didSet { updateOverlay() } }
  @Published var discovery = ""
  @Published var models: [DiscoveredModel] = []
  @Published var searchText = ""
  @Published var searchResult = ""
  @Published var token = ""
  @Published var working = false
  @Published var historySyncStatus = "History sync idle."
  @Published var historyWorking = false
  private let hotkey: Hotkey?
  private var overlay: RecordingOverlay?
  private var live: LiveDictation?
  private var plan: ProcessingPlan?
  private var liveFailure: String?
  private var overlayTask: Task<Void, Never>?
  private(set) var outbox: HistoryOutbox?
  private var historyRevision: UInt64 = 0
  private var microphone: Microphone?
  private var target: InsertionTarget?
  private var selected: Preset?
  private var recordingConfig: Configuration?
  private(set) var requestTask: Task<Void, Never>?
  private var previewTask: Task<Void, Never>?
  private var cadenceTask: Task<Void, Never>?
  private var timeoutTask: Task<Void, Never>?
  private var generation = UUID()
  private var registered: DictationCore.KeyboardShortcut?
  @Published var capturingShortcut = false
  @Published private(set) var fnSetupPending = false
  @Published private(set) var fnSetupStatus = ""
  private let keyboardAccess: @MainActor () -> Bool
  private let keyboardAccessRequest: @MainActor () -> Void
  private let settingsOpener: @MainActor (String) -> Void
  private var retryAudio: Data?
  private var retryPreset: Preset?
  private let clientFactory: (@Sendable (Configuration) throws -> AntflyClient)?
  private let directory: URL
  private var settingsFile: URL { directory.appendingPathComponent("settings.json") }
  private var historyFile: URL { directory.appendingPathComponent("history.json") }
  init(
    directory storageDirectory: URL? = nil, registerHotkey: Bool = true,
    historyUpload: HistoryOutbox.Upload? = nil,
    clientFactory: (@Sendable (Configuration) throws -> AntflyClient)? = nil,
    keyboardAccess: @escaping @MainActor () -> Bool = { AXIsProcessTrusted() },
    keyboardAccessRequest: @escaping @MainActor () -> Void = {
      let options =
        [kAXTrustedCheckOptionPrompt.takeUnretainedValue() as String: true] as CFDictionary
      _ = AXIsProcessTrustedWithOptions(options)
    },
    settingsOpener: @escaping @MainActor (String) -> Void = { DesktopSettings.open($0) }
  ) {
    self.keyboardAccess = keyboardAccess
    self.keyboardAccessRequest = keyboardAccessRequest
    self.settingsOpener = settingsOpener
    self.clientFactory = clientFactory
    self.directory =
      storageDirectory
      ?? FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
      .appendingPathComponent("AntflyDictation")
    hotkey = registerHotkey ? Hotkey() : nil
    do {
      try FileManager.default.createDirectory(
        at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
      if FileManager.default.fileExists(atPath: settingsFile.path) {
        config = try Configuration.decode(Data(contentsOf: settingsFile))
      }
    } catch { message = error.localizedDescription }
    if config.retainHistory, let data = try? Data(contentsOf: historyFile) {
      do { history = try JSONDecoder().decode([HistoryEntry].self, from: data) } catch {
        historySyncStatus = "Local history could not be loaded: " + error.localizedDescription
      }
    }
    do {
      try registerShortcut()
      message =
        "Ready. \(config.toggle ? "Press" : "Hold") \(config.effectiveShortcut.label) to dictate."
    } catch { message = error.localizedDescription }
    do {
      outbox = try HistoryOutbox(
        directory: self.directory.appendingPathComponent("outbox"), paused: true,
        upload: historyUpload ?? { entry, destination in
          let token = await MainActor.run { Credentials.read(server: destination.server) }
          try await AntflyClient(server: destination.server, token: token).saveHistory(
            entry, table: destination.table)
        }, changed: { [weak self] status in await self?.updateHistoryStatus(status) })
      if config.retainHistory { Task { await outbox?.resume() } }
    } catch { historySyncStatus = "History sync unavailable: " + error.localizedDescription }
    token = registerHotkey ? Credentials.read(server: config.server) : ""
    hotkey?.changed = { [weak self] down in self?.shortcut(down: down) }
    hotkey?.interrupted = { [weak self] in if self?.state != .idle { self?.cancel() } }
    if registerHotkey {
      overlay = RecordingOverlay(
        cancel: { [weak self] in self?.dismissFeedback() },
        copy: { [weak self] in
          guard let self else { return }
          self.copy(self.result.isEmpty ? self.preview : self.result)
        },
        insert: { [weak self] in self?.insertOriginal() },
        retry: { [weak self] in self?.retryCleanup() })
      refreshModels()
    }
  }
  private func client(_ snapshot: Configuration? = nil) throws -> AntflyClient {
    let settings = snapshot ?? config
    if let clientFactory { return try clientFactory(settings) }
    return try AntflyClient(
      server: settings.server, token: Credentials.read(server: settings.server))
  }
  private func registerShortcut() throws {
    let shortcut = try config.effectiveShortcut.validated()
    guard registered != shortcut || hotkey?.isAvailable == false else { return }
    try hotkey?.register(shortcut)
    registered = shortcut
  }
  func refreshShortcutAccess() {
    guard state == .idle, !capturingShortcut else { return }
    if fnSetupPending {
      if keyboardAccess() { completeFnSetup() }
      return
    }
    guard registered == nil || hotkey?.isAvailable == false, state == .idle, !capturingShortcut
    else { return }
    do {
      try registerShortcut()
      message = "Shortcut ready: " + config.effectiveShortcut.label
    } catch { message = error.localizedDescription }
  }
  func captureShortcut(_ capturing: Bool) {
    if capturing { cancelFnSetup() }
    capturingShortcut = capturing
    hotkey?.setSuspended(capturing)
  }

  func setUpFnShortcut() {
    guard state == .idle, !working, !capturingShortcut else { return }
    fnSetupPending = true
    if keyboardAccess() {
      completeFnSetup()
      return
    }
    fnSetupStatus =
      "Enable Antfly Dictation in Accessibility, then return here. Fn will be applied automatically."
    message = fnSetupStatus
    keyboardAccessRequest()
    settingsOpener("com.apple.preference.security?Privacy_Accessibility")
  }
  func cancelFnSetup() {
    guard fnSetupPending else { return }
    fnSetupPending = false
    fnSetupStatus = "Fn setup canceled. The previous shortcut is unchanged."
  }
  func openKeyboardSettings() {
    settingsOpener("com.apple.Keyboard-Settings.extension")
  }
  private func completeFnSetup() {
    guard fnSetupPending, keyboardAccess() else { return }
    let previous = registered
    do {
      // Apply only the shortcut, preserving saved preferences and credentials.
      let previousData =
        FileManager.default.fileExists(atPath: settingsFile.path)
        ? try Data(contentsOf: settingsFile) : nil
      var saved = try previousData.map { try Configuration.decode($0) } ?? Configuration()
      saved.effectiveShortcut = .fn
      let bytes = try JSONEncoder().encode(saved.validated())
      try hotkey?.register(.fn)
      do { try write(bytes, to: settingsFile) } catch {
        do {
          if let previous { try hotkey?.register(previous) } else { hotkey?.unregister() }
        } catch {
          hotkey?.unregister()
          registered = nil
        }
        if let previousData {
          try? previousData.write(to: settingsFile, options: .atomic)
        } else {
          try? FileManager.default.removeItem(at: settingsFile)
        }
        throw error
      }
      config.effectiveShortcut = .fn
      registered = .fn
      fnSetupPending = false
      fnSetupStatus = "Fn is enabled and saved. \(config.toggle ? "Press" : "Hold") Fn to dictate."
      message = fnSetupStatus
    } catch {
      fnSetupPending = false
      fnSetupStatus = "Could not set up Fn: " + error.localizedDescription
      message = fnSetupStatus
    }
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
      historyRevision += 1
      let revision = historyRevision
      Task {
        await outbox?.setEnabled(false)
        guard !config.retainHistory else {
          await outbox?.setEnabled(true)
          return
        }
        do {
          try await outbox?.discardAll()
          try await outbox?.storeArchive([], revision: revision)
        } catch { historySyncStatus = error.localizedDescription }
      }
    } else {
      Task { await outbox?.resume() }
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
    keyboardAccessRequest()
  }
  func shortcut(down: Bool) {
    guard !working, !capturingShortcut else { return }
    var next = state
    let toggle = recordingConfig?.toggle ?? config.toggle
    let transition = down ? next.press(toggle: toggle) : next.release(toggle: toggle)
    guard transition else { return }
    if next == .recording { start() } else if next == .processing { finish() }
  }
  func start() {
    guard state == .idle, !working, !capturingShortcut else { return }
    do {
      let snapshot = try config.validated()
      guard AVCaptureDevice.authorizationStatus(for: .audio) == .authorized else {
        throw DictationError.invalid("Allow microphone access in Settings before recording.")
      }
      generation = UUID()
      overlayTask?.cancel()
      let run = generation
      target = InsertionTarget.capture()
      let preset = snapshot.preset(for: target?.bundleID)
      let effective = try ProcessingPlan(
        preset: preset, model: models.first(where: { $0.id == preset.model }))
      plan = effective
      processingNote = effective.explanation
      processingWarning = ""
      if let error = models.first(where: { $0.id == preset.model })?.dictation?.readiness_error {
        throw DictationError.invalid("Transcriber unavailable on this server: " + error)
      }
      selected = preset
      recordingConfig = snapshot
      let mic = Microphone()
      try mic.start(deviceID: snapshot.microphoneID)
      microphone = mic
      state = .recording
      canRecoverRaw = false
      timing = nil
      draft = ""
      phase = .listening
      live = nil
      liveFailure = nil
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
      if effective.session {
        previewTask = Task { [weak self] in
          do {
            let pipeline = try await LiveDictation.open(
              client: connection, preset: preset,
              dictionary: snapshot.dictionary, plan: effective
            ) { [weak self] event in
              await self?.receive(event, run: run)
            }
            guard let self, generation == run, !Task.isCancelled else {
              await pipeline.cancel()
              return
            }
            live = pipeline
            while !Task.isCancelled && state == .recording {
              let tick = Task { _ = try? await Task.sleep(for: .milliseconds(750)) }
              cadenceTask = tick
              await tick.value
              try Task.checkCancellation()
              if state != .recording { break }
              let chunk = try buffer.drain()
              if !chunk.isEmpty { try await pipeline.append(chunk) }
            }
          } catch {
            guard let self, generation == run, !Task.isCancelled else { return }
            liveFailure = error.localizedDescription
            if let live { await live.cancel() }
            live = nil
            processingNote =
              preset.processingStrategy != .incremental
              ? "Live processing failed. The recorded clip will use the same transcriber on release."
              : "Incremental processing failed. Your recording remains available for retry."
            processingWarning = processingNote
            message = processingNote
            while !Task.isCancelled && state == .recording {
              try? await Task.sleep(for: .milliseconds(750))
              _ = try? buffer.drain()
            }
          }
        }
        return
      }
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
      phase = .failed
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
    cadenceTask?.cancel()
    microphone = nil
    if plan?.session != true { previewTask?.cancel() }
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
      if plan?.session == true {
        let pump = previewTask
        let run = generation
        runSubmission(preset: preset, snapshot: snapshot, destination: target) { [self] in
          await pump?.value
          try Task.checkCancellation()
          guard self.generation == run else { throw CancellationError() }
          if let pipeline = self.live {
            return try await pipeline.finish(mic.buffer.drain())
          }
          guard preset.processingStrategy != .incremental else {
            throw DictationError.invalid(self.liveFailure ?? "Incremental processing failed.")
          }
          let connection = try self.client(snapshot)
          return try await connection.dictate(
            audio: audio, preset: preset, dictionary: snapshot.dictionary,
            events: { [weak self] in await self?.receive($0, run: run) }, progress: { _ in })
        }
      } else {
        submit(audio: audio, preset: preset, snapshot: snapshot, destination: target)
      }
    } catch {
      previewTask?.cancel()
      if let live { Task { await live.cancel() } }
      phase = .failed
      state = .idle
      message = error.localizedDescription
    }
  }
  func submit(
    audio: Data, preset: Preset, snapshot: Configuration, destination: InsertionTarget?
  ) {
    let run = generation
    runSubmission(preset: preset, snapshot: snapshot, destination: destination) { [self] in
      let connection = try self.client(snapshot)
      return try await connection.dictate(
        audio: audio, preset: preset, dictionary: snapshot.dictionary,
        events: { [weak self] in await self?.receive($0, run: run) }, progress: { _ in })
    }
  }
  private func runSubmission(
    preset: Preset, snapshot: Configuration, destination: InsertionTarget?,
    operation: @escaping @MainActor () async throws -> DictationResult
  ) {
    state = .processing
    phase = .transcribing
    message = phase.label
    let run = generation
    requestTask = Task { [weak self] in
      guard let self else { return }
      do {
        let released = ProcessInfo.processInfo.systemUptime
        var output = try await operation()
        if output.timing == nil { output.timing = DictationTiming() }
        output.timing?.afterRelease = ProcessInfo.processInfo.systemUptime - released
        try Task.checkCancellation()
        guard generation == run else { return }
        result = output.text
        preview = output.raw
        draft = ""
        timing = output.timing
        canRecoverRaw = false
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
        if snapshot.retainHistory
          && !output.raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        {
          let entry = HistoryEntry(
            preset: preset.name, model: preset.model, application: destination?.bundleID ?? "",
            raw: output.raw, text: output.text, status: status)
          await recordHistory(entry, snapshot: snapshot)
        }
        guard generation == run else { return }
        if !snapshot.retainAudio { retryAudio = nil }
        state = .idle
        phase =
          output.raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? .noSpeech : .ready
        if phase == .noSpeech { message = phase.label }
        if status == "paste_sent" || phase == .noSpeech { dismissOverlayLater() }
      } catch {
        guard generation == run else { return }
        state = .idle
        phase = .failed
        message = error.localizedDescription
        if let failure = error as? DictationError, let original = failure.rawTranscript {
          preview = original
          canRecoverRaw = !original.isEmpty
          if !snapshot.retainAudio { retryAudio = nil }
        }
        let transcriptionFailed = result.isEmpty
        if snapshot.retainHistory && transcriptionFailed {
          let entry = HistoryEntry(
            preset: preset.name, model: preset.model, application: destination?.bundleID ?? "",
            raw: preview, text: "",
            status: "error: " + String(error.localizedDescription.prefix(2048)))
          await recordHistory(entry, snapshot: snapshot)
        }
      }
    }
  }
  private func receive(_ event: DictationProgress, run: UUID) {
    guard generation == run else { return }
    preview = event.raw
    draft = event.draft
    phase = state == .recording ? .listening : event.phase
    message = phase.label
  }
  private func updateOverlay() {
    overlay?.show(
      phase: phase, raw: phase == .ready ? result : preview, draft: draft, message: message,
      note: processingWarning,
      recover: canRecoverRaw)
  }
  func dismissFeedback() {
    if state == .idle { overlay?.hide() } else { cancel() }
  }
  private func dismissOverlayLater() {
    overlayTask?.cancel()
    overlayTask = Task { [weak self] in
      try? await Task.sleep(for: .seconds(2))
      guard !Task.isCancelled, self?.state == .idle else { return }
      self?.overlay?.hide()
    }
  }
  func insertOriginal() {
    guard state == .idle, canRecoverRaw, !preview.isEmpty else { return }
    let original = preview
    let captured = target
    let run = generation
    state = .processing
    Task {
      do {
        guard let captured else {
          throw DictationError.invalid("No original text field. Copy the transcript instead.")
        }
        try await captured.paste(original)
        guard generation == run else { return }
        state = .idle
        result = original
        canRecoverRaw = false
        phase = .ready
        message = "Original transcript pasted."
        dismissOverlayLater()
      } catch {
        guard generation == run else { return }
        state = .idle
        phase = .failed
        message = error.localizedDescription
      }
    }
  }
  func retryCleanup() {
    guard state == .idle, !working, canRecoverRaw else { return }
    do {
      var snapshot = try config.validated()
      let preset =
        snapshot.presets.first(where: { $0.id == retryPreset?.id }) ?? snapshot.preset(for: nil)
      guard preset.usesNamedTranscriber, preset.runsCleanup else {
        throw DictationError.invalid(
          "Choose a named transcriber and cleanup generator before retrying cleanup.")
      }
      let original = preview
      snapshot.autoPaste = false
      generation = UUID()
      let run = generation
      runSubmission(preset: preset, snapshot: snapshot, destination: nil) { [self] in
        try await self.client(snapshot).cleanupText(
          original, preset: preset, dictionary: snapshot.dictionary,
          events: { [weak self] in await self?.receive($0, run: run) })
      }
      phase = .cleaning
      message = phase.label
    } catch { message = error.localizedDescription }
  }
  func refreshModels() {
    let snapshot = config
    Task { [weak self] in
      do {
        guard let self else { return }
        let data = try await client(snapshot).models()
        let catalog = try JSONDecoder().decode(ModelCatalog.self, from: Data(data.utf8))
        guard config.server == snapshot.server else { return }
        models = catalog.data
      } catch { /* Explicit Discover models surfaces connection/readiness errors. */  }
    }
  }
  func connectionChanged() {
    models = []
    token = Credentials.read(server: config.server)
    refreshModels()
  }
  func processingDescription(_ preset: Preset) -> String {
    do {
      return try ProcessingPlan(preset: preset, model: models.first { $0.id == preset.model })
        .explanation
    } catch { return error.localizedDescription }
  }
  func cancel() {
    generation = UUID()
    requestTask?.cancel()
    previewTask?.cancel()
    cadenceTask?.cancel()
    if let live {
      let pump = previewTask
      let active = requestTask
      Task {
        await pump?.value
        await active?.value
        await live.cancel()
      }
    }
    live = nil
    overlayTask?.cancel()
    canRecoverRaw = false
    draft = ""
    phase = .canceled
    overlay?.hide()
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
  private func updateHistoryStatus(_ status: HistoryOutbox.Status) {
    historySyncStatus =
      status.error.map { "\(status.pending) pending · " + $0 }
      ?? (status.pending == 0 ? "History synced." : "\(status.pending) pending upload(s).")
  }
  /// Enqueue waits for durable local persistence only, never for the remote upload.
  func recordHistory(_ entry: HistoryEntry, snapshot: Configuration) async {
    guard config.retainHistory else { return }
    history.insert(entry, at: 0)
    history = Array(history.prefix(500))
    historyRevision += 1
    let revision = historyRevision
    let archive = history
    guard let outbox else {
      historySyncStatus = "History sync unavailable; transcript remains in this session."
      return
    }
    do {
      try await outbox.storeArchive(archive, revision: revision)
      guard config.retainHistory else { return }
      try await outbox.enqueue(
        entry, destination: HistoryDestination(server: snapshot.server, table: snapshot.table))
    } catch { historySyncStatus = "Local history / sync error: " + error.localizedDescription }
  }
  private func runHistory(_ action: @escaping @MainActor () async throws -> Void) {
    guard !historyWorking else { return }
    historyWorking = true
    Task {
      defer { historyWorking = false }
      do { try await action() } catch { historySyncStatus = error.localizedDescription }
    }
  }
  func search() {
    runHistory {
      let snapshot = try self.config.validated()
      self.searchResult = try await self.client(snapshot).search(
        self.searchText, table: snapshot.table)
    }
  }
  func syncHistory() {
    runHistory {
      let snapshot = try self.config.validated()
      guard let outbox = self.outbox else {
        throw DictationError.invalid("History sync unavailable.")
      }
      for entry in self.history {
        try await outbox.enqueue(
          entry, destination: HistoryDestination(server: snapshot.server, table: snapshot.table))
      }
      await outbox.resume()
    }
  }
  func clearHistory() {
    runHistory {
      let snapshot = try self.config.validated()
      let ids = Set(self.history.map(\.id))
      let pauseToken = await self.outbox?.pause()
      do {
        try await self.client(snapshot).deleteHistory(ids: Array(ids), table: snapshot.table)
        try await self.outbox?.discard(
          ids: ids, destination: HistoryDestination(server: snapshot.server, table: snapshot.table))
        self.history.removeAll { ids.contains($0.id) }
        self.historyRevision += 1
        try await self.outbox?.storeArchive(self.history, revision: self.historyRevision)
        if let pauseToken { await self.outbox?.resume(pauseToken) }
      } catch {
        if let pauseToken { await self.outbox?.resume(pauseToken) }
        throw error
      }
    }
  }
  func clearLocalHistory() {
    history = []
    historyRevision += 1
    let revision = historyRevision
    Task {
      do { try await outbox?.storeArchive([], revision: revision) } catch {
        historySyncStatus = error.localizedDescription
      }
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
