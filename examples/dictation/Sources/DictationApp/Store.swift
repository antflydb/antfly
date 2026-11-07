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
  @Published private(set) var shortcutStatus = ""
  private let hotkey: (any ShortcutManaging)?
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
  private(set) var generation = UUID()
  private(set) var discoveryTask: Task<Void, Never>?
  private var discoveryGeneration = UUID()
  private var registered: DictationCore.KeyboardShortcut?
  @Published var capturingShortcut = false
  @Published private(set) var shortcutSetupPending = false
  @Published private(set) var shortcutSetupStatus = ""
  private let keyboardAccess: @MainActor () -> Bool
  private let keyboardAccessRequest: @MainActor () -> Void
  private let settingsOpener: @MainActor (String) -> Void
  private let credentialReader: @MainActor (String) -> String
  private let credentialWriter: @MainActor (String, String) throws -> Void
  private var retryRecording: PCMRecording?
  private var retryPreset: Preset?
  private let clientFactory: (@Sendable (Configuration) throws -> AntflyClient)?
  private let directory: URL
  private let archive: HistoryArchive
  private var settingsFile: URL { directory.appendingPathComponent("settings.json") }
  private var historyFile: URL { directory.appendingPathComponent("history.json") }
  init(
    directory storageDirectory: URL? = nil, registerHotkey: Bool = true,
    historyUpload: HistoryOutbox.Upload? = nil,
    clientFactory: (@Sendable (Configuration) throws -> AntflyClient)? = nil,
    shortcutManager: (any ShortcutManaging)? = nil,
    credentialReader: @escaping @MainActor (String) -> String = { Credentials.read(server: $0) },
    credentialWriter: @escaping @MainActor (String, String) throws -> Void = {
      try Credentials.save($0, server: $1)
    },
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
    self.credentialReader = credentialReader
    self.credentialWriter = credentialWriter
    let directory =
      storageDirectory
      ?? FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
      .appendingPathComponent("AntflyDictation")
    self.directory = directory
    archive = HistoryArchive(file: directory.appendingPathComponent("history.json"))
    hotkey = shortcutManager ?? (registerHotkey ? Hotkey() : nil)
    do {
      try FileManager.default.createDirectory(
        at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
      try RecordingArchive.removeStaleCandidates(in: directory)
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
    } catch {
      shortcutStatus = error.localizedDescription
      message = error.localizedDescription
    }
    do {
      outbox = try HistoryOutbox(
        directory: self.directory.appendingPathComponent("outbox"), paused: true, archive: archive,
        upload: historyUpload ?? { entry, destination in
          let token = await MainActor.run { Credentials.read(server: destination.server) }
          try await AntflyClient(server: destination.server, token: token).saveHistory(
            entry, table: destination.table)
        }, changed: { [weak self] status in await self?.updateHistoryStatus(status) })
    } catch { historySyncStatus = "History sync unavailable: " + error.localizedDescription }
    if config.retainHistory {
      let legacyDestination = HistoryDestination(server: config.server, table: config.table)
      Task { await restoreHistoryDestinations(legacyDestination: legacyDestination) }
    }
    token = registerHotkey ? credentialReader(config.server) : ""
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
  private func client(_ snapshot: Configuration? = nil, token suppliedToken: String? = nil) throws
    -> AntflyClient
  {
    let settings = snapshot ?? config
    if let clientFactory { return try clientFactory(settings) }
    return try AntflyClient(
      configuration: settings, token: suppliedToken ?? credentialReader(settings.server))
  }
  private func registerShortcut() throws {
    let shortcut = try config.effectiveShortcut.validated()
    guard registered != shortcut || hotkey?.isAvailable == false else { return }
    try hotkey?.register(shortcut)
    registered = shortcut
    shortcutStatus = "Shortcut active: " + shortcut.label
  }
  func refreshShortcutAccess() {
    guard state == .idle, !capturingShortcut else { return }
    if shortcutSetupPending {
      if keyboardAccess() { completeShortcutSetup() }
      return
    }
    guard registered == nil || hotkey?.isAvailable == false, state == .idle, !capturingShortcut
    else { return }
    do {
      try registerShortcut()
      message = "Shortcut ready: " + config.effectiveShortcut.label
    } catch { shortcutStatus = error.localizedDescription }
  }
  func captureShortcut(_ capturing: Bool) {
    if capturing { cancelShortcutSetup() }
    capturingShortcut = capturing
    hotkey?.setSuspended(capturing)
  }

  func setUpShortcut() {
    guard state == .idle, !working, !capturingShortcut else { return }
    shortcutSetupPending = true
    if keyboardAccess() {
      completeShortcutSetup()
      return
    }
    shortcutSetupStatus =
      "Enable Antfly Dictation in Accessibility, then return here. Your selected shortcut will be applied automatically."
    message = shortcutSetupStatus
    keyboardAccessRequest()
    settingsOpener("com.apple.preference.security?Privacy_Accessibility")
  }
  func cancelShortcutSetup() {
    guard shortcutSetupPending else { return }
    shortcutSetupPending = false
    shortcutSetupStatus = "Shortcut setup canceled. The previous shortcut is unchanged."
  }
  func openKeyboardSettings() {
    settingsOpener("com.apple.Keyboard-Settings.extension")
  }
  private func completeShortcutSetup() {
    guard shortcutSetupPending, keyboardAccess() else { return }
    let previous = registered
    do {
      // Apply only the shortcut, preserving saved preferences and credentials.
      let previousData =
        FileManager.default.fileExists(atPath: settingsFile.path)
        ? try Data(contentsOf: settingsFile) : nil
      var saved = try previousData.map { try Configuration.decode($0) } ?? Configuration()
      let shortcut = try config.effectiveShortcut.validated()
      saved.effectiveShortcut = shortcut
      let bytes = try JSONEncoder().encode(saved.validated())
      if registered != shortcut || hotkey?.isAvailable == false { try hotkey?.register(shortcut) }
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
      config.effectiveShortcut = shortcut
      registered = shortcut
      shortcutStatus = "Shortcut active: " + shortcut.label
      shortcutSetupPending = false
      shortcutSetupStatus =
        "\(shortcut.label) is enabled and saved. \(config.toggle ? "Press" : "Hold") \(shortcut.label) to dictate."
      message = shortcutSetupStatus
    } catch {
      shortcutSetupPending = false
      shortcutSetupStatus = "Could not set up shortcut: " + error.localizedDescription
      message = shortcutSetupStatus
    }
  }

  private func write(_ data: Data, to url: URL) throws {
    try data.write(to: url, options: .atomic)
    try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
  }
  private func persistSettings() throws -> String? {
    config = try config.validated()
    try credentialWriter(token, config.server)
    try write(JSONEncoder().encode(config), to: settingsFile)
    var shortcutIssue: String?
    do { try registerShortcut() } catch {
      shortcutIssue = error.localizedDescription
      shortcutStatus = error.localizedDescription
    }
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
          try await archive.store([], revision: revision)
        } catch { historySyncStatus = error.localizedDescription }
      }
    } else {
      Task { await outbox?.resume() }
    }
    if !config.retainAudio { try removeAudio() }
    return shortcutIssue
  }
  func save() {
    perform {
      let issue = try self.persistSettings()
      self.message =
        "Settings saved locally." + (issue.map { " Shortcut needs attention: " + $0 } ?? "")
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
        preset: preset, model: ModelCatalog(data: models).transcriber(preset.model))
      plan = effective
      processingNote = effective.explanation
      processingWarning = ""
      if let error = ModelCatalog(data: models).transcriber(preset.model)?.dictation?
        .readiness_error
      {
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
      retryRecording = nil
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
            guard
              await recoverLiveFailure(
                error.localizedDescription, pipeline: live, run: run, preset: preset)
            else { return }
            while !Task.isCancelled && generation == run && state == .recording {
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
  /// The old pipeline can take time to close its network session. Never let its
  /// continuation mutate a later recording after that suspension.
  func recoverLiveFailure(_ detail: String, pipeline: LiveDictation?, run: UUID, preset: Preset)
    async -> Bool
  {
    guard generation == run, !Task.isCancelled else { return false }
    await pipeline?.cancel()
    guard generation == run, !Task.isCancelled else { return false }
    liveFailure = detail
    live = nil
    processingNote =
      preset.processingStrategy != .incremental
      ? "Live processing failed. The recorded clip will use the same transcriber on release."
      : "Incremental processing failed. Your recording remains available for retry."
    processingWarning = processingNote
    message = processingNote
    return true
  }
  func finish() {
    guard state == .recording, let mic = microphone, let preset = selected,
      let snapshot = recordingConfig
    else { return }
    let releasedAt = ProcessInfo.processInfo.systemUptime
    mic.stop()
    cadenceTask?.cancel()
    microphone = nil
    if plan?.session != true { previewTask?.cancel() }
    timeoutTask?.cancel()
    do {
      let recording = try mic.recording()
      guard !recording.samples.isEmpty else {
        throw DictationError.invalid("No microphone audio was captured.")
      }
      retryRecording = recording
      retryPreset = preset
      let pump = plan?.session == true ? previewTask : nil
      let usesSession = plan?.session == true
      let run = generation
      runSubmission(preset: preset, snapshot: snapshot, destination: target, releasedAt: releasedAt)
      { [self] in
        // Successful sessions need only the final PCM tail, never a full WAV copy.
        var audio: Data?
        if snapshot.retainAudio {
          audio = try await recording.encoded()
          try Task.checkCancellation()
          guard generation == run else { throw CancellationError() }
          let file = directory.appendingPathComponent("last-recording.wav")
          let candidate = try await RecordingArchive.stage(audio!, in: directory)
          defer { try? FileManager.default.removeItem(at: candidate) }
          try Task.checkCancellation()
          guard generation == run else { throw CancellationError() }
          if FileManager.default.fileExists(atPath: file.path) {
            _ = try FileManager.default.replaceItemAt(file, withItemAt: candidate)
          } else {
            try FileManager.default.moveItem(at: candidate, to: file)
          }
        }
        await pump?.value
        try Task.checkCancellation()
        guard generation == run else { throw CancellationError() }
        if usesSession, let pipeline = live { return try await pipeline.finish(mic.buffer.drain()) }
        if usesSession && preset.processingStrategy == .incremental {
          throw DictationError.invalid(liveFailure ?? "Incremental processing failed.")
        }
        if audio == nil { audio = try await recording.encoded() }
        try Task.checkCancellation()
        guard generation == run else { throw CancellationError() }
        return try await client(snapshot).dictate(
          audio: audio!, preset: preset, dictionary: snapshot.dictionary,
          events: { [weak self] in await self?.receive($0, run: run) }, progress: { _ in })
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
    releasedAt: TimeInterval? = nil,
    operation: @escaping @MainActor () async throws -> DictationResult
  ) {
    state = .processing
    result = ""
    canRecoverRaw = false
    phase = .transcribing
    message = phase.label
    let run = generation
    requestTask = Task { [weak self] in
      guard let self else { return }
      do {
        let released = releasedAt ?? ProcessInfo.processInfo.systemUptime
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
              guard generation == run, !Task.isCancelled else { return }
              status = "paste_sent"
              message = "Paste sent. Transcript remains available to copy."
            } catch {
              guard generation == run, !Task.isCancelled else { return }
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
        if !snapshot.retainAudio { retryRecording = nil }
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
          if !snapshot.retainAudio { retryRecording = nil }
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
  func refreshModels() { scheduleDiscovery(debounce: false, loadCredentials: false) }
  private func scheduleDiscovery(debounce: Bool, loadCredentials: Bool) {
    discoveryTask?.cancel()
    discoveryGeneration = UUID()
    let revision = discoveryGeneration
    var snapshot = config
    snapshot.server = snapshot.server.trimmingCharacters(in: .whitespacesAndNewlines)
    discoveryTask = Task { [weak self] in
      do {
        if debounce { try await Task.sleep(for: .milliseconds(350)) }
        try Task.checkCancellation()
        guard let self, discoveryGeneration == revision else { return }
        _ = try snapshot.validated()
        if loadCredentials && token.isEmpty { token = credentialReader(snapshot.server) }
        let data = try await client(snapshot, token: token).models()
        try Task.checkCancellation()
        let catalog = try JSONDecoder().decode(ModelCatalog.self, from: Data(data.utf8))
        guard discoveryGeneration == revision else { return }
        models = catalog.data
      } catch { /* Explicit Discover models surfaces connection/readiness errors. */  }
    }
  }
  func connectionChanged() {
    models = []
    token = ""
    scheduleDiscovery(debounce: true, loadCredentials: true)
  }
  func processingDescription(_ preset: Preset) -> String {
    do {
      return try ProcessingPlan(
        preset: preset, model: ModelCatalog(data: models).transcriber(preset.model)
      )
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
    retryRecording = nil
    message = "Canceled remaining work."
  }
  func retry() {
    guard state == .idle, !working, let recording = retryRecording, let original = retryPreset
    else {
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
      let run = generation
      runSubmission(preset: preset, snapshot: settings, destination: nil) { [self] in
        let audio = try await recording.encoded()
        try Task.checkCancellation()
        guard generation == run else { throw CancellationError() }
        return try await client(settings).dictate(
          audio: audio, preset: preset, dictionary: settings.dictionary,
          events: { [weak self] in await self?.receive($0, run: run) }, progress: { _ in })
      }
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
    discoveryTask?.cancel()
    discoveryGeneration = UUID()
    run {
      self.discovery = try await self.client(token: self.token).models()
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
      _ = try self.persistSettings()
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
      self.token = self.credentialReader(incoming.server)
      _ = try self.persistSettings()
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
    var entry = entry
    entry.destination =
      entry.destination ?? HistoryDestination(server: snapshot.server, table: snapshot.table)
    history.insert(entry, at: 0)
    history = Array(history.prefix(500))
    historyRevision += 1
    let revision = historyRevision
    let archive = history
    do {
      try await self.archive.store(archive, revision: revision)
      guard let outbox else {
        historySyncStatus = "Transcript saved locally; history sync unavailable."
        return
      }
      guard config.retainHistory else { return }
      try await outbox.enqueue(
        entry, destination: entry.destination!)
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
          entry,
          destination: entry.destination
            ?? HistoryDestination(server: snapshot.server, table: snapshot.table))
      }
      await outbox.resume()
    }
  }
  func clearHistory() {
    runHistory { try await self.deleteRetainedHistory() }
  }
  /// Stop uploads before reading queue destinations. Cancel every matching job
  /// before issuing remote deletes, so a partial failure or restart cannot
  /// implicitly upload a transcript that the user asked to delete.
  func deleteRetainedHistory() async throws {
    let snapshot = try config.validated()
    let entries = history
    let ids = Set(entries.map(\.id))
    guard !ids.isEmpty else { return }
    let pauseToken = await outbox?.pause()
    do {
      let pending = await outbox?.destinations(for: ids) ?? [:]
      var groups: [HistoryDestination: Set<String>] = [:]
      for entry in entries {
        var destinations = pending[entry.id] ?? []
        if let original = entry.destination { destinations.insert(original) }
        if destinations.isEmpty {
          // Legacy archives have no recoverable destination once an upload has
          // completed. Bind these once to the configuration that loaded them.
          destinations.insert(HistoryDestination(server: snapshot.server, table: snapshot.table))
        }
        for destination in destinations { groups[destination, default: []].insert(entry.id) }
      }
      try await outbox?.discard(ids: ids)
      for (destination, matching) in groups {
        var connection = snapshot
        connection.server = destination.server
        connection.table = destination.table
        try await client(connection).deleteHistory(
          ids: Array(matching).sorted(), table: destination.table)
      }
      history.removeAll { ids.contains($0.id) }
      historyRevision += 1
      try await archive.store(history, revision: historyRevision)
      if let pauseToken { await outbox?.resume(pauseToken) }
      historySyncStatus = "Matching history deleted from its original destinations."
    } catch {
      if let pauseToken { await outbox?.resume(pauseToken) }
      throw error
    }
  }
  private func restoreHistoryDestinations(legacyDestination: HistoryDestination) async {
    let pending = await outbox?.destinations(for: Set(history.map(\.id))) ?? [:]
    guard config.retainHistory else { return }
    var migrated = false
    for index in history.indices where history[index].destination == nil {
      history[index].destination =
        pending[history[index].id]?.sorted {
          ($0.server, $0.table) < ($1.server, $1.table)
        }.first ?? legacyDestination
      migrated = true
    }
    if migrated {
      historyRevision += 1
      do { try await archive.store(history, revision: historyRevision) } catch {
        historySyncStatus = "Could not migrate history destinations: " + error.localizedDescription
      }
    }
    guard config.retainHistory else { return }
    await outbox?.resume()
  }
  func clearLocalHistory() {
    history = []
    historyRevision += 1
    let revision = historyRevision
    Task {
      do { try await archive.store([], revision: revision) } catch {
        historySyncStatus = error.localizedDescription
      }
    }
  }
  private func removeAudio() throws {
    try RecordingArchive.removeStaleCandidates(in: directory)
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
        self.token = self.credentialReader(imported.server)
        _ = try self.persistSettings()
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
