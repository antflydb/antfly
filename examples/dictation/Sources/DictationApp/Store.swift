// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AVFoundation
@preconcurrency import ApplicationServices
import DictationCore
import SwiftUI

@MainActor final class Store: ObservableObject {
  @Published var config = Configuration()
  let runtime: ManagedRuntime
  private var runtimeModeTask: Task<Void, Never>?
  private var modelSetupTask: Task<Void, Never>?
  @Published private(set) var settingUpModel = false
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
  @Published var token = "" { didSet { if !loadingCredential { credentialEdited = true } } }
  private var loadingCredential = false
  private var credentialOrigin: String?
  private var credentialEdited = false
  private func loadCredential(for origin: String) {
    loadingCredential = true
    token = credentialReader(origin)
    loadingCredential = false
    credentialOrigin = origin
    credentialEdited = false
  }
  @Published var working = false
  @Published var historySyncStatus = "History sync idle."
  @Published var historyWorking = false
  @Published private(set) var settingsLoadError: String?
  @Published private(set) var shortcutStatus = ""
  private let hotkey: (any ShortcutManaging)?
  private var overlay: RecordingOverlay?
  private var live: LiveDictation?
  private var plan: ProcessingPlan?
  private var liveFailure: String?
  private var overlayTask: Task<Void, Never>?
  private(set) var outbox: HistoryOutbox?
  private var deletionJournal: HistoryDeletionJournal?
  private var deletingIDs: Set<String> = []
  @Published private(set) var pendingHistoryDeletions = 0
  private(set) var historyDeletionTask: Task<Void, Never>?
  private var historyRevision: UInt64 = 0
  private var historyEpoch = UUID()
  private var appliedHistoryRetention = false
  private var terminating = false
  private(set) var historyPersistenceTask: Task<Void, Never>?
  private(set) var pendingHistoryEntries = 0
  private var pendingHistoryBytes = 0
  private var archiveOnlyQueued = false
  private var historyPersistenceWarning: String?
  private(set) var historySettingsTask: Task<Void, Never>?
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
  private(set) var audioArchiveTask: Task<Void, Never>?
  private let audioArchiveStage: @Sendable (Data, URL) async throws -> URL
  private var audioRetentionEpoch = UUID()
  private var audioArchiveID: UUID?
  private var audioArchiveRun: UUID?
  @MainActor private final class AudioArchiveRequest {
    let id = UUID()
    let run: UUID
    let epoch: UUID
    let encoding: Task<Data, Error>
    // Eligibility belongs to the request, independently of the current selection.
    // Once canceled, an older writer must never regain publication rights.
    private(set) var eligible = true
    func invalidate() { eligible = false }
    init(run: UUID, epoch: UUID, encoding: Task<Data, Error>) {
      self.run = run
      self.epoch = epoch
      self.encoding = encoding
    }
  }
  private var activeAudioArchive: AudioArchiveRequest?
  private var pendingAudioArchive: AudioArchiveRequest?
  private var retryRecording: PCMRecording?
  private var retryPreset: Preset?
  private let clientFactory: (@Sendable (Configuration) throws -> AntflyClient)?
  private let originalPaster:
    (@MainActor (String, @escaping @MainActor () throws -> Void) async throws -> Void)?
  private let clipboardRestoration: @MainActor () async -> Void
  private let directory: URL
  private let archive: HistoryArchive
  private var settingsFile: URL { directory.appendingPathComponent("settings.json") }
  private var historyFile: URL { directory.appendingPathComponent("history.json") }
  init(
    directory storageDirectory: URL? = nil, registerHotkey: Bool = true,
    historyUpload: HistoryOutbox.Upload? = nil,
    historyArchiveBeforeWrite: @escaping @Sendable () async throws -> Void = {},
    audioArchiveStage: @escaping @Sendable (Data, URL) async throws -> URL = {
      try await RecordingArchive.stage($0, in: $1)
    },
    clientFactory: (@Sendable (Configuration) throws -> AntflyClient)? = nil,
    shortcutManager: (any ShortcutManaging)? = nil,
    originalPaster: (
      @MainActor (String, @escaping @MainActor () throws -> Void) async throws -> Void
    )? = nil,
    clipboardRestoration: @escaping @MainActor () async -> Void = {
      await InsertionTarget.finishPendingPaste()
    },
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
    self.clipboardRestoration = clipboardRestoration
    self.originalPaster = originalPaster
    self.audioArchiveStage = audioArchiveStage
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
    runtime = ManagedRuntime(directory: directory)
    archive = HistoryArchive(
      file: directory.appendingPathComponent("history.json"), beforeWrite: historyArchiveBeforeWrite
    )
    hotkey = shortcutManager ?? (registerHotkey ? Hotkey() : nil)
    var loadedSettings = false
    do {
      try FileManager.default.createDirectory(
        at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
      try RecordingArchive.removeStaleCandidates(in: directory)
      if FileManager.default.fileExists(atPath: settingsFile.path) {
        config = try Configuration.decode(Data(contentsOf: settingsFile))
        loadedSettings = true
      }
      if loadedSettings, !config.retainHistory,
        FileManager.default.fileExists(atPath: historyFile.path)
      {
        try FileManager.default.removeItem(at: historyFile)
      }
      if loadedSettings, !config.retainAudio { try removeAudio() }
    } catch {
      settingsLoadError =
        "Settings could not be loaded. Retained data is preserved and uploads are paused. "
        + error.localizedDescription
    }
    if !loadedSettings, settingsLoadError == nil,
      ["history.json", "last-recording.wav", "history-deletions.json"].contains(where: {
        FileManager.default.fileExists(atPath: directory.appendingPathComponent($0).path)
      })
        || (try? FileManager.default.contentsOfDirectory(
          atPath: directory.appendingPathComponent("outbox").path
        ).isEmpty) == false
    {
      settingsLoadError =
        "Settings are missing. Retained data is preserved and uploads are paused. Restore your settings or explicitly save the desired retention preferences."
    }
    appliedHistoryRetention = loadedSettings && config.retainHistory
    if config.retainHistory, let data = try? Data(contentsOf: historyFile) {
      do {
        history = HistoryArchive.bounded(try JSONDecoder().decode([HistoryEntry].self, from: data))
      } catch {
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
      // Disabled retention is authoritative after a crash, before any queue resumes.
      if loadedSettings, !config.retainHistory {
        let queue = directory.appendingPathComponent("outbox")
        if FileManager.default.fileExists(atPath: queue.path) {
          try FileManager.default.removeItem(at: queue)
        }
      }
      outbox = try HistoryOutbox(
        directory: self.directory.appendingPathComponent("outbox"), paused: true, archive: archive,
        upload: historyUpload ?? { entry, destination in
          let token = await MainActor.run { Credentials.read(server: destination.server) }
          let password =
            destination.server == ManagedRuntime.origin
            ? await MainActor.run { Credentials.read(server: ManagedRuntime.credentialAccount) }
            : ""
          try await AntflyClient(
            server: destination.server, token: token,
            basicPassword: password.isEmpty ? nil : password
          ).saveHistory(
            entry, table: destination.table)
        }, changed: { [weak self] status in await self?.updateHistoryStatus(status) })
    } catch { historySyncStatus = "History sync unavailable: " + error.localizedDescription }
    do {
      let journal = try HistoryDeletionJournal(
        file: directory.appendingPathComponent("history-deletions.json"))
      deletionJournal = journal
      deletingIDs = journal.recoveredIDs
      pendingHistoryDeletions = deletingIDs.count
    } catch {
      historyPersistenceWarning =
        "History deletion journal unavailable; uploads remain paused. " + error.localizedDescription
      historySyncStatus = historyPersistenceWarning!
    }
    if loadedSettings, config.retainHistory, settingsLoadError == nil {
      let legacyDestination = HistoryDestination(
        server: config.usesManagedRuntime ? ManagedRuntime.origin : config.server,
        table: config.table)
      let epoch = historyEpoch
      enqueueHistoryWork {
        await self.restoreHistoryDestinations(legacyDestination: legacyDestination, epoch: epoch)
      }
    }
    if registerHotkey, !config.usesManagedRuntime { loadCredential(for: config.server) }
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
      runtime.changed = { [weak self] in self?.refreshModels() }
      if config.usesManagedRuntime { runtime.start() }
      refreshModels()
    }
  }
  private func client(_ snapshot: Configuration? = nil, token suppliedToken: String? = nil) throws
    -> AntflyClient
  {
    var settings = snapshot ?? config
    if let clientFactory { return try clientFactory(settings) }
    if settings.usesManagedRuntime {
      guard runtime.ready else { throw DictationError.invalid(runtime.status) }
      settings.server = ManagedRuntime.origin
      return try AntflyClient(configuration: settings, basicPassword: runtime.password)
    }
    if settings.server == ManagedRuntime.origin {
      let password = credentialReader(ManagedRuntime.credentialAccount)
      if !password.isEmpty {
        return try AntflyClient(configuration: settings, basicPassword: password)
      }
    }
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
      if let settingsLoadError { throw DictationError.invalid(settingsLoadError) }
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
    let recoveringSettings = settingsLoadError != nil
    if recoveringSettings, config.retainHistory,
      FileManager.default.fileExists(atPath: historyFile.path)
    {
      history = HistoryArchive.bounded(
        try JSONDecoder().decode([HistoryEntry].self, from: Data(contentsOf: historyFile)))
      historyRevision += 1
    }
    // Unloaded credentials are different from a token explicitly cleared by the user.
    if !config.usesManagedRuntime {
      if credentialOrigin != config.server && !credentialEdited {
        loadCredential(for: config.server)
      }
      try credentialWriter(token, config.server)
      credentialOrigin = config.server
      credentialEdited = false
    }
    try write(JSONEncoder().encode(config), to: settingsFile)
    var shortcutIssue: String?
    do { try registerShortcut() } catch {
      shortcutIssue = error.localizedDescription
      shortcutStatus = error.localizedDescription
    }
    settingsLoadError = nil
    if !config.retainHistory || !appliedHistoryRetention { historyEpoch = UUID() }
    appliedHistoryRetention = config.retainHistory
    let epoch = historyEpoch
    if !config.retainHistory {
      history = []
      historyRevision += 1
      let revision = historyRevision
      historySettingsTask = enqueueHistoryWork { [self] in
        await outbox?.setEnabled(false)
        do {
          if let outbox {
            try await outbox.discardAll()
          } else {
            // No worker exists when initialization failed. Atomically retire the
            // whole queue, including malformed jobs, before removing its files.
            try await archive.retireOutbox(at: directory.appendingPathComponent("outbox"))
          }
          try await archive.store([], revision: revision)
          historyPersistenceWarning = nil
        } catch { historySyncStatus = error.localizedDescription }
      }
    } else {
      historySettingsTask = enqueueHistoryWork { [self] in
        guard self.historyEpoch == epoch, self.config.retainHistory else { return }
        if recoveringSettings {
          await self.restoreHistoryDestinations(
            legacyDestination: HistoryDestination(
              server: self.config.usesManagedRuntime ? ManagedRuntime.origin : self.config.server,
              table: self.config.table), epoch: epoch)
        } else {
          try await self.resumeHistoryUploads()
        }
      }
    }
    if !config.retainAudio { try removeAudio() }
    return shortcutIssue
  }
  func save() {
    guard !terminating else { return }
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
    guard !terminating, state == .idle, !working, !capturingShortcut else { return }
    do {
      if let settingsLoadError { throw DictationError.invalid(settingsLoadError) }
      var snapshot = try config.validated()
      if snapshot.usesManagedRuntime, clientFactory == nil {
        guard runtime.ready else { throw DictationError.invalid(runtime.status) }
        snapshot.server = ManagedRuntime.origin
        snapshot.connectionMode = .external
      }
      guard AVCaptureDevice.authorizationStatus(for: .audio) == .authorized else {
        throw DictationError.invalid("Allow microphone access in Settings before recording.")
      }
      generation = UUID()
      overlayTask?.cancel()
      let run = generation
      target = InsertionTarget.capture()
      let preset = snapshot.preset(for: target?.bundleID)
      var effective = try ProcessingPlan(
        preset: preset, model: ModelCatalog(data: models).transcriber(preset.model))
      if snapshot.usesPressEnterCommand && effective.incrementalCleanup {
        effective.incrementalCleanup = false
        effective.explanation =
          "Recognition runs while you speak. Cleanup waits for release to identify a terminal press enter command."
      }
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
              dictionary: snapshot.dictionary, plan: effective,
              formatting: VoiceFormatting.Options(snapshot)
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
      // Share one conversion between archival and a recorded-upload fallback.
      let retainedEncoding = snapshot.retainAudio ? Task { try await recording.encoded() } : nil
      if let retainedEncoding { archiveAudio(retainedEncoding, run: run) }
      runSubmission(preset: preset, snapshot: snapshot, destination: target, releasedAt: releasedAt)
      { [self] in
        // Successful sessions need only the final PCM tail, never a full WAV copy.
        var audio: Data?
        await pump?.value
        try Task.checkCancellation()
        guard generation == run else { throw CancellationError() }
        if usesSession, let pipeline = live { return try await pipeline.finish(mic.buffer.drain()) }
        if usesSession && preset.processingStrategy == .incremental {
          throw DictationError.invalid(liveFailure ?? "Incremental processing failed.")
        }
        if let retainedEncoding {
          audio = try await retainedEncoding.value
        } else {
          audio = try await recording.encoded()
        }
        try Task.checkCancellation()
        guard generation == run else { throw CancellationError() }
        return try await client(snapshot).dictate(
          audio: audio!, preset: preset, dictionary: snapshot.dictionary,
          formatting: VoiceFormatting.Options(snapshot),
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
        formatting: VoiceFormatting.Options(snapshot),
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
        // Only recognition can request a key action; generated text cannot.
        let formatted: VoiceFormatting.Output
        if let processed = output.formatting {
          formatted = processed
        } else {
          let plan = VoiceFormatting.Plan(output.raw, options: VoiceFormatting.Options(snapshot))
          formatted = try plan.restore(output.text)
        }
        output.text = formatted.text
        let pressEnter = formatted.pressEnter
        result = output.text
        preview = output.raw
        draft = ""
        timing = output.timing
        canRecoverRaw = false
        var status = "ready"
        if snapshot.autoPaste
          && !output.text.isEmpty
        {
          if let destination {
            do {
              try await destination.paste(
                output.text, smartSpacing: snapshot.usesSmartSpacing,
                pressEnter: pressEnter,
                validate: {
                  try Task.checkCancellation()
                  guard self.generation == run else { throw CancellationError() }
                })
              guard generation == run, !Task.isCancelled else { return }
              status = pressEnter ? "enter_sent" : "paste_sent"
              message =
                pressEnter
                ? "Paste confirmed; Enter sent. Transcript remains available to copy."
                : "Paste sent. Transcript remains available to copy."
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
          scheduleHistory(entry, snapshot: snapshot)
        }
        guard generation == run else { return }
        if !snapshot.retainAudio { retryRecording = nil }
        state = .idle
        phase =
          output.raw.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? .noSpeech : .ready
        if phase == .noSpeech { message = phase.label }
        if status == "paste_sent" || status == "enter_sent" || phase == .noSpeech {
          dismissOverlayLater()
        }
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
          scheduleHistory(entry, snapshot: snapshot)
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
    requestTask = Task { [self] in
      do {
        let validate: @MainActor () throws -> Void = { [self] in
          try Task.checkCancellation()
          guard generation == run else { throw CancellationError() }
        }
        try validate()
        if let originalPaster {
          try await originalPaster(original, validate)
        } else {
          guard let captured else {
            throw DictationError.invalid("No original text field. Copy the transcript instead.")
          }
          try await captured.paste(original, validate: validate)
        }
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
          formatting: VoiceFormatting.Options(snapshot),
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
        if loadCredentials && !snapshot.usesManagedRuntime
          && credentialOrigin != snapshot.server && !credentialEdited
        {
          loadCredential(for: snapshot.server)
        }
        let data = try await client(snapshot, token: token).models()
        try Task.checkCancellation()
        let catalog = try JSONDecoder().decode(ModelCatalog.self, from: Data(data.utf8))
        guard discoveryGeneration == revision else { return }
        models = catalog.data
        if state == .idle, !working {
          config.selectAvailableManagedTranscriber(
            from: catalog, supportsApple: runtime.supportsAppleSpeech)
        }
      } catch { /* Explicit Discover models surfaces connection/readiness errors. */  }
    }
  }
  func runtimeModeChanged() {
    let previous = runtimeModeTask
    previous?.cancel()
    models = []
    runtimeModeTask = Task {
      await previous?.value
      guard !Task.isCancelled else { return }
      if config.usesManagedRuntime { runtime.start() } else { await runtime.stop() }
      guard !Task.isCancelled else { return }
      refreshModels()
    }
  }
  private func runModelSetup(_ action: @escaping @MainActor () async throws -> Void) {
    guard !working, !terminating, state == .idle, config.usesManagedRuntime else { return }
    working = true
    settingUpModel = true
    message = "Setting up local recognition…"
    modelSetupTask = Task {
      defer {
        if !terminating { working = false }
        settingUpModel = false
        modelSetupTask = nil
      }
      do {
        try Task.checkCancellation()
        try await action()
        try Task.checkCancellation()
        guard !terminating else { throw CancellationError() }
        _ = try persistSettings()
      } catch is CancellationError {
        message = "Model setup canceled."
        if !terminating, config.usesManagedRuntime { runtime.start() }
      } catch {
        message = error.localizedDescription
      }
    }
  }
  func cancelModelSetup() {
    modelSetupTask?.cancel()
    runtime.cancelDownload()
  }
  func installLocalWhisper() {
    runModelSetup {
      try await self.runtime.installWhisper()
      try Task.checkCancellation()
      for index in self.config.presets.indices {
        self.config.presets[index].automaticallySelectLocalTranscriber = false
        self.config.presets[index].model = "transcribers/whisper"
        self.config.presets[index].transcriberRoute = .gateway
        self.config.presets[index].cleanupModel = ""
      }
      self.message = "Whisper selected. The local engine is restarting."
    }
  }
  func useLocalApple() {
    guard runtime.supportsAppleSpeech else {
      message =
        "Native Apple providers are unavailable in the pinned Antfly release. Use Whisper or an external Apple-enabled server."
      return
    }
    runModelSetup {
      try await self.runtime.enableAppleAssets()
      try Task.checkCancellation()
      for index in self.config.presets.indices {
        self.config.presets[index].automaticallySelectLocalTranscriber = false
        self.config.presets[index].model = "transcribers/apple-speech"
        self.config.presets[index].transcriberRoute = .gateway
        self.config.presets[index].cleanupModel = ""
      }
      self.message = "Apple speech selected. The engine will report asset and locale readiness."
    }
  }
  func connectionChanged() {
    models = []
    loadingCredential = true
    token = ""
    loadingCredential = false
    credentialOrigin = nil
    credentialEdited = false
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
    if state != .idle {
      if activeAudioArchive?.run == generation { activeAudioArchive?.invalidate() }
      if pendingAudioArchive?.run == generation {
        pendingAudioArchive?.invalidate()
        pendingAudioArchive = nil
      }
      if audioArchiveRun == generation {
        // A completed older clip may resume ownership, but cancellation and
        // retention changes permanently revoke that request's eligibility.
        if let activeAudioArchive, activeAudioArchive.eligible,
          activeAudioArchive.epoch == audioRetentionEpoch, config.retainAudio
        {
          audioArchiveID = activeAudioArchive.id
          audioArchiveRun = activeAudioArchive.run
        } else {
          audioArchiveID = nil
          audioArchiveRun = nil
        }
      }
    }
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
  /// Finish durable local work on normal app termination. Remote uploads remain
  /// in the outbox for restart; quitting does not wait for network synchronization.
  func prepareToQuit() async {
    terminating = true
    working = true
    hotkey?.unregister()
    cancel()
    await clipboardRestoration()
    historyDeletionTask?.cancel()
    await historyDeletionTask?.value
    await historyPersistenceTask?.value
    await audioArchiveTask?.value
    runtimeModeTask?.cancel()
    await runtimeModeTask?.value
    cancelModelSetup()
    await modelSetupTask?.value
    await runtime.stop()
  }
  /// Sparkle may fail or the user may cancel after local work has drained.
  /// Keep the current installed app usable when no replacement was installed.
  func resumeAfterFailedUpdate() {
    guard terminating else { return }
    terminating = false
    working = false
    do { try registerShortcut() } catch { shortcutStatus = error.localizedDescription }
    if config.usesManagedRuntime { runtime.start() }
    message = "Update canceled or failed. The current app is ready to use."
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
          formatting: VoiceFormatting.Options(settings),
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
      let catalog = try JSONDecoder().decode(ModelCatalog.self, from: Data(self.discovery.utf8))
      self.models = catalog.data
      self.config.selectAvailableManagedTranscriber(
        from: catalog, supportsApple: self.runtime.supportsAppleSpeech)
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
      incoming.connectionMode = current.connectionMode
      incoming.server = current.server
      incoming.table = current.table
      incoming.profile = current.profile
      self.config = incoming
      if !incoming.usesManagedRuntime { self.loadCredential(for: incoming.server) }
      _ = try self.persistSettings()
      self.message = "Settings replaced from Antfly."
    }
  }
  private func updateHistoryStatus(_ status: HistoryOutbox.Status) {
    let current =
      status.error.map { "\(status.pending) pending · " + $0 }
      ?? (status.pending == 0 ? "History synced." : "\(status.pending) pending upload(s).")
    historySyncStatus = historyPersistenceWarning.map { $0 + " " + current } ?? current
  }
  /// All producers and retention transitions share one ordered owner. A Save
  /// invalidates producers immediately, then drains them before purging the queue.
  @discardableResult private func enqueueHistoryWork(
    _ action: @escaping @MainActor () async throws -> Void
  ) -> Task<Void, Never> {
    let previous = historyPersistenceTask
    let task = Task { [self] in
      await previous?.value
      do { try await action() } catch {
        historyPersistenceWarning = "Local history / sync error: " + error.localizedDescription
        historySyncStatus = historyPersistenceWarning!
      }
    }
    historyPersistenceTask = task
    return task
  }
  @discardableResult private func scheduleHistory(_ original: HistoryEntry, snapshot: Configuration)
    -> Task<Void, Never>?
  {
    guard config.retainHistory, settingsLoadError == nil,
      !deletingIDs.contains(original.id)
    else { return nil }
    var entry = original
    entry.destination =
      entry.destination
      ?? HistoryDestination(server: snapshot.effectiveServer, table: snapshot.table)
    let bounded = HistoryArchive.bounded([entry] + history)
    guard bounded.contains(where: { $0.id == entry.id }) else {
      historyPersistenceWarning =
        "Transcript exceeds the local history byte limit. It remains available to copy or export."
      historySyncStatus = historyPersistenceWarning!
      return nil
    }
    history = bounded
    historyRevision += 1
    let epoch = historyEpoch
    let bytes = HistoryArchive.estimatedBytes(entry)
    guard pendingHistoryEntries < 500, bytes <= 64 * 1024 * 1024 - pendingHistoryBytes else {
      historyPersistenceWarning =
        "History upload backlog is full. Keeping bounded local history; upload it manually after storage recovers."
      historySyncStatus = historyPersistenceWarning!
      // One coalesced archive job preserves local retention without accumulating
      // entry tasks or old snapshots while disk persistence is suspended.
      guard !archiveOnlyQueued else { return historyPersistenceTask }
      archiveOnlyQueued = true
      return enqueueHistoryWork { [self] in
        defer { archiveOnlyQueued = false }
        while config.retainHistory, settingsLoadError == nil {
          let revision = historyRevision
          try await archive.store(history, revision: revision)
          if revision == historyRevision { break }
        }
      }
    }
    pendingHistoryEntries += 1
    pendingHistoryBytes += bytes
    return enqueueHistoryWork { [self, entry] in
      defer {
        pendingHistoryEntries -= 1
        pendingHistoryBytes -= bytes
      }
      guard historyEpoch == epoch, config.retainHistory else { return }
      // Capture the latest bounded snapshot only when the worker runs. Queued
      // records coalesce archive rewrites rather than retaining old arrays.
      try await archive.store(history, revision: historyRevision)
      guard historyEpoch == epoch, config.retainHistory else { return }
      guard let outbox, deletionJournal != nil else {
        historySyncStatus = "Transcript saved locally; history sync unavailable."
        return
      }
      guard !deletingIDs.contains(entry.id) else { return }
      try await outbox.enqueue(entry, destination: entry.destination!)
    }
  }
  /// Explicit callers can await durability; recording delivery only schedules it.
  func recordHistory(_ entry: HistoryEntry, snapshot: Configuration) async {
    await scheduleHistory(entry, snapshot: snapshot)?.value
  }
  private func runHistory(_ action: @escaping @MainActor () async throws -> Void) {
    guard !historyWorking, !terminating else { return }
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
    let epoch = historyEpoch
    let entries = history
    runHistory {
      var snapshot = try self.config.validated()
      if snapshot.usesManagedRuntime { snapshot.server = ManagedRuntime.origin }
      guard self.settingsLoadError == nil, self.config.retainHistory else {
        throw DictationError.invalid(
          "Enable and save history retention before uploading local history.")
      }
      guard let outbox = self.outbox, self.deletionJournal != nil else {
        throw DictationError.invalid(
          "History sync or deletion journal unavailable. Uploads remain paused.")
      }
      let work = self.enqueueHistoryWork {
        for entry in entries {
          guard self.historyEpoch == epoch, self.config.retainHistory else { return }
          if self.deletingIDs.contains(entry.id) { continue }
          try await outbox.enqueue(
            entry,
            destination: entry.destination
              ?? HistoryDestination(server: snapshot.effectiveServer, table: snapshot.table))
        }
        guard self.historyEpoch == epoch, self.config.retainHistory else { return }
        self.historyPersistenceWarning = nil
        try await self.resumeHistoryUploads()
      }
      await work.value
    }
  }
  func clearHistory(retryOnly: Bool = false) {
    guard !historyWorking, !terminating else { return }
    historyWorking = true
    historyDeletionTask = Task {
      defer { historyWorking = false }
      do { try await deleteRetainedHistory(retryOnly: retryOnly) } catch {
        historySyncStatus = error.localizedDescription
      }
    }
  }
  /// Stop uploads before reading queue destinations. Cancel every matching job
  /// before issuing remote deletes, so a partial failure or restart cannot
  /// implicitly upload a transcript that the user asked to delete.
  func deleteRetainedHistory(retryOnly: Bool = false) async throws {
    let snapshot = try config.validated()
    let entries = retryOnly ? [] : history
    let entryIDs = Set(entries.map(\.id))
    guard let journal = deletionJournal else {
      throw DictationError.invalid("History deletion journal unavailable. Uploads remain paused.")
    }
    let existingIDs = await journal.ids()
    let ids = entryIDs.union(existingIDs)
    guard !ids.isEmpty else { return }
    guard let outbox else {
      throw DictationError.invalid(
        "History sync is unavailable. Repair the outbox before deleting remote history; no deletion has been reported as complete."
      )
    }
    if !retryOnly { historyEpoch = UUID() }
    var barrier: HistoryOutbox.Barrier?
    var preparationFailure: Error?
    // Serialize the write-ahead plan with retention changes. Hold the uploader
    // first, persist every destination, then wait for its dispatched response.
    let preparation = enqueueHistoryWork { [self] in
      do {
        historyRevision += 1
        try await archive.store(history, revision: historyRevision)
        let held = await outbox.holdForDeletion(ids: ids)
        barrier = held
        var groups: [HistoryDestination: Set<String>] = [:]
        for entry in entries where !existingIDs.contains(entry.id) {
          var destinations = held.destinations[entry.id] ?? []
          if let original = entry.destination { destinations.insert(original) }
          if destinations.isEmpty {
            destinations.insert(
              HistoryDestination(server: snapshot.effectiveServer, table: snapshot.table))
          }
          for destination in destinations { groups[destination, default: []].insert(entry.id) }
        }
        try await journal.begin(groups)
        deletingIDs = await journal.ids()
        pendingHistoryDeletions = deletingIDs.count
      } catch {
        preparationFailure = error
        if let barrier {
          await outbox.setEnabled(false)
          await outbox.resume(barrier.token)
        }
      }
    }
    await preparation.value
    if let preparationFailure { throw preparationFailure }
    guard let barrier else {
      throw DictationError.invalid("History deletion could not be prepared.")
    }
    let pauseToken = barrier.token
    await outbox.waitForBarrier(pauseToken)
    do {
      try await outbox.discard(ids: ids)
      for (destination, matching) in await journal.pending() {
        try Task.checkCancellation()
        var connection = snapshot
        connection.server = destination.server
        connection.connectionMode = .external
        connection.table = destination.table
        try await client(connection).deleteHistory(
          ids: Array(matching).sorted(), table: destination.table)
        try await journal.acknowledge(destination, ids: matching)
      }
      try Task.checkCancellation()
      var cleanupFailure: Error?
      // Local cleanup and journal completion are one ordered persistence job.
      // A new dictation may queue a later snapshot, but cannot supersede an
      // uncommitted clear or make its journal disappear after a failed write.
      let cleanup = enqueueHistoryWork { [self] in
        do {
          history.removeAll { ids.contains($0.id) }
          historyRevision += 1
          try await archive.store(history, revision: historyRevision)
          try await journal.complete(ids)
          deletingIDs = await journal.ids()
          pendingHistoryDeletions = deletingIDs.count
        } catch { cleanupFailure = error }
      }
      await cleanup.value
      if let cleanupFailure { throw cleanupFailure }
      await outbox.resume(pauseToken)
      historySyncStatus = "Matching history deleted from its original destinations."
    } catch {
      await outbox.resume(pauseToken)
      throw error
    }
  }
  private func resumeHistoryUploads() async throws {
    guard let journal = deletionJournal else {
      throw DictationError.invalid("History deletion journal unavailable. Uploads remain paused.")
    }
    guard let outbox else { return }
    let hold = await outbox.pause()
    do {
      deletingIDs = await journal.ids()
      pendingHistoryDeletions = deletingIDs.count
      try await outbox.discard(ids: deletingIDs)
      await outbox.resume()
      await outbox.resume(hold)
    } catch {
      // Release only our hold, preserving disabled admission on failure.
      await outbox.setEnabled(false)
      await outbox.resume(hold)
      throw error
    }
  }
  private func restoreHistoryDestinations(legacyDestination: HistoryDestination, epoch: UUID) async
  {
    let pending = await outbox?.destinations(for: Set(history.map(\.id))) ?? [:]
    guard config.retainHistory, historyEpoch == epoch else { return }
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
    guard config.retainHistory, historyEpoch == epoch else { return }
    do { try await resumeHistoryUploads() } catch { historySyncStatus = error.localizedDescription }
  }
  func clearLocalHistory() {
    guard !terminating else { return }
    history = []
    historyRevision += 1
    let revision = historyRevision
    enqueueHistoryWork { [self] in
      try await archive.store([], revision: revision)
    }
  }
  func archiveAudio(_ encoding: Task<Data, Error>, run: UUID) {
    guard generation == run, !terminating else { return }
    let request = AudioArchiveRequest(
      run: run, epoch: audioRetentionEpoch, encoding: encoding)
    audioArchiveID = request.id
    audioArchiveRun = request.run
    // One active write plus only the newest pending clip. Cancellation cannot
    // stop a synchronous write already inside the kernel, so it is not admission.
    pendingAudioArchive = request
    guard audioArchiveTask == nil else { return }
    audioArchiveTask = Task { [weak self] in await self?.drainAudioArchives() }
  }
  private func drainAudioArchives() async {
    defer {
      activeAudioArchive = nil
      audioArchiveTask = nil
    }
    while let request = pendingAudioArchive {
      pendingAudioArchive = nil
      activeAudioArchive = request
      do {
        let data = try await request.encoding.value
        guard request.eligible, audioArchiveID == request.id,
          audioRetentionEpoch == request.epoch, config.retainAudio
        else { continue }
        let candidate = try await audioArchiveStage(data, directory)
        defer { try? FileManager.default.removeItem(at: candidate) }
        guard request.eligible, audioArchiveID == request.id,
          audioRetentionEpoch == request.epoch, config.retainAudio
        else { continue }
        let file = directory.appendingPathComponent("last-recording.wav")
        if FileManager.default.fileExists(atPath: file.path) {
          _ = try FileManager.default.replaceItemAt(file, withItemAt: candidate)
        } else {
          try FileManager.default.moveItem(at: candidate, to: file)
        }
      } catch is CancellationError {
      } catch {
        guard request.eligible, generation == request.run, audioArchiveID == request.id,
          audioRetentionEpoch == request.epoch, config.retainAudio
        else { continue }
        processingWarning = "Audio archive unavailable: " + error.localizedDescription
      }
      activeAudioArchive = nil
    }
  }
  private func removeAudio() throws {
    audioRetentionEpoch = UUID()
    audioArchiveID = nil
    audioArchiveRun = nil
    activeAudioArchive?.invalidate()
    pendingAudioArchive?.invalidate()
    pendingAudioArchive = nil
    // The active writer remains owned until it returns and removes its candidate.
    // Retention fencing prevents publication, including during normal shutdown.
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
        if !imported.usesManagedRuntime { self.loadCredential(for: imported.server) }
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
