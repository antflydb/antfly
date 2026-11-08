// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import Combine
import DictationCore
import Sparkle
import SwiftUI

@MainActor final class AppUpdates: NSObject, ObservableObject, SPUUpdaterDelegate {
  @Published private(set) var canCheck = false
  @Published private(set) var enabled = false
  @Published private(set) var status = "Updates are disabled in development builds."
  private var controller: SPUStandardUpdaterController?
  private var observations: Set<AnyCancellable> = []
  private weak var store: Store?
  private var configuration: UpdateConfiguration?
  private var installHandler: (() -> Void)?
  private var installing = false
  private var installAttempt = UUID()
  private var installationTask: Task<Void, Never>?
  private let defaults: UserDefaults
  init(defaults: UserDefaults = .standard) {
    self.defaults = defaults
    super.init()
  }
  var automaticallyChecks: Bool {
    get { controller?.updater.automaticallyChecksForUpdates ?? false }
    set {
      objectWillChange.send()
      controller?.updater.automaticallyChecksForUpdates = newValue
    }
  }
  var automaticallyDownloads: Bool {
    get { controller?.updater.automaticallyDownloadsUpdates ?? false }
    set {
      objectWillChange.send()
      controller?.updater.automaticallyDownloadsUpdates = newValue
    }
  }
  var includeBeta: Bool {
    get {
      defaults.object(forKey: "AntflyBetaUpdates") as? Bool ?? configuration?.initialBeta ?? false
    }
    set {
      objectWillChange.send()
      defaults.set(newValue, forKey: "AntflyBetaUpdates")
      controller?.updater.resetUpdateCycle()
    }
  }
  func start(store: Store) {
    guard self.store == nil else { return }
    self.store = store
    store.$state.combineLatest(store.$working, store.$historyWorking, store.$capturingShortcut)
      .combineLatest(store.$shortcutSetupPending)
      .sink { [weak self] _ in
        // @Published emits before assigning. Inspect the settled state on the
        // next actor turn so a transition into recording cannot look idle.
        Task { @MainActor in self?.resumeInstallationIfIdle() }
      }.store(in: &observations)
    let bundle = Bundle.main
    let applications =
      FileManager.default.urls(for: .applicationDirectory, in: .localDomainMask)
      + FileManager.default.urls(for: .applicationDirectory, in: .userDomainMask)
    let installed = applications.contains {
      bundle.bundleURL.deletingLastPathComponent().standardizedFileURL == $0.standardizedFileURL
    }
    guard let config = UpdateConfiguration(info: bundle.infoDictionary ?? [:], installed: installed)
    else {
      if bundle.object(forInfoDictionaryKey: "AntflyUpdatesEnabled") as? Bool == true {
        status = "Install this signed release in Applications to enable updates."
      }
      return
    }
    configuration = config
    if defaults.object(forKey: "AntflyBetaUpdates") == nil {
      defaults.set(config.initialBeta, forKey: "AntflyBetaUpdates")
    }
    let controller = SPUStandardUpdaterController(
      startingUpdater: false, updaterDelegate: self, userDriverDelegate: nil)
    self.controller = controller
    controller.updater.publisher(for: \.canCheckForUpdates).assign(to: &$canCheck)
    enabled = true
    status = "Updates preserve your settings, dictionary, credentials, and retained history."
    controller.startUpdater()
  }
  func check() { controller?.checkForUpdates(nil) }
  func feedURLString(for updater: SPUUpdater) -> String? {
    (includeBeta ? configuration?.betaFeed : configuration?.stableFeed)?.absoluteString
  }
  func updater(_ updater: SPUUpdater, mayPerform updateCheck: SPUUpdateCheck) throws {
    guard idle else {
      throw NSError(
        domain: "dev.antfly.dictation.updates", code: 1,
        userInfo: [
          NSLocalizedDescriptionKey: "Finish dictation or setup before checking for updates."
        ])
    }
  }
  func updater(
    _ updater: SPUUpdater, shouldPostponeRelaunchForUpdate item: SUAppcastItem,
    untilInvokingBlock installHandler: @escaping () -> Void
  ) -> Bool {
    postponeInstallation(installHandler)
    return true
  }
  func postponeInstallation(_ installHandler: @escaping () -> Void) {
    guard !installing else { return }
    installAttempt = UUID()
    self.installHandler = installHandler
    status = "Update ready. Installation will wait until dictation and setup finish."
    // Avoid reentering Sparkle from its delegate call, even when already idle.
    Task { [weak self] in self?.resumeInstallationIfIdle() }
  }
  func updater(_ updater: SPUUpdater, didAbortWithError error: Error) {
    abortInstallation(error)
  }
  @discardableResult func abortInstallation(_ error: Error) -> Task<Void, Never>? {
    installAttempt = UUID()
    installHandler = nil
    status = "Update unavailable: " + error.localizedDescription
    guard installing else { return nil }
    let pending = installationTask
    return Task { [self] in
      // A failed/canceled install must not revive the app while its shutdown
      // drain is still running, nor invoke a stale Sparkle installation block.
      await pending?.value
      store?.resumeAfterFailedUpdate()
      installing = false
    }
  }
  private var idle: Bool {
    guard let store else { return false }
    return store.state == .idle && !store.working && !store.historyWorking
      && !store.capturingShortcut && !store.shortcutSetupPending
  }
  private func resumeInstallationIfIdle() {
    guard idle, !installing, let installHandler, let store else { return }
    installing = true
    self.installHandler = nil
    let attempt = installAttempt
    installationTask = Task { [self] in
      defer { installationTask = nil }
      await store.prepareToQuit()
      guard installAttempt == attempt else { return }
      installHandler()
    }
  }
}

struct UpdateSettings: View {
  @ObservedObject var updates: AppUpdates
  var body: some View {
    Section("App updates") {
      Text(
        "Version \(Bundle.main.object(forInfoDictionaryKey: "CFBundleShortVersionString") as? String ?? "Development")"
      )
      Button("Check for Updates…") { updates.check() }.disabled(!updates.canCheck)
      Toggle("Automatically check for updates", isOn: $updates.automaticallyChecks)
        .disabled(!updates.enabled)
      Toggle("Download updates automatically", isOn: $updates.automaticallyDownloads)
        .disabled(!updates.enabled)
      Toggle("Include beta releases", isOn: $updates.includeBeta).disabled(!updates.enabled)
      Text(updates.status).font(.caption).foregroundStyle(.secondary)
      Text("The Antfly server and models are managed separately. App updates do not replace them.")
        .font(.caption).foregroundStyle(.secondary)
    }
  }
}
