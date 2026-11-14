// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

import AppKit
import DictationCore
import SwiftUI

@main enum DictationLauncher {
  static func main() {
    if CommandLine.arguments.dropFirst().first == "--managed-runtime-child" {
      executeManagedRuntimeChild(arguments: CommandLine.arguments)
    }
    if ["--managed-runtime", "--managed-runtime-pull"].contains(
      CommandLine.arguments.dropFirst().first ?? "")
    {
      exit(superviseManagedRuntime(arguments: CommandLine.arguments))
    }
    DictationApp.main()
  }
}

struct DictationApp: App {
  @NSApplicationDelegateAdaptor(DictationLifecycle.self) private var lifecycle
  @StateObject private var store = Store()
  @StateObject private var updates = AppUpdates()
  var body: some Scene {
    WindowGroup("Antfly Dictation", id: "settings") {
      SettingsView(store: store, updates: updates).frame(minWidth: 760, minHeight: 600)
        .onAppear {
          lifecycle.store = store
          store.enableAutomaticSaving()
          updates.start(store: store)
        }
    }
    .commands {
      CommandGroup(after: .appInfo) {
        Button("Check for Updates…") { updates.check() }.disabled(!updates.canCheck)
      }
    }
    MenuBarExtra {
      MenuContent(store: store, updates: updates).onAppear {
        lifecycle.store = store
        store.enableAutomaticSaving()
        updates.start(store: store)
      }
    } label: {
      Image(
        systemName: store.state == .recording
          ? "mic.fill" : store.state == .processing ? "ellipsis.circle" : "waveform")
    }
    .menuBarExtraStyle(.window)
  }
}

@MainActor final class DictationLifecycle: NSObject, NSApplicationDelegate {
  weak var store: Store?
  private var terminating = false
  func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
    guard let store else { return .terminateNow }
    guard !terminating else { return .terminateLater }
    terminating = true
    Task {
      await store.prepareToQuit()
      sender.reply(toApplicationShouldTerminate: true)
    }
    return .terminateLater
  }
}

struct MenuContent: View {
  @ObservedObject var store: Store
  @ObservedObject var updates: AppUpdates
  @Environment(\.openWindow) var openWindow
  var body: some View {
    VStack(alignment: .leading, spacing: 12) {
      Text("Antfly Dictation").font(.headline)
      Text(store.phase.label).font(.headline)
      Text(store.message).font(.caption).textSelection(.enabled)
      if store.config.usesManagedRuntime { RuntimeSummary(runtime: store.runtime) }
      if !store.processingWarning.isEmpty {
        Text(store.processingWarning).font(.caption).foregroundStyle(.secondary)
      }
      if !store.preview.isEmpty { Text(store.preview).lineLimit(8).textSelection(.enabled) }
      HStack {
        Button(store.state == .recording ? "Finish" : "Record") {
          if store.state == .recording { store.finish() } else { store.start() }
        }.disabled(store.state == .processing || store.isBusy)
        Button("Cancel") {
          if store.settingUpModel { store.cancelModelSetup() } else { store.cancel() }
        }.disabled(
          (store.state == .idle && !store.settingUpModel)
            || (store.settingUpModel && !store.canCancelModelSetup))
        Button("Copy") { store.copy(store.result.isEmpty ? store.preview : store.result) }.disabled(
          store.result.isEmpty && store.preview.isEmpty)
      }
      if store.canUseOriginalWhileCleaning {
        Button("Use original now") { store.useOriginalWhileCleaning() }
      }
      if store.canRecoverRaw {
        HStack {
          Button("Insert original") { store.insertOriginal() }
          Button("Retry cleanup") { store.retryCleanup() }
        }
      }
      Button("Open Antfly Dictation") {
        openWindow(id: "settings")
        NSApp.activate(ignoringOtherApps: true)
      }
      Button("Check for Updates…") { updates.check() }.disabled(!updates.canCheck)
      Button("Quit") {
        store.cancel()
        NSApp.terminate(nil)
      }
    }.padding().frame(width: 350)
  }
}

struct SettingsView: View {
  @ObservedObject var store: Store
  @ObservedObject var updates: AppUpdates
  @StateObject var navigation = SettingsNavigation()
  var body: some View {
    VStack(alignment: .leading, spacing: 12) {
      HStack {
        Image(nsImage: NSApplication.shared.applicationIconImage).resizable().scaledToFit()
          .frame(width: 28, height: 28).accessibilityLabel("Antfly Dictation")
        Text("Antfly Dictation").font(.headline)
        Spacer()
        Text(store.phase.label).foregroundStyle(.secondary)
      }
      if store.hasPendingSetupCommit {
        VStack(alignment: .leading, spacing: 8) {
          Text(store.modelSetup.message).textSelection(.enabled)
          HStack {
            Button("Retry saving settings") { store.savePendingSetup() }
            Button("Keep previous selection") { store.discardPendingSetup() }
          }.disabled(store.state != .idle || store.isBusy)
        }.padding(10).background(.quaternary, in: RoundedRectangle(cornerRadius: 8))
      }
      TabView(selection: $navigation.tab) {
        dictation.tabItem { Label("Dictation", systemImage: "waveform") }.tag(0)
        presets.tabItem { Label("Presets", systemImage: "slider.horizontal.3") }.tag(1)
        history.tabItem {
          Label(store.config.retainHistory ? "History" : "History · Off", systemImage: "clock")
        }.tag(2)
        settings.tabItem { Label("Settings", systemImage: "gearshape") }.tag(3)
      }
      HStack {
        Text(store.message).font(.callout).textSelection(.enabled).lineLimit(2)
        Spacer()
        if store.savingSettings { ProgressView().controlSize(.small) }
        Text(retentionChangesPending ? "Retention changes pending" : store.settingsSaveStatus)
          .font(.caption).foregroundStyle(.secondary)
        if store.settingsSaveError != nil || store.settingsLoadError != nil {
          Button("Review preferences") { navigation.tab = 3 }
        }
        if store.state != .idle || store.canCancelModelSetup {
          Button("Cancel") {
            if store.settingUpModel { store.cancelModelSetup() } else { store.cancel() }
          }.disabled(store.settingUpModel && !store.canCancelModelSetup)
        }
      }
    }.padding(16)
      .onChange(of: store.config.retainHistory) { _, _ in navigation.historyRetention = nil }
      .onChange(of: store.config.retainAudio) { _, _ in navigation.audioRetention = nil }
      .confirmationDialog("Apply retention changes?", isPresented: $navigation.confirmRetention) {
        Button("Apply retention changes", role: .destructive) {
          store.config.retainHistory = navigation.historyRetention ?? store.config.retainHistory
          store.config.retainAudio = navigation.audioRetention ?? store.config.retainAudio
          navigation.historyRetention = nil
          navigation.audioRetention = nil
          store.save()
        }
        .disabled(store.settingsLocked || store.state != .idle || store.capturingShortcut)
        Button("Cancel", role: .cancel) {
          navigation.historyRetention = nil
          navigation.audioRetention = nil
        }
      } message: {
        Text(
          "Turning retention off removes retained local data. Existing remote history is kept until you explicitly delete it. Other pending preferences will also be saved."
        )
      }
      .confirmationDialog("Import and replace preferences?", isPresented: $navigation.confirmImport)
    {
      Button("Choose preferences file…", role: .destructive) { store.importSettings() }
    } message: {
      Text(
        "Imported preferences replace local choices, including retention. Turning retention off removes retained local data."
      )
    }
      .confirmationDialog(
        "Publish preferences snapshot?", isPresented: $navigation.confirmPublication
      ) {
        Button("Publish preferences") { store.publish() }
          .disabled(store.settingsLocked || store.state != .idle || store.capturingShortcut)
      } message: {
        Text(
          "This overwrites the selected remote profile with your current preferences. Credentials are never published."
        )
      }
      .confirmationDialog(
        "Replace preferences from Antfly?", isPresented: $navigation.confirmReplacement
      ) {
        Button("Replace preferences", role: .destructive) { store.download() }
      } message: {
        Text(
          "This replaces local presets and preferences, including retention choices. Turning retention off removes retained local data."
        )
      }
      .confirmationDialog(
        "Delete local and matching server history?", isPresented: $navigation.confirmDeletion
      ) {
        Button("Delete history", role: .destructive) { store.clearHistory() }
      } message: {
        Text(
          "Matching documents are deleted from their original Antfly destinations. This cannot be undone."
        )
      }
      .confirmationDialog("Clear local history?", isPresented: $navigation.confirmLocalDeletion) {
        Button("Clear local history", role: .destructive) { store.clearLocalHistory() }
      } message: {
        Text("Server history is kept. This cannot be undone.")
      }
  }
  var dictation: some View {
    VStack(alignment: .leading, spacing: 12) {
      SetupChecklist(store: store, runtime: store.runtime)
      HStack {
        Picker("Preset", selection: $store.config.selectedPreset) {
          ForEach(store.config.presets) { Text($0.name).tag($0.id) }
        }.frame(maxWidth: 260).disabled(store.state != .idle || store.settingsLocked)
        Text(
          store.config.toggle
            ? "Press " + store.config.effectiveShortcut.label + " to dictate"
            : "Hold " + store.config.effectiveShortcut.label + " to speak"
        )
        .foregroundStyle(.secondary)
        Spacer()
        Button(store.state == .recording ? "Finish dictation" : "Test dictation") {
          if store.state == .recording { store.finish() } else { store.start() }
        }.disabled(store.state == .processing || store.isBusy || store.selectedPresetNeedsProcessor)
          .help(
            "Test here, or focus a text field in another app and use your shortcut to paste there.")
      }
      Text(store.activePresetSummary).font(.caption).foregroundStyle(.secondary)
      if store.selectedPresetNeedsProcessor {
        VStack(alignment: .leading, spacing: 8) {
          Label(
            store.config.preset(for: nil).name + " needs a text processor",
            systemImage: "exclamationmark.triangle"
          ).foregroundStyle(.orange)
          Text(
            "Clean up and Rewrite need a text processor. Verbatim keeps recognized wording without one."
          )
          .font(.callout)
          HStack {
            Button("Choose processor") { navigation.tab = 1 }
            Button("Use Verbatim") { store.useVerbatimForSelectedPreset() }
          }.disabled(store.settingsLocked || store.state != .idle)
        }.padding(12).frame(maxWidth: .infinity, alignment: .leading)
          .background(.quaternary, in: RoundedRectangle(cornerRadius: 8))
      }
      transcript
    }.padding(.top, 8)
  }
  var retentionChangesPending: Bool {
    store.requiresRetentionConfirmation
      || (navigation.historyRetention.map { $0 != store.config.retainHistory } ?? false)
      || (navigation.audioRetention.map { $0 != store.config.retainAudio } ?? false)
  }
  var preferenceStatus: some View {
    VStack(alignment: .leading, spacing: 8) {
      if let error = store.settingsLoadError {
        Text(error).foregroundStyle(.red).textSelection(.enabled)
        Button("Save recovered preferences…") { navigation.confirmRetention = true }
      }
      if retentionChangesPending {
        Text("Retention changes take effect after Apply. Other preferences save automatically.")
          .font(
            .caption)
        Button("Apply retention changes…") { navigation.confirmRetention = true }
      }
      if let error = store.settingsSaveError {
        Text(error).foregroundStyle(.red).textSelection(.enabled)
        if !store.requiresRetentionConfirmation && store.settingsLoadError == nil {
          Button("Retry saving") { store.save() }
        }
      }
    }.disabled(store.state != .idle || store.settingsLocked || store.capturingShortcut)
  }
  var settings: some View {
    ScrollView {
      Form {
        if store.settingsLoadError != nil || retentionChangesPending
          || store.settingsSaveError != nil
        {
          preferenceStatus
        }
        Picker("Settings", selection: $navigation.page) {
          ForEach(["General", "Speech & text", "Shortcuts & permissions", "Advanced"], id: \.self) {
            Text($0).tag($0)
          }
        }.pickerStyle(.segmented)
        if navigation.page == "Speech & text" || navigation.page == "Advanced" {
          Section("Speech engine — where your audio is processed") {
            Picker(
              "Processing location",
              selection: Binding(
                get: { store.config.connectionMode ?? .external },
                set: { store.selectConnectionMode($0) })
            ) {
              Text("On this Mac (bundled Antfly)").tag(ConnectionMode.managed)
              Text("External Antfly server").tag(ConnectionMode.external)
            }.disabled(store.state != .idle || store.isBusy || store.historyWorking)

            if store.config.usesManagedRuntime {
              RuntimeControls(
                store: store, runtime: store.runtime, showAdvanced: navigation.page == "Advanced")
            } else {
              TextField("Server origin", text: $store.config.server).onChange(
                of: store.config.server
              ) {
                _, _ in store.connectionChanged()
              }
              SecureField("Bearer token (Keychain only)", text: $store.token)
            }
            if navigation.page == "Advanced" {
              HStack {
                Button("Discover models") { store.discover() }
                Button("Create storage table") { store.setup() }
              }
              Toggle(
                "Allow parallel recognition and cleanup",
                isOn: Binding(
                  get: { store.config.concurrentRequests == 2 },
                  set: { store.config.concurrentRequests = $0 ? 2 : 1 }))
              Text(
                "Requires at least two inference slots on the server. Leave off for a single-slot local server."
              ).font(.caption).foregroundStyle(.secondary)
              TextField("Antfly table", text: $store.config.table)
              TextField("Settings profile", text: $store.config.profile)
              HStack {
                Button("Publish preferences snapshot…") { navigation.confirmPublication = true }
                Button("Replace preferences from Antfly…") { navigation.confirmReplacement = true }
              }
              Text(
                "Snapshots use explicit publish/replace. Local changes win until you replace them; publishing overwrites the selected remote profile. Credentials are never exported."
              ).font(.caption).foregroundStyle(.secondary)
            }
          }
        }
        if navigation.page == "Shortcuts & permissions" {
          Section("Microphone & shortcut") {
            Picker("Microphone", selection: $store.config.microphoneID) {
              Text("System default").tag("")
              ForEach(Microphone.devices, id: \.uniqueID) {
                Text($0.localizedName).tag($0.uniqueID)
              }
            }
            HStack {
              Button("Allow microphone") { store.requestMicrophone() }
            }
            ShortcutSettings(store: store)
            Toggle("Press to toggle recording instead of hold to talk", isOn: $store.config.toggle)
            Toggle("Paste into the original focused text field", isOn: $store.config.autoPaste)
          }
        }
        if navigation.page == "Speech & text" {
          Section("Text processing") {
            Toggle(
              "Prepare Apple text processor in advance",
              isOn: Binding(
                get: { store.config.usesCleanupPrewarming },
                set: { store.config.prewarmCleanup = $0 }))
            Text(
              "Uses a short local request when an Apple text processor is selected. No recording or transcript is sent. External processors are never prewarmed."
            ).font(.caption)
            if !store.cleanupWarmupStatus.isEmpty {
              Text(store.cleanupWarmupStatus).font(.caption).foregroundStyle(.secondary)
            }
          }
          Section("Voice formatting") {
            Toggle(
              "Spoken line and paragraph breaks",
              isOn: Binding(
                get: { store.config.usesVoiceFormatting },
                set: { store.config.voiceFormatting = $0 })
            )
            Text(
              "Say new line, next line, line break, skip a line, or new paragraph. Works without a cleanup model. Quote a phrase to keep it literal. For fully literal dictation, choose Verbatim and turn this off."
            )
            .font(.caption).foregroundStyle(.secondary)
            Toggle(
              "Add spaces around inserted dictation",
              isOn: Binding(
                get: { store.config.usesSmartSpacing }, set: { store.config.smartSpacing = $0 }))
            Toggle(
              "Allow a trailing ‘press enter’ voice command",
              isOn: Binding(
                get: { store.config.usesPressEnterCommand },
                set: { store.config.pressEnterCommand = $0 }))
            Text(
              "Off by default. Ending dictation with press enter can send a message or submit a prompt. Enter is sent only after insertion is confirmed in the original text field."
            )
            .font(.caption).foregroundStyle(.secondary)
          }
        }
        if navigation.page == "General" {
          UpdateSettings(updates: updates)
          Section("Retention") {
            Toggle(
              "Save transcript history locally and in Antfly",
              isOn: Binding(
                get: { navigation.historyRetention ?? store.config.retainHistory },
                set: { navigation.historyRetention = $0 }))
            Toggle(
              "Keep last audio recording locally",
              isOn: Binding(
                get: { navigation.audioRetention ?? store.config.retainAudio },
                set: { navigation.audioRetention = $0 }))
            Text(
              "History is off by default. Audio is never stored in Antfly. Retention changes take effect after you confirm Apply. Turning retention off removes local retained data; remote history requires explicit deletion."
            ).font(.caption)
          }
        }
        if navigation.page == "Advanced" {
          Section("Import & export") {
            Button("Import preferences JSON…") { navigation.confirmImport = true }
            Button("Export preferences JSON…") { store.exportSettings() }.disabled(store.exporting)
            Text(
              "Imported preferences replace local choices, including retention. Credentials are never exported."
            ).font(.caption)
          }
        }
        if navigation.page == "Shortcuts & permissions" {
          SetupChecklist(store: store, runtime: store.runtime)
        }
        if navigation.page == "Advanced", !store.discovery.isEmpty {
          Section("Server model discovery") {
            Text(store.discovery).font(.system(.caption, design: .monospaced)).textSelection(
              .enabled)
          }
        }
      }.formStyle(.grouped)
    }.disabled(store.state != .idle || store.settingsLocked)
  }
  var presets: some View {
    ScrollView {
      Form {
        if store.settingsLoadError != nil || retentionChangesPending
          || store.settingsSaveError != nil
        {
          preferenceStatus
        }
        Picker("Preset", selection: $store.config.selectedPreset) {
          ForEach(store.config.presets) { Text($0.name).tag($0.id) }
        }
        Text("Used by default unless an app-specific preset matches.").font(.caption)
          .foregroundStyle(.secondary)
        DisclosureGroup("Shared preferred spellings") {
          DictionaryEditor(title: "Preferred spellings", words: $store.config.dictionary)
          Text(
            "Preferred spellings guide text cleanup. Recognition support depends on your speech engine; these are hints, not guaranteed replacements."
          )
          .font(.caption).foregroundStyle(.secondary)
        }
        ForEach($store.config.presets) { $preset in
          if preset.id == store.config.selectedPreset {
            Section(preset.name) {
              TextField("Name", text: $preset.name)
              Picker(
                "Output",
                selection: Binding(get: { preset.outputMode }, set: { preset.outputMode = $0 })
              ) {
                ForEach(DictationMode.allCases, id: \.self) { Text($0.label).tag($0) }
              }.pickerStyle(.segmented)
              Text(preset.outputMode.description).font(.caption)
              if preset.outputMode != .verbatim {
                Text(
                  "Text processor: "
                    + (preset.cleanupModel.isEmpty
                      ? "Choose a processor" : processorName(preset.cleanupModel))
                ).font(.callout)
                Menu("Choose text processor") {
                  ForEach(preset.cleanupChoices(from: store.models)) { model in
                    Button(processorName(model.id)) { preset.cleanupModel = model.id }
                  }
                }
                if !preset.runsCleanup {
                  Label(
                    "Choose a text processor to use " + preset.outputMode.label + ".",
                    systemImage: "exclamationmark.triangle"
                  )
                  .foregroundStyle(.orange)
                  Button("Find available text processors") { store.discover() }
                  Button("Use Verbatim instead") { preset.outputMode = .verbatim }
                }
              }
              if preset.outputMode == .rewrite {
                Picker(
                  "Tone",
                  selection: Binding(
                    get: { preset.style == "verbatim" ? "clean" : preset.style },
                    set: { preset.style = $0 })
                ) {
                  Text("Natural").tag("clean")
                  Text("Formal").tag("formal")
                  Text("Casual").tag("casual")
                }
                TextField("Rewrite instructions", text: $preset.instructions, axis: .vertical)
              }
              TextField("Where you'll use this", text: $preset.context)
              Picker(
                "Speech language",
                selection: Binding(
                  get: { preset.speechLanguageChoice }, set: { preset.speechLanguageChoice = $0 })
              ) {
                Text("Use Mac language").tag(SpeechLanguageChoice.system)
                Text("Choose a language").tag(SpeechLanguageChoice.custom)
                if store.supportsLanguageDetection(preset) || preset.speechLanguageChoice == .detect
                {
                  Text("Detect spoken language").tag(SpeechLanguageChoice.detect)
                }
                if !store.config.usesManagedRuntime || preset.speechLanguageChoice == .server {
                  Text("Use server default").tag(SpeechLanguageChoice.server)
                }
              }
              if preset.speechLanguageChoice == .custom {
                TextField("Language code", text: $preset.language, prompt: Text("en-US"))
                Menu("Choose language") {
                  ForEach(store.nativeSpeechLanguages ?? SpeechLanguages.common, id: \.self) {
                    code in
                    Button(store.speechLanguageLabel(code, for: preset)) { preset.language = code }
                  }
                }
              } else if preset.speechLanguageChoice == .system {
                Text(
                  (try? store.resolvedPreset(preset)).map {
                    "Using " + SpeechLanguages.name($0.language)
                  }
                    ?? "Choose a supported speech language."
                ).font(.caption).foregroundStyle(.secondary)
              } else if preset.speechLanguageChoice == .detect
                && !store.supportsLanguageDetection(preset)
              {
                Label(
                  "This engine does not advertise language detection. Choose another language option.",
                  systemImage: "exclamationmark.triangle"
                ).foregroundStyle(.orange)
              }
              DictionaryEditor(
                title: "Preferred spellings for this preset", words: $preset.dictionary)
              DisclosureGroup("Advanced") {
                if store.config.usesManagedRuntime && preset.model == "transcribers/apple-speech" {
                  Toggle(
                    "Favor faster Apple speech results",
                    isOn: Binding(
                      get: { preset.fasterAppleSpeech ?? false },
                      set: { preset.fasterAppleSpeech = $0 }))
                  Text(
                    "Off by default to preserve accuracy. Uses a smaller recognition context and may make more mistakes."
                  ).font(.caption).foregroundStyle(.secondary)
                }
                Text(store.processingDescription(preset)).font(.caption).foregroundStyle(.secondary)
                TextField(
                  "App bundle IDs (automatic selection)",
                  text: Binding(
                    get: { preset.appIDs.joined(separator: ", ") },
                    set: { preset.appIDs = terms($0) }
                  ))
                TextField("Text processor model or alias", text: $preset.cleanupModel)
                Picker(
                  "Processing strategy",
                  selection: Binding(
                    get: { preset.processingStrategy }, set: { preset.processingStrategy = $0 })
                ) {
                  ForEach(ProcessingStrategy.allCases, id: \.self) { Text($0.label).tag($0) }
                }
                Text(
                  "Automatic processing uses advertised capabilities. Incremental requires final recognition segments; Rewrite needs the complete utterance. Explicit transcriber selections are preserved."
                ).font(.caption)
                TextField(
                  "Transcriber model or alias",
                  text: Binding(
                    get: { preset.model },
                    set: {
                      preset.model = $0
                      preset.automaticallySelectLocalTranscriber = false
                      preset.transcriberRoute = nil
                    }))
                Menu("Choose discovered transcriber") {
                  if store.config.usesManagedRuntime {
                    Button("Automatic (Apple when ready, otherwise Whisper)") {
                      store.selectAutomaticTranscriber(presetID: preset.id)
                    }
                  }
                  ForEach(store.models.filter { $0.task == "transcribe" }) { model in
                    Button("\(model.id) · \(model.executorName)") {
                      preset.selectTranscriber(model)
                    }
                  }
                }.disabled(store.models.isEmpty)
                if !preset.usesNamedTranscriber {
                  Text(
                    "Raw runtime transcribers use raw cleanup models and recorded-clip processing. Choose a named transcriber for shared incremental cleanup."
                  ).font(.caption)
                }
                Toggle(
                  "Legacy live preview when stable sessions are unavailable",
                  isOn: $preset.livePreview)
              }
              Button("Delete preset") {
                store.config.presets.removeAll { $0.id == preset.id }
                if store.config.selectedPreset == preset.id {
                  store.config.selectedPreset = store.config.presets.first?.id ?? ""
                }
              }.disabled(store.config.presets.count <= 1)
            }
          }
        }
        Button("Add preset") {
          let preset = Preset(id: UUID().uuidString, name: "New preset")
          store.config.presets.append(preset)
          store.config.selectedPreset = preset.id
        }
      }.formStyle(.grouped)
    }.disabled(store.state != .idle || store.settingsLocked)
  }
  var transcript: some View {
    VStack(alignment: .leading) {
      if store.state != .idle {
        Text(store.preview.isEmpty ? store.phase.label : store.preview)
          .foregroundStyle(.secondary).lineLimit(4)
      }
      if store.canUseOriginalWhileCleaning {
        HStack {
          Text("Original transcript is ready. Text processing is still running.").font(.caption)
          Spacer()
          Button("Use original now") { store.useOriginalWhileCleaning() }
        }
      }
      if !store.draft.isEmpty {
        Text("Cleanup preview — not inserted").font(.headline)
        Text(store.draft).lineLimit(5).textSelection(.enabled)
      }
      Text(store.result.isEmpty ? "Your next dictation" : "Latest result").font(.headline)
      if store.result.isEmpty && store.state == .idle {
        ContentUnavailableView(
          "Ready when you are", systemImage: "waveform",
          description: Text(
            "Use your shortcut in another app to dictate into its text field, or choose Test dictation here."
          )
        ).frame(maxWidth: .infinity, maxHeight: .infinity)
      } else {
        TextEditor(text: $store.result).disabled(store.state != .idle)
      }
      HStack {
        if !store.result.isEmpty {
          Button("Copy") { store.copy(store.result) }.disabled(store.result.isEmpty)
          Button("Export text…") { store.exportResult() }.disabled(
            store.exporting || store.result.isEmpty)
        }
        if store.canRetryRecording { Button("Retry recording (copy only)") { store.retry() } }
        if store.canRecoverRaw {
          Button("Insert original") { store.insertOriginal() }
          Button("Retry cleanup (copy only)") { store.retryCleanup() }
          Button("Copy original") { store.copy(store.preview) }
        }
      }
      if !store.preview.isEmpty || !store.processingNote.isEmpty || store.timing != nil {
        DisclosureGroup("Transcript & processing details") {
          if !store.preview.isEmpty { Text(store.preview).textSelection(.enabled) }
          Text(store.processingNote).font(.caption)
          if let timing = store.timing {
            Text(
              String(
                format:
                  "Recognition requests: %.2f s · Cleanup requests: %.2f s · Session: %.2f s · Release to result: %.2f s",
                timing.recognition, timing.cleanup, timing.total, timing.afterRelease)
            ).font(.caption)
            if let first = timing.firstEvent {
              Text(String(format: "First recognition/response event: %.2f s", first)).font(.caption)
            }
            if let first = timing.firstDraft {
              Text(String(format: "First cleanup text: %.2f s", first)).font(.caption)
            }
            Text(
              "Server queueing, prompt processing, and model loading are not separately reported by this runtime."
            ).font(.caption).foregroundStyle(.secondary)
            Text(
              "Durations include transport and queueing when applicable; overlapping stages do not add up to session duration."
            ).font(.caption).foregroundStyle(.secondary)
          }
        }
      }
    }.padding()
  }
  var history: some View {
    VStack(alignment: .leading, spacing: 12) {
      if !store.config.retainHistory {
        VStack(spacing: 12) {
          ContentUnavailableView(
            "History is off", systemImage: "clock.badge.xmark",
            description: Text(
              "New dictations stay out of local and server history. Your latest result is still available in Dictation."
            ))
          Button("Enable history…") {
            navigation.historyRetention = true
            navigation.confirmRetention = true
          }.disabled(store.state != .idle || store.settingsLocked || store.capturingShortcut)
          Button(
            navigation.showServerHistory
              ? "Hide server history search" : "Search existing server history"
          ) {
            navigation.showServerHistory.toggle()
          }
        }.frame(maxWidth: .infinity)
        if !store.history.isEmpty {
          Text("Earlier local entries").font(.headline)
          historyEntries
        }
      } else {
        HStack {
          Text("History is on").font(.headline)
          Spacer()
          Button("Upload local history") { store.syncHistory() }
            .disabled(store.history.isEmpty || store.historyWorking || store.settingsLocked)
        }
        Text("Transcripts are saved locally and synced to Antfly. Audio retention is separate.")
          .font(.caption).foregroundStyle(.secondary)
        historyEntries
      }
      if store.config.retainHistory || navigation.showServerHistory {
        DisclosureGroup("Search server history", isExpanded: $navigation.showServerHistory) {
          HStack {
            TextField("Search existing Antfly history", text: $store.searchText)
            Button("Search") { store.search() }.disabled(store.historyWorking)
          }
          if !store.searchResult.isEmpty {
            ScrollView { Text(store.searchResult).textSelection(.enabled) }.frame(maxHeight: 150)
          }
        }
      }
      if store.config.retainHistory || !store.history.isEmpty || store.pendingHistoryDeletions > 0 {
        Text(store.historySyncStatus).font(.caption).foregroundStyle(.secondary).textSelection(
          .enabled)
        HStack {
          Button("Clear local history…") { navigation.confirmLocalDeletion = true }.disabled(
            store.history.isEmpty)
          Button("Delete local & server history…") { navigation.confirmDeletion = true }
            .disabled(store.history.isEmpty)
          if store.pendingHistoryDeletions > 0 {
            Button("Retry pending deletions") { store.clearHistory(retryOnly: true) }
          }
        }.disabled(store.historyWorking || store.settingsLocked)
      }
    }.padding()
  }
  var historyEntries: some View {
    Group {
      if store.history.isEmpty {
        ContentUnavailableView(
          "No saved dictations yet", systemImage: "clock",
          description: Text("New dictations will appear here while history is enabled."))
      } else {
        List(store.history) { entry in
          VStack(alignment: .leading) {
            HStack {
              Text(entry.created, style: .date)
              Text(entry.preset)
              Text(entry.status).foregroundStyle(.secondary)
              Spacer()
              Button("Copy") { store.copy(entry.text) }
            }
            Text(entry.text).textSelection(.enabled)
          }
        }
      }
    }
  }
  private func processorName(_ identifier: String) -> String {
    if identifier == "generators/cleanup",
      store.models.first(where: { $0.id == identifier })?.executorName == "apple"
    {
      return "Apple text processor"
    }
    return identifier.hasPrefix("generators/")
      ? String(identifier.dropFirst("generators/".count)) : identifier
  }
  private func terms(_ text: String) -> [String] {
    text.split(separator: ",").map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }.filter {
      !$0.isEmpty
    }
  }
}

private struct RuntimeControls: View {
  @ObservedObject var store: Store
  @ObservedObject var runtime: ManagedRuntime
  var showAdvanced = false
  @StateObject private var consent = RuntimeSetupConsent()
  var body: some View {
    Text(runtime.status).font(.callout)
    if store.settingUpModel { ProgressView().controlSize(.small) }
    if !store.hasPendingSetupCommit && !store.modelSetup.message.isEmpty {
      Text(store.modelSetup.message).font(.callout)
    }
    if case .blocked(_, let conflict) = store.modelSetup {
      HStack {
        Button("Use Verbatim for incompatible presets") {
          store.retryModelSetup(resolution: .verbatim)
        }
        if conflict.canUseBundledProcessor {
          Button("Use bundled Apple text processor") { store.retryModelSetup(resolution: .bundled) }
        }
      }
      Text(
        "The bundled Apple text processor requires Apple Intelligence and its model assets. Verbatim skips text processing."
      ).font(.caption)
    }
    if case .blockedProcessing = store.modelSetup {
      Button("Use Automatic processing for incompatible presets") {
        store.retryModelSetup(processing: .automatic)
      }
    }
    if case .failed = store.modelSetup { Button("Retry setup") { store.retryModelSetup() } }
    if case .canceled = store.modelSetup { Button("Retry setup") { store.retryModelSetup() } }
    HStack {
      Button("Start / retry local engine") { runtime.start() }.disabled(
        runtime.busy || runtime.ready)
      Button("Download Whisper Tiny…") { consent.whisper = true }.disabled(
        !runtime.ready || runtime.busy)
      Button(store.appleSpeechSetupTitle) { consent.apple = true }.disabled(
        !runtime.ready || runtime.busy || !store.canUseManagedAppleSpeech)
      if showAdvanced { Button("Open engine log") { NSWorkspace.shared.open(runtime.logURL) } }
    }
    Text(
      "Apple speech needs macOS 26+ and language assets. Install / check downloads assets for the active preset’s language (using your Mac language by default) and verifies speech before selecting it. Whisper is an alternative. Text cleanup is configured separately in Presets."
    ).font(.caption)
      .confirmationDialog("Download Whisper Tiny?", isPresented: $consent.whisper) {
        Button("Download & set up Whisper") { store.installLocalWhisper() }
      } message: {
        Text(
          "This downloads the Whisper Tiny model and restarts your local engine. Preset selections change only after setup succeeds. You can cancel the download."
        )
      }
      .confirmationDialog("Allow Apple speech asset downloads?", isPresented: $consent.apple) {
        Button("Allow downloads & check speech") { store.useLocalApple() }
      } message: {
        Text(
          "Apple manages the language assets and download time on this Mac. Setup checks "
            + store.appleSpeechLanguage
            + " before selecting Apple speech. Text processing is configured separately.")
      }
  }
}

private struct RuntimeSummary: View {
  @ObservedObject var runtime: ManagedRuntime
  var body: some View { Text(runtime.status).font(.caption).foregroundStyle(.secondary) }
}

@MainActor final class SettingsNavigation: ObservableObject {
  @Published var page = "General"
  @Published var tab = 0
  @Published var historyRetention: Bool?
  @Published var audioRetention: Bool?
  @Published var confirmRetention = false
  @Published var confirmReplacement = false
  @Published var confirmPublication = false
  @Published var confirmImport = false
  @Published var confirmDeletion = false
  @Published var confirmLocalDeletion = false
  @Published var showServerHistory = false
}

@MainActor private final class RuntimeSetupConsent: ObservableObject {
  @Published var whisper = false
  @Published var apple = false
}
