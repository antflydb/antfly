// swift-tools-version: 6.0
import PackageDescription

let package = Package(
  name: "AntflyDictation", platforms: [.macOS(.v14)],
  products: [.executable(name: "AntflyDictation", targets: ["DictationApp"])],
  targets: [
    .target(name: "DictationCore"),
    .executableTarget(name: "DictationApp", dependencies: ["DictationCore"]),
    .testTarget(name: "DictationCoreTests", dependencies: ["DictationCore", "DictationApp"]),
  ]
)
