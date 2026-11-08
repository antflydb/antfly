// swift-tools-version: 6.0
import PackageDescription

let package = Package(
  name: "AntflyDictation", platforms: [.macOS(.v14)],
  products: [.executable(name: "AntflyDictation", targets: ["DictationApp"])],
  dependencies: [
    .package(url: "https://github.com/sparkle-project/Sparkle", exact: "2.10.1")
  ],
  targets: [
    .target(name: "DictationCore"),
    .executableTarget(
      name: "DictationApp",
      dependencies: ["DictationCore", .product(name: "Sparkle", package: "Sparkle")],
      linkerSettings: [
        .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "@executable_path/../Frameworks"])
      ]
    ),
    .testTarget(name: "DictationCoreTests", dependencies: ["DictationCore", "DictationApp"]),
  ]
)
