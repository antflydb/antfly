# Changelog

All notable changes to the Antfly Database Operator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed
- Consolidated the previous Antfly and Inference operator modules into
  `go/pkg/operator` with one `cmd/antfly-operator` binary and one operator image.
- Operator release tags now use `go/pkg/operator/v*`; the previous
  `pkg/antfly-operator/v*` and `go/pkg/termite-operator/v*` tag streams are
  removed.

### Added
- Added `AntflyCluster.spec.inference` for an AntflyCluster-owned InferencePool.

### Removed
- Removed the standalone Inference operator image and Dockerfile.

## [0.0.16] - 2026-03-16

### Changed
- Simplified architecture to two-tier design (leader + data nodes)
- Removed inference deployment components entirely
- Updated all documentation and examples to reflect simplified architecture
- Streamlined CRD definition to remove unused inference configurations
- Updated Makefile with improved targets and cleanup

### Removed
- Inference nodes deployment and configuration support
- All inference-related code from controller and CRD types
- Unused build artifacts and empty directories
- Kustomize references from documentation

### Added
- Comprehensive configuration validation for all config fields
- Enhanced operator auto-generation of complete network configurations
- Proper .gitignore file for build artifacts
- New Makefile target `all-check` for comprehensive testing
- New Makefile target `minikube-redeploy` for complete cleanup and redeploy
- Complete minikube cleanup and redeploy documentation section
- Automated script `scripts/minikube-redeploy.sh` for development workflow
- Improved documentation with updated architecture diagrams

### Fixed
- Inconsistencies between CRD manifests and Go types
- Sample configuration files to match actual implementation
- Build system to properly handle local development

## Previous Versions

This changelog starts from the refactored version that removed inference components.
For earlier history, see git commit history.
