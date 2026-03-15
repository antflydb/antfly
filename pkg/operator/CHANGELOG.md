# Changelog

All notable changes to the Antfly Database Operator will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Simplified architecture to two-tier design (leader + data nodes)
- Removed termite deployment components entirely
- Updated all documentation and examples to reflect simplified architecture
- Streamlined CRD definition to remove unused termite configurations
- Updated Makefile with improved targets and cleanup

### Removed
- Termite nodes deployment and configuration support
- All termite-related code from controller and CRD types
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

This changelog starts from the refactored version that removed termite components.
For earlier history, see git commit history.