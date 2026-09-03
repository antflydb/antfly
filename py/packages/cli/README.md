# antfly-cli

Python package for installing the native Antfly CLI.

Supported Python versions: 3.11 through 3.14.

```bash
pipx install antfly-cli
antfly --version
```

This package is separate from `antfly-sdk`, which contains the Python SDK.
Release wheels include the `antfly` executable, Antfarm dashboard assets, and
the Antfly C ABI header/library for the target platform.
