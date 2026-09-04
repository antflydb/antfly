"""Fail-closed registry adapters for the release controller."""

from .model import Lookup, LookupState, RegistryError

__all__ = ["Lookup", "LookupState", "RegistryError"]
