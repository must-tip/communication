from __future__ import annotations


class CommunicationError(Exception):
    """Base exception for the communication subsystem."""


class ConfigurationError(CommunicationError):
    """Raised when messaging configuration is invalid or incomplete."""


class TransportUnavailable(CommunicationError):
    """Raised when a requested transport is unavailable."""


class PublishError(CommunicationError):
    """Raised when an event cannot be delivered according to the publish policy."""


class ConsumeError(CommunicationError):
    """Raised when a consumer cannot start or process messages safely."""


class EnvelopeValidationError(CommunicationError, ValueError):
    """Raised when an event envelope or payload violates the wire contract."""


class SignatureError(CommunicationError):
    """Raised when event signing or verification fails."""


class DuplicateEvent(CommunicationError):
    """Raised only by strict callers when an event has already been processed."""
