"""Provider modules for ingesting listings from external sources."""

from rentbot.providers.api.immobiliare import ImmobiliareProvider
from rentbot.providers.base import BaseProvider

__all__ = ["BaseProvider", "ImmobiliareProvider"]
