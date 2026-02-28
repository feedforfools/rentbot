"""API-first providers: Immobiliare.it, Casa.it, Subito.it."""

from rentbot.providers.api.http_client import ProviderHttpClient
from rentbot.providers.api.immobiliare import ImmobiliareProvider

__all__ = ["ProviderHttpClient", "ImmobiliareProvider"]
