"""API-first providers: Immobiliare.it, Casa.it, Subito.it."""

from rentbot.providers.api.casa import CasaProvider
from rentbot.providers.api.http_client import ProviderHttpClient
from rentbot.providers.api.immobiliare import ImmobiliareProvider
from rentbot.providers.api.subito import SubitoProvider

__all__ = ["CasaProvider", "ProviderHttpClient", "ImmobiliareProvider", "SubitoProvider"]
