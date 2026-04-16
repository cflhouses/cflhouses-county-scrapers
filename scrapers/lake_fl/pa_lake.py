"""
Lake County Property Appraiser reverse-lookup client.

Called by the orchestrator to enrich recordings with current property data.
The cascade tries the strongest identifier first and falls back through
weaker ones:

    Tier 1: by Alt Key (parcel ID) — exact match
    Tier 2: by legal description — subdivision/lot/block
    Tier 3: by address — street address
    Tier 4: by owner name (filtered by STR code if present) — fuzzy

NOTE on implementation: the Lake PA site (www.lakecopropappr.com) is an
ASP.NET WebForms app which makes HTTP-level scraping awkward (hidden
__VIEWSTATE fields on every request). Phase 1 plan is to start with a thin
HTTP client, and fall back to driving Playwright if the WebForms state
management becomes unwieldy. This module currently returns mock/None
results so the orchestrator can be wired up end-to-end; the actual search
logic will be filled in during the first live dev session.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional

logger = logging.getLogger(__name__)


@dataclass
class PropertyRecord:
    """Enriched property record returned from a PA lookup."""
    alt_key: str = ""
    current_owner: str = ""
    mailing_address: str = ""
    site_address: str = ""
    just_value: str = ""
    year_built: str = ""
    living_area: str = ""
    lot_size: str = ""
    matched_via: str = ""  # "parcel_id" | "legal" | "address" | "owner_str" | "none"


class LakePropertyAppraiser:
    """Reverse-lookup from clerk-derived identifiers to Lake PA property records."""

    def __init__(self, cache_ttl_days: int = 30, rate_limit_seconds: float = 3.0):
        self.cache_ttl = timedelta(days=cache_ttl_days)
        self.rate_limit_seconds = rate_limit_seconds
        self._cache: Dict[str, tuple[datetime, Optional[PropertyRecord]]] = {}
        self._last_request_at: float = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.rate_limit_seconds:
            time.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_at = time.monotonic()

    def _cache_get(self, key: str) -> Optional[PropertyRecord]:
        if key in self._cache:
            cached_at, value = self._cache[key]
            if datetime.now() - cached_at < self.cache_ttl:
                return value
        return None

    def _cache_put(self, key: str, value: Optional[PropertyRecord]) -> None:
        self._cache[key] = (datetime.now(), value)

    # ------------------------------------------------------------------
    # Public cascade entry point
    # ------------------------------------------------------------------

    def lookup(
        self,
        parcel_id: Optional[str] = None,
        legal_description: Optional[str] = None,
        address: Optional[str] = None,
        owner_name: Optional[str] = None,
        str_code: Optional[str] = None,
    ) -> Optional[PropertyRecord]:
        """Cascade-lookup a property using the strongest available identifier first."""
        if parcel_id:
            result = self.lookup_by_parcel_id(parcel_id)
            if result:
                result.matched_via = "parcel_id"
                return result
        if legal_description:
            result = self.lookup_by_legal(legal_description)
            if result:
                result.matched_via = "legal"
                return result
        if address:
            result = self.lookup_by_address(address)
            if result:
                result.matched_via = "address"
                return result
        if owner_name:
            result = self.lookup_by_owner(owner_name, str_code=str_code)
            if result:
                result.matched_via = "owner_str"
                return result
        logger.warning(
            "No PA match: parcel=%s legal=%s addr=%s owner=%s",
            parcel_id, legal_description, address, owner_name,
        )
        return None

    # ------------------------------------------------------------------
    # Per-tier lookups — to be implemented during first live dev session
    # ------------------------------------------------------------------
    #
    # Placeholder implementations return None. The orchestrator handles
    # None gracefully by falling through the cascade. We intentionally
    # ship the scaffolding with these stubbed so the rest of the pipeline
    # can be tested end-to-end against real clerk data before we commit
    # to an HTTP vs Playwright approach for the PA side.
    #
    # When implementing:
    # - For HTTP approach: use requests.Session, capture __VIEWSTATE from
    #   the search-form GET, then POST the search with __VIEWSTATE carried
    #   forward. Parse the result table with BeautifulSoup.
    # - For Playwright approach: drive a headless browser for each lookup.
    #   Slower but robust against ASP.NET state management.
    # ------------------------------------------------------------------

    def lookup_by_parcel_id(self, alt_key: str) -> Optional[PropertyRecord]:
        cached = self._cache_get(f"parcel:{alt_key}")
        if cached is not None:
            return cached
        self._throttle()
        # TODO: implement HTTP search for Alt Key on PA site
        result: Optional[PropertyRecord] = None
        self._cache_put(f"parcel:{alt_key}", result)
        return result

    def lookup_by_legal(self, legal_description: str) -> Optional[PropertyRecord]:
        cached = self._cache_get(f"legal:{legal_description[:100]}")
        if cached is not None:
            return cached
        self._throttle()
        result: Optional[PropertyRecord] = None
        self._cache_put(f"legal:{legal_description[:100]}", result)
        return result

    def lookup_by_address(self, address: str) -> Optional[PropertyRecord]:
        cached = self._cache_get(f"addr:{address}")
        if cached is not None:
            return cached
        self._throttle()
        result: Optional[PropertyRecord] = None
        self._cache_put(f"addr:{address}", result)
        return result

    def lookup_by_owner(
        self, owner_name: str, str_code: Optional[str] = None
    ) -> Optional[PropertyRecord]:
        cache_key = f"owner:{owner_name}:{str_code or ''}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        self._throttle()
        result: Optional[PropertyRecord] = None
        self._cache_put(cache_key, result)
        return result
