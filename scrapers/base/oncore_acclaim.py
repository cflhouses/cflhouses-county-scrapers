"""
OnCore Acclaim (Harris Recording Solutions) clerk client.

This module handles the HTTP session, disclaimer acceptance, doc-type search
submission, and CSV export for any Florida county clerk running the OnCore
Acclaim platform. Lake, Marion, Brevard, Collier, Hillsborough, and many
other FL counties use this same platform, so this client is reusable.

Phase 0 finding: the clerk UI has a built-in "Export to CSV" endpoint at
GET /Search/ExportCsv which uses server-side session state. We POST a search
to /search/SearchTypeDocType first, then pull the CSV. No HTML grid parsing
needed.

Document viewer URL resolution is deferred — the viewer uses an internal
TransactionItemId that only the grid-open handler knows about. Will be
resolved on first live session run.
"""
from __future__ import annotations

import io
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Optional

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@dataclass
class OnCoreConfig:
    base_url: str
    disclaimer_accept_url: str = "/search/Disclaimer"
    search_endpoint: str = "/search/SearchTypeDocType"
    csv_export_endpoint: str = "/Search/ExportCsv"
    user_agent: str = "CFL-County-Scraper/1.0 (contact: CFLHousesLLC@gmail.com)"
    rate_limit_seconds: float = 3.0


class OnCoreAcclaimClient:
    """Session-aware client for the OnCore Acclaim clerk platform.

    Usage:
        client = OnCoreAcclaimClient(OnCoreConfig(base_url="https://officialrecords.lakecountyclerk.org"))
        client.accept_disclaimer()
        df = client.search_doc_types(["GOV"], date(2026, 3, 15), date(2026, 4, 15))
    """

    def __init__(self, config: OnCoreConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": config.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._last_request_at: float = 0.0
        self._disclaimer_accepted = False

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _throttle(self) -> None:
        """Sleep if necessary to respect the configured rate limit."""
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.config.rate_limit_seconds:
            time.sleep(self.config.rate_limit_seconds - elapsed)
        self._last_request_at = time.monotonic()

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=60))
    def _get(self, path: str, **kwargs) -> requests.Response:
        self._throttle()
        url = self.config.base_url + path
        logger.debug("GET %s", url)
        resp = self.session.get(url, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=60))
    def _post(self, path: str, data: dict, **kwargs) -> requests.Response:
        self._throttle()
        url = self.config.base_url + path
        logger.debug("POST %s data=%s", url, data)
        resp = self.session.post(url, data=data, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def accept_disclaimer(self) -> None:
        """Hit the disclaimer acceptance endpoint to establish session cookies.

        OnCore sites redirect first-visit users to a disclaimer page. Accepting
        it sets a session cookie that lets subsequent searches succeed. This
        must be called once per session before any search.
        """
        if self._disclaimer_accepted:
            return
        # Visit root first to get initial cookies
        self._get("/")
        # Post acceptance (the real button is a form POST; GET with the same
        # path also works on most OnCore deployments as it just renders the
        # post-acceptance state).
        accept_path = f"{self.config.disclaimer_accept_url}?st={self.config.search_endpoint}"
        self._get(accept_path)
        self._disclaimer_accepted = True
        logger.info("Disclaimer accepted; session established")

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search_doc_types(
        self,
        doc_type_codes: Iterable[str],
        date_from: date,
        date_to: date,
    ) -> pd.DataFrame:
        """Search Official Records by document type(s) and date range, return results as DataFrame.

        Uses the server-side session: POST the search criteria, then GET the
        CSV export. The CSV contains all results (no pagination handling needed
        unless we hit the platform's max rows, which we'll address if it happens).

        Args:
            doc_type_codes: e.g. ["GOV", "J/L", "LN"]. Codes must match the
                clerk's OnCore instance (see config/lake.yaml for Lake codes).
            date_from: inclusive lower bound on Record Date.
            date_to: inclusive upper bound on Record Date.

        Returns:
            DataFrame with OnCore's standard columns: Direct Name, Indirect
            Name, Record Date, Doc Type, Book Type, Book/Page, Instrument #,
            Doc Legal, Case #, Consideration.
        """
        if not self._disclaimer_accepted:
            self.accept_disclaimer()

        # NOTE: OnCore's exact form field names vary slightly between
        # deployments. The fields below match Lake County's observed POST
        # body (captured during Phase 0). If a different county's search
        # 500s, inspect the network tab in-browser and update the field names.
        form_data = {
            "DocTypesIDString": ",".join(doc_type_codes),
            "RecordDateFrom": date_from.strftime("%m/%d/%Y"),
            "RecordDateTo": date_to.strftime("%m/%d/%Y"),
            "DateRange": "SpecifyDateRange",
        }

        logger.info(
            "Submitting search doc_types=%s range=%s..%s",
            list(doc_type_codes), date_from, date_to,
        )
        self._post(self.config.search_endpoint, data=form_data)

        # CSV export reads the current session's search results
        logger.info("Downloading CSV export")
        csv_resp = self._get(self.config.csv_export_endpoint)

        df = pd.read_csv(io.BytesIO(csv_resp.content), dtype=str).fillna("")
        logger.info("CSV returned %d rows", len(df))
        return df

    # ------------------------------------------------------------------
    # Document viewer — TO BE FINALIZED during first live run
    # ------------------------------------------------------------------

    def resolve_document_url(self, instrument_number: str) -> Optional[str]:
        """Resolve a public instrument number to the clerk's document viewer URL.

        KNOWN LIMITATION: OnCore's document viewer at /Details/ uses an
        internal TransactionItemId rather than the public instrument number.
        The grid's row-click handler does this translation in JavaScript
        using grid state that's not easily accessible from a pure HTTP
        client.

        Options for resolving (to be decided after a live observation):
        - Parse the grid's row data attributes from the search HTML response
          and map instrument → TransactionItemId
        - Drive a headless browser (Playwright) for document downloads only
        - Reverse-engineer the TransactionItemId derivation (it may be a
          predictable function of instrument/book/page)

        For Phase 1 MVP, this returns None and the orchestrator proceeds
        with metadata-only rows. Fill this in once we can inspect a live
        session.
        """
        # TODO(Phase 1): implement once live session is observed
        return None

    def download_document_pdf(self, instrument_number: str) -> Optional[bytes]:
        """Download the PDF for a given instrument number.

        Returns PDF bytes on success, or None if the viewer URL can't be
        resolved yet (see resolve_document_url). Calling code should handle
        None gracefully — the pipeline should still produce metadata-only
        rows in that case.
        """
        url = self.resolve_document_url(instrument_number)
        if url is None:
            logger.warning(
                "Document URL unresolved for instrument %s — returning None",
                instrument_number,
            )
            return None

        resp = self._get(url)
        content_type = resp.headers.get("Content-Type", "").lower()
        if "pdf" not in content_type:
            logger.warning(
                "Expected PDF for %s but got Content-Type=%s",
                instrument_number, content_type,
            )
            return None
        return resp.content
