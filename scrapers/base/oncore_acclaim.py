"""
OnCore Acclaim (Harris Recording Solutions) clerk client.

This module handles the HTTP session, disclaimer acceptance, doc-type search
submission, and CSV export for any Florida county clerk running the OnCore
Acclaim platform. Lake, Marion, Brevard, Collier, Hillsborough, and many
other FL counties use this same platform, so this client is reusable.
"""
from __future__ import annotations

import io
import logging
import time
from dataclasses import dataclass
from datetime import date
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
    """Session-aware client for the OnCore Acclaim clerk platform."""

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

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.config.rate_limit_seconds:
            time.sleep(self.config.rate_limit_seconds - elapsed)
        self._last_request_at = time.monotonic()

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
        logger.debug("POST %s", url)
        resp = self.session.post(url, data=data, timeout=30, **kwargs)
        resp.raise_for_status()
        return resp

    def accept_disclaimer(self) -> None:
        """Hit the disclaimer acceptance endpoint to establish session cookies."""
        if self._disclaimer_accepted:
            return
        self._get("/")
        accept_path = f"{self.config.disclaimer_accept_url}?st={self.config.search_endpoint}"
        self._get(accept_path)
        self._disclaimer_accepted = True
        logger.info("Disclaimer accepted; session established")

    def search_doc_types(
        self,
        doc_type_ids: Iterable[int],
        date_from: date,
        date_to: date,
        display_label: str = "",
    ) -> pd.DataFrame:
        """Search Official Records by document type ID(s) and date range, return results as DataFrame.

        Args:
            doc_type_ids: e.g. [30, 31, 35]. CLERK'S INTERNAL NUMERIC IDs, not
                string codes. For Lake County: GOV=30, J/L=31, LN=35, ORD=45,
                SAT=55, JAS=32.
            date_from: inclusive lower bound on Record Date.
            date_to: inclusive upper bound on Record Date.
            display_label: optional human-readable label for DocTypesDisplay
                fields. Cosmetic but included for symmetry with real submission.
        """
        if not self._disclaimer_accepted:
            self.accept_disclaimer()

        # Form payload discovered by capturing a real browser submission.
        # Gotchas:
        #  * DocTypes takes NUMERIC IDs, not string codes
        #  * X-Requested-With goes in BODY as a form field (weird but required)
        #  * Dates use M/D/YYYY with no leading zeros
        ids_str = ",".join(str(i) for i in doc_type_ids)
        form_data = {
            "DocTypes": ids_str,
            "DocTypesDisplay-input": display_label,
            "DocTypesDisplay": display_label,
            "DateRangeList": "SpecifyDateRange",
            "RecordDateFrom": self._format_date(date_from),
            "RecordDateTo": self._format_date(date_to),
            "X-Requested-With": "XMLHttpRequest",
        }

        logger.info(
            "Submitting search doc_type_ids=%s range=%s..%s",
            ids_str, date_from, date_to,
        )
        self._post(
            self.config.search_endpoint,
            data=form_data,
            headers={"X-Requested-With": "XMLHttpRequest"},
        )

        logger.info("Downloading CSV export")
        csv_resp = self._get(self.config.csv_export_endpoint)
        df = pd.read_csv(io.BytesIO(csv_resp.content), dtype=str).fillna("")
        logger.info("CSV returned %d rows", len(df))
        return df

    @staticmethod
    def _format_date(d: date) -> str:
        """Format as M/D/YYYY with no leading zeros (what OnCore expects)."""
        return f"{d.month}/{d.day}/{d.year}"

    def resolve_document_url(self, instrument_number: str) -> Optional[str]:
        """Resolve a public instrument number to the clerk's viewer URL.

        KNOWN LIMITATION: OnCore's document viewer uses an internal
        TransactionItemId rather than the public instrument number. The
        grid's row-click handler does this translation in JS using grid
        state that's not accessible from a pure HTTP client.

        For Phase 1 MVP this returns None and the pipeline proceeds with
        metadata-only rows. Fill this in once we observe a live session.
        """
        return None

    def download_document_pdf(self, instrument_number: str) -> Optional[bytes]:
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
