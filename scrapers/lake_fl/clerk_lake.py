"""
Lake County, FL — clerk scraper.

Thin wrapper over the OnCore Acclaim base client with Lake-specific config
and a code-enforcement filtering helper.
"""
from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd
import yaml

from scrapers.base.oncore_acclaim import OnCoreAcclaimClient, OnCoreConfig

logger = logging.getLogger(__name__)

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "lake.yaml"


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


class LakeClerkScraper:
    """Lake County clerk scraper with code-enforcement filtering baked in."""

    def __init__(self):
        self.config = load_config()
        self.client = OnCoreAcclaimClient(OnCoreConfig(
            base_url=self.config["clerk"]["base_url"],
            disclaimer_accept_url=self.config["clerk"]["disclaimer_accept_url"],
            search_endpoint=self.config["clerk"]["search_endpoint"],
            csv_export_endpoint=self.config["clerk"]["csv_export_endpoint"],
            rate_limit_seconds=1.0 / self.config["rate_limits"]["clerk_requests_per_second"],
        ))
        self._filer_regex = re.compile(
            self.config["code_enforcement_filer_regex"], re.IGNORECASE,
        )

    def pull_code_enforcement_candidates(
        self, date_from: date, date_to: date
    ) -> pd.DataFrame:
        """Pull primary CE doc types (GOV + secondary) and filter to municipal filers."""
        primary = self.config["doc_types"]["primary"]
        ids = [d["id"] for d in primary]
        codes = [d["code"] for d in primary]
        display = ", ".join(f"{d['name']} ({d['code']})" for d in primary)
        df = self.client.search_doc_types(ids, date_from, date_to, display_label=display)

        if df.empty:
            return df

        df = self._normalize_column_names(df)
        df["is_ce_filer"] = df["indirect_name"].apply(self._is_government_filer)

        ce_rows = df[df["is_ce_filer"]].copy()
        non_ce_rows = df[~df["is_ce_filer"]]
        logger.info(
            "Pulled %d rows (%d CE, %d other-GOV) for doc types %s, %s..%s",
            len(df), len(ce_rows), len(non_ce_rows), codes, date_from, date_to,
        )
        return ce_rows

    def pull_satisfactions(
        self, date_from: date, date_to: date
    ) -> pd.DataFrame:
        """Pull satisfaction/release doc types for the same date range."""
        sats = self.config["doc_types"]["satisfactions"]
        ids = [d["id"] for d in sats]
        display = ", ".join(f"{d['name']} ({d['code']})" for d in sats)
        df = self.client.search_doc_types(ids, date_from, date_to, display_label=display)
        df = self._normalize_column_names(df) if not df.empty else df
        logger.info(
            "Pulled %d satisfactions for range %s..%s", len(df), date_from, date_to,
        )
        return df

    def download_document_pdf(self, instrument_number: str):
        """Delegate to base client (returns None until viewer URL is resolved)."""
        return self.client.download_document_pdf(instrument_number)

    def _is_government_filer(self, indirect_name: str) -> bool:
        if not indirect_name:
            return False
        return bool(self._filer_regex.match(indirect_name.strip()))

    @staticmethod
    def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """Lowercase, underscore-separated column names for internal use."""
        df = df.copy()
        df.columns = [
            re.sub(r'[^a-z0-9]+', '_', c.lower()).strip('_') for c in df.columns
        ]
        rename = {
            "direct_name": "direct_name",
            "indirect_name": "indirect_name",
            "record_date": "record_date",
            "doc_type": "doc_type",
            "book_page": "book_page",
            "book_type": "book_type",
            "instrument": "instrument_number",
            "instrument_num": "instrument_number",
            "doc_legal": "doc_legal",
            "case": "case_number",
            "consideration": "consideration",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        return df
