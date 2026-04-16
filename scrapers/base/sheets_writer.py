"""
Google Sheets writer — service-account-authenticated upsert.

Keeps the master stacked database in Google Sheets up to date by:
- Reading the existing sheet
- Matching rows by a stable key (typically `dedup_key` = instrument_number)
- Appending new rows
- Updating existing rows when enrichment data changes

Credentials are read from the env var GOOGLE_CREDENTIALS_JSON (the full JSON
contents of a Google Cloud service account key).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import List, Optional

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_service_account_creds() -> Credentials:
    """Load service account credentials from GOOGLE_CREDENTIALS_JSON env var."""
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not raw:
        raise RuntimeError(
            "GOOGLE_CREDENTIALS_JSON env var is not set. "
            "For local runs: export GOOGLE_CREDENTIALS_JSON=\"$(cat service-account.json)\". "
            "In GitHub Actions: set it as a repository secret."
        )
    info = json.loads(raw)
    return Credentials.from_service_account_info(info, scopes=SCOPES)


def _get_client() -> gspread.Client:
    return gspread.authorize(_get_service_account_creds())


class MasterSheetWriter:
    """Upsert rows into a master Google Sheet keyed by a stable dedup column."""

    def __init__(self, sheet_id: str, worksheet_name: str = "data"):
        self.sheet_id = sheet_id
        self.worksheet_name = worksheet_name
        self._ws: Optional[gspread.Worksheet] = None

    @property
    def ws(self) -> gspread.Worksheet:
        if self._ws is None:
            client = _get_client()
            sheet = client.open_by_key(self.sheet_id)
            self._ws = sheet.worksheet(self.worksheet_name)
        return self._ws

    # ------------------------------------------------------------------

    def read_existing(self) -> pd.DataFrame:
        """Read the current sheet contents into a DataFrame (header row = column names)."""
        records = self.ws.get_all_records()
        return pd.DataFrame(records).astype(str) if records else pd.DataFrame()

    def upsert(self, df: pd.DataFrame, key_column: str = "dedup_key") -> dict:
        """Upsert rows from df into the sheet using key_column as the stable identifier.

        Returns a summary dict: {inserted: N, updated: N, unchanged: N}
        """
        if df.empty:
            logger.info("Upsert called with empty DataFrame; nothing to do")
            return {"inserted": 0, "updated": 0, "unchanged": 0}

        headers = self.ws.row_values(1)
        if not headers:
            raise RuntimeError(
                f"Sheet {self.sheet_id} worksheet {self.worksheet_name} has no header row. "
                "Run the Apps Script setup first."
            )

        # Align incoming df to the sheet's header order; fill missing cols blank
        df = df.reindex(columns=headers).fillna("").astype(str)

        existing = self.read_existing()
        if existing.empty or key_column not in existing.columns:
            # Sheet is empty -- bulk append
            rows = df.values.tolist()
            self.ws.append_rows(rows, value_input_option="USER_ENTERED")
            logger.info("Bulk-appended %d rows to empty sheet", len(rows))
            return {"inserted": len(rows), "updated": 0, "unchanged": 0}

        existing = existing.reindex(columns=headers).fillna("")
        existing_keys = dict(zip(existing[key_column], existing.index))

        to_insert: List[list] = []
        updates_to_batch: List[dict] = []
        unchanged = 0

        for _, row in df.iterrows():
            key = row[key_column]
            if not key:
                logger.warning("Skipping row with empty dedup_key: %s", row.to_dict())
                continue

            if key in existing_keys:
                # Compare and update only if different
                row_idx = existing_keys[key]
                existing_row = existing.iloc[row_idx]
                if not row.equals(existing_row):
                    # Sheet is 1-indexed; +2 = header row + 0-indexed offset
                    sheet_row = row_idx + 2
                    updates_to_batch.append({
                        "range": f"A{sheet_row}:{_col_letter(len(headers))}{sheet_row}",
                        "values": [row.tolist()],
                    })
                else:
                    unchanged += 1
            else:
                to_insert.append(row.tolist())

        if updates_to_batch:
            self.ws.batch_update(updates_to_batch, value_input_option="USER_ENTERED")
            logger.info("Updated %d existing rows", len(updates_to_batch))
        if to_insert:
            self.ws.append_rows(to_insert, value_input_option="USER_ENTERED")
            logger.info("Inserted %d new rows", len(to_insert))

        return {
            "inserted": len(to_insert),
            "updated": len(updates_to_batch),
            "unchanged": unchanged,
        }


class RunLogWriter:
    """Append a single row per scraper run to the RunLog sheet."""

    def __init__(self, sheet_id: str, worksheet_name: str = "data"):
        self.sheet_id = sheet_id
        self.worksheet_name = worksheet_name

    def append(self, fields: dict) -> None:
        client = _get_client()
        ws = client.open_by_key(self.sheet_id).worksheet(self.worksheet_name)
        headers = ws.row_values(1)
        row = [str(fields.get(h, "")) for h in headers]
        ws.append_row(row, value_input_option="USER_ENTERED")
        logger.info("RunLog row appended")


def _col_letter(n: int) -> str:
    """Convert 1-indexed column number to A1-notation letter (1→A, 27→AA, etc.)."""
    result = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result
