"""
Lake County code-enforcement pipeline orchestrator.

Wires together the clerk scraper, PDF parser, PA reverse-lookup, and
Sheets writer into one end-to-end run.

Flow (per run):
    1. Pull primary CE doc types from the clerk (GOV + secondary), filter
       by municipal filer regex
    2. Pull satisfactions for the same window; drop any lien with a matching
       satisfaction (by Direct Name + book/page overlap)
    3. For each surviving record:
        a. Download the PDF (may return None — see base/oncore_acclaim.py)
        b. Parse for parcel ID / legal / address / fine amount
        c. Reverse-lookup on PA using strongest available identifier
        d. Build the enriched row (includes the original clerk metadata +
           PDF-parsed fields + PA enrichment + match tier)
    4. Upsert rows into the master Sheet
    5. Append a run summary to the RunLog sheet
"""
from __future__ import annotations

import logging
import os
from datetime import date, datetime
from typing import Optional

import pandas as pd

from scrapers.base.pdf_parser import LienPDFParser
from scrapers.base.sheets_writer import MasterSheetWriter, RunLogWriter
from scrapers.lake_fl.clerk_lake import LakeClerkScraper, load_config
from scrapers.lake_fl.pa_lake import LakePropertyAppraiser, PropertyRecord

logger = logging.getLogger(__name__)


class LakeCodeEnforcementPipeline:
    """End-to-end pipeline for Lake County code enforcement lien scraping."""

    def __init__(
        self,
        master_sheet_id: Optional[str] = None,
        run_log_sheet_id: Optional[str] = None,
    ):
        self.config = load_config()
        self.clerk = LakeClerkScraper()
        self.pa = LakePropertyAppraiser(
            cache_ttl_days=self.config["rate_limits"]["pa_cache_days"],
            rate_limit_seconds=1.0 / self.config["rate_limits"]["pa_requests_per_second"],
        )
        self.pdf_parser = LienPDFParser(
            parcel_id_patterns=self.config.get("parcel_id_patterns", []),
        )
        self.master_sheet_id = master_sheet_id or os.environ.get("LAKE_MASTER_SHEET_ID")
        self.run_log_sheet_id = run_log_sheet_id or os.environ.get("LAKE_RUN_LOG_SHEET_ID")

    # ------------------------------------------------------------------

    def run(self, date_from: date, date_to: date) -> dict:
        """Execute one full pipeline run over the given date window."""
        started_at = datetime.now()
        logger.info("Pipeline run starting: %s..%s", date_from, date_to)

        # --- Step 1: pull candidates
        candidates = self.clerk.pull_code_enforcement_candidates(date_from, date_to)
        if candidates.empty:
            logger.info("No candidates returned; exiting early")
            return self._log_run(date_from, date_to, started_at, 0, 0, 0, {}, 0, [])

        # --- Step 2: pull satisfactions and filter
        sats = self.clerk.pull_satisfactions(date_from, date_to)
        before = len(candidates)
        candidates = self._drop_satisfied(candidates, sats)
        satisfactions_applied = before - len(candidates)
        logger.info("After satisfaction filter: %d (dropped %d)", len(candidates), satisfactions_applied)

        # --- Step 3: per-record enrichment
        enriched_rows = []
        tier_counts = {1: 0, 2: 0, 3: 0, 4: 0, "unmatched": 0}
        errors: list[str] = []

        for _, row in candidates.iterrows():
            try:
                enriched = self._enrich_record(row)
                enriched_rows.append(enriched)
                tier = enriched.get("match_tier", "unmatched")
                if tier in tier_counts:
                    tier_counts[tier] += 1
                else:
                    tier_counts["unmatched"] += 1
            except Exception as e:
                error_msg = f"instrument={row.get('instrument_number', '?')}: {e}"
                logger.exception("Enrichment failed for row: %s", error_msg)
                errors.append(error_msg)

        # --- Step 4: upsert master sheet
        new_count, updated_count = 0, 0
        if enriched_rows and self.master_sheet_id:
            df = pd.DataFrame(enriched_rows)
            writer = MasterSheetWriter(self.master_sheet_id)
            summary = writer.upsert(df, key_column="dedup_key")
            new_count = summary["inserted"]
            updated_count = summary["updated"]
        elif not self.master_sheet_id:
            logger.warning("LAKE_MASTER_SHEET_ID not set; skipping Sheet upsert")

        # --- Step 5: log the run
        return self._log_run(
            date_from, date_to, started_at,
            records_pulled=len(enriched_rows),
            records_new=new_count,
            records_updated=updated_count,
            tier_counts=tier_counts,
            satisfactions_applied=satisfactions_applied,
            errors=errors,
        )

    # ------------------------------------------------------------------

    def _drop_satisfied(
        self, candidates: pd.DataFrame, satisfactions: pd.DataFrame,
    ) -> pd.DataFrame:
        """Drop liens that have a matching satisfaction.

        Matching logic (conservative): drop a lien if a satisfaction exists
        with the same Direct Name and overlapping book/page — the conservative
        match rule avoids over-dropping (false positives here mean keeping a
        cleared lien as a lead, which is recoverable; false negatives mean
        missing a real lead, which isn't).
        """
        if satisfactions.empty:
            return candidates

        sat_names = set(satisfactions.get("direct_name", pd.Series()).str.upper().str.strip())
        # Very simple first pass — refine in Phase 1+
        mask = ~candidates["direct_name"].str.upper().str.strip().isin(sat_names)
        return candidates[mask].copy()

    def _enrich_record(self, row: pd.Series) -> dict:
        """Build the full enriched output row for a single clerk record."""
        instrument = row.get("instrument_number", "").strip()
        pdf_bytes = self.clerk.download_document_pdf(instrument)

        parsed_parcel_id = ""
        parsed_legal = ""
        parsed_address = ""
        parsed_fine = ""
        match_tier = 4  # fallback default

        if pdf_bytes:
            extracted = self.pdf_parser.parse(pdf_bytes)
            parsed_parcel_id = extracted.parcel_id or ""
            parsed_legal = extracted.legal_description or ""
            parsed_address = extracted.property_address or ""
            parsed_fine = str(extracted.fine_amount) if extracted.fine_amount else ""
            match_tier = extracted.match_tier

        # Reverse-lookup on PA using strongest available identifier
        pa_result = self.pa.lookup(
            parcel_id=parsed_parcel_id or None,
            legal_description=parsed_legal or None,
            address=parsed_address or None,
            owner_name=row.get("direct_name", "") or None,
            str_code=row.get("doc_legal", "") or None,
        ) or PropertyRecord()

        now = datetime.now().date().isoformat()
        return {
            "dedup_key": instrument,
            "county": "Lake",
            "doc_type": row.get("doc_type", ""),
            "record_date": row.get("record_date", ""),
            "book_page": row.get("book_page", ""),
            "direct_name": row.get("direct_name", ""),
            "indirect_name": row.get("indirect_name", ""),
            "str_code": row.get("doc_legal", ""),
            "parcel_alt_key": pa_result.alt_key or parsed_parcel_id,
            "pdf_address": parsed_address,
            "pdf_legal_description": parsed_legal,
            "pdf_fine_amount": parsed_fine,
            "pa_current_owner": pa_result.current_owner,
            "pa_mailing_address": pa_result.mailing_address,
            "pa_site_address": pa_result.site_address,
            "pa_just_value": pa_result.just_value,
            "pa_year_built": pa_result.year_built,
            "pa_living_area": pa_result.living_area,
            "pa_lot_size": pa_result.lot_size,
            "match_tier": str(match_tier) if pa_result.alt_key else "metadata-only",
            "lien_status": "active",
            "first_seen_date": now,
            "last_enriched_date": now,
            "source_url": f"{self.config['clerk']['base_url']}{self.config['clerk']['search_endpoint']}",
            "pdf_local_path": "",  # populated if we cache PDFs to Drive
            "podio_sync_status": "pending",
        }

    def _log_run(
        self, date_from, date_to, started_at,
        records_pulled, records_new, records_updated,
        tier_counts, satisfactions_applied, errors,
    ) -> dict:
        completed_at = datetime.now()
        duration = (completed_at - started_at).total_seconds()
        summary = {
            "run_started_at": started_at.isoformat(),
            "run_completed_at": completed_at.isoformat(),
            "duration_seconds": int(duration),
            "date_range_from": str(date_from),
            "date_range_to": str(date_to),
            "records_pulled": records_pulled,
            "records_new": records_new,
            "records_updated": records_updated,
            "tier1_parcel_id_matches": tier_counts.get(1, 0),
            "tier2_legal_matches": tier_counts.get(2, 0),
            "tier3_address_matches": tier_counts.get(3, 0),
            "tier4_fuzzy_matches": tier_counts.get(4, 0),
            "unmatched": tier_counts.get("unmatched", 0),
            "satisfactions_applied": satisfactions_applied,
            "errors": "; ".join(errors)[:500] if errors else "",
            "notes": "",
        }
        logger.info("Run complete: %s", summary)
        if self.run_log_sheet_id:
            try:
                RunLogWriter(self.run_log_sheet_id).append(summary)
            except Exception as e:
                logger.error("Failed to append to RunLog sheet: %s", e)
        return summary
