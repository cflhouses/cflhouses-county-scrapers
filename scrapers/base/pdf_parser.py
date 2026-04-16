"""
Cascading identifier extraction from Florida code enforcement lien PDFs.

The goal is to pull the strongest available property identifier from a
recorded code enforcement lien document, in this order of preference:

    Tier 1: Parcel ID / Alt Key (exact match on PA)
    Tier 2: Full legal description (subdivision / lot / block)
    Tier 3: Property address
    Tier 4: Owner name + Section-Township-Range fallback

Each municipal filer formats liens slightly differently, so the parser is
intentionally permissive — it tries multiple regex patterns for each tier
and returns the first that matches.
"""
from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass
from typing import List, Optional

import pdfplumber

logger = logging.getLogger(__name__)


@dataclass
class ExtractedIdentifiers:
    """Identifiers pulled from a lien PDF, in preference order."""
    parcel_id: Optional[str] = None
    legal_description: Optional[str] = None
    property_address: Optional[str] = None
    fine_amount: Optional[float] = None
    raw_text: str = ""

    @property
    def match_tier(self) -> int:
        """Which tier will the PA enrichment use? 1=parcel, 2=legal, 3=address, 4=fallback."""
        if self.parcel_id:
            return 1
        if self.legal_description:
            return 2
        if self.property_address:
            return 3
        return 4


class LienPDFParser:
    """Parse Florida code enforcement lien PDFs for property identifiers.

    Usage:
        parser = LienPDFParser(parcel_id_patterns=[...])
        extracted = parser.parse(pdf_bytes)
        print(extracted.parcel_id)
    """

    # Default legal-description patterns. Florida liens typically include a
    # "legally described as" or "more particularly described as" phrase
    # followed by the description, often ending with a plat reference.
    DEFAULT_LEGAL_PATTERNS = [
        r'(?:legally\s+described\s+as|more\s+particularly\s+described\s+as|legal\s+description[:\s])[\s:]*(.+?)(?=\n\s*\n|\. )',
        r'Lot\s+\d+[^\n]+Block\s+\d+[^\n]+(?:Plat|PB|Page)[^\n]+',
    ]

    # Florida addresses in lien documents. Looks for "Property Address:" or
    # similar labels, then captures the street-address-shaped text.
    DEFAULT_ADDRESS_PATTERNS = [
        r'(?:Property\s+Address|Subject\s+Property|Located\s+at)[:\s]+([^\n]{10,120})',
        r'\b(\d{1,6}\s+[A-Z][A-Za-z0-9\s\.]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Boulevard|Blvd|Court|Ct|Way|Place|Pl|Circle|Cir|Trail|Trl|Terrace|Ter|Highway|Hwy)[^\n]{0,40})',
    ]

    # Fine amount — look for "total fine", "amount due", etc.
    FINE_AMOUNT_PATTERNS = [
        r'(?:total\s+fine|amount\s+due|lien\s+amount|total\s+amount)[:\s\$]+([0-9,]+\.?\d*)',
        r'\$([0-9,]+\.\d{2})',  # any dollar amount — last resort, weakest signal
    ]

    def __init__(
        self,
        parcel_id_patterns: Optional[List[str]] = None,
        legal_patterns: Optional[List[str]] = None,
        address_patterns: Optional[List[str]] = None,
    ):
        self.parcel_id_patterns = parcel_id_patterns or []
        self.legal_patterns = legal_patterns or self.DEFAULT_LEGAL_PATTERNS
        self.address_patterns = address_patterns or self.DEFAULT_ADDRESS_PATTERNS

    def parse(self, pdf_bytes: bytes) -> ExtractedIdentifiers:
        """Parse a PDF and return extracted identifiers.

        If the PDF is image-only (scanned without OCR), text extraction will
        return empty and we'll return an empty ExtractedIdentifiers.
        Phase 1+ will add a pytesseract OCR fallback for older scanned docs.
        """
        text = self._extract_text(pdf_bytes)
        result = ExtractedIdentifiers(raw_text=text)

        if not text.strip():
            logger.warning("PDF appears to be image-only; no text extracted")
            return result

        result.parcel_id = self._find_first(text, self.parcel_id_patterns)
        if not result.parcel_id:
            # Also try grabbing any long digit-sequence labeled as a parcel/alt key
            result.parcel_id = self._find_first(
                text,
                [r'(?:Alt(?:ernate)?\s*Key|Parcel\s*(?:ID|Number|#))[:\s]+([A-Z0-9\-]{5,30})'],
            )

        result.legal_description = self._find_first(text, self.legal_patterns)
        result.property_address = self._find_first(text, self.address_patterns)
        result.fine_amount = self._find_fine_amount(text)

        logger.info(
            "Parsed PDF: tier=%d parcel=%s legal=%s address=%s fine=%s",
            result.match_tier,
            bool(result.parcel_id),
            bool(result.legal_description),
            bool(result.property_address),
            result.fine_amount,
        )
        return result

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _extract_text(self, pdf_bytes: bytes) -> str:
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                return "\n".join((page.extract_text() or "") for page in pdf.pages)
        except Exception as e:
            logger.error("PDF text extraction failed: %s", e)
            return ""

    def _find_first(self, text: str, patterns: List[str]) -> Optional[str]:
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                # If the pattern has a capture group, return it; else return
                # the whole match. Clean up whitespace.
                value = match.group(1) if match.groups() else match.group(0)
                return re.sub(r'\s+', ' ', value).strip()
        return None

    def _find_fine_amount(self, text: str) -> Optional[float]:
        raw = self._find_first(text, self.FINE_AMOUNT_PATTERNS)
        if not raw:
            return None
        cleaned = raw.replace(",", "").replace("$", "").strip()
        try:
            return float(cleaned)
        except ValueError:
            return None
