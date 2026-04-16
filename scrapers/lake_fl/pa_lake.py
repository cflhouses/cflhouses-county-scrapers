"""
Lake County Property Appraiser - real implementation.

Flow:
    1. GET /property-search.aspx -> extract ASP.NET hidden state fields
    2. POST with owner name + hidden fields -> HTML result table
    3. Parse table rows for Alt Keys
    4. For each Alt Key: GET /property-details.aspx?AltKey=X
    5. Parse detail page into PropertyRecord

Book/Page search was tested and does NOT work for code-enforcement liens -
the PA only indexes ownership transfer deeds on Book/Page.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://lakecopropappr.com"
SEARCH_PATH = "/property-search.aspx"
DETAIL_PATH = "/property-details.aspx"
DISCLAIMER_PATH = "/property-disclaimer.aspx"


@dataclass
class PropertyRecord:
    alt_key: str = ""
    parcel_number: str = ""
    current_owner: str = ""
    mailing_address: str = ""
    site_address: str = ""
    just_value: str = ""
    year_built: str = ""
    living_area: str = ""
    lot_size: str = ""
    property_use: str = ""
    city: str = ""
    property_description: str = ""
    matched_via: str = ""
    alternate_matches: int = 0


class LakePropertyAppraiser:
    """Live HTTP client against the Lake County Property Appraiser."""

    def __init__(self, cache_ttl_days: int = 30, rate_limit_seconds: float = 3.0):
        self.cache_ttl = timedelta(days=cache_ttl_days)
        self.rate_limit_seconds = rate_limit_seconds
        self._cache: Dict[str, tuple] = {}
        self._last_request_at: float = 0.0
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "CFL-County-Scraper/1.0 (contact: CFLHousesLLC@gmail.com)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        self._disclaimer_accepted = False

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.rate_limit_seconds:
            time.sleep(self.rate_limit_seconds - elapsed)
        self._last_request_at = time.monotonic()

    def _cache_get(self, key: str):
        if key in self._cache:
            cached_at, value = self._cache[key]
            if datetime.now() - cached_at < self.cache_ttl:
                return value
        return None

    def _cache_put(self, key: str, value) -> None:
        self._cache[key] = (datetime.now(), value)

    def accept_disclaimer(self) -> None:
        if self._disclaimer_accepted:
            return
        self._throttle()
        try:
            self.session.get(
                BASE_URL + DISCLAIMER_PATH,
                params={"to": SEARCH_PATH},
                timeout=30,
            )
        except requests.RequestException as e:
            logger.warning("Disclaimer GET failed (continuing): %s", e)
        self._disclaimer_accepted = True

    def _extract_hidden_fields(self, soup: BeautifulSoup) -> Dict[str, str]:
        fields = {}
        for name in ["__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION",
                     "__VIEWSTATEENCRYPTED", "__EVENTTARGET", "__EVENTARGUMENT",
                     "__LASTFOCUS"]:
            el = soup.find("input", attrs={"name": name})
            if el is not None:
                fields[name] = el.get("value", "") or ""
        return fields

    def search_by_owner(self, owner_name: str) -> List[str]:
        if not self._disclaimer_accepted:
            self.accept_disclaimer()
        self._throttle()
        resp = self.session.get(BASE_URL + SEARCH_PATH, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        hidden = self._extract_hidden_fields(soup)
        payload = {
            **hidden,
            "ctl00$cphMain$rblRealTangible": "R",
            "ctl00$cphMain$txtOwnerName": owner_name.strip(),
            "ctl00$cphMain$txtStreet": "",
            "ctl00$cphMain$txtCity": "",
            "ctl00$cphMain$txtAlternateKey": "",
            "ctl00$cphMain$txtBook": "",
            "ctl00$cphMain$txtPage": "",
            "ctl00$cphMain$txtSubdivisionName": "",
            "ctl00$cphMain$txtPropertyName": "",
            "ctl00$cphMain$btnSearch": "Search",
        }
        self._throttle()
        resp = self.session.post(BASE_URL + SEARCH_PATH, data=payload, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        if "no results found" in resp.text.lower():
            logger.info("PA: no results for owner=%r", owner_name)
            return []
        alt_keys: List[str] = []
        for a in soup.find_all("a", href=True):
            m = re.search(r'AltKey=(\d+)', a["href"])
            if m:
                key = m.group(1)
                if key not in alt_keys:
                    alt_keys.append(key)
        logger.info("PA: owner=%r returned %d alt keys", owner_name, len(alt_keys))
        return alt_keys

    def get_details(self, alt_key: str) -> Optional[PropertyRecord]:
        cached = self._cache_get(f"detail:{alt_key}")
        if cached is not None:
            return cached
        self._throttle()
        resp = self.session.get(BASE_URL + DETAIL_PATH,
                                params={"AltKey": alt_key}, timeout=30)
        if resp.status_code != 200:
            logger.warning("PA detail fetch failed alt_key=%s status=%d",
                           alt_key, resp.status_code)
            self._cache_put(f"detail:{alt_key}", None)
            return None
        record = self._parse_detail_page(resp.text, alt_key)
        self._cache_put(f"detail:{alt_key}", record)
        return record

    def _parse_detail_page(self, html: str, alt_key: str) -> PropertyRecord:
        soup = BeautifulSoup(html, "html.parser")
        record = PropertyRecord(alt_key=alt_key)
        fields: Dict[str, str] = {}
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            for i in range(0, len(cells) - 1, 2):
                label = cells[i].get_text(separator=" ", strip=True).rstrip(":").strip()
                value = cells[i + 1].get_text(separator=" ", strip=True)
                if label and value and label not in fields:
                    fields[label] = value

        def get(*labels: str) -> str:
            for label in labels:
                for key, val in fields.items():
                    if key.lower().startswith(label.lower()):
                        return val
            return ""

        record.current_owner = get("Name").strip()
        record.mailing_address = re.sub(r'\s+Update.*$', '', get("Mailing Address")).strip()
        record.site_address = get("Property Location").strip()
        record.parcel_number = get("Parcel Number").strip()
        record.property_description = get("Property Description").strip()
        record.city = get("Millage Group and City").strip()
        record.year_built = _extract_year(html)
        record.living_area = _extract_living_area(html)
        record.just_value = _extract_just_value(html)
        record.property_use = _extract_property_use(html)
        return record

    def lookup_by_owner(self, owner_name: str, str_code: Optional[str] = None) -> Optional[PropertyRecord]:
        cache_key = f"owner:{owner_name}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        alt_keys = self.search_by_owner(owner_name)
        if not alt_keys:
            self._cache_put(cache_key, None)
            return None
        primary = self.get_details(alt_keys[0])
        if primary is not None:
            primary.matched_via = "owner_name"
            primary.alternate_matches = len(alt_keys) - 1
        self._cache_put(cache_key, primary)
        return primary

    def lookup_by_parcel_id(self, alt_key: str) -> Optional[PropertyRecord]:
        record = self.get_details(alt_key)
        if record is not None:
            record.matched_via = "parcel_id"
        return record

    def lookup_by_legal(self, legal_description: str) -> Optional[PropertyRecord]:
        return None

    def lookup_by_address(self, address: str) -> Optional[PropertyRecord]:
        return None

    def lookup(self, parcel_id: Optional[str] = None, legal_description: Optional[str] = None,
               address: Optional[str] = None, owner_name: Optional[str] = None,
               str_code: Optional[str] = None) -> Optional[PropertyRecord]:
        if parcel_id:
            r = self.lookup_by_parcel_id(parcel_id)
            if r:
                return r
        if owner_name:
            r = self.lookup_by_owner(owner_name, str_code=str_code)
            if r:
                return r
        return None


def _extract_year(html: str) -> str:
    m = re.search(r'Year\s*Built:\s*(\d{4})', html)
    return m.group(1) if m else ""


def _extract_living_area(html: str) -> str:
    m = re.search(r'Total\s*Living\s*Area:\s*([\d,]+)', html)
    return m.group(1).replace(',', '') if m else ""


def _extract_just_value(html: str) -> str:
    m = re.search(r'SCHOOL\s*BOARD\s*STATE.*?\$?([\d,]+)', html, re.DOTALL | re.IGNORECASE)
    return m.group(1).replace(',', '') if m else ""


def _extract_property_use(html: str) -> str:
    m = re.search(r'Property\s*Use[^<]*<[^>]+>\s*([^<\n]+)', html, re.IGNORECASE)
    return (m.group(1).strip() if m else "")[:60]

