"""Lake County Property Appraiser - real implementation with subdivision matching."""
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
            self.session.get(BASE_URL + DISCLAIMER_PATH,
                             params={"to": SEARCH_PATH}, timeout=30)
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

    def _search(self, owner_name: str = "", subdivision: str = "",
                address: str = "", city: str = "") -> List[str]:
        """Submit search form POST and return Alt Keys from result page."""
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
            "ctl00$cphMain$txtStreet": address.strip(),
            "ctl00$cphMain$txtCity": city.strip(),
            "ctl00$cphMain$txtAlternateKey": "",
            "ctl00$cphMain$txtBook": "",
            "ctl00$cphMain$txtPage": "",
            "ctl00$cphMain$txtSubdivisionName": subdivision.strip(),
            "ctl00$cphMain$txtPropertyName": "",
            "ctl00$cphMain$btnSearch": "Search",
        }
        self._throttle()
        resp = self.session.post(BASE_URL + SEARCH_PATH, data=payload, timeout=30)
        resp.raise_for_status()
        body = resp.text
        if "no results found" in body.lower():
            logger.info("PA: no results owner=%r sub=%r addr=%r",
                        owner_name, subdivision, address)
            return []
        soup = BeautifulSoup(body, "html.parser")
        alt_keys: List[str] = []
        for a in soup.find_all("a", href=True):
            m = re.search(r'AltKey=(\d+)', a["href"])
            if m:
                key = m.group(1)
                if key not in alt_keys:
                    alt_keys.append(key)
        if not alt_keys:
            snippet = re.sub(r'\s+', ' ', body)[:300]
            logger.warning("PA: 0 alt keys for owner=%r sub=%r snippet=%s",
                           owner_name, subdivision, snippet)
        else:
            logger.info("PA: owner=%r sub=%r -> %d alt keys",
                        owner_name, subdivision, len(alt_keys))
        return alt_keys

    def search_by_owner(self, owner_name: str) -> List[str]:
        return self._search(owner_name=owner_name)

    def search_by_subdivision_and_owner(self, subdivision: str, owner_name: str = "") -> List[str]:
        return self._search(owner_name=owner_name, subdivision=subdivision)

    def get_details(self, alt_key: str) -> Optional[PropertyRecord]:
        cached = self._cache_get(f"detail:{alt_key}")
        if cached is not None:
            return cached
        self._throttle()
        resp = self.session.get(BASE_URL + DETAIL_PATH,
                                params={"AltKey": alt_key}, timeout=30)
        if resp.status_code != 200:
            logger.warning("PA detail failed alt_key=%s status=%d",
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
        subdivision = _parse_subdivision(str_code) if str_code else ""
        cache_key = f"owner:{owner_name}|sub:{subdivision}"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached

        alt_keys: List[str] = []
        matched_via = "owner_name"
        if subdivision and owner_name:
            alt_keys = self.search_by_subdivision_and_owner(subdivision, owner_name)
            if alt_keys:
                matched_via = "owner+subdivision"
        if not alt_keys and owner_name:
            alt_keys = self.search_by_owner(owner_name)
        if not alt_keys and subdivision:
            alt_keys = self.search_by_subdivision_and_owner(subdivision, "")
            if alt_keys:
                matched_via = "subdivision_only"

        if not alt_keys:
            self._cache_put(cache_key, None)
            return None

        primary = self.get_details(alt_keys[0])
        if primary is not None:
            primary.matched_via = matched_via
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
        if owner_name or str_code:
            r = self.lookup_by_owner(owner_name or "", str_code=str_code)
            if r:
                return r
        return None


def _parse_subdivision(str_code: str) -> str:
    """Extract subdivision name from clerk Doc Legal like 'LT 229 RESERVES AT HAMMOCK OAKS PH 2A'."""
    if not str_code:
        return ""
    s = str_code.upper().strip()
    s = re.sub(r'^LT\s+\d+[A-Z]?\s+', '', s)
    s = re.sub(r'\s+(PH|PHASE|BLK|BLOCK|UNIT|SEC|ETC\.?).*$', '', s)
    s = s.strip()
    if len(s) < 3 or not re.search(r'[A-Z]', s):
        return ""
    return s


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
