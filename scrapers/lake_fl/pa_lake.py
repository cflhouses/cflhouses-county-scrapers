"""Lake County Property Appraiser - Playwright-driven implementation."""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

BASE_URL = "https://lakecopropappr.com"
SEARCH_URL = BASE_URL + "/property-search.aspx"
DETAIL_URL = BASE_URL + "/property-details.aspx"


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
    """Playwright-driven client. Spins up a single browser per instance."""

    def __init__(self, cache_ttl_days: int = 30, rate_limit_seconds: float = 1.5):
        self.cache_ttl = timedelta(days=cache_ttl_days)
        self.rate_limit_seconds = rate_limit_seconds
        self._cache: Dict[str, tuple] = {}
        self._last_request_at: float = 0.0
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None
        self._disclaimer_accepted = False

    def _ensure_browser(self) -> None:
        if self._page is not None:
            return
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=True)
        self._context = self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
            ),
        )
        self._page = self._context.new_page()
        self._page.set_default_timeout(30000)
        logger.info("PA: Playwright browser ready")

    def close(self) -> None:
        try:
            if self._context is not None:
                self._context.close()
            if self._browser is not None:
                self._browser.close()
            if self._pw is not None:
                self._pw.stop()
        except Exception as e:
            logger.warning("PA: error closing browser: %s", e)

    def __del__(self):
        self.close()

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

    def _accept_disclaimer_if_needed(self) -> None:
        if self._disclaimer_accepted:
            return
        self._ensure_browser()
        self._throttle()
        self._page.goto(SEARCH_URL, wait_until="domcontentloaded")
        try:
            btn = self._page.query_selector("a:has-text(\"I AGREE\"), input[value*=\"AGREE\"], img[alt*=\"AGREE\"]")
            if btn is None:
                btn = self._page.query_selector("a[href*=\"to=\"]")
            if btn is not None:
                btn.click()
                self._page.wait_for_load_state("domcontentloaded")
                logger.info("PA: clicked I AGREE on disclaimer")
        except Exception as e:
            logger.debug("PA: no disclaimer to click (%s)", e)
        self._disclaimer_accepted = True

    def _search(self, owner_name: str = "", subdivision: str = "") -> List[str]:
        self._accept_disclaimer_if_needed()
        self._throttle()
        self._page.goto(SEARCH_URL, wait_until="domcontentloaded")
        try:
            self._page.fill("#cphMain_txtOwnerName", owner_name.strip())
            if subdivision:
                sel = self._page.query_selector("#cphMain_txtSubdivisionName")
                if sel is not None:
                    sel.fill(subdivision.strip())
            self._page.click("#cphMain_btnSearch")
            self._page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as e:
            logger.warning("PA: form interaction failed owner=%r sub=%r err=%s",
                           owner_name, subdivision, e)
            return []

        body = self._page.content()
        if "no results found" in body.lower():
            logger.info("PA: no results owner=%r sub=%r", owner_name, subdivision)
            return []

        alt_keys: List[str] = []
        try:
            links = self._page.query_selector_all("a[href*=\"AltKey=\"]")
            for link in links:
                href = link.get_attribute("href") or ""
                m = re.search(r'AltKey=(\d+)', href)
                if m:
                    key = m.group(1)
                    if key not in alt_keys:
                        alt_keys.append(key)
        except Exception as e:
            logger.warning("PA: link extraction failed: %s", e)

        if alt_keys:
            logger.info("PA: owner=%r sub=%r -> %d alt keys",
                        owner_name, subdivision, len(alt_keys))
        else:
            logger.warning("PA: 0 alt keys for owner=%r sub=%r",
                           owner_name, subdivision)
        return alt_keys

    def search_by_owner(self, owner_name: str) -> List[str]:
        return self._search(owner_name=owner_name)

    def search_by_subdivision_and_owner(self, subdivision: str, owner_name: str = "") -> List[str]:
        return self._search(owner_name=owner_name, subdivision=subdivision)

    def get_details(self, alt_key: str) -> Optional[PropertyRecord]:
        cached = self._cache_get(f"detail:{alt_key}")
        if cached is not None:
            return cached
        self._ensure_browser()
        self._throttle()
        try:
            self._page.goto(f"{DETAIL_URL}?AltKey={alt_key}", wait_until="domcontentloaded")
        except Exception as e:
            logger.warning("PA detail nav failed alt_key=%s err=%s", alt_key, e)
            self._cache_put(f"detail:{alt_key}", None)
            return None
        html = self._page.content()
        record = self._parse_detail_page(html, alt_key)
        self._cache_put(f"detail:{alt_key}", record)
        return record

    def _parse_detail_page(self, html: str, alt_key: str) -> PropertyRecord:
        from bs4 import BeautifulSoup
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
