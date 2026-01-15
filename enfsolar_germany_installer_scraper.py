#!/usr/bin/env python3
"""
ENF Solar Germany installer directory scraper.

Dependencies:
    pip install requests beautifulsoup4 pandas openpyxl playwright
    python -m playwright install

Usage examples:
    python enfsolar_germany_installer_scraper.py --out enf_installers_germany.xlsx
    python enfsolar_germany_installer_scraper.py --max-pages 3 --workers 4 --delay 0.8
    python enfsolar_germany_installer_scraper.py --resume
    python enfsolar_germany_installer_scraper.py --retry-failed
    python enfsolar_germany_installer_scraper.py --mode playwright --headful --storage-state storage_state.json

Compliance notice:
    Please respect the target website's robots.txt and Terms of Service. Keep request
    rates low and only use this tool for lawful purposes. If the site explicitly
    disallows scraping, do not proceed.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright
except ImportError:  # pragma: no cover - optional dependency
    sync_playwright = None


BASE_URL = "https://www.enfsolar.com/directory/installer/Germany"
DEFAULT_CHECKPOINT = "checkpoint.jsonl"
DEFAULT_FAILED_URLS = "failed_urls.txt"


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]


CHALLENGE_TITLE_KEYWORDS = ("just a moment", "attention required")
CHALLENGE_BODY_KEYWORDS = ("captcha", "challenge", "verify you are human")


@dataclass
class ScraperConfig:
    out_path: Path
    max_pages: Optional[int]
    workers: int
    delay: float
    timeout: int
    resume: bool
    mode: str
    headful: bool
    storage_state: Optional[Path]
    checkpoint_path: Path
    failed_urls_path: Path


class RateLimiter:
    def __init__(self, delay: float) -> None:
        self.delay = delay
        self.lock = threading.Lock()
        self.last_request = 0.0
        self.penalty_multiplier = 1.0

    def sleep(self) -> None:
        with self.lock:
            now = time.monotonic()
            base_delay = self.delay * self.penalty_multiplier
            jitter = random.uniform(base_delay * 0.7, base_delay * 1.3)
            wait_for = self.last_request + jitter - now
            if wait_for > 0:
                time.sleep(wait_for)
            self.last_request = time.monotonic()

    def penalize(self) -> None:
        with self.lock:
            self.penalty_multiplier = min(self.penalty_multiplier * 1.5, 5.0)


class ChallengeMonitor:
    def __init__(self) -> None:
        self.triggered = threading.Event()
        self.serial_lock = threading.Lock()

    def mark_triggered(self) -> None:
        self.triggered.set()

    def should_serialize(self) -> bool:
        return self.triggered.is_set()


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
            "DNT": "1",
            "Upgrade-Insecure-Requests": "1",
            "Referer": "https://www.enfsolar.com/",
        }
    )
    return session


def is_challenge_page(response: requests.Response, html: str) -> bool:
    if response.status_code in {403, 503}:
        return True
    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.string or "").strip().lower() if soup.title else ""
    if any(keyword in title for keyword in CHALLENGE_TITLE_KEYWORDS):
        return True
    body_text = soup.get_text(" ", strip=True).lower()
    return any(keyword in body_text for keyword in CHALLENGE_BODY_KEYWORDS)


def _page_looks_challenged(title: str, html: str) -> bool:
    normalized_title = (title or "").strip().lower()
    if any(keyword in normalized_title for keyword in CHALLENGE_TITLE_KEYWORDS):
        return True
    body_text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True).lower()
    return any(keyword in body_text for keyword in CHALLENGE_BODY_KEYWORDS)


def fetch_html(
    session: requests.Session,
    url: str,
    rate_limiter: RateLimiter,
    challenge_monitor: ChallengeMonitor,
    timeout: int,
    max_retries: int = 6,
) -> str:
    backoff = 1.0
    for attempt in range(1, max_retries + 1):
        if challenge_monitor.should_serialize():
            with challenge_monitor.serial_lock:
                rate_limiter.sleep()
                html = _fetch_once(session, url, timeout)
        else:
            rate_limiter.sleep()
            html = _fetch_once(session, url, timeout)

        if isinstance(html, str):
            return html

        response, error = html
        if error and "Challenge" in str(error):
            challenge_monitor.mark_triggered()
            rate_limiter.penalize()
        if response is not None:
            if response.status_code in {429, 403} or response.status_code >= 500:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        time.sleep(float(retry_after))
                    except ValueError:
                        time.sleep(backoff)
                else:
                    time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
        if error:
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} attempts.")


def _fetch_once(
    session: requests.Session,
    url: str,
    timeout: int,
) -> str | Tuple[Optional[requests.Response], Optional[Exception]]:
    try:
        response = session.get(url, timeout=timeout)
        html = response.text
        if is_challenge_page(response, html):
            return response, RuntimeError("Challenge page detected.")
        response.raise_for_status()
        return html
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, requests.RequestException):
            return getattr(exc, "response", None), exc
        return None, exc


def parse_listing_page(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []
    for anchor in soup.select("a[href]"):
        href = anchor.get("href", "")
        if not href:
            continue
        absolute = urljoin(base_url, href)
        if "enfsolar.com" not in absolute:
            continue
        parsed = urlparse(absolute)
        if "page=" in parsed.query:
            continue
        if "/company/" in parsed.path or "/directory/installer/" in parsed.path:
            if parsed.path.rstrip("/").lower().endswith("/germany"):
                continue
            links.append(absolute)
    return sorted(set(links))


def parse_max_page(html: str) -> Optional[int]:
    soup = BeautifulSoup(html, "html.parser")
    page_numbers = []
    for anchor in soup.select("a[href]"):
        href = anchor.get("href", "")
        if "page=" not in href:
            continue
        match = re.search(r"[?&]page=(\d+)", href)
        if match:
            page_numbers.append(int(match.group(1)))
    return max(page_numbers) if page_numbers else None


def parse_key_value_table(container: BeautifulSoup) -> Dict[str, str]:
    data: Dict[str, str] = {}
    for row in container.select("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) < 2:
            continue
        key = cells[0].get_text(" ", strip=True)
        value = cells[1].get_text(" ", strip=True)
        if key:
            data[key] = value
    for dt in container.select("dt"):
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        key = dt.get_text(" ", strip=True)
        value = dd.get_text(" ", strip=True)
        if key:
            data[key] = value
    return data


def parse_business_details(soup: BeautifulSoup) -> Dict[str, str]:
    for header in soup.find_all(["h2", "h3", "h4"]):
        title = header.get_text(" ", strip=True).lower()
        if "business details" in title:
            section = header.find_next_sibling()
            if not section:
                section = header.parent
            if section:
                details = parse_key_value_table(section)
                if details:
                    return details
    return {}


def parse_profile_page(html: str, url: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    company_name = ""
    header = soup.find(["h1", "h2"])
    if header:
        company_name = header.get_text(" ", strip=True)

    info_data: Dict[str, str] = {}
    for table in soup.select("table"):
        info_data.update(parse_key_value_table(table))

    address_raw = _extract_field(info_data, ["address", "location"])
    phone = _extract_field(info_data, ["phone", "telephone", "tel"])
    website = _extract_field(info_data, ["website", "web"])
    country = _extract_field(info_data, ["country"])

    if not address_raw:
        address_node = soup.select_one(".address, .company-address")
        if address_node:
            address_raw = address_node.get_text(" ", strip=True)

    if not phone:
        phone_link = soup.select_one("a[href^='tel:']")
        if phone_link:
            phone = phone_link.get("href", "").replace("tel:", "").strip()

    if not website:
        website_link = soup.select_one("a[href^='http']")
        if website_link:
            website = website_link.get("href", "").strip()

    business_details = parse_business_details(soup)
    last_update = business_details.get("Last Update") or business_details.get("Last update")

    street_address, postal_code, city = normalize_address(address_raw or "")

    result = {
        "company_name": company_name,
        "street_address": street_address,
        "postal_code": postal_code,
        "city": city,
        "address_raw": address_raw,
        "phone": phone,
        "website": website,
        "country": country,
        "profile_url": url,
        "business_details": business_details,
        "last_update_raw": last_update,
        "last_update_date": parse_date(last_update) if last_update else None,
    }
    return result


def _extract_field(info_data: Dict[str, str], keys: Iterable[str]) -> Optional[str]:
    for key in info_data:
        normalized = key.lower().strip()
        for expected in keys:
            if expected in normalized:
                return info_data[key]
    return None


def normalize_address(address_raw: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not address_raw:
        return None, None, None
    match = re.search(r"\b(\d{5})\b\s*,?\s*([^,]+)", address_raw)
    postal_code = match.group(1) if match else None
    city = match.group(2).strip() if match else None
    street_address = address_raw
    if match:
        street_address = address_raw[: match.start()].strip().rstrip(",")
    if street_address == address_raw:
        street_address = address_raw.strip()
    return street_address or None, postal_code, city


def parse_date(text: str) -> Optional[str]:
    if not text:
        return None
    text = text.strip()
    formats = ["%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%b %Y", "%B %Y"]
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.date().isoformat()
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text)
        return parsed.date().isoformat()
    except ValueError:
        return None


def load_checkpoint(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def append_checkpoint(path: Path, row: Dict[str, str]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def export_excel(rows: List[Dict[str, str]], out_path: Path) -> None:
    base_columns = [
        "company_name",
        "street_address",
        "postal_code",
        "city",
        "address_raw",
        "phone",
        "website",
        "country",
        "profile_url",
        "last_update_raw",
        "last_update_date",
    ]
    business_keys = sorted(
        {key for row in rows for key in row.get("business_details", {}).keys()}
    )
    expanded_rows = []
    for row in rows:
        expanded = {column: row.get(column) for column in base_columns}
        for key in business_keys:
            expanded[f"business_details__{key}"] = row.get("business_details", {}).get(key)
        expanded_rows.append(expanded)
    df = pd.DataFrame(expanded_rows)
    df.to_excel(out_path, index=False)


def write_failed_urls(path: Path, failed_urls: Iterable[str]) -> None:
    if not failed_urls:
        return
    with path.open("a", encoding="utf-8") as handle:
        for url in failed_urls:
            handle.write(url + "\n")


class PlaywrightFetcher:
    def __init__(self, headful: bool, storage_state: Optional[Path]) -> None:
        if sync_playwright is None:
            raise RuntimeError("Playwright is not installed. Run `pip install playwright`.")
        self.headful = headful
        self.storage_state = storage_state
        self.playwright = None
        self.browser = None
        self.context = None

    def start(self) -> None:
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=not self.headful)
        if self.storage_state and self.storage_state.exists():
            self.context = self.browser.new_context(storage_state=str(self.storage_state))
        else:
            self.context = self.browser.new_context()

    def close(self) -> None:
        if self.context:
            if self.storage_state:
                self.context.storage_state(path=str(self.storage_state))
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

    def fetch(self, url: str, timeout: int) -> str:
        if not self.context:
            raise RuntimeError("Playwright context not initialized.")
        page = self.context.new_page()
        page.goto(url, timeout=timeout * 1000, wait_until="domcontentloaded")
        if _page_looks_challenged(page.title(), page.content()):
            if self.headful:
                print(
                    "Challenge detected in Playwright mode. Please solve it in the "
                    "browser window, then press Enter to continue."
                )
                input()
                page.reload(wait_until="domcontentloaded")
        html = page.content()
        page.close()
        return html


def collect_profile_urls(
    session: requests.Session,
    rate_limiter: RateLimiter,
    challenge_monitor: ChallengeMonitor,
    config: ScraperConfig,
) -> Tuple[List[str], int]:
    first_html = fetch_html(
        session,
        BASE_URL,
        rate_limiter,
        challenge_monitor,
        timeout=config.timeout,
    )
    max_page = parse_max_page(first_html) or config.max_pages or 1
    if config.max_pages:
        max_page = min(max_page, config.max_pages)

    profile_urls = set(parse_listing_page(first_html, BASE_URL))
    for page in range(2, max_page + 1):
        url = f"{BASE_URL}?page={page}"
        html = fetch_html(session, url, rate_limiter, challenge_monitor, timeout=config.timeout)
        profile_urls.update(parse_listing_page(html, BASE_URL))
    return sorted(profile_urls), max_page


def scrape_profiles_requests(
    profile_urls: List[str],
    session: requests.Session,
    rate_limiter: RateLimiter,
    challenge_monitor: ChallengeMonitor,
    config: ScraperConfig,
    processed_urls: set[str],
) -> Tuple[List[Dict[str, str]], List[str]]:
    rows: List[Dict[str, str]] = []
    failed: List[str] = []

    def worker(url: str) -> Optional[Dict[str, str]]:
        if url in processed_urls:
            return None
        try:
            html = fetch_html(session, url, rate_limiter, challenge_monitor, timeout=config.timeout)
            row = parse_profile_page(html, url)
            append_checkpoint(config.checkpoint_path, row)
            return row
        except Exception:  # noqa: BLE001
            failed.append(url)
            return None

    with ThreadPoolExecutor(max_workers=config.workers) as executor:
        futures = {executor.submit(worker, url): url for url in profile_urls}
        for future in as_completed(futures):
            result = future.result()
            if result:
                rows.append(result)
    return rows, failed


def scrape_profiles_playwright(
    profile_urls: List[str],
    fetcher: PlaywrightFetcher,
    config: ScraperConfig,
    processed_urls: set[str],
) -> Tuple[List[Dict[str, str]], List[str]]:
    rows: List[Dict[str, str]] = []
    failed: List[str] = []
    for url in profile_urls:
        if url in processed_urls:
            continue
        try:
            html = fetcher.fetch(url, config.timeout)
            row = parse_profile_page(html, url)
            append_checkpoint(config.checkpoint_path, row)
            rows.append(row)
        except Exception:  # noqa: BLE001
            failed.append(url)
    return rows, failed


def build_config(args: argparse.Namespace) -> ScraperConfig:
    return ScraperConfig(
        out_path=Path(args.out),
        max_pages=args.max_pages,
        workers=args.workers,
        delay=args.delay,
        timeout=args.timeout,
        resume=args.resume,
        mode=args.mode,
        headful=args.headful,
        storage_state=Path(args.storage_state) if args.storage_state else None,
        checkpoint_path=Path(args.checkpoint),
        failed_urls_path=Path(args.failed_urls),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="ENF Solar Germany installers scraper")
    parser.add_argument("--out", default="enf_installers_germany.xlsx")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--delay", type=float, default=0.8)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--mode", choices=["requests", "playwright"], default="requests")
    parser.add_argument("--headful", action="store_true")
    parser.add_argument("--storage-state", default=None)
    parser.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT)
    parser.add_argument("--failed-urls", default=DEFAULT_FAILED_URLS)
    parser.add_argument("--retry-failed", action="store_true")
    args = parser.parse_args()

    config = build_config(args)
    rate_limiter = RateLimiter(config.delay)
    challenge_monitor = ChallengeMonitor()
    session = build_session()

    start_time = time.monotonic()
    processed_rows = load_checkpoint(config.checkpoint_path) if config.resume else []
    processed_urls = {row.get("profile_url") for row in processed_rows if row.get("profile_url")}

    if args.retry_failed and config.failed_urls_path.exists():
        profile_urls = [
            line.strip()
            for line in config.failed_urls_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        max_page = 0
    else:
        profile_urls, max_page = collect_profile_urls(
            session, rate_limiter, challenge_monitor, config
        )

    if challenge_monitor.should_serialize():
        print("Challenge detected. Reducing concurrency to 1 and increasing delay.")
        config.workers = 1
        rate_limiter.penalize()
        print("Consider switching to --mode playwright if challenges persist.")

    if config.mode == "playwright":
        config.workers = 1
        fetcher = PlaywrightFetcher(config.headful, config.storage_state)
        fetcher.start()
        try:
            rows, failed = scrape_profiles_playwright(
                profile_urls, fetcher, config, processed_urls
            )
        finally:
            fetcher.close()
    else:
        rows, failed = scrape_profiles_requests(
            profile_urls, session, rate_limiter, challenge_monitor, config, processed_urls
        )

    all_rows = load_checkpoint(config.checkpoint_path)
    deduped: Dict[str, Dict[str, str]] = {}
    for row in all_rows:
        url = row.get("profile_url")
        if url:
            deduped[url] = row

    export_excel(list(deduped.values()), config.out_path)
    write_failed_urls(config.failed_urls_path, failed)

    elapsed = time.monotonic() - start_time
    print(
        "Done. Pages: {pages} | Profile URLs: {total_urls} | "
        "Success: {success} | Failed: {failed_count} | Elapsed: {elapsed:.1f}s".format(
            pages=max_page,
            total_urls=len(profile_urls),
            success=len(deduped),
            failed_count=len(failed),
            elapsed=elapsed,
        )
    )
    if failed:
        print(f"Failed URLs written to: {config.failed_urls_path}")


if __name__ == "__main__":
    main()
