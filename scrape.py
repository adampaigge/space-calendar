#!/usr/bin/env python3
"""
Space Calendar scraper — UK space events
Writes: data/events.json

Run:
    python scrape.py              # full scrape
    python scrape.py --source groundstation  # single source
    python scrape.py --dry-run    # no file write
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing deps — run: pip install requests beautifulsoup4 lxml")
    sys.exit(1)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
EVENTS_PATH = DATA_DIR / "events.json"
RUNS_PATH = DATA_DIR / "runs.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

# ─── Data model ───────────────────────────────────────────────────────────────

@dataclass
class Event:
    id: str
    title: str
    url: str
    start_date: str        # ISO 8601
    end_date: Optional[str]
    location: Optional[str]
    description: Optional[str]
    organiser: Optional[str]
    tags: list[str]
    country: str
    source_id: str
    source_name: str
    source_url: str
    online: bool
    confidence: float
    first_seen: str
    last_seen: str
    all_source_ids: list[str] = field(default_factory=list)
    all_source_names: list[str] = field(default_factory=list)


# ─── Date parsing ─────────────────────────────────────────────────────────────

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

def parse_date(raw: str | None) -> datetime | None:
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None

    # ISO 8601 variants
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d",
        "%Y%m%dT%H%M%SZ", "%Y%m%d",
    ):
        try:
            d = datetime.strptime(s[:len(fmt.replace("%", "XX"))], fmt)
            return d.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    # "15 June 2025" / "June 15, 2025" / "15 Jun 25"
    m = re.search(
        r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{2,4})|([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{2,4})",
        s
    )
    if m:
        g = m.groups()
        if g[0]:
            day, mon_str, year_str = g[0], g[1], g[2]
        else:
            mon_str, day, year_str = g[3], g[4], g[5]
        month = MONTH_MAP.get(mon_str.lower())
        if month:
            year = int(year_str) + (2000 if len(year_str) == 2 else 0)
            try:
                return datetime(int(year), month, int(day), tzinfo=timezone.utc)
            except ValueError:
                pass

    # DD/MM/YYYY
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)), tzinfo=timezone.utc)
        except ValueError:
            pass

    # datetime[attr] like "2025-06-15T09:00" (from HTML time elements)
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    return None


# ─── Tag inference ────────────────────────────────────────────────────────────

TAG_PATTERNS = [
    ("conference",  [r"\bconference\b", r"\bcongress\b", r"\bsummit\b", r"\bsymposium\b"]),
    ("workshop",    [r"\bworkshop\b", r"\btraining\b", r"\bcourse\b", r"\bmasterclass\b"]),
    ("webinar",     [r"\bwebinar\b", r"\bonline\s+talk\b", r"\bvirtual\b"]),
    ("meetup",      [r"\bmeetup\b", r"\bnetworking\b", r"\bmixer\b", r"\bgathering\b"]),
    ("lecture",     [r"\blecture\b", r"\btalk\b", r"\bpresentation\b", r"\bseminar\b"]),
    ("hackathon",   [r"\bhackathon\b", r"\bchallenge\b", r"\bcompetition\b"]),
    ("awards",      [r"\bawards?\b", r"\bprize\b", r"\bceremony\b"]),
    ("exhibition",  [r"\bexhibition\b", r"\bexpo\b", r"\bairshow\b"]),
    ("launch",      [r"\blaunch\b", r"\bannouncement\b"]),
]

def infer_tags(title: str, desc: str = "") -> list[str]:
    haystack = f"{title} {desc}".lower()
    tags = []
    for tag, patterns in TAG_PATTERNS:
        if any(re.search(p, haystack) for p in patterns):
            tags.append(tag)
    return tags or ["other"]


# ─── Utilities ────────────────────────────────────────────────────────────────

def make_id(source_id: str, title: str, date: datetime) -> str:
    key = f"{source_id}:{title.lower().strip()}:{date.strftime('%Y-%m-%d')}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]

def make_dedup_key(title: str, date: datetime) -> str:
    t = re.sub(r"[^a-z0-9\s]", "", title.lower())
    t = re.sub(r"\s+", " ", t).strip()
    return f"{t}::{date.strftime('%Y-%m-%d')}"

def is_online(loc: str | None) -> bool:
    if not loc:
        return False
    return any(w in loc.lower() for w in ("online", "virtual", "zoom", "teams", "webinar", "livestream"))

def resolve_url(href: str, base: str) -> str:
    if not href:
        return base
    try:
        return urljoin(base, href)
    except Exception:
        return href

def fetch(url: str, retries: int = 2, timeout: int = 15) -> str | None:
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            if r.status_code == 429:
                time.sleep((attempt + 1) * 3)
                continue
            if r.ok:
                return r.text
            print(f"    HTTP {r.status_code} — {url}")
            return None
        except Exception as e:
            if attempt == retries:
                print(f"    Fetch failed: {e}")
                return None
            time.sleep(1 * (attempt + 1))
    return None


# ─── Strategy: JSON-LD ───────────────────────────────────────────────────────

def scrape_jsonld(source: dict) -> list[dict]:
    html = fetch(source["url"])
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    events = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        items = data if isinstance(data, list) else data.get("@graph", [data])
        for item in items:
            t = item.get("@type", "")
            types = t if isinstance(t, list) else [t]
            if "Event" not in types:
                continue
            loc = item.get("location", {})
            if isinstance(loc, dict):
                loc_str = ", ".join(filter(None, [
                    loc.get("name"),
                    (loc.get("address") or {}).get("streetAddress") if isinstance(loc.get("address"), dict) else loc.get("address"),
                    (loc.get("address") or {}).get("addressLocality") if isinstance(loc.get("address"), dict) else None,
                ]))
            else:
                loc_str = str(loc) if loc else None
            org = item.get("organizer") or item.get("organiser")
            org_name = org.get("name") if isinstance(org, dict) else (str(org) if org else None)
            events.append({
                "title": str(item.get("name", "")),
                "url": str(item.get("url", source["url"])),
                "start_raw": str(item.get("startDate", "")),
                "end_raw": str(item.get("endDate", "")) if item.get("endDate") else None,
                "location_raw": loc_str,
                "description_raw": str(item.get("description", ""))[:500] if item.get("description") else None,
                "organiser_raw": org_name,
            })
    return events


# ─── Strategy: HTML ──────────────────────────────────────────────────────────

def scrape_html(source: dict) -> list[dict]:
    sel = source.get("selectors", {})
    if not sel:
        return []
    html = fetch(source["url"])
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    events = []

    for container in soup.select(sel["container"]):
        title_el = container.select_one(sel["title"])
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title:
            continue

        # URL
        href = ""
        if sel.get("url"):
            a = container.select_one(sel["url"])
            if a:
                href = a.get("href", "")
        if not href:
            a = container.select_one("a[href]")
            if a:
                href = a.get("href", "")
        url = resolve_url(href, source["url"])

        # Date — prefer datetime attr, fall back to text
        date_raw = ""
        if sel.get("date"):
            d_el = container.select_one(sel["date"])
            if d_el:
                date_raw = d_el.get("datetime") or d_el.get_text(strip=True)

        loc = None
        if sel.get("location"):
            loc_el = container.select_one(sel["location"])
            if loc_el:
                loc = loc_el.get_text(strip=True)

        desc = None
        if sel.get("description"):
            desc_el = container.select_one(sel["description"])
            if desc_el:
                desc = desc_el.get_text(strip=True)[:500]

        events.append({
            "title": title,
            "url": url or source["url"],
            "start_raw": date_raw,
            "end_raw": None,
            "location_raw": loc,
            "description_raw": desc,
            "organiser_raw": None,
        })

    return events


# ─── Strategy: iCal ──────────────────────────────────────────────────────────

def scrape_ical(source: dict) -> list[dict]:
    url = source.get("ical_url") or source["url"]
    text = fetch(url)
    if not text:
        return []

    events = []
    blocks = text.split("BEGIN:VEVENT")[1:]

    for block in blocks:
        def get(key):
            m = re.search(rf"^{key}[^:]*:(.+)$", block, re.MULTILINE)
            if not m:
                return None
            return m.group(1).strip().replace("\\n", "\n").replace("\\,", ",")

        title = get("SUMMARY")
        start_raw = get("DTSTART")
        if not title or not start_raw:
            continue

        events.append({
            "title": title,
            "url": get("URL") or source["url"],
            "start_raw": start_raw,
            "end_raw": get("DTEND"),
            "location_raw": get("LOCATION"),
            "description_raw": (get("DESCRIPTION") or "")[:500] or None,
            "organiser_raw": None,
        })

    return events


# ─── Normalise raw → Event ────────────────────────────────────────────────────

def normalise(raw: dict, source: dict) -> Event | None:
    title = (raw.get("title") or "").strip()
    if not title or len(title) < 3:
        return None

    start = parse_date(raw.get("start_raw"))
    if not start:
        return None

    # Skip events more than 2 years out (likely junk)
    if start > datetime.now(timezone.utc) + timedelta(days=730):
        return None

    end = parse_date(raw.get("end_raw"))
    now_iso = datetime.now(timezone.utc).isoformat()

    confidence = 0.5
    if raw.get("start_raw") and re.match(r"\d{4}-\d{2}-\d{2}", raw["start_raw"]):
        confidence += 0.2
    if raw.get("url") and raw["url"] != source["url"]:
        confidence += 0.1
    if raw.get("description_raw") and len(raw["description_raw"]) > 20:
        confidence += 0.1
    if len(title) < 5:
        confidence -= 0.2

    return Event(
        id=make_id(source["id"], title, start),
        title=re.sub(r"\s+", " ", title.strip("–— ")),
        url=raw.get("url") or source["url"],
        start_date=start.isoformat(),
        end_date=end.isoformat() if end else None,
        location=(raw.get("location_raw") or "").strip() or None,
        description=(raw.get("description_raw") or "").strip() or None,
        organiser=(raw.get("organiser_raw") or "").strip() or None,
        tags=infer_tags(title, raw.get("description_raw") or ""),
        country=source["country"],
        source_id=source["id"],
        source_name=source["name"],
        source_url=source["url"],
        online=is_online(raw.get("location_raw")),
        confidence=min(1.0, max(0.0, confidence)),
        first_seen=now_iso,
        last_seen=now_iso,
        all_source_ids=[source["id"]],
        all_source_names=[source["name"]],
    )


# ─── Deduplication ────────────────────────────────────────────────────────────

def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a):
        curr = [i + 1]
        for j, cb in enumerate(b):
            curr.append(min(prev[j + 1] + 1, curr[j] + 1, prev[j] + (ca != cb)))
        prev = curr
    return prev[-1]

def title_similarity(a: str, b: str) -> float:
    def norm(s):
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", "", s.lower())).strip()
    na, nb = norm(a), norm(b)
    max_len = max(len(na), len(nb))
    if max_len == 0:
        return 1.0
    return 1.0 - levenshtein(na, nb) / max_len

def dedup(events: list[Event]) -> list[Event]:
    FUZZY_THRESHOLD = 0.15
    DATE_WINDOW = 3  # days

    sorted_events = sorted(events, key=lambda e: -e.confidence)
    merged: list[Event] = []

    for ev in sorted_events:
        ev_date = datetime.fromisoformat(ev.start_date)
        ev_dedup = make_dedup_key(ev.title, ev_date)
        found = None

        for m in merged:
            m_date = datetime.fromisoformat(m.start_date)
            m_dedup = make_dedup_key(m.title, m_date)

            if ev_dedup == m_dedup:
                found = m
                break

            if abs((ev_date - m_date).days) <= DATE_WINDOW:
                if title_similarity(ev.title, m.title) >= (1 - FUZZY_THRESHOLD):
                    found = m
                    break

        if found:
            if ev.source_id not in found.all_source_ids:
                found.all_source_ids.append(ev.source_id)
                found.all_source_names.append(ev.source_name)
        else:
            merged.append(ev)

    return merged


# ─── Store ────────────────────────────────────────────────────────────────────

def read_events() -> list[Event]:
    if not EVENTS_PATH.exists():
        return []
    try:
        data = json.loads(EVENTS_PATH.read_text())
        return [Event(**e) for e in data.get("events", [])]
    except Exception:
        return []

def write_events(events: list[Event], total_sources: int):
    store = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_sources": total_sources,
        "events": [asdict(e) for e in events],
    }
    EVENTS_PATH.write_text(json.dumps(store, indent=2))

def merge_store(incoming: list[Event]) -> tuple[int, int, int]:
    existing = read_events()
    by_id = {e.id: e for e in existing}

    added = updated = 0
    for ev in incoming:
        if ev.id in by_id:
            old = by_id[ev.id]
            old.last_seen = ev.last_seen
            old.all_source_ids = list(set(old.all_source_ids + ev.all_source_ids))
            old.all_source_names = list(set(old.all_source_names + ev.all_source_names))
            updated += 1
        else:
            by_id[ev.id] = ev
            added += 1

    # Prune events more than 1 year in the past
    cutoff = datetime.now(timezone.utc) - timedelta(days=365)
    pruned = [e for e in by_id.values()
              if datetime.fromisoformat(e.start_date) >= cutoff]

    write_events(pruned, len(pruned))
    return added, updated, len(pruned)

def append_run(record: dict):
    runs = []
    if RUNS_PATH.exists():
        try:
            runs = json.loads(RUNS_PATH.read_text())
        except Exception:
            pass
    runs.insert(0, record)
    RUNS_PATH.write_text(json.dumps(runs[:100], indent=2))


# ─── Source registry — UK only ───────────────────────────────────────────────

SOURCES = [
    {
        "id": "bis",
        "name": "British Interplanetary Society",
        "url": "https://www.bis-space.com/events/",
        "country": "GB",
        "strategy": "html",
        "selectors": {
            "container": ".tribe-events-calendar-list__event-row, article.tribe_events",
            "title": ".tribe-events-calendar-list__event-title-link, h2.tribe-events-list-event-title a",
            "date": "time.tribe-events-calendar-list__event-datetime, abbr.tribe-events-abbr",
            "url": "a.tribe-events-calendar-list__event-title-link",
            "location": ".tribe-venue-location, .tribe-events-calendar-list__event-venue",
            "description": ".tribe-events-calendar-list__event-description p",
        },
        "enabled": True,
    },
    {
        "id": "ukastroverse",
        "name": "UK Astroverse",
        "url": "https://ukastroverse.com/events",
        "country": "GB",
        "strategy": "html",
        "selectors": {
            "container": ".tribe-events-calendar-list__event-row, article.type-tribe_events",
            "title": ".tribe-event-url, .tribe-events-calendar-list__event-title-link",
            "date": ".tribe-events-schedule abbr, time.tribe-events-calendar-list__event-datetime",
            "url": "a.tribe-event-url, .tribe-events-calendar-list__event-title-link",
            "location": ".tribe-venue, .tribe-events-calendar-list__event-venue",
            "description": ".tribe-events-calendar-list__event-description p",
        },
        "enabled": True,
    },
    {
        "id": "sa_catapult",
        "name": "Satellite Applications Catapult",
        "url": "https://sa.catapult.org.uk/upcoming-events/",
        "country": "GB",
        "strategy": "html",
        "selectors": {
            "container": ".card--event, .wp-block-catapult-event-card, article.event",
            "title": ".card__title, h3.event-title, .entry-title",
            "date": ".card__date, .event-date, time[datetime]",
            "url": "a.card__link, a[href*='event'], .card__title a",
            "location": ".card__location, .event-location, .location",
            "description": ".card__excerpt, .entry-summary",
        },
        "enabled": True,
    },
    {
        "id": "royalaero",
        "name": "Royal Aeronautical Society",
        "url": "https://www.aerosociety.com/events/",
        "country": "GB",
        "strategy": "html",
        "selectors": {
            "container": ".views-row, article.event-teaser, .event-listing__item",
            "title": "h3, h2.node__title, .event-teaser__title",
            "date": ".date-display-single, .field--name-field-event-date, time[datetime]",
            "url": "a[href]",
            "location": ".field--name-field-venue, .event-teaser__location",
            "description": ".field--name-body p, .event-teaser__summary",
        },
        "enabled": True,
    },
    {
        "id": "ras",
        "name": "Royal Astronomical Society",
        "url": "https://ras.ac.uk/events-and-meetings",
        "country": "GB",
        "strategy": "html",
        "selectors": {
            "container": ".views-row, .event-item, article.event",
            "title": "h3, h2, .event-title",
            "date": "time[datetime], .date-display-single",
            "url": "a[href]",
            "location": ".field-venue, .location",
        },
        "enabled": True,
    },
    {
        "id": "spaceenterprise",
        "name": "Space Enterprise UK",
        "url": "https://spaceenterprise.uk/events",
        "country": "GB",
        "strategy": "jsonld",
        "enabled": True,
    },
    {
        "id": "greenorbit",
        "name": "Green Orbit",
        "url": "https://greenorbit.space/events",
        "country": "GB",
        "strategy": "jsonld",
        "enabled": True,
    },
    {
        "id": "groundstation",
        "name": "Ground Station",
        "url": "https://groundstation.space/",
        "country": "GB",
        "strategy": "html",
        "selectors": {
            "container": ".eventlist-event, article.eventlist--upcoming",
            "title": ".eventlist-title, h1.eventlist-title",
            "date": "time.event-time-12hr, .eventlist-datetag-startdate, time[datetime]",
            "url": "a.eventlist-title-link, a[href*='event']",
            "location": ".eventlist-venue, .event-location",
            "description": ".eventlist-excerpt, .sqs-block-html p",
        },
        "enabled": True,
    },
    {
        "id": "uki_space",
        "name": "UKI Space",
        "url": "https://www.uki-space.org.uk/events",
        "country": "GB",
        "strategy": "jsonld",
        "enabled": True,
    },
    {
        "id": "ukspaceagency",
        "name": "UK Space Agency",
        "url": "https://www.gov.uk/search/news-and-communications?keywords=space+event&content_store_document_type=press_release,news_story",
        "country": "GB",
        "strategy": "html",
        "selectors": {
            "container": ".gem-c-document-list__item",
            "title": ".gem-c-document-list__item-title",
            "date": "time[datetime]",
            "url": "a[href]",
            "description": ".gem-c-document-list__item-description",
        },
        "enabled": True,
    },
]


# ─── Scrape dispatcher ───────────────────────────────────────────────────────

def scrape_source(source: dict) -> tuple[list[Event], list[str]]:
    errors = []
    raw_events = []

    try:
        strategy = source["strategy"]
        if strategy == "jsonld":
            raw_events = scrape_jsonld(source)
        elif strategy == "html":
            raw_events = scrape_html(source)
        elif strategy == "ical":
            raw_events = scrape_ical(source)
        else:
            errors.append(f"Unknown strategy: {strategy}")
    except Exception as e:
        errors.append(f"Scrape failed: {e}")

    events = []
    for raw in raw_events:
        try:
            ev = normalise(raw, source)
            if ev:
                events.append(ev)
        except Exception as e:
            errors.append(f"Normalise failed for '{raw.get('title', '?')}': {e}")

    return events, errors


# ─── iCal generator ──────────────────────────────────────────────────────────

def event_to_vevent(ev: Event) -> str:
    def ical_dt(iso: str) -> str:
        return iso.replace("-", "").replace(":", "").split(".")[0].rstrip("Z") + "Z"

    def escape(s: str) -> str:
        return s.replace("\\", "\\\\").replace(";", "\\;").replace(",", "\\,").replace("\n", "\\n")

    start = ical_dt(ev.start_date)
    if ev.end_date:
        end = ical_dt(ev.end_date)
    else:
        # Default: +1 day
        d = datetime.fromisoformat(ev.start_date) + timedelta(days=1)
        end = ical_dt(d.isoformat())

    lines = [
        "BEGIN:VEVENT",
        f"UID:{ev.id}@spacecalendar.supernovalabs",
        f"DTSTAMP:{ical_dt(datetime.now(timezone.utc).isoformat())}",
        f"DTSTART:{start}",
        f"DTEND:{end}",
        f"SUMMARY:{escape(ev.title)}",
        f"URL:{ev.url}",
    ]
    if ev.description:
        lines.append(f"DESCRIPTION:{escape(ev.description[:400])}")
    if ev.location:
        lines.append(f"LOCATION:{escape(ev.location)}")
    if ev.tags:
        lines.append(f"CATEGORIES:{','.join(t.upper() for t in ev.tags)}")
    lines.append("END:VEVENT")
    return "\r\n".join(lines) + "\r\n"

def generate_ical(events: list[Event], cal_name: str = "UK Space Events") -> str:
    now = datetime.now(timezone.utc)
    future = [e for e in events if datetime.fromisoformat(e.start_date) >= now]
    future.sort(key=lambda e: e.start_date)

    header = "\r\n".join([
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Space Calendar//supernovalabs.co.uk//EN",
        f"X-WR-CALNAME:{cal_name}",
        "X-WR-TIMEZONE:UTC",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALDESC:UK space industry events. Subscribe at https://space.supernovalabs.co.uk",
        "",
    ])

    body = "".join(event_to_vevent(e) for e in future)
    return header + body + "END:VCALENDAR\r\n"


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", help="Scrape a single source ID")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    t0 = time.time()
    print(f"\n🚀  Space Calendar scrape — {datetime.now(timezone.utc).isoformat()}")
    print(f"    Mode: {'DRY RUN' if args.dry_run else 'WRITE'}\n")

    sources = [s for s in SOURCES if s["enabled"]]
    if args.source:
        sources = [s for s in sources if s["id"] == args.source]
        if not sources:
            print(f"Unknown source: {args.source}")
            sys.exit(1)

    all_events: list[Event] = []
    all_errors: list[str] = []
    total_found = 0

    for source in sources:
        label = source["name"].ljust(45)
        print(f"  ⏳ {label}", end="", flush=True)
        st = time.time()

        events, errors = scrape_source(source)
        elapsed = time.time() - st
        total_found += len(events)
        all_events.extend(events)

        if errors:
            print(f"⚠  {len(events)} events, {len(errors)} errors ({elapsed:.1f}s)")
            for e in errors:
                print(f"     ↳ {e}")
            all_errors.extend(f"[{source['id']}] {e}" for e in errors)
        else:
            print(f"✓  {len(events)} events ({elapsed:.1f}s)")

    print(f"\n  📊 Deduplicating {total_found} raw events…")
    deduped = dedup(all_events)
    print(f"     → {len(deduped)} unique events after dedup")

    if not args.dry_run:
        added, updated, total = merge_store(deduped)
        print(f"\n  💾 Store: {added} new, {updated} updated, {total} total")

        # Write iCal
        store_events = read_events()
        ical_content = generate_ical(store_events)
        ical_path = DATA_DIR / "uk.ics"
        ical_path.write_text(ical_content)
        print(f"  📅 iCal written: {ical_path} ({len(store_events)} events)")

        append_run({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "sources": len(sources),
            "events_found": total_found,
            "events_added": added,
            "events_updated": updated,
            "errors": all_errors,
            "duration_ms": int((time.time() - t0) * 1000),
        })
    else:
        print("\n  [DRY RUN] — not writing")
        for e in deduped[:8]:
            print(f"  • {e.title[:60]} — {e.start_date[:10]} ({e.country})")

    duration = time.time() - t0
    print(f"\n✅  Done in {duration:.1f}s. Errors: {len(all_errors)}\n")
    if all_errors:
        sys.exit(1)

if __name__ == "__main__":
    main()
