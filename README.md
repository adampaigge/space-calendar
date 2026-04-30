# Space Calendar

UK space industry event aggregator. Static HTML frontend + Python scraper + GitHub Actions cron.

No framework. No build step. No hydration bugs.

## Structure

```
space-calendar/
├── index.html          ← entire frontend (single file, no deps)
├── scrape.py           ← Python scraper (requests + BeautifulSoup)
├── data/
│   ├── events.json     ← scraped + deduplicated events (committed by GHA)
│   ├── runs.json       ← scrape run history
│   └── uk.ics          ← iCal feed (committed by GHA)
└── .github/workflows/
    └── scrape.yml      ← cron every 6h, writes data/ and commits
```

## How it works

1. GitHub Actions runs `scrape.py` every 6 hours
2. Scraper fetches 10 UK space event sources (HTML, JSON-LD, iCal)
3. Events are normalised, deduplicated, and merged into `data/events.json`
4. A fresh `data/uk.ics` iCal feed is generated
5. GHA commits the updated data files
6. The static `index.html` fetches `data/events.json` on load — no server needed

## Deploy

### Vercel (recommended)
1. Fork/push this repo to GitHub
2. Import into Vercel — it auto-detects as a static site
3. Enable the GitHub Actions workflow (push → Actions tab → enable)
4. Trigger a manual scrape via Actions → `Scrape Space Events` → `Run workflow`

### GitHub Pages
1. Settings → Pages → Source: `main` branch, `/ (root)`
2. Enable the workflow

## Local scrape

```bash
pip install requests beautifulsoup4 lxml
python scrape.py --dry-run   # preview, no write
python scrape.py             # full scrape
python scrape.py --source bis  # single source
```

## Adding sources

Edit `SOURCES` in `scrape.py`. Strategies:
- `jsonld` — page has `<script type="application/ld+json">` with schema.org Event
- `html` — static HTML, add CSS selectors in `selectors` dict
- `ical` — source publishes a `.ics` feed, set `ical_url`

## iCal subscription

Subscribe in any calendar app:
- Google Calendar: paste the `data/uk.ics` URL into "Add from URL"
- Apple Calendar: File → New Calendar Subscription
- Outlook: File → Account Settings → Internet Calendars

The feed updates every 6 hours automatically.
