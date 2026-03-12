from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Optional
from urllib.parse import parse_qs, parse_qsl, quote_plus, unquote, urlencode, urljoin, urlparse, urlunparse

import feedparser
import httpx
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from newspaper import Article

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

CURRENT_YEAR = datetime.now(timezone.utc).year
MIN_ARTICLE_TEXT_LENGTH = 250
DISCOVERY_BUFFER_MULTIPLIER = 3
GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-IN&gl=IN&ceid=IN:en"
DUCKDUCKGO_HTML_SEARCH = "https://html.duckduckgo.com/html/"
TAVILY_SEARCH_API = "https://api.tavily.com/search"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-IN,en-US;q=0.9,en;q=0.8",
}

TRUSTED_PUBLICATIONS = {
    "economictimes.indiatimes.com": "Economic Times",
    "business-standard.com": "Business Standard",
    "businessstandard.com": "Business Standard",
    "livemint.com": "LiveMint",
    "moneycontrol.com": "Moneycontrol",
    "financialexpress.com": "Financial Express",
    "thehindu.com": "The Hindu",
    "thehindubusinessline.com": "BusinessLine",
    "reuters.com": "Reuters",
    "bloomberg.com": "Bloomberg",
    "businesstoday.in": "Business Today",
    "cnbctv18.com": "CNBC TV18",
    "ndtv.com": "NDTV",
    "indiatoday.in": "India Today",
    "timesofindia.indiatimes.com": "Times of India",
}

EXCLUDED_RESULT_DOMAINS = {
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "reddit.com",
    "twitter.com",
    "x.com",
    "youtube.com",
    "youtu.be",
    "thecompanycheck.com",
    "zaubacorp.com",
    "tofler.in",
    "tracxn.com",
    "screener.in",
    "crunchbase.com",
}

TRACKING_QUERY_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "ved",
    "usg",
}

DATE_META_KEYS = (
    "article:published_time",
    "og:published_time",
    "publish-date",
    "publish_date",
    "published_time",
    "published-date",
    "publication_date",
    "pubdate",
    "parsely-pub-date",
    "dc.date",
    "date",
)


def _quoted_phrase(value: str) -> str:
    cleaned = " ".join((value or "").split())
    return f'"{cleaned}"' if cleaned and not (cleaned.startswith('"') and cleaned.endswith('"')) else cleaned


def build_background_queries(company_name: str, promoter_name: str, sector: str) -> list[dict]:
    company = _quoted_phrase(company_name)
    promoter = _quoted_phrase(promoter_name or company_name)
    sector_name = " ".join((sector or "").split())
    return [
        {"query": f"{company} fraud scam controversy India", "scope": "company"},
        {"query": f"{company} insolvency default NPA bank India", "scope": "company"},
        {"query": f"{company} credit rating downgrade India", "scope": "company"},
        {"query": f"{company} auditor resignation qualification India", "scope": "company"},
        {"query": f"{company} NCLT insolvency proceedings India", "scope": "company"},
        {"query": f"{company} litigation court case India", "scope": "company"},
        {"query": f"{company} RBI penalty regulatory action India", "scope": "company"},
        {"query": f"{company} SEBI enforcement action India", "scope": "company"},
        {"query": f"{promoter} court case DRT NCLT India", "scope": "promoter"},
        {"query": f"{promoter} fraud allegation controversy India", "scope": "promoter"},
        {"query": f"{promoter} related party transaction controversy India", "scope": "promoter"},
        {"query": f"{promoter} wilful defaulter India", "scope": "promoter"},
        {"query": f"{promoter} criminal case FIR arrest India", "scope": "promoter"},
        {"query": f"{sector_name} RBI regulation circular {CURRENT_YEAR} India", "scope": "sector"},
        {"query": f"{sector_name} India outlook headwinds challenges {CURRENT_YEAR}", "scope": "sector"},
        {"query": f"{sector_name} India sector stress NPA {CURRENT_YEAR}", "scope": "sector"},
        {"query": f"{sector_name} India regulatory crackdown {CURRENT_YEAR}", "scope": "sector"},
        {"query": f"{sector_name} competitive threat disruption India", "scope": "sector"},
    ]


def build_live_refresh_queries(company_name: str, promoter_name: str) -> list[dict]:
    company = _quoted_phrase(company_name)
    promoter = _quoted_phrase(promoter_name or company_name)
    return [
        {"query": f"{company} latest news {CURRENT_YEAR}", "scope": "company"},
        {"query": f"{company} financial trouble default {CURRENT_YEAR}", "scope": "company"},
        {"query": f"{promoter} latest news controversy {CURRENT_YEAR}", "scope": "promoter"},
        {"query": f"{company} court order judgment {CURRENT_YEAR}", "scope": "company"},
        {"query": f"{company} credit rating action {CURRENT_YEAR}", "scope": "company"},
    ]


def _client(timeout: int = 20) -> httpx.Client:
    return httpx.Client(follow_redirects=True, timeout=timeout, headers=HEADERS)


def _tavily_key() -> Optional[str]:
    return os.getenv("TAVILY_API_KEY")


def _provider_order() -> list[str]:
    configured = [item.strip().lower() for item in os.getenv("NEWS_DISCOVERY_ORDER", "").split(",") if item.strip()]
    providers = [item for item in (configured or ["tavily", "duckduckgo", "google"]) if item in {"tavily", "duckduckgo", "google"}]
    if not _tavily_key():
        providers = [item for item in providers if item != "tavily"]
    return providers or ["duckduckgo", "google"]


def _coerce_date(value: object) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    try:
        return parsedate_to_datetime(text).date()
    except Exception:
        pass
    try:
        return date_parser.parse(text, fuzzy=True).date()
    except Exception:
        return None


def _clean_text(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text if len(text) >= MIN_ARTICLE_TEXT_LENGTH else None


def _normalize_url(url: str) -> str:
    if not url:
        return ""
    cleaned = unescape(url).strip()
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    parsed = urlparse(cleaned)
    if "duckduckgo.com" in parsed.netloc and parsed.path.startswith("/l/"):
        target = parse_qs(parsed.query).get("uddg", [None])[0]
        return _normalize_url(unquote(target)) if target else ""
    if not parsed.scheme:
        parsed = urlparse(urljoin("https://news.google.com", cleaned))
    netloc = parsed.netloc.lower().replace("www.", "")
    if not netloc or netloc in EXCLUDED_RESULT_DOMAINS or any(netloc.endswith(f".{d}") for d in EXCLUDED_RESULT_DOMAINS):
        return ""
    query = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k.lower() not in TRACKING_QUERY_PARAMS]
    return urlunparse((parsed.scheme or "https", netloc, parsed.path, parsed.params, urlencode(query, doseq=True), ""))


def _source_from_url(url: str) -> str:
    try:
        domain = urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return "Unknown"
    for known, name in TRUSTED_PUBLICATIONS.items():
        if domain == known or domain.endswith(f".{known}"):
            return name
    return domain or "Unknown"

def _resolve_google_news_url(gnews_url: str, timeout: int = 10) -> str:
    try:
        with _client(timeout=timeout) as client:
            response = client.get(gnews_url)
            final_url = _normalize_url(str(response.url))
            if final_url and "news.google.com" not in final_url:
                return final_url
            soup = BeautifulSoup(response.text, "lxml")
            canonical = soup.find("link", rel="canonical")
            if canonical and canonical.get("href"):
                canonical_url = _normalize_url(canonical["href"])
                if canonical_url and "news.google.com" not in canonical_url:
                    return canonical_url
    except Exception as exc:
        log.debug("Google redirect resolution failed for %s: %s", gnews_url, exc)
    return _normalize_url(gnews_url)


def fetch_google_news_rss(query: str, max_results: int = 8) -> list[dict]:
    rss_url = GOOGLE_NEWS_RSS.format(query=quote_plus(query))
    try:
        with _client(timeout=12) as client:
            response = client.get(rss_url)
            response.raise_for_status()
        feed = feedparser.parse(response.content)
        items = []
        for entry in feed.entries:
            raw_url = entry.get("link") or entry.get("id") or ""
            if not raw_url:
                continue
            url = _resolve_google_news_url(raw_url) if "news.google.com" in raw_url else _normalize_url(raw_url)
            if not url:
                continue
            source_obj = entry.get("source")
            source = source_obj.get("title", "") if isinstance(source_obj, dict) else ""
            items.append({
                "url": url,
                "headline": entry.get("title", "").strip(),
                "published_date": _coerce_date(entry.get("published") or entry.get("updated")),
                "source_publication": source or _source_from_url(url),
                "raw_content": None,
                "search_provider": "google",
            })
            if len(items) >= max_results:
                break
        log.info("Google RSS found %s candidates for: %s", len(items), query[:80])
        return items
    except Exception as exc:
        log.warning("Google RSS failed for '%s': %s", query, exc)
        return []


def fetch_duckduckgo_news(query: str, max_results: int = 8) -> list[dict]:
    try:
        with _client(timeout=12) as client:
            response = client.get(DUCKDUCKGO_HTML_SEARCH, params={"q": query, "kl": "in-en"})
            response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        items = []
        for container in soup.select("div.result"):
            anchor = container.select_one("a.result__a") or container.find("a", href=True)
            if not anchor or not anchor.get("href"):
                continue
            url = _normalize_url(urljoin(DUCKDUCKGO_HTML_SEARCH, anchor["href"]))
            if not url:
                continue
            items.append({
                "url": url,
                "headline": anchor.get_text(" ", strip=True),
                "published_date": None,
                "source_publication": _source_from_url(url),
                "raw_content": None,
                "search_provider": "duckduckgo",
            })
            if len(items) >= max_results:
                break
        log.info("DuckDuckGo found %s candidates for: %s", len(items), query[:80])
        return items
    except Exception as exc:
        log.warning("DuckDuckGo failed for '%s': %s", query, exc)
        return []


def fetch_tavily_news(query: str, max_results: int = 8, crawl_phase: str = "background_deep_crawl") -> list[dict]:
    api_key = _tavily_key()
    if not api_key:
        return []
    payload = {
        "query": query,
        "topic": "news",
        "search_depth": "advanced",
        "chunks_per_source": 3,
        "max_results": min(max_results, 20),
        "include_answer": False,
        "include_images": False,
        "include_raw_content": "text",
        "exact_match": '"' in query,
        "days": 30 if crawl_phase == "live_refresh" else 365,
    }
    include_domains = [d.strip() for d in os.getenv("NEWS_INCLUDE_DOMAINS", "").split(",") if d.strip()]
    if include_domains:
        payload["include_domains"] = include_domains
    try:
        with _client(timeout=20) as client:
            response = client.post(TAVILY_SEARCH_API, headers={"Authorization": f"Bearer {api_key}", **HEADERS}, json=payload)
            response.raise_for_status()
        data = response.json()
        items = []
        for result in data.get("results", []):
            url = _normalize_url(result.get("url", ""))
            if not url:
                continue
            items.append({
                "url": url,
                "headline": (result.get("title") or "").strip(),
                "published_date": _coerce_date(result.get("published_date")),
                "source_publication": _source_from_url(url),
                "raw_content": result.get("raw_content"),
                "search_provider": "tavily",
            })
        log.info("Tavily found %s candidates for: %s", len(items), query[:80])
        return items
    except Exception as exc:
        log.warning("Tavily search failed for '%s': %s", query, exc)
        return []


def fetch_news_candidates(query: str, max_results: int = 8, crawl_phase: str = "background_deep_crawl") -> list[dict]:
    target = max(max_results * DISCOVERY_BUFFER_MULTIPLIER, max_results)
    per_provider = min(max(max_results * 2, 6), 20)
    found, seen = [], set()
    for provider in _provider_order():
        if provider == "tavily":
            candidates = fetch_tavily_news(query, max_results=per_provider, crawl_phase=crawl_phase)
        elif provider == "duckduckgo":
            candidates = fetch_duckduckgo_news(query, max_results=per_provider)
        else:
            candidates = fetch_google_news_rss(query, max_results=per_provider)
        for item in candidates:
            url = _normalize_url(item.get("url", ""))
            if not url or url in seen:
                continue
            seen.add(url)
            item = dict(item)
            item["url"] = url
            item.setdefault("source_publication", _source_from_url(url))
            found.append(item)
            if len(found) >= target:
                return found
    return found


def _extract_date_from_soup(soup: BeautifulSoup) -> Optional[date]:
    for key in DATE_META_KEYS:
        for attr in ("property", "name", "itemprop"):
            node = soup.find("meta", attrs={attr: key})
            if node and node.get("content"):
                parsed = _coerce_date(node["content"])
                if parsed:
                    return parsed
    time_node = soup.find("time")
    if time_node:
        parsed = _coerce_date(time_node.get("datetime") or time_node.get_text(" ", strip=True))
        if parsed:
            return parsed
    for script in soup.find_all("script", attrs={"type": re.compile("ld\\+json", re.I)}):
        text = script.string or script.get_text(" ", strip=True)
        match = re.search(r'"datePublished"\s*:\s*"([^"]+)"', text) or re.search(r'"dateModified"\s*:\s*"([^"]+)"', text)
        if match:
            parsed = _coerce_date(match.group(1))
            if parsed:
                return parsed
    return None


def _extract_via_httpx_bs4(url: str, timeout: int = 15) -> dict:
    try:
        with _client(timeout=timeout) as client:
            response = client.get(url)
            response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        for tag in soup(["script", "style", "nav", "header", "footer", "aside", "form", "iframe", "noscript"]):
            tag.decompose()
        container = soup.find("article") or soup.find("main") or soup.body
        paragraphs = container.find_all("p") if container else []
        title = soup.title.get_text(" ", strip=True) if soup.title else None
        return {
            "text": _clean_text(" ".join(p.get_text(" ", strip=True) for p in paragraphs)),
            "headline": title,
            "published_date": _extract_date_from_soup(soup),
        }
    except Exception as exc:
        log.debug("BS4 fallback failed for %s: %s", url, exc)
        return {"text": None, "headline": None, "published_date": None}


def extract_article_details(url: str, timeout: int = 15, fallback_text: Optional[str] = None, fallback_headline: Optional[str] = None, fallback_published_date: Optional[date] = None) -> dict:
    details = {
        "text": _clean_text(fallback_text),
        "headline": fallback_headline.strip() if fallback_headline else None,
        "published_date": _coerce_date(fallback_published_date),
    }
    try:
        article = Article(url, language="en", fetch_images=False, browser_user_agent=HEADERS["User-Agent"])
        article.download()
        article.parse()
        if parsed := _clean_text(article.text):
            details["text"] = parsed
        if article.title and article.title.strip():
            details["headline"] = article.title.strip()
        if parsed_date := _coerce_date(article.publish_date):
            details["published_date"] = parsed_date
    except Exception as exc:
        log.debug("newspaper3k failed for %s: %s", url, exc)
    if not details["text"] or not details["headline"] or not details["published_date"]:
        fallback = _extract_via_httpx_bs4(url, timeout=timeout)
        details["text"] = details["text"] or fallback["text"]
        details["headline"] = details["headline"] or fallback["headline"]
        details["published_date"] = details["published_date"] or fallback["published_date"]
    return {
        "text": details["text"],
        "headline": details["headline"] or fallback_headline or "",
        "published_date": details["published_date"] or _coerce_date(fallback_published_date),
        "source_publication": _source_from_url(url),
    }


def extract_full_text(url: str, timeout: int = 15) -> Optional[str]:
    return extract_article_details(url, timeout=timeout).get("text")


def make_article_id(url: str) -> str:
    return "art_" + hashlib.md5(_normalize_url(url).encode("utf-8")).hexdigest()[:16]

class NewsCrawler:
    def __init__(self, db_conn=None, delay_seconds: float = 1.5):
        self.db = db_conn
        self.delay = delay_seconds
        self._seen_urls: set[str] = set()

    def _process_query(self, query_dict: dict, company_id: Optional[str], promoter_name: Optional[str], sector: Optional[str], crawl_phase: str, max_articles: int = 5) -> list[dict]:
        query = query_dict["query"]
        scope = query_dict["scope"]
        candidates = fetch_news_candidates(query, max_results=max_articles, crawl_phase=crawl_phase)
        records = []
        for item in candidates:
            url = _normalize_url(item["url"])
            if not url or url in self._seen_urls:
                continue
            self._seen_urls.add(url)
            time.sleep(self.delay)
            details = extract_article_details(url, fallback_text=item.get("raw_content"), fallback_headline=item.get("headline"), fallback_published_date=item.get("published_date"))
            if not details["text"]:
                continue
            records.append({
                "article_id": make_article_id(url),
                "company_id": company_id if scope == "company" else None,
                "promoter_name": promoter_name if scope == "promoter" else None,
                "sector": sector if scope == "sector" else None,
                "article_url": url,
                "source_publication": item.get("source_publication") or details["source_publication"],
                "published_date": details["published_date"] or item.get("published_date"),
                "article_headline": details["headline"] or item.get("headline", ""),
                "article_full_text": details["text"],
                "search_query_used": query,
                "crawl_phase": crawl_phase,
                "crawl_timestamp": datetime.now(timezone.utc),
            })
            log.info("Saved via %-10s [%s] %s", item.get("search_provider", "unknown"), records[-1]["source_publication"], records[-1]["article_headline"][:90])
            if len(records) >= max_articles:
                break
        return records

    def _write_to_db(self, records: list[dict]) -> None:
        if not self.db or not records:
            return
        cur = self.db.cursor()
        sql = """
            INSERT INTO news_articles_crawled (
                article_id, company_id, promoter_name, sector,
                article_url, source_publication, published_date,
                article_headline, article_full_text,
                search_query_used, crawl_phase, crawl_timestamp
            ) VALUES (
                %(article_id)s, %(company_id)s, %(promoter_name)s, %(sector)s,
                %(article_url)s, %(source_publication)s, %(published_date)s,
                %(article_headline)s, %(article_full_text)s,
                %(search_query_used)s, %(crawl_phase)s, %(crawl_timestamp)s
            )
            ON CONFLICT (article_id) DO NOTHING;
        """
        for record in records:
            cur.execute(sql, record)
        self.db.commit()
        cur.close()

    def run_background_deep_crawl(self, company_name: str, promoter_name: str, sector: str, company_id: Optional[str] = None, max_per_query: int = 5) -> list[dict]:
        log.info("=== BACKGROUND DEEP CRAWL: %s / %s / %s ===", company_name, promoter_name, sector)
        all_records = []
        queries = build_background_queries(company_name, promoter_name, sector)
        for index, query in enumerate(queries, 1):
            log.info("[%s/%s] Querying: %s", index, len(queries), query["query"])
            records = self._process_query(query, company_id or company_name.lower().replace(" ", "_"), promoter_name, sector, "background_deep_crawl", max_per_query)
            all_records.extend(records)
            self._write_to_db(records)
        return all_records

    def run_live_refresh(self, company_name: str, promoter_name: str, company_id: Optional[str] = None, max_per_query: int = 3) -> list[dict]:
        log.info("=== LIVE REFRESH: %s / %s ===", company_name, promoter_name)
        all_records = []
        queries = build_live_refresh_queries(company_name, promoter_name)
        for index, query in enumerate(queries, 1):
            log.info("[%s/%s] Live query: %s", index, len(queries), query["query"])
            records = self._process_query(query, company_id or company_name.lower().replace(" ", "_"), promoter_name, None, "live_refresh", max_per_query)
            all_records.extend(records)
            self._write_to_db(records)
        return all_records

    def get_summary(self, articles: list[dict]) -> dict:
        by_phase = {"background_deep_crawl": 0, "live_refresh": 0}
        by_scope = {"company": 0, "promoter": 0, "sector": 0}
        sources: dict[str, int] = {}
        for article in articles:
            if article.get("crawl_phase") in by_phase:
                by_phase[article["crawl_phase"]] += 1
            if article.get("company_id") and not article.get("promoter_name") and not article.get("sector"):
                by_scope["company"] += 1
            elif article.get("promoter_name"):
                by_scope["promoter"] += 1
            elif article.get("sector"):
                by_scope["sector"] += 1
            source = article.get("source_publication", "Unknown")
            sources[source] = sources.get(source, 0) + 1
        return {
            "total_articles": len(articles),
            "by_phase": by_phase,
            "by_scope": by_scope,
            "top_sources": dict(sorted(sources.items(), key=lambda item: -item[1])[:5]),
            "queries_run": list({article["search_query_used"] for article in articles}),
            "provider_order": _provider_order(),
            "tavily_enabled": bool(_tavily_key()),
        }


CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS news_articles_crawled (
    article_id          VARCHAR(64)  PRIMARY KEY,
    company_id          VARCHAR(255),
    promoter_name       VARCHAR(255),
    sector              VARCHAR(255),
    article_url         TEXT         NOT NULL,
    source_publication  VARCHAR(255),
    published_date      DATE,
    article_headline    TEXT,
    article_full_text   TEXT,
    search_query_used   TEXT,
    crawl_phase         VARCHAR(50)  CHECK (crawl_phase IN ('background_deep_crawl','live_refresh')),
    crawl_timestamp     TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS news_risk_signals (
    signal_id             VARCHAR(64)  PRIMARY KEY,
    article_id            VARCHAR(64)  REFERENCES news_articles_crawled(article_id),
    company_id            VARCHAR(255),
    signal_category       VARCHAR(100) CHECK (signal_category IN (
                                'promoter_fraud_allegation','promoter_legal_trouble',
                                'company_financial_stress','sector_regulatory_headwind',
                                'sector_competitive_threat','company_litigation_news',
                                'company_default_news','promoter_controversy'
                            )),
    severity_score        DECIMAL(3,2),
    is_high_severity      BOOLEAN      GENERATED ALWAYS AS (severity_score > 0.6) STORED,
    relevant_text_chunk   TEXT,
    finbert_risk_category VARCHAR(255)
);
"""


if __name__ == "__main__":
    print("Provider order:", ", ".join(_provider_order()))
    print("Tavily enabled:", "yes" if _tavily_key() else "no")
    crawler = NewsCrawler(db_conn=None, delay_seconds=1.0)
    articles = crawler.run_live_refresh(company_name="Byju's", promoter_name="Byju Raveendran")
    print(crawler.get_summary(articles))



