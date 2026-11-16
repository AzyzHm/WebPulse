"""HTML parsing, the CPU-bound stage of the pipeline.

parse_page() is deliberately a plain top-level function that takes strings
and returns a plain dict. That keeps it picklable, so the same function can
later be sent to a multiprocessing pool without changes.
"""
import re
from collections import Counter
from urllib.parse import urldefrag, urljoin, urlparse

from bs4 import BeautifulSoup

_PRICE_RE = re.compile(r"\d[\d,]*(?:\.\d+)?")
_WORD_RE = re.compile(r"[a-z']{3,}")


def parse_price(text: str | None) -> float | None:
    """Extract a number from text such as '$1,299.99'. Returns None if absent."""
    if not text:
        return None
    match = _PRICE_RE.search(text)
    if not match:
        return None
    return float(match.group().replace(",", ""))


def parse_page(html: str, url: str) -> dict:
    """Extract title, price, links and simple text stats from one HTML page."""
    soup = BeautifulSoup(html, "lxml")

    heading = soup.find("h1")
    if heading:
        title = heading.get_text(strip=True)
    elif soup.title and soup.title.string:
        title = soup.title.string.strip()
    else:
        title = ""

    price_tag = soup.select_one(".price")
    price = parse_price(price_tag.get_text(strip=True)) if price_tag else None

    links = set()
    for anchor in soup.find_all("a", href=True):
        absolute, _fragment = urldefrag(urljoin(url, anchor["href"]))
        if urlparse(absolute).scheme in ("http", "https"):
            links.add(absolute)

    text = soup.get_text(" ", strip=True)
    words = _WORD_RE.findall(text.lower())
    top_words = [word for word, _count in Counter(words).most_common(5)]

    return {
        "url": url,
        "title": title,
        "price": price,
        "links": sorted(links),
        "word_count": len(words),
        "top_words": top_words,
    }