"""Local mock e-commerce site used for reproducible benchmarks.

Routes
------
/                       redirects to /products
/robots.txt             disallows /admin/
/products?page=N        listing page with product links + next page link
/product/<id>           product page (title, price, description, related links)
/admin/secret           a page robots.txt tells crawlers to avoid

Every page (except robots.txt) is delayed by a random amount to simulate
network latency. Page content is deterministic per product id, and prices
can be shifted between runs with --epoch so change detection has something
to find later (epoch 0 = base prices, epoch 1 = some prices changed, ...).

Run:  python mock_server.py
"""
import argparse
import asyncio
import math
import random

from aiohttp import web

import config

WORDS = (
    "alpha amber anchor arrow basil beacon birch bridge bronze canyon cedar "
    "cobalt coral crystal dawn delta drift ember falcon fern flint forest "
    "garden glacier granite harbor hazel horizon indigo island jade jasmine "
    "juniper kestrel lagoon lantern lava lilac lotus lumen maple marble meadow "
    "meteor mist mocha nectar nimbus oak ocean olive onyx opal orchid pebble "
    "pine plume prism quartz quill raven reef ridge river saffron sage "
    "sierra silver solar spruce summit tidal timber topaz tundra velvet violet "
    "willow zephyr zinc quality durable premium compact classic modern light "
    "robust elegant practical reliable versatile smooth sturdy sleek"
).split()


def base_price(product_id: int) -> float:
    rng = random.Random(f"base-{product_id}")
    return round(rng.uniform(5, 500), 2)


def product_price(product_id: int, epoch: int) -> float:
    """Price of a product at a given epoch. ~20% of products change per epoch."""
    price = base_price(product_id)
    if epoch <= 0:
        return price
    rng = random.Random(f"epoch-{product_id}-{epoch}")
    if rng.random() < 0.20:
        price = round(price * rng.uniform(0.7, 1.3), 2)
    return price


def make_description(product_id: int, page_kb: int) -> str:
    """Deterministic filler text of roughly page_kb kilobytes."""
    rng = random.Random(f"desc-{product_id}")
    target_chars = page_kb * 1024
    paragraphs = []
    size = 0
    while size < target_chars:
        sentence_count = rng.randint(3, 6)
        sentences = []
        for _ in range(sentence_count):
            words = rng.choices(WORDS, k=rng.randint(8, 16))
            sentences.append(" ".join(words).capitalize() + ".")
        paragraph = " ".join(sentences)
        paragraphs.append(f"<p>{paragraph}</p>")
        size += len(paragraph) + 7
    return "\n".join(paragraphs)


@web.middleware
async def latency_middleware(request: web.Request, handler):
    if request.path != "/robots.txt":
        cfg = request.app["cfg"]
        delay_ms = random.uniform(cfg.min_delay, cfg.max_delay)
        await asyncio.sleep(delay_ms / 1000)
    return await handler(request)


async def index(request: web.Request):
    raise web.HTTPFound("/products")


async def robots(request: web.Request):
    return web.Response(text="User-agent: *\nDisallow: /admin/\n")


async def admin_secret(request: web.Request):
    return web.Response(text="Nothing to see here. Crawlers should not be here.")


async def listing(request: web.Request):
    cfg = request.app["cfg"]
    try:
        page = int(request.query.get("page", "1"))
    except ValueError:
        raise web.HTTPBadRequest(text="invalid page")
    total_pages = math.ceil(cfg.products / config.PRODUCTS_PER_PAGE)
    if page < 1 or page > total_pages:
        raise web.HTTPNotFound(text="no such page")

    first = (page - 1) * config.PRODUCTS_PER_PAGE + 1
    last = min(page * config.PRODUCTS_PER_PAGE, cfg.products)

    items = []
    for pid in range(first, last + 1):
        price = product_price(pid, cfg.epoch)
        items.append(
            f'<li><a href="/product/{pid}">Product {pid}</a> '
            f'<span class="item-price">${price:.2f}</span></li>'
        )

    nav = []
    if page > 1:
        nav.append(f'<a href="/products?page={page - 1}">Previous</a>')
    if page < total_pages:
        nav.append(f'<a href="/products?page={page + 1}">Next</a>')
    nav.append('<a href="/admin/secret">Admin</a>')  # robots.txt disallows this

    html = (
        "<!doctype html><html><head><title>Products - page "
        f"{page}</title></head><body><h1>Product listing (page {page} of "
        f"{total_pages})</h1><ul>{''.join(items)}</ul>"
        f"<nav>{' | '.join(nav)}</nav></body></html>"
    )
    return web.Response(text=html, content_type="text/html")


async def product(request: web.Request):
    cfg = request.app["cfg"]
    try:
        pid = int(request.match_info["pid"])
    except ValueError:
        raise web.HTTPBadRequest(text="invalid id")
    if pid < 1 or pid > cfg.products:
        raise web.HTTPNotFound(text="no such product")

    price = product_price(pid, cfg.epoch)
    rng = random.Random(f"related-{pid}")
    related_ids = {rng.randint(1, cfg.products) for _ in range(5)} - {pid}
    related = "".join(
        f'<li><a href="/product/{rid}">Product {rid}</a></li>'
        for rid in sorted(related_ids)
    )
    description = make_description(pid, cfg.page_kb)

    html = (
        f"<!doctype html><html><head><title>Product {pid}</title></head><body>"
        f"<h1>Product {pid}</h1>"
        f'<span class="price">${price:.2f}</span>'
        f'<div class="description">{description}</div>'
        f"<h2>Related products</h2><ul>{related}</ul>"
        '<a href="/products?page=1">Back to listing</a>'
        "</body></html>"
    )
    return web.Response(text=html, content_type="text/html")


def build_app(args: argparse.Namespace) -> web.Application:
    app = web.Application(middlewares=[latency_middleware])
    app["cfg"] = args
    app.add_routes(
        [
            web.get("/", index),
            web.get("/robots.txt", robots),
            web.get("/products", listing),
            web.get("/product/{pid}", product),
            web.get("/admin/secret", admin_secret),
        ]
    )
    return app


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WebPulse mock e-commerce server")
    p.add_argument("--host", default=config.MOCK_HOST)
    p.add_argument("--port", type=int, default=config.MOCK_PORT)
    p.add_argument("--products", type=int, default=config.NUM_PRODUCTS)
    p.add_argument("--min-delay", type=float, default=config.MOCK_MIN_DELAY_MS,
                   help="minimum simulated latency in ms")
    p.add_argument("--max-delay", type=float, default=config.MOCK_MAX_DELAY_MS,
                   help="maximum simulated latency in ms")
    p.add_argument("--page-kb", type=int, default=config.MOCK_PAGE_KB,
                   help="approximate size of each product page in KB")
    p.add_argument("--epoch", type=int, default=0,
                   help="price version: 0 = base prices, higher = more changes")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    print(
        f"Mock server on http://{args.host}:{args.port} | products={args.products} "
        f"delay={args.min_delay:.0f}-{args.max_delay:.0f}ms page={args.page_kb}KB "
        f"epoch={args.epoch}"
    )
    web.run_app(build_app(args), host=args.host, port=args.port, print=None)