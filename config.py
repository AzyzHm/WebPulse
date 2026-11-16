"""Central configuration for WebPulse.

Everything tunable lives here so the crawler, the mock server and (later)
the benchmark script all agree on the same values.
"""

# --- Mock server -----------------------------------------------------------
MOCK_HOST = "127.0.0.1"
MOCK_PORT = 8000
MOCK_BASE_URL = f"http://{MOCK_HOST}:{MOCK_PORT}"

NUM_PRODUCTS = 500          # how many fake product pages the server exposes
PRODUCTS_PER_PAGE = 20      # products per listing page
MOCK_MIN_DELAY_MS = 50      # simulated network latency (lower bound)
MOCK_MAX_DELAY_MS = 300     # simulated network latency (upper bound)
MOCK_PAGE_KB = 60           # approximate size of each product page body

# --- Crawler ---------------------------------------------------------------
SEEDS = [f"{MOCK_BASE_URL}/products?page=1"]
USER_AGENT = "WebPulseBot/0.1 (course project)"
REQUEST_TIMEOUT = 10        # seconds
MAX_PAGES = 200             # stop after this many successfully fetched pages
MAX_DEPTH = 4               # seed = depth 0
CONCURRENCY = 50            # max simultaneous requests (asyncio Semaphore size)
MAX_RETRIES = 3             # extra attempts after the first one fails
BACKOFF_BASE = 0.5          # seconds; delay = BACKOFF_BASE * 2**attempt + jitter